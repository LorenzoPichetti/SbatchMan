import json
import logging
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, asdict, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum
from rich.console import Console

import yaml
import sbatchman as sbm
from sbatchman.config import global_config
from sbatchman.core.jobs_manager import job_by_id

# ============================================================================
# Configuration
# ============================================================================

# Global flag to enable/disable stack trace printing
DEBUG_STACKTRACE = False

console = Console(width=shutil.get_terminal_size().columns)


# ============================================================================
# Custom Exception Classes
# ============================================================================


class CampaignRunnerError(Exception):
    """Base exception for campaign runner."""

    pass


class ConfigurationError(CampaignRunnerError):
    """Raised when configuration is invalid."""

    pass


class FileNotFoundError(CampaignRunnerError):
    """Raised when required file is not found."""

    pass


class StepExecutionError(CampaignRunnerError):
    """Raised when a step execution fails."""

    pass


class ScriptExecutionError(StepExecutionError):
    """Raised when script execution fails."""

    pass


class JobExecutionError(StepExecutionError):
    """Raised when job execution fails."""

    pass


class ProjectInitializationError(CampaignRunnerError):
    """Raised when project initialization fails."""

    pass


class StateRecoveryError(CampaignRunnerError):
    """Raised when state recovery fails."""

    pass


# ============================================================================
# Enums & Constants
# ============================================================================


class OnFailsPolicy(Enum):
    """Policy for handling step failures."""

    TERMINATE = "terminate"  # Exit campaign immediately
    CONTINUE = "continue"  # Move to next step in same app
    SKIP = "skip"  # Move to next app


class StepStatus(Enum):
    """Status of a step execution."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


TERMINAL_STATES = {
    "COMPLETED",
    "FAILED",
    "CANCELLED",
    "TIMEOUT",
    "FAILED_SUBMISSION",
}


# ============================================================================
# Data Models
# ============================================================================


@dataclass
class StepConfig:
    """Configuration for a single step."""

    name: str
    script: Optional[str] = None  # Bash script to run before jobs
    jobs: Optional[str] = None    # Path to jobs YAML file
    on_fails: str = "terminate"   # terminate | continue | skip

    def __post_init__(self):
        """Validate on_fails value."""
        if self.on_fails not in [p.value for p in OnFailsPolicy]:
            raise ConfigurationError(f"Invalid on_fails: {self.on_fails}")


@dataclass
class AppConfig:
    """Configuration for a single app."""

    name: str
    dir: Path
    blocking: bool = False
    cluster_whitelist: List[str] = field(default_factory=list)
    cluster_blacklist: List[str] = field(default_factory=list)
    configs: List[str] = field(default_factory=list)
    results_dir: Optional[str] = None
    steps: List[StepConfig] = field(default_factory=list)


@dataclass
class CampaignConfig:
    """Root configuration for campaign."""

    apps: List[AppConfig] = field(default_factory=list)


@dataclass
class StepExecutionLog:
    """Log entry for single step execution on single cluster."""

    status: str  # pending | running | completed | failed | skipped
    error_message: Optional[str] = None

    # Script execution
    script_executed: bool = False
    script_stdout: str = ""
    script_stderr: str = ""
    script_exit_code: Optional[int] = None

    # Job execution
    jobs_launched: bool = False
    jobs_count: int = 0
    jobs_successful: int = 0
    jobs_failed: int = 0
    job_errors: List[str] = field(default_factory=list)

    duration_seconds: float = 0.0


@dataclass
class ExecutionState:
    """Tracks execution state for recovery/resume."""

    campaign_id: str
    campaign_start: str
    campaign_end: Optional[str] = None
    clusters_processed: List[str] = field(default_factory=list)
    dry_run: bool = False
    resume_mode: str = "prompt"
    force_apps: List[str] = field(default_factory=list)

    # Structure: {app_name: {step_name: {cluster_name: StepExecutionLog}}}
    execution: Dict[str, Dict[str, Dict[str, StepExecutionLog]]] = field(
        default_factory=lambda: {}
    )


# ============================================================================
# Logging Setup
# ============================================================================


class RichLoggingHandler(logging.Handler):
    """Custom logging handler that uses Rich Console for proper color rendering."""

    def __init__(self, console: Console):
        """Initialize handler with Rich Console."""
        super().__init__()
        self.console = console

    def emit(self, record: logging.LogRecord) -> None:
        """Emit a log record using Rich Console."""
        try:
            message = self.format(record)
            
            # Format based on level
            if record.levelno == logging.DEBUG:
                self.console.print(f"[dim][DEBUG][/dim] {message}")
            elif record.levelno == logging.INFO:
                self.console.print(f"[blue][INFO][/blue] {message}")
            elif record.levelno == logging.WARNING:
                self.console.print(f"[yellow][WARN][/yellow] {message}")
            elif record.levelno == logging.ERROR:
                self.console.print(f"[red][ERROR][/red] {message}")
            elif record.levelno == logging.CRITICAL:
                self.console.print(f"[red bold][CRITICAL][/red bold] {message}")
            else:
                self.console.print(message)
        except Exception:
            self.handleError(record)


def setup_logger(name: str, verbose: bool = False) -> logging.Logger:
    """Configure logger with appropriate verbosity and Rich Console output."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)

    # Remove existing handlers to avoid duplicates
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Create Rich handler
    handler = RichLoggingHandler(console)
    handler.setLevel(logging.DEBUG if verbose else logging.INFO)
    handler.setFormatter(logging.Formatter("%(message)s"))

    logger.addHandler(handler)
    logger.propagate = False

    return logger


# ============================================================================
# YAML Parsing
# ============================================================================


def load_campaign_config(file: Path) -> CampaignConfig:
    """Load and parse campaign YAML file."""
    logger = logging.getLogger(__name__)
    try:
        logger.debug(f"Loading campaign config from: {file}")
        
        if not file.exists():
            raise FileNotFoundError(f"Campaign config file not found: {file}")

        with open(file, "r") as f:
            data = yaml.safe_load(f)
            logger.debug(f"Parsed YAML file")

        if not data or "apps" not in data:
            raise ConfigurationError("Invalid campaign config: missing 'apps' section")

        apps = []
        for app_data in data["apps"]:
            logger.debug(f"Loading app config: {app_data.get('name', 'unknown')}")
            app = load_app_config(app_data)
            apps.append(app)

        config = CampaignConfig(apps=apps)
        logger.debug(f"Successfully loaded campaign config with {len(apps)} app(s)")
        return config

    except CampaignRunnerError:
        raise
    except Exception as e:
        logger.error(f"Failed to load campaign config: {str(e)}")
        raise ConfigurationError(f"Failed to load campaign config: {str(e)}")


def load_app_config(app_data: dict) -> AppConfig:
    """Load single app configuration."""
    logger = logging.getLogger(__name__)
    try:
        name = app_data.get("name")
        if not name:
            raise ConfigurationError("App config missing 'name'")

        logger.debug(f"Loading app '{name}'")

        dir_str = app_data.get("dir")
        if not dir_str:
            raise ConfigurationError(f"App '{name}' missing 'dir'")

        # Convert to Path
        dir_path = Path(dir_str)
        if not dir_path.is_absolute():
            dir_path = Path.cwd() / dir_path

        logger.debug(f"App directory: {dir_path}")

        # Parse steps
        steps_data = app_data.get("steps", [])
        logger.debug(f"Loading {len(steps_data)} step(s)")
        steps = [load_step_config(s) for s in steps_data]

        if not steps:
            raise ConfigurationError(f"App '{name}' has no steps")

        # Parse configs
        configs_data = app_data.get("configs", [])
        if isinstance(configs_data, str):
            configs_data = [configs_data]

        logger.debug(f"Configs: {configs_data}")

        app = AppConfig(
            name=name,
            dir=dir_path,
            blocking=app_data.get("blocking", False),
            cluster_whitelist=app_data.get("cluster_whitelist", []),
            cluster_blacklist=app_data.get("cluster_blacklist", []),
            configs=configs_data,
            results_dir=app_data.get("results_dir"),
            steps=steps,
        )
        
        logger.debug(f"App '{name}' loaded successfully")
        return app

    except CampaignRunnerError:
        raise
    except Exception as e:
        logger.error(f"Failed to load app config: {str(e)}")
        raise ConfigurationError(f"Failed to load app config: {str(e)}")


def load_step_config(step_data: dict) -> StepConfig:
    """Load single step configuration."""
    logger = logging.getLogger(__name__)
    try:
        name = step_data.get("name")
        if not name:
            raise ConfigurationError("Step config missing 'name'")

        logger.debug(f"Loading step '{name}'")

        script = step_data.get("script")
        jobs = step_data.get("jobs")
        
        if not jobs and not script:
            raise ConfigurationError(f"Step '{name}' missing both 'script' and 'jobs'")

        on_fails = step_data.get("on_fails", "terminate")
        
        logger.debug(f"Script: {script if script else 'None'}")
        logger.debug(f"Jobs: {jobs}")
        logger.debug(f"On fails: {on_fails}")

        return StepConfig(
            name=name,
            script=script,
            jobs=jobs,
            on_fails=on_fails,
        )

    except CampaignRunnerError:
        raise
    except Exception as e:
        logger.error(f"Failed to load step config: {str(e)}")
        raise ConfigurationError(f"Failed to load step config: {str(e)}")


# ============================================================================
# State Recovery
# ============================================================================


def recover_execution_state(
    log_file: Path,
) -> Optional[ExecutionState]:
    """Load previous execution state from log file."""
    logger = logging.getLogger(__name__)
    
    if not log_file.exists():
        logger.debug(f"No previous log file found at {log_file}")
        return None

    try:
        logger.info(f"Attempting to recover execution state from {log_file}")
        
        with open(log_file, "r") as f:
            data = json.load(f)
            logger.debug(f"Log file loaded successfully")

        # Reconstruct ExecutionState
        state = ExecutionState(
            campaign_id=data.get("campaign_id"),
            campaign_start=data.get("campaign_start"),
            campaign_end=data.get("campaign_end"),
            clusters_processed=data.get("clusters_processed", []),
            dry_run=data.get("dry_run", False),
            resume_mode=data.get("resume_mode", "prompt"),
            force_apps=data.get("force_apps", []),
        )

        # Reconstruct execution dict
        for app_name, app_data in data.get("execution", {}).items():
            state.execution[app_name] = {}
            for step_name, step_data in app_data.get("steps", {}).items():
                state.execution[app_name][step_name] = {}
                for cluster_name, log_data in step_data.items():
                    state.execution[app_name][step_name][cluster_name] = (
                        StepExecutionLog(**log_data)
                    )

        logger.info(f"Successfully recovered execution state for campaign {state.campaign_id}")
        return state

    except Exception as e:
        if DEBUG_STACKTRACE:
            console.print_exception()
        logger.warning(f"Failed to recover state from {log_file}: {str(e)}")
        return None


def is_app_completed(
    app_name: str,
    cluster: str,
    state: ExecutionState,
) -> bool:
    """Check if app completed all steps on cluster."""
    if app_name not in state.execution:
        return False

    app_steps = state.execution[app_name]

    for step_logs in app_steps.values():
        if cluster not in step_logs:
            return False

        log = step_logs[cluster]
        if log.status != StepStatus.COMPLETED.value:
            return False

    return True


def is_step_completed(
    app_name: str,
    step_name: str,
    cluster: str,
    state: ExecutionState,
) -> bool:
    """Check if specific step completed on cluster."""
    if app_name not in state.execution:
        return False

    if step_name not in state.execution[app_name]:
        return False

    step_logs = state.execution[app_name][step_name]
    if cluster not in step_logs:
        return False

    return step_logs[cluster].status == StepStatus.COMPLETED.value


# ============================================================================
# Job Monitoring
# ============================================================================


def poll_jobs_until_completion(
    jobs: List[sbm.Job],
    poll_interval: int = 10,
    max_wait: int = 86400,  # 24 hours
    logger: Optional[logging.Logger] = None,
) -> Tuple[bool, int, int, List[str]]:
    """
    Poll jobs until all reach TERMINAL_STATES.
    Returns (all_succeeded, passed_count, failed_count, error_messages).
    """
    if not jobs:
        return True, 0, 0, []

    if logger is None:
        logger = logging.getLogger(__name__)

    start_time = time.time()
    elapsed = 0

    while elapsed < max_wait:
        # Refresh job statuses
        job_statuses = {}
        for job in jobs:
            # Re-fetch job from sbatchman
            try:
                updated = job_by_id(job.job_id)
                job_statuses[job.job_id] = updated.status
            except Exception as e:
                logger.warning(f"Failed to fetch status for job {job.job_id}: {e}")
                job_statuses[job.job_id] = "UNKNOWN"

        # Check if all terminal
        all_terminal = all(
            status in TERMINAL_STATES for status in job_statuses.values()
        )

        if all_terminal:
            logger.info(f"[blue]├────[/blue] All {len(jobs)} job(s) reached terminal state")
            break

        # Log progress
        completed = sum(1 for s in job_statuses.values() if s in TERMINAL_STATES)
        logger.debug(f"[blue]├──────[/blue] Job progress: {completed}/{len(jobs)} terminal ({elapsed}/{max_wait}s)")

        time.sleep(poll_interval)
        elapsed += poll_interval

    if elapsed >= max_wait:
        logger.warning(f"Job polling reached max wait time of {max_wait}s")

    # Analyze final results
    passed = 0
    failed = 0
    errors = []

    for job in jobs:
        try:
            updated = job_by_id(job.job_id)
            if updated.status == "COMPLETED":
                passed += 1
            else:
                failed += 1
                errors.append(f"Job {job.job_id}: {updated.status}")
        except Exception as e:
            failed += 1
            errors.append(f"Job {job.job_id}: {str(e)}")

    logger.info(f"[blue]├────[/blue] Job polling completed: {passed} completed, {failed} NOT completed")
    return failed == 0, passed, failed, errors


# ============================================================================
# Project Initialization
# ============================================================================


def initialize_project(app_dir: Path, dry_run: bool = False) -> None:
    """
    Initialize SbatchMan project in the app directory.
    
    Args:
        app_dir: Directory where to initialize the project
        dry_run: If True, don't actually initialize
        
    Raises:
        ProjectInitializationError: If initialization fails
    """
    logger = logging.getLogger(__name__)
    try:
        logger.debug(f"Initializing project in {app_dir}")
        
        if dry_run:
            logger.debug(f"[DRY-RUN] Would initialize project in {app_dir}")
            return

        sbm.init_project(app_dir, no_logo=True)
        logger.debug(f"Project initialized successfully in {app_dir}")

    except sbm.ProjectExistsError:
        pass
    except Exception as e:
        logger.error(f"Failed to initialize project in {app_dir}: {str(e)}")
        raise ProjectInitializationError(
            f"Failed to initialize project in {app_dir}: {str(e)}"
        )


# ============================================================================
# Main Campaign Runner
# ============================================================================


class CampaignRunner:
    """Orchestrates benchmark campaign execution."""

    def __init__(
        self,
        config: CampaignConfig,
        results_dir: Path,
        log_file: Path,
        verbose: bool = False,
        dry_run: bool = False,
    ):
        self.config = config
        self.results_dir = results_dir
        self.log_file = log_file
        self.verbose = verbose
        self.dry_run = dry_run
        self.logger = setup_logger(__name__, verbose)

        # Campaign state
        self.state = ExecutionState(
            campaign_id=datetime.now().strftime("%Y%m%d_%H%M%S"),
            campaign_start=datetime.now().isoformat(),
        )
        
        # Store original working directory
        self.original_cwd = Path.cwd()

    def run_campaign(
        self,
        clusters: List[str],
        resume: bool = False,
        resume_mode: str = "prompt",
        force_apps: Optional[List[str]] = None,
    ) -> bool:
        """
        Execute campaign across clusters.
        Returns True if all apps succeeded.
        """
        if force_apps is None:
            force_apps = []
 
        self.state.clusters_processed = clusters
        self.state.resume_mode = resume_mode
        self.state.force_apps = force_apps
        self.state.dry_run = self.dry_run
 
        self.logger.debug(f"Clusters to process: {clusters}")
        self.logger.debug(f"Resume mode: {resume_mode}, Force apps: {force_apps}")
 
        # Recover previous state if resuming
        previous_state = None
        if resume and self.log_file.exists():
            self.logger.info(f"Resume mode enabled, attempting to recover previous state")
            try:
                previous_state = recover_execution_state(self.log_file)
                if previous_state:
                    self.logger.info(f"Successfully recovered execution state")
            except StateRecoveryError as e:
                self.logger.warning(f"Failed to recover state: {str(e)}")
 
        try:
            self._print_campaign_header(clusters)
 
            for cluster in clusters:
                self._run_cluster(cluster, previous_state, force_apps)
 
            self._finalize()
            return all(
                all(
                    log.status == StepStatus.COMPLETED.value
                    for step_logs in app_steps.values()
                    for log in step_logs.values()
                )
                for app_steps in self.state.execution.values()
            )
 
        except CampaignRunnerError as e:
            self.logger.error(f"Campaign failed: {str(e)}")
            if DEBUG_STACKTRACE:
                console.print_exception()
            self._finalize()
            return False

    def _run_cluster(
        self,
        cluster: str,
        previous_state: Optional[ExecutionState],
        force_apps: List[str],
    ) -> None:
        """Execute all apps on a single cluster."""
        self.logger.info('')
        self.logger.info('')
        self.logger.info(f"[magenta]╔═══════════════════════════════════════════════════════════════════════════════╗[/magenta]")
        self.logger.info(f"[magenta]║ Running on cluster [green]{cluster:<58}[/green] ║[/magenta]")
        self.logger.info(f"[magenta]╚═══════════════════════════════════════════════════════════════════════════════╝[/magenta]")

        # Set cluster in sbatchman global config
        self.logger.debug(f"Setting sbatchman cluster to: {cluster}")
        global_config.set_cluster_name(cluster)

        for app in self.config.apps:
            # Check if app should be skipped (cluster filtering)
            if not self._should_run_app_on_cluster(app, cluster):
                self.logger.info(f"[yellow][SKIPPED][/yellow] {app.name} (cluster filter)")
                continue

            # Check resume logic
            if previous_state and not self._should_run_app(
                app.name, cluster, previous_state, force_apps
            ):
                self.logger.info(f"[yellow][SKIPPED][/yellow] {app.name} (already completed)")
                continue

            try:
                self._run_app(app, cluster, previous_state)
            except CampaignRunnerError as e:
                self.logger.error(f"App '{app.name}' failed on cluster '{cluster}': {str(e)}")
                raise

    def _should_run_app_on_cluster(self, app: AppConfig, cluster: str) -> bool:
        """Check if app should run on cluster (whitelist/blacklist filtering)."""
        if app.cluster_whitelist and cluster not in app.cluster_whitelist:
            return False
        if app.cluster_blacklist and cluster in app.cluster_blacklist:
            return False
        return True

    def _should_run_app(
        self,
        app_name: str,
        cluster: str,
        previous_state: ExecutionState,
        force_apps: List[str],
    ) -> bool:
        """Check if app should run (resume logic)."""
        # Force override
        if app_name in force_apps:
            return True

        # Check if completed
        if is_app_completed(app_name, cluster, previous_state):
            return False

        return True

    def _setup_app_for_cluster(
        self,
        app: AppConfig,
        cluster: str,
    ) -> None:
        """
        Setup app for cluster: initialize project and create configs once.
        This runs before any steps are executed.
        """
        self.logger.debug(f"[blue]├────[/blue] Setting up app '{app.name}' for cluster '{cluster}'")
        start_time = time.time()

        try:
            # Ensure we're in the app directory
            self.logger.debug(f"[blue]├────[/blue] Changing working directory to: {app.dir.resolve().absolute()}")
            os.chdir(app.dir)
            sbm.reset_cached_sbatchman_home() # Do not delete this

            # Initialize project
            self.logger.debug(f"Initializing project for app '{app.name}'")
            try:
                initialize_project(app.dir, dry_run=self.dry_run)
            except ProjectInitializationError as e:
                self.logger.error(f"Project initialization failed: {str(e)}")
                raise e

            # Create configs once for all steps
            if app.configs:
                self.logger.info(f"[blue]├────[/blue] ⚙️ Creating configs from files [green]'{app.configs}'[/green]")
                config_paths = []
                for config_file in app.configs:
                    config_path = app.dir / config_file
                    self.logger.debug(f"Processing config: {config_file}")
                    if not config_path.exists():
                        self.logger.error(f"Config file not found: {config_path}")
                        raise FileNotFoundError(
                            f"Config file not found: {config_path}"
                        )
                    if not self.dry_run:
                        self.logger.debug(f"Creating config from {config_file}")
                        sbm.create_configs_from_file(config_file, overwrite=True)
                    config_paths.append(config_path)
                self.logger.debug(f"[blue]├────[/blue] ✅ Configs created successfully")
            
            duration = time.time() - start_time
            self.logger.debug(f"[blue]├────[/blue] Setup completed in {duration:.1f}s")

        except CampaignRunnerError as e:
            error_msg = str(e)
            self.logger.error(f"App setup failed after {time.time() - start_time:.1f}s: {error_msg}")
            raise
        finally:
            # Always restore original working directory
            self.logger.debug(f"Restoring working directory to: {self.original_cwd}")
            os.chdir(self.original_cwd)

    def _run_app(
        self,
        app: AppConfig,
        cluster: str,
        previous_state: Optional[ExecutionState],
    ) -> None:
        """Execute single app on single cluster."""
        self.logger.info('')
        self.logger.info("[blue]┌" + "─" * 25 + f" APPLICATION [magenta]{app.name}[/magenta] ({app.dir.resolve().absolute()}) " + "─" * 25 + "[/blue]")

        # Initialize app in execution state
        if app.name not in self.state.execution:
            self.state.execution[app.name] = {}

        # Setup app for cluster (project init + config creation) - done once before all steps
        try:
            self._setup_app_for_cluster(app, cluster)
        except CampaignRunnerError as e:
            self.logger.error(f"App '{app.name}' setup failed on cluster '{cluster}': {str(e)}")
            raise

        for step in app.steps:
            # Check resume for step
            if previous_state and is_step_completed(
                app.name, step.name, cluster, previous_state
            ):
                self.logger.info(f"[blue]└──── {step.name} [yellow][SKIPPED] (already completed in previous run)[/yellow][/blue]")
                # Copy previous log
                self.state.execution[app.name][step.name] = previous_state.execution[
                    app.name
                ][step.name].copy()
                continue

            # Execute step
            self.logger.info(f"[blue]├────────────── STEP [magenta]{step.name}[/magenta][/blue]")
            try:
                success = self._execute_step(app, step, cluster)
            except StepExecutionError as e:
                success = False

            if not success:
                # Handle failure per policy
                policy = OnFailsPolicy(step.on_fails)
                self.logger.warning(f"Step '{step.name}' failed, applying policy: {policy.value}")

                if policy == OnFailsPolicy.TERMINATE:
                    self.logger.error(f"TERMINATE policy active, exiting campaign")
                    raise StepExecutionError(
                        f"Step [magenta]{step.name}[/magenta] failed with TERMINATE policy"
                    )
                elif policy == OnFailsPolicy.SKIP:
                    self.logger.info(f"SKIP policy active, moving to next app")
                    break
                # else: CONTINUE, move to next step

        self.logger.info("[blue]└" + "─" * 25 + f" APPLICATION [magenta]{app.name}[/magenta] COMPLETED " + "─" * (25 - len(' COMPLETED')) + "[/blue]")

    def _execute_step(
        self,
        app: AppConfig,
        step: StepConfig,
        cluster: str,
    ) -> bool:
        """
        Execute single step (script + jobs).
        Returns True if successful.
        """
        self.logger.debug(f"Starting step execution for '{step.name}'")
        start_time = time.time()

        # Initialize step log
        if step.name not in self.state.execution[app.name]:
            self.state.execution[app.name][step.name] = {}

        log = StepExecutionLog(status=StepStatus.RUNNING.value)
        self.state.execution[app.name][step.name][cluster] = log

        try:
            # Ensure we're in the app directory for all operations
            self.logger.debug(f"[blue]├────[/blue] Changing working directory to: {app.dir.resolve().absolute()}")
            os.chdir(app.dir)

            # Step 1: Execute script (if any)
            if step.script:
                self.logger.info(f"[blue]├────[/blue] 📜 Executing script:\n{step.script}")
                try:
                    success, stdout, stderr, exit_code = self._execute_script(
                        step.script, app.dir
                    )

                    log.script_executed = True
                    log.script_stdout = stdout
                    log.script_stderr = stderr
                    log.script_exit_code = exit_code

                    self.logger.debug(f"Script exit code: {exit_code}")

                    if not success:
                        self.logger.error(f"Script failed with exit code {exit_code}")
                        raise ScriptExecutionError(
                            f"Script failed with exit code {exit_code}"
                        )
                    self.logger.info(f"[blue]├────[/blue] ✅ Script executed successfully")
                except ScriptExecutionError:
                    raise

            # Step 2: Launch and monitor jobs
            if step.jobs:
                try:
                    # Launch jobs
                    jobs_path = app.dir / step.jobs
                    if not jobs_path.exists():
                        self.logger.error(f"Jobs file not found: {jobs_path}")
                        raise FileNotFoundError(f"Jobs file not found: {jobs_path}")

                    if self.dry_run:
                        self.logger.info(
                            f"[DRY-RUN] Would launch jobs from {step.jobs}"
                        )
                        jobs = []
                    else:
                        self.logger.info(f"[blue]├────[/blue] 🚀 Running jobs from file '{step.jobs}'")
                        jobs = sbm.launch_jobs_from_file(step.jobs, force=False)
                        self.logger.info(f"[blue]├────[/blue] Launched {len(jobs)} job(s)")

                    log.jobs_launched = True
                    log.jobs_count = len(jobs)

                    # Poll until completion
                    if jobs and not self.dry_run:
                        self.logger.info(f"[blue]├────[/blue] ☁️ Polling {len(jobs)} job(s) until completion")
                        all_success, passed, failed, errors = (
                            poll_jobs_until_completion(
                                jobs,
                                logger=self.logger,
                            )
                        )
                        log.jobs_successful = passed
                        log.jobs_failed = failed
                        log.job_errors = errors

                        self.logger.info(f"[blue]├────[/blue] ☁️ Jobs completed: {passed} passed, {failed} failed")

                        if not all_success:
                            self.logger.error(f"❌ Some jobs failed")
                            raise JobExecutionError(
                                f"❌ Jobs failed: {failed} failed, {passed} passed"
                            )
                        self.logger.info(f"[blue]├────[/blue] ✅ All jobs completed successfully")

                except (FileNotFoundError, JobExecutionError):
                    raise

            # Success
            log.status = StepStatus.COMPLETED.value
            log.duration_seconds = time.time() - start_time
            self.logger.info(f"[blue]├────────────── COMPLETED STEP [magenta]{step.name}[/magenta] in {log.duration_seconds:.1f}s[/blue]")
            return True

        except CampaignRunnerError as e:
            error_msg = str(e)
            log.status = StepStatus.FAILED.value
            log.error_message = error_msg
            log.duration_seconds = time.time() - start_time
            self.logger.error(f"Step '{step.name}' failed after {log.duration_seconds:.1f}s: {error_msg}")
            raise StepExecutionError(error_msg)
        finally:
            # Always restore original working directory
            self.logger.debug(f"Restoring working directory to: {self.original_cwd}")
            os.chdir(self.original_cwd)

    def _execute_script(
        self,
        script: str,
        cwd: Path,
    ) -> Tuple[bool, str, str, int]:
        """
        Execute bash script in the specified working directory.
        Returns (success, stdout, stderr, exit_code).
        """
        if self.dry_run:
            self.logger.info(f"[DRY-RUN] Would execute in {cwd}:\n{script}")
            return True, "", "", 0

        self.logger.debug(f"Executing script in working directory: {cwd}")
        self.logger.debug(f"Script content:\n{script}")

        try:
            result = subprocess.run(
                script,
                shell=True,
                capture_output=True,
                text=True,
                cwd=cwd,
                timeout=3600,  # 1 hour timeout
            )
            if result.stdout:
                print('-- STDOUT --')
                print(result.stdout)
            if result.stderr:
                print('-- STDERR --')
                print(result.stderr)

            success = result.returncode == 0
            
            self.logger.debug(f"Script completed with exit code: {result.returncode}")
            if result.stdout:
                self.logger.debug(f"Script stdout:\n{result.stdout}")
            if result.stderr:
                self.logger.debug(f"Script stderr:\n{result.stderr}")
            
            return success, result.stdout, result.stderr, result.returncode

        except subprocess.TimeoutExpired:
            self.logger.error(f"Script execution timed out (1 hour limit)")
            raise ScriptExecutionError("Script execution timed out (1 hour limit)")
        except Exception as e:
            self.logger.error(f"Script execution failed: {str(e)}")
            raise ScriptExecutionError(f"Script execution failed: {str(e)}")

    def _finalize(self) -> None:
        """Write log and print summary."""
        self.state.campaign_end = datetime.now().isoformat()

        # Build JSON log
        log_data = {
            "campaign_id": self.state.campaign_id,
            "campaign_start": self.state.campaign_start,
            "campaign_end": self.state.campaign_end,
            "clusters_processed": self.state.clusters_processed,
            "dry_run": self.state.dry_run,
            "resume_mode": self.state.resume_mode,
            "force_apps": self.state.force_apps,
            "execution": {},
        }

        # Serialize execution state
        for app_name, app_steps in self.state.execution.items():
            log_data["execution"][app_name] = {"steps": {}}
            for step_name, step_logs in app_steps.items():
                log_data["execution"][app_name]["steps"][step_name] = {}
                for cluster, log in step_logs.items():
                    log_data["execution"][app_name]["steps"][step_name][cluster] = (
                        asdict(log)
                    )

        # Write log
        if not self.dry_run:
            self.logger.info(f"Writing campaign log to '{self.log_file}'")
            self.log_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.log_file, "w") as f:
                json.dump(log_data, f, indent=2)
            self.logger.debug(f"Campaign log written successfully")

        # Print summary
        self._print_summary(log_data)

    def _print_campaign_header(self, clusters: List[str]) -> None:
        """Print campaign header."""
        self.logger.info("[yellow]" + "═" * 80 + "[/yellow]")
        self.logger.info("[yellow]CAMPAIGN EXECUTION LOG[/yellow]")
        self.logger.info("[yellow]" + "═" * 80 + "[/yellow]")
        self.logger.info(f"Campaign ID: {self.state.campaign_id}")
        self.logger.info(f"Start time: {self.state.campaign_start}")
        self.logger.info(f"Logs saved to: {self.results_dir.resolve().absolute()}")
        self.logger.info(f"Clusters: {', '.join(clusters)}")
        self.logger.info(f"Apps: {len(self.config.apps)} ({', '.join(a.name for a in self.config.apps)})")
        self.logger.info(f"Dry-run: {'Yes' if self.dry_run else 'No'}")
        self.logger.info(f"Resume: {self.state.resume_mode}")
        self.logger.info("[yellow]" + "═" * 80 + "[/yellow]")

    def _print_summary(self, log_data: dict) -> None:
        """Print execution summary with job statistics per cluster and app."""
        
        # Collect statistics
        total_steps = 0
        completed_steps = 0
        failed_steps = 0
        total_jobs = 0
        successful_jobs = 0
        failed_jobs = 0
        
        # Structure: {cluster: {app: {steps_count, jobs_total, jobs_success, jobs_failed}}}
        summary_by_cluster = {}

        for app_name, app_steps in log_data["execution"].items():
            for step_logs in app_steps["steps"].values():
                for cluster, log in step_logs.items():
                    total_steps += 1
                    if log["status"] == "completed":
                        completed_steps += 1
                    elif log["status"] == "failed":
                        failed_steps += 1
                    
                    # Aggregate jobs
                    total_jobs += log["jobs_count"]
                    successful_jobs += log["jobs_successful"]
                    failed_jobs += log["jobs_failed"]
                    
                    # Build cluster summary
                    if cluster not in summary_by_cluster:
                        summary_by_cluster[cluster] = {}
                    if app_name not in summary_by_cluster[cluster]:
                        summary_by_cluster[cluster][app_name] = {
                            "steps_count": 0,
                            "steps_completed": 0,
                            "steps_failed": 0,
                            "jobs_total": 0,
                            "jobs_successful": 0,
                            "jobs_failed": 0,
                        }
                    
                    summary_by_cluster[cluster][app_name]["steps_count"] += 1
                    if log["status"] == "completed":
                        summary_by_cluster[cluster][app_name]["steps_completed"] += 1
                    elif log["status"] == "failed":
                        summary_by_cluster[cluster][app_name]["steps_failed"] += 1
                    
                    summary_by_cluster[cluster][app_name]["jobs_total"] += log["jobs_count"]
                    summary_by_cluster[cluster][app_name]["jobs_successful"] += log["jobs_successful"]
                    summary_by_cluster[cluster][app_name]["jobs_failed"] += log["jobs_failed"]

        # Print overall summary
        self.logger.info("")
        self.logger.info("[yellow]" + "═" * 80 + "[/yellow]")
        self.logger.info("[yellow]EXECUTION SUMMARY[/yellow]")
        self.logger.info("[yellow]" + "═" * 80 + "[/yellow]")
        self.logger.info(f"Total steps: {total_steps}")
        self.logger.info(f"Completed: {completed_steps}")
        self.logger.info(f"Failed: {failed_steps}")
        self.logger.info(f"Total jobs: {total_jobs}")
        self.logger.info(f"Successful: {successful_jobs}")
        self.logger.info(f"Failed: {failed_jobs}")
        self.logger.info("")
        self.logger.info("")

        # Print per-cluster per-app summary
        for cluster in sorted(summary_by_cluster.keys()):
            self.logger.info(f"CLUSTER '{cluster}'")
            self.logger.info("─" * 80)
            
            for app_name in sorted(summary_by_cluster[cluster].keys()):
                stats = summary_by_cluster[cluster][app_name]
                steps_info = f"Steps: {stats['steps_completed']}/{stats['steps_count']}"
                jobs_info = f"Jobs: {stats['jobs_successful']}/{stats['jobs_total']}"
                
                status = "✅" if stats['steps_failed'] == 0 and stats['jobs_failed'] == 0 else "❌"
                self.logger.info(f"{status} {app_name:<30} {steps_info:<20} {jobs_info}")
            
            self.logger.info("")
            self.logger.info("")

        self.logger.info("[yellow]" + "═" * 80 + "[/yellow]")


# ============================================================================
# Entry Point
# ============================================================================


def run_campaign(
    config_file: Path,
    results_dir: Path,
    clusters: List[str],
    verbose: bool = False,
    dry_run: bool = False,
    resume: bool = False,
    resume_mode: str = "prompt",
    force_apps: Optional[List[str]] = None,
) -> bool:
    """
    Run campaign from YAML config.

    Args:
        config_file: Path to campaign YAML
        results_dir: Directory to store results
        clusters: List of clusters to run on
        verbose: Enable verbose logging
        dry_run: Don't execute commands, only log
        resume: Resume from previous run
        resume_mode: "auto" (skip completed) or "prompt" (ask user)
        force_apps: Force re-run specific apps

    Returns:
        True if all succeeded, False otherwise
    """
    try:
        logger = setup_logger(__name__, verbose)
        logger.debug(f"Config file: {config_file}")
        logger.debug(f"Results dir: {results_dir}")
        logger.debug(f"Clusters: {clusters}")
        logger.debug(f"Verbose: {verbose}, Dry-run: {dry_run}, Resume: {resume}")

        # Load config
        config = load_campaign_config(config_file)

        # Set up results directory
        results_dir = Path(results_dir)
        results_dir.mkdir(parents=True, exist_ok=True)

        # Log file
        log_file = (
            results_dir
            / f"campaign_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        logger.debug(f"Log file will be written to: {log_file}")

        # Run campaign
        runner = CampaignRunner(
            config=config,
            results_dir=results_dir,
            log_file=log_file,
            verbose=verbose,
            dry_run=dry_run,
        )

        success = runner.run_campaign(
            clusters=clusters,
            resume=resume,
            resume_mode=resume_mode,
            force_apps=force_apps or [],
        )

        if success:
            logger.info(f"Campaign completed [green]successfully[/green]")
        else:
            logger.info(f"Campaign completed [red]with failures[/red]")

        return success

    except ConfigurationError as e:
        logger = logging.getLogger(__name__)
        logger.error(f"Configuration Error: {str(e)}")
        if DEBUG_STACKTRACE:
            console.print_exception()
        return False
    except CampaignRunnerError as e:
        logger = logging.getLogger(__name__)
        logger.error(f"Error: {str(e)}")
        if DEBUG_STACKTRACE:
            console.print_exception()
        return False