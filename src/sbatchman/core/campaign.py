"""
campaign.py
===========

Library for orchestrating multi-app, multi-cluster benchmark campaigns on
top of sbatchman.

This module is UI-agnostic. It can be driven by:
  - A plain CLI (call `run_campaign(...)` and let it log to the console).
  - An interactive TUI (pass an `event_queue` to receive structured
    `CampaignEvent`s, and a `CampaignControl` to pause/resume/cancel a
    running campaign from a background thread).

Design notes for callers that want live monitoring / control
--------------------------------------------------------------
- Create a `queue.Queue()` and a `CampaignControl()`.
- Run `run_campaign(..., event_queue=q, control=control)` in a background
  thread (it is a blocking, synchronous call).
- Drain `q` from your UI thread/event loop to receive `CampaignEvent`
  objects (structured progress) and log lines (as LOG events).
- Call `control.request_pause()` / `control.resume()` / `control.request_cancel()`
  at any time; the runner checks these at safe points (between apps,
  between steps, during job polling, and while a script subprocess runs).
- Cancellation raises internally and is handled gracefully: partial state
  is still written to the campaign log file so it can be resumed later.
"""

import json
import logging
import os
import shutil
import subprocess
import sys
import time
import threading
import queue as queue_module
from dataclasses import dataclass, asdict, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Callable
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


class CampaignCancelledError(CampaignRunnerError):
    """Raised internally when a user requests cancellation of a running campaign."""

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
    CANCELLED = "cancelled"


class EventType(Enum):
    """Types of structured events emitted while a campaign runs.

    Consumers (e.g. a TUI) should treat unknown event types as no-ops so
    this enum can grow without breaking older UIs.
    """

    LOG = "log"  # generic log line, data={"level": str, "message": str}

    CAMPAIGN_START = "campaign_start"      # data={"campaign_id", "clusters", "apps"}
    CAMPAIGN_END = "campaign_end"          # data={"success": bool}
    CAMPAIGN_CANCELLED = "campaign_cancelled"  # data={"message": str}

    CLUSTER_START = "cluster_start"        # data={"cluster": str}
    CLUSTER_END = "cluster_end"            # data={"cluster": str}

    APP_START = "app_start"                # data={"app": str, "cluster": str}
    APP_SKIPPED = "app_skipped"            # data={"app": str, "cluster": str, "reason": str}
    APP_END = "app_end"                    # data={"app": str, "cluster": str}

    STEP_START = "step_start"              # data={"app", "cluster", "step"}
    STEP_SKIPPED = "step_skipped"          # data={"app", "cluster", "step", "reason"}
    STEP_PROGRESS = "step_progress"        # data={"app","cluster","step","completed","total"}
    STEP_END = "step_end"                  # data={"app","cluster","step","status","duration"}

    PAUSED = "paused"                      # data={}
    RESUMED = "resumed"                    # data={}


# Terminal statuses reported by sbatchman for a submitted job
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

    status: str  # pending | running | completed | failed | skipped | cancelled
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


@dataclass
class CampaignEvent:
    """A single structured event describing campaign progress."""

    type: EventType
    timestamp: str
    data: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {"type": self.type.value, "timestamp": self.timestamp, "data": self.data}


class CampaignControl:
    """Thread-safe handles used to pause/resume/cancel a running campaign.

    Create one instance per campaign run and share it between the thread
    running `run_campaign(...)` and the UI thread that reacts to user input.
    """

    def __init__(self) -> None:
        self.pause_event = threading.Event()
        self.cancel_event = threading.Event()

    def request_pause(self) -> None:
        self.pause_event.set()

    def resume(self) -> None:
        self.pause_event.clear()

    def request_cancel(self) -> None:
        # Cancelling also releases a pause, so the runner can observe the
        # cancellation promptly instead of sleeping in the pause loop.
        self.cancel_event.set()
        self.pause_event.clear()

    @property
    def is_paused(self) -> bool:
        return self.pause_event.is_set()

    @property
    def is_cancelled(self) -> bool:
        return self.cancel_event.is_set()

    def reset(self) -> None:
        self.pause_event.clear()
        self.cancel_event.clear()


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


class QueueLoggingHandler(logging.Handler):
    """Logging handler that forwards log records as LOG CampaignEvents onto a queue.

    Used so a TUI can render a live log panel without needing to attach to
    Python's logging machinery itself.
    """

    LEVEL_NAMES = {
        logging.DEBUG: "debug",
        logging.INFO: "info",
        logging.WARNING: "warning",
        logging.ERROR: "error",
        logging.CRITICAL: "critical",
    }

    def __init__(self, event_queue: "queue_module.Queue"):
        super().__init__()
        self.event_queue = event_queue

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = self.format(record)
            event = CampaignEvent(
                type=EventType.LOG,
                timestamp=datetime.now().isoformat(),
                data={
                    "level": self.LEVEL_NAMES.get(record.levelno, "info"),
                    "message": message,
                },
            )
            self.event_queue.put(event)
        except Exception:
            self.handleError(record)


def setup_logger(
    name: str,
    verbose: bool = False,
    console_output: bool = True,
    event_queue: Optional["queue_module.Queue"] = None,
) -> logging.Logger:
    """Configure logger with appropriate verbosity.

    Args:
        name: logger name.
        verbose: enable DEBUG level.
        console_output: attach the Rich console handler (set False when a
            TUI owns the terminal and would rather receive log lines via
            `event_queue`).
        event_queue: if given, attach a handler that forwards every log
            record as a LOG CampaignEvent onto this queue.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)

    # Remove existing handlers to avoid duplicates
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    level = logging.DEBUG if verbose else logging.INFO

    if console_output:
        handler = RichLoggingHandler(console)
        handler.setLevel(level)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)

    if event_queue is not None:
        qhandler = QueueLoggingHandler(event_queue)
        qhandler.setLevel(level)
        qhandler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(qhandler)

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


def discover_clusters_from_config(config: CampaignConfig) -> List[str]:
    """Best-effort suggestion of cluster names referenced by a config's
    per-app whitelists. Returns an empty list if no app declares one
    (callers should fall back to letting the user type cluster names).
    """
    clusters: List[str] = []
    for app in config.apps:
        for c in app.cluster_whitelist:
            if c not in clusters:
                clusters.append(c)
    return sorted(clusters)


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


def get_failed_apps(state: ExecutionState) -> List[str]:
    """Return names of apps that have at least one non-completed,
    non-skipped step logged in `state` (i.e. good candidates for a
    "re-run failed apps" action)."""
    failed = []
    for app_name, app_steps in state.execution.items():
        app_failed = False
        for step_logs in app_steps.values():
            for log in step_logs.values():
                if log.status in (StepStatus.FAILED.value, StepStatus.CANCELLED.value):
                    app_failed = True
        if app_failed:
            failed.append(app_name)
    return failed


def build_status_rows(state: ExecutionState) -> List[Dict[str, Any]]:
    """Flatten an ExecutionState into a list of row dicts, convenient for
    rendering in a table:
    {app, step, cluster, status, duration_seconds, jobs_count,
     jobs_successful, jobs_failed, error_message}
    """
    rows: List[Dict[str, Any]] = []
    for app_name, app_steps in state.execution.items():
        for step_name, step_logs in app_steps.items():
            for cluster, log in step_logs.items():
                rows.append(
                    {
                        "app": app_name,
                        "step": step_name,
                        "cluster": cluster,
                        "status": log.status,
                        "duration_seconds": log.duration_seconds,
                        "jobs_count": log.jobs_count,
                        "jobs_successful": log.jobs_successful,
                        "jobs_failed": log.jobs_failed,
                        "error_message": log.error_message,
                    }
                )
    return rows


def list_campaign_logs(results_dir: Path) -> List[Path]:
    """List previous campaign log files in `results_dir`, most recent first."""
    results_dir = Path(results_dir)
    if not results_dir.exists():
        return []
    logs = sorted(
        results_dir.glob("campaign_log_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return logs


def load_campaign_log_summary(log_file: Path) -> Optional[Dict[str, Any]]:
    """Load a campaign log file and return a small summary dict suitable
    for a history list view, without needing to reconstruct the full
    ExecutionState:
    {campaign_id, campaign_start, campaign_end, clusters_processed,
     dry_run, total_steps, completed_steps, failed_steps, failed_apps}
    """
    try:
        with open(log_file, "r") as f:
            data = json.load(f)
    except Exception:
        return None

    total_steps = 0
    completed_steps = 0
    failed_steps = 0
    failed_apps = set()

    for app_name, app_data in data.get("execution", {}).items():
        for step_name, step_logs in app_data.get("steps", {}).items():
            for cluster, log in step_logs.items():
                total_steps += 1
                status = log.get("status")
                if status == StepStatus.COMPLETED.value:
                    completed_steps += 1
                elif status in (StepStatus.FAILED.value, StepStatus.CANCELLED.value):
                    failed_steps += 1
                    failed_apps.add(app_name)

    return {
        "log_file": str(log_file),
        "campaign_id": data.get("campaign_id"),
        "campaign_start": data.get("campaign_start"),
        "campaign_end": data.get("campaign_end"),
        "clusters_processed": data.get("clusters_processed", []),
        "dry_run": data.get("dry_run", False),
        "total_steps": total_steps,
        "completed_steps": completed_steps,
        "failed_steps": failed_steps,
        "failed_apps": sorted(failed_apps),
    }


# ============================================================================
# Job Monitoring
# ============================================================================


def poll_jobs_until_completion(
    jobs: List[sbm.Job],
    poll_interval: int = 10,
    max_wait: int = 86400,  # 24 hours
    logger: Optional[logging.Logger] = None,
    control: Optional[CampaignControl] = None,
    on_progress: Optional[Callable[[int, int], None]] = None,
) -> Tuple[bool, int, int, List[str]]:
    """
    Poll jobs until all reach TERMINAL_STATES.

    Args:
        control: if given, checked every iteration; a cancel request raises
            `CampaignCancelledError`, a pause request blocks (in short
            increments) until resumed or cancelled.
        on_progress: optional callback(completed, total) invoked every
            iteration so a caller can report live progress.

    Returns (all_succeeded, passed_count, failed_count, error_messages).
    """
    if not jobs:
        return True, 0, 0, []

    if logger is None:
        logger = logging.getLogger(__name__)

    start_time = time.time()
    elapsed = 0

    while elapsed < max_wait:
        if control is not None:
            if control.is_cancelled:
                raise CampaignCancelledError("Job polling cancelled by user")
            while control.is_paused and not control.is_cancelled:
                time.sleep(0.2)
            if control.is_cancelled:
                raise CampaignCancelledError("Job polling cancelled by user")

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

        completed = sum(1 for s in job_statuses.values() if s in TERMINAL_STATES)
        if on_progress is not None:
            on_progress(completed, len(jobs))

        if all_terminal:
            logger.info(f"[blue]├────[/blue] All {len(jobs)} job(s) reached terminal state")
            break

        # Log progress
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
        event_queue: Optional["queue_module.Queue"] = None,
        control: Optional[CampaignControl] = None,
        console_output: bool = True,
    ):
        self.config = config
        self.results_dir = results_dir
        self.log_file = log_file
        self.verbose = verbose
        self.dry_run = dry_run

        # Live monitoring / remote control (both optional; a plain CLI run
        # leaves these as None and behaves exactly as before).
        self.event_queue = event_queue
        self.control = control
        self._pause_emitted = False

        self.logger = setup_logger(
            __name__,
            verbose,
            console_output=console_output,
            event_queue=event_queue,
        )

        # Campaign state
        self.state = ExecutionState(
            campaign_id=datetime.now().strftime("%Y%m%d_%H%M%S"),
            campaign_start=datetime.now().isoformat(),
        )

        # Store original working directory
        self.original_cwd = Path.cwd()

    # ------------------------------------------------------------------
    # Event / control helpers
    # ------------------------------------------------------------------

    def _emit(self, event_type: EventType, **data: Any) -> None:
        if self.event_queue is None:
            return
        self.event_queue.put(
            CampaignEvent(type=event_type, timestamp=datetime.now().isoformat(), data=data)
        )

    def _check_control(self) -> None:
        """Raise if cancellation was requested; block (in short increments)
        while paused. Call this at safe points between units of work
        (before an app, before a step)."""
        if self.control is None:
            return

        if self.control.is_cancelled:
            raise CampaignCancelledError("Campaign cancelled by user")

        if self.control.is_paused:
            if not self._pause_emitted:
                self._emit(EventType.PAUSED)
                self.logger.info("[yellow]Campaign paused[/yellow]")
                self._pause_emitted = True
            while self.control.is_paused and not self.control.is_cancelled:
                time.sleep(0.2)
            if self.control.is_cancelled:
                raise CampaignCancelledError("Campaign cancelled by user")
            self._emit(EventType.RESUMED)
            self.logger.info("[green]Campaign resumed[/green]")
            self._pause_emitted = False

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

        self._emit(
            EventType.CAMPAIGN_START,
            campaign_id=self.state.campaign_id,
            clusters=clusters,
            apps=[a.name for a in self.config.apps],
        )

        try:
            self._print_campaign_header(clusters)

            for cluster in clusters:
                self._run_cluster(cluster, previous_state, force_apps)

            self._finalize()
            success = all(
                all(
                    log.status == StepStatus.COMPLETED.value
                    for step_logs in app_steps.values()
                    for log in step_logs.values()
                )
                for app_steps in self.state.execution.values()
            )
            self._emit(EventType.CAMPAIGN_END, success=success)
            return success

        except CampaignCancelledError as e:
            self.logger.warning(f"Campaign cancelled: {str(e)}")
            self._emit(EventType.CAMPAIGN_CANCELLED, message=str(e))
            self._finalize()
            self._emit(EventType.CAMPAIGN_END, success=False)
            return False

        except CampaignRunnerError as e:
            self.logger.error(f"Campaign failed: {str(e)}")
            if DEBUG_STACKTRACE:
                console.print_exception()
            self._finalize()
            self._emit(EventType.CAMPAIGN_END, success=False)
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
        self._emit(EventType.CLUSTER_START, cluster=cluster)

        # Set cluster in sbatchman global config
        self.logger.debug(f"Setting sbatchman cluster to: {cluster}")
        global_config.set_cluster_name(cluster)

        for app in self.config.apps:
            self._check_control()

            # Check if app should be skipped (cluster filtering)
            if not self._should_run_app_on_cluster(app, cluster):
                self.logger.info(f"[yellow][SKIPPED][/yellow] {app.name} (cluster filter)")
                self._emit(EventType.APP_SKIPPED, app=app.name, cluster=cluster, reason="cluster filter")
                continue

            # Check resume logic
            if previous_state and not self._should_run_app(
                app.name, cluster, previous_state, force_apps
            ):
                self.logger.info(f"[yellow][SKIPPED][/yellow] {app.name} (already completed)")
                self._emit(EventType.APP_SKIPPED, app=app.name, cluster=cluster, reason="already completed")
                continue

            try:
                self._run_app(app, cluster, previous_state)
            except CampaignCancelledError:
                raise
            except CampaignRunnerError as e:
                self.logger.error(f"App '{app.name}' failed on cluster '{cluster}': {str(e)}")
                raise

        self._emit(EventType.CLUSTER_END, cluster=cluster)

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
            sbm.reset_cached_sbatchman_home()  # Do not delete this

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
        self._emit(EventType.APP_START, app=app.name, cluster=cluster)

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
            self._check_control()

            # Check resume for step
            if previous_state and is_step_completed(
                app.name, step.name, cluster, previous_state
            ):
                self.logger.info(f"[blue]└──── {step.name} [yellow][SKIPPED] (already completed in previous run)[/yellow][/blue]")
                self._emit(EventType.STEP_SKIPPED, app=app.name, cluster=cluster, step=step.name, reason="already completed")
                # Copy previous log
                self.state.execution[app.name][step.name] = previous_state.execution[
                    app.name
                ][step.name].copy()
                continue

            # Execute step
            self.logger.info(f"[blue]├────────────── STEP [magenta]{step.name}[/magenta][/blue]")
            self._emit(EventType.STEP_START, app=app.name, cluster=cluster, step=step.name)
            try:
                success = self._execute_step(app, step, cluster)
            except CampaignCancelledError:
                raise
            except StepExecutionError as e:
                success = False

            log = self.state.execution[app.name][step.name][cluster]
            self._emit(
                EventType.STEP_END,
                app=app.name,
                cluster=cluster,
                step=step.name,
                status=log.status,
                duration=log.duration_seconds,
            )

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
        self._emit(EventType.APP_END, app=app.name, cluster=cluster)

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

                        def _on_progress(completed: int, total: int) -> None:
                            self._emit(
                                EventType.STEP_PROGRESS,
                                app=app.name,
                                cluster=cluster,
                                step=step.name,
                                completed=completed,
                                total=total,
                            )

                        all_success, passed, failed, errors = (
                            poll_jobs_until_completion(
                                jobs,
                                logger=self.logger,
                                control=self.control,
                                on_progress=_on_progress,
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

        except CampaignCancelledError as e:
            log.status = StepStatus.CANCELLED.value
            log.error_message = str(e)
            log.duration_seconds = time.time() - start_time
            self.logger.warning(f"Step '{step.name}' cancelled after {log.duration_seconds:.1f}s")
            raise

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

        Runs via Popen (rather than a single blocking `subprocess.run`) so
        that a cancellation request can terminate the child process instead
        of waiting for it to finish naturally. Pausing does not suspend an
        already-running script (that isn't generally safe to do to an
        arbitrary subprocess); it only takes effect between steps/apps and
        during job polling.
        """
        if self.dry_run:
            self.logger.info(f"[DRY-RUN] Would execute in {cwd}:\n{script}")
            return True, "", "", 0

        self.logger.debug(f"Executing script in working directory: {cwd}")
        self.logger.debug(f"Script content:\n{script}")

        timeout = 3600  # 1 hour timeout
        start = time.time()

        try:
            proc = subprocess.Popen(
                script,
                shell=True,
                cwd=cwd,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except Exception as e:
            self.logger.error(f"Script execution failed to start: {str(e)}")
            raise ScriptExecutionError(f"Script execution failed to start: {str(e)}")

        try:
            while True:
                if self.control is not None and self.control.is_cancelled:
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                    raise CampaignCancelledError("Script execution cancelled by user")

                ret = proc.poll()
                if ret is not None:
                    break

                if time.time() - start > timeout:
                    proc.kill()
                    self.logger.error(f"Script execution timed out (1 hour limit)")
                    raise ScriptExecutionError("Script execution timed out (1 hour limit)")

                time.sleep(0.2)

            stdout, stderr = proc.communicate()
            if stdout:
                print('-- STDOUT --')
                print(stdout)
            if stderr:
                print('-- STDERR --')
                print(stderr)

            success = proc.returncode == 0

            self.logger.debug(f"Script completed with exit code: {proc.returncode}")
            if stdout:
                self.logger.debug(f"Script stdout:\n{stdout}")
            if stderr:
                self.logger.debug(f"Script stderr:\n{stderr}")

            return success, stdout, stderr, proc.returncode

        except CampaignCancelledError:
            raise
        except ScriptExecutionError:
            raise
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
    event_queue: Optional["queue_module.Queue"] = None,
    control: Optional[CampaignControl] = None,
    console_output: bool = True,
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
        event_queue: optional queue.Queue to receive live CampaignEvents
            (for a TUI or other live monitor). Safe to leave None for a
            plain CLI run.
        control: optional CampaignControl to allow pausing/cancelling this
            run from another thread. Safe to leave None for a plain CLI run.
        console_output: whether to also print logs to the console via Rich.
            A TUI that owns the terminal should pass False and rely on
            `event_queue` LOG events instead.

    Returns:
        True if all succeeded, False otherwise
    """
    try:
        logger = setup_logger(__name__, verbose, console_output=console_output, event_queue=event_queue)
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
            event_queue=event_queue,
            control=control,
            console_output=console_output,
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


def run_campaign_with_existing_log(
    config_file: Path,
    log_file: Path,
    clusters: List[str],
    verbose: bool = False,
    dry_run: bool = False,
    resume_mode: str = "auto",
    force_apps: Optional[List[str]] = None,
    event_queue: Optional["queue_module.Queue"] = None,
    control: Optional[CampaignControl] = None,
    console_output: bool = True,
) -> bool:
    """
    Convenience wrapper to resume a campaign against a *specific* previous
    log file (rather than the "most recent log in results_dir" convention
    used implicitly by `run_campaign`). Useful for a TUI's "resume from
    history" flow where the user picks an exact run from a list.

    A NEW log file is written for this run (so history is preserved);
    resume logic just uses `log_file` to seed prior state.
    """
    logger = setup_logger(__name__, verbose, console_output=console_output, event_queue=event_queue)
    try:
        config = load_campaign_config(config_file)

        results_dir = Path(log_file).parent
        new_log_file = (
            results_dir
            / f"campaign_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )

        runner = CampaignRunner(
            config=config,
            results_dir=results_dir,
            log_file=Path(log_file),
            verbose=verbose,
            dry_run=dry_run,
            event_queue=event_queue,
            control=control,
            console_output=console_output,
        )
        # After construction, redirect where the *new* log gets written,
        # while resume-state recovery below still reads from `log_file`.
        previous_log_file = runner.log_file
        runner.log_file = new_log_file

        # Manually mirror run_campaign()'s resume-recovery step against the
        # chosen historical log file, then run with resume state applied.
        return _run_with_explicit_previous_log(runner, previous_log_file, clusters, resume_mode, force_apps or [])

    except CampaignRunnerError as e:
        logger.error(f"Error: {str(e)}")
        if DEBUG_STACKTRACE:
            console.print_exception()
        return False


def _run_with_explicit_previous_log(
    runner: CampaignRunner,
    previous_log_file: Path,
    clusters: List[str],
    resume_mode: str,
    force_apps: List[str],
) -> bool:
    """Internal helper: run `runner` resuming from `previous_log_file`
    regardless of what `runner.log_file` (the *new* output log) is set to.
    """
    runner.state.clusters_processed = clusters
    runner.state.resume_mode = resume_mode
    runner.state.force_apps = force_apps
    runner.state.dry_run = runner.dry_run

    previous_state = recover_execution_state(previous_log_file)

    runner._emit(
        EventType.CAMPAIGN_START,
        campaign_id=runner.state.campaign_id,
        clusters=clusters,
        apps=[a.name for a in runner.config.apps],
    )

    try:
        runner._print_campaign_header(clusters)
        for cluster in clusters:
            runner._run_cluster(cluster, previous_state, force_apps)
        runner._finalize()
        success = all(
            all(
                log.status == StepStatus.COMPLETED.value
                for step_logs in app_steps.values()
                for log in step_logs.values()
            )
            for app_steps in runner.state.execution.values()
        )
        runner._emit(EventType.CAMPAIGN_END, success=success)
        return success
    except CampaignCancelledError as e:
        runner.logger.warning(f"Campaign cancelled: {str(e)}")
        runner._emit(EventType.CAMPAIGN_CANCELLED, message=str(e))
        runner._finalize()
        runner._emit(EventType.CAMPAIGN_END, success=False)
        return False
    except CampaignRunnerError as e:
        runner.logger.error(f"Campaign failed: {str(e)}")
        runner._finalize()
        runner._emit(EventType.CAMPAIGN_END, success=False)
        return False