"""
campaign.py
===========

Library for orchestrating multi-application campaigns across clusters
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
import hashlib
import argparse
import copy
import logging
import os
import shutil
import subprocess
import shlex
import sys
import time
import threading
import re
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue as queue_module
from dataclasses import dataclass, asdict, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Callable
from enum import Enum
from rich.console import Console
from rich.text import Text
from rich.markup import escape as rich_escape

import yaml
import sbatchman as sbm
from sbatchman.config import global_config, project_config

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

    CAMPAIGN_START = "campaign_start"      # data={"campaign_id", "campaign_name", "clusters", "apps"}
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
    STEP_NEEDS_ATTENTION = "step_needs_attention"  # data={"app", "cluster", "step", "error"}

    PAUSED = "paused"                      # data={}
    RESUMED = "resumed"                    # data={}


# Terminal statuses reported by sbatchman for a submitted job
TERMINAL_STATES = {
    "COMPLETED",
    "FAILED",
    "CANCELLED",
    "TIMEOUT",
    "FAILED_SUBMISSION",
    "CHECK_FAIL",
    "MISSING",
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
    jobs_args: Any = field(default_factory=list)  # list[str] or {default/cluster: list[str]}
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
    cluster_whitelist: List[str] = field(default_factory=list)
    configs: List[str] = field(default_factory=list)
    steps: List[StepConfig] = field(default_factory=list)


@dataclass
class CampaignConfig:
    """Root configuration for campaign."""

    apps: List[AppConfig] = field(default_factory=list)
    parallel_clusters: bool = False
    name: Optional[str] = None


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
    jobs: List[Dict[str, Any]] = field(default_factory=list)
    job_attempts: List[Dict[str, Any]] = field(default_factory=list)
    attempt_count: int = 0
    manual_resolution: Optional[str] = None
    resolution_history: List[Dict[str, Any]] = field(default_factory=list)
    definition_hash: Optional[str] = None

    duration_seconds: float = 0.0


@dataclass
class ExecutionState:
    """Tracks execution state for recovery/resume."""

    campaign_id: str
    campaign_start: str
    campaign_name: Optional[str] = None
    config_file: Optional[str] = None
    config_sha256: Optional[str] = None
    application_projects: Dict[str, str] = field(default_factory=dict)
    campaign_end: Optional[str] = None
    clusters_processed: List[str] = field(default_factory=list)
    dry_run: bool = False
    resume_mode: str = "prompt"
    force_apps: List[str] = field(default_factory=list)
    selection: Dict[str, Any] = field(default_factory=dict)

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
        self.resolutions: "queue_module.Queue[str]" = queue_module.Queue()

    def request_pause(self) -> None:
        self.pause_event.set()

    def resume(self) -> None:
        self.pause_event.clear()

    def request_cancel(self) -> None:
        # Cancelling also releases a pause, so the runner can observe the
        # cancellation promptly instead of sleeping in the pause loop.
        self.cancel_event.set()
        self.pause_event.clear()
        self.resolutions.put("stop")

    def request_resolution(self, action: str) -> None:
        if action not in {"retry", "continue", "skip", "stop"}:
            raise ValueError(f"Unknown campaign resolution: {action}")
        self.resolutions.put(action)

    def wait_for_resolution(self) -> str:
        while not self.is_cancelled:
            try:
                action = self.resolutions.get(timeout=0.2)
            except queue_module.Empty:
                continue
            if action == "stop" or self.is_cancelled:
                return "stop"
            return action
        return "stop"

    @property
    def is_paused(self) -> bool:
        return self.pause_event.is_set()

    @property
    def is_cancelled(self) -> bool:
        return self.cancel_event.is_set()

    def reset(self) -> None:
        self.pause_event.clear()
        self.cancel_event.clear()
        while True:
            try:
                self.resolutions.get_nowait()
            except queue_module.Empty:
                break


# ============================================================================
# Logging Setup
# ============================================================================


class RichLoggingHandler(logging.Handler):
    """Custom logging handler that uses Rich Console for proper color rendering."""

    def __init__(self, console: Console):
        """Initialize handler with Rich Console."""
        super().__init__()
        self.console = console
        self._output_lock = threading.RLock()
        self._has_output = False
        self._last_blank = False

    def emit(self, record: logging.LogRecord) -> None:
        """Emit a log record using Rich Console."""
        try:
            message = clean_campaign_log_message(self.format(record))
            if not message:
                with self._output_lock:
                    if self._has_output and not self._last_blank:
                        self.console.print()
                        self._last_blank = True
                return
            level_styles = {
                logging.DEBUG: ("DEBUG", "dim"),
                logging.INFO: ("INFO", "cyan"),
                logging.WARNING: ("WARN", "yellow"),
                logging.ERROR: ("ERROR", "red"),
                logging.CRITICAL: ("FATAL", "bold red"),
            }
            label, style = level_styles.get(record.levelno, (record.levelname, ""))
            prefix = Text(f"{label:<5}", style=style)
            body_lines = message.split("\n")
            rendered = Text.assemble(prefix, "  ", _campaign_body_text(body_lines[0], record.levelno))
            for line in body_lines[1:]:
                rendered.append("\n      ")
                rendered.append(_campaign_body_text(line, record.levelno))
            first_line = message.split("\n", 1)[0].lstrip(" -=")
            is_section = first_line.startswith((
                "Cluster:", "Application:", "Step:", "Step complete:", "Execution summary"
            ))
            with self._output_lock:
                if is_section and self._has_output and not self._last_blank:
                    self.console.print()
                self.console.print(rendered)
                self._has_output = True
                self._last_blank = False
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
            plain_message = clean_campaign_log_message(self.format(record))
            if not plain_message:
                return
            level = self.LEVEL_NAMES.get(record.levelno, "info")
            job_match = re.match(r"^\s*Job:\s*([^|]+?)(\s*\|.*)?$", plain_message)
            if job_match:
                level = "job"
            elif plain_message.lstrip().startswith("Step changed:"):
                level = "warning"
            message = rich_escape(plain_message)
            event = CampaignEvent(
                type=EventType.LOG,
                timestamp=datetime.now().isoformat(),
                data={
                    "level": level,
                    "message": message,
                    "job_title": job_match.group(1).strip() if job_match else None,
                    "job_details": (job_match.group(2) or "").strip() if job_match else None,
                },
            )
            self.event_queue.put(event)
        except Exception:
            self.handleError(record)


def clean_campaign_log_message(message: str) -> str:
    """Remove legacy inline styling and box drawing from campaign messages."""
    message = re.sub(
        r"\[/?(?:dim|blue|cyan|green|yellow|red|magenta|white|bold|italic|underline)(?:\s+(?:dim|blue|cyan|green|yellow|red|magenta|white|bold|italic|underline))*\]",
        "",
        message,
    )
    lines = []
    for line in message.splitlines():
        line = re.sub(r"^\s*[├└]─+\s*", "  • ", line)
        line = re.sub(r"^─{2,}\s*Cluster:\s*", "Cluster: ", line)
        line = re.sub(r"^(\s*)(?:✅|❌|🚀|☁️|⚙️|📜)\s*", r"\1", line)
        line = line.rstrip()
        if line:
            lines.append(line)
    return "\n".join(lines)


def _campaign_message_style(message: str, level: int) -> str:
    """Apply restrained semantic colors to plain campaign log messages."""
    lowered = message.lower()
    if lowered.lstrip().startswith("step changed:"):
        return "bold yellow"
    if level >= logging.ERROR or any(term in lowered for term in (" failed", "failure", "error:")):
        return "red"
    if level == logging.WARNING or "cancelled" in lowered or "warning" in lowered:
        return "yellow"
    if "dry run" in lowered or "would generate" in lowered or "planned" in lowered:
        return "yellow"
    if any(term in lowered for term in ("success", "completed", " passed")):
        return "green"
    heading = lowered.lstrip(" -=")
    if heading.startswith("cluster:"):
        return "bold magenta"
    if heading.startswith(("application:", "step:")):
        return "bold cyan"
    return ""


def _campaign_body_text(message: str, level: int) -> Text:
    """Highlight a job's config/tag title while retaining normal detail text."""
    match = re.match(r"^(\s*Job:\s*)([^|]+?)(\s*\|.*)?$", message)
    if not match:
        return Text(message, style=_campaign_message_style(message, level))
    prefix, title, details = match.groups()
    return Text.assemble(
        Text(prefix),
        Text(title.strip(), style="bold cyan"),
        Text(details or "", style="dim"),
    )


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
        app_names = set()
        for app_data in data["apps"]:
            logger.debug(f"Loading app config: {app_data.get('name', 'unknown')}")
            app = load_app_config(app_data, base_dir=Path(file).resolve().parent)
            if app.name in app_names:
                raise ConfigurationError(f"Duplicate application name '{app.name}'")
            app_names.add(app.name)
            apps.append(app)

        parallel_clusters = data.get("parallel_clusters", False)
        if not isinstance(parallel_clusters, bool):
            raise ConfigurationError("'parallel_clusters' must be true or false")
        campaign_name = data.get("name")
        if campaign_name is not None:
            if not isinstance(campaign_name, str) or not campaign_name.strip():
                raise ConfigurationError("'name' must be a non-empty string when provided")
            campaign_name = campaign_name.strip()
        config = CampaignConfig(
            apps=apps,
            parallel_clusters=parallel_clusters,
            name=campaign_name,
        )
        logger.debug(f"Successfully loaded campaign config with {len(apps)} app(s)")
        return config

    except CampaignRunnerError:
        raise
    except Exception as e:
        logger.error(f"Failed to load campaign config: {str(e)}")
        raise ConfigurationError(f"Failed to load campaign config: {str(e)}")


def load_app_config(app_data: dict, base_dir: Optional[Path] = None) -> AppConfig:
    """Load single app configuration."""
    logger = logging.getLogger(__name__)
    try:
        name = app_data.get("name")
        if not name:
            raise ConfigurationError("App config missing 'name'")
        if Path(name).name != name or name in (".", ".."):
            raise ConfigurationError(f"App name '{name}' must be a single safe path component")

        logger.debug(f"Loading app '{name}'")

        dir_str = app_data.get("dir")
        if not dir_str:
            raise ConfigurationError(f"App '{name}' missing 'dir'")

        # Convert to Path
        dir_path = Path(dir_str)
        if not dir_path.is_absolute():
            dir_path = (base_dir or Path.cwd()) / dir_path
        dir_path = dir_path.resolve()
        if not dir_path.is_dir():
            raise ConfigurationError(f"App '{name}' directory does not exist: {dir_path}")

        logger.debug(f"App directory: {dir_path}")

        # Parse steps
        steps_data = app_data.get("steps", [])
        logger.debug(f"Loading {len(steps_data)} step(s)")
        steps = [load_step_config(s) for s in steps_data]

        if not steps:
            raise ConfigurationError(f"App '{name}' has no steps")
        step_names = [step.name for step in steps]
        if len(step_names) != len(set(step_names)):
            raise ConfigurationError(f"App '{name}' contains duplicate step names")

        # Parse configs
        configs_data = app_data.get("configs") or []
        if isinstance(configs_data, str):
            configs_data = [configs_data]
        if not isinstance(configs_data, list) or not all(isinstance(path, str) for path in configs_data):
            raise ConfigurationError(f"App '{name}' configs must be a string or list of strings")

        logger.debug(f"Configs: {configs_data}")

        app = AppConfig(
            name=name,
            dir=dir_path,
            cluster_whitelist=app_data.get("cluster_whitelist", []),
            configs=configs_data,
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
        jobs_args = step_data.get("jobs_args", [])
        if isinstance(jobs_args, dict):
            if not all(isinstance(cluster, str) for cluster in jobs_args):
                raise ConfigurationError(f"Step '{name}' jobs_args map keys must be strings")
            jobs_args = {
                cluster: _normalize_jobs_args(value, f"Step '{name}' jobs_args for '{cluster}'")
                for cluster, value in jobs_args.items()
            }
        else:
            jobs_args = _normalize_jobs_args(jobs_args, f"Step '{name}' jobs_args")
        if jobs_args and not jobs:
            raise ConfigurationError(f"Step '{name}' has jobs_args but no jobs file")

        logger.debug(f"Script: {script if script else 'None'}")
        logger.debug(f"Jobs: {jobs}")
        logger.debug(f"On fails: {on_fails}")

        return StepConfig(
            name=name,
            script=script,
            jobs=jobs,
            jobs_args=jobs_args,
            on_fails=on_fails,
        )

    except CampaignRunnerError:
        raise
    except Exception as e:
        logger.error(f"Failed to load step config: {str(e)}")
        raise ConfigurationError(f"Failed to load step config: {str(e)}")


def parse_jobs_args(arguments: List[str]) -> Dict[str, Any]:
    """Parse supported `sbatchman launch` options for a campaign jobs batch."""
    parser = argparse.ArgumentParser(add_help=False, allow_abbrev=False)
    parser.add_argument("--tag", "-t", action="append", default=[])
    parser.add_argument("--variable", "-v", action="append", default=[])
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--ignore-archived", "-ia", action="store_true")
    parser.add_argument("--ignore-conf-in-dup-check", "-ic", action="store_true")
    parser.add_argument("--ignore-commands-in-dup-check", "-icomm", action="store_true")
    try:
        options = parser.parse_args(arguments)
    except SystemExit as exc:
        raise ConfigurationError(f"Invalid jobs_args: {' '.join(arguments)}") from exc

    variables = {}
    for value in options.variable:
        if "=" not in value:
            raise ConfigurationError(f"Invalid jobs_args variable '{value}'; expected key=value")
        key, val = value.split("=", 1)
        variables[key] = val

    return {
        "filter_tags": options.tag or None,
        "filter_variables": variables or None,
        "force": options.force,
        "ignore_archived": options.ignore_archived,
        "ignore_conf_in_dup_check": options.ignore_conf_in_dup_check,
        "ignore_commands_in_dup_check": options.ignore_commands_in_dup_check,
    }


def _normalize_jobs_args(value: Any, label: str) -> List[str]:
    if isinstance(value, str):
        value = shlex.split(value)
    if not isinstance(value, list) or not all(isinstance(arg, str) for arg in value):
        raise ConfigurationError(f"{label} must be a string or list of strings")
    return value


def resolve_jobs_args(arguments: Any, cluster: str) -> Dict[str, Any]:
    """Resolve shared and cluster-specific jobs arguments for one target."""
    if not isinstance(arguments, dict):
        return parse_jobs_args(arguments)

    common = parse_jobs_args(arguments.get("default", []))
    specific = parse_jobs_args(arguments.get(cluster, []))
    tags = list(common["filter_tags"] or [])
    tags.extend(tag for tag in (specific["filter_tags"] or []) if tag not in tags)
    variables = dict(common["filter_variables"] or {})
    variables.update(specific["filter_variables"] or {})
    return {
        "filter_tags": tags or None,
        "filter_variables": variables or None,
        "force": common["force"] or specific["force"],
        "ignore_archived": common["ignore_archived"] or specific["ignore_archived"],
        "ignore_conf_in_dup_check": common["ignore_conf_in_dup_check"] or specific["ignore_conf_in_dup_check"],
        "ignore_commands_in_dup_check": common["ignore_commands_in_dup_check"] or specific["ignore_commands_in_dup_check"],
    }


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


def select_campaign_workflow(
    config: CampaignConfig,
    applications: Optional[List[str]] = None,
    steps: Optional[List[str]] = None,
    steps_by_application: Optional[Dict[str, List[str]]] = None,
) -> CampaignConfig:
    """Apply optional application and step selections in campaign order."""
    requested_apps = set(applications or [])
    requested_steps = set(steps or [])
    requested_steps_by_app = {
        name: set(selected_steps)
        for name, selected_steps in (steps_by_application or {}).items()
    }
    known_apps = {app.name for app in config.apps}
    unknown_apps = requested_apps - known_apps
    unknown_apps.update(set(requested_steps_by_app) - known_apps)
    if unknown_apps:
        raise ConfigurationError(f"Unknown application(s): {', '.join(sorted(unknown_apps))}")
    selected_apps = [app for app in config.apps if not requested_apps or app.name in requested_apps]
    if requested_steps or requested_steps_by_app:
        unknown_steps = set()
        for app in selected_apps:
            selected_names = requested_steps_by_app.get(app.name, requested_steps)
            known_steps = {step.name for step in app.steps}
            unknown_steps.update(selected_names - known_steps)
            app.steps = [step for step in app.steps if step.name in selected_names]
        if unknown_steps:
            raise ConfigurationError(f"Unknown step(s): {', '.join(sorted(unknown_steps))}")
        selected_apps = [app for app in selected_apps if app.steps]
    if not selected_apps:
        raise ConfigurationError("The selected applications/steps contain no campaign work")
    config.apps = selected_apps
    return config


def _campaign_input_hash(config_file: Optional[Path], config: CampaignConfig) -> Optional[str]:
    """Hash the campaign definition and referenced config/jobs files for safe resume."""
    if config_file is None:
        return None
    digest = hashlib.sha256()
    digest.update(Path(config_file).read_bytes())
    for app in config.apps:
        for relative_path in list(app.configs) + [step.jobs for step in app.steps if step.jobs]:
            path = (app.dir / relative_path).resolve()
            digest.update(str(path).encode("utf-8"))
            if path.is_file():
                digest.update(path.read_bytes())
            else:
                digest.update(b"<missing>")
    return digest.hexdigest()


def _step_definition_hash(app: AppConfig, step: StepConfig) -> str:
    """Fingerprint a step and the files that determine its work."""
    digest = hashlib.sha256()
    digest.update(json.dumps(asdict(step), sort_keys=True).encode("utf-8"))
    digest.update(str(app.dir).encode("utf-8"))
    for relative_path in list(app.configs) + ([step.jobs] if step.jobs else []):
        path = (app.dir / relative_path).resolve()
        digest.update(str(path).encode("utf-8"))
        if path.is_file():
            digest.update(path.read_bytes())
        else:
            digest.update(b"<missing>")
    return digest.hexdigest()


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
            campaign_name=data.get("campaign_name"),
            config_file=data.get("config_file"),
            config_sha256=data.get("config_sha256"),
            application_projects=data.get("application_projects", {}),
            campaign_end=data.get("campaign_end"),
            clusters_processed=data.get("clusters_processed", []),
            dry_run=data.get("dry_run", False),
            resume_mode=data.get("resume_mode", "prompt"),
            force_apps=data.get("force_apps", []),
            selection=data.get("selection", {}),
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
        raise StateRecoveryError(f"Could not recover campaign journal {log_file}: {e}") from e


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
    definition_hash: Optional[str] = None,
) -> bool:
    """Check if specific step completed on cluster."""
    if app_name not in state.execution:
        return False

    if step_name not in state.execution[app_name]:
        return False

    step_logs = state.execution[app_name][step_name]
    if cluster not in step_logs:
        return False

    log = step_logs[cluster]
    return (
        log.status == StepStatus.COMPLETED.value
        and not _has_non_completed_jobs(log.jobs)
        and definition_hash is not None
        and log.definition_hash == definition_hash
    )


def _has_non_completed_jobs(jobs: List[Dict[str, Any]]) -> bool:
    """Whether a step has any recorded job that did not complete successfully."""
    return any(
        str(job.get("status", "")).upper() != "COMPLETED"
        for job in jobs
    )


def _job_runtime_seconds(start_timestamp: Optional[str], end_timestamp: Optional[str]) -> Optional[float]:
    """Calculate a job's execution runtime from its persisted scheduler timestamps."""
    if not start_timestamp or not end_timestamp:
        return None
    try:
        started = datetime.strptime(start_timestamp[:22], "%Y%m%d_%H%M%S.%f")
        ended = datetime.strptime(end_timestamp[:22], "%Y%m%d_%H%M%S.%f")
    except (TypeError, ValueError):
        return None
    return max(0.0, (ended - started).total_seconds())


def get_failed_apps(state: ExecutionState) -> List[str]:
    """Return names of apps that have at least one non-completed,
    non-skipped step logged in `state` (i.e. good candidates for a
    "re-run failed apps" action)."""
    failed = []
    for app_name, app_steps in state.execution.items():
        app_failed = False
        for step_logs in app_steps.values():
            for log in step_logs.values():
                if (
                    log.status in (StepStatus.FAILED.value, StepStatus.CANCELLED.value)
                    or _has_non_completed_jobs(log.jobs)
                ):
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


def current_recorded_job_status(
    job: Dict[str, Any], application_project: Optional[str] = None
) -> str:
    """Return status from the canonical SbatchMan jobs listing."""
    resolved = resolve_recorded_job(job, application_project)
    if resolved is not None:
        return str(resolved.status).upper()
    return str(job.get("status", "unknown")).upper()


def resolve_recorded_job(
    job: Dict[str, Any], application_project: Optional[str] = None
) -> Optional[sbm.Job]:
    """Resolve a campaign record to the matching job in its SbatchMan project."""
    project_root = job.get("project_root") or application_project
    cluster = job.get("cluster") or job.get("cluster_name")
    if project_root and cluster:
        root = Path(project_root).resolve()
        try:
            with project_config.use_project_root(root.parent):
                with global_config.use_cluster_name(str(cluster)):
                    # Always refresh from the project's current job state. Campaign
                    # journals can coexist with independent launches and status edits.
                    jobs = sbm.jobs_list(
                        cluster_name=str(cluster),
                        from_active=True,
                        from_archived=True,
                        update_jobs=True,
                    )
        except Exception:
            jobs = []
        job_id = job.get("job_id")
        exp_dir = str(job.get("exp_dir") or "")
        config = job.get("config") or job.get("config_name")
        tag = job.get("tag")
        match = next(
            (
                item for item in jobs
                if job_id is not None and str(item.job_id) == str(job_id)
                and item.cluster_name == str(cluster)
            ),
            None,
        )
        if match is None and exp_dir:
            match = next(
                (
                    item for item in jobs
                    if item.cluster_name == str(cluster)
                    and item.exp_dir == exp_dir
                    and (config is None or item.config_name == str(config))
                    and (tag is None or item.tag == str(tag))
                ),
                None,
            )
        if match is not None:
            return match
    return None


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
    application_projects = data.get("application_projects", {})

    for app_name, app_data in data.get("execution", {}).items():
        for step_name, step_logs in app_data.get("steps", {}).items():
            for cluster, log in step_logs.items():
                total_steps += 1
                status = log.get("status")
                jobs_by_key = {}
                for attempt in log.get("job_attempts") or []:
                    for job in attempt.get("jobs") or []:
                        key = (job.get("cluster"), job.get("job_id"), job.get("exp_dir"), job.get("tag"))
                        jobs_by_key[key] = job
                for job in log.get("jobs") or []:
                    key = (job.get("cluster"), job.get("job_id"), job.get("exp_dir"), job.get("tag"))
                    jobs_by_key[key] = job
                has_non_completed_jobs = any(
                    current_recorded_job_status(job, application_projects.get(app_name)) != "COMPLETED"
                    for job in jobs_by_key.values()
                )
                if status == StepStatus.COMPLETED.value and not has_non_completed_jobs:
                    completed_steps += 1
                elif status in (StepStatus.FAILED.value, StepStatus.CANCELLED.value) or has_non_completed_jobs:
                    failed_steps += 1
                    failed_apps.add(app_name)

    return {
        "log_file": str(log_file),
        "campaign_id": data.get("campaign_id"),
        "campaign_name": data.get("campaign_name"),
        "campaign_start": data.get("campaign_start"),
        "campaign_end": data.get("campaign_end"),
        "clusters_processed": data.get("clusters_processed", []),
        "application_projects": data.get("application_projects", {}),
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
    on_status: Optional[Callable[[Dict[int, str]], None]] = None,
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

    def _cancel_active_jobs() -> Tuple[int, List[str]]:
        cancelled = 0
        errors: List[str] = []
        for job in jobs:
            try:
                with open(job.get_metadata_path(), "r") as stream:
                    metadata = yaml.safe_load(stream) or {}
                status = str(metadata.get("status", job.status))
            except Exception:
                status = str(job.status)
            if status in TERMINAL_STATES:
                continue
            if not job.job_id:
                errors.append(f"Job {job.tag} has no scheduler ID; could not cancel it")
                continue
            try:
                job.get_job_config().cancel_job(job.job_id)
                job.status = "CANCELLED"
                job.end_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S.%f")
                job.write_metadata()
                cancelled += 1
                logger.info(f"Cancelled job {job.job_id} ({job.tag})")
            except Exception as exc:
                errors.append(f"Job {job.job_id} ({job.tag}): {exc}")
                logger.error(f"Could not cancel job {job.job_id} ({job.tag}): {exc}")
        return cancelled, errors

    def _raise_cancelled() -> None:
        try:
            sbm.update_jobs_status(cluster_name=jobs[0].cluster_name)
        except Exception as exc:
            logger.warning(f"Could not refresh scheduler state before cancellation: {exc}")
        cancelled, errors = _cancel_active_jobs()
        if on_status is not None:
            try:
                on_status({job.job_id: job.status for job in jobs})
            except Exception as exc:
                logger.warning(f"Could not publish final cancellation status: {exc}")
        message = f"Job polling cancelled by user; cancelled {cancelled} active job(s)"
        if errors:
            message += ". Cancellation errors: " + "; ".join(errors)
        raise CampaignCancelledError(message)

    def _read_job_status(job: sbm.Job) -> str:
        # Read the exact job record. Scheduler IDs may collide across clusters,
        # so a project-wide lookup by ID can return the different application's job.
        with open(job.get_metadata_path(), "r") as f:
            metadata = yaml.safe_load(f) or {}
        return str(metadata.get("status", "UNKNOWN"))

    while elapsed < max_wait:
        if control is not None:
            if control.is_cancelled:
                _raise_cancelled()
            while control.is_paused and not control.is_cancelled:
                time.sleep(0.2)
            if control.is_cancelled:
                _raise_cancelled()

        try:
            sbm.update_jobs_status(cluster_name=jobs[0].cluster_name)
        except Exception as exc:
            logger.warning(f"Could not refresh scheduler state for cluster {jobs[0].cluster_name}: {exc}")

        # Refresh job statuses
        job_statuses = {}
        for job in jobs:
            # Re-fetch job from sbatchman
            try:
                job_statuses[job.job_id] = _read_job_status(job)
            except Exception as e:
                logger.warning(f"Failed to fetch status for job {job.job_id}: {e}")
                job_statuses[job.job_id] = "UNKNOWN" if job.get_metadata_path().exists() else "MISSING"

        if on_status is not None:
            on_status(job_statuses)

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

        if control is not None:
            control.cancel_event.wait(poll_interval)
        else:
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
            final_status = _read_job_status(job)
            if final_status == "COMPLETED":
                passed += 1
            else:
                failed += 1
                errors.append(f"Job {job.job_id}: {final_status}")
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
    """Orchestrates application campaign execution."""

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
        config_file: Optional[Path] = None,
        clusters_by_step: Optional[Dict[str, Dict[str, List[str]]]] = None,
        campaign_selection: Optional[Dict[str, Any]] = None,
    ):
        self.config = config
        self.clusters_by_step = clusters_by_step or {}
        self.results_dir = Path(results_dir).resolve()
        self.log_file = Path(log_file).resolve()
        self.verbose = verbose
        self.dry_run = dry_run
        self.config_file = Path(config_file).resolve() if config_file else None
        self.config_sha256 = _campaign_input_hash(self.config_file, config)

        # Live monitoring / remote control (both optional; a plain CLI run
        # leaves these as None and behaves exactly as before).
        self.event_queue = event_queue
        self.control = control
        self._pause_emitted = False
        self._checkpoint_lock = threading.RLock()
        self._app_setup_locks = {app.name: threading.Lock() for app in config.apps}

        self.logger = setup_logger(
            __name__,
            verbose,
            console_output=console_output,
            event_queue=event_queue,
        )

        # Campaign state
        self.state = ExecutionState(
            campaign_id=datetime.now().strftime("%Y%m%d_%H%M%S_%f"),
            campaign_start=datetime.now().isoformat(),
            campaign_name=config.name,
            config_file=str(self.config_file) if self.config_file else None,
            config_sha256=self.config_sha256,
            application_projects={
                app.name: str(app.dir / "SbatchMan")
                for app in self.config.apps
            },
            selection=campaign_selection or {},
        )

        # Store original working directory
        self.original_cwd = Path.cwd()

    # ------------------------------------------------------------------
    # Event / control helpers
    # ------------------------------------------------------------------

    def _emit(self, event_type: EventType, **data: Any) -> None:
        self._checkpoint()
        if self.event_queue is None:
            return
        self.event_queue.put(
            CampaignEvent(type=event_type, timestamp=datetime.now().isoformat(), data=data)
        )

    def _state_as_dict(self) -> dict:
        log_data = {
            "campaign_id": self.state.campaign_id,
            "campaign_start": self.state.campaign_start,
            "campaign_name": self.state.campaign_name,
            "config_file": self.state.config_file,
            "config_sha256": self.state.config_sha256,
            "application_projects": self.state.application_projects,
            "campaign_end": self.state.campaign_end,
            "clusters_processed": self.state.clusters_processed,
            "dry_run": self.state.dry_run,
            "resume_mode": self.state.resume_mode,
            "force_apps": self.state.force_apps,
            "selection": self.state.selection,
            "execution": {},
        }
        for app_name, app_steps in self.state.execution.items():
            log_data["execution"][app_name] = {"steps": {}}
            for step_name, step_logs in app_steps.items():
                log_data["execution"][app_name]["steps"][step_name] = {
                    cluster: asdict(log) for cluster, log in step_logs.items()
                }
        return log_data

    def _step_status_snapshot(self) -> List[Dict[str, Any]]:
        """Return the final per-cluster step data needed to reconcile the TUI."""
        return [
            {
                "app": app_name,
                "step": step_name,
                "cluster": cluster,
                "status": log.status,
                "duration": log.duration_seconds,
                "jobs": log.jobs,
                "jobs_count": log.jobs_count,
                "jobs_successful": log.jobs_successful,
            }
            for app_name, app_steps in self.state.execution.items()
            for step_name, cluster_logs in app_steps.items()
            for cluster, log in cluster_logs.items()
        ]

    def _checkpoint(self) -> None:
        """Persist the current journal atomically so interruption loses little state."""
        if self.dry_run:
            return
        with self._checkpoint_lock:
            try:
                self.log_file.parent.mkdir(parents=True, exist_ok=True)
                temporary_file = self.log_file.with_suffix(self.log_file.suffix + ".tmp")
                with open(temporary_file, "w") as f:
                    json.dump(self._state_as_dict(), f, indent=2)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(temporary_file, self.log_file)
            except OSError as exc:
                raise CampaignRunnerError(f"Could not checkpoint campaign state at {self.log_file}: {exc}") from exc

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
                    if previous_state.application_projects:
                        # Keep using the existing project when resuming a campaign
                        # created before projects moved into application directories.
                        self.state.application_projects = previous_state.application_projects
            except StateRecoveryError as e:
                self.logger.warning(f"Failed to recover state: {str(e)}")
                raise

        self._emit(
            EventType.CAMPAIGN_START,
            campaign_id=self.state.campaign_id,
            campaign_name=self.state.campaign_name,
            clusters=clusters,
            apps=[a.name for a in self.config.apps],
        )

        try:
            self._print_campaign_header(clusters)

            if self.config.parallel_clusters and len(clusters) > 1:
                self.logger.info(f"Running {len(clusters)} clusters concurrently")
                with ThreadPoolExecutor(max_workers=len(clusters), thread_name_prefix="campaign-cluster") as pool:
                    futures = {
                        pool.submit(self._run_cluster_in_context, cluster, previous_state, force_apps): cluster
                        for cluster in clusters
                    }
                    errors = []
                    for future in as_completed(futures):
                        cluster = futures[future]
                        try:
                            future.result()
                        except CampaignCancelledError:
                            raise
                        except CampaignRunnerError as exc:
                            errors.append((cluster, exc))
                    if errors:
                        cluster, exc = errors[0]
                        raise CampaignRunnerError(f"Cluster '{cluster}' failed: {exc}") from exc
            else:
                for cluster in clusters:
                    self._run_cluster_in_context(cluster, previous_state, force_apps)

            self._finalize()
            success = all(
                all(
                    log.status == StepStatus.COMPLETED.value
                    and not _has_non_completed_jobs(log.jobs)
                    for step_logs in app_steps.values()
                    for log in step_logs.values()
                )
                for app_steps in self.state.execution.values()
            )
            self._emit(
                EventType.CAMPAIGN_END,
                success=success,
                steps=self._step_status_snapshot(),
            )
            return success

        except CampaignCancelledError as e:
            self.logger.warning(f"Campaign cancelled: {str(e)}")
            self._emit(EventType.CAMPAIGN_CANCELLED, message=str(e))
            self._finalize()
            self._emit(
                EventType.CAMPAIGN_END,
                success=False,
                steps=self._step_status_snapshot(),
            )
            return False

        except CampaignRunnerError as e:
            self.logger.error(f"Campaign failed: {str(e)}")
            if DEBUG_STACKTRACE:
                console.print_exception()
            self._finalize()
            self._emit(
                EventType.CAMPAIGN_END,
                success=False,
                steps=self._step_status_snapshot(),
            )
            return False

    def _run_cluster_in_context(self, cluster, previous_state, force_apps):
        with global_config.use_cluster_name(cluster):
            self._run_cluster(cluster, previous_state, force_apps)

    def _run_cluster(
        self,
        cluster: str,
        previous_state: Optional[ExecutionState],
        force_apps: List[str],
    ) -> None:
        """Execute all apps on a single cluster."""
        self.logger.info("")
        if self.event_queue is None:
            self.logger.info(f"=== Cluster: {cluster} ===")
        self._emit(EventType.CLUSTER_START, cluster=cluster)

        # The caller establishes a context-local cluster selection for this
        # worker. Do not write the process-wide SbatchMan default here.

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
                self._run_app(app, cluster, previous_state, force=app.name in force_apps)
            except CampaignCancelledError:
                raise
            except CampaignRunnerError as e:
                self.logger.error(f"App '{app.name}' failed on cluster '{cluster}': {str(e)}")
                raise

        self._emit(EventType.CLUSTER_END, cluster=cluster)

    def _should_run_app_on_cluster(self, app: AppConfig, cluster: str) -> bool:
        """Check whether an app's optional whitelist includes this cluster."""
        if app.cluster_whitelist and cluster not in app.cluster_whitelist:
            return False
        selected_steps = self.clusters_by_step.get(app.name, {})
        if selected_steps and not any(
            cluster in selected_steps.get(step.name, []) for step in app.steps
        ):
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

        # Compare individual step definitions so edited campaigns can reuse
        # only the completed work that is still valid.
        return True

    def _setup_app_for_cluster(
        self,
        app: AppConfig,
        cluster: str,
    ) -> None:
        # App projects are shared across clusters; initialize and update their
        # config files one cluster at a time to avoid concurrent writes.
        app_lock = self._app_setup_locks[app.name]
        if self.control is None:
            with app_lock:
                self._setup_app_for_cluster_locked(app, cluster)
            return

        # A parallel cluster can be waiting on setup while the user requests
        # cancellation. Use timed acquisition so that worker notices the request
        # without waiting indefinitely for another cluster's setup to finish.
        while not app_lock.acquire(timeout=0.2):
            self._check_control()
        try:
            self._check_control()
            self._setup_app_for_cluster_locked(app, cluster)
        finally:
            app_lock.release()

    def _setup_app_for_cluster_locked(self, app: AppConfig, cluster: str) -> None:
        """
        Setup app for cluster: initialize project and create configs once.
        This runs before any steps are executed.
        """
        self.logger.debug(f"[blue]├────[/blue] Setting up app '{app.name}' for cluster '{cluster}'")
        start_time = time.time()

        try:
            # Initialize project
            self.logger.debug(f"Initializing project for app '{app.name}'")
            try:
                initialize_project(self._application_project_parent(app), dry_run=self.dry_run)
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
                        sbm.create_configs_from_file(config_path, overwrite=True)
                    config_paths.append(config_path)
                self.logger.debug(f"[blue]├────[/blue] ✅ Configs created successfully")

            duration = time.time() - start_time
            self.logger.debug(f"[blue]├────[/blue] Setup completed in {duration:.1f}s")

        except CampaignRunnerError as e:
            error_msg = str(e)
            self.logger.error(f"App setup failed after {time.time() - start_time:.1f}s: {error_msg}")
            raise
        finally:
            self._checkpoint()

    def _application_project_parent(self, app: AppConfig) -> Path:
        project = self.state.application_projects.get(app.name)
        return Path(project).parent if project else app.dir

    def _preview_jobs(
        self,
        app: AppConfig,
        step: StepConfig,
        cluster: str,
        force: bool,
        options: Dict[str, Any],
        protect_completed: bool = False,
    ) -> List[sbm.Job]:
        """Expand a jobs YAML in an isolated temporary project without submitting jobs."""
        with tempfile.TemporaryDirectory(prefix="sbatchman-campaign-preview-") as temporary_dir:
            temporary_root = Path(temporary_dir)
            sbm.init_project(temporary_root, no_logo=True)
            temporary_project = temporary_root / "SbatchMan"

            # Keep already-defined project configs available to the preview,
            # while ensuring the preview cannot modify the real project.
            existing_configs = self._application_project_parent(app) / "SbatchMan" / "configs"
            if existing_configs.is_dir():
                shutil.copytree(existing_configs, temporary_project / "configs", dirs_exist_ok=True)

            with project_config.use_project_root(temporary_root):
                for config_file in app.configs:
                    sbm.create_configs_from_file(app.dir / config_file, overwrite=True)
                try:
                    return sbm.launch_jobs_from_file(
                        app.dir / step.jobs,
                        force=force or (options["force"] and not protect_completed),
                        dry_run=True,
                        quiet=True,
                        filter_tags=options["filter_tags"],
                        filter_variables=options["filter_variables"],
                        ignore_archived=(
                            options["ignore_archived"] and not protect_completed
                        ),
                        ignore_conf_in_dup_check=options["ignore_conf_in_dup_check"],
                        ignore_commands_in_dup_check=options["ignore_commands_in_dup_check"],
                        rerun_non_completed=True,
                    )
                except Exception as exc:
                    raise JobExecutionError(
                        f"Could not preview jobs for step '{step.name}' on cluster '{cluster}': {exc}"
                    ) from exc

    def _run_app(
        self,
        app: AppConfig,
        cluster: str,
        previous_state: Optional[ExecutionState],
        force: bool = False,
    ) -> None:
        """Run an application inside its isolated SbatchMan project context."""
        project_parent = self._application_project_parent(app)
        self.logger.debug(f"Application SbatchMan project: {project_parent / 'SbatchMan'}")
        with project_config.use_project_root(project_parent):
            self._run_app_in_project(app, cluster, previous_state, force=force)

    def _run_app_in_project(
        self,
        app: AppConfig,
        cluster: str,
        previous_state: Optional[ExecutionState],
        force: bool = False,
    ) -> None:
        """Execute single app on single cluster."""
        self.logger.info("")
        if self.event_queue is None:
            self.logger.info(f"--- Application: {app.name} ---")
        self.logger.info(f"Directory: {app.dir}")
        self._emit(EventType.APP_START, app=app.name, cluster=cluster)

        # Clusters may execute this app concurrently. Initialize its shared
        # state entry atomically so one worker cannot replace another's data.
        with self._checkpoint_lock:
            self.state.execution.setdefault(app.name, {})

        # Setup app for cluster (project init + config creation) - done once before all steps
        try:
            self._setup_app_for_cluster(app, cluster)
        except CampaignRunnerError as e:
            self.logger.error(f"App '{app.name}' setup failed on cluster '{cluster}': {str(e)}")
            raise

        for step in app.steps:
            self._check_control()

            selected_clusters = self.clusters_by_step.get(app.name, {}).get(step.name)
            if selected_clusters is not None and cluster not in selected_clusters:
                if previous_state is not None:
                    # A changed TUI selection limits this resume run; preserve
                    # the journal entry for work outside that selection.
                    continue
                self.logger.info(
                    f"Step '{step.name}' skipped on cluster '{cluster}' (cluster selection)"
                )
                skipped_log = StepExecutionLog(
                    status=StepStatus.SKIPPED.value,
                    error_message="Excluded by campaign selection",
                    definition_hash=_step_definition_hash(app, step),
                )
                with self._checkpoint_lock:
                    self.state.execution.setdefault(app.name, {}).setdefault(step.name, {})[
                        cluster
                    ] = skipped_log
                self._emit(
                    EventType.STEP_SKIPPED,
                    app=app.name,
                    cluster=cluster,
                    step=step.name,
                    reason="cluster selection",
                )
                continue

            # Check resume for step
            current_step_hash = _step_definition_hash(app, step)
            previous_step_log = (
                previous_state.execution.get(app.name, {}).get(step.name, {}).get(cluster)
                if previous_state else None
            )
            if (
                not force
                and previous_step_log
                and previous_step_log.status == StepStatus.COMPLETED.value
                and previous_step_log.definition_hash
                and previous_step_log.definition_hash != current_step_hash
            ):
                self.logger.warning(
                    f"Step changed: {step.name} (definition or input files changed); rerunning"
                )
            if previous_state and not force and is_step_completed(
                app.name,
                step.name,
                cluster,
                previous_state,
                current_step_hash,
            ):
                self.logger.info(f"Step unchanged: {step.name} (already completed; skipping)")
                self._emit(EventType.STEP_SKIPPED, app=app.name, cluster=cluster, step=step.name, reason="already completed")
                # Preserve the other clusters' current records. Replacing the
                # whole step map here can erase a concurrently running cluster.
                with self._checkpoint_lock:
                    self.state.execution.setdefault(app.name, {}).setdefault(step.name, {})[
                        cluster
                    ] = copy.deepcopy(
                        previous_state.execution[app.name][step.name][cluster]
                    )
                continue

            self.logger.info(f"Step: {step.name}")
            retry = False
            while True:
                self._emit(EventType.STEP_START, app=app.name, cluster=cluster, step=step.name)
                try:
                    success = self._execute_step(
                        app,
                        step,
                        cluster,
                        force=retry,
                        is_resume=previous_state is not None,
                    )
                except CampaignCancelledError:
                    raise
                except StepExecutionError:
                    success = False

                log = self.state.execution[app.name][step.name][cluster]
                self._emit(
                    EventType.STEP_END,
                    app=app.name,
                    cluster=cluster,
                    step=step.name,
                    status=log.status,
                    duration=log.duration_seconds,
                    jobs=log.jobs,
                    jobs_count=log.jobs_count,
                    jobs_successful=log.jobs_successful,
                    jobs_failed=log.jobs_failed,
                )
                if success:
                    break

                if self.control is not None and self.event_queue is not None:
                    self._emit(
                        EventType.STEP_NEEDS_ATTENTION,
                        app=app.name,
                        cluster=cluster,
                        step=step.name,
                        error=log.error_message or "Step failed.",
                        jobs=log.jobs,
                        job_attempts=log.job_attempts,
                    )
                    resolution = self.control.wait_for_resolution()
                    log.resolution_history.append({
                        "action": resolution,
                        "timestamp": datetime.now().isoformat(),
                    })
                    self._checkpoint()
                    if resolution == "retry":
                        retry = True
                        continue
                    if resolution == "continue":
                        log.status = StepStatus.COMPLETED.value
                        log.manual_resolution = "continued_after_manual_intervention"
                        log.error_message = None
                        self._checkpoint()
                        self._emit(
                            EventType.STEP_END, app=app.name, cluster=cluster, step=step.name,
                            status=log.status, duration=log.duration_seconds, jobs=log.jobs,
                            jobs_count=log.jobs_count, jobs_successful=log.jobs_successful,
                            jobs_failed=log.jobs_failed,
                        )
                        break
                    if resolution == "skip":
                        log.status = StepStatus.SKIPPED.value
                        log.manual_resolution = "skipped_by_user"
                        self._checkpoint()
                        self._emit(
                            EventType.STEP_END, app=app.name, cluster=cluster, step=step.name,
                            status=log.status, duration=log.duration_seconds, jobs=log.jobs,
                            jobs_count=log.jobs_count, jobs_successful=log.jobs_successful,
                            jobs_failed=log.jobs_failed,
                        )
                        break
                    raise CampaignCancelledError("Campaign stopped by user at a failed step")

                policy = OnFailsPolicy(step.on_fails)
                self.logger.warning(f"Step '{step.name}' failed, applying policy: {policy.value}")
                if policy == OnFailsPolicy.TERMINATE:
                    raise StepExecutionError(f"Step '{step.name}' failed with TERMINATE policy")
                if policy == OnFailsPolicy.SKIP:
                    log.status = StepStatus.SKIPPED.value
                    self._checkpoint()
                    break
                break

        self.logger.info(f"  Finished application: {app.name}")
        self._emit(EventType.APP_END, app=app.name, cluster=cluster)

    def _capture_job_runtimes(
        self,
        jobs: List[sbm.Job],
        job_records: List[Dict[str, Any]],
    ) -> None:
        """Copy scheduler execution runtimes from each submitted job's metadata."""
        records_by_id = {
            str(record.get("job_id")): record
            for record in job_records
            if record.get("submitted") and record.get("job_id") is not None
        }
        for job in jobs:
            record = records_by_id.get(str(job.job_id))
            if record is None:
                continue
            try:
                with open(job.get_metadata_path(), "r") as stream:
                    metadata = yaml.safe_load(stream) or {}
                job.start_timestamp = metadata.get("start_timestamp")
                job.end_timestamp = metadata.get("end_timestamp")
                runtime = _job_runtime_seconds(
                    metadata.get("start_timestamp"), metadata.get("end_timestamp")
                )
                if runtime is not None:
                    record["runtime_seconds"] = runtime
            except (OSError, TypeError, ValueError) as exc:
                self.logger.debug(
                    f"Could not read runtime for job {job.job_id}: {exc}"
                )

    def _execute_step(
        self,
        app: AppConfig,
        step: StepConfig,
        cluster: str,
        force: bool = False,
        is_resume: bool = False,
    ) -> bool:
        """
        Execute single step (script + jobs).
        Returns True if successful.
        """
        self.logger.debug(f"Starting step execution for '{step.name}'")
        start_time = time.time()
        jobs: List[sbm.Job] = []
        jobs_sequential = False

        def step_duration() -> float:
            wall_duration = time.time() - start_time
            runtimes = [
                float(record["runtime_seconds"])
                for record in log.jobs
                if record.get("runtime_seconds") is not None
            ]
            if step.jobs and not self.dry_run and runtimes:
                return sum(runtimes) if jobs_sequential else max(runtimes)
            return wall_duration

        # Initialize/update the cluster's record atomically. In parallel
        # cluster mode, separate workers share the app and step dictionaries.
        with self._checkpoint_lock:
            app_execution = self.state.execution.setdefault(app.name, {})
            step_execution = app_execution.setdefault(step.name, {})
            previous_log = step_execution.get(cluster)
            log = StepExecutionLog(
                status=StepStatus.RUNNING.value,
                attempt_count=(previous_log.attempt_count + 1) if previous_log else 1,
                job_attempts=list(previous_log.job_attempts) if previous_log else [],
                resolution_history=list(previous_log.resolution_history) if previous_log else [],
                definition_hash=_step_definition_hash(app, step),
            )
            step_execution[cluster] = log
        self._checkpoint()

        try:
            # Step 1: Execute script (if any)
            if step.script:
                if self.dry_run:
                    self.logger.info(f"[DRY RUN] Would execute script in {app.dir}")
                else:
                    self.logger.info(f"Executing script in {app.dir}")
                    try:
                        script_env = os.environ.copy()
                        script_env["SBATCHMAN_CLUSTER_NAME"] = cluster
                        script_env["SBATCHMAN_PROJECT_ROOT"] = str(
                            self._application_project_parent(app)
                        )
                        success, stdout, stderr, exit_code = self._execute_script(
                            step.script, app.dir, env=script_env
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
                    jobs_file_data = yaml.safe_load(jobs_path.read_text()) or {}
                    jobs_sequential = (
                        bool(jobs_file_data.get("sequential", False))
                        if isinstance(jobs_file_data, dict)
                        else False
                    )

                    if self.dry_run:
                        options = resolve_jobs_args(step.jobs_args, cluster)
                        force_jobs = options["force"] and not (force or is_resume)
                        jobs = self._preview_jobs(
                            app,
                            step,
                            cluster,
                            force_jobs,
                            options,
                            protect_completed=force or is_resume,
                        )
                        self.logger.info(
                            f"[DRY RUN] {len(jobs)} job(s) would be generated from {step.jobs}"
                        )
                        for job in jobs:
                            self.logger.info(
                                f"  Job: {job.config_name} / {job.tag} | scheduler={job.scheduler} | "
                                f"directory={job.exp_dir} | variables={job.variables} | command={job.command}"
                            )
                    else:
                        self.logger.info(f"[blue]├────[/blue] 🚀 Running jobs from file '{step.jobs}'")
                        baseline_jobs = sbm.jobs_list(
                            cluster_name=cluster,
                            from_active=True,
                            from_archived=False,
                            update_jobs=False,
                        )
                        baseline_keys = {(job.cluster_name, job.exp_dir) for job in baseline_jobs}
                        options = resolve_jobs_args(step.jobs_args, cluster)
                        force_jobs = options["force"] and not (force or is_resume)
                        try:
                            launch_result = sbm.launch_jobs_from_file(
                                app.dir / step.jobs,
                                force=force_jobs,
                                quiet=True,
                                filter_tags=options["filter_tags"],
                                filter_variables=options["filter_variables"],
                                ignore_archived=(
                                    options["ignore_archived"] and not (force or is_resume)
                                ),
                                ignore_conf_in_dup_check=options["ignore_conf_in_dup_check"],
                                ignore_commands_in_dup_check=options["ignore_commands_in_dup_check"],
                                working_directory=app.dir,
                                return_details=True,
                                rerun_non_completed=True,
                            )
                            jobs = launch_result.jobs
                        except Exception as exc:
                            raise JobExecutionError(
                                f"Could not launch jobs for step '{step.name}' on cluster '{cluster}': {exc}"
                            ) from exc
                        if not jobs:
                            candidate_count = launch_result.candidate_count
                            filtered_count = launch_result.filtered_count
                            duplicate_count = launch_result.duplicate_count
                            matching_count = candidate_count - filtered_count
                            if duplicate_count and duplicate_count >= matching_count:
                                message = (
                                    f"No new jobs were submitted for step '{step.name}' on cluster '{cluster}': "
                                    f"all {duplicate_count} matching job definition(s) were skipped because "
                                    f"equivalent jobs already exist in the SbatchMan project for '{app.name}' "
                                    f"({app.dir}). Use the force option to submit them again."
                                )
                            elif duplicate_count:
                                message = (
                                    f"No jobs were submitted for step '{step.name}' on cluster '{cluster}': "
                                    f"{duplicate_count} matching job(s) were skipped as existing duplicates, "
                                    f"and {filtered_count} candidate(s) did not match the configured filters."
                                )
                            elif filtered_count:
                                message = (
                                    f"No jobs were submitted for step '{step.name}' on cluster '{cluster}': "
                                    f"none of the {candidate_count} generated job definition(s) matched the "
                                    "configured tag or variable filters."
                                )
                            else:
                                message = (
                                    f"No jobs were submitted for step '{step.name}' on cluster '{cluster}': "
                                    "the jobs file produced no matching job definitions for this cluster."
                                )
                            self.logger.warning(message)
                        self.logger.info(f"[blue]├────[/blue] Launched {len(jobs)} job(s)")
                    if self.dry_run:
                        baseline_keys = set()

                    log.jobs_launched = bool(jobs) and not self.dry_run
                    submitted_job_records = [
                        {
                            "job_id": None if self.dry_run else job.job_id,
                            "cluster": job.cluster_name,
                            "config": job.config_name,
                            "tag": job.tag,
                            "command": job.command,
                            "status": "DRY RUN" if self.dry_run else job.status,
                            "exp_dir": job.exp_dir,
                            "scheduler": job.scheduler,
                            "project_root": job.project_root,
                            "archive_name": job.archive_name,
                            "submitted": not self.dry_run,
                        }
                        for job in jobs
                    ]
                    duplicate_job_records = []
                    for duplicate in (launch_result.duplicate_jobs if not self.dry_run else []):
                        existing = duplicate.get("existing_job") or {}
                        duplicate_job_records.append({
                            "job_id": existing.get("job_id"),
                            "cluster": existing.get("cluster_name") or duplicate.get("cluster"),
                            "config": existing.get("config_name") or duplicate.get("config"),
                            "tag": existing.get("tag") or duplicate.get("tag"),
                            "command": existing.get("command"),
                            "status": existing.get("status") or "DUPLICATE",
                            "exp_dir": existing.get("exp_dir"),
                            "scheduler": existing.get("scheduler"),
                            "project_root": existing.get("project_root"),
                            "archive_name": existing.get("archive_name"),
                            "submitted": False,
                            "skipped_reason": duplicate.get("reason"),
                            "duplicate_source": duplicate.get("source"),
                            "runtime_seconds": _job_runtime_seconds(
                                existing.get("start_timestamp"),
                                existing.get("end_timestamp"),
                            ),
                        })
                    log.jobs = submitted_job_records + duplicate_job_records
                    self._capture_job_runtimes(jobs, log.jobs)
                    log.jobs_count = len(log.jobs)
                    duplicate_statuses = [
                        str(record.get("status", "")).upper()
                        for record in duplicate_job_records
                    ]
                    duplicate_successful = sum(status == "COMPLETED" for status in duplicate_statuses)
                    duplicate_failed = sum(
                        status in {"FAILED", "FAILED_SUBMISSION", "CHECK_FAIL", "TIMEOUT", "CANCELLED"}
                        for status in duplicate_statuses
                    )
                    log.jobs_successful = duplicate_successful
                    log.jobs_failed = duplicate_failed
                    if jobs and not self.dry_run:
                        log.job_attempts.append({
                            "attempt": log.attempt_count,
                            "submitted_at": datetime.now().isoformat(),
                            "jobs": submitted_job_records,
                        })
                    self._checkpoint()

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
                                jobs=log.jobs,
                                duration=step_duration(),
                            )

                        def _on_job_status(statuses: Dict[int, str]) -> None:
                            for record in log.jobs:
                                if record.get("submitted"):
                                    record["status"] = statuses.get(record["job_id"], "UNKNOWN")
                            self._capture_job_runtimes(jobs, log.jobs)
                            known_keys = baseline_keys | {
                                (job.cluster_name, job.exp_dir) for job in jobs
                            }
                            current_jobs = sbm.jobs_list(
                                cluster_name=cluster,
                                from_active=True,
                                from_archived=False,
                                update_jobs=False,
                            )
                            unexpected = [
                                job for job in current_jobs
                                if (job.cluster_name, job.exp_dir) not in known_keys
                            ]
                            if unexpected:
                                details = ", ".join(
                                    f"{job.tag} (ID {job.job_id}, {job.status})"
                                    for job in unexpected[:10]
                                )
                                raise JobExecutionError(
                                    "Untracked jobs appeared in this application project while the step was running: "
                                    f"{details}. They were not adopted; review them and choose Continue or Retry."
                                )
                            self._checkpoint()

                        all_success, passed, failed, errors = (
                            poll_jobs_until_completion(
                                jobs,
                                logger=self.logger,
                                control=self.control,
                                on_progress=_on_progress,
                                on_status=_on_job_status,
                            )
                        )
                        self._capture_job_runtimes(jobs, log.jobs)
                        log.jobs_successful = passed + duplicate_successful
                        log.jobs_failed = failed + duplicate_failed
                        log.job_errors = errors
                        for record in log.jobs:
                            self.logger.info(
                                f"  Job: {record.get('config')} / {record.get('tag')} | "
                                f"status={record.get('status')} | id={record.get('job_id')}"
                            )

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
            log.duration_seconds = step_duration()
            self.logger.info(f"Step complete: {step.name} ({log.duration_seconds:.1f}s)")
            return True

        except CampaignCancelledError as e:
            self._capture_job_runtimes(jobs, log.jobs)
            log.status = StepStatus.CANCELLED.value
            log.error_message = str(e)
            log.duration_seconds = step_duration()
            self.logger.warning(f"Step '{step.name}' cancelled after {log.duration_seconds:.1f}s")
            raise

        except CampaignRunnerError as e:
            self._capture_job_runtimes(jobs, log.jobs)
            error_msg = str(e)
            log.status = StepStatus.FAILED.value
            log.error_message = error_msg
            log.duration_seconds = step_duration()
            self.logger.error(f"Step '{step.name}' failed after {log.duration_seconds:.1f}s: {error_msg}")
            raise StepExecutionError(error_msg)
        finally:
            # Always restore original working directory
            self.logger.debug(f"Restoring working directory to: {self.original_cwd}")
            os.chdir(self.original_cwd)
            self._checkpoint()

    def _execute_script(
        self,
        script: str,
        cwd: Path,
        env: Optional[Dict[str, str]] = None,
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
            self.logger.info(f"[DRY RUN] Would execute script in {cwd}")
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
                env=env,
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
        log_data = self._state_as_dict()
        if not self.dry_run:
            self.logger.info(f"Writing campaign journal to '{self.log_file}'")
            self._checkpoint()
            self.logger.debug("Campaign journal checkpointed")

        # Print summary
        self._print_summary(log_data)

    def _print_campaign_header(self, clusters: List[str]) -> None:
        """Print campaign header."""
        self.logger.info("Campaign run")
        self.logger.info(f"  ID:       {self.state.campaign_id}")
        if self.state.campaign_name:
            self.logger.info(f"  Name:     {self.state.campaign_name}")
        self.logger.info(f"  Started:  {self.state.campaign_start}")
        self.logger.info(f"  Results:  {self.results_dir}")
        self.logger.info(f"  Clusters: {', '.join(clusters)}")
        self.logger.info(f"  Apps:     {', '.join(a.name for a in self.config.apps)}")
        self.logger.info(f"  Mode:     {'dry run' if self.dry_run else 'execute'}; {'resume' if self.state.resume_mode else 'new run'}")

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
        self.logger.info("Execution summary")
        self.logger.info(f"Total steps: {total_steps}")
        self.logger.info(f"Completed: {completed_steps}")
        self.logger.info(f"Failed: {failed_steps}")
        if self.dry_run:
            self.logger.info(f"Planned jobs: {total_jobs}")
        else:
            self.logger.info(f"Total jobs: {total_jobs}")
            self.logger.info(f"Successful: {successful_jobs}")
            self.logger.info(f"Failed: {failed_jobs}")
        self.logger.info("")
        self.logger.info("")

        # Print per-cluster per-app summary
        for cluster in sorted(summary_by_cluster.keys()):
            self.logger.info(f"\nCluster: {cluster}")

            for app_name in sorted(summary_by_cluster[cluster].keys()):
                stats = summary_by_cluster[cluster][app_name]
                steps_info = f"Steps: {stats['steps_completed']}/{stats['steps_count']}"
                jobs_info = (
                    f"Jobs planned: {stats['jobs_total']}"
                    if self.dry_run else
                    f"Jobs: {stats['jobs_successful']}/{stats['jobs_total']}"
                )

                status = "✅" if stats['steps_failed'] == 0 and stats['jobs_failed'] == 0 else "❌"
                self.logger.info(f"{status} {app_name:<30} {steps_info:<20} {jobs_info}")

            self.logger.info("")
            self.logger.info("")



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
    applications: Optional[List[str]] = None,
    steps: Optional[List[str]] = None,
    steps_by_application: Optional[Dict[str, List[str]]] = None,
    clusters_by_step: Optional[Dict[str, Dict[str, List[str]]]] = None,
    campaign_selection: Optional[Dict[str, Any]] = None,
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
        config = select_campaign_workflow(
            load_campaign_config(config_file), applications, steps, steps_by_application
        )
        clusters = list(dict.fromkeys(clusters or discover_clusters_from_config(config)))
        if not clusters:
            raise ConfigurationError(
                "No cluster targets were supplied or declared in app cluster_whitelist fields."
            )

        # Set up results directory
        results_dir = Path(results_dir)
        results_dir.mkdir(parents=True, exist_ok=True)

        # Log file
        log_file = (
            results_dir
            / f"campaign_log_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.json"
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
            config_file=config_file,
            clusters_by_step=clusters_by_step,
            campaign_selection=campaign_selection,
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
    applications: Optional[List[str]] = None,
    steps: Optional[List[str]] = None,
    steps_by_application: Optional[Dict[str, List[str]]] = None,
    clusters_by_step: Optional[Dict[str, Dict[str, List[str]]]] = None,
) -> bool:
    """
    Convenience wrapper to resume a campaign against a *specific* previous
    log file (rather than the "most recent log in results_dir" convention
    used implicitly by `run_campaign`). Useful for a TUI's "resume from
    history" flow where the user picks an exact run from a list.

    The selected journal is updated in place, keeping resume and failed-app
    reruns attached to the same campaign in history.
    """
    logger = setup_logger(__name__, verbose, console_output=console_output, event_queue=event_queue)
    try:
        if (
            applications is None
            and steps is None
            and steps_by_application is None
            and clusters_by_step is None
        ):
            try:
                journal = json.loads(Path(log_file).read_text())
                saved_selection = journal.get("selection") or {}
            except (OSError, json.JSONDecodeError, AttributeError):
                saved_selection = {}
            if saved_selection:
                applications = saved_selection.get("applications") or None
                steps_by_application = saved_selection.get("steps_by_application") or None
                clusters_by_step = saved_selection.get("clusters_by_step") or None
                selected_clusters = sorted({
                    cluster
                    for app_steps in (clusters_by_step or {}).values()
                    for step_clusters in app_steps.values()
                    for cluster in step_clusters
                })
                if selected_clusters:
                    clusters = [cluster for cluster in clusters if cluster in selected_clusters]
                    if not clusters:
                        clusters = selected_clusters
        config = select_campaign_workflow(
            load_campaign_config(config_file), applications, steps, steps_by_application
        )

        results_dir = Path(log_file).parent

        runner = CampaignRunner(
            config=config,
            results_dir=results_dir,
            log_file=Path(log_file),
            verbose=verbose,
            dry_run=dry_run,
            event_queue=event_queue,
            control=control,
            console_output=console_output,
            config_file=config_file,
            clusters_by_step=clusters_by_step,
        )
        return _run_with_explicit_previous_log(
            runner, Path(log_file), clusters, resume_mode, force_apps or []
        )

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
    """Run `runner` resuming from and checkpointing to the selected journal."""
    runner.state.clusters_processed = clusters
    runner.state.resume_mode = resume_mode
    runner.state.force_apps = force_apps
    runner.state.dry_run = runner.dry_run

    previous_state = recover_execution_state(previous_log_file)
    if previous_state:
        # Keep the selected campaign identity and all prior state. The runner
        # updates this journal in place as resumed work completes.
        runner.state.campaign_id = previous_state.campaign_id
        runner.state.campaign_start = previous_state.campaign_start
        runner.state.campaign_name = previous_state.campaign_name or runner.state.campaign_name
        runner.state.application_projects = previous_state.application_projects or runner.state.application_projects
        runner.state.execution = copy.deepcopy(previous_state.execution)
        runner.state.selection = copy.deepcopy(previous_state.selection)

    runner._emit(
        EventType.CAMPAIGN_START,
        campaign_id=runner.state.campaign_id,
        campaign_name=runner.state.campaign_name,
        clusters=clusters,
        apps=[a.name for a in runner.config.apps],
    )

    try:
        runner._print_campaign_header(clusters)
        if runner.config.parallel_clusters and len(clusters) > 1:
            runner.logger.info(f"Running {len(clusters)} clusters concurrently")
            with ThreadPoolExecutor(
                max_workers=len(clusters),
                thread_name_prefix="campaign-cluster",
            ) as pool:
                futures = {
                    pool.submit(
                        runner._run_cluster_in_context,
                        cluster,
                        previous_state,
                        force_apps,
                    ): cluster
                    for cluster in clusters
                }
                errors = []
                for future in as_completed(futures):
                    cluster = futures[future]
                    try:
                        future.result()
                    except CampaignCancelledError:
                        raise
                    except CampaignRunnerError as exc:
                        errors.append((cluster, exc))
                if errors:
                    cluster, exc = errors[0]
                    raise CampaignRunnerError(
                        f"Cluster '{cluster}' failed: {exc}"
                    ) from exc
        else:
            for cluster in clusters:
                runner._run_cluster_in_context(cluster, previous_state, force_apps)
        runner._finalize()
        success = all(
            all(
                log.status == StepStatus.COMPLETED.value
                    and not _has_non_completed_jobs(log.jobs)
                for step_logs in app_steps.values()
                for log in step_logs.values()
            )
            for app_steps in runner.state.execution.values()
        )
        runner._emit(
            EventType.CAMPAIGN_END,
            success=success,
            steps=runner._step_status_snapshot(),
        )
        return success
    except CampaignCancelledError as e:
        runner.logger.warning(f"Campaign cancelled: {str(e)}")
        runner._emit(EventType.CAMPAIGN_CANCELLED, message=str(e))
        runner._finalize()
        runner._emit(
            EventType.CAMPAIGN_END,
            success=False,
            steps=runner._step_status_snapshot(),
        )
        return False
    except CampaignRunnerError as e:
        runner.logger.error(f"Campaign failed: {str(e)}")
        runner._finalize()
        runner._emit(
            EventType.CAMPAIGN_END,
            success=False,
            steps=runner._step_status_snapshot(),
        )
        return False
