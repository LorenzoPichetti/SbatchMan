"""
campaign.py
===========

Library for orchestrating multi-application campaigns across clusters on top
of sbatchman.

This module is UI-agnostic. It can be driven by:
  - A plain CLI (call `run_campaign(...)` and let it log to the console).
  - An interactive TUI (pass an `event_queue` to receive structured
    `CampaignEvent`s, and a `CampaignControl` to pause/resume/cancel a
    running campaign from a background thread).

Core principles (journal v2)
----------------------------
* The SbatchMan project of each application is the single source of truth for
  job state. The journal only stores *job identity* plus hints.
* A logical job is identified by ``(cluster, config, tag)``. Several SbatchMan
  jobs (attempts, manual re-runs, archived copies) may share that identity;
  the one with the latest ``queued_timestamp`` is the *current* one.
* Status, runtime and exp_dir shown to users are always resolved live from the
  project. Values stored in the journal (``last_known_status`` ...) are only
  hints and are never displayed as if they were live.
* Anything the user does by hand is valid: jobs already completed are adopted
  (never re-run), running jobs are followed, cancelled/failed jobs are re-run.
"""

import json
import hashlib
import argparse
import copy
import logging
import os
import shutil
import socket
import subprocess
import shlex
import time
import threading
import re
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue as queue_module
from contextlib import contextmanager
from dataclasses import dataclass, asdict, field, fields
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Callable, Iterable, Set
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

JOURNAL_VERSION = 2

console = Console(width=shutil.get_terminal_size().columns)


# ============================================================================
# Custom Exception Classes
# ============================================================================


class CampaignRunnerError(Exception):
    """Base exception for campaign runner."""


class ConfigurationError(CampaignRunnerError):
    """Raised when configuration is invalid."""


class CampaignFileNotFoundError(CampaignRunnerError):
    """Raised when a required file is not found (does not shadow the builtin)."""


class StepExecutionError(CampaignRunnerError):
    """Raised when a step execution fails."""


class ScriptExecutionError(StepExecutionError):
    """Raised when script execution fails."""


class JobExecutionError(StepExecutionError):
    """Raised when job execution fails."""


class ProjectInitializationError(CampaignRunnerError):
    """Raised when project initialization fails."""


class StateRecoveryError(CampaignRunnerError):
    """Raised when state recovery fails."""


class JournalLockedError(CampaignRunnerError):
    """Raised when a campaign journal is being used by another process."""


class CampaignCancelledError(CampaignRunnerError):
    """Raised internally when a user requests cancellation of a running campaign."""


class CampaignDetachedError(Exception):
    """Raised when the TUI exits while submitted jobs are left running."""


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
    CAMPAIGN_END = "campaign_end"          # data={"success": bool, "steps": [...]}
    CAMPAIGN_CANCELLED = "campaign_cancelled"  # data={"message": str}

    CLUSTER_START = "cluster_start"        # data={"cluster": str}
    CLUSTER_END = "cluster_end"            # data={"cluster": str}

    APP_START = "app_start"                # data={"app": str, "cluster": str}
    APP_SKIPPED = "app_skipped"            # data={"app": str, "cluster": str, "reason": str}
    APP_END = "app_end"                    # data={"app": str, "cluster": str}

    STEP_START = "step_start"              # data={"app", "cluster", "step"}
    STEP_SKIPPED = "step_skipped"          # data={"app", "cluster", "step", "reason"}
    STEP_PROGRESS = "step_progress"        # data={"app","cluster","step","completed","succeeded","total","jobs","duration"}
    STEP_END = "step_end"                  # data={"app","cluster","step","status","duration",...}
    STEP_NEEDS_ATTENTION = "step_needs_attention"  # data={"app", "cluster", "step", "error", "jobs"}

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

# A job whose identity is not found in a readable project must be missing on
# this many consecutive polls before it is considered MISSING. This absorbs the
# short window in which a user may be moving/archiving a job directory.
MISSING_GRACE_POLLS = 2

# Seconds between job polls while following a step's jobs.
DEFAULT_POLL_INTERVAL = 10.0


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
    """Log entry for single step execution on single cluster.

    ``jobs`` holds ONE record per logical job (cluster, config, tag). A record
    carries identity (cluster/config/tag/command/scheduler) and hints
    (exp_dir/job_id/archive_name/last_known_status/runtime_seconds). Hints may
    be outdated; consumers must resolve the live job through its identity.
    """

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
    jobs_sequential: bool = False

    duration_seconds: float = 0.0


@dataclass
class ExecutionState:
    """Tracks execution state for recovery/resume."""

    campaign_id: str
    campaign_start: str
    campaign_name: Optional[str] = None
    config_file: Optional[str] = None
    config_sha256: Optional[str] = None
    # app name -> the application's SbatchMan directory (the one that contains
    # `experiments/` and `archive/`; its parent is what `use_project_root` takes)
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

    Failed-step resolutions are addressed by a key (``"app|step|cluster"``) so
    that several clusters waiting at the same time (parallel_clusters) can be
    answered independently.
    """

    ANY = "*"

    def __init__(self) -> None:
        self.pause_event = threading.Event()
        self.cancel_event = threading.Event()
        self.detach_event = threading.Event()
        self._wake_event = threading.Event()
        self._lock = threading.Lock()
        self._resolutions: Dict[str, "queue_module.Queue[str]"] = {}
        self._waiting: List[str] = []

    def request_pause(self) -> None:
        self.pause_event.set()
        self._wake_event.set()

    def resume(self) -> None:
        self.pause_event.clear()
        self._wake_event.set()

    def request_cancel(self) -> None:
        # Cancelling also releases a pause, so the runner can observe the
        # cancellation promptly instead of sleeping in the pause loop.
        self.cancel_event.set()
        self.pause_event.clear()
        self._wake_event.set()

    def request_detach(self) -> None:
        """Stop campaign monitoring without cancelling submitted jobs."""
        self.detach_event.set()
        self.pause_event.clear()
        self._wake_event.set()

    def wait_for_change(self, timeout: float) -> None:
        self._wake_event.wait(timeout)
        self._wake_event.clear()

    def _queue_for(self, key: str) -> "queue_module.Queue[str]":
        with self._lock:
            return self._resolutions.setdefault(key, queue_module.Queue())

    def request_resolution(self, action: str, key: Optional[str] = None) -> None:
        if action not in {"retry", "continue", "skip", "stop"}:
            raise ValueError(f"Unknown campaign resolution: {action}")
        if key is None:
            with self._lock:
                key = self._waiting[0] if self._waiting else self.ANY
        self._queue_for(key).put(action)

    @property
    def waiting_keys(self) -> List[str]:
        with self._lock:
            return list(self._waiting)

    def wait_for_resolution(
        self,
        key: str = ANY,
        poll: Optional[Callable[[], Optional[str]]] = None,
        poll_interval: float = 10.0,
    ) -> str:
        """Block until the user resolves ``key`` (or the campaign is cancelled).

        ``poll`` is called every ``poll_interval`` seconds and may return an
        action when the situation was resolved externally (e.g. the user
        re-ran the failed jobs by hand).
        """
        q = self._queue_for(key)
        with self._lock:
            self._waiting.append(key)
        last_poll = time.monotonic()
        try:
            while not self.is_cancelled and not self.is_detached:
                try:
                    action = q.get(timeout=0.2)
                except queue_module.Empty:
                    if poll is not None and time.monotonic() - last_poll >= poll_interval:
                        last_poll = time.monotonic()
                        try:
                            action = poll()
                        except Exception:
                            action = None
                        if action:
                            return action
                    continue
                if self.is_cancelled:
                    return "stop"
                if self.is_detached:
                    return "detach"
                return action
            if self.is_detached:
                return "detach"
            return "stop"
        finally:
            with self._lock:
                if key in self._waiting:
                    self._waiting.remove(key)

    @property
    def is_paused(self) -> bool:
        return self.pause_event.is_set()

    @property
    def is_cancelled(self) -> bool:
        return self.cancel_event.is_set()

    @property
    def is_detached(self) -> bool:
        return self.detach_event.is_set()

    def reset(self) -> None:
        self.pause_event.clear()
        self.cancel_event.clear()
        self.detach_event.clear()
        self._wake_event.clear()
        with self._lock:
            self._resolutions.clear()
            self._waiting.clear()


# ============================================================================
# Logging Setup
# ============================================================================


class RichLoggingHandler(logging.Handler):
    """Custom logging handler that uses Rich Console for proper color rendering."""

    def __init__(self, console: Console):
        super().__init__()
        self.console = console
        self._output_lock = threading.RLock()
        self._has_output = False
        self._last_blank = False

    def emit(self, record: logging.LogRecord) -> None:
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
    """Logging handler that forwards log records as LOG CampaignEvents onto a queue."""

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
    """Configure logger with appropriate verbosity."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)

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
    file = Path(file)
    try:
        logger.debug(f"Loading campaign config from: {file}")

        if not file.exists():
            raise CampaignFileNotFoundError(f"Campaign config file not found: {file}")

        with open(file, "r") as f:
            data = yaml.safe_load(f)

        if not data or "apps" not in data:
            raise ConfigurationError("Invalid campaign config: missing 'apps' section")

        apps = []
        app_names = set()
        for app_data in data["apps"]:
            app = load_app_config(app_data, base_dir=file.resolve().parent)
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
        return CampaignConfig(apps=apps, parallel_clusters=parallel_clusters, name=campaign_name)

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

        dir_str = app_data.get("dir")
        if not dir_str:
            raise ConfigurationError(f"App '{name}' missing 'dir'")

        dir_path = Path(dir_str)
        if not dir_path.is_absolute():
            dir_path = (base_dir or Path.cwd()) / dir_path
        dir_path = dir_path.resolve()
        if not dir_path.is_dir():
            raise ConfigurationError(f"App '{name}' directory does not exist: {dir_path}")

        steps_data = app_data.get("steps", [])
        steps = [load_step_config(s) for s in steps_data]

        if not steps:
            raise ConfigurationError(f"App '{name}' has no steps")
        step_names = [step.name for step in steps]
        if len(step_names) != len(set(step_names)):
            raise ConfigurationError(f"App '{name}' contains duplicate step names")

        configs_data = app_data.get("configs") or []
        if isinstance(configs_data, str):
            configs_data = [configs_data]
        if not isinstance(configs_data, list) or not all(isinstance(path, str) for path in configs_data):
            raise ConfigurationError(f"App '{name}' configs must be a string or list of strings")

        whitelist = app_data.get("cluster_whitelist") or []
        if not isinstance(whitelist, list) or not all(isinstance(c, str) for c in whitelist):
            raise ConfigurationError(f"App '{name}' cluster_whitelist must be a list of strings")

        return AppConfig(
            name=name,
            dir=dir_path,
            cluster_whitelist=whitelist,
            configs=configs_data,
            steps=steps,
        )

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

        return StepConfig(name=name, script=script, jobs=jobs, jobs_args=jobs_args, on_fails=on_fails)

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
    """Best-effort suggestion of cluster names referenced by per-app whitelists."""
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


def campaign_input_hash(config_file: Optional[Path], config: CampaignConfig) -> Optional[str]:
    """Hash the campaign definition and referenced config/jobs files."""
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


_campaign_input_hash = campaign_input_hash  # backwards-compatible alias


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
# Live job state: identity, snapshots, views
# ============================================================================

JobKey = Tuple[str, str, str]  # (cluster, config, tag)


def job_key(record: Dict[str, Any], default_cluster: Optional[str] = None) -> JobKey:
    """Logical identity of a job record (or of a duplicate/existing-job dict)."""
    return (
        str(record.get("cluster") or record.get("cluster_name") or default_cluster or ""),
        str(record.get("config") or record.get("config_name") or ""),
        str(record.get("tag") or ""),
    )


def _status_str(value: Any) -> str:
    value = getattr(value, "value", value)
    return str(value).upper() if value is not None else "UNKNOWN"


def _job_recency(job: Any) -> Tuple[str, int, str]:
    """Sort key: newest queued job wins; on ties an active job beats an archived one."""
    return (
        str(getattr(job, "queued_timestamp", "") or ""),
        0 if getattr(job, "archive_name", None) else 1,
        str(getattr(job, "exp_dir", "") or ""),
    )


@dataclass
class ProjectSnapshot:
    """What an application's SbatchMan project contains for one cluster."""

    project_dir: Path
    cluster: str
    jobs: Dict[JobKey, Any] = field(default_factory=dict)  # current job per identity
    all_jobs: List[Any] = field(default_factory=list)
    error: Optional[str] = None      # project could not be read: nothing can be said about jobs
    warning: Optional[str] = None    # e.g. scheduler refresh failed; listing is still valid
    taken_at: float = 0.0

    def status_of(self, key: JobKey) -> str:
        if self.error:
            return "UNKNOWN"
        job = self.jobs.get(key)
        return "MISSING" if job is None else _status_str(job.status)


def load_project_snapshot(
    project_dir: Path, cluster: str, update_scheduler: bool = False
) -> ProjectSnapshot:
    """Read the jobs of ``cluster`` from the SbatchMan project at ``project_dir``.

    ``project_dir`` is the ``SbatchMan`` directory itself (the journal's
    ``application_projects`` value); ``use_project_root`` wants its parent.
    Failures never fall back to stale data: they are reported in ``error``.
    """
    project_dir = Path(project_dir).resolve()
    snapshot = ProjectSnapshot(project_dir=project_dir, cluster=str(cluster), taken_at=time.monotonic())
    if not project_dir.is_dir():
        snapshot.error = f"SbatchMan project directory not found: {project_dir}"
        return snapshot
    try:
        with project_config.use_project_root(project_dir.parent):
            with global_config.use_cluster_name(str(cluster)):
                if update_scheduler:
                    try:
                        sbm.update_jobs_status(cluster_name=str(cluster))
                    except Exception as exc:  # listing below is still valid
                        snapshot.warning = f"Could not refresh scheduler state: {exc}"
                jobs = sbm.jobs_list(
                    cluster_name=str(cluster),
                    from_active=True,
                    from_archived=True,
                    update_jobs=False,
                )
    except Exception as exc:
        snapshot.error = f"{type(exc).__name__}: {exc}"
        return snapshot

    for job in jobs:
        if job.cluster_name != str(cluster):
            continue
        # We know exactly which project we listed; do not trust per-job values.
        job.project_root = str(project_dir)
        snapshot.all_jobs.append(job)
        key = (job.cluster_name, job.config_name, job.tag)
        best = snapshot.jobs.get(key)
        if best is None or _job_recency(job) > _job_recency(best):
            snapshot.jobs[key] = job
    return snapshot


class ProjectJobsCache:
    """Short-lived cache of project snapshots shared by UI code.

    One directory scan per (project, cluster) serves every job lookup, instead
    of one scan (plus scheduler query) per job.
    """

    def __init__(self, max_age: float = 5.0) -> None:
        self.max_age = max_age
        self._guard = threading.Lock()
        self._locks: Dict[Tuple[str, str], threading.Lock] = {}
        self._entries: Dict[Tuple[str, str], ProjectSnapshot] = {}

    def snapshot(
        self,
        project_dir: Path,
        cluster: str,
        *,
        refresh: bool = False,
        update_scheduler: bool = False,
    ) -> ProjectSnapshot:
        key = (str(Path(project_dir).resolve()), str(cluster))
        with self._guard:
            lock = self._locks.setdefault(key, threading.Lock())
        with lock:
            entry = self._entries.get(key)
            if (
                entry is not None
                and not refresh
                and not update_scheduler
                and not entry.error
                and time.monotonic() - entry.taken_at < self.max_age
            ):
                return entry
            entry = load_project_snapshot(Path(key[0]), key[1], update_scheduler=update_scheduler)
            self._entries[key] = entry
            return entry

    def clear(self) -> None:
        with self._guard:
            self._entries.clear()


DEFAULT_JOBS_CACHE = ProjectJobsCache()


def _record_project(record: Dict[str, Any], application_project: Optional[str]) -> Optional[str]:
    # The application's project (from the journal) is authoritative; the
    # per-job value is only a fallback for journals that lack it.
    return application_project or record.get("project_root")


def resolve_recorded_job(
    job: Dict[str, Any],
    application_project: Optional[str] = None,
    cache: Optional[ProjectJobsCache] = None,
    snapshot: Optional[ProjectSnapshot] = None,
) -> Optional[Any]:
    """Return the current live job for a recorded job's identity, if any."""
    key = job_key(job)
    if snapshot is None:
        project = _record_project(job, application_project)
        if not project or not key[0]:
            return None
        snapshot = (cache or DEFAULT_JOBS_CACHE).snapshot(Path(project), key[0])
    return None if snapshot.error else snapshot.jobs.get(key)


def current_recorded_job_status(
    job: Dict[str, Any],
    application_project: Optional[str] = None,
    cache: Optional[ProjectJobsCache] = None,
    snapshot: Optional[ProjectSnapshot] = None,
) -> str:
    """Live status of a recorded job: COMPLETED, RUNNING, ..., MISSING (project
    readable, identity absent) or UNKNOWN (project unreadable). Never the
    journal's stale snapshot."""
    if str(job.get("status", "")).upper() == "DRY RUN":
        return "DRY RUN"
    key = job_key(job)
    if snapshot is None:
        project = _record_project(job, application_project)
        if not project or not key[0]:
            return "UNKNOWN"
        snapshot = (cache or DEFAULT_JOBS_CACHE).snapshot(Path(project), key[0])
    return snapshot.status_of(key)


def recorded_step_jobs(record: Dict[str, Any], default_cluster: Optional[str] = None) -> List[Dict[str, Any]]:
    """One record per logical job of a step (latest record wins)."""
    by_key: Dict[JobKey, Dict[str, Any]] = {}
    for job in record.get("jobs") or []:
        by_key[job_key(job, default_cluster)] = job
    return list(by_key.values())


@dataclass
class JobView:
    key: JobKey
    cluster: str
    config: str
    tag: str
    status: str                      # live: see current_recorded_job_status
    job_id: Any = None
    exp_dir: Optional[str] = None
    archive_name: Optional[str] = None
    runtime_seconds: Optional[float] = None
    command: Optional[str] = None
    resolved: bool = False
    last_known_status: Optional[str] = None


@dataclass
class StepView:
    app: str
    step: str
    cluster: str
    recorded_status: str             # what the journal says
    status: str                      # what to display
    jobs: List[JobView] = field(default_factory=list)
    jobs_completed: int = 0
    jobs_total: int = 0
    duration_seconds: float = 0.0
    error_message: Optional[str] = None
    job_errors: List[str] = field(default_factory=list)
    project_dir: Optional[str] = None
    needs_work: bool = False
    complete: bool = False
    sequential: bool = False
    snapshot_error: Optional[str] = None

    @property
    def key(self) -> str:
        return f"{self.app}|{self.step}|{self.cluster}"

    @property
    def exp_dirs(self) -> List[str]:
        return [j.exp_dir for j in self.jobs if j.resolved and j.exp_dir]

    @property
    def incomplete_jobs(self) -> List[JobView]:
        return [j for j in self.jobs if j.status != "COMPLETED"]


def build_step_view(
    app_name: str,
    step_name: str,
    cluster: str,
    record: Dict[str, Any],
    *,
    project_dir: Optional[str],
    snapshot: Optional[ProjectSnapshot],
    sequential: Optional[bool] = None,
) -> StepView:
    recorded = str(record.get("status", "unknown")).lower()
    jobs: List[JobView] = []
    for rec in recorded_step_jobs(record, cluster):
        key = job_key(rec, cluster)
        live = None if (snapshot is None or snapshot.error) else snapshot.jobs.get(key)
        if str(rec.get("status", "")).upper() == "DRY RUN":
            status = "DRY RUN"
        elif snapshot is None:
            status = "UNKNOWN"
        else:
            status = snapshot.status_of(key)
        jobs.append(JobView(
            key=key,
            cluster=key[0], config=key[1], tag=key[2],
            status=status,
            job_id=live.job_id if live is not None else rec.get("job_id"),
            exp_dir=live.exp_dir if live is not None else rec.get("exp_dir"),
            archive_name=live.archive_name if live is not None else rec.get("archive_name"),
            runtime_seconds=live.get_run_time() if live is not None else None,
            command=live.command if live is not None else rec.get("command"),
            resolved=live is not None,
            last_known_status=rec.get("last_known_status") or rec.get("status"),
        ))

    if sequential is None:
        sequential = bool(record.get("jobs_sequential", False))
    incomplete = any(j.status != "COMPLETED" for j in jobs)
    has_jobs = bool(jobs)
    needs_work = False
    complete = False
    display = recorded
    if recorded in (StepStatus.FAILED.value, StepStatus.CANCELLED.value):
        if has_jobs and not incomplete:
            display, complete = "recovered", True   # fixed by hand, outside the campaign
        else:
            needs_work = True
    elif recorded in (StepStatus.COMPLETED.value, StepStatus.SKIPPED.value):
        if incomplete:
            needs_work = True
            display = "incomplete jobs" if recorded == StepStatus.COMPLETED.value else recorded
        else:
            complete = recorded == StepStatus.COMPLETED.value

    if has_jobs:
        done, total = sum(j.status == "COMPLETED" for j in jobs), len(jobs)
    else:
        done, total = int(record.get("jobs_successful", 0) or 0), int(record.get("jobs_count", 0) or 0)

    duration = float(record.get("duration_seconds", 0) or 0)
    if complete and has_jobs and all(j.runtime_seconds is not None for j in jobs):
        runtimes = [float(j.runtime_seconds) for j in jobs]  # type: ignore[arg-type]
        duration = sum(runtimes) if sequential else max(runtimes)

    return StepView(
        app=app_name, step=step_name, cluster=cluster,
        recorded_status=recorded, status=display, jobs=jobs,
        jobs_completed=done, jobs_total=total, duration_seconds=duration,
        error_message=record.get("error_message"),
        job_errors=list(record.get("job_errors") or []),
        project_dir=project_dir, needs_work=needs_work, complete=complete,
        sequential=sequential,
        snapshot_error=snapshot.error if snapshot is not None else None,
    )


def build_campaign_view(
    state: Dict[str, Any],
    *,
    cache: Optional[ProjectJobsCache] = None,
    refresh: bool = False,
    sequential_lookup: Optional[Callable[[str, str], Optional[bool]]] = None,
    max_workers: int = 8,
) -> List[StepView]:
    """Resolve every recorded step of a journal dict against its live projects.

    One snapshot per (project, cluster); with ``refresh`` the scheduler is
    queried once per snapshot. Blocking: call from a worker thread.
    """
    cache = cache or DEFAULT_JOBS_CACHE
    projects = state.get("application_projects", {}) or {}

    def project_for(app_name: str, record: Dict[str, Any]) -> Optional[str]:
        if projects.get(app_name):
            return projects[app_name]
        return next((j.get("project_root") for j in record.get("jobs") or [] if j.get("project_root")), None)

    wanted: Dict[Tuple[str, str], None] = {}
    for app_name, app_data in (state.get("execution") or {}).items():
        for step_logs in (app_data.get("steps") or {}).values():
            for cluster, record in step_logs.items():
                project = project_for(app_name, record)
                if project and record.get("jobs"):
                    wanted[(project, cluster)] = None

    snapshots: Dict[Tuple[str, str], ProjectSnapshot] = {}
    if wanted:
        with ThreadPoolExecutor(max_workers=min(max_workers, len(wanted))) as pool:
            futures = {
                pool.submit(cache.snapshot, Path(p), c, refresh=refresh, update_scheduler=refresh): (p, c)
                for p, c in wanted
            }
            for future in as_completed(futures):
                snapshots[futures[future]] = future.result()

    views: List[StepView] = []
    for app_name, app_data in (state.get("execution") or {}).items():
        for step_name, step_logs in (app_data.get("steps") or {}).items():
            for cluster, record in step_logs.items():
                project = project_for(app_name, record)
                sequential = record.get("jobs_sequential")
                if sequential is None and sequential_lookup is not None:
                    sequential = sequential_lookup(app_name, step_name)
                views.append(build_step_view(
                    app_name, step_name, cluster, record,
                    project_dir=project,
                    snapshot=snapshots.get((project, cluster)) if project else None,
                    sequential=sequential,
                ))
    return views


def summarize_views(views: Iterable[StepView]) -> Dict[str, Any]:
    total = completed = failed = 0
    failed_apps: Set[str] = set()
    for view in views:
        total += 1
        if view.complete:
            completed += 1
        elif view.needs_work:
            failed += 1
            failed_apps.add(view.app)
    return {
        "total_steps": total,
        "completed_steps": completed,
        "failed_steps": failed,
        "failed_apps": sorted(failed_apps),
    }


def _in_selection(selection: Optional[Dict[str, Any]], app: str, step: str, cluster: str) -> bool:
    if not selection:
        return True
    apps = selection.get("applications")
    if apps and app not in apps:
        return False
    steps = (selection.get("steps_by_application") or {}).get(app)
    if steps is not None and step not in steps:
        return False
    clusters = ((selection.get("clusters_by_step") or {}).get(app, {})).get(step)
    return clusters is None or cluster in clusters


def _job_plan_record(view: StepView, job: JobView) -> Dict[str, Any]:
    return {
        "app": view.app, "step": view.step, "cluster": job.cluster or view.cluster,
        "config": job.config, "tag": job.tag, "job_id": job.job_id,
        "status": job.status, "exp_dir": job.exp_dir, "command": job.command,
    }


def plan_incomplete_work(
    views: Iterable[StepView],
    *,
    clusters: Optional[Iterable[str]] = None,
    selection: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Which steps/clusters/jobs still need work (live), within scope."""
    cluster_filter = set(clusters or [])
    steps_by_app: Dict[str, List[str]] = {}
    clusters_by_step: Dict[str, Dict[str, List[str]]] = {}
    affected_clusters: Set[str] = set()
    jobs: List[Dict[str, Any]] = []
    steps: List[Dict[str, Any]] = []
    for view in views:
        if cluster_filter and view.cluster not in cluster_filter:
            continue
        if not _in_selection(selection, view.app, view.step, view.cluster) or not view.needs_work:
            continue
        if view.step not in steps_by_app.setdefault(view.app, []):
            steps_by_app[view.app].append(view.step)
        step_clusters = clusters_by_step.setdefault(view.app, {}).setdefault(view.step, [])
        if view.cluster not in step_clusters:
            step_clusters.append(view.cluster)
        affected_clusters.add(view.cluster)
        steps.append({"app": view.app, "step": view.step, "cluster": view.cluster, "status": view.status})
        jobs.extend(_job_plan_record(view, j) for j in view.incomplete_jobs)
    return {
        "applications": list(steps_by_app),
        "steps_by_application": steps_by_app,
        "clusters_by_step": clusters_by_step,
        "clusters": sorted(affected_clusters),
        "jobs": jobs,
        "steps": steps,
    }


def plan_resume_jobs(
    views: Iterable[StepView],
    *,
    clusters: Optional[Iterable[str]] = None,
    selection: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Live non-COMPLETED jobs that a plain resume would act on."""
    cluster_filter = set(clusters or [])
    return [
        _job_plan_record(view, job)
        for view in views
        if (not cluster_filter or view.cluster in cluster_filter)
        and _in_selection(selection, view.app, view.step, view.cluster)
        for job in view.incomplete_jobs
    ]


# ============================================================================
# State Recovery & journal access
# ============================================================================


def execution_state_to_dict(state: ExecutionState) -> dict:
    log_data: Dict[str, Any] = {
        "journal_version": JOURNAL_VERSION,
        "campaign_id": state.campaign_id,
        "campaign_start": state.campaign_start,
        "campaign_name": state.campaign_name,
        "config_file": state.config_file,
        "config_sha256": state.config_sha256,
        "application_projects": state.application_projects,
        "campaign_end": state.campaign_end,
        "clusters_processed": state.clusters_processed,
        "dry_run": state.dry_run,
        "resume_mode": state.resume_mode,
        "force_apps": state.force_apps,
        "selection": state.selection,
        "execution": {},
    }
    for app_name, app_steps in state.execution.items():
        log_data["execution"][app_name] = {"steps": {}}
        for step_name, step_logs in app_steps.items():
            log_data["execution"][app_name]["steps"][step_name] = {
                cluster: asdict(log) for cluster, log in step_logs.items()
            }
    return log_data


def recover_execution_state(log_file: Path) -> Optional[ExecutionState]:
    """Load previous execution state from log file (tolerates older/newer schemas)."""
    logger = logging.getLogger(__name__)
    log_file = Path(log_file)
    if not log_file.exists():
        logger.debug(f"No previous log file found at {log_file}")
        return None
    try:
        with open(log_file, "r") as f:
            data = json.load(f)
        known = {f.name for f in fields(StepExecutionLog)}
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
        for app_name, app_data in data.get("execution", {}).items():
            state.execution[app_name] = {}
            for step_name, step_data in app_data.get("steps", {}).items():
                state.execution[app_name][step_name] = {}
                for cluster_name, log_data in step_data.items():
                    state.execution[app_name][step_name][cluster_name] = StepExecutionLog(
                        **{k: v for k, v in log_data.items() if k in known}
                    )
        logger.info(f"Recovered execution state for campaign {state.campaign_id}")
        return state
    except Exception as e:
        if DEBUG_STACKTRACE:
            console.print_exception()
        raise StateRecoveryError(f"Could not recover campaign journal {log_file}: {e}") from e


def _lock_path(log_file: Path) -> Path:
    return Path(str(log_file) + ".lock")


def journal_lock_owner(log_file: Path) -> Optional[str]:
    """Describe the live process holding the journal, or None if it is free."""
    path = _lock_path(log_file)
    try:
        info = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None if not path.exists() else "unknown process"
    pid, host = info.get("pid"), info.get("host")
    description = f"pid {pid} on {host}"
    if host == socket.gethostname() and isinstance(pid, int):
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return None  # stale lock
        except PermissionError:
            pass
    return description  # other hosts cannot be verified: assume alive


@contextmanager
def journal_lock(log_file: Path):
    """Advisory exclusive lock: one process at a time may write a journal."""
    log_file = Path(log_file)
    owner = journal_lock_owner(log_file)
    if owner:
        raise JournalLockedError(
            f"Campaign journal {log_file.name} is in use by {owner}. "
            f"If that is wrong, delete {_lock_path(log_file)}."
        )
    path = _lock_path(log_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        path.unlink()
    except FileNotFoundError:
        pass
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as exc:
        raise JournalLockedError(f"Campaign journal {log_file.name} was just locked by another process") from exc
    with os.fdopen(fd, "w") as f:
        json.dump({"pid": os.getpid(), "host": socket.gethostname(), "started": datetime.now().isoformat()}, f)
    try:
        yield
    finally:
        try:
            path.unlink()
        except FileNotFoundError:
            pass


def save_campaign_selection(log_file: Path, selection: Dict[str, Any]) -> None:
    """Persist an edited selection without racing a running campaign."""
    log_file = Path(log_file)
    with journal_lock(log_file):
        journal = json.loads(log_file.read_text())
        if not isinstance(journal, dict):
            raise CampaignRunnerError("Campaign journal must contain a JSON object")
        journal["selection"] = selection
        tmp = log_file.with_suffix(log_file.suffix + ".tmp")
        tmp.write_text(json.dumps(journal, indent=2) + "\n")
        tmp.replace(log_file)


def is_app_completed(app_name: str, cluster: str, state: ExecutionState) -> bool:
    """Check if app completed all steps on cluster (journal status only)."""
    if app_name not in state.execution:
        return False
    for step_logs in state.execution[app_name].values():
        if cluster not in step_logs or step_logs[cluster].status != StepStatus.COMPLETED.value:
            return False
    return True


def is_step_completed(
    app_name: str,
    step_name: str,
    cluster: str,
    state: ExecutionState,
    definition_hash: Optional[str] = None,
    snapshot: Optional[ProjectSnapshot] = None,
) -> bool:
    """Step is up to date: completed, same definition, and every recorded job
    COMPLETED *now* (live, via ``snapshot``)."""
    log = state.execution.get(app_name, {}).get(step_name, {}).get(cluster)
    if log is None or log.status != StepStatus.COMPLETED.value:
        return False
    if definition_hash is None or log.definition_hash != definition_hash:
        return False
    return all(
        current_recorded_job_status(job, snapshot=snapshot) == "COMPLETED"
        for job in recorded_step_jobs(asdict(log), cluster)
    )


def _has_non_completed_jobs(jobs: List[Dict[str, Any]], snapshot: Optional[ProjectSnapshot] = None) -> bool:
    """Whether any recorded job is not COMPLETED (live when a snapshot is given)."""
    if snapshot is not None:
        return any(current_recorded_job_status(j, snapshot=snapshot) != "COMPLETED" for j in jobs)
    return any(str(j.get("last_known_status") or j.get("status", "")).upper() != "COMPLETED" for j in jobs)


def get_failed_apps(state: ExecutionState, cache: Optional[ProjectJobsCache] = None) -> List[str]:
    """Names of apps with at least one step needing work (live)."""
    return summarize_views(build_campaign_view(execution_state_to_dict(state), cache=cache))["failed_apps"]


def build_status_rows(state: ExecutionState) -> List[Dict[str, Any]]:
    """Flatten an ExecutionState into row dicts (journal values, no live lookup)."""
    rows: List[Dict[str, Any]] = []
    for app_name, app_steps in state.execution.items():
        for step_name, step_logs in app_steps.items():
            for cluster, log in step_logs.items():
                rows.append({
                    "app": app_name, "step": step_name, "cluster": cluster,
                    "status": log.status, "duration_seconds": log.duration_seconds,
                    "jobs_count": log.jobs_count, "jobs_successful": log.jobs_successful,
                    "jobs_failed": log.jobs_failed, "error_message": log.error_message,
                })
    return rows


def list_campaign_logs(results_dir: Path) -> List[Path]:
    """List previous campaign log files in `results_dir`, most recent first."""
    results_dir = Path(results_dir)
    if not results_dir.exists():
        return []
    return sorted(results_dir.glob("campaign_log_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)


def load_campaign_log_summary(
    log_file: Path,
    cache: Optional[ProjectJobsCache] = None,
    refresh: bool = False,
) -> Optional[Dict[str, Any]]:
    """Summary dict for a history list, with step counts resolved live.

    Blocking (reads project listings): call from a worker thread.
    """
    try:
        with open(log_file, "r") as f:
            data = json.load(f)
    except Exception:
        return None
    summary = summarize_views(build_campaign_view(data, cache=cache, refresh=refresh))
    return {
        "log_file": str(log_file),
        "campaign_id": data.get("campaign_id"),
        "campaign_name": data.get("campaign_name"),
        "campaign_start": data.get("campaign_start"),
        "campaign_end": data.get("campaign_end"),
        "clusters_processed": data.get("clusters_processed", []),
        "application_projects": data.get("application_projects", {}),
        "config_file": data.get("config_file"),
        "dry_run": data.get("dry_run", False),
        **summary,
    }


# ============================================================================
# Job Monitoring (by identity, against the live project)
# ============================================================================


def monitor_jobs(
    keys: List[JobKey],
    project_dir: Path,
    cluster: str,
    *,
    owned_exp_dirs: Optional[Set[str]] = None,
    poll_interval: Optional[float] = None,
    max_wait: float = 86400,
    logger: Optional[logging.Logger] = None,
    control: Optional[CampaignControl] = None,
    on_progress: Optional[Callable[[int, int, int], None]] = None,
    on_snapshot: Optional[Callable[[ProjectSnapshot, Dict[JobKey, str]], None]] = None,
) -> Dict[JobKey, str]:
    """Poll the project until every job identity in ``keys`` is terminal.

    Jobs are looked up by (cluster, config, tag) on every iteration, so the
    monitor follows whatever the user does by hand: a job that is re-run, moved
    to an archive or replaced is followed to its latest incarnation. Jobs that
    appear in the project but are not tracked are reported once and ignored.

    ``on_progress(terminal, succeeded, total)``; ``on_snapshot(snapshot, statuses)``.
    Cancellation cancels only jobs whose exp_dir is in ``owned_exp_dirs``
    (those submitted by the campaign), never jobs started by the user.
    """
    logger = logger or logging.getLogger(__name__)
    if poll_interval is None:
        poll_interval = DEFAULT_POLL_INTERVAL
    owned = owned_exp_dirs or set()
    tracked = set(keys)
    statuses: Dict[JobKey, str] = {k: "UNKNOWN" for k in keys}
    if not keys:
        return statuses

    def cancel_owned() -> None:
        snapshot = load_project_snapshot(project_dir, cluster, update_scheduler=True)
        cancelled, errors = 0, []
        if snapshot.error:
            errors.append(snapshot.error)
        for key in keys:
            job = None if snapshot.error else snapshot.jobs.get(key)
            if job is None or _status_str(job.status) in TERMINAL_STATES:
                continue
            if job.archive_name or job.exp_dir not in owned:
                logger.info(f"Leaving externally launched job {job.tag} ({job.job_id}) running")
                continue
            if not job.job_id:
                errors.append(f"Job {job.tag} has no scheduler ID; could not cancel it")
                continue
            try:
                with project_config.use_project_root(Path(project_dir).resolve().parent):
                    with global_config.use_cluster_name(cluster):
                        job.get_job_config().cancel_job(job.job_id)
                job.status = "CANCELLED"
                job.end_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S.%f")
                job.write_metadata()
                cancelled += 1
                logger.info(f"Cancelled job {job.job_id} ({job.tag})")
            except Exception as exc:
                errors.append(f"Job {job.job_id} ({job.tag}): {exc}")
                logger.error(f"Could not cancel job {job.job_id} ({job.tag}): {exc}")
        message = f"Job polling cancelled by user; cancelled {cancelled} active job(s)"
        if errors:
            message += ". Cancellation errors: " + "; ".join(errors)
        raise CampaignCancelledError(message)

    started = time.monotonic()
    missing_polls: Dict[JobKey, int] = {}
    baseline: Optional[Set[Tuple[str, str]]] = None
    reported_external: Set[Tuple[str, str]] = set()
    warned: Set[str] = set()
    consecutive_errors = 0

    while True:
        if control is not None:
            if control.is_detached:
                raise CampaignDetachedError("Campaign monitoring detached; submitted jobs remain active")
            if control.is_cancelled:
                cancel_owned()
            while control.is_paused and not control.is_cancelled and not control.is_detached:
                time.sleep(0.2)
            if control.is_cancelled:
                cancel_owned()
            if control.is_detached:
                raise CampaignDetachedError("Campaign monitoring detached; submitted jobs remain active")

        snapshot = load_project_snapshot(project_dir, cluster, update_scheduler=True)
        if snapshot.warning and snapshot.warning not in warned:
            warned.add(snapshot.warning)
            logger.warning(snapshot.warning)

        if snapshot.error:
            consecutive_errors += 1
            logger.warning(f"Could not read project jobs ({consecutive_errors}/5): {snapshot.error}")
            if consecutive_errors >= 5:
                raise JobExecutionError(f"Cannot read the SbatchMan project for monitoring: {snapshot.error}")
            statuses = {k: "UNKNOWN" for k in keys}
        else:
            consecutive_errors = 0
            for key in keys:
                status = snapshot.status_of(key)
                if status == "MISSING":
                    missing_polls[key] = missing_polls.get(key, 0) + 1
                    if missing_polls[key] < MISSING_GRACE_POLLS:
                        status = "UNKNOWN"
                else:
                    missing_polls.pop(key, None)
                statuses[key] = status

            present = {(j.cluster_name, j.exp_dir) for j in snapshot.all_jobs}
            if baseline is None:
                baseline = present
            else:
                for job in snapshot.all_jobs:
                    ident = (job.cluster_name, job.exp_dir)
                    key = (job.cluster_name, job.config_name, job.tag)
                    if ident in baseline or ident in reported_external or key in tracked:
                        continue
                    reported_external.add(ident)
                    logger.info(
                        f"External job detected (not part of this step, ignored): "
                        f"{job.config_name} / {job.tag} (ID {job.job_id}, {_status_str(job.status)})"
                    )

        if on_snapshot is not None:
            on_snapshot(snapshot, dict(statuses))
        terminal = sum(s in TERMINAL_STATES for s in statuses.values())
        succeeded = sum(s == "COMPLETED" for s in statuses.values())
        if on_progress is not None:
            on_progress(terminal, succeeded, len(keys))

        if terminal == len(keys):
            logger.info(f"All {len(keys)} job(s) reached terminal state")
            break
        logger.debug(f"Job progress: {terminal}/{len(keys)} terminal ({time.monotonic() - started:.0f}/{max_wait}s)")
        if time.monotonic() - started >= max_wait:
            logger.warning(f"Job polling reached max wait time of {max_wait}s")
            break
        if control is not None:
            control.wait_for_change(poll_interval)
        else:
            time.sleep(poll_interval)

    logger.info(f"Job polling completed: {sum(s == 'COMPLETED' for s in statuses.values())} completed, "
                f"{sum(s != 'COMPLETED' for s in statuses.values())} NOT completed")
    return statuses


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


# ============================================================================
# Project Initialization
# ============================================================================


def initialize_project(app_dir: Path, dry_run: bool = False) -> None:
    """Initialize SbatchMan project in the app directory."""
    logger = logging.getLogger(__name__)
    try:
        if dry_run:
            logger.debug(f"[DRY-RUN] Would initialize project in {app_dir}")
            return
        sbm.init_project(app_dir, no_logo=True)
    except sbm.ProjectExistsError:
        pass
    except Exception as e:
        logger.error(f"Failed to initialize project in {app_dir}: {str(e)}")
        raise ProjectInitializationError(f"Failed to initialize project in {app_dir}: {str(e)}")


# ============================================================================
# Main Campaign Runner
# ============================================================================

EXCLUDED_BY_SELECTION = "Excluded by campaign selection"


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
        config_sha256: Optional[str] = None,
    ):
        self.config = config
        self.clusters_by_step = clusters_by_step or {}
        self.results_dir = Path(results_dir).resolve()
        self.log_file = Path(log_file).resolve()
        self.verbose = verbose
        self.dry_run = dry_run
        self.config_file = Path(config_file).resolve() if config_file else None
        # Hash of the FULL campaign definition (before any app/step selection).
        self.config_sha256 = config_sha256 or campaign_input_hash(self.config_file, config)

        self.event_queue = event_queue
        self.control = control
        self._pause_emitted = False
        self._resuming = False
        self._checkpoint_lock = threading.RLock()
        self._app_setup_locks = {app.name: threading.Lock() for app in config.apps}

        self.logger = setup_logger(
            __name__, verbose, console_output=console_output, event_queue=event_queue,
        )

        self.state = ExecutionState(
            campaign_id=datetime.now().strftime("%Y%m%d_%H%M%S_%f"),
            campaign_start=datetime.now().isoformat(),
            campaign_name=config.name,
            config_file=str(self.config_file) if self.config_file else None,
            config_sha256=self.config_sha256,
            application_projects={app.name: str(app.dir / "SbatchMan") for app in self.config.apps},
            selection=campaign_selection or {},
        )
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
        with self._checkpoint_lock:
            return execution_state_to_dict(self.state)

    @staticmethod
    def _jobs_copy(log: StepExecutionLog) -> List[Dict[str, Any]]:
        # Events cross threads; never share the mutable journal records.
        return copy.deepcopy(log.jobs)

    def _step_status_snapshot(self) -> List[Dict[str, Any]]:
        """Final per-cluster step data needed to reconcile the TUI."""
        with self._checkpoint_lock:
            return [
                {
                    "app": app_name, "step": step_name, "cluster": cluster,
                    "status": log.status, "duration": log.duration_seconds,
                    "jobs": self._jobs_copy(log), "jobs_count": log.jobs_count,
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
                    json.dump(execution_state_to_dict(self.state), f, indent=2)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(temporary_file, self.log_file)
            except OSError as exc:
                raise CampaignRunnerError(f"Could not checkpoint campaign state at {self.log_file}: {exc}") from exc

    def _check_control(self) -> None:
        """Raise if cancellation was requested; block while paused."""
        if self.control is None:
            return
        if self.control.is_detached:
            raise CampaignDetachedError("Campaign detached from the TUI")
        if self.control.is_cancelled:
            raise CampaignCancelledError("Campaign cancelled by user")
        if self.control.is_paused:
            if not self._pause_emitted:
                self._emit(EventType.PAUSED)
                self.logger.info("Campaign paused")
                self._pause_emitted = True
            while (self.control.is_paused and not self.control.is_cancelled
                   and not self.control.is_detached):
                time.sleep(0.2)
            if self.control.is_cancelled:
                raise CampaignCancelledError("Campaign cancelled by user")
            if self.control.is_detached:
                raise CampaignDetachedError("Campaign detached from the TUI")
            self._emit(EventType.RESUMED)
            self.logger.info("Campaign resumed")
            self._pause_emitted = False

    # ------------------------------------------------------------------
    # Campaign / cluster / app loops
    # ------------------------------------------------------------------

    def _compute_success(self) -> bool:
        for app_steps in self.state.execution.values():
            for step_logs in app_steps.values():
                for log in step_logs.values():
                    if log.status == StepStatus.COMPLETED.value and not _has_non_completed_jobs(log.jobs):
                        continue
                    if log.status == StepStatus.SKIPPED.value and log.error_message == EXCLUDED_BY_SELECTION:
                        continue
                    return False
        return True

    def run_campaign(
        self,
        clusters: List[str],
        resume: bool = False,
        resume_mode: str = "prompt",
        force_apps: Optional[List[str]] = None,
    ) -> bool:
        """Execute the campaign across clusters. Returns True if everything succeeded.

        With ``resume`` the existing journal at ``self.log_file`` is continued
        in place (identity, history and selection are kept).
        """
        force_apps = force_apps or []
        if self.dry_run:
            return self._run_campaign_locked(clusters, resume, resume_mode, force_apps)
        with journal_lock(self.log_file):
            return self._run_campaign_locked(clusters, resume, resume_mode, force_apps)

    def _run_campaign_locked(
        self, clusters: List[str], resume: bool, resume_mode: str, force_apps: List[str]
    ) -> bool:
        self.state.clusters_processed = clusters
        self.state.resume_mode = resume_mode
        self.state.force_apps = force_apps
        self.state.dry_run = self.dry_run

        previous_state: Optional[ExecutionState] = None
        if resume and self.log_file.exists():
            self.logger.info("Resuming from the existing campaign journal")
            previous_state = recover_execution_state(self.log_file)  # raises StateRecoveryError
            if previous_state:
                # Keep the campaign identity and every earlier record; this
                # run updates the same journal in place.
                self.state.campaign_id = previous_state.campaign_id
                self.state.campaign_start = previous_state.campaign_start
                self.state.campaign_name = previous_state.campaign_name or self.state.campaign_name
                self.state.application_projects = {
                    **self.state.application_projects, **previous_state.application_projects
                }
                self.state.execution = copy.deepcopy(previous_state.execution)
                self.state.selection = copy.deepcopy(previous_state.selection) or self.state.selection
        self._resuming = previous_state is not None

        self._emit(
            EventType.CAMPAIGN_START,
            campaign_id=self.state.campaign_id,
            campaign_name=self.state.campaign_name,
            clusters=clusters,
            apps=[a.name for a in self.config.apps],
        )

        try:
            self._print_campaign_header(clusters)
            self._execute_clusters(clusters, previous_state, force_apps)
            self._finalize()
            success = self._compute_success()
            self._emit(EventType.CAMPAIGN_END, success=success, steps=self._step_status_snapshot())
            return success
        except CampaignDetachedError as e:
            self.logger.info(f"Campaign detached; leaving queued/running jobs alone: {e}")
            self.state.campaign_end = None
            self._checkpoint()
            self._emit(EventType.CAMPAIGN_END, success=False, detached=True,
                       steps=self._step_status_snapshot())
            return False
        except CampaignCancelledError as e:
            self.logger.warning(f"Campaign cancelled: {str(e)}")
            self._emit(EventType.CAMPAIGN_CANCELLED, message=str(e))
            self._finalize()
            self._emit(EventType.CAMPAIGN_END, success=False, steps=self._step_status_snapshot())
            return False
        except CampaignRunnerError as e:
            self.logger.error(f"Campaign failed: {str(e)}")
            if DEBUG_STACKTRACE:
                console.print_exception()
            self._finalize()
            self._emit(EventType.CAMPAIGN_END, success=False, steps=self._step_status_snapshot())
            return False

    def _execute_clusters(
        self, clusters: List[str], previous_state: Optional[ExecutionState], force_apps: List[str]
    ) -> None:
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

    def _run_cluster_in_context(self, cluster, previous_state, force_apps):
        with global_config.use_cluster_name(cluster):
            self._run_cluster(cluster, previous_state, force_apps)

    def _run_cluster(
        self, cluster: str, previous_state: Optional[ExecutionState], force_apps: List[str]
    ) -> None:
        """Execute all apps on a single cluster."""
        self.logger.info("")
        if self.event_queue is None:
            self.logger.info(f"=== Cluster: {cluster} ===")
        self._emit(EventType.CLUSTER_START, cluster=cluster)

        for app in self.config.apps:
            self._check_control()

            if not self._should_run_app_on_cluster(app, cluster):
                self.logger.info(f"[SKIPPED] {app.name} (cluster filter)")
                self._emit(EventType.APP_SKIPPED, app=app.name, cluster=cluster, reason="cluster filter")
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
        if selected_steps and not any(cluster in selected_steps.get(step.name, []) for step in app.steps):
            return False
        return True

    def _project_dir(self, app: AppConfig) -> Path:
        """The application's SbatchMan directory (contains experiments/ and archive/)."""
        return Path(self.state.application_projects.get(app.name) or (app.dir / "SbatchMan"))

    def _application_project_parent(self, app: AppConfig) -> Path:
        """What `use_project_root` / `init_project` expect."""
        return self._project_dir(app).parent

    def _setup_app_for_cluster(self, app: AppConfig, cluster: str) -> None:
        # App projects are shared across clusters; initialize and update their
        # config files one cluster at a time to avoid concurrent writes.
        app_lock = self._app_setup_locks[app.name]
        if self.control is None:
            with app_lock:
                self._setup_app_for_cluster_locked(app, cluster)
            return
        # A parallel cluster can be waiting on setup while the user requests
        # cancellation: acquire with a timeout so the request is noticed.
        while not app_lock.acquire(timeout=0.2):
            self._check_control()
        try:
            self._check_control()
            self._setup_app_for_cluster_locked(app, cluster)
        finally:
            app_lock.release()

    def _setup_app_for_cluster_locked(self, app: AppConfig, cluster: str) -> None:
        """Initialize the project and create configs once, before any step runs."""
        self.logger.debug(f"Setting up app '{app.name}' for cluster '{cluster}'")
        start_time = time.time()
        try:
            try:
                initialize_project(self._application_project_parent(app), dry_run=self.dry_run)
            except ProjectInitializationError as e:
                self.logger.error(f"Project initialization failed: {str(e)}")
                raise
            if app.configs:
                self.logger.info(f"Creating configs from files '{app.configs}'")
                for config_file in app.configs:
                    config_path = app.dir / config_file
                    if not config_path.exists():
                        self.logger.error(f"Config file not found: {config_path}")
                        raise CampaignFileNotFoundError(f"Config file not found: {config_path}")
                    if not self.dry_run:
                        sbm.create_configs_from_file(config_path, overwrite=True)
            self.logger.debug(f"Setup completed in {time.time() - start_time:.1f}s")
        except CampaignRunnerError as e:
            self.logger.error(f"App setup failed after {time.time() - start_time:.1f}s: {str(e)}")
            raise
        finally:
            self._checkpoint()

    def _preview_jobs(
        self, app: AppConfig, step: StepConfig, cluster: str, options: Dict[str, Any], fresh: bool
    ) -> List[Any]:
        """Expand a jobs YAML in an isolated temporary project without submitting jobs."""
        with tempfile.TemporaryDirectory(prefix="sbatchman-campaign-preview-") as temporary_dir:
            temporary_root = Path(temporary_dir)
            sbm.init_project(temporary_root, no_logo=True)
            temporary_project = temporary_root / "SbatchMan"
            existing_configs = self._project_dir(app) / "configs"
            if existing_configs.is_dir():
                shutil.copytree(existing_configs, temporary_project / "configs", dirs_exist_ok=True)
            with project_config.use_project_root(temporary_root):
                for config_file in app.configs:
                    sbm.create_configs_from_file(app.dir / config_file, overwrite=True)
                try:
                    return sbm.launch_jobs_from_file(
                        app.dir / step.jobs,
                        force=options["force"] and fresh,
                        dry_run=True,
                        quiet=True,
                        filter_tags=options["filter_tags"],
                        filter_variables=options["filter_variables"],
                        ignore_archived=options["ignore_archived"] and fresh,
                        ignore_conf_in_dup_check=options["ignore_conf_in_dup_check"],
                        ignore_commands_in_dup_check=options["ignore_commands_in_dup_check"],
                        rerun_non_completed=True,
                    )
                except Exception as exc:
                    raise JobExecutionError(
                        f"Could not preview jobs for step '{step.name}' on cluster '{cluster}': {exc}"
                    ) from exc

    def _run_app(
        self, app: AppConfig, cluster: str, previous_state: Optional[ExecutionState], force: bool = False,
    ) -> None:
        """Run an application inside its isolated SbatchMan project context."""
        project_parent = self._application_project_parent(app)
        self.logger.debug(f"Application SbatchMan project: {self._project_dir(app)}")
        with project_config.use_project_root(project_parent):
            self._run_app_in_project(app, cluster, previous_state, force=force)

    def _step_up_to_date(
        self,
        app: AppConfig,
        cluster: str,
        previous_log: Optional[StepExecutionLog],
        definition_hash: str,
    ) -> bool:
        """Completed with this exact definition AND every recorded job COMPLETED right now."""
        if (
            previous_log is None
            or previous_log.status != StepStatus.COMPLETED.value
            or previous_log.definition_hash != definition_hash
        ):
            return False
        if not previous_log.jobs:
            return True
        snapshot = load_project_snapshot(self._project_dir(app), cluster, update_scheduler=True)
        if snapshot.error:
            self.logger.warning(f"Cannot verify previous jobs ({snapshot.error}); step will be re-run")
            return False
        return not _has_non_completed_jobs(previous_log.jobs, snapshot)

    def _run_app_in_project(
        self, app: AppConfig, cluster: str, previous_state: Optional[ExecutionState], force: bool = False,
    ) -> None:
        """Execute single app on single cluster."""
        self.logger.info("")
        if self.event_queue is None:
            self.logger.info(f"--- Application: {app.name} ---")
        self.logger.info(f"Directory: {app.dir}")
        self._emit(EventType.APP_START, app=app.name, cluster=cluster)

        with self._checkpoint_lock:
            self.state.execution.setdefault(app.name, {})

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
                    # A resume limited by a changed selection must not touch
                    # (or overwrite) records outside that selection.
                    continue
                self.logger.info(f"Step '{step.name}' skipped on cluster '{cluster}' (cluster selection)")
                skipped_log = StepExecutionLog(
                    status=StepStatus.SKIPPED.value,
                    error_message=EXCLUDED_BY_SELECTION,
                    definition_hash=_step_definition_hash(app, step),
                )
                with self._checkpoint_lock:
                    self.state.execution.setdefault(app.name, {}).setdefault(step.name, {})[cluster] = skipped_log
                self._emit(EventType.STEP_SKIPPED, app=app.name, cluster=cluster, step=step.name,
                           reason="cluster selection")
                continue

            current_step_hash = _step_definition_hash(app, step)
            previous_step_log = (
                previous_state.execution.get(app.name, {}).get(step.name, {}).get(cluster)
                if previous_state else None
            )
            if (
                not force and previous_step_log
                and previous_step_log.status == StepStatus.COMPLETED.value
                and previous_step_log.definition_hash
                and previous_step_log.definition_hash != current_step_hash
            ):
                self.logger.warning(f"Step changed: {step.name} (definition or input files changed); rerunning")
            if previous_state and not force and self._step_up_to_date(app, cluster, previous_step_log, current_step_hash):
                self.logger.info(f"Step unchanged: {step.name} (completed, all jobs COMPLETED; skipping)")
                self._emit(EventType.STEP_SKIPPED, app=app.name, cluster=cluster, step=step.name,
                           reason="already completed")
                with self._checkpoint_lock:
                    self.state.execution.setdefault(app.name, {}).setdefault(step.name, {})[cluster] = (
                        copy.deepcopy(previous_state.execution[app.name][step.name][cluster])
                    )
                continue

            self.logger.info(f"Step: {step.name}")
            while True:
                self._emit(EventType.STEP_START, app=app.name, cluster=cluster, step=step.name)
                try:
                    success = self._execute_step(app, step, cluster)
                except CampaignCancelledError:
                    raise
                except StepExecutionError:
                    success = False

                log = self.state.execution[app.name][step.name][cluster]
                self._emit(
                    EventType.STEP_END, app=app.name, cluster=cluster, step=step.name,
                    status=log.status, duration=log.duration_seconds, jobs=self._jobs_copy(log),
                    jobs_count=log.jobs_count, jobs_successful=log.jobs_successful,
                    jobs_failed=log.jobs_failed,
                )
                if success:
                    break

                if self.control is not None and self.event_queue is not None:
                    key = f"{app.name}|{step.name}|{cluster}"
                    self._emit(
                        EventType.STEP_NEEDS_ATTENTION, app=app.name, cluster=cluster, step=step.name,
                        error=log.error_message or "Step failed.", jobs=self._jobs_copy(log),
                    )

                    def externally_fixed() -> Optional[str]:
                        # The user may repair the failed jobs by hand (re-run,
                        # fix) while we wait: notice it instead of blocking.
                        if not log.jobs or log.script_exit_code not in (None, 0):
                            return None
                        snap = load_project_snapshot(self._project_dir(app), cluster, update_scheduler=True)
                        if snap.error or _has_non_completed_jobs(log.jobs, snap):
                            return None
                        self.logger.info(
                            f"All jobs of {app.name}/{step.name} on {cluster} are now COMPLETED "
                            "(fixed outside the campaign)"
                        )
                        return "external"

                    resolution = self.control.wait_for_resolution(key, poll=externally_fixed)
                    if resolution == "detach":
                        raise CampaignDetachedError("Campaign detached from the TUI")
                    log.resolution_history.append({"action": resolution, "timestamp": datetime.now().isoformat()})
                    self._checkpoint()
                    if resolution == "retry":
                        continue
                    if resolution in ("continue", "external"):
                        log.status = StepStatus.COMPLETED.value
                        log.manual_resolution = (
                            "completed_externally" if resolution == "external"
                            else "continued_after_manual_intervention"
                        )
                        log.error_message = None
                    elif resolution == "skip":
                        log.status = StepStatus.SKIPPED.value
                        log.manual_resolution = "skipped_by_user"
                    else:
                        raise CampaignCancelledError("Campaign stopped by user at a failed step")
                    self._checkpoint()
                    self._emit(
                        EventType.STEP_END, app=app.name, cluster=cluster, step=step.name,
                        status=log.status, duration=log.duration_seconds, jobs=self._jobs_copy(log),
                        jobs_count=log.jobs_count, jobs_successful=log.jobs_successful,
                        jobs_failed=log.jobs_failed,
                    )
                    break

                policy = OnFailsPolicy(step.on_fails)
                self.logger.warning(f"Step '{step.name}' failed, applying policy: {policy.value}")
                if policy == OnFailsPolicy.TERMINATE:
                    raise StepExecutionError(f"Step '{step.name}' failed with TERMINATE policy")
                if policy == OnFailsPolicy.SKIP:
                    log.status = StepStatus.SKIPPED.value
                    self._checkpoint()
                break

        self.logger.info(f"  Finished application: {app.name}")
        self._emit(EventType.APP_END, app=app.name, cluster=cluster)

    # ------------------------------------------------------------------
    # Step execution
    # ------------------------------------------------------------------

    @staticmethod
    def _submitted_record(job: Any) -> Dict[str, Any]:
        return {
            "cluster": job.cluster_name, "config": job.config_name, "tag": job.tag,
            "command": job.command, "scheduler": job.scheduler,
            "job_id": job.job_id, "exp_dir": job.exp_dir, "archive_name": job.archive_name,
            "project_root": job.project_root,
            "last_known_status": _status_str(job.status),
            "submitted": True,
        }

    @staticmethod
    def _adopted_record(duplicate: Dict[str, Any], cluster: str) -> Dict[str, Any]:
        existing = duplicate.get("existing_job") or {}
        return {
            "cluster": existing.get("cluster_name") or duplicate.get("cluster") or cluster,
            "config": existing.get("config_name") or duplicate.get("config"),
            "tag": existing.get("tag") or duplicate.get("tag"),
            "command": existing.get("command"), "scheduler": existing.get("scheduler"),
            "job_id": existing.get("job_id"), "exp_dir": existing.get("exp_dir"),
            "archive_name": existing.get("archive_name"), "project_root": existing.get("project_root"),
            "last_known_status": _status_str(existing.get("status") or "DUPLICATE"),
            "submitted": False, "adopted": True,
            "skipped_reason": duplicate.get("reason"), "duplicate_source": duplicate.get("source"),
            "_queued": str(existing.get("queued_timestamp") or ""),
        }

    def _refresh_records_from_snapshot(
        self, records: List[Dict[str, Any]], snapshot: ProjectSnapshot, cluster: str
    ) -> None:
        """Refresh the hints in the journal records from the live project."""
        if snapshot.error:
            return
        for rec in records:
            live = snapshot.jobs.get(job_key(rec, cluster))
            if live is None:
                continue
            rec["last_known_status"] = _status_str(live.status)
            rec["job_id"], rec["exp_dir"] = live.job_id, live.exp_dir
            rec["archive_name"], rec["project_root"] = live.archive_name, live.project_root
            runtime = live.get_run_time()
            if runtime is not None:
                rec["runtime_seconds"] = runtime

    def _execute_step(self, app: AppConfig, step: StepConfig, cluster: str) -> bool:
        """Execute single step (script + jobs). Returns True if successful."""
        self.logger.debug(f"Starting step execution for '{step.name}'")
        start_time = time.time()
        jobs_sequential = False

        def step_duration() -> float:
            wall_duration = time.time() - start_time
            runtimes = [
                float(r["runtime_seconds"]) for r in log.jobs if r.get("runtime_seconds") is not None
            ]
            if step.jobs and not self.dry_run and runtimes:
                return sum(runtimes) if jobs_sequential else max(runtimes)
            return wall_duration

        with self._checkpoint_lock:
            step_execution = self.state.execution.setdefault(app.name, {}).setdefault(step.name, {})
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
        # The jobs_args options `force` / `ignore_archived` apply to the first
        # launch of a step only; once the step has recorded jobs, SbatchMan's
        # own duplicate handling (re-running only non-COMPLETED jobs) decides.
        fresh = not (previous_log and previous_log.jobs)

        try:
            if step.script:
                if self.dry_run:
                    self.logger.info(f"[DRY RUN] Would execute script in {app.dir}")
                else:
                    self.logger.info(f"Executing script in {app.dir}")
                    script_env = os.environ.copy()
                    script_env["SBATCHMAN_CLUSTER_NAME"] = cluster
                    script_env["SBATCHMAN_PROJECT_ROOT"] = str(self._application_project_parent(app))
                    success, stdout, stderr, exit_code = self._execute_script(step.script, app.dir, env=script_env)
                    log.script_executed = True
                    log.script_stdout, log.script_stderr, log.script_exit_code = stdout, stderr, exit_code
                    if not success:
                        self.logger.error(f"Script failed with exit code {exit_code}")
                        raise ScriptExecutionError(f"Script failed with exit code {exit_code}")
                    self.logger.info("Script executed successfully")

            if step.jobs:
                jobs_path = app.dir / step.jobs
                if not jobs_path.exists():
                    self.logger.error(f"Jobs file not found: {jobs_path}")
                    raise CampaignFileNotFoundError(f"Jobs file not found: {jobs_path}")
                jobs_file_data = yaml.safe_load(jobs_path.read_text()) or {}
                jobs_sequential = bool(jobs_file_data.get("sequential", False)) if isinstance(jobs_file_data, dict) else False
                log.jobs_sequential = jobs_sequential
                options = resolve_jobs_args(step.jobs_args, cluster)

                if self.dry_run:
                    planned = self._preview_jobs(app, step, cluster, options, fresh)
                    self.logger.info(f"[DRY RUN] {len(planned)} job(s) would be generated from {step.jobs}")
                    for job in planned:
                        self.logger.info(
                            f"  Job: {job.config_name} / {job.tag} | scheduler={job.scheduler} | "
                            f"directory={job.exp_dir} | variables={job.variables} | command={job.command}"
                        )
                    log.jobs = [
                        {**self._submitted_record(job), "job_id": None, "status": "DRY RUN", "submitted": False}
                        for job in planned
                    ]
                    log.jobs_count = len(log.jobs)
                else:
                    self._launch_and_monitor(app, step, cluster, log, options, fresh, step_duration)

            log.status = StepStatus.COMPLETED.value
            log.duration_seconds = step_duration()
            self.logger.info(f"Step complete: {step.name} ({log.duration_seconds:.1f}s)")
            return True

        except CampaignCancelledError as e:
            log.status = StepStatus.CANCELLED.value
            log.error_message = str(e)
            log.duration_seconds = step_duration()
            self.logger.warning(f"Step '{step.name}' cancelled after {log.duration_seconds:.1f}s")
            raise
        except CampaignRunnerError as e:
            log.status = StepStatus.FAILED.value
            log.error_message = str(e)
            log.duration_seconds = step_duration()
            self.logger.error(f"Step '{step.name}' failed after {log.duration_seconds:.1f}s: {e}")
            raise StepExecutionError(str(e))
        finally:
            self.logger.debug(f"Restoring working directory to: {self.original_cwd}")
            os.chdir(self.original_cwd)
            self._checkpoint()

    def _launch_and_monitor(
        self,
        app: AppConfig,
        step: StepConfig,
        cluster: str,
        log: StepExecutionLog,
        options: Dict[str, Any],
        fresh: bool,
        step_duration: Callable[[], float],
    ) -> None:
        """Submit what is missing, adopt what already exists, follow all of it.

        Jobs are tracked by identity (cluster, config, tag). Whatever the user
        already ran is adopted (COMPLETED: never re-run; RUNNING/PENDING:
        followed); non-COMPLETED terminal jobs are submitted again by SbatchMan.
        """
        # Make sure jobs killed behind our back (scancel, timeouts) show their
        # real state before SbatchMan decides what is a duplicate.
        try:
            sbm.update_jobs_status(cluster_name=cluster)
        except Exception as exc:
            self.logger.warning(f"Could not refresh scheduler state before launching: {exc}")

        self.logger.info(f"Running jobs from file '{step.jobs}'")
        try:
            launch_result = sbm.launch_jobs_from_file(
                app.dir / step.jobs,
                force=options["force"] and fresh,
                quiet=True,
                filter_tags=options["filter_tags"],
                filter_variables=options["filter_variables"],
                ignore_archived=options["ignore_archived"] and fresh,
                ignore_conf_in_dup_check=options["ignore_conf_in_dup_check"],
                ignore_commands_in_dup_check=options["ignore_commands_in_dup_check"],
                working_directory=app.dir,
                return_details=True,
                rerun_non_completed=True,
            )
        except Exception as exc:
            raise JobExecutionError(
                f"Could not launch jobs for step '{step.name}' on cluster '{cluster}': {exc}"
            ) from exc

        records: Dict[JobKey, Dict[str, Any]] = {}
        for duplicate in launch_result.duplicate_jobs:
            rec = self._adopted_record(duplicate, cluster)
            key = job_key(rec, cluster)
            if key not in records or rec["_queued"] >= records[key].get("_queued", ""):
                records[key] = rec
        submitted = [self._submitted_record(job) for job in launch_result.jobs]
        for rec in submitted:
            records[job_key(rec, cluster)] = rec  # a fresh submission supersedes an old match
        for rec in records.values():
            rec.pop("_queued", None)

        keys = list(records)
        log.jobs = list(records.values())
        log.jobs_count = len(keys)
        log.jobs_launched = bool(submitted)
        if submitted:
            log.job_attempts.append({
                "attempt": log.attempt_count,
                "submitted_at": datetime.now().isoformat(),
                "jobs": copy.deepcopy(submitted),
            })
        self._checkpoint()
        self.logger.info(
            f"{len(submitted)} job(s) submitted, "
            f"{len(keys) - len(submitted)} existing job(s) adopted"
        )

        if not keys:
            candidates, filtered = launch_result.candidate_count, launch_result.filtered_count
            if filtered and filtered >= candidates:
                reason = (f"none of the {candidates} generated job definition(s) matched the "
                          "configured tag or variable filters")
            else:
                reason = "the jobs file produced no matching job definitions for this cluster"
            self.logger.warning(f"No jobs for step '{step.name}' on cluster '{cluster}': {reason}.")
            return

        owned = {
            j["exp_dir"] for attempt in log.job_attempts for j in attempt.get("jobs", []) if j.get("exp_dir")
        }

        def on_progress(terminal: int, succeeded: int, total: int) -> None:
            self._emit(
                EventType.STEP_PROGRESS, app=app.name, cluster=cluster, step=step.name,
                completed=terminal, succeeded=succeeded, total=total,
                jobs=self._jobs_copy(log), duration=step_duration(),
            )

        def on_snapshot(snapshot: ProjectSnapshot, statuses: Dict[JobKey, str]) -> None:
            self._refresh_records_from_snapshot(log.jobs, snapshot, cluster)
            self._checkpoint()

        self.logger.info(f"Following {len(keys)} job(s) until completion")
        statuses = monitor_jobs(
            keys, self._project_dir(app), cluster,
            owned_exp_dirs=owned, logger=self.logger, control=self.control,
            on_progress=on_progress, on_snapshot=on_snapshot,
        )

        passed = sum(s == "COMPLETED" for s in statuses.values())
        failed = len(keys) - passed
        log.jobs_successful, log.jobs_failed = passed, failed
        log.job_errors = [
            f"Job {key[1]} / {key[2]}: {status}" for key, status in statuses.items() if status != "COMPLETED"
        ]
        for key, rec in zip(keys, log.jobs):
            self.logger.info(
                f"  Job: {rec.get('config')} / {rec.get('tag')} | "
                f"status={statuses.get(key)} | id={rec.get('job_id')}"
            )
        self.logger.info(f"Jobs completed: {passed} passed, {failed} not completed")
        if failed:
            raise JobExecutionError(f"Jobs failed: {failed} not completed, {passed} passed")
        self.logger.info("All jobs completed successfully")

    def _execute_script(
        self, script: str, cwd: Path, env: Optional[Dict[str, str]] = None,
    ) -> Tuple[bool, str, str, int]:
        """Execute bash script in `cwd`. Returns (success, stdout, stderr, exit_code).

        Runs via Popen so a cancellation request can terminate the child.
        Pausing does not suspend a running script; it applies between
        steps/apps and during job polling.
        """
        if self.dry_run:
            self.logger.info(f"[DRY RUN] Would execute script in {cwd}")
            return True, "", "", 0

        self.logger.debug(f"Executing script in working directory: {cwd}")
        self.logger.debug(f"Script content:\n{script}")

        timeout = 3600
        start = time.time()
        try:
            proc = subprocess.Popen(
                script, shell=True, cwd=cwd, text=True,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env,
            )
        except Exception as e:
            raise ScriptExecutionError(f"Script execution failed to start: {str(e)}")

        # Drain the pipes while waiting: a chatty script would otherwise block
        # on a full pipe buffer and look like it hangs until the timeout.
        outputs: Dict[str, str] = {"stdout": "", "stderr": ""}

        def drain(name: str, stream) -> None:
            outputs[name] = stream.read()

        readers = [
            threading.Thread(target=drain, args=("stdout", proc.stdout), daemon=True),
            threading.Thread(target=drain, args=("stderr", proc.stderr), daemon=True),
        ]
        for reader in readers:
            reader.start()

        try:
            while proc.poll() is None:
                if self.control is not None and self.control.is_detached:
                    # The script is independent work; let it continue after the
                    # TUI stops supervising the campaign.
                    raise CampaignDetachedError("Campaign detached from the TUI")
                if self.control is not None and self.control.is_cancelled:
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                    raise CampaignCancelledError("Script execution cancelled by user")
                if time.time() - start > timeout:
                    proc.kill()
                    raise ScriptExecutionError("Script execution timed out (1 hour limit)")
                time.sleep(0.2)
            for reader in readers:
                reader.join(timeout=5)
            stdout, stderr = outputs["stdout"], outputs["stderr"]
            self.logger.debug(f"Script completed with exit code: {proc.returncode}")
            if stdout:
                self.logger.debug(f"Script stdout:\n{stdout}")
            if stderr:
                self.logger.debug(f"Script stderr:\n{stderr}")
            return proc.returncode == 0, stdout, stderr, proc.returncode
        except (CampaignCancelledError, CampaignDetachedError, ScriptExecutionError):
            raise
        except Exception as e:
            raise ScriptExecutionError(f"Script execution failed: {str(e)}")

    def _finalize(self) -> None:
        """Write log and print summary."""
        self.state.campaign_end = datetime.now().isoformat()
        if not self.dry_run:
            self.logger.info(f"Writing campaign journal to '{self.log_file}'")
            self._checkpoint()
        self._print_summary(self._state_as_dict())

    def _print_campaign_header(self, clusters: List[str]) -> None:
        self.logger.info("Campaign run")
        self.logger.info(f"  ID:       {self.state.campaign_id}")
        if self.state.campaign_name:
            self.logger.info(f"  Name:     {self.state.campaign_name}")
        self.logger.info(f"  Started:  {self.state.campaign_start}")
        self.logger.info(f"  Results:  {self.results_dir}")
        self.logger.info(f"  Clusters: {', '.join(clusters)}")
        self.logger.info(f"  Apps:     {', '.join(a.name for a in self.config.apps)}")
        self.logger.info(f"  Mode:     {'dry run' if self.dry_run else 'execute'}; "
                         f"{'resume' if self._resuming else 'new run'}")

    def _print_summary(self, log_data: dict) -> None:
        """Print execution summary with job statistics per cluster and app."""
        total_steps = completed_steps = failed_steps = 0
        total_jobs = successful_jobs = failed_jobs = 0
        summary_by_cluster: Dict[str, Dict[str, Dict[str, int]]] = {}

        for app_name, app_steps in log_data["execution"].items():
            for step_logs in app_steps["steps"].values():
                for cluster, log in step_logs.items():
                    total_steps += 1
                    bad = log["status"] in ("failed", "cancelled")
                    completed_steps += log["status"] == "completed"
                    failed_steps += bad
                    total_jobs += log["jobs_count"]
                    successful_jobs += log["jobs_successful"]
                    failed_jobs += log["jobs_failed"]
                    stats = summary_by_cluster.setdefault(cluster, {}).setdefault(app_name, {
                        "steps_count": 0, "steps_completed": 0, "steps_failed": 0,
                        "jobs_total": 0, "jobs_successful": 0, "jobs_failed": 0,
                    })
                    stats["steps_count"] += 1
                    stats["steps_completed"] += log["status"] == "completed"
                    stats["steps_failed"] += bad
                    stats["jobs_total"] += log["jobs_count"]
                    stats["jobs_successful"] += log["jobs_successful"]
                    stats["jobs_failed"] += log["jobs_failed"]

        self.logger.info("")
        self.logger.info("Execution summary")
        self.logger.info(f"Total steps: {total_steps}")
        self.logger.info(f"Completed: {completed_steps}")
        self.logger.info(f"Failed or cancelled: {failed_steps}")
        if self.dry_run:
            self.logger.info(f"Planned jobs: {total_jobs}")
        else:
            self.logger.info(f"Total jobs: {total_jobs}")
            self.logger.info(f"Successful: {successful_jobs}")
            self.logger.info(f"Failed: {failed_jobs}")
        self.logger.info("")

        for cluster in sorted(summary_by_cluster):
            self.logger.info(f"\nCluster: {cluster}")
            for app_name in sorted(summary_by_cluster[cluster]):
                stats = summary_by_cluster[cluster][app_name]
                steps_info = f"Steps: {stats['steps_completed']}/{stats['steps_count']}"
                jobs_info = (
                    f"Jobs planned: {stats['jobs_total']}" if self.dry_run
                    else f"Jobs: {stats['jobs_successful']}/{stats['jobs_total']}"
                )
                status = "OK" if stats["steps_failed"] == 0 and stats["jobs_failed"] == 0 else "FAILED"
                self.logger.info(f"{status:<7}{app_name:<30} {steps_info:<20} {jobs_info}")
            self.logger.info("")


# ============================================================================
# Entry Points
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
    """Run a campaign from a YAML config.

    With ``resume=True`` the most recent journal in ``results_dir`` is continued
    in place (a new campaign is started when there is none).

    ``event_queue`` / ``control`` enable live monitoring and remote control from
    another thread; a TUI that owns the terminal should pass
    ``console_output=False``.
    """
    logger = setup_logger(__name__, verbose, console_output=console_output, event_queue=event_queue)
    try:
        results_dir = Path(results_dir)
        if resume:
            logs = list_campaign_logs(results_dir)
            if logs:
                return run_campaign_with_existing_log(
                    config_file=config_file, log_file=logs[0], clusters=clusters, verbose=verbose,
                    dry_run=dry_run, resume_mode=resume_mode, force_apps=force_apps,
                    event_queue=event_queue, control=control, console_output=console_output,
                    applications=applications, steps=steps, steps_by_application=steps_by_application,
                    clusters_by_step=clusters_by_step,
                )
            logger.warning(f"No previous campaign journal in {results_dir}; starting a new campaign")

        full_config = load_campaign_config(config_file)
        full_hash = campaign_input_hash(Path(config_file), full_config)
        config = select_campaign_workflow(full_config, applications, steps, steps_by_application)
        clusters = list(dict.fromkeys(clusters or discover_clusters_from_config(config)))
        if not clusters:
            raise ConfigurationError("No cluster targets were supplied or declared in app cluster_whitelist fields.")

        results_dir.mkdir(parents=True, exist_ok=True)
        log_file = results_dir / f"campaign_log_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.json"

        runner = CampaignRunner(
            config=config, results_dir=results_dir, log_file=log_file, verbose=verbose, dry_run=dry_run,
            event_queue=event_queue, control=control, console_output=console_output,
            config_file=config_file, clusters_by_step=clusters_by_step,
            campaign_selection=campaign_selection, config_sha256=full_hash,
        )
        success = runner.run_campaign(clusters=clusters, resume=False, resume_mode=resume_mode,
                                      force_apps=force_apps or [])
        logger.info("Campaign completed successfully" if success else "Campaign completed with failures")
        return success

    except CampaignRunnerError as e:
        logger.error(f"{'Configuration Error' if isinstance(e, ConfigurationError) else 'Error'}: {e}")
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
    """Resume the campaign recorded in ``log_file`` (updated in place).

    Steps whose definition is unchanged and whose recorded jobs are all
    COMPLETED *now* are skipped; everything else is (re)evaluated against the
    live project, so work done or undone by hand is honoured. If no explicit
    selection is given, the selection saved in the journal is used.
    """
    logger = setup_logger(__name__, verbose, console_output=console_output, event_queue=event_queue)
    log_file = Path(log_file)
    try:
        try:
            journal = json.loads(log_file.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            raise StateRecoveryError(f"Could not read campaign journal {log_file}: {exc}") from exc

        if applications is None and steps is None and steps_by_application is None and clusters_by_step is None:
            saved = journal.get("selection") or {}
            if saved:
                applications = saved.get("applications") or None
                steps_by_application = saved.get("steps_by_application") or None
                clusters_by_step = saved.get("clusters_by_step") or None
                selected = sorted({
                    c for app_steps in (clusters_by_step or {}).values()
                    for step_clusters in app_steps.values() for c in step_clusters
                })
                if selected:
                    clusters = [c for c in clusters if c in selected] or selected
        clusters = list(dict.fromkeys(clusters or journal.get("clusters_processed") or []))

        full_config = load_campaign_config(config_file)
        full_hash = campaign_input_hash(Path(config_file), full_config)
        if journal.get("config_sha256") and journal["config_sha256"] != full_hash:
            logger.info("Campaign configuration changed since this journal was written; "
                        "step definitions are compared individually and only changed steps re-run")
        config = select_campaign_workflow(full_config, applications, steps, steps_by_application)

        runner = CampaignRunner(
            config=config, results_dir=log_file.parent, log_file=log_file, verbose=verbose,
            dry_run=dry_run, event_queue=event_queue, control=control, console_output=console_output,
            config_file=config_file, clusters_by_step=clusters_by_step, config_sha256=full_hash,
        )
        return runner.run_campaign(clusters=clusters, resume=True, resume_mode=resume_mode,
                                   force_apps=force_apps or [])

    except CampaignRunnerError as e:
        logger.error(f"Error: {e}")
        if DEBUG_STACKTRACE:
            console.print_exception()
        return False
