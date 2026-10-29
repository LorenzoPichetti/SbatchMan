"""
campaign_tui.py
================

Interactive terminal UI (Textual) for managing and monitoring application
campaigns built on top of `campaign.py`.

Usage
-----
    python campaign_tui.py [--config CAMPAIGN.yaml] [--results-dir DIR]
                            [--clusters cluster1,cluster2] [--dry-run] [--verbose]

All flags just pre-fill the start screen; everything can also be entered
interactively. This script is meant to be launched by your external CLI,
e.g.:

    campaign-cli tui --config campaign.yaml --results-dir ./results

Screens
-------
- MenuScreen: configure campaigns, inspect previous runs, view step jobs,
  resume, and re-run failed apps from one page.
- RunScreen: live table of app/step/cluster status, a job-progress bar,
  a streaming log panel, and Pause / Cancel / Back controls. The campaign
  runs in a background thread; a queue.Queue carries structured events
  from that thread into the UI.
"""

from __future__ import annotations

import argparse
import json
import queue
import sys
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple

import yaml
from rich.text import Text
from rich.markup import escape as rich_escape

from textual.app import App, ComposeResult
from textual.screen import ModalScreen, Screen
from textual.binding import Binding
from textual.containers import Grid, Horizontal, Vertical, VerticalScroll
from textual.widgets import (
    Header,
    Footer,
    Static,
    Button,
    Input,
    Switch,
    DataTable,
    RichLog,
    ProgressBar,
    ListView,
    ListItem,
    Label,
    Checkbox,
)
from textual.worker import Worker, WorkerState
from textual import work

from sbatchman.core.campaign import (
    CampaignControl,
    CampaignEvent,
    EventType,
    StepStatus,
    CampaignRunnerError,
    load_campaign_config,
    discover_clusters_from_config,
    list_campaign_logs,
    load_campaign_log_summary,
    run_campaign,
    run_campaign_with_existing_log,
    current_recorded_job_status,
    resolve_recorded_job,
)
from sbatchman.tui.jobs_screen import JobsScreen


# ============================================================================
# Helpers
# ============================================================================


STATUS_STYLES = {
    "pending": "dim",
    "running": "bold yellow",
    "completed": "bold green",
    "failed": "bold red",
    "skipped": "dim cyan",
    "cancelled": "bold magenta",
    "needs attention": "bold red on yellow",
}


class CampaignJobsScreen(JobsScreen):
    """Read-only SbatchMan status view that returns to the campaign screen."""

    BINDINGS = [binding for binding in JobsScreen.BINDINGS if binding.key != "q"] + [
        Binding("q", "campaign_back", "Back to campaign")
    ]

    def action_campaign_back(self) -> None:
        self.app.pop_screen()


def status_text(status: str) -> Text:
    return Text(status, style=STATUS_STYLES.get(status, ""))


def parse_clusters(raw: str) -> List[str]:
    return [c.strip() for c in raw.split(",") if c.strip()]


def campaign_display_label(campaign_name: Optional[str], campaign_id: str) -> str:
    """Format a campaign name and ID for TUI text that uses Rich markup."""
    if campaign_name:
        return f"{rich_escape(str(campaign_name))} ({rich_escape(str(campaign_id or 'unknown'))})"
    return rich_escape(str(campaign_id or "unknown"))


def _recorded_step_jobs(step_record: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return unique jobs across attempts, with the current attempt taking precedence."""
    jobs_by_key: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    for attempt in step_record.get("job_attempts") or []:
        for job in attempt.get("jobs") or []:
            key = (
                str(job.get("cluster", "")),
                str(job.get("job_id") or ""),
                str(job.get("exp_dir") or ""),
                str(job.get("tag") or ""),
            )
            jobs_by_key[key] = dict(job)
    for job in step_record.get("jobs") or []:
        key = (
            str(job.get("cluster", "")),
            str(job.get("job_id") or ""),
            str(job.get("exp_dir") or ""),
            str(job.get("tag") or ""),
        )
        jobs_by_key[key] = dict(job)
    return list(jobs_by_key.values())


def _recorded_job_status(
    job: Dict[str, Any], application_project: Optional[str] = None
) -> str:
    """Resolve job identity before falling back to its journal snapshot."""
    return current_recorded_job_status(job, application_project)


def _recorded_job_counts(
    record: Dict[str, Any], application_project: Optional[str] = None
) -> Tuple[int, int]:
    jobs = _recorded_step_jobs(record)
    if not jobs:
        return int(record.get("jobs_successful", 0)), int(record.get("jobs_count", 0))
    completed = sum(
        _recorded_job_status(job, application_project) == "COMPLETED"
        for job in jobs
    )
    return completed, len(jobs)


def _display_recorded_step_status(
    record: Dict[str, Any], application_project: Optional[str] = None
) -> str:
    status = str(record.get("status", "unknown"))
    if status.lower() == StepStatus.COMPLETED.value and any(
        _recorded_job_status(job, application_project) != "COMPLETED"
        for job in _recorded_step_jobs(record)
    ):
        return "incomplete jobs"
    return status


def _save_campaign_selection_to_journal(log_file: Path, selection: Dict[str, Any]) -> None:
    log_file = Path(log_file)
    journal = json.loads(log_file.read_text())
    if not isinstance(journal, dict):
        raise ValueError("Campaign journal must contain a JSON object")
    journal["selection"] = selection
    temporary_file = log_file.with_suffix(log_file.suffix + ".tmp")
    temporary_file.write_text(json.dumps(journal, indent=2) + "\n")
    temporary_file.replace(log_file)


class ConfirmCampaignRerunScreen(ModalScreen[bool]):
    """Show the recorded jobs affected by a campaign rerun before proceeding."""

    BINDINGS = [
        Binding("escape", "cancel", "Cancel"),
        Binding("q", "cancel", "Cancel"),
    ]

    CSS = """
    ConfirmCampaignRerunScreen { align: center middle; }
    #rerun_box { width: 90%; max-width: 110; height: 85%; border: round $warning; background: $surface; padding: 1 2; }
    #rerun_title { height: 2; text-style: bold; color: $warning; }
    #rerun_description { height: auto; padding-bottom: 1; }
    .rerun_job_heading { height: 1; color: $text-muted; padding-bottom: 1; }
    #rerun_jobs { height: 1fr; border: solid $surface-lighten-2; padding: 0 1; }
    .rerun_job { height: auto; padding: 0 0 1 0; }
    #rerun_buttons { height: 3; align: center middle; }
    #rerun_buttons Button { margin: 0 1; }
    """

    def __init__(
        self,
        title: str,
        description: str,
        jobs: List[Dict[str, Any]],
        steps: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        super().__init__()
        self.title = title
        self.description = description
        self.jobs = jobs
        self.steps = steps or []

    def compose(self) -> ComposeResult:
        with Vertical(id="rerun_box"):
            yield Static(self.title, id="rerun_title")
            yield Static(self.description, id="rerun_description")
            with VerticalScroll(id="rerun_jobs"):
                if self.steps:
                    yield Static("Steps and clusters to retry:", classes="rerun_job_heading")
                    for item in self.steps:
                        app = rich_escape(str(item.get("app", "")))
                        step = rich_escape(str(item.get("step", "")))
                        cluster = rich_escape(str(item.get("cluster", "unknown")))
                        status = rich_escape(str(item.get("status", "unknown")))
                        yield Static(
                            f"[b]{app}/{step}[/b]  cluster=[cyan]{cluster}[/cyan]  status={status}",
                            classes="rerun_job",
                        )
                if self.jobs:
                    yield Static(
                        "Jobs to be re-submitted (only jobs not COMPLETED):",
                        classes="rerun_job_heading",
                    )
                for index, job in enumerate(self.jobs, start=1):
                    app = rich_escape(str(job.get("app", "")))
                    step = rich_escape(str(job.get("step", "")))
                    cluster = rich_escape(str(job.get("cluster", "unknown")))
                    config = rich_escape(str(job.get("config", "unknown")))
                    tag = rich_escape(str(job.get("tag", "unknown")))
                    job_id = rich_escape(str(job.get("job_id") or "not assigned"))
                    status = rich_escape(str(job.get("status", "unknown")))
                    exp_dir = rich_escape(str(job.get("exp_dir", "unknown")))
                    command = rich_escape(str(job.get("command", "")))
                    command_line = f"\nCommand: {command}" if command else ""
                    yield Static(
                        f"[b]{index}. {app}/{step}[/b]  cluster=[cyan]{cluster}[/cyan]\n"
                        f"Job ID={job_id}  status={status}  config={config}  tag={tag}\n"
                        f"Directory: {exp_dir}{command_line}",
                        classes="rerun_job",
                    )
            with Horizontal(id="rerun_buttons"):
                yield Button("Confirm rerun", variant="error", id="rerun_confirm_btn")
                yield Button("Cancel", id="rerun_cancel_btn")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "rerun_confirm_btn":
            self.dismiss(True)
        elif event.button.id == "rerun_cancel_btn":
            self.dismiss(False)

    def action_cancel(self) -> None:
        self.dismiss(False)


class CampaignSelectionScreen(Screen):
    """Choose the applications and steps included in a new campaign run."""

    CSS = """
    #selection_title { height: 3; padding: 0 1; }
    #selection_body { height: 1fr; padding: 0 1; }
    .app_card { border: round $primary; padding: 0 1; margin: 0 0 1 0; height: auto; }
    .app_heading { height: 3; align: left middle; }
    .step_card { border: tall $surface-lighten-2; padding: 0 1; margin: 0 0 1 2; height: auto; }
    .step_heading { height: 2; }
    .cluster_label { height: 1; color: $text-muted; padding-left: 2; }
    .cluster_grid { grid-size: 2; grid-columns: 1fr 1fr; grid-gutter: 0 1; height: auto; padding: 0 2; }
    #selection_error { height: 2; color: $error; }
    #selection_controls { height: 3; align: center middle; }
    """

    def __init__(
        self,
        *,
        config_file: Path,
        results_dir: Path,
        clusters: List[str],
        dry_run: bool,
        verbose: bool,
        selection: Optional[Dict[str, Any]] = None,
        edit_log: Optional[Path] = None,
    ) -> None:
        super().__init__()
        self.config_file = Path(config_file)
        self.results_dir = Path(results_dir)
        self.clusters = clusters
        self.dry_run = dry_run
        self.verbose = verbose
        self.config = load_campaign_config(self.config_file)
        self.campaign_selection = selection or {}
        self.edit_log = Path(edit_log) if edit_log else None
        self.saved = self._selection_preferences()
        self.app_checks: Dict[str, Checkbox] = {}
        self.step_checks: Dict[Tuple[str, str], Checkbox] = {}
        self.cluster_checks: Dict[Tuple[str, str, str], Checkbox] = {}
        self._ids: Dict[str, Tuple[str, Optional[str], Optional[str]]] = {}

    def _selection_preferences(self) -> Dict[str, Any]:
        ui_preferences = self.campaign_selection.get("ui")
        if isinstance(ui_preferences, dict):
            return ui_preferences
        selected_apps = self.campaign_selection.get("applications")
        selected_steps = self.campaign_selection.get("steps_by_application") or {}
        selected_clusters = self.campaign_selection.get("clusters_by_step") or {}
        preferences: Dict[str, Any] = {"applications": {}}
        for app in self.config.apps:
            app_selected = app.name in selected_apps if isinstance(selected_apps, list) else True
            app_steps: Dict[str, Any] = {}
            selected_app_steps = selected_steps.get(app.name)
            app_cluster_steps = selected_clusters.get(app.name, {})
            for step in app.steps:
                step_selected = (
                    step.name in selected_app_steps
                    if isinstance(selected_app_steps, list) else True
                )
                stored_clusters = app_cluster_steps.get(step.name)
                app_steps[step.name] = {
                    "enabled": step_selected,
                    "clusters": {
                        cluster: cluster in stored_clusters
                        if isinstance(stored_clusters, list) else True
                        for cluster in self._available_clusters(app)
                    },
                }
            preferences["applications"][app.name] = {
                "enabled": app_selected,
                "steps": app_steps,
            }
        return preferences

    def _available_clusters(self, app) -> List[str]:
        if app.cluster_whitelist:
            return list(app.cluster_whitelist)
        return list(self.clusters)

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static(
            "Select applications, steps, and clusters. All available choices start selected "
            "unless this campaign already has a saved selection.",
            id="selection_title",
        )
        with VerticalScroll(id="selection_body"):
            saved_apps = self.saved.get("applications", {})
            if not isinstance(saved_apps, dict):
                saved_apps = {}
            for app_index, app in enumerate(self.config.apps):
                saved_app = saved_apps.get(app.name, {})
                if not isinstance(saved_app, dict):
                    saved_app = {}
                saved_steps = saved_app.get("steps", {})
                if not isinstance(saved_steps, dict):
                    saved_steps = {}
                app_enabled = bool(saved_app.get("enabled", True))
                app_id = f"app_{app_index}"
                app_check = Checkbox(app.name, value=app_enabled, id=app_id)
                self.app_checks[app.name] = app_check
                self._ids[app_id] = (app.name, None, None)
                with Vertical(classes="app_card"):
                    with Horizontal(classes="app_heading"):
                        yield app_check
                    for step_index, step in enumerate(app.steps):
                        saved_step = saved_steps.get(step.name, True)
                        if isinstance(saved_step, dict):
                            step_enabled = bool(saved_step.get("enabled", True))
                            saved_clusters = saved_step.get("clusters", {})
                        else:
                            step_enabled = bool(saved_step)
                            saved_clusters = {}
                        if not isinstance(saved_clusters, dict):
                            saved_clusters = {}
                        step_id = f"step_{app_index}_{step_index}"
                        step_check = Checkbox(step.name, value=step_enabled, id=step_id)
                        self.step_checks[(app.name, step.name)] = step_check
                        self._ids[step_id] = (app.name, step.name, None)
                        with Vertical(classes="step_card"):
                            with Horizontal(classes="step_heading"):
                                yield step_check
                            if app.cluster_whitelist:
                                whitelist_text = ", ".join(app.cluster_whitelist)
                                cluster_heading = f"Select clusters (YAML whitelist: {whitelist_text})"
                            else:
                                cluster_heading = "Select clusters (campaign targets)"
                            yield Static(cluster_heading, classes="cluster_label")
                            with Grid(classes="cluster_grid"):
                                for cluster_index, cluster in enumerate(self._available_clusters(app)):
                                    cluster_id = f"cluster_{app_index}_{step_index}_{cluster_index}"
                                    cluster_check = Checkbox(
                                        cluster,
                                        value=bool(saved_clusters.get(cluster, True)),
                                        id=cluster_id,
                                    )
                                    yield cluster_check
                                    self.cluster_checks[(app.name, step.name, cluster)] = cluster_check
                                    self._ids[cluster_id] = (app.name, step.name, cluster)

        yield Static("", id="selection_error")
        with Horizontal(id="selection_controls"):
            yield Button(
                "Save selection" if self.edit_log else "Run selected",
                id="run_selected_btn",
                variant="success",
            )
            yield Button("Back", id="selection_back_btn")
        yield Footer()

    def on_checkbox_changed(self, event: Checkbox.Changed) -> None:
        info = self._ids.get(event.checkbox.id or "")
        if info is None:
            return
        app_name, step_name, cluster_name = info
        if step_name is None and event.value:
            app_steps = [
                (step, check) for (name, step), check in self.step_checks.items()
                if name == app_name
            ]
            if app_steps and not any(check.value for _, check in app_steps):
                for step, check in app_steps:
                    check.value = True
                    for (cluster_app, cluster_step, _), cluster_check in self.cluster_checks.items():
                        if cluster_app == app_name and cluster_step == step:
                            cluster_check.value = True
        elif step_name is not None and cluster_name is None:
            if event.value:
                step_clusters = [
                    check for (name, step, _), check in self.cluster_checks.items()
                    if name == app_name and step == step_name
                ]
                if step_clusters and not any(check.value for check in step_clusters):
                    for check in step_clusters:
                        check.value = True

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "selection_back_btn":
            self.app.pop_screen()
            return
        if event.button.id != "run_selected_btn":
            return

        selected_apps = []
        steps_by_app: Dict[str, List[str]] = {}
        clusters_by_app_step: Dict[str, Dict[str, List[str]]] = {}
        selected_campaign_clusters = set()
        remembered: Dict[str, Any] = {"applications": {}}
        for app in self.config.apps:
            enabled = self.app_checks[app.name].value
            selected_steps = []
            remembered_steps: Dict[str, Any] = {}
            selected_clusters_by_step: Dict[str, List[str]] = {}
            for step in app.steps:
                enabled_step = self.step_checks[(app.name, step.name)].value
                selected_clusters = [
                    cluster for cluster in self._available_clusters(app)
                    if self.cluster_checks[(app.name, step.name, cluster)].value
                ]
                remembered_steps[step.name] = {
                    "enabled": enabled_step,
                    "clusters": {
                        cluster: self.cluster_checks[(app.name, step.name, cluster)].value
                        for cluster in self._available_clusters(app)
                    },
                }
                if enabled_step:
                    if enabled and self._available_clusters(app) and not selected_clusters:
                        self.query_one("#selection_error", Static).update(
                            f"Choose at least one cluster for step '{step.name}' in '{app.name}'."
                        )
                        return
                    selected_steps.append(step.name)
                    selected_clusters_by_step[step.name] = selected_clusters
            remembered["applications"][app.name] = {
                "enabled": enabled,
                "steps": remembered_steps,
            }
            if enabled:
                if not selected_steps:
                    self.query_one("#selection_error", Static).update(
                        f"Select at least one step for application '{app.name}', or uncheck the application."
                    )
                    return
                selected_apps.append(app.name)
                steps_by_app[app.name] = selected_steps
                clusters_by_app_step[app.name] = selected_clusters_by_step
                for step_clusters in selected_clusters_by_step.values():
                    selected_campaign_clusters.update(step_clusters)
        if not selected_apps:
            self.query_one("#selection_error", Static).update(
                "Select at least one application and step."
            )
            return

        campaign_selection = {
            "applications": selected_apps,
            "steps_by_application": steps_by_app,
            "clusters_by_step": clusters_by_app_step,
            "ui": remembered,
        }
        if self.edit_log is not None:
            try:
                _save_campaign_selection_to_journal(self.edit_log, campaign_selection)
            except (OSError, ValueError, json.JSONDecodeError) as e:
                self.query_one("#selection_error", Static).update(
                    f"Could not save the campaign selection: {e}"
                )
                return
            self.app.pop_screen()
            self.app.notify("Campaign selection saved to its journal.", severity="information")
            return
        self.app.switch_screen(
            RunScreen(
                mode="new",
                config_file=self.config_file,
                results_dir=self.results_dir,
                clusters=sorted(selected_campaign_clusters),
                dry_run=self.dry_run,
                verbose=self.verbose,
                applications=selected_apps,
                steps_by_application=steps_by_app,
                clusters_by_step=clusters_by_app_step,
                campaign_selection=campaign_selection,
            )
        )


# ============================================================================
# Run Screen
# ============================================================================


class RunScreen(Screen):
    """Live monitoring + control screen for a single campaign run."""

    CSS = """
    #status_table { height: 1fr; }
    #log_view { height: 16; border: solid $accent; }
    #progress_label { height: 1; }
    #controls { height: 3; align: center middle; }
    """

    BINDINGS = [("escape", "try_back", "Back (when finished)"), ("j", "view_step_jobs", "View step jobs")]

    def __init__(
        self,
        *,
        mode: str,  # "new" | "resume"
        config_file: Path,
        results_dir: Path,
        clusters: List[str],
        dry_run: bool,
        verbose: bool,
        resume_mode: str = "auto",
        force_apps: Optional[List[str]] = None,
        log_file: Optional[Path] = None,
        applications: Optional[List[str]] = None,
        steps_by_application: Optional[Dict[str, List[str]]] = None,
        clusters_by_step: Optional[Dict[str, Dict[str, List[str]]]] = None,
        campaign_selection: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__()
        self.mode = mode
        self.config_file = Path(config_file)
        self.results_dir = Path(results_dir)
        campaign_config = load_campaign_config(self.config_file)
        self.application_projects = {
            app.name: app.dir / "SbatchMan"
            for app in campaign_config.apps
        }
        self.campaign_name = campaign_config.name
        self.clusters = clusters
        self.dry_run = dry_run
        self.verbose = verbose
        self.resume_mode = resume_mode
        self.force_apps = force_apps or []
        self.applications = applications
        self.steps_by_application = steps_by_application
        self.clusters_by_step = clusters_by_step
        self.campaign_selection = campaign_selection
        self.log_file = Path(log_file) if log_file else None
        self.campaign_id: Optional[str] = None
        if self.log_file is not None:
            try:
                journal = json.loads(self.log_file.read_text())
                self.campaign_id = journal.get("campaign_id")
                self.campaign_name = journal.get("campaign_name") or self.campaign_name
            except (OSError, json.JSONDecodeError):
                pass

        self.event_queue: "queue.Queue[CampaignEvent]" = queue.Queue()
        self.control = CampaignControl()
        self.success: Optional[bool] = None
        self.finished = False
        self.awaiting_resolution = False
        self.pending_step_resolution: Optional[Dict[str, Any]] = None
        self._exit_after_campaign = False

        # row key -> row data
        self.rows: Dict[str, Dict[str, Any]] = {}
        self.row_order: List[str] = []
        self.selected_row_key: Optional[str] = None

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Static(self._header_text(), id="run_header")
        table = DataTable(id="status_table")
        yield table
        yield Static("", id="progress_label")
        yield ProgressBar(id="job_progress", show_eta=False)
        yield RichLog(id="log_view", markup=True, highlight=False, wrap=True)
        with Horizontal(id="controls"):
            yield Button("Pause", id="pause_btn")
            yield Button("Retry Step", id="retry_btn", disabled=True)
            yield Button("Continue", id="continue_btn", disabled=True)
            yield Button("Skip Step", id="skip_btn", disabled=True)
            yield Button("Stop Campaign", id="cancel_btn", variant="error")
            yield Button("Back", id="back_btn", disabled=True)
        yield Footer()

    def _header_text(self) -> str:
        mode_label = "Resuming" if self.mode == "resume" else "Starting"
        campaign = f"  campaign=[cyan]{self.campaign_id}[/cyan]" if self.campaign_id else ""
        name = f"[b]{rich_escape(self.campaign_name)}[/b]  " if self.campaign_name else ""
        return (
            f"[b]{mode_label} campaign[/b]  {name}{campaign}  "
            f"config=[cyan]{self.config_file}[/cyan]  "
            f"clusters=[cyan]{', '.join(self.clusters)}[/cyan]  "
            f"dry_run=[cyan]{self.dry_run}[/cyan]"
        )

    def on_mount(self) -> None:
        table = self.query_one("#status_table", DataTable)
        table.add_columns("App", "Step", "Cluster", "Status", "Duration", "Jobs")
        table.cursor_type = "row"

        progress = self.query_one("#job_progress", ProgressBar)
        progress.update(total=100, progress=0)

        self.set_interval(0.15, self._drain_queue)
        self._run_campaign_worker()

    # ------------------------------------------------------------------
    # Background worker
    # ------------------------------------------------------------------

    @work(thread=True, exclusive=True, group="campaign")
    def _run_campaign_worker(self) -> None:
        try:
            if self.mode == "resume" and self.log_file is not None:
                self.success = run_campaign_with_existing_log(
                    config_file=self.config_file,
                    log_file=self.log_file,
                    clusters=self.clusters,
                    verbose=self.verbose,
                    dry_run=self.dry_run,
                    resume_mode=self.resume_mode,
                    force_apps=self.force_apps,
                    event_queue=self.event_queue,
                    control=self.control,
                    console_output=False,
                    applications=self.applications,
                    steps_by_application=self.steps_by_application,
                    clusters_by_step=self.clusters_by_step,
                )
            else:
                self.success = run_campaign(
                    config_file=self.config_file,
                    results_dir=self.results_dir,
                    clusters=self.clusters,
                    verbose=self.verbose,
                    dry_run=self.dry_run,
                    resume=False,
                    event_queue=self.event_queue,
                    control=self.control,
                    console_output=False,
                    applications=self.applications,
                    steps_by_application=self.steps_by_application,
                    clusters_by_step=self.clusters_by_step,
                    campaign_selection=self.campaign_selection,
                )
        except CampaignRunnerError as e:
            self.event_queue.put(
                CampaignEvent(
                    type=EventType.LOG,
                    timestamp="",
                    data={"level": "error", "message": f"Fatal error: {e}"},
                )
            )
            self.success = False

    def on_worker_state_changed(self, event: Worker.StateChanged) -> None:
        if event.state in (WorkerState.SUCCESS, WorkerState.ERROR, WorkerState.CANCELLED):
            self.finished = True
            self.query_one("#pause_btn", Button).disabled = True
            self.query_one("#retry_btn", Button).disabled = True
            self.query_one("#continue_btn", Button).disabled = True
            self.query_one("#skip_btn", Button).disabled = True
            self.query_one("#cancel_btn", Button).disabled = True
            self.query_one("#back_btn", Button).disabled = False
            log = self.query_one("#log_view", RichLog)
            if self.success:
                log.write("[bold green]Campaign finished successfully.[/bold green]")
            else:
                log.write("[bold red]Campaign finished with failures (or was cancelled).[/bold red]")
            if self.mode == "resume" and self.log_file is not None:
                log.write(f"Campaign journal updated in place: {self.log_file}")
            if self._exit_after_campaign:
                self.app.exit()

    # ------------------------------------------------------------------
    # Event draining / rendering
    # ------------------------------------------------------------------

    def _drain_queue(self) -> None:
        log = self.query_one("#log_view", RichLog)
        table_dirty = False

        while True:
            try:
                event = self.event_queue.get_nowait()
            except queue.Empty:
                break

            if event.type == EventType.LOG:
                level = event.data.get("level", "info")
                message = event.data.get("message", "")
                color = {
                    "debug": "dim",
                    "info": "blue",
                    "warning": "yellow",
                    "error": "red",
                    "critical": "bold red",
                    "job": "bold cyan",
                }.get(level, "white")
                if message.lstrip().startswith(("Step:", "Step complete:", "Execution summary")):
                    log.write(Text(" "))
                if level == "job" and event.data.get("job_title"):
                    job_line = Text("Job: ")
                    job_line.append(event.data["job_title"], style="bold cyan")
                    details = event.data.get("job_details")
                    if details:
                        job_line.append(f"  {details}", style="dim")
                    log.write(job_line)
                else:
                    log.write(f"[{color}]{message}[/{color}]")

            elif event.type == EventType.CAMPAIGN_START:
                campaign_name = event.data.get("campaign_name")
                title = f": {rich_escape(campaign_name)}" if campaign_name else ""
                log.write(f"[bold]Campaign started{title}.[/bold]")

            elif event.type == EventType.CLUSTER_START:
                log.write(Text(" "))
                log.write(f"[bold magenta]== Cluster: {event.data['cluster']} ==[/bold magenta]")

            elif event.type == EventType.APP_START:
                log.write(Text(" "))
                log.write(f"[bold blue]-- App: {event.data['app']} --[/bold blue]")

            elif event.type == EventType.APP_SKIPPED:
                log.write(f"[yellow]App skipped: {event.data['app']} ({event.data.get('reason')})[/yellow]")

            elif event.type == EventType.STEP_START:
                key = self._row_key(event.data)
                self._ensure_row(key, event.data, status="running")
                self.rows[key]["duration"] = ""
                table_dirty = True

            elif event.type == EventType.STEP_SKIPPED:
                key = self._row_key(event.data)
                self._ensure_row(key, event.data, status="skipped")
                table_dirty = True

            elif event.type == EventType.STEP_PROGRESS:
                completed = event.data.get("completed", 0)
                total = event.data.get("total", 0) or 1
                label = self.query_one("#progress_label", Static)
                label.update(
                    f"Polling jobs for {event.data.get('app')}/{event.data.get('step')}: "
                    f"{completed}/{total}"
                )
                progress = self.query_one("#job_progress", ProgressBar)
                progress.update(total=total, progress=completed)

                key = self._row_key(event.data)
                if key in self.rows:
                    self.rows[key]["jobs"] = f"{completed}/{total}"
                    self.rows[key]["job_records"] = event.data.get("jobs", [])
                    duration = event.data.get("duration")
                    if duration is not None:
                        self.rows[key]["duration"] = f"{duration:.1f}s"
                    table_dirty = True

            elif event.type == EventType.STEP_END:
                key = self._row_key(event.data)
                self._ensure_row(key, event.data, status=event.data.get("status", "completed"))
                duration = event.data.get("duration")
                if duration is not None and key in self.rows:
                    self.rows[key]["duration"] = f"{duration:.1f}s"
                if key in self.rows:
                    self.rows[key]["job_records"] = event.data.get("jobs", [])
                    if self.dry_run:
                        self.rows[key]["jobs"] = f"{len(self.rows[key]['job_records'])} planned"
                    elif "jobs_count" in event.data:
                        self.rows[key]["jobs"] = (
                            f"{event.data.get('jobs_successful', 0)}/"
                            f"{event.data.get('jobs_count', 0)}"
                        )
                table_dirty = True

            elif event.type == EventType.STEP_NEEDS_ATTENTION:
                key = self._row_key(event.data)
                self._ensure_row(key, event.data, status="needs attention")
                table_dirty = True
                self.awaiting_resolution = True
                self.pending_step_resolution = dict(event.data)
                self.query_one("#pause_btn", Button).disabled = True
                self.query_one("#retry_btn", Button).disabled = False
                self.query_one("#continue_btn", Button).disabled = False
                self.query_one("#skip_btn", Button).disabled = False
                log.write(
                    f"[bold yellow]Needs attention: {event.data.get('app')}/"
                    f"{event.data.get('step')} on {event.data.get('cluster')}[/bold yellow]\n"
                    f"{event.data.get('error', '')}"
                )
                for job in event.data.get("jobs", []):
                    log.write(
                        f"  Job {job.get('job_id')} [{job.get('status')}]: "
                        f"{job.get('tag')} ({job.get('exp_dir')})"
                    )

            elif event.type == EventType.PAUSED:
                self.query_one("#pause_btn", Button).label = "Resume"
                log.write("[yellow]Paused.[/yellow]")

            elif event.type == EventType.RESUMED:
                self.query_one("#pause_btn", Button).label = "Pause"
                log.write("[green]Resumed.[/green]")

            elif event.type == EventType.CAMPAIGN_CANCELLED:
                log.write(f"[bold magenta]Cancelled: {event.data.get('message')}[/bold magenta]")

            elif event.type == EventType.CAMPAIGN_END:
                # A final snapshot makes the table authoritative even if a
                # progress event was delayed or lost during parallel runs.
                for step in event.data.get("steps", []):
                    key = self._row_key(step)
                    self._ensure_row(key, step, status=step.get("status", "unknown"))
                    if key in self.rows:
                        duration = step.get("duration")
                        if duration is not None:
                            self.rows[key]["duration"] = f"{duration:.1f}s"
                        self.rows[key]["job_records"] = step.get("jobs", [])
                        self.rows[key]["jobs"] = (
                            f"{len(self.rows[key]['job_records'])} planned"
                            if self.dry_run
                            else f"{step.get('jobs_successful', 0)}/{step.get('jobs_count', 0)}"
                        )
                    table_dirty = True

        if table_dirty:
            self._refresh_table()

    def _row_key(self, data: Dict[str, Any]) -> str:
        return f"{data.get('app')}|{data.get('step')}|{data.get('cluster')}"

    def _ensure_row(self, key: str, data: Dict[str, Any], status: str) -> None:
        if key not in self.rows:
            self.rows[key] = {
                "app": data.get("app", ""),
                "step": data.get("step", ""),
                "cluster": data.get("cluster", ""),
                "status": status,
                "duration": "",
                "jobs": "",
                "job_records": data.get("jobs", []),
            }
            self.row_order.append(key)
        else:
            self.rows[key]["status"] = status

    def _refresh_table(self) -> None:
        table = self.query_one("#status_table", DataTable)
        selected_key = None
        selected_column = 0
        selected_row = 0
        try:
            if table.row_count:
                coordinate = table.cursor_coordinate
                selected_row, selected_column = coordinate
                selected_key = table.coordinate_to_cell_key(coordinate).row_key.value
        except Exception:
            pass
        table.clear()
        for key in self.row_order:
            r = self.rows[key]
            table.add_row(
                r["app"],
                r["step"],
                r["cluster"],
                status_text(r["status"]),
                r["duration"],
                r["jobs"],
                key=key,
            )
        if table.row_count:
            restored_row = None
            if selected_key in self.rows:
                restored_row = self.row_order.index(selected_key)
                self.selected_row_key = selected_key
            elif selected_row < table.row_count:
                restored_row = selected_row
            if restored_row is not None:
                table.move_cursor(
                    row=restored_row,
                    column=min(selected_column, 5),
                    animate=False,
                )

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        self.selected_row_key = str(event.row_key.value)

    def action_view_step_jobs(self) -> None:
        key = self.selected_row_key
        try:
            table = self.query_one("#status_table", DataTable)
            row_key = table.coordinate_to_cell_key(table.cursor_coordinate).row_key.value
            if row_key:
                key = str(row_key)
        except Exception:
            pass
        if not key or not self.rows.get(key, {}).get("job_records"):
            self.notify("Select a step with recorded jobs to inspect.", severity="information")
            return
        self._open_step_jobs(key)

    def _open_step_jobs(self, key: str) -> None:
        row = self.rows.get(key)
        if not row or not row.get("job_records"):
            return
        records = row["job_records"]
        recorded_project = next(
            (record.get("project_root") for record in records if record.get("project_root")),
            None,
        )
        project = Path(recorded_project) if recorded_project else self.application_projects.get(row["app"])
        if project is None:
            self.notify(f"Could not locate the application project for {row['app']}.", severity="error")
            return
        self.app.push_screen(
            CampaignJobsScreen(
                project_root=project,
                cluster_name=row["cluster"],
                exp_dirs=[record["exp_dir"] for record in records if record.get("exp_dir")],
                read_only=True,
            )
        )

    # ------------------------------------------------------------------
    # Controls
    # ------------------------------------------------------------------

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "pause_btn":
            if self.control.is_paused:
                self.control.resume()
                event.button.label = "Pause"
            else:
                self.control.request_pause()
                event.button.label = "Resume"

        elif event.button.id == "cancel_btn":
            self.request_campaign_cancel()

        elif event.button.id in ("retry_btn", "continue_btn", "skip_btn"):
            resolution = {
                "retry_btn": "retry",
                "continue_btn": "continue",
                "skip_btn": "skip",
            }[event.button.id]
            if resolution == "retry" and self.pending_step_resolution:
                jobs = self._pending_retry_jobs(self.pending_step_resolution)
                if jobs:
                    detail = self.pending_step_resolution
                    title = (
                        f"Retry {detail.get('app')}/{detail.get('step')} "
                        f"on {detail.get('cluster')}?"
                    )
                    self.app.push_screen(
                        ConfirmCampaignRerunScreen(
                            title,
                            "Retry forces a fresh launch of this step's job definitions. "
                            "Existing job records and directories are retained; matching job "
                            "definitions may be submitted again.",
                            jobs,
                        ),
                        self._on_retry_confirmation,
                    )
                    return
            self._resolve_step(resolution)

        elif event.button.id == "back_btn":
            if self.finished:
                self.app.pop_screen()

    def action_try_back(self) -> None:
        if self.finished:
            self.app.pop_screen()

    def _on_retry_confirmation(self, confirmed: bool) -> None:
        if confirmed:
            self._resolve_step("retry")
        else:
            self.query_one("#log_view", RichLog).write(
                "[yellow]Retry cancelled; choose another resolution or retry later.[/yellow]"
            )

    @staticmethod
    def _pending_retry_jobs(detail: Dict[str, Any]) -> List[Dict[str, Any]]:
        jobs = _recorded_step_jobs(detail)
        unique_jobs = []
        seen = set()
        for job in jobs:
            status = _recorded_job_status(job)
            if status == "COMPLETED":
                continue
            job["status"] = status
            key = (
                str(job.get("cluster", detail.get("cluster", ""))),
                str(job.get("job_id") or ""),
                str(job.get("exp_dir") or ""),
                str(job.get("config") or ""),
                str(job.get("tag") or ""),
            )
            if key in seen:
                continue
            seen.add(key)
            unique_jobs.append(job)
        return unique_jobs

    def _resolve_step(self, resolution: str) -> None:
        self.control.request_resolution(resolution)
        self.awaiting_resolution = False
        self.pending_step_resolution = None
        self.query_one("#pause_btn", Button).disabled = False
        for button_id in ("#retry_btn", "#continue_btn", "#skip_btn"):
            self.query_one(button_id, Button).disabled = True
        self.query_one("#log_view", RichLog).write(
            f"[cyan]{resolution.capitalize()} selected; campaign resuming…[/cyan]"
        )

    def request_campaign_cancel(self, exit_after: bool = False) -> None:
        if self.finished:
            if exit_after:
                self.app.exit()
            return
        self._exit_after_campaign = self._exit_after_campaign or exit_after
        self.control.request_cancel()
        self.query_one("#cancel_btn", Button).disabled = True
        self.query_one("#pause_btn", Button).disabled = True
        self.query_one("#retry_btn", Button).disabled = True
        self.query_one("#continue_btn", Button).disabled = True
        self.query_one("#skip_btn", Button).disabled = True
        message = (
            "Cancellation requested; waiting for submitted jobs to stop before exiting…"
            if exit_after else "Cancellation requested…"
        )
        self.query_one("#log_view", RichLog).write(f"[bold magenta]{message}[/bold magenta]")


# ============================================================================
# History Screen
# ============================================================================


class HistoryScreen(Screen):
    """Browse previous campaign runs and resume / re-run failed apps."""

    CSS = """
    #history_list { height: 1fr; border: solid $accent; }
    #detail_panel { height: 10; border: solid $accent; padding: 1; }
    #history_controls { height: 3; align: center middle; }
    """

    def __init__(
        self,
        *,
        config_file: Path,
        results_dir: Path,
        clusters: List[str],
        dry_run: bool,
        verbose: bool,
    ) -> None:
        super().__init__()
        self.config_file = Path(config_file)
        self.results_dir = Path(results_dir)
        self.clusters = clusters
        self.dry_run = dry_run
        self.verbose = verbose

        self.summaries: List[Dict[str, Any]] = []
        self.selected: Optional[Dict[str, Any]] = None

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static(f"Previous runs in [cyan]{self.results_dir}[/cyan]")
        yield ListView(id="history_list")
        yield Static("Select a run to see details.", id="detail_panel")
        with Horizontal(id="history_controls"):
            yield Button("Open selected campaign", id="open_campaign_btn", disabled=True)
            yield Button("Back", id="back_btn")
        yield Footer()

    def on_mount(self) -> None:
        list_view = self.query_one("#history_list", ListView)
        logs = list_campaign_logs(self.results_dir)
        if not logs:
            list_view.append(ListItem(Label("No previous campaign logs found.")))
            return

        for log_file in logs:
            summary = load_campaign_log_summary(log_file)
            if summary is None:
                continue
            self.summaries.append(summary)
            status = "OK" if summary["failed_steps"] == 0 else f"{summary['failed_steps']} failed"
            label = (
                f"{summary.get('campaign_name') + '  |  ' if summary.get('campaign_name') else ''}"
                f"{summary['campaign_id']}  "
                f"[{summary['completed_steps']}/{summary['total_steps']} steps]  "
                f"({status})  clusters={','.join(summary['clusters_processed'])}"
            )
            list_view.append(ListItem(Label(label)))

    def on_list_view_selected(self, event: ListView.Selected) -> None:
        index = event.list_view.index
        if index is None or index >= len(self.summaries):
            return
        self.selected = self.summaries[index]

        detail = self.query_one("#detail_panel", Static)
        s = self.selected
        failed_apps = ", ".join(s["failed_apps"]) if s["failed_apps"] else "(none)"
        project_lines = "\n".join(
            f"  {name}: {path}"
            for name, path in s.get("application_projects", {}).items()
        ) or "  (none recorded)"
        detail.update(
            f"Campaign: {campaign_display_label(s.get('campaign_name'), s['campaign_id'])}\n"
            f"Started: {s['campaign_start']}   Ended: {s['campaign_end']}\n"
            f"Steps: {s['completed_steps']} completed / {s['failed_steps']} failed / {s['total_steps']} total\n"
            f"Failed apps: {failed_apps}\n"
            f"Application projects:\n{project_lines}\n"
            f"Log file: {s['log_file']}"
        )

        self.query_one("#open_campaign_btn", Button).disabled = False

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "back_btn":
            self.app.pop_screen()
            return

        if self.selected is None:
            return

        if event.button.id == "open_campaign_btn":
            self.app.push_screen(
                CampaignStateScreen(
                    Path(self.selected["log_file"]),
                    config_file=self.config_file,
                    results_dir=self.results_dir,
                    clusters=self.clusters or self.selected["clusters_processed"],
                    dry_run=self.dry_run,
                    verbose=self.verbose,
                )
            )

class CampaignStateScreen(Screen):
    """Campaign detail page with inspection and resume/rerun actions."""

    CSS = """
    #state_header { height: auto; padding: 1; }
    #state_table { height: 1fr; }
    #state_detail { height: 5; border: solid $accent; padding: 1; }
    #state_controls { height: 6; }
    .state_control_row { height: 3; align: center middle; }
    """

    def __init__(
        self,
        log_file: Path,
        *,
        config_file: Optional[Path] = None,
        results_dir: Optional[Path] = None,
        clusters: Optional[List[str]] = None,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> None:
        super().__init__()
        self.log_file = Path(log_file)
        config_path = Path(config_file).expanduser() if config_file else None
        self.config_file = config_path if config_path and config_path.is_file() else None
        self.results_dir = Path(results_dir) if results_dir else self.log_file.parent
        self.clusters = clusters or []
        self.dry_run = dry_run
        self.verbose = verbose
        try:
            self.state = json.loads(self.log_file.read_text())
            self.error = None
        except (OSError, json.JSONDecodeError) as exc:
            self.state = {}
            self.error = str(exc)
        try:
            self.campaign_config = (
                load_campaign_config(self.config_file) if self.config_file else None
            )
        except CampaignRunnerError:
            self.campaign_config = None
        self._sequential_steps: Dict[Tuple[str, str], bool] = {}

    def compose(self) -> ComposeResult:
        yield Header()
        if self.error:
            yield Static(f"Could not read campaign journal: {self.error}", id="state_header")
        else:
            yield Static(
                f"Campaign [cyan]{campaign_display_label(self.state.get('campaign_name'), self.state.get('campaign_id', 'unknown'))}[/cyan]  "
                f"Started: {self.state.get('campaign_start', 'unknown')}  "
                f"Ended: {self.state.get('campaign_end') or 'incomplete'}\n"
                f"Clusters: {', '.join(self.state.get('clusters_processed', []))}\n"
                f"Journal: {self.log_file}", id="state_header"
            )
            table = DataTable(id="state_table")
            yield table
            yield Static("Select a step to inspect its error and job counts.", id="state_detail")
        with Vertical(id="state_controls"):
            with Horizontal(classes="state_control_row"):
                yield Button("Resume / apply edits", id="state_resume_btn", disabled=self.error is not None or self.config_file is None)
                yield Button("Re-run failed apps", id="state_rerun_btn", disabled=self.error is not None or self.config_file is None or not self._failed_apps())
                yield Button("Edit selection", id="state_edit_selection_btn", disabled=self.error is not None or self.config_file is None)
            with Horizontal(classes="state_control_row"):
                yield Button("View Step Jobs", id="state_view_jobs_btn", disabled=True)
                yield Button("Back", id="state_back_btn")
        yield Footer()

    def _failed_apps(self) -> List[str]:
        failed = []
        application_projects = self.state.get("application_projects", {})
        for app_name, app_data in self.state.get("execution", {}).items():
            if any(
                record.get("status") in (StepStatus.FAILED.value, StepStatus.CANCELLED.value)
                or any(
                    _recorded_job_status(job, application_projects.get(app_name)) != "COMPLETED"
                    for job in _recorded_step_jobs(record)
                )
                for step in app_data.get("steps", {}).values()
                for record in step.values()
            ):
                failed.append(app_name)
        return failed

    def _step_is_sequential(self, app_name: str, step_name: str) -> bool:
        key = (app_name, step_name)
        if key in self._sequential_steps:
            return self._sequential_steps[key]
        sequential = False
        if self.campaign_config is not None:
            app = next((app for app in self.campaign_config.apps if app.name == app_name), None)
            step = next((step for step in app.steps if step.name == step_name), None) if app else None
            if app is not None and step is not None and step.jobs:
                try:
                    jobs_yaml = yaml.safe_load((app.dir / step.jobs).read_text()) or {}
                    sequential = bool(jobs_yaml.get("sequential", False))
                except (OSError, yaml.YAMLError, AttributeError):
                    pass
        self._sequential_steps[key] = sequential
        return sequential

    @staticmethod
    def _job_runtime_from_record(
        job: Dict[str, Any], application_project: Optional[str] = None
    ) -> Optional[float]:
        try:
            runtime = job.get("runtime_seconds")
            if runtime is not None:
                return max(0.0, float(runtime))
        except (TypeError, ValueError):
            pass

        resolved = resolve_recorded_job(job, application_project)
        if resolved is None:
            return None
        return resolved.get_run_time()

    def _display_step_duration(self, app_name: str, step_name: str, record: Dict[str, Any]) -> float:
        fallback = float(record.get("duration_seconds", 0) or 0)
        jobs = record.get("jobs") or []
        if record.get("status") != StepStatus.COMPLETED.value or not jobs:
            return fallback
        if any(str(job.get("status", "")).upper() != "COMPLETED" for job in jobs):
            return fallback
        application_project = self.state.get("application_projects", {}).get(app_name)
        runtimes = [
            self._job_runtime_from_record(job, application_project) for job in jobs
        ]
        if not runtimes or any(runtime is None for runtime in runtimes):
            return fallback
        values = [float(runtime) for runtime in runtimes if runtime is not None]
        return sum(values) if self._step_is_sequential(app_name, step_name) else max(values)

    async def on_screen_resume(self) -> None:
        """Refresh state after a resume/rerun returns to this campaign page."""
        try:
            self.state = json.loads(self.log_file.read_text())
        except (OSError, json.JSONDecodeError):
            return
        self.query_one("#state_header", Static).update(
            f"Campaign [cyan]{campaign_display_label(self.state.get('campaign_name'), self.state.get('campaign_id', 'unknown'))}[/cyan]  "
            f"Started: {self.state.get('campaign_start', 'unknown')}  "
            f"Ended: {self.state.get('campaign_end') or 'incomplete'}\n"
            f"Clusters: {', '.join(self.state.get('clusters_processed', []))}\n"
            f"Journal: {self.log_file}"
        )
        table = self.query_one("#state_table", DataTable)
        table.clear()
        self._row_details = []
        application_projects = self.state.get("application_projects", {})
        for app_name, app_data in self.state.get("execution", {}).items():
            for step_name, cluster_logs in app_data.get("steps", {}).items():
                for cluster, record in cluster_logs.items():
                    table.add_row(
                        app_name, step_name, cluster,
                        _display_recorded_step_status(record, application_projects.get(app_name)),
                        f"{self._display_step_duration(app_name, step_name, record):.1f}s",
                        "/".join(map(str, _recorded_job_counts(
                            record, application_projects.get(app_name)
                        ))),
                        record.get("error_message") or "",
                    )
                    record["_campaign_app"] = app_name
                    record["_campaign_cluster"] = cluster
                    self._row_details.append(record)
        if not self._row_details:
            table.add_row("—", "—", "—", "No steps recorded", "—", "—", "")
        self.query_one("#state_resume_btn", Button).disabled = self.config_file is None
        self.query_one("#state_rerun_btn", Button).disabled = (
            self.config_file is None or not bool(self._failed_apps())
        )

    def _start_resume(self, rerun_failed: bool = False) -> None:
        if self.config_file is None:
            self.notify("A campaign config file is required to resume.", severity="error")
            return
        target_clusters = self.clusters or self.state.get("clusters_processed", [])
        saved_selection = self.state.get("selection") or None
        rerun_plan = (
            self._failed_rerun_plan(target_clusters, saved_selection)
            if rerun_failed else saved_selection
        )
        failed_apps = list(rerun_plan["steps_by_application"]) if rerun_plan else []
        jobs = (
            rerun_plan["jobs"] if rerun_failed and rerun_plan else
            self._jobs_for_resume(
                applications=None,
                clusters=target_clusters,
                incomplete_only=True,
                selection=saved_selection,
            )
        )
        if jobs:
            if rerun_failed:
                title = f"Re-run {len(jobs)} incomplete job(s)?"
                description = (
                    "Only the listed non-COMPLETED jobs will be submitted again. Completed jobs "
                    "are kept and will be skipped."
                )
            else:
                title = f"Resume with {len(jobs)} non-COMPLETED job(s)?"
                description = (
                    "Only jobs whose current status is not COMPLETED will be submitted again. "
                    "Completed jobs will be reused."
                )
            self.app.push_screen(
                ConfirmCampaignRerunScreen(
                    title,
                    description,
                    jobs,
                    steps=rerun_plan["steps"] if rerun_plan else None,
                ),
                lambda confirmed: self._on_history_rerun_confirmation(
                    confirmed, rerun_failed, rerun_plan
                ),
            )
            return
        if rerun_failed and rerun_plan and not rerun_plan["steps"]:
            self.notify(
                "No incomplete campaign work exists on the selected clusters.",
                severity="information",
            )
            return
        if rerun_failed and rerun_plan and rerun_plan["steps"]:
            self.app.push_screen(
                ConfirmCampaignRerunScreen(
                    f"Retry {len(rerun_plan['steps'])} incomplete step(s)?",
                    "No non-COMPLETED jobs were recorded for these steps. The step scripts "
                    "will be retried; completed jobs will be skipped.",
                    [],
                    steps=rerun_plan["steps"],
                ),
                lambda confirmed: self._on_history_rerun_confirmation(
                    confirmed, True, rerun_plan
                ),
            )
            return
        self._launch_resume(rerun_failed, rerun_plan)

    def _failed_rerun_plan(
        self,
        clusters: List[str],
        selection: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        cluster_filter = set(clusters)
        steps_by_application: Dict[str, List[str]] = {}
        affected_clusters = set()
        incomplete_jobs: List[Dict[str, Any]] = []
        affected_steps: List[Dict[str, Any]] = []
        clusters_by_app_step: Dict[str, Dict[str, List[str]]] = {}
        application_projects = self.state.get("application_projects", {})
        for app_name, app_data in self.state.get("execution", {}).items():
            selected_apps = selection.get("applications") if selection else None
            if selected_apps and app_name not in selected_apps:
                continue
            selected_app_steps = (selection or {}).get("steps_by_application", {}).get(app_name)
            for step_name, cluster_logs in app_data.get("steps", {}).items():
                if selected_app_steps is not None and step_name not in selected_app_steps:
                    continue
                for cluster, step_record in cluster_logs.items():
                    if cluster_filter and cluster not in cluster_filter:
                        continue
                    selected_clusters = (
                        (selection or {}).get("clusters_by_step", {})
                        .get(app_name, {})
                        .get(step_name)
                    )
                    if selected_clusters is not None and cluster not in selected_clusters:
                        continue
                    jobs = _recorded_step_jobs(step_record)
                    unfinished = [
                        job for job in jobs
                        if _recorded_job_status(
                            job, application_projects.get(app_name)
                        ) != "COMPLETED"
                    ]
                    for job in unfinished:
                        job["status"] = _recorded_job_status(job)
                    step_status = str(step_record.get("status", "")).upper()
                    if not unfinished and step_status not in {
                        StepStatus.FAILED.value.upper(),
                        StepStatus.CANCELLED.value.upper(),
                    }:
                        continue
                    steps_by_application.setdefault(app_name, [])
                    if step_name not in steps_by_application[app_name]:
                        steps_by_application[app_name].append(step_name)
                    affected_clusters.add(cluster)
                    app_cluster_steps = clusters_by_app_step.setdefault(app_name, {})
                    app_step_clusters = app_cluster_steps.setdefault(step_name, [])
                    if cluster not in app_step_clusters:
                        app_step_clusters.append(cluster)
                    affected_steps.append({
                        "app": app_name,
                        "step": step_name,
                        "cluster": cluster,
                        "status": step_record.get("status", "unknown"),
                    })
                    for job in unfinished:
                        incomplete_jobs.append({
                            **job,
                            "app": app_name,
                            "step": step_name,
                            "cluster": job.get("cluster") or cluster,
                        })
        return {
            "applications": list(steps_by_application),
            "steps_by_application": steps_by_application,
            "clusters_by_step": clusters_by_app_step,
            "clusters": sorted(affected_clusters),
            "jobs": incomplete_jobs,
            "steps": affected_steps,
        }

    def _jobs_for_resume(
        self,
        *,
        applications: Optional[List[str]],
        clusters: List[str],
        incomplete_only: bool,
        selection: Optional[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        app_filter = set(applications) if applications is not None else None
        cluster_filter = set(clusters)
        application_projects = self.state.get("application_projects", {})
        jobs = []
        for app_name, app_data in self.state.get("execution", {}).items():
            if app_filter is not None and app_name not in app_filter:
                continue
            selected_apps = (selection or {}).get("applications")
            if selected_apps and app_name not in selected_apps:
                continue
            selected_app_steps = (selection or {}).get("steps_by_application", {}).get(app_name)
            for step_name, cluster_logs in app_data.get("steps", {}).items():
                if selected_app_steps is not None and step_name not in selected_app_steps:
                    continue
                for cluster, step_record in cluster_logs.items():
                    if cluster_filter and cluster not in cluster_filter:
                        continue
                    selected_clusters = (
                        (selection or {}).get("clusters_by_step", {})
                        .get(app_name, {})
                        .get(step_name)
                    )
                    if selected_clusters is not None and cluster not in selected_clusters:
                        continue
                    for job in _recorded_step_jobs(step_record):
                        status = _recorded_job_status(
                            job, application_projects.get(app_name)
                        )
                        if incomplete_only and status == "COMPLETED":
                            continue
                        jobs.append({
                            **job,
                            "status": status,
                            "app": app_name,
                            "step": step_name,
                            "cluster": job.get("cluster") or cluster,
                        })
        return jobs

    def _on_history_rerun_confirmation(
        self, confirmed: bool, rerun_failed: bool, rerun_plan: Optional[Dict[str, Any]]
    ) -> None:
        if confirmed:
            self._launch_resume(rerun_failed, rerun_plan)
        else:
            self.notify("Campaign rerun cancelled.", severity="information")

    def _launch_resume(
        self, rerun_failed: bool, rerun_plan: Optional[Dict[str, Any]] = None
    ) -> None:
        applications = rerun_plan["applications"] if rerun_failed and rerun_plan else None
        if rerun_failed and rerun_plan:
            applications = rerun_plan["applications"]
        else:
            applications = rerun_plan.get("applications") if rerun_plan else None
        steps_by_application = (
            rerun_plan["steps_by_application"] if rerun_failed and rerun_plan else None
        )
        if not rerun_failed and rerun_plan:
            steps_by_application = rerun_plan.get("steps_by_application")
        clusters_by_step = rerun_plan.get("clusters_by_step") if rerun_plan else None
        selected_clusters = (
            rerun_plan["clusters"] if rerun_failed and rerun_plan
            else self._selection_clusters(rerun_plan)
        )
        if selected_clusters and self.clusters:
            clusters = [cluster for cluster in selected_clusters if cluster in self.clusters]
        else:
            clusters = selected_clusters or self.clusters or self.state.get("clusters_processed", [])
        self.app.push_screen(
            RunScreen(
                mode="resume",
                config_file=self.config_file,
                results_dir=self.results_dir,
                clusters=clusters,
                dry_run=self.dry_run,
                verbose=self.verbose,
                resume_mode="auto",
                force_apps=[],
                log_file=self.log_file,
                applications=applications,
                steps_by_application=steps_by_application,
                clusters_by_step=clusters_by_step,
            )
        )

    @staticmethod
    def _selection_clusters(selection: Optional[Dict[str, Any]]) -> List[str]:
        if not selection:
            return []
        return sorted({
            cluster
            for app_steps in (selection.get("clusters_by_step") or {}).values()
            for step_clusters in app_steps.values()
            for cluster in step_clusters
        })

    def on_mount(self) -> None:
        if self.error:
            return
        table = self.query_one("#state_table", DataTable)
        table.add_columns("App", "Step", "Cluster", "Status", "Duration", "Jobs", "Error")
        table.cursor_type = "row"
        self._row_details = []
        application_projects = self.state.get("application_projects", {})
        for app_name, app_data in self.state.get("execution", {}).items():
            for step_name, cluster_logs in app_data.get("steps", {}).items():
                for cluster, record in cluster_logs.items():
                    table.add_row(
                        app_name, step_name, cluster,
                        _display_recorded_step_status(record, application_projects.get(app_name)),
                        f"{self._display_step_duration(app_name, step_name, record):.1f}s",
                        "/".join(map(str, _recorded_job_counts(
                            record, application_projects.get(app_name)
                        ))),
                        record.get("error_message") or "",
                    )
                    self._row_details.append(record)
                    self._row_details[-1]["_campaign_app"] = app_name
                    self._row_details[-1]["_campaign_cluster"] = cluster
        if not self._row_details:
            table.add_row("—", "—", "—", "No steps recorded", "—", "—", "")

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        index = event.cursor_row
        if index >= len(getattr(self, "_row_details", [])):
            return
        record = self._row_details[index]
        self.query_one("#state_view_jobs_btn", Button).disabled = not bool(record.get("jobs"))
        errors = "; ".join(record.get("job_errors", [])) or "None"
        self.query_one("#state_detail", Static).update(
            f"Step error: {record.get('error_message') or 'None'}\n"
            f"Jobs: {record.get('jobs_successful', 0)} successful, "
            f"{record.get('jobs_failed', 0)} failed, {record.get('jobs_count', 0)} total\n"
            f"Job errors: {errors}"
        )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "state_back_btn":
            self.app.pop_screen()
        elif event.button.id == "state_resume_btn":
            self._start_resume()
        elif event.button.id == "state_rerun_btn":
            self._start_resume(rerun_failed=True)
        elif event.button.id == "state_edit_selection_btn":
            if self.config_file is None:
                return
            self.app.push_screen(
                CampaignSelectionScreen(
                    config_file=self.config_file,
                    results_dir=self.results_dir,
                    clusters=self.state.get("clusters_processed") or self.clusters,
                    dry_run=self.dry_run,
                    verbose=self.verbose,
                    selection=self.state.get("selection"),
                    edit_log=self.log_file,
                )
            )
        elif event.button.id == "state_view_jobs_btn":
            index = self.query_one("#state_table", DataTable).cursor_row
            if index >= len(getattr(self, "_row_details", [])):
                return
            record = self._row_details[index]
            app_name = record.get("_campaign_app")
            project = self.state.get("application_projects", {}).get(app_name)
            job_records = record.get("jobs", [])
            if project and job_records:
                self.app.push_screen(
                    CampaignJobsScreen(
                        project_root=Path(project),
                        cluster_name=record.get("_campaign_cluster"),
                        exp_dirs=[job["exp_dir"] for job in job_records if job.get("exp_dir")],
                        read_only=True,
                    )
                )


# ============================================================================
# Menu Screen
# ============================================================================


class MenuScreen(Screen):
    """Campaign setup, history, state, and resume controls in one view."""

    CSS = """
    #campaign_layout { height: 1fr; }
    #form { padding: 0 1; width: 1fr; height: 1fr; border: solid $accent; }
    #form Input { margin-bottom: 1; }
    .field_label { margin-top: 0; height: 1; }
    #flag_row { height: 4; }
    .flag_option { width: 1fr; height: 4; }
    .flag_option Switch { margin-left: 1; }
    #campaign_browser { width: 2fr; padding: 0 1; border: solid $accent; }
    #campaign_history_list { height: 5; }
    #campaign_detail { height: 3; }
    #campaign_steps { height: 1fr; }
    #status_msg { color: $error; height: 2; }
    #menu_controls { height: 6; }
    .menu_button_row { height: 3; align: center middle; }
    """

    def __init__(
        self,
        *,
        config_file: str = "campaign.yaml",
        results_dir: str = "./campaign_results",
        clusters: str = "",
        dry_run: bool = False,
        verbose: bool = False,
    ) -> None:
        super().__init__()
        self._config_file = config_file
        self._results_dir = results_dir
        self._clusters = clusters
        self._dry_run = dry_run
        self._verbose = verbose
        self.summaries: List[Dict[str, Any]] = []
        self.selected_summary: Optional[Dict[str, Any]] = None
        self.selected_state: Dict[str, Any] = {}
        self.step_records: List[Dict[str, Any]] = []

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal(id="campaign_layout"):
            with VerticalScroll(id="form"):
                yield Static("[b]Campaign configuration[/b]")
                yield Label("Config YAML file", classes="field_label")
                yield Input(value=self._config_file, placeholder="campaign.yaml", id="config_input")
                yield Label("Results directory", classes="field_label")
                yield Input(value=self._results_dir, placeholder="./campaign_results", id="results_input")
                yield Label("Clusters (comma-separated)", classes="field_label")
                yield Input(value=self._clusters, placeholder="cluster1,cluster2", id="clusters_input")
                with Horizontal(id="flag_row"):
                    with Vertical(classes="flag_option"):
                        yield Label("Dry run")
                        yield Switch(value=self._dry_run, id="dry_run_switch")
                    with Vertical(classes="flag_option"):
                        yield Label("Verbose logging")
                        yield Switch(value=self._verbose, id="verbose_switch")
                yield Static("", id="status_msg")

            with Vertical(id="campaign_browser"):
                yield Label("[b]Campaigns and state[/b]")
                yield ListView(id="campaign_history_list")
                yield Static("Select a campaign to inspect its steps.", id="campaign_detail")
                yield DataTable(id="campaign_steps")
        with Vertical(id="menu_controls"):
            with Horizontal(classes="menu_button_row"):
                yield Button("Suggest clusters", id="suggest_btn")
                yield Button("Refresh campaigns", id="refresh_campaigns_btn")
                yield Button("Start new campaign", id="start_btn", variant="success")
            with Horizontal(classes="menu_button_row"):
                yield Button("Open selected campaign", id="open_campaign_btn", disabled=True)
                yield Button("View selected step jobs", id="view_jobs_btn", disabled=True)
        yield Footer()

    async def on_mount(self) -> None:
        """Populate campaign state and prefill suggested cluster names."""
        table = self.query_one("#campaign_steps", DataTable)
        table.add_columns("App", "Step", "Cluster", "Status", "Jobs", "Error")
        table.cursor_type = "row"
        await self._refresh_campaign_history()
        if parse_clusters(self._clusters):
            return
        config_path = Path(self._config_file).expanduser()
        if not config_path.is_file():
            return
        try:
            config = load_campaign_config(config_path)
        except CampaignRunnerError:
            return
        clusters = discover_clusters_from_config(config)
        if clusters:
            self.query_one("#clusters_input", Input).value = ",".join(clusters)
            self._set_status(f"Loaded {len(clusters)} cluster(s) from app whitelists.")

    async def on_screen_resume(self) -> None:
        await self._refresh_campaign_history()

    def _read_form(self):
        config_file = self.query_one("#config_input", Input).value.strip()
        results_dir = self.query_one("#results_input", Input).value.strip()
        clusters_raw = self.query_one("#clusters_input", Input).value.strip()
        dry_run = self.query_one("#dry_run_switch", Switch).value
        verbose = self.query_one("#verbose_switch", Switch).value
        return config_file, results_dir, clusters_raw, dry_run, verbose

    def _set_status(self, message: str) -> None:
        self.query_one("#status_msg", Static).update(message)

    async def _refresh_campaign_history(self) -> None:
        list_view = self.query_one("#campaign_history_list", ListView)
        await list_view.clear()
        self.summaries = []
        selected_log = self.selected_summary.get("log_file") if self.selected_summary else None
        selected_index = None
        results_dir = Path(self.query_one("#results_input", Input).value).expanduser()
        for log_file in list_campaign_logs(results_dir):
            summary = load_campaign_log_summary(log_file)
            if summary is None:
                continue
            self.summaries.append(summary)
            status = "OK" if summary["failed_steps"] == 0 else f"{summary['failed_steps']} failed"
            label = (
                f"{summary.get('campaign_name') + '  |  ' if summary.get('campaign_name') else ''}"
                f"{summary['campaign_id']}  [{summary['completed_steps']}/{summary['total_steps']} steps]  "
                f"({status})  clusters={','.join(summary['clusters_processed'])}"
            )
            list_view.append(ListItem(Label(label)))
            if str(summary["log_file"]) == str(selected_log):
                selected_index = len(self.summaries) - 1
        if selected_index is not None:
            list_view.index = selected_index
            self._select_campaign(self.summaries[selected_index])
        elif self.summaries:
            list_view.index = 0
            self._select_campaign(self.summaries[0])
        elif not self.summaries:
            self._select_campaign(None)

    def _select_campaign(self, summary: Optional[Dict[str, Any]]) -> None:
        previous_log = self.selected_summary.get("log_file") if self.selected_summary else None
        preserve_step_key = None
        table = self.query_one("#campaign_steps", DataTable)
        if summary and str(summary.get("log_file")) == str(previous_log) and table.row_count:
            try:
                preserve_step_key = table.coordinate_to_cell_key(table.cursor_coordinate).row_key.value
            except Exception:
                pass
        self.selected_summary = summary
        table.clear()
        self.step_records = []
        self.selected_state = {}
        if summary is None:
            self.query_one("#campaign_detail", Static).update("No campaign journals found.")
            self.query_one("#open_campaign_btn", Button).disabled = True
            self.query_one("#view_jobs_btn", Button).disabled = True
            return
        try:
            self.selected_state = json.loads(Path(summary["log_file"]).read_text())
        except (OSError, json.JSONDecodeError) as exc:
            self.query_one("#campaign_detail", Static).update(f"Could not read journal: {exc}")
            self.query_one("#open_campaign_btn", Button).disabled = True
            self.query_one("#view_jobs_btn", Button).disabled = True
            return
        failed_apps = ", ".join(summary["failed_apps"]) if summary["failed_apps"] else "(none)"
        self.query_one("#campaign_detail", Static).update(
            f"Campaign: {campaign_display_label(summary.get('campaign_name'), summary['campaign_id'])} "
            f" | Started: {summary['campaign_start']}\n"
            f"Steps: {summary['completed_steps']}/{summary['total_steps']} complete, "
            f"{summary['failed_steps']} failed  |  Failed apps: {failed_apps}\n"
            f"Journal: {summary['log_file']}"
        )
        for app_name, app_data in self.selected_state.get("execution", {}).items():
            for step_name, cluster_logs in app_data.get("steps", {}).items():
                for cluster, record in cluster_logs.items():
                    row = dict(record)
                    row["_campaign_app"] = app_name
                    row["_campaign_step"] = step_name
                    row["_campaign_cluster"] = cluster
                    self.step_records.append(row)
                    table.add_row(
                        app_name,
                        step_name,
                        cluster,
                        _display_recorded_step_status(
                            record,
                            self.selected_state.get("application_projects", {}).get(app_name),
                        ),
                        "/".join(map(str, _recorded_job_counts(
                            record,
                            self.selected_state.get("application_projects", {}).get(app_name),
                        ))),
                        record.get("error_message") or "",
                        key=f"{app_name}|{step_name}|{cluster}",
                    )
        self.query_one("#open_campaign_btn", Button).disabled = False
        self.query_one("#view_jobs_btn", Button).disabled = True
        if preserve_step_key is not None:
            try:
                table.move_cursor(row=table.get_row_index(preserve_step_key), animate=False)
            except Exception:
                pass

    def on_list_view_selected(self, event: ListView.Selected) -> None:
        if event.list_view.id != "campaign_history_list":
            return
        index = event.list_view.index
        if index is not None and index < len(self.summaries):
            self._select_campaign(self.summaries[index])

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        if event.data_table.id == "campaign_steps":
            record = self.step_records[event.cursor_row] if event.cursor_row < len(self.step_records) else {}
            self.query_one("#view_jobs_btn", Button).disabled = not bool(record.get("jobs"))

    def _open_selected_step_jobs(self) -> None:
        if not self.step_records:
            return
        index = self.query_one("#campaign_steps", DataTable).cursor_row
        if index >= len(self.step_records):
            return
        record = self.step_records[index]
        app_name = record.get("_campaign_app")
        project = self.selected_state.get("application_projects", {}).get(app_name)
        jobs = record.get("jobs", [])
        if project and jobs:
            self.app.push_screen(
                CampaignJobsScreen(
                    project_root=Path(project),
                    cluster_name=record.get("_campaign_cluster"),
                    exp_dirs=[job["exp_dir"] for job in jobs if job.get("exp_dir")],
                    read_only=True,
                )
            )

    def _open_selected_campaign(self) -> None:
        if self.selected_summary is None:
            return
        summary = self.selected_summary
        config_file, results_dir, clusters_raw, dry_run, verbose = self._read_form()
        clusters = parse_clusters(clusters_raw) or summary["clusters_processed"]
        self.app.push_screen(
            CampaignStateScreen(
                Path(summary["log_file"]),
                config_file=Path(config_file) if config_file and Path(config_file).is_file() else None,
                results_dir=Path(results_dir) if results_dir else Path(summary["log_file"]).parent,
                clusters=clusters,
                dry_run=dry_run,
                verbose=verbose,
            )
        )

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        config_file, results_dir, clusters_raw, dry_run, verbose = self._read_form()

        if event.button.id == "refresh_campaigns_btn":
            await self._refresh_campaign_history()
            self._set_status("Campaign list refreshed.")
            return
        if event.button.id == "open_campaign_btn":
            self._open_selected_campaign()
            return
        if event.button.id == "view_jobs_btn":
            self._open_selected_step_jobs()
            return

        if event.button.id == "suggest_btn":
            if not config_file:
                self._set_status("Enter a config file path first.")
                return
            try:
                config = load_campaign_config(Path(config_file))
            except CampaignRunnerError as e:
                self._set_status(f"Failed to load config: {e}")
                return
            suggested = discover_clusters_from_config(config)
            if suggested:
                self.query_one("#clusters_input", Input).value = ",".join(suggested)
                self._set_status(f"Suggested {len(suggested)} cluster(s) from app whitelists.")
            else:
                self._set_status("No cluster whitelists found in config; enter clusters manually.")
            return

        if not config_file:
            self._set_status("Config file is required.")
            return
        if not Path(config_file).exists():
            self._set_status(f"Config file not found: {config_file}")
            return
        if not results_dir:
            self._set_status("Results directory is required.")
            return

        clusters = parse_clusters(clusters_raw)

        if event.button.id == "start_btn":
            if not clusters:
                self._set_status("Enter at least one cluster.")
                return
            try:
                self.app.push_screen(
                    CampaignSelectionScreen(
                        config_file=Path(config_file),
                        results_dir=Path(results_dir),
                        clusters=clusters,
                        dry_run=dry_run,
                        verbose=verbose,
                    )
                )
            except CampaignRunnerError as e:
                self._set_status(f"Failed to load campaign config: {e}")
                return

# ============================================================================
# App
# ============================================================================


class CampaignTUIApp(App):
    """Root Textual application."""

    TITLE = "Campaign Manager"
    BINDINGS = [("q", "quit", "Quit")]

    def __init__(
        self,
        *,
        config_file: str = "campaign.yaml",
        results_dir: str = "./campaign_results",
        clusters: str = "",
        dry_run: bool = False,
        verbose: bool = False,
    ) -> None:
        super().__init__()
        self._initial = dict(
            config_file=config_file,
            results_dir=results_dir,
            clusters=clusters,
            dry_run=dry_run,
            verbose=verbose,
        )

    def on_mount(self) -> None:
        self.push_screen(MenuScreen(**self._initial))

    def action_quit(self) -> None:
        """Cancel active campaign work before shutting down the Textual app."""
        screen = self.screen
        if isinstance(screen, RunScreen) and not screen.finished:
            screen.request_campaign_cancel(exit_after=True)
            return
        self.exit()


# ============================================================================
# CLI entry point
# ============================================================================


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Interactive TUI for managing application campaigns.")
    parser.add_argument("--config", default="campaign.yaml", help="Path to campaign YAML config file")
    parser.add_argument("--results-dir", default="./campaign_results", help="Directory for campaign logs/results")
    parser.add_argument("--clusters", default="", help="Comma-separated list of clusters to pre-fill")
    parser.add_argument("--dry-run", action="store_true", help="Pre-check the dry-run switch")
    parser.add_argument("--verbose", action="store_true", help="Pre-check the verbose switch")
    return parser


def run_campaign_tui(argv: Optional[List[str]] = None) -> int:
    # parser = build_arg_parser()
    # args = parser.parse_args(argv)

    app = CampaignTUIApp(
        # config_file=args.config,
        # results_dir=args.results_dir,
        # clusters=args.clusters,
        # dry_run=args.dry_run,
        # verbose=args.verbose,
    )
    app.run()
    return 0
