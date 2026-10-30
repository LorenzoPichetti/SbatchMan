"""
campaign_tui.py
===============

Interactive terminal UI (Textual) for managing and monitoring application
campaigns built on top of `campaign.py`.

Usage
-----
    python campaign_tui.py [--config CAMPAIGN.yaml] [--results-dir DIR]
                            [--clusters cluster1,cluster2] [--dry-run] [--verbose]

or, from your CLI wrapper:

    run_campaign_tui(config_file="campaign.yaml", results_dir="./results")

Screens
-------
- MenuScreen: configure campaigns, inspect previous runs, view step jobs.
- CampaignStateScreen: live state of one campaign; resume / re-run / edit.
- RunScreen: live table of app/step/cluster status, progress bar, log and
  Pause / Cancel / Retry / Continue / Skip controls. The campaign runs in a
  background thread; a queue.Queue carries structured events into the UI.

Data rules
----------
Every job status, count and duration shown here is resolved LIVE from the
application's SbatchMan project (by cluster/config/tag identity). The journal
only says which jobs belong to which step. All blocking project reads run in
worker threads, never on the UI thread.
"""

from __future__ import annotations

import argparse
import json
import queue
import time
import traceback
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
    Header, Footer, Static, Button, Input, Switch, DataTable, RichLog,
    ProgressBar, ListView, ListItem, Label, Checkbox,
)
from textual.worker import Worker, WorkerState
from textual import work

from sbatchman.core.campaign import (
    CampaignControl,
    CampaignEvent,
    EventType,
    CampaignRunnerError,
    StepView,
    JobView,
    job_key,
    load_campaign_config,
    discover_clusters_from_config,
    list_campaign_logs,
    load_campaign_log_summary,
    build_campaign_view,
    plan_incomplete_work,
    plan_resume_jobs,
    load_project_snapshot,
    save_campaign_selection,
    journal_lock_owner,
    run_campaign,
    run_campaign_with_existing_log,
    DEFAULT_JOBS_CACHE,
)
from sbatchman.tui.jobs_screen import JobsScreen


# ============================================================================
# Helpers
# ============================================================================


STATUS_STYLES = {
    "pending": "dim",
    "running": "bold yellow",
    "completed": "bold green",
    "recovered": "bold green",
    "failed": "bold red",
    "skipped": "dim cyan",
    "cancelled": "bold magenta",
    "incomplete jobs": "bold red",
    "needs attention": "bold red on yellow",
    "unknown": "dim",
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


def summary_label(summary: Dict[str, Any]) -> str:
    status = "OK" if summary["failed_steps"] == 0 else f"{summary['failed_steps']} need work"
    name = f"{summary['campaign_name']}  |  " if summary.get("campaign_name") else ""
    return (
        f"{name}{summary['campaign_id']}  "
        f"[{summary['completed_steps']}/{summary['total_steps']} steps]  "
        f"({status})  clusters={','.join(summary['clusters_processed'])}"
    )


def step_detail_text(view: StepView) -> Text:
    """Detail of one step, entirely from live data."""
    text = Text()
    text.append(f"Step error: {view.error_message or 'None'}\n")
    text.append(f"Jobs (live): {view.jobs_completed}/{view.jobs_total} completed")
    if view.recorded_status != view.status:
        text.append(f"   (journal says: {view.recorded_status})", style="dim")
    text.append("\n")
    if view.snapshot_error:
        text.append(f"Project unreadable: {view.snapshot_error}\n", style="bold red")
    bad = view.incomplete_jobs
    for job in bad[:6]:
        text.append(f"  {job.config} / {job.tag}: ")
        text.append(job.status, style=STATUS_STYLES.get(job.status.lower(), "bold red"))
        text.append("\n")
    if len(bad) > 6:
        text.append(f"  ... and {len(bad) - 6} more\n", style="dim")
    return text


def open_step_jobs(app: App, view: Optional[StepView]) -> None:
    """Open the project's own jobs view, limited to the step's CURRENT jobs."""
    if view is None or not view.project_dir:
        app.notify("Could not locate the application project for this step.", severity="error")
        return
    exp_dirs = view.exp_dirs
    if not exp_dirs:
        app.notify("No live jobs found for this step in the project.", severity="warning")
        return
    app.push_screen(
        CampaignJobsScreen(
            project_root=Path(view.project_dir),
            cluster_name=view.cluster,
            exp_dirs=exp_dirs,
            read_only=True,
        )
    )


class ConfirmCampaignRerunScreen(ModalScreen[bool]):
    """Show the live jobs affected by a campaign rerun before proceeding."""

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
        self._title_text = title
        self._description_text = description
        self._jobs = jobs
        self._steps = steps or []

    def compose(self) -> ComposeResult:
        esc = lambda v, d="unknown": rich_escape(str(v if v not in (None, "") else d))
        with Vertical(id="rerun_box"):
            yield Static(rich_escape(self._title_text), id="rerun_title")
            yield Static(rich_escape(self._description_text), id="rerun_description")
            with VerticalScroll(id="rerun_jobs"):
                if self._steps:
                    yield Static("Steps and clusters to retry:", classes="rerun_job_heading")
                    for item in self._steps:
                        yield Static(
                            f"[b]{esc(item.get('app'), '')}/{esc(item.get('step'), '')}[/b]  "
                            f"cluster=[cyan]{esc(item.get('cluster'))}[/cyan]  status={esc(item.get('status'))}",
                            classes="rerun_job",
                        )
                if self._jobs:
                    yield Static("Jobs that are not COMPLETED right now (will be submitted again):",
                                 classes="rerun_job_heading")
                for index, job in enumerate(self._jobs, start=1):
                    command = job.get("command")
                    command_line = f"\nCommand: {esc(command)}" if command else ""
                    yield Static(
                        f"[b]{index}. {esc(job.get('app'), '')}/{esc(job.get('step'), '')}[/b]  "
                        f"cluster=[cyan]{esc(job.get('cluster'))}[/cyan]\n"
                        f"Job ID={esc(job.get('job_id'), 'not assigned')}  status={esc(job.get('status'))}  "
                        f"config={esc(job.get('config'))}  tag={esc(job.get('tag'))}\n"
                        f"Directory: {esc(job.get('exp_dir'))}{command_line}",
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


class CampaignExitScreen(ModalScreen[str]):
    """Choose whether exiting an active campaign cancels its owned jobs."""

    BINDINGS = [
        Binding("escape", "stay", "Stay in campaign"),
        Binding("q", "stay", "Stay in campaign"),
        Binding("ctrl+c", "stay", "Stay in campaign"),
    ]

    CSS = """
    CampaignExitScreen { align: center middle; }
    #campaign_exit_box { width: 76; height: auto; border: round $warning; background: $surface; padding: 1 2; }
    #campaign_exit_title { height: 2; text-style: bold; color: $warning; }
    #campaign_exit_message { height: auto; padding-bottom: 1; }
    #campaign_exit_buttons { height: 5; align: center middle; }
    #campaign_exit_buttons Button { margin: 0 1; }
    """

    def compose(self) -> ComposeResult:
        with Vertical(id="campaign_exit_box"):
            yield Static("Campaign is still active", id="campaign_exit_title")
            yield Static(
                "Choose whether to cancel queued/running jobs submitted by this campaign or leave "
                "them running in the SbatchMan projects. Leaving stops campaign monitoring; "
                "you can reopen the campaign later to refresh its state.",
                id="campaign_exit_message",
            )
            with Vertical(id="campaign_exit_buttons"):
                yield Button("Cancel campaign jobs and exit", id="campaign_exit_cancel", variant="error")
                yield Button("Leave jobs running and exit", id="campaign_exit_leave", variant="success")
                yield Button("Stay in campaign", id="campaign_exit_stay")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        choices = {
            "campaign_exit_cancel": "cancel",
            "campaign_exit_leave": "leave",
            "campaign_exit_stay": "stay",
        }
        if event.button.id in choices:
            self.dismiss(choices[event.button.id])

    def action_stay(self) -> None:
        self.dismiss("stay")


class CampaignSelectionScreen(Screen):
    """Choose the applications and steps included in a campaign run."""

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
                    step.name in selected_app_steps if isinstance(selected_app_steps, list) else True
                )
                stored_clusters = app_cluster_steps.get(step.name)
                app_steps[step.name] = {
                    "enabled": step_selected,
                    "clusters": {
                        cluster: cluster in stored_clusters if isinstance(stored_clusters, list) else True
                        for cluster in self._available_clusters(app)
                    },
                }
            preferences["applications"][app.name] = {"enabled": app_selected, "steps": app_steps}
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
                app_id = f"app_{app_index}"
                app_check = Checkbox(app.name, value=bool(saved_app.get("enabled", True)), id=app_id)
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
                                cluster_heading = f"Select clusters (YAML whitelist: {', '.join(app.cluster_whitelist)})"
                            else:
                                cluster_heading = "Select clusters (campaign targets)"
                            yield Static(rich_escape(cluster_heading), classes="cluster_label")
                            with Grid(classes="cluster_grid"):
                                for cluster_index, cluster in enumerate(self._available_clusters(app)):
                                    cluster_id = f"cluster_{app_index}_{step_index}_{cluster_index}"
                                    cluster_check = Checkbox(
                                        cluster, value=bool(saved_clusters.get(cluster, True)), id=cluster_id,
                                    )
                                    yield cluster_check
                                    self.cluster_checks[(app.name, step.name, cluster)] = cluster_check
                                    self._ids[cluster_id] = (app.name, step.name, cluster)

        yield Static("", id="selection_error")
        with Horizontal(id="selection_controls"):
            yield Button("Save selection" if self.edit_log else "Run selected",
                         id="run_selected_btn", variant="success")
            yield Button("Back", id="selection_back_btn")
        yield Footer()

    def on_checkbox_changed(self, event: Checkbox.Changed) -> None:
        info = self._ids.get(event.checkbox.id or "")
        if info is None:
            return
        app_name, step_name, cluster_name = info
        if step_name is None and event.value:
            app_steps = [(step, check) for (name, step), check in self.step_checks.items() if name == app_name]
            if app_steps and not any(check.value for _, check in app_steps):
                for step, check in app_steps:
                    check.value = True
                    for (cluster_app, cluster_step, _), cluster_check in self.cluster_checks.items():
                        if cluster_app == app_name and cluster_step == step:
                            cluster_check.value = True
        elif step_name is not None and cluster_name is None and event.value:
            step_clusters = [
                check for (name, step, _), check in self.cluster_checks.items()
                if name == app_name and step == step_name
            ]
            if step_clusters and not any(check.value for check in step_clusters):
                for check in step_clusters:
                    check.value = True

    def _error(self, message: str) -> None:
        self.query_one("#selection_error", Static).update(rich_escape(message))

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "selection_back_btn":
            self.app.pop_screen()
            return
        if event.button.id != "run_selected_btn":
            return

        selected_apps: List[str] = []
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
                        self._error(f"Choose at least one cluster for step '{step.name}' in '{app.name}'.")
                        return
                    selected_steps.append(step.name)
                    selected_clusters_by_step[step.name] = selected_clusters
            remembered["applications"][app.name] = {"enabled": enabled, "steps": remembered_steps}
            if enabled:
                if not selected_steps:
                    self._error(f"Select at least one step for application '{app.name}', or uncheck the application.")
                    return
                selected_apps.append(app.name)
                steps_by_app[app.name] = selected_steps
                clusters_by_app_step[app.name] = selected_clusters_by_step
                for step_clusters in selected_clusters_by_step.values():
                    selected_campaign_clusters.update(step_clusters)
        if not selected_apps:
            self._error("Select at least one application and step.")
            return

        campaign_selection = {
            "applications": selected_apps,
            "steps_by_application": steps_by_app,
            "clusters_by_step": clusters_by_app_step,
            "ui": remembered,
        }
        if self.edit_log is not None:
            try:
                save_campaign_selection(self.edit_log, campaign_selection)
            except (OSError, ValueError, CampaignRunnerError) as e:
                self._error(f"Could not save the campaign selection: {e}")
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
    #attention_label { height: 1; color: $warning; }
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
        self.application_projects: Dict[str, str] = {
            app.name: str(app.dir / "SbatchMan") for app in campaign_config.apps
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
                # A resumed campaign keeps using the projects it was created with.
                self.application_projects.update(journal.get("application_projects") or {})
            except (OSError, json.JSONDecodeError):
                pass

        self.event_queue: "queue.Queue[CampaignEvent]" = queue.Queue()
        self.control = CampaignControl()
        self.success: Optional[bool] = None
        self.finished = False
        self.detached = False
        self.pending: List[Dict[str, Any]] = []  # steps waiting for a user decision (FIFO)
        self._exit_after_campaign = False

        self.rows: Dict[str, Dict[str, Any]] = {}
        self.row_order: List[str] = []
        self.selected_row_key: Optional[str] = None

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Static(self._header_text(), id="run_header")
        yield DataTable(id="status_table")
        yield Static("", id="progress_label")
        yield ProgressBar(id="job_progress", show_eta=False)
        yield Static("", id="attention_label")
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
        campaign = f"  campaign=[cyan]{rich_escape(str(self.campaign_id))}[/cyan]" if self.campaign_id else ""
        name = f"[b]{rich_escape(self.campaign_name)}[/b]  " if self.campaign_name else ""
        return (
            f"[b]{mode_label} campaign[/b]  {name}{campaign}  "
            f"config=[cyan]{rich_escape(str(self.config_file))}[/cyan]  "
            f"clusters=[cyan]{rich_escape(', '.join(self.clusters))}[/cyan]  "
            f"dry_run=[cyan]{self.dry_run}[/cyan]"
        )

    def on_mount(self) -> None:
        table = self.query_one("#status_table", DataTable)
        table.add_columns("App", "Step", "Cluster", "Status", "Duration", "Jobs")
        table.cursor_type = "row"
        self.query_one("#job_progress", ProgressBar).update(total=100, progress=0)
        self.set_interval(0.15, self._drain_queue)
        self._run_campaign_worker()

    # ------------------------------------------------------------------
    # Background worker
    # ------------------------------------------------------------------

    def _log_error(self, message: str) -> None:
        self.event_queue.put(CampaignEvent(
            type=EventType.LOG, timestamp="", data={"level": "error", "message": rich_escape(message)},
        ))

    @work(thread=True, exclusive=True, group="campaign")
    def _run_campaign_worker(self) -> None:
        try:
            if self.mode == "resume" and self.log_file is not None:
                self.success = run_campaign_with_existing_log(
                    config_file=self.config_file, log_file=self.log_file, clusters=self.clusters,
                    verbose=self.verbose, dry_run=self.dry_run, resume_mode=self.resume_mode,
                    force_apps=self.force_apps, event_queue=self.event_queue, control=self.control,
                    console_output=False, applications=self.applications,
                    steps_by_application=self.steps_by_application, clusters_by_step=self.clusters_by_step,
                )
            else:
                self.success = run_campaign(
                    config_file=self.config_file, results_dir=self.results_dir, clusters=self.clusters,
                    verbose=self.verbose, dry_run=self.dry_run, resume=False,
                    event_queue=self.event_queue, control=self.control, console_output=False,
                    applications=self.applications, steps_by_application=self.steps_by_application,
                    clusters_by_step=self.clusters_by_step, campaign_selection=self.campaign_selection,
                )
        except CampaignRunnerError as e:
            self._log_error(f"Fatal error: {e}")
            self.success = False
        except Exception as e:  # never end a run silently
            self._log_error(f"Unexpected error: {type(e).__name__}: {e}")
            self._log_error(traceback.format_exc(limit=8))
            self.success = False

    def on_worker_state_changed(self, event: Worker.StateChanged) -> None:
        if event.worker.group != "campaign":
            return
        if event.state not in (WorkerState.SUCCESS, WorkerState.ERROR, WorkerState.CANCELLED):
            return
        self._drain_queue()  # flush the last events before the verdict
        self.finished = True
        self.pending.clear()
        for button_id in ("#pause_btn", "#retry_btn", "#continue_btn", "#skip_btn", "#cancel_btn"):
            self.query_one(button_id, Button).disabled = True
        self.query_one("#back_btn", Button).disabled = False
        self.query_one("#attention_label", Static).update("")
        log = self.query_one("#log_view", RichLog)
        if self.detached:
            log.write("[bold yellow]Campaign monitoring stopped; queued/running jobs were left alone.[/bold yellow]")
        elif self.success:
            log.write("[bold green]Campaign finished successfully.[/bold green]")
        else:
            log.write("[bold red]Campaign finished with failures (or was cancelled).[/bold red]")
        if self.mode == "resume" and self.log_file is not None:
            log.write(rich_escape(f"Campaign journal updated in place: {self.log_file}"))
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
            data = event.data

            if event.type == EventType.LOG:
                level = data.get("level", "info")
                message = data.get("message", "")
                color = {
                    "debug": "dim", "info": "blue", "warning": "yellow", "error": "red",
                    "critical": "bold red", "job": "bold cyan",
                }.get(level, "white")
                if message.lstrip().startswith(("Step:", "Step complete:", "Execution summary")):
                    log.write(Text(" "))
                if level == "job" and data.get("job_title"):
                    job_line = Text("Job: ")
                    job_line.append(data["job_title"], style="bold cyan")
                    if data.get("job_details"):
                        job_line.append(f"  {data['job_details']}", style="dim")
                    log.write(job_line)
                else:
                    log.write(f"[{color}]{message}[/{color}]")

            elif event.type == EventType.CAMPAIGN_START:
                if data.get("campaign_id"):
                    self.campaign_id = data["campaign_id"]
                    self.query_one("#run_header", Static).update(self._header_text())
                title = f": {rich_escape(data['campaign_name'])}" if data.get("campaign_name") else ""
                log.write(f"[bold]Campaign started{title}.[/bold]")

            elif event.type == EventType.CLUSTER_START:
                log.write(Text(" "))
                log.write(f"[bold magenta]== Cluster: {rich_escape(str(data['cluster']))} ==[/bold magenta]")

            elif event.type == EventType.APP_START:
                log.write(Text(" "))
                log.write(f"[bold blue]-- App: {rich_escape(str(data['app']))} ({rich_escape(str(data.get('cluster')))}) --[/bold blue]")

            elif event.type == EventType.APP_SKIPPED:
                log.write(f"[yellow]App skipped: {rich_escape(str(data['app']))} ({rich_escape(str(data.get('reason')))})[/yellow]")

            elif event.type == EventType.STEP_START:
                key = self._row_key(data)
                self._clear_pending(key)
                self._ensure_row(key, data, status="running")
                self.rows[key]["duration"] = ""
                table_dirty = True

            elif event.type == EventType.STEP_SKIPPED:
                key = self._row_key(data)
                self._clear_pending(key)
                self._ensure_row(key, data, status="skipped")
                table_dirty = True

            elif event.type == EventType.STEP_PROGRESS:
                completed = data.get("completed", 0)
                succeeded = data.get("succeeded", completed)
                total = data.get("total", 0) or 1
                self.query_one("#progress_label", Static).update(
                    rich_escape(f"Following jobs for {data.get('app')}/{data.get('step')} on "
                                f"{data.get('cluster')}: {completed}/{total} finished")
                )
                self.query_one("#job_progress", ProgressBar).update(total=total, progress=completed)
                key = self._row_key(data)
                if key in self.rows:
                    self.rows[key]["jobs"] = f"{succeeded}/{total}"
                    self.rows[key]["job_records"] = data.get("jobs", [])
                    if data.get("duration") is not None:
                        self.rows[key]["duration"] = f"{data['duration']:.1f}s"
                    table_dirty = True

            elif event.type == EventType.STEP_END:
                key = self._row_key(data)
                self._clear_pending(key)
                self._ensure_row(key, data, status=data.get("status", "completed"))
                row = self.rows[key]
                if data.get("duration") is not None:
                    row["duration"] = f"{data['duration']:.1f}s"
                row["job_records"] = data.get("jobs", [])
                if self.dry_run:
                    row["jobs"] = f"{len(row['job_records'])} planned"
                elif "jobs_count" in data:
                    row["jobs"] = f"{data.get('jobs_successful', 0)}/{data.get('jobs_count', 0)}"
                table_dirty = True

            elif event.type == EventType.STEP_NEEDS_ATTENTION:
                key = self._row_key(data)
                self._ensure_row(key, data, status="needs attention")
                table_dirty = True
                self.pending = [p for p in self.pending if self._row_key(p) != key] + [dict(data)]
                self._update_attention()
                log.write(Text(
                    f"Needs attention: {data.get('app')}/{data.get('step')} on {data.get('cluster')}\n"
                    f"{data.get('error', '')}", style="bold yellow"))
                for job in data.get("jobs", []):
                    if str(job.get("last_known_status", "")).upper() != "COMPLETED":
                        log.write(Text(f"  {job.get('config')} / {job.get('tag')} "
                                       f"[{job.get('last_known_status')}] ({job.get('exp_dir')})"))
                log.write(Text("  If you fix these jobs by hand, the campaign notices and continues "
                               "on its own; or choose Retry / Continue / Skip.", style="dim"))

            elif event.type == EventType.PAUSED:
                self.query_one("#pause_btn", Button).label = "Resume"
                log.write("[yellow]Paused.[/yellow]")

            elif event.type == EventType.RESUMED:
                self.query_one("#pause_btn", Button).label = "Pause"
                log.write("[green]Resumed.[/green]")

            elif event.type == EventType.CAMPAIGN_CANCELLED:
                log.write(f"[bold magenta]Cancelled: {rich_escape(str(data.get('message')))}[/bold magenta]")

            elif event.type == EventType.CAMPAIGN_END:
                # Final snapshot: the table is authoritative even if a
                # progress event was delayed or lost during parallel runs.
                for step in data.get("steps", []):
                    key = self._row_key(step)
                    self._ensure_row(key, step, status=step.get("status", "unknown"))
                    row = self.rows[key]
                    if step.get("duration") is not None:
                        row["duration"] = f"{step['duration']:.1f}s"
                    row["job_records"] = step.get("jobs", [])
                    row["jobs"] = (
                        f"{len(row['job_records'])} planned" if self.dry_run
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
                "app": data.get("app", ""), "step": data.get("step", ""),
                "cluster": data.get("cluster", ""), "status": status,
                "duration": "", "jobs": "", "job_records": data.get("jobs", []),
            }
            self.row_order.append(key)
        else:
            self.rows[key]["status"] = status

    def _refresh_table(self) -> None:
        table = self.query_one("#status_table", DataTable)
        selected_key, selected_column, selected_row = None, 0, 0
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
            table.add_row(r["app"], r["step"], r["cluster"], status_text(r["status"]),
                          r["duration"], r["jobs"], key=key)
        if table.row_count:
            restored_row = None
            if selected_key in self.rows:
                restored_row = self.row_order.index(selected_key)
                self.selected_row_key = selected_key
            elif selected_row < table.row_count:
                restored_row = selected_row
            if restored_row is not None:
                table.move_cursor(row=restored_row, column=min(selected_column, 5), animate=False)

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        self.selected_row_key = str(event.row_key.value)

    # ------------------------------------------------------------------
    # Step jobs (resolved live, off the UI thread)
    # ------------------------------------------------------------------

    def action_view_step_jobs(self) -> None:
        key = self.selected_row_key
        try:
            table = self.query_one("#status_table", DataTable)
            row_key = table.coordinate_to_cell_key(table.cursor_coordinate).row_key.value
            if row_key:
                key = str(row_key)
        except Exception:
            pass
        row = self.rows.get(key or "")
        if not row or not row.get("job_records"):
            self.notify("Select a step with recorded jobs to inspect.", severity="information")
            return
        self._load_step_view(key)

    @work(thread=True, exclusive=True, group="step_jobs")
    def _load_step_view(self, key: str) -> None:
        row = self.rows[key]
        project = self.application_projects.get(row["app"]) or next(
            (r.get("project_root") for r in row["job_records"] if r.get("project_root")), None
        )
        if not project:
            self.app.call_from_thread(self.notify, f"Could not locate the project for {row['app']}.", severity="error")
            return
        snapshot = DEFAULT_JOBS_CACHE.snapshot(Path(project), row["cluster"], refresh=True)
        view = StepView(app=row["app"], step=row["step"], cluster=row["cluster"],
                        recorded_status=row["status"], status=row["status"], project_dir=project,
                        snapshot_error=snapshot.error)
        for rec in row["job_records"]:
            k = job_key(rec, row["cluster"])
            live = None if snapshot.error else snapshot.jobs.get(k)
            if live is not None:
                view.jobs.append(JobView(key=k, cluster=k[0], config=k[1], tag=k[2],
                                         status=str(live.status), exp_dir=live.exp_dir,
                                         job_id=live.job_id, resolved=True))
        self.app.call_from_thread(open_step_jobs, self.app, view)

    # ------------------------------------------------------------------
    # Controls
    # ------------------------------------------------------------------

    def _clear_pending(self, key: str) -> None:
        before = len(self.pending)
        self.pending = [p for p in self.pending if self._row_key(p) != key]
        if len(self.pending) != before:
            self._update_attention()

    def _update_attention(self) -> None:
        has = bool(self.pending) and not self.finished
        for button_id in ("#retry_btn", "#continue_btn", "#skip_btn"):
            self.query_one(button_id, Button).disabled = not has
        label = self.query_one("#attention_label", Static)
        if has:
            p = self.pending[0]
            more = f" (+{len(self.pending) - 1} more waiting)" if len(self.pending) > 1 else ""
            label.update(rich_escape(f"Needs attention: {p.get('app')}/{p.get('step')} on {p.get('cluster')}{more}"))
        else:
            label.update("")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        button = event.button.id
        if button == "pause_btn":
            if self.control.is_paused:
                self.control.resume()
                event.button.label = "Pause"
            else:
                self.control.request_pause()
                event.button.label = "Resume"
        elif button == "cancel_btn":
            self.request_campaign_cancel()
        elif button in ("retry_btn", "continue_btn", "skip_btn") and self.pending:
            detail = self.pending[0]
            if button == "retry_btn":
                self._prepare_retry(detail)  # fresh live job list, then confirm
            else:
                self._resolve_step("continue" if button == "continue_btn" else "skip", detail)
        elif button == "back_btn" and self.finished:
            self.app.pop_screen()

    def action_try_back(self) -> None:
        if self.finished:
            self.app.pop_screen()

    @work(thread=True, exclusive=True, group="retry_prepare")
    def _prepare_retry(self, detail: Dict[str, Any]) -> None:
        """List what a retry would actually re-submit, from the live project."""
        project = self.application_projects.get(detail.get("app", ""))
        jobs: List[Dict[str, Any]] = []
        if project:
            snapshot = load_project_snapshot(Path(project), detail["cluster"], update_scheduler=True)
            for rec in detail.get("jobs", []):
                k = job_key(rec, detail["cluster"])
                status = snapshot.status_of(k)
                if status != "COMPLETED":
                    live = snapshot.jobs.get(k)
                    jobs.append({**rec, "app": detail["app"], "step": detail["step"], "status": status,
                                 "exp_dir": live.exp_dir if live else rec.get("exp_dir"),
                                 "job_id": live.job_id if live else rec.get("job_id")})
        self.app.call_from_thread(self._confirm_retry, detail, jobs)

    def _confirm_retry(self, detail: Dict[str, Any], jobs: List[Dict[str, Any]]) -> None:
        title = f"Retry {detail.get('app')}/{detail.get('step')} on {detail.get('cluster')}?"
        description = (
            "The step's script runs again and its jobs are re-evaluated against the project: "
            "COMPLETED jobs (including ones you ran by hand) are kept; running jobs are followed; "
            "only the jobs listed below are submitted again."
        )
        self.app.push_screen(
            ConfirmCampaignRerunScreen(title, description, jobs),
            lambda confirmed: self._on_retry_confirmation(confirmed, detail),
        )

    def _on_retry_confirmation(self, confirmed: bool, detail: Dict[str, Any]) -> None:
        if confirmed:
            self._resolve_step("retry", detail)
        else:
            self.query_one("#log_view", RichLog).write(
                "[yellow]Retry cancelled; choose another resolution or retry later.[/yellow]"
            )

    def _resolve_step(self, resolution: str, detail: Dict[str, Any]) -> None:
        key = self._row_key(detail)
        self.control.request_resolution(resolution, key)
        self._clear_pending(key)
        self.query_one("#log_view", RichLog).write(
            Text(f"{resolution.capitalize()} selected for {detail.get('app')}/{detail.get('step')} "
                 f"on {detail.get('cluster')}; campaign resuming…", style="cyan")
        )

    def request_campaign_cancel(self, exit_after: bool = False) -> None:
        if self.finished:
            if exit_after:
                self.app.exit()
            return
        self._exit_after_campaign = self._exit_after_campaign or exit_after
        self.control.request_cancel()
        for button_id in ("#cancel_btn", "#pause_btn", "#retry_btn", "#continue_btn", "#skip_btn"):
            self.query_one(button_id, Button).disabled = True
        message = (
            "Cancellation requested; waiting for the campaign's own submitted jobs to stop before exiting…"
            if exit_after else "Cancellation requested… (only jobs submitted by this campaign are cancelled)"
        )
        self.query_one("#log_view", RichLog).write(f"[bold magenta]{message}[/bold magenta]")

    def request_campaign_detach(self) -> None:
        if self.finished:
            self.app.exit()
            return
        self.detached = True
        self._exit_after_campaign = True
        self.control.request_detach()
        for button_id in ("#cancel_btn", "#pause_btn", "#retry_btn", "#continue_btn", "#skip_btn"):
            self.query_one(button_id, Button).disabled = True
        self.query_one("#log_view", RichLog).write(
            "[bold yellow]Stopping campaign monitoring and leaving submitted jobs running…[/bold yellow]"
        )


# ============================================================================
# Campaign State Screen
# ============================================================================


class CampaignStateScreen(Screen):
    """Campaign detail page with inspection and resume/rerun actions."""

    CSS = """
    #state_header { height: auto; padding: 1; }
    #state_table { height: 1fr; }
    #state_detail { height: 9; border: solid $accent; padding: 0 1; }
    #state_controls { height: 6; }
    .state_control_row { height: 3; align: center middle; }
    """

    BINDINGS = [
        Binding("r", "refresh_state", "Refresh from scheduler"),
        Binding("j", "view_step_jobs", "View step jobs"),
    ]

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
        self.results_dir = Path(results_dir) if results_dir else self.log_file.parent
        self.clusters = clusters or []
        self.dry_run = dry_run
        self.verbose = verbose
        self.state: Dict[str, Any] = {}
        self.error: Optional[str] = None
        try:
            self.state = json.loads(self.log_file.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            self.error = str(exc)

        # Prefer the config the campaign was started with; fall back to the one
        # given by the caller (e.g. when the journal's file was moved).
        candidates = [Path(self.state["config_file"])] if self.state.get("config_file") else []
        if config_file:
            candidates.append(Path(config_file).expanduser())
        self.config_file: Optional[Path] = next((p for p in candidates if p.is_file()), None)
        try:
            self.campaign_config = load_campaign_config(self.config_file) if self.config_file else None
        except CampaignRunnerError:
            self.campaign_config = None

        self.views: List[StepView] = []
        self.view_by_key: Dict[str, StepView] = {}
        self.lock_owner: Optional[str] = None
        self._last_load = 0.0
        self._sequential: Dict[Tuple[str, str], bool] = {}

    # ------------------------------------------------------------------
    def compose(self) -> ComposeResult:
        yield Header()
        yield Static(self._header_text(), id="state_header")
        if not self.error:
            yield DataTable(id="state_table")
            yield Static("Loading live job state…", id="state_detail")
        with Vertical(id="state_controls"):
            with Horizontal(classes="state_control_row"):
                yield Button("Resume / apply edits", id="state_resume_btn", disabled=True)
                yield Button("Re-run failed apps", id="state_rerun_btn", disabled=True)
                yield Button("Edit selection", id="state_edit_selection_btn", disabled=True)
            with Horizontal(classes="state_control_row"):
                yield Button("Refresh (query scheduler)", id="state_refresh_btn", disabled=self.error is not None)
                yield Button("View Step Jobs", id="state_view_jobs_btn", disabled=True)
                yield Button("Back", id="state_back_btn")
        yield Footer()

    def _header_text(self) -> str:
        if self.error:
            return f"Could not read campaign journal: {rich_escape(self.error)}"
        s = self.state
        config = rich_escape(str(self.config_file)) if self.config_file else "[red]not found[/red]"
        lock = f"\n[bold red]In use by {rich_escape(self.lock_owner)}[/bold red]" if self.lock_owner else ""
        return (
            f"Campaign [cyan]{campaign_display_label(s.get('campaign_name'), s.get('campaign_id', 'unknown'))}[/cyan]  "
            f"Started: {rich_escape(str(s.get('campaign_start', 'unknown')))}  "
            f"Ended: {rich_escape(str(s.get('campaign_end') or 'incomplete'))}\n"
            f"Clusters: {rich_escape(', '.join(s.get('clusters_processed', [])))}   Config: {config}\n"
            f"Journal: {rich_escape(str(self.log_file))}{lock}"
        )

    def on_mount(self) -> None:
        if self.error:
            return
        table = self.query_one("#state_table", DataTable)
        table.add_columns("App", "Step", "Cluster", "Status", "Duration", "Jobs", "Error")
        table.cursor_type = "row"
        self._load_state(refresh=True)

    def on_screen_resume(self) -> None:
        """Re-read everything after returning from a run / selection edit."""
        if self.error or time.monotonic() - self._last_load < 1.0:
            return
        DEFAULT_JOBS_CACHE.clear()
        self._load_state(refresh=False)

    def action_refresh_state(self) -> None:
        if not self.error:
            self.notify("Querying schedulers…")
            self._load_state(refresh=True)

    # ------------------------------------------------------------------
    # Loading (worker thread) -> applying (UI thread)
    # ------------------------------------------------------------------

    def _sequential_lookup(self, app_name: str, step_name: str) -> Optional[bool]:
        """Journals written before `jobs_sequential` existed: read the jobs YAML."""
        key = (app_name, step_name)
        if key in self._sequential:
            return self._sequential[key]
        sequential = None
        if self.campaign_config is not None:
            app = next((a for a in self.campaign_config.apps if a.name == app_name), None)
            step = next((s for s in app.steps if s.name == step_name), None) if app else None
            if app is not None and step is not None and step.jobs:
                try:
                    sequential = bool((yaml.safe_load((app.dir / step.jobs).read_text()) or {}).get("sequential", False))
                except (OSError, yaml.YAMLError, AttributeError):
                    pass
        self._sequential[key] = sequential  # type: ignore[assignment]
        return sequential

    @work(thread=True, exclusive=True, group="state_load")
    def _load_state(self, refresh: bool, then: Optional[str] = None) -> None:
        try:
            state = json.loads(self.log_file.read_text())
            views = build_campaign_view(
                state, cache=DEFAULT_JOBS_CACHE, refresh=refresh, sequential_lookup=self._sequential_lookup,
            )
            owner = journal_lock_owner(self.log_file)
        except Exception as exc:  # report, do not die silently
            self.app.call_from_thread(self.notify, f"Could not load campaign state: {exc}", severity="error")
            return
        self.app.call_from_thread(self._apply_state, state, views, owner, then)

    def _apply_state(self, state: Dict[str, Any], views: List[StepView], owner: Optional[str], then: Optional[str]) -> None:
        self._last_load = time.monotonic()
        self.state, self.views, self.lock_owner = state, views, owner
        self.view_by_key = {v.key: v for v in views}
        self.query_one("#state_header", Static).update(self._header_text())
        table = self.query_one("#state_table", DataTable)
        keep = None
        try:
            if table.row_count:
                keep = table.coordinate_to_cell_key(table.cursor_coordinate).row_key.value
        except Exception:
            pass
        table.clear()
        for v in views:
            table.add_row(
                v.app, v.step, v.cluster, status_text(v.status), f"{v.duration_seconds:.1f}s",
                f"{v.jobs_completed}/{v.jobs_total}", v.error_message or "", key=v.key,
            )
        if not views:
            self.query_one("#state_detail", Static).update("No steps recorded.")
        elif keep in self.view_by_key:
            table.move_cursor(row=[v.key for v in views].index(keep), animate=False)
        usable = self.config_file is not None and not owner
        self.query_one("#state_resume_btn", Button).disabled = not usable
        self.query_one("#state_rerun_btn", Button).disabled = not (usable and any(v.needs_work for v in views))
        self.query_one("#state_edit_selection_btn", Button).disabled = not usable
        self._show_highlighted()
        if then == "resume":
            self._start_resume(rerun_failed=False)
        elif then == "rerun":
            self._start_resume(rerun_failed=True)

    # ------------------------------------------------------------------
    # Selection / details
    # ------------------------------------------------------------------

    def _highlighted_view(self) -> Optional[StepView]:
        try:
            table = self.query_one("#state_table", DataTable)
            key = table.coordinate_to_cell_key(table.cursor_coordinate).row_key.value
        except Exception:
            return None
        return self.view_by_key.get(str(key)) if key else None

    def _show_highlighted(self) -> None:
        view = self._highlighted_view()
        self.query_one("#state_view_jobs_btn", Button).disabled = not (view and view.exp_dirs)
        if view is not None:
            self.query_one("#state_detail", Static).update(step_detail_text(view))

    def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        if event.data_table.id == "state_table":
            self._show_highlighted()

    def action_view_step_jobs(self) -> None:
        open_step_jobs(self.app, self._highlighted_view())

    # ------------------------------------------------------------------
    # Resume / re-run
    # ------------------------------------------------------------------

    def _start_resume(self, rerun_failed: bool = False) -> None:
        """Plan from the live views loaded a moment ago, confirm, then launch."""
        if self.config_file is None:
            self.notify("A campaign config file is required to resume.", severity="error")
            return
        if self.lock_owner:
            self.notify(f"This campaign is in use by {self.lock_owner}.", severity="error")
            return
        target_clusters = self.clusters or self.state.get("clusters_processed", [])
        saved_selection = self.state.get("selection") or None
        plan: Optional[Dict[str, Any]] = None
        if rerun_failed:
            plan = plan_incomplete_work(self.views, clusters=target_clusters, selection=saved_selection)
            jobs, steps = plan["jobs"], plan["steps"]
            if not steps:
                self.notify("No incomplete campaign work exists on the selected clusters.", severity="information")
                return
            title = f"Re-run {len(steps)} incomplete step(s)?"
            description = (
                "Only the listed steps run again. Jobs that are COMPLETED now (including ones you "
                "ran by hand) are kept; running jobs are followed; the non-COMPLETED jobs below "
                "are submitted again."
            )
        else:
            jobs, steps = plan_resume_jobs(self.views, clusters=target_clusters, selection=saved_selection), None
            title = f"Resume with {len(jobs)} non-COMPLETED job(s)?"
            description = (
                "Steps whose definition is unchanged and whose jobs are all COMPLETED now are skipped. "
                "Only jobs that are not COMPLETED are submitted again."
            )
        if jobs or steps:
            self.app.push_screen(
                ConfirmCampaignRerunScreen(title, description, jobs, steps=steps),
                lambda confirmed: self._launch_resume(plan, target_clusters) if confirmed
                else self.notify("Campaign rerun cancelled.", severity="information"),
            )
        else:
            self._launch_resume(plan, target_clusters)

    def _launch_resume(self, plan: Optional[Dict[str, Any]], target_clusters: List[str]) -> None:
        if plan is not None:
            clusters = [c for c in plan["clusters"] if not self.clusters or c in self.clusters]
            applications, steps_by_app, clusters_by_step = (
                plan["applications"], plan["steps_by_application"], plan["clusters_by_step"],
            )
        else:  # the runner applies the journal's saved selection
            clusters, applications, steps_by_app, clusters_by_step = target_clusters, None, None, None
        self.app.push_screen(
            RunScreen(
                mode="resume", config_file=self.config_file, results_dir=self.results_dir,
                clusters=clusters, dry_run=self.dry_run, verbose=self.verbose,
                resume_mode="auto", force_apps=[], log_file=self.log_file,
                applications=applications, steps_by_application=steps_by_app,
                clusters_by_step=clusters_by_step,
            )
        )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        button = event.button.id
        if button == "state_back_btn":
            self.app.pop_screen()
        elif button == "state_refresh_btn":
            self.action_refresh_state()
        elif button in ("state_resume_btn", "state_rerun_btn"):
            # Re-read the live state first: the user may have changed things
            # since this page was opened.
            self.notify("Checking live job state…")
            self._load_state(refresh=True, then="resume" if button == "state_resume_btn" else "rerun")
        elif button == "state_edit_selection_btn" and self.config_file is not None:
            self.app.push_screen(
                CampaignSelectionScreen(
                    config_file=self.config_file, results_dir=self.results_dir,
                    clusters=self.state.get("clusters_processed") or self.clusters,
                    dry_run=self.dry_run, verbose=self.verbose,
                    selection=self.state.get("selection"), edit_log=self.log_file,
                )
            )
        elif button == "state_view_jobs_btn":
            self.action_view_step_jobs()


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
        self.views: List[StepView] = []
        self.view_by_key: Dict[str, StepView] = {}
        self._last_history_load = 0.0

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

    def on_mount(self) -> None:
        table = self.query_one("#campaign_steps", DataTable)
        table.add_columns("App", "Step", "Cluster", "Status", "Jobs", "Error")
        table.cursor_type = "row"
        self._load_history(refresh=False)
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

    def on_screen_resume(self) -> None:
        if time.monotonic() - self._last_history_load < 1.0:
            return
        DEFAULT_JOBS_CACHE.clear()
        self._load_history(refresh=False)

    def _read_form(self):
        return (
            self.query_one("#config_input", Input).value.strip(),
            self.query_one("#results_input", Input).value.strip(),
            self.query_one("#clusters_input", Input).value.strip(),
            self.query_one("#dry_run_switch", Switch).value,
            self.query_one("#verbose_switch", Switch).value,
        )

    def _set_status(self, message: str) -> None:
        self.query_one("#status_msg", Static).update(rich_escape(message))

    # ------------------------------------------------------------------
    # History (worker thread -> UI)
    # ------------------------------------------------------------------

    @work(thread=True, exclusive=True, group="history")
    def _load_history(self, refresh: bool) -> None:
        results_dir = Path(self.query_one("#results_input", Input).value).expanduser()
        summaries = []
        for log_file in list_campaign_logs(results_dir):
            summary = load_campaign_log_summary(log_file, cache=DEFAULT_JOBS_CACHE, refresh=refresh)
            if summary is not None:
                summaries.append(summary)
        self.app.call_from_thread(self._apply_history, summaries)

    async def _apply_history(self, summaries: List[Dict[str, Any]]) -> None:
        self._last_history_load = time.monotonic()
        keep = self.selected_summary.get("log_file") if self.selected_summary else None
        list_view = self.query_one("#campaign_history_list", ListView)
        await list_view.clear()
        self.summaries = summaries
        for summary in summaries:
            list_view.append(ListItem(Label(summary_label(summary), markup=False)))
        index = next((i for i, s in enumerate(summaries) if s["log_file"] == keep), 0 if summaries else None)
        if index is None:
            self._select_campaign(None)
            return
        list_view.index = index
        self._select_campaign(summaries[index])

    def _select_campaign(self, summary: Optional[Dict[str, Any]]) -> None:
        self.selected_summary = summary
        self.query_one("#campaign_steps", DataTable).clear()
        self.views, self.view_by_key = [], {}
        self.query_one("#view_jobs_btn", Button).disabled = True
        self.query_one("#open_campaign_btn", Button).disabled = summary is None
        if summary is None:
            self.query_one("#campaign_detail", Static).update("No campaign journals found.")
            return
        self.query_one("#campaign_detail", Static).update("Loading live job state…")
        self._load_selected(summary)

    @work(thread=True, exclusive=True, group="menu_view")
    def _load_selected(self, summary: Dict[str, Any]) -> None:
        try:
            state = json.loads(Path(summary["log_file"]).read_text())
            views = build_campaign_view(state, cache=DEFAULT_JOBS_CACHE)
        except Exception as exc:
            self.app.call_from_thread(self.query_one("#campaign_detail", Static).update,
                                      Text(f"Could not read journal: {exc}"))
            return
        self.app.call_from_thread(self._apply_selected, summary, views)

    def _apply_selected(self, summary: Dict[str, Any], views: List[StepView]) -> None:
        if not self.selected_summary or self.selected_summary["log_file"] != summary["log_file"]:
            return  # the user moved on while this was loading
        self.views, self.view_by_key = views, {v.key: v for v in views}
        failed_apps = ", ".join(summary["failed_apps"]) if summary["failed_apps"] else "(none)"
        self.query_one("#campaign_detail", Static).update(Text(
            f"Campaign: {summary.get('campaign_name') or ''} ({summary['campaign_id']}) | Started: {summary['campaign_start']}\n"
            f"Steps: {summary['completed_steps']}/{summary['total_steps']} complete, "
            f"{summary['failed_steps']} need work  |  Apps needing work: {failed_apps}\n"
            f"Journal: {summary['log_file']}"
        ))
        table = self.query_one("#campaign_steps", DataTable)
        table.clear()
        for v in views:
            table.add_row(v.app, v.step, v.cluster, status_text(v.status),
                          f"{v.jobs_completed}/{v.jobs_total}", v.error_message or "", key=v.key)

    def on_list_view_selected(self, event: ListView.Selected) -> None:
        if event.list_view.id != "campaign_history_list":
            return
        index = event.list_view.index
        if index is not None and index < len(self.summaries):
            self._select_campaign(self.summaries[index])

    def _highlighted_view(self) -> Optional[StepView]:
        try:
            table = self.query_one("#campaign_steps", DataTable)
            key = table.coordinate_to_cell_key(table.cursor_coordinate).row_key.value
        except Exception:
            return None
        return self.view_by_key.get(str(key)) if key else None

    def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        if event.data_table.id == "campaign_steps":
            view = self._highlighted_view()
            self.query_one("#view_jobs_btn", Button).disabled = not (view and view.exp_dirs)

    def _open_selected_campaign(self) -> None:
        if self.selected_summary is None:
            return
        summary = self.selected_summary
        config_file, results_dir, clusters_raw, dry_run, verbose = self._read_form()
        self.app.push_screen(
            CampaignStateScreen(
                Path(summary["log_file"]),
                config_file=Path(config_file) if config_file else None,
                results_dir=Path(results_dir) if results_dir else Path(summary["log_file"]).parent,
                clusters=parse_clusters(clusters_raw) or summary["clusters_processed"],
                dry_run=dry_run,
                verbose=verbose,
            )
        )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        config_file, results_dir, clusters_raw, dry_run, verbose = self._read_form()
        button = event.button.id

        if button == "refresh_campaigns_btn":
            DEFAULT_JOBS_CACHE.clear()
            self._load_history(refresh=True)
            self._set_status("Refreshing campaigns (querying schedulers)…")
            return
        if button == "open_campaign_btn":
            self._open_selected_campaign()
            return
        if button == "view_jobs_btn":
            open_step_jobs(self.app, self._highlighted_view())
            return

        if button == "suggest_btn":
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

        if button != "start_btn":
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
        if not clusters:
            self._set_status("Enter at least one cluster.")
            return
        try:
            self.app.push_screen(
                CampaignSelectionScreen(
                    config_file=Path(config_file), results_dir=Path(results_dir),
                    clusters=clusters, dry_run=dry_run, verbose=verbose,
                )
            )
        except CampaignRunnerError as e:
            self._set_status(f"Failed to load campaign config: {e}")


# ============================================================================
# App
# ============================================================================


class CampaignTUIApp(App):
    """Root Textual application."""

    TITLE = "Campaign Manager"
    BINDINGS = [("q", "quit", "Quit"), ("ctrl+c", "quit", "Quit")]

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
            config_file=config_file, results_dir=results_dir, clusters=clusters,
            dry_run=dry_run, verbose=verbose,
        )

    def on_mount(self) -> None:
        self.push_screen(MenuScreen(**self._initial))

    def action_quit(self) -> None:
        """Ask whether active campaign jobs should be cancelled before exit."""
        screen = self.screen
        if isinstance(screen, RunScreen) and not screen.finished:
            self.push_screen(CampaignExitScreen(), self._on_campaign_exit_choice)
            return
        self.exit()

    def _on_campaign_exit_choice(self, choice: str) -> None:
        screen = next(
            (candidate for candidate in reversed(self.screen_stack)
             if isinstance(candidate, RunScreen)),
            None,
        )
        if not isinstance(screen, RunScreen):
            return
        if choice == "cancel":
            screen.request_campaign_cancel(exit_after=True)
        elif choice == "leave":
            screen.request_campaign_detach()


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


def run_campaign_tui(
    argv: Optional[List[str]] = None,
    *,
    config_file: Optional[str] = None,
    results_dir: Optional[str] = None,
    clusters: Optional[str] = None,
    dry_run: Optional[bool] = None,
    verbose: Optional[bool] = None,
) -> int:
    """Launch the TUI. Defaults come from ``argv`` (if given), then keyword overrides."""
    options: Dict[str, Any] = {}
    if argv is not None:
        args = build_arg_parser().parse_args(argv)
        options = dict(config_file=args.config, results_dir=args.results_dir, clusters=args.clusters,
                       dry_run=args.dry_run, verbose=args.verbose)
    overrides = dict(config_file=config_file, results_dir=results_dir, clusters=clusters,
                     dry_run=dry_run, verbose=verbose)
    options.update({k: v for k, v in overrides.items() if v is not None})
    CampaignTUIApp(**options).run()
    return 0


if __name__ == "__main__":
    import sys
    raise SystemExit(run_campaign_tui(sys.argv[1:]))
