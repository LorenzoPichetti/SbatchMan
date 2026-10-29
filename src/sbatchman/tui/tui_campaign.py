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
from typing import Optional, List, Dict, Any

from rich.text import Text
from rich.markup import escape as rich_escape

from textual.app import App, ComposeResult
from textual.screen import Screen
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
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
            self.control.request_resolution(resolution)
            self.awaiting_resolution = False
            self.query_one("#pause_btn", Button).disabled = False
            for button_id in ("#retry_btn", "#continue_btn", "#skip_btn"):
                self.query_one(button_id, Button).disabled = True
            self.query_one("#log_view", RichLog).write(
                f"[cyan]{resolution.capitalize()} selected; campaign resuming…[/cyan]"
            )

        elif event.button.id == "back_btn":
            if self.finished:
                self.app.pop_screen()

    def action_try_back(self) -> None:
        if self.finished:
            self.app.pop_screen()

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
    #state_controls { height: 3; align: center middle; }
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
        with Horizontal(id="state_controls"):
            yield Button("Resume / apply edits", id="state_resume_btn", disabled=self.error is not None or self.config_file is None)
            yield Button("Re-run failed apps", id="state_rerun_btn", disabled=self.error is not None or self.config_file is None or not self._failed_apps())
            yield Button("View Step Jobs", id="state_view_jobs_btn", disabled=True)
            yield Button("Back", id="state_back_btn")
        yield Footer()

    def _failed_apps(self) -> List[str]:
        failed = []
        for app_name, app_data in self.state.get("execution", {}).items():
            if any(
                record.get("status") in (StepStatus.FAILED.value, StepStatus.CANCELLED.value)
                or any(
                    str(job.get("status", "")).upper() != "COMPLETED"
                    for job in (record.get("jobs") or [])
                )
                for step in app_data.get("steps", {}).values()
                for record in step.values()
            ):
                failed.append(app_name)
        return failed

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
        for app_name, app_data in self.state.get("execution", {}).items():
            for step_name, cluster_logs in app_data.get("steps", {}).items():
                for cluster, record in cluster_logs.items():
                    table.add_row(
                        app_name, step_name, cluster, record.get("status", "unknown"),
                        f"{record.get('duration_seconds', 0):.1f}s",
                        f"{record.get('jobs_successful', 0)}/{record.get('jobs_count', 0)}",
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
        self.app.push_screen(
            RunScreen(
                mode="resume",
                config_file=self.config_file,
                results_dir=self.results_dir,
                clusters=self.clusters or self.state.get("clusters_processed", []),
                dry_run=self.dry_run,
                verbose=self.verbose,
                resume_mode="auto",
                force_apps=self._failed_apps() if rerun_failed else [],
                log_file=self.log_file,
            )
        )

    def on_mount(self) -> None:
        if self.error:
            return
        table = self.query_one("#state_table", DataTable)
        table.add_columns("App", "Step", "Cluster", "Status", "Duration", "Jobs", "Error")
        table.cursor_type = "row"
        self._row_details = []
        for app_name, app_data in self.state.get("execution", {}).items():
            for step_name, cluster_logs in app_data.get("steps", {}).items():
                for cluster, record in cluster_logs.items():
                    table.add_row(
                        app_name, step_name, cluster, record.get("status", "unknown"),
                        f"{record.get('duration_seconds', 0):.1f}s",
                        f"{record.get('jobs_successful', 0)}/{record.get('jobs_count', 0)}",
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
                        record.get("status", "unknown"),
                        f"{record.get('jobs_successful', 0)}/{record.get('jobs_count', 0)}",
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
            self._set_status("")
            self.app.push_screen(
                RunScreen(
                    mode="new",
                    config_file=Path(config_file),
                    results_dir=Path(results_dir),
                    clusters=clusters,
                    dry_run=dry_run,
                    verbose=verbose,
                )
            )

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
