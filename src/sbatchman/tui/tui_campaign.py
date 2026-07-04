"""
campaign_tui.py
================

Interactive terminal UI (Textual) for managing and monitoring benchmark
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
- MenuScreen: configure and launch a new campaign, or jump to history.
- HistoryScreen: browse previous campaign_log_*.json files in the results
  directory; resume (skip completed) or re-run only failed apps.
- RunScreen: live table of app/step/cluster status, a job-progress bar,
  a streaming log panel, and Pause / Cancel / Back controls. The campaign
  runs in a background thread; a queue.Queue carries structured events
  from that thread into the UI.
"""

from __future__ import annotations

import argparse
import queue
import sys
from pathlib import Path
from typing import Optional, List, Dict, Any

from rich.text import Text

from textual.app import App, ComposeResult
from textual.screen import Screen
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
}


def status_text(status: str) -> Text:
    return Text(status, style=STATUS_STYLES.get(status, ""))


def parse_clusters(raw: str) -> List[str]:
    return [c.strip() for c in raw.split(",") if c.strip()]


# ============================================================================
# Run Screen
# ============================================================================


class RunScreen(Screen):
    """Live monitoring + control screen for a single campaign run."""

    CSS = """
    #status_table { height: 1fr; }
    #log_view { height: 12; border: solid $accent; }
    #progress_label { height: 1; }
    #controls { height: 3; align: center middle; }
    """

    BINDINGS = [("escape", "try_back", "Back (when finished)")]

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
        self.clusters = clusters
        self.dry_run = dry_run
        self.verbose = verbose
        self.resume_mode = resume_mode
        self.force_apps = force_apps or []
        self.log_file = Path(log_file) if log_file else None

        self.event_queue: "queue.Queue[CampaignEvent]" = queue.Queue()
        self.control = CampaignControl()
        self.success: Optional[bool] = None
        self.finished = False

        # row key -> row data
        self.rows: Dict[str, Dict[str, Any]] = {}
        self.row_order: List[str] = []

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
            yield Button("Cancel", id="cancel_btn", variant="error")
            yield Button("Back", id="back_btn", disabled=True)
        yield Footer()

    def _header_text(self) -> str:
        mode_label = "Resuming" if self.mode == "resume" else "Starting"
        return (
            f"[b]{mode_label} campaign[/b]  "
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
            self.query_one("#cancel_btn", Button).disabled = True
            self.query_one("#back_btn", Button).disabled = False
            log = self.query_one("#log_view", RichLog)
            if self.success:
                log.write("[bold green]Campaign finished successfully.[/bold green]")
            else:
                log.write("[bold red]Campaign finished with failures (or was cancelled).[/bold red]")

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
                }.get(level, "white")
                log.write(f"[{color}]{message}[/{color}]")

            elif event.type == EventType.CAMPAIGN_START:
                log.write("[bold]Campaign started.[/bold]")

            elif event.type == EventType.CLUSTER_START:
                log.write(f"[bold magenta]== Cluster: {event.data['cluster']} ==[/bold magenta]")

            elif event.type == EventType.APP_START:
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
                    table_dirty = True

            elif event.type == EventType.STEP_END:
                key = self._row_key(event.data)
                self._ensure_row(key, event.data, status=event.data.get("status", "completed"))
                duration = event.data.get("duration")
                if duration is not None and key in self.rows:
                    self.rows[key]["duration"] = f"{duration:.1f}s"
                table_dirty = True

            elif event.type == EventType.PAUSED:
                self.query_one("#pause_btn", Button).label = "Resume"
                log.write("[yellow]Paused.[/yellow]")

            elif event.type == EventType.RESUMED:
                self.query_one("#pause_btn", Button).label = "Pause"
                log.write("[green]Resumed.[/green]")

            elif event.type == EventType.CAMPAIGN_CANCELLED:
                log.write(f"[bold magenta]Cancelled: {event.data.get('message')}[/bold magenta]")

            elif event.type == EventType.CAMPAIGN_END:
                pass  # handled via on_worker_state_changed as well

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
            }
            self.row_order.append(key)
        else:
            self.rows[key]["status"] = status

    def _refresh_table(self) -> None:
        table = self.query_one("#status_table", DataTable)
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
            self.control.request_cancel()
            event.button.disabled = True
            self.query_one("#pause_btn", Button).disabled = True
            self.query_one("#log_view", RichLog).write("[bold magenta]Cancellation requested…[/bold magenta]")

        elif event.button.id == "back_btn":
            if self.finished:
                self.app.pop_screen()

    def action_try_back(self) -> None:
        if self.finished:
            self.app.pop_screen()


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
            yield Button("Resume (skip completed)", id="resume_btn", disabled=True)
            yield Button("Re-run failed apps only", id="rerun_failed_btn", disabled=True)
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
        detail.update(
            f"Campaign: {s['campaign_id']}\n"
            f"Started: {s['campaign_start']}   Ended: {s['campaign_end']}\n"
            f"Steps: {s['completed_steps']} completed / {s['failed_steps']} failed / {s['total_steps']} total\n"
            f"Failed apps: {failed_apps}\n"
            f"Log file: {s['log_file']}"
        )

        self.query_one("#resume_btn", Button).disabled = False
        self.query_one("#rerun_failed_btn", Button).disabled = not s["failed_apps"]

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "back_btn":
            self.app.pop_screen()
            return

        if self.selected is None:
            return

        if event.button.id == "resume_btn":
            self.app.push_screen(
                RunScreen(
                    mode="resume",
                    config_file=self.config_file,
                    results_dir=self.results_dir,
                    clusters=self.clusters or self.selected["clusters_processed"],
                    dry_run=self.dry_run,
                    verbose=self.verbose,
                    resume_mode="auto",
                    force_apps=[],
                    log_file=Path(self.selected["log_file"]),
                )
            )

        elif event.button.id == "rerun_failed_btn":
            self.app.push_screen(
                RunScreen(
                    mode="resume",
                    config_file=self.config_file,
                    results_dir=self.results_dir,
                    clusters=self.clusters or self.selected["clusters_processed"],
                    dry_run=self.dry_run,
                    verbose=self.verbose,
                    resume_mode="auto",
                    force_apps=self.selected["failed_apps"],
                    log_file=Path(self.selected["log_file"]),
                )
            )


# ============================================================================
# Menu Screen
# ============================================================================


class MenuScreen(Screen):
    """Landing screen: configure and launch a new campaign, or browse history."""

    CSS = """
    #form { padding: 1 2; height: auto; }
    #form Input { margin-bottom: 1; }
    .field_label { margin-top: 1; }
    #status_msg { color: $error; height: auto; }
    #menu_controls { height: 3; align: center middle; margin-top: 1; }
    """

    def __init__(
        self,
        *,
        config_file: str = "",
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

    def compose(self) -> ComposeResult:
        yield Header()
        with VerticalScroll(id="form"):
            yield Static("[b]Campaign configuration[/b]")

            yield Label("Config YAML file", classes="field_label")
            yield Input(value=self._config_file, placeholder="campaign.yaml", id="config_input")

            yield Label("Results directory", classes="field_label")
            yield Input(value=self._results_dir, placeholder="./campaign_results", id="results_input")

            yield Label("Clusters (comma-separated)", classes="field_label")
            yield Input(value=self._clusters, placeholder="cluster1,cluster2", id="clusters_input")

            with Horizontal():
                yield Label("Dry run")
                yield Switch(value=self._dry_run, id="dry_run_switch")
            with Horizontal():
                yield Label("Verbose logging")
                yield Switch(value=self._verbose, id="verbose_switch")

            yield Static("", id="status_msg")

        with Horizontal(id="menu_controls"):
            yield Button("Suggest clusters from config", id="suggest_btn")
            yield Button("Start new campaign", id="start_btn", variant="success")
            yield Button("Resume / history", id="history_btn")
        yield Footer()

    def _read_form(self):
        config_file = self.query_one("#config_input", Input).value.strip()
        results_dir = self.query_one("#results_input", Input).value.strip()
        clusters_raw = self.query_one("#clusters_input", Input).value.strip()
        dry_run = self.query_one("#dry_run_switch", Switch).value
        verbose = self.query_one("#verbose_switch", Switch).value
        return config_file, results_dir, clusters_raw, dry_run, verbose

    def _set_status(self, message: str) -> None:
        self.query_one("#status_msg", Static).update(message)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        config_file, results_dir, clusters_raw, dry_run, verbose = self._read_form()

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

        elif event.button.id == "history_btn":
            self._set_status("")
            self.app.push_screen(
                HistoryScreen(
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
        config_file: str = "",
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


# ============================================================================
# CLI entry point
# ============================================================================


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Interactive TUI for managing benchmark campaigns.")
    parser.add_argument("--config", default="", help="Path to campaign YAML config file")
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
