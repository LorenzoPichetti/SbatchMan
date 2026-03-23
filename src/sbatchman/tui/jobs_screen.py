from typing import List, Optional, Dict, Any
import yaml
from sbatchman import Job, jobs_list
from textual.app import ComposeResult
from textual.widgets import (
    Header,
    Footer,
    DataTable,
    TabbedContent,
    TabPane,
    Input,
    Static,
    Button,
    Label,
)
from textual.binding import Binding
from textual.screen import Screen
from textual.coordinate import Coordinate
from textual.widgets.data_table import RowDoesNotExist
from textual.containers import Container, Vertical, Horizontal, ScrollableContainer
from pathlib import Path
from datetime import datetime

from sbatchman.config.project_config import get_archive_dir, get_experiments_dir
from sbatchman.core.launcher import Status
from sbatchman.tui.log_screen import LogScreen

from sbatchman import delete_jobs, archive_job, unarchive_job


# ---------------------------------------------------------------------------
# Column registry
# ---------------------------------------------------------------------------

# Maps display name → (label, extractor_fn)
# extractor_fn receives a Job and returns the string to show in the cell.
def _fmt_ts(ts: Optional[str]) -> str:
    """Parse a raw sbatchman timestamp and return a human-readable string."""
    if not ts:
        return "N/A"
    try:
        if '.' in ts:
            base, frac = ts.split('.', 1)
            ts = f"{base}.{frac[:6]}"  # keep only microseconds
        return datetime.strptime(ts, "%Y%m%d_%H%M%S.%f").strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return ts

COLUMN_REGISTRY: Dict[str, tuple] = {
    # display_name: (header_label, extractor)
    "config":   ("Config",   lambda j: getattr(j, "config_name", "N/A") or "N/A"),
    "id":       ("Job ID",   lambda j: getattr(j, "job_id", "N/A") or "N/A"),
    "tag":      ("Tag",      lambda j: getattr(j, "tag", "N/A") or "N/A"),
    "status":   ("Status",   lambda j: getattr(j, "status", "UNKNOWN") or "UNKNOWN"),
    "command":  ("Command",  lambda j: getattr(j, "command", "") or ""),
    "queued":   ("Queued",   lambda j: _fmt_ts(getattr(j, "queued_timestamp", None))),
    "start":    ("Start",    lambda j: _fmt_ts(getattr(j, "start_timestamp", None))),
    "end":      ("End",      lambda j: _fmt_ts(getattr(j, "end_timestamp", None))),
    "runtime":  ("Runtime",  lambda j: str(j.get_run_time()) if callable(getattr(j, "get_run_time", None)) else "N/A"),
}

DEFAULT_COLUMNS = ["id", "status", "config", "tag", "command"]


# ---------------------------------------------------------------------------
# ScriptViewerScreen
# ---------------------------------------------------------------------------

class ScriptViewerScreen(Screen):
    """Screen to view job script content."""

    BINDINGS = [
        Binding("escape", "app.pop_screen", "Close"),
        Binding("q", "app.pop_screen", "Close"),
    ]

    def __init__(self, job: Job, **kwargs):
        super().__init__(**kwargs)
        self.job = job
        self.script_content = self._load_script()

    def _load_script(self) -> str:
        try:
            script_path = self.job.get_job_script_path()
            if script_path.exists():
                return script_path.read_text()
            return f"Script not found at: {script_path}"
        except Exception as e:
            return f"Error reading script: {str(e)}"

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static(
            f"Job Script: {self.job.config_name} - {self.job.tag}", id="script-title"
        )
        with ScrollableContainer(id="script-container"):
            yield Static(self.script_content, id="script-content")
        yield Footer()

    CSS = """
    #script-title {
        background: $boost;
        padding: 1;
        text-align: center;
    }
    #script-container {
        height: 1fr;
        border: solid $primary;
    }
    #script-content {
        padding: 1;
        width: 100%;
    }
    """


# ---------------------------------------------------------------------------
# ConfirmDeleteScreen
# ---------------------------------------------------------------------------

class ConfirmDeleteScreen(Screen):
    """Modal confirmation dialog before deleting a job."""

    BINDINGS = [
        Binding("escape", "cancel", "Cancel"),
        Binding("q", "cancel", "Cancel"),
    ]

    def __init__(self, job: Job, **kwargs):
        super().__init__(**kwargs)
        self.job = job

    def compose(self) -> ComposeResult:
        yield Header()
        with Vertical(id="confirm-container"):
            yield Static("⚠  Delete Job", id="confirm-title")
            yield Static(
                f"Are you sure you want to permanently delete:\n\n"
                f"  Config  : {self.job.config_name}\n"
                f"  Tag     : {self.job.tag}\n"
                f"  Job ID  : {self.job.job_id}\n"
                f"  Status  : {self.job.status}\n"
                + (f"  Archive : {self.job.archive_name}\n" if self.job.archive_name else "")
                + "\nThis action cannot be undone.",
                id="confirm-body",
            )
            with Horizontal(id="confirm-buttons"):
                yield Button("Delete", variant="error", id="confirm-yes")
                yield Button("Cancel", variant="default", id="confirm-no")
        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "confirm-yes":
            self.dismiss(True)
        else:
            self.dismiss(False)

    def action_cancel(self) -> None:
        self.dismiss(False)

    CSS = """
    #confirm-container {
        align: center middle;
        height: 100%;
    }
    #confirm-title {
        background: $error;
        color: $text;
        text-align: center;
        text-style: bold;
        padding: 1 4;
        width: 60;
    }
    #confirm-body {
        background: $panel;
        border: solid $error;
        padding: 2 4;
        width: 60;
    }
    #confirm-buttons {
        align: center middle;
        width: 60;
        margin-top: 1;
    }
    #confirm-buttons Button {
        margin: 0 2;
        width: 16;
    }
    """


# ---------------------------------------------------------------------------
# ArchivePickerScreen
# ---------------------------------------------------------------------------

class ArchivePickerScreen(Screen):
    """
    Let the user pick an existing archive or type a new archive name,
    then dismiss with the chosen name (or None to cancel).
    """

    BINDINGS = [
        Binding("escape", "cancel", "Cancel"),
        Binding("q", "cancel", "Cancel"),
    ]

    def __init__(self, job: Job, archives_dir: Path, **kwargs):
        super().__init__(**kwargs)
        self.job = job
        self.archives_dir = archives_dir
        self.existing_archives = self._get_archives()

    def _get_archives(self) -> List[str]:
        archives = []
        if self.archives_dir.exists():
            for item in sorted(self.archives_dir.iterdir()):
                if item.is_dir():
                    archives.append(item.name)
        return archives

    def compose(self) -> ComposeResult:
        yield Header()
        with Vertical(id="picker-container"):
            yield Static("Archive Job", id="picker-title")
            yield Static(
                f"Config: {self.job.config_name}  |  Tag: {self.job.tag}  |  ID: {self.job.job_id}",
                id="picker-job-info",
            )

            if self.existing_archives:
                yield Label("Select an existing archive:", classes="picker-label")
                with ScrollableContainer(id="archives-scroll"):
                    for name in self.existing_archives:
                        yield Button(name, id=f"archive-pick-{name}", classes="archive-pick-btn")
            else:
                yield Static("No existing archives found.", id="no-archives-msg")

            yield Label("— or create a new archive —", classes="picker-label", id="new-label")
            yield Input(placeholder="New archive name…", id="new-archive-input")
            with Horizontal(id="picker-buttons"):
                yield Button("Archive", variant="primary", id="picker-confirm")
                yield Button("Cancel", variant="default", id="picker-cancel")
        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "picker-cancel":
            self.dismiss(None)
        elif event.button.id == "picker-confirm":
            name = self.query_one("#new-archive-input", Input).value.strip()
            if name:
                self.dismiss(name)
            else:
                self.query_one("#new-archive-input", Input).focus()
        elif event.button.id and event.button.id.startswith("archive-pick-"):
            name = event.button.id.replace("archive-pick-", "")
            self.dismiss(name)

    def action_cancel(self) -> None:
        self.dismiss(None)

    CSS = """
    #picker-container {
        align: center middle;
        height: 100%;
    }
    #picker-title {
        background: $boost;
        color: $text;
        text-align: center;
        text-style: bold;
        padding: 1 4;
        width: 70;
    }
    #picker-job-info {
        text-align: center;
        color: $text-muted;
        padding: 0 4;
        width: 70;
    }
    .picker-label {
        margin-top: 1;
        color: $text-muted;
        text-style: bold;
        width: 70;
    }
    #new-label {
        text-align: center;
        margin-top: 1;
    }
    #archives-scroll {
        width: 70;
        max-height: 10;
        border: solid $primary;
        background: $panel;
    }
    .archive-pick-btn {
        width: 100%;
        padding: 0;
        margin: 0;
    }
    #new-archive-input {
        width: 70;
    }
    #picker-buttons {
        align: center middle;
        width: 70;
        margin-top: 1;
    }
    #picker-buttons Button {
        margin: 0 2;
        width: 16;
    }
    #no-archives-msg {
        color: $text-muted;
        text-style: italic;
        width: 70;
        text-align: center;
    }
    """


# ---------------------------------------------------------------------------
# FilterScreen
# ---------------------------------------------------------------------------

class FilterScreen(Screen):
    """Interactive filter configuration screen."""

    BINDINGS = [
        Binding("escape", "cancel", "Cancel"),
        Binding("ctrl+s", "apply", "Apply Filters"),
    ]

    STATUS_OPTIONS = [
        "SUBMITTING",
        "FAILED_SUBMISSION",
        "QUEUED",
        "RUNNING",
        "COMPLETED",
        "FAILED",
        "CANCELLED",
        "TIMEOUT",
        "OTHER",
        "UNKNOWN",
    ]

    def __init__(self, current_filters: Optional[Dict[str, str]] = None, **kwargs):
        super().__init__(**kwargs)
        self.filters = current_filters or {}
        self.selected_statuses = set()
        if "status" in self.filters:
            self.selected_statuses = set(
                s.strip().upper() for s in self.filters["status"].split(",")
            )
        self.variable_filters = {}
        if "variables" in self.filters:
            try:
                for var_filter in self.filters["variables"].split(";"):
                    if ":" in var_filter:
                        var_name, values = var_filter.split(":", 1)
                        self.variable_filters[var_name.strip()] = values.strip()
            except Exception:
                pass
        self.next_var_index = len(self.variable_filters)

    def compose(self) -> ComposeResult:
        yield Header()
        with Vertical():
            with Horizontal(id="filter-main-container"):
                with Vertical(id="left-column"):
                    yield Label("Status (select multiple):", classes="section-label")
                    for status in self.STATUS_OPTIONS:
                        is_selected = status in self.selected_statuses
                        yield Button(
                            f"{'✓' if is_selected else '☐'} {status}",
                            id=f"status-{status}",
                            variant="primary" if is_selected else "default",
                            classes="checkbox-btn",
                            compact=True,
                        )
                    yield Label("Config Name (comma-separated):", classes="section-label")
                    yield Input(value=self.filters.get("config", ""), placeholder="e.g., config_a, config_b", id="config-input")
                    yield Label("Tag (comma-separated):", classes="section-label")
                    yield Input(value=self.filters.get("tag", ""), placeholder="e.g., exp_1, test", id="tag-input")
                    yield Label("Start Time (after, format: %Y-%m-%d %H:%M:%S):", classes="section-label")
                    yield Input(value=self.filters.get("time__gt", ""), placeholder="e.g., 2024-01-01 01:02:00", id="time-gt-input")
                    yield Label("End Time (before, format: %Y-%m-%d %H:%M:%S):", classes="section-label")
                    yield Input(value=self.filters.get("time__lt", ""), placeholder="e.g., 2024-12-31 12:00:10", id="time-lt-input")

                with Vertical(id="right-column"):
                    yield Label("Variable Filters:", classes="section-label")
                    yield Static("Filter by job variables. Click + to add rows.", classes="help-text")
                    yield Button("+ Add Variable Filter", id="add-var-btn", variant="success", compact=True)
                    with Vertical(id="variable-filters-container"):
                        for i, (var_name, values) in enumerate(self.variable_filters.items()):
                            with Horizontal(classes="variable-filter-row"):
                                yield Input(value=var_name, placeholder="Variable", id=f"var-name-{i}", classes="var-name-input", compact=True)
                                yield Static("=", classes="equals-sign")
                                yield Input(value=values, placeholder="values (comma-separated)", id=f"var-values-{i}", classes="var-values-input", compact=True)
                                yield Button("✕", id=f"remove-var-{i}", classes="remove-btn", variant="error", compact=True)

            with Horizontal(id="button-container"):
                yield Button("Apply Filters", variant="primary", id="apply-btn")
                yield Button("Clear All", variant="default", id="clear-btn")
                yield Button("Cancel", variant="default", id="cancel-btn")
        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "apply-btn":
            self.action_apply()
        elif event.button.id == "clear-btn":
            self.action_clear()
        elif event.button.id == "cancel-btn":
            self.action_cancel()
        elif event.button.id == "add-var-btn":
            self._add_variable_row()
        elif event.button.id and event.button.id.startswith("status-"):
            status = event.button.id.replace("status-", "")
            self._toggle_status(status, event.button)
        elif event.button.id and event.button.id.startswith("remove-var-"):
            self._remove_variable_row(event.button.id)

    def _toggle_status(self, status: str, button: Button) -> None:
        if status in self.selected_statuses:
            self.selected_statuses.remove(status)
            button.label = f"☐ {status}"
            button.variant = "default"
        else:
            self.selected_statuses.add(status)
            button.label = f"✓ {status}"
            button.variant = "primary"

    def _add_variable_row(self) -> None:
        container = self.query_one("#variable-filters-container", Vertical)
        index = self.next_var_index
        self.next_var_index += 1

        async def do_mount():
            new_row = Horizontal(classes="variable-filter-row")
            await container.mount(new_row)
            await new_row.mount(
                Input(placeholder="Variable", id=f"var-name-{index}", classes="var-name-input", compact=True),
                Static("=", classes="equals-sign"),
                Input(placeholder="values (comma-separated)", id=f"var-values-{index}", classes="var-values-input", compact=True),
                Button("✕", id=f"remove-var-{index}", classes="remove-btn", variant="error", compact=True),
            )

        self.call_after_refresh(do_mount)

    def _remove_variable_row(self, button_id: str) -> None:
        try:
            index = button_id.replace("remove-var-", "")
            name_input = self.query_one(f"#var-name-{index}", Input)
            name_input.parent.remove()
        except Exception:
            pass

    def action_apply(self) -> None:
        filters = {}
        if self.selected_statuses:
            filters["status"] = ",".join(sorted(self.selected_statuses))
        config = self.query_one("#config-input", Input).value.strip()
        if config:
            filters["config"] = config
        tag = self.query_one("#tag-input", Input).value.strip()
        if tag:
            filters["tag"] = tag
        time_gt = self.query_one("#time-gt-input", Input).value.strip()
        if time_gt:
            filters["time__gt"] = time_gt
        time_lt = self.query_one("#time-lt-input", Input).value.strip()
        if time_lt:
            filters["time__lt"] = time_lt
        var_filters = []
        container = self.query_one("#variable-filters-container", Vertical)
        for row in container.children:
            if isinstance(row, Horizontal):
                try:
                    inputs = list(row.query(Input))
                    if len(inputs) >= 2:
                        var_name = inputs[0].value.strip()
                        var_values = inputs[1].value.strip()
                        if var_name and var_values:
                            var_filters.append(f"{var_name}:{var_values}")
                except Exception:
                    pass
        if var_filters:
            filters["variables"] = ";".join(var_filters)
        self.dismiss(filters)

    def action_clear(self) -> None:
        self.selected_statuses.clear()
        for status in self.STATUS_OPTIONS:
            try:
                button = self.query_one(f"#status-{status}", Button)
                button.label = f"☐ {status}"
                button.variant = "default"
            except Exception:
                pass
        for input_id in ["#config-input", "#tag-input", "#time-gt-input", "#time-lt-input"]:
            try:
                self.query_one(input_id, Input).value = ""
            except Exception:
                pass
        container = self.query_one("#variable-filters-container", Vertical)
        for row in list(container.children):
            if isinstance(row, Horizontal):
                row.remove()
        self.next_var_index = 0

    def action_cancel(self) -> None:
        self.dismiss(None)

    CSS = """    
    #filter-main-container {
        padding: 0;
        margin: 0;
        height: 90%;
    }
    #left-column {
        width: 50%;
        padding-right: 1;
    }
    #right-column {
        width: 50%;
        padding-left: 1;
    }
    .section-label {
        margin-top: 1;
        margin-bottom: 0;
        color: $text-muted;
        text-style: bold;
    }
    .help-text {
        color: $text-muted;
        margin-bottom: 1;
        text-style: italic;
    }
    Input {
        margin-bottom: 0;
    }
    .checkbox-btn {
        width: 100%;
        height: auto;
        padding: 0;
        margin: 0;
        text-align: left;
    }
    #add-var-btn {
        width: 100%;
        margin-bottom: 0;
    }
    #variable-filters-container {
        padding: 0;
        height: 85%;
        border: solid $primary;
        background: $panel;
    }
    .variable-filter-row {
        margin-bottom: 0;
        align: left middle;
    }
    .var-name-input {
        width: 35%;
    }
    .var-values-input {
        width: 1fr;
    }
    .equals-sign {
        width: 2;
        text-align: center;
        margin-left: 0;
    }
    .remove-btn {
        width: 3;
        min-width: 3;
        height: 1;
    }    
    #button-container {
        margin-top: 0;
        align: center middle;
        padding: 0;
    }
    #button-container Button {
        margin-left: 4;
        margin-right: 4;
        padding: 0;
    }
    """


# ---------------------------------------------------------------------------
# ArchiveScreen
# ---------------------------------------------------------------------------

class ArchiveScreen(Screen):
    """Screen to select which archives to display."""

    BINDINGS = [
        Binding("escape", "cancel", "Cancel"),
        Binding("ctrl+s", "apply", "Apply Selection"),
    ]

    def __init__(self, archives_dir: Path, current_selection: Dict[str, bool], **kwargs):
        super().__init__(**kwargs)
        self.archives_dir = archives_dir
        self.selection = current_selection.copy()
        self.available_archives = self._get_available_archives()

    def _get_available_archives(self) -> List[str]:
        archives = []
        if self.archives_dir.exists():
            for item in self.archives_dir.iterdir():
                if item.is_dir():
                    archives.append(item.name)
        return sorted(archives)

    def compose(self) -> ComposeResult:
        yield Header()
        with ScrollableContainer():
            yield Label("Select Job Sources", id="archive-title")
            with Vertical(id="archive-container"):
                yield Label("Active Jobs:", id="active-title")
                yield Button(
                    "✓ Active Jobs" if self.selection.get("active", True) else "☐ Active Jobs",
                    id="active-toggle",
                    variant="primary" if self.selection.get("active", True) else "default",
                )
                if self.available_archives:
                    yield Label("Archives:", id="archives-label")
                    for archive in self.available_archives:
                        is_selected = self.selection.get(f"archive:{archive}", False)
                        yield Button(
                            f"✓ {archive}" if is_selected else f"☐ {archive}",
                            id=f"archive-{archive}",
                            variant="primary" if is_selected else "default",
                            classes="archive-btn",
                        )
                else:
                    yield Label("No archives found", id="no-archives")
                with Horizontal(id="button-container"):
                    yield Button("Apply", variant="success", id="apply-btn")
                    yield Button("Cancel", variant="default", id="cancel-btn")
        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "apply-btn":
            self.action_apply()
        elif event.button.id == "cancel-btn":
            self.action_cancel()
        elif event.button.id == "active-toggle":
            self._toggle_active()
        elif event.button.id and event.button.id.startswith("archive-"):
            archive_name = event.button.id.replace("archive-", "")
            self._toggle_archive(archive_name, event.button)

    def _toggle_active(self) -> None:
        current = self.selection.get("active", True)
        self.selection["active"] = not current
        button = self.query_one("#active-toggle", Button)
        button.label = "✓ Active Jobs" if self.selection["active"] else "☐ Active Jobs"
        button.variant = "primary" if self.selection["active"] else "default"

    def _toggle_archive(self, archive_name: str, button: Button) -> None:
        key = f"archive:{archive_name}"
        current = self.selection.get(key, False)
        self.selection[key] = not current
        button.label = f"✓ {archive_name}" if self.selection[key] else f"☐ {archive_name}"
        button.variant = "primary" if self.selection[key] else "default"

    def action_apply(self) -> None:
        self.dismiss(self.selection)

    def action_cancel(self) -> None:
        self.dismiss(None)

    CSS = """
    #archive-title {
        text-align: center;
        background: $boost;
        padding: 1
    }
    #archive-container {
        padding: 1 2;
    }
    #archive-container Label {
        margin-top: 1;
        color: $text-muted;
    }
    #archive-title,
    #archives-label {
        text-style: bold;
    }
    #archives-label {
        margin-top: 1;
    }
    #archive-container Button {
        width: 100%;
        padding: 0;
        margin: 0;
        align: center middle;
    }
    #button-container {
        align: center middle;
    }
    #button-container Button {
        width: 40%;
        align: center middle;
    }
    """


# ---------------------------------------------------------------------------
# JobsScreen
# ---------------------------------------------------------------------------

class JobsScreen(Screen):
    """The main screen with job tables."""

    all_jobs: List[Job]
    filter: Optional[Dict[str, str]]

    BINDINGS = [
        Binding("q", "app.quit", "Quit"),
        Binding("r", "refresh_jobs", "Refresh"),
        Binding("f", "open_filter", "Filter"),
        Binding("a", "open_archives", "Archives"),
        Binding("c", "clear_filter", "Clear Filter"),
        Binding("enter", "select_cursor", "View Logs", priority=True),
        Binding("s", "view_script", "View Script"),
        Binding("x", "delete_job", "Delete Job"),
        Binding("v", "archive_job", "Archive Job"),
        Binding("u", "unarchive_job", "Unarchive Job"),
    ]

    CSS = """
    DataTable {
        height: 1fr;
    }
    #filter-status {
        background: $boost;
        padding: 0 1;
        color: $text;
        text-align: center;
    }
    """

    def __init__(
        self,
        experiments_dir: Optional[Path] = None,
        columns: Optional[List[str]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.experiments_root = experiments_dir or get_experiments_dir()
        self.all_jobs = []
        self.filter = None
        self.archive_selection = {"active": True}
        self.archives_dir = get_archive_dir()
        self._is_loading = False
        self._needs_refresh = False

        # Validate and store column spec
        requested = columns if columns is not None else list(DEFAULT_COLUMNS)
        unknown = [c for c in requested if c not in COLUMN_REGISTRY]
        if unknown:
            raise ValueError(
                f"Unknown column(s): {unknown}. "
                f"Valid options are: {list(COLUMN_REGISTRY)}"
            )
        self._columns: List[str] = requested
        self._sort_key: str = self._columns[0]

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static("", id="filter-status")
        with TabbedContent(id="tabs"):
            with TabPane("Ended", id="finished-tab"):
                yield DataTable(id="finished-table")
            with TabPane("Running", id="running-tab"):
                yield DataTable(id="running-table")
            with TabPane("Queued", id="queued-tab"):
                yield DataTable(id="queued-table")
        yield Footer()

    def on_mount(self) -> None:
        for table_id in ["#queued-table", "#running-table", "#finished-table"]:
            table = self.query_one(table_id, DataTable)
            table.cursor_type = "row"
            for col in self._columns:
                label, _ = COLUMN_REGISTRY[col]
                # Use the display-name as the key so sort can reference it
                table.add_column(label, key=col)
        self.load_and_update_jobs()
        self.timer = self.set_interval(90, self.load_and_update_jobs, pause=False)

    def on_screen_resume(self) -> None:
        if hasattr(self, "timer"):
            self.timer.resume()

    def on_screen_suspend(self) -> None:
        if hasattr(self, "timer"):
            self.timer.pause()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_selected_finished_job(self) -> Optional[Job]:
        """Return the Job currently selected in the Ended table, or None."""
        active_tab_id = self.query_one(TabbedContent).active
        if active_tab_id != "finished-tab":
            return None
        table = self.query_one("#finished-table", DataTable)
        if table.row_count == 0:
            return None
        try:
            coord = table.cursor_coordinate
            exp_dir_str = table.coordinate_to_cell_key(coord).row_key.value or ""
            return self._get_job_by_exp_dir(exp_dir_str)
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Delete action
    # ------------------------------------------------------------------

    def action_delete_job(self) -> None:
        """Prompt the user and delete the selected finished job."""
        job = self._get_selected_finished_job()
        if job is None:
            return

        def handle_confirm(confirmed: bool) -> None:
            if not confirmed:
                return
            try:
                if job.job_id:
                    delete_jobs(id=int(job.job_id), archived=True, not_archived=True)
                else:
                    import shutil
                    exp_path = self.experiments_root / job.exp_dir
                    if exp_path.exists():
                        shutil.rmtree(exp_path)
                self.load_and_update_jobs()
            except Exception as exc:
                self.notify(f"Delete failed: {exc}", severity="error")

        self.app.push_screen(ConfirmDeleteScreen(job=job), handle_confirm)

    # ------------------------------------------------------------------
    # Archive action  (active jobs only)
    # ------------------------------------------------------------------

    def action_archive_job(self) -> None:
        """Open archive picker and move the selected active finished job to an archive."""
        job = self._get_selected_finished_job()
        if job is None:
            return

        if job.archive_name:
            self.notify(
                f"Job is already in archive '{job.archive_name}'. Use 'u' to unarchive it first.",
                severity="warning",
            )
            return

        def handle_archive_name(archive_name: Optional[str]) -> None:
            if not archive_name:
                return
            try:
                archive_job(job, archive_name)
                self.load_and_update_jobs()
                self.notify(f"Job archived to '{archive_name}'.", severity="information")
            except Exception as exc:
                self.notify(f"Archive failed: {exc}", severity="error")

        self.app.push_screen(
            ArchivePickerScreen(job=job, archives_dir=self.archives_dir),
            handle_archive_name,
        )

    # ------------------------------------------------------------------
    # Unarchive action  (archived jobs only)
    # ------------------------------------------------------------------

    def action_unarchive_job(self) -> None:
        """Move the selected archived finished job back to active."""
        job = self._get_selected_finished_job()
        if job is None:
            return

        if not job.archive_name:
            self.notify(
                "Job is not archived. Use 'v' to archive it.",
                severity="warning",
            )
            return

        try:
            archive_name = job.archive_name  # capture before unarchive_job clears it
            unarchive_job(job)
            self.load_and_update_jobs()
            self.notify(f"Job restored from archive '{archive_name}'.", severity="information")
        except Exception as exc:
            self.notify(f"Unarchive failed: {exc}", severity="error")

    # ------------------------------------------------------------------
    # Existing actions
    # ------------------------------------------------------------------

    def action_open_filter(self) -> None:
        def handle_filter_result(filters: Optional[Dict[str, str]]) -> None:
            if filters is not None:
                self.filter = filters if filters else None
                self.update_filter_status()
                self.update_tables()
        self.app.push_screen(FilterScreen(self.filter), handle_filter_result)

    def action_open_archives(self) -> None:
        def handle_archive_result(selection: Optional[Dict[str, bool]]) -> None:
            if selection is not None:
                self.archive_selection = selection
                self.load_and_update_jobs()
        self.app.push_screen(
            ArchiveScreen(self.archives_dir, self.archive_selection),
            handle_archive_result,
        )

    def action_clear_filter(self) -> None:
        self.filter = None
        self.update_filter_status()
        self.update_tables()

    def update_filter_status(self) -> None:
        status_widget = self.query_one("#filter-status", Static)
        if self.filter:
            filter_parts = []
            for k, v in self.filter.items():
                if k in ("status", "config", "tag"):
                    filter_parts.append(f"{k}={v}")
                elif k == "variables":
                    var_parts = []
                    for var_filter in v.split(";"):
                        if ":" in var_filter:
                            var_name, values = var_filter.split(":", 1)
                            var_parts.append(f"{var_name}={values}")
                    if var_parts:
                        filter_parts.append(f"vars({', '.join(var_parts)})")
                elif k == "time__gt":
                    filter_parts.append(f"after {v}")
                elif k == "time__lt":
                    filter_parts.append(f"before {v}")
            status_widget.update(f"🔍 Active Filters: {', '.join(filter_parts)}")
        else:
            status_widget.update("")

    def load_and_update_jobs(self) -> None:
        if self._is_loading:
            self._needs_refresh = True
            return
        self._is_loading = True
        try:
            new_jobs = []
            from_active = self.archive_selection.get("active", True)
            selected_archives = [
                key.replace("archive:", "")
                for key, value in self.archive_selection.items()
                if key.startswith("archive:") and value
            ]
            if from_active:
                try:
                    new_jobs.extend(jobs_list(from_active=True, from_archived=False, update_jobs=True))
                except Exception:
                    pass
            for archive_name in selected_archives:
                try:
                    new_jobs.extend(
                        jobs_list(archive_name=archive_name, from_active=False, from_archived=True, update_jobs=False)
                    )
                except Exception:
                    pass
            for j in new_jobs:
                if j.status == Status.FAILED.value and j.exitcode:
                    j.status += f"({j.exitcode})"
            self.all_jobs = new_jobs
            self.update_tables()
        finally:
            self._is_loading = False
            if self._needs_refresh:
                self._needs_refresh = False
                self.call_later(self.load_and_update_jobs)

    def update_tables(self):
        tables = {
            "queued-table": self.query_one("#queued-table", DataTable),
            "running-table": self.query_one("#running-table", DataTable),
            "finished-table": self.query_one("#finished-table", DataTable),
        }
        job_list = self.filter_jobs(self.filter) if self.filter else self.all_jobs
        queued_jobs, running_jobs, finished_jobs = [], [], []
        for job in job_list:
            if not job.exp_dir:
                continue
            status = getattr(job, "status", None)
            if status in [Status.SUBMITTING.value, Status.QUEUED.value]:
                queued_jobs.append(job)
            elif status == Status.RUNNING.value:
                running_jobs.append(job)
            else:
                finished_jobs.append(job)
        self._update_table_with_jobs(tables["queued-table"], queued_jobs)
        self._update_table_with_jobs(tables["running-table"], running_jobs)
        self._update_table_with_jobs(tables["finished-table"], finished_jobs)

    def _update_table_with_jobs(self, table: DataTable, jobs: List[Job]) -> None:
        job_map = {job.get_stdout_path().parent: job for job in jobs if job.exp_dir}
        current_keys = set(job_map.keys())
        existing_keys = set(table.rows.keys())
        for key in existing_keys - current_keys:
            try:
                table.remove_row(key)
            except RowDoesNotExist:
                pass
        for exp_dir, job in job_map.items():
            row_data = tuple(
                COLUMN_REGISTRY[col][1](job) for col in self._columns
            )
            if exp_dir in existing_keys:
                try:
                    row_index = table.get_row_index(exp_dir)
                    for i, cell_value in enumerate(row_data):
                        table.update_cell_at(Coordinate(row_index, i), cell_value)
                except RowDoesNotExist:
                    table.add_row(*row_data, key=exp_dir)
            else:
                table.add_row(*row_data, key=exp_dir)
        if table.row_count > 0:
            try:
                table.sort(self._sort_key, reverse=True)
            except Exception:
                pass

    async def action_refresh_jobs(self) -> None:
        self.load_and_update_jobs()

    def action_select_cursor(self) -> None:
        active_tab_id = self.query_one(TabbedContent).active
        if not active_tab_id:
            return
        table_id = f"#{active_tab_id.replace('tab', 'table')}"
        active_table = self.query_one(table_id, DataTable)
        if active_table.row_count > 0:
            coord = active_table.cursor_coordinate
            try:
                exp_dir_str = active_table.coordinate_to_cell_key(coord).row_key.value or ""
                job = self._get_job_by_exp_dir(exp_dir_str)
                if job:
                    self.app.push_screen(LogScreen(job=job))
            except (RowDoesNotExist, Exception):
                pass

    def action_view_script(self) -> None:
        active_tab_id = self.query_one(TabbedContent).active
        if not active_tab_id:
            return
        table_id = f"#{active_tab_id.replace('tab', 'table')}"
        active_table = self.query_one(table_id, DataTable)
        if active_table.row_count > 0:
            coord = active_table.cursor_coordinate
            try:
                exp_dir_str = active_table.coordinate_to_cell_key(coord).row_key.value or ""
                job = self._get_job_by_exp_dir(exp_dir_str)
                if job:
                    self.app.push_screen(ScriptViewerScreen(job=job))
            except (RowDoesNotExist, Exception):
                pass

    def _get_job_by_exp_dir(self, exp_dir: str) -> Optional[Job]:
        try:
            metadata_path = self.experiments_root / exp_dir / "metadata.yaml"
            if metadata_path.exists():
                return Job(**yaml.safe_load(open(metadata_path, "r")))
        except Exception:
            pass
        return None

    def filter_jobs(self, filters: Optional[Dict[str, str]]) -> List[Job]:
        if not filters:
            return self.all_jobs

        def match(job: Job) -> bool:
            for key, val in filters.items():
                if key == "status":
                    allowed_statuses = [s.strip().upper() for s in val.split(",")]
                    job_status = getattr(job, "status", "").upper()
                    if not any(status in job_status for status in allowed_statuses):
                        return False
                elif key == "config":
                    allowed_configs = [c.strip().lower() for c in val.split(",")]
                    job_config = getattr(job, "config_name", "").lower()
                    if not any(config in job_config for config in allowed_configs):
                        return False
                elif key == "tag":
                    allowed_tags = [t.strip().lower() for t in val.split(",")]
                    job_tag = getattr(job, "tag", "").lower()
                    if not any(tag in job_tag for tag in allowed_tags):
                        return False
                elif key == "variables":
                    job_vars = getattr(job, "variables", {})
                    if not isinstance(job_vars, dict):
                        return False
                    try:
                        for var_filter in val.split(";"):
                            if ":" in var_filter:
                                var_name, allowed_values_str = var_filter.split(":", 1)
                                var_name = var_name.strip()
                                allowed_values = [v.strip() for v in allowed_values_str.split(",")]
                                if var_name not in job_vars:
                                    return False
                                if str(job_vars[var_name]) not in allowed_values:
                                    return False
                    except Exception:
                        return False
                elif key == "time__gt":
                    try:
                        job_time = datetime.strptime(job.queued_timestamp, "%Y%m%d_%H%M%S.%f")
                        val_time = datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
                        if not (job_time > val_time):
                            return False
                    except Exception:
                        return False
                elif key == "time__lt":
                    try:
                        job_time = datetime.strptime(job.queued_timestamp, "%Y%m%d_%H%M%S.%f")
                        val_time = datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
                        if not (job_time < val_time):
                            return False
                    except Exception:
                        return False
            return True

        return [job for job in self.all_jobs if match(job)]