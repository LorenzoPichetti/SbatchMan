"""
tui_remote.py – Textual TUI for managing sbatchman cluster configs.

Launch with:
    sbatchman config   (or call `run_config_tui()` directly)

Keybindings
-----------
a   Add a new cluster
d   Delete the selected cluster
e   Edit the selected cluster
x   Edit global common_excludes
o   Open the raw config file in $EDITOR
r   Reload config from disk
q   Quit

Each cluster form has two tabs:
  • Fetch dirs  – remote → local pull pairs   (clusters.fetch_dirs)
  • Sync dirs   – local  → remote push pairs  (clusters.sync_dirs)

The global common_excludes list is edited in a dedicated modal (key x).
Per-cluster excludes are edited inside the cluster form.
"""

from __future__ import annotations

import os
import subprocess
from copy import deepcopy
from pathlib import Path
from typing import Optional

from textual import on
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, ScrollableContainer, Vertical
from textual.screen import ModalScreen
from textual.widgets import (
    Button,
    DataTable,
    Footer,
    Header,
    Input,
    Label,
    Select,
    Static,
    TabbedContent,
    TabPane,
)

from sbatchman.remote.ssh import (
    CONFIG_FILE,
    ensure_config,
    load_config,
    save_config,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_clusters() -> tuple[dict, list[dict]]:
    ensure_config()
    cfg = load_config()
    return cfg, cfg.get("clusters", [])


def _save_clusters(cfg: dict, clusters: list[dict]) -> None:
    cfg = deepcopy(cfg)
    cfg["clusters"] = clusters
    save_config(cfg)


def _open_in_editor(path: Path) -> None:
    editor = os.environ.get("EDITOR", "nano")
    subprocess.call([editor, str(path)])


# ---------------------------------------------------------------------------
# Fetch dir row: alias | remote | local | ✕
# (mirrors SyncDirRow layout)
# ---------------------------------------------------------------------------

class FetchDirRow(Horizontal):
    """Four-field row: alias | remote | local | ✕"""

    DEFAULT_CSS = """
    FetchDirRow {
        height: auto;
        margin-top: 1;
    }
    FetchDirRow Input {
        width: 1fr;
    }
    FetchDirRow Button {
        width: 2;
        margin-left: 1;
    }
    """

    def __init__(
        self,
        alias: str = "",
        remote: str = "",
        local: str = "",
        row_id: int = 0,
    ) -> None:
        super().__init__()
        self._row_id = row_id
        self._alias  = alias
        self._remote = remote
        self._local  = local

    def compose(self) -> ComposeResult:
        yield Input(value=self._alias,  placeholder="alias",                        id=f"fetch-alias-{self._row_id}")
        yield Input(value=self._remote, placeholder="~/app1  (remote)",             id=f"fetch-remote-{self._row_id}")
        yield Input(value=self._local,  placeholder="~/results/app1  (local)",      id=f"fetch-local-{self._row_id}")
        yield Button("✕", variant="error", id=f"fetch-del-{self._row_id}",)


# ---------------------------------------------------------------------------
# Sync dir row: alias | local | remote | ✕
# ---------------------------------------------------------------------------

class SyncDirRow(Horizontal):
    """Four-field row: alias | local | remote | ✕"""

    DEFAULT_CSS = """
    SyncDirRow {
        height: auto;
        margin-top: 1;
    }
    SyncDirRow Input {
        width: 1fr;
    }
    SyncDirRow Button {
        width: 2;
        margin-left: 1;
    }
    """

    def __init__(
        self,
        alias: str = "",
        local: str = "",
        remote: str = "",
        row_id: int = 0,
    ) -> None:
        super().__init__()
        self._row_id = row_id
        self._alias  = alias
        self._local  = local
        self._remote = remote

    def compose(self) -> ComposeResult:
        yield Input(value=self._alias,  placeholder="alias",                        id=f"sync-alias-{self._row_id}")
        yield Input(value=self._local,  placeholder="~/projects/app  (local)",      id=f"sync-local-{self._row_id}")
        yield Input(value=self._remote, placeholder="~/app  (remote)",              id=f"sync-remote-{self._row_id}")
        yield Button("✕", variant="error", id=f"sync-del-{self._row_id}",)


# ---------------------------------------------------------------------------
# Single-string input row used in the excludes editor
# ---------------------------------------------------------------------------

class ExcludeRow(Horizontal):
    """Row: [value input] [✕ button]."""

    DEFAULT_CSS = """
    ExcludeRow {
        height: auto;
        margin-top: 1;
    }
    ExcludeRow Input {
        width: 1fr;
    }
    ExcludeRow Button {
        width: 5;
        margin-left: 1;
    }
    """

    def __init__(self, value: str = "", row_id: int = 0, prefix: str = "excl") -> None:
        super().__init__()
        self._value  = value
        self._row_id = row_id
        self._prefix = prefix

    def compose(self) -> ComposeResult:
        yield Input(
            value=self._value,
            placeholder="directory or file name to exclude",
            id=f"{self._prefix}-val-{self._row_id}",
        )
        yield Button("✕", variant="error", id=f"{self._prefix}-del-{self._row_id}")


# ---------------------------------------------------------------------------
# Modal: edit global common_excludes
# ---------------------------------------------------------------------------

class GlobalExcludesScreen(ModalScreen[Optional[list[str]]]):
    """
    Modal for editing the global common_excludes list.

    Returns the new list on Save, or None on Cancel.
    """

    CSS = """
    GlobalExcludesScreen {
        align: center middle;
    }
    #excl-container {
        width: 70;
        height: auto;
        max-height: 88vh;
        background: $surface;
        border: round $primary;
        padding: 1 2;
    }
    #excl-title { margin-bottom: 1; }
    #excl-subtitle { color: $text-muted; margin-bottom: 1; }
    #excl-rows-container { height: auto; }
    .add-excl-btn { margin-top: 1; width: auto; }
    #btn-row {
        margin-top: 1;
        height: auto;
    }
    """

    def __init__(self, current: list[str]) -> None:
        super().__init__()
        self._current = list(current)
        self._next_id = 0
        self._row_ids: list[int] = []

    def _new_id(self) -> int:
        rid = self._next_id
        self._next_id += 1
        return rid

    def compose(self) -> ComposeResult:
        with ScrollableContainer(id="excl-container"):
            yield Label("Global common_excludes", id="excl-title")
            yield Label(
                "These are merged with per-cluster and per-dir excludes.",
                id="excl-subtitle",
            )

            with Vertical(id="excl-rows-container"):
                entries = self._current if self._current else [""]
                for val in entries:
                    rid = self._new_id()
                    self._row_ids.append(rid)
                    yield ExcludeRow(value=val, row_id=rid, prefix="excl")

            yield Button(
                "+ Add entry",
                id="add-excl-btn",
                variant="default",
                classes="add-excl-btn",
            )

            with Horizontal(id="excl-btn-row"):
                yield Button("Save",   variant="primary", id="excl-btn-save")
                yield Button("Cancel", variant="default", id="excl-btn-cancel")

    @on(Button.Pressed, "#add-excl-btn")
    def _add_row(self) -> None:
        rid = self._new_id()
        self._row_ids.append(rid)
        self.query_one("#excl-rows-container", Vertical).mount(
            ExcludeRow(row_id=rid, prefix="excl")
        )

    @on(Button.Pressed)
    def _maybe_delete(self, event: Button.Pressed) -> None:
        bid = event.button.id or ""
        if bid.startswith("excl-del-"):
            rid = int(bid.split("-")[-1])
            self._row_ids.remove(rid)
            self.query_one(f"#excl-val-{rid}").parent.remove()

    def _collect(self) -> list[str]:
        result: list[str] = []
        for rid in self._row_ids:
            try:
                val = self.query_one(f"#excl-val-{rid}", Input).value.strip()
            except Exception:
                continue
            if val:
                result.append(val)
        return result

    @on(Button.Pressed, "#excl-btn-save")
    def _save(self) -> None:
        self.dismiss(self._collect())

    @on(Button.Pressed, "#excl-btn-cancel")
    def _cancel(self) -> None:
        self.dismiss(None)


# ---------------------------------------------------------------------------
# Modal: Add / Edit cluster
# ---------------------------------------------------------------------------

BASIC_FIELDS = [
    ("name",     "Cluster name",        "my-cluster"),
    ("host",     "Hostname / IP",       "login.hpc.example.com"),
    ("port",     "SSH port",            "22"),
    ("user",     "SSH user",            "myname"),
    ("key_path", "Key path (optional)", "~/.ssh/id_ed25519"),
]

_BACKEND_OPTIONS: list[tuple[str, str]] = [
    ("Default (from global config)", ""),
    ("rsync", "rsync"),
    ("sftp",  "sftp"),
]


class ClusterFormScreen(ModalScreen[Optional[dict]]):
    """Modal form for creating or editing a cluster entry."""

    CSS = """
    ClusterFormScreen {
        align: center middle;
    }
    #form-container {
        width: 98%;
        height: auto;
        max-height: 96vh;
        background: $surface;
        border: round $primary;
        padding: 1 2 2 2;
    }
    .field-label {
        margin-top: 1;
        color: $text-muted;
    }
    .cols-header {
        height: auto;
        margin-top: 1;
    }
    .cols-header Label {
        width: 1fr;
        color: $text-disabled;
    }
    #fetch-pairs-container, #sync-pairs-container {
        height: auto;
    }
    #cluster-excl-rows-container { height: auto; }
    .add-pair-btn { margin-top: 1; width: auto; }
    #backend-label { margin-top: 1; color: $text-muted; }
    #btn-row { height: auto; }
    """

    def __init__(self, existing: Optional[dict] = None) -> None:
        super().__init__()
        self._existing = existing or {}
        self._next_id  = 0
        self._fetch_ids: list[int] = []
        self._sync_ids:  list[int] = []
        self._excl_ids:  list[int] = []

    def _new_id(self) -> int:
        rid = self._next_id
        self._next_id += 1
        return rid

    # ------------------------------------------------------------------
    # Compose
    # ------------------------------------------------------------------

    def compose(self) -> ComposeResult:
        with ScrollableContainer(id="form-container"):
            yield Label(
                "Edit cluster" if self._existing else "Add cluster",
                id="form-title",
            )

            # SSH basics
            for key, label, placeholder in BASIC_FIELDS:
                yield Label(label, classes="field-label")
                value = str(self._existing.get(key, ""))
                yield Input(value=value, placeholder=placeholder, id=f"input-{key}")

            # Per-cluster backend override
            yield Label("Transfer backend (per-cluster override)", id="backend-label")
            current_backend = self._existing.get("transfer_backend", "")
            yield Select(
                options=_BACKEND_OPTIONS,
                value=current_backend,
                id="input-backend",
            )

            with TabbedContent():

                # ---- Fetch tab ----
                with TabPane("Fetch dirs  (remote → local)", id="tab-fetch"):
                    with Horizontal(classes="cols-header"):
                        yield Label("Alias")
                        yield Label("Remote path")
                        yield Label("Local path")
                        yield Label("   ")

                    with Vertical(id="fetch-pairs-container"):
                        pairs: list[dict] = self._existing.get("fetch_dirs", [])
                        if not pairs:
                            pairs = [{}]
                        for p in pairs:
                            rid = self._new_id()
                            self._fetch_ids.append(rid)
                            yield FetchDirRow(
                                alias=p.get("alias", ""),
                                remote=p.get("remote", ""),
                                local=p.get("local", ""),
                                row_id=rid,
                            )

                    yield Button(
                        "+ Add fetch mapping",
                        id="add-fetch-btn",
                        variant="default",
                        classes="add-pair-btn",
                    )

                # ---- Sync tab ----
                with TabPane("Sync dirs  (local → remote)", id="tab-sync"):
                    with Horizontal(classes="cols-header"):
                        yield Label("Alias")
                        yield Label("Local path")
                        yield Label("Remote path")
                        yield Label("   ")

                    with Vertical(id="sync-pairs-container"):
                        spairs: list[dict] = self._existing.get("sync_dirs", [])
                        if not spairs:
                            spairs = [{}]
                        for p in spairs:
                            rid = self._new_id()
                            self._sync_ids.append(rid)
                            yield SyncDirRow(
                                alias=p.get("alias", ""),
                                local=p.get("local", ""),
                                remote=p.get("remote", ""),
                                row_id=rid,
                            )

                    yield Button(
                        "+ Add sync mapping",
                        id="add-sync-btn",
                        variant="default",
                        classes="add-pair-btn",
                    )

                # ---- Excludes tab ----
                with TabPane("Cluster excludes  (merged with global)", id="tab-excl"):
                    yield Label(
                        "Names listed here are merged on top of global common_excludes.",
                        classes="field-label",
                    )

                    with Vertical(id="cluster-excl-rows-container"):
                        cl_excls: list[str] = self._existing.get("excludes", [])
                        if not cl_excls:
                            cl_excls = [""]
                        for val in cl_excls:
                            rid = self._new_id()
                            self._excl_ids.append(rid)
                            yield ExcludeRow(value=val, row_id=rid, prefix="cl-excl")

                    yield Button(
                        "+ Add exclude",
                        id="add-cl-excl-btn",
                        variant="default",
                        classes="add-pair-btn",
                    )

            with Horizontal(id="btn-row"):
                yield Button("Save",   variant="primary", id="btn-save")
                yield Button("Cancel", variant="default", id="btn-cancel")

    # ------------------------------------------------------------------
    # Dynamic row management
    # ------------------------------------------------------------------

    @on(Button.Pressed, "#add-fetch-btn")
    def _add_fetch(self) -> None:
        rid = self._new_id()
        self._fetch_ids.append(rid)
        self.query_one("#fetch-pairs-container", Vertical).mount(FetchDirRow(row_id=rid))

    @on(Button.Pressed, "#add-sync-btn")
    def _add_sync(self) -> None:
        rid = self._new_id()
        self._sync_ids.append(rid)
        self.query_one("#sync-pairs-container", Vertical).mount(SyncDirRow(row_id=rid))

    @on(Button.Pressed, "#add-cl-excl-btn")
    def _add_cl_excl(self) -> None:
        rid = self._new_id()
        self._excl_ids.append(rid)
        self.query_one("#cluster-excl-rows-container", Vertical).mount(
            ExcludeRow(row_id=rid, prefix="cl-excl")
        )

    @on(Button.Pressed)
    def _maybe_delete_row(self, event: Button.Pressed) -> None:
        bid = event.button.id or ""

        if bid.startswith("fetch-del-"):
            rid = int(bid.split("-")[-1])
            if len(self._fetch_ids) <= 1:
                self._set_error("At least one fetch mapping is required.")
                return
            self._fetch_ids.remove(rid)
            self.query_one(f"#fetch-alias-{rid}").parent.remove()

        elif bid.startswith("sync-del-"):
            rid = int(bid.split("-")[-1])
            if len(self._sync_ids) <= 1:
                self._set_error("At least one sync mapping is required.")
                return
            self._sync_ids.remove(rid)
            self.query_one(f"#sync-alias-{rid}").parent.remove()

        elif bid.startswith("cl-excl-del-"):
            rid = int(bid.split("-")[-1])
            self._excl_ids.remove(rid)
            self.query_one(f"#cl-excl-val-{rid}").parent.remove()

    # ------------------------------------------------------------------
    # Collect & validate
    # ------------------------------------------------------------------

    def _set_error(self, msg: str) -> None:
        self.query_one("#form-title", Label).update(f"[red]{msg}[/red]")

    def _collect(self) -> dict:
        result: dict = {}

        for key, _, _ in BASIC_FIELDS:
            widget: Input = self.query_one(f"#input-{key}", Input)
            value = widget.value.strip()
            if not value:
                continue
            result[key] = int(value) if key == "port" else value

        # Per-cluster backend override (empty string → omit key)
        backend_sel: Select = self.query_one("#input-backend", Select)
        if backend_sel.value:
            result["transfer_backend"] = backend_sel.value

        # fetch_dirs
        fetch_pairs: list[dict] = []
        for rid in self._fetch_ids:
            try:
                alias  = self.query_one(f"#fetch-alias-{rid}",  Input).value.strip()
                remote = self.query_one(f"#fetch-remote-{rid}", Input).value.strip()
                local  = self.query_one(f"#fetch-local-{rid}",  Input).value.strip()
            except Exception:
                continue
            if remote and local:
                entry: dict = {"remote": remote, "local": local}
                if alias:
                    entry["alias"] = alias
                fetch_pairs.append(entry)
        if fetch_pairs:
            result["fetch_dirs"] = fetch_pairs

        # sync_dirs
        sync_pairs: list[dict] = []
        for rid in self._sync_ids:
            try:
                alias  = self.query_one(f"#sync-alias-{rid}",  Input).value.strip()
                local  = self.query_one(f"#sync-local-{rid}",  Input).value.strip()
                remote = self.query_one(f"#sync-remote-{rid}", Input).value.strip()
            except Exception:
                continue
            if local and remote:
                entry = {"local": local, "remote": remote}
                if alias:
                    entry["alias"] = alias
                sync_pairs.append(entry)
        if sync_pairs:
            result["sync_dirs"] = sync_pairs

        # cluster-level excludes
        cl_excls: list[str] = []
        for rid in self._excl_ids:
            try:
                val = self.query_one(f"#cl-excl-val-{rid}", Input).value.strip()
            except Exception:
                continue
            if val:
                cl_excls.append(val)
        if cl_excls:
            result["excludes"] = cl_excls

        return result

    @on(Button.Pressed, "#btn-save")
    def _save(self) -> None:
        data = self._collect()
        missing = [f for f in ("name", "host", "user") if not data.get(f)]
        if missing:
            self._set_error(f"{', '.join(missing)} are required.")
            return
        self.dismiss(data)

    @on(Button.Pressed, "#btn-cancel")
    def _cancel(self) -> None:
        self.dismiss(None)


# ---------------------------------------------------------------------------
# Confirmation modal
# ---------------------------------------------------------------------------

class ConfirmScreen(ModalScreen[bool]):
    CSS = """
    ConfirmScreen { align: center middle; }
    #confirm-box {
        width: 50; height: auto;
        background: $surface;
        border: round $warning;
        padding: 1 2;
        align: center middle;
    }
    #confirm-row { margin-top: 1; }
    """

    def __init__(self, message: str) -> None:
        super().__init__()
        self._message = message

    def compose(self) -> ComposeResult:
        with Vertical(id="confirm-box"):
            yield Static(self._message)
            with Horizontal(id="confirm-row"):
                yield Button("Yes", variant="error",   id="btn-yes")
                yield Button("No",  variant="default", id="btn-no")

    @on(Button.Pressed, "#btn-yes")
    def yes(self) -> None:
        self.dismiss(True)

    @on(Button.Pressed, "#btn-no")
    def no(self) -> None:
        self.dismiss(False)


# ---------------------------------------------------------------------------
# Main TUI app
# ---------------------------------------------------------------------------

class SbatchManTUI(App):
    TITLE     = "SbatchMan – cluster config manager"
    SUB_TITLE = str(CONFIG_FILE)

    BINDINGS = [
        Binding("a", "add_cluster",      "Add"),
        Binding("e", "edit_cluster",     "Edit"),
        Binding("d", "delete_cluster",   "Delete"),
        Binding("x", "edit_excludes",    "Global excludes"),
        Binding("o", "open_editor",      "Open in $EDITOR"),
        Binding("r", "reload",           "Reload"),
        Binding("q", "quit",             "Quit"),
    ]

    CSS = """
    Screen { layout: vertical; }
    #status-bar { color: $text-muted; padding: 0 1; }
    DataTable { height: 1fr; }
    """

    def __init__(self) -> None:
        super().__init__()
        self._cfg: dict = {}
        self._clusters: list[dict] = []

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def compose(self) -> ComposeResult:
        yield Header()
        with ScrollableContainer():
            yield Static("", id="status-bar")
            yield DataTable(id="cluster-table", cursor_type="row")
        yield Footer()

    def on_mount(self) -> None:
        self._setup_table()
        self._reload_data()

    def _setup_table(self) -> None:
        table: DataTable = self.query_one("#cluster-table", DataTable)
        table.add_columns(
            "Name", "Host", "Port", "User", "Key path",
            "Backend", "Fetch dirs", "Sync dirs",
        )

    def _reload_data(self) -> None:
        self._cfg, self._clusters = _load_clusters()
        table: DataTable = self.query_one("#cluster-table", DataTable)
        table.clear()

        global_backend = self._cfg.get("global", {}).get("transfer_backend", "rsync")
        global_excls   = self._cfg.get("global", {}).get("common_excludes", [])
        status = (
            f"Config: {CONFIG_FILE}   "
            f"[dim]global backend:[/dim] {global_backend}   "
            f"[dim]global excludes ({len(global_excls)}):[/dim] "
            + (", ".join(global_excls[:5]) + ("…" if len(global_excls) > 5 else ""))
        )
        self.query_one("#status-bar", Static).update(status)

        for c in self._clusters:
            fetch_pairs: list[dict] = c.get("fetch_dirs", c.get("dirs", []))
            sync_pairs:  list[dict] = c.get("sync_dirs", [])

            fetch_summary = "|".join(
                p.get("alias") or f"{p.get('remote', '?')} → {p.get('local', '?')}"
                for p in fetch_pairs
            ) or "—"

            sync_summary = "|".join(
                p.get("alias") or f"{p.get('local','?')} → {p.get('remote','?')}"
                for p in sync_pairs
            ) or "—"

            cluster_backend = c.get("transfer_backend") or f"[dim]{global_backend}[/dim]"

            table.add_row(
                c.get("name",     ""),
                c.get("host",     ""),
                str(c.get("port", 22)),
                c.get("user",     ""),
                c.get("key_path", "—"),
                cluster_backend,
                fetch_summary,
                sync_summary,
            )

    def _selected_index(self) -> Optional[int]:
        table: DataTable = self.query_one("#cluster-table", DataTable)
        if table.cursor_row is None or table.cursor_row < 0:
            return None
        return table.cursor_row

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------

    def action_reload(self) -> None:
        self._reload_data()
        self.notify("Config reloaded.")

    def action_add_cluster(self) -> None:
        def _on_result(data: Optional[dict]) -> None:
            if data:
                self._clusters.append(data)
                _save_clusters(self._cfg, self._clusters)
                self._reload_data()
                self.notify(f"Added cluster '{data['name']}'.")

        self.push_screen(ClusterFormScreen(), _on_result)

    def action_edit_cluster(self) -> None:
        idx = self._selected_index()
        if idx is None:
            self.notify("Select a cluster first.", severity="warning")
            return

        def _on_result(data: Optional[dict]) -> None:
            if data:
                self._clusters[idx] = data
                _save_clusters(self._cfg, self._clusters)
                self._reload_data()
                self.notify(f"Updated cluster '{data['name']}'.")

        self.push_screen(ClusterFormScreen(existing=self._clusters[idx]), _on_result)

    def action_delete_cluster(self) -> None:
        idx = self._selected_index()
        if idx is None:
            self.notify("Select a cluster first.", severity="warning")
            return
        name = self._clusters[idx].get("name", "?")

        def _on_confirm(confirmed: bool) -> None:
            if confirmed:
                del self._clusters[idx]
                _save_clusters(self._cfg, self._clusters)
                self._reload_data()
                self.notify(f"Deleted cluster '{name}'.")

        self.push_screen(
            ConfirmScreen(f"Delete cluster [bold]{name}[/bold]?"),
            _on_confirm,
        )

    def action_edit_excludes(self) -> None:
        """Open the global common_excludes editor."""
        current = self._cfg.get("global", {}).get("common_excludes", [])

        def _on_result(new_list: Optional[list[str]]) -> None:
            if new_list is not None:
                if "global" not in self._cfg:
                    self._cfg["global"] = {}
                self._cfg["global"]["common_excludes"] = new_list
                save_config(self._cfg)
                self._reload_data()
                self.notify(f"Saved {len(new_list)} global excludes.")

        self.push_screen(GlobalExcludesScreen(current=current), _on_result)

    def action_open_editor(self) -> None:
        """Suspend the TUI, open the raw TOML in $EDITOR, then reload."""
        with self.suspend():
            _open_in_editor(CONFIG_FILE)
        self._reload_data()
        self.notify("Config reloaded after editor.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_remotes_config_tui() -> None:
    """Call this from your typer/click app or directly."""
    ensure_config()
    SbatchManTUI().run()