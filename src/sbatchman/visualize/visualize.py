"""
A self-contained local web server for interactive SQLite data visualization.

Plugin API — any .py in a plugins dir:
    PLOT_NAME        = "my_plot"
    PLOT_LABEL       = "My Custom Plot"
    PLOT_DESCRIPTION = "Does something"
    PLOT_DEFAULTS    = {"param": 42}
    def plot(df_data, config): ...  -> list[dict]  (Plotly traces)

The HTML/JS front-end lives in webapp.html, next to this file, and is loaded
from disk on every request (so it can be tweaked without restarting the
server).

If a file named `plots.json` exists in the current working directory when
the server starts, it is loaded and offered to the front-end as the initial
workspace (same format produced by "Export workspace").
"""

from collections import defaultdict
import importlib.util
import json
import sqlite3
import sys
import traceback
import shutil
import typer
import pandas as pd
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import urlparse
from typing import Dict, List, Optional
from rich.console import Console

from sbatchman.config.project_config import get_project_root
from sbatchman.parser import parse_jobs_and_generate_sqlite_db

console = Console(width=shutil.get_terminal_size().columns)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

MODULE_DIR = Path(__file__).resolve().parent
HTML_PATH = MODULE_DIR / "webapp.html"

# ---------------------------------------------------------------------------
# Hook functions — customise these to integrate with your infrastructure
# ---------------------------------------------------------------------------

def hook_reparse(db_name: str, db_path: str) -> dict:
    """
    Called when the user requests a data re-parse / re-load for a database.
    Replace this with your own ETL / ingestion logic.

    Args:
        db_name:  The logical name registered in DB_REGISTRY.
        db_path:  Absolute path to the SQLite file on disk.

    Returns:
        dict with keys:
            "ok"      (bool)   – whether the operation succeeded
            "message" (str)    – human-readable status shown in the UI log
    """
    # --- DEFAULT: just verify the file is readable and return row counts ---
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [r[0] for r in cur.fetchall()]
        counts = {}
        for t in tables:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            counts[t] = cur.fetchone()[0]
        conn.close()
        summary = ", ".join(f"{t}={n}" for t, n in counts.items())
        return {"ok": True, "message": f"Re-parsed '{db_name}': {summary}"}
    except Exception as e:
        return {"ok": False, "message": f"Re-parse failed for '{db_name}': {e}"}


def hook_fetch_remote(system: str, paths: list[str]) -> dict:
    """
    Called when the user requests a remote fetch for selected paths.
    Replace this with your own scp / rsync / API / S3 / etc. logic.

    Args:
        system:  The system key (e.g. "cluster-a", "hpc-login-01").
        paths:   List of remote path strings the user selected.

    Returns:
        dict with keys:
            "ok"          (bool)        – overall success
            "message"     (str)         – summary shown in UI log
            "loaded_dbs"  (list[str])   – names of any new DBs registered
    """
    loaded = []
    messages = []
    from sbatchman.remote.fetch import fetch_remotes
    try:
      fetch_remotes([system], paths)
      messages.append(f"Fetched {paths} from {system}!")
      # log(f"Fetched {paths} from {system}!")
      loaded.extend([f'{p} @ {system}' for p in paths])
    except Exception as e:
      log(f'Error during fetch: {e}', level='error')
    return {
        "ok": True,
        "message": "; ".join(messages) or "Nothing selected.",
        "loaded_dbs": loaded,
    }


# ---------------------------------------------------------------------------
# Remote systems registry
# ---------------------------------------------------------------------------

def get_remotes():
  from sbatchman.remote.ssh import load_config
  remotes_config = load_config()
  remotes = defaultdict(list)
  for cluster in remotes_config.get('clusters', []):
    for dir in cluster.get('fetch_dirs'):
      remotes[cluster['name']].append(dir['alias'])
    
  return remotes

# ---------------------------------------------------------------------------
# Built-in plot types
# ---------------------------------------------------------------------------

BUILTIN_PLOTS: dict = {}


def register_plot(name, label, description, defaults=None):
    def decorator(fn):
        BUILTIN_PLOTS[name] = {
            "name": name, "label": label,
            "description": description,
            "defaults": defaults or {}, "fn": fn,
        }
        return fn
    return decorator


def _col_idx(columns):
    return {c: i for i, c in enumerate(columns)}


def _split_groups(rows, col_idx, group_col):
    groups: dict = {}
    for row in rows:
        g = row[col_idx[group_col]]
        groups.setdefault(g, []).append(row)
    return groups


@register_plot("line", "Line Chart", "X vs Y with optional grouping", {"mode": "lines+markers"})
def plot_line(df, cfg):
    ci = _col_idx(df["columns"]); rows = df["rows"]
    x, ys, grp = cfg.get("x"), cfg.get("y", []), cfg.get("group")
    if isinstance(ys, str): ys = [ys]
    if not x or not ys: raise ValueError("Line chart requires x and at least one y.")
    traces = []
    if grp and grp in ci:
        for g, gr in sorted(_split_groups(rows, ci, grp).items()):
            for y in ys:
                traces.append({"type":"scatter","mode":cfg.get("mode","lines+markers"),
                    "name":f"{g} — {y}" if len(ys)>1 else str(g),
                    "x":[r[ci[x]] for r in gr],"y":[r[ci[y]] for r in gr]})
    else:
        for y in ys:
            traces.append({"type":"scatter","mode":cfg.get("mode","lines+markers"),
                "name":y,"x":[r[ci[x]] for r in rows],"y":[r[ci[y]] for r in rows]})
    return traces


@register_plot("bar", "Bar Chart", "Categorical comparisons", {"barmode": "group"})
def plot_bar(df, cfg):
    ci = _col_idx(df["columns"]); rows = df["rows"]
    x, ys, grp = cfg.get("x"), cfg.get("y", []), cfg.get("group")
    if isinstance(ys, str): ys = [ys]
    if not x or not ys: raise ValueError("Bar chart requires x and at least one y.")
    traces = []
    if grp and grp in ci:
        for g, gr in sorted(_split_groups(rows, ci, grp).items()):
            for y in ys:
                traces.append({"type":"bar","name":f"{g} — {y}" if len(ys)>1 else str(g),
                    "x":[r[ci[x]] for r in gr],"y":[r[ci[y]] for r in gr]})
    else:
        for y in ys:
            traces.append({"type":"bar","name":y,
                "x":[r[ci[x]] for r in rows],"y":[r[ci[y]] for r in rows]})
    return traces


@register_plot("scatter", "Scatter Plot", "X vs Y correlation", {})
def plot_scatter(df, cfg):
    ci = _col_idx(df["columns"]); rows = df["rows"]
    x, ys, grp = cfg.get("x"), cfg.get("y", []), cfg.get("group")
    if isinstance(ys, str): ys = [ys]
    if not x or not ys: raise ValueError("Scatter requires x and at least one y.")
    traces = []
    if grp and grp in ci:
        for g, gr in sorted(_split_groups(rows, ci, grp).items()):
            for y in ys:
                traces.append({"type":"scatter","mode":"markers",
                    "name":f"{g} — {y}" if len(ys)>1 else str(g),
                    "x":[r[ci[x]] for r in gr],"y":[r[ci[y]] for r in gr]})
    else:
        for y in ys:
            traces.append({"type":"scatter","mode":"markers","name":y,
                "x":[r[ci[x]] for r in rows],"y":[r[ci[y]] for r in rows]})
    return traces


@register_plot("histogram", "Histogram", "Distribution of a numeric column", {"nbinsx": 30})
def plot_histogram(df, cfg):
    ci = _col_idx(df["columns"]); rows = df["rows"]
    x = cfg.get("x")
    if not x: raise ValueError("Histogram requires an x column.")
    return [{"type":"histogram","name":x,"x":[r[ci[x]] for r in rows],"nbinsx":int(cfg.get("nbinsx",30))}]


@register_plot("box", "Box Plot", "Distribution summary per category", {})
def plot_box(df, cfg):
    ci = _col_idx(df["columns"]); rows = df["rows"]
    x, ys = cfg.get("x"), cfg.get("y", [])
    if isinstance(ys, str): ys = [ys]
    if not ys: raise ValueError("Box plot requires at least one y column.")
    traces = []
    for y in ys:
        t = {"type":"box","name":y,"y":[r[ci[y]] for r in rows]}
        if x and x in ci: t["x"] = [r[ci[x]] for r in rows]
        traces.append(t)
    return traces


@register_plot("heatmap", "Heatmap", "2D density / matrix view", {})
def plot_heatmap(df, cfg):
    ci = _col_idx(df["columns"]); rows = df["rows"]
    x, yc, z = cfg.get("x"), cfg.get("y", []), cfg.get("z")
    if isinstance(yc, list): yc = yc[0] if yc else None
    if not x or not yc or not z: raise ValueError("Heatmap requires x, y, and z columns.")
    xs = sorted(set(r[ci[x]] for r in rows))
    ys = sorted(set(r[ci[yc]] for r in rows))
    xi = {v: i for i, v in enumerate(xs)}
    yi = {v: i for i, v in enumerate(ys)}
    mat = [[None]*len(xs) for _ in range(len(ys))]
    for row in rows: mat[yi[row[ci[yc]]]][xi[row[ci[x]]]] = row[ci[z]]
    return [{"type":"heatmap","x":xs,"y":ys,"z":mat,"colorscale":"Viridis"}]


@register_plot("violin", "Violin Plot", "Distribution shape per category", {})
def plot_violin(df, cfg):
    ci = _col_idx(df["columns"]); rows = df["rows"]
    x, ys = cfg.get("x"), cfg.get("y", [])
    if isinstance(ys, str): ys = [ys]
    if not ys: raise ValueError("Violin plot requires at least one y column.")
    traces = []
    for y in ys:
        t = {"type":"violin","name":y,"y":[r[ci[y]] for r in rows],
             "box":{"visible":True},"meanline":{"visible":True}}
        if x and x in ci: t["x"] = [r[ci[x]] for r in rows]
        traces.append(t)
    return traces


@register_plot("pie", "Pie / Donut", "Proportional breakdown", {"hole": 0})
def plot_pie(df, cfg):
    ci = _col_idx(df["columns"]); rows = df["rows"]
    lc, vc = cfg.get("x"), cfg.get("y", [])
    if isinstance(vc, list): vc = vc[0] if vc else None
    if not lc or not vc: raise ValueError("Pie chart requires x (labels) and y (values).")
    return [{"type":"pie","labels":[r[ci[lc]] for r in rows],
             "values":[r[ci[vc]] for r in rows],"hole":float(cfg.get("hole",0))}]


# ---------------------------------------------------------------------------
# Plugin loader
# ---------------------------------------------------------------------------

PLUGIN_PLOTS: dict = {}


def load_plugins(plugin_dirs):
    for d in plugin_dirs:
        p = Path(d)
        if not p.is_dir():
            print(f"[warn] Plugin dir not found: {d}", file=sys.stderr); continue
        for pyfile in p.glob("*.py"):
            try:
                spec = importlib.util.spec_from_file_location(pyfile.stem, pyfile)
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                if hasattr(mod, "PLOT_NAME") and hasattr(mod, "plot"):
                    PLUGIN_PLOTS[mod.PLOT_NAME] = {
                        "name": mod.PLOT_NAME,
                        "label": getattr(mod, "PLOT_LABEL", mod.PLOT_NAME),
                        "description": getattr(mod, "PLOT_DESCRIPTION", ""),
                        "defaults": getattr(mod, "PLOT_DEFAULTS", {}),
                        "fn": mod.plot, "source": str(pyfile),
                    }
                    print(f"[plugin] Loaded: {mod.PLOT_NAME} from {pyfile.name}")
            except Exception as e:
                print(f"[warn] Failed to load plugin {pyfile}: {e}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

DB_REGISTRY: dict = {}  # logical_name -> abs_path


def load_databases(paths):
    for path in paths:
        p = Path(path).resolve()
        if not p.exists():
            print(f"[warn] Database not found: {path}", file=sys.stderr); continue
        name = p.stem
        base, n = name, 1
        while name in DB_REGISTRY:
            name = f"{base}_{n}"; n += 1
        DB_REGISTRY[name] = str(p)
        print(f"[db] Registered: {name} -> {p}")


def is_single_db() -> bool:
    return len(DB_REGISTRY) == 1


def only_db_name() -> Optional[str]:
    if is_single_db():
        return next(iter(DB_REGISTRY))
    return None


def get_db_schema(db_name):
    path = DB_REGISTRY.get(db_name)
    if not path: return None
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = {}
    for (tname,) in cur.fetchall():
        cur.execute(f"PRAGMA table_info({tname})")
        tables[tname] = [{"name": row[1], "type": row[2]} for row in cur.fetchall()]
    conn.close()
    return tables


def get_all_tables(db_name: str) -> List[str]:
    path = DB_REGISTRY.get(db_name)
    if not path: raise ValueError(f"Unknown database: {db_name}")
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [r[0] for r in cur.fetchall()]
    conn.close()
    return tables


def get_all_tables_as_dataframes(db_name: str) -> Dict[str, "pd.DataFrame"]:
    """Load every table of a database into a dict of pandas DataFrames."""
    path = DB_REGISTRY.get(db_name)
    if not path: raise ValueError(f"Unknown database: {db_name}")
    conn = sqlite3.connect(path)
    dfs = {}
    try:
        for t in get_all_tables(db_name):
            dfs[t] = pd.read_sql_query(f"SELECT * FROM {t}", conn)
    finally:
        conn.close()
    return dfs


def run_query(db_name, sql, limit=10000):
    path = DB_REGISTRY.get(db_name)
    if not path: raise ValueError(f"Unknown database: {db_name}")
    s = sql.strip().upper()
    if not (s.startswith("SELECT") or s.startswith("WITH")):
        raise ValueError("Only SELECT / WITH queries are allowed.")
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(sql)
    rows_raw = cur.fetchmany(limit)
    columns = [d[0] for d in cur.description]
    rows = [list(r) for r in rows_raw]
    conn.close()
    return {"columns": columns, "rows": rows, "truncated": len(rows) == limit}


def df_data_to_dataframe(df_data: dict) -> "pd.DataFrame":
    return pd.DataFrame(df_data["rows"], columns=df_data["columns"])


def dataframe_to_df_data(df: "pd.DataFrame", limit=10000) -> dict:
    truncated = len(df) > limit
    if truncated:
        df = df.head(limit)
    # Replace NaN/NaT with None so it survives JSON serialisation.
    safe = df.astype(object).where(pd.notnull(df), None)
    return {
        "columns": [str(c) for c in df.columns],
        "rows": safe.values.tolist(),
        "truncated": truncated,
    }


def run_custom_plot_script(source, df_data, config):
    ns: dict = {}
    exec(compile(source, "<custom_plot>", "exec"), ns)
    if "plot" not in ns:
        raise ValueError("Custom script must define a `plot(df_data, config)` function.")
    return ns["plot"](df_data, config)


def run_transform_script(source: str, data: Dict[str, "pd.DataFrame"], log_fn):
    """
    Runs a user-supplied Python snippet that can inspect/modify `data`, a
    dict[str, pandas.DataFrame]. The query result (if any) lives under the
    key "result". Whatever ends up under data["result"] after the script
    runs becomes the data that gets plotted (or previewed).

    `log_fn(msg)` is exposed to the script as `log(...)`, writing into the
    server log panel — handy for debugging the transform interactively.
    """
    ns = {"data": data, "pd": pd, "log": log_fn}
    exec(compile(source, "<transform_script>", "exec"), ns)
    return ns.get("data", data)


def run_layout_script(source: str, layout: dict, config: dict, log_fn):
    """
    Runs a user-supplied Python snippet that can tweak the Plotly `layout`
    dict in place (tick formats, fonts, annotations, axis ranges, ...)
    after the built-in layout has already been assembled. `config` (the
    same config dict sent to the plot function) is available read-only,
    and `log(...)` writes to the server log panel.
    """
    ns = {"layout": layout, "config": config, "log": log_fn}
    exec(compile(source, "<layout_script>", "exec"), ns)
    return ns.get("layout", layout)


def build_layout(config, layout_overrides):
    y_label = config.get("y", [])
    if isinstance(y_label, list): y_label = ", ".join(y_label)
    layout = {
        "title": {"text": config.get("title", ""), "font": {"size": 16}},
        "xaxis": {
            "title": {"text": config.get("x_label") or config.get("x", "")},
            "type": config.get("x_scale", "linear"),
            "showgrid": True, "gridcolor": "#21262d",
            "tickformat": config.get("x_tickformat", ""),
        },
        "yaxis": {
            "title": {"text": config.get("y_label") or y_label},
            "type": config.get("y_scale", "linear"),
            "showgrid": True, "gridcolor": "#21262d",
            "tickformat": config.get("y_tickformat", ""),
        },
        "barmode": config.get("barmode", "group"),
        "template": "plotly_dark",
        "paper_bgcolor": "#0d1117",
        "plot_bgcolor": "#0d1117",
        "font": {"color": "#e6edf3", "family": "JetBrains Mono, monospace"},
        "legend": {"bgcolor": "rgba(0,0,0,0)", "bordercolor": "#30363d", "borderwidth": 1},
        "margin": {"l": 60, "r": 20, "t": 50, "b": 50},
    }
    layout.update(layout_overrides)
    return layout


# ---------------------------------------------------------------------------
# Server-side log
# ---------------------------------------------------------------------------

SERVER_LOG: list = []  # list of {"ts": str, "level": str, "msg": str}


def log(msg, level="info"):
    entry = {"ts": datetime.now().strftime("%H:%M:%S"), "level": level, "msg": str(msg)}
    SERVER_LOG.append(entry)
    print(f"[{entry['ts']}] [{level.upper()}] {msg}", file=sys.stderr)
    return entry


def make_script_logger(prefix: str):
    """Builds a `log(msg)` callable for user scripts that tags entries."""
    entries = []
    def _log(msg, level="script"):
        entry = log(f"[{prefix}] {msg}", level)
        entries.append(entry)
        return entry
    return _log, entries


# ---------------------------------------------------------------------------
# Initial workspace (plots.json autoload)
# ---------------------------------------------------------------------------

INITIAL_WORKSPACE: Optional[dict] = None


def load_initial_workspace():
    global INITIAL_WORKSPACE
    candidate = Path.cwd() / "plots.json"
    if candidate.exists():
        try:
            with open(candidate, "r", encoding="utf-8") as f:
                INITIAL_WORKSPACE = json.load(f)
            log(f"Loaded initial workspace from {candidate}")
        except Exception as e:
            INITIAL_WORKSPACE = None
            log(f"Failed to load {candidate}: {e}", "error")
    else:
        INITIAL_WORKSPACE = None


# ---------------------------------------------------------------------------
# HTTP handler
# ---------------------------------------------------------------------------

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args): pass  # suppress default stdout log

    def send_json(self, data, status=200):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_html(self, html):
        body = html.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def read_body(self):
        length = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(length)

    def do_GET(self):
        path = urlparse(self.path).path
        if path in ("/", "/index.html"):
            try:
                html = HTML_PATH.read_text(encoding="utf-8")
            except Exception as e:
                self.send_json({"error": f"Could not load {HTML_PATH}: {e}"}, 500); return
            self.send_html(html)
        elif path == "/api/databases":
            schema = {n: get_db_schema(n) for n in DB_REGISTRY}
            self.send_json({
                "databases": schema,
                "single_db": is_single_db(),
                "default_database": only_db_name(),
            })
        elif path == "/api/plot_types":
            out = {}
            for k, v in BUILTIN_PLOTS.items():
                out[k] = {x: v[x] for x in ("name","label","description","defaults")}
            for k, v in PLUGIN_PLOTS.items():
                out[k] = {x: v[x] for x in ("name","label","description","defaults")}
                out[k]["is_plugin"] = True
                out[k]["source"] = v.get("source","")
            self.send_json({"plot_types": out})
        elif path == "/api/remote_systems":
            self.send_json({"systems": {s: paths for s, paths in get_remotes().items()}})
        elif path == "/api/logs":
            self.send_json({"logs": SERVER_LOG[-200:]})
        elif path == "/api/initial_workspace":
            self.send_json({"workspace": INITIAL_WORKSPACE})
        else:
            self.send_json({"error": "Not found"}, 404)

    def do_POST(self):
        path = urlparse(self.path).path
        try:
            body = self.read_body()
            payload = json.loads(body) if body else {}
        except Exception:
            self.send_json({"error": "Invalid JSON"}, 400); return

        if path == "/api/query":
            try:
                result = run_query(payload["database"], payload["sql"],
                                   int(payload.get("limit", 10000)))
                log(f"Query on '{payload['database']}': {len(result['rows'])} rows")
                self.send_json(result)
            except Exception as e:
                log(str(e), "error")
                self.send_json({"error": str(e)}, 400)

        elif path == "/api/transform_test":
            # Lets the user dry-run their pandas transform script and see a
            # preview + any log() output, without rendering a plot.
            try:
                db = payload["database"]
                sql = payload.get("sql", "")
                script = payload.get("transform_script", "")
                data = get_all_tables_as_dataframes(db)
                if sql.strip():
                    df_data = run_query(db, sql)
                    data["result"] = df_data_to_dataframe(df_data)
                script_log, entries = make_script_logger("transform")
                if script.strip():
                    data = run_transform_script(script, data, script_log)
                result_df = data.get("result")
                if result_df is None:
                    raise ValueError("The transform script must leave a DataFrame in data['result'].")
                preview = dataframe_to_df_data(result_df, limit=500)
                self.send_json({"ok": True, "preview": preview, "log_entries": entries})
            except Exception as e:
                entry = log(str(e), "error")
                self.send_json({"ok": False, "error": str(e),
                                 "traceback": traceback.format_exc(), "log_entries": [entry]}, 400)

        elif path == "/api/plot":
            try:
                db, sql = payload["database"], payload["sql"]
                plot_type = payload.get("plot_type", "line")
                config = payload.get("config", {})
                custom_script = payload.get("custom_script", "")
                transform_script = payload.get("transform_script", "")
                layout_script = payload.get("layout_script", "")
                layout_overrides = payload.get("layout", {})

                df_data = run_query(db, sql)
                log_entries = []

                if transform_script.strip():
                    data = get_all_tables_as_dataframes(db)
                    data["result"] = df_data_to_dataframe(df_data)
                    script_log, entries = make_script_logger("transform")
                    data = run_transform_script(transform_script, data, script_log)
                    log_entries.extend(entries)
                    result_df = data.get("result")
                    if result_df is None:
                        raise ValueError("The transform script must leave a DataFrame in data['result'].")
                    df_data = dataframe_to_df_data(result_df)

                if custom_script.strip():
                    traces = run_custom_plot_script(custom_script, df_data, config)
                    log(f"Custom script plot on '{db}'")
                elif plot_type in PLUGIN_PLOTS:
                    traces = PLUGIN_PLOTS[plot_type]["fn"](df_data, config)
                    log(f"Plugin plot '{plot_type}' on '{db}'")
                elif plot_type in BUILTIN_PLOTS:
                    traces = BUILTIN_PLOTS[plot_type]["fn"](df_data, config)
                    log(f"Plot '{plot_type}' on '{db}': {len(traces)} trace(s)")
                else:
                    raise ValueError(f"Unknown plot type: {plot_type}")

                layout = build_layout(config, layout_overrides)

                if layout_script.strip():
                    script_log, entries = make_script_logger("layout")
                    layout = run_layout_script(layout_script, layout, config, script_log)
                    log_entries.extend(entries)

                preview = {
                    "columns": df_data["columns"],
                    "rows": df_data["rows"][:500],
                    "truncated": df_data.get("truncated", False) or len(df_data["rows"]) > 500,
                }

                self.send_json({"traces": traces, "layout": layout,
                                "columns": df_data["columns"],
                                "truncated": df_data.get("truncated", False),
                                "preview": preview,
                                "log_entries": log_entries})
            except Exception as e:
                log(str(e), "error")
                self.send_json({"error": str(e), "traceback": traceback.format_exc()}, 400)

        elif path == "/api/reparse":
            try:
                db_name = payload.get("database")
                if not db_name or db_name not in DB_REGISTRY:
                    raise ValueError(f"Unknown database: {db_name}")
                result = hook_reparse(db_name, DB_REGISTRY[db_name])
                entry = log(result["message"], "info" if result["ok"] else "error")
                # refresh schema
                schema = {n: get_db_schema(n) for n in DB_REGISTRY}
                self.send_json({"ok": result["ok"], "message": result["message"],
                                "log_entry": entry, "databases": schema,
                                "single_db": is_single_db(), "default_database": only_db_name()})
            except Exception as e:
                log(str(e), "error")
                self.send_json({"error": str(e)}, 400)

        elif path == "/api/fetch_remote":
            try:
                system = payload.get("system")
                paths = payload.get("paths", [])
                if not system: raise ValueError("No system specified.")
                result = hook_fetch_remote(system, paths)
                entry = log(result["message"], "info" if result["ok"] else "error")
                # reload schema in case new DBs were added
                schema = {n: get_db_schema(n) for n in DB_REGISTRY}
                self.send_json({"ok": result["ok"], "message": result["message"],
                                "loaded_dbs": result.get("loaded_dbs", []),
                                "log_entry": entry, "databases": schema,
                                "single_db": is_single_db(), "default_database": only_db_name()})
            except Exception as e:
                log(str(e), "error")
                self.send_json({"error": str(e)}, 400)

        elif path == "/api/reload_plugins":
            try:
                dirs = payload.get("dirs", [])
                if dirs: load_plugins(dirs)
                out = {}
                for k, v in PLUGIN_PLOTS.items():
                    out[k] = {x: v[x] for x in ("name","label","description","defaults")}
                    out[k]["is_plugin"] = True
                entry = log(f"Plugins reloaded: {list(PLUGIN_PLOTS.keys()) or 'none'}")
                self.send_json({"reloaded": list(PLUGIN_PLOTS.keys()),
                                "plot_types": out, "log_entry": entry})
            except Exception as e:
                log(str(e), "error")
                self.send_json({"error": str(e)}, 400)

        elif path == "/api/log":
            try:
                entry = log(payload.get("message",""), payload.get("level","info"))
                self.send_json({"log_entry": entry})
            except Exception as e:
                self.send_json({"error": str(e)}, 400)

        else:
            self.send_json({"error": "Not found"}, 404)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def launch_visualize_web_server(parser: Path, presets: Path, port: int = 8765, plugins: List[Path] = []):
  # Plugin API (any .py file in the plugins dir):
  # PLOT_NAME        = "my_plot"          # unique key
  # PLOT_LABEL       = "My Custom Plot"   # dilay name
  # PLOT_DESCRIPTION = "Does something"   # tooltip
  # PLOT_DEFAULTS    = {"param": 42}      # extra config fields

  # def plot(df_data, config):
  #     # df_data: {"columns": [...], "rows": [[...], ...]}
  #     # config: {"x": ..., "y": [...], "group": ..., "param": 42, ...}
  #     # return: list of Plotly trace dicts
  #     ...

    db_path = get_project_root() / "data.sqlite"
    parse_jobs_and_generate_sqlite_db(parser=parser, output_path=db_path)
    databases = [db_path]

    if not databases:
        console.print(f"[bold red]At least one SQLite database file is required.[/bold red]")
        raise typer.Exit(1)

    load_databases(databases)

    if not DB_REGISTRY:
        console.print("[bold red]No valid databases loaded. Exiting.[/bold red]")
        raise typer.exit(1)

    if plugins:
        load_plugins(plugins)

    load_initial_workspace()

    if not HTML_PATH.exists():
        console.print(f"[bold red]Missing front-end file: {HTML_PATH}[/bold red]")
        raise typer.Exit(1)

    HOST = "127.0.0.1"
    server = HTTPServer((HOST, port), Handler)
    url = f"http://{HOST}:{port}"
    print("\n  Plot Builder")
    print("  ─────────────────────────")
    print(f"  URL     : {url}")
    print(f"  DBs     : {', '.join(DB_REGISTRY.keys())}")
    print(f"  Plots   : {', '.join(BUILTIN_PLOTS.keys())}")
    if PLUGIN_PLOTS:
        print(f"  Plugins : {', '.join(PLUGIN_PLOTS.keys())}")
    if INITIAL_WORKSPACE:
        print(f"  Workspace: loaded from ./plots.json ({len(INITIAL_WORKSPACE.get('tabs', []))} tab(s))")
    print("\n  Press Ctrl+C to stop\n")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[stopped]")
        server.server_close()