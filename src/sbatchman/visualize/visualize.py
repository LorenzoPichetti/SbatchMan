"""
A self-contained local web server for interactive SQLite data visualization.

Plugin API — any .py in a plugins dir:
    PLOT_NAME        = "my_plot"
    PLOT_LABEL       = "My Custom Plot"
    PLOT_DESCRIPTION = "Does something"
    PLOT_DEFAULTS    = {"param": 42}
    def plot(df_data, config): ...  -> list[dict]  (Plotly traces)

The front-end (HTML/CSS/JS) lives in webapp.html and docs.html, next to this
file, and is loaded from disk on every request (so it can be tweaked without
restarting the server).

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
from typing import Dict, List, Optional, Tuple
from rich.console import Console

from sbatchman.config.project_config import get_project_root
from sbatchman.parser import parse_jobs_and_generate_sqlite_db

console = Console(width=shutil.get_terminal_size().columns)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

MODULE_DIR = Path(__file__).resolve().parent
HTML_PATH = MODULE_DIR / "webapp.html"
DOCS_PATH = MODULE_DIR / "docs.html"

# ---------------------------------------------------------------------------
# Re-parse bookkeeping — lets hook_reparse() actually regenerate a database
# instead of just re-reading whatever is already on disk.
# ---------------------------------------------------------------------------

# db_name -> {"parser": Path, "output_path": Path}   (populated for any DB
# that was produced by parse_jobs_and_generate_sqlite_db at startup)
REPARSE_SOURCES: dict = {}


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
    src = REPARSE_SOURCES.get(db_name)
    try:
        if src:
            # Actually regenerate the SQLite file from the job logs, so
            # columns/tables added by the parser since the server started
            # actually show up (previously this only re-read the stale file).
            parse_jobs_and_generate_sqlite_db(parser=src["parser"], output_path=src["output_path"])
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
    """
    loaded = []
    messages = []
    from sbatchman.remote.fetch import fetch_remotes
    try:
      fetch_remotes([system], paths)
      messages.append(f"Fetched {paths} from {system}!")
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

REMOTE_SYSTEMS = {
    "cluster-a": [
        "/scratch/benchmarks/2024/run_001.db",
        "/scratch/benchmarks/2024/run_002.db",
        "/scratch/benchmarks/2025/mpi_scaling.db",
    ],
    "hpc-login-01": [
        "/home/user/results/omp_bench.db",
        "/home/user/results/gpu_roofline.db",
    ],
    "cloud-runner": [
        "/data/ci/nightly/latest.db",
        "/data/ci/weekly/summary.db",
    ],
}


# ---------------------------------------------------------------------------
# Visual style — scientific-paper-grade defaults (matplotlib/seaborn-like)
# ---------------------------------------------------------------------------

# matplotlib "tab10" colour cycle
COLORWAY = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
            "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf"]

DASH_SEQUENCE = ["solid", "dash", "dot", "dashdot", "longdash", "longdashdot"]
MARKER_SEQUENCE = ["circle", "square", "diamond", "cross", "x",
                    "triangle-up", "triangle-down", "star", "pentagon", "hexagon"]

AXIS_FONT = {"color": "#222222", "family": "Georgia, 'Times New Roman', serif", "size": 13}


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


def _norm_cols(g) -> List[str]:
    """Normalise a group-by spec (None / str / list) into a list of column names."""
    if not g:
        return []
    if isinstance(g, str):
        return [g]
    return [c for c in g if c]


def _split_groups(rows, col_idx, group_cols: List[str]):
    """Split rows into groups keyed by a tuple of one or more column values."""
    groups: dict = {}
    for row in rows:
        key = tuple(row[col_idx[c]] for c in group_cols)
        groups.setdefault(key, []).append(row)
    return groups


def _group_label(key: Tuple) -> str:
    return " | ".join(str(v) for v in key)


def _style_map(rows, col_idx, col: Optional[str], sequence: List[str]) -> dict:
    """Assigns each distinct value of `col` a symbol/dash from `sequence`, in
    stable sorted order, so the same value always maps to the same style."""
    if not col or col not in col_idx:
        return {}
    values = sorted(set(r[col_idx[col]] for r in rows), key=lambda v: str(v))
    return {v: sequence[i % len(sequence)] for i, v in enumerate(values)}


@register_plot("line", "Line Chart", "X vs Y with multi-column grouping, marker & linestyle mapping", {"mode": "lines+markers"})
def plot_line(df, cfg):
    ci = _col_idx(df["columns"]); rows = df["rows"]
    x, ys = cfg.get("x"), cfg.get("y", [])
    if isinstance(ys, str): ys = [ys]
    if not x or not ys: raise ValueError("Line chart requires x and at least one y.")

    group_cols = _norm_cols(cfg.get("group"))
    marker_by = cfg.get("marker_by") or None
    dash_by = cfg.get("dash_by") or None

    marker_map = _style_map(rows, ci, marker_by, MARKER_SEQUENCE)
    dash_map = _style_map(rows, ci, dash_by, DASH_SEQUENCE)

    # linestyle is a trace-level property, so a column mapped to linestyle is
    # folded into the split so each distinct value gets its own trace.
    split_cols = list(group_cols)
    if dash_by and dash_by not in split_cols:
        split_cols.append(dash_by)

    def make_trace(gr, label, y):
        trace = {"type": "scatter", "mode": cfg.get("mode", "lines+markers"),
                  "name": f"{label} — {y}" if (label and len(ys) > 1) else (label or y),
                  "x": [r[ci[x]] for r in gr], "y": [r[ci[y]] for r in gr]}
        if dash_by:
            dash_val = gr[0][ci[dash_by]]
            trace["line"] = {"dash": dash_map.get(dash_val, "solid")}
        if marker_by:
            trace["marker"] = {"symbol": [marker_map.get(r[ci[marker_by]], "circle") for r in gr]}
        return trace

    traces = []
    if split_cols:
        for key, gr in sorted(_split_groups(rows, ci, split_cols).items(), key=lambda kv: [str(v) for v in kv[0]]):
            label = _group_label(key)
            for y in ys:
                traces.append(make_trace(gr, label, y))
    else:
        for y in ys:
            traces.append(make_trace(rows, None, y))
    return traces


@register_plot("bar", "Bar Chart", "Categorical comparisons with multi-column grouping", {"barmode": "group"})
def plot_bar(df, cfg):
    ci = _col_idx(df["columns"]); rows = df["rows"]
    x, ys = cfg.get("x"), cfg.get("y", [])
    if isinstance(ys, str): ys = [ys]
    if not x or not ys: raise ValueError("Bar chart requires x and at least one y.")
    group_cols = _norm_cols(cfg.get("group"))
    traces = []
    if group_cols:
        for key, gr in sorted(_split_groups(rows, ci, group_cols).items(), key=lambda kv: [str(v) for v in kv[0]]):
            label = _group_label(key)
            for y in ys:
                traces.append({"type": "bar", "name": f"{label} — {y}" if len(ys) > 1 else label,
                    "x": [r[ci[x]] for r in gr], "y": [r[ci[y]] for r in gr]})
    else:
        for y in ys:
            traces.append({"type": "bar", "name": y,
                "x": [r[ci[x]] for r in rows], "y": [r[ci[y]] for r in rows]})
    return traces


@register_plot("scatter", "Scatter Plot", "X vs Y correlation with grouping and marker mapping", {})
def plot_scatter(df, cfg):
    ci = _col_idx(df["columns"]); rows = df["rows"]
    x, ys = cfg.get("x"), cfg.get("y", [])
    if isinstance(ys, str): ys = [ys]
    if not x or not ys: raise ValueError("Scatter requires x and at least one y.")
    group_cols = _norm_cols(cfg.get("group"))
    marker_by = cfg.get("marker_by") or None
    marker_map = _style_map(rows, ci, marker_by, MARKER_SEQUENCE)

    def make_trace(gr, label, y):
        trace = {"type": "scatter", "mode": "markers",
                  "name": f"{label} — {y}" if (label and len(ys) > 1) else (label or y),
                  "x": [r[ci[x]] for r in gr], "y": [r[ci[y]] for r in gr]}
        if marker_by:
            trace["marker"] = {"symbol": [marker_map.get(r[ci[marker_by]], "circle") for r in gr]}
        return trace

    traces = []
    if group_cols:
        for key, gr in sorted(_split_groups(rows, ci, group_cols).items(), key=lambda kv: [str(v) for v in kv[0]]):
            label = _group_label(key)
            for y in ys:
                traces.append(make_trace(gr, label, y))
    else:
        for y in ys:
            traces.append(make_trace(rows, None, y))
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
    return next(iter(DB_REGISTRY)) if is_single_db() else None


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


def get_all_table_names(db_name: str) -> List[str]:
    path = DB_REGISTRY.get(db_name)
    if not path: raise ValueError(f"Unknown database: {db_name}")
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [r[0] for r in cur.fetchall()]
    conn.close()
    return tables


def get_all_tables_as_dataframes(db_name: str) -> Dict[str, "pd.DataFrame"]:
    path = DB_REGISTRY.get(db_name)
    if not path: raise ValueError(f"Unknown database: {db_name}")
    conn = sqlite3.connect(path)
    dfs = {}
    try:
        for t in get_all_table_names(db_name):
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
    dict[str, pandas.DataFrame]. The SQL query result lives under the key
    "result"; whatever ends up in data["result"] after the script runs is
    what gets shown/plotted. `log_fn(msg)` is exposed as `log(...)`.
    """
    ns = {"data": data, "pd": pd, "log": log_fn}
    exec(compile(source, "<transform_script>", "exec"), ns)
    return ns.get("data", data)


def run_layout_script(source: str, layout: dict, config: dict, log_fn):
    ns = {"layout": layout, "config": config, "log": log_fn}
    exec(compile(source, "<layout_script>", "exec"), ns)
    return ns.get("layout", layout)


def _parse_tickvals(spec):
    """Accepts a comma-separated string (from the UI) or a list of numbers
    and returns a list of floats, or None if nothing usable was given."""
    if not spec:
        return None
    if isinstance(spec, (list, tuple)):
        vals = spec
    else:
        vals = str(spec).split(",")
    out = []
    for v in vals:
        v = str(v).strip()
        if v == "":
            continue
        try:
            out.append(float(v))
        except ValueError:
            return None
    return out or None


def _format_tick_number(v: float) -> str:
    return str(int(v)) if float(v).is_integer() else str(v)


def _apply_custom_ticks(axis: dict, spec):
    vals = _parse_tickvals(spec)
    if not vals:
        return
    axis["tickmode"] = "array"
    axis["tickvals"] = vals
    axis["ticktext"] = [_format_tick_number(v) for v in vals]


def build_layout(config, layout_overrides=None):
    """Scientific-paper-grade default styling (white background, serif font,
    matplotlib tab10 colour cycle, mirrored axis lines, light gridlines) —
    deliberately close to a default matplotlib/seaborn figure rather than a
    dashboard theme.

    `config["x_tickvals"]` / `config["y_tickvals"]` accept a comma-separated
    list of numbers (e.g. "1,2,4,8,16") to pin ticks to specific values —
    handy for a log-scaled axis that should only show powers of two, for
    instance: set x_scale to "log" and x_tickvals to "1,2,4,8,16,32".
    """
    layout_overrides = layout_overrides or {}
    y_label = config.get("y", [])
    if isinstance(y_label, list): y_label = ", ".join(y_label)
    axis_common = {
        "showgrid": True, "gridcolor": "#e6e6e6", "gridwidth": 1,
        "zeroline": False, "showline": True, "linecolor": "#333333",
        "linewidth": 1, "mirror": True, "ticks": "outside", "tickcolor": "#333333",
        "automargin": True, "ticklen": 5,
    }
    xaxis = {**axis_common,
        "title": {"text": config.get("x_label") or config.get("x", ""), "standoff": 12},
        "type": config.get("x_scale", "linear"),
        "tickformat": config.get("x_tickformat", ""),
    }
    yaxis = {**axis_common,
        "title": {"text": config.get("y_label") or y_label, "standoff": 12},
        "type": config.get("y_scale", "linear"),
        "tickformat": config.get("y_tickformat", ""),
    }
    _apply_custom_ticks(xaxis, config.get("x_tickvals"))
    _apply_custom_ticks(yaxis, config.get("y_tickvals"))

    layout = {
        "title": {"text": config.get("title", ""), "font": {"size": 16, **AXIS_FONT}, "pad": {"t": 8, "b": 8}, "x": 0.02, "xanchor": "left"},
        "xaxis": xaxis,
        "yaxis": yaxis,
        "barmode": config.get("barmode", "group"),
        "paper_bgcolor": "#ffffff",
        "plot_bgcolor": "#ffffff",
        "font": dict(AXIS_FONT),
        "colorway": COLORWAY,
        "legend": {"bgcolor": "rgba(255,255,255,0.85)", "bordercolor": "#cccccc", "borderwidth": 1},
        # Generous, auto-expanding margins so long tick labels (e.g. large
        # sample counts) never get clipped; automargin on each axis will
        # grow these further if needed.
        "margin": {"l": 80, "r": 30, "t": 64, "b": 70, "pad": 4},
    }
    layout.update(layout_overrides)
    return layout


def compute_grid_domains(rows: int, cols: int, xgap=0.10, ygap=0.16):
    """Evenly spaced (x-domain, y-domain) pairs for a `rows` x `cols` grid of
    subplots, filled left-to-right, top-to-bottom, with a gap between cells."""
    col_width = (1 - xgap * (cols - 1)) / cols
    row_height = (1 - ygap * (rows - 1)) / rows
    domains = []
    for r in range(rows):
        for c in range(cols):
            x0 = c * (col_width + xgap)
            x1 = x0 + col_width
            y1 = 1 - r * (row_height + ygap)
            y0 = y1 - row_height
            domains.append(((round(x0, 4), round(x1, 4)), (round(y0, 4), round(y1, 4))))
    return domains


# ---------------------------------------------------------------------------
# Shared "query -> transform -> plot" pipeline
# ---------------------------------------------------------------------------

def run_pipeline(database: str, sql: str, transform_script: str = ""):
    """SQL query, optionally followed by a pandas transform script. Returns
    (df_data, log_entries)."""
    df_data = run_query(database, sql)
    log_entries = []
    if transform_script and transform_script.strip():
        data = get_all_tables_as_dataframes(database)
        data["result"] = df_data_to_dataframe(df_data)
        script_log, entries = make_script_logger("transform")
        data = run_transform_script(transform_script, data, script_log)
        log_entries.extend(entries)
        result_df = data.get("result")
        if result_df is None:
            raise ValueError("The transform script must leave a DataFrame in data['result'].")
        df_data = dataframe_to_df_data(result_df)
    return df_data, log_entries


def compute_traces(df_data: dict, plot_type: str, custom_script: str, config: dict):
    if custom_script and custom_script.strip():
        log(f"Custom script plot")
        return run_custom_plot_script(custom_script, df_data, config)
    if plot_type in PLUGIN_PLOTS:
        log(f"Plugin plot '{plot_type}'")
        return PLUGIN_PLOTS[plot_type]["fn"](df_data, config)
    if plot_type in BUILTIN_PLOTS:
        traces = BUILTIN_PLOTS[plot_type]["fn"](df_data, config)
        log(f"Plot '{plot_type}': {len(traces)} trace(s)")
        return traces
    raise ValueError(f"Unknown plot type: {plot_type}")


def preview_of(df_data: dict, limit=500):
    return {
        "columns": df_data["columns"],
        "rows": df_data["rows"][:limit],
        "truncated": df_data.get("truncated", False) or len(df_data["rows"]) > limit,
    }


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
    def log_message(self, fmt, *args): pass

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
        elif path in ("/docs", "/docs.html"):
            try:
                html = DOCS_PATH.read_text(encoding="utf-8")
            except Exception as e:
                self.send_json({"error": f"Could not load {DOCS_PATH}: {e}"}, 500); return
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

        if path == "/api/preview":
            # Runs SQL + (optional) pandas transform and returns the
            # resulting table — this powers the single "Run & Show" button.
            try:
                db = payload["database"]
                sql = payload["sql"]
                transform_script = payload.get("transform_script", "")
                df_data, log_entries = run_pipeline(db, sql, transform_script)
                log(f"Preview on '{db}': {len(df_data['rows'])} row(s)")
                self.send_json({"ok": True, "preview": preview_of(df_data),
                                 "columns": df_data["columns"], "log_entries": log_entries})
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

                df_data, log_entries = run_pipeline(db, sql, transform_script)
                traces = compute_traces(df_data, plot_type, custom_script, config)
                layout = build_layout(config, layout_overrides)

                if layout_script.strip():
                    script_log, entries = make_script_logger("layout")
                    layout = run_layout_script(layout_script, layout, config, script_log)
                    log_entries.extend(entries)

                self.send_json({"traces": traces, "layout": layout,
                                "columns": df_data["columns"],
                                "truncated": df_data.get("truncated", False),
                                "preview": preview_of(df_data),
                                "log_entries": log_entries})
            except Exception as e:
                log(str(e), "error")
                self.send_json({"error": str(e), "traceback": traceback.format_exc()}, 400)

        elif path == "/api/plot_grid":
            # One figure made of several independently-configured subplots.
            try:
                rows = int(payload.get("rows", 1))
                cols = int(payload.get("cols", 1))
                panels = payload.get("panels", [])
                if rows < 1 or cols < 1:
                    raise ValueError("Grid rows/cols must be >= 1.")
                domains = compute_grid_domains(rows, cols)

                all_traces = []
                layout = {
                    "paper_bgcolor": "#ffffff", "plot_bgcolor": "#ffffff",
                    "font": dict(AXIS_FONT), "colorway": COLORWAY,
                    "margin": {"l": 20, "r": 20, "t": 30, "b": 20},
                    "annotations": [],
                }
                previews = []
                log_entries = []

                for i, panel in enumerate(panels):
                    if i >= len(domains):
                        break  # more panels than grid cells — ignore extras
                    suffix = "" if i == 0 else str(i + 1)
                    db, sql = panel["database"], panel["sql"]
                    plot_type = panel.get("plot_type", "line")
                    config = panel.get("config", {})
                    custom_script = panel.get("custom_script", "")
                    transform_script = panel.get("transform_script", "")
                    layout_script = panel.get("layout_script", "")

                    df_data, entries = run_pipeline(db, sql, transform_script)
                    log_entries.extend(entries)
                    traces = compute_traces(df_data, plot_type, custom_script, config)
                    for t in traces:
                        t["xaxis"] = f"x{suffix}"
                        t["yaxis"] = f"y{suffix}"
                    all_traces.extend(traces)

                    panel_layout = build_layout(config, {})
                    (x0, x1), (y0, y1) = domains[i]
                    xaxis = dict(panel_layout["xaxis"]); xaxis["domain"] = [x0, x1]
                    yaxis = dict(panel_layout["yaxis"]); yaxis["domain"] = [y0, y1]
                    layout[f"xaxis{suffix}"] = xaxis
                    layout[f"yaxis{suffix}"] = yaxis

                    if layout_script.strip():
                        script_log, lentries = make_script_logger(f"layout(panel {i+1})")
                        sub_layout = {"xaxis": xaxis, "yaxis": yaxis}
                        sub_layout = run_layout_script(layout_script, sub_layout, config, script_log)
                        log_entries.extend(lentries)
                        layout[f"xaxis{suffix}"] = sub_layout.get("xaxis", xaxis)
                        layout[f"yaxis{suffix}"] = sub_layout.get("yaxis", yaxis)

                    title = config.get("title")
                    if title:
                        layout["annotations"].append({
                            "text": title, "x": (x0 + x1) / 2, "y": y1 + 0.02,
                            "xref": "paper", "yref": "paper", "showarrow": False,
                            "xanchor": "center", "yanchor": "bottom",
                            "font": {"size": 13, **AXIS_FONT},
                        })

                    previews.append(preview_of(df_data, limit=200))

                self.send_json({"traces": all_traces, "layout": layout,
                                 "previews": previews, "log_entries": log_entries})
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
  # PLOT_LABEL       = "My Custom Plot"   # display name
  # PLOT_DESCRIPTION = "Does something"   # tooltip
  # PLOT_DEFAULTS    = {"param": 42}      # extra config fields

  # def plot(df_data, config):
  #     # df_data: {"columns": [...], "rows": [[...], ...]}
  #     # config: {"x": ..., "y": [...], "group": [...], "param": 42, ...}
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

    # Remember how this database was produced so "Re-parse" in the UI can
    # actually regenerate it from the job logs, not just re-read stale data.
    db_name = Path(db_path).stem
    if db_name in DB_REGISTRY:
        REPARSE_SOURCES[db_name] = {"parser": parser, "output_path": db_path}

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
    print(f"  Docs    : {url}/docs")
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