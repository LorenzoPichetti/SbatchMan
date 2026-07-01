"""
A self-contained local web server for interactive SQLite data visualization.

Plugin API — any .py in a plugins dir:
    PLOT_NAME        = "my_plot"
    PLOT_LABEL       = "My Custom Plot"
    PLOT_DESCRIPTION = "Does something"
    PLOT_DEFAULTS    = {"param": 42}
    def plot(df_data, config): ...  -> list[dict]  (Plotly traces)
"""

import importlib.util
import json
import sqlite3
import sys
import traceback
import shutil
import typer
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import urlparse
from typing import List
from rich.console import Console

from sbatchman.config.project_config import get_project_root
from sbatchman.parser import parse_jobs_and_generate_sqlite_db

console = Console(width=shutil.get_terminal_size().columns)

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
    # --- DEFAULT: simulate a fetch with a log message ---
    loaded = []
    messages = []
    for path in paths:
        # TODO: implement actual transfer, e.g.:
        #   local = Path("/tmp") / Path(path).name
        #   subprocess.run(["scp", f"{system}:{path}", str(local)], check=True)
        #   load_databases([str(local)])
        #   loaded.append(local.stem)
        messages.append(f"[stub] Would fetch {system}:{path}")
    return {
        "ok": True,
        "message": "; ".join(messages) or "Nothing selected.",
        "loaded_dbs": loaded,
    }


# ---------------------------------------------------------------------------
# Remote systems registry — customise with your actual systems/paths
# ---------------------------------------------------------------------------

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


def run_custom_plot_script(source, df_data, config):
    ns: dict = {}
    exec(compile(source, "<custom_plot>", "exec"), ns)
    if "plot" not in ns:
        raise ValueError("Custom script must define a `plot(df_data, config)` function.")
    return ns["plot"](df_data, config)


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
    entry = {"ts": datetime.now().strftime("%H:%M:%S"), "level": level, "msg": msg}
    SERVER_LOG.append(entry)
    print(f"[{entry['ts']}] [{level.upper()}] {msg}", file=sys.stderr)
    return entry


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
            self.send_html(HTML_APP)
        elif path == "/api/databases":
            schema = {n: get_db_schema(n) for n in DB_REGISTRY}
            self.send_json({"databases": schema})
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
            self.send_json({"systems": {s: paths for s, paths in REMOTE_SYSTEMS.items()}})
        elif path == "/api/logs":
            self.send_json({"logs": SERVER_LOG[-200:]})
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

        elif path == "/api/plot":
            try:
                db, sql = payload["database"], payload["sql"]
                plot_type = payload.get("plot_type", "line")
                config = payload.get("config", {})
                custom_script = payload.get("custom_script", "")
                layout_overrides = payload.get("layout", {})

                df_data = run_query(db, sql)
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
                self.send_json({"traces": traces, "layout": layout,
                                "columns": df_data["columns"],
                                "truncated": df_data.get("truncated", False)})
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
                                "log_entry": entry, "databases": schema})
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
                                "log_entry": entry, "databases": schema})
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
# HTML application (embedded)
# ---------------------------------------------------------------------------

HTML_APP = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>HPC Benchmark Plot Builder</title>
<script src="https://cdn.jsdelivr.net/npm/plotly.js-dist@2.32.0/plotly.min.js"></script>
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600&family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
<style>
:root {
  --bg:#0d1117; --bg2:#161b22; --bg3:#1c2128; --bg4:#21262d;
  --border:#30363d; --border2:#444c56;
  --text:#e6edf3; --text2:#8b949e; --text3:#6e7681;
  --accent:#00d4d4; --accent2:#58a6ff; --accent3:#3fb950;
  --warn:#f78166; --warn2:#e3b341; --plugin:#bc8cff;
  --radius:6px;
  --mono:'JetBrains Mono',monospace; --sans:'Inter',sans-serif;
}
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
html,body{height:100%;background:var(--bg);color:var(--text);font-family:var(--sans);font-size:13px;overflow:hidden}

/* ── Top-level layout: sidebar | centre | log-panel ── */
#app{display:grid;grid-template-columns:260px 1fr;grid-template-rows:46px 1fr;height:100vh}
#topbar{grid-column:1/-1;background:var(--bg2);border-bottom:1px solid var(--border);
        display:flex;align-items:center;padding:0 14px;gap:12px;z-index:10}
#sidebar{background:var(--bg2);border-right:1px solid var(--border);overflow-y:auto;display:flex;flex-direction:column}
#centre{display:flex;flex-direction:column;overflow:hidden;position:relative}

/* ── Log panel (right, collapsible) ── */
#log-panel{position:fixed;top:46px;right:0;bottom:0;width:0;background:var(--bg2);
           border-left:1px solid var(--border);display:flex;flex-direction:column;
           transition:width .2s ease;z-index:20;overflow:hidden}
#log-panel.open{width:320px}
#log-header{display:flex;align-items:center;padding:8px 12px;border-bottom:1px solid var(--border);gap:8px;flex-shrink:0}
#log-header span{font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:.08em;color:var(--text3);flex:1}
#log-body{flex:1;overflow-y:auto;padding:6px 0;font-family:var(--mono);font-size:10px}
.log-entry{padding:3px 12px;display:flex;gap:8px;border-bottom:1px solid rgba(48,54,61,.4);line-height:1.4}
.log-ts{color:var(--text3);flex-shrink:0}
.log-msg{color:var(--text2);word-break:break-all}
.log-entry.error .log-msg{color:var(--warn)}
.log-entry.warn  .log-msg{color:var(--warn2)}
.log-entry.info  .log-msg{color:var(--text2)}
#log-clear{font-size:10px;padding:2px 8px}

/* ── Topbar ── */
.logo{font-family:var(--mono);font-size:13px;font-weight:600;color:var(--accent);letter-spacing:-.02em;white-space:nowrap}
.logo span{color:var(--text2);font-weight:400}
.topbar-mid{flex:1;display:flex;align-items:center;gap:6px;overflow-x:auto;padding:0 8px}
.topbar-right{display:flex;gap:6px;flex-shrink:0}

/* ── Plot tabs ── */
#plot-tabs-bar{display:flex;align-items:center;background:var(--bg2);border-bottom:1px solid var(--border);
               padding:0 8px;gap:0;min-height:36px;overflow-x:auto}
.plot-tab{display:flex;align-items:center;gap:6px;padding:6px 14px;border-bottom:2px solid transparent;
          cursor:pointer;font-size:12px;color:var(--text2);white-space:nowrap;transition:all .12s;user-select:none}
.plot-tab:hover{color:var(--text)}
.plot-tab.active{color:var(--accent);border-bottom-color:var(--accent)}
.plot-tab-close{opacity:.4;font-size:14px;line-height:1;padding:0 2px}
.plot-tab-close:hover{opacity:1;color:var(--warn)}
#btn-add-tab{padding:4px 10px;font-size:18px;color:var(--text3);cursor:pointer;border:none;background:none;line-height:1}
#btn-add-tab:hover{color:var(--accent)}

/* ── Plot workspace (builder + plot) ── */
#workspaces{flex:1;overflow:hidden;position:relative}
.workspace{display:none;flex-direction:column;height:100%;overflow:hidden}
.workspace.active{display:flex}

/* ── Builder panel ── */
.builder{background:var(--bg2);border-bottom:1px solid var(--border);overflow-y:auto;max-height:50%;flex-shrink:0}
.builder-inner{display:flex;gap:0;flex-wrap:wrap}

/* Builder columns */
.b-col{padding:10px 14px;border-right:1px solid var(--border);display:flex;flex-direction:column;gap:7px;min-width:180px}
.b-col:last-child{border-right:none;flex:1}
.b-col-title{font-size:9px;font-weight:600;text-transform:uppercase;letter-spacing:.1em;color:var(--text3);margin-bottom:2px}

/* Plot chips */
.plot-type-grid{display:flex;flex-wrap:wrap;gap:4px}
.plot-chip{padding:3px 9px;border-radius:12px;border:1px solid var(--border);background:transparent;
           color:var(--text2);font-size:10px;cursor:pointer;transition:all .12s;font-family:var(--sans)}
.plot-chip:hover{border-color:var(--accent);color:var(--accent)}
.plot-chip.active{border-color:var(--accent);background:rgba(0,212,212,.12);color:var(--accent);font-weight:500}
.plot-chip.plugin{border-style:dashed;color:var(--plugin);border-color:var(--plugin)}
.plot-chip.plugin.active{background:rgba(188,140,255,.12)}

/* Form elements */
label{font-size:10px;color:var(--text2);display:block;margin-bottom:3px}
input,select,textarea{width:100%;background:var(--bg3);border:1px solid var(--border);border-radius:var(--radius);
  color:var(--text);font-family:var(--mono);font-size:11px;padding:5px 7px;transition:border-color .15s}
input:focus,select:focus,textarea:focus{outline:none;border-color:var(--accent)}
select option{background:var(--bg3)}
textarea{resize:vertical}

/* Y-col multi-select pills */
.multi-col-list{display:flex;flex-wrap:wrap;gap:3px;min-height:28px;max-height:72px;overflow-y:auto;
  padding:4px;background:var(--bg3);border:1px solid var(--border);border-radius:var(--radius)}
.col-pill{display:inline-flex;align-items:center;gap:2px;padding:2px 7px;border-radius:10px;
  font-size:9px;font-family:var(--mono);cursor:pointer;border:1px solid var(--border);
  background:var(--bg4);color:var(--text2);transition:all .1s}
.col-pill:hover{border-color:var(--accent2);color:var(--accent2)}
.col-pill.selected{background:rgba(88,166,255,.15);border-color:var(--accent2);color:var(--accent2)}

/* Custom script */
.script-toggle{display:flex;align-items:center;gap:5px;cursor:pointer;color:var(--text2);
  font-size:10px;padding:4px 0;user-select:none}
.script-toggle:hover{color:var(--accent)}
.script-box{display:none}
.script-box.open{display:block}

/* Extra options */
.extra-opts{display:flex;flex-wrap:wrap;gap:6px}
.extra-opts>div{flex:1;min-width:90px}

/* Plot area */
.plot-area{flex:1;display:flex;flex-direction:column;overflow:hidden;min-height:0}
.plotly-wrap{flex:1;min-height:0;position:relative}
.plotly-div{width:100%;height:100%}
.plot-overlay{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;
  flex-direction:column;gap:10px;color:var(--text3);font-size:12px;pointer-events:none}
.overlay-icon{font-size:36px;opacity:.25}
.statusbar{padding:5px 14px;font-size:10px;background:var(--bg2);border-top:1px solid var(--border);
  font-family:var(--mono);display:flex;gap:14px;align-items:center;flex-shrink:0}
.s-ok{color:var(--accent3)} .s-err{color:var(--warn)} .s-warn{color:var(--warn2)}

/* Buttons */
.btn{display:inline-flex;align-items:center;gap:5px;padding:4px 11px;border-radius:var(--radius);
  border:1px solid var(--border2);background:transparent;color:var(--text);font-size:11px;
  font-family:var(--sans);cursor:pointer;transition:all .15s;white-space:nowrap}
.btn:hover{border-color:var(--accent);color:var(--accent);background:rgba(0,212,212,.07)}
.btn.primary{background:var(--accent);border-color:var(--accent);color:#000;font-weight:600}
.btn.primary:hover{background:#00b8b8}
.btn.sm{padding:3px 8px;font-size:10px}
.btn:disabled{opacity:.35;pointer-events:none}

/* Sidebar */
.sb-tabs{display:flex;border-bottom:1px solid var(--border)}
.sb-tab{flex:1;padding:7px;text-align:center;font-size:10px;cursor:pointer;color:var(--text2);
  border-bottom:2px solid transparent;transition:all .12s}
.sb-tab:hover{color:var(--text)}
.sb-tab.active{color:var(--accent);border-bottom-color:var(--accent)}
.sb-pane{display:none;flex-direction:column;overflow-y:auto}
.sb-pane.active{display:flex}
.sb-section{border-bottom:1px solid var(--border)}
.sb-section-hdr{padding:8px 12px 6px;font-size:9px;font-weight:600;text-transform:uppercase;
  letter-spacing:.08em;color:var(--text3);display:flex;align-items:center;justify-content:space-between}
.sb-section-body{padding:0 12px 10px}

/* Schema tree */
.db-item{margin-bottom:4px}
.db-name{font-family:var(--mono);font-size:10px;font-weight:600;color:var(--accent2);padding:3px 0;
  cursor:pointer;display:flex;align-items:center;gap:4px}
.db-name:hover{color:var(--accent)}
.table-list{margin-left:10px}
.table-item{display:flex;align-items:center;gap:5px;padding:2px 4px;border-radius:4px;cursor:pointer;
  color:var(--text2);font-family:var(--mono);font-size:10px;transition:all .12s}
.table-item:hover{background:var(--bg4);color:var(--text)}
.table-cols{margin-left:18px;margin-top:1px}
.col-item{font-family:var(--mono);font-size:9px;color:var(--text3);padding:1px 0;display:flex;align-items:center;gap:3px}
.col-type{color:var(--warn2);font-size:8px}
.badge{font-size:8px;padding:1px 4px;border-radius:3px;background:var(--bg4);color:var(--text3);font-family:var(--mono)}

/* Actions panel */
.action-btn{display:flex;align-items:center;gap:6px;padding:5px 8px;border-radius:var(--radius);
  border:1px solid var(--border);background:var(--bg3);color:var(--text2);font-size:10px;
  cursor:pointer;transition:all .12s;width:100%;text-align:left;margin-bottom:5px;font-family:var(--sans)}
.action-btn:hover{border-color:var(--accent);color:var(--accent);background:rgba(0,212,212,.05)}
.action-btn svg{flex-shrink:0;opacity:.6}
.fetch-system-sel{margin-bottom:6px}
.fetch-path-list{max-height:120px;overflow-y:auto;background:var(--bg3);border:1px solid var(--border);
  border-radius:var(--radius);padding:4px}
.fetch-path-item{display:flex;align-items:center;gap:5px;padding:2px 4px;font-family:var(--mono);
  font-size:9px;color:var(--text2);border-radius:3px;cursor:pointer}
.fetch-path-item:hover{background:var(--bg4)}
.fetch-path-item input{width:auto;flex-shrink:0;height:12px;width:12px;padding:0;cursor:pointer}
#fetch-run-btn{margin-top:6px;width:100%}

/* Scrollbar */
::-webkit-scrollbar{width:5px;height:5px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:var(--border2);border-radius:3px}

/* Tooltip */
[data-tip]{position:relative}
[data-tip]:hover::after{content:attr(data-tip);position:absolute;bottom:calc(100% + 6px);left:50%;
  transform:translateX(-50%);background:var(--bg4);border:1px solid var(--border2);padding:4px 9px;
  border-radius:var(--radius);font-size:10px;white-space:nowrap;z-index:200;pointer-events:none;
  color:var(--text);font-family:var(--sans);box-shadow:0 4px 12px rgba(0,0,0,.4)}

/* Log toggle button */
#btn-log-toggle{position:relative}
.log-badge{position:absolute;top:-4px;right:-4px;background:var(--warn);color:#000;
  border-radius:8px;font-size:8px;padding:1px 4px;font-weight:700;min-width:14px;text-align:center}

@keyframes flash{0%,100%{opacity:1}50%{opacity:.3}}
.flash{animation:flash .4s}
</style>
</head>
<body>
<div id="app">

<!-- ═══ TOPBAR ═══ -->
<div id="topbar">
  <div class="logo">hpc<span>bench</span>.plot</div>

  <div class="topbar-mid">
    <button class="btn primary" id="btn-run" data-tip="Render the active plot (Ctrl+Enter)">
      <svg width="12" height="12" viewBox="0 0 16 16" fill="currentColor"><path d="M3 2l10 6-10 6V2z"/></svg>Run
    </button>
    <button class="btn" id="btn-export-config" data-tip="Export active tab config as JSON">
      <svg width="12" height="12" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M8 2v8M5 7l3 3 3-3M2 12v2h12v-2"/></svg>Export config
    </button>
    <button class="btn" id="btn-import-config" data-tip="Import a JSON config into the active tab">
      <svg width="12" height="12" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M8 11V3M5 6l3-3 3 3M2 12v2h12v-2"/></svg>Import config
    </button>
    <button class="btn" id="btn-export-all" data-tip="Export ALL tabs as a single JSON workspace file">
      <svg width="12" height="12" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="2" y="2" width="10" height="12" rx="1"/><path d="M5 2v12M12 6h2v8H5"/></svg>Export workspace
    </button>
    <button class="btn" id="btn-import-all" data-tip="Import a workspace JSON (replaces all tabs)">
      <svg width="12" height="12" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="2" y="2" width="10" height="12" rx="1"/><path d="M5 2v12M14 6h-2V2"/></svg>Import workspace
    </button>
    <button class="btn" id="btn-export-png" data-tip="Save active plot as PNG (1600×900)">
      <svg width="12" height="12" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="2" y="2" width="12" height="12" rx="2"/><path d="M2 10l3-3 3 3 2-2 4 4"/></svg>Save PNG
    </button>
  </div>

  <div class="topbar-right">
    <button class="btn" id="btn-log-toggle" data-tip="Toggle log panel">
      <svg width="12" height="12" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="2" y="3" width="12" height="10" rx="1"/><path d="M5 6h6M5 9h4"/></svg>
      Logs<span class="log-badge" id="log-badge" style="display:none">0</span>
    </button>
  </div>
</div>

<!-- ═══ SIDEBAR ═══ -->
<div id="sidebar">
  <div class="sb-tabs">
    <div class="sb-tab active" data-sbtab="schema">Schema</div>
    <div class="sb-tab" data-sbtab="actions">Actions</div>
  </div>

  <!-- Schema tab -->
  <div id="sbtab-schema" class="sb-pane active">
    <div class="sb-section">
      <div class="sb-section-hdr">Databases <span id="db-count" class="badge">0</span></div>
      <div class="sb-section-body" id="schema-tree">Loading…</div>
    </div>
  </div>

  <!-- Actions tab -->
  <div id="sbtab-actions" class="sb-pane">
    <!-- Re-parse -->
    <div class="sb-section">
      <div class="sb-section-hdr" data-tip="Re-read SQLite file(s) from disk and refresh schema">Re-parse data</div>
      <div class="sb-section-body">
        <div style="margin-bottom:6px">
          <label>Database</label>
          <select id="reparse-db-sel"></select>
        </div>
        <button class="action-btn" id="btn-reparse" data-tip="Calls hook_reparse() — customise in the Python backend">
          <svg width="12" height="12" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M2 8a6 6 0 1 0 1-3.5"/><path d="M2 2v4h4"/></svg>
          Re-parse selected DB
        </button>
        <div id="reparse-result" style="font-size:10px;color:var(--text3);font-family:var(--mono);margin-top:4px"></div>
      </div>
    </div>

    <!-- Remote fetch -->
    <div class="sb-section">
      <div class="sb-section-hdr" data-tip="Fetch remote SQLite files — configure systems in REMOTE_SYSTEMS dict in the Python backend">Fetch from remote</div>
      <div class="sb-section-body">
        <div class="fetch-system-sel">
          <label>System</label>
          <select id="fetch-system-sel" onchange="onFetchSystemChange()"></select>
        </div>
        <label data-tip="Check paths you want to pull; uncheck to skip">Paths (select to fetch)</label>
        <div class="fetch-path-list" id="fetch-path-list"><span style="color:var(--text3);font-size:9px">Select a system</span></div>
        <button class="btn sm" id="fetch-run-btn" onclick="runFetch()" data-tip="Calls hook_fetch_remote() — customise in the Python backend">
          <svg width="10" height="10" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M8 11V3M5 6l3-3 3 3M2 12v2h12v-2"/></svg>
          Fetch selected
        </button>
        <div id="fetch-result" style="font-size:10px;color:var(--text3);font-family:var(--mono);margin-top:4px"></div>
      </div>
    </div>

    <!-- Plugins -->
    <div class="sb-section">
      <div class="sb-section-hdr" data-tip="Python plugin files that define custom plot types">Plugins</div>
      <div class="sb-section-body">
        <button class="action-btn" id="btn-reload-plugins" data-tip="Reload all plugin .py files from disk without restarting the server">
          <svg width="12" height="12" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M2 8a6 6 0 1 0 1-3.5"/><path d="M2 2v4h4"/></svg>
          Reload plugins
        </button>
        <div id="plugin-list" style="font-size:9px;color:var(--text3);font-family:var(--mono)"></div>
      </div>
    </div>
  </div>
</div>

<!-- ═══ CENTRE ═══ -->
<div id="centre">
  <!-- Plot tabs bar -->
  <div id="plot-tabs-bar">
    <!-- tabs injected by JS -->
    <button id="btn-add-tab" data-tip="Add a new plot tab (clones the active one)">＋</button>
  </div>

  <!-- Workspaces -->
  <div id="workspaces"></div>
</div>

<!-- ═══ LOG PANEL ═══ -->
<div id="log-panel">
  <div id="log-header">
    <span>Logs</span>
    <button class="btn sm" id="log-clear">Clear</button>
    <button class="btn sm" id="log-close">✕</button>
  </div>
  <div id="log-body"></div>
</div>

<!-- Hidden file inputs -->
<input type="file" id="import-file-input"    accept=".json" style="display:none">
<input type="file" id="import-ws-file-input" accept=".json" style="display:none">

<script>
// ═══════════════════════════════════════════════════════════════
// Global state
// ═══════════════════════════════════════════════════════════════
const G = {
  databases:  {},   // name -> {tableName: [{name,type}]}
  plotTypes:  {},   // key -> descriptor
  remoteSystems: {}, // system -> [paths]
  tabs:       [],   // [{id, label, state}]
  activeTab:  null,
  nextTabId:  1,
  logCount:   0,
};

// ═══════════════════════════════════════════════════════════════
// API
// ═══════════════════════════════════════════════════════════════
async function api(method, path, body) {
  const opts = {method, headers:{'Content-Type':'application/json'}};
  if (body) opts.body = JSON.stringify(body);
  const r = await fetch(path, opts);
  return r.json();
}

function clientLog(msg, level='info') {
  addLogEntry({ts: new Date().toLocaleTimeString('en',{hour12:false}), level, msg});
  api('POST', '/api/log', {message: msg, level});
}

// ═══════════════════════════════════════════════════════════════
// Log panel
// ═══════════════════════════════════════════════════════════════
function addLogEntry(e) {
  const body = document.getElementById('log-body');
  const div = document.createElement('div');
  div.className = `log-entry ${e.level}`;
  div.innerHTML = `<span class="log-ts">${e.ts}</span><span class="log-msg">${escHtml(e.msg)}</span>`;
  body.appendChild(div);
  body.scrollTop = body.scrollHeight;
  G.logCount++;
  const badge = document.getElementById('log-badge');
  const panel = document.getElementById('log-panel');
  if (!panel.classList.contains('open')) {
    badge.textContent = G.logCount;
    badge.style.display = 'block';
  }
}

function escHtml(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

document.getElementById('btn-log-toggle').addEventListener('click', () => {
  const p = document.getElementById('log-panel');
  p.classList.toggle('open');
  if (p.classList.contains('open')) {
    G.logCount = 0;
    document.getElementById('log-badge').style.display = 'none';
  }
});
document.getElementById('log-close').addEventListener('click', () => {
  document.getElementById('log-panel').classList.remove('open');
});
document.getElementById('log-clear').addEventListener('click', () => {
  document.getElementById('log-body').innerHTML = '';
  G.logCount = 0;
  document.getElementById('log-badge').style.display = 'none';
});

// Poll server logs on init then stop
async function fetchServerLogs() {
  const res = await api('GET', '/api/logs');
  (res.logs || []).forEach(e => addLogEntry(e));
}

// ═══════════════════════════════════════════════════════════════
// Schema tree
// ═══════════════════════════════════════════════════════════════
function renderSchemaTree() {
  const el = document.getElementById('schema-tree');
  const dbs = G.databases;
  document.getElementById('db-count').textContent = Object.keys(dbs).length;
  if (!Object.keys(dbs).length) { el.innerHTML = '<span style="color:var(--text3)">No databases loaded</span>'; return; }
  el.innerHTML = '';
  for (const [dbName, tables] of Object.entries(dbs)) {
    const dbDiv = document.createElement('div');
    dbDiv.className = 'db-item';
    const nameEl = document.createElement('div');
    nameEl.className = 'db-name';
    nameEl.innerHTML = `<svg width="9" height="9" viewBox="0 0 16 16" fill="currentColor"><ellipse cx="8" cy="4" rx="6" ry="2.5"/><path d="M2 4v4c0 1.38 2.69 2.5 6 2.5S14 9.38 14 8V4"/><path d="M2 8v4c0 1.38 2.69 2.5 6 2.5S14 13.38 14 12V8"/></svg> ${dbName}`;
    let open = false;
    const tableList = document.createElement('div');
    tableList.className = 'table-list';
    tableList.style.display = 'none';
    for (const [tname, cols] of Object.entries(tables || {})) {
      const ti = document.createElement('div');
      ti.className = 'table-item';
      ti.innerHTML = `<svg width="9" height="9" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="1" y="1" width="14" height="14" rx="1"/><line x1="1" y1="5.5" x2="15" y2="5.5"/><line x1="6" y1="5.5" x2="6" y2="15"/></svg> ${tname} <span class="badge">${cols.length}</span>`;
      let colsOpen = false;
      const colsDiv = document.createElement('div');
      colsDiv.className = 'table-cols';
      colsDiv.style.display = 'none';
      cols.forEach(c => {
        const ci = document.createElement('div');
        ci.className = 'col-item';
        ci.innerHTML = `▸ ${c.name} <span class="col-type">${c.type}</span>`;
        colsDiv.appendChild(ci);
      });
      ti.addEventListener('click', e => {
        e.stopPropagation();
        colsOpen = !colsOpen;
        colsDiv.style.display = colsOpen ? 'block' : 'none';
        const tab = activeTabObj();
        if (tab) {
          tabEl(tab.id, 'db-select').value = dbName;
          tab.state.database = dbName;
          const sql = tabEl(tab.id, 'sql-input');
          if (!sql.value.trim()) sql.value = `SELECT * FROM ${tname} LIMIT 1000`;
        }
      });
      tableList.appendChild(ti);
      tableList.appendChild(colsDiv);
    }
    nameEl.addEventListener('click', () => { open = !open; tableList.style.display = open ? 'block' : 'none'; });
    dbDiv.appendChild(nameEl); dbDiv.appendChild(tableList);
    el.appendChild(dbDiv);
  }
}

// ═══════════════════════════════════════════════════════════════
// Plot type chips (per workspace)
// ═══════════════════════════════════════════════════════════════
function renderPlotChips(tabId) {
  const wrap = document.getElementById(`chips-${tabId}`);
  if (!wrap) return;
  wrap.innerHTML = '';
  const tab = G.tabs.find(t => t.id === tabId);
  if (!tab) return;
  for (const [key, pt] of Object.entries(G.plotTypes)) {
    const ch = document.createElement('div');
    ch.className = 'plot-chip' + (pt.is_plugin ? ' plugin' : '') + (key === tab.state.plotType ? ' active' : '');
    ch.textContent = pt.label;
    ch.title = pt.description;
    ch.dataset.type = key;
    ch.addEventListener('click', () => {
      tab.state.plotType = key;
      wrap.querySelectorAll('.plot-chip').forEach(c => c.classList.toggle('active', c.dataset.type === key));
      renderExtraOpts(tabId);
    });
    wrap.appendChild(ch);
  }
}

function renderExtraOpts(tabId) {
  const tab = G.tabs.find(t => t.id === tabId);
  const area = document.getElementById(`extra-opts-${tabId}`);
  if (!tab || !area) return;
  area.innerHTML = '';
  const pt = G.plotTypes[tab.state.plotType];
  if (!pt) return;
  for (const [key, val] of Object.entries(pt.defaults || {})) {
    const wrap = document.createElement('div');
    const lbl = document.createElement('label'); lbl.textContent = key;
    const inp = document.createElement('input');
    inp.id = `extra-${tabId}-${key}`;
    inp.value = tab.state.extra?.[key] ?? val;
    inp.type = typeof val === 'number' ? 'number' : 'text';
    wrap.appendChild(lbl); wrap.appendChild(inp);
    area.appendChild(wrap);
  }
}

// ═══════════════════════════════════════════════════════════════
// Axis / column controls (per tab)
// ═══════════════════════════════════════════════════════════════
function updateAxisControls(tabId, columns) {
  const tab = G.tabs.find(t => t.id === tabId);
  if (!tab) return;
  tab.state.columns = columns;
  tab.state.yCols = (tab.state.yCols || []).filter(c => columns.includes(c));

  const xSel   = tabEl(tabId, 'x-col');
  const grpSel = tabEl(tabId, 'group-col');
  const zSel   = tabEl(tabId, 'z-col');
  const pills  = document.getElementById(`y-pills-${tabId}`);
  if (!xSel || !grpSel || !pills) return;

  const prevX = xSel.value, prevGrp = grpSel.value, prevZ = zSel?.value;
  const mkOpt = (v, sel) => { const o = document.createElement('option'); o.value=v; o.textContent=v; if(v===sel)o.selected=true; return o; };

  xSel.innerHTML   = '<option value="">— x column —</option>';
  grpSel.innerHTML = '<option value="">— none —</option>';
  if (zSel) zSel.innerHTML = '<option value="">— none —</option>';
  pills.innerHTML  = '';

  for (const col of columns) {
    xSel.appendChild(mkOpt(col, prevX || tab.state.x));
    grpSel.appendChild(mkOpt(col, prevGrp || tab.state.group));
    if (zSel) zSel.appendChild(mkOpt(col, prevZ || tab.state.z));
    const pill = document.createElement('div');
    pill.className = 'col-pill' + (tab.state.yCols.includes(col) ? ' selected' : '');
    pill.textContent = col; pill.dataset.col = col;
    pill.addEventListener('click', () => {
      if (tab.state.yCols.includes(col)) {
        tab.state.yCols = tab.state.yCols.filter(c => c !== col);
        pill.classList.remove('selected');
      } else {
        tab.state.yCols.push(col);
        pill.classList.add('selected');
      }
    });
    pills.appendChild(pill);
  }
}

// ═══════════════════════════════════════════════════════════════
// Tab management
// ═══════════════════════════════════════════════════════════════
function defaultTabState(base) {
  return Object.assign({
    database: Object.keys(G.databases)[0] || '',
    sql: '', plotType: 'line', columns: [], yCols: [],
    x:'', group:'', z:'', extra:{},
    chartTitle:'', xLabel:'', yLabel:'', xScale:'linear', yScale:'linear',
    xTickFmt:'', yTickFmt:'', customScript:'',
  }, base || {});
}

function createTab(label, stateOverride) {
  const id = G.nextTabId++;
  const state = defaultTabState(stateOverride);
  const tab = {id, label: label || `Plot ${id}`, state};
  G.tabs.push(tab);

  // Create tab button
  const bar = document.getElementById('plot-tabs-bar');
  const btn = document.createElement('div');
  btn.className = 'plot-tab';
  btn.id = `tabbtn-${id}`;
  btn.innerHTML = `<span class="tab-label" ondblclick="renameTab(${id})">${escHtml(tab.label)}</span><span class="plot-tab-close" onclick="closeTab(${id},event)">×</span>`;
  btn.addEventListener('click', () => activateTab(id));
  bar.insertBefore(btn, document.getElementById('btn-add-tab'));

  // Create workspace
  const ws = document.createElement('div');
  ws.className = 'workspace';
  ws.id = `ws-${id}`;
  ws.innerHTML = workspaceHTML(id);
  document.getElementById('workspaces').appendChild(ws);

  // Wire up events in this workspace
  wireWorkspace(id);
  activateTab(id);
  return id;
}

function workspaceHTML(id) {
  const dbOpts = Object.keys(G.databases).map(n => `<option value="${n}">${n}</option>`).join('');
  return `
<div class="builder">
  <div class="builder-inner">
    <!-- Col 1: Plot type + custom script -->
    <div class="b-col" style="min-width:200px;max-width:240px">
      <div class="b-col-title" data-tip="Choose how data is rendered">Plot type</div>
      <div class="plot-type-grid" id="chips-${id}"></div>
      <div style="margin-top:6px">
        <div class="script-toggle" id="script-toggle-${id}" data-tip="Define a custom plot() function in Python — overrides the plot type above">
          <svg width="10" height="10" viewBox="0 0 16 16" fill="currentColor"><path d="M9.5 2l4 6-4 6h-3l4-6-4-6h3zm-6 0l4 6-4 6H0l4-6L0 2h3.5z"/></svg>
          Custom script
          <svg id="script-arrow-${id}" width="9" height="9" viewBox="0 0 16 16" fill="currentColor" style="margin-left:auto;transition:transform .2s"><path d="M4 6l4 4 4-4"/></svg>
        </div>
        <div class="script-box" id="script-box-${id}">
          <div style="font-size:9px;color:var(--text3);margin-bottom:4px;font-family:var(--mono)">def <span style="color:var(--accent)">plot</span>(df_data, config) → traces</div>
          <textarea id="script-${id}" rows="6" placeholder="def plot(df_data, config):&#10;    # return list of Plotly trace dicts&#10;    ..."></textarea>
        </div>
      </div>
      <div class="extra-opts" id="extra-opts-${id}"></div>
    </div>

    <!-- Col 2: SQL -->
    <div class="b-col" style="min-width:240px;flex:1.2">
      <div class="b-col-title">SQL query</div>
      <select id="db-select-${id}" style="margin-bottom:5px">${dbOpts}</select>
      <textarea id="sql-input-${id}" rows="5" spellcheck="false" placeholder="SELECT * FROM results LIMIT 1000" style="font-size:11px;flex:1"></textarea>
      <button class="btn sm" id="run-query-btn-${id}" data-tip="Run SQL without rendering — populates column selectors">Load columns</button>
    </div>

    <!-- Col 3: Axes -->
    <div class="b-col" style="min-width:180px">
      <div class="b-col-title">Axes</div>
      <label data-tip="Column mapped to the X axis">X column</label>
      <select id="x-col-${id}"><option value="">— x column —</option></select>
      <label style="margin-top:5px" data-tip="One or more columns mapped to Y — click to toggle">Y column(s)</label>
      <div class="multi-col-list" id="y-pills-${id}"></div>
      <label style="margin-top:5px" data-tip="Split data into separate traces by this column's values">Group / color by</label>
      <select id="group-col-${id}"><option value="">— none —</option></select>
      <label style="margin-top:5px" data-tip="Z column for heatmaps">Z column (heatmap)</label>
      <select id="z-col-${id}"><option value="">— none —</option></select>
    </div>

    <!-- Col 4: Labels + scales -->
    <div class="b-col" style="min-width:160px">
      <div class="b-col-title">Labels &amp; scales</div>
      <label>Chart title</label>
      <input type="text" id="chart-title-${id}" placeholder="My benchmark">
      <label style="margin-top:5px">X axis label</label>
      <input type="text" id="x-label-${id}" placeholder="(auto from column)">
      <label style="margin-top:5px">Y axis label</label>
      <input type="text" id="y-label-${id}" placeholder="(auto from column)">
      <label style="margin-top:5px" data-tip="linear, log, date, category">X scale</label>
      <select id="x-scale-${id}">
        <option value="linear">linear</option><option value="log">log</option>
        <option value="date">date</option><option value="category">category</option>
      </select>
      <label style="margin-top:5px" data-tip="linear, log, date, category">Y scale</label>
      <select id="y-scale-${id}">
        <option value="linear">linear</option><option value="log">log</option>
        <option value="date">date</option><option value="category">category</option>
      </select>
      <label style="margin-top:5px" data-tip="Plotly d3 tick format string, e.g. .2f or %Y-%m">X tick format</label>
      <input type="text" id="x-tickfmt-${id}" placeholder=".2f">
      <label style="margin-top:5px">Y tick format</label>
      <input type="text" id="y-tickfmt-${id}" placeholder=".2f">
    </div>
  </div>
</div>

<div class="plot-area">
  <div class="plotly-wrap">
    <div class="plotly-div" id="plot-${id}"></div>
    <div class="plot-overlay" id="overlay-${id}">
      <div class="overlay-icon">⬡</div>
      <div>Write a SQL query, configure axes, then click <strong>Run</strong></div>
    </div>
  </div>
  <div class="statusbar">
    <span id="status-${id}" class="s-ok">Ready</span>
    <span id="status-rows-${id}" style="margin-left:auto;color:var(--text3)"></span>
  </div>
</div>`;
}

function wireWorkspace(id) {
  const tab = G.tabs.find(t => t.id === id);
  if (!tab) return;

  renderPlotChips(id);
  renderExtraOpts(id);

  // Restore state into form fields
  const s = tab.state;
  const f = (sid, val) => { const el = document.getElementById(sid); if(el && val!==undefined) el.value = val; };
  f(`db-select-${id}`, s.database);
  f(`sql-input-${id}`, s.sql);
  f(`chart-title-${id}`, s.chartTitle);
  f(`x-label-${id}`, s.xLabel);
  f(`y-label-${id}`, s.yLabel);
  f(`x-scale-${id}`, s.xScale);
  f(`y-scale-${id}`, s.yScale);
  f(`x-tickfmt-${id}`, s.xTickFmt);
  f(`y-tickfmt-${id}`, s.yTickFmt);
  f(`script-${id}`, s.customScript);

  if (s.columns && s.columns.length) updateAxisControls(id, s.columns);

  // Custom script toggle
  document.getElementById(`script-toggle-${id}`).addEventListener('click', () => {
    const box = document.getElementById(`script-box-${id}`);
    const arrow = document.getElementById(`script-arrow-${id}`);
    const open = box.classList.toggle('open');
    arrow.style.transform = open ? 'rotate(180deg)' : '';
  });

  // SQL blur → load columns
  document.getElementById(`sql-input-${id}`).addEventListener('blur', () => {
    if (document.getElementById(`sql-input-${id}`).value.trim()) loadColumns(id);
  });
  document.getElementById(`run-query-btn-${id}`).addEventListener('click', () => loadColumns(id));
}

function tabEl(id, suffix) {
  return document.getElementById(`${suffix}-${id}`);
}

function activateTab(id) {
  G.activeTab = id;
  document.querySelectorAll('.plot-tab').forEach(b => b.classList.toggle('active', b.id === `tabbtn-${id}`));
  document.querySelectorAll('.workspace').forEach(w => w.classList.toggle('active', w.id === `ws-${id}`));
}

function closeTab(id, ev) {
  ev?.stopPropagation();
  if (G.tabs.length <= 1) return; // keep at least one tab
  G.tabs = G.tabs.filter(t => t.id !== id);
  document.getElementById(`tabbtn-${id}`)?.remove();
  document.getElementById(`ws-${id}`)?.remove();
  if (G.activeTab === id) activateTab(G.tabs[G.tabs.length - 1].id);
}

function renameTab(id) {
  const tab = G.tabs.find(t => t.id === id);
  if (!tab) return;
  const label = prompt('Rename tab:', tab.label);
  if (label && label.trim()) {
    tab.label = label.trim();
    const el = document.querySelector(`#tabbtn-${id} .tab-label`);
    if (el) el.textContent = tab.label;
  }
}

function activeTabObj() {
  return G.tabs.find(t => t.id === G.activeTab);
}

document.getElementById('btn-add-tab').addEventListener('click', () => {
  const src = activeTabObj();
  const baseState = src ? JSON.parse(JSON.stringify(src.state)) : {};
  const newId = createTab(`Plot ${G.nextTabId}`, baseState);
  clientLog(`New tab created (cloned from "${src?.label}")`);
});

// ═══════════════════════════════════════════════════════════════
// Run query (column loading)
// ═══════════════════════════════════════════════════════════════
async function loadColumns(tabId) {
  const db  = tabEl(tabId, 'db-select').value;
  const sql = tabEl(tabId, 'sql-input').value.trim();
  if (!db || !sql) return;
  try {
    const res = await api('POST', '/api/query', {database:db, sql, limit:50});
    if (res.error) throw new Error(res.error);
    updateAxisControls(tabId, res.columns);
    setStatus(tabId, `Columns loaded (${res.columns.length})`, 'warn');
    clientLog(`Columns loaded for tab "${G.tabs.find(t=>t.id===tabId)?.label}": ${res.columns.join(', ')}`);
  } catch(e) {
    setStatus(tabId, e.message, 'err');
    clientLog(e.message, 'error');
  }
}

// ═══════════════════════════════════════════════════════════════
// Run plot
// ═══════════════════════════════════════════════════════════════
async function runPlot() {
  const tabId = G.activeTab;
  const tab = activeTabObj();
  if (!tab) return;

  const db  = tabEl(tabId, 'db-select').value;
  const sql = tabEl(tabId, 'sql-input').value.trim();
  if (!db || !sql) { setStatus(tabId,'Set a database and SQL query first','err'); return; }

  // Collect extra opts
  const pt = G.plotTypes[tab.state.plotType] || {};
  const extra = {};
  for (const key of Object.keys(pt.defaults || {})) {
    const inp = document.getElementById(`extra-${tabId}-${key}`);
    if (inp) extra[key] = isNaN(inp.value) ? inp.value : Number(inp.value);
  }

  const config = {
    title:       tabEl(tabId,'chart-title').value,
    x:           tabEl(tabId,'x-col').value,
    y:           tab.state.yCols,
    group:       tabEl(tabId,'group-col').value,
    z:           tabEl(tabId,'z-col').value,
    x_label:     tabEl(tabId,'x-label').value,
    y_label:     tabEl(tabId,'y-label').value,
    x_scale:     tabEl(tabId,'x-scale').value,
    y_scale:     tabEl(tabId,'y-scale').value,
    x_tickformat:tabEl(tabId,'x-tickfmt').value,
    y_tickformat:tabEl(tabId,'y-tickfmt').value,
    ...extra,
  };
  const customScript = tabEl(tabId,'script').value;

  setStatus(tabId,'Running…','');
  document.getElementById('btn-run').disabled = true;

  try {
    const res = await api('POST','/api/plot',{
      database:db, sql, plot_type:tab.state.plotType,
      config, custom_script:customScript,
    });
    document.getElementById('btn-run').disabled = false;
    if (res.error) { setStatus(tabId, res.error, 'err'); clientLog(res.error,'error'); return; }

    if (res.columns?.length) updateAxisControls(tabId, res.columns);

    document.getElementById(`overlay-${tabId}`).style.display = 'none';
    Plotly.react(`plot-${tabId}`, res.traces, res.layout, {
      responsive:true, displaylogo:false,
      modeBarButtonsToRemove:['sendDataToCloud'],
    });
    tab.state.plotted = true;
    const hint = res.truncated ? ' (truncated to 10k rows)' : '';
    setStatus(tabId, `OK — ${res.traces.length} trace(s), ${res.columns?.length||0} column(s)${hint}`, 'ok');
    clientLog(`Plot rendered: "${tab.label}" — ${res.traces.length} trace(s)${hint}`);
  } catch(e) {
    document.getElementById('btn-run').disabled = false;
    setStatus(tabId, e.message, 'err');
    clientLog(e.message, 'error');
  }
}

function setStatus(tabId, msg, level) {
  const el = document.getElementById(`status-${tabId}`);
  if (!el) return;
  el.textContent = msg;
  el.className = level==='err' ? 's-err' : level==='warn' ? 's-warn' : 's-ok';
}

// ═══════════════════════════════════════════════════════════════
// Config import / export (single tab)
// ═══════════════════════════════════════════════════════════════
function getTabConfig(tabId) {
  const tab = G.tabs.find(t => t.id === tabId);
  if (!tab) return null;
  const pt = G.plotTypes[tab.state.plotType] || {};
  const extra = {};
  for (const key of Object.keys(pt.defaults || {})) {
    const inp = document.getElementById(`extra-${tabId}-${key}`);
    if (inp) extra[key] = isNaN(inp.value) ? inp.value : Number(inp.value);
  }
  return {
    version: 2, label: tab.label,
    database:    tabEl(tabId,'db-select')?.value    || '',
    sql:         tabEl(tabId,'sql-input')?.value    || '',
    plotType:    tab.state.plotType,
    chartTitle:  tabEl(tabId,'chart-title')?.value  || '',
    x:           tabEl(tabId,'x-col')?.value        || '',
    y:           tab.state.yCols,
    group:       tabEl(tabId,'group-col')?.value    || '',
    z:           tabEl(tabId,'z-col')?.value        || '',
    xLabel:      tabEl(tabId,'x-label')?.value      || '',
    yLabel:      tabEl(tabId,'y-label')?.value      || '',
    xScale:      tabEl(tabId,'x-scale')?.value      || 'linear',
    yScale:      tabEl(tabId,'y-scale')?.value      || 'linear',
    xTickFmt:    tabEl(tabId,'x-tickfmt')?.value    || '',
    yTickFmt:    tabEl(tabId,'y-tickfmt')?.value    || '',
    extra,
    customScript:tabEl(tabId,'script')?.value       || '',
    columns:     tab.state.columns || [],
  };
}

function applyTabConfig(tabId, cfg) {
  if (!cfg || cfg.version < 2) { alert('Invalid or incompatible config (expected version 2).'); return; }
  const tab = G.tabs.find(t => t.id === tabId);
  if (!tab) return;
  tab.label = cfg.label || tab.label;
  const lbl = document.querySelector(`#tabbtn-${tabId} .tab-label`);
  if (lbl) lbl.textContent = tab.label;

  const f = (sid, val) => { const el = tabEl(tabId, sid); if(el && val!==undefined) el.value = val; };
  f('db-select',  cfg.database);
  f('sql-input',  cfg.sql);
  f('chart-title',cfg.chartTitle);
  f('x-label',    cfg.xLabel);
  f('y-label',    cfg.yLabel);
  f('x-scale',    cfg.xScale);
  f('y-scale',    cfg.yScale);
  f('x-tickfmt',  cfg.xTickFmt);
  f('y-tickfmt',  cfg.yTickFmt);
  f('script',     cfg.customScript);

  tab.state.plotType = cfg.plotType || 'line';
  tab.state.yCols    = cfg.y || [];
  tab.state.extra    = cfg.extra || {};

  renderPlotChips(tabId);
  renderExtraOpts(tabId);
  for (const [key, val] of Object.entries(cfg.extra || {})) {
    const inp = document.getElementById(`extra-${tabId}-${key}`);
    if (inp) inp.value = val;
  }
  if (cfg.columns?.length) updateAxisControls(tabId, cfg.columns);
  setTimeout(() => { f('x-col', cfg.x); f('group-col', cfg.group); f('z-col', cfg.z); }, 0);

  setStatus(tabId,'Config loaded — click Run to render','warn');
  clientLog(`Config imported into tab "${tab.label}"`);
}

document.getElementById('btn-export-config').addEventListener('click', () => {
  const cfg = getTabConfig(G.activeTab);
  if (!cfg) return;
  download(JSON.stringify(cfg, null, 2), `plot-config-${cfg.label.replace(/\s+/g,'-')}.json`);
  clientLog(`Config exported for tab "${cfg.label}"`);
});

document.getElementById('btn-import-config').addEventListener('click', () => {
  document.getElementById('import-file-input').click();
});
document.getElementById('import-file-input').addEventListener('change', e => {
  const f = e.target.files[0]; if (!f) return;
  readJson(f, cfg => applyTabConfig(G.activeTab, cfg));
  e.target.value = '';
});

// ═══════════════════════════════════════════════════════════════
// Workspace (all tabs) import / export
// ═══════════════════════════════════════════════════════════════
document.getElementById('btn-export-all').addEventListener('click', () => {
  const ws = { version:2, tabs: G.tabs.map(t => getTabConfig(t.id)) };
  download(JSON.stringify(ws, null, 2), `workspace-${Date.now()}.json`);
  clientLog(`Workspace exported: ${G.tabs.length} tab(s)`);
});

document.getElementById('btn-import-all').addEventListener('click', () => {
  document.getElementById('import-ws-file-input').click();
});
document.getElementById('import-ws-file-input').addEventListener('change', e => {
  const f = e.target.files[0]; if (!f) return;
  readJson(f, ws => {
    if (!ws?.tabs?.length) { alert('Invalid workspace file.'); return; }
    // Close all existing tabs
    [...G.tabs].forEach(t => closeTab(t.id));
    G.tabs = [];
    ws.tabs.forEach(cfg => {
      const newId = createTab(cfg.label || `Plot ${G.nextTabId}`, {});
      applyTabConfig(newId, cfg);
    });
    clientLog(`Workspace imported: ${ws.tabs.length} tab(s)`);
  });
  e.target.value = '';
});

// ═══════════════════════════════════════════════════════════════
// Save PNG
// ═══════════════════════════════════════════════════════════════
document.getElementById('btn-export-png').addEventListener('click', () => {
  const tab = activeTabObj();
  if (!tab?.state?.plotted) { clientLog('No plot rendered yet','warn'); return; }
  Plotly.downloadImage(`plot-${G.activeTab}`, {
    format:'png', width:1600, height:900,
    filename:`hpc-plot-${tab.label.replace(/\s+/g,'-')}`
  });
  clientLog(`PNG saved for tab "${tab.label}"`);
});

// ═══════════════════════════════════════════════════════════════
// Actions sidebar — re-parse
// ═══════════════════════════════════════════════════════════════
function populateReparseSel() {
  const sel = document.getElementById('reparse-db-sel');
  sel.innerHTML = Object.keys(G.databases).map(n => `<option value="${n}">${n}</option>`).join('');
}

document.getElementById('btn-reparse').addEventListener('click', async () => {
  const db = document.getElementById('reparse-db-sel').value;
  if (!db) return;
  clientLog(`Re-parsing database "${db}"…`);
  document.getElementById('reparse-result').textContent = 'Running…';
  const res = await api('POST', '/api/reparse', {database: db});
  document.getElementById('reparse-result').textContent = res.message || res.error;
  if (res.databases) { G.databases = res.databases; renderSchemaTree(); populateReparseSel(); }
  if (res.log_entry) addLogEntry(res.log_entry);
});

// ═══════════════════════════════════════════════════════════════
// Actions sidebar — remote fetch
// ═══════════════════════════════════════════════════════════════
function populateFetchSystems() {
  const sel = document.getElementById('fetch-system-sel');
  sel.innerHTML = '<option value="">— select system —</option>' +
    Object.keys(G.remoteSystems).map(s => `<option value="${s}">${s}</option>`).join('');
}

function onFetchSystemChange() {
  const sys = document.getElementById('fetch-system-sel').value;
  const paths = G.remoteSystems[sys] || [];
  const list = document.getElementById('fetch-path-list');
  if (!paths.length) { list.innerHTML = '<span style="color:var(--text3);font-size:9px">No paths configured</span>'; return; }
  list.innerHTML = paths.map(p =>
    `<label class="fetch-path-item"><input type="checkbox" value="${escHtml(p)}"> <span>${escHtml(p)}</span></label>`
  ).join('');
}

async function runFetch() {
  const sys = document.getElementById('fetch-system-sel').value;
  if (!sys) { clientLog('Select a remote system first', 'warn'); return; }
  const checked = [...document.querySelectorAll('#fetch-path-list input:checked')].map(i => i.value);
  if (!checked.length) { clientLog('No paths selected', 'warn'); return; }
  clientLog(`Fetching ${checked.length} path(s) from "${sys}"…`);
  document.getElementById('fetch-result').textContent = 'Running…';
  const res = await api('POST', '/api/fetch_remote', {system: sys, paths: checked});
  document.getElementById('fetch-result').textContent = res.message || res.error;
  if (res.databases) { G.databases = res.databases; renderSchemaTree(); populateReparseSel(); populateAllDbSelects(); }
  if (res.log_entry) addLogEntry(res.log_entry);
}

function populateAllDbSelects() {
  const opts = Object.keys(G.databases).map(n => `<option value="${n}">${n}</option>`).join('');
  G.tabs.forEach(tab => {
    const sel = tabEl(tab.id, 'db-select');
    if (sel) { const prev = sel.value; sel.innerHTML = opts; if (prev) sel.value = prev; }
  });
}

// ═══════════════════════════════════════════════════════════════
// Actions sidebar — plugins
// ═══════════════════════════════════════════════════════════════
document.getElementById('btn-reload-plugins').addEventListener('click', async () => {
  clientLog('Reloading plugins…');
  const res = await api('POST', '/api/reload_plugins', {dirs:[]});
  if (res.error) { clientLog(res.error,'error'); return; }
  Object.assign(G.plotTypes, res.plot_types || {});
  G.tabs.forEach(t => { renderPlotChips(t.id); renderExtraOpts(t.id); });
  document.getElementById('plugin-list').textContent = res.reloaded.length
    ? res.reloaded.join(', ') : 'No plugins found';
  if (res.log_entry) addLogEntry(res.log_entry);
});

// ═══════════════════════════════════════════════════════════════
// Sidebar tabs
// ═══════════════════════════════════════════════════════════════
document.querySelectorAll('.sb-tab').forEach(t => t.addEventListener('click', () => {
  const name = t.dataset.sbtab;
  document.querySelectorAll('.sb-tab').forEach(x => x.classList.toggle('active', x.dataset.sbtab === name));
  document.querySelectorAll('.sb-pane').forEach(p => p.classList.toggle('active', p.id === `sbtab-${name}`));
}));

// ═══════════════════════════════════════════════════════════════
// Topbar run + keyboard
// ═══════════════════════════════════════════════════════════════
document.getElementById('btn-run').addEventListener('click', runPlot);
document.addEventListener('keydown', e => {
  if (e.ctrlKey && e.key === 'Enter') { e.preventDefault(); runPlot(); }
});

// ═══════════════════════════════════════════════════════════════
// Utility
// ═══════════════════════════════════════════════════════════════
function download(text, filename) {
  const a = document.createElement('a');
  a.href = URL.createObjectURL(new Blob([text], {type:'application/json'}));
  a.download = filename; a.click(); URL.revokeObjectURL(a.href);
}

function readJson(file, cb) {
  const r = new FileReader();
  r.onload = ev => { try { cb(JSON.parse(ev.target.result)); } catch(e) { alert('Could not parse JSON: '+e.message); } };
  r.readAsText(file);
}

// ═══════════════════════════════════════════════════════════════
// Boot
// ═══════════════════════════════════════════════════════════════
async function init() {
  const [dbRes, ptRes, rsRes] = await Promise.all([
    api('GET', '/api/databases'),
    api('GET', '/api/plot_types'),
    api('GET', '/api/remote_systems'),
  ]);
  G.databases     = dbRes.databases    || {};
  G.plotTypes     = ptRes.plot_types   || {};
  G.remoteSystems = rsRes.systems      || {};

  renderSchemaTree();
  populateReparseSel();
  populateFetchSystems();

  // Create the first tab
  createTab('Plot 1');

  await fetchServerLogs();
  clientLog('Application ready');
}

init();
</script>
</body>
</html>"""


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
    print("\n  Press Ctrl+C to stop\n")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[stopped]")
        server.server_close()
