"""Server log, user-script execution, initial-workspace autoload."""
import json
import math
import sys
from datetime import datetime
from pathlib import Path
import pandas as pd
from . import agg

SERVER_LOG: list = []


def log(msg, level="info"):
    e = {"ts": datetime.now().strftime("%H:%M:%S"), "level": level, "msg": str(msg)}
    SERVER_LOG.append(e)
    print(f"[{e['ts']}] [{level.upper()}] {msg}", file=sys.stderr)
    return e


def make_script_logger(prefix: str):
    entries = []
    def _log(msg, level="script"):
        e = log(f"[{prefix}] {msg}", level); entries.append(e); return e
    return _log, entries


def _exec(source, name, ns):
    exec(compile(source, name, "exec"), ns)
    return ns


def run_custom_plot_script(source, df_data, config):
    ns = load_custom_plot_script(source)
    plot = ns.get("plot")
    if not callable(plot):
        raise ValueError(
            "For Plotly, custom script must define plot(df_data, config). "
            "For Matplotlib, define plot_matplotlib(ax, df_data, config)."
        )
    return plot(df_data, config)


def load_custom_plot_script(source):
    """Execute a custom plot script and return its namespace for either backend."""
    return _exec(source, "<custom_plot>", {"pd": pd, "log": log, "agg": agg})


def run_transform_script(source, data, log_fn):
    """`data` is dict[str, DataFrame]; the query result lives in data['result'].
    `agg` exposes summarize()/speedup()/efficiency() for collapsing repeated
    runs into mean+error rows and computing scaling metrics — see agg.py."""
    ns = _exec(source, "<transform_script>", {"data": data, "pd": pd, "log": log_fn, "agg": agg})
    return ns.get("data", data)


def run_layout_script(source, layout, config, log_fn):
    ns = _exec(source, "<layout_script>", {"layout": layout, "config": config, "log": log_fn})
    return ns.get("layout", layout)


def run_layout_script_with_hooks(source, layout, config, log_fn):
    """Run Plotly layout edits and return an optional Matplotlib layout hook."""
    ns = _exec(source, "<layout_script>", {"layout": layout, "config": config, "log": log_fn})
    return ns.get("layout", layout), ns.get("customize_matplotlib")


def load_tick_formatter(source):
    """Compile a user tick formatter, which must define format_tick(value, axis)."""
    ns = _exec(source, "<tick_formatter>", {"math": math, "pd": pd})
    formatter = ns.get("format_tick")
    if not callable(formatter):
        raise ValueError("Custom tick formatter must define format_tick(value, axis).")
    return formatter


_WORKSPACE = None


def load_initial_workspace(path=None):
    global _WORKSPACE
    p = Path(path) if path else Path.cwd() / "plots.json"
    _WORKSPACE = None
    if p.exists():
        try:
            _WORKSPACE = json.loads(p.read_text(encoding="utf-8"))
            log(f"Loaded initial workspace from {p}")
        except Exception as e:
            log(f"Failed to load {p}: {e}", "error")
    return _WORKSPACE


def get_initial_workspace():
    return _WORKSPACE
