"""Server log, user-script execution, initial-workspace autoload."""
import json
import inspect
import math
import sys
from datetime import datetime
from pathlib import Path
import pandas as pd
from . import agg
from .script_paths import add_script_paths_to_sys_path, merge_script_paths

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
    add_script_paths_to_sys_path()
    exec(compile(source, name, "exec"), ns)
    return ns


def add_zoom_inset(ax, zoom_region, inset_position=(0.6, 0.6, 0.3, 0.3),
                   draw_rect=True, rect_kwargs=None, zoom_ax_kwargs=None,
                   rect_scaling=1.0, rect_region=None):
    """Add a Matplotlib inset showing a zoomed region of an axes' lines."""
    from matplotlib.patches import Rectangle
    from mpl_toolkits.axes_grid1.inset_locator import inset_axes

    rect_kwargs = rect_kwargs or {
        "edgecolor": "black", "linestyle": "dashed", "linewidth": 1,
    }
    zoom_ax_kwargs = zoom_ax_kwargs or {}
    # Keep a full snapshot of the parent y-axis. The inset toolkit and
    # annotation artists can trigger Matplotlib's axis callbacks, so restoring
    # only the data limits is not enough when a custom locator/formatter is
    # in use.
    original_ylim = ax.get_ylim()
    original_y_locator = ax.yaxis.get_major_locator()
    original_y_formatter = ax.yaxis.get_major_formatter()
    original_y_minor_locator = ax.yaxis.get_minor_locator()
    original_y_minor_formatter = ax.yaxis.get_minor_formatter()
    axins = inset_axes(
        ax, width="100%", height="100%", bbox_to_anchor=inset_position,
        bbox_transform=ax.transAxes, loc="lower left", borderpad=0,
    )
    x1, x2, y1, y2 = zoom_region
    for line in ax.get_lines():
        axins.plot(
            line.get_xdata(), line.get_ydata(), color=line.get_color(),
            linestyle=line.get_linestyle(), linewidth=line.get_linewidth(),
            marker=line.get_marker(), markersize=line.get_markersize(),
            markeredgecolor=line.get_markeredgecolor(),
            markerfacecolor=line.get_markerfacecolor(), alpha=line.get_alpha(),
            label=line.get_label(), zorder=line.get_zorder(),
        )

    # Plotting the copied lines can autoscale the inset, so apply the requested
    # view window afterward.
    axins.set_xlim(x1, x2)
    axins.set_ylim(y1, y2)

    for method, args in zoom_ax_kwargs.items():
        if isinstance(args, dict):
            getattr(axins, method)(**args)
        elif isinstance(args, tuple):
            getattr(axins, method)(*args)
        else:
            getattr(axins, method)(args)

    if draw_rect:
        if rect_region:
            x, y, width, height = rect_region
        else:
            x_center, y_center = (x1 + x2) / 2, (y1 + y2) / 2
            width = (x2 - x1) * rect_scaling
            height = (y2 - y1) * rect_scaling
            x, y = x_center - width / 2, y_center - height / 2
        # add_patch updates the parent axes' data limits from the rectangle,
        # which can autoscale its y-axis and change the original tick marks.
        # The rectangle is an annotation in existing data coordinates, so add
        # it as an artist without changing those limits.
        rect = Rectangle((x, y), width, height, fill=False,
                         transform=ax.transData, **rect_kwargs)
        ax.add_artist(rect)

    # Reinstate the parent axis state after the inset has been fully built.
    ax.yaxis.set_major_locator(original_y_locator)
    ax.yaxis.set_major_formatter(original_y_formatter)
    ax.yaxis.set_minor_locator(original_y_minor_locator)
    ax.yaxis.set_minor_formatter(original_y_minor_formatter)
    ax.set_ylim(original_ylim)
    # set_ylim disables y autoscaling. Keep it disabled: re-enabling it here
    # lets the subsequent figure draw recompute limits from stale data bounds,
    # which can shift the main ticks toward the inset's zoom region.
    ax.set_autoscaley_on(False)
    return axins


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
    return _exec(source, "<custom_plot>", {"pd": pd, "log": log, "agg": agg,
                                             "add_zoom_inset": add_zoom_inset})


def run_transform_script(source, df, log_fn, data=None):
    """Run a pandas transform with the query result as ``df``.

    ``df`` is both the input and output; scripts may edit it in place or
    reassign it. ``data`` remains available for source-table lookups and for
    compatibility with older scripts that assigned ``data['result']``.
    ``agg`` exposes summarize()/speedup()/efficiency() helpers.
    """
    tables = data if data is not None else {}
    original_df = df
    tables["result"] = df
    ns = _exec(source, "<transform_script>",
               {"df": df, "data": tables, "pd": pd, "log": log_fn, "agg": agg})
    transformed = ns.get("df", original_df)
    # If an older script replaced data['result'] without reassigning df, keep
    # its behavior. In-place changes are already reflected in the same object.
    if transformed is original_df and tables.get("result") is not original_df:
        transformed = tables["result"]
    return transformed


def _layout_script_namespace(layout, config, log_fn, df_data=None, traces=None):
    dataframe = (pd.DataFrame(df_data.get("rows", []), columns=df_data.get("columns", []))
                 if isinstance(df_data, dict) else pd.DataFrame())
    return {"layout": layout, "config": config, "log": log_fn,
            "df_data": df_data or {"columns": [], "rows": []}, "df": dataframe,
            "traces": traces if traces is not None else [], "pd": pd,
            "add_zoom_inset": add_zoom_inset}


def run_layout_script(source, layout, config, log_fn, df_data=None, traces=None):
    ns = _exec(source, "<layout_script>",
               _layout_script_namespace(layout, config, log_fn, df_data, traces))
    return ns.get("layout", layout)


def run_layout_script_with_hooks(source, layout, config, log_fn, df_data=None, traces=None):
    """Run Plotly layout edits and return optional Matplotlib hooks."""
    ns = _exec(source, "<layout_script>",
               _layout_script_namespace(layout, config, log_fn, df_data, traces))
    return (ns.get("layout", layout), ns.get("customize_matplotlib"),
            ns.get("customize_matplotlib_legend"))


def load_tick_formatter(source):
    """Compile a user tick formatter, optionally accepting a tick index."""
    ns = _exec(source, "<tick_formatter>", {"math": math, "pd": pd})
    formatter = ns.get("format_tick")
    if not callable(formatter):
        raise ValueError("Custom tick formatter must define format_tick(value, axis).")
    return formatter


def call_tick_formatter(formatter, value, axis, index):
    """Call a formatter with its tick index when its signature accepts it."""
    try:
        parameters = inspect.signature(formatter).parameters.values()
    except (TypeError, ValueError):
        return formatter(value, axis)
    positional = [parameter for parameter in parameters
                  if parameter.kind in (parameter.POSITIONAL_ONLY, parameter.POSITIONAL_OR_KEYWORD)]
    if any(parameter.kind == parameter.VAR_POSITIONAL for parameter in parameters) or len(positional) >= 3:
        return formatter(value, axis, index)
    if any(parameter.name == "index" and parameter.kind == parameter.KEYWORD_ONLY
           for parameter in parameters):
        return formatter(value, axis, index=index)
    if any(parameter.kind == parameter.VAR_KEYWORD for parameter in parameters):
        return formatter(value, axis, index=index)
    return formatter(value, axis)


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
    if isinstance(_WORKSPACE, dict) and _WORKSPACE.get("script_paths"):
        try:
            merge_script_paths(_WORKSPACE["script_paths"])
            add_script_paths_to_sys_path()
        except Exception as e:
            log(f"Could not register workspace Python paths: {e}", "warn")
    return _WORKSPACE


def get_initial_workspace():
    return _WORKSPACE
