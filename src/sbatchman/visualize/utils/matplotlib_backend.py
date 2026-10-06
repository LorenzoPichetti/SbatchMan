"""Server-side Matplotlib/Seaborn renderer for the visualization builder.

It consumes the same data, configuration, and trace dictionaries as the
Plotly renderer, then returns browser-ready PNG and SVG images.
"""
from __future__ import annotations

import base64
from io import BytesIO
import math

from . import styles

_MPL_MODULES = None


def _modules():
    global _MPL_MODULES
    if _MPL_MODULES is not None:
        return _MPL_MODULES
    try:
        import matplotlib
        matplotlib.use("Agg", force=True)
        import matplotlib.pyplot as plt
        from matplotlib.lines import Line2D
        import numpy as np
        import seaborn as sns
    except ImportError as exc:
        raise RuntimeError("Matplotlib rendering requires the optional dependencies. Install them with: pip install 'sbatchman[visualization]'") from exc
    _MPL_MODULES = (plt, Line2D, np, sns)
    return _MPL_MODULES


def _scale(ax, axis, value):
    if value == "log2":
        getattr(ax, f"set_{axis}scale")("log", base=2)
    elif value == "log":
        getattr(ax, f"set_{axis}scale")("log")
    elif value in ("linear", "symlog", "logit"):
        getattr(ax, f"set_{axis}scale")(value)


def _limit(ax, axis, config, prefix=None):
    prefix = prefix or axis
    low, high = config.get(f"{prefix}_min"), config.get(f"{prefix}_max")
    if low not in (None, "") or high not in (None, ""):
        old = getattr(ax, f"get_{axis}lim")()
        try:
            getattr(ax, f"set_{axis}lim")(
                float(low) if low not in (None, "") else old[0],
                float(high) if high not in (None, "") else old[1],
            )
        except (TypeError, ValueError):
            pass
    if config.get(f"{prefix}_reversed"):
        getattr(ax, f"invert_{axis}axis")()


_MARKERS = {"circle": "o", "square": "s", "diamond": "D", "cross": "+", "x": "x",
            "triangle-up": "^", "triangle-down": "v", "star": "*", "pentagon": "p", "hexagon": "h"}
_DASHES = {"solid": "-", "dash": "--", "dot": ":", "dashdot": "-.", "longdash": (0, (8, 3)),
           "longdashdot": (0, (8, 3, 2, 3))}


def _font_family(style):
    from matplotlib import font_manager

    installed = {font.name.casefold(): font.name for font in font_manager.fontManager.ttflist}
    requested = [part.strip().strip("'\"") for part in str(style.get("font_family", "DejaVu Serif")).split(",")]
    for family in requested:
        if family.casefold() in installed:
            return installed[family.casefold()]
        generic = {"serif": "DejaVu Serif", "sans-serif": "DejaVu Sans", "sans": "DejaVu Sans",
                   "monospace": "DejaVu Sans Mono"}.get(family.casefold())
        if generic and generic.casefold() in installed:
            return installed[generic.casefold()]
    # DejaVu ships with Matplotlib and is a stable fallback for headless servers.
    return installed.get("dejavu serif", "DejaVu Serif")


def _errorbar_kwargs(trace):
    err = trace.get("error_y") or {}
    if not err.get("visible"):
        return {}
    array = err.get("array")
    if array is None:
        return {}
    if err.get("symmetric") is False:
        return {"yerr": [err.get("arrayminus", []), array], "capsize": 2}
    return {"yerr": array, "capsize": 2}


def _has_errorbars(trace, config):
    return config.get("error_style") != "band" and bool((trace.get("error_y") or {}).get("visible"))


def _draw_trace(ax, right_ax, trace, config, style, color, bar_index=0, bar_count=1):
    """Draw one Plotly-compatible trace; return its legend handle and label."""
    plt, Line2D, np, sns = _modules()
    kind = trace.get("type", "scatter")
    name = str(trace.get("name", ""))
    if name.endswith(" (band)") and trace.get("showlegend") is False:
        return None
    if trace.get("showlegend") is False or (name.endswith(" (band)") and trace.get("hoverinfo") == "skip"):
        show_legend = False
    else:
        show_legend = True
    y2 = config.get("y2") or []
    y2 = {y2} if isinstance(y2, str) else set(y2)
    axis = right_ax if trace.get("yaxis") == "y2" or trace.get("_ycol") in y2 else ax
    x, y = trace.get("x"), trace.get("y")
    line = trace.get("line") or {}
    marker = trace.get("marker") or {}
    lw = float(style.get("line_width", line.get("width", 1.5)))
    ms = float(config.get("legend_marker_size") or style.get("marker_size", 5))
    marker_value = marker.get("symbol", "o")
    marker_values = marker_value if isinstance(marker_value, list) else None
    if marker_values:
        marker_value = marker_values[0]
    marker_value = _MARKERS.get(marker_value, marker_value)
    color = color or line.get("color") or marker.get("color")
    handle = None

    if kind == "scatter":
        mode = trace.get("mode", "markers")
        if config.get("error_style") == "band" and (trace.get("error_y") or {}).get("visible"):
            err = trace["error_y"]
            low = err.get("arrayminus", err["array"])
            axis.fill_between(x or [], [v - e for v, e in zip(y or [], low)],
                              [v + e for v, e in zip(y or [], err["array"])],
                              color=color, alpha=.2, linewidth=0)
        if "lines" in mode:
            linestyle = _DASHES.get(line.get("dash", "solid"), "-")
            if _has_errorbars(trace, config) and not marker_values:
                artist = axis.errorbar(x or [], y or [], color=color, linewidth=lw, linestyle=linestyle,
                                       marker=marker_value if "markers" in mode and not marker_values else None,
                                       markersize=ms, label=name, **_errorbar_kwargs(trace))
                handle = artist.lines[0]
            else:
                line_handle, = axis.plot(x or [], y or [], color=color, linewidth=lw,
                                         linestyle=linestyle,
                                         marker=marker_value if "markers" in mode and not marker_values else None,
                                         markersize=ms, label=name)
                handle = line_handle
            if marker_values and "markers" in mode:
                for marker_name in dict.fromkeys(marker_values):
                    indexes = [i for i, value in enumerate(marker_values) if value == marker_name]
                    axis.scatter([x[i] for i in indexes], [y[i] for i in indexes],
                                 marker=_MARKERS.get(marker_name, marker_name), color=color,
                                 s=ms ** 2, label="_nolegend_")
                if _has_errorbars(trace, config):
                    # Error bars are rendered independently when marker
                    # symbols vary from point to point.
                    axis.errorbar(x or [], y or [], color=color, fmt="none", label="_nolegend_",
                                  **_errorbar_kwargs(trace))
        else:
            if marker_values:
                collections = []
                for marker_name in dict.fromkeys(marker_values):
                    indexes = [i for i, value in enumerate(marker_values) if value == marker_name]
                    collections.append(axis.scatter([x[i] for i in indexes], [y[i] for i in indexes],
                                                     marker=_MARKERS.get(marker_name, marker_name), color=color,
                                                     s=ms ** 2, label=name if not collections else "_nolegend_"))
                handle = collections[0] if collections else None
                if _has_errorbars(trace, config):
                    axis.errorbar(x or [], y or [], color=color, fmt="none", label="_nolegend_",
                                  **_errorbar_kwargs(trace))
            else:
                artist = axis.errorbar(x or [], y or [], color=color, linestyle="none",
                                       marker=marker_value, markersize=ms, label=name,
                                       **(_errorbar_kwargs(trace) if _has_errorbars(trace, config) else {}))
                handle = artist.lines[0]
    elif kind == "bar":
        xx = np.arange(len(x or []))
        width = .8 / max(bar_count, 1)
        xx = xx - .4 + width / 2 + bar_index * width
        bars = axis.bar(xx, y or [], width=width, color=color, label=name,
                        **_errorbar_kwargs(trace))
        axis.set_xticks(np.arange(len(x or [])), [str(v) for v in (x or [])])
        handle = bars[0] if len(bars) else None
    elif kind == "histogram":
        patches = axis.hist(x or [], bins=int(trace.get("nbinsx", 30)), color=color,
                            alpha=.8, label=name)[2]
        handle = patches[0] if len(patches) else None
    elif kind in ("box", "violin"):
        values = list(y or [])
        categories = list(x or [])
        if categories and len(categories) == len(values):
            levels = sorted(set(categories), key=str)
            grouped = [[value for value, category in zip(values, categories) if category == level]
                       for level in levels]
            labels = [str(v) for v in levels]
        else:
            grouped, labels = [values], [name]
        if kind == "box":
            artists = axis.boxplot(grouped, labels=labels, patch_artist=True)
            for patch in artists["boxes"]:
                patch.set_facecolor(color); patch.set_alpha(.65)
            handle = artists["boxes"][0] if artists["boxes"] else None
        else:
            artists = axis.violinplot(grouped, showmeans=True)
            for body in artists["bodies"]:
                body.set_facecolor(color); body.set_alpha(.65)
            axis.set_xticks(range(1, len(labels) + 1), labels)
            handle = artists["bodies"][0] if artists["bodies"] else None
    elif kind == "heatmap":
        image = axis.imshow(trace.get("z", []), aspect="auto", cmap="viridis", origin="lower")
        axis.set_xticks(range(len(trace.get("x", []))), [str(v) for v in trace.get("x", [])])
        axis.set_yticks(range(len(trace.get("y", []))), [str(v) for v in trace.get("y", [])])
        axis.figure.colorbar(image, ax=axis)
    elif kind == "pie":
        wedges, _ = axis.pie(trace.get("values", []), labels=trace.get("labels", []),
                             colors=style.get("palette", styles.OKABE_ITO),
                             wedgeprops={"width": 1 - float(trace.get("hole", 0))})
        handle = wedges[0] if wedges else None
    else:
        raise ValueError(f"Matplotlib backend does not support trace type '{kind}'.")
    return (handle, name) if show_legend and handle is not None and name else None


def _axis_label(config, axis):
    if config.get(f"{axis}_label"):
        return config[f"{axis}_label"]
    if axis == "x":
        return config.get("x", "")
    y = config.get("y") or []
    y = [y] if isinstance(y, str) else list(y)
    y2 = config.get("y2") or []
    y2 = {y2} if isinstance(y2, str) else set(y2)
    return ", ".join(col for col in y if col not in y2)


def _style_axes(ax, right_ax, config, style, title=""):
    plt, Line2D, np, sns = _modules()
    font = _font_family(style)
    ax.set_title(title or config.get("title", ""), fontsize=style.get("title_size", 14), fontfamily=font)
    ax.set_xlabel(_axis_label(config, "x"), fontsize=style.get("label_size", 12), fontfamily=font)
    ax.set_ylabel(_axis_label(config, "y"), fontsize=style.get("label_size", 12), fontfamily=font)
    _scale(ax, "x", config.get("x_scale", "linear"))
    _scale(ax, "y", config.get("y_scale", "linear"))
    if right_ax:
        y2_cols = config.get("y2") or []
        y2_cols = [y2_cols] if isinstance(y2_cols, str) else y2_cols
        right_ax.set_ylabel(config.get("y2_label") or ", ".join(y2_cols),
                            fontsize=style.get("label_size", 12), fontfamily=font)
        _scale(right_ax, "y", config.get("y2_scale", "linear"))
        _limit(right_ax, "y", config, "y2")
        y_cols = config.get("y") or []
        y_cols = [y_cols] if isinstance(y_cols, str) else list(y_cols)
        y2_cols = config.get("y2") or []
        y2_cols = {y2_cols} if isinstance(y2_cols, str) else set(y2_cols)
        if y_cols and all(col in y2_cols for col in y_cols):
            ax.set_ylabel("")
            ax.tick_params(axis="y", left=False, labelleft=False)
            ax.spines["left"].set_visible(False)
    _limit(ax, "x", config); _limit(ax, "y", config)
    angle = config.get("_mpl_x_tick_angle", style.get("tick_angle"))
    if angle not in (None, ""):
        ax.tick_params(axis="x", labelrotation=float(angle))
    y_angle = config.get("_mpl_y_tick_angle")
    if y_angle not in (None, ""):
        ax.tick_params(axis="y", labelrotation=float(y_angle))
    direction = {"outside": "out", "inside": "in"}.get(style.get("tick_dir"), style.get("tick_dir", "out"))
    ax.tick_params(axis="both", labelsize=style.get("tick_size", 10), direction=direction)
    ax.grid(bool(style.get("grid", True)), color=style.get("grid_color", "#e6e6e6"), linewidth=.7)
    if style.get("mirror", True):
        ax.spines["top"].set_visible(True); ax.spines["right"].set_visible(True)
        if right_ax:
            ax.spines["right"].set_visible(False)
    if config.get("x_tickformat"):
        from matplotlib.ticker import FormatStrFormatter
        from matplotlib.dates import DateFormatter
        fmt = str(config["x_tickformat"])
        if fmt.endswith("f") and fmt.startswith("."):
            ax.xaxis.set_major_formatter(FormatStrFormatter("%" + fmt))
        elif fmt.startswith("%"):
            ax.xaxis.set_major_formatter(DateFormatter(fmt))
    if config.get("y_tickformat"):
        from matplotlib.ticker import FormatStrFormatter
        from matplotlib.dates import DateFormatter
        fmt = str(config["y_tickformat"])
        if fmt.endswith("f") and fmt.startswith("."):
            ax.yaxis.set_major_formatter(FormatStrFormatter("%" + fmt))
        elif fmt.startswith("%"):
            ax.yaxis.set_major_formatter(DateFormatter(fmt))
    for axis in ("x", "y"):
        vals = config.get(f"_mpl_{axis}_tickvals")
        labels = config.get(f"_mpl_{axis}_ticktext")
        if vals is not None:
            getattr(ax, f"set_{axis}ticks")(vals, labels=labels if labels is not None else None)
        if f"_mpl_{axis}_grid" in config:
            getattr(ax, f"{axis}axis").grid(config[f"_mpl_{axis}_grid"])


def _legend(ax, handles, config, style):
    if not handles or config.get("show_legend", True) is False:
        return
    orientation = config.get("legend_orientation") or style.get("legend_orientation", "auto")
    position = config.get("legend_position", "right")
    horizontal = orientation == "horizontal" or (orientation == "auto" and position in ("top", "bottom"))
    if horizontal and position == "right":
        position = "top"
    loc = {"top": "upper center", "bottom": "lower center", "inside-top-right": "upper right",
           "inside-top-left": "upper left", "inside-bottom-right": "lower right",
           "inside-bottom-left": "lower left"}.get(position, "center left" if not horizontal else "upper center")
    ncols = int(config.get("legend_columns") or style.get("legend_columns") or 0)
    nrows = int(config.get("legend_rows") or style.get("legend_rows") or 0)
    if not ncols and nrows:
        ncols = max(1, math.ceil(len(handles) / nrows))
    if not ncols:
        ncols = len(handles) if horizontal else 1
    kwargs = {"handles": [h for h, _ in handles], "labels": [n for _, n in handles],
              "title": config.get("legend_title") or None, "fontsize": style.get("legend_size", 10),
              "ncol": ncols, "frameon": position.startswith("inside")}
    if position == "right":
        kwargs["loc"] = "center left"; kwargs["bbox_to_anchor"] = (1.02, .5)
    elif position == "top":
        kwargs["loc"] = "lower center"; kwargs["bbox_to_anchor"] = (.5, 1.02)
    elif position == "bottom":
        kwargs["loc"] = "upper center"; kwargs["bbox_to_anchor"] = (.5, -.18)
    else:
        kwargs["loc"] = loc
    ax.legend(**kwargs)


def _encode_figure(fig, dpi):
    png, svg, pdf = BytesIO(), BytesIO(), BytesIO()
    fig.savefig(png, format="png", dpi=dpi, bbox_inches="tight", facecolor="white")
    fig.savefig(svg, format="svg", bbox_inches="tight", facecolor="white")
    fig.savefig(pdf, format="pdf", bbox_inches="tight", facecolor="white")
    plt, _, _, _ = _modules()
    plt.close(fig)
    return {"image_png": base64.b64encode(png.getvalue()).decode("ascii"),
            "image_svg": base64.b64encode(svg.getvalue()).decode("ascii"),
            "image_pdf": base64.b64encode(pdf.getvalue()).decode("ascii")}


def render_matplotlib_plot(df, traces, config, style=None, title=""):
    plt, _, np, sns = _modules()
    style = styles.resolve(style)
    sns.set_theme(style="whitegrid" if style.get("grid", True) else "white", context="paper",
                  font=_font_family(style))
    width, height = float(style.get("width_in") or 10), float(style.get("height_in") or 5.6)
    fig, ax = plt.subplots(figsize=(width, height), constrained_layout=True)
    y2_cols = config.get("y2") or []
    y2_cols = {y2_cols} if isinstance(y2_cols, str) else set(y2_cols)
    right_ax = ax.twinx() if y2_cols else None
    colors = style.get("palette") if isinstance(style.get("palette"), list) else styles.OKABE_ITO
    names = list(dict.fromkeys(t.get("name") for t in traces if t.get("name")))
    color_map = {name: colors[i % len(colors)] for i, name in enumerate(names)}
    bar_traces = [t for t in traces if t.get("type") == "bar"]
    bar_indexes = {id(t): i for i, t in enumerate(bar_traces)}
    handles = []
    for t in traces:
        ycol = t.get("_ycol", t.get("name"))
        trace_color = (t.get("line") or {}).get("color") or (t.get("marker") or {}).get("color")
        color = color_map.get(t.get("name"), colors[len(handles) % len(colors)]) if style.get("recolor", True) else trace_color
        result = _draw_trace(ax, right_ax, t, config, style, color,
                             bar_indexes.get(id(t), 0), len(bar_traces) or 1)
        if result: handles.append(result)
    _style_axes(ax, right_ax, config, style, title)
    _legend(ax, handles, config, style)
    return _encode_figure(fig, int(style.get("dpi", 300)))


def render_matplotlib_grid(panels, rows, cols, style=None, share_x=False, share_y=False,
                           panel_labels=False, legend=None, xgap=.20, ygap=.24, share_legend=True):
    plt, Line2D, np, sns = _modules()
    style = styles.resolve(style)
    sns.set_theme(style="whitegrid" if style.get("grid", True) else "white", context="paper",
                  font=_font_family(style))
    width = float(style.get("width_in") or (4 * cols))
    height = float(style.get("height_in") or (3.3 * rows))
    fig, axes = plt.subplots(rows, cols, figsize=(width, height), squeeze=False,
                             sharex=bool(share_x), sharey=bool(share_y))
    colors = style.get("palette") if isinstance(style.get("palette"), list) else styles.OKABE_ITO
    color_map, all_handles = {}, []
    shared = legend or {}
    independent = not share_legend
    for i, panel in enumerate(panels):
        r, c = divmod(i, cols)
        ax = axes[r][c]
        config = panel.get("config", {})
        traces = panel.get("_traces", [])
        y2 = config.get("y2") or []
        y2 = {y2} if isinstance(y2, str) else set(y2)
        right_ax = ax.twinx() if y2 else None
        bar_traces = [t for t in traces if t.get("type") == "bar"]
        bar_indexes = {id(t): j for j, t in enumerate(bar_traces)}
        handles = []
        for t in traces:
            name = t.get("name")
            if name not in color_map:
                color_map[name] = colors[len(color_map) % len(colors)]
            if not style.get("recolor", True):
                color_map[name] = (t.get("line") or {}).get("color") or (t.get("marker") or {}).get("color") or colors[0]
            result = _draw_trace(ax, right_ax, t, config, style, color_map[name],
                                 bar_indexes.get(id(t), 0), len(bar_traces) or 1)
            if result:
                handles.append(result)
                if name not in {n for _, n in all_handles}:
                    all_handles.append(result)
        _style_axes(ax, right_ax, config, style, config.get("title", ""))
        if share_x and r < rows - 1:
            ax.set_xlabel("")
        if share_y:
            if c > 0:
                ax.set_ylabel("")
                ax.tick_params(axis="y", labelleft=False)
            else:
                ax.yaxis.set_label_position("left")
                ax.tick_params(axis="y", labelleft=True)
        panel_legend_config = {**shared, **config}
        if independent:
            panel_legend_config = {
                **config,
                "legend_position": config.get("legend_position", shared.get("position", "right")),
                "legend_title": config.get("legend_title") or shared.get("title", ""),
                "show_legend": shared.get("show", True) is not False and config.get("show_legend", True) is not False,
            }
            _legend(ax, handles, panel_legend_config, style)
        if panel_labels:
            label = f"({chr(97 + i)})" if panel_labels is True else str(panel_labels[i]) if i < len(panel_labels) else ""
            if label:
                ax.text(.02, .98, label, transform=ax.transAxes, ha="left", va="top", fontweight="bold")
    for i in range(len(panels), rows * cols):
        axes[i // cols][i % cols].set_visible(False)
    if shared.get("show", True) and not independent and all_handles:
        orientation = style.get("legend_orientation", "auto")
        position = shared.get("position", "right")
        horizontal = orientation == "horizontal" or (orientation == "auto" and position in ("top", "bottom"))
        if horizontal and position == "right":
            position = "top"
        ncols = int(style.get("legend_columns") or 0)
        nrows = int(style.get("legend_rows") or 0)
        if not ncols:
            ncols = max(1, math.ceil(len(all_handles) / nrows)) if nrows else (len(all_handles) if horizontal else 1)
        loc = {"right": "center left", "top": "lower center", "bottom": "upper center"}.get(position, "center left")
        anchor = {"right": (1.01, .5), "top": (.5, 1.01), "bottom": (.5, -.01)}.get(position, (1.01, .5))
        fig.legend([h for h, _ in all_handles], [n for _, n in all_handles],
                   title=shared.get("title") or None, loc=loc, bbox_to_anchor=anchor,
                   ncol=ncols, fontsize=style.get("legend_size", 10), frameon=False)
    fig.tight_layout()
    if rows > 1 or cols > 1:
        fig.subplots_adjust(wspace=max(0, float(xgap)), hspace=max(0, float(ygap)))
    return _encode_figure(fig, int(style.get("dpi", 300)))
