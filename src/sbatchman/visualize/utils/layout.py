"""Plotly layout construction: axes, ticks, legend sizing, subplot grid geometry."""
import math
from numbers import Real

from . import styles
from .core import load_tick_formatter, log

BASE_H, MT, MB, MR = 420, 64, 70, 30
MAX_MR, MAX_CENTER_W, ITEM_H, CHAR_W, CHROME, TOP_GAP = 520, 1400, 24, 0.62, 46, 14
AXES_H = BASE_H - MT - MB

LEGEND_POS = {
    "right": {},
    "top": {"x": 0.5, "xanchor": "center", "y": 1 + TOP_GAP / AXES_H, "yanchor": "bottom"},
    "bottom": {"x": 0.5, "xanchor": "center", "y": -55 / AXES_H, "yanchor": "top"},
    "inside-top-right": {"x": .98, "xanchor": "right", "y": .98, "yanchor": "top"},
    "inside-top-left": {"x": .02, "xanchor": "left", "y": .98, "yanchor": "top"},
    "inside-bottom-right": {"x": .98, "xanchor": "right", "y": .02, "yanchor": "bottom"},
    "inside-bottom-left": {"x": .02, "xanchor": "left", "y": .02, "yanchor": "bottom"},
}


def _legend_width(config, traces, font_px):
    labels = {t.get("name") for t in traces if t.get("name")}
    if config.get("legend_title"):
        labels.add(config["legend_title"])
    return max((len(l) * font_px * CHAR_W for l in labels), default=0) + CHROME


def _ticks_from(spec):
    if not spec: return None
    vals = spec if isinstance(spec, (list, tuple)) else str(spec).split(",")
    try:
        out = [float(str(v).strip()) for v in vals if str(v).strip() != ""]
    except ValueError:
        return None
    return out or None


def _set_ticks(axis, vals):
    if vals:
        axis.update(tickmode="array", tickvals=vals,
                    ticktext=[str(int(v)) if float(v).is_integer() else str(v) for v in vals])


def _apply_custom_tick_formatter(axis_spec, axis_name, config, traces):
    source = config.get("tick_formatter", "")
    if not isinstance(source, str) or not source.strip():
        return
    # Preserve explicitly requested tick positions; otherwise use values from
    # the plotted traces because Plotly cannot call a Python formatter in the browser.
    explicit = config.get(f"{axis_name}_tickvals")
    if explicit:
        values = list(explicit) if isinstance(explicit, (list, tuple)) else str(explicit).split(",")
    else:
        values, seen = [], set()
        for trace in traces:
            raw = trace.get(axis_name)
            if raw is None:
                continue
            raw = raw if isinstance(raw, (list, tuple)) else [raw]
            for value in raw:
                if value is None:
                    continue
                key = repr(value)
                if key not in seen:
                    seen.add(key)
                    values.append(value)
        if values and all(isinstance(v, Real) and not isinstance(v, bool) for v in values):
            values.sort(key=float)
        # For numeric and date axes, select at most ten representative
        # positions. Keep all categories so the labels still match each value.
        numeric = bool(values) and all(isinstance(v, Real) and not isinstance(v, bool) for v in values)
        limit = 10 if numeric or config.get(f"{axis_name}_scale") == "date" else None
        if limit and len(values) > limit:
            indexes = sorted({round(i * (len(values) - 1) / (limit - 1)) for i in range(limit)})
            values = [values[i] for i in indexes]
    if not values:
        return
    formatter = load_tick_formatter(source)
    axis_spec.update(tickmode="array", tickvals=values,
                     ticktext=[str(formatter(value, axis_name)) for value in values])
    axis_spec.pop("tickformat", None)


def _apply_range(axis, config, k):
    """x_min/x_max (or y_min/y_max): either bound alone is enough — Plotly
    autoscales the side that's left as None. x_reversed/y_reversed flips the
    axis (e.g. so a 'rank' axis reads 1 at the top)."""
    lo, hi = config.get(f"{k}_min"), config.get(f"{k}_max")
    if lo not in (None, "") or hi not in (None, ""):
        try:
            lo_v = float(lo) if lo not in (None, "") else None
            hi_v = float(hi) if hi not in (None, "") else None
        except (TypeError, ValueError):
            return
        if lo_v is not None and hi_v is not None:
            axis["range"] = [lo_v, hi_v][::-1] if config.get(f"{k}_reversed") else [lo_v, hi_v]
        else:
            axis["autorange"] = True  # partial bound: Plotly still needs autorange for the open side
            axis["range"] = [lo_v, hi_v]
    elif config.get(f"{k}_reversed"):
        axis["autorange"] = "reversed"


def build_layout(config, overrides=None, traces=None, style=None):
    """Base layout. Look & feel (fonts, tick style, size) is applied afterwards by styles.apply.

    Secondary y-axis: any trace whose column is listed in config['y2'] (a
    subset of config['y']) gets 'yaxis':'y2' and a second axis is added,
    positioned opposite the primary one and with its own scale/label.
    """
    traces = traces or []
    y_cols = config.get("y", [])
    y_cols = [y_cols] if isinstance(y_cols, str) else list(y_cols or [])
    y2_value = config.get("y2") or []
    y2_cols = {y2_value} if isinstance(y2_value, str) else set(y2_value)
    primary_y_cols = [col for col in y_cols if col not in y2_cols]
    ylab = ", ".join(primary_y_cols)
    common = dict(showline=True, linecolor="#333", linewidth=1, zeroline=False, automargin=True)
    def axis(k, text):
        scale = config.get(f"{k}_scale", "linear")
        is_log2 = scale == "log2"
        out = {**common, "title": {"text": text, "standoff": 12},
               "type": "log" if is_log2 else scale,
               "tickformat": config.get(f"{k}_tickformat", "") or ("~g" if is_log2 else "")}
        if is_log2:
            # Plotly stores log axes in base-10 coordinates. This step spaces
            # major ticks by powers of two without changing the data values.
            out["dtick"] = 0.3010299956639812
        return out
    xaxis = axis("x", config.get("x_label") or config.get("x", ""))
    yaxis = axis("y", config.get("y_label") or ylab)
    # If every selected series uses y2, the primary y axis has no data.
    # Leaving it visible duplicates the y2 ticks and axis label, especially
    # when Panel 1 groups are expanded into many small facets.
    if y2_cols and not primary_y_cols:
        yaxis.update(showticklabels=False, showline=False)
    # Let Plotly choose a readable interval for numeric data. Forcing every
    # distinct value as a tick made even modest datasets produce overlapping
    # labels. Explicit user tick values still take precedence.
    x_ticks, y_ticks = _ticks_from(config.get("x_tickvals")), _ticks_from(config.get("y_tickvals"))
    _set_ticks(xaxis, x_ticks)
    _set_ticks(yaxis, y_ticks)
    _apply_custom_tick_formatter(xaxis, "x", config, traces)
    _apply_custom_tick_formatter(yaxis, "y", config, traces)
    if x_ticks: xaxis.pop("dtick", None)
    if y_ticks: yaxis.pop("dtick", None)
    _apply_range(xaxis, config, "x")
    _apply_range(yaxis, config, "y")

    y2axis = None
    if y2_cols:
        y2_scale = config.get("y2_scale", "linear")
        y2axis = {**common, "overlaying": "y", "side": "right", "showgrid": False,
                  "title": {"text": config.get("y2_label") or ", ".join(sorted(y2_cols)), "standoff": 12},
                  "type": "log" if y2_scale == "log2" else y2_scale}
        if y2_scale == "log2": y2axis.update(dtick=0.3010299956639812, tickformat="~g")
        _apply_range(y2axis, config, "y2")
        for t in traces:
            # Prefer the "_ycol" tag set by plots.py (reliable even when the
            # display name is just a group label); custom-script traces
            # won't have it, so fall back to matching on the trace name.
            col = t.get("_ycol", t.get("name"))
            if col in y2_cols:
                t["yaxis"] = "y2"
    for t in traces:
        t.pop("_ycol", None)

    pos, show = config.get("legend_position", "right"), config.get("show_legend", True)
    style_cfg = styles.resolve(style)
    n_legend = min(len({t.get("name") for t in traces if t.get("name")}) or 1, 60)
    requested_rows = min(n_legend, max(0, int(float(config.get("legend_rows") or style_cfg.get("legend_rows") or 0))))
    requested_cols = max(0, int(float(config.get("legend_columns") or style_cfg.get("legend_columns") or 0)))
    orientation = config.get("legend_orientation") or style_cfg.get("legend_orientation", "auto")
    if orientation == "auto":
        orientation = "horizontal" if pos in ("top", "bottom") or requested_rows or requested_cols else "vertical"
    horizontal = orientation == "horizontal"
    legend_pos = "top" if horizontal and pos == "right" else pos
    font_px = styles.resolve(style)["legend_size"] * 96 / 72
    mt, mb, mr, extra_h, extra_w, center_w = MT, MB, MR, 0, 0, 0
    if legend_pos in ("top", "bottom"):
        ncols = min(requested_cols, n_legend) if requested_cols else 0
        if not ncols and requested_rows:
            ncols = max(1, math.ceil(n_legend / requested_rows))
        rows_needed = math.ceil(n_legend / ncols) if ncols else (math.ceil(n_legend / 4) if horizontal else n_legend)
        rows_needed = max(1, rows_needed)
        extra_h = rows_needed * ITEM_H
        if legend_pos == "top": mt += extra_h + TOP_GAP
        else: mb += extra_h
        if show: center_w = min(round(_legend_width(config, traces, font_px)), MAX_CENTER_W)
        if n_legend > 4: log(f"Legend has {n_legend} entries: figure reserves {extra_h}px for it.", "info")
    elif pos == "right" and show:
        mr = min(max(MR, round(_legend_width(config, traces, font_px))), MAX_MR)
        extra_w = mr - MR
        if mr >= MAX_MR: log("Legend labels very long: right margin capped, labels may clip.", "warn")

    legend = {"bgcolor": "rgba(255,255,255,.9)", "bordercolor": "#999", "borderwidth": 1,
              "itemsizing": "trace", "tracegroupgap": 2,
              "orientation": "h" if horizontal else "v", **LEGEND_POS.get(legend_pos, {})}
    if horizontal:
        ncols = min(requested_cols, n_legend) if requested_cols else 0
        if not ncols and requested_rows:
            ncols = max(1, math.ceil(n_legend / requested_rows))
        if ncols:
            legend.update(entrywidth=1 / ncols, entrywidthmode="fraction")
    if config.get("legend_title"):
        legend["title"] = {"text": config["legend_title"]}
    layout = {"title": {"text": config.get("title", ""), "x": 0.02, "xanchor": "left", "pad": {"t": 8, "b": 8}},
              "xaxis": xaxis, "yaxis": yaxis, "barmode": config.get("barmode", "group"),
              "paper_bgcolor": "#fff", "plot_bgcolor": "#fff", "showlegend": show, "legend": legend,
              "margin": {"l": 80, "r": mr, "t": mt, "b": mb, "pad": 4},
              "_legend_extra_height": extra_h, "_legend_extra_width": extra_w,
              "_legend_center_required_width": center_w}
    if y2axis:
        layout["yaxis2"] = y2axis
        layout["margin"]["r"] = max(layout["margin"]["r"], 80)  # room for the second axis + its title
    layout.update(overrides or {})
    return layout


def grid_domains(rows, cols, xgap=0.10, ygap=0.16):
    """(x-domain, y-domain) per cell, row-major, top-to-bottom."""
    w, h = (1 - xgap * (cols - 1)) / cols, (1 - ygap * (rows - 1)) / rows
    out = []
    for r in range(rows):
        for c in range(cols):
            x0, y1 = c * (w + xgap), 1 - r * (h + ygap)
            out.append(((round(x0, 4), round(x0 + w, 4)), (round(y1 - h, 4), round(y1, 4))))
    return out
