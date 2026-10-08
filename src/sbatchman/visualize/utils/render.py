"""High-level pipeline: SQL -> pandas transform -> traces -> layout -> style.
The HTTP handler only needs render_plot / render_grid / run_preview."""
from . import styles
from .core import (load_custom_plot_script, log, make_script_logger,
                   run_custom_plot_script, run_layout_script,
                   run_layout_script_with_hooks, run_transform_script)
from .db import (dataframe_to_df_data, df_data_to_dataframe,
                 get_all_tables_as_dataframes, run_query)
from .layout import build_layout, grid_domains
from .plots import BUILTIN_PLOTS, PLUGIN_PLOTS

_EXTRA = ("_legend_extra_height", "_legend_extra_width", "_legend_center_required_width")


def _grid_figure_size(p, rows, cols, style):
    """Resolve optional grid dimensions in inches (figure dimensions override per-cell size)."""
    if not any(k in p for k in ("figure_width_in", "figure_height_in", "subplot_width_in", "subplot_height_in")):
        return None
    style = styles.resolve(style)
    cell_w = max(.5, float(p.get("subplot_width_in") or 4))
    cell_h = max(.5, float(p.get("subplot_height_in") or 3.3))
    fig_w = float(p.get("figure_width_in") or style.get("width_in") or cell_w * cols)
    fig_h = float(p.get("figure_height_in") or style.get("height_in") or cell_h * rows)
    if fig_w <= 0 or fig_h <= 0:
        raise ValueError("Figure width and height must be greater than zero.")
    return fig_w, fig_h


def _facet_title(group_by, values, template):
    parts = []
    for col, value in zip(group_by, values):
        value = "NULL" if value is None else value
        try:
            parts.append(str(template).format(column=col, col=col, value=value, name=col))
        except (KeyError, ValueError, IndexError):
            parts.append(f"{col}={value}")
    return ", ".join(parts)


def run_pipeline(database, sql, transform_script=""):
    df_data, entries = run_query(database, sql), []
    if transform_script and transform_script.strip():
        data = get_all_tables_as_dataframes(database)
        # The SQL projection is the base plotting dataset. Keep track of all
        # source-table fields so assigning a whole table to data['result']
        # cannot silently bring unselected columns into the figure.
        selected_columns = list(df_data["columns"])
        source_columns = {str(col) for table in data.values() for col in table.columns}
        data["result"] = df_data_to_dataframe(df_data)
        script_log, entries = make_script_logger("transform")
        data = run_transform_script(transform_script, data, script_log)
        if data.get("result") is None:
            raise ValueError("The transform script must leave a DataFrame in data['result'].")
        result = data["result"]
        # Keep projected columns (including SQL aliases) and any genuinely
        # derived columns added by the transform. Existing database fields
        # omitted from the SELECT stay out of the data used to plot.
        keep = [c for c in selected_columns if c in result.columns]
        keep += [c for c in result.columns if c not in source_columns and c not in keep]
        df_data = dataframe_to_df_data(result.loc[:, keep])
    return df_data, entries


def compute_traces(df_data, plot_type, custom_script, config):
    if custom_script and custom_script.strip():
        return run_custom_plot_script(custom_script, df_data, config)
    reg = PLUGIN_PLOTS.get(plot_type) or BUILTIN_PLOTS.get(plot_type)
    if not reg:
        raise ValueError(f"Unknown plot type: {plot_type}")
    traces = reg["fn"](df_data, config)
    log(f"Plot '{plot_type}': {len(traces)} trace(s)")
    return traces


def _apply_matplotlib_layout_script(config, traces, style, source, scope="layout"):
    """Run the existing Plotly layout snippet and translate common axis and
    legend edits into the Matplotlib config understood by the image renderer."""
    from copy import deepcopy

    work_config = deepcopy(config)
    work_traces = deepcopy(traces)
    layout = build_layout(work_config, {}, work_traces, style)
    if scope.startswith("layout(panel"):
        layout = {"xaxis": layout["xaxis"], "yaxis": layout["yaxis"]}
    script_log, entries = make_script_logger(scope)
    edited, matplotlib_layout_fn = run_layout_script_with_hooks(source, layout, config, script_log)
    for axis in ("x", "y"):
        spec = edited.get(f"{axis}axis") or {}
        title = spec.get("title")
        if isinstance(title, dict) and title.get("text") is not None:
            config[f"{axis}_label"] = title["text"]
        scale = spec.get("type")
        if scale in ("linear", "log", "date", "category", "symlog", "logit"):
            config[f"{axis}_scale"] = scale
        if spec.get("tickformat"):
            config[f"{axis}_tickformat"] = spec["tickformat"]
        if isinstance(spec.get("range"), (list, tuple)) and len(spec["range"]) == 2:
            config[f"{axis}_min"], config[f"{axis}_max"] = spec["range"]
        if spec.get("autorange") == "reversed":
            config[f"{axis}_reversed"] = True
        if "tickangle" in spec:
            config[f"_mpl_{axis}_tick_angle"] = spec["tickangle"]
        if "showgrid" in spec:
            config[f"_mpl_{axis}_grid"] = bool(spec["showgrid"])
        if spec.get("tickvals") is not None:
            config[f"_mpl_{axis}_tickvals"] = spec["tickvals"]
        if spec.get("ticktext") is not None:
            config[f"_mpl_{axis}_ticktext"] = spec["ticktext"]
    title = edited.get("title")
    if isinstance(title, dict) and title.get("text") is not None:
        config["title"] = title["text"]
    if edited.get("barmode"):
        config["barmode"] = edited["barmode"]
    legend = edited.get("legend") or {}
    if legend.get("orientation") in ("h", "v"):
        config["legend_orientation"] = "horizontal" if legend["orientation"] == "h" else "vertical"
    if isinstance(legend.get("title"), dict) and legend["title"].get("text") is not None:
        config["legend_title"] = legend["title"]["text"]
    return config, entries, matplotlib_layout_fn


def preview_of(df_data, limit=500):
    return {"columns": df_data["columns"], "rows": df_data["rows"][:limit],
            "truncated": df_data.get("truncated", False) or len(df_data["rows"]) > limit}


def run_preview(p):
    df, entries = run_pipeline(p["database"], p["sql"], p.get("transform_script", ""))
    log(f"Preview on '{p['database']}': {len(df['rows'])} row(s)")
    return {"ok": True, "preview": preview_of(df), "columns": df["columns"], "log_entries": entries}


def render_plot(p):
    """Single figure. Order: build -> global style -> user layout script (script wins)."""
    config, style = p.get("config", {}), p.get("style")
    backend = p.get("backend", "matplotlib")
    if backend not in ("plotly", "matplotlib"):
        raise ValueError(f"Unknown plotting backend: {backend}")
    df, entries = run_pipeline(p["database"], p["sql"], p.get("transform_script", ""))
    if backend == "matplotlib":
        from .matplotlib_backend import render_matplotlib_plot
        custom_script = p.get("custom_script", "")
        custom_plot_fn = None
        if custom_script and custom_script.strip():
            script_ns = load_custom_plot_script(custom_script)
            custom_plot_fn = script_ns.get("plot_matplotlib")
            if callable(custom_plot_fn):
                traces = []
            else:
                plot_fn = script_ns.get("plot")
                if not callable(plot_fn):
                    raise ValueError(
                        "Custom plot script must define plot(df_data, config) for Plotly, "
                        "or plot_matplotlib(ax, df_data, config) for Matplotlib."
                    )
                traces = plot_fn(df, config)
        else:
            traces = compute_traces(df, p.get("plot_type", "line"), "", config)
        matplotlib_layout_fn = None
        if p.get("layout_script", "").strip():
            config, script_entries, matplotlib_layout_fn = _apply_matplotlib_layout_script(
                config, traces, style, p["layout_script"], "layout")
            entries += script_entries
        images = render_matplotlib_plot(
            df, traces, config, style,
            custom_plot_fn=custom_plot_fn, layout_fn=matplotlib_layout_fn,
        )
        return {**images, "columns": df["columns"], "truncated": df.get("truncated", False),
                "preview": preview_of(df), "log_entries": entries, "panel_count": 1}
    traces = compute_traces(df, p.get("plot_type", "line"), p.get("custom_script", ""), config)
    marker_size = config.get("legend_marker_size")
    if isinstance(marker_size, (int, float)) and marker_size > 0:
        for t in traces:
            t["_panel_marker_size"] = marker_size
    layout = build_layout(config, p.get("layout", {}), traces, style)
    # styles.apply needs the legend-sizing hints (still on layout, as _-keys)
    # to grow a fixed-size canvas correctly, so read them before popping.
    extras = {k.lstrip("_"): layout.get(k, 0) for k in _EXTRA}
    layout, traces = styles.apply(layout, traces, style)
    for k in _EXTRA:
        layout.pop(k, None)
    dpi = layout.pop("_export_dpi", 300)
    if p.get("layout_script", "").strip():
        script_log, e = make_script_logger("layout")
        layout = run_layout_script(p["layout_script"], layout, config, script_log); entries += e
    return {"traces": traces, "layout": layout, "columns": df["columns"],
            "truncated": df.get("truncated", False), "preview": preview_of(df),
            "log_entries": entries, "export_dpi": dpi, **extras}


import string
import math


def _dedupe_legend(traces):
    """A grid's traces all share one Plotly legend by default (there's only
    one 'legend' key on the whole figure). Without this, the same series
    name repeated across panels (e.g. 'A'/'B' groups in every panel) shows
    up as duplicate legend entries. Show only the first occurrence of each
    name; leave traces with no name (or showlegend already False) alone."""
    seen = set()
    for t in traces:
        name = t.get("name")
        if not name or t.get("showlegend") is False:
            continue
        if name in seen:
            t["showlegend"] = False
        else:
            seen.add(name)


def _panel_label_annotations(panel_labels, doms, rows, cols):
    """panel_labels: True -> automatic (a), (b), (c)... in row-major order;
    a list -> that list's entries by index (missing/blank entries are
    skipped); anything else (None, False) -> no labels."""
    if not panel_labels:
        return []
    if panel_labels is True:
        labels = [f"({c})" for c in string.ascii_lowercase[:len(doms)]]
    else:
        labels = list(panel_labels)
    out = []
    for i, ((x0, x1), (y0, y1)) in enumerate(doms):
        if i >= len(labels) or not labels[i]:
            continue
        out.append({"text": labels[i], "x": x0, "y": y1 + .04, "xref": "paper", "yref": "paper",
                    "showarrow": False, "xanchor": "left", "yanchor": "bottom",
                    "font": {"size": 13}})
    return out


def render_grid(p):
    """Subplot grid. Optional: share_x / share_y (per column / per row), xgap,
    ygap, panel_labels (True for automatic (a)/(b)/(c)..., or a list of
    custom strings), legend ({'position','title','show'} for the one legend
    the whole figure shares — duplicate-named entries are always deduped)."""
    backend = p.get("backend", "matplotlib")
    if backend not in ("plotly", "matplotlib"):
        raise ValueError(f"Unknown plotting backend: {backend}")
    if backend == "matplotlib":
        return _render_matplotlib_grid(p)
    rows, cols = int(p.get("rows", 1)), int(p.get("cols", 1))
    if rows < 1 or cols < 1: raise ValueError("Grid rows/cols must be >= 1.")
    style = p.get("style")
    # Axis titles and tick labels live outside each subplot's domain. A tight
    # domain gap makes the inner axes of a 2×2 figure draw into their
    # neighbours, so reserve enough gutter for those labels even for saved
    # workspaces that predate the larger defaults.
    xgap = min(max(float(p.get("xgap", .20)), .20 if cols > 1 else 0), .35 / max(cols - 1, 1))
    ygap = min(max(float(p.get("ygap", .24)), .24 if rows > 1 else 0), .35 / max(rows - 1, 1))
    doms = grid_domains(rows, cols, xgap, ygap)
    layout = {"paper_bgcolor": "#fff", "plot_bgcolor": "#fff", "annotations": [],
              "margin": {"l": 76, "r": 120, "t": 52, "b": 72}}
    all_traces, previews, entries, scripts = [], [], [], []

    # Optional faceting: query and transform the first panel once, then plot
    # each distinct selected-column combination as its own panel.
    group_by = list(dict.fromkeys(p.get("group_by") or []))
    panels = list(p.get("panels", []))
    if group_by:
        if not panels:
            raise ValueError("Configure Panel 1 before grouping it into a grid.")
        source = panels[0]
        source_df, source_entries = run_pipeline(
            source["database"], source["sql"], source.get("transform_script", ""))
        entries += source_entries
        missing = [col for col in group_by if col not in source_df["columns"]]
        if missing:
            raise ValueError("Group-by column(s) not found in Panel 1 query result: " + ", ".join(missing))
        positions = [source_df["columns"].index(col) for col in group_by]
        groups = {}
        for row in source_df["rows"]:
            key = tuple(row[i] for i in positions)
            groups.setdefault(key, []).append(row)
        if not groups:
            raise ValueError("Panel 1 query returned no rows to group.")
        panels = []
        for values, rows_for_group in groups.items():
            panel = dict(source)
            panel["config"] = dict(source.get("config", {}))
            label = _facet_title(group_by, values, p.get("facet_label_format", "{column}={value}"))
            base_title = str(panel["config"].get("title") or "").strip()
            panel["config"]["title"] = f"{base_title} — {label}" if base_title else label
            # Facet columns identify the panel, so do not also create a
            # same-named color group inside every panel.
            color_groups = panel["config"].get("group") or []
            if isinstance(color_groups, str):
                color_groups = [color_groups]
            panel["config"]["group"] = [c for c in color_groups if c not in group_by]
            panel["_grouped_df"] = {"columns": source_df["columns"], "rows": rows_for_group,
                                    "truncated": source_df.get("truncated", False)}
            panels.append(panel)
        count = len(panels)
        facet_axis = p.get("facet_axis", "cols")
        facet_count = int(p.get("facet_count", cols if facet_axis == "cols" else rows))
        if facet_count < 1 or facet_count > 6:
            raise ValueError("Data-driven grid row/column count must be between 1 and 6.")
        if facet_axis == "rows":
            rows, cols = facet_count, math.ceil(count / facet_count)
        else:
            cols, rows = facet_count, math.ceil(count / facet_count)
        if rows > 6 or cols > 6:
            raise ValueError(f"Grouping produced {count} panels; the grid supports at most 6 rows and 6 columns.")
        doms = grid_domains(rows, cols,
                            min(max(float(p.get("xgap", .20)), .20 if cols > 1 else 0), .35 / max(cols - 1, 1)),
                            min(max(float(p.get("ygap", .24)), .24 if rows > 1 else 0), .35 / max(rows - 1, 1)))
    else:
        count = min(len(panels), rows * cols)

    # The grid-level switch is authoritative: when enabled, always combine
    # traces into one deduplicated legend; when disabled, create one per panel.
    per_panel_legends = p.get("share_legend", True) is False
    grid_legend_show = (p.get("legend") or {}).get("show", True) is not False
    panel_legend_keys = []

    # Plotly axis keys ('xaxis', 'xaxis2', ...) must be unique figure-wide.
    # Each panel's primary x/y pair gets the axis number matching its slot
    # (panel i -> number i+1, same scheme share_x/share_y's 'matches' below
    # assumes), same as before. A panel's own secondary y-axis (config['y2'])
    # would collide with this if it also claimed a number in 1..len(doms) —
    # build_layout always calls it 'yaxis2' internally — so secondary axes
    # are instead numbered starting right after the last panel slot.
    def main_sfx(i):
        n = i + 1
        return "" if n == 1 else str(n)
    next_extra = [len(doms) + 1]
    def alloc_extra():
        n = next_extra[0]; next_extra[0] += 1
        return str(n)

    panel_xsfx, panel_ysfx = [], []
    for i, panel in enumerate(panels[:len(doms)]):
        r, c = divmod(i, cols)
        xsfx, ysfx = main_sfx(i), main_sfx(i)
        panel_xsfx.append(xsfx); panel_ysfx.append(ysfx)
        config = panel.get("config", {})
        if "_grouped_df" in panel:
            df, e = panel["_grouped_df"], []
        else:
            df, e = run_pipeline(panel["database"], panel["sql"], panel.get("transform_script", ""))
            entries += e
        traces = compute_traces(df, panel.get("plot_type", "line"), panel.get("custom_script", ""), config)
        # build_layout runs BEFORE trace axis assignment: it's what tags a
        # y2-column trace with t['yaxis'] = 'y2' (see layout.py), and that
        # tag has to be read before we overwrite it with this panel's real
        # (possibly renumbered) axis names below.
        sub = build_layout(config, {}, traces, style)
        if per_panel_legends:
            legend_key = "legend" if i == 0 else f"legend{i + 1}"
            panel_legend_keys.append((legend_key, sub["legend"], config))
            marker_size = config.get("legend_marker_size")
            if isinstance(marker_size, (int, float)) and marker_size > 0:
                for t in traces:
                    t["_panel_marker_size"] = marker_size
            for t in traces:
                t["legend"] = legend_key
                if config.get("show_legend", True) is False:
                    t["showlegend"] = False
        has_y2 = "yaxis2" in sub
        y2sfx = alloc_extra() if has_y2 else None
        for t in traces:
            t["xaxis"] = f"x{xsfx}"
            t["yaxis"] = f"y{y2sfx}" if (has_y2 and t.get("yaxis") == "y2") else f"y{ysfx}"
        all_traces += traces

        (x0, x1), (y0, y1) = doms[i]
        xa, ya = dict(sub["xaxis"], domain=[x0, x1]), dict(sub["yaxis"], domain=[y0, y1])
        # Put the right-hand column's primary y axis on the outside edge.
        # Its title otherwise sits in the center gutter and can intrude into
        # the left-hand panel when labels are long.
        if cols > 1 and c > 0 and not has_y2 and not p.get("share_y"):
            ya["side"] = "right"
        if p.get("share_x") and r > 0:   # match the top panel of this column, hide inner tick labels
            xa["matches"] = f"x{panel_xsfx[c]}"
        if p.get("share_x") and r < rows - 1:
            xa["showticklabels"] = False; xa["title"] = {"text": ""}
        if p.get("share_y") and c > 0:   # match the leftmost panel of this row
            ya["matches"] = f"y{panel_ysfx[r * cols]}"
        if p.get("share_y") and c > 0:
            ya["showticklabels"] = False; ya["title"] = {"text": ""}
        layout[f"xaxis{xsfx}"], layout[f"yaxis{ysfx}"] = xa, ya
        if has_y2:
            # 'overlaying'/'anchor' must point at this panel's own renamed
            # primary axes (not the figure-wide default 'y'/'x'), and the
            # domain has to be repeated explicitly or Plotly won't place the
            # overlaying axis in this panel's cell.
            y2a = dict(sub["yaxis2"], domain=[y0, y1], overlaying=f"y{ysfx}", anchor=f"x{xsfx}")
            layout[f"yaxis{y2sfx}"] = y2a
            layout["margin"]["r"] = max(layout["margin"]["r"], 120)  # room for the extra axis + its title
        if config.get("title"):
            layout["annotations"].append({"text": config["title"], "x": (x0 + x1) / 2, "y": y1 + .02,
                "xref": "paper", "yref": "paper", "showarrow": False, "xanchor": "center", "yanchor": "bottom"})
        if panel.get("layout_script", "").strip():
            scripts.append((xsfx, ysfx, i, panel["layout_script"], config))
        previews.append(preview_of(df, 200))

    layout["annotations"] += _panel_label_annotations(p.get("panel_labels"), doms, rows, cols)

    lg = p.get("legend") or {}
    source_legend_config = (panels[0].get("config", {}) if panels else {})
    legend_cfg = {
        **source_legend_config,
        "legend_position": lg.get("position", "right"),
        "legend_title": lg.get("title", ""),
        "show_legend": lg.get("show", True),
    }
    if per_panel_legends:
        # Legend coordinates are set inside each subplot cell so neighboring
        # panels cannot paint one another's labels. Position follows each
        # panel's position choice; orientation and row/column counts come from
        # that panel's own layout.
        for i, panel in enumerate(panels[:len(doms)]):
            key, legend, config = panel_legend_keys[i]
            (x0, x1), (y0, y1) = doms[i]
            pos = config.get("legend_position", "right")
            horizontal = legend.get("orientation") == "h"
            if horizontal:
                legend.update(x=(x0 + x1) / 2, xanchor="center")
                if pos == "bottom" or pos == "inside-bottom-left" or pos == "inside-bottom-right":
                    legend.update(y=y0 + .015, yanchor="bottom")
                else:
                    legend.update(y=y1 - .015, yanchor="top")
            else:
                if pos in ("inside-top-left", "inside-bottom-left"):
                    legend.update(x=x0 + .015, xanchor="left")
                else:
                    legend.update(x=x1 - .015, xanchor="right")
                if pos in ("inside-bottom-left", "inside-bottom-right", "bottom"):
                    legend.update(y=y0 + .015, yanchor="bottom")
                else:
                    legend.update(y=y1 - .015, yanchor="top")
            layout[key] = legend
        layout["showlegend"] = any(
            config.get("show_legend", True) is not False
            for _, _, config in panel_legend_keys
        )
        legend_layout = build_layout(legend_cfg, {}, [], style)
    else:
        marker_size = source_legend_config.get("legend_marker_size")
        if isinstance(marker_size, (int, float)) and marker_size > 0:
            for trace in all_traces:
                trace["_panel_marker_size"] = marker_size
        _dedupe_legend(all_traces)
        legend_layout = build_layout(legend_cfg, {}, all_traces, style)
        layout["legend"], layout["showlegend"] = legend_layout["legend"], legend_layout["showlegend"]
    # Grow the grid's own margin the same way a single plot's right-side
    # legend does, so the shared legend gets real space instead of
    # overlapping the rightmost panel.
    if legend_cfg["show_legend"] and not per_panel_legends:
        orientation = legend_layout["legend"].get("orientation", "v")
        if orientation == "h":
            margin_side = "b" if legend_cfg["legend_position"] == "bottom" else "t"
            layout["margin"][margin_side] = max(layout["margin"][margin_side], legend_layout["margin"][margin_side])
        elif legend_cfg["legend_position"] == "right":
            layout["margin"]["r"] = max(layout["margin"]["r"], legend_layout["margin"]["r"])
    legend_extra_height = legend_layout.get("_legend_extra_height", 0) if not per_panel_legends else 0
    legend_extra_width = legend_layout.get("_legend_extra_width", 0) if not per_panel_legends else 0

    layout, all_traces = styles.apply(layout, all_traces, style)
    figure_size = _grid_figure_size(p, rows, cols, style)
    if figure_size:
        layout["width"] = round(figure_size[0] * 96)
        layout["height"] = round(figure_size[1] * 96)
        layout["autosize"] = False
    dpi = layout.pop("_export_dpi", 300)
    for xsfx, ysfx, i, src, config in scripts:      # panel scripts run last so they win over the global style
        script_log, e = make_script_logger(f"layout(panel {i + 1})")
        sub = run_layout_script(src, {"xaxis": layout[f"xaxis{xsfx}"], "yaxis": layout[f"yaxis{ysfx}"]}, config, script_log)
        layout[f"xaxis{xsfx}"], layout[f"yaxis{ysfx}"] = sub.get("xaxis"), sub.get("yaxis"); entries += e
    return {"traces": all_traces, "layout": layout, "previews": previews, "log_entries": entries,
            "panel_count": count,
            "grid_rows": rows, "grid_cols": cols,
            "export_dpi": dpi, "legend_extra_height": legend_extra_height, "legend_extra_width": legend_extra_width}


def _render_matplotlib_grid(p):
    from .matplotlib_backend import render_matplotlib_grid

    rows, cols = int(p.get("rows", 1)), int(p.get("cols", 1))
    if rows < 1 or cols < 1:
        raise ValueError("Grid rows/cols must be >= 1.")
    panels = list(p.get("panels", []))
    style = p.get("style")
    entries, previews = [], []
    group_by = list(dict.fromkeys(p.get("group_by") or []))
    if group_by:
        if not panels:
            raise ValueError("Configure Panel 1 before grouping it into a grid.")
        source = panels[0]
        source_df, source_entries = run_pipeline(
            source["database"], source["sql"], source.get("transform_script", ""))
        entries += source_entries
        missing = [col for col in group_by if col not in source_df["columns"]]
        if missing:
            raise ValueError("Group-by column(s) not found in Panel 1 query result: " + ", ".join(missing))
        indexes = [source_df["columns"].index(col) for col in group_by]
        groups = {}
        for row in source_df["rows"]:
            groups.setdefault(tuple(row[index] for index in indexes), []).append(row)
        if not groups:
            raise ValueError("Panel 1 query returned no rows to group.")
        panels = []
        for values, group_rows in groups.items():
            panel = dict(source)
            panel["config"] = dict(source.get("config", {}))
            label = _facet_title(group_by, values, p.get("facet_label_format", "{column}={value}"))
            title = str(panel["config"].get("title") or "").strip()
            panel["config"]["title"] = f"{title} — {label}" if title else label
            group_cols = panel["config"].get("group") or []
            group_cols = [group_cols] if isinstance(group_cols, str) else group_cols
            panel["config"]["group"] = [col for col in group_cols if col not in group_by]
            panel["_grouped_df"] = {"columns": source_df["columns"], "rows": group_rows,
                                    "truncated": source_df.get("truncated", False)}
            panels.append(panel)
        facet_axis = p.get("facet_axis", "cols")
        facet_count = int(p.get("facet_count", cols if facet_axis == "cols" else rows))
        if facet_count < 1 or facet_count > 6:
            raise ValueError("Data-driven grid row/column count must be between 1 and 6.")
        if facet_axis == "rows":
            rows, cols = facet_count, math.ceil(len(panels) / facet_count)
        else:
            cols, rows = facet_count, math.ceil(len(panels) / facet_count)
        if rows > 6 or cols > 6:
            raise ValueError(f"Grouping produced {len(panels)} panels; the grid supports at most 6 rows and 6 columns.")
    else:
        panels = panels[:rows * cols]
    for panel in panels:
        if "_grouped_df" in panel:
            df = panel["_grouped_df"]
        else:
            df, panel_entries = run_pipeline(
                panel["database"], panel["sql"], panel.get("transform_script", ""))
            entries += panel_entries
        panel["_df_data"] = df
        panel_config = panel.get("config", {})
        custom_script = panel.get("custom_script", "")
        panel["_custom_mpl_plot_fn"] = None
        if custom_script and custom_script.strip():
            script_ns = load_custom_plot_script(custom_script)
            panel["_custom_mpl_plot_fn"] = script_ns.get("plot_matplotlib")
            if callable(panel["_custom_mpl_plot_fn"]):
                panel_traces = []
            else:
                plot_fn = script_ns.get("plot")
                if not callable(plot_fn):
                    raise ValueError(
                        "Custom plot script must define plot(df_data, config) for Plotly, "
                        "or plot_matplotlib(ax, df_data, config) for Matplotlib."
                    )
                panel_traces = plot_fn(df, panel_config)
        else:
            panel_traces = compute_traces(df, panel.get("plot_type", "line"), "", panel_config)
        panel["_matplotlib_layout_fn"] = None
        if panel.get("layout_script", "").strip():
            panel_config, script_entries, panel["_matplotlib_layout_fn"] = _apply_matplotlib_layout_script(
                panel_config, panel_traces, p.get("style"), panel["layout_script"], f"layout(panel {len(previews) + 1})")
            entries += script_entries
            panel["config"] = panel_config
        panel["_traces"] = panel_traces
        previews.append(preview_of(df, 200))
    figure_size = _grid_figure_size(p, rows, cols, style)
    if figure_size:
        style = {**(style or {}), "width_in": figure_size[0], "height_in": figure_size[1]}
    images = render_matplotlib_grid(
        panels, rows, cols, style, p.get("share_x", False), p.get("share_y", False),
        p.get("panel_labels", False), p.get("legend") or {},
        p.get("xgap", .20), p.get("ygap", .24), p.get("share_legend", True))
    return {**images, "previews": previews, "log_entries": entries, "panel_count": len(panels),
            "grid_rows": rows, "grid_cols": cols}
