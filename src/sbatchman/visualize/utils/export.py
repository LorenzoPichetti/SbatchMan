"""
Headless export: regenerate every tab of a saved workspace (the same JSON
produced by the app's "Export workspace" button / plots.json) as image
files, without opening a browser. Meant for a CLI hook (e.g.
`sbatchman plots export`) so figures can be regenerated in CI or a paper's
build script straight from the same config the interactive app uses.

Needs `plotly` and `kaleido` (pip install plotly kaleido). These are not
imported at module load time, so the rest of the app works without them;
only calling export_workspace requires them.
"""
import re
import base64
from pathlib import Path

from .render import render_grid, render_plot

VALID_FORMATS = {"png", "svg", "pdf"}


def _safe_name(label: str) -> str:
    return re.sub(r"[^\w.-]+", "-", label).strip("-") or "figure"


def _panel_payload(node_cfg: dict, style: dict | None) -> dict:
    """A tab's exported config has database/sql/plotType/... at the top
    level (see app.js getNodeConfig, extended by axis_addon.js for the
    xMin/y2Cols/errorY/... fields); render_plot wants plot_type/config."""
    extra = node_cfg.get("extra", {})
    config = {
        "title": node_cfg.get("chartTitle", ""), "x": node_cfg.get("x", ""),
        "y": node_cfg.get("y", []), "group": node_cfg.get("group", []),
        "marker_by": node_cfg.get("markerBy", ""), "dash_by": node_cfg.get("dashBy", ""),
        "z": node_cfg.get("z", ""), "x_label": node_cfg.get("xLabel", ""),
        "y_label": node_cfg.get("yLabel", ""), "x_scale": node_cfg.get("xScale") or "linear",
        "y_scale": node_cfg.get("yScale") or "linear", "x_tickformat": node_cfg.get("xTickFmt", ""),
        "y_tickformat": node_cfg.get("yTickFmt", ""), "legend_position": node_cfg.get("legendPosition") or "right",
        "legend_orientation": node_cfg.get("legendOrientation", ""),
        "legend_rows": node_cfg.get("legendRows", 0), "legend_columns": node_cfg.get("legendColumns", 0),
        "legend_marker_size": node_cfg.get("legendMarkerSize", 0),
        "legend_title": node_cfg.get("legendTitle", ""), "show_legend": node_cfg.get("showLegend", True),
        "x_min": node_cfg.get("xMin", ""), "x_max": node_cfg.get("xMax", ""),
        "y_min": node_cfg.get("yMin", ""), "y_max": node_cfg.get("yMax", ""),
        "x_reversed": node_cfg.get("xReversed", False), "y_reversed": node_cfg.get("yReversed", False),
        "y2": node_cfg.get("y2Cols", []), "y2_label": node_cfg.get("y2Label", ""),
        "y2_min": node_cfg.get("y2Min", ""), "y2_max": node_cfg.get("y2Max", ""),
        "error_y": node_cfg.get("errorY", ""), "error_y_low": node_cfg.get("errorYLow", ""),
        "error_y_high": node_cfg.get("errorYHigh", ""), "error_style": node_cfg.get("errorStyle") or "bars",
        **extra,
    }
    return {"database": node_cfg.get("database", ""), "sql": node_cfg.get("sql", ""),
            "plot_type": node_cfg.get("plotType", "line"), "config": config,
            "custom_script": node_cfg.get("customScript", ""),
            "transform_script": node_cfg.get("transformScript", ""),
            "layout_script": node_cfg.get("layoutScript", ""), "style": style,
        "backend": node_cfg.get("rendererBackend", "matplotlib")}


def _write(fig_json: dict, out_path: Path, fmt: str, dpi: int):
    """fig_json is {'traces':..., 'layout':...} as returned by render_plot/render_grid."""
    import plotly.graph_objects as go
    fig = go.Figure(data=fig_json["traces"], layout=fig_json["layout"])
    scale = max(dpi, 1) / 96  # layout width/height are in CSS px @96dpi; kaleido scales from there
    fig.write_image(str(out_path), format=fmt, scale=scale if fmt == "png" else 1)


def _write_rendered_image(result: dict, out_path: Path, fmt: str):
    encoded = result.get(f"image_{fmt}")
    if not encoded:
        raise ValueError(f"The selected renderer did not produce a {fmt.upper()} image.")
    out_path.write_bytes(base64.b64decode(encoded))


def export_workspace(workspace: dict, out_dir="figures", formats=("png",), style: dict | None = None):
    """workspace: the dict produced by 'Export workspace' ({'tabs': [...]});
    a plain plots.json also has this shape. Returns the list of written paths.
    Any figure that fails to render is skipped, logged to stderr via the
    returned 'errors' list, and does not stop the rest of the export.

    Style precedence (lowest to highest): the `style` argument (e.g. a CLI
    --style preset) < workspace['style'] (the global style saved into the
    workspace by the app's Style tab) < a tab's own style override, if any.
    """
    formats = [f for f in formats if f in VALID_FORMATS]
    if not formats:
        raise ValueError(f"No valid export format given; choose from {sorted(VALID_FORMATS)}")
    out = Path(out_dir); out.mkdir(parents=True, exist_ok=True)
    written, errors = [], []
    base_style = {**(style or {}), **(workspace.get("style") or {})}

    for tab in workspace.get("tabs", []):
        name = _safe_name(tab.get("label", "figure"))
        tab_style = {**base_style, **(tab.get("style") or {})}
        try:
            if tab.get("grid"):
                grid = tab["grid"]
                panels = [_panel_payload(p, tab_style) for p in grid["panels"]]
                group_by = grid.get("groupBy", grid.get("facetGroupCols", [])) if tab.get("mode") == "facet" else []
                lg = {"position": grid.get("legendPosition") or "right", "title": grid.get("legendTitle", ""),
                     "show": grid.get("showLegend", True)}
                result = render_grid({
                    "rows": grid["rows"], "cols": grid["cols"], "panels": panels, "style": tab_style,
                    "backend": tab.get("rendererBackend", "matplotlib"),
                    "share_x": grid.get("shareX", False), "share_y": grid.get("shareY", False),
                    "share_legend": grid.get("shareLegend", True), "group_by": group_by,
                    "facet_axis": grid.get("facetAxis", "cols"), "facet_count": grid.get("facetCount", grid.get("cols", 1)),
                    "facet_label_format": grid.get("facetLabelFormat", "{column}={value}"),
                    "xgap": grid.get("xgap") if grid.get("xgap") is not None else 0.10,
                    "ygap": grid.get("ygap") if grid.get("ygap") is not None else 0.16,
                    "panel_labels": grid.get("panelLabels", False), "legend": lg,
                })
            else:
                result = render_plot(_panel_payload(tab, tab_style))
        except Exception as e:
            errors.append({"tab": tab.get("label", "?"), "error": str(e)})
            continue

        dpi = result.get("export_dpi", 300)
        for fmt in formats:
            path = out / f"{name}.{fmt}"
            try:
                if result.get(f"image_{fmt}"):
                    _write_rendered_image(result, path, fmt)
                else:
                    _write(result, path, fmt, dpi)
                written.append(str(path))
            except Exception as e:
                errors.append({"tab": tab.get("label", "?"), "format": fmt, "error": str(e)})

    return {"written": written, "errors": errors}
