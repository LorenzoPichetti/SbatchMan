"""
Global figure styling. Applied as a post-process on (layout, traces), so it
works for built-in plots, plugins and custom scripts alike, and for grids.
Sizes are in points (pt); width_in/height_in fix the figure size in inches
(0 = fill the window). The frontend exports at dpi/96 scale.
"""
from copy import deepcopy

OKABE_ITO = ["#E69F00", "#56B4E9", "#009E73", "#F0E442", "#0072B2",
             "#D55E00", "#CC79A7", "#000000"]

DEFAULT = {
    "font_family": "Times New Roman, Times, serif",
    "title_size": 12, "label_size": 10, "tick_size": 9, "legend_size": 9,
    "palette": OKABE_ITO, "recolor": True,
    "line_width": 1.5, "marker_size": 5,
    "grid": True, "grid_color": "#e6e6e6", "mirror": True,
    "tick_dir": "outside", "tick_len": 4, "tick_width": 1,
    "minor_ticks": "auto",          # "auto" (log axes only) | "on" | "off"
    "exponent": "power",            # "power" -> 10^3 ; "e" -> 1e3 ; "SI"
    "tick_angle": "",               # "" = auto
    "width_in": 0, "height_in": 0,  # 0 = fill window
    "dpi": 300,
}

PRESETS = {
    "screen": {"width_in": 0, "height_in": 0, "label_size": 12, "tick_size": 11,
               "legend_size": 11, "title_size": 14, "line_width": 2, "marker_size": 7},
    "paper_single_column": {"width_in": 3.4, "height_in": 2.6, "label_size": 8,
                            "tick_size": 7, "legend_size": 7, "title_size": 9,
                            "line_width": 1.2, "marker_size": 4},
    "paper_double_column": {"width_in": 7.0, "height_in": 3.0, "label_size": 9,
                            "tick_size": 8, "legend_size": 8, "title_size": 10},
    "slides": {"width_in": 10, "height_in": 5.6, "label_size": 16, "tick_size": 14,
               "legend_size": 14, "title_size": 18, "line_width": 3, "marker_size": 9,
               "font_family": "Arial, Helvetica, sans-serif"},
}


def resolve(style=None):
    s = deepcopy(DEFAULT)
    s.update({k: v for k, v in (style or {}).items() if v is not None and v != ""
              or k == "tick_angle"})
    return s


def _px(pt):
    return round(float(pt) * 96 / 72, 2)


def _style_axis(ax, s):
    minor = s["minor_ticks"] == "on" or (s["minor_ticks"] == "auto" and ax.get("type") == "log")
    ax.update({
        "showgrid": bool(s["grid"]), "gridcolor": s["grid_color"], "mirror": bool(s["mirror"]),
        "ticks": s["tick_dir"], "ticklen": s["tick_len"], "tickwidth": s["tick_width"],
        "exponentformat": s["exponent"], "showexponent": "all",
        "tickfont": {"family": s["font_family"], "size": _px(s["tick_size"])},
    })
    title = ax.get("title")
    if isinstance(title, dict):
        title["font"] = {"family": s["font_family"], "size": _px(s["label_size"])}
    if s["tick_angle"] not in ("", None):
        ax["tickangle"] = float(s["tick_angle"])
    if minor:
        ax["minor"] = {"ticks": s["tick_dir"], "ticklen": max(1, s["tick_len"] * 0.6),
                       "tickwidth": s["tick_width"], "showgrid": False}


def apply(layout, traces, style=None):
    s = resolve(style)
    pal = s["palette"] if isinstance(s["palette"], list) and s["palette"] else OKABE_ITO

    if s["recolor"]:
        for i, t in enumerate(traces):
            c = pal[i % len(pal)]
            for key in ("line", "marker"):
                d = t.get(key)
                if isinstance(d, dict) and isinstance(d.get("color"), str):
                    d["color"] = c
    for t in traces:
        if isinstance(t.get("line"), dict) and "width" in t["line"] and t.get("type") != "bar":
            t["line"]["width"] = s["line_width"]
        m = t.get("marker")
        if isinstance(m, dict) and isinstance(m.get("size"), (int, float)):
            m["size"] = s["marker_size"]

    layout["font"] = {"family": s["font_family"], "size": _px(s["tick_size"])}
    layout["colorway"] = pal
    for k, v in list(layout.items()):
        if k.startswith(("xaxis", "yaxis")) and isinstance(v, dict):
            _style_axis(v, s)
    lg = layout.get("legend")
    if isinstance(lg, dict):
        lg["font"] = {"family": s["font_family"], "size": _px(s["legend_size"])}
        if isinstance(lg.get("title"), dict):
            lg["title"]["font"] = dict(lg["font"])
    if isinstance(layout.get("title"), dict):
        layout["title"]["font"] = {"family": s["font_family"], "size": _px(s["title_size"])}
    for a in layout.get("annotations", []) or []:
        a["font"] = {"family": s["font_family"], "size": _px(s["title_size"])}

    if s["width_in"] and s["height_in"]:
        layout.update({"autosize": False, "width": round(s["width_in"] * 96),
                       "height": round(s["height_in"] * 96)})
    layout["_export_dpi"] = s["dpi"]
    return layout, traces
