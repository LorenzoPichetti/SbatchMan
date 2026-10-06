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
    "title_size": 14, "label_size": 12, "tick_size": 10, "legend_size": 10,
    "palette": OKABE_ITO, "recolor": True,
    "line_width": 1.5, "marker_size": 5,
    "grid": True, "grid_color": "#e6e6e6", "mirror": True,
    "tick_dir": "outside", "tick_len": 4, "tick_width": 1,
    "minor_ticks": "auto",          # "auto" (log axes only) | "on" | "off"
    "exponent": "power",            # "power" -> 10^3 ; "e" -> 1e3 ; "SI"
    "tick_angle": "",               # "" = auto
    "legend_orientation": "auto",    # auto | vertical | horizontal
    "legend_rows": 0, "legend_columns": 0,  # horizontal legend wrapping
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


def with_alpha(color, alpha):
    """'#rrggbb' -> 'rgba(r,g,b,alpha)'. Non-hex colors (already rgb()/rgba())
    pass through untouched."""
    if isinstance(color, str) and color.startswith("#") and len(color) == 7:
        r, g, b = int(color[1:3], 16), int(color[3:5], 16), int(color[5:7], 16)
        return f"rgba({r},{g},{b},{alpha})"
    return color


def _is_band_helper(t):
    """True for the two invisible fill-region traces plots.py emits for
    error_style='band' (identified by name suffix, since they carry no other
    marker linking them to the visible trace they shade)."""
    return isinstance(t.get("name"), str) and t["name"].endswith(" (band)")


def _style_axis(ax, s):
    minor = s["minor_ticks"] == "on" or (s["minor_ticks"] == "auto" and ax.get("type") == "log")
    ax.update({
        "showgrid": bool(s["grid"]), "gridcolor": s["grid_color"], "mirror": bool(s["mirror"]),
        "ticks": s["tick_dir"], "ticklen": s["tick_len"], "tickwidth": s["tick_width"],
        "exponentformat": s["exponent"], "showexponent": "all",
        "tickfont": {"family": s["font_family"], "size": _px(s["tick_size"])},
        "ticklabelstandoff": 5,
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
        # Color is assigned per unique series NAME (first-seen order), not
        # per trace position. This matters for grids: the same group name
        # ('A', say) can appear in every panel as a separate trace, and it
        # must get the same color in all of them — otherwise a deduplicated
        # shared legend entry ends up showing one color while some panel's
        # actual line is drawn in another. Band-helper traces share their
        # visible trace's name-derived color via fillcolor, not this map.
        name_color, next_i = {}, 0
        pending_band = []
        for t in traces:
            if _is_band_helper(t):
                pending_band.append(t)
                continue
            key = t.get("name")
            if key not in name_color:
                name_color[key] = pal[next_i % len(pal)]
                next_i += 1
            c = name_color[key]
            for attr in ("line", "marker"):
                d = t.get(attr)
                if isinstance(d, dict) and isinstance(d.get("color"), str):
                    d["color"] = c
            for bt in pending_band:
                if bt.get("fill") == "tonexty":
                    bt["fillcolor"] = with_alpha(c, 0.2)
            pending_band = []
    for t in traces:
        if isinstance(t.get("line"), dict) and "width" in t["line"] and t.get("type") != "bar":
            t["line"]["width"] = s["line_width"]
        panel_marker_size = t.pop("_panel_marker_size", None)
        m = t.get("marker")
        if isinstance(m, dict) and isinstance(m.get("size"), (int, float)):
            m["size"] = panel_marker_size if isinstance(panel_marker_size, (int, float)) else s["marker_size"]

    layout["font"] = {"family": s["font_family"], "size": _px(s["tick_size"])}
    layout["colorway"] = pal
    for k, v in list(layout.items()):
        if k.startswith(("xaxis", "yaxis")) and isinstance(v, dict):
            _style_axis(v, s)
    for legend_key, lg in layout.items():
        if (legend_key == "legend" or legend_key.startswith("legend")) and isinstance(lg, dict):
            lg["font"] = {"family": s["font_family"], "size": _px(s["legend_size"])}
            # Let each legend glyph reflect its trace's marker size.
            lg["itemsizing"] = "trace"
            if isinstance(lg.get("title"), dict):
                lg["title"]["font"] = dict(lg["font"])
    if isinstance(layout.get("title"), dict):
        layout["title"]["font"] = {"family": s["font_family"], "size": _px(s["title_size"])}
        layout["title"].setdefault("pad", {"t": 12, "b": 10})
    for a in layout.get("annotations", []) or []:
        a["font"] = {"family": s["font_family"], "size": _px(s["title_size"])}

    if s["width_in"] and s["height_in"]:
        # Grow the fixed canvas by whatever extra room the legend needs
        # (already computed by build_layout), the same way the on-screen
        # canvas grows — otherwise a right/top/bottom legend eats into the
        # plot area instead of getting its own space.
        extra_w = layout.get("_legend_extra_width", 0)
        extra_h = layout.get("_legend_extra_height", 0)
        layout.update({"autosize": False,
                       "width": round(s["width_in"] * 96) + extra_w,
                       "height": round(s["height_in"] * 96) + extra_h})
    layout["_export_dpi"] = s["dpi"]
    return layout, traces
