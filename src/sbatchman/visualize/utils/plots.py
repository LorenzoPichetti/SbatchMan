"""Built-in plot types + plugin loader. A plot fn is fn(df_data, config) -> [plotly trace dicts]."""
import importlib.util
import math
import numbers
from pathlib import Path
from typing import List, Optional
from .core import log
from .styles import OKABE_ITO as COLORWAY, with_alpha

BUILTIN_PLOTS: dict = {}
PLUGIN_PLOTS: dict = {}
DASHES = ["solid", "dash", "dot", "dashdot", "longdash", "longdashdot"]
MARKERS = ["circle", "square", "diamond", "cross", "x", "triangle-up", "triangle-down", "star", "pentagon", "hexagon"]
LW, MS = 2.0, 7


def register_plot(name, label, description, defaults=None):
    def deco(fn):
        BUILTIN_PLOTS[name] = dict(name=name, label=label, description=description, defaults=defaults or {}, fn=fn)
        return fn
    return deco


def _cols(g) -> List[str]:
    return [] if not g else [g] if isinstance(g, str) else [c for c in g if c]


def _color(i):
    return COLORWAY[i % len(COLORWAY)]


def _style_map(rows, ci, columns, seq):
    columns = _cols(columns)
    if not columns or any(col not in ci for col in columns):
        return {}
    vals = sorted({tuple(r[ci[col]] for col in columns) for r in rows}, key=lambda values: [str(v) for v in values])
    return {value: seq[i % len(seq)] for i, value in enumerate(vals)}


def _drop_unplottable(rows, ci, col, is_log):
    """Log axes cannot place None/<=0 values."""
    if not is_log or col not in ci:
        return rows
    return [r for r in rows if r[ci[col]] is not None and r[ci[col]] > 0]


def _line_x_sort_key(row, x_index):
    """Sort line points by x, handling nulls and mixed SQLite value types."""
    value = row[x_index]
    if value is None:
        return (2, "")
    if isinstance(value, numbers.Number):
        return (0, value)
    return (1, str(value))


def _ys(cfg):
    ys = cfg.get("y", [])
    return [ys] if isinstance(ys, str) else list(ys)


def _error_y(cfg, ci, pts):
    """Builds a Plotly error_y dict from config['error_y'] (a symmetric error
    column) or config['error_y_low']/['error_y_high'] (asymmetric bounds,
    e.g. from agg.summarize(err='minmax')). None of these need to be present."""
    sym, lo, hi = cfg.get("error_y"), cfg.get("error_y_low"), cfg.get("error_y_high")
    if sym and sym in ci:
        return {"type": "data", "array": [r[ci[sym]] for r in pts], "visible": True}
    if lo and hi and lo in ci and hi in ci:
        return {"type": "data", "symmetric": False,
                "array": [r[ci[hi]] for r in pts], "arrayminus": [r[ci[lo]] for r in pts], "visible": True}
    return None


def _xy(df, cfg, kind):
    """Shared engine for line / scatter / bar: groups x y-columns -> traces."""
    ci = {c: i for i, c in enumerate(df["columns"])}; rows = df["rows"]
    x, ys = cfg.get("x"), _ys(cfg)
    if not x or not ys:
        raise ValueError(f"{kind} chart requires x and at least one y.")
    groups = _cols(cfg.get("group"))
    marker_by = [c for c in _cols(cfg.get("marker_by")) if c in ci] if kind != "bar" else []
    dash_by = [c for c in _cols(cfg.get("dash_by")) if c in ci] if kind == "line" else []
    color_groups = [c for c in groups if c not in dash_by]
    mmap, dmap = _style_map(rows, ci, marker_by, MARKERS), _style_map(rows, ci, dash_by, DASHES)
    split = groups + [c for c in dash_by if c not in groups]
    lx = cfg.get("x_scale") in ("log", "log2")
    y2_cols = set(_cols(cfg.get("y2")))

    def legend_value(value):
        return "(missing)" if value is None or (isinstance(value, str) and not value.strip()) else str(value)

    def legend_fields(columns, values):
        template = cfg.get("legend_field_format") or "{column}={value}"
        separator = cfg.get("legend_field_separator", " | ")
        separator = " | " if separator is None else str(separator)
        fields = []
        for column, value in zip(columns, values):
            value = legend_value(value)
            try:
                fields.append(str(template).format(column=column, value=value))
            except (KeyError, ValueError, IndexError, TypeError):
                fields.append(f"{column}={value}")
        return separator.join(fields)

    def make(gr, label, y, color):
        y_scale = cfg.get("y2_scale", "linear") if y in y2_cols else cfg.get("y_scale")
        y_log = y_scale in ("log", "log2")
        pts = _drop_unplottable(_drop_unplottable(gr, ci, x, lx), ci, y, y_log)
        if kind == "line":
            # Keep each series' x, y, marker, and error values aligned while
            # ensuring connected line segments run in ascending x order.
            pts = sorted(pts, key=lambda row: _line_x_sort_key(row, ci[x]))
        name = f"{label} — {y}" if (label and len(ys) > 1) else (label or y)
        xs, yv = [r[ci[x]] for r in pts], [r[ci[y]] for r in pts]
        # "_ycol" records which data column this trace's y-values came from,
        # so a secondary-axis assignment (config['y2']) can match on the
        # actual column even when the display name is just a group label
        # (single-y-column + grouping). layout.py strips this before the
        # trace is sent to the client.
        err = _error_y(cfg, ci, pts)
        band = bool(err) and cfg.get("error_style") == "band" and kind != "bar"
        if color_groups and gr:
            t_color_label = legend_fields(color_groups, [gr[0][ci[c]] for c in color_groups])
        else:
            t_color_label = None
        if dash_by and gr:
            t_dash_value = legend_fields(dash_by, [gr[0][ci[col]] for col in dash_by])
        else:
            t_dash_value = None
        marker_items = {}
        if marker_by:
            for row in pts:
                marker_key = tuple(row[ci[col]] for col in marker_by)
                marker_label = legend_fields(marker_by, marker_key)
                marker_items.setdefault(marker_label, mmap.get(marker_key, "circle"))
        color_key = tuple(gr[0][ci[c]] for c in color_groups) if color_groups and gr else y
        if kind == "bar":
            t = {"type": "bar", "name": name, "x": xs, "y": yv, "_ycol": y,
                 "marker": {"color": color, "line": {"width": 0.8, "color": "#333"}},
                 "_series_color_key": color_key}
            if t_color_label is not None: t["_legend_color_label"] = t_color_label
            if t_dash_value is not None: t["_legend_dash_value"] = t_dash_value
            if marker_items: t["_legend_marker_items"] = list(marker_items.items())
            if err: t["error_y"] = err
            return [t]
        t = {"type": "scatter", "mode": cfg.get("mode", "lines+markers") if kind == "line" else "markers",
             "name": name, "x": xs, "y": yv, "_ycol": y,
             "marker": {"size": MS, "color": color, "line": {"width": 1, "color": "#fff"}},
             "_series_color_key": color_key}
        if kind == "line":
            t["line"] = {"width": LW, "color": color}
            if dash_by and gr:
                dash_key = tuple(gr[0][ci[col]] for col in dash_by)
                t["line"]["dash"] = dmap.get(dash_key, "solid")
        if marker_by:
            t["marker"]["symbol"] = [mmap.get(tuple(r[ci[col]] for col in marker_by), "circle") for r in pts]
        if marker_items: t["_legend_marker_items"] = list(marker_items.items())
        if t_color_label is not None: t["_legend_color_label"] = t_color_label
        if t_dash_value is not None: t["_legend_dash_value"] = t_dash_value

        if kind == "line" and marker_by:
            # Each marker category is its own line series. This connects its
            # points across x values while preventing a line from joining
            # different marker categories. Color and dash categories have
            # already been separated by the outer grouping.
            marker_groups = {}
            for row in pts:
                marker_key = tuple(row[ci[col]] for col in marker_by)
                marker_groups.setdefault(marker_key, []).append(row)

            segments = []
            for marker_key, run_pts in marker_groups.items():
                segment = dict(t)
                segment["x"] = [row[ci[x]] for row in run_pts]
                segment["y"] = [row[ci[y]] for row in run_pts]
                segment["marker"] = dict(t["marker"])
                symbol = mmap.get(marker_key, "circle")
                segment["marker"]["symbol"] = symbol
                marker_label = legend_fields(marker_by, marker_key)
                segment["_legend_marker_items"] = [(marker_label, symbol)]

                run_err = _error_y(cfg, ci, run_pts)
                if band and run_err:
                    run_y = segment["y"]
                    hi = [value + delta if value is not None and delta is not None else None
                          for value, delta in zip(run_y, run_err["array"])]
                    lo = [value - delta if value is not None and delta is not None else None
                          for value, delta in zip(run_y, run_err.get("arrayminus", run_err["array"]))]
                    segment_x = segment["x"]
                    segments.extend([
                        {"type": "scatter", "x": segment_x, "y": lo, "_ycol": y,
                         "_series_color_key": color_key,
                         "mode": "lines", "line": {"width": 0}, "showlegend": False,
                         "hoverinfo": "skip", "name": name + " (band)"},
                        {"type": "scatter", "x": segment_x, "y": hi, "_ycol": y,
                         "_series_color_key": color_key,
                         "mode": "lines", "line": {"width": 0}, "fill": "tonexty",
                         "fillcolor": with_alpha(color, 0.2), "showlegend": False,
                         "hoverinfo": "skip", "name": name + " (band)"},
                    ])
                elif run_err:
                    segment["error_y"] = run_err
                segments.append(segment)
            return segments

        if not band:
            if err: t["error_y"] = err
            return [t]

        # error_style='band': draw a shaded region instead of whiskers — two
        # invisible line traces (lower, then upper with fill='tonexty') sit
        # behind the main trace, built from the same array/arrayminus as
        # error_y would use, so it's a pure presentation switch.
        high_errors = err["array"]
        low_errors = err.get("arrayminus", high_errors)
        hi = [v + d if v is not None and d is not None else None for v, d in zip(yv, high_errors)]
        lo = [v - d if v is not None and d is not None else None for v, d in zip(yv, low_errors)]
        band_lower = {"type": "scatter", "x": xs, "y": lo, "_ycol": y, "_series_color_key": color_key,
                     "mode": "lines", "line": {"width": 0},
                     "showlegend": False, "hoverinfo": "skip", "name": name + " (band)"}
        band_upper = {"type": "scatter", "x": xs, "y": hi, "_ycol": y, "_series_color_key": color_key,
                     "mode": "lines", "line": {"width": 0},
                     "fill": "tonexty", "fillcolor": with_alpha(color, 0.2),
                     "showlegend": False, "hoverinfo": "skip", "name": name + " (band)"}
        return [band_lower, band_upper, t]

    traces, n_series = [], 0  # n_series (not len(traces)) drives the color cycle, since a
    # shaded error band contributes 2 extra invisible traces per series that
    # must not shift the palette
    if split:
        buckets = {}
        for r in rows:
            buckets.setdefault(tuple(r[ci[c]] for c in split), []).append(r)
        color_keys = []
        for key in buckets:
            values = dict(zip(split, key))
            group_color_key = tuple(values[c] for c in color_groups) if color_groups else None
            for y in ys:
                color_key = group_color_key if group_color_key is not None else y
                if color_key not in color_keys:
                    color_keys.append(color_key)
        color_map = {key: _color(i) for i, key in enumerate(color_keys)}
        for key, gr in sorted(buckets.items(), key=lambda kv: [str(v) for v in kv[0]]):
            values = dict(zip(split, key))
            group_color_key = tuple(values[c] for c in color_groups) if color_groups else None
            for y in ys:
                color_key = group_color_key if group_color_key is not None else y
                color = color_map[color_key]
                traces += make(gr, legend_fields(split, key), y, color); n_series += 1
    else:
        for y in ys:
            traces += make(rows, None, y, _color(n_series)); n_series += 1
    return traces


register_plot("line", "Line Chart", "X vs Y with grouping, marker & linestyle mapping", {"mode": "lines+markers"})(lambda d, c: _xy(d, c, "line"))
register_plot("bar", "Bar Chart", "Categorical comparisons with grouping", {"barmode": "group"})(lambda d, c: _xy(d, c, "bar"))
register_plot("scatter", "Scatter Plot", "X vs Y correlation with grouping and markers", {})(lambda d, c: _xy(d, c, "scatter"))


@register_plot("histogram", "Histogram", "Distribution of a numeric column", {"nbinsx": 30})
def plot_histogram(df, cfg):
    ci = {c: i for i, c in enumerate(df["columns"])}; x = cfg.get("x")
    if not x: raise ValueError("Histogram requires an x column.")
    pts = _drop_unplottable(df["rows"], ci, x, cfg.get("x_scale") in ("log", "log2"))
    return [{"type": "histogram", "name": x, "x": [r[ci[x]] for r in pts], "nbinsx": int(cfg.get("nbinsx", 30))}]


def _dist(kind):
    def fn(df, cfg):
        ci = {c: i for i, c in enumerate(df["columns"])}; x, ys = cfg.get("x"), _ys(cfg)
        if not ys: raise ValueError(f"{kind} plot requires at least one y column.")
        out = []
        for y in ys:
            pts = _drop_unplottable(df["rows"], ci, y, cfg.get("y_scale") in ("log", "log2"))
            t = {"type": kind, "name": y, "y": [r[ci[y]] for r in pts],
                 "line": {"width": 1.5, "color": _color(len(out))}}
            if kind == "violin": t.update(box={"visible": True}, meanline={"visible": True})
            if x and x in ci: t["x"] = [r[ci[x]] for r in pts]
            out.append(t)
        return out
    return fn


register_plot("box", "Box Plot", "Distribution summary per category", {})(_dist("box"))
register_plot("violin", "Violin Plot", "Distribution shape per category", {})(_dist("violin"))


@register_plot("heatmap", "Heatmap", "2D matrix view", {})
def plot_heatmap(df, cfg):
    ci = {c: i for i, c in enumerate(df["columns"])}; rows = df["rows"]
    x, y, z = cfg.get("x"), (_ys(cfg) or [None])[0], cfg.get("z")
    if not (x and y and z): raise ValueError("Heatmap requires x, y, and z columns.")
    xs = sorted({r[ci[x]] for r in rows}); ys = sorted({r[ci[y]] for r in rows})
    xi, yi = {v: i for i, v in enumerate(xs)}, {v: i for i, v in enumerate(ys)}
    mat = [[None] * len(xs) for _ in ys]
    for r in rows:
        value = r[ci[z]]
        if value is None or (isinstance(value, str) and value.strip().lower() in ("", "na", "n/a", "nan", "null")):
            value = None
        else:
            try:
                value = float(value)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"Heatmap z column '{z}' must contain numbers or missing values; "
                    f"found {value!r} at x={r[ci[x]]!r}, y={r[ci[y]]!r}."
                ) from exc
            if math.isnan(value):
                value = None
        mat[yi[r[ci[y]]]][xi[r[ci[x]]]] = value
    trace = {"type": "heatmap", "x": xs, "y": ys, "z": mat, "colorscale": "Viridis"}
    z_label = str(cfg.get("z_label") or "").strip()
    if z_label:
        trace["colorbar"] = {"title": {"text": z_label}}
    return [trace]


@register_plot("pie", "Pie / Donut", "Proportional breakdown", {"hole": 0})
def plot_pie(df, cfg):
    ci = {c: i for i, c in enumerate(df["columns"])}; lc, vc = cfg.get("x"), (_ys(cfg) or [None])[0]
    if not lc or not vc: raise ValueError("Pie chart requires x (labels) and y (values).")
    return [{"type": "pie", "labels": [r[ci[lc]] for r in df["rows"]],
             "values": [r[ci[vc]] for r in df["rows"]], "hole": float(cfg.get("hole", 0))}]


def load_plugins(dirs):
    """Plugin API: PLOT_NAME, PLOT_LABEL, PLOT_DESCRIPTION, PLOT_DEFAULTS, plot(df_data, config)."""
    for d in dirs:
        p = Path(d)
        if not p.is_dir():
            log(f"Plugin dir not found: {d}", "warn"); continue
        for f in sorted(p.glob("*.py")):
            try:
                spec = importlib.util.spec_from_file_location(f.stem, f)
                mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
                if hasattr(mod, "PLOT_NAME") and hasattr(mod, "plot"):
                    PLUGIN_PLOTS[mod.PLOT_NAME] = dict(
                        name=mod.PLOT_NAME, label=getattr(mod, "PLOT_LABEL", mod.PLOT_NAME),
                        description=getattr(mod, "PLOT_DESCRIPTION", ""), defaults=getattr(mod, "PLOT_DEFAULTS", {}),
                        fn=mod.plot, source=str(f))
                    log(f"Plugin loaded: {mod.PLOT_NAME} ({f.name})")
            except Exception as e:
                log(f"Failed to load plugin {f}: {e}", "warn")


def plot_type_catalog() -> dict:
    keys = ("name", "label", "description", "defaults")
    out = {k: {x: v[x] for x in keys} for k, v in BUILTIN_PLOTS.items()}
    for k, v in PLUGIN_PLOTS.items():
        out[k] = {x: v[x] for x in keys}; out[k].update(is_plugin=True, source=v.get("source", ""))
    return out
