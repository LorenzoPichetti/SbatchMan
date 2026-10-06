"""Built-in plot types + plugin loader. A plot fn is fn(df_data, config) -> [plotly trace dicts]."""
import importlib.util
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


def _style_map(rows, ci, col, seq):
    if not col or col not in ci:
        return {}
    vals = sorted({r[ci[col]] for r in rows}, key=str)
    return {v: seq[i % len(seq)] for i, v in enumerate(vals)}


def _drop_unplottable(rows, ci, col, is_log):
    """Log axes cannot place None/<=0 values."""
    if not is_log or col not in ci:
        return rows
    return [r for r in rows if r[ci[col]] is not None and r[ci[col]] > 0]


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
    marker_by = (cfg.get("marker_by") or None) if kind != "bar" else None
    dash_by = (cfg.get("dash_by") or None) if kind == "line" else None
    mmap, dmap = _style_map(rows, ci, marker_by, MARKERS), _style_map(rows, ci, dash_by, DASHES)
    split = groups + ([dash_by] if dash_by and dash_by not in groups else [])
    lx = cfg.get("x_scale") in ("log", "log2")
    y2_cols = set(_cols(cfg.get("y2")))

    def make(gr, label, y, color):
        y_scale = cfg.get("y2_scale", "linear") if y in y2_cols else cfg.get("y_scale")
        y_log = y_scale in ("log", "log2")
        pts = _drop_unplottable(_drop_unplottable(gr, ci, x, lx), ci, y, y_log)
        name = f"{label} — {y}" if (label and len(ys) > 1) else (label or y)
        xs, yv = [r[ci[x]] for r in pts], [r[ci[y]] for r in pts]
        # "_ycol" records which data column this trace's y-values came from,
        # so a secondary-axis assignment (config['y2']) can match on the
        # actual column even when the display name is just a group label
        # (single-y-column + grouping). layout.py strips this before the
        # trace is sent to the client.
        err = _error_y(cfg, ci, pts)
        band = bool(err) and cfg.get("error_style") == "band" and kind != "bar"
        if kind == "bar":
            t = {"type": "bar", "name": name, "x": xs, "y": yv, "_ycol": y,
                 "marker": {"color": color, "line": {"width": 0.8, "color": "#333"}}}
            if err: t["error_y"] = err
            return [t]
        t = {"type": "scatter", "mode": cfg.get("mode", "lines+markers") if kind == "line" else "markers",
             "name": name, "x": xs, "y": yv, "_ycol": y,
             "marker": {"size": MS, "color": color, "line": {"width": 1, "color": "#fff"}}}
        if kind == "line":
            t["line"] = {"width": LW, "color": color}
            if dash_by and gr:
                t["line"]["dash"] = dmap.get(gr[0][ci[dash_by]], "solid")
        if marker_by:
            t["marker"]["symbol"] = [mmap.get(r[ci[marker_by]], "circle") for r in pts]

        if not band:
            if err: t["error_y"] = err
            return [t]

        # error_style='band': draw a shaded region instead of whiskers — two
        # invisible line traces (lower, then upper with fill='tonexty') sit
        # behind the main trace, built from the same array/arrayminus as
        # error_y would use, so it's a pure presentation switch.
        hi = [v + d for v, d in zip(yv, err["array"])]
        lo = [v - d for v, d in zip(yv, err.get("arrayminus", err["array"]))]
        band_lower = {"type": "scatter", "x": xs, "y": lo, "mode": "lines", "line": {"width": 0},
                     "showlegend": False, "hoverinfo": "skip", "name": name + " (band)"}
        band_upper = {"type": "scatter", "x": xs, "y": hi, "mode": "lines", "line": {"width": 0},
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
        for key, gr in sorted(buckets.items(), key=lambda kv: [str(v) for v in kv[0]]):
            for y in ys:
                traces += make(gr, " | ".join(map(str, key)), y, _color(n_series)); n_series += 1
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
    for r in rows: mat[yi[r[ci[y]]]][xi[r[ci[x]]]] = r[ci[z]]
    return [{"type": "heatmap", "x": xs, "y": ys, "z": mat, "colorscale": "Viridis"}]


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
