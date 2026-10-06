"""
Benchmark-specific aggregation helpers. These are exposed inside transform
scripts as `agg` (see core.py's exec namespace), so a transform script can
do e.g.:

    df = agg.summarize(data['result'], group=['algorithm', 'nodes'], value='time')
    data['result'] = df

which collapses repeated runs into one row per group with mean/median and an
error column, ready to feed straight into a line/bar/scatter plot's
`error_y` config field.
"""
import pandas as pd

AGG_FUNCS = {"mean": "mean", "median": "median", "min": "min", "max": "max"}
ERR_FUNCS = {"std": lambda s: s.std(ddof=1) if len(s) > 1 else 0.0,
             "sem": lambda s: s.std(ddof=1) / max(len(s), 1) ** 0.5 if len(s) > 1 else 0.0,
             "minmax": None,  # handled separately: produces err_low/err_high instead of a symmetric err
             "none": lambda s: 0.0}


def summarize(df: pd.DataFrame, group, value: str, center: str = "mean", err: str = "std",
             n_col: str = "n_reps") -> pd.DataFrame:
    """Collapse repeated-measurement rows into one row per `group`, with a
    `value` column (the center statistic) and an `{value}_err` column (or
    `{value}_err_low` / `{value}_err_high` for err='minmax'). Non-numeric,
    non-group columns are kept via 'first' so labels survive the groupby.

    group: str or list of column names to group by.
    center: 'mean' | 'median' | 'min' | 'max'.
    err: 'std' | 'sem' | 'minmax' | 'none'.
    n_col: name for the added repetition-count column.
    """
    if isinstance(group, str):
        group = [group]
    if center not in AGG_FUNCS:
        raise ValueError(f"Unknown center statistic '{center}'; choose one of {list(AGG_FUNCS)}")
    if err not in ERR_FUNCS:
        raise ValueError(f"Unknown error statistic '{err}'; choose one of {list(ERR_FUNCS)}")

    other_cols = [c for c in df.columns if c not in group and c != value]
    gb = df.groupby(group, dropna=False)
    out = gb[value].agg(AGG_FUNCS[center]).reset_index()
    out[n_col] = gb[value].size().values

    if err == "minmax":
        out[f"{value}_err_low"] = (out[value].values - gb[value].min().values)
        out[f"{value}_err_high"] = (gb[value].max().values - out[value].values)
    elif err != "none":
        out[f"{value}_err"] = gb[value].apply(ERR_FUNCS[err]).values
    else:
        out[f"{value}_err"] = 0.0

    if other_cols:
        firsts = gb[other_cols].first().reset_index()
        out = out.merge(firsts, on=group, how="left")
    return out


def speedup(df: pd.DataFrame, group, value: str, baseline_col: str, baseline_value,
           out_col: str = "speedup") -> pd.DataFrame:
    """Adds `out_col` = (baseline's `value` within each group) / `value`, where
    the baseline row is the one whose `baseline_col` equals `baseline_value`
    (e.g. baseline_col='threads', baseline_value=1 for a strong-scaling plot).
    `group` should be the columns that identify "the same experiment run at
    different settings of baseline_col" — everything except baseline_col itself.
    """
    if isinstance(group, str):
        group = [group]
    base = df[df[baseline_col] == baseline_value][group + [value]].rename(columns={value: "_base"})
    if base.empty:
        raise ValueError(f"No rows with {baseline_col} == {baseline_value!r} to use as baseline.")
    merged = df.merge(base, on=group, how="left")
    merged[out_col] = merged["_base"] / merged[value]
    return merged.drop(columns="_base")


def efficiency(df: pd.DataFrame, speedup_col: str, workers_col: str, out_col: str = "efficiency") -> pd.DataFrame:
    """Adds `out_col` = speedup / workers (parallel efficiency, 1.0 = ideal)."""
    df = df.copy()
    df[out_col] = df[speedup_col] / df[workers_col]
    return df
