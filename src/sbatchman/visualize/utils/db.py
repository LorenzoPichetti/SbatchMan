"""SQLite registry, schema introspection and (read-only) querying."""
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd
from .core import log

DB_REGISTRY: dict = {}  # logical name -> absolute path


def _q(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def _connect(name: str, readonly=True):
    path = DB_REGISTRY.get(name)
    if not path:
        raise ValueError(f"Unknown database: {name}")
    if readonly:  # user SQL can never modify the data
        return sqlite3.connect(Path(path).as_uri() + "?mode=ro", uri=True)
    return sqlite3.connect(path)


def load_databases(paths):
    for path in paths:
        p = Path(path).resolve()
        if not p.exists():
            log(f"Database not found: {path}", "warn"); continue
        name, n = p.stem, 1
        while name in DB_REGISTRY:
            name = f"{p.stem}_{n}"; n += 1
        DB_REGISTRY[name] = str(p)
        log(f"Registered database {name} -> {p}")


def is_single_db() -> bool:
    return len(DB_REGISTRY) == 1


def only_db_name() -> Optional[str]:
    return next(iter(DB_REGISTRY)) if is_single_db() else None


def get_all_table_names(db: str) -> List[str]:
    with _connect(db) as c:
        return [r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]


def get_db_schema(db: str):
    if db not in DB_REGISTRY:
        return None
    conn = _connect(db)
    try:
        return {t: [{"name": r[1], "type": r[2]} for r in conn.execute(f"PRAGMA table_info({_q(t)})")]
                for t in get_all_table_names(db)}
    finally:
        conn.close()


def all_schemas() -> dict:
    return {n: get_db_schema(n) for n in DB_REGISTRY}


def table_counts(db: str) -> dict:
    with _connect(db) as c:
        return {t: c.execute(f"SELECT COUNT(*) FROM {_q(t)}").fetchone()[0] for t in get_all_table_names(db)}


def get_all_tables_as_dataframes(db: str) -> Dict[str, pd.DataFrame]:
    conn = _connect(db)
    try:
        return {t: pd.read_sql_query(f"SELECT * FROM {_q(t)}", conn) for t in get_all_table_names(db)}
    finally:
        conn.close()


def run_query(db, sql, limit=10000):
    if not sql.strip().upper().startswith(("SELECT", "WITH")):
        raise ValueError("Only SELECT / WITH queries are allowed.")
    conn = _connect(db)
    try:
        cur = conn.execute(sql)
        rows = [list(r) for r in cur.fetchmany(limit)]
        return {"columns": [d[0] for d in cur.description], "rows": rows, "truncated": len(rows) == limit}
    finally:
        conn.close()


def df_data_to_dataframe(d: dict) -> pd.DataFrame:
    return pd.DataFrame(d["rows"], columns=d["columns"])


def dataframe_to_df_data(df: pd.DataFrame, limit=10000) -> dict:
    truncated = len(df) > limit
    df = df.head(limit) if truncated else df
    safe = df.astype(object).where(pd.notnull(df), None)
    return {"columns": [str(c) for c in df.columns], "rows": safe.values.tolist(), "truncated": truncated}
