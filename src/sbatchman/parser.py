import importlib.util
import sqlite3
import pandas as pd
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from sbatchman.core.jobs_manager import jobs_list


ParseResult = Optional[Dict[str, Union[Dict[str, Any], List[Dict[str, Any]]]]]


def _load_parser_module(parser: Path):
    """Dynamically import the user-supplied parser script as a module."""
    spec = importlib.util.spec_from_file_location(parser.stem, parser)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load parser module from {parser}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _normalize_rows(value: Union[Dict[str, Any], List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """Normalize a table's parse() output into a list of row dicts."""
    if isinstance(value, dict):
        return [value]
    if isinstance(value, list):
        return list(value)
    raise TypeError(
        f"Expected a dict (single row) or list of dicts (multiple rows), got {type(value)!r}"
    )


def parse_jobs_and_generate_sqlite_db(parser: Path, output_path: Path) -> None:
    """
    Run the user-defined parse(job) function (found in `parser`) over every job
    returned by jobs_list(), and write the accumulated results to a SQLite
    database at `output_path`.

    Parser API contract
    --------------------
    The parser script must define:

        def parse(job: sbm.Job) -> dict | None:
            ...

    `parse` should return either:
      - None / {} if the job produced no rows, or
      - a dict mapping table_name -> row(s), where each value is either
          - a single row: a dict of {column_name: value}, or
          - multiple rows: a list of such dicts.

    This lets the user:
      - choose table names freely (dict keys)
      - emit any number of rows per job (list values)
      - emit rows into multiple tables from one job (multiple dict keys)

    Example
    -------
        def parse(job: sbm.Job) -> dict:
            return {
                "jobs": {"id": job.id, "status": job.status},
                "job_tags": [{"job_id": job.id, "tag": t} for t in job.tags],
            }
    """
    parser = Path(parser)
    output_path = Path(output_path)

    parser_module = _load_parser_module(parser)
    if not hasattr(parser_module, "parse"):
        raise AttributeError(f"{parser} must define a `parse(job)` function")
    user_parse = parser_module.parse

    # Accumulate rows per table across all jobs before touching the DB.
    tables: Dict[str, List[Dict[str, Any]]] = {}

    for job in jobs_list():
        try:
            result: ParseResult = user_parse(job)
        except Exception as exc:  # noqa: BLE001
            print(f"[parse_jobs_and_generate_sqlite_db] parse() failed for job {job!r}: {exc}")
            continue

        if not result:
            continue

        if not isinstance(result, dict):
            raise TypeError(
                f"parse() must return a dict of {{table_name: rows}} or None, got {type(result)!r}"
            )

        for table_name, rows in result.items():
            tables.setdefault(table_name, []).extend(_normalize_rows(rows))

    # Build a DataFrame per table, then write everything to SQLite.
    output_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(output_path)
    try:
        for table_name, rows in tables.items():
            if not rows:
                continue
            df = pd.DataFrame(rows)
            df.to_sql(table_name, conn, index=False, if_exists="replace")
        conn.commit()
    finally:
        conn.close()


def print_sqlite_db(db_path: Path, verbose: bool = False, sample_rows: int = 5) -> None:
    """
    Print the contents of a SQLite database to the terminal.

    Parameters
    ----------
    db_path : Path
        Path to the SQLite database file.
    verbose : bool
        If False (default), print each table's schema plus a small sample
        of rows (`sample_rows`). If True, print the full contents of every
        table.
    sample_rows : int
        Number of example rows to show per table in non-verbose mode.
        Ignored when verbose=True.
    """
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"No such database file: {db_path}")

    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        table_names = [row[0] for row in cursor.fetchall()]

        if not table_names:
            print(f"No tables found in {db_path}")
            return

        for table_name in table_names:
            cursor.execute(f"SELECT COUNT(*) FROM '{table_name}'")
            row_count = cursor.fetchone()[0]

            print("=" * 80)
            print(f"TABLE: {table_name}  ({row_count} rows)")
            print("=" * 80)

            # Schema (column names + declared types)
            cursor.execute(f"PRAGMA table_info('{table_name}')")
            columns_info = cursor.fetchall()
            col_names = [c[1] for c in columns_info]

            print("Columns:")
            for _, col_name, col_type, *_ in columns_info:
                col_type = col_type or "ANY"
                print(f"  - {col_name} ({col_type})")
            print()

            # Data, either full or sample, via pandas for clean tabular printing
            limit_clause = "" if verbose else f" LIMIT {sample_rows}"
            query = f"SELECT * FROM '{table_name}'{limit_clause}"
            df = pd.read_sql_query(query, conn)

            if df.empty:
                print("(no rows)")
            else:
                if not verbose and row_count > sample_rows:
                    print(f"Showing first {sample_rows} of {row_count} rows:")
                print(df.to_string(index=False))
            print()
    finally:
        conn.close()