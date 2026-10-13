"""
NammaMart: DuckDB warehouse helpers.

Both pipelines write to the same file, include/warehouse_bhavani.duckdb, into
separate table namespaces (etl_* and raw_/stg_/mart_). Sharing one file is what
lets Part B4 compare ETL and ELT totals in a single query.

Idempotency (acceptance criterion 5) is handled by upsert_by_key: delete the
incoming keys, then insert. Re-running with the same data leaves row counts
unchanged. Running the increment file updates the 10 corrected orders in place
and appends the 150 new ones, which is criterion 6 using the same mechanism.
"""

from __future__ import annotations

import os
from contextlib import contextmanager

import duckdb
import pandas as pd

DEFAULT_DB_PATH = "/usr/local/airflow/include/warehouse_bhavani.duckdb"


def db_path() -> str:
    """Overridable so the same code runs in Airflow, in tests, and on a laptop."""
    return os.environ.get("NAMMAMART_DB", DEFAULT_DB_PATH)


@contextmanager
def connect(path: str | None = None):
    """
    DuckDB takes an exclusive lock on the file, so the connection is always closed,
    even on failure. Leaving one open is the usual cause of a task failing with a
    lock error on the next run.
    """
    conn = duckdb.connect(path or db_path())
    try:
        yield conn
    finally:
        conn.close()


def replace_table(conn, table: str, df: pd.DataFrame) -> int:
    """Full replace. Used for the small dimension tables, which are masters."""
    conn.register("_incoming", df)
    conn.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM _incoming")
    conn.unregister("_incoming")
    return len(df)


def upsert_by_key(conn, table: str, df: pd.DataFrame, key: str) -> dict:
    """
    Delete every incoming key, then insert. Returns counts so the run audit and the
    Part F experiments can report inserted versus updated without guessing.
    """
    if df.empty:
        return {"incoming": 0, "updated": 0, "inserted": 0, "final_rows": table_count(conn, table)}

    conn.register("_incoming", df)

    exists = conn.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = ?", [table]
    ).fetchone()[0]

    if not exists:
        conn.execute(f"CREATE TABLE {table} AS SELECT * FROM _incoming")
        conn.unregister("_incoming")
        return {"incoming": len(df), "updated": 0, "inserted": len(df), "final_rows": len(df)}

    updated = conn.execute(
        f"SELECT count(*) FROM {table} WHERE {key} IN (SELECT {key} FROM _incoming)"
    ).fetchone()[0]

    conn.execute(f"DELETE FROM {table} WHERE {key} IN (SELECT {key} FROM _incoming)")
    conn.execute(f"INSERT INTO {table} SELECT * FROM _incoming")
    conn.unregister("_incoming")

    return {
        "incoming": len(df),
        "updated": int(updated),
        "inserted": int(len(df) - updated),
        "final_rows": table_count(conn, table),
    }


def append_table(conn, table: str, df: pd.DataFrame) -> int:
    """Append only. Used for dq_results and pipeline_runs, which are run logs."""
    if df.empty:
        return 0
    conn.register("_incoming", df)
    exists = conn.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = ?", [table]
    ).fetchone()[0]
    if exists:
        conn.execute(f"INSERT INTO {table} SELECT * FROM _incoming")
    else:
        conn.execute(f"CREATE TABLE {table} AS SELECT * FROM _incoming")
    conn.unregister("_incoming")
    return len(df)


def replace_run_slice(conn, table: str, df: pd.DataFrame, run_id: str) -> int:
    """
    For derived reporting tables that are rebuilt whole each run. Dropping and
    recreating keeps them consistent with the fact table they come from.
    """
    return replace_table(conn, table, df)


def table_count(conn, table: str) -> int:
    exists = conn.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = ?", [table]
    ).fetchone()[0]
    if not exists:
        return 0
    return conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
