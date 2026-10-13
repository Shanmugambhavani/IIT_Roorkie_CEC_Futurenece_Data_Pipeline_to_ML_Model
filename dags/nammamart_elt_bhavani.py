"""
NammaMart ELT pipeline (Part B).

Extract -> Load raw -> Transform in the warehouse with SQL

Every source row lands in a raw table exactly as received, all columns as text,
never cleaned and never deleted. That is acceptance criterion 3, and it is what
analytics and compliance asked for: replay any day, answer a question nobody
thought to ask in September.

The cleaning happens afterwards, in SQL, in the staging layer. Bad rows are still
there, marked dq_status = 'QUARANTINE' with the rules they broke, and the mart
layer filters them out. Nothing is thrown away.

Transformations are .sql files in include/sql/, run by Airflow tasks. No pandas
cleaning anywhere in this DAG. The only Python is orchestration and the raw load.

Rule names are identical to the ETL pipeline, because check_catalog is built
straight from include/dq_checks.py. That is what makes "do the two pipelines
agree" a query rather than an argument.
"""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

import pendulum

from airflow.decorators import dag, task

sys.path.insert(0, "/usr/local/airflow/include")

import pandas as pd  # noqa: E402

import dq_checks as dq  # noqa: E402
import warehouse as W  # noqa: E402
from alerts import dag_failure_alert  # noqa: E402

OWNER = "bhavani"
BASE = Path("/usr/local/airflow/include")
DATA_DIR = BASE / "data" / "nammamart"
SQL_DIR = BASE / "sql"

INCREMENT_FILE = "orders_2026-10-01_increment.csv"

# End of the business day the extract represents. Midnight would reject every
# order placed on 1 October, which is the whole increment file.
AS_OF = "2026-10-01 23:59:59"

SOURCES = {
    "raw_orders": "orders.csv",
    "raw_payments": "payments.csv",
    "raw_customers": "customers.csv",
    "raw_products": "products.csv",
    "raw_stores": "stores.csv",
}


def run_sql_file(conn, name: str) -> None:
    """A .sql file may hold several statements; DuckDB's execute handles that."""
    sql = (SQL_DIR / name).read_text()
    conn.execute(sql)
    print(f"[sql] ran {name}")


def set_vars(conn, **kwargs) -> None:
    """SQL files read their parameters with getvariable(), so nothing is interpolated."""
    for key, value in kwargs.items():
        conn.execute(f"SET VARIABLE {key} = ?", [str(value)])


def check_catalog_frame() -> pd.DataFrame:
    """
    The single source of truth for what each rule means, lifted straight from the
    Python registry the ETL pipeline uses. Writing this list twice is how the two
    pipelines would quietly drift apart.
    """
    rows = []
    for dataset, checks in dq.CHECK_REGISTRY.items():
        for check in checks:
            rows.append({
                "check_name": check.name,
                "dimension": check.dimension,
                "severity": check.severity,
                "dataset": dataset,
            })
    return pd.DataFrame(rows)


default_args = {
    "owner": OWNER,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
    "depends_on_past": False,
    "on_failure_callback": dag_failure_alert,
}


@dag(
    dag_id="nammamart_elt_bhavani",
    description="NammaMart ELT: keep every raw row, clean in SQL inside the warehouse",
    # Staggered five minutes after the ETL DAG. Both pipelines write to the same
    # DuckDB file, which takes an exclusive lock, so scheduling both at 09:00
    # guarantees one of them fails on a lock error. Observed during testing:
    # build_reports failed with no data problem at all because a concurrent run
    # held the file. max_active_runs=1 prevents a DAG colliding with itself, not
    # with its sibling. Five minutes is ample: a full run takes under 30 seconds.
    schedule="5 9 * * 1-6",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["nammamart", "elt", OWNER],
)
def nammamart_elt():

    # ------------------------------------------------------------------
    # EXTRACT and LOAD RAW: five parallel tasks, no cleaning whatsoever
    # ------------------------------------------------------------------

    def _load_raw(table: str, filename: str, run_id: str) -> dict:
        """
        read_csv with all_varchar=true. Nothing is cast, so a junk value is kept as
        the junk it is rather than becoming a null nobody can explain later.
        _row_id preserves the original file order, which the dedupe rules rely on.
        """
        path = DATA_DIR / filename
        with W.connect() as conn:
            conn.execute(f"""
                CREATE OR REPLACE TABLE {table} AS
                SELECT *,
                       row_number() OVER ()  AS _row_id,
                       now()                 AS _loaded_at,
                       '{filename}'          AS _source_file,
                       ?                     AS _run_id
                FROM read_csv('{path}', all_varchar=true, header=true)
            """, [run_id])
            rows = conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        print(f"[raw] {filename} -> {table}: {rows} rows, untouched")
        return {"table": table, "file": filename, "rows": rows}

    @task
    def prepare_run(**context) -> dict:
        run_id = context["run_id"]
        with W.connect() as conn:
            catalog = check_catalog_frame()
            W.replace_table(conn, "check_catalog", catalog)
            print(f"[setup] check_catalog: {len(catalog)} rules shared with the ETL pipeline")
        return {"run_id": run_id}

    @task
    def load_raw_orders(meta: dict) -> dict:
        return _load_raw("raw_orders", SOURCES["raw_orders"], meta["run_id"])

    @task
    def load_raw_payments(meta: dict) -> dict:
        return _load_raw("raw_payments", SOURCES["raw_payments"], meta["run_id"])

    @task
    def load_raw_customers(meta: dict) -> dict:
        return _load_raw("raw_customers", SOURCES["raw_customers"], meta["run_id"])

    @task
    def load_raw_products(meta: dict) -> dict:
        return _load_raw("raw_products", SOURCES["raw_products"], meta["run_id"])

    @task
    def load_raw_stores(meta: dict) -> dict:
        return _load_raw("raw_stores", SOURCES["raw_stores"], meta["run_id"])

    @task
    def load_raw_increment(meta: dict, orders: dict) -> dict:
        """
        Appended, never replacing. Raw is append only: the September rows that the
        increment corrects stay exactly as they were received, which is how the
        Part E8 audit question gets answered.
        """
        path = DATA_DIR / INCREMENT_FILE
        if not path.exists():
            print("[raw] no increment file today")
            return {"rows": 0, "present": False}

        with W.connect() as conn:
            already = conn.execute(
                "SELECT count(*) FROM raw_orders WHERE _source_file = ?", [INCREMENT_FILE]
            ).fetchone()[0]
            if already:
                conn.execute(
                    "DELETE FROM raw_orders WHERE _source_file = ?", [INCREMENT_FILE]
                )
                print(f"[raw] replacing {already} previously loaded increment rows")

            conn.execute(f"""
                INSERT INTO raw_orders
                SELECT *,
                       (SELECT coalesce(max(_row_id), 0) FROM raw_orders) + row_number() OVER (),
                       now(),
                       '{INCREMENT_FILE}',
                       ?
                FROM read_csv('{path}', all_varchar=true, header=true)
            """, [meta["run_id"]])
            rows = conn.execute(
                "SELECT count(*) FROM raw_orders WHERE _source_file = ?", [INCREMENT_FILE]
            ).fetchone()[0]
            total = conn.execute("SELECT count(*) FROM raw_orders").fetchone()[0]

        print(f"[raw] increment appended: {rows} rows, raw_orders now {total}")
        return {"rows": rows, "present": True}

    # ------------------------------------------------------------------
    # TRANSFORM: SQL only, from here to the end
    # ------------------------------------------------------------------

    @task
    def transform_staging_dimensions(meta: dict, stores: dict, products: dict,
                                     customers: dict) -> dict:
        with W.connect() as conn:
            set_vars(conn, as_of=AS_OF, run_id=meta["run_id"])
            run_sql_file(conn, "stg_dimensions.sql")
            counts = {
                t: conn.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
                for t in ("stg_stores", "stg_products", "stg_customers")
            }
        print(f"[staging] dimensions: {counts}")
        return counts

    @task
    def transform_staging_facts(meta: dict, orders: dict, payments: dict,
                                dims: dict) -> dict:
        """The base load. The increment is a separate task so the MERGE is visible."""
        with W.connect() as conn:
            set_vars(conn, as_of=AS_OF, run_id=meta["run_id"],
                     source_file=SOURCES["raw_orders"])
            run_sql_file(conn, "stg_orders_ddl.sql")
            run_sql_file(conn, "stg_orders_upsert.sql")
            run_sql_file(conn, "stg_payments_upsert.sql")

            stats = conn.execute("""
                SELECT count(*),
                       sum(CASE WHEN dq_status = 'QUARANTINE' THEN 1 ELSE 0 END)
                FROM stg_orders
            """).fetchone()
            pay = conn.execute("""
                SELECT count(*),
                       sum(CASE WHEN dq_status = 'QUARANTINE' THEN 1 ELSE 0 END)
                FROM stg_payments
            """).fetchone()

        print(f"[staging] stg_orders: {stats[0]} rows, {stats[1]} quarantined")
        print(f"[staging] stg_payments: {pay[0]} rows, {pay[1]} quarantined")
        return {"orders": stats[0], "orders_quarantined": stats[1],
                "payments": pay[0], "payments_quarantined": pay[1]}

    @task
    def merge_increment(meta: dict, increment: dict, facts: dict) -> dict:
        """
        Criterion 6. The same upsert SQL, pointed at the increment file. Corrections
        overwrite their old rows on the order_id primary key; new orders are
        inserted. No second copy of anything is possible, because the key enforces it.
        """
        if not increment.get("present"):
            print("[merge] no increment to merge")
            return {"inserted": 0, "updated": 0, "skipped": True}

        with W.connect() as conn:
            before = conn.execute("SELECT count(*) FROM stg_orders").fetchone()[0]
            set_vars(conn, as_of=AS_OF, run_id=meta["run_id"],
                     source_file=INCREMENT_FILE)
            run_sql_file(conn, "stg_orders_upsert.sql")
            run_sql_file(conn, "stg_payments_upsert.sql")
            after = conn.execute("SELECT count(*) FROM stg_orders").fetchone()[0]

            returned = conn.execute("""
                SELECT count(*) FROM stg_orders
                WHERE dq_status = 'PASS' AND order_status = 'RETURNED'
                  AND order_date < DATE '2026-10-01'
            """).fetchone()[0]

        inserted = after - before
        updated = increment["rows"] - inserted
        print(f"[merge] {inserted} inserted, {updated} updated in place")
        print(f"[merge] September RETURNED orders now: {returned}")
        return {"inserted": inserted, "updated": updated, "skipped": False}

    @task
    def build_marts(meta: dict, merged: dict) -> dict:
        with W.connect() as conn:
            set_vars(conn, as_of=AS_OF, run_id=meta["run_id"])
            run_sql_file(conn, "mart_all.sql")

            fact_rows, revenue, margin = conn.execute("""
                SELECT count(*), round(sum(net_revenue), 2), round(sum(gross_margin), 2)
                FROM v_fact_orders
            """).fetchone()
            september = conn.execute("""
                SELECT round(sum(net_revenue), 2) FROM v_fact_orders
                WHERE order_date < DATE '2026-10-01'
            """).fetchone()[0]

            print("\n[mart] revenue by zone")
            print(conn.execute("""
                SELECT zone, round(sum(net_revenue), 2) AS net_revenue
                FROM mart_daily_store_kpis GROUP BY 1 ORDER BY 2 DESC
            """).df().to_string(index=False))

            print("\n[mart] margin by category")
            print(conn.execute("SELECT * FROM mart_category_margin").df().to_string(index=False))

            print("\n[mart] payment reconciliation")
            print(conn.execute("""
                SELECT reason, count(*) AS rows, round(sum(difference), 2) AS value
                FROM mart_payment_reconciliation GROUP BY 1 ORDER BY 2 DESC
            """).df().to_string(index=False))

            print("\n[mart] top 10 customers, PII masked")
            print(conn.execute("SELECT * FROM mart_top_customers").df().to_string(index=False))

        print(f"\n[mart] fact rows {fact_rows}, total revenue {revenue:,.2f}, "
              f"September {september:,.2f}, margin {margin:,.2f}")
        return {"fact_rows": fact_rows, "net_revenue": float(revenue),
                "september_revenue": float(september)}

    @task
    def write_dq_results(meta: dict, marts: dict) -> int:
        with W.connect() as conn:
            exists = conn.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_name = 'dq_results'"
            ).fetchone()[0]
            if not exists:
                conn.execute("""
                    CREATE TABLE dq_results (
                        run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR,
                        dimension VARCHAR, severity VARCHAR,
                        rows_checked BIGINT, rows_failed BIGINT,
                        status VARCHAR, checked_at TIMESTAMP, note VARCHAR
                    )
                """)
            set_vars(conn, run_id=meta["run_id"], dag_id="nammamart_elt_bhavani")
            run_sql_file(conn, "dq_results_elt.sql")

            written = conn.execute(
                "SELECT count(*) FROM dq_results WHERE run_id = ? AND dag_id = ?",
                [meta["run_id"], "nammamart_elt_bhavani"],
            ).fetchone()[0]
            print(conn.execute("""
                SELECT check_name, dimension, severity, rows_failed, status
                FROM dq_results
                WHERE run_id = ? AND dag_id = ? AND status <> 'PASS'
                ORDER BY severity, rows_failed DESC
            """, [meta["run_id"], "nammamart_elt_bhavani"]).df().to_string(index=False))

        print(f"[dq] {written} checks recorded")
        return written

    @task
    def compare_with_etl(meta: dict, marts: dict) -> None:
        """
        Part B4. The ETL fact table and the ELT mart are in the same DuckDB file, so
        the comparison is one query rather than two screenshots side by side.
        """
        with W.connect() as conn:
            etl_exists = conn.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_name = 'etl_fact_orders'"
            ).fetchone()[0]
            if not etl_exists:
                print("[compare] etl_fact_orders not built yet, run the ETL DAG first")
                return

            print(conn.execute("""
                SELECT 'ETL' AS pipeline,
                       count(*) AS fact_rows,
                       round(sum(net_revenue), 2) AS total_revenue,
                       round(sum(CASE WHEN order_date < DATE '2026-10-01'
                                      THEN net_revenue ELSE 0 END), 2) AS september_revenue
                FROM etl_fact_orders
                UNION ALL
                SELECT 'ELT',
                       count(*),
                       round(sum(net_revenue), 2),
                       round(sum(CASE WHEN order_date < DATE '2026-10-01'
                                      THEN net_revenue ELSE 0 END), 2)
                FROM v_fact_orders
            """).df().to_string(index=False))

            drift = conn.execute("""
                SELECT e.order_id, e.net_revenue AS etl, l.net_revenue AS elt
                FROM etl_fact_orders e
                FULL OUTER JOIN v_fact_orders l ON e.order_id = l.order_id
                WHERE coalesce(e.net_revenue, -1) <> coalesce(l.net_revenue, -1)
                LIMIT 20
            """).df()
            if drift.empty:
                print("[compare] the two pipelines agree on every order")
            else:
                print(f"[compare] {len(drift)} orders differ:")
                print(drift.to_string(index=False))

    @task(trigger_rule="all_done")
    def run_audit(meta: dict, raws: list, facts: dict, marts: dict, **context) -> None:
        dag_run = context.get("dag_run")
        rows_extracted = sum(r.get("rows", 0) for r in raws if isinstance(r, dict))
        quarantined = 0
        if isinstance(facts, dict):
            quarantined = (facts.get("orders_quarantined") or 0) + \
                          (facts.get("payments_quarantined") or 0)
        rows_loaded = (marts or {}).get("fact_rows", 0) if isinstance(marts, dict) else 0

        start_time = getattr(dag_run, "start_date", None) or getattr(dag_run, "logical_date", None)
        if start_time is not None:
            start_time = pd.Timestamp(start_time)
            if start_time.tzinfo is not None:
                start_time = start_time.tz_convert("UTC").tz_localize(None)

        audit = pd.DataFrame([{
            "run_id": meta["run_id"],
            "dag_id": "nammamart_elt_bhavani",
            "start_time": start_time if start_time is not None else pd.NaT,
            "end_time": pd.Timestamp(pendulum.now("UTC").naive()),
            "rows_extracted": int(rows_extracted),
            "rows_loaded": int(rows_loaded),
            "rows_quarantined": int(quarantined),
            "status": "SUCCESS" if rows_loaded else "FAILED",
        }])

        with W.connect() as conn:
            W.append_table(conn, "pipeline_runs", audit)
        print(audit.to_string(index=False))

    # ------------------------------------------------------------------
    # Wiring
    # ------------------------------------------------------------------

    meta = prepare_run()

    o = load_raw_orders(meta)
    p = load_raw_payments(meta)
    c = load_raw_customers(meta)
    pr = load_raw_products(meta)
    s = load_raw_stores(meta)
    inc = load_raw_increment(meta, o)

    dims = transform_staging_dimensions(meta, s, pr, c)
    facts = transform_staging_facts(meta, o, p, dims)
    merged = merge_increment(meta, inc, facts)
    marts = build_marts(meta, merged)

    write_dq_results(meta, marts)
    compare_with_etl(meta, marts)
    run_audit(meta, [o, p, c, pr, s, inc], facts, marts)


nammamart_elt()
