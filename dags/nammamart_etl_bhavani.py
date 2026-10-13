"""
NammaMart ETL pipeline (Part A).

Extract -> Validate -> Quarantine -> Transform -> Load -> Reconcile -> Report

Bad rows are rejected in Python before anything touches the warehouse, so the
finance tables only ever contain rows that passed every Critical rule. That is
acceptance criterion 2.

Scheduling (Part C): 09:00 Asia/Kolkata, Monday to Saturday, ahead of Kavya's
09:30 leadership stand-up.

Idempotency (criterion 5) and incremental load (criterion 6) share one mechanism:
upsert_by_key deletes the incoming order_ids and re-inserts them. Re-running
changes nothing; the increment file updates the 10 corrected orders in place and
appends the 150 new ones.

Business logic lives in include/transforms.py and include/dq_checks.py so it can
be unit tested without a scheduler. This file only orchestrates.
"""

from __future__ import annotations

import os
import sys
from datetime import timedelta
from pathlib import Path

import pendulum

from airflow.decorators import dag, task

sys.path.insert(0, "/usr/local/airflow/include")

import pandas as pd  # noqa: E402

import dq_checks as dq  # noqa: E402
import transforms as T  # noqa: E402
import warehouse as W  # noqa: E402

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

OWNER = "bhavani"
BASE = Path("/usr/local/airflow/include")
DATA_DIR = BASE / "data" / "nammamart"
QUARANTINE_DIR = BASE / "quarantine" / OWNER
STAGING_DIR = BASE / "staging" / OWNER

INCREMENT_FILE = DATA_DIR / "orders_2026-10-01_increment.csv"

# The extract represents the business day of 1 October 2026. Timeliness checks
# compare against this rather than wall-clock time, so the assignment data does not
# fail a freshness rule every day it is marked.
#
# End of day, not midnight. Set to midnight, the future-timestamp rule rejects every
# order placed on 1 October, which is the entire increment file. A date boundary that
# excludes the current business day is a classic way to quarantine good data.
AS_OF = pendulum.datetime(2026, 10, 1, 23, 59, 59, tz="Asia/Kolkata")

# Volume floors, used by the Part F experiment 7 volume anomaly check.
EXPECTED_MIN_ROWS = {
    "orders": 1500,
    "payments": 1400,
    "customers": 300,
    "products": 100,
    "stores": 10,
}

MASTERS = ["stores", "products", "customers"]
TRANSACTIONS = ["orders", "payments"]


def safe_run_id(run_id: str) -> str:
    """Airflow run_ids contain colons and plus signs, which are illegal in filenames."""
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in run_id)


def read_raw(name: str) -> pd.DataFrame:
    """Everything as text. Type casting happens after validation, never before."""
    return pd.read_csv(DATA_DIR / f"{name}.csv", dtype=str, keep_default_na=False)


default_args = {
    "owner": OWNER,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
    "depends_on_past": False,
}


@dag(
    dag_id="nammamart_etl_bhavani",
    description="NammaMart ETL: clean before the warehouse, finance tables only",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["nammamart", "etl", OWNER],
)
def nammamart_etl():

    # ------------------------------------------------------------------
    # EXTRACT: five parallel tasks, one per source file
    # ------------------------------------------------------------------

    @task
    def prepare_run(**context) -> dict:
        run_id = safe_run_id(context["run_id"])
        run_dir = STAGING_DIR / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
        return {"run_id": run_id, "raw_run_id": context["run_id"], "run_dir": str(run_dir)}

    def _extract(name: str, meta: dict) -> dict:
        df = read_raw(name)
        path = Path(meta["run_dir"]) / f"raw_{name}.pkl"
        df.to_pickle(path)
        print(f"[extract] {name}.csv -> {len(df)} rows, {len(df.columns)} columns")
        return {"dataset": name, "path": str(path), "rows": len(df)}

    @task
    def extract_orders(meta: dict) -> dict:
        return _extract("orders", meta)

    @task
    def extract_payments(meta: dict) -> dict:
        return _extract("payments", meta)

    @task
    def extract_customers(meta: dict) -> dict:
        return _extract("customers", meta)

    @task
    def extract_products(meta: dict) -> dict:
        return _extract("products", meta)

    @task
    def extract_stores(meta: dict) -> dict:
        return _extract("stores", meta)

    @task
    def extract_increment(meta: dict) -> dict:
        """Optional. Absent on a normal day, present when the next-day file lands."""
        if not INCREMENT_FILE.exists():
            print("[extract] no increment file today")
            return {"dataset": "increment", "path": None, "rows": 0}
        df = pd.read_csv(INCREMENT_FILE, dtype=str, keep_default_na=False)
        path = Path(meta["run_dir"]) / "raw_increment.pkl"
        df.to_pickle(path)
        print(f"[extract] increment -> {len(df)} rows")
        return {"dataset": "increment", "path": str(path), "rows": len(df)}

    # ------------------------------------------------------------------
    # VALIDATE and QUARANTINE
    # ------------------------------------------------------------------

    def _write_quarantine(df: pd.DataFrame, dataset: str, run_id: str) -> int:
        if df.empty:
            return 0
        path = QUARANTINE_DIR / f"{dataset}_failed_{run_id}.csv"
        df.to_csv(path, index=False)
        print(f"[quarantine] {dataset}: {len(df)} rows -> {path.name}")
        print(df["failure_reason"].value_counts().head(10).to_string())
        return len(df)

    @task
    def validate_masters(meta: dict, stores: dict, products: dict, customers: dict) -> dict:
        """
        Dimension rows are repaired, not rejected, wherever the entity itself is
        real. Rejecting a store because its zone is blank would silently delete
        every order placed there, which is the failure mode this pipeline exists
        to remove. Only a missing primary key is fatal here.
        """
        run_dir = Path(meta["run_dir"])
        ctx = dq.build_context(meta["raw_run_id"], "nammamart_etl_bhavani", AS_OF)

        results, quarantined, out = [], {}, {}
        standardise = {
            "stores": T.standardise_stores,
            "products": T.standardise_products,
            "customers": T.standardise_customers,
        }

        for name, info in (("stores", stores), ("products", products), ("customers", customers)):
            df = pd.read_pickle(info["path"])
            passed, failed, res = dq.run_checks(name, df, ctx, EXPECTED_MIN_ROWS[name])
            results.append(res)
            quarantined[name] = _write_quarantine(failed, name, meta["run_id"])

            clean = standardise[name](passed)
            path = run_dir / f"dim_{name}.pkl"
            clean.to_pickle(path)
            out[name] = {"path": str(path), "rows_in": len(df), "rows_out": len(clean)}
            print(f"[validate] {name}: {len(df)} in, {len(passed)} passed, {len(failed)} quarantined, "
                  f"{len(clean)} after dedupe")

        pd.concat(results, ignore_index=True).to_pickle(run_dir / "dq_masters.pkl")
        return {"dims": out, "quarantined": quarantined}

    @task
    def validate_transactions(meta: dict, orders: dict, payments: dict,
                              increment: dict, masters: dict) -> dict:
        """
        Facts are rejected, not repaired. A row with an impossible amount or a
        store that does not exist has no defensible value, so it goes to
        quarantine with the exact rules it broke.
        """
        run_dir = Path(meta["run_dir"])

        dim_store = pd.read_pickle(masters["dims"]["stores"]["path"])
        dim_product = pd.read_pickle(masters["dims"]["products"]["path"])
        dim_customer = pd.read_pickle(masters["dims"]["customers"]["path"])

        orders_df = pd.read_pickle(orders["path"])
        if increment["path"]:
            inc = pd.read_pickle(increment["path"])
            # Corrections win over the original row, so the increment goes last
            # and the duplicate rule keeps the first occurrence of each order_id.
            orders_df = pd.concat([inc, orders_df], ignore_index=True)
            print(f"[validate] merged increment: {len(inc)} rows on top of {orders['rows']}")

        ctx = dq.build_context(
            meta["raw_run_id"], "nammamart_etl_bhavani", AS_OF,
            store_ids=dim_store["store_id"],
            product_ids=dim_product["product_id"],
            customer_ids=dim_customer["customer_id"],
            order_ids=orders_df["order_id"],
        )

        results, out, quarantined = [], {}, {}

        o_pass, o_fail, o_res = dq.run_checks("orders", orders_df, ctx, EXPECTED_MIN_ROWS["orders"])
        results.append(o_res)
        quarantined["orders"] = _write_quarantine(o_fail, "orders", meta["run_id"])
        T.standardise_orders(o_pass).to_pickle(run_dir / "clean_orders.pkl")
        o_fail[["order_id"]].to_pickle(run_dir / "rejected_order_ids.pkl")
        out["orders"] = {"rows_in": len(orders_df), "rows_out": len(o_pass)}
        print(f"[validate] orders: {len(orders_df)} in, {len(o_pass)} passed, {len(o_fail)} quarantined")

        p_df = pd.read_pickle(payments["path"])
        p_pass, p_fail, p_res = dq.run_checks("payments", p_df, ctx, EXPECTED_MIN_ROWS["payments"])
        results.append(p_res)
        quarantined["payments"] = _write_quarantine(p_fail, "payments", meta["run_id"])
        T.standardise_payments(p_pass).to_pickle(run_dir / "clean_payments.pkl")
        out["payments"] = {"rows_in": len(p_df), "rows_out": len(p_pass)}
        print(f"[validate] payments: {len(p_df)} in, {len(p_pass)} passed, {len(p_fail)} quarantined")

        pd.concat(results, ignore_index=True).to_pickle(run_dir / "dq_transactions.pkl")
        return {"datasets": out, "quarantined": quarantined}

    # ------------------------------------------------------------------
    # TRANSFORM and LOAD
    # ------------------------------------------------------------------

    @task
    def build_star(meta: dict, masters: dict, transactions: dict) -> dict:
        run_dir = Path(meta["run_dir"])
        orders = pd.read_pickle(run_dir / "clean_orders.pkl")
        customers = pd.read_pickle(masters["dims"]["customers"]["path"])
        products = pd.read_pickle(masters["dims"]["products"]["path"])
        stores = pd.read_pickle(masters["dims"]["stores"]["path"])

        fact = T.build_fact_orders(orders, customers, products, stores)
        fact.to_pickle(run_dir / "fact_orders.pkl")

        print(f"[transform] fact_orders: {len(fact)} rows")
        print(f"[transform] net revenue: {fact['net_revenue'].sum():,.2f}")
        print(f"[transform] gross margin: {fact['gross_margin'].sum():,.2f}")
        return {"fact_rows": len(fact), "net_revenue": float(fact["net_revenue"].sum())}

    @task
    def load_warehouse(meta: dict, masters: dict, star: dict) -> dict:
        """
        Dimensions are replaced whole, they are small masters.
        The fact is upserted on order_id, which makes re-runs and the incremental
        load behave identically.
        """
        run_dir = Path(meta["run_dir"])
        fact = pd.read_pickle(run_dir / "fact_orders.pkl")

        with W.connect() as conn:
            for name, table in (("customers", "etl_dim_customer"),
                                ("products", "etl_dim_product"),
                                ("stores", "etl_dim_store")):
                df = pd.read_pickle(masters["dims"][name]["path"])
                W.replace_table(conn, table, df)
                print(f"[load] {table}: {len(df)} rows")

            stats = W.upsert_by_key(conn, "etl_fact_orders", fact, "order_id")
            print(f"[load] etl_fact_orders: {stats}")

        return stats

    @task
    def reconcile(meta: dict, load_stats: dict) -> dict:
        """Acceptance criterion 7: every order and payment mismatch, with a reason."""
        run_dir = Path(meta["run_dir"])
        fact = pd.read_pickle(run_dir / "fact_orders.pkl")
        payments = pd.read_pickle(run_dir / "clean_payments.pkl")
        rejected = set(pd.read_pickle(run_dir / "rejected_order_ids.pkl")["order_id"])

        recon = T.reconcile_payments(fact, payments, rejected)

        with W.connect() as conn:
            W.replace_table(conn, "etl_payment_reconciliation", recon)

        print(f"[reconcile] {len(recon)} mismatches")
        if not recon.empty:
            print(recon["reason"].value_counts().to_string())
            print(f"[reconcile] unreconciled value: {recon['difference'].sum():,.2f}")
        return {"rows": len(recon), "net_difference": float(recon["difference"].sum()) if len(recon) else 0.0}

    @task
    def build_reports(meta: dict, recon_stats: dict) -> dict:
        run_dir = Path(meta["run_dir"])
        fact = pd.read_pickle(run_dir / "fact_orders.pkl")
        customers = pd.read_pickle(run_dir / "dim_customers.pkl")

        zone = T.build_daily_zone_revenue(fact)
        category = T.build_category_margin(fact)
        top = T.build_top_customers(fact, customers)

        with W.connect() as conn:
            W.replace_table(conn, "etl_daily_zone_revenue", zone)
            W.replace_table(conn, "etl_category_margin", category)
            W.replace_table(conn, "etl_top_customers", top)

        print("\n[report] revenue by zone")
        print(zone.groupby("zone")["net_revenue"].sum().round(2).to_string())
        print("\n[report] margin by category")
        print(category.to_string(index=False))
        print("\n[report] top 10 customers, PII masked")
        print(top.to_string(index=False))

        return {"zone_rows": len(zone), "category_rows": len(category)}

    @task
    def write_dq_results(meta: dict, masters: dict, transactions: dict, reports: dict) -> int:
        """Part D: the quality scorecard, appended every run so trends build up."""
        run_dir = Path(meta["run_dir"])
        frames = [
            pd.read_pickle(run_dir / "dq_masters.pkl"),
            pd.read_pickle(run_dir / "dq_transactions.pkl"),
        ]
        results = pd.concat(frames, ignore_index=True)[dq.DQ_RESULTS_COLUMNS]

        with W.connect() as conn:
            W.append_table(conn, "dq_results", results)

        failed = results[results["status"] != "PASS"]
        print(f"[dq] {len(results)} checks, {len(failed)} not passing")
        print(failed[["check_name", "severity", "rows_failed", "status"]].to_string(index=False))
        return len(results)

    # ------------------------------------------------------------------
    # RUN AUDIT (Part F)
    # ------------------------------------------------------------------

    @task(trigger_rule="all_done")
    def run_audit(meta: dict, extracts: list, masters: dict,
                  transactions: dict, load_stats: dict, **context) -> None:
        """
        Writes one row per run whatever happened upstream, so a failed run is as
        visible as a successful one. That is the point of all_done here.
        """
        dag_run = context.get("dag_run")

        rows_extracted = sum(e.get("rows", 0) for e in extracts if isinstance(e, dict))
        quarantined = 0
        for block in (masters, transactions):
            if isinstance(block, dict):
                quarantined += sum((block.get("quarantined") or {}).values())

        rows_loaded = (load_stats or {}).get("final_rows", 0) if isinstance(load_stats, dict) else 0

        # The Airflow 3 task SDK does not expose get_task_instances() on the dag_run
        # object the way Airflow 2 did. Rather than depend on an API that varies by
        # version, the run status is inferred from whether the load actually produced
        # rows. An audit task that itself crashes is worse than a slightly coarser
        # status, because then nobody learns anything about the run at all.
        status = "SUCCESS" if rows_loaded else "FAILED"

        start_time = getattr(dag_run, "start_date", None) or getattr(dag_run, "logical_date", None)
        if start_time is not None:
            start_time = pd.Timestamp(start_time)
            if start_time.tzinfo is not None:
                start_time = start_time.tz_convert("UTC").tz_localize(None)

        audit = pd.DataFrame([{
            "run_id": meta["raw_run_id"],
            "dag_id": "nammamart_etl_bhavani",
            "start_time": start_time if start_time is not None else pd.NaT,
            "end_time": pd.Timestamp(pendulum.now("UTC").naive()),
            "rows_extracted": int(rows_extracted),
            "rows_loaded": int(rows_loaded),
            "rows_quarantined": int(quarantined),
            "status": status,
        }])

        with W.connect() as conn:
            W.append_table(conn, "pipeline_runs", audit)

        print(audit.to_string(index=False))

    # ------------------------------------------------------------------
    # Wiring
    # ------------------------------------------------------------------

    meta = prepare_run()

    o = extract_orders(meta)
    p = extract_payments(meta)
    c = extract_customers(meta)
    pr = extract_products(meta)
    s = extract_stores(meta)
    inc = extract_increment(meta)

    masters = validate_masters(meta, s, pr, c)
    transactions = validate_transactions(meta, o, p, inc, masters)
    star = build_star(meta, masters, transactions)
    load_stats = load_warehouse(meta, masters, star)
    recon_stats = reconcile(meta, load_stats)
    reports = build_reports(meta, recon_stats)
    write_dq_results(meta, masters, transactions, reports)

    run_audit(meta, [o, p, c, pr, s, inc], masters, transactions, load_stats)


nammamart_etl()
