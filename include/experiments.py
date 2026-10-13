"""
NammaMart Part F: experiment scenarios.

Sets up each observability experiment and restores afterwards, so you never edit a
CSV by hand or re-download the dataset.

Usage, from the repo folder on Windows:

    py -3.12 include/experiments.py backup        make the safety copy (do this first)
    py -3.12 include/experiments.py <name>        set up a scenario
    astro dev run dags test nammamart_etl_bhavani
    py -3.12 include/experiments.py restore       put everything back

Scenarios:

    baddata      exp 5: append 5 rows that break 5 different rules
    schema       exp 6: rename order_amount to total
    volume       exp 7: cut orders down to 50 rows
    drift        exp 8: multiply S01 unit_price and order_amount by 100
    stale        exp 9: shift every order_ts back 30 days
    nofile       exp 4: hide products.csv

Always run `restore` before the next scenario, or you will be testing two things
at once and will not know which caused what.
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

import pandas as pd

DATA = Path("include/data/nammamart")
BACKUP = Path("include/data/_backup")
ORDERS = DATA / "orders.csv"

BAD_ROWS = [
    # order_id, order_ts, customer_id, store_id, product_id, qty, unit_price,
    # discount, amount, mode, status, channel, minutes
    ("ORD900001", "2026-09-15 10:00:00", "",        "S01", "P0001", "2",  "50.00", "0", "100.00",  "UPI", "DELIVERED", "App", "12"),
    ("ORD900002", "2026-09-15 10:05:00", "C00001",  "S01", "P0001", "-3", "50.00", "0", "-150.00", "UPI", "DELIVERED", "App", "12"),
    ("ORD900003", "2026-09-15 10:10:00", "C00001",  "S99", "P0001", "2",  "50.00", "0", "100.00",  "UPI", "DELIVERED", "App", "12"),
    ("ORD900004", "2026-09-15 10:15:00", "C00001",  "S01", "P0001", "2",  "50.00", "0", "100.00",  "UPI", "SHIPPED",   "App", "12"),
    ("ORD900005", "2027-05-01 10:00:00", "C00001",  "S01", "P0001", "2",  "50.00", "0", "100.00",  "UPI", "DELIVERED", "App", "12"),
]

EXPECTED_REASONS = {
    "ORD900001": "orders_missing_customer_id",
    "ORD900002": "orders_quantity_not_positive; orders_amount_formula_mismatch",
    "ORD900003": "orders_store_fk_missing",
    "ORD900004": "orders_invalid_status",
    "ORD900005": "orders_future_order_ts",
}


def read_orders() -> pd.DataFrame:
    return pd.read_csv(ORDERS, dtype=str, keep_default_na=False)


def write_orders(df: pd.DataFrame) -> None:
    df.to_csv(ORDERS, index=False)


def backup() -> None:
    BACKUP.mkdir(parents=True, exist_ok=True)
    for f in DATA.glob("*.csv"):
        shutil.copy2(f, BACKUP / f.name)
    print(f"backed up {len(list(BACKUP.glob('*.csv')))} files to {BACKUP}")


def restore() -> None:
    if not BACKUP.exists():
        print("no backup found. run `backup` first")
        return
    for f in BACKUP.glob("*.csv"):
        shutil.copy2(f, DATA / f.name)
    # undo the nofile scenario if it is still in effect
    hidden = DATA / "products_HIDDEN.csv"
    if hidden.exists() and not (DATA / "products.csv").exists():
        hidden.rename(DATA / "products.csv")
    if hidden.exists():
        hidden.unlink()
    print(f"restored {len(list(BACKUP.glob('*.csv')))} files")


def baddata() -> None:
    """Experiment 5. Five rows, five different critical rules, one firing twice."""
    df = read_orders()
    new = pd.DataFrame(BAD_ROWS, columns=df.columns)
    write_orders(pd.concat([df, new], ignore_index=True))
    print(f"appended 5 bad rows. orders.csv now {len(df) + 5} rows")
    print("expected failure_reason per row:")
    for order_id, reason in EXPECTED_REASONS.items():
        print(f"  {order_id}: {reason}")
    print("\nwatch for quarantined count rising by 5")


def schema() -> None:
    """Experiment 6. Rename a column the pipeline depends on."""
    df = read_orders()
    df = df.rename(columns={"order_amount": "total"})
    write_orders(df)
    print("renamed order_amount -> total")
    print("ETL: expect a Schema row in dq_results, or a KeyError in validate_transactions")
    print("ELT: raw_orders loads fine (all_varchar), stg_orders_upsert.sql fails on binder error")
    print("record WHICH pipeline fails EARLIER, and why that matters")


def volume() -> None:
    """Experiment 7. A 98% volume drop that no row-level rule can see."""
    df = read_orders()
    write_orders(df.head(50))
    print(f"cut orders.csv from {len(df)} to 50 rows")
    print("expect orders_volume_drop to fire WARN (threshold is 1500)")
    print("note what does NOT fire: no revenue check, no per-store check")


def drift() -> None:
    """
    Experiment 8. The important one. Both unit_price and order_amount are
    multiplied, so the formula check still passes. Change only one and you are
    testing orders_amount_formula_mismatch instead of distribution drift.
    """
    df = read_orders()
    mask = df["store_id"] == "S01"
    for col in ("unit_price", "order_amount"):
        df.loc[mask, col] = (pd.to_numeric(df.loc[mask, col]) * 100).round(2).astype(str)
    write_orders(df)
    print(f"inflated {int(mask.sum())} S01 rows by 100x (price AND amount)")
    print("expect: ZERO new quarantined rows. every rule still passes.")
    print("expect: S01 goes from ~7.1% of revenue to ~88%")
    print("this is the experiment that shows what row-level rules cannot catch")


def stale() -> None:
    """Experiment 9. Shift the whole file back 30 days."""
    df = read_orders()
    ts = pd.to_datetime(df["order_ts"], errors="coerce", format="mixed")
    shifted = (ts - pd.Timedelta(days=30)).dt.strftime("%Y-%m-%d %H:%M:%S")
    df["order_ts"] = shifted.fillna(df["order_ts"])
    write_orders(df)
    print(f"shifted {ts.notna().sum()} order_ts values back 30 days")
    print(f"latest order_ts now {shifted.max()}")
    print("expect orders_freshness to fire WARN against AS_OF 2026-10-01")


def nofile() -> None:
    """Experiment 4. Hide a source file the DAG depends on."""
    src = DATA / "products.csv"
    if not src.exists():
        print("products.csv already hidden")
        return
    src.rename(DATA / "products_HIDDEN.csv")
    print("hid products.csv")
    print("expect: extract_products up_for_retry, 2 retries 5 min apart, then failed")
    print("expect: downstream tasks upstream_failed, run_audit still runs (all_done)")
    print("expect: Slack alert, or a console fallback alert in the task log")
    print("\nthis one takes 10+ minutes to finish because of the retry delay")


SCENARIOS = {
    "backup": backup, "restore": restore,
    "baddata": baddata, "schema": schema, "volume": volume,
    "drift": drift, "stale": stale, "nofile": nofile,
}

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in SCENARIOS:
        print(__doc__)
        print("scenarios:", ", ".join(SCENARIOS))
        sys.exit(1)
    SCENARIOS[sys.argv[1]]()
