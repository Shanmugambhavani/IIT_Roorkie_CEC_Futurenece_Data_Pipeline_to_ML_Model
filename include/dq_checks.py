"""
NammaMart Part D: data quality checks.

One registry of named rules, each tagged with a quality dimension and a severity.
Both pipelines use this same vocabulary:

    ETL  imports run_checks() and splits rows into passed / quarantined
    ELT  mirrors each rule as a SQL predicate in include/sql/stg_*.sql,
         reusing the identical check_name so the two can be compared

Severity:
    Critical  the row is quarantined and never reaches a fact or mart table
    Warning   the row loads, standardised, but the issue is recorded

Every run of run_checks() produces rows for the dq_results table:
    run_id, dag_id, check_name, dimension, severity,
    rows_checked, rows_failed, status, checked_at

Design note: unlike a single combined mask, each rule is evaluated separately and
its name is appended to a per-row failure_reason. A quarantined row therefore says
exactly which rules it broke, which is what acceptance criterion 4 asks for.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

import pandas as pd

# --------------------------------------------------------------------------
# Reference values agreed with the client (Section 3 business rules)
# --------------------------------------------------------------------------

VALID_ORDER_STATUS = {"DELIVERED", "CANCELLED", "RETURNED"}
VALID_PAYMENT_MODE = {"UPI", "Card", "Cash on Delivery", "Wallet"}
VALID_PAYMENT_STATUS = {"SUCCESS", "FAILED", "REFUNDED"}
VALID_LOYALTY_TIER = {"Bronze", "Silver", "Gold", "Platinum"}

MAX_DELIVERY_MINUTES = 120
STALE_AFTER_DAYS = 1

PAYMENT_MODE_MAP = {
    "upi": "UPI",
    "card": "Card",
    "cod": "Cash on Delivery",
    "cash on delivery": "Cash on Delivery",
    "cashh": "Cash on Delivery",
    "wallet": "Wallet",
}

EMAIL_PATTERN = r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$"
PHONE_PATTERN = r"^[6-9]\d{9}$"
PINCODE_PATTERN = r"^\d{6}$"


# --------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------

def to_num(series: pd.Series) -> pd.Series:
    """Numeric or NaN. Never raises, so a junk value fails a rule instead of a task."""
    return pd.to_numeric(series, errors="coerce")


def to_ts(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", format="mixed")


def blank(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip() == ""


def canonical_payment_mode(series: pd.Series) -> pd.Series:
    """'upi', 'UPI ', 'Cashh' all become their canonical form."""
    key = series.fillna("").astype(str).str.strip().str.lower()
    return key.map(PAYMENT_MODE_MAP).fillna(series.fillna("").astype(str).str.strip())


def canonical_status(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().str.upper()


def canonical_tier(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().str.title()


# --------------------------------------------------------------------------
# Check definition
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class Check:
    """A rule. fn returns a boolean Series that is True where the row FAILS."""

    name: str
    dimension: str          # Completeness | Uniqueness | Validity | Accuracy | Consistency | Timeliness
    severity: str           # Critical | Warning
    column: str
    description: str
    fn: Callable[[pd.DataFrame, dict], pd.Series]


# --------------------------------------------------------------------------
# ORDERS
# --------------------------------------------------------------------------

ORDER_CHECKS: list[Check] = [
    Check(
        "orders_duplicate_order_id", "Uniqueness", "Critical", "order_id",
        "order_id appears more than once",
        lambda df, ctx: df["order_id"].duplicated(keep="first"),
    ),
    Check(
        "orders_missing_customer_id", "Completeness", "Critical", "customer_id",
        "customer_id is blank",
        lambda df, ctx: blank(df["customer_id"]),
    ),
    Check(
        "orders_unparseable_order_ts", "Validity", "Critical", "order_ts",
        "order_ts does not parse as a timestamp",
        lambda df, ctx: to_ts(df["order_ts"]).isna(),
    ),
    Check(
        "orders_future_order_ts", "Timeliness", "Critical", "order_ts",
        "order_ts is dated after the run date",
        lambda df, ctx: to_ts(df["order_ts"]) > ctx["as_of"],
    ),
    Check(
        "orders_invalid_status", "Validity", "Critical", "order_status",
        "order_status outside the allowed list after upper-casing",
        lambda df, ctx: ~canonical_status(df["order_status"]).isin(VALID_ORDER_STATUS),
    ),
    Check(
        "orders_status_casing", "Consistency", "Warning", "order_status",
        "order_status not stored in canonical upper case",
        lambda df, ctx: (
            df["order_status"].fillna("").astype(str).str.strip() != canonical_status(df["order_status"])
        ) & canonical_status(df["order_status"]).isin(VALID_ORDER_STATUS),
    ),
    Check(
        "orders_payment_mode_casing", "Consistency", "Warning", "payment_mode",
        "payment_mode needed mapping to a canonical value",
        lambda df, ctx: ~df["payment_mode"].fillna("").astype(str).str.strip().isin(VALID_PAYMENT_MODE),
    ),
    Check(
        "orders_quantity_not_positive", "Accuracy", "Critical", "quantity",
        "quantity is zero or negative",
        lambda df, ctx: ~(to_num(df["quantity"]) > 0),
    ),
    Check(
        "orders_discount_out_of_range", "Validity", "Critical", "discount_pct",
        "discount_pct outside 0 to 100",
        lambda df, ctx: ~to_num(df["discount_pct"]).between(0, 100),
    ),
    Check(
        "orders_amount_formula_mismatch", "Accuracy", "Critical", "order_amount",
        "order_amount does not equal quantity x unit_price x (1 - discount/100)",
        lambda df, ctx: (
            (to_num(df["quantity"]) * to_num(df["unit_price"])
             * (1 - to_num(df["discount_pct"]) / 100)).round(2)
            - to_num(df["order_amount"])
        ).abs() > 0.011,
    ),
    Check(
        "orders_delivered_without_minutes", "Completeness", "Critical", "delivery_minutes",
        "DELIVERED order has no delivery_minutes",
        lambda df, ctx: (canonical_status(df["order_status"]) == "DELIVERED")
        & to_num(df["delivery_minutes"]).isna(),
    ),
    Check(
        "orders_delivery_over_limit", "Accuracy", "Critical", "delivery_minutes",
        f"delivery_minutes above {MAX_DELIVERY_MINUTES}, hours logged as minutes",
        lambda df, ctx: to_num(df["delivery_minutes"]) > MAX_DELIVERY_MINUTES,
    ),
    Check(
        "orders_store_fk_missing", "Consistency", "Critical", "store_id",
        "store_id not present in the store master",
        lambda df, ctx: ~df["store_id"].isin(ctx["store_ids"]),
    ),
    Check(
        "orders_product_fk_missing", "Consistency", "Critical", "product_id",
        "product_id not present in the product catalogue",
        lambda df, ctx: ~df["product_id"].isin(ctx["product_ids"]),
    ),
    Check(
        "orders_customer_fk_missing", "Consistency", "Critical", "customer_id",
        "customer_id not present in the customer master",
        lambda df, ctx: ~blank(df["customer_id"]) & ~df["customer_id"].isin(ctx["customer_ids"]),
    ),
]


# --------------------------------------------------------------------------
# PAYMENTS
# --------------------------------------------------------------------------

PAYMENT_CHECKS: list[Check] = [
    Check(
        "payments_duplicate_payment_id", "Uniqueness", "Critical", "payment_id",
        "payment_id appears more than once",
        lambda df, ctx: df["payment_id"].duplicated(keep="first"),
    ),
    Check(
        "payments_orphan_order_id", "Consistency", "Critical", "order_id",
        "payment references an order_id that does not exist",
        lambda df, ctx: ~df["order_id"].isin(ctx["order_ids"]),
    ),
    Check(
        "payments_invalid_status", "Validity", "Critical", "payment_status",
        "payment_status outside the allowed list",
        lambda df, ctx: ~canonical_status(df["payment_status"]).isin(VALID_PAYMENT_STATUS),
    ),
    Check(
        "payments_non_positive_amount", "Accuracy", "Critical", "amount_paid",
        "SUCCESS payment with a zero or negative amount",
        lambda df, ctx: (canonical_status(df["payment_status"]) == "SUCCESS")
        & ~(to_num(df["amount_paid"]) > 0),
    ),
    Check(
        "payments_unparseable_payment_ts", "Validity", "Critical", "payment_ts",
        "payment_ts does not parse as a timestamp",
        lambda df, ctx: to_ts(df["payment_ts"]).isna(),
    ),
    Check(
        "payments_future_payment_ts", "Timeliness", "Critical", "payment_ts",
        "payment_ts is dated after the run date",
        lambda df, ctx: to_ts(df["payment_ts"]) > ctx["as_of"],
    ),
    Check(
        "payments_missing_gateway_ref", "Completeness", "Warning", "gateway_ref",
        "gateway_ref blank on a non cash payment",
        lambda df, ctx: blank(df["gateway_ref"])
        & (canonical_payment_mode(df["payment_mode"]) != "Cash on Delivery"),
    ),
    Check(
        "payments_payment_mode_casing", "Consistency", "Warning", "payment_mode",
        "payment_mode needed mapping to a canonical value",
        lambda df, ctx: ~df["payment_mode"].fillna("").astype(str).str.strip().isin(VALID_PAYMENT_MODE),
    ),
]


# --------------------------------------------------------------------------
# CUSTOMERS
# --------------------------------------------------------------------------

CUSTOMER_CHECKS: list[Check] = [
    Check(
        "customers_duplicate_id", "Uniqueness", "Warning", "customer_id",
        "customer_id appears more than once; deduplicated in transform, not rejected",
        lambda df, ctx: df["customer_id"].duplicated(keep="last"),
    ),
    Check(
        "customers_missing_id", "Completeness", "Critical", "customer_id",
        "customer_id is blank",
        lambda df, ctx: blank(df["customer_id"]),
    ),
    Check(
        "customers_future_signup", "Timeliness", "Warning", "signup_date",
        "signup_date is dated after the run date",
        lambda df, ctx: to_ts(df["signup_date"]) > ctx["as_of"],
    ),
    Check(
        "customers_bad_email", "Validity", "Warning", "email",
        "email does not match a valid address pattern",
        lambda df, ctx: ~df["email"].fillna("").astype(str).str.match(EMAIL_PATTERN),
    ),
    Check(
        "customers_bad_phone", "Validity", "Warning", "phone",
        "phone is not a 10 digit Indian mobile number",
        lambda df, ctx: ~df["phone"].fillna("").astype(str).str.match(PHONE_PATTERN),
    ),
    Check(
        "customers_bad_pincode", "Validity", "Warning", "pincode",
        "pincode is not 6 digits",
        lambda df, ctx: ~df["pincode"].fillna("").astype(str).str.match(PINCODE_PATTERN),
    ),
    Check(
        "customers_missing_city", "Completeness", "Warning", "city",
        "city is blank",
        lambda df, ctx: blank(df["city"]),
    ),
    Check(
        "customers_tier_casing", "Consistency", "Warning", "loyalty_tier",
        "loyalty_tier not in canonical title case",
        lambda df, ctx: ~df["loyalty_tier"].fillna("").astype(str).str.strip().isin(VALID_LOYALTY_TIER),
    ),
]


# --------------------------------------------------------------------------
# PRODUCTS
# --------------------------------------------------------------------------

PRODUCT_CHECKS: list[Check] = [
    Check(
        "products_duplicate_id", "Uniqueness", "Warning", "product_id",
        "product_id appears more than once; the (old) row is dropped in transform",
        lambda df, ctx: df["product_id"].duplicated(keep="first"),
    ),
    Check(
        "products_missing_category", "Completeness", "Warning", "category",
        "category is blank; defaulted to Uncategorised so revenue is not lost",
        lambda df, ctx: blank(df["category"]),
    ),
    Check(
        "products_non_positive_mrp", "Accuracy", "Warning", "mrp",
        "mrp is zero or negative",
        lambda df, ctx: ~(to_num(df["mrp"]) > 0),
    ),
    Check(
        "products_cost_above_mrp", "Accuracy", "Warning", "cost_price",
        "cost_price exceeds mrp; margin for these products is flagged, not dropped",
        lambda df, ctx: to_num(df["cost_price"]) > to_num(df["mrp"]),
    ),
    Check(
        "products_future_launch", "Timeliness", "Warning", "launch_date",
        "launch_date is dated after the run date",
        lambda df, ctx: to_ts(df["launch_date"]) > ctx["as_of"],
    ),
]


# --------------------------------------------------------------------------
# STORES
# --------------------------------------------------------------------------

STORE_CHECKS: list[Check] = [
    Check(
        "stores_duplicate_id", "Uniqueness", "Warning", "store_id",
        "store_id appears more than once",
        lambda df, ctx: df["store_id"].duplicated(keep="first"),
    ),
    Check(
        "stores_missing_zone", "Completeness", "Warning", "zone",
        "zone is blank; defaulted to Unknown pending the store master owner's answer",
        lambda df, ctx: blank(df["zone"]),
    ),
    Check(
        "stores_bad_pincode", "Validity", "Warning", "pincode",
        "pincode is not 6 digits",
        lambda df, ctx: ~df["pincode"].fillna("").astype(str).str.match(PINCODE_PATTERN),
    ),
    Check(
        "stores_non_positive_capacity", "Accuracy", "Warning", "capacity_orders_per_day",
        "capacity_orders_per_day is zero or negative, breaking utilisation metrics",
        lambda df, ctx: ~(to_num(df["capacity_orders_per_day"]) > 0),
    ),
]


CHECK_REGISTRY: dict[str, list[Check]] = {
    "orders": ORDER_CHECKS,
    "payments": PAYMENT_CHECKS,
    "customers": CUSTOMER_CHECKS,
    "products": PRODUCT_CHECKS,
    "stores": STORE_CHECKS,
}


# --------------------------------------------------------------------------
# Dataset level checks: volume and freshness
# --------------------------------------------------------------------------

def dataset_checks(
    dataset: str,
    df: pd.DataFrame,
    ctx: dict,
    expected_min_rows: int | None = None,
) -> list[dict]:
    """
    Checks that look at the file as a whole rather than row by row.
    These cannot quarantine anything, so they always load as Warnings.
    Covers Part F experiments 7 (volume) and 9 (freshness).
    """
    results = []

    if expected_min_rows is not None:
        failed = int(len(df) < expected_min_rows)
        results.append(
            _result_row(
                ctx, f"{dataset}_volume_drop", "Volume", "Warning",
                rows_checked=len(df), rows_failed=failed,
                note=f"expected at least {expected_min_rows} rows, found {len(df)}",
            )
        )

    ts_column = {"orders": "order_ts", "payments": "payment_ts"}.get(dataset)
    if ts_column and ts_column in df.columns:
        # Future dated rows are excluded before taking the maximum. Seven orders in
        # this export are dated 2027, and using a plain max() lets those junk rows
        # mask a genuinely stale file: the data looks fresh forever because one bad
        # timestamp is always in the future. Freshness has to be measured against
        # the latest *plausible* record, not the latest record.
        parsed = to_ts(df[ts_column])
        latest = parsed[parsed <= ctx["as_of"]].max()
        stale = bool(
            pd.isna(latest)
            or (ctx["as_of"] - latest).days > STALE_AFTER_DAYS
        )
        results.append(
            _result_row(
                ctx, f"{dataset}_freshness", "Timeliness", "Warning",
                rows_checked=len(df), rows_failed=int(stale),
                note=f"latest {ts_column} is {latest}",
            )
        )

    return results


# --------------------------------------------------------------------------
# Runner
# --------------------------------------------------------------------------

def _result_row(
    ctx: dict, check_name: str, dimension: str, severity: str,
    rows_checked: int, rows_failed: int, note: str = "",
) -> dict:
    if rows_failed == 0:
        status = "PASS"
    else:
        status = "FAIL" if severity == "Critical" else "WARN"
    return {
        "run_id": ctx["run_id"],
        "dag_id": ctx["dag_id"],
        "check_name": check_name,
        "dimension": dimension,
        "severity": severity,
        "rows_checked": int(rows_checked),
        "rows_failed": int(rows_failed),
        "status": status,
        "checked_at": datetime.now(timezone.utc),
        "note": note,
    }


def build_context(
    run_id: str,
    dag_id: str,
    as_of: pd.Timestamp,
    store_ids=(),
    product_ids=(),
    customer_ids=(),
    order_ids=(),
) -> dict:
    """Everything a check might need beyond the dataframe it is checking."""
    # The CSV timestamps parse as timezone naive. A timezone aware as_of (for
    # example pendulum in Asia/Kolkata) cannot be compared against them, so it is
    # normalised to naive local time here rather than at every call site.
    as_of_ts = pd.Timestamp(as_of)
    if as_of_ts.tzinfo is not None:
        as_of_ts = as_of_ts.tz_localize(None)

    return {
        "run_id": run_id,
        "dag_id": dag_id,
        "as_of": as_of_ts,
        "store_ids": set(store_ids),
        "product_ids": set(product_ids),
        "customer_ids": set(customer_ids),
        "order_ids": set(order_ids),
    }


def run_checks(
    dataset: str,
    df: pd.DataFrame,
    ctx: dict,
    expected_min_rows: int | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Apply every registered check for a dataset.

    Returns (passed, quarantined, dq_results).

    passed       rows that broke no Critical rule. Warning rows are included here,
                 carrying a warning_reason column so the issue stays visible.
    quarantined  rows that broke at least one Critical rule, with failure_reason
                 listing every rule they broke, semicolon separated.
    dq_results   one row per check, ready to append to the dq_results table.
    """
    checks = CHECK_REGISTRY.get(dataset, [])
    if df.empty:
        empty = df.copy()
        empty["failure_reason"] = pd.Series(dtype="object")
        return df.copy(), empty, pd.DataFrame(dataset_checks(dataset, df, ctx, expected_min_rows))

    work = df.reset_index(drop=True).copy()
    critical_reasons = pd.Series([""] * len(work), index=work.index)
    warning_reasons = pd.Series([""] * len(work), index=work.index)
    results: list[dict] = []

    for check in checks:
        if check.column not in work.columns:
            # Schema drift. Record it loudly rather than skipping in silence.
            results.append(
                _result_row(
                    ctx, check.name, "Schema", "Critical",
                    rows_checked=len(work), rows_failed=len(work),
                    note=f"column {check.column} missing from {dataset}",
                )
            )
            critical_reasons = critical_reasons.str.cat(
                pd.Series([f"schema_missing_{check.column}"] * len(work), index=work.index),
                sep="; ",
            ).str.strip("; ")
            continue

        mask = check.fn(work, ctx).fillna(False).astype(bool)
        results.append(
            _result_row(
                ctx, check.name, check.dimension, check.severity,
                rows_checked=len(work), rows_failed=int(mask.sum()),
                note=check.description,
            )
        )

        target = critical_reasons if check.severity == "Critical" else warning_reasons
        target.loc[mask] = (target.loc[mask] + "; " + check.name).str.strip("; ")
        if check.severity == "Critical":
            critical_reasons = target
        else:
            warning_reasons = target

    results.extend(dataset_checks(dataset, work, ctx, expected_min_rows))

    is_quarantined = critical_reasons != ""

    quarantined = work.loc[is_quarantined].copy()
    quarantined["failure_reason"] = critical_reasons.loc[is_quarantined]
    quarantined["_run_id"] = ctx["run_id"]
    quarantined["_source_file"] = f"{dataset}.csv"

    passed = work.loc[~is_quarantined].copy()
    passed["warning_reason"] = warning_reasons.loc[~is_quarantined]

    return passed, quarantined, pd.DataFrame(results)


DQ_RESULTS_COLUMNS = [
    "run_id", "dag_id", "check_name", "dimension", "severity",
    "rows_checked", "rows_failed", "status", "checked_at", "note",
]
