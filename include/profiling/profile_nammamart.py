"""
NammaMart Part 0: discovery and data profiling.

Run standalone, before any DAG code exists:

    python include/profiling/profile_nammamart.py \
        --data include/data/nammamart \
        --out  include/profiling/out

Produces:
    out/profile_summary.txt    column level profile of all six files
    out/data_issues_log.csv    one row per detected issue group
    out/issue_rows/<rule>.csv  the exact offending rows, for evidence

Everything here is read only. It never writes back to the source CSVs.
"""

from __future__ import annotations

import argparse
import os
import re
from pathlib import Path

import pandas as pd

FILES = [
    "orders",
    "payments",
    "customers",
    "products",
    "stores",
    "orders_2026-10-01_increment",
]

VALID_STATUS = {"DELIVERED", "CANCELLED", "RETURNED"}
VALID_PAYMENT_MODE = {"UPI", "Card", "Cash on Delivery", "Wallet"}
VALID_TIER = {"Bronze", "Silver", "Gold", "Platinum"}

# As agreed with ops: the business day the September extract represents.
AS_OF = pd.Timestamp("2026-10-01 00:00:00")

PAYMENT_MODE_MAP = {
    "upi": "UPI",
    "card": "Card",
    "cod": "Cash on Delivery",
    "cash on delivery": "Cash on Delivery",
    "cashh": "Cash on Delivery",
    "wallet": "Wallet",
}


def load_raw(data_dir: Path) -> dict[str, pd.DataFrame]:
    """Read every CSV as text. No type inference, no NA coercion, nothing lost."""
    frames = {}
    for name in FILES:
        frames[name] = pd.read_csv(
            data_dir / f"{name}.csv", dtype=str, keep_default_na=False
        )
    return frames


def num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def ts(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", format="mixed")


def profile_columns(frames: dict[str, pd.DataFrame]) -> str:
    lines = []
    for name, df in frames.items():
        lines.append("=" * 78)
        lines.append(f"{name}  rows={len(df)}  cols={len(df.columns)}")
        lines.append("=" * 78)
        for col in df.columns:
            blank = int((df[col].str.strip() == "").sum())
            distinct = int(df[col].nunique())
            numeric = num(df[col])
            if numeric.notna().mean() > 0.8:
                rng = f"min={numeric.min()} max={numeric.max()}"
            else:
                top = df[col].value_counts().head(4).to_dict()
                rng = f"top={top}"
            lines.append(
                f"  {col:26s} blank={blank:5d} distinct={distinct:6d}  {rng}"
            )
        lines.append("")
    return "\n".join(lines)


class IssueCollector:
    """Accumulates issue groups and the rows that triggered them."""

    def __init__(self, out_dir: Path):
        self.rows: list[dict] = []
        self.out_dir = out_dir
        (out_dir / "issue_rows").mkdir(parents=True, exist_ok=True)

    def add(
        self,
        file: str,
        column: str,
        rule: str,
        problem: str,
        dimension: str,
        severity: str,
        proposed_rule: str,
        offenders: pd.DataFrame,
        key_col: str | None = None,
    ) -> None:
        count = len(offenders)
        if count == 0:
            return
        keys = []
        if key_col and key_col in offenders.columns:
            keys = offenders[key_col].head(6).tolist()
        self.rows.append(
            {
                "file": f"{file}.csv",
                "column": column,
                "rule_id": rule,
                "problem": problem,
                "quality_dimension": dimension,
                "severity": severity,
                "rows_affected": count,
                "example_keys": ", ".join(map(str, keys)),
                "proposed_rule": proposed_rule,
            }
        )
        offenders.to_csv(self.out_dir / "issue_rows" / f"{rule}.csv", index=False)

    def to_frame(self) -> pd.DataFrame:
        return pd.DataFrame(self.rows).sort_values(
            ["file", "severity", "rows_affected"], ascending=[True, True, False]
        )


def check_orders(df: pd.DataFrame, masters: dict, col: IssueCollector, file: str) -> None:
    q, u, d = num(df.quantity), num(df.unit_price), num(df.discount_pct)
    amount = num(df.order_amount)
    minutes = num(df.delivery_minutes)
    stamp = ts(df.order_ts)
    status = df.order_status.str.strip().str.upper()

    col.add(file, "order_id", "orders_duplicate_order_id",
            "Same order_id appears more than once (identical rows)",
            "Uniqueness", "Critical",
            "Keep first row per order_id; drop later copies before loading",
            df[df.order_id.duplicated(keep=False)].sort_values("order_id"), "order_id")

    col.add(file, "customer_id", "orders_missing_customer_id",
            "customer_id is blank",
            "Completeness", "Critical",
            "customer_id must be present and non blank",
            df[df.customer_id.str.strip() == ""], "order_id")

    col.add(file, "order_ts", "orders_unparseable_order_ts",
            "order_ts is blank, 'N/A', an impossible date (31/09) or a non ISO format",
            "Validity", "Critical",
            "order_ts must parse as a real timestamp",
            df[stamp.isna()], "order_id")

    col.add(file, "order_ts", "orders_future_order_ts",
            "order_ts is dated after the extract date",
            "Timeliness", "Critical",
            "order_ts must be <= run date",
            df[stamp > AS_OF], "order_id")

    col.add(file, "order_status", "orders_invalid_status",
            "order_status outside DELIVERED / CANCELLED / RETURNED",
            "Validity", "Critical",
            "Uppercase and trim, then reject anything outside the allowed list",
            df[~status.isin(VALID_STATUS)], "order_id")

    raw_status = df.order_status.str.strip()
    col.add(file, "order_status", "orders_status_casing",
            "order_status recorded in lowercase ('delivered')",
            "Consistency", "Warning",
            "Trim and upper-case before validating; the row itself is fine",
            df[raw_status.isin(VALID_STATUS) == False][  # noqa: E712
                raw_status[raw_status.isin(VALID_STATUS) == False].str.upper().isin(VALID_STATUS)  # noqa: E712
            ], "order_id")

    bad_mode = ~df.payment_mode.str.strip().isin(VALID_PAYMENT_MODE)
    col.add(file, "payment_mode", "orders_payment_mode_casing",
            "payment_mode casing or spelling variants (upi, CARD, cod, Cashh, trailing space)",
            "Consistency", "Warning",
            "Map via lookup to the canonical four values; load the row",
            df[bad_mode], "order_id")

    col.add(file, "quantity", "orders_quantity_not_positive",
            "quantity is zero or negative",
            "Accuracy", "Critical",
            "quantity must be > 0; a return is a status, not a negative quantity",
            df[~(q > 0)], "order_id")

    col.add(file, "discount_pct", "orders_discount_out_of_range",
            "discount_pct above 100",
            "Validity", "Critical",
            "discount_pct must be between 0 and 100",
            df[~d.between(0, 100)], "order_id")

    expected = (q * u * (1 - d / 100)).round(2)
    mismatch = (expected - amount).abs() > 0.011
    col.add(file, "order_amount", "orders_amount_formula_mismatch",
            "order_amount does not equal quantity x unit_price x (1 - discount/100)",
            "Accuracy", "Critical",
            "Recompute and compare to 2 decimals; quarantine on mismatch",
            df[mismatch].assign(expected_amount=expected[mismatch]), "order_id")

    col.add(file, "delivery_minutes", "orders_delivered_without_minutes",
            "DELIVERED order has no delivery_minutes",
            "Completeness", "Critical",
            "DELIVERED implies a delivery time; reject if blank",
            df[(status == "DELIVERED") & minutes.isna()], "order_id")

    col.add(file, "delivery_minutes", "orders_delivery_over_120",
            "delivery_minutes of 480, 900 or 1440 (hours recorded as minutes)",
            "Accuracy", "Critical",
            "delivery_minutes must be <= 120 for the 10 minute promise KPI",
            df[minutes > 120], "order_id")

    col.add(file, "store_id", "orders_store_fk_missing",
            "store_id not present in stores.csv",
            "Consistency", "Critical",
            "store_id must exist in the store master",
            df[~df.store_id.isin(masters["stores"].store_id)], "order_id")

    col.add(file, "product_id", "orders_product_fk_missing",
            "product_id not present in products.csv",
            "Consistency", "Critical",
            "product_id must exist in the product catalogue",
            df[~df.product_id.isin(masters["products"].product_id)], "order_id")


def check_payments(df: pd.DataFrame, orders: pd.DataFrame, col: IssueCollector) -> None:
    amount = num(df.amount_paid)
    order_amount = orders.drop_duplicates("order_id").set_index("order_id").order_amount

    col.add("payments", "payment_id", "payments_duplicate_payment_id",
            "Same payment_id and gateway_ref ingested twice",
            "Uniqueness", "Critical",
            "Deduplicate on payment_id before reconciliation",
            df[df.payment_id.duplicated(keep=False)].sort_values("payment_id"), "payment_id")

    col.add("payments", "order_id", "payments_orphan_order_id",
            "Payment references an order_id that does not exist in orders.csv",
            "Consistency", "Critical",
            "Route to reconciliation as PAYMENT_WITHOUT_ORDER, never into revenue",
            df[~df.order_id.isin(orders.order_id)], "payment_id")

    col.add("payments", "amount_paid", "payments_non_positive_amount",
            "SUCCESS payment with zero or negative amount_paid",
            "Accuracy", "Critical",
            "A SUCCESS payment must be > 0",
            df[(df.payment_status == "SUCCESS") & ~(amount > 0)], "payment_id")

    joined = df.assign(expected=df.order_id.map(order_amount))
    success = joined[joined.payment_status == "SUCCESS"]
    gap = (num(success.amount_paid) - num(success.expected)).abs() > 0.011
    col.add("payments", "amount_paid", "payments_amount_mismatch",
            "SUCCESS payment amount differs from the order amount",
            "Consistency", "Critical",
            "Write to the reconciliation table with reason AMOUNT_MISMATCH",
            success[gap], "payment_id")

    col.add("payments", "gateway_ref", "payments_missing_gateway_ref",
            "gateway_ref blank (mostly Cash on Delivery, but also some card and UPI rows)",
            "Completeness", "Warning",
            "Load the row; flag blanks so finance can chase the gateway",
            df[df.gateway_ref.str.strip() == ""], "payment_id")

    bad_mode = ~df.payment_mode.str.strip().isin(VALID_PAYMENT_MODE)
    col.add("payments", "payment_mode", "payments_payment_mode_casing",
            "payment_mode casing or spelling variants",
            "Consistency", "Warning",
            "Map to canonical values before grouping revenue by mode",
            df[bad_mode], "payment_id")


def check_customers(df: pd.DataFrame, col: IssueCollector) -> None:
    signup = ts(df.signup_date)

    col.add("customers", "customer_id", "customers_duplicate_id",
            "Same customer_id twice with a different phone and area",
            "Uniqueness", "Critical",
            "Keep the most recent row per customer_id; log the conflict",
            df[df.customer_id.duplicated(keep=False)].sort_values("customer_id"), "customer_id")

    email_ok = df.email.str.match(r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$")
    col.add("customers", "email", "customers_bad_email",
            "Email is 'N/A', missing the @ or missing the domain suffix",
            "Validity", "Warning",
            "Load the customer, set email_valid = false, mask as usual",
            df[~email_ok], "customer_id")

    phone_ok = df.phone.str.match(r"^[6-9]\d{9}$")
    col.add("customers", "phone", "customers_bad_phone",
            "Phone is not a 10 digit Indian mobile number",
            "Validity", "Warning",
            "Load the customer, set phone_valid = false",
            df[~phone_ok], "customer_id")

    pin_ok = df.pincode.str.match(r"^\d{6}$")
    col.add("customers", "pincode", "customers_bad_pincode",
            "Pincode contains letters (5600XX)",
            "Validity", "Warning",
            "Load, flag; do not use for geo rollups",
            df[~pin_ok], "customer_id")

    col.add("customers", "city", "customers_missing_city",
            "City blank",
            "Completeness", "Warning",
            "Default to Bengaluru (single city operation) and flag",
            df[df.city.str.strip() == ""], "customer_id")

    col.add("customers", "loyalty_tier", "customers_tier_casing",
            "loyalty_tier casing variants (BRONZE, bronze, SILVER)",
            "Consistency", "Warning",
            "Title case before use so tiers group correctly",
            df[~df.loyalty_tier.str.strip().isin(VALID_TIER)], "customer_id")

    col.add("customers", "signup_date", "customers_future_signup",
            "signup_date in the future",
            "Timeliness", "Critical",
            "signup_date must be <= run date",
            df[signup > AS_OF], "customer_id")


def check_products(df: pd.DataFrame, col: IssueCollector) -> None:
    mrp, cost = num(df.mrp), num(df.cost_price)

    col.add("products", "product_id", "products_duplicate_id",
            "Same product_id twice, second row suffixed '(old)' with a different mrp",
            "Uniqueness", "Critical",
            "Keep the current row, drop the '(old)' version, or model as SCD2",
            df[df.product_id.duplicated(keep=False)].sort_values("product_id"), "product_id")

    col.add("products", "category", "products_missing_category",
            "Category blank, so margin by category loses these products",
            "Completeness", "Critical",
            "Category must be present; derive from sub_category if agreed with merchandising",
            df[df.category.str.strip() == ""], "product_id")

    col.add("products", "mrp", "products_negative_mrp",
            "mrp is negative",
            "Accuracy", "Critical",
            "mrp must be > 0",
            df[~(mrp > 0)], "product_id")

    col.add("products", "cost_price", "products_cost_above_mrp",
            "cost_price above mrp, which would make every sale loss making",
            "Accuracy", "Critical",
            "cost_price must be <= mrp",
            df[cost > mrp], "product_id")


def check_stores(df: pd.DataFrame, col: IssueCollector) -> None:
    col.add("stores", "zone", "stores_missing_zone",
            "Zone blank, so this store falls out of every zone rollup",
            "Completeness", "Critical",
            "Zone must be present; BTM Layout is South per the store master owner",
            df[df.zone.str.strip() == ""], "store_id")

    col.add("stores", "capacity_orders_per_day", "stores_zero_capacity",
            "capacity_orders_per_day is 0, which breaks any utilisation metric",
            "Accuracy", "Warning",
            "Capacity must be > 0; confirm the real figure with ops",
            df[num(df.capacity_orders_per_day) <= 0], "store_id")


def check_increment(df: pd.DataFrame, orders: pd.DataFrame, col: IssueCollector) -> None:
    overlap = df[df.order_id.isin(orders.order_id)]
    col.add("orders_2026-10-01_increment", "order_id", "increment_status_corrections",
            "Rows restate September orders with a new status (DELIVERED to RETURNED)",
            "Consistency", "Critical",
            "MERGE on order_id so status is updated, not appended",
            overlap, "order_id")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default="include/data/nammamart")
    parser.add_argument("--out", default="include/profiling/out")
    args = parser.parse_args()

    data_dir, out_dir = Path(args.data), Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    frames = load_raw(data_dir)
    (out_dir / "profile_summary.txt").write_text(profile_columns(frames))

    collector = IssueCollector(out_dir)
    masters = {"stores": frames["stores"], "products": frames["products"]}

    check_orders(frames["orders"], masters, collector, "orders")
    check_payments(frames["payments"], frames["orders"], collector)
    check_customers(frames["customers"], collector)
    check_products(frames["products"], collector)
    check_stores(frames["stores"], collector)
    check_increment(frames["orders_2026-10-01_increment"], frames["orders"], collector)

    issues = collector.to_frame()
    issues.to_csv(out_dir / "data_issues_log.csv", index=False)

    print(f"Profiled {len(frames)} files into {out_dir}/profile_summary.txt")
    print(f"{len(issues)} issue groups, {int(issues.rows_affected.sum())} offending rows")
    print(issues[["file", "rule_id", "severity", "rows_affected"]].to_string(index=False))


if __name__ == "__main__":
    main()
