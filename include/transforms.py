"""
NammaMart Part A: transformations.

Pure pandas functions, no Airflow imports, so they can be unit tested without a
scheduler. The DAG orchestrates these; it does not contain business logic itself.

Mirrored by SQL in include/sql/ for the ELT pipeline. Where a rule changes here,
it changes there too, and the Part B4 totals comparison is how you find out if it
did not.
"""

from __future__ import annotations

import pandas as pd

from dq_checks import (
    canonical_payment_mode,
    canonical_status,
    canonical_tier,
    to_num,
    to_ts,
)
from masking import mask_customer_frame

WITHIN_SLA_MINUTES = 15


# --------------------------------------------------------------------------
# Standardisation
# --------------------------------------------------------------------------

def standardise_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Trim, fix casing, map payment mode variants, cast to real types."""
    out = df.copy()
    out["order_id"] = out["order_id"].str.strip()
    out["customer_id"] = out["customer_id"].str.strip()
    out["store_id"] = out["store_id"].str.strip()
    out["product_id"] = out["product_id"].str.strip()
    out["order_status"] = canonical_status(out["order_status"])
    out["payment_mode"] = canonical_payment_mode(out["payment_mode"])
    out["channel"] = out["channel"].str.strip().str.title()

    out["order_ts"] = to_ts(out["order_ts"])
    out["order_date"] = out["order_ts"].dt.date
    out["quantity"] = to_num(out["quantity"]).astype("Int64")
    out["unit_price"] = to_num(out["unit_price"])
    out["discount_pct"] = to_num(out["discount_pct"])
    out["order_amount"] = to_num(out["order_amount"]).round(2)
    out["delivery_minutes"] = to_num(out["delivery_minutes"])
    return out


def standardise_payments(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["payment_id"] = out["payment_id"].str.strip()
    out["order_id"] = out["order_id"].str.strip()
    out["payment_status"] = canonical_status(out["payment_status"])
    out["payment_mode"] = canonical_payment_mode(out["payment_mode"])
    out["payment_ts"] = to_ts(out["payment_ts"])
    out["amount_paid"] = to_num(out["amount_paid"]).round(2)
    out["gateway_ref"] = out["gateway_ref"].str.strip()
    return out


def standardise_customers(df: pd.DataFrame) -> pd.DataFrame:
    """Standardise, deduplicate, then mask. Masking last so dedupe can use the raw key."""
    out = df.copy()
    out["customer_id"] = out["customer_id"].str.strip()
    out["loyalty_tier"] = canonical_tier(out["loyalty_tier"])
    out["city"] = out["city"].str.strip().replace("", "Bengaluru")
    out["area"] = out["area"].str.strip()
    out["signup_date"] = to_ts(out["signup_date"]).dt.date
    out["is_active"] = out["is_active"].str.strip().str.upper().isin(["TRUE", "1", "YES"])

    # Conflicting duplicates: keep the last row seen. Recorded as an assumption in
    # SUBMISSION.md because the export carries no updated_at to decide properly.
    out = out.drop_duplicates(subset="customer_id", keep="last")

    return mask_customer_frame(out)


def standardise_products(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["product_id"] = out["product_id"].str.strip()
    out["product_name"] = out["product_name"].str.strip()
    out["category"] = out["category"].str.strip().str.title().replace("", "Uncategorised")
    out["sub_category"] = out["sub_category"].str.strip().str.title()
    out["brand"] = out["brand"].str.strip()
    out["mrp"] = to_num(out["mrp"])
    out["cost_price"] = to_num(out["cost_price"])
    out["launch_date"] = to_ts(out["launch_date"]).dt.date
    out["is_perishable"] = out["is_perishable"].str.strip().str.upper().isin(["TRUE", "1", "YES"])

    # Repair rather than reject: a product with a bad cost price is still a real
    # product, and dropping it would delete real revenue from the fact table.
    # The flag travels with the row so margin can be qualified downstream.
    out["cost_price_suspect"] = (out["cost_price"] > out["mrp"]) | ~(out["mrp"] > 0)

    # The second copy of a duplicated product is the "(old)" version, which sits
    # after the current row in the export. Keeping the first keeps the live price.
    return out.drop_duplicates(subset="product_id", keep="first")


def standardise_stores(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["store_id"] = out["store_id"].str.strip()
    out["store_name"] = out["store_name"].str.strip()
    out["zone"] = out["zone"].str.strip().str.title().replace("", "Unknown")
    out["area"] = out["area"].str.strip()
    out["manager_name"] = out["manager_name"].str.strip()
    out["open_date"] = to_ts(out["open_date"]).dt.date
    out["capacity_orders_per_day"] = to_num(out["capacity_orders_per_day"]).astype("Int64")
    return out.drop_duplicates(subset="store_id", keep="first")


# --------------------------------------------------------------------------
# Fact build
# --------------------------------------------------------------------------

def build_fact_orders(
    orders: pd.DataFrame,
    customers: pd.DataFrame,
    products: pd.DataFrame,
    stores: pd.DataFrame,
) -> pd.DataFrame:
    """
    Join the clean orders to the three dimensions and derive the measures the
    six business questions need.

    Only DELIVERED orders carry revenue, per the client's rule, so net_revenue
    and gross_margin are zero for CANCELLED and RETURNED. The rows stay in the
    fact table so cancellation and return rates remain answerable.
    """
    fact = (
        orders
        .merge(
            products[["product_id", "product_name", "category", "sub_category", "brand",
                      "cost_price", "mrp", "cost_price_suspect"]],
            on="product_id", how="left", validate="many_to_one",
        )
        .merge(
            stores[["store_id", "store_name", "zone", "area"]].rename(columns={"area": "store_area"}),
            on="store_id", how="left", validate="many_to_one",
        )
        .merge(
            customers[["customer_id", "loyalty_tier", "city", "area"]].rename(columns={"area": "customer_area"}),
            on="customer_id", how="left", validate="many_to_one",
        )
    )

    is_delivered = fact["order_status"] == "DELIVERED"

    fact["net_revenue"] = fact["order_amount"].where(is_delivered, 0.0).round(2)
    fact["cogs"] = (fact["quantity"].astype("float") * fact["cost_price"]).where(is_delivered, 0.0).round(2)
    fact["gross_margin"] = (fact["net_revenue"] - fact["cogs"]).round(2)
    fact["within_15_min"] = (is_delivered & (fact["delivery_minutes"] <= WITHIN_SLA_MINUTES))
    fact["is_delivered"] = is_delivered

    columns = [
        "order_id", "order_ts", "order_date", "customer_id", "store_id", "product_id",
        "quantity", "unit_price", "discount_pct", "order_amount", "payment_mode",
        "order_status", "channel", "delivery_minutes",
        "product_name", "category", "sub_category", "brand", "cost_price", "mrp",
        "store_name", "zone", "store_area", "cost_price_suspect",
        "loyalty_tier", "city", "customer_area",
        "net_revenue", "cogs", "gross_margin", "within_15_min", "is_delivered",
    ]
    return fact[[c for c in columns if c in fact.columns]]


# --------------------------------------------------------------------------
# Reconciliation (acceptance criterion 7)
# --------------------------------------------------------------------------

def reconcile_payments(
    fact: pd.DataFrame,
    payments: pd.DataFrame,
    quarantined_order_ids: set[str] | None = None,
) -> pd.DataFrame:
    """
    One row per mismatch, each with a reason finance can act on.

    Reasons:
        MISSING_PAYMENT              delivered order with no successful payment
        PAYMENT_WITHOUT_ORDER        payment whose order_id is not in the fact table
        AMOUNT_MISMATCH              amounts differ by more than a paisa
        PAYMENT_FOR_NON_DELIVERED    successful payment against a cancelled or returned order
        PAYMENT_FOR_REJECTED_ORDER   payment against an order this pipeline quarantined
        MODE_MISMATCH                payment mode differs between the two systems
    """
    quarantined_order_ids = quarantined_order_ids or set()
    success = payments[payments["payment_status"] == "SUCCESS"].copy()

    joined = fact[
        ["order_id", "order_amount", "order_status", "payment_mode", "store_id", "order_date"]
    ].merge(
        success[["payment_id", "order_id", "amount_paid", "payment_mode", "payment_ts"]],
        on="order_id", how="outer", suffixes=("_order", "_payment"), indicator=True,
    )

    rows = []

    missing = joined[(joined["_merge"] == "left_only") & (joined["order_status"] == "DELIVERED")]
    for _, r in missing.iterrows():
        rows.append(_recon_row(r, "MISSING_PAYMENT", r["order_amount"], 0.0))

    orphan = joined[joined["_merge"] == "right_only"]
    for _, r in orphan.iterrows():
        reason = (
            "PAYMENT_FOR_REJECTED_ORDER"
            if r["order_id"] in quarantined_order_ids
            else "PAYMENT_WITHOUT_ORDER"
        )
        rows.append(_recon_row(r, reason, 0.0, r["amount_paid"]))

    both = joined[joined["_merge"] == "both"].copy()

    non_delivered = both[both["order_status"] != "DELIVERED"]
    for _, r in non_delivered.iterrows():
        rows.append(_recon_row(r, "PAYMENT_FOR_NON_DELIVERED", r["order_amount"], r["amount_paid"]))

    delivered = both[both["order_status"] == "DELIVERED"]
    gap = (delivered["amount_paid"] - delivered["order_amount"]).abs() > 0.011
    for _, r in delivered[gap].iterrows():
        rows.append(_recon_row(r, "AMOUNT_MISMATCH", r["order_amount"], r["amount_paid"]))

    mode_gap = delivered["payment_mode_order"].fillna("") != delivered["payment_mode_payment"].fillna("")
    for _, r in delivered[mode_gap & ~gap].iterrows():
        rows.append(_recon_row(r, "MODE_MISMATCH", r["order_amount"], r["amount_paid"]))

    if not rows:
        return pd.DataFrame(
            columns=["order_id", "payment_id", "order_date", "store_id",
                     "order_amount", "amount_paid", "difference", "reason"]
        )
    return pd.DataFrame(rows).sort_values(["reason", "order_id"]).reset_index(drop=True)


def _recon_row(r: pd.Series, reason: str, order_amount, amount_paid) -> dict:
    order_amount = 0.0 if pd.isna(order_amount) else float(order_amount)
    amount_paid = 0.0 if pd.isna(amount_paid) else float(amount_paid)
    return {
        "order_id": r.get("order_id"),
        "payment_id": None if pd.isna(r.get("payment_id")) else r.get("payment_id"),
        "order_date": r.get("order_date"),
        "store_id": r.get("store_id"),
        "order_amount": round(order_amount, 2),
        "amount_paid": round(amount_paid, 2),
        "difference": round(amount_paid - order_amount, 2),
        "reason": reason,
    }


# --------------------------------------------------------------------------
# Reporting tables (business questions 1 and 2)
# --------------------------------------------------------------------------

def build_daily_zone_revenue(fact: pd.DataFrame) -> pd.DataFrame:
    delivered = fact[fact["is_delivered"]]
    out = (
        delivered
        .groupby(["order_date", "zone", "store_id", "store_name"], dropna=False)
        .agg(
            orders=("order_id", "nunique"),
            net_revenue=("net_revenue", "sum"),
            avg_delivery_minutes=("delivery_minutes", "mean"),
            within_15_min=("within_15_min", "sum"),
        )
        .reset_index()
    )
    out["pct_within_15_min"] = (out["within_15_min"] / out["orders"] * 100).round(1)
    out["net_revenue"] = out["net_revenue"].round(2)
    out["avg_delivery_minutes"] = out["avg_delivery_minutes"].round(1)
    return out.sort_values(["order_date", "zone", "store_id"]).reset_index(drop=True)


def build_category_margin(fact: pd.DataFrame) -> pd.DataFrame:
    delivered = fact[fact["is_delivered"]]
    out = (
        delivered
        .groupby(["category"], dropna=False)
        .agg(
            orders=("order_id", "nunique"),
            units=("quantity", "sum"),
            net_revenue=("net_revenue", "sum"),
            cogs=("cogs", "sum"),
            gross_margin=("gross_margin", "sum"),
            revenue_on_suspect_cost=("net_revenue", lambda s: s[delivered.loc[s.index, "cost_price_suspect"]].sum()),
        )
        .reset_index()
    )
    out["revenue_on_suspect_cost"] = out["revenue_on_suspect_cost"].round(2)
    out["margin_pct"] = (out["gross_margin"] / out["net_revenue"] * 100).round(1)
    for col in ("net_revenue", "cogs", "gross_margin"):
        out[col] = out[col].round(2)
    return out.sort_values("net_revenue", ascending=False).reset_index(drop=True)


def build_top_customers(fact: pd.DataFrame, customers: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    """Business question 5. No raw email or phone ever reaches this table."""
    delivered = fact[fact["is_delivered"]]
    agg = (
        delivered
        .groupby("customer_id", dropna=False)
        .agg(orders=("order_id", "nunique"), total_spend=("net_revenue", "sum"))
        .reset_index()
    )
    agg["total_spend"] = agg["total_spend"].round(2)
    cols = [c for c in ("customer_id", "customer_name_masked", "loyalty_tier", "city", "email_masked")
            if c in customers.columns]
    out = agg.merge(customers[cols], on="customer_id", how="left")
    return out.sort_values("total_spend", ascending=False).head(n).reset_index(drop=True)
