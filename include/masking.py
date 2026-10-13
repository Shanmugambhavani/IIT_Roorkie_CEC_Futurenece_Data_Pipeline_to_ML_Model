"""
NammaMart: PII masking.

Acceptance criterion 8 requires email and phone to be masked in every reporting
table. Masking rather than deleting, because support still needs to confirm they
are talking to the right customer, and finance needs a stable key to join on.

The customer_id is the join key everywhere downstream, so the masked values are
display only.

Mirrored in SQL in include/sql/stg_customers.sql so ETL and ELT produce the same
strings. If you change a rule here, change it there too.
"""

from __future__ import annotations

import pandas as pd


def mask_email(value) -> str:
    """
    priya.sharma@example.com  ->  pr***@example.com
    Keeps the first two characters and the full domain, so a support agent can
    verify an address a customer reads out without the table exposing it.
    """
    text = "" if value is None else str(value).strip()
    if not text or "@" not in text:
        return ""
    local, _, domain = text.partition("@")
    if len(local) <= 2:
        return f"{local}***@{domain}"
    return f"{local[:2]}***@{domain}"


def mask_phone(value) -> str:
    """
    9876543221  ->  98******21
    First two and last two digits kept. Enough to confirm a number, not enough
    to dial it.
    """
    text = "" if value is None else str(value).strip()
    digits = "".join(c for c in text if c.isdigit())
    if len(digits) < 4:
        return "*" * len(digits)
    return f"{digits[:2]}{'*' * (len(digits) - 4)}{digits[-2:]}"


def mask_name(value) -> str:
    """
    Priya Sharma  ->  Priya S.
    Used on the top customers report, where a first name makes the list readable
    without publishing the full identity.
    """
    text = "" if value is None else str(value).strip()
    if not text:
        return ""
    parts = text.split()
    if len(parts) == 1:
        return parts[0]
    return f"{parts[0]} {parts[-1][0]}."


def mask_customer_frame(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply every mask and drop the raw columns in one step, so no downstream task
    can accidentally carry an unmasked value forward.
    """
    out = df.copy()
    if "email" in out.columns:
        out["email_masked"] = out["email"].map(mask_email)
    if "phone" in out.columns:
        out["phone_masked"] = out["phone"].map(mask_phone)
    if "full_name" in out.columns:
        out["customer_name_masked"] = out["full_name"].map(mask_name)

    return out.drop(columns=[c for c in ("email", "phone", "full_name") if c in out.columns])


PII_COLUMNS = ["full_name", "email", "phone"]
QUASI_IDENTIFIER_COLUMNS = ["area", "pincode", "signup_date"]
