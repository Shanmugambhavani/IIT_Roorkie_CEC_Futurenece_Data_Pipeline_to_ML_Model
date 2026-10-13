"""
Assemble the final SUBMISSION.md from the individual part files.

    py -3.12 include/assemble_submission.py

Reads whichever SUBMISSION_part*.md files exist in the repo root, stitches them
together in rubric order with a header and contents block, and writes
SUBMISSION.md. The old SUBMISSION.md is kept as SUBMISSION_part0.md the first time
it runs, because it holds the Part 0 profiling section.

Re-runnable. It never reads its own output, so running it twice does not nest.
"""

from __future__ import annotations

import shutil
from pathlib import Path

ROOT = Path(".")
OUT = ROOT / "SUBMISSION.md"

# Rubric order, not alphabetical.
ORDER = [
    ("SUBMISSION_part0.md", "Part 0: Discovery and Data Profiling"),
    ("SUBMISSION_partA.md", "Part A: ETL Pipeline"),
    ("SUBMISSION_partB.md", "Part B: ELT Pipeline"),
    ("SUBMISSION_partC.md", "Part C: Scheduling"),
    ("SUBMISSION_partD.md", "Part D: Data Quality Dimensions"),
    ("SUBMISSION_partE.md", "Part E: Data Governance"),
    ("SUBMISSION_partF.md", "Part F: Observability Tests"),
    ("SUBMISSION_partG.md", "Part G: Recommendation to the Client"),
]

HEADER = """# NammaMart Quick Commerce: ETL and ELT Pipelines

FDAIE Module 2, Data Engineering Foundations
Submitted by **Bhavani**

Repository: https://github.com/Shanmugambhavani/IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model

## Headline

The current manual morning report overstates September revenue by **83,392, about
6.8%**. Reported 12,19,020.88 against 11,35,628.80 after quality rules. 102 of
2,412 order rows fail at least one critical rule.

Both pipelines were built, run and reconciled. They agree on every order:
2,460 fact rows, 12,11,087.30 total, 11,31,435.10 for September, with zero
differences on a row-by-row join.

## Contents

"""

PART_D_FALLBACK = """# Part D: Data Quality Dimensions

Rules live in `include/dq_checks.py` as one registry, imported directly by the ETL
DAG and loaded into `check_catalog` by the ELT DAG. Both pipelines therefore use
identical `check_name`, dimension and severity values, which is what makes the
Part B4 comparison meaningful rather than coincidental.

**43 rules across the five files.** Severity decides the fate of the row:
Critical rows are quarantined and never reach a fact or mart table; Warning rows
load, standardised, with the issue recorded.

| Dimension | Example checks | Severity |
|---|---|---|
| Completeness | `orders_missing_customer_id`, `orders_delivered_without_minutes`, `products_missing_category`, `stores_missing_zone` | Critical for facts, Warning for repairable dimensions |
| Uniqueness | `orders_duplicate_order_id`, `payments_duplicate_payment_id`, `customers_duplicate_id`, `products_duplicate_id` | Critical for facts, Warning for dimensions |
| Validity | `orders_invalid_status`, `orders_discount_out_of_range`, `orders_unparseable_order_ts`, `customers_bad_email`, `customers_bad_phone`, `customers_bad_pincode` | Critical for status and timestamp, Warning for format |
| Accuracy | `orders_quantity_not_positive`, `orders_amount_formula_mismatch`, `orders_delivery_over_limit`, `products_non_positive_mrp`, `products_cost_above_mrp` | Critical for facts, Warning for dimensions |
| Consistency | `orders_store_fk_missing`, `orders_product_fk_missing`, `payments_orphan_order_id`, `orders_payment_mode_casing`, `customers_tier_casing` | Critical for keys, Warning for casing |
| Timeliness | `orders_future_order_ts`, `payments_future_payment_ts`, `customers_future_signup`, `orders_freshness` | Critical for future dates, Warning for staleness |

Two extra dimensions beyond the six required, because Part F needs them:
**Volume** (`orders_volume_drop` and siblings) and **Schema** (recorded when a
required column is missing rather than crashing the task).

## Every check, every run

`dq_results` is appended on every run of both DAGs, with the columns the brief
specifies: `run_id`, `dag_id`, `check_name`, `dimension`, `severity`,
`rows_checked`, `rows_failed`, `status`, `checked_at`.

```sql
SELECT dag_id, check_name, dimension, severity, rows_checked, rows_failed, status
FROM dq_results
WHERE run_id = (SELECT max(run_id) FROM dq_results)
ORDER BY severity, rows_failed DESC;
```

47 rows per ETL run, 40 per ELT run. The seven-row difference is the dataset-level
volume and freshness checks, which have no staging column for the ELT pipeline to
reconstruct them from; explained in Part B.5.

Checks that pass are recorded too, with `rows_failed = 0`. A rule that silently
stops running looks identical to a rule that passes, unless the zeroes are stored.

## Per-row failure reasons

Each rule is evaluated separately and its name appended to that row's
`failure_reason`, so a quarantined row states exactly what it broke:

```
ORD000142, orders_discount_out_of_range; orders_amount_formula_mismatch
```

This is the difference from a single combined mask, and it is what acceptance
criterion 4 requires. Verified in Part F experiment 5, where a deliberately bad row
fired two rules at once.
"""


def main() -> None:
    # Preserve the existing Part 0 content on first run.
    part0 = ROOT / "SUBMISSION_part0.md"
    if OUT.exists() and not part0.exists():
        shutil.copy2(OUT, part0)
        print(f"preserved existing SUBMISSION.md as {part0.name}")

    # Write a Part D section if one was never created.
    partd = ROOT / "SUBMISSION_partD.md"
    if not partd.exists():
        partd.write_text(PART_D_FALLBACK, encoding="utf-8")
        print(f"created {partd.name}")

    sections, contents, missing = [], [], []
    for filename, title in ORDER:
        path = ROOT / filename
        if not path.exists():
            missing.append(filename)
            continue
        body = path.read_text(encoding="utf-8").strip()
        anchor = title.lower().replace(":", "").replace(" ", "-").replace(",", "")
        contents.append(f"- [{title}](#{anchor})")
        sections.append(body)
        print(f"added {filename}")

    document = HEADER + "\n".join(contents) + "\n\n---\n\n" + "\n\n---\n\n".join(sections) + "\n"
    OUT.write_text(document, encoding="utf-8")

    words = len(document.split())
    print(f"\nwrote SUBMISSION.md: {len(sections)} sections, about {words:,} words")
    if missing:
        print("missing (skipped):", ", ".join(missing))
    print("\nNext: check it renders, then delete the SUBMISSION_part*.md files:")
    print("  del SUBMISSION_part*.md")
    print("Then commit and push.")


if __name__ == "__main__":
    main()
