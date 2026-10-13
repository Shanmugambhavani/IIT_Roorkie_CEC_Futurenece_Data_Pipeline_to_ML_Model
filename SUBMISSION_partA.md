# Part A: ETL Pipeline

`dags/nammamart_etl_bhavani.py`
Flow: Extract, Validate, Quarantine, Transform, Load, Reconcile, Report.

Business logic sits in `include/transforms.py`, `include/dq_checks.py` and
`include/masking.py` so it can be tested without a scheduler. The DAG orchestrates
only. Every number below was produced by running those functions over the real
dataset.

## A.1 Task graph

```
prepare_run
   ├── extract_orders ─────┐
   ├── extract_payments ───┤
   ├── extract_customers ──┤
   ├── extract_products ───┤   five parallel extracts
   ├── extract_stores ─────┤
   └── extract_increment ──┘
                 ↓
        validate_masters          repair dimensions
                 ↓
      validate_transactions       reject facts, write quarantine
                 ↓
            build_star            join and derive
                 ↓
          load_warehouse          upsert on order_id
                 ↓
            reconcile             etl_payment_reconciliation
                 ↓
          build_reports           zone revenue, category margin, top customers
                 ↓
         write_dq_results         append to dq_results
                 ↓
            run_audit             trigger_rule="all_done"
```

## A.2 The design decision worth defending

**Dimensions are repaired. Facts are rejected.**

The first version of this pipeline quarantined any dimension row that failed a
Critical rule. One store with a blank zone and ten imperfect product rows removed
**316 orders** from the fact table, because the foreign key check then failed for
every order pointing at them. Revenue disappearing because of a missing lookup
value is the exact failure NammaMart hired an FDE to remove.

So the rule changed:

| Row type | Policy | Why |
|---|---|---|
| Dimension (store, product, customer) | Repair and flag | A store with no zone is still a real store. Zone defaults to `Unknown`, category to `Uncategorised`, duplicates are deduplicated. Only a missing primary key is fatal |
| Fact (order, payment) | Reject and quarantine | An order with an impossible amount or a store that does not exist has no defensible value. It goes to quarantine with the rules it broke |

Products whose `cost_price` exceeds `mrp` keep their row but carry a
`cost_price_suspect` flag, and `etl_category_margin` reports
`revenue_on_suspect_cost` so finance can see how much margin rests on a figure
nobody trusts. 25,075.45 of September revenue sits on suspect costs, concentrated
in Staples.

## A.3 Verified results, September extract only

| Measure | Value |
|---|---|
| Orders in | 2,412 |
| Orders quarantined | 102 |
| Rows in `etl_fact_orders` | 2,310 |
| Net revenue | 11,35,628.80 |
| Gross margin | 2,60,999.43 |
| Delivered within 15 minutes | 28.2% |
| Payments quarantined | 29 of 2,228 |
| Reconciliation rows | 152 |

Null `category` and null `zone` in the fact table: **zero**. That is the dimension
repair policy working.

Revenue by category:

| Category | Net revenue | Gross margin | Margin % | Revenue on suspect cost |
|---|---|---|---|---|
| Staples | 2,66,500.90 | 58,728.22 | 22.0 | 16,195.55 |
| Personal Care | 2,05,016.80 | 44,444.98 | 21.7 | 3,511.90 |
| Household | 1,99,699.45 | 51,145.33 | 25.6 | 0.00 |
| Dairy & Bakery | 1,69,528.75 | 44,791.84 | 26.4 | 0.00 |
| Snacks & Beverages | 1,61,886.95 | 37,724.49 | 23.3 | 0.00 |
| Fruits & Vegetables | 93,284.30 | 19,600.62 | 21.0 | 5,368.00 |
| Uncategorised | 39,711.65 | 4,563.95 | 11.5 | 0.00 |

The Uncategorised row is the three products with a blank category. Under the first
design that was 39,711.65 of revenue silently deleted. Now it is visible and
chaseable, which is the point.

## A.4 Idempotency, criterion 5

`upsert_by_key` deletes the incoming `order_id` set, then inserts. Two consecutive
loads of identical data:

```
load 1: {'incoming': 2310, 'updated': 0,    'inserted': 2310, 'final_rows': 2310}
load 2: {'incoming': 2310, 'updated': 2310, 'inserted': 0,    'final_rows': 2310}
```

Row count unchanged. Revenue unchanged. The counts also tell you which happened,
so the Part F experiment 2 answer is a query, not an inspection.

## A.5 Incremental load, criterion 6

Loading `orders_2026-10-01_increment.csv` alongside the September file. The
increment is concatenated **first**, so when the duplicate rule keeps the first
occurrence of each `order_id`, the correction wins and the stale September row is
the one quarantined.

| Measure | Without increment | With increment |
|---|---|---|
| Rows read | 2,412 | 2,572 |
| Quarantined | 102 | 112 |
| Rows in fact | 2,310 | 2,460 |
| September net revenue | 11,35,628.80 | **11,31,435.10** |
| September RETURNED orders | 68 | **78** |

The 150 new orders were appended, the 10 corrected orders changed status without
duplicating, and September revenue fell by exactly **4,193.70**, which is the sum
of the ten orders that turned out to be returns.

The extra 10 quarantined rows are the superseded September versions, correctly
labelled `orders_duplicate_order_id`. They are not errors, they are history, and
they stay in the quarantine file as the audit trail for why September moved.

One boundary bug found during testing, worth recording. `AS_OF` was first set to
midnight on 1 October, which made the future-timestamp rule reject every order
placed that day, the entire 160 row increment. It is now end of day. A date
boundary that excludes the current business day is an easy way to quarantine
perfectly good data and see it only as a volume drop.

## A.6 Reconciliation, criterion 7

`etl_payment_reconciliation`, one row per mismatch with a reason:

| Reason | Rows | Meaning |
|---|---|---|
| MISSING_PAYMENT | 70 | Delivered order with no successful payment |
| PAYMENT_FOR_REJECTED_ORDER | 69 | Payment exists, but this pipeline quarantined the order |
| AMOUNT_MISMATCH | 13 | Successful payment differs from the order amount |

`PAYMENT_FOR_REJECTED_ORDER` is the reason finance will care about most. It is the
bridge between "the gateway says we collected this" and "our fact table does not
contain that order", which is the half day a week they currently spend.

## A.7 PII, criterion 8

`include/masking.py`. Applied in `standardise_customers`, which drops the raw
columns in the same step so no downstream task can carry an unmasked value
forward.

| Column | Example | Masked |
|---|---|---|
| email | priya.sharma@example.com | pr***@example.com |
| phone | 9876543221 | 98******21 |
| full_name | Priya Sharma | Priya S. |

`etl_dim_customer` and `etl_top_customers` contain no raw email, phone or full
name. `customer_id` remains the join key.

## A.8 Quarantine files

`include/quarantine/bhavani/<file>_failed_<run_id>.csv`, written per run, with a
`failure_reason` column naming every rule the row broke:

```
ORD000142, orders_discount_out_of_range; orders_amount_formula_mismatch
```

Run ids contain colons, which Windows rejects in filenames, so the DAG sanitises
the run id before building the path.

## A.9 Assumptions recorded

1. **Negative quantities are data entry errors, not refunds.** Six orders carry a
   negative quantity and a matching negative SUCCESS payment. They are quarantined
   under `orders_quantity_not_positive`. If ops confirms they are refunds, the rule
   changes and September revenue moves. This is question 1 for the client.
2. **The second copy of a duplicated product is the stale one.** The "(old)" rows
   sit after the current rows in the export, so keeping the first keeps the live
   price. This relies on file ordering, which the export does not guarantee.
3. **For duplicated customers, the last row wins.** The export has no `updated_at`,
   so any choice is a guess. The conflict is logged.
4. **Blank zone defaults to `Unknown` rather than being inferred.** S07 is BTM
   Layout, which is plainly South, but inventing the value hides a master data gap
   the store master owner should close.
