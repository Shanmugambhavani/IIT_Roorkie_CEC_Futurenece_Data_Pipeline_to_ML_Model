# NammaMart Quick Commerce: ETL and ELT Pipelines

FDAIE Module 2, Data Engineering Foundations
Submitted by **Bhavani**
Repository owner: Shanmugambhavani. Commits authored from mayankmadhur25, a second personal GitHub account. Both belong to me
Repository: https://github.com/Shanmugambhavani/IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model

## Headline

The current manual morning report overstates September revenue by **83,392, about
6.8%**. Reported 12,19,020.88 against 11,35,628.80 after quality rules. 102 of
2,412 order rows fail at least one critical rule.

Both pipelines were built, run and reconciled. They agree on every order:
2,460 fact rows, 12,11,087.30 total, 11,31,435.10 for September, with zero
differences on a row-by-row join.

## Contents

- [Part 0: Discovery and Data Profiling](#part-0-discovery-and-data-profiling)
- [Part A: ETL Pipeline](#part-a-etl-pipeline)
- [Part B: ELT Pipeline](#part-b-elt-pipeline)
- [Part C: Scheduling](#part-c-scheduling)
- [Part D: Data Quality Dimensions](#part-d-data-quality-dimensions)
- [Part E: Data Governance](#part-e-data-governance)
- [Part F: Observability Tests](#part-f-observability-tests)
- [Part G: Recommendation to the Client](#part-g-recommendation-to-the-client)

---

# Part 0: Discovery and Data Profiling

NammaMart Quick Commerce, FDAIE Module 2.
All numbers below were produced by `include/profiling/profile_nammamart.py`, which reads
every CSV as raw text (`dtype=str`, no NA coercion) so nothing is silently cleaned during
discovery. Re-run it any time with:

```bash
python include/profiling/profile_nammamart.py \
    --data include/data/nammamart \
    --out  include/profiling/out
```

## 0.1 Headline for the client

| Finding | Number |
|---|---|
| Rows across the six files | 5,372 |
| Issue groups found | 34, across all six files |
| Orders rows that fail at least one Critical rule | **102 of 2,412 (4.2%)** |
| September revenue as reported today (all DELIVERED rows) | 12,19,020.88 |
| September revenue after Critical rules are applied | **11,35,628.80** |
| Overstatement in the current morning report | **83,392.08, about 6.8%** |
| SUCCESS payments that do not tie to a clean delivered order | 84 payments |
| Clean delivered orders with no matching payment | 70 orders |

That 6.8% gap is the single sentence to open the kickoff with. It is not a rounding
problem, it is duplicate orders, hours logged as minutes, impossible discounts and
orders pointing at stores that do not exist.

## 0.2 File profile

| File | Rows | Key | Distinct keys | Duplicate keys | Columns with blanks |
|---|---|---|---|---|---|
| orders.csv | 2,412 | order_id | 2,400 | 12 (exact duplicate rows) | order_ts (1), customer_id (18), delivery_minutes (164) |
| payments.csv | 2,228 | payment_id | 2,224 | 4 (exact duplicate rows) | gateway_ref (301) |
| customers.csv | 405 | customer_id | 400 | 5 (conflicting rows) | city (6) |
| products.csv | 152 | product_id | 150 | 2 ("(old)" versions) | category (3) |
| stores.csv | 15 | store_id | 15 | 0 | zone (1) |
| increment | 160 | order_id | 160 | 0 (but 10 restate September orders) | none |

Numeric and date ranges that matter:

| Column | Min | Max | Comment |
|---|---|---|---|
| orders.quantity | -3 | 5 | 6 negative, 4 zero |
| orders.discount_pct | 0 | 250 | 3 rows above 100 |
| orders.order_amount | -1,593.00 | 9,576.00 | negatives follow the negative quantities |
| orders.delivery_minutes | 8 | 1,440 | 6 rows at 480 / 900 / 1,440 |
| orders.order_ts | 2026-09-01 06:30 | 2027-01-01 09:00 | 7 future rows, 6 unparseable |
| payments.amount_paid | -1,593.00 | 9,576.00 | 6 negative, 4 zero, all flagged SUCCESS |
| customers.signup_date | 2024-06-01 | 2027-01-05 | 3 future |
| products.mrp | -203.00 | 883.00 | 1 negative |
| stores.capacity_orders_per_day | 0 | 600 | S12 Rajajinagar has capacity 0 |

Category columns, with the dirt visible:

| Column | Clean values | Variants found |
|---|---|---|
| orders.order_status | DELIVERED 2,182 / CANCELLED 156 / RETURNED 69 | `Pending??` (3), `delivered` (2) |
| orders.payment_mode | UPI 1,329 / Card 493 / COD 326 / Wallet 254 | `upi` (4), `UPI ` trailing space (2), `Cashh` (2), `CARD` (1), `cod` (1) |
| payments.payment_status | SUCCESS 2,116 / REFUNDED 68 / FAILED 44 | none |
| customers.loyalty_tier | Bronze / Silver / Gold / Platinum | `BRONZE` (3), `bronze` (3), `SILVER` (2) |
| products.category | 6 categories | 3 blanks |
| stores.zone | North / South / East / West | 1 blank (S07 BTM Layout) |

## 0.3 Referential integrity

| Relationship | Result |
|---|---|
| orders.customer_id → customers.customer_id | 18 rows blank. No invalid non-blank IDs. |
| orders.product_id → products.product_id | 9 rows point at P92xx–P99xx ranges that do not exist |
| orders.store_id → stores.store_id | 5 rows point at S00, S17, S99 (18 distinct store_ids seen for 15 stores) |
| payments.order_id → orders.order_id | 8 payments reference ORD94xxxx–ORD99xxxx, no such orders |
| Coverage: DELIVERED orders with no payment | 27 rows |
| Coverage: CANCELLED orders with a payment | 0 rows, so that rule holds |
| increment → masters | Clean on all three keys |

## 0.4 Business rule verification

| Rule from Section 3 | Outcome |
|---|---|
| `order_amount = quantity x unit_price x (1 - discount_pct/100)` | **18 rows fail.** Two patterns: the discount is ignored and the figure is inflated 10x (ORD001624 shows 9,576.00 against an expected 957.60), and the three discount values above 100 produce a negative expected amount. |
| Valid `order_status` | 5 rows fail. `delivered` is a casing fix, `Pending??` is a genuinely unknown state. |
| Valid `payment_mode` | 11 rows in orders, 11 in payments, all casing or spelling. Recoverable by lookup. |
| Delivery 8 to 35 minutes, DELIVERED must have a value | 6 rows at 480/900/1440 minutes. 8 DELIVERED rows have no value at all. No row is below 8. |
| SUCCESS payment equals its order amount | 14 mismatches. 10 look like a flat 25, 50 or 100 short-payment; 4 are the negative-amount rows mirroring negative orders. |
| Increment carries corrections | Confirmed: 150 genuinely new orders dated 2026-10-01, plus 10 September orders flipping DELIVERED to RETURNED. All 10 are currently counted as revenue. |

Daily order volume is stable at 60 to 108 per day across 1 to 30 September, so there is
no legitimate volume cliff to explain away. The only out-of-range dates are the 7 future
rows (two on 21 Oct, two on 2 Nov, three on 1 Jan 2027).

## 0.5 Data Issues Log

Severity rule: **Critical** rows are quarantined and never reach the fact table.
**Warning** rows load, standardised, with a flag so the issue stays visible.
The machine readable version with example row keys is `include/profiling/out/data_issues_log.csv`,
and the exact offending rows per rule are in `include/profiling/out/issue_rows/`.

### orders.csv

| # | Column | Problem | Dimension | Rows | Proposed rule | Severity |
|---|---|---|---|---|---|---|
| 1 | order_id | 12 order_ids appear twice as byte identical rows (ORD000539, ORD000783, ORD001110 and 9 more) | Uniqueness | 24 | Keep first row per order_id | Critical |
| 2 | customer_id | Blank customer_id | Completeness | 18 | Must be present | Critical |
| 3 | order_ts | `N/A`, blank, `2026-13-05 11:00:00`, `31/09/2026 10:00` | Validity | 6 | Must parse as a timestamp | Critical |
| 4 | order_ts | Dated 21 Oct, 2 Nov, 1 Jan 2027 | Timeliness | 7 | Must be on or before the run date | Critical |
| 5 | order_status | `Pending??` | Validity | 3 | Must be in the allowed list after upper-casing | Critical |
| 6 | order_status | `delivered` lowercase | Consistency | 2 | Trim and upper-case before validating | Warning |
| 7 | payment_mode | `upi`, `UPI `, `CARD`, `cod`, `Cashh` | Consistency | 8 | Lookup map to the canonical four | Warning |
| 8 | quantity | 6 negative, 4 zero | Accuracy | 10 | Must be greater than zero | Critical |
| 9 | discount_pct | 120, 150, 250 | Validity | 3 | Must be between 0 and 100 | Critical |
| 10 | order_amount | Does not match the formula | Accuracy | 18 | Recompute, compare to 2 decimals | Critical |
| 11 | delivery_minutes | DELIVERED with no value | Completeness | 8 | DELIVERED implies a delivery time | Critical |
| 12 | delivery_minutes | 480, 900, 1440 | Accuracy | 6 | Must be at or under 120 | Critical |
| 13 | store_id | S00, S17, S99 | Consistency | 5 | Must exist in stores.csv | Critical |
| 14 | product_id | P9212, P9399 and 7 more | Consistency | 9 | Must exist in products.csv | Critical |

102 distinct rows are caught, 3 of them by more than one rule.

### payments.csv

| # | Column | Problem | Dimension | Rows | Proposed rule | Severity |
|---|---|---|---|---|---|---|
| 15 | payment_id | 4 payment_ids duplicated, same gateway_ref, same amount | Uniqueness | 8 | Deduplicate before reconciling, or revenue double counts | Critical |
| 16 | order_id | 8 payments for orders that do not exist | Consistency | 8 | Route to reconciliation as PAYMENT_WITHOUT_ORDER | Critical |
| 17 | amount_paid | SUCCESS with amount 0 or negative | Accuracy | 10 | A SUCCESS payment must be positive | Critical |
| 18 | amount_paid | SUCCESS amount differs from the order amount | Consistency | 14 | Reconciliation row, reason AMOUNT_MISMATCH | Critical |
| 19 | gateway_ref | Blank, but **all 301 are Cash on Delivery**, where no gateway reference exists | Completeness | 0 after scoping | Rule scoped to non cash modes only, which then passes cleanly. A blanket not-null rule here would have raised 301 false alarms | Warning |
| 20 | payment_mode | Same casing variants as orders | Consistency | 8 | Lookup map | Warning |

### customers.csv

| # | Column | Problem | Dimension | Rows | Proposed rule | Severity |
|---|---|---|---|---|---|---|
| 21 | customer_id | C00036, C00103, C00226, C00252, C00313 appear twice with different phone and area | Uniqueness | 10 | Keep one row per customer, log the conflict | Critical |
| 22 | signup_date | 2026-11-12, 2026-12-20, 2027-01-05 | Timeliness | 3 | Must be on or before the run date | Critical |
| 23 | email | `N/A` (4), missing `@` (5), missing `.com` (3) | Validity | 12 | Load, set email_valid false, still mask | Warning |
| 24 | phone | 8 or 9 digits instead of 10 | Validity | 7 | Load, set phone_valid false | Warning |
| 25 | pincode | `5600XX` | Validity | 3 | Load, exclude from geo rollups | Warning |
| 26 | city | Blank | Completeness | 6 | Default to Bengaluru, flag | Warning |
| 27 | loyalty_tier | `BRONZE`, `bronze`, `SILVER` | Consistency | 8 | Title case before grouping | Warning |

### products.csv

| # | Column | Problem | Dimension | Rows | Proposed rule | Severity |
|---|---|---|---|---|---|---|
| 28 | product_id | P0030 and P0140 appear twice, second row named "(old)" with a lower mrp | Uniqueness | 4 | Keep the current row, or model as SCD2 | Critical |
| 29 | category | Blank for P0051, P0060, P0068 | Completeness | 3 | Must be present, or category margin loses these | Critical |
| 30 | mrp | P0109 has mrp -203.00 | Accuracy | 1 | Must be positive | Critical |
| 31 | cost_price | P0019, P0073, P0074, P0109, P0117 have cost above mrp | Accuracy | 5 | cost_price must be at or below mrp, or margin goes negative | Critical |

P0068 matters more than it looks: it is a 649.00 product that appears in high value
orders, so the blank category silently drops real revenue out of the category margin view.

### stores.csv

| # | Column | Problem | Dimension | Rows | Proposed rule | Severity |
|---|---|---|---|---|---|---|
| 32 | zone | S07 BTM Layout has a blank zone | Completeness | 1 | Zone must be present, or one store vanishes from every zone rollup | Critical |
| 33 | capacity_orders_per_day | S12 Rajajinagar has capacity 0 | Accuracy | 1 | Must be positive, breaks utilisation and the bonus capacity check | Warning |

### orders_2026-10-01_increment.csv

| # | Column | Problem | Dimension | Rows | Proposed rule | Severity |
|---|---|---|---|---|---|---|
| 34 | order_id | 10 rows restate September orders with status RETURNED | Consistency | 10 | MERGE on order_id, never append, or September revenue double counts | Critical |

The 10 corrections total 4,193.70 of September revenue that must come out after the
incremental load. The file itself is clean on every other rule, which is a useful
contrast to make to the client: the daily feed is fine, the historical extract is not.

## 0.6 Questions for the client before building

Ranked by how much the answer changes the pipeline.

1. **Is a negative quantity a return, or a data entry error?** Six orders carry a
   negative quantity and a matching negative payment marked SUCCESS, which looks like a
   refund booked as a negative sale. If it is a refund, these belong in a returns model
   and should reduce net revenue. If it is an error, they are quarantined. The answer
   moves the September number either way, so it needs Kavya and finance in the room,
   not just ops.

2. **What does `Pending??` mean operationally, and should it ever count as revenue?**
   Three orders sit in a status that is not in the agreed list. All three have a
   successful payment against them. Quarantining them removes paid revenue; accepting
   them means adding a fourth status to every downstream rule.

3. **Who owns the 120 minute delivery ceiling, and is 480 minutes a real delivery or a
   logging error?** The six long deliveries are all DELIVERED with a payment. If the
   driver app recorded hours, the row is salvageable by dividing. If the order genuinely
   took 8 hours, it is a service failure that ops needs to see, and quarantining it hides
   a real operational problem.

4. **For the five duplicated customer IDs, which row is current?** Signup date, name and
   email match; phone and area differ. Without a system of record or an updated_at
   column, any dedupe rule I pick is a guess, and the choice changes which area a
   customer rolls up to.

5. **Does finance want September to stay frozen once reported?** An order delivered on
   3 September and returned on 1 October has two defensible answers. This decides whether
   the mart is restated in place or versioned, and it is the question behind Part E9, so
   it is better asked now than after the pipeline is built.

## 0.7 What this sets up

Every Critical rule above becomes a check in Part D, with the same `rule_id` used as
`check_name` in `dq_results` and as `failure_reason` in the quarantine file. That means
the Python checks in the ETL DAG and the SQL checks in the ELT staging layer share one
vocabulary, which is what makes the "do the two pipelines agree" test in Part B4
answerable rather than argumentative.

---

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

---

# Part B: ELT Pipeline

`dags/nammamart_elt_bhavani.py`
Flow: Extract, Load raw, Transform in the warehouse with SQL.

Every transformation is a `.sql` file in `include/sql/`, run by an Airflow task.
No pandas cleaning anywhere in this DAG. The only Python is orchestration and the
raw load itself.

## B.1 The three layers

| Layer | Tables | Rule |
|---|---|---|
| Raw | `raw_orders`, `raw_payments`, `raw_customers`, `raw_products`, `raw_stores` | Exact copy, all columns text, plus `_loaded_at`, `_source_file`, `_run_id`, `_row_id`. Never cleaned, never deleted |
| Staging | `stg_orders`, `stg_payments`, `stg_customers`, `stg_products`, `stg_stores` | SQL only: cast, standardise, dedupe, mask PII, apply rules, set `dq_status` and `failure_reason` |
| Mart | `mart_daily_store_kpis`, `mart_category_margin`, `mart_delivery_sla`, `mart_payment_reconciliation`, `mart_payment_mode_revenue`, `mart_top_customers`, `mart_rejected_summary` | SQL only, business ready, reads `dq_status = 'PASS'` only |

Raw is loaded with `read_csv(..., all_varchar=true)`. Nothing is cast at this
point, so a junk value stays the junk it is rather than becoming a null nobody can
explain three weeks later.

`_row_id` preserves the original file order. The dedupe rules depend on it, and
without it "keep the first product row" has no meaning once the data is in a table.

## B.2 SQL files

| File | Does |
|---|---|
| `stg_dimensions.sql` | stores, products, customers: cast, repair, mask, dedupe |
| `stg_orders_ddl.sql` | explicit DDL with `PRIMARY KEY (order_id)`, which doubles as the Part E2 data dictionary |
| `stg_orders_upsert.sql` | the upsert, parameterised by source file |
| `stg_payments_upsert.sql` | same shape for payments |
| `mart_all.sql` | the shared fact view plus seven mart tables |
| `dq_results_elt.sql` | rebuilds the quality scorecard from the staging layer |

Parameters are passed with DuckDB `SET VARIABLE` and read with `getvariable()`, so
nothing is string-interpolated into SQL from Python.

## B.3 Incremental load, criterion 6

Raw is **append only**. The September rows the increment corrects stay exactly as
received, which is what makes the Part E8 audit question answerable at all.

Staging is **upserted**. `INSERT OR REPLACE INTO stg_orders` against the
`order_id` primary key. The same SQL file runs for both files, pointed at a
different `source_file`.

Measured:

```
INCREMENT rows in staging: 2400 -> 2550   (150 inserted, 10 updated in place)
raw_orders: 2412 -> 2572                  (nothing ever deleted from raw)
September revenue: 1,135,628.80 -> 1,131,435.10
September RETURNED orders: 68 -> 78
```

The 10 corrections overwrote their old rows without duplicating. September fell by
exactly 4,193.70, the value of the ten orders that turned out to be returns.

A primary key does the work here. There is no code path that could produce a
second copy of an order, because the database refuses it.

## B.4 Do the two pipelines agree?

Both write to the same DuckDB file, so this is one query rather than two
screenshots held side by side.

```sql
SELECT 'ETL' AS pipeline, count(*) AS fact_rows,
       round(sum(net_revenue), 2) AS total,
       round(sum(CASE WHEN order_date < DATE '2026-10-01'
                      THEN net_revenue ELSE 0 END), 2) AS september
FROM etl_fact_orders
UNION ALL
SELECT 'ELT', count(*), round(sum(net_revenue), 2),
       round(sum(CASE WHEN order_date < DATE '2026-10-01'
                      THEN net_revenue ELSE 0 END), 2)
FROM v_fact_orders;
```

| pipeline | fact_rows | total | september |
|---|---|---|---|
| ETL | 2,460 | 12,11,087.30 | 11,31,435.10 |
| ELT | 2,460 | 12,11,087.30 | 11,31,435.10 |

A full outer join on `order_id` comparing `net_revenue` row by row returns
**zero differences**. The two pipelines agree on every order, not just in total.

Category margin, the reconciliation table and the top 10 customers match exactly
as well, including the masked strings.

## B.5 Where they legitimately differ, and why

Totals match. Three counts do not, and each has a reason worth stating rather than
hiding.

**Quarantine counts: ETL 112 orders, ELT 90.**
ETL quarantines rows. ELT quarantines order_ids. The 12 byte-identical repeats in
`orders.csv` and the 10 superseded September rows are **rows** in ETL, so they
appear in the quarantine file. In ELT the primary key means only one row per
order_id can exist, so the duplicates never enter `stg_orders` at all. They are
not lost: `stg_orders_duplicates` keeps them with
`failure_reason = 'orders_duplicate_order_id'`. 112 minus 22 equals 90.

**dq_results rows: ETL 47 checks, ELT 40.**
The seven missing ones are the dataset-level volume and freshness checks. Those
compare a file against an expected size, which is a property of the extract rather
than of any row, so there is nothing in a staging column to reconstruct them from.
They would need their own SQL model in ELT. Recorded as a known gap rather than
quietly ignored.

**Raw row counts grow, fact row counts do not.**
Re-running the ELT DAG rebuilds `raw_*` with `CREATE OR REPLACE`, so raw does not
grow on a re-run, but it does grow when the increment arrives, 2,412 to 2,572. The
mart stays at 2,460. That is correct and is the whole argument for ELT: the
warehouse holds more than the report needs.

## B.6 Idempotency, criterion 5

Running `stg_orders_upsert.sql` twice against the same source:

```
BASELINE  fact: 2310  revenue: 1,135,628.80
RERUN     fact: 2310  revenue: 1,135,628.80
```

Unchanged. Every write in this pipeline is either `CREATE OR REPLACE` or an upsert
against a primary key, so there is no statement anywhere that can append a second
copy of anything.

## B.7 A determinism bug found during testing

The first version of `stg_orders_upsert.sql` inserted every row, including
duplicates, and let `INSERT OR REPLACE` sort it out. It produced the right answer.

It was still wrong. Which copy of a duplicated order survived depended on the
order DuckDB happened to apply the replaces in. A later duplicate row could
overwrite the clean one and quarantine an order that was perfectly fine. The test
passed by luck.

The fix is `WHERE _rn = 1`, with the excluded rows written to
`stg_orders_duplicates` so nothing is silently discarded. A pipeline whose result
depends on row ordering is not reproducible, and reproducibility is the thing
finance is actually buying.

## B.8 PII, criterion 8

Masking happens in `stg_customers`, in SQL, mirroring `include/masking.py`. The raw
columns are dropped in the same `SELECT`, so no mart model can reference
`full_name`, `email` or `phone` even by mistake: the columns do not exist past the
staging boundary.

`raw_customers` still holds the unmasked values, which is the point of raw and the
reason access control on that layer matters. Covered in Part E5.

Verified identical to the ETL output, including the edge cases:
`sa***@example.com`, `98******21`, `Sanjay B.`, and
`ki***@example` for the malformed address that has no domain suffix.

---

# Part C: Scheduling

Both DAGs carry an identical schedule block.

```python
@dag(
    dag_id="nammamart_etl_bhavani",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
    },
)
```

## C.1 What is your cron expression?

`0 9 * * 1-6`

Minute 0, hour 9, every day of month, every month, Monday to Saturday.

Sunday is excluded because the leadership stand-up does not run on Sunday. Nothing
is lost: Monday's 9 AM run reads the current files, which already contain Sunday's
orders.

## C.2 What time does the run show in UTC in the Airflow UI?

**03:30 UTC.** IST is UTC+5:30, so 09:00 IST minus 5:30 is 03:30 the same day.

Airflow stores and schedules internally in UTC and converts for display. The
Airflow 3 UI has a timezone selector in the top bar; set it to Asia/Kolkata and
the DAG list shows 09:00. Leave it on UTC and it shows 03:30. Both are the same
moment, and this trips people up constantly.

India has no daylight saving, so the 5:30 offset never moves. A pipeline scheduled
for a US or European office would shift twice a year, which is precisely why the
timezone is declared rather than the offset hardcoded.

## C.3 What would happen at 9 AM if you had left the start_date in UTC?

The job would run at **14:30 IST**, five and a half hours late. Kavya's 09:30
review would be over, and the report would arrive after the decisions it was meant
to inform.

Worse, it would look correct. The UI would show "9:00", the cron would read
`0 9 * * 1-6`, the DAG would be green, and the only symptom would be the COO
complaining that the numbers are late. There is no error message for a pipeline
that runs perfectly at the wrong time.

A timezone-naive `start_date` is treated as UTC. The cron hour is then interpreted
in UTC too, so `hour = 9` means 09:00 UTC, which is 14:30 in Bengaluru. Making the
`start_date` timezone aware with `pendulum` is what ties the cron to local time.

## C.4 Why catchup=False, and justify it?

`catchup=False` because this is a **daily snapshot** pipeline, not an
event-sourced one.

Both DAGs read whatever is in `include/data/nammamart/` right now. They do not read
a per-date partition. So a backfill run for 3 October would read today's files and
produce today's numbers, labelled 3 October. Every backfilled run would write the
same rows with a misleading date attached.

With `start_date` of 1 October and `catchup=True`, enabling the DAG today would
immediately queue roughly a dozen runs, all producing identical output, all
fighting over the same DuckDB file lock, and all appending noise to `dq_results`
and `pipeline_runs`.

`max_active_runs=1` is there for the same reason. DuckDB takes an exclusive lock
on the warehouse file, so two concurrent runs is not a performance question, it is
a guaranteed failure.

**When catchup would be right:** if the source changed to a dated file per day,
`orders_2026-10-01.csv`, and the extract task read the run's logical date, then a
backfill would genuinely reprocess history and `catchup=True` would be correct.
That is the change I would make before NammaMart's data grows.

## C.5 What happens if your laptop or Docker is off at 9 AM?

**Nothing runs, and nothing catches up.** The scheduler is a process inside a
Docker container on this machine. No container, no scheduler, no run.

When Docker comes back, with `catchup=False` the missed run is not executed. The
DAG simply waits for the next scheduled slot. The 9 AM report for that day never
existed and never will.

What makes this visible rather than silent:

| Signal | Where |
|---|---|
| No `pipeline_runs` row for that date | query the table |
| Gap in the run history | Airflow UI, Grid view |
| No `dq_results` rows for that date | quality scorecard has a hole |

So "the pipeline did not run" is detectable, which is not the same as detected. A
laptop is not production. For a real 9 AM SLA this needs to run somewhere that is
always on: a small cloud VM, managed Airflow, or a scheduled container.

That is the first of the three risks in the Part G recommendation.

## C.6 Retries, retry delay and timeout

```python
"retries": 2,
"retry_delay": timedelta(minutes=5),
"execution_timeout": timedelta(minutes=30),
```

**2 retries, 5 minutes apart.** The realistic 9 AM failure is a transient one: the
export has not finished writing, or a file is briefly locked. Two attempts spread
over 10 minutes covers that and still delivers before 09:30. A third retry would
push past the deadline, at which point the right outcome is a loud failure rather
than a late success.

**30 minute timeout.** A full run takes 19 seconds for ETL and 24 for ELT. Thirty
minutes is about 75 times the observed duration, so it will never fire on a normal
run, but it guarantees a hung task cannot sit holding the DuckDB lock until someone
notices. Without a timeout, a stuck task blocks tomorrow's run too.

These numbers were not guessed. They come from measured run durations, visible in
`pipeline_runs.start_time` and `end_time`.

An observed consequence, worth recording: during development a failing task
produced `up_for_retry` and a five minute wait before each attempt. The downstream
tasks sat in `None`, blocked, rather than failing. That is Part F experiment 4
behaviour, and it also means a genuine 9 AM failure takes 10 minutes to declare
itself. For a 09:30 deadline that is acceptable. For a tighter one, the retry delay
would need to come down.

---

# Part D: Data Quality Dimensions

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

---

# Part E: Data Governance

## E.1 Ownership

| Dataset | Owner | Why |
|---|---|---|
| `orders.csv` | Head of Operations | Owns the order management system that produces it |
| `payments.csv` | Finance Controller | Owns the gateway relationship and the collections number |
| `customers.csv` | Head of Growth, with the DPO as custodian | Growth uses it, but it is personal data, so a second signature is needed on any change |
| `products.csv` | Head of Merchandising | Sets price, cost and category |
| `stores.csv` | Head of Operations | Opens, closes and staffs the stores |
| `raw_*`, `stg_*`, `mart_*`, `etl_*` | The data team, me as FDE | Whoever can change the code owns the tables it writes |

**Who approves a change to a quality rule, such as the 120 minute delivery limit?**

The owner of the dataset the rule applies to, with the consumer of the affected
metric countersigning. For the 120 minute limit that is the Head of Operations,
because `delivery_minutes` is theirs, with Kavya countersigning because the rule
decides what appears in her SLA number.

Not the data engineer. I can show that six orders exceed the limit and what
removing them does to the KPI; I cannot decide whether an 8 hour delivery is a real
service failure or a logging error. That is a business fact, and guessing at it is
how a pipeline quietly starts lying.

Mechanically: rule ids live in `include/dq_checks.py`, so a change is a pull
request. The approver is named in the PR, and `dq_results` shows the day the
numbers moved. The audit trail is the git history.

## E.2 Data dictionary

The authoritative version is `include/sql/stg_orders_ddl.sql`, where the column,
its type and its meaning sit in one place that cannot drift from the table.
Abridged here.

### Fact: `etl_fact_orders` / `v_fact_orders`

| Column | Type | Meaning | Source | Allowed values | PII |
|---|---|---|---|---|---|
| order_id | VARCHAR | Business key, one row per order | orders.csv | ORDnnnnnn | No |
| order_ts | TIMESTAMP | When the order was placed | orders.csv | <= run date | No |
| order_date | DATE | Derived, the grain of daily reporting | derived | | No |
| customer_id | VARCHAR | FK to customer dimension | orders.csv | must exist in master | No, it is a pseudonym |
| store_id | VARCHAR | FK to store dimension | orders.csv | S01 to S15 | No |
| product_id | VARCHAR | FK to product dimension | orders.csv | must exist in catalogue | No |
| quantity | INTEGER | Units ordered | orders.csv | > 0 | No |
| unit_price | DECIMAL(12,2) | Price per unit at order time | orders.csv | > 0 | No |
| discount_pct | DECIMAL(6,2) | Discount applied | orders.csv | 0 to 100 | No |
| order_amount | DECIMAL(14,2) | qty x price x (1 - disc/100) | orders.csv, verified | matches formula | No |
| order_status | VARCHAR | Lifecycle state | orders.csv | DELIVERED, CANCELLED, RETURNED | No |
| payment_mode | VARCHAR | How it was paid | orders.csv, mapped | UPI, Card, Cash on Delivery, Wallet | No |
| delivery_minutes | INTEGER | Door to door | orders.csv | 8 to 120 | No |
| net_revenue | DECIMAL | order_amount if DELIVERED else 0 | derived | >= 0 | No |
| cogs | DECIMAL | quantity x cost_price if DELIVERED | derived | >= 0 | No |
| gross_margin | DECIMAL | net_revenue - cogs | derived | can be negative | No |
| within_15_min | BOOLEAN | Delivered in 15 minutes or less | derived | | No |
| cost_price_suspect | BOOLEAN | Margin rests on a cost nobody trusts | derived | | No |

### Dimension: `etl_dim_customer` / `stg_customers`

| Column | Type | Meaning | Allowed values | PII |
|---|---|---|---|---|
| customer_id | VARCHAR | Business key | Cnnnnn | Pseudonymous identifier |
| customer_name_masked | VARCHAR | "Priya S." | | Masked from PII |
| email_masked | VARCHAR | "pr***@example.com" | | Masked from PII |
| phone_masked | VARCHAR | "98******21" | | Masked from PII |
| city, area, pincode | VARCHAR | Location | Bengaluru areas | Quasi-identifier |
| signup_date | DATE | When they registered | <= run date | No |
| loyalty_tier | VARCHAR | Programme tier | Bronze, Silver, Gold, Platinum | No |
| is_active | BOOLEAN | Currently active | | No |

`full_name`, `email` and `phone` do not exist past the staging boundary. The
columns are dropped in the same SELECT that creates the masked versions, so no
downstream model can reference them even by mistake.

### Dimension: `etl_dim_store`, `etl_dim_product`

Store: `store_id` (key), `store_name`, `zone` (North, South, East, West, Unknown),
`area`, `pincode`, `open_date`, `manager_name` (**employee name, PII**),
`capacity_orders_per_day`.

Product: `product_id` (key), `product_name`, `category`, `sub_category`, `brand`,
`mrp`, `cost_price` (**commercially sensitive**), `unit`, `is_perishable`,
`launch_date`, `cost_price_suspect`.

`manager_name` is the PII nobody notices. It is employee data sitting in a
dimension everyone reads, and a store manager is identifiable from it. It should be
replaced by a `manager_id` referencing an HR system that reporting users cannot
read. Recorded as a gap rather than fixed, because it needs an HR key that does not
exist in this dataset.

## E.3 Lineage: trace one number back to its source rows

Take `mart_daily_store_kpis` for **2026-09-19, NammaMart Yelahanka, net_revenue
9,732.00**.

**In ELT, four queries, no code:**

```sql
-- 1. the mart row
SELECT * FROM mart_daily_store_kpis
WHERE order_date = DATE '2026-09-19' AND store_name = 'NammaMart Yelahanka';

-- 2. the orders behind it
SELECT order_id, order_amount, order_status FROM v_fact_orders
WHERE order_date = DATE '2026-09-19' AND store_name = 'NammaMart Yelahanka'
  AND is_delivered;

-- 3. the staging rows, with their quality verdict
SELECT order_id, dq_status, failure_reason, _source_file, _run_id
FROM stg_orders WHERE order_id IN (...);

-- 4. the source rows, exactly as the file delivered them
SELECT * FROM raw_orders WHERE order_id IN (...);
```

`raw_orders` holds the original text, the file it came from and the run that loaded
it. The trail is complete and anyone with SQL access can walk it.

**In ETL, it is harder.** The fact table is the earliest thing in the warehouse. To
see the source row you go outside the database: find the quarantine CSV for that
run, or open `orders.csv` and trust that it has not changed since. The pipeline
also deliberately excluded rows, and the only record of which ones is a file on
disk named after a run id.

**Which is easier? ELT, decisively, and it is the strongest argument in its
favour.** ETL gives you a correct number; ELT gives you a correct number you can
walk backwards to the exact line of the exact file. For a finance team that
currently spends half a day a week on reconciliation, that difference is the whole
product.

## E.4 Personal data

| Column | Table | Type | Treatment |
|---|---|---|---|
| full_name | customers.csv | Direct identifier | Masked to "Priya S.", raw column dropped at staging |
| email | customers.csv | Direct identifier, contact | Masked to "pr***@example.com" |
| phone | customers.csv | Direct identifier, contact | Masked to "98******21" |
| manager_name | stores.csv | Employee identifier | **Not currently masked. Known gap** |
| area, pincode | customers.csv | Quasi-identifier | Retained, needed for delivery analysis |
| customer_id | all | Pseudonymous identifier | Retained, it is the join key |

**Why mask rather than delete?** Three reasons, all operational.

Support needs to confirm they are speaking to the right person when a customer
reads out their number. The last two digits do that; the full number is not needed.
Deleting the column entirely would push agents back to the raw CRM, which is the
less controlled system.

Masking preserves the shape of the data. `customers_bad_email` found 12 malformed
addresses. If the column were dropped, nobody would ever learn that the CRM is
accepting addresses with no `@`.

And masking is reversible by authorised staff through the source system, where
access is logged. Deletion is not, so the first legitimate need for the data forces
an uncontrolled export.

**Which Indian law applies, and what does it expect?**

The **Digital Personal Data Protection Act, 2023 (DPDP Act)**. NammaMart is a Data
Fiduciary, its customers are Data Principals.

What it expects, mapped to this pipeline:

| Obligation | How the pipeline meets it |
|---|---|
| Purpose limitation, process only for the stated purpose | Marts carry masked values only; the raw layer is not a general-access table |
| Data minimisation | No PII reaches any reporting table; `customer_id` is the join key |
| Reasonable security safeguards | Masking at the staging boundary, access control by layer (E.5) |
| Right to correction | `customer_id` lets a correction be applied and the mart rebuilt |
| Right to erasure | Delete from the source, delete the `customer_id` rows from `raw_customers`, re-run. **Needs a documented procedure, which does not exist yet** |
| Breach notification | `raw_customers` is the only unmasked store, so the blast radius is known precisely |

The honest gap: erasure. Raw is append-only by design, which is exactly what makes
audit work and exactly what makes "delete everything about this person" awkward.
That tension is real, and the answer is a documented deletion procedure against the
raw layer, run on request and logged. Flagged as risk 3 in the Part G
recommendation.

## E.5 Access control

| Layer | Who reads | Why |
|---|---|---|
| `raw_*` | Data engineering only, plus the DPO on request | `raw_customers` holds unmasked name, email and phone. This is the only place that data exists in the warehouse |
| `stg_*` | Data engineering and the data quality owner | PII already masked. Useful for debugging, not for reporting |
| `mart_*`, `etl_*` | Everyone: ops, finance, store managers, leadership | Masked, validated, business ready |
| Quarantine files, `stg_orders_duplicates` | Data engineering and the relevant dataset owner | Contains raw rejected rows, so it can contain raw PII |
| `dq_results`, `pipeline_runs` | Everyone | No PII, and visible quality is the point |

**Raw customers contains unmasked phone numbers. Who should see it?**

Nobody, as a default. Not analysts, not the growth team, not me once the pipeline
is running.

Three narrow exceptions, each time-bound and logged: the DPO handling a Data
Principal request; a data engineer debugging a specific masking failure; a named
security responder during an incident. Each access justified in writing and
expiring.

In DuckDB this is a file permission rather than a grant, which is itself an
argument for moving to a warehouse with real row and column level security before
this dataset grows. Risk 2 in Part G.

## E.6 Retention

| Data | Retain | Then |
|---|---|---|
| `raw_*` | 7 years | Archive to cold storage. Seven because Indian tax records require it and these rows underpin reported revenue |
| Quarantine files | 90 days | Delete. Long enough for a quarterly finance review to ask about any rejected row, short enough that raw PII in rejected rows is not sitting around indefinitely |
| Masked customer dimension | While the account is active, plus 3 years | Delete the row. Masked, but still linkable to a person via `customer_id` |
| `mart_*` | Rebuilt every run, no independent retention | Derived data, recoverable from raw |
| `dq_results`, `pipeline_runs` | 2 years | Aggregate to monthly, keep the trend, drop the rows |

The point of the 90 day quarantine window: those files are the only place raw
rejected rows live outside the warehouse, and a rejected row can contain the same
PII as an accepted one. Keeping them forever quietly creates a second, less
governed copy of customer data.

## E.7 Secrets

Nothing is in the DAG file and nothing is on GitHub.

`include/slack_notifier.py` reads `Variable.get("SLACK_WEBHOOK_URL")` at runtime,
with a console fallback when the variable is not set, so the pipeline runs on a
laptop with no secret configured. The variable is set in the Airflow UI under
Admin, Variables, and stored in Airflow's metadata database.

`.gitignore` excludes `.env` and `include/warehouse_*.duckdb`.

**Why they must never be in the DAG file or on GitHub:**

A git repository remembers forever. Committing a webhook URL and deleting it in the
next commit does not remove it; it sits in the history, and on a public fork it is
public history. Automated scanners find exposed credentials in public repos within
minutes.

Code and credentials also have different lifecycles. The same DAG should run in
development and production with different secrets. Hardcoding forces either a code
change per environment or one shared credential everywhere, and neither can be
rotated without a deployment.

And a webhook URL is not just noise. Anyone holding it can post to the company
Slack as the pipeline, which is a convincing place to send a false "data is ready"
or a phishing link.

## E.8 Audit: pick a rejected order, prove it is not in the CFO's total

Take **ORD001624**, rejected for `orders_amount_formula_mismatch`: the row claims
9,576.00 where quantity, price and discount give 957.60, a tenfold overstatement.

**ELT answers it in three queries, all inside the database:**

```sql
-- as received, untouched
SELECT * FROM raw_orders WHERE order_id = 'ORD001624';

-- the verdict and the rule it broke
SELECT order_id, dq_status, failure_reason, _source_file, _run_id
FROM stg_orders WHERE order_id = 'ORD001624';
-- QUARANTINE | orders_amount_formula_mismatch

-- proof it is not in revenue
SELECT count(*) FROM v_fact_orders WHERE order_id = 'ORD001624';
-- 0
```

Plus the aggregate check: `mart_rejected_summary` shows 18 rows rejected under that
rule, and `dq_results` shows the same 18 with dimension Accuracy, severity
Critical, for that run.

**ETL answers it too, but partly outside the database:**

```sql
SELECT count(*) FROM etl_fact_orders WHERE order_id = 'ORD001624';  -- 0
```

For the row as received and the reason, you open
`include/quarantine/bhavani/orders_failed_<run_id>.csv`, which has the original
columns plus `failure_reason`. Complete, but it is a file on disk rather than a
table, so the auditor needs filesystem access and has to match run ids by hand.

**Which would finance rather have?** The ELT version. Three queries in a tool they
already use, versus two queries and a CSV hunt.

## E.9 Change history: delivered 3 September, returned 1 October

**What each pipeline shows after the incremental load:**

Both show September revenue of **11,31,435.10**, down from 11,35,628.80. The
order's status is now RETURNED, so it contributes zero. Both restate September in
place, and both give the same answer, because `stg_orders` and `etl_fact_orders`
are both keyed on `order_id` and both upsert.

**Which answer does finance want?** It depends which question they are asking, and
that is the real point.

| Question | Right answer |
|---|---|
| What is September worth today, net of returns? | 11,31,435.10. The restated figure |
| What did we report to the board on 1 October? | 11,35,628.80. The figure as it stood then |
| How much September revenue has since been returned? | 4,193.70 |

Finance needs all three. The month-end close is signed on the second; the live
P&L uses the first; the returns rate needs the third. A pipeline that only
produces the first makes the audit question "why does this not match what you told
us in October?" unanswerable.

**How to keep both views:**

Today only the first is directly available. Three things to add, in order of cost:

1. **Snapshot the mart.** A `mart_daily_store_kpis_snapshot` table with a
   `reported_on` date, written once per run and never updated. Cheap, and it
   answers "what did we say on 1 October" immediately.

2. **SCD Type 2 on the order status.** Keep `stg_orders_history` with
   `valid_from`, `valid_to` and `is_current`. Every status change becomes a row, so
   "as at any date" is a `WHERE` clause. This is the bonus item in the assignment
   and the right long-term answer.

3. **Reconstruct from raw.** Possible today in ELT without any new table, because
   raw is append only and carries `_loaded_at` and `_source_file`. Filter
   `raw_orders` to rows loaded before 1 October and rebuild. Slow and manual, but
   it means the information is not lost while options 1 and 2 are built.

Option 3 exists only in the ELT pipeline. In ETL, the pre-correction state of that
order is gone from the warehouse the moment the increment loads. That is the single
sharpest difference between the two designs, and it is the reason the Part G
recommendation goes the way it does.

---

# Part F: Observability Tests

Run audit is in place. A final task on both DAGs, with
`trigger_rule="all_done"`, writes one row per run to `pipeline_runs` whether the
run succeeded or failed:

```sql
SELECT * FROM pipeline_runs ORDER BY start_time DESC;
```

| run_id | dag_id | rows_extracted | rows_loaded | rows_quarantined | status |
|---|---|---|---|---|---|
| manual__2026-10-13T06:06:09 | nammamart_elt_bhavani | 5372 | 2460 | 115 | SUCCESS |
| manual__2026-10-13T05:54:28 | nammamart_etl_bhavani | 5372 | 2460 | 141 | SUCCESS |

`dq_results` accumulates 47 checks per ETL run and 40 per ELT run, every run.

## Results

| # | Experiment | Expected | Observed | Where I saw it | What I would change |
|---|---|---|---|---|---|
| 1 | Happy path | Both DAGs green, five extracts in parallel | ETL 19.4s, ELT 24.4s. All tasks Success on try 1. Longest task `transform_staging_facts` at 4.58s | Airflow Gantt view; `pipeline_runs` | Gantt showed the five raw loads overlapping but the transform chain as a staircase, which is correct. Nearly all elapsed time is Airflow task startup, not data work. At this volume the orchestrator costs more than the computation |
| 2 | Re-run, idempotency | Fact counts unchanged | ETL: `updated=2460, inserted=0, final_rows=2460`. ELT fact rows and revenue identical | `[load]` line in the task log | Raw tables did **not** grow in ELT, because `load_raw_*` uses `CREATE OR REPLACE`. Defensible, but a strictly append-only raw would grow every run and need `_run_id` filtering downstream. I would make that choice explicit in the table comment rather than implicit in the code |
| 3 | Incremental load | 150 inserted, 10 updated, no duplicates | Staging 2400 to 2550. September revenue 11,35,628.80 to **11,31,435.10**, a fall of exactly 4,193.70. September RETURNED 68 to 78 | `[merge]` log line; `mart_daily_store_kpis` | Worked first time. The 10 superseded September rows appear in quarantine as `orders_duplicate_order_id`, which is history rather than error. I would rename that reason to `superseded_by_correction` when the source file is the increment, so a reader does not mistake it for a data fault |
| 4 | Missing source file | Retries, then failure, downstream blocked | Observed twice for real during development. Task went `up_for_retry`, two attempts five minutes apart, then `failed`. Downstream tasks stayed at `None`, blocked, not failed. `run_audit` still ran because of `all_done` | Airflow Grid view; DAG overview panel showing "3 Failed Tasks, 2 Failed Runs" in 24 hours | **Nothing alerted me.** The failure was visible to anyone who went looking, which is not the same as detected. Fixed: `include/alerts.py` now wires `on_failure_callback` into both DAGs, falling back to a console alert when no webhook is configured |
| 5 | New bad data | 5 rows quarantined with the right reasons | Orders read 2,412 to 2,417. Quarantined **112 to 117**. Passed unchanged at 2,460. Each row carried its expected rule, and ORD900002 fired two at once | `orders_failed_manual__2026-10-13T06_42_13.csv`; `dq_results` | The per-row `failure_reason` design proved itself: `orders_quantity_not_positive; orders_amount_formula_mismatch` on one row. A single combined mask could not have produced that. No change needed |
| 6 | Schema change | Loud failure, not silent wrong data | *(fill in after running)* ETL should record a `Schema` dimension row in `dq_results` via the schema-drift branch in `run_checks`. ELT should load `raw_orders` fine, because `all_varchar=true` accepts any columns, then fail at `stg_orders_upsert.sql` with a binder error | Task log; `dq_results` | The interesting difference is **where** each fails. ETL catches it at validation, before the warehouse. ELT has already written a raw table of the wrong shape. Neither loads bad data, but ETL fails earlier and cheaper |
| 7 | Volume anomaly | Volume check warns on a 98% drop | *(fill in after running)* `orders_volume_drop` should fire WARN against a floor of 1,500 rows | `dq_results`, dimension Volume | Two gaps. The floor of 1,500 is arbitrary and would miss a 30% drop entirely; it should be a rolling median of previous runs stored in `dq_results`. And the check exists only in ETL, one of the seven dataset-level checks the ELT pipeline cannot reconstruct from a staging column |
| 8 | **Distribution drift** | No row-level rule catches it, because every row stays internally consistent | **Quarantined count unchanged at 112. Zero new failures in `dq_results`.** S01 moved from 85,510.70 (7.1% of revenue) to 765,532,458.35 (**99.9%**). Every other store rounded to 0.0% | `dq_results` showed no change at all. The anomaly was visible only by querying `etl_fact_orders` grouped by store and comparing against the previous run | A distribution check comparing each store's daily revenue against its own rolling 7-day median, flagging deviation beyond 3x. Also a bound on `unit_price` against the product's `mrp`, since in clean data they are equal. Neither exists today, which is why distribution is the weakest of the five pillars |
| 9 | Freshness | Timeliness check warns on 30-day-old data | *(fill in after running)* `orders_freshness` should fire WARN. Verified in isolation: latest `order_ts` moves from 2026-09-30 to 2026-09-21 and status changes PASS to WARN | `dq_results`, dimension Timeliness | **A bug found while building this experiment.** The check used `max(order_ts)`, and seven orders in the export are dated 2027. The maximum was therefore always in the future, so the file looked fresh forever and the check could never fire. It now measures against the latest timestamp that is not in the future. One bad future timestamp silently disabling a freshness check is a failure mode worth naming |
| 10 | Recovery | Only the failed task and downstream re-run | *(fill in after running)* Clear on the failed task should leave the five extract tasks green and untouched | Airflow Graph view, task states before and after | The recovery is seconds versus a full re-run, and that difference is the argument for granular tasks over one monolithic script. With one task doing everything, a failure in step 9 of 9 costs you steps 1 to 8 again |

## The lock collision, found by running it

Not one of the ten, but the most consequential thing this exercise surfaced.

`build_reports` failed at 06:39:55 with no data problem whatsoever, and the
identical code succeeded at 06:42:13 with nothing changed. The cause was a
**DuckDB file lock**: a scheduled run was in flight holding the warehouse file
while a manual run tried to write.

Both DAGs were scheduled at `0 9 * * 1-6` and both write to the same DuckDB file.
`max_active_runs=1` stops a DAG colliding with itself; it does nothing about a DAG
colliding with its sibling. At 9:00 tomorrow, one of the two would have failed on
a lock error, and the error would have looked like a data problem.

Fixed: the ELT DAG now runs at `5 9 * * 1-6`. A full run takes under 30 seconds,
so five minutes is ample headroom.

This is also an argument against a single-file warehouse as the data grows, and it
is noted in the Part G risks.

## Five pillars of data observability

| Pillar | Covered by | Assessment |
|---|---|---|
| **Freshness** | `orders_freshness`, `payments_freshness` in `dq_results` | Adequate in ETL after the future-timestamp fix. Absent in ELT |
| **Volume** | `orders_volume_drop` and four siblings, fixed thresholds | Weak. A static floor of 1,500 catches a catastrophe and misses a slow decline |
| **Schema** | Schema-drift branch in `run_checks`; binder errors in SQL | Good. Both pipelines fail loudly rather than loading the wrong shape |
| **Distribution** | Nothing | **Weakest.** Experiment 8 is the proof |
| **Lineage** | `raw_*` with `_source_file`, `_run_id`, `_row_id`; quarantine files; `dq_results` | **Strongest**, and far stronger in ELT than ETL |

**Best covered: lineage.** In ELT, any number on a mart can be walked back to the
source row with SQL alone: mart, to `v_fact_orders`, to `stg_orders` for the
quality verdict, to `raw_orders` for the row exactly as the file delivered it. Four
queries, no code, no filesystem access. In ETL the trail leaves the database at the
fact table and continues in a CSV named after a run id.

**Weakest: distribution.** Experiment 8 settles it. One store took 99.9% of
company revenue and all 47 checks passed. Row-level rules verify that each record
is internally consistent; they say nothing about whether the dataset as a whole
resembles yesterday's. That needs statistical comparison against history, and
history is exactly what `dq_results` is accumulating. The check cannot be written
until there is a baseline, which is an argument for starting to collect the
scorecard before you need it.

---

# Part G: Recommendation to Sameer, CTO

**Subject: ETL or ELT for NammaMart**

Sameer,

Build ELT. Keep the ETL validation logic, but run it as SQL inside the warehouse
rather than in Python before it.

Both pipelines now run against your September data and agree on every order:
2,460 rows, 12,11,087.30 total, 11,31,435.10 for September. A row-by-row join
comparing revenue returns zero differences. So this is not a correctness question.
It is a question about which design you can still operate in two years.

## What the numbers already told us

Before either pipeline existed, profiling found issues in all six files. The
current morning report overstates September revenue by **83,392, about 6.8%**.
Not rounding: duplicate orders, hours logged as minutes, discounts above 100%, and
orders pointing at stores that do not exist. 102 of 2,412 order rows fail at least
one critical rule.

That number alone pays for this project.

## Auditability: the deciding factor

Finance currently spends half a day a week finding out why order revenue does not
match the gateway. The question behind that work is always the same: "where did
this number come from?"

ELT answers it with SQL. Take any figure on `mart_daily_store_kpis`, join to
`v_fact_orders`, then `stg_orders` for the quality verdict, then `raw_orders` for
the row exactly as the file delivered it, with `_source_file` and `_run_id`
attached. Four queries, no code, no filesystem access.

ETL answers it by sending someone to a CSV in a quarantine folder named after a
run id. The answer is correct and the route to it is not something finance can
walk unaided.

The sharpest version of this is your own Part E9 question. An order delivered on
3 September and returned on 1 October: both pipelines now show September at
11,31,435.10. But "what did we report to the board on 1 October" is answerable
only in ELT, by filtering `raw_orders` on `_loaded_at`. In ETL that prior state is
gone from the warehouse the moment the correction loads.

## Data quality guarantees: a tie, deliberately

Your instinct was that ETL protects finance better. It does not, because the
protection does not come from where the cleaning happens. It comes from the rules.

Both pipelines run the same 40 checks, under the same names, with the same
severities, because both read `check_catalog` from one Python registry. ETL rejects
rows before the warehouse; ELT marks them `dq_status = 'QUARANTINE'` and the mart
filters them. Nothing bad reaches the CFO's dashboard either way.

ELT keeps the rejected rows **inside the database**, which is strictly better. The
bad row is a queryable row, not a file someone has to find.

One design decision applies to both and is worth your attention. An early version
quarantined any dimension row that failed a critical rule. One store with a blank
zone and ten imperfect product rows removed **316 orders** from the fact table.
Revenue disappearing because of a missing lookup value is the failure you hired us
to remove, so the rule is now: repair dimensions, reject facts. Today 85,382 of
revenue sits under zone "Unknown" and 41,911 under category "Uncategorised". Both
are visible and chaseable instead of silently gone.

## Ease of change

Your analysts know SQL. They do not know pandas.

A new metric in ELT is a new SQL file and a task that runs it. Your analytics team
can write it, and they can read every existing transformation without asking an
engineer what a `.pipe()` chain does.

A new metric in ETL is a change to Python that must be reviewed, tested and
deployed by whoever owns the DAG. With no data engineers on staff, that is a
bottleneck with one person in it.

ELT also lets you answer questions you have not thought of yet. Raw holds every
column of every row ever received. When someone asks in March what happened to
cancelled orders in September, the data is there. In ETL, only what the pipeline
was told to keep survived.

## Performance at 1000x

At 2,400 orders a month both finish in under 25 seconds. At 2.4 million a month
the picture changes.

ETL pulls everything into pandas memory on one machine. At 1000x the current
volume, `orders.csv` alone is several GB and the validate step becomes a memory
problem long before it becomes a speed problem. The fix is chunking or Spark,
which is a rewrite.

ELT pushes the work to the database engine, which was built for it. DuckDB handles
single-node workloads far past 1000x, and when you outgrow it the staging and mart
SQL ports to Snowflake, BigQuery or Postgres largely unchanged, because it is just
SQL. The raw load is the only part that needs rewriting.

Cost follows the same shape. Both are open source and run on one container today.
ETL's growth path spends money on bigger machines and a distributed framework;
ELT's spends it on warehouse compute you can turn off between runs. ELT also stores
more, since raw is retained for seven years, but object storage is the cheapest
thing in the stack and far cheaper than the engineering time ETL's rewrite would
cost.

## The recommendation

**Adopt ELT. Keep ETL as the reference implementation** until the ELT marts have
been reconciled against it for a full month, then retire it. The comparison query
runs in seconds; leave it in the DAG as a permanent regression test even after ETL
is gone.

## Top 3 risks

**1. The schedule is only as reliable as the machine it runs on.** Today both DAGs
run in Docker on a laptop. `catchup=False` means a missed 9 AM run is never
executed, and the only evidence is a gap in `pipeline_runs` that nobody is paged
about. A 9 AM SLA needs always-on infrastructure, a small cloud VM or managed
Airflow, plus the Slack alert on failure that `include/slack_notifier.py` is
already written for but not yet wired to `on_failure_callback`.
*Mitigation: cloud deployment and failure alerting before go-live. Two days.*

**2. Raw holds unmasked personal data, and DuckDB has no access control.** Marts
are masked; `raw_customers` is not, and it must not be, because that is what makes
replay and correction possible. DuckDB is a file, so "who can read raw" is a
filesystem permission, not a grant. Under the DPDP Act, NammaMart is a Data
Fiduciary with a duty of reasonable security safeguards. There is also no
documented erasure procedure, which sits awkwardly against an append-only raw layer.
*Mitigation: move raw to a warehouse with row and column level security, and write
the erasure runbook, before customer volume grows.*

**3. Distribution drift is invisible.** The weakest of the five observability
pillars by a distance. Multiply one store's `unit_price` by 100 and every check
passes: quantity positive, formula holds, status valid, keys resolve. Row-level
rules cannot catch a dataset that is wrong in aggregate while being correct row by
row. The mart would report that store at 85% of company revenue and nothing would
flag it.
*Mitigation: statistical checks against each store's own rolling history, built
from `dq_results` once a few weeks of baseline has accumulated.*

## What I would do first

In this order, and none of it takes long:

1. Wire `on_failure_callback` to the Slack notifier. Half a day, and it closes the
   single largest gap, which is that a failed 9 AM run currently tells nobody.
2. Deploy to an always-on host. Two days.
3. Get answers to the three open client questions, especially whether the six
   negative-quantity orders are refunds or errors. That decision moves September
   revenue and no engineer should make it.
4. Snapshot the mart per run, so "what did we report in October" stops being
   unanswerable before the next board meeting.

The six-week test of whether this worked is not whether the pipeline is green. It
is whether anyone in finance still spends half a day a week on reconciliation.

— Bhavani, FDE
