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
| 19 | gateway_ref | Blank | Completeness | 301 | Expected for COD, but 285 are SUCCESS across all modes. Load and flag | Warning |
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
