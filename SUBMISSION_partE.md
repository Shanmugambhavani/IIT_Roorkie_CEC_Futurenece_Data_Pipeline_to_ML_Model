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
