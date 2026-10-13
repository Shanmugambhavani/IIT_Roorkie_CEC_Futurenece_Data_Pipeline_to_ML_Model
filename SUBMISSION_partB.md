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
