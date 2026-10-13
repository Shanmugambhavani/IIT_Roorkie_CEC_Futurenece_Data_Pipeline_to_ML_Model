# Part F: Observability Experiments

Ten experiments, both DAGs, 15 marks. This is the most time-consuming part left
and the one most people under-document. Budget two hours.

**Record each one as: Expected, Observed, Where I saw it, What I would change.**
The marks are in the fourth column. "It failed as expected" earns little; "it
failed, and here is the check I added because the failure was invisible for six
minutes" earns full credit.

Two of these you have already run accidentally, noted below.

---

## Before you start

Make a backup folder so restoring a file is one copy command, never a re-download:

```
mkdir D:\BHAVANI\backup
copy include\data\nammamart\*.csv D:\BHAVANI\backup\
```

Restore after any experiment:

```
copy D:\BHAVANI\backup\*.csv include\data\nammamart\
```

Useful queries throughout. Run with:

```
astro dev bash
python -c "import duckdb; c=duckdb.connect('include/warehouse_bhavani.duckdb'); print(c.sql(\"<SQL>\"))"
```

| What | SQL |
|---|---|
| Run history | `select * from pipeline_runs order by start_time desc limit 10` |
| Quality trend | `select run_id, status, count(*) from dq_results group by 1,2 order by 1 desc` |
| Fact count | `select count(*), round(sum(net_revenue),2) from etl_fact_orders` |
| Raw counts | `select _source_file, count(*) from raw_orders group by 1` |

---

## 1. Happy path

```
astro dev run dags test nammamart_etl_bhavani
astro dev run dags test nammamart_elt_bhavani
```

**Already done.** You have the numbers: ETL 19.4 seconds, ELT 24.4 seconds.

Record: task durations from the Gantt view, rows in / passed / quarantined per
file from the log, and which tasks ran in parallel.

Note for the write-up: `astro dev run dags test` runs tasks **sequentially** even
where the graph allows parallelism. To observe real parallelism you must trigger
from the UI, where the five extract tasks start together. Worth stating, because it
is a genuine difference between the two ways of running a DAG and most people miss
it.

## 2. Re-run, idempotency

Trigger the same DAG a second time with no changes.

**Expected:** ETL `updated=2460, inserted=0, final_rows=2460`. ELT fact count
unchanged.

**The question the assignment asks:** did raw tables grow in ELT, and is that
correct? Check `select count(*) from raw_orders` before and after.

Answer: no, because `load_raw_*` uses `CREATE OR REPLACE`. Raw grows when a **new
file** arrives, not when the same file is re-read. Both behaviours are defensible;
state which you implemented and why. A true append-only raw would grow on every
run and need `_run_id` filtering downstream.

## 3. Incremental load

**Already covered.** Your current runs include the increment file.

To show the before and after properly:

```
move include\data\nammamart\orders_2026-10-01_increment.csv D:\BHAVANI\backup\
astro dev run dags test nammamart_etl_bhavani
```
Record: 2,310 fact rows, 11,35,628.80, 68 September RETURNED.

```
copy D:\BHAVANI\backup\orders_2026-10-01_increment.csv include\data\nammamart\
astro dev run dags test nammamart_etl_bhavani
```
Record: 2,460 fact rows, September 11,31,435.10, 78 RETURNED. 150 inserted, 10
updated, no duplicates.

## 4. Missing source file

```
rename include\data\nammamart\products.csv products_HIDDEN.csv
astro dev run dags test nammamart_etl_bhavani
```

**You have already seen this behaviour** during the timezone bug: `up_for_retry`,
two retries five minutes apart, downstream tasks sitting at `None` rather than
failing.

Record the task states precisely: the failing task `up_for_retry` then `failed`,
everything downstream `upstream_failed`, and `run_audit` still running because of
`trigger_rule="all_done"`.

**What alerted you?** Nothing, and that is the finding. The log shows it and the
UI shows red, but no message was sent. The fix is already in the repo:
`include/slack_notifier.py` wired to `on_failure_callback`. Doing that is the
assignment bonus.

Restore the file afterwards.

## 5. New bad data

```
copy include\data\nammamart\orders.csv include\data\nammamart\orders_BACKUP.csv
```

Append five rows to `orders.csv` (open in Notepad, add at the end):

```
ORD900001,2026-09-15 10:00:00,,S01,P0001,2,50.00,0,100.00,UPI,DELIVERED,App,12
ORD900002,2026-09-15 10:05:00,C00001,S01,P0001,-3,50.00,0,-150.00,UPI,DELIVERED,App,12
ORD900003,2026-09-15 10:10:00,C00001,S99,P0001,2,50.00,0,100.00,UPI,DELIVERED,App,12
ORD900004,2026-09-15 10:15:00,C00001,S01,P0001,2,50.00,0,100.00,UPI,SHIPPED,App,12
ORD900005,2027-05-01 10:00:00,C00001,S01,P0001,2,50.00,0,100.00,UPI,DELIVERED,App,12
```

**Expected failure_reason per row:**

| Order | Rule |
|---|---|
| ORD900001 | `orders_missing_customer_id` |
| ORD900002 | `orders_quantity_not_positive; orders_amount_formula_mismatch` |
| ORD900003 | `orders_store_fk_missing` |
| ORD900004 | `orders_invalid_status` |
| ORD900005 | `orders_future_order_ts` |

Check the quarantine CSV and `dq_results`. Note that ORD900002 fires two rules,
which is the per-row `failure_reason` design doing its job.

Restore `orders.csv` from the backup copy afterwards.

## 6. Schema change

In a copy of `orders.csv`, rename the header `order_amount` to `total`.

**Expected:** ETL fails in `validate_transactions` with a `KeyError`, because
`dq_checks` looks for a column that is not there. **Check what it actually does:**
`run_checks` has a schema-drift branch that records
`schema_missing_order_amount` in `dq_results` with dimension `Schema` rather than
crashing. Report which happened.

ELT behaves differently and that is the interesting part. `raw_orders` loads fine,
because `all_varchar=true` accepts any columns. The failure appears at
`stg_orders_upsert.sql` with a binder error on an unknown column.

**The question that earns the marks:** did it fail loudly or load wrong data
silently? Both fail loudly here. Say so, and say which failed **earlier** and why
that matters. ETL catches it at validation; ELT has already written a raw table
with the wrong shape.

## 7. Volume anomaly

Make a copy of `orders.csv` keeping only the header and 50 rows.

**Expected:** `orders_volume_drop` fires in `dq_results` with status WARN, because
`EXPECTED_MIN_ROWS["orders"] = 1500`.

This check already exists in the ETL pipeline. **It does not exist in ELT**, which
is one of the seven missing checks noted in Part B.5. Write that up honestly, and
describe the SQL model you would add: a `dq_volume` check comparing
`count(*)` per `_source_file` against a rolling median of previous runs stored in
`dq_results`.

Note the second-order effect: revenue drops roughly 98%, but no revenue check
fires. A volume check on rows is not a check on value.

## 8. Distribution drift

In a copy of `orders.csv`, multiply `unit_price` by 100 for store S01 only. You
must also multiply `order_amount` by 100, or every row trips
`orders_amount_formula_mismatch` and you have tested the wrong thing.

**Expected:** no existing check catches it. Every rule passes. Quantity is
positive, the formula holds, the status is valid, the keys resolve.

**What moves:** `mart_daily_store_kpis` for S01 jumps roughly 100x, and S01's share
of total revenue goes from about 7% to over 85%. Record the before and after
numbers.

**What you would change:** a distribution check comparing each store's daily
revenue against its own 7-day median, flagging a deviation beyond 3x. Also a
check that `unit_price` stays within a sane band of the product's `mrp`, which this
dataset makes easy because `unit_price` normally equals `mrp`.

This is the experiment that best demonstrates the limit of row-level rules. Every
row is individually valid, and the dataset is still obviously wrong.

## 9. Freshness

In a copy of `orders.csv`, shift every `order_ts` back 30 days, or simply set
`AS_OF` in the DAG forward by 30 days, which is faster and does not touch the data.

**Expected:** `orders_freshness` fires with status WARN. The check already exists
in `dq_checks.dataset_checks`, comparing the latest `order_ts` against `AS_OF`
with a one day tolerance.

Note the design decision worth defending: `AS_OF` is pinned to 2026-10-01 rather
than wall-clock time, because otherwise this assignment's data fails a freshness
check every single day it is marked. In production `AS_OF` would be the run's
logical date. State that explicitly, it shows you know the difference between a
test fixture and a production setting.

## 10. Recovery

After experiment 4 or 6 leaves a failed task:

1. Fix the cause, restore the file
2. Airflow UI, open the failed run, Graph view
3. Click the failed task, then **Clear**
4. Confirm the tick box for downstream tasks

**Expected:** only the failed task and everything after it re-runs. The five
extract tasks stay green and are not repeated.

**Record:** which tasks changed state, and how long the recovery took compared with
re-running the whole DAG. That difference is the argument for granular tasks over
one monolithic script, and it belongs in your Part G recommendation.

---

## Five pillars of data observability

Fill this in after running the experiments. My assessment of the current pipeline,
for you to confirm or disagree with:

| Pillar | Covered by | Strength |
|---|---|---|
| **Freshness** | `orders_freshness`, `payments_freshness` in `dq_results` | Good in ETL, absent in ELT |
| **Volume** | `orders_volume_drop` and four others, fixed thresholds | Weak. A fixed floor of 1,500 is arbitrary and will not catch a 30% drop |
| **Schema** | Schema-drift branch in `run_checks`, binder errors in SQL | Good. Both pipelines fail loudly rather than loading the wrong shape |
| **Distribution** | Nothing | **Weakest.** Experiment 8 passes every check while being catastrophically wrong |
| **Lineage** | `raw_*` with `_source_file` and `_run_id`, quarantine files, `dq_results` | **Strongest in ELT**, weaker in ETL where the trail leaves the database |

**Best covered: lineage**, because the raw layer plus `_run_id` means any number
can be walked back to the source row with SQL alone.

**Weakest: distribution.** Experiment 8 is the proof. Every row valid, every check
green, revenue inflated 100x for one store. The fix is a statistical check against
each store's own history rather than a static rule, and that needs the run history
in `dq_results` to accumulate for a couple of weeks before it has a baseline.

---

## Results table template

Copy into `SUBMISSION.md` and fill in as you go.

| # | Experiment | Expected | Observed | Where I saw it | What I would change |
|---|---|---|---|---|---|
| 1 | Happy path | | | | |
| 2 | Re-run | | | | |
| 3 | Incremental | | | | |
| 4 | Missing file | | | | |
| 5 | Bad data | | | | |
| 6 | Schema change | | | | |
| 7 | Volume | | | | |
| 8 | Drift | | | | |
| 9 | Freshness | | | | |
| 10 | Recovery | | | | |

Do this for **both DAGs**. Where behaviour is identical, say so in one line rather
than duplicating the row; where it differs, that difference is usually the most
interesting thing you will write in Part F.
