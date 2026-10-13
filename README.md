# NammaMart Quick Commerce: ETL and ELT Pipelines

FDAIE Module 2, Data Engineering Foundations.
Submitted by **Bhavani**.

NammaMart is a 10 minute grocery delivery business in Bengaluru running 15 dark stores
across 4 zones. Its morning operations review runs on a manual Excel report that arrives
at 10:30 AM, overstates revenue and never reconciles against the payment gateway.

This repo replaces that report with two Airflow pipelines, ETL and ELT, built on the same
five source files, scheduled for 9:00 AM IST Monday to Saturday, plus a written
recommendation on which one NammaMart should adopt.

## Headline finding

The current morning report overstates September revenue by **83,392, about 6.8%**.

| | Amount |
|---|---|
| September revenue as reported today | 12,19,020.88 |
| September revenue after quality rules | 11,35,628.80 |
| Order rows failing at least one Critical rule | 102 of 2,412 (4.2%) |

Full evidence in [`SUBMISSION.md`](SUBMISSION.md), Part 0.

## Where everything lives

```
.
├── README.md                     this file
├── SUBMISSION.md                 all graded written answers, Parts 0, C, E, F, G
├── dags/
│   ├── meridian_pipeline.py      trainer's reference DAG, unmodified
│   ├── nammamart_etl_bhavani.py  Part A: extract, validate, transform, load
│   └── nammamart_elt_bhavani.py  Part B: extract, load raw, transform in SQL
├── include/
│   ├── data/nammamart/           the six source CSVs, never edited by hand
│   ├── profiling/
│   │   ├── profile_nammamart.py  Part 0 profiler, re-runnable
│   │   └── out/                  profile summary, issues log, offending rows
│   ├── dq_checks.py              Part D quality rules used by the ETL DAG
│   ├── masking.py                email and phone masking helpers
│   ├── sql/                      Part B transformations, one file per table
│   └── quarantine/bhavani/        rejected rows with failure_reason, per run
└── screenshots/                  Airflow UI evidence
```

## Pipelines

| | ETL | ELT |
|---|---|---|
| DAG | `nammamart_etl_bhavani.py` | `nammamart_elt_bhavani.py` |
| Cleaning happens | in Python, before the warehouse | in SQL, inside the warehouse |
| Bad rows | quarantined, never reach the fact table | land in raw, filtered at staging |
| Output tables | `etl_fact_orders`, `etl_dim_*`, `etl_payment_reconciliation` | `raw_*`, `stg_*`, `mart_*` |
| Warehouse | `include/warehouse_bhavani.duckdb` | same file |

Both pipelines share one rule vocabulary. Every quality rule has a stable id such as
`orders_amount_formula_mismatch`, used as the `failure_reason` in quarantine, the
`check_name` in `dq_results`, and the `failure_reason` column in `stg_orders`. That is
what makes the "do the two pipelines agree" comparison answerable rather than a debate.

## Running it locally

```bash
astro dev start                                  # Airflow at localhost:8080, admin/admin
astro dev run dags list-import-errors            # confirm both DAGs parse
astro dev run dags test nammamart_etl_bhavani     # single run from the terminal
astro dev run dags test nammamart_elt_bhavani
```

Re-run the profiler at any time without Airflow:

```bash
python include/profiling/profile_nammamart.py \
    --data include/data/nammamart \
    --out  include/profiling/out
```

Query the warehouse:

```bash
astro dev bash
python -c "import duckdb; print(duckdb.connect('include/warehouse_bhavani.duckdb').sql('SELECT * FROM etl_fact_orders LIMIT 5'))"
```

Note: DuckDB locks the file. Close any VS Code SQLTools connection before triggering a run.

## Build status

| Part | Deliverable | Status |
|---|---|---|
| 0 | Discovery, profiling, data issues log | Done |
| A | ETL pipeline | Pending |
| B | ELT pipeline | Pending |
| C | 9:00 AM IST schedule, both DAGs | Pending |
| D | Six quality dimensions, `dq_results` | Pending |
| E | Governance answers | Pending |
| F | Ten observability experiments, `pipeline_runs` | Pending |
| G | Recommendation to the CTO | Pending |

## Notes

- No credentials, webhook URLs or unmasked customer data are committed. Secrets are read
  with `Variable.get()` at runtime.
- The source CSVs are committed as the assignment requires, and are never modified.
- `include/warehouse_bhavani.duckdb` is gitignored. Generate it by running the DAGs.
