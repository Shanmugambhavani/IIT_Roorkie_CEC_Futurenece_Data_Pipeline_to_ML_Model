Part A: ETL pipeline
====================

INSTALL (from inside your cloned repo folder)

  tar -xf "%USERPROFILE%\Downloads\nammamart_partA_bhavani.zip" --strip-components=1

This overwrites include/dq_checks.py with an updated version. That is intended:
several dimension rules changed from Critical to Warning. See SUBMISSION_partA.md
section A.2 for why.

NEW FILES
  dags/nammamart_etl_bhavani.py   the DAG
  include/transforms.py           standardise, join, derive, reconcile, report
  include/warehouse.py            DuckDB connection and idempotent upsert
  SUBMISSION_partA.md             paste into SUBMISSION.md after the Part 0 section

UPDATED
  include/dq_checks.py            dimension severities changed

RUN IT

  astro dev run dags list-import-errors        should print nothing
  astro dev run dags test nammamart_etl_bhavani

Watch for these lines in the log:
  [transform] fact_orders: 2310 rows
  [transform] net revenue: 1,135,628.80
  [load] etl_fact_orders: {'incoming': 2310, 'updated': 0, 'inserted': 2310, ...}
  [reconcile] 152 mismatches

THEN TEST IDEMPOTENCY (Part F experiment 2)

  astro dev run dags test nammamart_etl_bhavani

Second run should show updated=2310, inserted=0, final_rows=2310.

QUERY THE WAREHOUSE

  astro dev bash
  python -c "import duckdb; c=duckdb.connect('include/warehouse_bhavani.duckdb'); print(c.sql('select zone, round(sum(net_revenue),2) from etl_fact_orders where is_delivered group by 1 order by 2 desc'))"

TABLES CREATED
  etl_fact_orders              2310 rows
  etl_dim_customer             400
  etl_dim_product              150
  etl_dim_store                15
  etl_payment_reconciliation   152
  etl_daily_zone_revenue       445
  etl_category_margin          7
  etl_top_customers            10
  dq_results                   appended every run
  pipeline_runs                appended every run

NOTE ON THE INCREMENT FILE
The DAG picks up orders_2026-10-01_increment.csv automatically if it is present in
include/data/nammamart/. To run WITHOUT it (the baseline September numbers), move
it out of that folder temporarily.
