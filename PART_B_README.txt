Part B: ELT pipeline
====================

INSTALL (from inside your repo folder)

  tar -xf "%USERPROFILE%\Downloads\nammamart_partB_bhavani.zip" --strip-components=1

NEW FILES
  dags/nammamart_elt_bhavani.py        the ELT DAG
  include/sql/stg_dimensions.sql       stores, products, customers
  include/sql/stg_orders_ddl.sql       explicit DDL with PRIMARY KEY (also the data dictionary)
  include/sql/stg_orders_upsert.sql    the MERGE, parameterised by source file
  include/sql/stg_payments_upsert.sql  same shape for payments
  include/sql/mart_all.sql             fact view plus seven mart tables
  include/sql/dq_results_elt.sql       quality scorecard rebuilt from staging
  SUBMISSION_partB.md                  paste into SUBMISSION.md after Part A

Nothing from Part A is overwritten.

RUN IT

  astro dev run dags list-import-errors
  astro dev run dags test nammamart_elt_bhavani

IMPORTANT: run the ETL DAG at least once first. The last task compares the two
pipelines, and it needs etl_fact_orders to exist.

EXPECT TO SEE
  [raw] orders.csv -> raw_orders: 2412 rows, untouched
  [raw] increment appended: 160 rows, raw_orders now 2572
  [staging] stg_orders: 2550 rows, 90 quarantined
  [merge] 150 inserted, 10 updated in place
  [merge] September RETURNED orders now: 78
  [mart] fact rows 2460, total revenue 1,211,087.30, September 1,131,435.10
  [dq] 40 checks recorded

  then the Part B4 comparison:
    pipeline  fact_rows     total  september
         ETL       2460 1211087.3  1131435.1
         ELT       2460 1211087.3  1131435.1
    [compare] the two pipelines agree on every order

QUERY THE WAREHOUSE

  astro dev bash
  python -c "import duckdb; c=duckdb.connect('include/warehouse_bhavani.duckdb'); print(c.sql('select * from mart_category_margin'))"

BUSINESS QUESTIONS, one query each
  Q1 select * from mart_daily_store_kpis
  Q2 select * from mart_category_margin
  Q3 select * from mart_delivery_sla
  Q4 select * from mart_payment_mode_revenue
     select reason, count(*), round(sum(difference),2) from mart_payment_reconciliation group by 1
  Q5 select * from mart_top_customers
  Q6 select * from mart_rejected_summary
