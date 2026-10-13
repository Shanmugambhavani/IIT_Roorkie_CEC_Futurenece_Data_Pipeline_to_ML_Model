Final fixes and the Part F toolkit
==================================

INSTALL (from inside your repo folder)

  tar -xf "%USERPROFILE%\Downloads\nammamart_final_bhavani.zip" --strip-components=1

This OVERWRITES both DAGs and include/dq_checks.py. All three changes are listed
below. Nothing else is touched.

--------------------------------------------------------------------
THREE FIXES
--------------------------------------------------------------------

1. SLACK ALERT ON FAILURE  (bonus marks)
   include/alerts.py wraps the trainer's slack_notifier.py as an Airflow callback.
   Both DAGs now carry "on_failure_callback": dag_failure_alert in default_args.
   With no SLACK_WEBHOOK_URL variable set it prints the alert to the task log
   instead, so nothing breaks on a laptop. Set it in the UI under Admin > Variables
   if you want real Slack messages.

   This closes the gap experiment 4 exposed: the pipeline failed correctly and
   told nobody.

2. ELT SCHEDULE STAGGERED TO 09:05
   Both DAGs were set to 0 9 * * 1-6 and both write to the same DuckDB file, which
   takes an exclusive lock. That is what made build_reports fail earlier with no
   data problem at all. max_active_runs=1 stops a DAG colliding with itself, not
   with its sibling. ELT is now 5 9 * * 1-6.

   Mention this in Part F and Part C. It is a real bug found by running the thing.

3. FRESHNESS CHECK HARDENED
   orders_freshness used max(order_ts). Seven orders in the export are dated 2027,
   so the maximum was always in the future and the file looked fresh forever.
   It now measures against the latest timestamp that is not in the future.
   Without this, experiment 9 could never fire.

--------------------------------------------------------------------
PART F TOOLKIT
--------------------------------------------------------------------

include/experiments.py sets up each scenario and restores afterwards. No hand
editing of CSVs, no re-downloading.

  py -3.12 include/experiments.py backup     <- DO THIS FIRST, once

Then for each experiment:

  py -3.12 include/experiments.py drift
  astro dev run dags test nammamart_etl_bhavani
  py -3.12 include/experiments.py restore

Scenarios: baddata (5), schema (6), volume (7), drift (8), stale (9), nofile (4)

ALWAYS restore before the next scenario, or you are testing two things at once.

include/query.py queries the warehouse in one line:

  py -3.12 include/query.py "select * from pipeline_runs order by start_time desc"

Store revenue share, the before/after for experiment 8:

  py -3.12 include/query.py "select store_id, round(sum(net_revenue),2) as rev, round(100.0*sum(net_revenue)/(select sum(net_revenue) from etl_fact_orders),1) as pct from etl_fact_orders where is_delivered group by 1 order by 2 desc"

Your baseline, already captured: S01 = 85,510.70 = 7.1%

--------------------------------------------------------------------
AFTER INSTALLING
--------------------------------------------------------------------

  astro dev run dags list-import-errors     should print nothing
  astro dev run dags test nammamart_etl_bhavani

Then carry on with Part F.
