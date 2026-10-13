HOTFIX 2: run_audit failure
===========================

Only dags/nammamart_etl_bhavani.py changes.

WHAT WAS WRONG
  run_audit called dag_run.get_task_instances(), which the Airflow 3 task SDK
  does not expose the way Airflow 2 did. It also mixed a timezone aware
  start_time with a naive end_time.

WHAT CHANGED
  - run status is now inferred from whether the load produced rows, instead of
    depending on an API that varies between Airflow versions
  - start_time is normalised to naive UTC before writing
  - dag_run is read defensively with .get() and getattr()

INSTALL (from inside your repo folder)

  tar -xf "%USERPROFILE%\Downloads\nammamart_hotfix_runaudit.zip" --strip-components=1

RE-RUN

  astro dev run dags test nammamart_etl_bhavani

EXPECT TO SEE
  [transform] fact_orders: 2310 rows        (or 2460 with the increment file)
  [transform] net revenue: 1,135,628.80     (or 1,131,435.10 with the increment)
  [load] etl_fact_orders: {'incoming': 2310, 'updated': 0, 'inserted': 2310, ...}
  [reconcile] 152 mismatches
  [dq] 44 checks, N not passing
  then one pipeline_runs row printed at the end

If it loops on "No tasks to run / up_for_retry", press Ctrl+C. That is retries
waiting out the 5 minute retry_delay, which is Part C working as configured.
