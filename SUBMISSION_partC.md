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
