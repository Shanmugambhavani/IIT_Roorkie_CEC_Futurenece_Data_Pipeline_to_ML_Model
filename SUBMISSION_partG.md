# Part G: Recommendation to Sameer, CTO

**Subject: ETL or ELT for NammaMart**

Sameer,

Build ELT. Keep the ETL validation logic, but run it as SQL inside the warehouse
rather than in Python before it.

Both pipelines now run against your September data and agree on every order:
2,460 rows, 12,11,087.30 total, 11,31,435.10 for September. A row-by-row join
comparing revenue returns zero differences. So this is not a correctness question.
It is a question about which design you can still operate in two years.

## What the numbers already told us

Before either pipeline existed, profiling found issues in all six files. The
current morning report overstates September revenue by **83,392, about 6.8%**.
Not rounding: duplicate orders, hours logged as minutes, discounts above 100%, and
orders pointing at stores that do not exist. 102 of 2,412 order rows fail at least
one critical rule.

That number alone pays for this project.

## Auditability: the deciding factor

Finance currently spends half a day a week finding out why order revenue does not
match the gateway. The question behind that work is always the same: "where did
this number come from?"

ELT answers it with SQL. Take any figure on `mart_daily_store_kpis`, join to
`v_fact_orders`, then `stg_orders` for the quality verdict, then `raw_orders` for
the row exactly as the file delivered it, with `_source_file` and `_run_id`
attached. Four queries, no code, no filesystem access.

ETL answers it by sending someone to a CSV in a quarantine folder named after a
run id. The answer is correct and the route to it is not something finance can
walk unaided.

The sharpest version of this is your own Part E9 question. An order delivered on
3 September and returned on 1 October: both pipelines now show September at
11,31,435.10. But "what did we report to the board on 1 October" is answerable
only in ELT, by filtering `raw_orders` on `_loaded_at`. In ETL that prior state is
gone from the warehouse the moment the correction loads.

## Data quality guarantees: a tie, deliberately

Your instinct was that ETL protects finance better. It does not, because the
protection does not come from where the cleaning happens. It comes from the rules.

Both pipelines run the same 40 checks, under the same names, with the same
severities, because both read `check_catalog` from one Python registry. ETL rejects
rows before the warehouse; ELT marks them `dq_status = 'QUARANTINE'` and the mart
filters them. Nothing bad reaches the CFO's dashboard either way.

ELT keeps the rejected rows **inside the database**, which is strictly better. The
bad row is a queryable row, not a file someone has to find.

One design decision applies to both and is worth your attention. An early version
quarantined any dimension row that failed a critical rule. One store with a blank
zone and ten imperfect product rows removed **316 orders** from the fact table.
Revenue disappearing because of a missing lookup value is the failure you hired us
to remove, so the rule is now: repair dimensions, reject facts. Today 85,382 of
revenue sits under zone "Unknown" and 41,911 under category "Uncategorised". Both
are visible and chaseable instead of silently gone.

## Ease of change

Your analysts know SQL. They do not know pandas.

A new metric in ELT is a new SQL file and a task that runs it. Your analytics team
can write it, and they can read every existing transformation without asking an
engineer what a `.pipe()` chain does.

A new metric in ETL is a change to Python that must be reviewed, tested and
deployed by whoever owns the DAG. With no data engineers on staff, that is a
bottleneck with one person in it.

ELT also lets you answer questions you have not thought of yet. Raw holds every
column of every row ever received. When someone asks in March what happened to
cancelled orders in September, the data is there. In ETL, only what the pipeline
was told to keep survived.

## Performance at 1000x

At 2,400 orders a month both finish in under 25 seconds. At 2.4 million a month
the picture changes.

ETL pulls everything into pandas memory on one machine. At 1000x the current
volume, `orders.csv` alone is several GB and the validate step becomes a memory
problem long before it becomes a speed problem. The fix is chunking or Spark,
which is a rewrite.

ELT pushes the work to the database engine, which was built for it. DuckDB handles
single-node workloads far past 1000x, and when you outgrow it the staging and mart
SQL ports to Snowflake, BigQuery or Postgres largely unchanged, because it is just
SQL. The raw load is the only part that needs rewriting.

Cost follows the same shape. Both are open source and run on one container today.
ETL's growth path spends money on bigger machines and a distributed framework;
ELT's spends it on warehouse compute you can turn off between runs. ELT also stores
more, since raw is retained for seven years, but object storage is the cheapest
thing in the stack and far cheaper than the engineering time ETL's rewrite would
cost.

## The recommendation

**Adopt ELT. Keep ETL as the reference implementation** until the ELT marts have
been reconciled against it for a full month, then retire it. The comparison query
runs in seconds; leave it in the DAG as a permanent regression test even after ETL
is gone.

## Top 3 risks

**1. The schedule is only as reliable as the machine it runs on.** Today both DAGs
run in Docker on a laptop. `catchup=False` means a missed 9 AM run is never
executed, and the only evidence is a gap in `pipeline_runs` that nobody is paged
about. A 9 AM SLA needs always-on infrastructure, a small cloud VM or managed
Airflow, plus the Slack alert on failure that `include/slack_notifier.py` is
already written for but not yet wired to `on_failure_callback`.
*Mitigation: cloud deployment and failure alerting before go-live. Two days.*

**2. Raw holds unmasked personal data, and DuckDB has no access control.** Marts
are masked; `raw_customers` is not, and it must not be, because that is what makes
replay and correction possible. DuckDB is a file, so "who can read raw" is a
filesystem permission, not a grant. Under the DPDP Act, NammaMart is a Data
Fiduciary with a duty of reasonable security safeguards. There is also no
documented erasure procedure, which sits awkwardly against an append-only raw layer.
*Mitigation: move raw to a warehouse with row and column level security, and write
the erasure runbook, before customer volume grows.*

**3. Distribution drift is invisible.** The weakest of the five observability
pillars by a distance. Multiply one store's `unit_price` by 100 and every check
passes: quantity positive, formula holds, status valid, keys resolve. Row-level
rules cannot catch a dataset that is wrong in aggregate while being correct row by
row. The mart would report that store at 85% of company revenue and nothing would
flag it.
*Mitigation: statistical checks against each store's own rolling history, built
from `dq_results` once a few weeks of baseline has accumulated.*

## What I would do first

In this order, and none of it takes long:

1. Wire `on_failure_callback` to the Slack notifier. Half a day, and it closes the
   single largest gap, which is that a failed 9 AM run currently tells nobody.
2. Deploy to an always-on host. Two days.
3. Get answers to the three open client questions, especially whether the six
   negative-quantity orders are refunds or errors. That decision moves September
   revenue and no engineer should make it.
4. Snapshot the mart per run, so "what did we report in October" stops being
   unanswerable before the next board meeting.

The six-week test of whether this worked is not whether the pipeline is green. It
is whether anyone in finance still spends half a day a week on reconciliation.

— Bhavani, FDE
