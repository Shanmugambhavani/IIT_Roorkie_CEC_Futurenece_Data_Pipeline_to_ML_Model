"""
NammaMart: failure alerting.

Wraps the trainer's include/slack_notifier.py as an Airflow callback.

This closes the gap Part F experiment 4 exposed. When products.csv went missing,
the pipeline behaved correctly: it retried twice, failed, blocked everything
downstream and left a red run in the UI. And nobody was told. The failure was
visible to anyone who went looking, which is not the same as detected.

The webhook URL is read with Variable.get, never hardcoded. With no variable set,
slack_notifier falls back to printing the alert, so the pipeline still runs on a
laptop with no Slack workspace attached. That fallback is why this is safe to wire
into both DAGs by default.
"""

from __future__ import annotations

import sys

sys.path.insert(0, "/usr/local/airflow/include")

from slack_notifier import send_slack_alert  # noqa: E402


def _get_webhook() -> str | None:
    """Airflow Variable first, then the environment. Never a literal in code."""
    try:
        from airflow.models import Variable
        return Variable.get("SLACK_WEBHOOK_URL", default_var=None)
    except Exception:
        return None


def dag_failure_alert(context) -> None:
    """
    Attach via default_args["on_failure_callback"]. Airflow calls it once per
    failed task instance, after the final retry is exhausted.

    The message names the task, the run and the log URL, because an alert that
    only says "the pipeline failed" still leaves someone hunting.
    """
    ti = context.get("task_instance")
    dag_run = context.get("dag_run")
    exception = context.get("exception")

    dag_id = getattr(ti, "dag_id", "unknown")
    task_id = getattr(ti, "task_id", "unknown")
    try_number = getattr(ti, "try_number", "?")
    run_id = getattr(dag_run, "run_id", "unknown")
    log_url = getattr(ti, "log_url", "")

    message = (
        f":rotating_light: NammaMart pipeline failure\n"
        f"DAG: {dag_id}\n"
        f"Task: {task_id} (attempt {try_number})\n"
        f"Run: {run_id}\n"
        f"Error: {str(exception)[:400]}\n"
        f"Log: {log_url}\n"
        f"The 9:00 AM report may be late. Check before the 9:30 stand-up."
    )

    send_slack_alert(message, webhook_url=_get_webhook())


def dq_warning_alert(dag_id: str, failed_checks) -> None:
    """
    For a run that succeeds but whose quality checks did not. A green DAG with 18
    rows failing a Critical rule is exactly the situation nobody notices, because
    the only signal is a table nobody opens.
    """
    if not len(failed_checks):
        return

    lines = [f"  {r['check_name']}: {r['rows_failed']} rows ({r['severity']})"
             for _, r in failed_checks.iterrows()]
    message = (
        f":warning: NammaMart data quality\n"
        f"DAG: {dag_id}\n"
        f"{len(failed_checks)} checks did not pass:\n" + "\n".join(lines[:15])
    )
    send_slack_alert(message, webhook_url=_get_webhook())
