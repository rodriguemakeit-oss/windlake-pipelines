"""Apply (create or update) the LTSA "PDFs non traités" alert for an environment.

Usage:
    python -m teams_alert.apply preprod
    python -m teams_alert.apply prod

Idempotent: lookups by display_name, so re-running just updates.
Does NOT add the destination or schedule (Databricks SQL Alerts V2 schedules
are managed by hidden Jobs not exposed via the public API). After the first
apply, finish setup in the UI:
  - Add destination: see ENVIRONMENTS[<env>].destination_id
  - Add schedule:    Every 1 day at chosen UTC time
"""
from __future__ import annotations

import sys

from .config import (
    ALERT_NAME,
    EMPTY_RESULT_STATE,
    ENVIRONMENTS,
    QUERY_NAME,
    SECONDS_TO_RETRIGGER,
    THRESHOLD,
)
from ._databricks_api import (
    load_body,
    load_sql,
    load_subject,
    set_alert_template,
    upsert_alert,
    upsert_query,
)


CONDITION_COLUMN = "n_pdfs"  # last column from sql/pdfs_non_traites.sql (referenced by name)


def apply(env_name: str) -> None:
    if env_name not in ENVIRONMENTS:
        raise SystemExit(f"unknown env '{env_name}' (allowed: {list(ENVIRONMENTS)})")
    env = ENVIRONMENTS[env_name]

    display_query = f"{QUERY_NAME} ({env.name})"
    display_alert = f"{ALERT_NAME} ({env.name})"

    sql = load_sql(env.catalog)
    subject = load_subject(env.name)
    body = load_body(env.name)

    print(f"[{env.name}] upserting query …")
    query_id = upsert_query(env, display_query, sql)
    print(f"[{env.name}]   query_id = {query_id}")

    print(f"[{env.name}] upserting alert (condition + retrigger) …")
    alert_id = upsert_alert(
        env=env,
        display_name=display_alert,
        query_id=query_id,
        column=CONDITION_COLUMN,
        threshold=THRESHOLD,
        seconds_to_retrigger=SECONDS_TO_RETRIGGER,
        empty_result_state=EMPTY_RESULT_STATE,
    )
    print(f"[{env.name}]   alert_id = {alert_id}")

    print(f"[{env.name}] setting custom subject + body (legacy API) …")
    set_alert_template(
        env=env,
        alert_id=alert_id,
        display_name=display_alert,
        query_id=query_id,
        column=CONDITION_COLUMN,
        threshold=THRESHOLD,
        empty_result_state=EMPTY_RESULT_STATE,
        custom_subject=subject,
        custom_body=body,
    )

    # Legacy PUT clobbers the v2 numeric threshold; re-apply it.
    print(f"[{env.name}] re-enforcing v2 numeric threshold …")
    upsert_alert(
        env=env,
        display_name=display_alert,
        query_id=query_id,
        column=CONDITION_COLUMN,
        threshold=THRESHOLD,
        seconds_to_retrigger=SECONDS_TO_RETRIGGER,
        empty_result_state=EMPTY_RESULT_STATE,
    )

    print()
    print(f"✅ {env.name}: alert ready.")
    print(f"   Alert URL: {env.host}/sql/alerts/{alert_id}")
    print()
    print("Manual steps in the UI (V2 alerts schedules/destinations are not API-exposed):")
    print(f"  1. Add destination: {env.destination_id}  (LTSA Teams)")
    print("  2. Add schedule: Every 1 day at desired UTC time")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: python -m teams_alert.apply <preprod|prod>")
    apply(sys.argv[1])
