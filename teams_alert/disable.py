"""Disable (delete) the LTSA "PDFs non traités" alert + query for an environment.

Usage:
    python -m teams_alert.disable preprod
    python -m teams_alert.disable prod
"""
from __future__ import annotations

import sys

from .config import ALERT_NAME, ENVIRONMENTS, QUERY_NAME
from ._databricks_api import (
    delete_alert,
    delete_query,
    find_alert_id,
    find_query_id,
)


def disable(env_name: str) -> None:
    if env_name not in ENVIRONMENTS:
        raise SystemExit(f"unknown env '{env_name}' (allowed: {list(ENVIRONMENTS)})")
    env = ENVIRONMENTS[env_name]

    display_query = f"{QUERY_NAME} ({env.name})"
    display_alert = f"{ALERT_NAME} ({env.name})"

    alert_id = find_alert_id(display_alert, env.profile)
    if alert_id:
        print(f"[{env.name}] deleting alert {alert_id}")
        delete_alert(alert_id, env.profile)
    else:
        print(f"[{env.name}] no alert named '{display_alert}'")

    query_id = find_query_id(display_query, env.profile)
    if query_id:
        print(f"[{env.name}] deleting query {query_id}")
        delete_query(query_id, env.profile)
    else:
        print(f"[{env.name}] no query named '{display_query}'")

    print(f"✅ {env.name}: disabled.")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: python -m teams_alert.disable <preprod|prod>")
    disable(sys.argv[1])
