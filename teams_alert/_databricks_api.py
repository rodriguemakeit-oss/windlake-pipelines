"""Shared helpers: Databricks CLI/API wrappers (idempotent lookups, updates)."""
from __future__ import annotations

import json
import subprocess
import urllib.error
import urllib.request
from pathlib import Path

from .config import EnvConfig


HERE = Path(__file__).resolve().parent


# ---------- file loaders ----------------------------------------------------

def load_sql(catalog: str) -> str:
    raw = (HERE / "sql" / "pdfs_non_traites.sql").read_text()
    return raw.replace("{{CATALOG}}", catalog)


def load_subject(env_name: str) -> str:
    raw = (HERE / "templates" / "subject.txt").read_text().rstrip("\n")
    return raw.replace("{{ENV}}", env_name)


def load_body(env_name: str) -> str:
    raw = (HERE / "templates" / "body.html").read_text()
    return raw.replace("{{ENV}}", env_name)


# ---------- CLI helpers -----------------------------------------------------

def _cli(args: list[str], profile: str) -> tuple[int, str, str]:
    r = subprocess.run(
        ["databricks", *args, "-p", profile, "--output", "json"],
        capture_output=True, text=True,
    )
    return r.returncode, r.stdout, r.stderr


def _cli_json(args: list[str], payload: dict | None, profile: str) -> dict:
    cmd = ["databricks", *args, "-p", profile]
    if payload is not None:
        cmd += ["--json", json.dumps(payload)]
    cmd += ["--output", "json"]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"databricks {' '.join(args)} failed: {r.stderr.strip()}")
    return json.loads(r.stdout) if r.stdout.strip() else {}


def get_token(profile: str) -> str:
    out = subprocess.run(
        ["databricks", "auth", "token", "-p", profile],
        capture_output=True, text=True, check=True,
    ).stdout
    return json.loads(out)["access_token"]


# ---------- idempotent lookups ---------------------------------------------

def find_query_id(display_name: str, profile: str) -> str | None:
    code, out, _ = _cli(["queries", "list"], profile)
    if code != 0:
        return None
    data = json.loads(out) if out.strip() else []
    items = data if isinstance(data, list) else data.get("results", [])
    for q in items:
        if q.get("display_name") == display_name:
            return q.get("id")
    return None


def find_alert_id(display_name: str, profile: str) -> str | None:
    code, out, _ = _cli(["alerts", "list"], profile)
    if code != 0:
        return None
    data = json.loads(out) if out.strip() else []
    items = data if isinstance(data, list) else data.get("results", [])
    for a in items:
        if a.get("display_name") == display_name:
            return a.get("id")
    return None


# ---------- upserts ---------------------------------------------------------

def upsert_query(env: EnvConfig, display_name: str, sql: str) -> str:
    """Create or update the SQL query. Returns query_id."""
    qid = find_query_id(display_name, env.profile)
    base = {
        "query_text": sql,
        "warehouse_id": env.warehouse_id,
        "catalog": env.catalog,
        "schema": "silver",
    }
    if qid is None:
        r = _cli_json(
            ["queries", "create"],
            {"query": {"display_name": display_name,
                       "parent_path": env.parent_path,
                       **base}},
            env.profile,
        )
        return r["id"]
    _cli_json(
        ["queries", "update", qid],
        {"update_mask": ",".join(base.keys()), "query": base},
        env.profile,
    )
    return qid


def upsert_alert(
    env: EnvConfig,
    display_name: str,
    query_id: str,
    column: str,
    threshold: float,
    seconds_to_retrigger: int,
    empty_result_state: str,
) -> str:
    """Create or update the alert (condition only). Returns alert_id."""
    aid = find_alert_id(display_name, env.profile)
    condition = {
        "op": "GREATER_THAN",
        "operand": {"column": {"name": column}},
        "threshold": {"value": {"double_value": float(threshold)}},
        "empty_result_state": empty_result_state.upper(),
    }
    if aid is None:
        r = _cli_json(
            ["alerts", "create"],
            {"alert": {
                "display_name": display_name,
                "query_id": query_id,
                "parent_path": env.parent_path,
                "condition": condition,
                "seconds_to_retrigger": seconds_to_retrigger,
            }},
            env.profile,
        )
        return r["id"]
    _cli_json(
        ["alerts", "update", aid],
        {"update_mask": "condition,seconds_to_retrigger,query_id",
         "alert": {"condition": condition,
                   "seconds_to_retrigger": seconds_to_retrigger,
                   "query_id": query_id}},
        env.profile,
    )
    return aid


def set_alert_template(
    env: EnvConfig,
    alert_id: str,
    display_name: str,
    query_id: str,
    column: str,
    threshold: float,
    empty_result_state: str,
    custom_subject: str,
    custom_body: str,
) -> None:
    """Set custom_subject + custom_body via the legacy alerts API.

    NOTE: legacy PUT also overwrites condition; we re-include matching values
    here, then the v2 upsert_alert is re-applied right after to enforce
    numeric threshold (string/double dance).
    """
    token = get_token(env.profile)
    url = f"{env.host}/api/2.0/preview/sql/alerts/{alert_id}"
    payload = {
        "name": display_name,
        "query_id": query_id,
        "options": {
            "column": column,
            "op": ">",
            "value": str(int(threshold)),
            "empty_result_state": empty_result_state,
            "custom_subject": custom_subject,
            "custom_body": custom_body,
            "notify_on_ok": False,
        },
    }
    req = urllib.request.Request(
        url, data=json.dumps(payload).encode("utf-8"),
        headers={"Authorization": f"Bearer {token}",
                 "Content-Type": "application/json"},
        method="PUT",
    )
    with urllib.request.urlopen(req) as resp:
        if resp.status != 200:
            raise RuntimeError(f"legacy PUT failed: HTTP {resp.status}")


# ---------- deletion --------------------------------------------------------

def delete_alert(alert_id: str, profile: str) -> None:
    subprocess.run(
        ["databricks", "alerts", "delete", alert_id, "-p", profile],
        check=False,  # may already be deleted
    )


def delete_query(query_id: str, profile: str) -> None:
    subprocess.run(
        ["databricks", "queries", "delete", query_id, "-p", profile],
        check=False,
    )
