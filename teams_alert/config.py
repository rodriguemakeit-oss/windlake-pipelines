"""Environment configuration for the LTSA Teams alert (declarative IaaC)."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class EnvConfig:
    name: str                    # 'preprod' | 'prod'
    profile: str                 # databricks CLI profile
    host: str                    # workspace URL
    warehouse_id: str            # SQL warehouse running the alert query
    catalog: str                 # UC catalog containing silver.*
    destination_id: str          # notification destination id (Teams email)
    parent_path: str             # workspace folder owning the alert + query


ENVIRONMENTS: dict[str, EnvConfig] = {
    "preprod": EnvConfig(
        name="preprod",
        profile="preprod",
        host="https://workspace.example.invalid",
        warehouse_id="1000000000000000",       # ltsa_warehouse
        catalog="ltsa_dev",
        destination_id="00000000-0000-0000-0000-000000000000",  # LTSA Teams preprod
        parent_path="/Workspace/Users/contributor@example.com",
    ),
    "prod": EnvConfig(
        name="prod",
        profile="prod",
        host="https://workspace.example.invalid",
        warehouse_id="1000000000000000",       # Serverless Starter Warehouse
        catalog="ltsa_prod",
        destination_id="00000000-0000-0000-0000-000000000000",  # LTSA Teams prod
        parent_path="/Workspace/Users/contributor@example.com",
    ),
}


# Logical resource names (used as display_name for idempotent lookups)
QUERY_NAME = "LTSA - PDFs non traités"
ALERT_NAME = "LTSA — PDFs non traités"

# Alert behaviour
SECONDS_TO_RETRIGGER = 60   # re-notify on every scheduled eval while TRIGGERED
EMPTY_RESULT_STATE = "ok"   # query returns 0 rows => state=OK (no notif)
THRESHOLD = 0               # n_pdfs > 0 triggers
