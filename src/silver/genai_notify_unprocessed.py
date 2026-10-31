# Databricks notebook source
# MAGIC %md
# MAGIC # GenAI — Notify Unprocessed Files (Teams)
# MAGIC
# MAGIC Posts a Teams Adaptive Card listing reports that did NOT make it through the
# MAGIC GenAI availability pipeline, so operators can act on them.
# MAGIC
# MAGIC **Two categories**
# MAGIC 1. **On SharePoint but never extracted** — present in the source folder but
# MAGIC    absent from `silver.availability_genai` (failed extraction or never picked up).
# MAGIC    Enriched with the latest quarantine error (`silver.files_genai_quarantine`).
# MAGIC 2. **Extracted but not matched to Telemetry** — present in
# MAGIC    `silver.availability_genai_telemetry_no_match`.
# MAGIC
# MAGIC Files are grouped by **age bucket** (how long they have been stuck):
# MAGIC `> 15 jours`, `8–15 jours`, `2–7 jours`, `≤ 1 jour`.
# MAGIC
# MAGIC The notification is sent only when there is at least one file to report.
# MAGIC Runs after `genai_availability_match` in the pipeline DAG.

# COMMAND ----------

import sys
import os
import json
from datetime import datetime, timezone

import requests
from pyspark.sql import functions as F

from helpers.volume_adapter import VolumeAdapter

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration (widgets)

# COMMAND ----------


def _widget(name: str, default: str = "") -> str:
    """Read a widget defensively — returns ``default`` if the widget is absent."""
    try:
        val = dbutils.widgets.get(name)
        return val if val is not None else default
    except Exception:
        return default


CATALOG = _widget("catalog")
SILVER = f"{CATALOG}.silver"

# Source of truth for "files present" = the S3-backed raw Volume populated by the
# SharePoint→S3 download task. We trust that mirror instead of calling the
# SharePoint Graph API directly (slow + auth-heavy).
REPORTS_VOLUME_PATH = _widget(
    "reports_volume_path", f"/Volumes/{CATALOG}/raw/maintainer_reports"
)

TEAMS_WEBHOOK_URL = _widget("teams_webhook_url")

try:
    ALLOWED_EXT = set(json.loads(_widget("allowed_file_extensions", '["pdf", "xlsx"]')))
except Exception:
    ALLOWED_EXT = {"pdf", "xlsx"}

# Max file lines printed in the card before collapsing into a "+N autres" line.
try:
    MAX_LINES = int(_widget("max_lines", "60"))
except Exception:
    MAX_LINES = 60

print(f"   Catalog        : {CATALOG}")
print(f"   Reports volume : {REPORTS_VOLUME_PATH}")
print(f"   Allowed ext    : {sorted(ALLOWED_EXT)}")
print(f"   Max lines      : {MAX_LINES}")
print(f"   Webhook set    : {bool(TEAMS_WEBHOOK_URL)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers

# COMMAND ----------


def _parse_file_path(file_path: str) -> dict:
    """Extract metadata from the source file path.

    Expected format: ``<country>/<maintainer>/<year>/<month>/<filename>.<ext>``.
    Month segment may be a plain integer ('01') or have a label suffix ('01-JAN').
    """
    parts = file_path.replace("\\", "/").split("/")
    ext = file_path.rsplit(".", 1)[-1].lower() if "." in file_path else "unknown"
    _month_raw = parts[3].split("-")[0] if len(parts) > 3 else ""
    return {
        "country": parts[0] if len(parts) > 0 else "unknown",
        "maintainer": parts[1] if len(parts) > 1 else "unknown",
        "year": int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0,
        "month": int(_month_raw) if _month_raw.isdigit() else 0,
        "filename": parts[-1] if parts else file_path,
        "extension": ext,
    }


def _classify_error(error: str) -> str:
    """Map a raw quarantine error message to a short human-readable cause."""
    if not error:
        return "jamais tenté"
    e = error.lower()
    if "429" in e or "too many requests" in e:
        return "429 rate-limit"
    if "401" in e or "unauthorized" in e:
        return "401 SharePoint"
    if "positional indexers" in e or "out-of-bounds" in e:
        return "df vide (OCR/SharePoint)"
    if "0 availability rows" in e or "empty or unparsable" in e:
        return "0 ligne extraite"
    if "future-dated" in e or "hallucination" in e:
        return "date future (hallucination LLM)"
    if "invalid date" in e:
        return "date invalide LLM"
    if "pickle" in e:
        return "pickle/APIStatusError"
    return error[:50]


def _age_bucket(first_seen) -> tuple:
    """Return (sort_key, label) for an age bucket based on ``first_seen``.

    Lower sort_key = older = more urgent. Unknown first_seen → treated as recent.
    """
    if first_seen is None:
        return (4, "🟢 ≤ 1 jour / jamais tenté")
    now = datetime.now(timezone.utc)
    fs = first_seen
    if fs.tzinfo is None:
        fs = fs.replace(tzinfo=timezone.utc)
    days = (now - fs).days
    if days > 15:
        return (1, "🔴 > 15 jours")
    if days > 7:
        return (2, "🟠 8–15 jours")
    if days > 1:
        return (3, "🟡 2–7 jours")
    return (4, "🟢 ≤ 1 jour (récent)")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Catégorie A — présents dans le S3 (raw) mais jamais extraits

# COMMAND ----------

print("[A] List raw S3 volume + diff against availability_genai")

# Trust the SharePoint→S3 download task: list the raw Volume (POSIX path on the
# cluster, recursive) rather than calling SharePoint directly.
reports_adapter = VolumeAdapter(REPORTS_VOLUME_PATH)
all_files = reports_adapter.list_files()
source_files = {
    f for f in all_files if f.rsplit(".", 1)[-1].lower() in ALLOWED_EXT
}
print(f"   • {len(source_files)} candidate files in raw S3 volume")

# Files that produced at least one availability row = successfully extracted
extracted_files = {
    r["file"]
    for r in spark.table(f"{SILVER}.availability_genai").select("file").distinct().collect()
}
print(f"   • {len(extracted_files)} files successfully extracted")

not_extracted = sorted(source_files - extracted_files)
print(f"   • {len(not_extracted)} in S3 but NOT extracted")

# Latest quarantine error + first time the file was seen failing
quarantine_info = {}
try:
    q = (
        spark.table(f"{SILVER}.files_genai_quarantine")
        .groupBy("file")
        .agg(
            F.min("run_date").alias("first_seen"),
            F.max_by("error", "run_date").alias("last_error"),
        )
    )
    for r in q.collect():
        quarantine_info[r["file"]] = (r["first_seen"], r["last_error"])
except Exception as e:
    print(f"   ⚠️  quarantine table unavailable: {e}")

cat_a = []
for f in not_extracted:
    first_seen, last_error = quarantine_info.get(f, (None, None))
    meta = _parse_file_path(f)
    cat_a.append(
        {
            "file": f,
            "filename": meta["filename"],
            "country": meta["country"],
            "cause": _classify_error(last_error),
            "first_seen": first_seen,
        }
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catégorie B — extraits mais non matchés Telemetry

# COMMAND ----------

print("[B] Read availability_genai_telemetry_no_match")

cat_b = []
try:
    nm = spark.table(f"{SILVER}.availability_genai_telemetry_no_match")
    nm_cols = set(nm.columns)
    label_col = next(
        (c for c in ("report_turbine_name", "turbine_id", "wind_turbine") if c in nm_cols),
        None,
    )
    agg = [F.min("run_date").alias("first_seen")] if "run_date" in nm_cols else []
    grp = nm.groupBy("file").agg(F.count(F.lit(1)).alias("n_turbines"), *agg)
    for r in grp.collect():
        d = r.asDict()
        meta = _parse_file_path(d["file"])
        cat_b.append(
            {
                "file": d["file"],
                "filename": meta["filename"],
                "country": meta["country"],
                "cause": f"{d.get('n_turbines', 0)} turbine(s) non matchée(s)",
                "first_seen": d.get("first_seen"),
            }
        )
    print(f"   • {len(cat_b)} files extracted but not matched to Telemetry")
except Exception as e:
    print(f"   ⚠️  no_match table unavailable: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Construction de la carte Adaptive

# COMMAND ----------


def _group_by_bucket(items):
    """Group items by age bucket → ordered list of (label, [items]) oldest first."""
    buckets = {}
    for it in items:
        key, label = _age_bucket(it["first_seen"])
        buckets.setdefault((key, label), []).append(it)
    return [
        (label, sorted(rows, key=lambda r: (r["country"], r["filename"])))
        for (key, label), rows in sorted(buckets.items(), key=lambda kv: kv[0][0])
    ]


def _section_blocks(title, color, items, lines_budget):
    """Build TextBlocks for one category, respecting a shared line budget."""
    blocks = [
        {
            "type": "TextBlock",
            "weight": "Bolder",
            "size": "Medium",
            "color": color,
            "text": f"{title} — {len(items)} fichier(s)",
            "spacing": "Medium",
        }
    ]
    if not items:
        blocks.append({"type": "TextBlock", "isSubtle": True, "text": "_aucun_", "wrap": True})
        return blocks, lines_budget

    for label, rows in _group_by_bucket(items):
        blocks.append(
            {"type": "TextBlock", "weight": "Bolder", "text": label, "spacing": "Small"}
        )
        detail = []
        for r in rows:
            if lines_budget <= 0:
                detail.append("… (voir la table de quarantaine)")
                break
            detail.append(f"• [{r['country']}] {r['filename']}  ({r['cause']})")
            lines_budget -= 1
        blocks.append({"type": "TextBlock", "wrap": True, "text": "\n".join(detail)})
        if lines_budget <= 0:
            break
    return blocks, lines_budget


total = len(cat_a) + len(cat_b)
today = datetime.now(timezone.utc).date().isoformat()

body = [
    {
        "type": "TextBlock",
        "size": "Large",
        "weight": "Bolder",
        "text": f"⚠️ LTSA · Rapports non traités — {today}",
    },
    {
        "type": "TextBlock",
        "wrap": True,
        "text": (
            f"**{total}** rapport(s) à traiter · "
            f"**{len(cat_a)}** dans S3 non extrait(s) · "
            f"**{len(cat_b)}** extrait(s) non matché(s) Telemetry · catalogue `{CATALOG}`"
        ),
    },
]

budget = MAX_LINES
blocks_a, budget = _section_blocks(
    "📥 Dans le S3 mais non extraits", "Attention", cat_a, budget
)
blocks_b, budget = _section_blocks(
    "🔗 Extraits mais non matchés Telemetry", "Warning", cat_b, budget
)
body.extend(blocks_a)
body.extend(blocks_b)

card = {
    "type": "message",
    "attachments": [
        {
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "type": "AdaptiveCard",
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "version": "1.4",
                "body": body,
            },
        }
    ],
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Envoi Teams (seulement si count > 0)

# COMMAND ----------

result = {"total": total, "cat_a": len(cat_a), "cat_b": len(cat_b), "notified": False}

if total == 0:
    print("   ✓ Aucun fichier non traité — pas de notification.")
elif not TEAMS_WEBHOOK_URL:
    print("   ⚠️  teams_webhook_url non renseigné — notification ignorée.")
else:
    try:
        resp = requests.post(TEAMS_WEBHOOK_URL, json=card, timeout=30)
        if resp.status_code >= 300:
            print(f"   ⚠️  HTTP {resp.status_code}: {resp.text[:300]}")
            result["http_status"] = resp.status_code
        else:
            print(f"   ✓ Notification Teams envoyée ({total} fichiers)")
            result["notified"] = True
            result["http_status"] = resp.status_code
    except Exception as e:
        print(f"   ⚠️  Échec de l'envoi: {e}")
        result["error"] = str(e)

# COMMAND ----------

result["status"] = "SUCCESS"
print(json.dumps(result, indent=2, default=str))
dbutils.notebook.exit(json.dumps(result, default=str))
