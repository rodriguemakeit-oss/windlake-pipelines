"""LLM disambiguation of report turbine labels that map to several Telemetry serials.

Some report labels (e.g. ``A01``) are reused across several wind farms in the
same country, so ``(country, report_turbine_name)`` resolves to more than one
Telemetry serial. The report *file name* carries the farm/site, so an LLM picks the
correct serial out of a **closed candidate list**, abstaining (``None``) when it
is not sure rather than guessing.

Results are cached in ``silver.turbine_alias_llm_resolution`` keyed by
``(country_pic, report_turbine_name, file_key)`` so a given site is resolved only
once: subsequent runs (and reruns/backfills) are a plain join, no LLM call.
"""

import hashlib
import json
from typing import Dict, List


def candidates_hash(candidates: List[Dict]) -> str:
    """Stable hash of the candidate set.

    Stored alongside each cache entry so the cache is invalidated automatically
    if the SGRE mapping changes the candidate serials/sites for a key.
    """
    items = sorted(
        (str(c["serial"]), str(c.get("farm") or ""), str(c.get("farm_name") or ""))
        for c in candidates
    )
    payload = json.dumps(items, ensure_ascii=False).encode("utf-8")
    return hashlib.sha1(payload).hexdigest()


SYSTEM_PROMPT = (
    "You map a wind-turbine availability report to the correct Telemetry turbine "
    "serial number. The report turbine label is ambiguous because the same label "
    "is reused across several wind farms in the same country. You are given the "
    "report file name and a closed list of candidate sites, each with its Telemetry "
    "serial number, human-readable site name, and internal farm code. Pick the "
    "candidate whose SITE NAME best matches the site referenced in the file name "
    "(the file name uses the real site name, e.g. 'Serra do Ralo'; the farm code "
    "is cryptic, e.g. 'WIF_PTSED').\n"
    "Rules:\n"
    "- Only answer with a serial that appears in the candidate list.\n"
    "- If you are not sure which candidate the file refers to, return null. "
    "Never guess.\n"
    'Respond with STRICT JSON only: '
    '{"serial": <serial string or null>, "matched_site": <site name or null>, '
    '"reason": <short explanation>}.'
)


def build_user_prompt(country: str, label: str, file_name: str, candidates: List[Dict]) -> str:
    """Render the user message: country, label, file name and candidate list."""
    lines = [
        f"- serial={c['serial']!r}, site_name={c.get('farm_name')!r}, farm_code={c.get('farm')!r}"
        for c in candidates
    ]
    return (
        f"Country: {country}\n"
        f"Report turbine label: {label}\n"
        f"Report file name: {file_name}\n"
        "Candidates:\n" + "\n".join(lines)
    )


def _extract_json(text: str) -> str:
    """Pull the first JSON object out of a possibly code-fenced LLM response."""
    t = (text or "").strip()
    if t.startswith("```"):
        t = t.strip("`")
        if t.lower().startswith("json"):
            t = t[4:]
    start, end = t.find("{"), t.rfind("}")
    return t[start : end + 1] if start != -1 and end != -1 else t


def resolve_one(client, model: str, country: str, label: str, file_name: str,
                candidates: List[Dict]) -> Dict:
    """Resolve one ambiguous (label, file) pair to a Telemetry serial.

    ``client`` is an ``openai.AzureOpenAI`` instance.

    Returns ``{"serial": str | None, "reason": str}``. Abstains (serial=None) on
    any uncertainty, JSON/parse error, or if the model returns a serial that is
    not in the candidate list — never guesses.
    """
    valid_serials = {str(c["serial"]) for c in candidates}
    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": build_user_prompt(country, label, file_name, candidates)},
            ],
        )
        data = json.loads(_extract_json(resp.choices[0].message.content))
        serial = data.get("serial")
        serial = str(serial) if serial is not None else None
        if serial is not None and serial not in valid_serials:
            return {"serial": None, "reason": f"LLM returned out-of-list serial {serial!r}"}
        return {"serial": serial, "reason": str(data.get("reason", ""))[:500]}
    except Exception as exc:  # noqa: BLE001 — abstain on any failure, never guess
        return {"serial": None, "reason": f"LLM error: {exc}"}
