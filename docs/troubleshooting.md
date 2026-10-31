# Troubleshooting — ltsa_data

## 1. Reprocess a stuck or badly extracted GenAI file

A PDF/Excel file is skipped on subsequent runs if it is present in `silver.files_genai_processed`. To force a reprocess:

```sql
-- Check that the file is recorded
SELECT * FROM ltsa_dev.silver.files_genai_processed
WHERE file LIKE '%<file_name>%';

-- Remove only this file from the history
DELETE FROM ltsa_dev.silver.files_genai_processed
WHERE file = '<exact_file_path>';
```

On the next run the pipeline will reprocess this file normally.

> To also discard the data already extracted from this file, delete the matching rows in `silver.availability_genai` (filter on `file = '<exact_path>'`).

---

## 2. Re-run the pipeline for a past date (backfill)

By default, notebooks use the current date. To reprocess a past month, pass the `run_date` parameter in ISO 8601 format:

```bash
# Via CLI — dev target
databricks bundle run -t dev ltsa_pipeline_wrapper \
  --job-parameters '{"run_date": "2026-03-01"}'

# Via CLI — noprod target
databricks bundle run -t noprod ltsa_pipeline_wrapper \
  --job-parameters '{"run_date": "2026-03-01"}'
```

Via the Databricks UI: Workflows → `ltsa_pipeline_wrapper` → Run now → Add parameter → `run_date = 2026-03-01`.

---

## 3. Service Principal permission errors (Unity Catalog)

If a job fails with `AnalysisException: User does not have privilege` or `PERMISSION_DENIED`, replay the relevant `GRANT` statements.

**NOPROD** — SP `00000000-0000-0000-0000-000000000000`:

```sql
GRANT USE CATALOG ON CATALOG ltsa_dev
  TO `00000000-0000-0000-0000-000000000000`;

GRANT USE SCHEMA, CREATE TABLE, MODIFY ON SCHEMA ltsa_dev.silver
  TO `00000000-0000-0000-0000-000000000000`;

GRANT USE SCHEMA, CREATE TABLE, MODIFY ON SCHEMA ltsa_dev.gold
  TO `00000000-0000-0000-0000-000000000000`;

GRANT ACCESS ON EXTERNAL LOCATION rfgbu_ltsa_secrets_service_credential_noprod
  TO `00000000-0000-0000-0000-000000000000`;
```

**PROD** — SP `00000000-0000-0000-0000-000000000000`:

```sql
GRANT USE CATALOG ON CATALOG ltsa_prod
  TO `00000000-0000-0000-0000-000000000000`;

GRANT USE SCHEMA, CREATE TABLE, MODIFY ON SCHEMA ltsa_prod.silver
  TO `00000000-0000-0000-0000-000000000000`;

GRANT USE SCHEMA, CREATE TABLE, MODIFY ON SCHEMA ltsa_prod.gold
  TO `00000000-0000-0000-0000-000000000000`;

GRANT ACCESS ON EXTERNAL LOCATION rfgbu_ltsa_secrets_service_credential_prod
  TO `00000000-0000-0000-0000-000000000000`;
```

---

## 4. Service Principal PAT expired (403 error in GitHub Actions)

PATs are valid for 90 days. If the CD fails with `Error: 403 Forbidden` or `INVALID_PARAMETER_VALUE: Token has expired`, renew the PAT.

Full procedure: see [README.md — Generate a PAT for the SP](../README.md#4-generate-a-pat-for-the-sp) for noprod and [README.md — Generate a PAT for the SP (prod)](../README.md#4-generate-a-pat-for-the-sp-1) for prod.

Once the new token is generated, update it in GitHub → Settings → Environments → `noprod` or `prod` → Secret `DATABRICKS_TOKEN`.

> Set a calendar reminder 7 days before expiration to avoid any CD interruption.

---

## 5. Risk of `load_pickle: true` in prod

The `load_pickle` variable is enabled (`true`) in all targets, including `prod`. When active, some notebooks may load data from a pickle cache instead of recomputing from Telemetry sources.

**Risk**: if Telemetry sources have been updated between two runs, the cache may return stale data.

To force a full recompute (e.g., after a Telemetry data correction):

```bash
# Override the variable at run time
databricks bundle run -t prod ltsa_pipeline_wrapper \
  --job-parameters '{"load_pickle": "false"}'
```

Or temporarily edit `databricks.yml` → target `prod` → `load_pickle: "false"`, deploy, run, then revert to `"true"`.
