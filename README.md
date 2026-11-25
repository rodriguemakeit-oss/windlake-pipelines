# Windlake Pipelines

A Databricks Asset Bundle prototype for combining document-derived operational information with structured energy data.

## What it demonstrates

- Batch ingestion and transformation with Spark
- Document parsing and language-model-assisted extraction
- Availability calculations and curated Delta outputs
- Bundle-based environment configuration and deployment workflows

## Repository map

- `src/` — reusable pipeline logic
- `resources/` — Databricks job definitions
- `docs/` — architecture, pipeline, and troubleshooting notes
- `tests/` — local checks using fixtures

## Before running

See [GUIDE.md](GUIDE.md). You will need your own documents, telemetry sources, model endpoints, and Databricks workspace. Example hosts, identifiers, and storage settings are placeholders; no operational source files are included.

## Scope

This portfolio adaptation is not a production availability calculation. Validate source definitions, formulas, extraction quality, and review controls with your own domain experts. See [VALIDATION.md](VALIDATION.md).