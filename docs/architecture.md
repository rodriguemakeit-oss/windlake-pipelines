# Technical Architecture — ltsa_data

## Overview

The `ltsa_data` project is a **Databricks Asset Bundle** that computes LTSA wind turbine availability by cross-referencing two sources:
1. **GenAI** availability — extracted automatically from maintainer PDF/Excel reports via LLM.
2. **portfolio** availability — computed from the technical formula using SCADA data and the Stop Log Book (SLB).

The final result (3 Delta tables `turbine_availability`, `substation_availability`, `farm_availability`) feeds the LTSA Power BI dashboard.

> **Fact / dimension model**: see the proposed model diagram on Confluence — [LTSA fact/dim model](https://service.example.invalid).

---

## Data sources

| Source | Type | Content |
|--------|------|---------|
| **Telemetry static** | Parquet (Delta read) | Turbine/substation reference, maintenance contracts, 10-min SCADA data (`cds_10min_wind`), Stop Log Book (`stop_wind`, `loss_10min_wind`) |
| **Databricks Volume** | Binary files (PDF, Excel) | Maintainer availability reports, organised by `<country>/<maintainer>/<year>/<month>/` |
| **AWS Secrets Manager** | JSON secret | Azure credentials (Document Intelligence endpoint, OpenAI key) — accessed via the Databricks `service_credential_name` |

---

## External services

| Service | Role |
|---------|------|
| **Azure Document Intelligence** | OCR — extracts raw text from PDFs and complex Excel files |
| **Azure OpenAI GPT-4o** | LLM — analyses extracted text and returns structured data (turbine, date, availability) |
| **AWS Secrets Manager** | Stores Azure credentials (endpoint, API key) — region `eu-west-1` |
| **Amazon S3** | Exports final tables in Parquet for external consumers (Power BI, etc.) |

---

## Databricks environments

| Target | Workspace | Unity Catalog | Usage |
|--------|-----------|---------------|-------|
| `dev` | `dbc-a1be2581-5271` | `ltsa_dev` | Local development — resources prefixed `[dev <username>]`, schedules paused |
| `noprod` | `dbc-a1be2581-5271` | `ltsa_dev` | Continuous integration — deployed automatically on `develop` merge |
| `prod` | `dbc-45cc3217-7374` | `ltsa_prod` | Production — manual trigger with GitHub approval gate |

---

## Data layers

```
┌─────────────────────────────────────────────────────────────────────────┐
│  Sources                                                                 │
│  Telemetry static · Databricks Volume (PDF/Excel) · Telemetry SCADA/SLB       │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │  Silver Layer            │
                    │  ltsa_dev/prod.silver    │
                    │                          │
                    │  turbine_scheduled_pm    │  ← maintenance contracts
                    │  maintenance_substation  │
                    │  maintenance_farm        │
                    │  turbine_slb             │  ← transformed Stop Log Book
                    │  availability_genai      │  ← raw LLM extraction
                    │  files_genai_processed   │  ← processed-files tracking
                    │  availability_genai_*    │  ← Telemetry matching (8 tables)
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │  Gold Layer              │
                    │  ltsa_dev/prod.gold      │
                    │                          │
                    │  dim_farm/substation/    │  ← Kimball dimensions
                    │    turbine               │
                    │  wind_turbine_ltsa_scope │  ← filtered LTSA perimeter
                    │  turbine/substation/     │  ← monthly production
                    │    farm_production       │
                    │  loss_10min_wind         │  ← SLB 10-min losses
                    │  slb_and_production      │  ← aggregated SLB + production
                    │  fact_*_availability     │  ← portfolio availabilities (×3)
                    │  fact_genai_*_           │  ← GenAI availabilities (×3)
                    │    availability          │
                    │  ianosm_budget_tracker   │  ← maintenance budget tracker
                    │  fact_slb_completeness_  │  ← SLB data quality
                    │    turbine               │
                    │                          │
                    │  turbine_availability    │  ← final delivery (×3)
                    │  substation_availability │    + S3 export
                    │  farm_availability       │
                    └─────────────────────────┘
```

---

## Service overview diagram

```
                        ┌─────────────────┐
                        │  GitHub Actions │
                        │  CI/CD          │
                        └────────┬────────┘
                                 │ bundle deploy + run
                    ┌────────────▼────────────────────┐
                    │        Databricks Workspace      │
                    │                                  │
                    │  ltsa_pipeline_wrapper (job)     │
                    │    └─► ltsa_pipeline (job)       │
                    │          └─► 11 notebooks        │
                    │                                  │
                    │  Serverless compute (Python whl) │
                    └──┬──────────────┬───────────────┘
                       │              │
           ┌───────────▼──┐   ┌───────▼───────────┐
           │  Telemetry       │   │  Databricks Volume │
           │  (Delta read) │   │  PDF / Excel       │
           └───────────────┘   └───────┬────────────┘
                                       │
                          ┌────────────▼──────────────┐
                          │  Azure Document Intelligence│
                          │  + Azure OpenAI GPT-4o      │
                          │  (credentials via AWS SM)   │
                          └────────────┬───────────────┘
                                       │
                          ┌────────────▼──────────────┐
                          │  Unity Catalog             │
                          │  silver + gold (Delta)     │
                          └────────────┬───────────────┘
                                       │ Parquet export
                          ┌────────────▼──────────────┐
                          │  Amazon S3                 │
                          │  (Power BI source)         │
                          └───────────────────────────┘
```

---

## Pipeline orchestration

The pipeline is orchestrated by two nested Databricks jobs:

- **`ltsa_pipeline_wrapper`** — owns the daily schedule (04:00 Paris) and a global retry (1× after 5 min).
- **`ltsa_pipeline`** — runs the 11 notebooks as a DAG with per-task retries (2× in noprod, 3× in prod).

This "wrapper job" pattern combines per-task retry and global retry without duplicating the schedule.

→ See [pipeline.md](pipeline.md) for the full DAG, execution order, and per-notebook description.
