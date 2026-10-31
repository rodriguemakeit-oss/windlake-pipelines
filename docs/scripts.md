# Scripts walkthrough

Concise per-notebook reference: what each script does, in which order, and the rules that matter. Pair with [pipeline.md](pipeline.md) (DAG + I/O tables) and [architecture.md](architecture.md) (overall design).

## Table of contents

| # | Notebook | One-liner |
|---|----------|-----------|
| 1 | [`init_schemas`](#1-init_schemas) | Create `silver` / `gold` schemas in the catalog. |
| 2 | [`wind_turbine_scope`](#2-wind_turbine_scope) | Filter Telemetry to LTSA scope, build `dim_turbine/substation/farm`. |
| 3 | [`wind_contracts_load`](#3-wind_contracts_load) | Load maintenance contracts; dedup per substation/year. |
| 4 | [`wind_production_compute`](#4-wind_production_compute) | Aggregate 10-min ActivePower to monthly production. |
| 4b | [`wind_slb_loss_compute`](#4-wind_slb_loss_compute) | Aggregate 10-min loss data per stop per month. |
| 4c | [`genai_availability_extract`](#4-genai_availability_extract) | Extract availability from PDF/Excel via OCR + GPT-4o. |
| 5 | [`wind_slb_transform`](#5-wind_slb_transform) | Explode multi-month stops; pro-rata duration & loss. |
| 6 | [`wind_slb_aggregate`](#6-wind_slb_aggregate) | Pivot SLB on IEC × responsible-party; join production. |
| 7 | [`wind_availability_compute`](#7-wind_availability_compute) | Compute portfolio time/energy availability (v1 + v2 above-cap). |
| 7b | [`genai_availability_match`](#7-genai_availability_match) | Match GenAI turbines to Telemetry (exact + fuzzy). |
| 8 | [`wind_availability_merge`](#8-wind_availability_merge) | Merge portfolio + GenAI; resolve guarantees per contract. |
| 10 | [`wind_slb_completeness_compute`](#10-wind_slb_completeness_compute) | Measure % of stops with valid IEC category. |

> Numbering follows [pipeline.md](pipeline.md) (some duplicates `4.` / `7.` reflect parallel branches in the DAG).

## Reading order for new joiners

Suggested first read to grasp the end-to-end flow:

1. **`wind_turbine_scope`** — start here, it defines what "LTSA turbine" means and builds the dimension tables every other script depends on.
2. **`wind_contracts_load`** — explains the contract / guarantee model (key business concept).
3. **`wind_slb_transform`** → **`wind_slb_aggregate`** — the SLB pipeline, where most of the algorithmic complexity lives.
4. **`wind_availability_compute`** — the core portfolio availability formula (v1 + v2 above-cap).
5. **`genai_availability_extract`** → **`genai_availability_match`** — the AI branch (see also [generative_ai.md](generative_ai.md)).
6. **`wind_availability_merge`** — final delivery: how portfolio and GenAI are reconciled into the gold tables consumed downstream.

---

## 1. `init_schemas`

**Goal**: Create the `silver` and `gold` Unity Catalog schemas if they don't already exist. One-time bootstrap that must run before any other notebook in the pipeline.

**Steps**:
1. Read `catalog`, `silver_schema`, `gold_schema` widget parameters.
2. `CREATE SCHEMA IF NOT EXISTS <catalog>.<silver_schema>`.
3. `CREATE SCHEMA IF NOT EXISTS <catalog>.<gold_schema>`.

**Key helpers**: none (pure DDL).

**Special rules**: —

---

## 2. `wind_turbine_scope`

**Goal**: Filter Telemetry's wind turbine and substation reference to the LTSA scope (regex on `farm_code`), then build the Kimball dimension tables `dim_farm`, `dim_substation`, `dim_turbine` with surrogate IDs and parent-FK links.

**Steps**:
1. Read `wind_turbine/data` and `substation/data` from Telemetry static volumes.
2. Filter rows whose `farm_code` matches the LTSA regex (9 countries) → `wind_turbine_ltsa_scope`.
3. Build `dim_farm`: distinct `(farm_code, farm)`, add monotonic surrogate `id`.
4. Build `dim_substation`: distinct substations joined to `dim_farm` on `farm_code` for the FK.
5. Build `dim_turbine`: turbines joined to `dim_substation` on `substation_code` for the FK.
6. Reorder columns so `id` is first; export each dimension to S3 as well.
7. Write `gold.wind_turbine_scope`, `gold.dim_farm`, `gold.dim_substation`, `gold.dim_turbine`.

**Key helpers**: `add_id_column` (`gold/utils/kimball.py`), `reorder_columns` (`gold/utils/helpers.py`).

**Special rules**: surrogate `id` is monotonic, not stable across reruns — use natural keys (`farm_code`, `substation_code`, `turbine_id`) when joining from outside the bundle.

---

## 3. `wind_contracts_load`

**Goal**: Load Telemetry maintenance contracts and produce four silver reference tables (substation/farm contract level, scheduled PM, guarantee farm referential), with the canonical "active-first then most-recent" deduplication rule.

**Steps**:
1. Read `maintenance_contract_general`, `_guarantee`, `_contractual_availability_flate_rate` from Telemetry.
2. Filter `technology = Wind`; override `start_date` with `enforcement_date` when present.
3. Normalize status (Unicode NFKD), keep `active` and `terminated` only.
4. Join general × guarantee one row per guarantee year; clean availability fields (replace `"None"`, cross-fill farm/turbine fields).
5. Deduplicate per `(substation_code, guarantee_year)` via `_keep_one_contract` (active-first, then most recent `contract_start_date`, then smallest `contract_technical_id`).
6. Aggregate to farm level (collapse substations); compute `schedule_maintenance_contract_level` (`wtg` vs `farm`).
7. Filter contracts overlapping the 13-month lookback; keep the most recent per farm.
8. Write `silver.turbine_scheduled_pm`, `silver.maintenance_substation`, `silver.maintenance_farm`, plus the `referential_guarantee_farm` S3 export.

**Key helpers**: `_keep_one_contract`, `clean_availability` (`silver/helpers/`), Spark `Window.partitionBy/orderBy`.

**Special rules**: contract dedup priority is the canonical rule — **exactly one active wins, otherwise most recent `contract_start_date` DESC, then `contract_technical_id` ASC**. The same rule is reused in `wind_availability_merge`.

---

## 4. `wind_production_compute`

**Goal**: Read 10-minute SCADA ActivePower from Telemetry partition by partition (farm × year) and aggregate to monthly turbine production. Aggregations to substation/farm production are derived from the turbine table.

**Steps**:
1. Read `gold.wind_turbine_ltsa_scope` to enumerate `(farm_code, year)` partitions to scan.
2. For each partition, read `cds_10min_wind/data` with partition pruning (`WHERE farm_code=… AND year=…`).
3. Skip empty partitions.
4. Filter `telemetry_tag_name == "ActivePower"`; reconstruct missing `farm_code` from `ent_code` (regex `WIF_<country>`).
5. Extract `year` / `month` from timestamp; project required columns.
6. Group by `ent_code` and aggregate (sum production, coalesce flags).
7. Concatenate all partitions; write `gold.turbine_production`, `gold.substation_production`, `gold.farm_production` and S3 exports.

**Key helpers**: partition-pruning predicate built per loop; `groupBy().agg()`.

**Special rules**: partition overwrite scoped to the processed `(farm, year)` cells — re-running for one farm/year does not invalidate the rest.

---

## 4. `wind_slb_loss_compute`

**Goal**: Read 10-minute loss data from Telemetry Stop Log Book, join to the turbine reference, and aggregate by `(stop_id, year, month)` to feed `wind_slb_transform`.

**Steps**:
1. Read `gold.wind_turbine_ltsa_scope` for the scope.
2. Iterate `(farm, year)` and read `loss_10min_wind/data` per partition (skip empty ones).
3. Project `stop_id, year, month, ent_code, stop_duration_sec, loss_value_kwh`.
4. Append all partitions via `unionByName`.
5. Left join the wind turbine reference on `ent_code → wind_turbine_code`.
6. Group by `(stop_id, year, month)`; sum duration / loss; first farm and ent codes.
7. Convert duration to minutes (`stop_duration_min`) and loss to MWh (`loss_value_mwh`).
8. Write `gold.loss_10min_wind` with partition overwrite on the last + current year only.

**Key helpers**: `unionByName`, `groupBy().agg()`.

**Special rules**: partition overwrite limited to the rolling `[last_year, current_year]` window.

> **Why partition overwrite?** Same rationale as in `wind_slb_transform`: the most recent year-partitions are the ones most likely to receive late corrections from Telemetry, so they are fully refreshed on each run. Older year-partitions are left intact.

---

## 4. `genai_availability_extract`

**Goal**: Extract turbine availability from maintainer PDF/Excel reports using Azure Document Intelligence (OCR) and Azure OpenAI GPT-4o. Idempotent: previously processed files are skipped via the tracking table. See [generative_ai.md](generative_ai.md) for the wider context.

**Steps**:
1. Read widget configuration (catalog, schema, file limits, model name).
2. Fetch Azure DI / OpenAI keys from AWS Secrets Manager.
3. Load `silver.files_genai_processed` to identify new files.
4. List files from a Volume (default) or SharePoint adapter; filter by allowed extensions.
5. Parse each file path → `(country, maintainer, year, month)` metadata.
6. Optional Tesseract pre-OCR keyword-filters pages to send (cost reduction).
7. Per file: call Document Intelligence (`prebuilt-layout`) → markdown; call the LLM with a per-maintainer prompt; parse the structured output.
8. Aggregate results via `ResultAggregator`; update tracking with `run_date` / `error`.
9. Write `silver.availability_genai` and `silver.files_genai_processed` (Append mode).

**Key helpers**: `VolumeAdapter` / `SharePointAdapter`, `get_document_intelligence_client` (`document_intelligence_helper.py`), `call_model` (`openai_helper.py`), `build_ocr_extract` (`tesseract_helper.py`), `LLMExtractor` (`silver/compute_generative_ai/extractors.py`).

**Special rules**: optional Tesseract pre-OCR; per-file `sleep_time` throttling; `max_retries` retries with backoff; **15-day shift on Nordex Acciona dates** (see `compute_generative_ai/constants.py`); Excel reader mode (`openpyxl` vs Document Intelligence) selected per maintainer.

---

## 5. `wind_slb_transform`

**Goal**: Transform raw Stop Log Book data so each stop becomes one row per target month, with duration and loss pro-rated; then refine the loss pro-rata using the 10-min granular loss data.

**Steps**:
1. Read `stop_wind/data` filtered to the LTSA scope and the 13-month lookback window.
2. Filter out `start_date > end_date` (data quality fix).
3. Compute `target_months` for each stop via `sequence(date_trunc('month', start), date_trunc('month', end), interval 1 month)`.
4. `explode` → one row per `(stop_id, month)`; compute `first_day` / `last_day` boundaries.
5. Compute `prorata = stop_duration_during_month_sec / stop_duration_total_sec`.
6. Apply pro-rata to duration and loss columns; rename to `month_*` form.
7. Left join `gold.loss_10min_wind` on `(stop_id, year)` to refine the loss pro-rata where granular data exists.
8. Write `silver.turbine_slb` with partition overwrite on the lookback window.

**Key helpers**: `end_of_day`, `date_trunc`, `sequence`, `explode`, `date_diff`, `coalesce`, `least`.

**Special rules**: null `end_date` is replaced by either the last-month boundary or `write_time`; lookback window defaults to **13 months**; partition overwrite is scoped to that window so older partitions are preserved.

> **Why `explode`?** A single stop event can span several calendar months (e.g., a breakdown starting in February and ending in April). Downstream scripts work at month granularity, so each stop must produce one independent row per month it overlaps — hence the explode. Without it, a multi-month stop would be attributed entirely to its start month.

> **Why partition overwrite?** Telemetry data is ingested incrementally and the most-recent months are the ones most likely to have been corrected or back-filled at source. Re-writing only the last N months on each run ensures those partitions always reflect the freshest available data, while leaving older, stable partitions untouched and avoiding a costly full rewrite.

---

## 6. `wind_slb_aggregate`

**Goal**: Aggregate the silver SLB by IEC category × responsible party, pivot into a wide schema, and join with turbine production to produce the unified `gold.slb_and_production` table.

**Steps**:
1. Read `silver.turbine_slb`.
2. Clean `responsible_party` (null → `"other"`, anything not `"maintainer"` → `"other"`).
3. Clean `iec_stop_category_label` (null → `"IU"`).
4. Group by `(ent_code, iec, responsible_party, year, month)`; sum duration and loss.
5. Pivot on the `iec_responsible` concat (e.g., `ianopca_maintainer_*`, `ianofo_*`); fill nulls with `0`.
6. Rename: `ent_code → wind_turbine_code`, `slb_month → month`, `slb_year → year`.
7. Left join with `gold.turbine_production` on `(wind_turbine_code, year, month)`.
8. Write `gold.slb_and_production`.

**Key helpers**: `groupBy().pivot().agg()`, `fillna()`, `withColumnsRenamed()`.

**Special rules**: scheduled-PM exclusion columns are produced here as input for the v2 above-cap algorithm in `wind_availability_compute`.

---

## 7. `wind_availability_compute`

**Goal**: Compute portfolio time and energy availabilities at turbine, substation, and farm level from SLB and production data, applying the IEC above-cap algorithm and scheduled-PM logic. Resolves dimension surrogate FKs and writes the three `fact_*_availability` tables.

**Steps**:
1. Load `gold.slb_and_production`, `silver.turbine_scheduled_pm`, and the three `dim_*` tables.
2. Prepare PM reference: filter on guarantee dates, deduplicate per `(substation, guarantee_year)` with the active-first rule.
3. Join SLB + PM on `substation_code`; compute `slb_date` (month ref); filter to within the guarantee window.
4. Determine farm case **@A** (single maintainer/farm) vs **@B** (multiple maintainers).
5. Compute IANOSM hours / MWh per turbine from the SLB pivot.
6. Apply `compute_iec_above_cap_algorithm_by_turbine`: cumulative sum of IANOSM, split at `pm_budget` (`hours_per_wt` or `mwh_per_wt`).
7. Call `calculate_availabilities` (v1 time/energy) and `calculate_v2_availabilities` (v2, with above-cap).
8. Build the `computed_availabilities` JSON column; aggregate to substation/farm via `aggregate_and_calculate`.
9. Write `gold.fact_turbine_availability`, `fact_substation_availability`, `fact_farm_availability`, and `gold.ianosm_budget_tracker`.

**Key helpers**: `calculate_availabilities`, `calculate_v2_availabilities`, `compute_iec_above_cap_algorithm_by_turbine`, `aggregate_and_calculate`, `build_computed_availabilities_json`, `safe_col` (all in `gold/utils/turbine_availability.py`).

**Special rules**: the **IEC above-cap algorithm** (Eq. 12 / 16) is the heart of v2 — IANOSM is cumulated and split at the PM budget; rows above cap are excluded from availability denominators. **v2 is suppressed for case @B**. Scheduled-PM exclusion rows from `wind_slb_aggregate` feed this step. Date shifts per maintainer (e.g., 15 days for Nordex Acciona) come from `constants.py`.

---

## 7. `genai_availability_match`

**Goal**: Match turbines extracted by the GenAI pipeline against Telemetry reference turbines using exact serial-number matching followed by fuzzy matching. Produces silver match/no-match tables and gold `fact_genai_*_availability` tables enriched with dimension surrogate FKs.

**Steps**:
1. Read `silver.availability_genai`; clean serial numbers (strip whitespace, prefixes `WEA`, `SEN`, `NX`).
2. Split `turbine_id` on `"|"` into `serial_number_1` / `_2`; map `country → PIC code`.
3. Build composite keys `serial_number_{1,2}_country`.
4. Read `gold.dim_turbine`; build the matching `serial_number_country` key.
5. **Phase 2a**: exact join on `serial_number_1_country`; collect unmatched residual.
6. **Phase 2b**: exact join on `serial_number_2_country` for the remaining unmatched.
7. **Phase 3**: fuzzy match the residual via `rapidfuzz.fuzz.partial_ratio` with **cutoff = 85**.
8. Combine matched + fuzzy → `silver.availability_genai_telemetry_match`; residual → `silver.availability_genai_telemetry_no_match`; misclassifications → `silver.mapping_file_classify_fail`.
9. **Phase 4**: site-level aggregation → `silver.availability_genai_site_availability`.
10. **Phase 5**: resolve `dim_*` FKs; write `gold.fact_genai_turbine_availability`, `_substation_`, `_farm_`.

**Key helpers**: `_clean_sn`, `mapping_pic_country_code`, `rapidfuzz.fuzz.process`, `broadcast()`, exact join (`inner` / `anti`).

**Special rules**: serial cleaning is intentionally aggressive (`WEA…`, `SEN`, `NX` prefixes are stripped); fuzzy cutoff is **85** — anything below is left in `_no_match` for manual review; site-level rows bypass turbine matching entirely.

---

## 8. `wind_availability_merge`

**Goal**: Merge portfolio-computed availabilities with GenAI maintainer availabilities via full-outer joins at turbine, substation, and farm levels. Applies the canonical contract dedup rule and enriches each row with the guaranteed availability from maintenance contracts. Writes the final delivery tables and S3 exports.

**Steps**:
1. Load `fact_turbine_availability` (portfolio) and `fact_genai_turbine_availability`; deduplicate GenAI by latest `reporting_date` per `(turbine, year, month)`.
2. Load `silver.maintenance_substation`; deduplicate per `(substation, year, month)` via active-first / most-recent.
3. Full outer join portfolio ⊕ GenAI on `(turbine_id, year, month)`; resolve `turbine_id → wind_turbine_code` via `dim_turbine`.
4. Resolve `substation_code` and `farm_code` via `dim_substation` / `dim_farm` FKs.
5. Left join the maintenance contract on `substation_code` and `date BETWEEN guarantee_start AND guarantee_end`.
6. Cast the `computed_availabilities` JSON column → `MapType` for the final schema.
7. Aggregate to substation level: same join + dedup pattern using `fact_substation_*` tables.
8. Aggregate to farm level: same join + dedup pattern using `fact_farm_*` tables.
9. Write `gold.turbine_availability`, `substation_availability`, `farm_availability` and the corresponding S3 Parquet exports.

**Key helpers**: `_dedup_genai_latest`, `_keep_one_contract`, `_add_ref_date`, `from_json`, `broadcast`, `coalesce`.

**Special rules**: GenAI rows with `reporting_date` ties keep the latest; rows with null guarantee dates are excluded; full-outer join means a turbine can appear with portfolio-only, GenAI-only, or both — downstream consumers must handle nulls in either source.

---

## 10. `wind_slb_completeness_compute`

**Goal**: Compute SLB data quality (completeness) per turbine per month — the percentage of stops with a valid `iec_stop_category_label`, measured both by row count and by duration.

**Steps**:
1. Read `silver.turbine_slb`.
2. Group by `(ent_code, substation_code, farm_code, slb_year, slb_month)`.
3. Count rows with null `iec_stop_category_label` and total rows; sum the same on `duration_min`.
4. `completeness_percentage_row = 1 - missing_rows / total_rows`.
5. `completeness_percentage = 1 - missing_min / total_min`.
6. Add `run_date = current_timestamp()`.
7. Join `gold.dim_turbine` to resolve `ent_code → turbine_id` (FK).
8. Write `gold.fact_slb_completeness_turbine` (with FK) and S3 export `slb_completeness_turbine/data`.

**Key helpers**: `add_id_column`, `reorder_columns`, SQL `try_divide`.

**Special rules**: —
