# ltsa_data

Databricks Asset Bundle (DAB) that computes LTSA wind turbine availability by combining maintainer GenAI reports with the portfolio technical formula.

## Table of Contents

- [CI/CD Architecture](#cicd-architecture)
- [GitHub Setup](#github-setup)
- [Databricks Setup — NOPROD](#databricks-setup--noprod)
- [Databricks Setup — PROD](#databricks-setup--prod)
- [Bundle Variables](#bundle-variables)
- [Local Developer Workflow](#local-developer-workflow)

**Technical documentation**: [Architecture](docs/architecture.md) · [Pipeline & DAG](docs/pipeline.md) · [Scripts walkthrough](docs/scripts.md) · [GenAI](docs/generative_ai.md) · [Troubleshooting](docs/troubleshooting.md)

---

## CI/CD Architecture

```
feature branch
      │
      │  Pull Request → develop
      ▼
  CI: bundle validate (noprod) + ruff + pytest
      │
      │  merge
      ▼
  branch develop
      │
      │  Automatic CD
      ▼
  bundle deploy -t noprod
  bundle run   -t noprod  ltsa_pipeline_wrapper  ← pipeline triggered automatically
      │
      ▼
  noprod  (catalog: ltsa_dev  |  workspace: dbc-a1be2581)


  Pull Request → main
      │
  CI: bundle validate (noprod) + bundle validate (prod) + ruff + pytest
      │
      │  merge
      ▼
  branch main
      │
      │  workflow_dispatch  (manual trigger: GitHub → Actions → Run workflow)
      ▼
  gate: reviewer approval (environment "prod")
      │
      ▼
  bundle deploy -t prod
  bundle run   -t prod  ltsa_pipeline_wrapper  ← pipeline triggered after approval
      │
      ▼
  prod  (catalog: ltsa_prod  |  workspace: dbc-45cc3217)
```

### Workflow summary

| File | Trigger | Jobs |
|------|---------|------|
| `databricks-ci.yml` | PR → `develop` or `main` | `validate`: bundle validate noprod + ruff + pytest |
| `databricks-ci.yml` | PR → `main` only | + `validate-prod`: bundle validate prod |
| `databricks-cd.yml` | push → `develop` | `build` → `deploy-noprod` → `run-noprod` |
| `databricks-cd.yml` | `workflow_dispatch` on `main` | `build` → `deploy-prod` (gate) → `run-prod` |

---

## GitHub Setup

### 1. Environments (Settings → Environments → New environment)

Create two environments: **`noprod`** and **`prod`**.
Each environment has its own secrets, isolated from the other.

**Environment `noprod`**:

| Secret | Value |
|--------|-------|
| `DATABRICKS_HOST` | `https://workspace.example.invalid/` |
| `DATABRICKS_TOKEN` | PAT for SP `00000000-0000-0000-0000-000000000000` (see [PAT generation noprod](#4-generate-a-pat-for-the-sp)) |

**Environment `prod`**:

| Secret | Value |
|--------|-------|
| `DATABRICKS_HOST` | `https://workspace.example.invalid` |
| `DATABRICKS_TOKEN` | PAT for SP `00000000-0000-0000-0000-000000000000` (see [PAT generation prod](#4-generate-a-pat-for-the-sp-1)) |

On the **`prod`** environment only, enable:
- **Required reviewers** → add the members allowed to approve a prod deployment
- **Deployment branches → Selected branches → `main`** (prevents triggers from any other branch)

### 2. Branch protection rules (Settings → Branches → Add rule)

**Branch `develop`**:
- [x] Require a pull request before merging
- [x] Require status checks to pass → add `validate`
- [x] Require branches to be up to date before merging

**Branch `main`**:
- [x] Require a pull request before merging
- [x] Require status checks to pass → add `validate` and `validate-prod`
- [x] Require branches to be up to date before merging
- [x] Restrict who can push (service principals only)

---

## Databricks Setup — NOPROD

**Workspace**: `https://workspace.example.invalid/`  
**Service principal**: `00000000-0000-0000-0000-000000000000`

### 1. Add the SP to the workspace

Databricks UI → Settings → Identity and access → Service principals → Add service principal → `00000000-0000-0000-0000-000000000000`

Minimum required role: **Can Manage** (to create/update jobs and upload files to the workspace path `/Workspace/Shared/.bundle/ltsa_data/noprod`).

### 2. Unity Catalog — catalog `ltsa_dev`

```sql
GRANT USE CATALOG ON CATALOG ltsa_dev
  TO `00000000-0000-0000-0000-000000000000`;

GRANT USE SCHEMA, CREATE TABLE, MODIFY ON SCHEMA ltsa_dev.silver
  TO `00000000-0000-0000-0000-000000000000`;

GRANT USE SCHEMA, CREATE TABLE, MODIFY ON SCHEMA ltsa_dev.gold
  TO `00000000-0000-0000-0000-000000000000`;
```

### 3. Service credential (AWS Secrets Manager access)

The bundle uses `service_credential_name: rfgbu_ltsa_secrets_service_credential_noprod`.  
Verify the service credential exists and grant access to the SP:

```sql
GRANT ACCESS ON EXTERNAL LOCATION rfgbu_ltsa_secrets_service_credential_noprod
  TO `00000000-0000-0000-0000-000000000000`;
```

### 4. Generate a PAT for the SP

From an admin account on the noprod workspace, use the Databricks Token Management API to create a token on behalf of the service principal:

```bash
# Get an OAuth2 token for the SP (client_credentials flow)
SP_CLIENT_ID="00000000-0000-0000-0000-000000000000"
SP_CLIENT_SECRET="<sp_client_secret>"
WORKSPACE_HOST="https://workspace.example.invalid/"

TOKEN=$(curl -s -X POST "${WORKSPACE_HOST}/oidc/v1/token" \
  -u "${SP_CLIENT_ID}:${SP_CLIENT_SECRET}" \
  -d "grant_type=client_credentials&scope=all-apis" \
  | jq -r '.access_token')

# Create a long-lived PAT (90 days) on behalf of the SP
curl -s -X POST "${WORKSPACE_HOST}/api/2.0/token/create" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"comment": "GitHub Actions CI/CD sp_ltsa_noprod", "lifetime_seconds": 7776000}' \
  | jq -r '.token_value'
```

Copy the returned value into the **`DATABRICKS_TOKEN`** secret of the GitHub **`noprod`** environment.

> **Renewal**: the PAT expires after 90 days. Set a reminder to renew it before expiration.

---

## Databricks Setup — PROD

**Workspace**: `https://workspace.example.invalid`  
**Service principal**: `00000000-0000-0000-0000-000000000000`

### 1. Add the SP to the workspace

Same procedure as noprod: add `00000000-0000-0000-0000-000000000000` with the **Can Manage** role on `/Workspace/Shared/.bundle/ltsa_data/prod`.

### 2. Unity Catalog — catalog `ltsa_prod`

```sql
GRANT USE CATALOG ON CATALOG ltsa_prod
  TO `00000000-0000-0000-0000-000000000000`;

GRANT USE SCHEMA, CREATE TABLE, MODIFY ON SCHEMA ltsa_prod.silver
  TO `00000000-0000-0000-0000-000000000000`;

GRANT USE SCHEMA, CREATE TABLE, MODIFY ON SCHEMA ltsa_prod.gold
  TO `00000000-0000-0000-0000-000000000000`;
```

### 3. Service credential (AWS Secrets Manager access)

```sql
GRANT ACCESS ON EXTERNAL LOCATION rfgbu_ltsa_secrets_service_credential_prod
  TO `00000000-0000-0000-0000-000000000000`;
```

### 4. Generate a PAT for the SP

Same procedure as noprod, replacing the credentials of SP `51c02b7a` and the prod workspace host. Copy into the **`DATABRICKS_TOKEN`** secret of the GitHub **`prod`** environment.

---

## Bundle Variables

All variables are configured in `databricks.yml` for the three targets. The table below is the full reference.

| Variable | dev | noprod | prod | Role |
|---|---|---|---|---|
| `catalog` | `ltsa_dev` | `ltsa_dev` | `ltsa_prod` | Target Unity Catalog |
| `gold_bucket` | `demo-data-bucket` | `demo-data-bucket` | `demo-data-bucket` | S3 bucket for Parquet exports |
| `max_retries` | `0` | `2` | `3` | Per-task retry attempts on failure |
| `service_credential_name` | `rfgbu_ltsa_secrets_service_credential_noprod` | `rfgbu_ltsa_secrets_service_credential_noprod` | `rfgbu_ltsa_secrets_service_credential_prod` | Databricks → AWS Secrets Manager credential |
| `secrets_manager_id` | `demo-document-bucket` | `demo-document-bucket` | `demo-document-bucket` | AWS secret ID (Azure OpenAI / Document Intelligence credentials) |
| `secrets_manager_region` | `eu-west-1` | `eu-west-1` | `eu-west-1` | AWS region of the Secrets Manager |
| `ltsa_scope_regex` | *(default)* | *(default)* | *(default)* | Turbine scope filter regex (9 countries) |
| `ia_files_limit_per_run` | `200` | `200` | `-1` (unlimited) | Max GenAI files processed per run |
| `debug` | `true` | `true` | `false` | Verbose notebook logs |
| `load_pickle` | `true` | `true` | `true` | Load pickle caches instead of recomputing |

> **Note**: failure email notifications are prepared in `databricks.yml` (commented `# TODO` blocks) for noprod and prod. Uncomment and fill in the recipients when the team is ready.

---

## Local Developer Workflow

### Prerequisites

```bash
# Install uv (package manager)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install the Databricks CLI
brew install databricks/tap/databricks  # macOS
# or: pip install databricks-cli
```

### Dev deployment

```bash
# 1. Authenticate (one-time)
databricks configure
# → enter: host = https://workspace.example.invalid/
# → enter: personal token (generated in Databricks UI → User settings → Developer → Access tokens)

# 2. Deploy in dev mode
#    - Deploys to the noprod workspace (dbc-a1be2581) but prefixes all
#      resources with [dev <username>] → no risk of conflict with noprod
#    - debug: true and load_pickle: true are enabled automatically
#    - The daily schedule is automatically paused
databricks bundle deploy -t dev

# 3. Trigger the pipeline manually (optional)
databricks bundle run -t dev ltsa_pipeline_wrapper

# 4. Tear down dev resources after use
databricks bundle destroy -t dev
```

### Noprod & prod
databricks bundle deploy -p noprod -t noprod
databricks bundle deploy -p prod -t prod