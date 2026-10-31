# Databricks CLI Makefile for ltsa_data
#
# Usage:
#   make test NOTEBOOK=silver/wind_contracts_load       - Test a single notebook
#   make test-pdf PDF=~/Downloads/GE-PDF/Poland/1-JAN/report.pdf \
#                 VOLUME_PATH=poland/ge/2025/1-JAN      - Upload 1 PDF + run extraction + verify
#   make upload-pdf PDF=~/Downloads/GE-PDF/Poland/1-JAN/report.pdf \
#                   VOLUME_PATH=poland/ge/2025/1-JAN    - Just upload a PDF to the Volume
#   make upload-pdfs LOCAL_DIR=~/Downloads/GE-PDF/Poland/1-JAN \
#                    VOLUME_PATH=poland/ge/2025/1-JAN   - Upload all PDFs from a local folder
#   make verify                                          - Show latest rows in availability_genai
#   make upload                                          - Upload all notebooks & dependencies to workspace
#   make deploy                                          - Deploy the Databricks bundle (dev)
#   make deploy-prod                                     - Deploy the Databricks bundle (prod, via deployer_default_prod SP)
#   make jira-comment TICKET=LTSA-153 COMMENT="hello"    - Post a comment on a Jira ticket
#   make pr BASE=release/ltsa-121/migration \
#           TITLE="feat: ..." BODY="..."                  - Open a GitHub PR for the current branch
#
# Configure your environment in .env.<ENV> file (copy from .env.example)
# Usage: make <target>              (defaults to ENV=preprod)
#        ENV=prod make <target>     (uses .env.prod)

ENV ?= preprod
-include .env.$(ENV)
export

SRC_DIR := src
TOOLS_DIR := tools
REMOTE_DIR := /Users/$(USER_EMAIL)/ltsa_data

# ─────────────────────────────────────────────────────────────────────────────
# test: Upload a single .ipynb notebook + its dependencies, then run it as a one-time job
#   PARAMS (optional): JSON object of widget overrides, e.g. PARAMS='{"catalog":"ltsa_dev"}'
# ─────────────────────────────────────────────────────────────────────────────
PARAMS ?= {}

test:
	@echo "=== Testing $(NOTEBOOK) ==="
	@echo "\n[1/3] Uploading notebook and dependencies..."
	@# Upload Python helpers/utils that notebooks may import
	@databricks workspace import-dir $(SRC_DIR)/silver/helpers $(REMOTE_DIR)/silver/helpers --overwrite -p $(PROFILE) 2>/dev/null || true
	@databricks workspace import-dir $(SRC_DIR)/silver/compute_generative_ai $(REMOTE_DIR)/silver/compute_generative_ai --overwrite -p $(PROFILE) 2>/dev/null || true
	@databricks workspace import-dir $(SRC_DIR)/gold/utils $(REMOTE_DIR)/gold/utils --overwrite -p $(PROFILE) 2>/dev/null || true
	@for nb in $(TOOLS_DIR)/*.ipynb; do \
		[ -f "$$nb" ] || continue; \
		NAME=$$(basename "$$nb" .ipynb); \
		databricks workspace import --file "$$nb" $(REMOTE_DIR)/tools/$$NAME --format JUPYTER --overwrite -p $(PROFILE) 2>/dev/null || true; \
	done
	@# Upload the target notebook (.py = Databricks source format, .ipynb = Jupyter)
	@if [ -f $(SRC_DIR)/$(NOTEBOOK).py ]; then \
		databricks workspace import --file $(SRC_DIR)/$(NOTEBOOK).py $(REMOTE_DIR)/$(NOTEBOOK) --language PYTHON --format SOURCE --overwrite -p $(PROFILE); \
	else \
		databricks workspace import --file $(SRC_DIR)/$(NOTEBOOK).ipynb $(REMOTE_DIR)/$(NOTEBOOK) --format JUPYTER --overwrite -p $(PROFILE); \
	fi
	@echo "\n[2/3] Running..."
	@RUN_OUTPUT=$$(databricks jobs submit --no-wait --json '{"run_name": "Test_$(NOTEBOOK)", "tasks": [{"task_key": "task", "notebook_task": {"notebook_path": "$(REMOTE_DIR)/$(NOTEBOOK)", "source": "WORKSPACE", "base_parameters": $(PARAMS)}}], "queue": {"enabled": true}}' -p $(PROFILE) 2>&1); \
	RUN_ID=$$(echo "$$RUN_OUTPUT" | jq -r '.run_id // empty' 2>/dev/null); \
	if [ -z "$$RUN_ID" ]; then \
		echo "Error submitting job:"; \
		echo "$$RUN_OUTPUT" | jq . 2>/dev/null || echo "$$RUN_OUTPUT"; \
		exit 1; \
	fi; \
	echo "Run ID: $$RUN_ID"; \
	TASK_RUN_ID=$$(databricks jobs get-run $$RUN_ID -p $(PROFILE) | jq -r '.tasks[0].run_id'); \
	RUN_URL=$$(databricks jobs get-run $$TASK_RUN_ID -p $(PROFILE) | jq -r '.run_page_url'); \
	echo "Task run ID: $$TASK_RUN_ID"; \
	echo "Run URL: $$RUN_URL"; \
	echo "\n[3/3] Waiting for completion..."; \
	while true; do \
		OUTPUT=$$(databricks jobs get-run $$TASK_RUN_ID -p $(PROFILE) 2>/dev/null); \
		STATE=$$(echo "$$OUTPUT" | jq -r '.state.life_cycle_state' 2>/dev/null); \
		if [ "$$STATE" = "TERMINATED" ] || [ "$$STATE" = "SKIPPED" ] || [ "$$STATE" = "INTERNAL_ERROR" ]; then \
			break; \
		fi; \
		sleep 2; \
	done; \
	OUTPUT=$$(databricks jobs get-run-output $$TASK_RUN_ID -p $(PROFILE) 2>/dev/null); \
	RESULT_STATE=$$(echo "$$OUTPUT" | jq -r '.state.result_state // empty' 2>/dev/null); \
	echo "\n📊 Result state: $$RESULT_STATE"; \
	if [ "$$RESULT_STATE" = "FAILED" ]; then \
		echo "\n❌ Job failed!"; \
		echo "$$OUTPUT" | jq . 2>/dev/null || echo "$$OUTPUT"; \
		exit 1; \
	fi; \
	ERROR=$$(echo "$$OUTPUT" | jq -r '.error // empty' 2>/dev/null); \
	if [ -n "$$ERROR" ]; then \
		echo "\n❌ Error:"; \
		echo "$$OUTPUT" | jq -r '.error' 2>/dev/null; \
		echo "\n📋 Traceback:"; \
		echo "$$OUTPUT" | jq -r '.error_trace' 2>/dev/null; \
		exit 1; \
	fi; \
	RESULT=$$(echo "$$OUTPUT" | jq -r '.notebook_output.result // empty' 2>/dev/null); \
	if [ -n "$$RESULT" ]; then \
		echo "$$RESULT" | jq . 2>/dev/null || echo "$$RESULT"; \
	else \
		echo "$$OUTPUT" | jq . 2>/dev/null || echo "$$OUTPUT"; \
	fi

# ─────────────────────────────────────────────────────────────────────────────
# upload: Upload all notebooks and dependencies to the workspace
# ─────────────────────────────────────────────────────────────────────────────
upload:
	@echo "=== Uploading all notebooks to $(REMOTE_DIR) ==="
	@databricks workspace import-dir $(SRC_DIR)/silver $(REMOTE_DIR)/silver --overwrite -p $(PROFILE)
	@databricks workspace import-dir $(SRC_DIR)/gold $(REMOTE_DIR)/gold --overwrite -p $(PROFILE)
	@if [ -f $(SRC_DIR)/init_schemas.py ]; then \
		databricks workspace import --file $(SRC_DIR)/init_schemas.py $(REMOTE_DIR)/init_schemas --language PYTHON --format SOURCE --overwrite -p $(PROFILE); \
	elif [ -f $(SRC_DIR)/init_schemas.ipynb ]; then \
		databricks workspace import --file $(SRC_DIR)/init_schemas.ipynb $(REMOTE_DIR)/init_schemas --format JUPYTER --overwrite -p $(PROFILE); \
	fi
	@echo "✓ All notebooks uploaded"

# ─────────────────────────────────────────────────────────────────────────────
# deploy / deploy-prod: Deploy the Databricks asset bundle
# ─────────────────────────────────────────────────────────────────────────────
deploy:
	@echo "=== Deploying bundle (dev — target=dev, profile=$(PROFILE)) ==="
	@databricks bundle deploy -p $(PROFILE)

deploy-noprod:
	@echo "=== Deploying bundle (noprod — target=noprod, profile=preprod) ==="
	@DATABRICKS_TF_EXEC_PATH=/opt/homebrew/bin/terraform DATABRICKS_TF_VERSION=1.15.3 \
		databricks bundle deploy -t noprod -p preprod

run-noprod:
	@echo "=== Running ltsa_pipeline (noprod) ==="
	@databricks bundle run -t noprod ltsa_pipeline -p preprod

deploy-prod:
	@echo "=== Deploying bundle (prod — target=prod, profile=deployer_default_prod) ==="
	@DATABRICKS_TF_EXEC_PATH=/opt/homebrew/bin/terraform DATABRICKS_TF_VERSION=1.15.3 \
		databricks bundle deploy -t prod -p deployer_default_prod

run-prod:
	@echo "=== Running ltsa_pipeline (prod) ==="
	@databricks bundle run -t prod ltsa_pipeline -p deployer_default_prod

# ─────────────────────────────────────────────────────────────────────────────
# test-all: Run every notebook in silver/ and gold/ sequentially
# ─────────────────────────────────────────────────────────────────────────────
test-all:
	@echo "=== Testing all notebooks ==="
	@for nb in $(SRC_DIR)/silver/*.py $(SRC_DIR)/silver/*.ipynb; do \
		[ -f "$$nb" ] || continue; \
		nb_name=$$(echo $$nb | sed 's|$(SRC_DIR)/||; s|\.py$$||; s|\.ipynb$$||'); \
		echo "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"; \
		$(MAKE) test NOTEBOOK=$$nb_name || exit 1; \
	done
	@for nb in $(SRC_DIR)/gold/*.py $(SRC_DIR)/gold/*.ipynb; do \
		[ -f "$$nb" ] || continue; \
		nb_name=$$(echo $$nb | sed 's|$(SRC_DIR)/||; s|\.py$$||; s|\.ipynb$$||'); \
		echo "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"; \
		$(MAKE) test NOTEBOOK=$$nb_name || exit 1; \
	done
	@echo "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo "✅ All notebooks passed!"

# ─────────────────────────────────────────────────────────────────────────────
# upload-pdf: Upload a single PDF/Excel file to the Databricks Volume
#   PDF         = local file path
#   VOLUME_PATH = destination inside the Volume (country/maintainer/year/month)
#
#   Example: make upload-pdf \
#              PDF="~/Downloads/GE-PDF/Poland/1-JAN/Dabrowice MCR PBA1.1 APS_10 (2025-01-01 to 2025-12-31)_2501.pdf" \
#              VOLUME_PATH="poland/ge/2025/1-JAN"
# ─────────────────────────────────────────────────────────────────────────────
upload-pdf:
	@FILENAME=$$(basename "$(PDF)"); \
	echo "📤 Uploading $$FILENAME → $(REPORTS_VOLUME)/$(VOLUME_PATH)/"; \
	databricks fs mkdir "dbfs:$(REPORTS_VOLUME)/$(VOLUME_PATH)" -p $(PROFILE) 2>/dev/null || true; \
	databricks fs cp "$(PDF)" "dbfs:$(REPORTS_VOLUME)/$(VOLUME_PATH)/$$FILENAME" --overwrite -p $(PROFILE); \
	echo "✓ Uploaded"

# ─────────────────────────────────────────────────────────────────────────────
# upload-pdfs: Upload all PDF/Excel files from a local directory to the Volume
#   LOCAL_DIR   = local directory containing files
#   VOLUME_PATH = destination inside the Volume
#
#   Example: make upload-pdfs \
#              LOCAL_DIR="~/Downloads/GE-PDF/Poland/1-JAN" \
#              VOLUME_PATH="poland/ge/2025/1-JAN"
# ─────────────────────────────────────────────────────────────────────────────
upload-pdfs:
	@echo "📤 Uploading all files from $(LOCAL_DIR) → $(REPORTS_VOLUME)/$(VOLUME_PATH)/"
	@databricks fs mkdir "dbfs:$(REPORTS_VOLUME)/$(VOLUME_PATH)" -p $(PROFILE) 2>/dev/null || true
	@for f in "$(LOCAL_DIR)"/*.pdf "$(LOCAL_DIR)"/*.xlsx "$(LOCAL_DIR)"/*.xls; do \
		[ -f "$$f" ] || continue; \
		FILENAME=$$(basename "$$f"); \
		echo "   ├─ $$FILENAME"; \
		databricks fs cp "$$f" "dbfs:$(REPORTS_VOLUME)/$(VOLUME_PATH)/$$FILENAME" --overwrite -p $(PROFILE); \
	done
	@echo "✓ Upload complete"

# ─────────────────────────────────────────────────────────────────────────────
# test-pdf: Upload 1 PDF + run the extraction notebook + show results
#   PDF         = local file path
#   VOLUME_PATH = destination inside the Volume (country/maintainer/year/month)
#
#   Volume structure expected by the notebook:
#     <country>/<maintainer>/<year>/<month-label>/filename.ext
#     e.g. poland/ge/2025/1-JAN/Dabrowice...pdf
#
#   Example: make test-pdf \
#              PDF="~/Downloads/GE-PDF/Poland/1-JAN/Dabrowice MCR PBA1.1 APS_10 (2025-01-01 to 2025-12-31)_2501.pdf" \
#              VOLUME_PATH="poland/ge/2025/1-JAN"
# ─────────────────────────────────────────────────────────────────────────────
test-pdf:
	@echo "=== Test Single PDF ==="
	@FILENAME=$$(basename "$(PDF)"); \
	echo "\n[1/3] Uploading $$FILENAME → $(REPORTS_VOLUME)/$(VOLUME_PATH)/"; \
	databricks fs mkdir "dbfs:$(REPORTS_VOLUME)/$(VOLUME_PATH)" -p $(PROFILE) 2>/dev/null || true; \
	databricks fs cp "$(PDF)" "dbfs:$(REPORTS_VOLUME)/$(VOLUME_PATH)/$$FILENAME" --overwrite -p $(PROFILE)
	@echo "\n[2/3] Running extraction notebook..."
	@$(MAKE) test NOTEBOOK=silver/genai_availability_extract \
		PARAMS='{"ia_files_limit_per_run": "1", "catalog": "$(CATALOG)", "schema": "$(SCHEMA)", "reports_volume_path": "$(REPORTS_VOLUME)", "data_volume_path": "$(DATA_VOLUME)", "AZURE_DI_ENDPOINT": "$(AZURE_DI_ENDPOINT)", "AZURE_DI_KEY": "$(AZURE_DI_KEY)", "AZURE_OPENAI_ENDPOINT": "$(AZURE_OPENAI_ENDPOINT)", "AZURE_OPENAI_KEY": "$(AZURE_OPENAI_KEY)", "debug": "true"}'
	@echo "\n[3/3] Done! Check results with: make verify"

# ─────────────────────────────────────────────────────────────────────────────
# verify: Show the latest rows written to the availability table
# ─────────────────────────────────────────────────────────────────────────────
verify:
	@echo "=== Latest availability_genai rows ==="
	@RUN_OUTPUT=$$(databricks jobs submit --no-wait --json '{"run_name": "Verify_availability", "tasks": [{"task_key": "verify", "notebook_task": {"notebook_path": "$(REMOTE_DIR)/tools/verify_extraction", "source": "WORKSPACE", "base_parameters": {"catalog": "$(CATALOG)", "schema": "$(SCHEMA)"}}}], "queue": {"enabled": true}}' -p $(PROFILE) 2>&1); \
	RUN_ID=$$(echo "$$RUN_OUTPUT" | jq -r '.run_id // empty' 2>/dev/null); \
	if [ -z "$$RUN_ID" ]; then \
		echo "Error submitting job:"; \
		echo "$$RUN_OUTPUT"; \
		exit 1; \
	fi; \
	TASK_RUN_ID=$$(databricks jobs get-run $$RUN_ID -p $(PROFILE) | jq -r '.tasks[0].run_id'); \
	echo "Waiting for verification run $$TASK_RUN_ID..."; \
	while true; do \
		STATE=$$(databricks jobs get-run $$TASK_RUN_ID -p $(PROFILE) 2>/dev/null | jq -r '.state.life_cycle_state'); \
		if [ "$$STATE" = "TERMINATED" ] || [ "$$STATE" = "SKIPPED" ] || [ "$$STATE" = "INTERNAL_ERROR" ]; then break; fi; \
		sleep 2; \
	done; \
	OUTPUT=$$(databricks jobs get-run-output $$TASK_RUN_ID -p $(PROFILE) 2>/dev/null); \
	RESULT=$$(echo "$$OUTPUT" | jq -r '.notebook_output.result // empty' 2>/dev/null); \
	if [ -n "$$RESULT" ]; then \
		echo "$$RESULT" | jq . 2>/dev/null || echo "$$RESULT"; \
	else \
		echo "$$OUTPUT" | jq . 2>/dev/null || echo "$$OUTPUT"; \
	fi

# ─────────────────────────────────────────────────────────────────────────────
# jira-comment: Post a comment on a Jira ticket
#   TICKET   = Jira issue key (e.g. LTSA-153)
#   COMMENT  = comment body (quote it)
#
#   Requires JIRA_URL + JIRA_TOKEN (PAT) in .env.$(ENV)
#   Example:
#     make jira-comment TICKET=LTSA-153 \
#       COMMENT="run URL: https://dbc-...#job/123/run/456 — SUCCESS"
# ─────────────────────────────────────────────────────────────────────────────
jira-comment:
	@if [ -z "$(TICKET)" ] || [ -z "$(COMMENT)" ]; then \
		echo "Usage: make jira-comment TICKET=LTSA-XXX COMMENT=\"...\""; exit 1; \
	fi
	@if [ -z "$(JIRA_URL)" ] || [ -z "$(JIRA_TOKEN)" ]; then \
		echo "Missing JIRA_URL / JIRA_TOKEN in .env.$(ENV)"; exit 1; \
	fi
	@RESP=$$(mktemp); \
	BODY=$$(printf '%s' "$(COMMENT)" | python3 -c "import json,sys; print(json.dumps({'body': sys.stdin.read()}))"); \
	HTTP=$$(curl -sS -o "$$RESP" -w "%{http_code}" \
		-X POST \
		-H "Authorization: Bearer $(JIRA_TOKEN)" \
		-H "Content-Type: application/json" \
		"$(JIRA_URL)/rest/api/2/issue/$(TICKET)/comment" \
		--data "$$BODY"); \
	if [ "$$HTTP" = "201" ]; then \
		ID=$$(python3 -c "import json,sys; print(json.load(open(sys.argv[1])).get('id',''))" "$$RESP"); \
		echo "✓ Comment $$ID posted on $(TICKET) — $(JIRA_URL)/browse/$(TICKET)"; \
	else \
		echo "❌ HTTP $$HTTP"; cat "$$RESP"; echo; rm -f "$$RESP"; exit 1; \
	fi; \
	rm -f "$$RESP"

# ─────────────────────────────────────────────────────────────────────────────
# pr: Open a GitHub Pull Request for the current branch (REST API)
#   BASE    = target branch (default: main)
#   TITLE   = PR title (default: last commit subject)
#   BODY    = PR body (default: empty)
#   DRAFT   = 1 to open as draft
#
#   Requires GH_TOKEN (GitHub PAT with `repo` scope) in .env.$(ENV).
#   Uses the GitHub REST API directly (works on portfolio GitHub Enterprise
#   where the gh CLI fails on /api/graphql with HTTP 401).
#
#   Example:
#     make pr BASE=release/ltsa-121/migration \
#       TITLE="feat(gold): split code into farm/substation/turbine (LTSA-153)" \
#       BODY="See LTSA-153"
# ─────────────────────────────────────────────────────────────────────────────
BASE ?= main
TITLE ?=
BODY ?=
DRAFT ?=

pr:
	@if [ -z "$(GH_TOKEN)" ]; then \
		echo "❌ Missing GH_TOKEN in .env.$(ENV) (GitHub Enterprise PAT, repo scope)"; exit 1; \
	fi
	@BRANCH=$$(git rev-parse --abbrev-ref HEAD); \
	if [ "$$BRANCH" = "HEAD" ] || [ -z "$$BRANCH" ]; then echo "❌ Not on a branch"; exit 1; fi; \
	REMOTE=$$(git config --get remote.origin.url); \
	HOST=$$(echo "$$REMOTE" | sed -E 's|https?://([^/]+)/.*|\1|; s|git@([^:]+):.*|\1|'); \
	REPO=$$(echo "$$REMOTE" | sed -E 's|.*[/:]([^/]+/[^/]+)(\.git)?$$|\1|; s|\.git$$||'); \
	if [ "$$HOST" = "github.com" ]; then API="https://api.github.com"; else API="https://$$HOST/api/v3"; fi; \
	TITLE_FINAL="$(TITLE)"; \
	if [ -z "$$TITLE_FINAL" ]; then TITLE_FINAL=$$(git log -1 --pretty=%s); fi; \
	DRAFT_BOOL=false; if [ -n "$(DRAFT)" ]; then DRAFT_BOOL=true; fi; \
	echo "📤 Pushing $$BRANCH to origin..."; \
	git push -u origin "$$BRANCH" 2>&1 | tail -3; \
	echo "🔀 Creating PR: $$BRANCH → $(BASE) ($$REPO on $$HOST)"; \
	PAYLOAD=$$(TITLE_FINAL="$$TITLE_FINAL" BASE="$(BASE)" BRANCH="$$BRANCH" BODY="$(BODY)" DRAFT_BOOL="$$DRAFT_BOOL" python3 -c "import os,json; print(json.dumps({'title':os.environ['TITLE_FINAL'],'head':os.environ['BRANCH'],'base':os.environ['BASE'],'body':os.environ['BODY'],'draft':os.environ['DRAFT_BOOL']=='true'}))"); \
	RESP=$$(mktemp); \
	HTTP=$$(curl -sS -o "$$RESP" -w "%{http_code}" -X POST \
		-H "Authorization: token $(GH_TOKEN)" \
		-H "Accept: application/vnd.github+json" \
		"$$API/repos/$$REPO/pulls" \
		--data "$$PAYLOAD"); \
	if [ "$$HTTP" = "201" ]; then \
		URL=$$(python3 -c "import json,sys; print(json.load(open(sys.argv[1])).get('html_url',''))" "$$RESP"); \
		NUM=$$(python3 -c "import json,sys; print(json.load(open(sys.argv[1])).get('number',''))" "$$RESP"); \
		echo "✅ PR #$$NUM opened: $$URL"; \
	else \
		echo "❌ HTTP $$HTTP"; cat "$$RESP"; echo; rm -f "$$RESP"; exit 1; \
	fi; \
	rm -f "$$RESP"
