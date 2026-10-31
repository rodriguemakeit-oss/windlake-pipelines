#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# Cluster-scoped init script: install LibreOffice (headless) on the driver.
#
# Purpose: provide the `soffice` binary so notebooks can convert Office
# documents (.doc/.docx/.xls/.xlsx/.ppt/.pptx) to PDF, e.g.:
#   soffice --headless --convert-to pdf --outdir /tmp input.docx
#
# Runs on every node at startup. On a single-node cluster that is just the
# driver. Output is captured in the cluster's init-script logs.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

echo "[init] $(date -u) installing LibreOffice (headless)…"

apt-get update -y

# --no-install-recommends keeps the install lean (no GUI extras). The core
# metapackage pulls in Writer/Calc/Impress + the headless soffice binary.
# fonts-liberation gives PDFs sane default fonts; add locales for non-ASCII text.
apt-get install -y --no-install-recommends \
  libreoffice \
  fonts-liberation \
  fonts-dejavu \
  locales

# Verify the binary is present and print its version to the init log.
if ! command -v soffice >/dev/null 2>&1; then
  echo "[init] ERROR: soffice not found after install" >&2
  exit 1
fi

echo "[init] $(date -u) LibreOffice installed: $(soffice --version 2>/dev/null | head -1)"
