#!/bin/bash
# Databricks cluster init script — Tesseract + pdf2image dependencies
#
# Attach this script to the cluster that runs llm_document_to_dataframe.ipynb
# when use_tesseract=true.
#
# Path to add in Databricks: Compute > your cluster > Advanced > Init Scripts
# Store this file in a Volume: /Volumes/my_catalog/silver/init_scripts/install_tesseract.sh

set -euo pipefail

echo "=== Installing Tesseract OCR ==="
apt-get update -qq
apt-get install -y tesseract-ocr \
                   tesseract-ocr-fra \
                   tesseract-ocr-spa \
                   poppler-utils   # required by pdf2image

echo "=== Installing Python packages ==="
pip install --quiet pytesseract pdf2image

echo "=== Tesseract version ==="
tesseract --version
