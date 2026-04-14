#!/bin/bash
# RiskLens — Dataproc initialization action
# Installs Python dependencies needed by bronze Spark jobs.
set -euo pipefail
pip install --quiet fredapi==0.5.0 yfinance==0.2.55 google-cloud-secret-manager
echo "RiskLens pip packages installed."
