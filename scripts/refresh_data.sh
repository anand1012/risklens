#!/bin/bash
# RiskLens — Manual Data Refresh
#
# Run this whenever you want fresh data loaded (e.g. before a demo).
# Spins up a Dataproc cluster, runs the pipeline, tears it down.
# No Composer needed.
#
# Usage:
#   ./scripts/refresh_data.sh YOUR_GCP_PROJECT_ID [--days 7]

set -euo pipefail

PROJECT_ID="${1:?Usage: ./refresh_data.sh YOUR_GCP_PROJECT_ID}"
REGION="us-central1"
DAYS="${2:-1}"    # default: 1 day refresh
CLUSTER_NAME="risklens-dataproc-$(date +%Y%m%d%H%M)"
BUCKET_NAME="risklens-raw-${PROJECT_ID}"

echo "=== RiskLens Data Refresh ==="
echo "Project : $PROJECT_ID"
echo "Days    : $DAYS"
echo "Cluster : $CLUSTER_NAME"
echo ""

# ── Create ephemeral Dataproc cluster ─────────────────────────────────────────
echo "--- Creating Dataproc cluster ---"
gcloud dataproc clusters create "$CLUSTER_NAME" \
  --region="$REGION" \
  --single-node \
  --master-machine-type="n1-standard-2" \
  --master-boot-disk-size=50 \
  --image-version="2.1-debian11" \
  --service-account="risklens-sa@${PROJECT_ID}.iam.gserviceaccount.com" \
  --quiet

echo "  Cluster ready: $CLUSTER_NAME"

# ── Upload Spark jobs to GCS ──────────────────────────────────────────────────
echo "--- Uploading Spark jobs ---"
gcloud storage cp ingestion/jobs/*.py "gs://${BUCKET_NAME}/jobs/"
gcloud storage cp ingestion/synthetic/generate.py "gs://${BUCKET_NAME}/jobs/"
echo "  Jobs uploaded."

# ── Run pipeline: Bronze → Silver → Gold ─────────────────────────────────────
run_job() {
  local job_name="$1"
  local script="$2"
  shift 2
  echo "  Running $job_name..."
  gcloud dataproc jobs submit pyspark "gs://${BUCKET_NAME}/jobs/${script}" \
    --cluster="$CLUSTER_NAME" \
    --region="$REGION" \
    -- \
    --project="$PROJECT_ID" \
    --bucket="$BUCKET_NAME" \
    --days="$DAYS" \
    "$@" \
    --quiet
  echo "  $job_name complete."
}

echo "--- Running pipeline ---"
run_job "Bronze: DTCC trades"    "bronze_trades.py"
run_job "Bronze: FRED rates"     "bronze_rates.py"
run_job "Bronze: Yahoo prices"   "bronze_prices.py"
run_job "Bronze: Synthetic data" "bronze_synthetic.py"
run_job "Silver: Transform"      "silver_transform.py"
run_job "Gold: Aggregate"        "gold_aggregate.py"

# ── Delete cluster ────────────────────────────────────────────────────────────
echo "--- Deleting Dataproc cluster ---"
gcloud dataproc clusters delete "$CLUSTER_NAME" \
  --region="$REGION" \
  --quiet

echo "  Cluster deleted."

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "=== Refresh complete ==="
echo "  Data refreshed: last $DAYS day(s)"
echo "  Dataproc cluster: DELETED (cost stopped)"
echo ""
echo "Verify:"
echo "  bq query --use_legacy_sql=false \\"
echo "    'SELECT calc_date, desk, var_99_1d FROM risklens_gold.var_outputs ORDER BY calc_date DESC LIMIT 10'"
