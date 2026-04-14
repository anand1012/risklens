#!/bin/bash
# RiskLens — Manual Data Refresh
#
# Run this whenever you want fresh data loaded (e.g. before a demo).
# Spins up a Dataproc cluster, runs the pipeline, tears it down.
# No Composer needed.
#
# Usage:
#   ./scripts/refresh_data.sh YOUR_GCP_PROJECT_ID [--days 7]
#
# Pipeline order:
#   Bronze (parallel): trades + rates + prices + synthetic
#   Silver (sequential, depends on bronze): transform all sources
#   Gold   (sequential, depends on silver): aggregate + FRTB IMA metrics

set -euo pipefail

PROJECT_ID="${1:?Usage: ./refresh_data.sh YOUR_GCP_PROJECT_ID [--days N]}"
REGION="us-central1"
DAYS=1

# Parse --days flag from remaining args
shift
while [[ $# -gt 0 ]]; do
    case "$1" in
        --days) DAYS="$2"; shift 2 ;;
        *)      echo "Unknown arg: $1"; exit 1 ;;
    esac
done

CLUSTER_NAME="risklens-dataproc-$(date +%Y%m%d%H%M)"
BUCKET_NAME="risklens-raw-${PROJECT_ID}"
TODAY=$(date -u +%Y-%m-%d)

echo "=== RiskLens Data Refresh ==="
echo "Project  : $PROJECT_ID"
echo "Days     : $DAYS"
echo "Date     : $TODAY"
echo "Cluster  : $CLUSTER_NAME"
echo ""

# ── Create ephemeral Dataproc cluster ─────────────────────────────────────────
echo "--- Creating Dataproc cluster ---"
gcloud dataproc clusters create "$CLUSTER_NAME" \
  --region="$REGION" \
  --single-node \
  --master-machine-type="n1-standard-4" \
  --master-boot-disk-size=100 \
  --image-version="2.1-debian11" \
  --service-account="risklens-sa@${PROJECT_ID}.iam.gserviceaccount.com" \
  --properties="spark:spark.jars.packages=com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1" \
  --quiet
echo "  Cluster ready: $CLUSTER_NAME"

# ── Upload Spark jobs to GCS ──────────────────────────────────────────────────
echo "--- Uploading Spark jobs ---"
gcloud storage cp ingestion/jobs/*.py          "gs://${BUCKET_NAME}/jobs/"
gcloud storage cp ingestion/synthetic/generate.py "gs://${BUCKET_NAME}/jobs/"
echo "  Jobs uploaded."

# ── Helper: submit and optionally wait ───────────────────────────────────────
submit_job() {
    local job_name="$1"
    local script="$2"
    local extra_args="${3:-}"
    echo "  Submitting: $job_name"
    gcloud dataproc jobs submit pyspark "gs://${BUCKET_NAME}/jobs/${script}" \
        --cluster="$CLUSTER_NAME" \
        --region="$REGION" \
        --py-files="gs://${BUCKET_NAME}/jobs/generate.py" \
        -- \
        --project="$PROJECT_ID" \
        --bucket="$BUCKET_NAME" \
        $extra_args \
        --quiet
}

# ── Bronze layer: run in parallel ─────────────────────────────────────────────
echo "--- Bronze layer (parallel) ---"

submit_job "Bronze: DTCC trades"    "bronze_trades.py"    "--days=$DAYS" &  PID_TRADES=$!
submit_job "Bronze: FRED rates"     "bronze_rates.py"     "--days=$DAYS" &  PID_RATES=$!
submit_job "Bronze: Yahoo prices"   "bronze_prices.py"    "--days=$DAYS" &  PID_PRICES=$!
submit_job "Bronze: Synthetic data" "bronze_synthetic.py" "--days=$DAYS" &  PID_SYNTH=$!

echo "  Waiting for bronze jobs..."
wait $PID_TRADES  && echo "  trades: done"    || { echo "  WARN: trades failed"; }
wait $PID_RATES   && echo "  rates: done"     || { echo "  WARN: rates failed";  }
wait $PID_PRICES  && echo "  prices: done"    || { echo "  WARN: prices failed"; }
wait $PID_SYNTH   && echo "  synthetic: done" || { echo "  WARN: synthetic failed"; }

echo "  Bronze layer complete."

# ── Silver layer: sequential (one date at a time) ─────────────────────────────
echo "--- Silver layer ---"
submit_job "Silver: Transform" "silver_transform.py" "--date=$TODAY"
echo "  Silver layer complete."

# ── Gold layer: sequential (reads from silver + bronze) ──────────────────────
echo "--- Gold layer ---"
submit_job "Gold: Aggregate" "gold_aggregate.py" "--days=$DAYS"
echo "  Gold layer complete."

# ── Delete cluster ────────────────────────────────────────────────────────────
echo "--- Deleting Dataproc cluster ---"
gcloud dataproc clusters delete "$CLUSTER_NAME" \
  --region="$REGION" \
  --quiet
echo "  Cluster deleted."

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "=== Refresh complete ==="
echo "  Data refreshed: last $DAYS day(s) through $TODAY"
echo "  Dataproc cluster: DELETED (cost stopped)"
echo ""
echo "Quick verify:"
echo "  bq query --use_legacy_sql=false \\"
echo "    'SELECT calc_date, desk, traffic_light_zone, capital_multiplier"
echo "     FROM \`${PROJECT_ID}.risklens_gold.backtesting\`"
echo "     ORDER BY calc_date DESC LIMIT 10'"
echo ""
echo "  bq query --use_legacy_sql=false \\"
echo "    'SELECT calc_date, desk, risk_class, capital_charge_usd"
echo "     FROM \`${PROJECT_ID}.risklens_gold.capital_charge\`"
echo "     ORDER BY calc_date DESC LIMIT 10'"
