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

# Always delete cluster on exit (success or failure)
cleanup() {
    if [[ -n "${CLUSTER_NAME:-}" ]]; then
        echo "--- Cleanup: deleting Dataproc cluster $CLUSTER_NAME ---"
        gcloud dataproc clusters delete "$CLUSTER_NAME" \
          --region="$REGION" --project="$PROJECT_ID" --quiet 2>/dev/null || true
    fi
}
trap cleanup EXIT

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

# ── Upload init script + Spark jobs (must be in GCS before cluster starts) ────
echo "--- Uploading init script and Spark jobs ---"
gcloud storage cp scripts/dataproc_init.sh        "gs://${BUCKET_NAME}/scripts/"
gcloud storage cp ingestion/jobs/*.py              "gs://${BUCKET_NAME}/jobs/"
gcloud storage cp ingestion/synthetic/generate.py  "gs://${BUCKET_NAME}/jobs/"
echo "  Jobs uploaded."

# ── Create ephemeral Dataproc cluster ─────────────────────────────────────────
echo "--- Creating Dataproc cluster ---"
gcloud dataproc clusters create "$CLUSTER_NAME" \
  --region="$REGION" \
  --single-node \
  --master-machine-type="e2-standard-4" \
  --master-boot-disk-size=100 \
  --image-version="2.1-debian11" \
  --service-account="risklens-sa@${PROJECT_ID}.iam.gserviceaccount.com" \
  --initialization-actions="gs://${BUCKET_NAME}/scripts/dataproc_init.sh" \
  --quiet
echo "  Cluster ready: $CLUSTER_NAME"

# ── Helper: submit and optionally wait ───────────────────────────────────────
submit_job() {
    local job_name="$1"
    local script="$2"
    local extra_args="${3:-}"
    echo "  Submitting: $job_name"
    gcloud dataproc jobs submit pyspark "gs://${BUCKET_NAME}/jobs/${script}" \
        --cluster="$CLUSTER_NAME" \
        --region="$REGION" \
        --jars="gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.36.1.jar" \
        --py-files="gs://${BUCKET_NAME}/jobs/generate.py" \
        --quiet \
        -- \
        --project="$PROJECT_ID" \
        --bucket="$BUCKET_NAME" \
        $extra_args
}

# ── Bronze layer (step 1): external sources in parallel ───────────────────────
# Keep to 30 days — enough for RFET eligibility (90-day window) and back-testing
# seed without pulling years of data unnecessarily.
EXT_DAYS=30
echo "--- Bronze layer step 1: external sources (parallel, --days=$EXT_DAYS) ---"

submit_job "Bronze: DTCC trades"  "bronze_trades.py"  "--days=$EXT_DAYS" &  PID_TRADES=$!
submit_job "Bronze: FRED rates"   "bronze_rates.py"   "--days=$EXT_DAYS" &  PID_RATES=$!
submit_job "Bronze: Yahoo prices" "bronze_prices.py"  "--days=$EXT_DAYS" &  PID_PRICES=$!

echo "  Waiting for external bronze jobs..."
wait $PID_TRADES && echo "  trades: done"  || { echo "  WARN: trades failed"; }
wait $PID_RATES  && echo "  rates: done"   || { echo "  WARN: rates failed";  }
wait $PID_PRICES && echo "  prices: done"  || { echo "  WARN: prices failed"; }

# ── Bronze layer (step 2): synthetic — reads real rates to seed VaR/ES ────────
# Runs AFTER step 1 so bronze.rates_r is available for market param seeding.
# Match EXT_DAYS so synthetic and real data cover the same date window.
echo "--- Bronze layer step 2: synthetic (seeded from real rates, --days=$EXT_DAYS) ---"
submit_job "Bronze: Synthetic data" "bronze_synthetic.py" "--days=$EXT_DAYS"
echo "  synthetic: done"

echo "  Bronze layer complete."

# ── Silver layer (Job 1): clean each source independently ─────────────────────
echo "--- Silver layer: clean ---"
submit_job "Silver: Transform (clean)" "silver_transform.py" "--date=$TODAY"
echo "  Silver clean complete."

# ── Silver layer (Job 2): join + enrich (depends on Job 1) ────────────────────
echo "--- Silver layer: enrich ---"
submit_job "Silver: Enrich (join)" "silver_enrich.py" "--date=$TODAY"
echo "  Silver enrich complete."

# ── Gold layer: FRTB IMA metrics (reads from silver only) ────────────────────
echo "--- Gold layer ---"
submit_job "Gold: Aggregate" "gold_aggregate.py" "--date=$TODAY"
echo "  Gold layer complete."

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
