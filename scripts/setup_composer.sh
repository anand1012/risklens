#!/bin/bash
# RiskLens — Cloud Composer: Initial Data Load
#
# This script:
#   1. Creates a Cloud Composer environment
#   2. Uploads the DAG
#   3. Triggers the initial 30-day data load
#   4. Waits for completion
#   5. Deletes the Composer environment (stops billing)
#
# The DAG code remains in the repo. Cloud Scheduler handles future runs.
#
# Usage:
#   chmod +x scripts/setup_composer.sh
#   ./scripts/setup_composer.sh YOUR_GCP_PROJECT_ID

set -euo pipefail

PROJECT_ID="${1:?Usage: ./setup_composer.sh YOUR_GCP_PROJECT_ID}"
REGION="us-central1"
COMPOSER_ENV="risklens-composer"
BUCKET_NAME="risklens-raw-${PROJECT_ID}"

echo "=== RiskLens — Initial Data Load via Cloud Composer ==="
echo "Project  : $PROJECT_ID"
echo "Composer : $COMPOSER_ENV"
echo ""
echo "NOTE: Composer will be deleted after the load completes."
echo "      Estimated cost: ~\$5-10 for 2-3 hours of runtime."
echo ""

# ── Create Composer environment (smallest size) ───────────────────────────────
echo "--- Creating Cloud Composer environment (this takes ~15 min) ---"
gcloud composer environments create "$COMPOSER_ENV" \
  --location="$REGION" \
  --image-version="composer-2-airflow-2" \
  --environment-size="small" \
  --service-account="risklens-sa@${PROJECT_ID}.iam.gserviceaccount.com" \
  --quiet

echo "  Composer environment created: $COMPOSER_ENV"

# ── Get Composer DAG bucket ───────────────────────────────────────────────────
DAG_BUCKET=$(gcloud composer environments describe "$COMPOSER_ENV" \
  --location="$REGION" \
  --format="value(config.dagGcsPrefix)")

echo "  DAG bucket: $DAG_BUCKET"

# ── Upload DAG ────────────────────────────────────────────────────────────────
echo "--- Uploading DAG ---"
gcloud storage cp ingestion/dags/risklens_pipeline.py "${DAG_BUCKET}/"
echo "  DAG uploaded."

# ── Install Python dependencies in Composer ───────────────────────────────────
echo "--- Installing dependencies in Composer ---"
gcloud composer environments update "$COMPOSER_ENV" \
  --location="$REGION" \
  --update-pypi-packages-from-file=requirements.txt \
  --quiet
echo "  Dependencies installed."

# ── Trigger initial data load DAG ────────────────────────────────────────────
echo "--- Triggering initial data load ---"
gcloud composer environments run "$COMPOSER_ENV" \
  --location="$REGION" \
  dags trigger -- risklens_pipeline \
  --conf '{"mode": "initial_load", "days": 30}'

echo "  DAG triggered. Waiting for completion..."
echo "  (Check progress at: https://console.cloud.google.com/composer)"
echo ""

# ── Poll for DAG completion ───────────────────────────────────────────────────
echo "--- Polling DAG status every 60 seconds ---"
MAX_WAIT=7200   # 2 hours max
ELAPSED=0

while [ $ELAPSED -lt $MAX_WAIT ]; do
  STATUS=$(gcloud composer environments run "$COMPOSER_ENV" \
    --location="$REGION" \
    dags state -- risklens_pipeline "$(date -u +%Y-%m-%dT%H:%M:%S)" 2>&1 | tail -1 || echo "running")

  echo "  [${ELAPSED}s] Status: $STATUS"

  if echo "$STATUS" | grep -q "success"; then
    echo "  DAG completed successfully."
    break
  elif echo "$STATUS" | grep -q "failed"; then
    echo "  DAG failed. Check Airflow logs before proceeding."
    echo "  Composer environment kept alive for debugging."
    echo "  To delete manually: gcloud composer environments delete $COMPOSER_ENV --location=$REGION"
    exit 1
  fi

  sleep 60
  ELAPSED=$((ELAPSED + 60))
done

# ── Delete Composer environment ───────────────────────────────────────────────
echo ""
echo "--- Deleting Composer environment (stops billing) ---"
gcloud composer environments delete "$COMPOSER_ENV" \
  --location="$REGION" \
  --quiet

echo "  Composer environment deleted. No further Composer charges."

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "=== Initial data load complete ==="
echo "  30 days of data loaded into BigQuery medallion layers"
echo "  Composer environment: DELETED (cost stopped)"
echo "  DAG code: ingestion/dags/risklens_pipeline.py (in repo)"
echo "  Future refreshes: ./scripts/refresh_data.sh ${PROJECT_ID}"
echo ""
echo "Next step: verify data in BigQuery"
echo "  bq query --use_legacy_sql=false 'SELECT COUNT(*) FROM risklens_gold.trade_positions'"
