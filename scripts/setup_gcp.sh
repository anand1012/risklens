#!/bin/bash
# RiskLens — GCP Infrastructure Setup
# Run once before anything else.
#
# Usage:
#   chmod +x scripts/setup_gcp.sh
#   ./scripts/setup_gcp.sh YOUR_GCP_PROJECT_ID

set -euo pipefail

PROJECT_ID="${1:?Usage: ./setup_gcp.sh YOUR_GCP_PROJECT_ID}"
REGION="us-central1"
BUCKET_NAME="risklens-raw-${PROJECT_ID}"

echo "=== RiskLens GCP Setup ==="
echo "Project : $PROJECT_ID"
echo "Region  : $REGION"
echo ""

# ── Set project ───────────────────────────────────────────────────────────────
gcloud config set project "$PROJECT_ID"

# ── Enable APIs ───────────────────────────────────────────────────────────────
echo "--- Enabling APIs ---"
gcloud services enable \
  bigquery.googleapis.com \
  dataproc.googleapis.com \
  composer.googleapis.com \
  container.googleapis.com \
  containerregistry.googleapis.com \
  secretmanager.googleapis.com \
  storage.googleapis.com \
  cloudscheduler.googleapis.com \
  cloudfunctions.googleapis.com \
  run.googleapis.com \
  iam.googleapis.com \
  --quiet

echo "  APIs enabled."

# ── GCS Bucket (private, no public access) ────────────────────────────────────
echo "--- Creating GCS bucket ---"
gcloud storage buckets create "gs://${BUCKET_NAME}" \
  --location="$REGION" \
  --uniform-bucket-level-access \
  --pap=enforced 2>/dev/null || echo "  Bucket already exists."

# Create folder structure
for folder in dtcc fred yahoo synthetic indexes dags; do
  echo "" | gcloud storage cp - "gs://${BUCKET_NAME}/${folder}/.keep" 2>/dev/null || true
done

echo "  Bucket: gs://${BUCKET_NAME} (private, public access prevented)"

# ── Service Account ───────────────────────────────────────────────────────────
echo "--- Creating service account ---"
SA_NAME="risklens-sa"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud iam service-accounts create "$SA_NAME" \
  --display-name="RiskLens Service Account" \
  --quiet 2>/dev/null || echo "  Service account already exists."

# Least-privilege roles
for role in \
  roles/bigquery.dataEditor \
  roles/bigquery.jobUser \
  roles/storage.objectAdmin \
  roles/dataproc.editor \
  roles/composer.worker \
  roles/secretmanager.secretAccessor \
  roles/logging.logWriter \
  roles/run.invoker; do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="$role" \
    --quiet
done

echo "  Service account: ${SA_EMAIL}"

# ── Secret Manager ────────────────────────────────────────────────────────────
echo "--- Creating secrets in Secret Manager ---"
for secret in anthropic-api-key cohere-api-key fred-api-key; do
  gcloud secrets create "risklens-${secret}" \
    --replication-policy="automatic" \
    --quiet 2>/dev/null || echo "  Secret risklens-${secret} already exists."
done

echo ""
echo "  Add your API keys now:"
echo "    echo -n 'YOUR_KEY' | gcloud secrets versions add risklens-anthropic-api-key --data-file=-"
echo "    echo -n 'YOUR_KEY' | gcloud secrets versions add risklens-cohere-api-key --data-file=-"
echo "    echo -n 'YOUR_KEY' | gcloud secrets versions add risklens-fred-api-key --data-file=-"

# ── GKE Cluster ───────────────────────────────────────────────────────────────
echo "--- Creating GKE cluster ---"
gcloud container clusters create "risklens-cluster" \
  --region="$REGION" \
  --num-nodes=1 \
  --machine-type="e2-standard-2" \
  --enable-ip-alias \
  --workload-pool="${PROJECT_ID}.svc.id.goog" \
  --quiet 2>/dev/null || echo "  Cluster already exists."

# Workload Identity — pods authenticate to GCP without key files
gcloud iam service-accounts add-iam-policy-binding "${SA_EMAIL}" \
  --role="roles/iam.workloadIdentityUser" \
  --member="serviceAccount:${PROJECT_ID}.svc.id.goog[default/risklens-ksa]" \
  --quiet

echo "  GKE cluster: risklens-cluster (Workload Identity enabled)"

# ── Cloud Scheduler (for future data refreshes after Composer is deleted) ─────
echo "--- Creating Cloud Scheduler job (paused) ---"
gcloud scheduler jobs create http "risklens-daily-refresh" \
  --location="$REGION" \
  --schedule="0 6 * * 1-5" \
  --uri="https://placeholder.run.app/trigger" \
  --message-body='{"job":"daily_refresh"}' \
  --time-zone="America/New_York" \
  --paused \
  --quiet 2>/dev/null || echo "  Scheduler job already exists."

echo "  Cloud Scheduler: risklens-daily-refresh (paused — enable when needed)"

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "=== Setup complete ==="
echo "  GCS bucket      : gs://${BUCKET_NAME}"
echo "  Service account : ${SA_EMAIL}"
echo "  GKE cluster     : risklens-cluster (${REGION})"
echo "  Scheduler       : risklens-daily-refresh (paused)"
echo ""
echo "Next steps:"
echo "  1. Add API keys to Secret Manager (commands above)"
echo "  2. python scripts/setup_bigquery.py --project ${PROJECT_ID}"
echo "  3. python ingestion/synthetic/generate.py --days 30"
echo "  4. ./scripts/setup_composer.sh ${PROJECT_ID}   ← initial data load, then auto-deletes"
