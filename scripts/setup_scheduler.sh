#!/bin/bash
# RiskLens — Cloud Scheduler + Cloud Workflows Setup
#
# Sets up a daily pipeline run using:
#   Cloud Scheduler (cron) → Cloud Workflows → Dataproc cluster → Spark jobs
#
# Only needs to be run once. Idempotent — safe to re-run.
#
# Usage:
#   ./scripts/setup_scheduler.sh YOUR_GCP_PROJECT_ID [--region us-east1] [--schedule "0 2 * * *"]
#
# Defaults:
#   region   : us-east1
#   schedule : 0 2 * * *  (2:00 AM UTC daily — after US market close data settles)

set -euo pipefail

PROJECT_ID="${1:?Usage: ./setup_scheduler.sh YOUR_GCP_PROJECT_ID [--region REGION] [--schedule CRON]}"
REGION="us-east1"
SCHEDULE="0 2 * * *"

shift
while [[ $# -gt 0 ]]; do
    case "$1" in
        --region)   REGION="$2";   shift 2 ;;
        --schedule) SCHEDULE="$2"; shift 2 ;;
        *)          echo "Unknown arg: $1"; exit 1 ;;
    esac
done

WORKFLOW_NAME="risklens-daily-refresh"
SCHEDULER_JOB="risklens-daily-pipeline"
SA_EMAIL="risklens-sa@${PROJECT_ID}.iam.gserviceaccount.com"

echo "=== RiskLens Scheduler Setup ==="
echo "Project  : $PROJECT_ID"
echo "Region   : $REGION"
echo "Schedule : $SCHEDULE (UTC)"
echo "Workflow : $WORKFLOW_NAME"
echo ""

# ── Enable required APIs ───────────────────────────────────────────────────────
echo "--- Enabling APIs ---"
gcloud services enable \
    workflows.googleapis.com \
    cloudscheduler.googleapis.com \
    --project="$PROJECT_ID" --quiet
echo "  APIs enabled."

# ── Grant Workflows Invoker role to service account ──────────────────────────
echo "--- Granting IAM roles ---"
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/workflows.invoker" \
    --condition=None \
    --quiet > /dev/null

# Workflow needs to call Dataproc and GCS APIs on behalf of risklens-sa
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/dataproc.editor" \
    --condition=None \
    --quiet > /dev/null

echo "  IAM roles granted."

# ── Deploy the Cloud Workflow ─────────────────────────────────────────────────
echo "--- Deploying Cloud Workflow ---"
gcloud workflows deploy "$WORKFLOW_NAME" \
    --source="infra/workflows/daily_refresh.yaml" \
    --location="$REGION" \
    --service-account="$SA_EMAIL" \
    --project="$PROJECT_ID" \
    --quiet
echo "  Workflow deployed: $WORKFLOW_NAME"

# ── Create Cloud Scheduler job ────────────────────────────────────────────────
WORKFLOW_RESOURCE="projects/${PROJECT_ID}/locations/${REGION}/workflows/${WORKFLOW_NAME}"
SCHEDULER_BODY="{\"project_id\": \"${PROJECT_ID}\", \"region\": \"${REGION}\"}"

echo "--- Creating Cloud Scheduler job ---"
# Delete existing if present (idempotent re-run)
gcloud scheduler jobs delete "$SCHEDULER_JOB" \
    --location="$REGION" \
    --project="$PROJECT_ID" \
    --quiet 2>/dev/null || true

gcloud scheduler jobs create http "$SCHEDULER_JOB" \
    --location="$REGION" \
    --schedule="$SCHEDULE" \
    --time-zone="UTC" \
    --uri="https://workflowexecutions.googleapis.com/v1/${WORKFLOW_RESOURCE}/executions" \
    --message-body="{\"argument\": \"${SCHEDULER_BODY}\"}" \
    --oauth-service-account-email="$SA_EMAIL" \
    --project="$PROJECT_ID" \
    --quiet

echo "  Scheduler job created: $SCHEDULER_JOB"
echo ""

# ── Summary ───────────────────────────────────────────────────────────────────
echo "=== Setup complete ==="
echo ""
echo "Daily pipeline will run at $SCHEDULE UTC."
echo "Workflow: https://console.cloud.google.com/workflows/detail/${REGION}/${WORKFLOW_NAME}/executions?project=${PROJECT_ID}"
echo "Scheduler: https://console.cloud.google.com/cloudscheduler?project=${PROJECT_ID}"
echo ""
echo "To trigger a manual run (test):"
echo "  gcloud workflows run $WORKFLOW_NAME \\"
echo "    --location=$REGION \\"
echo "    --project=$PROJECT_ID \\"
echo "    --data='{\"project_id\": \"$PROJECT_ID\", \"region\": \"$REGION\"}'"
echo ""
echo "To check last execution:"
echo "  gcloud workflows executions list $WORKFLOW_NAME \\"
echo "    --location=$REGION \\"
echo "    --project=$PROJECT_ID \\"
echo "    --limit=5"
