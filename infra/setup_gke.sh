#!/usr/bin/env bash
# RiskLens — GKE Cluster + Workload Identity Setup
# Run once before the first CI deploy.
#
# Usage:
#   export PROJECT=risklens-frtb-2026
#   export REGION=us-central1
#   bash infra/setup_gke.sh

set -euo pipefail

PROJECT="${PROJECT:-risklens-frtb-2026}"
REGION="${REGION:-us-central1}"
CLUSTER="risklens-cluster"
AR_REPO="risklens"
GCP_SA="risklens-api-sa"
K8S_SA="risklens-api-ksa"
NAMESPACE="risklens"

echo "=== RiskLens GKE Setup ==="
echo "  Project : $PROJECT"
echo "  Region  : $REGION"
echo "  Cluster : $CLUSTER"
echo ""

# ── Artifact Registry ────────────────────────────────────────────────────────
echo "--- Creating Artifact Registry repository ---"
gcloud artifacts repositories create "$AR_REPO" \
    --repository-format=docker \
    --location="$REGION" \
    --project="$PROJECT" \
    --description="RiskLens Docker images" \
    --quiet 2>/dev/null || echo "  (already exists)"

# ── Reserve static IP ────────────────────────────────────────────────────────
echo "--- Reserving global static IP ---"
gcloud compute addresses create risklens-ip \
    --global \
    --project="$PROJECT" \
    --quiet 2>/dev/null || echo "  (already exists)"

IP=$(gcloud compute addresses describe risklens-ip --global --project="$PROJECT" --format="value(address)")
echo "  Static IP: $IP  →  point your DNS A record here"

# ── GKE Cluster ──────────────────────────────────────────────────────────────
echo "--- Creating GKE Standard cluster ---"
gcloud container clusters create "$CLUSTER" \
    --project="$PROJECT" \
    --region="$REGION" \
    --num-nodes=1 \
    --machine-type="e2-standard-2" \
    --workload-pool="${PROJECT}.svc.id.goog" \
    --enable-autoscaling \
    --min-nodes=1 \
    --max-nodes=3 \
    --enable-ip-alias \
    --quiet 2>/dev/null || echo "  (already exists)"

gcloud container clusters get-credentials "$CLUSTER" \
    --region="$REGION" \
    --project="$PROJECT"

# ── GCP Service Account ──────────────────────────────────────────────────────
echo "--- Creating GCP Service Account for Workload Identity ---"
gcloud iam service-accounts create "$GCP_SA" \
    --display-name="RiskLens API" \
    --project="$PROJECT" \
    --quiet 2>/dev/null || echo "  (already exists)"

GCP_SA_EMAIL="${GCP_SA}@${PROJECT}.iam.gserviceaccount.com"

for ROLE in \
    roles/bigquery.dataEditor \
    roles/bigquery.jobUser \
    roles/storage.objectAdmin \
    roles/secretmanager.secretAccessor \
    roles/aiplatform.user; do
    gcloud projects add-iam-policy-binding "$PROJECT" \
        --member="serviceAccount:${GCP_SA_EMAIL}" \
        --role="$ROLE" \
        --quiet > /dev/null
    echo "  Bound: $ROLE"
done

# ── Workload Identity binding ─────────────────────────────────────────────────
echo "--- Binding Workload Identity ---"
gcloud iam service-accounts add-iam-policy-binding "$GCP_SA_EMAIL" \
    --role="roles/iam.workloadIdentityUser" \
    --member="serviceAccount:${PROJECT}.svc.id.goog[${NAMESPACE}/${K8S_SA}]" \
    --project="$PROJECT" \
    --quiet

# ── CI/CD GitHub Actions service account ─────────────────────────────────────
echo "--- Creating CI/CD service account ---"
CI_SA="risklens-cicd"
gcloud iam service-accounts create "$CI_SA" \
    --display-name="RiskLens CI/CD" \
    --project="$PROJECT" \
    --quiet 2>/dev/null || echo "  (already exists)"

CI_SA_EMAIL="${CI_SA}@${PROJECT}.iam.gserviceaccount.com"

for ROLE in \
    roles/container.developer \
    roles/artifactregistry.writer \
    roles/secretmanager.secretAccessor; do
    gcloud projects add-iam-policy-binding "$PROJECT" \
        --member="serviceAccount:${CI_SA_EMAIL}" \
        --role="$ROLE" \
        --quiet > /dev/null
    echo "  CI bound: $ROLE"
done

echo ""
echo "--- Creating CI/CD key (add to GitHub secret GCP_SA_KEY) ---"
gcloud iam service-accounts keys create /tmp/risklens-cicd-key.json \
    --iam-account="$CI_SA_EMAIL" \
    --project="$PROJECT"
echo "  Key written to /tmp/risklens-cicd-key.json"
echo "  ⚠ Add this JSON as GitHub secret GCP_SA_KEY, then delete the file."

echo ""
echo "=== Setup complete ==="
echo "  Next: add GitHub secrets (see .github/workflows/ci.yml for required names)"
echo "  Then: git push to main to trigger first deploy"
