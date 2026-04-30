#!/bin/bash
# RiskLens — Bring App Back Up After Full Shutdown
#
# Pairs with shutdown done on 2026-04-30:
#   - GKE node pool scaled to 0
#   - K8s Ingress deleted (LB gone)
#   - Static IP risklens-ip released
#   - Pods exist but Pending (no nodes to schedule on)
#
# What this script does:
#   1. Scale GKE node pool back to 1 node (~3 min for VM provisioning)
#   2. Re-apply the ingress manifest (creates a NEW global LB + EPHEMERAL IP)
#   3. Wait for pods to roll out
#   4. Print the new IP — you'll need to update README + DNS
#
# Usage:
#   ./scripts/bring_up.sh
#
# Estimated bring-up time: 5–8 minutes
# Estimated cost while live: ~$68/mo (GKE node + global LB)
# Park again with:
#   ./scripts/scale_gke.sh down  # only scales node pool
# OR for full shutdown again, re-run the same steps from this session.

set -euo pipefail

CLUSTER="${CLUSTER:-risklens-cluster}"
ZONE="${ZONE:-us-central1-a}"
PROJECT="${PROJECT:-risklens-frtb-2026}"
NAMESPACE="${NAMESPACE:-risklens}"

echo "=== RiskLens bring-up ==="
echo "Cluster   : $CLUSTER"
echo "Zone      : $ZONE"
echo "Project   : $PROJECT"
echo "Namespace : $NAMESPACE"
echo ""

# ── Step 1: Scale GKE node pool back to 1 ────────────────────────────────────
echo "--- Step 1/4: Scaling GKE to 1 node (~3 min) ---"
gcloud container clusters resize "$CLUSTER" \
    --zone="$ZONE" --project="$PROJECT" \
    --num-nodes=1 --quiet
echo "  Resize requested. Waiting for node Ready ..."
for i in {1..40}; do
    ready=$(kubectl get nodes -o jsonpath='{.items[*].status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "")
    if [[ "$ready" == "True" ]]; then
        echo "  Node Ready."
        break
    fi
    sleep 15
    echo "  ... waiting (${i}/40)"
done

# ── Step 2: Re-apply the ingress (creates new LB + new ephemeral IP) ─────────
echo ""
echo "--- Step 2/4: Re-applying K8s manifests (creates new ingress + LB) ---"
kubectl apply -f infra/k8s/namespace.yaml
kubectl apply -f infra/k8s/ -n "$NAMESPACE"

# ── Step 3: Wait for pods + ingress to roll out ──────────────────────────────
echo ""
echo "--- Step 3/4: Waiting for deployments to be ready ---"
kubectl rollout status deployment/risklens-api      -n "$NAMESPACE" --timeout=300s || true
kubectl rollout status deployment/risklens-frontend -n "$NAMESPACE" --timeout=300s || true

echo ""
echo "--- Step 4/4: Waiting for ingress to get an IP (~2-5 min) ---"
NEW_IP=""
for i in {1..30}; do
    NEW_IP=$(kubectl get ingress risklens-ingress -n "$NAMESPACE" -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "")
    if [[ -n "$NEW_IP" && "$NEW_IP" != "<none>" ]]; then
        echo "  Ingress IP assigned: $NEW_IP"
        break
    fi
    sleep 15
    echo "  ... waiting for IP (${i}/30)"
done

if [[ -z "$NEW_IP" ]]; then
    echo "  WARN: No ingress IP yet. Run again later:"
    echo "    kubectl get ingress -n $NAMESPACE"
    exit 1
fi

# ── Sanity check ──────────────────────────────────────────────────────────────
echo ""
echo "--- Final state ---"
kubectl get pods,ingress -n "$NAMESPACE"

echo ""
echo "=== BRING-UP COMPLETE ==="
echo ""
echo "  New URL : http://$NEW_IP"
echo ""
echo "  Don't forget:"
echo "    1. Update README.md  'Live:' line  →  http://$NEW_IP"
echo "    2. (Optional) Point Cloudflare risklens.preciseguess.com → $NEW_IP"
echo ""
echo "  HTTPS may take 5-10 min after ingress for the GKE-managed cert to provision."
echo "  Hit the URL once to warm up the LB:"
echo "    curl -I http://$NEW_IP/"
