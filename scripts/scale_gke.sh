#!/bin/bash
# RiskLens — GKE Cost Helper
#
# Park the cluster between demos to stop burning ~$50/mo on the GKE node VM.
# Site goes offline when scaled to 0; takes ~3-4 min to come back.
#
# Usage:
#   ./scripts/scale_gke.sh down     # 0 nodes (site offline, ~$0/mo)
#   ./scripts/scale_gke.sh up       # 1 node  (site live, ~$50/mo)
#   ./scripts/scale_gke.sh status   # show current state
#
# Override defaults with env vars if your cluster is named differently:
#   CLUSTER=my-cluster ZONE=us-central1-a ./scripts/scale_gke.sh status

set -euo pipefail

CLUSTER="${CLUSTER:-risklens-cluster}"
ZONE="${ZONE:-us-central1-a}"
PROJECT="${PROJECT:-risklens-frtb-2026}"
NAMESPACE="${NAMESPACE:-risklens}"

cmd="${1:-status}"

case "$cmd" in
    down)
        echo "Scaling $CLUSTER down to 0 nodes (site will go offline) ..."
        gcloud container clusters resize "$CLUSTER" \
            --zone="$ZONE" --project="$PROJECT" \
            --num-nodes=0 --quiet
        echo "Done. Bring back up with: $0 up"
        ;;
    up)
        echo "Scaling $CLUSTER up to 1 node ..."
        gcloud container clusters resize "$CLUSTER" \
            --zone="$ZONE" --project="$PROJECT" \
            --num-nodes=1 --quiet
        echo "Waiting for node to become Ready ..."
        for i in {1..40}; do
            ready=$(kubectl get nodes -o jsonpath='{.items[*].status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "")
            if [[ "$ready" == "True" ]]; then
                echo "  Node ready."
                break
            fi
            sleep 10
            echo "  ... still waiting (${i}/40)"
        done
        echo "Waiting for pods to roll out ..."
        kubectl rollout status deployment/risklens-api      -n "$NAMESPACE" --timeout=300s || true
        kubectl rollout status deployment/risklens-frontend -n "$NAMESPACE" --timeout=300s || true
        echo ""
        echo "Site should be live at the ingress IP in ~30s."
        kubectl get pods -n "$NAMESPACE"
        ;;
    status)
        echo "=== GKE cluster $CLUSTER ==="
        node_count=$(gcloud container clusters describe "$CLUSTER" \
            --zone="$ZONE" --project="$PROJECT" \
            --format='value(currentNodeCount)' 2>/dev/null || echo "?")
        echo "Current node count: $node_count"
        echo ""
        echo "=== Nodes ==="
        kubectl get nodes 2>&1 | head -10
        echo ""
        echo "=== Pods in namespace $NAMESPACE ==="
        kubectl get pods -n "$NAMESPACE" 2>&1 | head -20
        ;;
    *)
        echo "Usage: $0 {up|down|status}"
        echo ""
        echo "  down    — scale to 0 nodes (site offline, saves ~\$50/mo)"
        echo "  up      — scale to 1 node  (site live,    ~\$50/mo)"
        echo "  status  — show current cluster + pod state"
        exit 1
        ;;
esac
