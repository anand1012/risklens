#!/usr/bin/env bash
# RiskLens — Load API keys into GCP Secret Manager
#
# Run this ONCE before the first deploy (or when rotating keys).
# Keys are read interactively — never stored in shell history.
#
# Usage:
#   export PROJECT=risklens-frtb-2026
#   bash scripts/create_secrets.sh

set -euo pipefail

PROJECT="${PROJECT:-risklens-frtb-2026}"

echo "=== RiskLens Secret Setup ==="
echo "  Project: $PROJECT"
echo "  Keys will be stored in GCP Secret Manager (encrypted, versioned)."
echo ""

_upsert_secret() {
    local NAME="$1"
    local VALUE="$2"

    if gcloud secrets describe "$NAME" --project="$PROJECT" &>/dev/null; then
        # Secret exists — add a new version
        echo "$VALUE" | gcloud secrets versions add "$NAME" \
            --data-file=- \
            --project="$PROJECT"
        echo "  Updated: $NAME (new version added)"
    else
        # Create secret and set initial value
        echo "$VALUE" | gcloud secrets create "$NAME" \
            --data-file=- \
            --replication-policy=automatic \
            --project="$PROJECT"
        echo "  Created: $NAME"
    fi
}

# ── Anthropic API key ─────────────────────────────────────────────────────────
echo -n "Anthropic API key (sk-ant-...): "
read -rs ANTHROPIC_KEY
echo ""
_upsert_secret "risklens-anthropic-api-key" "$ANTHROPIC_KEY"

# ── LangSmith API key ─────────────────────────────────────────────────────────
echo -n "LangSmith API key (ls__...) [press Enter to skip]: "
read -rs LANGSMITH_KEY
echo ""
if [ -n "$LANGSMITH_KEY" ]; then
    _upsert_secret "risklens-langsmith-api-key" "$LANGSMITH_KEY"
else
    # Store a placeholder so the secret exists (CI won't fail trying to read it)
    _upsert_secret "risklens-langsmith-api-key" "disabled"
    echo "  LangSmith skipped — tracing will be disabled"
fi

echo ""
echo "=== Secrets stored in Secret Manager ==="
gcloud secrets list --project="$PROJECT" --filter="name~risklens" \
    --format="table(name, createTime)"

echo ""
echo "Next steps:"
echo "  1. Add GCP_SA_KEY to GitHub repo secrets (from infra/setup_gke.sh output)"
echo "  2. git push origin main  →  CI/CD will deploy automatically"
