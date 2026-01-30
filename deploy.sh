#!/bin/bash
# ===========================================
# Demand Planning Demo - Deploy Script
# ===========================================
# Two-stage deployment:
#   1. Setup job - Creates UC catalog + schema (best-effort)
#   2. Pipeline job - Lakeflow SDP/DLT medallion + ML training
#
# Usage:
#   ./deploy.sh [target]
#
# Examples:
#   ./deploy.sh          # Deploy to default (dev) target
#   ./deploy.sh azure    # Deploy to azure target
#
# Authentication:
#   - Option 1: Set DATABRICKS_HOST and DATABRICKS_TOKEN in .env (auto-loaded)
#   - Option 2: Configure a CLI profile and pass as second arg or DATABRICKS_CONFIG_PROFILE
#   - Option 3: Use `databricks auth login --host <host>` for interactive login

set -e

# Load .env if present (provides DATABRICKS_HOST / DATABRICKS_TOKEN)
if [[ -f .env ]]; then
  echo "📄 Loading environment from .env..."
  set -a
  source .env
  set +a
fi

TARGET="${1:-dev}"
PROFILE="${2:-}"

# Build profile flag only if explicitly provided as arg
PROFILE_FLAG=""
if [[ -n "$PROFILE" ]]; then
  PROFILE_FLAG="--profile $PROFILE"
else
  # If using token auth (DATABRICKS_HOST + DATABRICKS_TOKEN), unset any stale profile
  # env var so CLI doesn't get confused by mixed auth sources.
  if [[ -n "$DATABRICKS_HOST" && -n "$DATABRICKS_TOKEN" ]]; then
    unset DATABRICKS_CONFIG_PROFILE
  fi
fi

if [[ "$TARGET" == "-h" || "$TARGET" == "--help" ]]; then
  echo "Usage: ./deploy.sh [target] [profile]"
  echo ""
  echo "Targets: dev, azure"
  echo ""
  echo "Authentication (in order of precedence):"
  echo "  1. DATABRICKS_HOST + DATABRICKS_TOKEN env vars (auto-loaded from .env)"
  echo "  2. CLI profile: ./deploy.sh <target> <profile>"
  echo "  3. Interactive: databricks auth login --host <host>"
  exit 0
fi

echo "🚀 Deploying Demand Planning Demo to target: $TARGET"
echo "=============================================="

# Validate the bundle first
echo "📋 Validating bundle configuration..."
databricks bundle validate --target "$TARGET" $PROFILE_FLAG

# Deploy the bundle
echo ""
echo "📦 Deploying bundle..."
databricks bundle deploy --target "$TARGET" $PROFILE_FLAG

echo ""
echo "🔄 Running Demand Planning Demo Job (UC → Bronze → DLT → Forecast → ML → KPI+Metric refresh → Dashboards)..."
databricks bundle run Demand_Planning_Demo_Job --target "$TARGET" $PROFILE_FLAG

echo ""
echo "✅ Deployment complete!"
echo ""
echo "Pipeline stages completed:"
echo "  1. ✅ Setup: UC catalog + schema ensured (best-effort)"
echo "  2. ✅ DLT: Bronze/Silver/Gold tables materialized"
echo "  3. ✅ Forecasting: demand_forecast tables written"
echo "  4. ✅ ML: late-delivery risk model trained + registered + scored into Gold"
echo "  5. ✅ KPI+Metric refresh: post-forecast KPI views + UC metric views"
echo "  6. ✅ Dashboards: notebook dashboards refreshed + AI/BI dashboard refresh task"

