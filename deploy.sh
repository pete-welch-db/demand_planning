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

set -e

TARGET="${1:-dev}"
PROFILE="${2:-${DATABRICKS_CONFIG_PROFILE:-}}"

PROFILE_FLAG=""
if [[ -n "$PROFILE" ]]; then
  PROFILE_FLAG="--profile $PROFILE"
fi

if [[ "$TARGET" == "-h" || "$TARGET" == "--help" ]]; then
  echo "Usage: ./deploy.sh [target]"
  echo "Targets: dev, azure"
  echo "Optional: ./deploy.sh <target> <profile>   (or set DATABRICKS_CONFIG_PROFILE)"
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

