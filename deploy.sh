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
echo "🤖 Deploying Genie space (optional - requires databricks-sdk)..."
if command -v python3 &> /dev/null && python3 -c "import databricks.sdk" 2>/dev/null; then
  python3 scripts/deploy_genie_space.py && GENIE_STATUS="✅" || GENIE_STATUS="⚠️  (manual setup required)"
else
  echo "  Skipping: databricks-sdk not installed (pip install databricks-sdk)"
  GENIE_STATUS="⏭️  Skipped (install databricks-sdk to enable)"
fi

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
echo "  7. ${GENIE_STATUS} Genie space: AI/BI Genie for natural language queries"

# Get dashboard ID from bundle summary if available
echo ""
echo "=============================================="
echo "📋 App Configuration (add to .env for Streamlit):"
echo "=============================================="
echo ""

# Try to get dashboard ID from bundle
DASHBOARD_ID=""
if databricks bundle summary --target "$TARGET" $PROFILE_FLAG 2>/dev/null | grep -q "demand_planning_control_tower"; then
  DASHBOARD_ID=$(databricks bundle summary --target "$TARGET" $PROFILE_FLAG 2>/dev/null | grep -A2 "demand_planning_control_tower" | grep "id:" | awk '{print $2}' | tr -d '"' || echo "")
fi

# Get org ID from host
ORG_ID=$(echo "$DATABRICKS_HOST" | grep -oE 'o=[0-9]+' | cut -d'=' -f2 || echo "")
if [[ -z "$ORG_ID" ]]; then
  # Try to extract from workspace URL pattern (adb-XXXXX.YY)
  ORG_ID=$(echo "$DATABRICKS_HOST" | grep -oE 'adb-[0-9]+' | cut -d'-' -f2 || echo "")
fi

echo "# Databricks connection"
echo "DATABRICKS_HOST=${DATABRICKS_HOST}"
echo "DATABRICKS_TOKEN=<your-token>"
echo ""
echo "# Dashboard embed URL"
if [[ -n "$DASHBOARD_ID" && -n "$ORG_ID" ]]; then
  echo "DASHBOARD_EMBED_URL=${DATABRICKS_HOST}/embed/dashboardsv3/${DASHBOARD_ID}?o=${ORG_ID}"
elif [[ -n "$DASHBOARD_ID" ]]; then
  echo "DASHBOARD_EMBED_URL=${DATABRICKS_HOST}/embed/dashboardsv3/${DASHBOARD_ID}"
else
  echo "# DASHBOARD_EMBED_URL=<get from Databricks UI: Dashboard > Share > Embed>"
fi

echo ""
echo "# Genie space ID"
if [[ -f .genie_space_id ]]; then
  echo "GENIE_SPACE_ID=$(cat .genie_space_id)"
else
  echo "# GENIE_SPACE_ID=<create Genie space in UI or run: python scripts/deploy_genie_space.py>"
fi

echo ""
echo "To auto-update your .env file with these IDs:"
echo "  ./scripts/update_env_ids.sh $TARGET"
echo ""
echo "=============================================="

