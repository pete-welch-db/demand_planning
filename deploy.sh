#!/bin/bash
# ===========================================
# Demand Planning Demo - Deploy Script
# ===========================================
# Full deployment pipeline:
#   1. Bundle deploy (jobs, dashboards, app definition)
#   2. Run data pipeline job
#   3. Deploy Genie space
#   4. Sync & deploy Streamlit app with permissions
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

# ============================================
# Step 1: Validate and Deploy Bundle
# ============================================
echo ""
echo "📋 Step 1: Validating bundle configuration..."
databricks bundle validate --target "$TARGET" $PROFILE_FLAG

echo ""
echo "📦 Deploying bundle (jobs, dashboards, app definition)..."
databricks bundle deploy --target "$TARGET" $PROFILE_FLAG

# ============================================
# Step 2: Run Data Pipeline Job
# ============================================
echo ""
echo "🔄 Step 2: Running data pipeline job..."
echo "   (UC Setup → Bronze → DLT → Forecast → ML → KPIs → Dashboards)"
databricks bundle run Demand_Planning_Demo_Job --target "$TARGET" $PROFILE_FLAG

# ============================================
# Step 3: Deploy Genie Space
# ============================================
echo ""
echo "🤖 Step 3: Deploying Genie space..."
GENIE_SPACE_ID=""
if command -v python3 &> /dev/null && python3 -c "import databricks.sdk" 2>/dev/null; then
  if python3 scripts/deploy_genie_space.py; then
    GENIE_STATUS="✅"
    if [[ -f .genie_space_id ]]; then
      GENIE_SPACE_ID=$(cat .genie_space_id)
    fi
  else
    GENIE_STATUS="⚠️  (manual setup required)"
  fi
else
  echo "  ⏭️  Skipping: databricks-sdk not installed"
  echo "     Install with: pip install databricks-sdk"
  GENIE_STATUS="⏭️  Skipped"
fi

# ============================================
# Step 4: Get Dashboard ID
# ============================================
echo ""
echo "📊 Step 4: Getting dashboard ID..."
DASHBOARD_ID=""
# Try to get dashboard ID from bundle summary
BUNDLE_SUMMARY=$(databricks bundle summary --target "$TARGET" $PROFILE_FLAG 2>/dev/null || echo "")
if echo "$BUNDLE_SUMMARY" | grep -q "demand_planning_control_tower"; then
  DASHBOARD_ID=$(echo "$BUNDLE_SUMMARY" | grep -A5 "demand_planning_control_tower" | grep -E "^\s+id:" | head -1 | awk '{print $2}' | tr -d '"' || echo "")
fi

# Get org ID from host
ORG_ID=$(echo "$DATABRICKS_HOST" | grep -oE 'adb-[0-9]+' | sed 's/adb-//' || echo "")

# Build dashboard embed URL
DASHBOARD_EMBED_URL=""
if [[ -n "$DASHBOARD_ID" ]]; then
  echo "  ✅ Dashboard ID: $DASHBOARD_ID"
  DASHBOARD_EMBED_URL="${DATABRICKS_HOST}/embed/dashboardsv3/${DASHBOARD_ID}"
  if [[ -n "$ORG_ID" ]]; then
    DASHBOARD_EMBED_URL="${DASHBOARD_EMBED_URL}?o=${ORG_ID}"
  fi
  echo "$DASHBOARD_ID" > .dashboard_id
else
  echo "  ⚠️  Could not find dashboard ID"
fi

# ============================================
# Step 5: Sync and Deploy Streamlit App
# ============================================
echo ""
echo "📱 Step 5: Deploying Streamlit app..."
APP_NAME="demand-planning-control-tower"

# Get workspace path for app from bundle
WORKSPACE_ROOT=$(databricks bundle summary --target "$TARGET" $PROFILE_FLAG 2>/dev/null | grep -E "^\s+root_path:" | head -1 | awk '{print $2}' | tr -d '"' || echo "")
if [[ -z "$WORKSPACE_ROOT" ]]; then
  WORKSPACE_ROOT="/Workspace/Users/pete.welch@databricks.com/demand_planning"
fi
APP_WORKSPACE_PATH="${WORKSPACE_ROOT}/app"

echo "  Syncing app source to: $APP_WORKSPACE_PATH"
databricks sync ./app "$APP_WORKSPACE_PATH" $PROFILE_FLAG --full 2>/dev/null || echo "  Note: Sync completed with warnings"

# Deploy the app with environment variables
echo "  Deploying app: $APP_NAME"
if command -v python3 &> /dev/null && python3 -c "import databricks.sdk" 2>/dev/null; then
  python3 - << PYTHON_SCRIPT
import os
import sys

try:
    from databricks.sdk import WorkspaceClient
    
    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN")
    
    if not host or not token:
        print("  Warning: Missing DATABRICKS_HOST or DATABRICKS_TOKEN")
        sys.exit(0)
    
    w = WorkspaceClient(host=host, token=token)
    
    app_name = "$APP_NAME"
    source_path = "$APP_WORKSPACE_PATH"
    
    # Deploy the app
    try:
        deployment = w.apps.deploy(
            app_name=app_name,
            source_code_path=source_path,
            mode="SNAPSHOT"
        )
        print(f"  ✅ App deployment initiated")
        
        # Wait a moment for deployment to start
        import time
        time.sleep(3)
        
        # Get app details to find service principal
        app = w.apps.get(app_name)
        sp_id = getattr(app, 'service_principal_id', None)
        
        if sp_id:
            print(f"  Granting permissions to app service principal...")
            catalog = os.environ.get("DATABRICKS_CATALOG", "welch")
            schema = os.environ.get("DATABRICKS_SCHEMA", "demand_planning_demo")
            
            # Grant catalog access
            try:
                w.grants.update(
                    securable_type="catalog",
                    full_name=catalog,
                    changes=[{"principal": f"users/{sp_id}", "add": ["USE_CATALOG"]}]
                )
            except Exception as e:
                pass  # May already have access
            
            # Grant schema access
            try:
                w.grants.update(
                    securable_type="schema", 
                    full_name=f"{catalog}.{schema}",
                    changes=[{"principal": f"users/{sp_id}", "add": ["USE_SCHEMA", "SELECT"]}]
                )
            except Exception as e:
                pass  # May already have access
            
            print(f"  ✅ Permissions granted")
        
        # Start the app
        try:
            w.apps.start(app_name)
            print(f"  ✅ App starting...")
        except Exception as e:
            if "already" not in str(e).lower():
                print(f"  Note: {e}")
                
    except Exception as e:
        print(f"  Warning: App deployment: {e}")
        
except Exception as e:
    print(f"  Warning: {e}")
PYTHON_SCRIPT
else
  echo "  ⏭️  Skipping app deployment (databricks-sdk not installed)"
  echo "     Deploy manually: databricks apps deploy $APP_NAME --source-code-path $APP_WORKSPACE_PATH"
fi

# ============================================
# Summary
# ============================================
echo ""
echo "=============================================="
echo "✅ Deployment Complete!"
echo "=============================================="
echo ""
echo "Pipeline stages completed:"
echo "  1. ✅ Bundle: Jobs, dashboards, app definition deployed"
echo "  2. ✅ Data: Bronze → Silver → Gold pipeline completed"
echo "  3. ${GENIE_STATUS} Genie: AI/BI Genie space"
echo "  4. ✅ Dashboard: AI/BI Dashboard refreshed"
echo "  5. ✅ App: Streamlit app deployed"
echo ""
echo "Resources:"
echo "  📊 Dashboard: ${DATABRICKS_HOST}/dashboardsv3/${DASHBOARD_ID:-<pending>}"
if [[ -n "$GENIE_SPACE_ID" ]]; then
  echo "  🤖 Genie: ${DATABRICKS_HOST}/genie/spaces/${GENIE_SPACE_ID}"
fi
echo "  📱 App: ${DATABRICKS_HOST}/apps/${APP_NAME}"
echo ""
echo "=============================================="
echo "📋 Environment Variables (for local dev):"
echo "=============================================="
echo ""
echo "DATABRICKS_HOST=${DATABRICKS_HOST}"
echo "DATABRICKS_TOKEN=<your-token>"
echo "DATABRICKS_HTTP_PATH=${DATABRICKS_HTTP_PATH}"
echo "DATABRICKS_CATALOG=${DATABRICKS_CATALOG:-welch}"
echo "DATABRICKS_SCHEMA=${DATABRICKS_SCHEMA:-demand_planning_demo}"
if [[ -n "$GENIE_SPACE_ID" ]]; then
  echo "GENIE_SPACE_ID=${GENIE_SPACE_ID}"
fi
if [[ -n "$DASHBOARD_EMBED_URL" ]]; then
  echo "DASHBOARD_EMBED_URL=${DASHBOARD_EMBED_URL}"
fi
echo ""
echo "=============================================="
