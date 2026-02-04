#!/bin/bash
# ===========================================
# Demand Planning Demo - Fully Automated Deploy
# ===========================================
# Full deployment pipeline:
#   1. Bundle deploy (jobs, dashboards, app definition) with --force
#   2. Run data pipeline job
#   3. Deploy Genie space
#   4. Get Dashboard ID
#   5. Update app.yaml with Genie + Dashboard IDs
#   6. Sync & deploy Streamlit app (with IDs already configured)
#   7. Grant permissions to app service principal
#
# Usage:
#   ./deploy.sh [target]
#
# Examples:
#   ./deploy.sh          # Deploy to default (dev) target
#   ./deploy.sh azure    # Deploy to azure target
#
# Requirements:
#   - databricks CLI installed and authenticated
#   - python3 with databricks-sdk installed
#   - .env file with DATABRICKS_HOST, DATABRICKS_TOKEN, etc.

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}✅${NC} $1"; }
log_warn() { echo -e "${YELLOW}⚠️${NC} $1"; }
log_error() { echo -e "${RED}❌${NC} $1"; }

# Load .env if present
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
  if [[ -n "$DATABRICKS_HOST" && -n "$DATABRICKS_TOKEN" ]]; then
    unset DATABRICKS_CONFIG_PROFILE
  fi
fi

if [[ "$TARGET" == "-h" || "$TARGET" == "--help" ]]; then
  echo "Usage: ./deploy.sh [target] [profile]"
  echo ""
  echo "Targets: dev, azure"
  echo ""
  echo "Authentication:"
  echo "  1. DATABRICKS_HOST + DATABRICKS_TOKEN env vars (from .env)"
  echo "  2. CLI profile: ./deploy.sh <target> <profile>"
  exit 0
fi

echo ""
echo "🚀 Deploying Demand Planning Demo to target: $TARGET"
echo "=============================================="

# Validate required tools
if ! command -v databricks &> /dev/null; then
  log_error "databricks CLI not found. Install with: pip install databricks-cli"
  exit 1
fi

if ! command -v python3 &> /dev/null; then
  log_error "python3 not found"
  exit 1
fi

if ! python3 -c "import databricks.sdk" 2>/dev/null; then
  log_warn "databricks-sdk not installed. Install with: pip install databricks-sdk"
fi

# ============================================
# Step 1: Validate and Deploy Bundle (with --force)
# ============================================
echo ""
echo "📋 Step 1: Validating and deploying bundle..."
databricks bundle validate --target "$TARGET" $PROFILE_FLAG

echo "  Deploying bundle with --force (overrides remote changes)..."
databricks bundle deploy --target "$TARGET" $PROFILE_FLAG --force
log_info "Bundle deployed"

# ============================================
# Step 2: Run Data Pipeline Job
# ============================================
echo ""
echo "🔄 Step 2: Running data pipeline job..."
echo "   (UC Setup → Bronze → DLT → Forecast → ML → KPIs → Dashboards)"
if databricks bundle run Demand_Planning_Demo_Job --target "$TARGET" $PROFILE_FLAG; then
  log_info "Data pipeline completed"
else
  log_warn "Data pipeline had issues (may already have data)"
fi

# ============================================
# Step 3: Deploy Genie Space
# ============================================
echo ""
echo "🤖 Step 3: Deploying Genie space..."

# Use existing GENIE_SPACE_ID from .env or cached file if available
GENIE_SPACE_ID="${GENIE_SPACE_ID:-}"
[[ -z "$GENIE_SPACE_ID" && -f .genie_space_id ]] && GENIE_SPACE_ID=$(cat .genie_space_id)

if [[ -n "$GENIE_SPACE_ID" ]]; then
  log_info "Using existing Genie space: $GENIE_SPACE_ID"
elif [[ -f scripts/deploy_genie_space.py ]]; then
  if python3 scripts/deploy_genie_space.py 2>&1; then
    if [[ -f .genie_space_id ]]; then
      GENIE_SPACE_ID=$(cat .genie_space_id)
      log_info "Genie space deployed: $GENIE_SPACE_ID"
    fi
  else
    log_warn "Genie space deployment had issues (using existing if available)"
    [[ -f .genie_space_id ]] && GENIE_SPACE_ID=$(cat .genie_space_id)
  fi
else
  log_warn "Genie deploy script not found"
fi

# ============================================
# Step 4: Get Dashboard ID from Bundle Summary
# ============================================
echo ""
echo "📊 Step 4: Getting dashboard ID..."

# Use existing dashboard ID from cached file if available
DASHBOARD_ID=""
[[ -f .dashboard_id ]] && DASHBOARD_ID=$(cat .dashboard_id)

if [[ -z "$DASHBOARD_ID" ]]; then
  # Method 1: Parse bundle summary for dashboard URL
  BUNDLE_SUMMARY=$(databricks bundle summary --target "$TARGET" $PROFILE_FLAG 2>/dev/null || echo "")
  if [[ -n "$BUNDLE_SUMMARY" ]]; then
    # Extract dashboard ID from URL like: https://...databricks.net/dashboardsv3/01f101d4d3ed17309d990aa37698f99b/published
    DASHBOARD_ID=$(echo "$BUNDLE_SUMMARY" | grep -oE 'dashboardsv3/[a-f0-9]+' | head -1 | sed 's|dashboardsv3/||' || echo "")
  fi
  
  # Method 2: If still empty, try Python SDK
  if [[ -z "$DASHBOARD_ID" ]]; then
    DASHBOARD_ID=$(python3 -c "
import os
from databricks.sdk import WorkspaceClient
w = WorkspaceClient(host=os.environ.get('DATABRICKS_HOST'), token=os.environ.get('DATABRICKS_TOKEN'))
dashboards = list(w.lakeview.list())
for d in dashboards:
    if 'Demand Planning' in (d.display_name or ''):
        print(d.dashboard_id)
        break
" 2>/dev/null || echo "")
  fi
else
  log_info "Using cached dashboard ID: $DASHBOARD_ID"
fi

# Get org ID from host for embed URL
ORG_ID=$(echo "$DATABRICKS_HOST" | grep -oE 'adb-[0-9]+' | sed 's/adb-//' || echo "")

DASHBOARD_EMBED_URL=""
if [[ -n "$DASHBOARD_ID" ]]; then
  log_info "Dashboard ID: $DASHBOARD_ID"
  echo "$DASHBOARD_ID" > .dashboard_id
  DASHBOARD_EMBED_URL="${DATABRICKS_HOST}/embed/dashboardsv3/${DASHBOARD_ID}"
  [[ -n "$ORG_ID" ]] && DASHBOARD_EMBED_URL="${DASHBOARD_EMBED_URL}?o=${ORG_ID}"
else
  log_warn "Could not find dashboard ID"
fi

# ============================================
# Step 5: Update app.yaml with Resource IDs BEFORE deploying
# ============================================
echo ""
echo "📝 Step 5: Updating app.yaml with resource IDs..."
APP_NAME="demand-planning-control-tower"

# Update GENIE_SPACE_ID in app.yaml
if [[ -n "$GENIE_SPACE_ID" ]]; then
  echo "  Setting GENIE_SPACE_ID=$GENIE_SPACE_ID"
  python3 << YAML_UPDATE
import re
with open('app/app.yaml', 'r') as f:
    content = f.read()
# Update GENIE_SPACE_ID
if 'GENIE_SPACE_ID' in content:
    content = re.sub(
        r'(- name: GENIE_SPACE_ID\s+value: ")[^"]*(")',
        r'\g<1>$GENIE_SPACE_ID\g<2>',
        content
    )
else:
    # Add it before USE_MOCK_DATA
    content = content.replace(
        '  - name: USE_MOCK_DATA',
        '  - name: GENIE_SPACE_ID\n    value: "$GENIE_SPACE_ID"\n  - name: USE_MOCK_DATA'
    )
with open('app/app.yaml', 'w') as f:
    f.write(content)
YAML_UPDATE
else
  log_warn "No GENIE_SPACE_ID to configure"
fi

# Update DASHBOARD_EMBED_URL in app.yaml
if [[ -n "$DASHBOARD_EMBED_URL" ]]; then
  echo "  Setting DASHBOARD_EMBED_URL"
  python3 << YAML_UPDATE
import re
with open('app/app.yaml', 'r') as f:
    content = f.read()
if 'DASHBOARD_EMBED_URL' in content:
    content = re.sub(
        r'(- name: DASHBOARD_EMBED_URL\s+value: ")[^"]*(")',
        r'\g<1>$DASHBOARD_EMBED_URL\g<2>',
        content
    )
else:
    # Add before USE_MOCK_DATA
    content = content.replace(
        '  - name: USE_MOCK_DATA',
        '  - name: DASHBOARD_EMBED_URL\n    value: "$DASHBOARD_EMBED_URL"\n  - name: USE_MOCK_DATA'
    )
with open('app/app.yaml', 'w') as f:
    f.write(content)
YAML_UPDATE
fi

log_info "app.yaml updated with resource IDs"

# ============================================
# Step 6: Sync and Deploy Streamlit App
# ============================================
echo ""
echo "📱 Step 6: Deploying Streamlit app..."

# Get workspace path from bundle
WORKSPACE_ROOT=$(echo "$BUNDLE_SUMMARY" | grep -E 'root_path:' | head -1 | awk '{print $2}' | tr -d '"' || echo "")
[[ -z "$WORKSPACE_ROOT" ]] && WORKSPACE_ROOT="/Workspace/Users/pete.welch@databricks.com/demand_planning"
APP_WORKSPACE_PATH="${WORKSPACE_ROOT}/files/app"

echo "  Syncing app source to: $APP_WORKSPACE_PATH"
databricks sync ./app "$APP_WORKSPACE_PATH" $PROFILE_FLAG --full 2>/dev/null || true

echo "  Deploying app via CLI..."
if databricks apps deploy "$APP_NAME" --source-code-path "$APP_WORKSPACE_PATH" $PROFILE_FLAG 2>&1; then
  log_info "App deployed with GENIE_SPACE_ID=$GENIE_SPACE_ID"
else
  log_warn "App deploy had issues (may already be running)"
fi

# Wait for app to be ready
sleep 5

# ============================================
# Step 7: Grant App Service Principal Permissions
# ============================================
echo ""
echo "🔐 Step 7: Granting permissions to app service principal..."

python3 << PYTHON_SCRIPT
import os
import sys
import requests
import time

try:
    from databricks.sdk import WorkspaceClient
    
    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN")
    
    if not host or not token:
        print("  ⚠️  Missing DATABRICKS_HOST or DATABRICKS_TOKEN")
        sys.exit(0)
    
    w = WorkspaceClient(host=host, token=token)
    
    app_name = "$APP_NAME"
    catalog = os.environ.get("DATABRICKS_CATALOG", "welch")
    schema = os.environ.get("DATABRICKS_SCHEMA", "demand_planning_demo")
    http_path = os.environ.get("DATABRICKS_HTTP_PATH", "")
    warehouse_id = http_path.split("/")[-1] if "/" in http_path else http_path
    genie_space_id = "$GENIE_SPACE_ID" if "$GENIE_SPACE_ID" and "$GENIE_SPACE_ID" != "" else None
    dashboard_id = "$DASHBOARD_ID" if "$DASHBOARD_ID" and "$DASHBOARD_ID" != "" else None
    
    # Get app service principal
    try:
        # Retry a few times in case app is still initializing
        for attempt in range(3):
            try:
                app = w.apps.get(app_name)
                break
            except Exception:
                if attempt < 2:
                    time.sleep(3)
                else:
                    raise
        
        sp_id = getattr(app, 'service_principal_id', None)
        sp_name = getattr(app, 'service_principal_name', None)
        
        if not sp_id:
            print("  ⚠️  Could not find app service principal ID")
            sys.exit(0)
            
        print(f"  Service principal: {sp_name}")
        
    except Exception as e:
        print(f"  ⚠️  Could not get app details: {e}")
        sys.exit(0)
    
    # Get service principal application_id (UUID)
    try:
        sp_details = w.service_principals.get(str(sp_id))
        app_uuid = getattr(sp_details, 'application_id', None)
        print(f"  Application ID (UUID): {app_uuid}")
    except Exception as e:
        print(f"  ⚠️  Could not get SP details: {e}")
        app_uuid = None
    
    if not app_uuid:
        print("  ⚠️  No application UUID found, cannot grant permissions")
        sys.exit(0)
    
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    # 1. Grant SQL Warehouse access (CAN_USE)
    if warehouse_id:
        print(f"  Granting SQL Warehouse access ({warehouse_id})...")
        try:
            resp = requests.patch(
                f"{host}/api/2.0/permissions/sql/warehouses/{warehouse_id}",
                headers=headers,
                json={"access_control_list": [{"service_principal_name": app_uuid, "permission_level": "CAN_USE"}]}
            )
            if resp.status_code == 200:
                print(f"    ✅ Warehouse CAN_USE granted")
            else:
                print(f"    ⚠️  Warehouse: {resp.status_code}")
        except Exception as e:
            print(f"    ⚠️  Warehouse: {e}")
    
    # 2. Grant Unity Catalog access via SQL GRANT statements
    print(f"  Granting Unity Catalog access ({catalog}.{schema})...")
    grants = [
        f"GRANT USE CATALOG ON CATALOG \`{catalog}\` TO \`{app_uuid}\`",
        f"GRANT USE SCHEMA ON SCHEMA \`{catalog}\`.\`{schema}\` TO \`{app_uuid}\`",
        f"GRANT SELECT ON SCHEMA \`{catalog}\`.\`{schema}\` TO \`{app_uuid}\`"
    ]
    
    for stmt in grants:
        try:
            resp = w.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=stmt,
                wait_timeout="30s"
            )
            status = resp.status.state.value if resp.status else "UNKNOWN"
            grant_type = stmt.split(" ON ")[0].replace("GRANT ", "")
            if status == "SUCCEEDED":
                print(f"    ✅ {grant_type}")
            else:
                print(f"    ⚠️  {grant_type}: {status}")
        except Exception as e:
            print(f"    ⚠️  Grant failed: {e}")
    
    # 3. Grant Dashboard access
    if dashboard_id:
        print(f"  Granting Dashboard access ({dashboard_id})...")
        try:
            resp = requests.patch(
                f"{host}/api/2.0/permissions/dashboards/{dashboard_id}",
                headers=headers,
                json={"access_control_list": [{"service_principal_name": app_uuid, "permission_level": "CAN_VIEW"}]}
            )
            if resp.status_code == 200:
                print(f"    ✅ Dashboard CAN_VIEW granted")
            else:
                print(f"    ⚠️  Dashboard: {resp.status_code}")
        except Exception as e:
            print(f"    ⚠️  Dashboard: {e}")
    
    # 4. Grant Genie Space access
    if genie_space_id:
        print(f"  Granting Genie Space access ({genie_space_id})...")
        try:
            resp = requests.patch(
                f"{host}/api/2.0/genie/spaces/{genie_space_id}/permissions",
                headers=headers,
                json={"access_control_list": [{"service_principal_name": app_uuid, "permission_level": "CAN_QUERY"}]}
            )
            if resp.status_code in (200, 201):
                print(f"    ✅ Genie CAN_QUERY granted")
            else:
                print(f"    ⚠️  Genie: {resp.status_code}")
        except Exception as e:
            print(f"    ⚠️  Genie: {e}")
    
    print("  ✅ Permissions configured")
    
except Exception as e:
    print(f"  ⚠️  Permissions script error: {e}")
PYTHON_SCRIPT

# ============================================
# Summary
# ============================================
echo ""
echo "=============================================="
echo "✅ Deployment Complete!"
echo "=============================================="
echo ""
echo "Resources deployed:"
echo "  📊 Dashboard: ${DATABRICKS_HOST}/dashboardsv3/${DASHBOARD_ID:-<unknown>}"
[[ -n "$GENIE_SPACE_ID" ]] && echo "  🤖 Genie: ${DATABRICKS_HOST}/genie/spaces/${GENIE_SPACE_ID}"
echo "  📱 App: ${DATABRICKS_HOST}/apps/${APP_NAME}"
echo ""
echo "Permissions granted to app service principal:"
echo "  ✅ SQL Warehouse (CAN_USE)"
echo "  ✅ Unity Catalog (USE_CATALOG, USE_SCHEMA, SELECT)"
[[ -n "$DASHBOARD_ID" ]] && echo "  ✅ Dashboard (CAN_VIEW)"
[[ -n "$GENIE_SPACE_ID" ]] && echo "  ✅ Genie Space (CAN_QUERY)"
echo ""
echo "=============================================="
echo "📋 Local .env should contain:"
echo "=============================================="
echo "DATABRICKS_HOST=${DATABRICKS_HOST}"
echo "DATABRICKS_TOKEN=<your-token>"
echo "DATABRICKS_HTTP_PATH=${DATABRICKS_HTTP_PATH}"
echo "DATABRICKS_CATALOG=${DATABRICKS_CATALOG:-welch}"
echo "DATABRICKS_SCHEMA=${DATABRICKS_SCHEMA:-demand_planning_demo}"
[[ -n "$GENIE_SPACE_ID" ]] && echo "GENIE_SPACE_ID=${GENIE_SPACE_ID}"
[[ -n "$DASHBOARD_EMBED_URL" ]] && echo "DASHBOARD_EMBED_URL=${DASHBOARD_EMBED_URL}"
echo ""
echo "=============================================="
