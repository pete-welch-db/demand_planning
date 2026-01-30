#!/bin/bash
# ===========================================
# Update .env with deployed dashboard and Genie IDs
# ===========================================
# Run this after deploy.sh to auto-update your local .env
# with the IDs from the deployed resources.
#
# Usage:
#   ./scripts/update_env_ids.sh [target]

set -e

TARGET="${1:-azure}"

# Load existing .env
if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

echo "🔄 Updating .env with deployed resource IDs..."

# Get dashboard ID from bundle
DASHBOARD_ID=""
if command -v databricks &> /dev/null; then
  DASHBOARD_ID=$(databricks bundle summary --target "$TARGET" 2>/dev/null | grep -A2 "demand_planning_control_tower" | grep "id:" | awk '{print $2}' | tr -d '"' || echo "")
fi

# Get org ID from host
ORG_ID=""
if [[ -n "$DATABRICKS_HOST" ]]; then
  ORG_ID=$(echo "$DATABRICKS_HOST" | grep -oE 'adb-[0-9]+' | cut -d'-' -f2 || echo "")
fi

# Read Genie space ID if available
GENIE_ID=""
if [[ -f .genie_space_id ]]; then
  GENIE_ID=$(cat .genie_space_id)
fi

# Build the embed URL
EMBED_URL=""
if [[ -n "$DASHBOARD_ID" && -n "$DATABRICKS_HOST" ]]; then
  if [[ -n "$ORG_ID" ]]; then
    EMBED_URL="${DATABRICKS_HOST}/embed/dashboardsv3/${DASHBOARD_ID}?o=${ORG_ID}"
  else
    EMBED_URL="${DATABRICKS_HOST}/embed/dashboardsv3/${DASHBOARD_ID}"
  fi
fi

# Update .env file
update_or_add_env() {
  local key=$1
  local value=$2
  local file=".env"
  
  if [[ -z "$value" ]]; then
    return
  fi
  
  if grep -q "^${key}=" "$file" 2>/dev/null; then
    # Update existing
    if [[ "$(uname)" == "Darwin" ]]; then
      sed -i '' "s|^${key}=.*|${key}=${value}|" "$file"
    else
      sed -i "s|^${key}=.*|${key}=${value}|" "$file"
    fi
    echo "  Updated: ${key}"
  else
    # Add new
    echo "${key}=${value}" >> "$file"
    echo "  Added: ${key}"
  fi
}

# Ensure .env exists
touch .env

# Update the IDs
if [[ -n "$EMBED_URL" ]]; then
  update_or_add_env "DASHBOARD_EMBED_URL" "$EMBED_URL"
else
  echo "  Skipped: DASHBOARD_EMBED_URL (dashboard ID not found)"
fi

if [[ -n "$GENIE_ID" ]]; then
  update_or_add_env "GENIE_SPACE_ID" "$GENIE_ID"
else
  echo "  Skipped: GENIE_SPACE_ID (not deployed yet)"
fi

echo ""
echo "✅ Done! Your .env is updated."
echo ""
echo "To run the Streamlit app:"
echo "  cd app && streamlit run app.py"
