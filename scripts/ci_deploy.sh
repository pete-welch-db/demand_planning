#!/usr/bin/env bash
# CI-friendly Databricks Asset Bundle deploy + run
#
# Auth options:
# - PAT (simple): set DATABRICKS_HOST + DATABRICKS_TOKEN
# - OIDC (recommended): see docs/AUTOMATED_DEPLOYMENT.md
#
# Usage:
#   scripts/ci_deploy.sh [target]
#
# Example:
#   scripts/ci_deploy.sh azure

set -euo pipefail

TARGET="${1:-azure}"

if [[ -z "${DATABRICKS_HOST:-}" ]]; then
  echo "ERROR: DATABRICKS_HOST is not set"
  exit 1
fi

# If using PAT auth, token is required. For OIDC, this may be unset.
if [[ "${DATABRICKS_AUTH_TYPE:-pat}" == "pat" && -z "${DATABRICKS_TOKEN:-}" ]]; then
  echo "ERROR: DATABRICKS_TOKEN is not set (PAT auth)"
  exit 1
fi

echo "Validating bundle..."
databricks bundle validate --target "$TARGET"

echo "Deploying bundle..."
databricks bundle deploy --target "$TARGET"

echo "Running job: Demand_Planning_Demo_Job"
databricks bundle run Demand_Planning_Demo_Job --target "$TARGET"

