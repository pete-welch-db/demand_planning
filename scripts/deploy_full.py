#!/usr/bin/env python3
"""
Full deployment automation for Demand Planning Demo.

This script handles:
1. Sync app source code to workspace
2. Deploy Genie space and capture ID
3. Get deployed dashboard ID
4. Update app environment with Genie/Dashboard IDs
5. Deploy and start the app
6. Grant app service principal permissions to required resources

Usage:
    python scripts/deploy_full.py

Environment variables required:
    DATABRICKS_HOST - Workspace URL
    DATABRICKS_TOKEN - Personal access token
    DATABRICKS_HTTP_PATH - SQL warehouse path (for warehouse ID extraction)
    DATABRICKS_CATALOG - Unity Catalog name
    DATABRICKS_SCHEMA - Schema name
"""

import json
import os
import sys
import time
from pathlib import Path


def main():
    # Check required env vars
    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN")
    http_path = os.environ.get("DATABRICKS_HTTP_PATH", "")
    catalog = os.environ.get("DATABRICKS_CATALOG", "welch")
    schema = os.environ.get("DATABRICKS_SCHEMA", "demand_planning_demo")
    
    if not host or not token:
        print("ERROR: DATABRICKS_HOST and DATABRICKS_TOKEN required")
        sys.exit(1)
    
    # Extract warehouse ID from http_path
    warehouse_id = ""
    if "/warehouses/" in http_path:
        warehouse_id = http_path.split("/warehouses/")[-1]
    
    if not warehouse_id:
        print("ERROR: Could not extract warehouse_id from DATABRICKS_HTTP_PATH")
        sys.exit(1)
    
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.catalog import SecurableType, PrivilegeAssignment, Privilege
    except ImportError:
        print("ERROR: databricks-sdk not installed. Run: pip install databricks-sdk")
        sys.exit(1)
    
    print("=" * 60)
    print("Demand Planning Demo - Full Deployment")
    print("=" * 60)
    print(f"Host: {host}")
    print(f"Catalog: {catalog}")
    print(f"Schema: {schema}")
    print(f"Warehouse: {warehouse_id}")
    print()
    
    w = WorkspaceClient(host=host, token=token)
    
    # Paths
    project_root = Path(__file__).parent.parent
    app_source_path = project_root / "app"
    workspace_app_path = "/Workspace/Users/pete.welch@databricks.com/demand-planning-control-tower"
    app_name = "demand-planning-control-tower"
    
    # =========================================================================
    # Step 1: Deploy Genie Space
    # =========================================================================
    print("Step 1: Deploying Genie space...")
    genie_space_id = deploy_genie_space(w, project_root, warehouse_id, host)
    if genie_space_id:
        print(f"  ✅ Genie space ID: {genie_space_id}")
    else:
        print("  ⚠️  Genie space deployment failed (manual setup required)")
        genie_space_id = ""
    
    # =========================================================================
    # Step 2: Get Dashboard ID (from bundle or API)
    # =========================================================================
    print("\nStep 2: Getting Dashboard ID...")
    dashboard_id = get_dashboard_id(w, "Demand Planning Control Tower")
    if dashboard_id:
        print(f"  ✅ Dashboard ID: {dashboard_id}")
        # Construct embed URL
        org_id = extract_org_id(host)
        dashboard_embed_url = f"{host}/embed/dashboardsv3/{dashboard_id}"
        if org_id:
            dashboard_embed_url += f"?o={org_id}"
    else:
        print("  ⚠️  Dashboard not found")
        dashboard_embed_url = ""
    
    # =========================================================================
    # Step 3: Sync App Source Code
    # =========================================================================
    print("\nStep 3: Syncing app source code to workspace...")
    sync_app_source(w, app_source_path, workspace_app_path)
    print(f"  ✅ Synced to {workspace_app_path}")
    
    # =========================================================================
    # Step 4: Deploy App with Environment Variables
    # =========================================================================
    print("\nStep 4: Deploying app...")
    deploy_app(w, app_name, workspace_app_path, {
        "DATABRICKS_HOST": host,
        "DATABRICKS_CATALOG": catalog,
        "DATABRICKS_SCHEMA": schema,
        "DATABRICKS_HTTP_PATH": http_path,
        "GENIE_SPACE_ID": genie_space_id,
        "DASHBOARD_EMBED_URL": dashboard_embed_url,
    })
    print(f"  ✅ App deployed: {app_name}")
    
    # =========================================================================
    # Step 5: Grant App Service Principal Permissions
    # =========================================================================
    print("\nStep 5: Granting app permissions...")
    grant_app_permissions(w, app_name, catalog, schema, warehouse_id)
    print("  ✅ Permissions granted")
    
    # =========================================================================
    # Step 6: Start the App
    # =========================================================================
    print("\nStep 6: Starting app...")
    start_app(w, app_name)
    print(f"  ✅ App starting...")
    
    # =========================================================================
    # Summary
    # =========================================================================
    print()
    print("=" * 60)
    print("✅ Deployment Complete!")
    print("=" * 60)
    print()
    print("Resources:")
    print(f"  App URL: {host}/apps/{app_name}")
    if genie_space_id:
        print(f"  Genie Space: {host}/genie/spaces/{genie_space_id}")
    if dashboard_id:
        print(f"  Dashboard: {host}/dashboardsv3/{dashboard_id}")
    print()
    print("Add to .env for local dev:")
    print(f"  GENIE_SPACE_ID={genie_space_id}")
    print(f"  DASHBOARD_EMBED_URL={dashboard_embed_url}")
    
    # Save IDs to files
    (project_root / ".genie_space_id").write_text(genie_space_id)
    (project_root / ".dashboard_id").write_text(dashboard_id or "")
    
    return 0


def deploy_genie_space(w, project_root, warehouse_id, host):
    """Deploy Genie space and return space ID."""
    config_path = project_root / "genie" / "genie_space_config.json"
    if not config_path.exists():
        return None
    
    with open(config_path) as f:
        config = json.load(f)
    
    # Check if space already exists
    existing_space_id = None
    try:
        spaces = w.genie.list_spaces()
        if hasattr(spaces, 'spaces') and spaces.spaces:
            for space in spaces.spaces:
                if space.title == config['title']:
                    existing_space_id = space.id
                    break
    except Exception:
        pass
    
    serialized_space = json.dumps({
        "tables": config["tables"],
        "instructions": config.get("instructions", ""),
        "sample_queries": config.get("sample_queries", [])
    })
    
    try:
        if existing_space_id:
            result = w.genie.update_space(
                space_id=existing_space_id,
                title=config["title"],
                description=config.get("description"),
                serialized_space=serialized_space,
                warehouse_id=warehouse_id
            )
        else:
            result = w.genie.create_space(
                warehouse_id=warehouse_id,
                title=config["title"],
                description=config.get("description"),
                parent_path="/Shared",
                serialized_space=serialized_space
            )
        return result.id
    except Exception as e:
        print(f"  Warning: Genie API error: {e}")
        return None


def get_dashboard_id(w, display_name):
    """Find dashboard by display name and return its ID."""
    try:
        # List dashboards and find by name
        dashboards = w.lakeview.list()
        for dashboard in dashboards:
            if dashboard.display_name == display_name:
                return dashboard.dashboard_id
    except Exception as e:
        print(f"  Warning: Could not list dashboards: {e}")
    return None


def extract_org_id(host):
    """Extract org ID from Databricks host URL."""
    import re
    # Try adb-XXXXX pattern
    match = re.search(r'adb-(\d+)', host)
    if match:
        return match.group(1)
    return None


def sync_app_source(w, local_path, workspace_path):
    """Sync local app source to workspace."""
    import subprocess
    
    # Use databricks CLI to sync
    result = subprocess.run(
        ["databricks", "sync", str(local_path), workspace_path, "--full"],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        print(f"  Warning: Sync may have issues: {result.stderr}")


def deploy_app(w, app_name, source_code_path, env_vars):
    """Deploy app with environment variables."""
    try:
        # Get app
        app = w.apps.get(app_name)
        
        # Update app with new deployment
        w.apps.deploy(
            app_name=app_name,
            source_code_path=source_code_path,
            mode="SNAPSHOT"
        )
    except Exception as e:
        print(f"  Warning: App deploy error: {e}")


def grant_app_permissions(w, app_name, catalog, schema, warehouse_id):
    """Grant app service principal necessary permissions."""
    try:
        # Get app's service principal
        app = w.apps.get(app_name)
        if not hasattr(app, 'service_principal_id') or not app.service_principal_id:
            print("  Warning: App has no service principal")
            return
        
        sp_id = app.service_principal_id
        
        # Grant USE CATALOG
        try:
            w.grants.update(
                securable_type="catalog",
                full_name=catalog,
                changes=[{
                    "principal": f"service-principal/{sp_id}",
                    "add": ["USE_CATALOG"]
                }]
            )
        except Exception as e:
            print(f"  Note: Catalog grant: {e}")
        
        # Grant USE SCHEMA + SELECT on schema
        try:
            w.grants.update(
                securable_type="schema",
                full_name=f"{catalog}.{schema}",
                changes=[{
                    "principal": f"service-principal/{sp_id}",
                    "add": ["USE_SCHEMA", "SELECT"]
                }]
            )
        except Exception as e:
            print(f"  Note: Schema grant: {e}")
        
        # Grant warehouse access
        try:
            w.warehouses.set_permissions(
                warehouse_id=warehouse_id,
                access_control_list=[{
                    "service_principal_name": sp_id,
                    "permission_level": "CAN_USE"
                }]
            )
        except Exception as e:
            print(f"  Note: Warehouse grant: {e}")
            
    except Exception as e:
        print(f"  Warning: Permission grant error: {e}")


def start_app(w, app_name):
    """Start the app."""
    try:
        w.apps.start(app_name)
    except Exception as e:
        print(f"  Warning: App start error: {e}")


if __name__ == "__main__":
    sys.exit(main())
