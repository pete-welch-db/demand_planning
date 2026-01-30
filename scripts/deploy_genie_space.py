#!/usr/bin/env python3
"""
Deploy a Genie space to Databricks using the Genie Management API.

This script creates or updates a Genie space based on the configuration in
genie/genie_space_config.json. It uses the Databricks SDK for Python.

Usage:
    python scripts/deploy_genie_space.py

Environment variables required:
    DATABRICKS_HOST - Workspace URL (e.g., https://adb-xxx.azuredatabricks.net)
    DATABRICKS_TOKEN - Personal access token or OAuth token
    
Optional:
    GENIE_WAREHOUSE_ID - SQL warehouse ID (defaults to DATABRICKS_HTTP_PATH extraction)
    GENIE_PARENT_PATH - Folder path for the space (defaults to /Shared)
"""

import json
import os
import sys
from pathlib import Path

def main():
    # Check for required environment variables
    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    
    if not host or not token:
        print("ERROR: DATABRICKS_HOST and DATABRICKS_TOKEN environment variables required")
        sys.exit(1)
    
    # Try to import databricks SDK
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.dashboards import GenieAPI
    except ImportError:
        print("ERROR: databricks-sdk not installed. Run: pip install databricks-sdk")
        sys.exit(1)
    
    # Load configuration
    config_path = Path(__file__).parent.parent / "genie" / "genie_space_config.json"
    if not config_path.exists():
        print(f"ERROR: Config file not found: {config_path}")
        sys.exit(1)
    
    with open(config_path) as f:
        config = json.load(f)
    
    # Get warehouse ID
    warehouse_id = os.environ.get("GENIE_WAREHOUSE_ID")
    if not warehouse_id:
        # Try to extract from DATABRICKS_HTTP_PATH
        http_path = os.environ.get("DATABRICKS_HTTP_PATH", "")
        if "/warehouses/" in http_path:
            warehouse_id = http_path.split("/warehouses/")[-1]
        else:
            print("ERROR: GENIE_WAREHOUSE_ID or DATABRICKS_HTTP_PATH with warehouse ID required")
            sys.exit(1)
    
    parent_path = os.environ.get("GENIE_PARENT_PATH", "/Shared")
    
    print(f"Deploying Genie space: {config['title']}")
    print(f"  Host: {host}")
    print(f"  Warehouse: {warehouse_id}")
    print(f"  Parent path: {parent_path}")
    print(f"  Tables: {len(config['tables'])}")
    
    # Initialize client
    w = WorkspaceClient(host=host, token=token)
    
    # Check if space already exists by listing spaces
    existing_space_id = None
    try:
        spaces_response = w.genie.list_spaces()
        if hasattr(spaces_response, 'spaces') and spaces_response.spaces:
            for space in spaces_response.spaces:
                if space.title == config['title']:
                    existing_space_id = space.id
                    print(f"  Found existing space: {existing_space_id}")
                    break
    except Exception as e:
        print(f"  Warning: Could not list existing spaces: {e}")
    
    # Build the serialized space payload
    # Note: The exact format depends on Databricks' internal schema
    # This is a simplified version - you may need to capture a real serialized_space
    # from an existing Genie space using: w.genie.get_space(space_id, include_serialized_space=True)
    
    serialized_space = json.dumps({
        "tables": config["tables"],
        "instructions": config.get("instructions", ""),
        "sample_queries": config.get("sample_queries", [])
    })
    
    try:
        if existing_space_id:
            # Update existing space
            print(f"  Updating existing Genie space...")
            result = w.genie.update_space(
                space_id=existing_space_id,
                title=config["title"],
                description=config.get("description"),
                serialized_space=serialized_space,
                warehouse_id=warehouse_id
            )
            print(f"  Updated space: {result.id}")
        else:
            # Create new space
            print(f"  Creating new Genie space...")
            result = w.genie.create_space(
                warehouse_id=warehouse_id,
                title=config["title"],
                description=config.get("description"),
                parent_path=parent_path,
                serialized_space=serialized_space
            )
            print(f"  Created space: {result.id}")
        
        # Output the space ID for downstream use
        space_url = f"{host}/genie/spaces/{result.id}"
        print(f"\n  Genie Space URL: {space_url}")
        print(f"  Space ID: {result.id}")
        
        # Write space ID to a file for reference
        output_file = Path(__file__).parent.parent / ".genie_space_id"
        with open(output_file, "w") as f:
            f.write(result.id)
        print(f"  Space ID saved to: {output_file}")
        
        # Also output the env var format for easy copy-paste
        print(f"\n  Add to .env for Streamlit app:")
        print(f"  GENIE_SPACE_ID={result.id}")
        
        return 0
        
    except Exception as e:
        print(f"\nERROR: Failed to deploy Genie space: {e}")
        print("\nNote: The Genie Management API requires specific serialized_space format.")
        print("You may need to:")
        print("1. Create a Genie space manually in the UI first")
        print("2. Export its serialized_space using:")
        print("   w.genie.get_space(space_id, include_serialized_space=True)")
        print("3. Use that format as a template for automation")
        return 1


if __name__ == "__main__":
    sys.exit(main())
