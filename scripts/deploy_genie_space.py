#!/usr/bin/env python3
"""
Deploy a Genie space to Databricks using the correct serialized_space format.

Usage:
    python scripts/deploy_genie_space.py
"""

import json
import os
import sys
import uuid
from pathlib import Path


def generate_id():
    """Generate a Databricks-style ID."""
    return uuid.uuid4().hex[:32]


def main():
    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN")
    
    if not host or not token:
        print("ERROR: DATABRICKS_HOST and DATABRICKS_TOKEN required")
        sys.exit(1)
    
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        print("ERROR: pip install databricks-sdk")
        sys.exit(1)
    
    config_path = Path(__file__).parent.parent / "genie" / "genie_space_config.json"
    with open(config_path) as f:
        config = json.load(f)
    
    warehouse_id = os.environ.get("GENIE_WAREHOUSE_ID")
    if not warehouse_id:
        http_path = os.environ.get("DATABRICKS_HTTP_PATH", "")
        if "/warehouses/" in http_path:
            warehouse_id = http_path.split("/warehouses/")[-1]
        else:
            print("ERROR: Warehouse ID required")
            sys.exit(1)
    
    parent_path = os.environ.get("GENIE_PARENT_PATH", "/Shared")
    
    print(f"Deploying: {config['title']}")
    print(f"  Warehouse: {warehouse_id}")
    print(f"  Tables: {len(config['tables'])}")
    
    w = WorkspaceClient(host=host, token=token)
    
    # Build serialized_space in correct Databricks format (version 2)
    serialized_space_obj = {
        "version": 2,
        "config": {
            "sample_questions": [
                {
                    "id": generate_id(),
                    "question": [q["question"]]
                }
                for q in config.get("sample_queries", [])
            ]
        },
        "data_sources": {
            "tables": [
                {"identifier": table}
                for table in sorted(config["tables"])  # Must be sorted!
            ]
        },
        "instructions": {
            "text_instructions": [
                {
                    "id": generate_id(),
                    "content": [config.get("instructions", "")]
                }
            ]
        }
    }
    
    serialized_space = json.dumps(serialized_space_obj)
    print(f"  Serialized space: {len(serialized_space)} chars")
    
    # Check for existing space
    import requests
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    existing_space_id = None
    try:
        resp = requests.get(f"{host}/api/2.0/genie/spaces", headers=headers)
        if resp.status_code == 200:
            for space in resp.json().get("spaces", []):
                if space.get("title") == config["title"]:
                    existing_space_id = space.get("space_id")
                    print(f"  Found existing: {existing_space_id}")
                    break
    except Exception as e:
        print(f"  Note: {e}")
    
    try:
        if existing_space_id:
            # Update existing space
            print(f"  Updating existing space...")
            resp = requests.patch(
                f"{host}/api/2.0/genie/spaces/{existing_space_id}",
                headers=headers,
                json={
                    "title": config["title"],
                    "description": config.get("description", ""),
                    "warehouse_id": warehouse_id,
                    "serialized_space": serialized_space
                }
            )
            
            if resp.status_code in [200, 201]:
                result_id = existing_space_id
                print(f"\n✅ Updated space: {result_id}")
            else:
                raise Exception(f"Update failed: {resp.status_code} - {resp.text}")
        else:
            # Create new space
            print(f"  Creating new space...")
            result = w.genie.create_space(
                warehouse_id=warehouse_id,
                title=config["title"],
                description=config.get("description", ""),
                parent_path=parent_path,
                serialized_space=serialized_space
            )
            
            # Extract the space_id from the result object
            if hasattr(result, 'space_id'):
                result_id = result.space_id
            elif hasattr(result, 'id'):
                result_id = result.id
            else:
                # Parse from string representation
                import re
                match = re.search(r"space_id='([^']+)'", str(result))
                result_id = match.group(1) if match else str(result)
            
            print(f"\n✅ Created space: {result_id}")
        
        print(f"   URL: {host}/genie/spaces/{result_id}")
        
        # Save ID
        output_file = Path(__file__).parent.parent / ".genie_space_id"
        with open(output_file, "w") as f:
            f.write(result_id)
        
        print(f"\n   Add to .env: GENIE_SPACE_ID={result_id}")
        return 0
        
    except Exception as e:
        print(f"\nError: {e}")
        print("\n" + "="*60)
        print("MANUAL CREATION")
        print("="*60)
        print(f"Go to: {host}/genie")
        print(f"Create space with tables from welch.demand_planning_demo")
        return 1


if __name__ == "__main__":
    sys.exit(main())
