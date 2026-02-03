from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Optional

import pandas as pd
from databricks import sql

from config import AppConfig


class DatabricksAuthError(RuntimeError):
    pass


def _is_databricks_apps() -> bool:
    """Check if running in Databricks Apps environment."""
    return bool(os.getenv("DATABRICKS_APP_NAME"))


@dataclass(frozen=True)
class SqlClient:
    cfg: AppConfig

    def query(self, query: str, params: Optional[dict[str, Any]] = None):
        """
        Returns a pandas.DataFrame from Databricks SQL.
        Supports both PAT auth (local dev) and Databricks Apps OAuth.
        """
        server_hostname = self.cfg.databricks_host.replace("https://", "").replace("http://", "")
        http_path = self.cfg.databricks_http_path
        
        # Try Databricks Apps OAuth first
        if _is_databricks_apps():
            try:
                from databricks.sdk import WorkspaceClient
                from databricks.sdk.service.sql import StatementState
                
                # Use SDK client which automatically picks up Apps OAuth
                w = WorkspaceClient()
                
                # Extract warehouse ID from http_path
                warehouse_id = http_path.split("/")[-1] if "/" in http_path else http_path
                
                # Execute statement
                response = w.statement_execution.execute_statement(
                    warehouse_id=warehouse_id,
                    statement=query,
                    wait_timeout="30s",
                )
                
                if response.status and response.status.state == StatementState.SUCCEEDED:
                    if response.result and response.result.data_array:
                        cols = [c.name for c in response.manifest.schema.columns] if response.manifest else []
                        return pd.DataFrame(response.result.data_array, columns=cols)
                    return pd.DataFrame()
                else:
                    error_msg = response.status.error.message if response.status and response.status.error else "Unknown error"
                    raise RuntimeError(f"SQL execution failed: {error_msg}")
                    
            except ImportError:
                pass  # Fall through to PAT auth
            except Exception as e:
                # If SDK auth fails, fall through to PAT auth
                print(f"Databricks Apps OAuth failed, trying PAT: {e}")
        
        # Fall back to PAT authentication (local dev)
        if not self.cfg.databricks_token:
            raise DatabricksAuthError(
                "Missing DATABRICKS_TOKEN for Databricks SQL authentication. "
                "Set DATABRICKS_TOKEN (PAT) for local dev, or run in Databricks Apps for automatic auth."
            )

        with sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=self.cfg.databricks_token,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(query, params or {})
                rows = cur.fetchall()
                cols = [d[0] for d in (cur.description or [])]
                return pd.DataFrame(rows, columns=cols)


def get_sql_client(cfg: AppConfig) -> SqlClient:
    return SqlClient(cfg=cfg)
