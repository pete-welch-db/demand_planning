from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import pandas as pd
from databricks import sql

from config import AppConfig


class DatabricksAuthError(RuntimeError):
    pass


@dataclass(frozen=True)
class SqlClient:
    cfg: AppConfig

    def query(self, query: str, params: Optional[dict[str, Any]] = None):
        """
        Returns a pandas.DataFrame from Databricks SQL.
        Any failure is expected to be caught upstream for graceful fallback.
        """
        if not self.cfg.databricks_token:
            raise DatabricksAuthError(
                "Missing DATABRICKS_TOKEN for Databricks SQL authentication. "
                "Set DATABRICKS_TOKEN (PAT) for local dev, or rely on Databricks Apps auth if configured."
            )

        with sql.connect(
            server_hostname=self.cfg.databricks_host.replace("https://", "").replace("http://", ""),
            http_path=self.cfg.databricks_http_path,
            access_token=self.cfg.databricks_token,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(query, params or {})
                rows = cur.fetchall()
                cols = [d[0] for d in (cur.description or [])]
                return pd.DataFrame(rows, columns=cols)


def get_sql_client(cfg: AppConfig) -> SqlClient:
    return SqlClient(cfg=cfg)

