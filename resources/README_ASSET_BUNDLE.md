## Databricks Asset Bundle (DAB) contents

This repo is bundled by default using **Databricks Asset Bundles**.

### What the bundle deploys

- **Files**: the bundle syncs repo files into the bundle `workspace.root_path` (as supported by your Databricks CLI/bundle schema).
- **DLT / Lakeflow SDP pipeline**: `resources/pipelines/medallion.yml`
  - Bronze → Silver → Gold tables in `${var.catalog}.${var.schema}`
- **Job**: `resources/jobs/demand_planning_demo_job.yml`
  - End-to-end workflow (UC → Bronze → DLT → Forecast → ML → KPI+Metric refresh → Dashboards)
- **SQL assets**: `sql/kpi_starter_queries.sql`
  - Starter queries for DBSQL and Genie instructions

### Variables

Key bundle variables (see `databricks.yml`):
- `catalog`
- `schema`
- `demo_name`
- `late_risk_model_name`
- `warehouse_id` (for dashboard refresh)
- `dashboard_id` (for dashboard refresh)

### Typical commands

```bash
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run -t dev Demand_Planning_Demo_Job
```

