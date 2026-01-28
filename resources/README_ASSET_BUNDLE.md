## Databricks Asset Bundle (DAB) contents

This repo is bundled by default using **Databricks Asset Bundles**.

### What the bundle deploys

- **Files**: the bundle syncs repo files into the bundle `workspace.root_path` (as supported by your Databricks CLI/bundle schema).
- **DLT / Lakeflow SDP pipeline**: `resources/pipelines/medallion.yml`
  - Bronze → Silver → Gold tables in `${var.catalog}.${var.schema}`
- **Job**: `resources/jobs/train_and_register_late_risk.yml`
  - Trains + registers the late-delivery risk model (MLflow) and writes `order_late_risk_scored`
- **SQL assets**: `sql/kpi_starter_queries.sql`
  - Starter queries for DBSQL and Genie instructions

### Variables

Key bundle variables (see `databricks.yml`):
- `catalog`
- `schema`
- `demo_name`
- `late_risk_model_name`

### Typical commands

```bash
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run -t dev train_and_register_late_risk
```

