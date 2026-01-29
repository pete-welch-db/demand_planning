# Demand Planning + Supply Chain Visibility Demo (Azure Databricks)

End-to-end **Databricks Lakehouse** demo for a pipe / stormwater manufacturer (ADS-like) focused on **supply chain visibility and demand planning**.

- **What you get**: synthetic ERP/WMS/TMS/production/external data in **Delta**, KPI **gold views**, and a scalable **MLflow forecasting** workflow + notebook dashboards.
- **Audience**: demand planners, logistics managers, finance & sustainability stakeholders.
- **Scale story**: the notebooks are written to demonstrate how a **small team** can standardize pipelines and scale forecasting patterns toward **25,000 SKUs** (the synthetic generator defaults to a smaller number so it runs quickly).

## Notebooks (import into Databricks)

This repo contains Databricks “source format” notebooks under `notebooks/`.

Run order:

0. `notebooks/00_common_setup` – common config/helpers (imported by the other notebooks via `%run`)
1. `notebooks/01_uc_setup` – best-effort UC catalog/schema creation (jobs/workflows)
2. `notebooks/02_generate_bronze` – generates **Bronze/raw** Delta tables:
   - `bronze_erp_orders_raw`
   - `bronze_inventory_positions_raw`
   - `bronze_tms_shipments_raw`
   - `bronze_production_output_raw`
   - `bronze_external_signals_raw`
3. Run the **DLT/SDP pipeline** `pipelines/dlt_supply_chain_medallion.py` – produces **Silver/Gold**
4. `notebooks/03_forecast_weekly_mlflow` – weekly forecasting by `(sku_family, region)` (writes `demand_forecast*`)
5. `notebooks/04_post_forecast_kpis.py` – refreshes post-forecast views (e.g., `kpi_mape_weekly`)
6. `notebooks/05_ml_late_risk` – MLflow training + scoring into Gold (`order_late_risk_scored_ml`)
6.5. (Optional) `notebooks/06_uc_metric_views.py` – creates Unity Catalog **metric views** over the Gold tables for consistent KPI definitions in Dashboards/Genie
7. Optional notebook “dashboard” views:
   - `notebooks/90_dashboard_demand_planner`
   - `notebooks/91_dashboard_logistics_service`
   - `notebooks/92_dashboard_sustainability`

## Streamlit Control Tower App (mock-first, Databricks SQL optional)

There’s also a modular Streamlit app under `app/` following a “routing-only `app.py` + views/components/data” layout.

- **Graceful degradation**: any Databricks SQL failure automatically falls back to Faker-based mock data.
- **Settings toggle**: sidebar switch between mock and real data.
- **Centralized config**: `app/config.py` is the only place env vars are read.

Run locally:

```bash
cd /Users/pete.welch/Documents/GitHub/demand_planning
source .venv/bin/activate
streamlit run app/app.py
```

Configure real data mode by copying `.env.example` → `.env` and setting:
- `DATABRICKS_HOST`
- `DATABRICKS_HTTP_PATH`
- `DATABRICKS_CATALOG`
- `DATABRICKS_SCHEMA`
- `DATABRICKS_TOKEN` (PAT for local dev)

## Lakeflow SDP / DLT 7.1 (Bronze → Silver → Gold) — preferred pipeline path

This demo includes a Lakeflow Spark Declarative Pipelines (DLT/SDP) medallion pipeline:
- **Pipeline source**: `pipelines/dlt_supply_chain_medallion.py`
- **Bronze**: `bronze_*` raw synthetic tables (ERP/inventory/TMS/production/external)
- **Silver**: `silver_*` cleaned operational tables (standardized types + derived flags)
- **Gold** (powers the Streamlit app):
  - `weekly_demand_actual`
  - `kpi_otif_weekly`
  - `kpi_freight_weekly`
  - `kpi_premium_freight_weekly`
  - `kpi_energy_intensity_weekly`
  - `control_tower_weekly`

### Create the SDP/DLT pipeline (UI steps)

In Databricks:
- Go to **Workflows → Delta Live Tables / Lakeflow Declarative Pipelines** (name varies by workspace).
- **Create pipeline**
  - **Source**: add the notebook at `pipelines/dlt_supply_chain_medallion`
  - **Target**: set to your Unity Catalog schema (e.g., `welch.demand_planning_demo`)
  - **Runtime**: use a **DLT / Lakeflow SDP** runtime (your “7.1” standard)
  - **Configuration** (optional): set knobs like `demo.num_skus`, `demo.orders_per_day`, etc.
- Click **Start** to materialize Bronze/Silver/Gold.

### Unity Catalog note: catalog creation

If the `demand_planning_demo` catalog does not exist yet, you may need an admin (or a user with UC privileges) to create it:

```sql
CREATE CATALOG IF NOT EXISTS welch;
```

After the pipeline finishes:
- Set the Streamlit env vars (`DATABRICKS_CATALOG`, `DATABRICKS_SCHEMA`) to the same target.
- Turn off “Mock data” in the sidebar to query the **Gold** tables via Databricks SQL.

## Real ML in the loop (late-delivery risk)

This demo includes a minimal “ML that changes a decision” workflow:

- **Train + register model (MLflow)**: run `notebooks/05_ml_late_risk`
  - Trains a logistic regression model to predict late delivery at order time
  - Logs metrics and registers a UC model (default name: `<catalog>.<schema>.order_late_risk_model`)
  - Writes a **Gold** table: `order_late_risk_scored_ml` (to avoid colliding with the DLT-owned `order_late_risk_scored`)

### Optional: score inside the DLT pipeline

The DLT pipeline includes a Gold table `order_late_risk_scored` that:
- uses the MLflow registered model if configured, otherwise
- **falls back to a heuristic** risk score (so the pipeline never breaks)

To enable true ML scoring in DLT, set pipeline configuration:
- `demo.late_risk_model_name` = `<catalog>.<schema>.order_late_risk_model`

The Streamlit app uses `order_late_risk_scored_ml` by default (created by Notebook 5) to show **late-risk hotspots** and drive scenarios.

## Genie on the same Gold tables (AI/BI Genie)

Create a **Genie space** scoped to the Gold tables, then set `GENIE_SPACE_ID` for the Streamlit app.

### Create a Genie space (UI)

High-level steps (see the official “Set up and manage an AI/BI Genie space” documentation for the latest UI flow):
- Open **Genie** in the Databricks UI
- Create a **new space**
- Add data sources (Unity Catalog tables/views). Recommended Gold assets:
  - `control_tower_weekly`
  - `weekly_demand_actual`
  - `kpi_otif_weekly`
  - `kpi_freight_weekly`
  - `kpi_premium_freight_weekly`
  - `kpi_energy_intensity_weekly`
  - `order_late_risk_scored_ml` (Notebook 5 output) and/or `order_late_risk_scored` (DLT output)
- Copy the **Space ID** and set `GENIE_SPACE_ID`

### Wire Genie into the app

- Set env vars (Databricks Apps or local `.env`):
  - `GENIE_SPACE_ID`
  - `DATABRICKS_HOST`
  - `DATABRICKS_TOKEN` (for local dev; Apps auth varies by workspace configuration)

The Assistant tab includes:
- KPI-driven starter questions (OTIF, freight, MAPE, late-risk)
- Query phrasing tips (time window, grouping, output shape)

## Databricks Asset Bundles (default packaging)

This repo is structured to be deployed as a **Databricks Asset Bundle**:

- **Databricks App**: `app/` (Streamlit) + `app/app.yaml`
- **DLT/SDP pipeline**: `pipelines/dlt_supply_chain_medallion.py` (declared in `resources/pipelines/medallion.yml`)
- **ML training job**: `notebooks/05_ml_late_risk.py` (declared in `resources/jobs/train_and_register_late_risk.yml`)
- **SQL objects/assets**: `sql/kpi_starter_queries.sql`

Bundle entrypoint: `databricks.yml`

Typical workflow:

```bash
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run -t dev train_and_register_late_risk
```

### One-command demo deploy (recommended)

Use `deploy.sh` to run a two-stage rollout (setup → pipeline+ML).

If your Databricks CLI has multiple profiles for the same host, pass one explicitly:

```bash
./deploy.sh azure azure
```

Or set it once:

```bash
export DATABRICKS_CONFIG_PROFILE=azure
./deploy.sh azure
```

### Recommended run order (when using DLT + ML notebooks)

- `notebooks/01_uc_setup`
- `notebooks/02_generate_bronze`
- Run the DLT/SDP pipeline (`pipelines/dlt_supply_chain_medallion.py`)
- `notebooks/03_forecast_weekly_mlflow`
- `notebooks/04_post_forecast_kpis.py`
- `notebooks/05_ml_late_risk`

This is automated in the `demand_planning_end_to_end` workflow job.

### If you already created the DLT pipeline in the UI

If you already created a DLT pipeline in the workspace (and just want the bundle to *run it*), set:
- `existing_dlt_pipeline_id` in `databricks.yml` (under your target), or override at runtime, then run:
  - `demand_planning_end_to_end_existing_dlt`


## Requirements / assumptions

- Designed for **Azure Databricks** using **PySpark + Delta + MLflow**.
- Runs best on a **Databricks Runtime ML** (for `pandas`/`scikit-learn`), but avoids nonstandard time-series libraries (e.g., Prophet).

## How to demo (20–30 mins)

- Start from the problem (volatile project demand, freight sensitivity, owned fleet, sustainability).
- Show unified Delta tables, then KPI views, then forecasting + MAPE, then dashboard drill-downs.
- Close with: “same pattern scales to real ERP/WMS/TMS/telematics and to part-level forecasting at 25k SKUs via Spark parallelism + standardized feature pipelines + MLflow governance.”

## “Business app + real backend” positioning

- **Business-facing**: use the Streamlit control tower (`app/`) for a clean story and consistent UX.
- **Technical credibility**: show the real backend assets:
  - Lakeflow SDP/DLT Bronze→Silver→Gold (`pipelines/dlt_supply_chain_medallion.py`)
  - MLflow training + UC model registry + scoring into Gold (`notebooks/05_ml_late_risk.py`)
  - Genie over the same Gold tables (`GENIE_SPACE_ID`)
  - Deployable packaging via Databricks Asset Bundles (`databricks.yml`)

See `docs/DEMO_RUNBOOK.md` (two-track demo) and `docs/TECH_TOUR.md`.
