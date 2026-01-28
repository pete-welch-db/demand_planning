## Tech tour: “real working backend” behind the Streamlit demo

### What’s real (not smoke-and-mirrors)

- **Lakehouse tables**: Bronze/Silver/Gold are materialized via **Lakeflow SDP/DLT**.
- **Governance**: Unity Catalog schema targets, Gold tables are the contract for apps/BI/Genie.
- **ML lifecycle**: model training + metrics tracked in **MLflow**, model registered in UC Model Registry.
- **Production pattern**: scoring writes back to **Gold** and directly drives the UI’s decisions.
- **GenAI/BI**: Genie space is defined over the same Gold assets.
- **Repeatable deploy**: packaged as a **Databricks Asset Bundle**.

---

## Key assets (where to look)

### 1) Lakeflow SDP/DLT pipeline
- `pipelines/dlt_supply_chain_medallion.py`
- Declared in bundle: `resources/pipelines/medallion.yml`
- Gold tables the app expects:
  - `control_tower_weekly`
  - `weekly_demand_actual`
  - `kpi_otif_weekly`
  - `kpi_freight_weekly`
  - `kpi_premium_freight_weekly`
  - `kpi_energy_intensity_weekly`
  - `order_late_risk_scored`

### 2) ML in the loop (late-risk)
- Training notebook: `notebooks/07_ml_in_loop_late_risk.py`
- Bundle job: `resources/jobs/train_and_register_late_risk.yml`
- Model (UC): `<catalog>.<schema>.order_late_risk_model`
- DLT scoring config: `demo.late_risk_model_name`

### 3) Streamlit app contract
- App entrypoint: `app/app.py`
- Config-only env reads: `app/config.py`
- SQL queries: `app/data/queries.py` (all point at Gold tables)
- Fallback behavior: `app/data/service.py` (graceful degradation to Faker mock data)

### 4) Genie (optional)
- `GENIE_SPACE_ID` wired in `app/config.py`
- Assistant UX: `app/views/assistant.py` (starter questions + phrasing tips)

### 5) Asset Bundle (DAB)
- Root: `databricks.yml`
- Bundled resources: `resources/**`

