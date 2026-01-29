## Genie Space — Tables Configuration (Demand Planning Control Tower)

Add these tables/views to your Genie Space for demand planning, service, freight, sustainability, and late-risk analytics.

## Catalog & Schema
- **Catalog**: `welch`
- **Schema**: `demand_planning_demo`

---

## Required Tables (Add These First)

### Metric Views (preferred for KPI questions)

| Metric View | What it standardizes | Key dimensions / measures |
|---|---|---|
| `mv_control_tower_kpis` | Service + cost + sustainability KPIs | dims: week, region, plant_id, dc_id, sku_family; measures: OTIF, Freight $/ton, Premium freight %, CO2 kg/ton, Energy kWh/unit |
| `mv_forecast_accuracy` | Forecast accuracy KPI | dims: week, sku_family, region; measure: MAPE |
| `mv_late_delivery_risk` | Late-delivery risk KPIs | dims: order_date, order_week, customer_region, channel, plant_id, dc_id, sku_family; measures: Avg late-risk prob, Observed late rate, Orders scored |

**Note:** Metric views require `MEASURE(...)` in queries and do not support `SELECT *`.

### Gold / Pre-Aggregated (Fast + preferred when you need raw columns)

| Table / View | Description | Key Columns |
|---|---|---|
| `control_tower_weekly` | Executive weekly rollup across service/cost/sustainability/risk | week, region, otif_rate, freight_cost_usd, premium_freight_pct, co2_kg, energy_kwh_per_unit, late_risk_prob |
| `weekly_demand_actual` | Weekly demand + shipped units by family/region | week, sku_family, region, actual_units, shipped_units |
| `kpi_otif_weekly` | OTIF by week/region/DC | week, region, dc_id, otif_rate, orders |
| `kpi_freight_weekly` | Freight + CO₂ by week/lane/mode | week, plant_id, dc_id, mode, freight_cost_usd, total_weight_tons, co2_kg |
| `kpi_premium_freight_weekly` | Premium freight % by week/lane | week, plant_id, dc_id, premium_freight_pct |
| `kpi_energy_intensity_weekly` | Energy intensity by week/plant/sku_family | week, plant_id, sku_family, energy_kwh_per_unit |
| `order_late_risk_scored_ml` | Scored late risk for hotspot prioritization (Notebook 5 output) | order_id, order_date, customer_region, dc_id, plant_id, sku_family, late_risk_prob, late_risk_flag |

### Forecast Outputs (Created by Notebook 3 + 4)

| Table / View | Description | Key Columns |
|---|---|---|
| `demand_forecast_all` | Backtest + future forecasts for multiple models | week, sku_family, region, forecast_units, model_name, is_backtest |
| `demand_forecast` | Selected backtest forecast (used for MAPE joins) | week, sku_family, region, forecast_units |
| `demand_forecast_future` | Future horizon forecast | week, sku_family, region, forecast_units, model_name |
| `kpi_mape_weekly` | Forecast accuracy by family/region/week (post-forecast KPI refresh) | week, sku_family, region, mape |
| `demand_vs_forecast_weekly` | Convenience join: actual vs forecast vs future | week, sku_family, region, actual_units, forecast_units, forecast_units_future |

---

## Drill-Down Tables (Silver)

Only add these if you want deep root-cause and “why” exploration.

| Table | Use | Key Columns |
|---|---|---|
| `silver_erp_orders` | Order-level drill-down for service/backorder drivers | order_id, order_date, requested_delivery_date, actual_delivery_date, customer_region, sku_family, units_ordered, units_shipped, order_status |
| `silver_tms_shipments` | Shipment-level drill-down for lane/mode cost and emissions | shipment_id, ship_date, delivery_date, plant_id, dc_id, mode, route_distance_km, total_weight_tons, freight_cost_usd, co2_kg |
| `silver_inventory_positions` | Inventory snapshot drill-down | snapshot_date, plant_id, dc_id, sku_id, on_hand_units, on_order_units, days_of_supply |
| `silver_production_output` | Production + energy drill-down | date, plant_id, sku_family, units_produced, energy_kwh |
| `silver_external_signals` | Weekly external drivers (construction/weather proxies) | week, region, construction_index, precipitation_mm, avg_temp_c |

---

## SQL Formatting (Databricks SQL)

### Currency
```sql
FORMAT_NUMBER(value_usd, '$#,##0.00') AS `USD`
```

### Percentages
```sql
CONCAT(ROUND(rate * 100, 1), '%') AS `Rate`
```

### Units / integers
```sql
FORMAT_NUMBER(units, '#,##0') AS `Units`
```

**Avoid double-aggregation**: if a table is already weekly/rolled up, don’t re-sum unless you’re rolling up across dimensions/time.

---

## Sample Queries for Genie

### 1) Executive control tower — last 13 weeks (all regions)
```sql
SELECT
  week,
  CONCAT(ROUND(MEASURE(`OTIF`) * 100, 1), '%') AS `OTIF`,
  FORMAT_NUMBER(ROUND(MEASURE(`Freight $/ton`), 2), '$#,##0.00') AS `Freight $/ton`,
  CONCAT(ROUND(MEASURE(`Premium freight %`) * 100, 1), '%') AS `Premium Freight`,
  ROUND(MEASURE(`CO2 kg/ton`), 3) AS `CO2 kg/ton`,
  ROUND(MEASURE(`Energy kWh/unit`), 3) AS `Energy kWh/unit`
FROM welch.demand_planning_demo.mv_control_tower_kpis
WHERE week >= date_sub(current_date(), 7 * 13)
GROUP BY week
ORDER BY week;
```

### 2) OTIF trend by region (last 13 weeks)
```sql
SELECT
  week,
  region,
  CONCAT(ROUND(MEASURE(`OTIF`) * 100, 1), '%') AS `OTIF`
FROM welch.demand_planning_demo.mv_control_tower_kpis
WHERE week >= date_sub(current_date(), 7 * 13)
GROUP BY week, region
ORDER BY week, region;
```

### 3) Worst DCs by OTIF (last 13 weeks)
```sql
SELECT
  dc_id AS `DC`,
  CONCAT(ROUND(MEASURE(`OTIF`) * 100, 1), '%') AS `OTIF (13w)`
FROM welch.demand_planning_demo.mv_control_tower_kpis
WHERE week >= date_sub(current_date(), 7 * 13)
GROUP BY dc_id
ORDER BY MEASURE(`OTIF`) ASC
LIMIT 10;
```

### 4) Highest premium freight lanes (last 13 weeks)
```sql
SELECT
  plant_id AS `Plant`,
  dc_id AS `DC`,
  CONCAT(ROUND(MEASURE(`Premium freight %`) * 100, 1), '%') AS `Premium Freight (13w)`
FROM welch.demand_planning_demo.mv_control_tower_kpis
WHERE week >= date_sub(current_date(), 7 * 13)
GROUP BY plant_id, dc_id
ORDER BY MEASURE(`Premium freight %`) DESC
LIMIT 10;
```

### 5) Freight cost per ton by lane (last 13 weeks)
```sql
SELECT
  plant_id AS `Plant`,
  dc_id AS `DC`,
  FORMAT_NUMBER(ROUND(MEASURE(`Freight $/ton`), 2), '$#,##0.00') AS `Freight $/ton`,
  CONCAT(ROUND(MEASURE(`Premium freight %`) * 100, 1), '%') AS `Premium Freight %`,
  ROUND(MEASURE(`CO2 kg/ton`), 3) AS `CO2 kg/ton`
FROM welch.demand_planning_demo.mv_control_tower_kpis
WHERE week >= date_sub(current_date(), 7 * 13)
GROUP BY plant_id, dc_id
ORDER BY MEASURE(`Freight $/ton`) DESC
LIMIT 10;
```

### 6) CO₂ per ton by mode (last 13 weeks)
```sql
-- Metric views don’t currently support ad-hoc joins at query time.
-- Use the underlying table for mode-level breakdowns.
SELECT
  mode AS `Mode`,
  ROUND(SUM(co2_kg) / NULLIF(SUM(total_weight_tons), 0), 3) AS `CO2 kg/ton`,
  FORMAT_NUMBER(ROUND(SUM(total_weight_tons), 1), '#,##0.0') AS `Tons (13w)`
FROM welch.demand_planning_demo.kpi_freight_weekly
WHERE week >= date_sub(current_date(), 7 * 13)
GROUP BY mode
ORDER BY `CO2 kg/ton` DESC;
```

### 7) Forecast accuracy (MAPE) — worst family/region (last 13 weeks)
```sql
SELECT
  sku_family AS `Family`,
  region AS `Region`,
  CONCAT(ROUND(MEASURE(`MAPE`) * 100, 1), '%') AS `MAPE (13w)`
FROM welch.demand_planning_demo.mv_forecast_accuracy
WHERE week >= date_sub(current_date(), 7 * 13)
GROUP BY sku_family, region
ORDER BY MEASURE(`MAPE`) DESC
LIMIT 10;
```

### 8) Demand vs forecast (one family/region, last 26 weeks + future)
```sql
SELECT
  week,
  sku_family,
  region,
  FORMAT_NUMBER(actual_units, '#,##0') AS `Actual Units`,
  FORMAT_NUMBER(forecast_units, '#,##0') AS `Forecast Units`,
  FORMAT_NUMBER(forecast_units_future, '#,##0') AS `Future Forecast Units`
FROM welch.demand_planning_demo.demand_vs_forecast_weekly
WHERE sku_family = 'pipe'
  AND region = 'Midwest'
ORDER BY week DESC
LIMIT 52;
```

### 9) Late-risk hotspots (next actions list)
```sql
SELECT
  order_id,
  order_date,
  customer_region AS region,
  plant_id,
  dc_id,
  sku_family,
  ROUND(late_risk_prob, 3) AS late_risk_prob
FROM welch.demand_planning_demo.order_late_risk_scored_ml
ORDER BY late_risk_prob DESC
LIMIT 25;
```

---

## Entity Relationships (high-level)

```
bronze_*_raw ──► (DLT) ──► silver_* ──► (DLT) ──► Gold KPIs / rollups
                         │
                         ├──► weekly_demand_actual ──► (Notebook 3) demand_forecast* ──► (Notebook 4) kpi_mape_weekly
                         │
                         └──► silver_erp_orders ──► (Notebook 5) order_late_risk_scored_ml
```

