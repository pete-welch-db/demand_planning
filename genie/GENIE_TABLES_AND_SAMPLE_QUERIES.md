## Genie Space — Tables Configuration (Demand Planning Control Tower)

Add these tables/views to your Genie Space for demand planning, service, freight, sustainability, and late-risk analytics.

## Catalog & Schema
- **Catalog**: `welch`
- **Schema**: `demand_planning_demo`

---

## Required Tables (Add These First)

### Gold / Pre-Aggregated (Fast + preferred for summaries)

| Table / View | Description | Key Columns |
|---|---|---|
| `control_tower_weekly` | Executive weekly rollup across service/cost/sustainability/risk | week, region, otif_rate, freight_cost_usd, premium_freight_pct, co2_kg, energy_kwh_per_unit, late_risk_prob |
| `weekly_demand_actual` | Weekly demand + shipped units by family/region | week, sku_family, region, actual_units, shipped_units |
| `kpi_otif_weekly` | OTIF by week/region/DC | week, region, dc_id, otif_rate, orders |
| `kpi_freight_weekly` | Freight + CO₂ by week/lane/mode | week, plant_id, dc_id, mode, freight_cost_usd, total_weight_tons, co2_kg |
| `kpi_premium_freight_weekly` | Premium freight % by week/lane | week, plant_id, dc_id, premium_freight_pct |
| `kpi_energy_intensity_weekly` | Energy intensity by week/plant/sku_family | week, plant_id, sku_family, energy_kwh_per_unit |
| `order_late_risk_scored` | Scored late risk for hotspot prioritization | order_id, order_date, customer_region, dc_id, plant_id, sku_family, late_risk_prob, late_risk_flag |

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
  CONCAT(ROUND(AVG(otif_rate) * 100, 1), '%') AS `OTIF`,
  FORMAT_NUMBER(ROUND(SUM(freight_cost_usd), 2), '$#,##0.00') AS `Freight`,
  CONCAT(ROUND(AVG(premium_freight_pct) * 100, 1), '%') AS `Premium Freight`,
  FORMAT_NUMBER(ROUND(SUM(co2_kg) / 1000.0, 1), '#,##0.0') AS `CO2 (metric tons)`,
  ROUND(AVG(energy_kwh_per_unit), 3) AS `Energy kWh/unit`,
  ROUND(AVG(late_risk_prob), 3) AS `Avg Late Risk`
FROM welch.demand_planning_demo.control_tower_weekly
WHERE week >= date_sub(current_date(), 7 * 13)
GROUP BY week
ORDER BY week;
```

### 2) OTIF trend by region (last 13 weeks)
```sql
SELECT
  week,
  region,
  CONCAT(ROUND(AVG(otif_rate) * 100, 1), '%') AS `OTIF`,
  FORMAT_NUMBER(SUM(orders), '#,##0') AS `Orders`
FROM welch.demand_planning_demo.kpi_otif_weekly
WHERE week >= date_sub(current_date(), 7 * 13)
GROUP BY week, region
ORDER BY week, region;
```

### 3) Worst DCs by OTIF (last 13 weeks)
```sql
WITH recent AS (
  SELECT *
  FROM welch.demand_planning_demo.kpi_otif_weekly
  WHERE week >= date_sub(current_date(), 7 * 13)
)
SELECT
  dc_id AS `DC`,
  CONCAT(ROUND(AVG(otif_rate) * 100, 1), '%') AS `OTIF (13w)`,
  FORMAT_NUMBER(SUM(orders), '#,##0') AS `Orders (13w)`
FROM recent
GROUP BY dc_id
ORDER BY AVG(otif_rate) ASC
LIMIT 10;
```

### 4) Highest premium freight lanes (last 13 weeks)
```sql
WITH recent AS (
  SELECT *
  FROM welch.demand_planning_demo.kpi_premium_freight_weekly
  WHERE week >= date_sub(current_date(), 7 * 13)
)
SELECT
  plant_id AS `Plant`,
  dc_id AS `DC`,
  CONCAT(ROUND(AVG(premium_freight_pct) * 100, 1), '%') AS `Premium Freight (13w)`
FROM recent
GROUP BY plant_id, dc_id
ORDER BY AVG(premium_freight_pct) DESC
LIMIT 10;
```

### 5) Freight cost per ton by lane (last 13 weeks)
```sql
WITH recent AS (
  SELECT *
  FROM welch.demand_planning_demo.kpi_freight_weekly
  WHERE week >= date_sub(current_date(), 7 * 13)
)
SELECT
  plant_id AS `Plant`,
  dc_id AS `DC`,
  FORMAT_NUMBER(ROUND(SUM(freight_cost_usd) / NULLIF(SUM(total_weight_tons), 0), 2), '$#,##0.00') AS `Freight $/ton`,
  FORMAT_NUMBER(ROUND(SUM(freight_cost_usd), 2), '$#,##0.00') AS `Freight (13w)`
FROM recent
GROUP BY plant_id, dc_id
ORDER BY (SUM(freight_cost_usd) / NULLIF(SUM(total_weight_tons), 0)) DESC
LIMIT 10;
```

### 6) CO₂ per ton by mode (last 13 weeks)
```sql
WITH recent AS (
  SELECT *
  FROM welch.demand_planning_demo.kpi_freight_weekly
  WHERE week >= date_sub(current_date(), 7 * 13)
)
SELECT
  mode AS `Mode`,
  ROUND(SUM(co2_kg) / NULLIF(SUM(total_weight_tons), 0), 3) AS `CO2 kg/ton`,
  FORMAT_NUMBER(ROUND(SUM(total_weight_tons), 1), '#,##0.0') AS `Tons (13w)`
FROM recent
GROUP BY mode
ORDER BY `CO2 kg/ton` DESC;
```

### 7) Forecast accuracy (MAPE) — worst family/region (last 13 weeks)
```sql
WITH recent AS (
  SELECT *
  FROM welch.demand_planning_demo.kpi_mape_weekly
  WHERE week >= date_sub(current_date(), 7 * 13)
)
SELECT
  sku_family AS `Family`,
  region AS `Region`,
  CONCAT(ROUND(AVG(mape) * 100, 1), '%') AS `MAPE (13w)`
FROM recent
GROUP BY sku_family, region
ORDER BY AVG(mape) DESC
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
FROM welch.demand_planning_demo.order_late_risk_scored
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
                         └──► silver_erp_orders ──► (Notebook 5) order_late_risk_scored
```

