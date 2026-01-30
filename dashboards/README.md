# Databricks AI/BI Dashboard

## Overview

The `demand_planning_control_tower.lvdash.json` file is a comprehensive Databricks AI/BI dashboard that provides unified visibility across service, cost, forecasting, and sustainability metrics.

## Pages

| Page | Focus | Key Visualizations |
|------|-------|-------------------|
| 🎯 **Executive Overview** | High-level KPIs and trends | 8 counter KPIs, OTIF trend by region, demand area chart, risk heatmap, cost lanes |
| 🌿 **Sustainability** | Environmental impact | CO₂ trends, energy by plant, premium freight hotspots |
| 📊 **Forecasting** | Demand planning accuracy | MAPE trends, accuracy heatmap by family×region, demand vs forecast |
| 🚚 **Operations & Risk** | ML-powered risk | Late-risk trends, risk by region, detailed order table |

## Data Sources

The dashboard leverages **Unity Catalog Metric Views** for semantic layer consistency:

| Metric View | Purpose |
|-------------|---------|
| `mv_control_tower_kpis` | Service (OTIF), cost (freight $/ton), sustainability (CO₂, energy) |
| `mv_forecast_accuracy` | Forecast MAPE by week, family, region |
| `mv_late_delivery_risk` | ML-predicted late-delivery probabilities |

Direct table references:
- `weekly_demand_actual` - Actual demand aggregated weekly
- `demand_forecast` - Forecast outputs from MLflow model
- `order_late_risk_scored_ml` - Scored orders with late-risk predictions

## Import Instructions

1. Open your Databricks workspace
2. Navigate to **Dashboards** in the left sidebar
3. Click **Create dashboard** → **Import from file**
4. Select `demand_planning_control_tower.lvdash.json`
5. Update the catalog/schema if different from `welch.demand_planning_demo`

## Features

- **Cross-filtering**: Click on any chart element to filter related visualizations
- **Semantic layer**: Uses `MEASURE()` function for consistent aggregations
- **Responsive layout**: Optimized grid layout for various screen sizes
- **Multi-page navigation**: Tab-based navigation between focus areas

## Customization

To modify the dashboard:
1. Import and edit in the Databricks UI
2. Export changes back to `.lvdash.json`
3. Commit to version control

Key configuration points:
- Dataset `queryLines`: Update catalog/schema references
- Widget `position`: Adjust grid layout (8-column grid)
- Widget `spec.frame.title`: Update titles and descriptions

## Reference

- [Databricks AI/BI Documentation](https://learn.microsoft.com/en-us/azure/databricks/ai-bi/)
- [Dashboard Visualizations](https://docs.databricks.com/en/dashboards/visualizations/)
- [Map Visualizations](https://docs.databricks.com/en/dashboards/visualizations/maps)
