-- SQL assets (bundle-friendly)
-- These are intended as starter queries to paste into Databricks SQL / Genie “Instructions”.

-- Control tower snapshot (latest 13 weeks)
SELECT *
FROM control_tower_weekly
WHERE week >= date_sub(current_date(), 91)
ORDER BY week DESC;

-- OTIF by region (trend)
SELECT week, region, avg(otif_rate) AS otif
FROM control_tower_weekly
GROUP BY 1,2
ORDER BY week, region;

-- Freight cost per ton + premium freight %
SELECT week, dc_id, plant_id, freight_cost_per_ton, premium_freight_pct
FROM control_tower_weekly
ORDER BY week DESC;

-- Late-risk hotspots (ML-in-loop)
SELECT
  date_trunc('week', order_date) AS week,
  customer_region,
  sku_family,
  avg(late_risk_prob) AS avg_late_risk
FROM order_late_risk_scored
GROUP BY 1,2,3
ORDER BY avg_late_risk DESC
LIMIT 50;

