#!/usr/bin/env python3
"""
Generate an importable Databricks dashboard JSON from a workspace-exported template.

Why this exists:
- Databricks dashboard export formats can differ by product/workspace release.
- We keep a *real exported* template in-repo and patch in our queries/labels.

Usage:
  python scripts/generate_vdash.py \
    --template dashboards/template.vdash.json \
    --out dashboards/demand_planning_control_tower.vdash.json \
    --catalog main --schema demand_planning_demo
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


QUERIES = {
    "control_tower_weekly": """
SELECT *
FROM {catalog}.{schema}.control_tower_weekly
WHERE week >= date_sub(current_date(), 91)
ORDER BY week DESC
""".strip(),
    "otif_trend": """
SELECT week, region, avg(otif_rate) AS otif_rate
FROM {catalog}.{schema}.control_tower_weekly
GROUP BY 1,2
ORDER BY week, region
""".strip(),
    "late_risk_hotspots": """
SELECT
  date_trunc('week', order_date) AS week,
  customer_region,
  sku_family,
  avg(late_risk_prob) AS avg_late_risk_prob
FROM {catalog}.{schema}.order_late_risk_scored
GROUP BY 1,2,3
ORDER BY avg_late_risk_prob DESC
LIMIT 50
""".strip(),
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--template", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schema", required=True)
    args = ap.parse_args()

    template_path = Path(args.template)
    out_path = Path(args.out)

    obj = json.loads(template_path.read_text(encoding="utf-8"))

    # NOTE: The exact patch points depend on the template schema.
    # We intentionally keep this conservative: store queries in a top-level field
    # so a human (or a follow-up automation) can map them into datasets/widgets.
    obj["_generated_by"] = "scripts/generate_vdash.py"
    obj["_demo_queries"] = {k: v.format(catalog=args.catalog, schema=args.schema) for k, v in QUERIES.items()}

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    print(f"Wrote: {out_path}")


if __name__ == "__main__":
    main()

