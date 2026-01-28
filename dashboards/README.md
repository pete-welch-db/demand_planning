## AI/BI Dashboard import file (`*.lvdash.json`)

Databricks has multiple dashboard products (legacy SQL dashboards vs AI/BI “Lakeview” dashboards) with **different JSON export/import formats**.

This repo includes a ready-to-import dashboard JSON:
- `dashboards/demand_planning_control_tower.lvdash.json`

It is based on the working schema you provided (datasets + pages + widgets).

### Customize the target schema

The file currently points at:
- `welch.demand_planning_demo`

If your target is different, do a simple find/replace on:
- `welch.demand_planning_demo` → `<your_catalog>.<your_schema>`

### If your workspace export schema differs

If Databricks export/import formats change in your workspace release, use your workspace-exported JSON as a template.

To generate an importable dashboard JSON that matches *your* workspace:

1. In Databricks, create a **new (empty) AI/BI Dashboard**.
2. Use **Export** to download its JSON (often named something like `*.lvdash.json` or `*.vdash.json` depending on workspace release).
3. Paste that exported JSON into `dashboards/template.lvdash.json` in this repo.
4. Run `scripts/generate_vdash.py` to produce a new `dashboards/*.lvdash.json` with the correct schema + our prebuilt SQL queries.

If you paste the first ~50 lines of your exported dashboard JSON here, I can wire the generator to your exact schema and produce the final `vdash.json` directly.

