## Automated deployment (Databricks Asset Bundles)

This repo is designed to deploy via **Databricks Asset Bundles (DAB)** and run a single canonical job:
- `Demand_Planning_Demo_Job`

---

## Workflows

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| `databricks-bundle-validate.yml` | Every PR to `main` | Validates bundle config (no deploy) |
| `databricks-bundle-deploy.yml` | Push to `main` / manual | Deploys bundle + runs job |

---

### Option A (recommended): GitHub Actions + PAT (fastest to set up)

1) In your GitHub repo, add **Secrets**:
- `DATABRICKS_HOST` = `https://adb-984752964297111.11.azuredatabricks.net/`
- `DATABRICKS_TOKEN` = a PAT with permissions to deploy/run bundles in the target workspace

2) Push to `main` (or run manually)
- Workflow: `.github/workflows/databricks-bundle-deploy.yml`
- Trigger:
  - push to `main`, or
  - **Actions → Databricks Bundle Deploy → Run workflow** (choose target)

### Option B (better security): GitHub Actions + OIDC

If you want to avoid PATs, use Databricks-supported OIDC auth for GitHub Actions.
High-level flow:
- Configure a Databricks OAuth app/service principal for GitHub OIDC
- Set `DATABRICKS_AUTH_TYPE=github-oidc` and required client variables in the workflow

See Databricks CI/CD docs for GitHub for the current steps.

### Local automation (non-interactive)

You can also run the same steps locally (or from any CI runner) using:

```bash
export DATABRICKS_HOST="https://adb-984752964297111.11.azuredatabricks.net/"
export DATABRICKS_TOKEN="<pat>"
scripts/ci_deploy.sh azure
```

### Notes
- The pipeline is executed by the job using the **existing DLT pipeline id** configured in `databricks.yml`.
- The job includes a “KPI + Metric Refresh” step that creates **UC metric views** (semantic layer) and refreshes post-forecast KPI views.

