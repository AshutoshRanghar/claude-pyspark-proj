# DAB Quick Start Guide

Your project is now set up with Databricks Asset Bundles (DAB) and GitHub Actions CI/CD.

## What Changed

✓ **databricks.yml** — Updated with DAB job definitions  
✓ **resources/jobs.yml** — Job and cluster configuration  
✓ **src/jobs/event_hub_streaming.py** — Job entry point  
✓ **.github/workflows/deploy-and-run.yml** — GitHub Actions CI/CD  
✓ **.gitignore** — Updated to exclude venv and build artifacts  

## 5-Minute Setup

### 1. Set GitHub Secrets

Go to **GitHub** > **Settings** > **Secrets and variables** > **Actions**

Add these secrets:
- `DATABRICKS_HOST` = `https://adb-5226613806345386.6.azuredatabricks.net`
- `DATABRICKS_TOKEN` = Your [personal access token](https://docs.databricks.com/en/dev-tools/auth/personal-access-tokens.html)

### 2. Update databricks.yml with Your Cluster

Edit the `resources/jobs.yml` to match your cluster:

```yaml
resources:
  clusters:
    job_cluster:
      spark_version: 14.3.x-scala2.12  # Change to match your cluster
      node_type_id: Standard_D4s_v3     # Change to match your cluster
      num_workers: 2                    # Adjust as needed
```

Or use your existing cluster ID:

```yaml
resources:
  jobs:
    event_hub_streaming_job:
      existing_cluster_id: 0317-084801-hofwe5r6  # Your cluster ID
```

### 3. Deploy and Run

**Option A: Push to trigger GitHub Actions**
```bash
git add .
git commit -m "Setup DAB with GitHub Actions"
git push origin main
```

**Option B: Deploy locally first**
```bash
# Install CLI
pip install databricks-cli

# Configure
databricks configure --token

# Validate
databricks bundle validate --target dev

# Deploy
databricks bundle deploy --target dev

# Run
databricks bundle run event_hub_streaming_job --target dev
```

## GitHub Actions Workflow

Automatically triggered on:
- ✓ Push to `main` or `develop`
- ✓ Pull requests
- ✓ Manual trigger (workflow_dispatch)

Pipeline:
1. **Validate** — Checks bundle syntax
2. **Deploy** — Uploads code to Databricks workspace
3. **Run Job** — Executes the streaming pipeline
4. **Monitor** — Waits for completion and reports status

## Project Structure

```
src/jobs/
└── event_hub_streaming.py    ← Databricks job entry point

src/claude_pyspark_proj/
└── streaming/
    └── get_data_from_event_hubs.py   ← Your pipeline logic

resources/
└── jobs.yml                  ← Job & cluster definitions

databricks.yml               ← DAB configuration

.github/workflows/
└── deploy-and-run.yml       ← GitHub Actions CI/CD
```

## Environment Variables in Workflow

The workflow uses these secrets (set in GitHub):
- `DATABRICKS_HOST` — Your workspace URL
- `DATABRICKS_TOKEN` — Authentication token

No hardcoded credentials in code! ✓

## Troubleshooting

**Job not found in GitHub Actions?**
→ Ensure job name in `resources/jobs.yml` matches exactly

**Deployment fails?**
→ Run `databricks bundle validate --target dev -v` locally first

**Token expired?**
→ Generate a new [personal access token](https://docs.databricks.com/en/dev-tools/auth/personal-access-tokens.html)

## Next Steps

1. ✓ Commit and push code
2. ✓ Check GitHub Actions tab for workflow status
3. ✓ Monitor job runs in Databricks workspace
4. ✓ Adjust cluster config in `resources/jobs.yml` as needed

## Useful Commands

```bash
# Validate bundle
databricks bundle validate --target dev

# Deploy
databricks bundle deploy --target dev

# Run job
databricks bundle run event_hub_streaming_job --target dev

# View status
databricks runs get --run-id <run-id>

# List jobs
databricks jobs list --output json | jq '.jobs[]'
```

For more details, see [DAB_SETUP.md](./DAB_SETUP.md)
