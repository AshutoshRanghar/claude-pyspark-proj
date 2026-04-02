# Databricks Asset Bundle (DAB) Setup

This project uses Databricks Asset Bundles for Infrastructure-as-Code deployment.

## Project Structure

```
claude-pyspark-proj/
├── databricks.yml              # DAB configuration
├── resources/
│   └── jobs.yml               # Job definitions
├── src/
│   ├── jobs/
│   │   └── event_hub_streaming.py   # Job entry point
│   └── claude_pyspark_proj/
│       └── streaming/
│           └── get_data_from_event_hubs.py  # Pipeline logic
└── .github/workflows/
    └── deploy-and-run.yml     # GitHub Actions CI/CD
```

## Local Development

### Install DAB CLI

```bash
pip install databricks-cli
```

### Authenticate

```bash
databricks configure --token
# Enter host: https://adb-5226613806345386.6.azuredatabricks.net
# Enter token: <your personal access token>
```

### Validate Bundle

```bash
databricks bundle validate --target dev
```

### Deploy to Dev

```bash
databricks bundle deploy --target dev
```

### Run Job Locally

```bash
databricks bundle run event_hub_streaming_job --target dev
```

## GitHub Actions Deployment

### Prerequisites

Set up these secrets in GitHub repository settings:

- `DATABRICKS_HOST` — Your Databricks workspace URL
- `DATABRICKS_TOKEN` — Your personal access token

### Workflows

**Auto-deploy on push to main:**
```bash
git push origin main
# Workflow automatically validates, deploys, and runs the job
```

**Manual trigger:**
```
GitHub > Actions > Deploy & Run Databricks Job > Run workflow
```

## Targets

- **dev** — Development environment (default)
- **prod** — Production environment

Switch targets:
```bash
databricks bundle deploy --target prod
```

## Troubleshooting

### Bundle validation fails

```bash
databricks bundle validate --target dev -v
```

### Job not found

Ensure the job name matches exactly in `resources/jobs.yml`:
```bash
databricks jobs list --output json | jq '.jobs[] | {id: .job_id, name: .name}'
```

### Deployment issues

Check workspace permissions and token validity:
```bash
databricks workspace list /
```
