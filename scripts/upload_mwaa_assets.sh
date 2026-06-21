#!/usr/bin/env bash
# upload_mwaa_assets.sh
#
# Syncs DAGs, requirements.txt, and the dbt project to S3 before
# applying or updating the MWAA environment.
#
# Run from the repository root:
#   bash scripts/upload_mwaa_assets.sh [bucket-name]
#
# Defaults to gridoscope-raw-prod if no bucket is given.

set -euo pipefail

BUCKET="${1:-gridoscope-raw-prod}"
MWAA_PREFIX="mwaa"

echo "Uploading MWAA assets to s3://${BUCKET}/${MWAA_PREFIX}/"

# DAG files and SQL templates
aws s3 sync airflow/dags/ "s3://${BUCKET}/${MWAA_PREFIX}/dags/" \
  --exclude "__pycache__/*" \
  --exclude "*.pyc" \
  --delete

echo "  DAGs synced"

# requirements.txt — MWAA installs these on environment start/update
aws s3 cp airflow/requirements.txt "s3://${BUCKET}/${MWAA_PREFIX}/requirements.txt"

echo "  requirements.txt uploaded"

# dbt project — placed under dags/dbt/ so MWAA syncs it to all workers
# alongside the DAGs. Exclude compiled artefacts and local dev files.
aws s3 sync gridoscope_dbt/ "s3://${BUCKET}/${MWAA_PREFIX}/dags/dbt/gridoscope_dbt/" \
  --exclude "target/*" \
  --exclude "logs/*" \
  --exclude ".gitignore" \
  --exclude ".user.yml" \
  --delete

echo "  dbt project synced"
echo ""
echo "Done. Run 'terraform apply -target=module.mwaa -var-file=prod.tfvars' next."
