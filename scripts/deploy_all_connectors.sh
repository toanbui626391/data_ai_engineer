#!/usr/bin/env bash
# ==============================================================================
# Master Deployment Script: Simple Python AWS Glue Ingestion Connectors
# ==============================================================================
set -euo pipefail

S3_BUCKET="${1:-${S3_LANDING_BUCKET:-}}"
GLUE_ROLE_ARN="${2:-${GLUE_IAM_ROLE_ARN:-}}"
AWS_REGION="${3:-${AWS_DEFAULT_REGION:-us-east-1}}"

if [[ -z "$S3_BUCKET" || -z "$GLUE_ROLE_ARN" ]]; then
    echo "Usage: ./scripts/deploy_all_connectors.sh <S3_LANDING_BUCKET> <GLUE_IAM_ROLE_ARN> [AWS_REGION]"
    echo "Example: ./scripts/deploy_all_connectors.sh my-lakehouse-raw arn:aws:iam::123456789012:role/GlueRole us-east-1"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "========================================================================"
echo "==> Deploying AWS Glue Connectors (Simple Python S3 Deployment)"
echo "==> S3 Bucket:    ${S3_BUCKET}"
echo "==> IAM Role:     ${GLUE_ROLE_ARN}"
echo "==> AWS Region:   ${AWS_REGION}"
echo "========================================================================"

# 1. Validate Python Syntax
echo "==> [Step 1/3] Validating Python Syntax..."
python3 -m py_compile "${ROOT_DIR}/src/connectors/sharepoint/connector.py"
python3 -m py_compile "${ROOT_DIR}/src/connectors/confluence/connector.py"
echo "Syntax check passed!"

# 2. Deploy SharePoint Connector
echo "==> [Step 2/3] Deploying SharePoint Connector..."
chmod +x "${ROOT_DIR}/src/connectors/sharepoint/deploy_glue.sh"
"${ROOT_DIR}/src/connectors/sharepoint/deploy_glue.sh" \
    "${S3_BUCKET}" \
    "${GLUE_ROLE_ARN}" \
    "enterprise-sharepoint-ingestion"

# 3. Deploy Confluence Connector
echo "==> [Step 3/3] Deploying Confluence Connector..."
chmod +x "${ROOT_DIR}/src/connectors/confluence/deploy_glue.sh"
"${ROOT_DIR}/src/connectors/confluence/deploy_glue.sh" \
    "${S3_BUCKET}" \
    "${GLUE_ROLE_ARN}" \
    "enterprise-confluence-ingestion"

echo "========================================================================"
echo "==> All Connectors Successfully Deployed to AWS Glue Python Shell!"
echo "==> You can test runs with:"
echo "    aws glue start-job-run --job-name enterprise-sharepoint-ingestion"
echo "    aws glue start-job-run --job-name enterprise-confluence-ingestion"
echo "========================================================================"
