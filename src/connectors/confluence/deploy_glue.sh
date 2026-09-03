#!/usr/bin/env bash
# ==============================================================================
# Deploy Self-Contained Confluence Connector to AWS Glue Python Shell
# ==============================================================================
set -euo pipefail

S3_BUCKET="${1:-}"
GLUE_ROLE_ARN="${2:-}"
JOB_NAME="${3:-enterprise-confluence-ingestion}"

if [[ -z "$S3_BUCKET" || -z "$GLUE_ROLE_ARN" ]]; then
    echo "Usage: ./deploy_glue.sh <S3_LANDING_BUCKET> <GLUE_IAM_ROLE_ARN> [JOB_NAME]"
    echo "Example: ./deploy_glue.sh my-lakehouse-raw arn:aws:iam::123456789012:role/GlueRole"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
S3_SCRIPT_URI="s3://${S3_BUCKET}/glue_scripts/confluence/connector.py"

echo "==> 1. Uploading self-contained connector.py to ${S3_SCRIPT_URI}..."
aws s3 cp "${SCRIPT_DIR}/connector.py" "${S3_SCRIPT_URI}"

echo "==> 2. Checking if Glue Job [${JOB_NAME}] exists..."
if aws glue get-job --job-name "${JOB_NAME}" >/dev/null 2>&1; then
    echo "Updating existing Glue Job [${JOB_NAME}]..."
    aws glue update-job \
        --job-name "${JOB_NAME}" \
        --job-update "{
            \"Role\": \"${GLUE_ROLE_ARN}\",
            \"Command\": {
                \"Name\": \"pythonshell\",
                \"ScriptLocation\": \"${S3_SCRIPT_URI}\",
                \"PythonVersion\": \"3.9\"
            },
            \"DefaultArguments\": {
                \"--S3_LANDING_BUCKET\": \"${S3_BUCKET}\",
                \"--CONFLUENCE_SECRET_NAME\": \"enterprise/rag/confluence_auth\",
                \"--MAX_WORKERS\": \"8\",
                \"--MAX_REQUESTS_PER_SEC\": \"10.0\",
                \"--additional-python-modules\": \"requests>=2.31.0\"
            },
            \"MaxCapacity\": 0.0625,
            \"Timeout\": 120,
            \"GlueVersion\": \"3.0\"
        }"
else
    echo "Creating new Glue Job [${JOB_NAME}]..."
    aws glue create-job \
        --name "${JOB_NAME}" \
        --role "${GLUE_ROLE_ARN}" \
        --command "{
            \"Name\": \"pythonshell\",
            \"ScriptLocation\": \"${S3_SCRIPT_URI}\",
            \"PythonVersion\": \"3.9\"
        }" \
        --default-arguments "{
            \"--S3_LANDING_BUCKET\": \"${S3_BUCKET}\",
            \"--CONFLUENCE_SECRET_NAME\": \"enterprise/rag/confluence_auth\",
            \"--MAX_WORKERS\": \"8\",
            \"--MAX_REQUESTS_PER_SEC\": \"10.0\",
            \"--additional-python-modules\": \"requests>=2.31.0\"
        }" \
        --max-capacity 0.0625 \
        --timeout 120 \
        --glue-version "3.0"
fi

echo "==> Deployment Complete! You can run the job via:"
echo "    aws glue start-job-run --job-name ${JOB_NAME}"
