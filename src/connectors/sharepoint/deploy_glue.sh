#!/usr/bin/env bash
# ==============================================================================
# Deploy Self-Contained SharePoint Connector to AWS Glue Python Shell
# ==============================================================================
set -euo pipefail

S3_BUCKET="${1:-}"
GLUE_ROLE_ARN="${2:-}"
TIER1_JOB_NAME="${3:-enterprise-sharepoint-ingestion}"
TIER2_JOB_NAME="${4:-enterprise-sharepoint-heavy-ingestion}"

if [[ -z "$S3_BUCKET" || -z "$GLUE_ROLE_ARN" ]]; then
    echo "Usage: ./deploy_glue.sh <S3_LANDING_BUCKET> <GLUE_IAM_ROLE_ARN> [TIER1_JOB_NAME] [TIER2_JOB_NAME]"
    echo "Example: ./deploy_glue.sh my-lakehouse-raw arn:aws:iam::123456789012:role/GlueRole"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
S3_SCRIPT_URI="s3://${S3_BUCKET}/glue_scripts/sharepoint/connector.py"

echo "==> 1. Uploading self-contained connector.py to ${S3_SCRIPT_URI}..."
aws s3 cp "${SCRIPT_DIR}/connector.py" "${S3_SCRIPT_URI}"

echo "==> 2. Deploying Tier 1 (Fast-Lane Delta) Glue Job [${TIER1_JOB_NAME}] (0.0625 DPU)..."
if aws glue get-job --job-name "${TIER1_JOB_NAME}" >/dev/null 2>&1; then
    echo "Updating existing Tier 1 Glue Job [${TIER1_JOB_NAME}]..."
    aws glue update-job \
        --job-name "${TIER1_JOB_NAME}" \
        --job-update "{
            \"Role\": \"${GLUE_ROLE_ARN}\",
            \"Command\": {
                \"Name\": \"pythonshell\",
                \"ScriptLocation\": \"${S3_SCRIPT_URI}\",
                \"PythonVersion\": \"3.9\"
            },
            \"DefaultArguments\": {
                \"--S3_LANDING_BUCKET\": \"${S3_BUCKET}\",
                \"--SHAREPOINT_SECRET_NAME\": \"enterprise/rag/sharepoint_auth\",
                \"--MODE\": \"delta\",
                \"--HEAVY_FILE_THRESHOLD_BYTES\": \"524288000\",
                \"--MAX_WORKERS\": \"4\",
                \"--MAX_REQUESTS_PER_SEC\": \"10.0\",
                \"library-set\": \"analytics\"
            },
            \"ExecutionProperty\": {
                \"MaxConcurrentRuns\": 1
            },
            \"MaxCapacity\": 0.0625,
            \"Timeout\": 45,
            \"GlueVersion\": \"3.0\"
        }"
else
    echo "Creating new Tier 1 Glue Job [${TIER1_JOB_NAME}]..."
    aws glue create-job \
        --name "${TIER1_JOB_NAME}" \
        --role "${GLUE_ROLE_ARN}" \
        --command "{
            \"Name\": \"pythonshell\",
            \"ScriptLocation\": \"${S3_SCRIPT_URI}\",
            \"PythonVersion\": \"3.9\"
        }" \
        --default-arguments "{
            \"--S3_LANDING_BUCKET\": \"${S3_BUCKET}\",
            \"--SHAREPOINT_SECRET_NAME\": \"enterprise/rag/sharepoint_auth\",
            \"--MODE\": \"delta\",
            \"--HEAVY_FILE_THRESHOLD_BYTES\": \"524288000\",
            \"--MAX_WORKERS\": \"4\",
            \"--MAX_REQUESTS_PER_SEC\": \"10.0\",
            \"library-set\": \"analytics\"
        }" \
        --execution-property "{
            \"MaxConcurrentRuns\": 1
        }" \
        --max-capacity 0.0625 \
        --timeout 45 \
        --glue-version "3.0"
fi

echo "==> 3. Deploying Tier 2 (Heavy-Lane Bulk) Glue Job [${TIER2_JOB_NAME}] (1.0 DPU)..."
if aws glue get-job --job-name "${TIER2_JOB_NAME}" >/dev/null 2>&1; then
    echo "Updating existing Tier 2 Glue Job [${TIER2_JOB_NAME}]..."
    aws glue update-job \
        --job-name "${TIER2_JOB_NAME}" \
        --job-update "{
            \"Role\": \"${GLUE_ROLE_ARN}\",
            \"Command\": {
                \"Name\": \"pythonshell\",
                \"ScriptLocation\": \"${S3_SCRIPT_URI}\",
                \"PythonVersion\": \"3.9\"
            },
            \"DefaultArguments\": {
                \"--S3_LANDING_BUCKET\": \"${S3_BUCKET}\",
                \"--SHAREPOINT_SECRET_NAME\": \"enterprise/rag/sharepoint_auth\",
                \"--MODE\": \"heavy_worker\",
                \"--MAX_WORKERS\": \"8\",
                \"--MAX_REQUESTS_PER_SEC\": \"10.0\",
                \"library-set\": \"analytics\"
            },
            \"ExecutionProperty\": {
                \"MaxConcurrentRuns\": 3
            },
            \"MaxCapacity\": 1.0,
            \"Timeout\": 120,
            \"GlueVersion\": \"3.0\"
        }"
else
    echo "Creating new Tier 2 Glue Job [${TIER2_JOB_NAME}]..."
    aws glue create-job \
        --name "${TIER2_JOB_NAME}" \
        --role "${GLUE_ROLE_ARN}" \
        --command "{
            \"Name\": \"pythonshell\",
            \"ScriptLocation\": \"${S3_SCRIPT_URI}\",
            \"PythonVersion\": \"3.9\"
        }" \
        --default-arguments "{
            \"--S3_LANDING_BUCKET\": \"${S3_BUCKET}\",
            \"--SHAREPOINT_SECRET_NAME\": \"enterprise/rag/sharepoint_auth\",
            \"--MODE\": \"heavy_worker\",
            \"--MAX_WORKERS\": \"8\",
            \"--MAX_REQUESTS_PER_SEC\": \"10.0\",
            \"library-set\": \"analytics\"
        }" \
        --execution-property "{
            \"MaxConcurrentRuns\": 3
        }" \
        --max-capacity 1.0 \
        --timeout 120 \
        --glue-version "3.0"
fi

echo "==> Two-Tier Deployment Complete!"
echo "    Tier 1 (Fast-Lane):  aws glue start-job-run --job-name ${TIER1_JOB_NAME}"
echo "    Tier 2 (Heavy-Lane): aws glue start-job-run --job-name ${TIER2_JOB_NAME} --arguments '{\"--ITEM_ID\":\"01ABCD...\",\"--DRIVE_ID\":\"b!xyz...\"}'"
