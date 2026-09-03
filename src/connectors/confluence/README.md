# Self-Contained Atlassian Confluence Ingestion Connector

A fully self-contained, enterprise-grade ingestion connector for Atlassian Confluence into an Amazon S3 Lakehouse using pure Python on AWS Glue Python Shell.

## Features
- **Zero Glue Friction:** Standalone single-file Python script with zero external package dependencies in Glue.
- **Serverless Compute:** Runs on lightweight AWS Glue Python Shell (0.0625 DPU $\approx \$0.0027/\text{hr}$) with ~10-second cold starts.
- **Confluence REST API v2:** Multi-space pagination with modified-date cursor watermarking and space tree hierarchy preservation.
- **Content Formatting:** Extracts raw Confluence storage XHTML (`?body-format=storage`) for RAG chunking.
- **RAG Security Trimming:** Ingests Confluence page restrictions (restricted user account IDs and group names) into companion `metadata.json` files.
- **Idempotency Gate:** Sub-millisecond version.number checks skip unchanged pages without re-downloading.

---

## 1. Running Locally via Python CLI

You can execute the connector locally for testing using standard environment variables:

```bash
pip install -r requirements.txt

export AWS_REGION="us-east-1"
export S3_LANDING_BUCKET="my-enterprise-lakehouse-raw"
export CONFLUENCE_SECRET_NAME="enterprise/rag/confluence_auth"
export MAX_WORKERS="8"
export MAX_REQUESTS_PER_SEC="10.0"

python connector.py
```

---

## 2. Deploying to AWS Glue Python Shell

Deploy directly to AWS Glue using the included deployment script:

```bash
chmod +x deploy_glue.sh
./deploy_glue.sh my-enterprise-lakehouse-raw arn:aws:iam::123456789012:role/GlueRole
```

Or trigger a run via AWS CLI:
```bash
aws glue start-job-run --job-name enterprise-confluence-ingestion
```
