# Self-Contained Microsoft SharePoint Ingestion Connector

A fully self-contained, enterprise-grade ingestion connector for Microsoft SharePoint into an Amazon S3 Lakehouse using pure Python on AWS Glue Python Shell.

## Features
- **Zero Glue Friction:** Standalone single-file Python script with zero external package dependencies in Glue.
- **Serverless Compute:** Runs on lightweight AWS Glue Python Shell (0.0625 DPU $\approx \$0.0027/\text{hr}$) with ~10-second cold starts.
- **Microsoft Graph Delta Protocol:** Follows `@odata.nextLink` pagination and opaque `@odata.deltaLink` cursors with self-healing on `HTTP 410 Gone`.
- **Zero-RAM Direct Streaming:** Passes raw HTTP TCP sockets directly to S3 multipart uploads via `upload_fileobj` (streams multi-GB files with ~16 MB RAM).
- **RAG Security Trimming:** Ingests Entra ID Security Group SIDs and user UPNs into companion `metadata.json` files.
- **Idempotency Gate:** Sub-millisecond ETag checks skip unchanged files without re-downloading.

---

## 1. Running Locally via Python CLI

You can execute the connector locally for testing using standard environment variables:

```bash
pip install -r requirements.txt

export AWS_REGION="us-east-1"
export S3_LANDING_BUCKET="my-enterprise-lakehouse-raw"
export SHAREPOINT_SECRET_NAME="enterprise/rag/sharepoint_auth"
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
aws glue start-job-run --job-name enterprise-sharepoint-ingestion
```
