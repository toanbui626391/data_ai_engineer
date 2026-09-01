# AWS Glue Ingestion Pipeline: SharePoint & Confluence to Amazon S3

Production-grade AWS Glue Python Shell job that ingests enterprise documents and wikis from **Microsoft SharePoint Online** (via Microsoft Graph Delta API) and **Atlassian Confluence** (via REST API v2) into an **Amazon S3 Bronze Data Lakehouse**.

---

## 1. Secrets Manager Schema Configuration

Before running the Glue job, create two secrets in **AWS Secrets Manager** with the following JSON schemas:

### A. SharePoint Secret (`enterprise/rag/sharepoint_auth`)
```json
{
  "tenant_id": "00000000-1111-2222-3333-444444444444",
  "client_id": "55555555-6666-7777-8888-999999999999",
  "client_secret": "YOUR_AZURE_AD_CLIENT_SECRET",
  "site_id": "company.sharepoint.com,a1b2c3d4-0000-0000-0000-000000000000,e5f6a7b8-0000-0000-0000-000000000000",
  "drive_id": "" 
}
```
> **Note**: `drive_id` is optional. If left blank, it automatically targets the default `/drive/root/delta` library for the site.

### B. Confluence Secret (`enterprise/rag/confluence_auth`)
```json
{
  "base_url": "https://your-company.atlassian.net/wiki",
  "user_email": "svc-rag-bot@your-company.com",
  "api_token": "YOUR_CONFLUENCE_API_TOKEN",
  "space_keys": "PLATFORM,ENG,SECURITY,FINANCE"
}
```

---

## 2. AWS Glue Job Parameters & Deployment

### Recommended Job Settings
* **Job Type**: `Python Shell` (Do not use Spark / PySpark to avoid 429 throttling and DPU waste).
* **Python Version**: `Python 3.9`
* **Allocated Capacity**: `0.0625 DPU` (Minimal possible compute cost $\approx$ **$0.027 / hour**).
* **Max Concurrency**: `1` (Ensures sequential, deterministic delta checkpoint commits).
* **Timeout**: `30 minutes`.

### Job Arguments (`DefaultArguments`)
```bash
--S3_LANDING_BUCKET="my-enterprise-rag-lakehouse"
--SHAREPOINT_SECRET_NAME="enterprise/rag/sharepoint_auth"
--CONFLUENCE_SECRET_NAME="enterprise/rag/confluence_auth"
--MAX_WORKERS="8"
--MAX_REQUESTS_PER_SEC="10.0"
--additional-python-modules="requests>=2.31.0"
```

---

## 3. Workload Discovery & Concurrency Controls

1. **Workload Discovery Sweep**:
   * Evaluates delta streams per batch page to calculate total discovered document count and batch volume in MB before heavy streaming.
   * Emits structured JSON progress logs to CloudWatch.
2. **Bounded Thread Pool Concurrency**:
   * `--MAX_WORKERS=8`: Runs 8 concurrent download threads within a single Python Shell job (0.0625 DPU) for high I/O throughput.
   * `--MAX_REQUESTS_PER_SEC=10.0`: Shared thread-safe token-bucket rate limiter to guarantee compliance with tenant Graph API & Confluence rate limits.

---

## 4. 100% Idempotency & Zero-Cost Replay

1. **Deterministic S3 Key Addressing**:
   * All documents land at immutable paths based on upstream IDs: `raw/{source}/{site_id}/{doc_id}/{filename}`.
2. **ETag & Version Cache Validation**:
   * Before downloading a file binary, the script checks if `metadata.json` already exists with matching upstream `eTag` or Confluence `version`.
   * If matched, the download is skipped instantly (`metrics.skipped_existing += 1`), enabling fast, zero-cost recovery after job interruptions.
3. **Atomic Commit-After-Write**:
   * Delta cursors (`@odata.deltaLink` and modified watermarks) are only saved to `state/` after all batch items are written to S3.
4. **Idempotent Tombstone Deletion**:
   * Deleted upstream files write empty `DELETED` markers that overwrite cleanly whether triggered once or multiple times.

---

## 5. Minimal IAM Execution Policy

Attach the following policy to the AWS Glue IAM Role (`AWSGlueServiceRole-RAGIngestion`):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3LakehouseAccess",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:HeadObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::my-enterprise-rag-lakehouse",
        "arn:aws:s3:::my-enterprise-rag-lakehouse/*"
      ]
    },
    {
      "Sid": "SecretsManagerAccess",
      "Effect": "Allow",
      "Action": "secretsmanager:GetSecretValue",
      "Resource": [
        "arn:aws:secretsmanager:*:*:secret:enterprise/rag/*"
      ]
    },
    {
      "Sid": "CloudWatchLogs",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:*"
    }
  ]
}
```

---

## 4. Landed Amazon S3 Data Structure

```text
s3://my-enterprise-rag-lakehouse/
│
├── state/                                     <-- Incremental Sync Cursors
│   ├── sharepoint_company_site_delta.json     <-- Holds Graph @odata.deltaLink
│   └── confluence_space_PLATFORM_cursor.json  <-- Holds ISO-8601 modified watermark
│
├── raw/                                       <-- Bronze Landing Zone
│   ├── sharepoint/site_id/item_10928/
│   │   ├── Architecture_Spec.docx             <-- Streamed binary document
│   │   └── metadata.json                      <-- Sidecar ACLs (Entra ID SIDs, URLs, dates)
│   │
│   ├── sharepoint/site_id/item_10929/
│   │   └── DELETED                            <-- Tombstone for deleted file
│   │
│   └── confluence/PLATFORM/page_884920/
│       ├── content.json                       <-- Page storage format (XHTML/ADF)
│       └── metadata.json                      <-- Page/Space restrictions metadata
│
└── quarantine/                                <-- Bad-Data Isolation
    └── sharepoint/site_id/item_10930/
        └── error.json                         <-- Error message & stack trace
```
