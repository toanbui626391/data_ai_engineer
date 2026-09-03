# Microsoft SharePoint Custom Ingestion Connector: Architectural Specification

> **Document Type:** Connector Technical Specification  
> **Source Platform:** Microsoft SharePoint Online / Microsoft 365 (via Microsoft Graph API v1.0) & SharePoint Server On-Premises  
> **Destination:** Amazon S3 Bronze Data Lakehouse (Parquet / Raw Binary & Metadata)  
> **Runtime:** AWS Glue Python Shell (0.0625 DPU)  
> **Reference Implementation:** [connector.py](file:///Users/toanbui/dev/data_ai_engineer/src/connectors/sharepoint/connector.py)

---

## 1. Protocol Architecture & Ingestion Lifecycle

The SharePoint Ingestion Connector utilizes the **Microsoft Graph API Delta Query Protocol** (`/root/delta`) to achieve incremental change detection across document libraries containing hundreds of thousands of files without recursive tree traversal.

### Architecture Logic Flowchart

```mermaid
flowchart TD
    %% ── Universal Contrast Palette (Light & Dark Mode Safe) ──
    classDef default fill:#1e293b,stroke:#38bdf8,stroke-width:1.5px,color:#f8fafc;
    classDef brain fill:#1e3a5f,stroke:#60a5fa,stroke-width:2px,color:#eff6ff;
    classDef storage fill:#0f172a,stroke:#818cf8,stroke-width:2px,color:#f8fafc;
    classDef decision fill:#451a03,stroke:#fbbf24,stroke-width:2px,color:#fffbeb;
    classDef guard fill:#450a0a,stroke:#f87171,stroke-width:2px,color:#fef2f2;
    classDef success fill:#064e3b,stroke:#34d399,stroke-width:2px,color:#ecfdf5;

    subgraph Init ["1. Initialization & Security Handshake"]
        A["Start AWS Glue Job<br/>(sharepoint_sync.py)"]:::brain --> B["Fetch Secrets<br/>(AWS Secrets Manager)"]:::default
        B --> C["Entra ID Token Exchange<br/>POST /oauth2/v2.0/token"]:::default
        C --> D["Bearer Access Token Acquired"]:::default
        D --> E{"Read S3 Checkpoint<br/>state/sharepoint_{site}_delta.json"}:::decision
        E -->|"Checkpoint Exists"| F["current_url = @odata.deltaLink"]:::default
        E -->|"No Checkpoint"| G["current_url = /drive/root/delta"]:::default
    end

    subgraph DeltaLoop ["2. Graph Delta Query & Ingress Gate"]
        F & G --> H["Rate Limiter Acquire<br/>(BoundedRateLimiter: 10 QPS)"]:::default
        H --> I["GET current_url<br/>(Authorization: Bearer)"]:::default
        I --> J{"HTTP Response Status?"}:::decision
        
        J -->|"HTTP 429"| K["Sleep: Retry-After + Full Jitter<br/>Exponential Backoff"]:::decision
        K --> H
        
        J -->|"HTTP 410 Gone"| L["Self-Healing Reset:<br/>Clear cursor, restart baseline crawl"]:::guard
        L --> G
        
        J -->|"HTTP 200 OK"| M["Parse Delta Batch Payload<br/>Count items & sum bytes (Telemetry)"]:::default
    end

    subgraph ConcurrentPool ["3. Concurrent Worker Processing (ThreadPoolExecutor)"]
        M --> N["Dispatch Items to Worker Pool"]:::default
        N --> O{"Item Type / State?"}:::decision
        
        O -->|"Folder"| P["Skip Item (No Binary)"]:::default
        O -->|"Deleted (@removed)"| Q["Write S3 /DELETED Tombstone<br/>Record Manifest DELETE"]:::guard
        
        O -->|"File Item"| R{"ETag Cache Check<br/>(S3 HeadObject metadata)"}:::decision
        R -->|"ETag Changed / New"| T["Get @microsoft.graph.downloadUrl"]:::success
        T --> U["Zero-RAM Socket Stream<br/>upload_fileobj to S3"]:::success
        
        R -->|"ETag Unchanged"| S["Skip Binary Download (0 ms)<br/>(Prevents Redundant Stream)"]:::default
        
        U & S --> V["GET /drive/items/{id}/permissions<br/>(Always Executed for Delta Items)"]:::default
        V --> W["Extract grantedToV2<br/>(Entra ID SIDs & user UPNs)"]:::default
        W --> X["Write S3 metadata.json Sidecar<br/>Record Manifest INSERT/UPDATE/ACL_REFRESH"]:::storage
        
        T -.->|"Exception / Corrupted"| Y["Quarantine Side-Output<br/>quarantine/sharepoint/{id}/error.json"]:::guard
    end

    subgraph Commit ["4. Atomic Commit-After-Write & Manifest"]
        P & Q & X & Y --> Z{"Has @odata.nextLink?"}:::decision
        Z -->|"Yes (More Pages)"| AA["current_url = @odata.nextLink"]:::default
        AA --> H
        
        Z -->|"No (Terminal Page)"| AB["Extract Terminal @odata.deltaLink"]:::default
        AB --> AC["Atomically Commit S3 Checkpoint<br/>state/sharepoint_{site}_delta.json"]:::success
        AC --> AD["Flush Batch Manifest (JSONL)<br/>state/manifests/sharepoint/*.jsonl"]:::storage
        AD --> AE["Job Finished Successfully"]:::success
    end
```

### Protocol Sequence Diagram

```mermaid
sequenceDiagram
    autonumber
    participant Glue as AWS Glue Python Shell
    participant Secrets as AWS Secrets Manager
    participant Entra as Microsoft Entra ID
    participant Graph as Microsoft Graph API
    participant S3 as Amazon S3 Lakehouse

    Glue->>Secrets: Fetch tenant_id, client_id, client_secret
    Glue->>Entra: POST /oauth2/v2.0/token (client_credentials)
    Entra-->>Glue: Bearer Access Token (JWT)
    
    Glue->>S3: Read state/sharepoint_{site}_delta.json
    alt Checkpoint Found
        S3-->>Glue: Last Delta Link URL
    else First Run
        Glue->>Glue: Construct initial /drive/root/delta URL
    end

    loop Delta Batch Pagination
        Glue->>Graph: GET delta_url (Bearer Token)
        alt HTTP 410 Gone (Token Expired)
            Graph-->>Glue: 410 Gone
            Glue->>Glue: Reset cursor, restart baseline delta crawl
        else HTTP 200 OK
            Graph-->>Glue: Page Payload (@odata.nextLink or @odata.deltaLink)
            
            par Concurrent Item Processing (ThreadPool)
                alt Item Deleted (@removed)
                    Glue->>S3: Write raw/.../{item_id}/DELETED tombstone
                else Item Modified / Created
                    Glue->>S3: Check S3 ETag (HeadObject)
                    alt ETag Changed or New
                        Glue->>Graph: Stream @microsoft.graph.downloadUrl
                        Graph-->>S3: upload_fileobj(resp.raw) [Direct Zero-RAM]
                    else ETag Matches
                        Glue->>Glue: Skip binary stream (0 ms)
                    end
                    Note over Glue,Graph: Always refresh permissions on delta to prevent ACL drift
                    Glue->>Graph: GET /drive/items/{id}/permissions
                    Graph-->>Glue: Permissions (grantedToV2)
                    Glue->>S3: Write raw/.../{item_id}/metadata.json
                end
            end
        end
    end

    Glue->>S3: Atomically save terminal @odata.deltaLink to state/
    Glue->>S3: Flush batch manifest (manifest_{run_id}.jsonl)
```

---

## 2. Microsoft Graph Delta Query Protocol Mechanics

### 1. Delta URL Initialization
The sync starts at the document library root:
```http
GET https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root/delta
Authorization: Bearer {access_token}
Prefer: deltashowremoveddatamotion
```

### 2. Pagination & State Retention
* **In-Flight Batches:** Graph returns pages of changes with an `@odata.nextLink` property. The connector iterates through these links to drain the change backlog.
* **Terminal Checkpoint (`@odata.deltaLink`):** The final page of changes contains `@odata.deltaLink` instead of `@odata.nextLink`. This opaque URL contains the sync watermark:
  ```json
  {
    "@odata.deltaLink": "https://graph.microsoft.com/v1.0/sites/.../root/delta?token=aHR0cHM6Ly9n..."
  }
  ```
* **Atomic Persistence:** The connector commits `@odata.deltaLink` to `s3://{bucket}/state/sharepoint_{site}_delta.json` **only after** all documents in that run have been durably written.

### 3. Self-Healing on HTTP 410 Gone
* Microsoft Graph delta tokens expire if a pipeline is paused beyond the tenant's transaction retention window (typically 30 to 60 days).
* When a stale token is passed, Graph responds with `HTTP 410 Gone`.
* **Automated Self-Healing:** The connector catches `HTTP 410`, invalidates the cached cursor, and restarts a baseline delta crawl. Because the **ETag Cache Gate** is active, existing unchanged files in S3 are skipped in milliseconds, eliminating redundant binary re-downloads.

---

## 3. Zero-RAM Direct Binary Streaming

Managed lakehouse connectors (such as Databricks Lakeflow) load file contents into memory as single records, causing failures on files larger than 100 MB. 

The custom connector circumvents this through **two-stage direct-to-S3 streaming**:

```python
# 1. Graph returns a temporary, pre-authenticated CDN download URL
download_url = item.get("@microsoft.graph.downloadUrl")

# 2. Stream directly from CDN socket into S3 without memory buffer
with http_client.session.get(download_url, stream=True) as stream_resp:
    stream_resp.raise_for_status()
    s3_client.upload_fileobj(
        Fileobj=stream_resp.raw,
        Bucket=bucket_name,
        Key=f"raw/sharepoint/{site_id}/{item_id}/{file_name}",
        ExtraArgs={
            "ContentType": mime_type,
            "Metadata": {
                "upstream_etag": upstream_etag,
                "sharepoint_item_id": item_id
            }
        }
    )
```

* **Socket Plumbing:** `stream_resp.raw` is the underlying `urllib3` HTTPResponse socket. 
* **Buffer Isolation:** `upload_fileobj` streams the socket in 8 MB chunks directly to S3's multipart upload API. Process memory remains flat at ~16 MB even when streaming multi-gigabyte ISOs or CAD files.

---

## 4. Microsoft Entra ID Security Trimming (ACL Extraction)

To support enterprise Retrieval-Augmented Generation (RAG) where users must only query documents they have permission to view:

### 1. Permissions Query
For each ingested document, the connector queries:
```http
GET https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{item_id}/permissions
```

### 2. Sidecar Metadata Generation
The connector extracts the `grantedToV2` structure, resolving:
* **Entra ID Security Groups:** Unique object GUIDs (e.g., `group:8e45f210-91a2-4a0b-bc11-123456789abc`)
* **User Principals:** User Principal Names (e.g., `user:cfo@company.com`)
* **Inheritance Flag:** Whether permissions are inherited from parent folders or broken explicitly.

The sidecar is written alongside the raw document:
```json
// s3://{bucket}/raw/sharepoint/{site_id}/{item_id}/metadata.json
{
  "doc_id": "01ABCD56789...",
  "file_name": "FY26_Executive_Compensation.xlsx",
  "site_id": "company.sharepoint.com,site-uuid,web-uuid",
  "upstream_etag": "\"{12345678-ABCD-EF01},2\"",
  "size_bytes": 482910,
  "mime_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  "web_url": "https://company.sharepoint.com/sites/hr/Shared Documents/...",
  "created_at_utc": "2026-01-15T08:30:00Z",
  "modified_at_utc": "2026-02-28T14:12:00Z",
  "allowed_principals": [
    "group:8e45f210-91a2-4a0b-bc11-123456789abc",
    "user:cfo@company.com"
  ],
  "inherited_from_parent": true,
  "synced_at_utc": "2026-09-03T10:30:00Z"
}
```

---

## 5. Deletion, Move & Rename Semantics

| Upstream Event | Microsoft Graph Delta Payload | Connector Behavior |
| :--- | :--- | :--- |
| **File Deletion** | Emits item with `"@removed": {"reason": "deleted"}` | Writes `raw/.../{item_id}/DELETED` tombstone. Emits `DELETE` manifest entry. Downstream vector cleaner purges document. |
| **File Rename** | Same `item_id`, new `name` | Updates `metadata.json` with new file name. Preserves deterministic folder pointer. |
| **File Move** | Same `item_id`, new `parentReference.id` | Updates `parent_id` in `metadata.json`. No re-download of binary required. |
| **Permission Change** | File metadata unchanged, but `/permissions` updated | Connector refreshes `metadata.json` sidecar without re-streaming binary. |

---

## 6. Secrets Manager & Configuration Schema

Store credentials in **AWS Secrets Manager** under secret name `enterprise/rag/sharepoint_auth`:

```json
{
  "tenant_id": "00000000-1111-2222-3333-444444444444",
  "client_id": "55555555-6666-7777-8888-999999999999",
  "client_secret": "AQ...YOUR_AZURE_AD_APP_CLIENT_SECRET",
  "site_id": "company.sharepoint.com,11111111-2222-3333-4444-555555555555,66666666-7777-8888-9999-000000000000",
  "drive_id": ""
}
```
*Note: If `drive_id` is omitted, the connector defaults to the primary document library of the site (`/drive/root/delta`).*

---

## 7. Circuit Breaker & Failure Isolation Governance

To prevent cascading downstream outages, credential exhaust, and infinite error cascades:

### 1. Poison-Pill Quarantine Boundary
Individual document failures (corrupted PDFs, broken TCP streams, malformed filenames) are caught and directed to:
```
s3://{bucket}/quarantine/sharepoint/{item_id}/error.json
```
The batch continues processing the remaining 99.9% of healthy items without blocking the pipeline.

### 2. Batch-Level Cascading Circuit Breaker
If systemic failures occur (e.g., Entra ID token revoked mid-run, S3 bucket permissions altered, enterprise proxy drops connections):
* **Consecutive Failure Limit:** If **20 consecutive items** fail across worker threads, the circuit breaker immediately trips.
* **Batch Error Rate Threshold:** If the overall batch error rate exceeds **15%** of discovered items, the worker pool aborts execution.
* **State Safety:** When the circuit breaker trips, the `@odata.deltaLink` checkpoint is **not** committed. The next job execution restarts from the last durable checkpoint, safely retrying the batch after root-cause remediation.

---

## 8. Observability, Metric Filters & Enterprise Alerting Architecture

A production lakehouse ingestion connector must provide real-time observability, telemetry, and automated alerting for SLA breaches, rate limit spikes, and data quarantine anomalies.

### Observability & Alerting Architecture

```mermaid
flowchart TD
    %% ── Universal Contrast Palette (Light & Dark Mode Safe) ──
    classDef default fill:#1e293b,stroke:#38bdf8,stroke-width:1.5px,color:#f8fafc;
    classDef brain fill:#1e3a5f,stroke:#60a5fa,stroke-width:2px,color:#eff6ff;
    classDef storage fill:#0f172a,stroke:#818cf8,stroke-width:2px,color:#f8fafc;
    classDef decision fill:#451a03,stroke:#fbbf24,stroke-width:2px,color:#fffbeb;
    classDef guard fill:#450a0a,stroke:#f87171,stroke-width:2px,color:#fef2f2;
    classDef success fill:#064e3b,stroke:#34d399,stroke-width:2px,color:#ecfdf5;

    subgraph GlueJob ["1. Glue Connector Execution"]
        Job["AWS Glue Python Shell<br/>(sharepoint_sync.py)"]:::brain
        Logs["Structured JSON Logs<br/>(stdout / CloudWatch)"]:::default
        Job --> Logs
    end

    subgraph CloudWatch ["2. Amazon CloudWatch Observability Engine"]
        CWLogs["CloudWatch Log Group<br/>/aws-glue/python-jobs/"]:::storage
        MetricFilter["CloudWatch Metric Filters<br/>• ErrorCount<br/>• 429ThrottlingCount<br/>• QuarantineCount"]:::decision
        Alarms["CloudWatch Alarms<br/>• JobFailedAlarm<br/>• QuarantineSpikeAlarm<br/>• ThrottlingSpikeAlarm"]:::guard
        
        Logs --> CWLogs
        CWLogs --> MetricFilter
        MetricFilter --> Alarms
    end

    subgraph EventBridge ["3. Orchestration & Event Routing"]
        GlueEvents["EventBridge Rule<br/>Glue Job State Change: FAILED / TIMEOUT"]:::decision
    end

    subgraph Notification ["4. Enterprise Incident Alerting"]
        SNS["Amazon SNS Topic<br/>(data-platform-alerts)"]:::guard
        PagerDuty["PagerDuty / Slack Webhook<br/>(On-Call Engineer Notification)"]:::guard
        
        Alarms --> SNS
        GlueEvents --> SNS
        SNS --> PagerDuty
    end
```

### CloudWatch Metric Filters (Derived from `log_json`)

Every ingestion event emits structured JSON logs. CloudWatch Metric Filters automatically extract continuous metric data without external agents:

| Metric Name | CloudWatch Metric Filter Pattern | Metric Namespace | Unit |
| :--- | :--- | :--- | :--- |
| **`SharePointQuarantineCount`** | `{ $.level = "error" && $.action = "quarantine_item" }` | `DataLake/Ingestion/SharePoint` | Count |
| **`SharePointThrottlingCount`** | `{ $.level = "warning" && $.status_code = 429 }` | `DataLake/Ingestion/SharePoint` | Count |
| **`SharePointDeltaResetCount`** | `{ $.action = "delta_reset_410" }` | `DataLake/Ingestion/SharePoint` | Count |
| **`SharePointBytesIngested`** | `{ $.metrics.bytes_transferred = * }` | `DataLake/Ingestion/SharePoint` | Bytes |

### CloudWatch Alarms & Incident Escalation SLA

| Alarm Name | Evaluation Condition | Severity | Action & Target | Rationale |
| :--- | :--- | :--- | :--- | :--- |
| **`SharePointJobFailedAlarm`** | EventBridge state $\in$ `["FAILED", "TIMEOUT", "STOPPED"]` | **P1 - Critical** | SNS $\to$ PagerDuty | Immediate on-call engagement for broken ingestion pipeline. |
| **`SharePointQuarantineSpike`** | `SharePointQuarantineCount >= 5` in 15 mins | **P2 - High** | SNS $\to$ Slack (`#data-ops-alerts`) | Flags corrupted documents, upstream encoding breaks, or malformed PDFs. |
| **`SharePointExcessiveThrottling`** | `SharePointThrottlingCount >= 50` in 10 mins | **P3 - Medium** | SNS $\to$ Slack (`#data-ops-alerts`) | Tenant rate limits saturated; tune down `MAX_REQUESTS_PER_SEC`. |
| **`SharePointRunDurationSLA`** | Glue `ExecutionTime > 7200s` (2 hours) | **P2 - High** | SNS $\to$ Slack (`#data-ops-alerts`) | Catches socket hangs or unthrottled massive batch backlogs. |
| **`SharePointDeltaTokenExpired`** | `SharePointDeltaResetCount >= 1` | **P3 - Medium** | SNS $\to$ CloudWatch Dashboard | Informs platform team that tenant token was stale and full baseline sync completed. |

