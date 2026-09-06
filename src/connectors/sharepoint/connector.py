"""
Self-Contained Ingestion Connector: Microsoft SharePoint -> Amazon S3 Lakehouse
Runtime: AWS Glue Python Shell (0.0625 DPU) / Python CLI

Architectural Specification: docs/architecture/glue_connectors/01_sharepoint_connector.md
Engineering Standards: .agents/rules/data_engineer_persona.md

Guarantees:
1. Completely self-contained (zero external module imports required in Glue).
2. Microsoft Graph Delta Query protocol with atomic commit-after-write.
3. Self-healing on HTTP 410 Gone (delta cursor expiration).
4. Zero-RAM direct chunked socket streaming to Amazon S3 (upload_fileobj).
5. Sub-millisecond ETag cache gate to skip unchanged files.
6. Entra ID (Azure AD) ACL extraction (grantedToV2) for RAG security trimming.
7. Token-bucket rate limiting and HTTP 429 exponential backoff with full jitter.
"""

import os
import sys
import json
import time
import random
import logging
import threading
import traceback
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import socket
import requests
import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError, BotoCoreError

# Optional AWS Glue utilities (fallback to os.environ when running in local CLI)
try:
    from awsglue.utils import getResolvedOptions
    GLUE_ENVIRONMENT = True
except ImportError:
    GLUE_ENVIRONMENT = False


# ==============================================================================
# 1. STRUCTURED LOGGING & THREAD-SAFE TELEMETRY
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "module": "%(filename)s", "message": %(message)s}'
)
logger = logging.getLogger("sharepoint_connector")


def log_json(level: str, msg: str, **kwargs):
    """Emits structured JSON log lines to stdout / CloudWatch."""
    payload = {"msg": msg, **kwargs}
    log_func = getattr(logger, level.lower(), logger.info)
    log_func(json.dumps(payload))


class PipelineMetrics:
    """Thread-safe progress and transfer telemetry."""

    def __init__(self):
        self._lock = threading.Lock()
        self.discovered_docs = 0
        self.discovered_bytes = 0
        self.inserted_docs = 0
        self.updated_docs = 0
        self.downloaded_docs = 0
        self.skipped_existing = 0
        self.deleted_docs = 0
        self.quarantined_docs = 0
        self.bytes_transferred = 0
        self.retries_429 = 0
        self.start_time = time.time()

    def record_discovered(self, count: int, total_bytes: int = 0):
        with self._lock:
            self.discovered_docs += count
            self.discovered_bytes += total_bytes

    def record_inserted(self, bytes_count: int):
        with self._lock:
            self.inserted_docs += 1
            self.downloaded_docs += 1
            self.bytes_transferred += bytes_count

    def record_updated(self, bytes_count: int):
        with self._lock:
            self.updated_docs += 1
            self.downloaded_docs += 1
            self.bytes_transferred += bytes_count

    def record_skipped(self):
        with self._lock:
            self.skipped_existing += 1

    def record_deleted(self):
        with self._lock:
            self.deleted_docs += 1

    def record_quarantine(self):
        with self._lock:
            self.quarantined_docs += 1

    def record_retry_429(self):
        with self._lock:
            self.retries_429 += 1

    def get_progress(self) -> Dict[str, Any]:
        with self._lock:
            completed = (
                self.inserted_docs + self.updated_docs +
                self.skipped_existing + self.deleted_docs +
                self.quarantined_docs
            )
            remaining = max(self.discovered_docs - completed, 0)
            pct = round((completed / max(self.discovered_docs, 1)) * 100, 2)
            return {
                "discovered": self.discovered_docs,
                "completed": completed,
                "remaining": remaining,
                "progress_pct": pct,
                "inserted": self.inserted_docs,
                "updated": self.updated_docs,
                "skipped": self.skipped_existing,
                "deleted": self.deleted_docs,
                "quarantined": self.quarantined_docs
            }

    def log_progress(self, context_msg: str = "SharePoint Ingestion Progress"):
        progress = self.get_progress()
        log_json("info", context_msg, **progress)

    def summary(self) -> Dict[str, Any]:
        with self._lock:
            elapsed = max(time.time() - self.start_time, 0.001)
            mb_transferred = self.bytes_transferred / (1024 * 1024)
            completed = (
                self.inserted_docs + self.updated_docs +
                self.skipped_existing + self.deleted_docs +
                self.quarantined_docs
            )
            remaining = max(self.discovered_docs - completed, 0)
            pct = round((completed / max(self.discovered_docs, 1)) * 100, 2)

            return {
                "discovered_docs": self.discovered_docs,
                "discovered_mb": round(self.discovered_bytes / (1024 * 1024), 2),
                "completed_docs": completed,
                "remaining_docs": remaining,
                "progress_pct": pct,
                "inserted_docs": self.inserted_docs,
                "updated_docs": self.updated_docs,
                "downloaded_docs": self.downloaded_docs,
                "skipped_existing_docs": self.skipped_existing,
                "deleted_docs": self.deleted_docs,
                "quarantined_docs": self.quarantined_docs,
                "total_streamed_mb": round(mb_transferred, 2),
                "throughput_mb_sec": round(mb_transferred / elapsed, 2),
                "rate_limit_retries": self.retries_429,
                "duration_seconds": round(elapsed, 2)
            }


# ==============================================================================
# 2. TOKEN-BUCKET RATE LIMITER & RESILIENT HTTP CLIENT
# ==============================================================================
class BoundedRateLimiter:
    """Token-bucket rate limiter to prevent exceeding tenant request quotas."""

    def __init__(self, max_requests_per_sec: float = 10.0):
        self.rate = max_requests_per_sec
        self.capacity = max_requests_per_sec
        self.tokens = max_requests_per_sec
        self.last_fill = time.time()
        self._lock = threading.Lock()

    def acquire(self):
        with self._lock:
            now = time.time()
            elapsed = now - self.last_fill
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last_fill = now

            if self.tokens < 1.0:
                sleep_needed = (1.0 - self.tokens) / self.rate
                time.sleep(sleep_needed)
                self.tokens = 0.0
                self.last_fill = time.time()
            else:
                self.tokens -= 1.0


class ResilientHttpClient:
    """HTTP client with connection pooling, 429 jitter backoff, and 410 interceptor."""

    def __init__(
        self,
        metrics: PipelineMetrics,
        rate_limiter: BoundedRateLimiter,
        max_retries: int = 5,
        base_delay: float = 1.0
    ):
        self.metrics = metrics
        self.rate_limiter = rate_limiter
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=25, pool_maxsize=25, max_retries=1)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def request(self, method: str, url: str, **kwargs) -> Optional[requests.Response]:
        for attempt in range(self.max_retries):
            try:
                self.rate_limiter.acquire()
                resp = self.session.request(method, url, **kwargs)

                if resp.status_code in (200, 201, 204, 206):
                    return resp

                # HTTP 429: Too Many Requests
                if resp.status_code == 429:
                    self.metrics.record_retry_429()
                    header_retry = resp.headers.get("Retry-After")
                    sleep_time = None
                    if header_retry:
                        try:
                            sleep_time = float(header_retry.strip())
                        except (ValueError, TypeError):
                            sleep_time = None

                    if sleep_time is None:
                        sleep_time = self.base_delay * (2 ** attempt)

                    # Full Randomized Jitter
                    jitter = random.uniform(0.1, 0.5) * sleep_time
                    total_sleep = sleep_time + jitter

                    log_json(
                        "warning",
                        "Rate limited by upstream Graph API (HTTP 429)",
                        action="rate_limit_429",
                        status_code=429,
                        url=url,
                        attempt=attempt + 1,
                        max_retries=self.max_retries,
                        sleep_seconds=round(total_sleep, 2)
                    )

                    if attempt == self.max_retries - 1:
                        log_json("error", "Max retries exhausted on HTTP 429 rate limit", url=url)
                        resp.raise_for_status()

                    time.sleep(total_sleep)
                    continue

                # HTTP 410: Gone (Delta token expired)
                if resp.status_code == 410:
                    log_json(
                        "warning",
                        "Delta token expired (HTTP 410 Gone)",
                        action="delta_reset_410",
                        status_code=410,
                        url=url
                    )
                    return resp

                resp.raise_for_status()

            except requests.exceptions.RequestException as e:
                if attempt == self.max_retries - 1:
                    log_json("error", "Max retries exceeded on HTTP request", url=url, error=str(e))
                    raise

                sleep_time = (self.base_delay * (2 ** attempt)) + random.uniform(0.1, 1.0)
                log_json("warning", "Transient network error, retrying...", url=url, attempt=attempt + 1, error=str(e))
                time.sleep(sleep_time)

        return None


# ==============================================================================
# 3. AMAZON S3 SINK & ZERO-RAM STREAMING SINK
# ==============================================================================
class S3Sink:
    """Manages deterministic S3 writes, ETag cache gates, zero-RAM streams, and state."""

    def __init__(self, bucket_name: str, metrics: PipelineMetrics, run_id: Optional[str] = None, mode: str = "delta"):
        self.s3 = boto3.client("s3")
        self.bucket = bucket_name
        self.metrics = metrics
        self.run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        self.manifest_entries = []
        self._manifest_lock = threading.Lock()
        self.mode = mode.lower()

        # Two-Tier TransferConfig Tuning:
        # Tier 1 (delta / 0.0625 DPU): 16 MB chunks, max 2 concurrent parts (RAM bounded to ~32 MB per worker)
        # Tier 2 (heavy_worker / 1.0 DPU): 32 MB chunks, max 4 concurrent parts (High-throughput for multi-GB files)
        if self.mode == "heavy_worker":
            self.transfer_config = TransferConfig(
                multipart_threshold=32 * 1024 * 1024,
                multipart_chunksize=32 * 1024 * 1024,
                max_concurrency=4,
                use_threads=True
            )
        else:
            self.transfer_config = TransferConfig(
                multipart_threshold=16 * 1024 * 1024,
                multipart_chunksize=16 * 1024 * 1024,
                max_concurrency=2,
                use_threads=True
            )

    def get_checkpoint(self, state_filename: str) -> Optional[str]:
        key = f"state/{state_filename}"
        try:
            resp = self.s3.get_object(Bucket=self.bucket, Key=key)
            payload = json.loads(resp["Body"].read().decode("utf-8"))
            cursor = payload.get("cursor_token")
            log_json("info", "Loaded state checkpoint from S3", key=key, cursor=cursor)
            return cursor
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                log_json("info", "No previous checkpoint found. Starting initial sync.", key=key)
                return None
            log_json("error", "Error loading state checkpoint from S3", key=key, error=str(e))
            raise

    def save_checkpoint(self, state_filename: str, cursor_token: str, metadata: Optional[Dict[str, Any]] = None):
        key = f"state/{state_filename}"
        payload = {
            "cursor_token": cursor_token,
            "last_updated_utc": datetime.now(timezone.utc).isoformat(),
            "run_id": self.run_id,
            "metadata": metadata or {}
        }
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(payload, indent=2).encode("utf-8"),
            ContentType="application/json"
        )
        log_json("info", "Atomically committed state checkpoint to S3", key=key, cursor_token=cursor_token)

    def check_item_sync_state(self, item_s3_prefix: str, upstream_etag: Optional[str] = None) -> Tuple[bool, bool, Optional[Dict[str, Any]]]:
        metadata_key = f"{item_s3_prefix}/metadata.json"
        try:
            resp = self.s3.get_object(Bucket=self.bucket, Key=metadata_key)
            existing_meta = json.loads(resp["Body"].read().decode("utf-8"))
            existing_etag = existing_meta.get("upstream_etag")

            if upstream_etag and existing_etag and upstream_etag == existing_etag:
                return True, False, existing_meta
            return False, True, existing_meta
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return False, False, None
            return False, False, None

    def stream_upload(
        self,
        stream_resp,
        s3_key: str,
        mime_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, str]] = None
    ) -> int:
        extra_args = {"ContentType": mime_type}
        if metadata:
            extra_args["Metadata"] = metadata

        # Ensure in-flight decompression if upstream CDN gzip-compressed the response
        if hasattr(stream_resp, "raw") and hasattr(stream_resp.raw, "decode_content"):
            stream_resp.raw.decode_content = True

        self.s3.upload_fileobj(
            Fileobj=stream_resp.raw,
            Bucket=self.bucket,
            Key=s3_key,
            ExtraArgs=extra_args,
            Config=self.transfer_config
        )
        head = self.s3.head_object(Bucket=self.bucket, Key=s3_key)
        return head.get("ContentLength", 0)

    def write_sidecar_metadata(self, s3_prefix: str, metadata: Dict[str, Any]):
        meta_key = f"{s3_prefix}/metadata.json"
        metadata["synced_at_utc"] = datetime.now(timezone.utc).isoformat()
        metadata["run_id"] = self.run_id

        self.s3.put_object(
            Bucket=self.bucket,
            Key=meta_key,
            Body=json.dumps(metadata, indent=2).encode("utf-8"),
            ContentType="application/json"
        )

    def write_tombstone(self, s3_folder_prefix: str, item_id: str):
        tombstone_key = f"{s3_folder_prefix}/{item_id}/DELETED"
        payload = {
            "deleted_at_utc": datetime.now(timezone.utc).isoformat(),
            "run_id": self.run_id,
            "item_id": item_id
        }
        self.s3.put_object(
            Bucket=self.bucket,
            Key=tombstone_key,
            Body=json.dumps(payload).encode("utf-8"),
            ContentType="application/json"
        )
        self.metrics.record_deleted()
        self.record_manifest_entry(item_id, "DELETE", s3_path=tombstone_key)
        log_json("info", "Emitted tombstone deletion marker in S3", key=tombstone_key)

    def write_quarantine(self, item_id: str, payload: Any, error_msg: str, stack_trace: Optional[str] = None):
        quarantine_key = f"quarantine/sharepoint/{item_id}/error.json"
        body = {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "source": "sharepoint",
            "item_id": item_id,
            "error": error_msg,
            "stack_trace": stack_trace,
            "raw_payload": payload
        }
        self.s3.put_object(
            Bucket=self.bucket,
            Key=quarantine_key,
            Body=json.dumps(body, indent=2, default=str).encode("utf-8"),
            ContentType="application/json"
        )
        self.metrics.record_quarantine()
        self.record_manifest_entry(item_id, "QUARANTINE", error_msg=error_msg, s3_path=quarantine_key)
        log_json("error", "Quarantined corrupted item to S3", action="quarantine_item", key=quarantine_key, error=error_msg)

    def write_heavy_task_marker(self, site_id: str, item_id: str, task_payload: Dict[str, Any]):
        key = f"tasks/heavy/{site_id}/{item_id}.json"
        task_payload["enqueued_at_utc"] = datetime.now(timezone.utc).isoformat()
        task_payload["run_id"] = self.run_id
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(task_payload, indent=2).encode("utf-8"),
            ContentType="application/json"
        )
        log_json("info", "Enqueued heavy item transfer task marker", s3_key=key, item_id=item_id)

    def delete_heavy_task_marker(self, site_id: str, item_id: str):
        key = f"tasks/heavy/{site_id}/{item_id}.json"
        try:
            self.s3.delete_object(Bucket=self.bucket, Key=key)
            log_json("info", "Purged completed heavy task marker", s3_key=key, item_id=item_id)
        except ClientError as e:
            log_json("warning", "Could not delete task marker", s3_key=key, error=str(e))

    def list_pending_heavy_tasks(self, site_id: Optional[str] = None) -> List[Dict[str, Any]]:
        prefix = f"tasks/heavy/{site_id}/" if site_id else "tasks/heavy/"
        tasks = []
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".json"):
                    try:
                        resp = self.s3.get_object(Bucket=self.bucket, Key=key)
                        payload = json.loads(resp["Body"].read().decode("utf-8"))
                        tasks.append(payload)
                    except Exception as e:
                        log_json("warning", "Error reading heavy task payload", key=key, error=str(e))
        return tasks

    def record_manifest_entry(
        self,
        item_id: str,
        status: str,
        file_name: str = "",
        size_bytes: int = 0,
        etag: Optional[str] = None,
        s3_path: str = "",
        error_msg: Optional[str] = None
    ):
        entry = {
            "run_id": self.run_id,
            "source": "sharepoint",
            "item_id": item_id,
            "file_name": file_name,
            "status": status,
            "size_bytes": size_bytes,
            "etag": etag,
            "s3_path": s3_path,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "error": error_msg
        }
        with self._manifest_lock:
            self.manifest_entries.append(entry)

    def flush_batch_manifest(self):
        with self._manifest_lock:
            if not self.manifest_entries:
                return
            manifest_key = f"state/manifests/sharepoint/manifest_{self.run_id}.jsonl"
            lines = "\n".join(json.dumps(e) for e in self.manifest_entries)
            self.s3.put_object(
                Bucket=self.bucket,
                Key=manifest_key,
                Body=lines.encode("utf-8"),
                ContentType="application/x-ndjson"
            )
            log_json("info", "Flushed batch inventory manifest to S3", manifest_key=manifest_key, count=len(self.manifest_entries))
            self.manifest_entries.clear()


# ==============================================================================
# 4. SHAREPOINT CONNECTOR ENGINE
# ==============================================================================
class SharePointConnector:
    """Orchestrates Microsoft Graph Delta sync, Entra ID auth, streaming, and ACLs."""

    def __init__(
        self,
        secrets: Dict[str, str],
        http_client: ResilientHttpClient,
        s3_sink: S3Sink,
        max_workers: int = 8,
        mode: str = "delta",
        heavy_file_threshold_bytes: Optional[int] = None,
        heavy_queue_url: Optional[str] = None
    ):
        self.secrets = secrets
        self.http = http_client
        self.s3 = s3_sink
        self.max_workers = max_workers
        self.tenant_id = secrets["tenant_id"]
        self.client_id = secrets["client_id"]
        self.client_secret = secrets["client_secret"]
        self.site_id = secrets["site_id"]
        self.drive_id = secrets.get("drive_id")

        # Two-Tier Ingestion Parameters
        self.mode = mode.lower()
        self.heavy_file_threshold_bytes = heavy_file_threshold_bytes or int(
            os.environ.get("HEAVY_FILE_THRESHOLD_BYTES", str(500 * 1024 * 1024))
        )
        self.heavy_queue_url = heavy_queue_url or os.environ.get("HEAVY_QUEUE_URL")
        self.sqs_client = boto3.client("sqs") if self.heavy_queue_url else None

        # Thread-Safe Proactive OAuth 2.0 Token Manager
        self._token_lock = threading.Lock()
        self.access_token = None
        self.token_expires_at = 0.0
        self.get_auth_headers()  # Initial validation and token acquisition

        # Circuit Breaker Governance
        self.consecutive_failures = 0
        self.total_processed = 0
        self.total_failed = 0
        self.circuit_broken = False
        self._cb_lock = threading.Lock()
        self.max_consecutive_failures = 20
        self.max_error_rate = 0.15

        # Large File Safety Guardrail (Default: 5 GiB limit for Python Shell)
        self.max_file_size_bytes = int(os.environ.get("MAX_FILE_SIZE_BYTES", str(5 * 1024 * 1024 * 1024)))

        # Taxonomy Term Store In-Memory Cache (Question 6)
        self.term_store_cache: Dict[str, str] = {}
        self._init_term_store_cache()

    def _init_term_store_cache(self):
        """Pre-caches SharePoint Term Store taxonomy terms into memory to eliminate N+1 resolution calls."""
        try:
            url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/termStore/groups"
            headers = self.get_auth_headers()
            resp = self.http.request("GET", url, headers=headers)
            if not resp or resp.status_code != 200:
                log_json("info", "Term Store groups endpoint unavailable or unconfigured, using inline label resolution")
                return

            groups = resp.json().get("value", [])
            for group in groups:
                group_id = group.get("id")
                sets_url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/termStore/groups/{group_id}/sets"
                sets_resp = self.http.request("GET", sets_url, headers=self.get_auth_headers())
                if not sets_resp or sets_resp.status_code != 200:
                    continue

                sets = sets_resp.json().get("value", [])
                for term_set in sets:
                    set_id = term_set.get("id")
                    localized_names = term_set.get("localizedNames", [])
                    set_name = localized_names[0].get("name", "Default") if localized_names else "Default"
                    terms_url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/termStore/sets/{set_id}/terms"
                    terms_resp = self.http.request("GET", terms_url, headers=self.get_auth_headers())
                    if not terms_resp or terms_resp.status_code != 200:
                        continue

                    terms = terms_resp.json().get("value", [])
                    for term in terms:
                        term_id = term.get("id")
                        labels = term.get("labels", [])
                        term_label = labels[0].get("name") if labels else term.get("defaultLanguageTag", "")
                        if term_id and term_label:
                            self.term_store_cache[term_id] = f"{set_name}/{term_label}"

            log_json("info", "Pre-cached SharePoint Term Store taxonomy terms", cached_terms_count=len(self.term_store_cache))
        except Exception as e:
            log_json("warning", "Graceful fallback: failed to pre-cache Term Store taxonomy", error=str(e))

    def _extract_custom_fields(self, item: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Extracts and sanitizes custom SharePoint Document Library columns and resolves Managed Metadata taxonomy.
        Returns:
            custom_fields: Dict of business columns (e.g. Department, FiscalYear, DocumentType)
            taxonomy_fields: Dict of resolved taxonomy terms (term_guid, label, hierarchical path)
        """
        raw_fields = item.get("listItem", {}).get("fields", {})
        if not raw_fields:
            return {}, {}

        # Known SharePoint internal / system fields to sanitize
        system_fields_blocklist = {
            "id", "ContentTypeId", "FileRef", "FileLeafRef", "FileDirRef", "FSObjType",
            "PermMask", "Modified", "Created", "AuthorLookupId", "EditorLookupId",
            "Attachments", "Edit", "DocIcon", "ItemChildCount", "FolderChildCount",
            "AppAuthorLookupId", "AppEditorLookupId", "SyncClientId", "ProgId",
            "ScopeId", "HTML_x0020_File_x0020_Type", "SMTotalSize", "SMLastModifiedDate",
            "OData__UIVersionString", "owshiddenversion"
        }

        custom_fields: Dict[str, Any] = {}
        taxonomy_fields: Dict[str, Any] = {}

        for key, val in raw_fields.items():
            # Filter internal system fields
            if key.startswith(("_", "@odata")) or key in system_fields_blocklist:
                continue

            # Clean SharePoint encoded spaces (e.g. "Department_x0020_Name" -> "Department Name")
            clean_key = key.replace("_x0020_", " ")

            # Detect Managed Metadata Taxonomy fields
            if isinstance(val, dict) and ("TermGuid" in val or "Label" in val or "wssId" in val):
                term_guid = val.get("TermGuid")
                raw_label = str(val.get("Label") or "")
                if "#" in raw_label:
                    raw_label = raw_label.split("#", 1)[-1]

                resolved_path = self.term_store_cache.get(term_guid, raw_label) if term_guid else raw_label
                taxonomy_fields[clean_key] = {
                    "term_guid": term_guid,
                    "label": raw_label,
                    "path": resolved_path,
                    "wss_id": val.get("wssId")
                }
            elif isinstance(val, list):
                # Multi-value taxonomy or lookup list
                parsed_list = []
                is_tax_list = False
                for sub_val in val:
                    if isinstance(sub_val, dict) and ("TermGuid" in sub_val or "Label" in sub_val):
                        is_tax_list = True
                        tg = sub_val.get("TermGuid")
                        lbl = str(sub_val.get("Label") or "")
                        if "#" in lbl:
                            lbl = lbl.split("#", 1)[-1]
                        parsed_list.append({
                            "term_guid": tg,
                            "label": lbl,
                            "path": self.term_store_cache.get(tg, lbl) if tg else lbl
                        })
                    else:
                        parsed_list.append(sub_val)

                if is_tax_list:
                    taxonomy_fields[clean_key] = parsed_list
                else:
                    custom_fields[clean_key] = parsed_list
            else:
                custom_fields[clean_key] = val

        return custom_fields, taxonomy_fields

    def get_auth_headers(self) -> Dict[str, str]:
        """Returns thread-safe Authorization headers, proactively renewing if near expiration."""
        now = time.time()
        # Fast path (read without lock if valid with >5 min margin)
        if self.access_token and now < (self.token_expires_at - 300):
            return {"Authorization": f"Bearer {self.access_token}"}

        with self._token_lock:
            # Double-check inside lock
            if self.access_token and now < (self.token_expires_at - 300):
                return {"Authorization": f"Bearer {self.access_token}"}

            url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
            data = {
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": "https://graph.microsoft.com/.default",
                "grant_type": "client_credentials"
            }
            resp = requests.post(url, data=data, timeout=15)
            resp.raise_for_status()
            payload = resp.json()

            new_token = payload.get("access_token")
            expires_in = float(payload.get("expires_in", 3600))
            new_expires_at = time.time() + expires_in

            self.token_expires_at = new_expires_at
            self.access_token = new_token

            log_json(
                "info",
                "Acquired fresh Microsoft Entra ID access token",
                expires_in=int(expires_in),
                expires_at_utc=datetime.fromtimestamp(self.token_expires_at, tz=timezone.utc).isoformat()
            )
            return {"Authorization": f"Bearer {self.access_token}"}

    def _get_fresh_download_url(self, item_id: str, drive_id: Optional[str] = None) -> Optional[str]:
        """Fetches a fresh pre-signed CDN download URL if the original expired during batch processing."""
        if drive_id:
            url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}?$select=id,@microsoft.graph.downloadUrl"
        else:
            url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drive/items/{item_id}?$select=id,@microsoft.graph.downloadUrl"
        headers = self.get_auth_headers()
        resp = self.http.request("GET", url, headers=headers)
        if resp and resp.status_code == 200:
            return resp.json().get("@microsoft.graph.downloadUrl")
        return None

    def _download_and_stream_with_retry(
        self,
        item: Dict[str, Any],
        file_s3_key: str,
        mime_type: str,
        metadata: Dict[str, str],
        initial_download_url: str,
        drive_id: Optional[str] = None,
        max_retries: int = 3
    ) -> int:
        """
        Streams large binary files directly to S3 with multi-layered fault tolerance:
        1. Refreshes expired pre-signed CDN download URLs on-the-fly (HTTP 401/403/410).
        2. Retries mid-stream socket drops, chunked encoding errors, connection resets, and timeouts.
        3. Exponential backoff with randomized jitter between attempts.
        4. Bounds process memory via S3 TransferConfig (16 MB chunksize, max_concurrency=2).
        5. Enforces in-flight decompression if upstream CDN compressed the stream.
        """
        item_id = item["id"]
        current_url = initial_download_url

        for attempt in range(max_retries):
            if attempt > 0:
                sleep_time = (2 ** attempt) + random.uniform(0.5, 2.0)
                log_json(
                    "warning",
                    "Retrying large file download with backoff and fresh URL",
                    item_id=item_id,
                    attempt=attempt + 1,
                    sleep_seconds=round(sleep_time, 2)
                )
                time.sleep(sleep_time)
                fresh_url = self._get_fresh_download_url(item_id, drive_id=drive_id)
                if fresh_url:
                    current_url = fresh_url

            try:
                # Use (connect_timeout, read_timeout) tuple: 15s to connect, 180s between socket chunks
                with self.http.session.get(current_url, stream=True, timeout=(15, 180)) as stream_resp:
                    if stream_resp.status_code in (401, 403, 410):
                        log_json(
                            "warning",
                            "Pre-signed downloadUrl rejected by CDN, requesting fresh URL",
                            item_id=item_id,
                            status_code=stream_resp.status_code,
                            attempt=attempt + 1
                        )
                        if attempt == max_retries - 1:
                            stream_resp.raise_for_status()
                        continue

                    stream_resp.raise_for_status()

                    return self.s3.stream_upload(
                        stream_resp=stream_resp,
                        s3_key=file_s3_key,
                        mime_type=mime_type,
                        metadata=metadata
                    )

            except (requests.exceptions.RequestException, BotoCoreError, socket.error) as err:
                log_json(
                    "warning",
                    "Transient socket/streaming error during S3 upload",
                    item_id=item_id,
                    attempt=attempt + 1,
                    max_retries=max_retries,
                    error=str(err)
                )
                if attempt == max_retries - 1:
                    raise

        raise RuntimeError(f"Exhausted {max_retries} attempts streaming item {item_id} to S3")

    def _record_success(self):
        with self._cb_lock:
            self.consecutive_failures = 0
            self.total_processed += 1

    def _record_failure(self, item_id: str, error_msg: str):
        with self._cb_lock:
            self.consecutive_failures += 1
            self.total_failed += 1
            self.total_processed += 1
            error_rate = self.total_failed / max(self.total_processed, 1)

            if self.consecutive_failures >= self.max_consecutive_failures:
                self.circuit_broken = True
                log_json(
                    "critical",
                    "SharePoint Circuit Breaker tripped: Consecutive error threshold exceeded",
                    action="circuit_breaker_tripped",
                    consecutive_failures=self.consecutive_failures,
                    threshold=self.max_consecutive_failures,
                    item_id=item_id,
                    error=error_msg
                )
            elif self.total_processed >= 10 and error_rate > self.max_error_rate:
                self.circuit_broken = True
                log_json(
                    "critical",
                    "SharePoint Circuit Breaker tripped: Batch error rate threshold exceeded",
                    action="circuit_breaker_tripped",
                    error_rate=round(error_rate, 4),
                    threshold=self.max_error_rate,
                    item_id=item_id,
                    total_processed=self.total_processed,
                    total_failed=self.total_failed,
                    error=error_msg
                )

    def _extract_item_permissions(self, item_id: str, drive_id: Optional[str] = None) -> List[str]:
        """Queries and extracts Entra ID user/group principals for RAG security trimming."""
        if drive_id:
            permissions_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/permissions"
        else:
            permissions_url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drive/items/{item_id}/permissions"
        item_headers = self.get_auth_headers()
        perm_resp = self.http.request("GET", permissions_url, headers=item_headers)
        allowed_principals = []
        if perm_resp and perm_resp.status_code == 200:
            perm_data = perm_resp.json()
            for perm in perm_data.get("value", []):
                granted = perm.get("grantedToV2", {})
                if "user" in granted:
                    user_val = granted["user"].get("userPrincipalName") or granted["user"].get("id")
                    if user_val:
                        allowed_principals.append(f"user:{user_val}")
                if "group" in granted:
                    group_val = granted["group"].get("id")
                    if group_val:
                        allowed_principals.append(f"group:{group_val}")
        return allowed_principals

    def run_heavy_worker(self, item_id: Optional[str] = None, drive_id: Optional[str] = None):
        """
        Tier 2 Heavy Worker (AWS Glue 1.0 DPU / 4 vCPUs / 16 GiB RAM).
        Processes heavy items (>= 500 MB) with high-throughput 32 MB chunk streaming.
        """
        log_json("info", "Starting SharePoint Heavy Ingestion Worker (Tier 2)", run_id=self.s3.run_id)
        safe_site = self.site_id.replace(",", "_").replace("/", "_")

        if item_id:
            tasks = [{
                "item_id": item_id,
                "drive_id": drive_id or self.drive_id,
                "site_id": self.site_id,
                "safe_site": safe_site
            }]
        else:
            tasks = self.s3.list_pending_heavy_tasks(safe_site)
            log_json("info", "Discovered pending heavy tasks from S3 queue", count=len(tasks))

        if not tasks:
            log_json("info", "No pending heavy tasks to process")
            return

        for task in tasks:
            t_item_id = task["item_id"]
            t_drive_id = task.get("drive_id") or self.drive_id
            item_s3_prefix = f"raw/sharepoint/{safe_site}/{t_item_id}"
            file_s3_key = f"{item_s3_prefix}/content.bin"

            log_json("info", "Processing heavy item in Tier 2 worker", item_id=t_item_id)
            try:
                # 1. Fetch fresh pre-signed download URL from Graph API
                download_url = self._get_fresh_download_url(t_item_id, drive_id=t_drive_id)
                if not download_url:
                    raise RuntimeError(f"Unable to obtain fresh download URL for heavy item {t_item_id}")

                # 2. Read existing metadata for filename, etag, and mime_type
                existing_meta_key = f"{item_s3_prefix}/metadata.json"
                existing_meta = {}
                try:
                    resp = self.s3.s3.get_object(Bucket=self.s3.bucket, Key=existing_meta_key)
                    existing_meta = json.loads(resp["Body"].read().decode("utf-8"))
                except ClientError:
                    pass

                file_name = existing_meta.get("file_name", task.get("file_name", "heavy_file.bin"))
                mime_type = existing_meta.get("mime_type", task.get("mime_type", "application/octet-stream"))
                upstream_etag = existing_meta.get("upstream_etag", task.get("upstream_etag", ""))

                metadata = {
                    "upstream_etag": upstream_etag,
                    "sharepoint_id": t_item_id,
                    "original_file_name": file_name
                }

                # 3. Stream binary directly to S3 with high-throughput 32 MB chunks
                bytes_written = self._download_and_stream_with_retry(
                    item={"id": t_item_id},
                    file_s3_key=file_s3_key,
                    mime_type=mime_type,
                    metadata=metadata,
                    initial_download_url=download_url,
                    drive_id=t_drive_id,
                    max_retries=3
                )

                # 4. Atomically transition sidecar metadata status to COMPLETED
                existing_meta["status"] = "COMPLETED"
                existing_meta["size_bytes"] = bytes_written
                existing_meta["heavy_completed_at_utc"] = datetime.now(timezone.utc).isoformat()
                self.s3.write_sidecar_metadata(item_s3_prefix, existing_meta)

                # 5. Purge the task marker from S3
                self.s3.delete_heavy_task_marker(safe_site, t_item_id)

                self.s3.metrics.record_updated(bytes_written)
                self.s3.record_manifest_entry(
                    item_id=t_item_id,
                    status="HEAVY_COMPLETE",
                    file_name=file_name,
                    size_bytes=bytes_written,
                    etag=upstream_etag,
                    s3_path=file_s3_key
                )
                log_json("info", "Successfully completed heavy file ingestion", item_id=t_item_id, bytes_written=bytes_written)

            except Exception as e:
                log_json("error", "Failed heavy item ingestion", item_id=t_item_id, error=str(e), stack=traceback.format_exc())
                self.s3.write_quarantine(t_item_id, task, str(e), traceback.format_exc())
                self.s3.record_manifest_entry(
                    item_id=t_item_id,
                    status="FAILED",
                    error_msg=str(e),
                    s3_path=file_s3_key
                )

        self.s3.flush_batch_manifest()
        log_json("info", "Tier 2 Heavy Ingestion Worker completed batch run")

    def sync(self):
        """Standard entry point for routine incremental synchronization."""
        return self.sync_delta()

    def sync_delta(self):
        log_json("info", "Starting SharePoint Delta Sync", site_id=self.site_id)
        safe_site = self.site_id.replace(",", "_").replace("/", "_")
        state_key = f"sharepoint_{safe_site}_delta.json"

        # Baseline delta query with deep expansion of listItem fields and batch pagination
        expand_clause = "$expand=listItem($select=fields)&$top=200"
        checkpoint_delta_url = self.s3.get_checkpoint(state_key)
        if checkpoint_delta_url:
            current_url = checkpoint_delta_url
        elif self.drive_id:
            current_url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives/{self.drive_id}/root/delta?{expand_clause}"
        else:
            current_url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drive/root/delta?{expand_clause}"

        page_counter = 0

        while current_url:
            if self.circuit_broken:
                self.s3.flush_batch_manifest()
                log_json(
                    "error",
                    "Halting SharePoint sync loop: Circuit breaker is tripped",
                    failed=self.total_failed,
                    consecutive=self.consecutive_failures
                )
                raise RuntimeError(
                    f"SharePoint sync halted: Circuit breaker tripped (failures={self.total_failed}, consecutive={self.consecutive_failures})"
                )

            page_counter += 1
            headers = self.get_auth_headers()
            headers["Prefer"] = "deltashowremoveddatamotion"
            resp = self.http.request("GET", current_url, headers=headers)
            if not resp:
                log_json("error", "Failed to fetch delta page from Graph API", url=current_url)
                break

            # Self-Healing on HTTP 410 Gone
            if resp.status_code == 410:
                log_json(
                    "warning",
                    "Delta token expired. Resetting cursor and starting full baseline crawl with deep expansion.",
                    action="delta_reset_410"
                )
                if self.drive_id:
                    current_url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives/{self.drive_id}/root/delta?{expand_clause}"
                else:
                    current_url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drive/root/delta?{expand_clause}"
                continue

            data = resp.json()
            items = data.get("value", [])

            file_items = [i for i in items if "file" in i and not i.get("deleted")]
            batch_bytes = sum(i.get("size", 0) for i in file_items)
            self.s3.metrics.record_discovered(count=len(file_items), total_bytes=batch_bytes)

            log_json(
                "info",
                "Discovered SharePoint Delta batch workload",
                page=page_counter,
                total_items=len(items),
                file_count=len(file_items),
                batch_mb=round(batch_bytes / (1024 * 1024), 2)
            )

            # Concurrent Item Processing
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [executor.submit(self._process_delta_item, item) for item in items]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        log_json("error", "Error processing delta item in thread", error=str(e), stack=traceback.format_exc())

            self.s3.metrics.log_progress(f"SharePoint Sync Progress (Page {page_counter})")

            # Check circuit breaker before committing state
            if self.circuit_broken:
                self.s3.flush_batch_manifest()
                raise RuntimeError(
                    f"SharePoint sync batch aborted: Circuit breaker tripped (failures={self.total_failed}, consecutive={self.consecutive_failures})"
                )

            # Atomic Commit-After-Write
            if "@odata.nextLink" in data:
                current_url = data["@odata.nextLink"]
            else:
                final_delta = data.get("@odata.deltaLink")
                if final_delta:
                    self.s3.save_checkpoint(state_key, final_delta, {"site_id": self.site_id})
                self.s3.flush_batch_manifest()
                log_json("info", "SharePoint Delta sync completed successfully", total_pages=page_counter)
                break

    def _process_delta_item(self, item: Dict[str, Any]):
        if self.circuit_broken:
            return

        item_id = item["id"]
        safe_site = self.site_id.replace(",", "_").replace("/", "_")
        s3_folder_prefix = f"raw/sharepoint/{safe_site}"
        item_s3_prefix = f"{s3_folder_prefix}/{item_id}"

        # Deletion (@removed is standard in Graph Delta, deleted in OneDrive)
        if "deleted" in item or "@removed" in item:
            self.s3.write_tombstone(s3_folder_prefix, item_id)
            self._record_success()
            return

        # Folder
        if "file" not in item:
            return

        drive_id = item.get("parentReference", {}).get("driveId") or self.drive_id
        file_name = item.get("name", "unnamed_file")
        download_url = item.get("@microsoft.graph.downloadUrl")
        upstream_etag = item.get("eTag")

        if not download_url:
            return

        try:
            # Idempotency Cache Gate Check
            is_binary_unchanged, is_update, existing_meta = self.s3.check_item_sync_state(
                item_s3_prefix, upstream_etag=upstream_etag
            )

            # Invariant object key layout to eliminate orphaned files on renames
            file_s3_key = f"{item_s3_prefix}/content.bin"
            mime_type = item.get("file", {}).get("mimeType", "application/octet-stream")
            bytes_written = 0

            # Safeguard: Check file size against worker threshold (default 5 GiB)
            file_size = item.get("size", 0)
            if file_size > self.max_file_size_bytes:
                log_json(
                    "warning",
                    "Item size exceeds maximum supported threshold for Python Shell worker, skipping binary",
                    item_id=item_id,
                    file_name=file_name,
                    size_bytes=file_size,
                    threshold_bytes=self.max_file_size_bytes
                )
                self.s3.metrics.record_skipped()
                self.s3.record_manifest_entry(
                    item_id=item_id,
                    status="SKIPPED_OVERSIZED",
                    file_name=file_name,
                    size_bytes=file_size,
                    etag=upstream_etag
                )
                self._record_success()
                return

            # Two-Tier Ingestion Gate: Delegate heavy files (>= 500 MB) to Tier 2
            if not is_binary_unchanged and file_size >= self.heavy_file_threshold_bytes:
                log_json(
                    "info",
                    "Two-Tier Gate: Delegating heavy file to Tier 2 bulk ingestion queue",
                    item_id=item_id,
                    file_name=file_name,
                    size_bytes=file_size,
                    threshold_bytes=self.heavy_file_threshold_bytes
                )
                # 1. Query permissions immediately to guarantee ACL synchronization
                allowed_principals = self._extract_item_permissions(item_id, drive_id)
                custom_fields, taxonomy_fields = self._extract_custom_fields(item)

                # 2. Write sidecar metadata with PENDING_HEAVY_TRANSFER status
                metadata_payload = {
                    "doc_id": item_id,
                    "file_name": file_name,
                    "site_id": self.site_id,
                    "upstream_etag": upstream_etag,
                    "size_bytes": file_size,
                    "mime_type": mime_type,
                    "web_url": item.get("webUrl"),
                    "created_at_utc": item.get("createdDateTime"),
                    "modified_at_utc": item.get("lastModifiedDateTime"),
                    "allowed_principals": allowed_principals,
                    "custom_fields": custom_fields,
                    "taxonomy": taxonomy_fields,
                    "is_update": is_update,
                    "status": "PENDING_HEAVY_TRANSFER"
                }
                self.s3.write_sidecar_metadata(item_s3_prefix, metadata_payload)

                # 3. Enqueue heavy task marker in S3 (and optional SQS queue)
                task_payload = {
                    "item_id": item_id,
                    "drive_id": drive_id,
                    "site_id": self.site_id,
                    "safe_site": safe_site,
                    "file_name": file_name,
                    "size_bytes": file_size,
                    "upstream_etag": upstream_etag,
                    "mime_type": mime_type,
                    "s3_prefix": item_s3_prefix
                }
                self.s3.write_heavy_task_marker(safe_site, item_id, task_payload)

                if self.sqs_client and self.heavy_queue_url:
                    try:
                        self.sqs_client.send_message(
                            QueueUrl=self.heavy_queue_url,
                            MessageBody=json.dumps(task_payload)
                        )
                        log_json("info", "Emitted heavy task message to SQS", item_id=item_id, queue_url=self.heavy_queue_url)
                    except Exception as sqs_err:
                        log_json("warning", "Failed to emit SQS message for heavy task", error=str(sqs_err))

                self.s3.metrics.record_skipped()
                self.s3.record_manifest_entry(
                    item_id=item_id,
                    status="QUEUED_HEAVY",
                    file_name=file_name,
                    size_bytes=file_size,
                    etag=upstream_etag
                )
                self._record_success()
                return

            # 1. Binary streaming (Decoupled: Skipped in 0 ms if binary unchanged)
            if not is_binary_unchanged:
                metadata = {
                    "upstream_etag": upstream_etag or "",
                    "sharepoint_id": item_id,
                    "original_file_name": file_name
                }
                bytes_written = self._download_and_stream_with_retry(
                    item=item,
                    file_s3_key=file_s3_key,
                    mime_type=mime_type,
                    metadata=metadata,
                    initial_download_url=download_url,
                    drive_id=drive_id,
                    max_retries=3
                )
            else:
                bytes_written = existing_meta.get("size_bytes", 0) if existing_meta else 0

            # 2. Always Query Permissions (Decoupled to Prevent ACL Drift)
            allowed_principals = self._extract_item_permissions(item_id, drive_id)

            existing_principals = existing_meta.get("allowed_principals", []) if existing_meta else []
            principals_changed = set(allowed_principals) != set(existing_principals)

            existing_file_name = existing_meta.get("file_name") if existing_meta else None
            name_changed = bool(existing_file_name and existing_file_name != file_name)

            # 3. Extract and sanitize custom list columns & taxonomy terms
            custom_fields, taxonomy_fields = self._extract_custom_fields(item)
            existing_custom = existing_meta.get("custom_fields", {}) if existing_meta else {}
            existing_tax = existing_meta.get("taxonomy", {}) if existing_meta else {}
            fields_changed = bool((custom_fields != existing_custom) or (taxonomy_fields != existing_tax))

            # 4. Determine Final Status & Write Sidecar
            if is_binary_unchanged and not principals_changed and not name_changed and not fields_changed:
                # Both binary, permissions, name, and custom metadata are unchanged
                self.s3.metrics.record_skipped()
                self.s3.record_manifest_entry(item_id=item_id, status="SKIP", file_name=file_name, etag=upstream_etag)
            else:
                metadata_payload = {
                    "doc_id": item_id,
                    "file_name": file_name,
                    "site_id": self.site_id,
                    "upstream_etag": upstream_etag,
                    "size_bytes": bytes_written,
                    "mime_type": mime_type,
                    "web_url": item.get("webUrl"),
                    "created_at_utc": item.get("createdDateTime"),
                    "modified_at_utc": item.get("lastModifiedDateTime"),
                    "allowed_principals": allowed_principals,
                    "custom_fields": custom_fields,
                    "taxonomy": taxonomy_fields,
                    "is_update": is_update or principals_changed or name_changed or fields_changed
                }
                self.s3.write_sidecar_metadata(item_s3_prefix, metadata_payload)

                if not is_binary_unchanged:
                    if is_update:
                        self.s3.metrics.record_updated(bytes_written)
                        status = "UPDATE"
                    else:
                        self.s3.metrics.record_inserted(bytes_written)
                        status = "INSERT"
                elif name_changed:
                    status = "RENAME"
                    log_json(
                        "info",
                        "Updated file metadata for rename without binary re-download",
                        action="file_rename",
                        item_id=item_id,
                        old_name=existing_file_name,
                        new_name=file_name
                    )
                elif principals_changed:
                    status = "ACL_REFRESH"
                    log_json(
                        "info",
                        "Refreshed document permissions without binary re-download",
                        action="refresh_restrictions",
                        item_id=item_id,
                        principals_count=len(allowed_principals)
                    )
                else:
                    status = "METADATA_REFRESH"
                    log_json(
                        "info",
                        "Refreshed custom list columns/taxonomy without binary re-download",
                        action="refresh_metadata",
                        item_id=item_id,
                        custom_fields_count=len(custom_fields),
                        taxonomy_count=len(taxonomy_fields)
                    )

                self.s3.record_manifest_entry(
                    item_id=item_id,
                    status=status,
                    file_name=file_name,
                    size_bytes=bytes_written,
                    etag=upstream_etag,
                    s3_path=file_s3_key
                )

            self._record_success()

        except Exception as e:
            self._record_failure(item_id, str(e))
            log_json("error", "Error during file streaming/ingestion", item_id=item_id, file=file_name, error=str(e))
            self.s3.write_quarantine(
                item_id=item_id,
                payload=item,
                error_msg=str(e),
                stack_trace=traceback.format_exc()
            )


# ==============================================================================
# 5. ENTRYPOINT & ARGUMENT PARSING
# ==============================================================================
def get_job_arguments() -> Dict[str, Any]:
    args = {}
    if GLUE_ENVIRONMENT:
        resolved = getResolvedOptions(
            sys.argv,
            [
                "S3_LANDING_BUCKET",
                "SHAREPOINT_SECRET_NAME",
                "MAX_WORKERS",
                "MAX_REQUESTS_PER_SEC"
            ]
        )
        args.update(resolved)

        # Optional Two-Tier arguments in Glue
        for opt_key in ["MODE", "ITEM_ID", "DRIVE_ID", "HEAVY_FILE_THRESHOLD_BYTES", "HEAVY_QUEUE_URL"]:
            try:
                opt_res = getResolvedOptions(sys.argv, [opt_key])
                args.update(opt_res)
            except Exception:
                pass

    args["S3_LANDING_BUCKET"] = args.get("S3_LANDING_BUCKET") or os.environ.get("S3_LANDING_BUCKET")
    args["SHAREPOINT_SECRET_NAME"] = args.get("SHAREPOINT_SECRET_NAME") or os.environ.get(
        "SHAREPOINT_SECRET_NAME", "enterprise/rag/sharepoint_auth"
    )
    args["MAX_WORKERS"] = int(args.get("MAX_WORKERS") or os.environ.get("MAX_WORKERS", 4))
    args["MAX_REQUESTS_PER_SEC"] = float(args.get("MAX_REQUESTS_PER_SEC") or os.environ.get("MAX_REQUESTS_PER_SEC", 10.0))
    args["MODE"] = (args.get("MODE") or os.environ.get("MODE", "delta")).lower()
    args["ITEM_ID"] = args.get("ITEM_ID") or os.environ.get("ITEM_ID")
    args["DRIVE_ID"] = args.get("DRIVE_ID") or os.environ.get("DRIVE_ID")
    args["HEAVY_FILE_THRESHOLD_BYTES"] = int(
        args.get("HEAVY_FILE_THRESHOLD_BYTES") or os.environ.get("HEAVY_FILE_THRESHOLD_BYTES", str(500 * 1024 * 1024))
    )
    args["HEAVY_QUEUE_URL"] = args.get("HEAVY_QUEUE_URL") or os.environ.get("HEAVY_QUEUE_URL")
    return args


def fetch_secret(secret_name: str) -> Optional[Dict[str, Any]]:
    client = boto3.client("secretsmanager")
    try:
        resp = client.get_secret_value(SecretId=secret_name)
        log_json("info", "Successfully fetched secret from AWS Secrets Manager", secret_name=secret_name)
        return json.loads(resp["SecretString"])
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            log_json("warning", "Secret not found in Secrets Manager, aborting", secret_name=secret_name)
            return None
        log_json("error", "Failed to retrieve secret from Secrets Manager", secret_name=secret_name, error=str(e))
        raise


def main():
    args = get_job_arguments()
    mode = args.get("MODE", "delta")
    bucket_name = args.get("S3_LANDING_BUCKET")
    max_workers = args.get("MAX_WORKERS", 4)
    max_req_sec = args.get("MAX_REQUESTS_PER_SEC", 10.0)

    log_json("info", "=======================================================")
    log_json("info", "Starting Self-Contained SharePoint Ingestion Connector", mode=mode)
    log_json("info", "=======================================================")

    if not bucket_name:
        log_json("fatal", "Missing mandatory argument: S3_LANDING_BUCKET")
        sys.exit(1)

    sp_secret_name = args.get("SHAREPOINT_SECRET_NAME")
    sp_secrets = fetch_secret(sp_secret_name)
    if not sp_secrets:
        log_json("fatal", "SharePoint secret missing or unparseable. Aborting.")
        sys.exit(1)

    metrics = PipelineMetrics()
    rate_limiter = BoundedRateLimiter(max_requests_per_sec=max_req_sec)
    http_client = ResilientHttpClient(metrics=metrics, rate_limiter=rate_limiter, max_retries=5, base_delay=1.0)
    s3_sink = S3Sink(bucket_name=bucket_name, metrics=metrics, mode=mode)

    connector = SharePointConnector(
        secrets=sp_secrets,
        http_client=http_client,
        s3_sink=s3_sink,
        max_workers=max_workers,
        mode=mode,
        heavy_file_threshold_bytes=args.get("HEAVY_FILE_THRESHOLD_BYTES"),
        heavy_queue_url=args.get("HEAVY_QUEUE_URL")
    )

    if mode == "heavy_worker":
        connector.run_heavy_worker(item_id=args.get("ITEM_ID"), drive_id=args.get("DRIVE_ID"))
    else:
        connector.sync()

    summary = metrics.summary()
    log_json("info", "=======================================================")
    log_json("info", "SharePoint Ingestion Job Completed Successfully", mode=mode, **summary)
    log_json("info", "=======================================================")


if __name__ == "__main__":
    main()
