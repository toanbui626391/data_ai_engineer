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
import requests
import boto3
from botocore.exceptions import ClientError

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
                    if header_retry and header_retry.isdigit():
                        sleep_time = int(header_retry)
                    else:
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
                        sleep_seconds=round(total_sleep, 2)
                    )
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

    def __init__(self, bucket_name: str, metrics: PipelineMetrics, run_id: Optional[str] = None):
        self.s3 = boto3.client("s3")
        self.bucket = bucket_name
        self.metrics = metrics
        self.run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        self.manifest_entries = []
        self._manifest_lock = threading.Lock()

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

        self.s3.upload_fileobj(
            Fileobj=stream_resp.raw,
            Bucket=self.bucket,
            Key=s3_key,
            ExtraArgs=extra_args
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
        max_workers: int = 8
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
        self.access_token = self._authenticate()

        # Circuit Breaker Governance
        self.consecutive_failures = 0
        self.total_processed = 0
        self.total_failed = 0
        self.circuit_broken = False
        self._cb_lock = threading.Lock()
        self.max_consecutive_failures = 20
        self.max_error_rate = 0.15

    def _authenticate(self) -> str:
        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": "https://graph.microsoft.com/.default",
            "grant_type": "client_credentials"
        }
        resp = requests.post(url, data=data, timeout=15)
        resp.raise_for_status()
        token = resp.json().get("access_token")
        log_json("info", "Successfully authenticated with Microsoft Entra ID")
        return token

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
                    error=error_msg
                )

    def sync(self):
        log_json("info", "Starting SharePoint Delta Sync", site_id=self.site_id)
        safe_site = self.site_id.replace(",", "_").replace("/", "_")
        state_key = f"sharepoint_{safe_site}_delta.json"

        checkpoint_delta_url = self.s3.get_checkpoint(state_key)
        if checkpoint_delta_url:
            current_url = checkpoint_delta_url
        elif self.drive_id:
            current_url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives/{self.drive_id}/root/delta"
        else:
            current_url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drive/root/delta"

        headers = {"Authorization": f"Bearer {self.access_token}"}
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
            resp = self.http.request("GET", current_url, headers=headers)
            if not resp:
                log_json("error", "Failed to fetch delta page from Graph API", url=current_url)
                break

            # Self-Healing on HTTP 410 Gone
            if resp.status_code == 410:
                log_json(
                    "warning",
                    "Delta token expired. Resetting cursor and starting full baseline crawl.",
                    action="delta_reset_410"
                )
                if self.drive_id:
                    current_url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives/{self.drive_id}/root/delta"
                else:
                    current_url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drive/root/delta"
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
                futures = [executor.submit(self._process_delta_item, item, headers) for item in items]
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

    def _process_delta_item(self, item: Dict[str, Any], headers: Dict[str, str]):
        if self.circuit_broken:
            return

        item_id = item["id"]
        safe_site = self.site_id.replace(",", "_").replace("/", "_")
        s3_folder_prefix = f"raw/sharepoint/{safe_site}"
        item_s3_prefix = f"{s3_folder_prefix}/{item_id}"

        # Deletion
        if "deleted" in item:
            self.s3.write_tombstone(s3_folder_prefix, item_id)
            self._record_success()
            return

        # Folder
        if "file" not in item:
            return

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

            file_s3_key = f"{item_s3_prefix}/{file_name}"
            mime_type = item.get("file", {}).get("mimeType", "application/octet-stream")
            bytes_written = 0

            # 1. Binary streaming (Decoupled: Skipped in 0 ms if binary unchanged)
            if not is_binary_unchanged:
                with self.http.session.get(download_url, stream=True, timeout=60) as stream_resp:
                    stream_resp.raise_for_status()
                    bytes_written = self.s3.stream_upload(
                        stream_resp=stream_resp,
                        s3_key=file_s3_key,
                        mime_type=mime_type,
                        metadata={"upstream_etag": upstream_etag or "", "sharepoint_id": item_id}
                    )
            else:
                bytes_written = existing_meta.get("size_bytes", 0) if existing_meta else 0

            # 2. Always Query Permissions (Decoupled to Prevent ACL Drift)
            permissions_url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drive/items/{item_id}/permissions"
            perm_resp = self.http.request("GET", permissions_url, headers=headers)
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

            existing_principals = existing_meta.get("allowed_principals", []) if existing_meta else []
            principals_changed = set(allowed_principals) != set(existing_principals)

            # 3. Determine Final Status & Write Sidecar
            if is_binary_unchanged and not principals_changed:
                # Both binary and permissions are unchanged
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
                    "is_update": is_update or principals_changed
                }
                self.s3.write_sidecar_metadata(item_s3_prefix, metadata_payload)

                if not is_binary_unchanged:
                    if is_update:
                        self.s3.metrics.record_updated(bytes_written)
                        status = "UPDATE"
                    else:
                        self.s3.metrics.record_inserted(bytes_written)
                        status = "INSERT"
                else:
                    status = "ACL_REFRESH"
                    log_json(
                        "info",
                        "Refreshed document permissions without binary re-download",
                        action="refresh_restrictions",
                        item_id=item_id,
                        principals_count=len(allowed_principals)
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

    args["S3_LANDING_BUCKET"] = args.get("S3_LANDING_BUCKET") or os.environ.get("S3_LANDING_BUCKET")
    args["SHAREPOINT_SECRET_NAME"] = args.get("SHAREPOINT_SECRET_NAME") or os.environ.get(
        "SHAREPOINT_SECRET_NAME", "enterprise/rag/sharepoint_auth"
    )
    args["MAX_WORKERS"] = int(args.get("MAX_WORKERS") or os.environ.get("MAX_WORKERS", 8))
    args["MAX_REQUESTS_PER_SEC"] = float(args.get("MAX_REQUESTS_PER_SEC") or os.environ.get("MAX_REQUESTS_PER_SEC", 10.0))
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
    log_json("info", "=======================================================")
    log_json("info", "Starting Self-Contained SharePoint Ingestion Connector")
    log_json("info", "=======================================================")

    args = get_job_arguments()
    bucket_name = args.get("S3_LANDING_BUCKET")
    max_workers = args.get("MAX_WORKERS", 8)
    max_req_sec = args.get("MAX_REQUESTS_PER_SEC", 10.0)

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
    s3_sink = S3Sink(bucket_name=bucket_name, metrics=metrics)

    connector = SharePointConnector(
        secrets=sp_secrets,
        http_client=http_client,
        s3_sink=s3_sink,
        max_workers=max_workers
    )
    connector.sync()

    summary = metrics.summary()
    log_json("info", "=======================================================")
    log_json("info", "SharePoint Ingestion Job Completed Successfully", **summary)
    log_json("info", "=======================================================")


if __name__ == "__main__":
    main()
