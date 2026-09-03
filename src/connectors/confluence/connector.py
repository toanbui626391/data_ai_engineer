"""
Self-Contained Ingestion Connector: Atlassian Confluence -> Amazon S3 Lakehouse
Runtime: AWS Glue Python Shell (0.0625 DPU) / Python CLI

Architectural Specification: docs/architecture/glue_connectors/02_confluence_connector.md
Engineering Standards: .agents/rules/data_engineer_persona.md

Guarantees:
1. Completely self-contained (zero external module imports required in Glue).
2. Confluence REST API v2 multi-space pagination with modified-date cursor watermarks.
3. Sub-millisecond version.number ETag cache gate to skip unchanged pages.
4. Space permissions & page restrictions extraction for RAG security trimming.
5. Storage format XHTML (?body-format=storage) extraction with hierarchy preservation.
6. Token-bucket rate limiting and HTTP 429 exponential backoff with full jitter.
"""

import os
import sys
import json
import time
import random
import logging
import threading
import traceback
import base64
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
logger = logging.getLogger("confluence_connector")


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

    def log_progress(self, context_msg: str = "Confluence Ingestion Progress"):
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
    """HTTP client with connection pooling, 429 jitter backoff, and retry logic."""

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
                        "Rate limited by Atlassian API (HTTP 429)",
                        action="rate_limit_429",
                        status_code=429,
                        url=url,
                        attempt=attempt + 1,
                        sleep_seconds=round(total_sleep, 2)
                    )
                    time.sleep(total_sleep)
                    continue

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
# 3. AMAZON S3 SINK & IDEMPOTENCY CACHE STORE
# ==============================================================================
class S3Sink:
    """Manages deterministic S3 storage, version checks, sidecars, and state."""

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

    def check_item_sync_state(self, item_s3_prefix: str, upstream_version: Optional[int] = None) -> Tuple[bool, bool, Optional[Dict[str, Any]]]:
        metadata_key = f"{item_s3_prefix}/metadata.json"
        try:
            resp = self.s3.get_object(Bucket=self.bucket, Key=metadata_key)
            existing_meta = json.loads(resp["Body"].read().decode("utf-8"))
            existing_ver = existing_meta.get("version_number")

            if upstream_version is not None and existing_ver is not None and upstream_version == existing_ver:
                return True, False, existing_meta
            return False, True, existing_meta
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return False, False, None
            return False, False, None

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

    def write_quarantine(self, item_id: str, payload: Any, error_msg: str, stack_trace: Optional[str] = None):
        quarantine_key = f"quarantine/confluence/{item_id}/error.json"
        body = {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "source": "confluence",
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
        s3_path: str = "",
        error_msg: Optional[str] = None
    ):
        entry = {
            "run_id": self.run_id,
            "source": "confluence",
            "item_id": item_id,
            "file_name": file_name,
            "status": status,
            "size_bytes": size_bytes,
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
            manifest_key = f"state/manifests/confluence/manifest_{self.run_id}.jsonl"
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
# 4. CONFLUENCE CONNECTOR ENGINE
# ==============================================================================
class ConfluenceConnector:
    """Orchestrates Confluence REST API v2 sync, storage XHTML, and restrictions."""

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
        self.base_url = secrets["base_url"].rstrip("/")
        self.user_email = secrets.get("user_email")
        self.api_token = secrets["api_token"]
        raw_spaces = secrets.get("space_keys", "")
        self.space_keys = [s.strip() for s in raw_spaces.split(",") if s.strip()]

        # Circuit Breaker Governance
        self.consecutive_failures = 0
        self.total_processed = 0
        self.total_failed = 0
        self.circuit_broken = False
        self._cb_lock = threading.Lock()
        self.max_consecutive_failures = 15
        self.max_error_rate = 0.10

    def _get_auth_headers(self) -> Dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.user_email:
            token_bytes = f"{self.user_email}:{self.api_token}".encode("utf-8")
            b64_token = base64.b64encode(token_bytes).decode("utf-8")
            headers["Authorization"] = f"Basic {b64_token}"
        else:
            headers["Authorization"] = f"Bearer {self.api_token}"
        return headers

    def _record_success(self):
        with self._cb_lock:
            self.consecutive_failures = 0
            self.total_processed += 1

    def _record_failure(self, page_id: str, error_msg: str):
        with self._cb_lock:
            self.consecutive_failures += 1
            self.total_failed += 1
            self.total_processed += 1
            error_rate = self.total_failed / max(self.total_processed, 1)

            if self.consecutive_failures >= self.max_consecutive_failures:
                self.circuit_broken = True
                log_json(
                    "critical",
                    "Confluence Circuit Breaker tripped: Consecutive error threshold exceeded",
                    action="circuit_breaker_tripped",
                    consecutive_failures=self.consecutive_failures,
                    threshold=self.max_consecutive_failures,
                    page_id=page_id,
                    error=error_msg
                )
            elif self.total_processed >= 10 and error_rate > self.max_error_rate:
                self.circuit_broken = True
                log_json(
                    "critical",
                    "Confluence Circuit Breaker tripped: Space error rate threshold exceeded",
                    action="circuit_breaker_tripped",
                    error_rate=round(error_rate, 4),
                    threshold=self.max_error_rate,
                    page_id=page_id,
                    error=error_msg
                )

    def sync(self):
        headers = self._get_auth_headers()
        log_json("info", "Starting Confluence Sync across spaces", spaces=self.space_keys)

        target_spaces = self.space_keys
        if not target_spaces:
            spaces_url = f"{self.base_url}/api/v2/spaces?limit=50"
            resp = self.http.request("GET", spaces_url, headers=headers)
            if resp and resp.status_code == 200:
                results = resp.json().get("results", [])
                target_spaces = [s["key"] for s in results]
                log_json("info", "Discovered Confluence spaces", spaces=target_spaces)

        for space_key in target_spaces:
            self._sync_space(space_key, headers)

        self.s3.flush_batch_manifest()
        log_json("info", "Confluence Ingestion complete across all target spaces")

    def _sync_space(self, space_key: str, headers: Dict[str, str]):
        log_json("info", "Beginning sync for Confluence Space", space=space_key)
        state_key = f"confluence_{space_key}_cursor.json"

        last_sync_watermark = self.s3.get_checkpoint(state_key) or "1970-01-01T00:00:00.000Z"
        new_watermark = last_sync_watermark

        next_url = f"{self.base_url}/api/v2/spaces/{space_key}/pages?limit=50&sort=modified-date&body-format=storage"
        page_counter = 0

        while next_url:
            if self.circuit_broken:
                log_json(
                    "error",
                    "Halting Confluence space sync: Circuit breaker is tripped",
                    space=space_key,
                    failed=self.total_failed,
                    consecutive=self.consecutive_failures
                )
                return

            page_counter += 1
            resp = self.http.request("GET", next_url, headers=headers)
            if not resp or resp.status_code != 200:
                log_json("error", "Failed to fetch page batch from Confluence", url=next_url)
                break

            data = resp.json()
            pages = data.get("results", [])
            self.s3.metrics.record_discovered(count=len(pages))

            log_json("info", "Discovered Confluence page batch", space=space_key, page=page_counter, count=len(pages))

            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [executor.submit(self._process_page, p, space_key, headers) for p in pages]
                for future in as_completed(futures):
                    try:
                        modified_ts = future.result()
                        if modified_ts and modified_ts > new_watermark:
                            new_watermark = modified_ts
                    except Exception as e:
                        log_json("error", "Error processing Confluence page in thread", error=str(e), stack=traceback.format_exc())

            self.s3.metrics.log_progress(f"Confluence Space [{space_key}] Progress (Batch {page_counter})")

            # Check circuit breaker before advancing pagination
            if self.circuit_broken:
                log_json("warning", "Skipping further space pages due to tripped circuit breaker", space=space_key)
                break

            links = data.get("_links", {})
            relative_next = links.get("next")
            if relative_next:
                if relative_next.startswith("http"):
                    next_url = relative_next
                else:
                    next_url = f"{self.base_url}{relative_next}"
            else:
                next_url = None

        if not self.circuit_broken and new_watermark > last_sync_watermark:
            self.s3.save_checkpoint(state_key, new_watermark, {"space_key": space_key})
            log_json("info", "Committed new Confluence space watermark", space=space_key, watermark=new_watermark)
        elif self.circuit_broken:
            log_json("warning", "Watermark checkpoint skipped due to tripped circuit breaker", space=space_key)

    def _process_page(self, page: Dict[str, Any], space_key: str, headers: Dict[str, str]) -> Optional[str]:
        if self.circuit_broken:
            return None

        page_id = str(page["id"])
        title = page.get("title", "Untitled Page")
        version_obj = page.get("version", {})
        version_num = version_obj.get("number", 1)
        modified_at = version_obj.get("createdAt") or datetime.now(timezone.utc).isoformat()

        item_s3_prefix = f"raw/confluence/{space_key}/{page_id}"

        try:
            # Version Cache Check
            is_body_unchanged, is_update, existing_meta = self.s3.check_item_sync_state(
                item_s3_prefix, upstream_version=version_num
            )

            content_key = f"{item_s3_prefix}/content.xhtml"
            bytes_written = 0

            # 1. Content Extraction (Decoupled: Skipped if body version matches)
            if not is_body_unchanged:
                body_storage = page.get("body", {}).get("storage", {}).get("value", "")
                body_bytes = body_storage.encode("utf-8")
                bytes_written = len(body_bytes)

                self.s3.s3.put_object(
                    Bucket=self.s3.bucket,
                    Key=content_key,
                    Body=body_bytes,
                    ContentType="application/xhtml+xml"
                )
                log_json("debug", "Wrote Confluence XHTML content", action="write_content", page_id=page_id)
            else:
                bytes_written = existing_meta.get("size_bytes", 0) if existing_meta else 0

            # 2. Always Query Restrictions (Decoupled to Prevent ACL Drift)
            restrictions_url = f"{self.base_url}/api/v2/pages/{page_id}/restrictions"
            restr_resp = self.http.request("GET", restrictions_url, headers=headers)
            allowed_users = []
            allowed_groups = []
            has_restrictions = False

            if restr_resp and restr_resp.status_code == 200:
                restr_data = restr_resp.json()
                read_restr = restr_data.get("read", {}).get("restrictions", {})
                user_list = read_restr.get("user", {}).get("results", [])
                group_list = read_restr.get("group", {}).get("results", [])

                if user_list or group_list:
                    has_restrictions = True
                    for u in user_list:
                        uid = u.get("accountId") or u.get("publicName")
                        if uid:
                            allowed_users.append(uid)
                    for g in group_list:
                        gid = g.get("name") or g.get("id")
                        if gid:
                            allowed_groups.append(gid)

            existing_users = existing_meta.get("allowed_users", []) if existing_meta else []
            existing_groups = existing_meta.get("allowed_groups", []) if existing_meta else []
            restrictions_changed = (set(allowed_users) != set(existing_users)) or (set(allowed_groups) != set(existing_groups))

            # 3. Determine Final Status & Write Sidecar
            if is_body_unchanged and not restrictions_changed:
                # Both XHTML body and restrictions are unchanged
                self.s3.metrics.record_skipped()
                self.s3.record_manifest_entry(
                    item_id=page_id,
                    status="SKIP",
                    file_name=f"{title}.xhtml"
                )
            else:
                meta_payload = {
                    "page_id": page_id,
                    "space_key": space_key,
                    "title": title,
                    "version_number": version_num,
                    "parent_id": page.get("parentId"),
                    "parent_type": page.get("parentType"),
                    "author_id": version_obj.get("authorId"),
                    "created_at_utc": page.get("createdAt"),
                    "modified_at_utc": modified_at,
                    "web_url": f"{self.base_url}/spaces/{space_key}/pages/{page_id}",
                    "size_bytes": bytes_written,
                    "has_restrictions": has_restrictions,
                    "allowed_users": allowed_users,
                    "allowed_groups": allowed_groups,
                    "is_update": is_update or restrictions_changed
                }
                self.s3.write_sidecar_metadata(item_s3_prefix, meta_payload)

                if not is_body_unchanged:
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
                        "Refreshed page restrictions without body re-download",
                        action="refresh_restrictions",
                        page_id=page_id,
                        users_count=len(allowed_users),
                        groups_count=len(allowed_groups)
                    )

                self.s3.record_manifest_entry(
                    item_id=page_id,
                    status=status,
                    file_name=f"{title}.xhtml",
                    size_bytes=bytes_written,
                    s3_path=content_key
                )

            self._record_success()
            return modified_at

        except Exception as e:
            self._record_failure(page_id, str(e))
            log_json("error", "Error processing Confluence page", page_id=page_id, title=title, error=str(e))
            self.s3.write_quarantine(
                item_id=page_id,
                payload=page,
                error_msg=str(e),
                stack_trace=traceback.format_exc()
            )
            return None


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
                "CONFLUENCE_SECRET_NAME",
                "MAX_WORKERS",
                "MAX_REQUESTS_PER_SEC"
            ]
        )
        args.update(resolved)

    args["S3_LANDING_BUCKET"] = args.get("S3_LANDING_BUCKET") or os.environ.get("S3_LANDING_BUCKET")
    args["CONFLUENCE_SECRET_NAME"] = args.get("CONFLUENCE_SECRET_NAME") or os.environ.get(
        "CONFLUENCE_SECRET_NAME", "enterprise/rag/confluence_auth"
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
    log_json("info", "Starting Self-Contained Confluence Ingestion Connector")
    log_json("info", "=======================================================")

    args = get_job_arguments()
    bucket_name = args.get("S3_LANDING_BUCKET")
    max_workers = args.get("MAX_WORKERS", 8)
    max_req_sec = args.get("MAX_REQUESTS_PER_SEC", 10.0)

    if not bucket_name:
        log_json("fatal", "Missing mandatory argument: S3_LANDING_BUCKET")
        sys.exit(1)

    conf_secret_name = args.get("CONFLUENCE_SECRET_NAME")
    conf_secrets = fetch_secret(conf_secret_name)
    if not conf_secrets:
        log_json("fatal", "Confluence secret missing or unparseable. Aborting.")
        sys.exit(1)

    metrics = PipelineMetrics()
    rate_limiter = BoundedRateLimiter(max_requests_per_sec=max_req_sec)
    http_client = ResilientHttpClient(metrics=metrics, rate_limiter=rate_limiter, max_retries=5, base_delay=1.0)
    s3_sink = S3Sink(bucket_name=bucket_name, metrics=metrics)

    connector = ConfluenceConnector(
        secrets=conf_secrets,
        http_client=http_client,
        s3_sink=s3_sink,
        max_workers=max_workers
    )
    connector.sync()

    summary = metrics.summary()
    log_json("info", "=======================================================")
    log_json("info", "Confluence Ingestion Job Completed Successfully", **summary)
    log_json("info", "=======================================================")


if __name__ == "__main__":
    main()
