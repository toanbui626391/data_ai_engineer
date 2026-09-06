"""
Confluence synchronization engine and cascading circuit breaker.
"""

import base64
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set
from .config import ConfluenceConfig
from .exceptions import CircuitBreakerTrippedError
from .models import PageMetadata, PageRestrictions, PageSummary, SyncAction
from .rate_limiter import ResilientHttpClient
from .s3_sink import S3Sink
from .sanitizer import ConfluenceMacroSanitizer
from .telemetry import log_json


class CircuitBreaker:
    """Thread-safe two-tier cascading circuit breaker."""

    def __init__(
        self,
        max_consecutive_failures: int = 15,
        max_error_rate: float = 0.10,
        min_processed_for_rate: int = 10
    ):
        self.max_consecutive_failures = max_consecutive_failures
        self.max_error_rate = max_error_rate
        self.min_processed_for_rate = min_processed_for_rate
        self.consecutive_failures = 0
        self.total_processed = 0
        self.total_failed = 0
        self.circuit_broken = False
        self._lock = threading.Lock()

    def record_success(self):
        with self._lock:
            self.consecutive_failures = 0
            self.total_processed += 1

    def record_failure(self, page_id: str, error_msg: str):
        with self._lock:
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
            elif self.total_processed >= self.min_processed_for_rate and error_rate > self.max_error_rate:
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


class ConfluenceSyncEngine:
    """Orchestrates space pagination, two-phase body downloads, and delete reconciliation."""

    def __init__(
        self,
        config: ConfluenceConfig,
        http_client: ResilientHttpClient,
        s3_sink: S3Sink,
        circuit_breaker: Optional[CircuitBreaker] = None,
        sanitizer: Optional[ConfluenceMacroSanitizer] = None,
        page_processor: Optional[Any] = None
    ):
        self.config = config
        self.http = http_client
        self.s3 = s3_sink
        self.circuit_breaker = circuit_breaker or CircuitBreaker(
            max_consecutive_failures=config.max_consecutive_failures,
            max_error_rate=config.max_error_rate
        )
        self.sanitizer = sanitizer or ConfluenceMacroSanitizer()
        self.page_processor = page_processor

    def get_auth_headers(self) -> Dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.config.user_email:
            token_bytes = f"{self.config.user_email}:{self.config.api_token}".encode("utf-8")
            b64_token = base64.b64encode(token_bytes).decode("utf-8")
            headers["Authorization"] = f"Basic {b64_token}"
        else:
            headers["Authorization"] = f"Bearer {self.config.api_token}"
        return headers

    def sync_space(self, space_key: str, headers: Dict[str, str]):
        """Crawls all pages in a single space with watermark check and delete reconciliation."""
        log_json("info", "Beginning sync for Confluence Space", space=space_key)
        state_key = f"confluence_{space_key}_cursor.json"

        last_sync_watermark = self.s3.get_checkpoint(state_key) or "1970-01-01T00:00:00.000Z"
        new_watermark = last_sync_watermark

        next_url = f"{self.config.base_url}/api/v2/spaces/{space_key}/pages?limit={self.config.batch_limit}&sort=modified-date"
        page_counter = 0
        discovered_space_page_ids: Set[str] = set()

        while next_url:
            if self.circuit_breaker.circuit_broken:
                log_json(
                    "error",
                    "Halting Confluence space sync: Circuit breaker is tripped",
                    space=space_key,
                    failed=self.circuit_breaker.total_failed,
                    consecutive=self.circuit_breaker.consecutive_failures
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
            discovered_space_page_ids.update(str(p["id"]) for p in pages if "id" in p)

            log_json("info", "Discovered Confluence page batch", space=space_key, page=page_counter, count=len(pages))

            with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
                processor = self.page_processor or self.process_page
                futures = [executor.submit(processor, p, space_key, headers) for p in pages]
                for future in as_completed(futures):
                    try:
                        modified_ts = future.result()
                        if modified_ts and modified_ts > new_watermark:
                            new_watermark = modified_ts
                    except Exception as e:
                        log_json("error", "Error processing Confluence page in thread", error=str(e), stack=traceback.format_exc())

            self.s3.metrics.log_progress(f"Confluence Space [{space_key}] Progress (Batch {page_counter})")

            if self.circuit_breaker.circuit_broken:
                log_json("warning", "Skipping further space pages due to tripped circuit breaker", space=space_key)
                break

            links = data.get("_links", {})
            relative_next = links.get("next")
            if relative_next:
                if relative_next.startswith("http"):
                    next_url = relative_next
                else:
                    next_url = f"{self.config.base_url}{relative_next}"
            else:
                next_url = None

        # Reconcile hard deletes and commit checkpoints only if crawl completed without circuit breaker trip
        if not self.circuit_breaker.circuit_broken:
            previous_active_ids = self.s3.get_active_page_ids(space_key)
            if previous_active_ids:
                deleted_ids = previous_active_ids - discovered_space_page_ids
                if deleted_ids:
                    log_json(
                        "info",
                        f"Detected {len(deleted_ids)} deleted pages in Confluence space via manifest reconciliation",
                        space=space_key,
                        deleted_count=len(deleted_ids)
                    )
                    for del_id in sorted(deleted_ids):
                        _, _, existing_meta = self.s3.check_item_sync_state(f"raw/confluence/{space_key}/{del_id}")
                        self.s3.write_tombstone(space_key, del_id, existing_metadata=existing_meta)

            self.s3.save_active_page_ids(space_key, discovered_space_page_ids)

            if new_watermark > last_sync_watermark:
                self.s3.save_checkpoint(state_key, new_watermark, {"space_key": space_key})
                log_json("info", "Committed new Confluence space watermark", space=space_key, watermark=new_watermark)
        else:
            log_json("warning", "Skipping delete reconciliation and state checkpoints due to tripped circuit breaker", space=space_key)

    def process_page(self, page: Dict[str, Any], space_key: str, headers: Dict[str, str]) -> Optional[str]:
        """Processes a single Confluence page: ETag check, body fetch, restrictions, sidecar."""
        if self.circuit_breaker.circuit_broken:
            return None

        page_summary = PageSummary.from_api_dict(page, space_key)
        page_id = page_summary.page_id
        title = page_summary.title
        version_num = page_summary.version_number
        modified_at = page_summary.modified_at_utc or datetime.now(timezone.utc).isoformat()
        item_s3_prefix = f"raw/confluence/{space_key}/{page_id}"

        try:
            is_body_unchanged, is_update, existing_meta = self.s3.check_item_sync_state(
                item_s3_prefix, upstream_version=version_num
            )

            content_key = f"{item_s3_prefix}/content.xhtml"
            bytes_written = 0

            # 1. Content Extraction (Phase 2 targeted fetch only if body changed or new)
            if not is_body_unchanged:
                detail_url = f"{self.config.base_url}/api/v2/pages/{page_id}?body-format=storage"
                detail_resp = self.http.request("GET", detail_url, headers=headers)
                if not detail_resp or detail_resp.status_code != 200:
                    status_code = getattr(detail_resp, "status_code", None)
                    raise RuntimeError(f"Failed to fetch storage body for Confluence page {page_id} (Status: {status_code})")

                detail_data = detail_resp.json()
                body_storage = detail_data.get("body", {}).get("storage", {}).get("value", "")
                body_bytes = body_storage.encode("utf-8")
                bytes_written = len(body_bytes)

                # Write Bronze authoritative raw storage XHTML
                self.s3.s3.put_object(
                    Bucket=self.s3.bucket,
                    Key=content_key,
                    Body=body_bytes,
                    ContentType="application/xhtml+xml"
                )
                log_json("debug", "Wrote Confluence XHTML content", action="write_content", page_id=page_id)

                # Write Silver clean Markdown sidecar (Macro Sanitized)
                clean_md = self.sanitizer.sanitize(body_storage)
                md_key = f"{item_s3_prefix}/content.md"
                self.s3.s3.put_object(
                    Bucket=self.s3.bucket,
                    Key=md_key,
                    Body=clean_md.encode("utf-8"),
                    ContentType="text/markdown"
                )
                log_json("debug", "Wrote Confluence clean Markdown sidecar", action="write_markdown", page_id=page_id)
            else:
                bytes_written = existing_meta.get("size_bytes", 0) if existing_meta else 0

            # 2. Always Query Restrictions (Decoupled to Prevent ACL Drift)
            restrictions_url = f"{self.config.base_url}/api/v2/pages/{page_id}/restrictions"
            restr_resp = self.http.request("GET", restrictions_url, headers=headers)
            restrictions = PageRestrictions()

            if restr_resp and restr_resp.status_code == 200:
                restrictions = PageRestrictions.from_api_dict(restr_resp.json())

            existing_users = existing_meta.get("allowed_users", []) if existing_meta else []
            existing_groups = existing_meta.get("allowed_groups", []) if existing_meta else []
            existing_edit_users = existing_meta.get("edit_users", []) if existing_meta else []
            existing_edit_groups = existing_meta.get("edit_groups", []) if existing_meta else []

            restrictions_changed = (
                (set(restrictions.allowed_users) != set(existing_users)) or
                (set(restrictions.allowed_groups) != set(existing_groups)) or
                (set(restrictions.edit_users) != set(existing_edit_users)) or
                (set(restrictions.edit_groups) != set(existing_edit_groups))
            )

            # 3. Determine Final Status & Write Sidecar
            if is_body_unchanged and not restrictions_changed:
                self.s3.metrics.record_skipped()
                self.s3.record_manifest_entry(
                    item_id=page_id,
                    status=SyncAction.SKIP.value,
                    file_name=f"{title}.xhtml"
                )
            else:
                now_utc = datetime.now(timezone.utc).isoformat()
                meta_payload = PageMetadata(
                    page_id=page_id,
                    space_key=space_key,
                    title=title,
                    version_number=version_num,
                    parent_id=page_summary.parent_id,
                    parent_type=page_summary.parent_type,
                    author_id=page_summary.author_id,
                    created_at_utc=page_summary.created_at_utc,
                    modified_at_utc=modified_at,
                    web_url=f"{self.config.base_url}/spaces/{space_key}/pages/{page_id}",
                    size_bytes=bytes_written,
                    has_restrictions=restrictions.has_restrictions,
                    allowed_users=restrictions.allowed_users,
                    allowed_groups=restrictions.allowed_groups,
                    edit_users=restrictions.edit_users,
                    edit_groups=restrictions.edit_groups,
                    is_update=is_update or restrictions_changed,
                    acl_synced_at_utc=now_utc
                )
                self.s3.write_sidecar_metadata(item_s3_prefix, meta_payload.to_dict())

                if not is_body_unchanged:
                    if is_update:
                        self.s3.metrics.record_updated(bytes_written)
                        status = SyncAction.UPDATE.value
                    else:
                        self.s3.metrics.record_inserted(bytes_written)
                        status = SyncAction.INSERT.value
                else:
                    status = SyncAction.ACL_REFRESH.value
                    log_json(
                        "info",
                        "Refreshed page restrictions without body re-download",
                        action="refresh_restrictions",
                        page_id=page_id,
                        users_count=len(restrictions.allowed_users),
                        groups_count=len(restrictions.allowed_groups)
                    )

                self.s3.record_manifest_entry(
                    item_id=page_id,
                    status=status,
                    file_name=f"{title}.xhtml",
                    size_bytes=bytes_written,
                    s3_path=content_key
                )

            self.circuit_breaker.record_success()
            return modified_at

        except Exception as e:
            err_type = type(e).__name__
            self.circuit_breaker.record_failure(page_id, str(e))
            log_json("error", "Error processing Confluence page", page_id=page_id, title=title, error=str(e), error_type=err_type)
            self.s3.write_quarantine(
                item_id=page_id,
                payload=page,
                error_msg=str(e),
                error_type=err_type,
                stack_trace=traceback.format_exc()
            )
            return None
