"""
Core sync orchestration engines for SharePoint delta synchronization and heavy file transfers.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import json
import threading
from typing import Any, Dict, List, Optional

import boto3

from .auth import TokenManager
from .config import ConnectorConfig
from .exceptions import CircuitBreakerTrippedError, DeltaTokenExpiredError
from .extractor import FieldSanitizer, PermissionsExtractor
from .models import ItemMetadata, SyncAction
from .rate_limiter import ResilientHttpClient
from .s3_sink import S3Sink
from .telemetry import log_json


class CircuitBreaker:
    """Monitors consecutive errors and error rate to trip execution and protect downstream pipelines."""

    def __init__(self, max_consecutive_failures: int = 20, max_error_rate: float = 0.15):
        self.max_consecutive_failures = max_consecutive_failures
        self.max_error_rate = max_error_rate
        self.consecutive_failures = 0
        self.total_processed = 0
        self.total_failed = 0
        self.is_tripped = False
        self._lock = threading.Lock()

    def record_success(self):
        with self._lock:
            self.total_processed += 1
            self.consecutive_failures = 0

    def record_failure(self, item_id: str, error_msg: str):
        with self._lock:
            self.total_processed += 1
            self.total_failed += 1
            self.consecutive_failures += 1

            error_rate = self.total_failed / max(self.total_processed, 1)

            if self.consecutive_failures >= self.max_consecutive_failures:
                self.is_tripped = True
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
                self.is_tripped = True
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


class DeltaSyncEngine:
    """Orchestrates Tier 1 fast-lane Microsoft Graph Delta synchronization."""

    def __init__(
        self,
        config: ConnectorConfig,
        token_manager: TokenManager,
        http_client: ResilientHttpClient,
        s3_sink: S3Sink,
        field_sanitizer: FieldSanitizer,
        permissions_extractor: PermissionsExtractor,
        circuit_breaker: CircuitBreaker
    ):
        self.config = config
        self.token_manager = token_manager
        self.http = http_client
        self.s3 = s3_sink
        self.sanitizer = field_sanitizer
        self.permissions = permissions_extractor
        self.circuit_breaker = circuit_breaker
        self.sqs_client = boto3.client("sqs") if self.config.heavy_queue_url else None

    def sync(self):
        """Executes full or incremental delta synchronization loop."""
        safe_site = self.config.safe_site_id
        delta_link = self.s3.get_delta_token(safe_site)

        if delta_link:
            log_json("info", "Resuming SharePoint sync from persisted delta link", safe_site=safe_site)
            url = delta_link
        else:
            log_json("info", "No delta link found; initializing baseline sync with custom field expansion", safe_site=safe_site)
            if self.config.drive_id:
                url = f"https://graph.microsoft.com/v1.0/drives/{self.config.drive_id}/root/delta?$expand=listItem($select=fields)&$top=200"
            else:
                url = f"https://graph.microsoft.com/v1.0/sites/{self.config.site_id}/drive/root/delta?$expand=listItem($select=fields)&$top=200"

        terminal_delta_link = None

        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            while url:
                if self.circuit_breaker.is_tripped:
                    log_json("error", "Halting sync: Circuit Breaker is tripped")
                    raise CircuitBreakerTrippedError("Sync stopped due to tripped circuit breaker")

                headers = self.token_manager.get_auth_headers()
                headers["Prefer"] = "deltashowremoveddatamotion"

                resp = self.http.request("GET", url, headers=headers)
                if not resp:
                    log_json("error", "Null response received during delta pagination. Aborting.", url=url)
                    break

                # HTTP 410: Delta Token Expired -> Restart Full Sync
                if resp.status_code == 410:
                    log_json("warning", "Delta link expired (HTTP 410). Resetting delta token and restarting full crawl.")
                    if self.config.drive_id:
                        url = f"https://graph.microsoft.com/v1.0/drives/{self.config.drive_id}/root/delta?$expand=listItem($select=fields)&$top=200"
                    else:
                        url = f"https://graph.microsoft.com/v1.0/sites/{self.config.site_id}/drive/root/delta?$expand=listItem($select=fields)&$top=200"
                    continue

                if resp.status_code != 200:
                    log_json("error", "Graph Delta request failed", status_code=resp.status_code, body=resp.text)
                    break

                data = resp.json()
                items = data.get("value", [])

                futures = [executor.submit(self._process_delta_item, item) for item in items]
                for f in as_completed(futures):
                    f.result()

                next_url = data.get("@odata.nextLink")
                delta_url = data.get("@odata.deltaLink")

                if next_url:
                    url = next_url
                elif delta_url:
                    terminal_delta_link = delta_url
                    url = None
                else:
                    url = None

        if terminal_delta_link and not self.circuit_breaker.is_tripped:
            self.s3.save_delta_token(safe_site, terminal_delta_link)
            log_json("info", "Successfully persisted new terminal delta link", safe_site=safe_site)

        self.s3.flush_manifest()

    def _get_fresh_download_url(self, item_id: str, drive_id: Optional[str] = None) -> Optional[str]:
        """Fetches a fresh pre-signed download URL from Graph API."""
        target_drive_id = drive_id or self.config.drive_id
        if target_drive_id:
            url = f"https://graph.microsoft.com/v1.0/drives/{target_drive_id}/items/{item_id}"
        else:
            url = f"https://graph.microsoft.com/v1.0/sites/{self.config.site_id}/drive/items/{item_id}"

        headers = self.token_manager.get_auth_headers()
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
        """Streams binary directly to S3 with URL expiration recovery and retry logic."""
        item_id = item["id"]
        current_url = initial_download_url

        for attempt in range(max_retries):
            try:
                resp = self.http.session.get(current_url, stream=True, timeout=(10, 300))

                # If URL expired (HTTP 401/403/404 from storage CDN), re-fetch from Graph
                if resp.status_code in (401, 403, 404) and attempt < max_retries - 1:
                    log_json("warning", "Download URL expired or unauthorized, fetching fresh URL", item_id=item_id, attempt=attempt + 1)
                    fresh_url = self._get_fresh_download_url(item_id, drive_id=drive_id)
                    if fresh_url:
                        current_url = fresh_url
                        continue

                resp.raise_for_status()
                return self.s3.stream_binary_to_s3(
                    response_stream=resp.raw,
                    s3_key=file_s3_key,
                    mime_type=mime_type,
                    metadata=metadata
                )

            except Exception as e:
                log_json("warning", "Error during binary streaming attempt", item_id=item_id, attempt=attempt + 1, error=str(e))
                if attempt == max_retries - 1:
                    raise

        return 0

    def _process_delta_item(self, item: Dict[str, Any]):
        """Processes an individual driveItem delta record."""
        if self.circuit_breaker.is_tripped:
            return

        item_id = item["id"]
        safe_site = self.config.safe_site_id
        s3_folder_prefix = f"raw/sharepoint/{safe_site}"
        item_s3_prefix = f"{s3_folder_prefix}/{item_id}"

        # 1. Deletion / Tombstone
        if "deleted" in item or "@removed" in item:
            self.s3.write_tombstone(s3_folder_prefix, item_id)
            self.circuit_breaker.record_success()
            return

        # 2. Folder skip
        if "file" not in item:
            return

        drive_id = item.get("parentReference", {}).get("driveId") or self.config.drive_id
        file_name = item.get("name", "unnamed_file")
        download_url = item.get("@microsoft.graph.downloadUrl")
        upstream_etag = item.get("eTag")

        if not download_url:
            return

        try:
            is_binary_unchanged, is_update, existing_meta = self.s3.check_item_sync_state(
                item_s3_prefix, upstream_etag=upstream_etag
            )

            file_s3_key = f"{item_s3_prefix}/content.bin"
            mime_type = item.get("file", {}).get("mimeType", "application/octet-stream")
            file_size = item.get("size", 0)
            bytes_written = 0

            # Guardrail: Check oversized threshold (e.g. 5 GiB)
            if file_size > self.config.max_file_size_bytes:
                log_json(
                    "warning",
                    "Item size exceeds maximum supported threshold for worker, skipping binary",
                    item_id=item_id,
                    file_name=file_name,
                    size_bytes=file_size,
                    threshold_bytes=self.config.max_file_size_bytes
                )
                self.s3.metrics.record_skipped()
                self.s3.record_manifest_entry(
                    item_id=item_id,
                    status=SyncAction.SKIPPED_OVERSIZED.value,
                    file_name=file_name,
                    size_bytes=file_size,
                    etag=upstream_etag
                )
                self.circuit_breaker.record_success()
                return

            # Two-Tier Delegation Gate: Heavy files (>= 500 MB)
            if not is_binary_unchanged and file_size >= self.config.heavy_file_threshold_bytes:
                log_json(
                    "info",
                    "Two-Tier Gate: Delegating heavy file to Tier 2 bulk ingestion queue",
                    item_id=item_id,
                    file_name=file_name,
                    size_bytes=file_size,
                    threshold_bytes=self.config.heavy_file_threshold_bytes
                )
                allowed_principals = self.permissions.extract_permissions(item_id, drive_id)
                custom_fields, taxonomy_fields = self.sanitizer.extract_custom_fields(item)

                meta_model = ItemMetadata(
                    doc_id=item_id,
                    file_name=file_name,
                    site_id=self.config.site_id,
                    upstream_etag=upstream_etag,
                    size_bytes=file_size,
                    mime_type=mime_type,
                    web_url=item.get("webUrl"),
                    created_at_utc=item.get("createdDateTime"),
                    modified_at_utc=item.get("lastModifiedDateTime"),
                    allowed_principals=allowed_principals,
                    custom_fields=custom_fields,
                    taxonomy=taxonomy_fields,
                    is_update=is_update,
                    status="PENDING_HEAVY_TRANSFER"
                )
                self.s3.write_sidecar_metadata(item_s3_prefix, meta_model.to_dict())

                task_payload = {
                    "item_id": item_id,
                    "drive_id": drive_id,
                    "site_id": self.config.site_id,
                    "safe_site": safe_site,
                    "file_name": file_name,
                    "size_bytes": file_size,
                    "upstream_etag": upstream_etag,
                    "mime_type": mime_type,
                    "s3_prefix": item_s3_prefix
                }
                self.s3.write_heavy_task_marker(safe_site, item_id, task_payload)

                if self.sqs_client and self.config.heavy_queue_url:
                    try:
                        self.sqs_client.send_message(
                            QueueUrl=self.config.heavy_queue_url,
                            MessageBody=json.dumps(task_payload)
                        )
                        log_json("info", "Emitted heavy task message to SQS", item_id=item_id, queue_url=self.config.heavy_queue_url)
                    except Exception as sqs_err:
                        log_json("warning", "Failed to emit SQS message for heavy task", error=str(sqs_err))

                self.s3.metrics.record_skipped()
                self.s3.record_manifest_entry(
                    item_id=item_id,
                    status=SyncAction.QUEUED_HEAVY.value,
                    file_name=file_name,
                    size_bytes=file_size,
                    etag=upstream_etag
                )
                self.circuit_breaker.record_success()
                return

            # Binary Streaming (Skipped in 0 ms if binary unchanged)
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

            # Always Query Permissions to prevent ACL drift
            allowed_principals = self.permissions.extract_permissions(item_id, drive_id)
            existing_principals = existing_meta.get("allowed_principals", []) if existing_meta else []
            principals_changed = set(allowed_principals) != set(existing_principals)

            existing_file_name = existing_meta.get("file_name") if existing_meta else None
            name_changed = bool(existing_file_name and existing_file_name != file_name)

            # Extract custom columns & taxonomy terms
            custom_fields, taxonomy_fields = self.sanitizer.extract_custom_fields(item)
            existing_custom = existing_meta.get("custom_fields", {}) if existing_meta else {}
            existing_tax = existing_meta.get("taxonomy", {}) if existing_meta else {}
            fields_changed = bool((custom_fields != existing_custom) or (taxonomy_fields != existing_tax))

            # Determine Final Status
            if is_binary_unchanged and not principals_changed and not name_changed and not fields_changed:
                self.s3.metrics.record_skipped()
                self.s3.record_manifest_entry(
                    item_id=item_id,
                    status=SyncAction.SKIP.value,
                    file_name=file_name,
                    etag=upstream_etag
                )
            else:
                meta_payload = ItemMetadata(
                    doc_id=item_id,
                    file_name=file_name,
                    site_id=self.config.site_id,
                    upstream_etag=upstream_etag,
                    size_bytes=bytes_written,
                    mime_type=mime_type,
                    web_url=item.get("webUrl"),
                    created_at_utc=item.get("createdDateTime"),
                    modified_at_utc=item.get("lastModifiedDateTime"),
                    allowed_principals=allowed_principals,
                    custom_fields=custom_fields,
                    taxonomy=taxonomy_fields,
                    is_update=is_update or principals_changed or name_changed or fields_changed
                )
                self.s3.write_sidecar_metadata(item_s3_prefix, meta_payload.to_dict())

                if not is_binary_unchanged:
                    if is_update:
                        self.s3.metrics.record_updated(bytes_written)
                        status = SyncAction.UPDATE.value
                    else:
                        self.s3.metrics.record_inserted(bytes_written)
                        status = SyncAction.INSERT.value
                elif name_changed:
                    status = SyncAction.RENAME.value
                    log_json("info", "Updated file metadata for rename without binary re-download", action="file_rename", item_id=item_id, old_name=existing_file_name, new_name=file_name)
                elif principals_changed:
                    status = SyncAction.ACL_REFRESH.value
                    log_json("info", "Refreshed document permissions without binary re-download", action="refresh_restrictions", item_id=item_id, principals_count=len(allowed_principals))
                else:
                    status = SyncAction.METADATA_REFRESH.value
                    log_json("info", "Refreshed custom list columns/taxonomy without binary re-download", action="refresh_metadata", item_id=item_id, custom_fields_count=len(custom_fields), taxonomy_count=len(taxonomy_fields))

                self.s3.record_manifest_entry(
                    item_id=item_id,
                    status=status,
                    file_name=file_name,
                    size_bytes=bytes_written,
                    etag=upstream_etag,
                    s3_path=file_s3_key
                )

            self.circuit_breaker.record_success()

        except Exception as e:
            self.s3.metrics.record_failed()
            log_json("error", "Failed processing delta item", item_id=item_id, error=str(e))
            self.circuit_breaker.record_failure(item_id=item_id, error_msg=str(e))


class HeavyTransferWorker:
    """Tier 2 Heavy Worker (AWS Glue 1.0 DPU) for high-throughput streaming of large files."""

    def __init__(
        self,
        config: ConnectorConfig,
        token_manager: TokenManager,
        http_client: ResilientHttpClient,
        s3_sink: S3Sink
    ):
        self.config = config
        self.token_manager = token_manager
        self.http = http_client
        self.s3 = s3_sink

    def _get_fresh_download_url(self, item_id: str, drive_id: Optional[str] = None) -> Optional[str]:
        target_drive_id = drive_id or self.config.drive_id
        if target_drive_id:
            url = f"https://graph.microsoft.com/v1.0/drives/{target_drive_id}/items/{item_id}"
        else:
            url = f"https://graph.microsoft.com/v1.0/sites/{self.config.site_id}/drive/items/{item_id}"

        headers = self.token_manager.get_auth_headers()
        resp = self.http.request("GET", url, headers=headers)
        if resp and resp.status_code == 200:
            return resp.json().get("@microsoft.graph.downloadUrl")
        return None

    def run(self, item_id: Optional[str] = None, drive_id: Optional[str] = None):
        """Processes queued heavy items using 32 MB chunk multipart streaming."""
        log_json("info", "Starting SharePoint Heavy Ingestion Worker (Tier 2)", run_id=self.s3.run_id)
        safe_site = self.config.safe_site_id

        if item_id:
            tasks = [{
                "item_id": item_id,
                "drive_id": drive_id or self.config.drive_id,
                "site_id": self.config.site_id,
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
            t_drive_id = task.get("drive_id") or self.config.drive_id
            item_s3_prefix = f"raw/sharepoint/{safe_site}/{t_item_id}"
            file_s3_key = f"{item_s3_prefix}/content.bin"

            log_json("info", "Processing heavy item in Tier 2 worker", item_id=t_item_id)
            try:
                download_url = self._get_fresh_download_url(t_item_id, drive_id=t_drive_id)
                if not download_url:
                    raise RuntimeError(f"Unable to obtain fresh download URL for heavy item {t_item_id}")

                existing_meta_key = f"{item_s3_prefix}/metadata.json"
                existing_meta = {}
                try:
                    resp = self.s3.s3.get_object(Bucket=self.s3.bucket, Key=existing_meta_key)
                    existing_meta = json.loads(resp["Body"].read().decode("utf-8"))
                except Exception:
                    pass

                file_name = existing_meta.get("file_name", task.get("file_name", "unnamed_heavy_file"))
                upstream_etag = existing_meta.get("upstream_etag", task.get("upstream_etag"))
                mime_type = existing_meta.get("mime_type", task.get("mime_type", "application/octet-stream"))

                s3_meta = {
                    "upstream_etag": upstream_etag or "",
                    "sharepoint_id": t_item_id,
                    "original_file_name": file_name
                }

                stream_resp = self.http.session.get(download_url, stream=True, timeout=(15, 1800))
                stream_resp.raise_for_status()

                bytes_written = self.s3.stream_binary_to_s3(
                    response_stream=stream_resp.raw,
                    s3_key=file_s3_key,
                    mime_type=mime_type,
                    metadata=s3_meta
                )

                if existing_meta:
                    existing_meta["status"] = "COMPLETED"
                    existing_meta["size_bytes"] = bytes_written
                    existing_meta["synced_at_utc"] = datetime.now(timezone.utc).isoformat()
                    self.s3.write_sidecar_metadata(item_s3_prefix, existing_meta)

                self.s3.delete_heavy_task_marker(safe_site, t_item_id)
                self.s3.metrics.record_inserted(bytes_written)
                self.s3.record_manifest_entry(
                    item_id=t_item_id,
                    status=SyncAction.HEAVY_COMPLETE.value,
                    file_name=file_name,
                    size_bytes=bytes_written,
                    etag=upstream_etag,
                    s3_path=file_s3_key
                )
                log_json("info", "Successfully completed heavy file ingestion in Tier 2 worker", item_id=t_item_id, bytes_written=bytes_written)

            except Exception as e:
                self.s3.metrics.record_failed()
                log_json("error", "Failed processing heavy file in Tier 2 worker", item_id=t_item_id, error=str(e))

        self.s3.flush_manifest()
