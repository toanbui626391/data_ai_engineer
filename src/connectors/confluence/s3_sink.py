"""
Amazon S3 Sink: Manages deterministic object storage, version checks, sidecars, and state.
"""

import json
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple
import boto3
from botocore.exceptions import ClientError
from .models import ManifestRecord, TombstoneMarker
from .telemetry import PipelineMetrics, log_json


class S3Sink:
    """Manages deterministic S3 storage, version checks, sidecars, and state."""

    def __init__(self, bucket_name: str, metrics: PipelineMetrics, run_id: Optional[str] = None):
        self.s3 = boto3.client("s3")
        self.bucket = bucket_name
        self.metrics = metrics
        self.run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        self.manifest_entries: List[Dict[str, Any]] = []
        self._manifest_lock = threading.Lock()

    def get_checkpoint(self, state_filename: str) -> Optional[str]:
        """Loads cursor watermark from S3 state prefix."""
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
        """Atomically saves cursor watermark checkpoint to S3 state prefix."""
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

    def get_active_page_ids(self, space_key: str) -> Set[str]:
        """Loads the set of active page IDs from the previous successful run for this space."""
        key = f"state/confluence_{space_key}_active_ids.json"
        try:
            resp = self.s3.get_object(Bucket=self.bucket, Key=key)
            payload = json.loads(resp["Body"].read().decode("utf-8"))
            active_ids = set(payload.get("active_page_ids", []))
            log_json("info", "Loaded previous active page inventory from S3", key=key, count=len(active_ids))
            return active_ids
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                log_json("info", "No previous active page inventory found. Initial sync baseline.", key=key)
                return set()
            log_json("error", "Error loading active page inventory from S3", key=key, error=str(e))
            raise

    def save_active_page_ids(self, space_key: str, active_ids: Set[str]):
        """Atomically commits the current active page inventory for this space to S3."""
        key = f"state/confluence_{space_key}_active_ids.json"
        payload = {
            "space_key": space_key,
            "active_page_ids": sorted(list(active_ids)),
            "total_count": len(active_ids),
            "last_reconciled_utc": datetime.now(timezone.utc).isoformat(),
            "run_id": self.run_id
        }
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(payload, indent=2).encode("utf-8"),
            ContentType="application/json"
        )
        log_json("info", "Atomically committed active page inventory to S3", key=key, count=len(active_ids))

    def check_item_sync_state(self, item_s3_prefix: str, upstream_version: Optional[int] = None) -> Tuple[bool, bool, Optional[Dict[str, Any]]]:
        """Sub-millisecond ETag/version check against existing metadata.json sidecar."""
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
        """Persists sidecar metadata.json with run_id and timestamps."""
        meta_key = f"{s3_prefix}/metadata.json"
        metadata["synced_at_utc"] = datetime.now(timezone.utc).isoformat()
        metadata["run_id"] = self.run_id

        self.s3.put_object(
            Bucket=self.bucket,
            Key=meta_key,
            Body=json.dumps(metadata, indent=2).encode("utf-8"),
            ContentType="application/json"
        )

    def write_raw_content(self, s3_prefix: str, content_bytes: bytes):
        """Writes Bronze raw authoritative storage XHTML (content.xhtml)."""
        content_key = f"{s3_prefix}/content.xhtml"
        self.s3.put_object(
            Bucket=self.bucket,
            Key=content_key,
            Body=content_bytes,
            ContentType="application/xhtml+xml"
        )

    def write_markdown(self, s3_prefix: str, md_text: str):
        """Writes Silver clean Markdown sidecar (content.md)."""
        md_key = f"{s3_prefix}/content.md"
        self.s3.put_object(
            Bucket=self.bucket,
            Key=md_key,
            Body=md_text.encode("utf-8"),
            ContentType="text/markdown"
        )

    def write_tombstone(self, space_key: str, page_id: str, existing_metadata: Optional[Dict[str, Any]] = None):
        """Emits soft tombstone marker in S3, updates metadata.json, and records manifest."""
        now_utc = datetime.now(timezone.utc).isoformat()
        item_s3_prefix = f"raw/confluence/{space_key}/{page_id}"
        tombstone_key = f"{item_s3_prefix}/DELETED"

        marker = TombstoneMarker(
            page_id=page_id,
            space_key=space_key,
            deleted_at_utc=now_utc,
            run_id=self.run_id
        )
        self.s3.put_object(
            Bucket=self.bucket,
            Key=tombstone_key,
            Body=json.dumps(marker.to_dict(), indent=2).encode("utf-8"),
            ContentType="application/json"
        )

        meta = existing_metadata or {}
        meta["page_id"] = page_id
        meta["space_key"] = space_key
        meta["is_deleted"] = True
        meta["status"] = "DELETE"
        meta["deleted_at_utc"] = now_utc
        self.write_sidecar_metadata(item_s3_prefix, meta)

        self.record_manifest_entry(
            item_id=page_id,
            status="DELETE",
            file_name=f"{page_id}.xhtml",
            size_bytes=0,
            s3_path=tombstone_key
        )

        self.metrics.record_deleted()
        log_json(
            "info",
            "Emitted soft-delete tombstone for purged Confluence page",
            action="page_tombstone_emitted",
            space=space_key,
            page_id=page_id,
            tombstone_path=tombstone_key
        )

    def write_quarantine(
        self,
        item_id: str,
        payload: Any,
        error_msg: str,
        stack_trace: Optional[str] = None,
        error_type: Optional[str] = None
    ):
        """Writes forensic dead-letter audit packet to S3 quarantine prefix."""
        quarantine_key = f"quarantine/confluence/{item_id}/error.json"
        body = {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "source": "confluence",
            "item_id": item_id,
            "error": error_msg,
            "error_type": error_type or "UnhandledException",
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
        log_json(
            "error",
            "Quarantined corrupted item to S3",
            action="quarantine_item",
            key=quarantine_key,
            error=error_msg,
            error_type=error_type
        )

    def record_manifest_entry(
        self,
        item_id: str,
        status: str,
        file_name: str = "",
        size_bytes: int = 0,
        s3_path: str = "",
        error_msg: Optional[str] = None
    ):
        """Buffers a manifest entry for atomic batch flush."""
        entry = ManifestRecord(
            run_id=self.run_id,
            source="confluence",
            item_id=item_id,
            file_name=file_name,
            status=status,
            size_bytes=size_bytes,
            s3_path=s3_path,
            timestamp_utc=datetime.now(timezone.utc).isoformat(),
            error=error_msg
        )
        with self._manifest_lock:
            self.manifest_entries.append(entry.to_dict())

    def flush_batch_manifest(self):
        """Atomically flushes buffered manifest entries to S3 as JSONL."""
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
