"""
Amazon S3 storage sink with zero-RAM multipart streaming, idempotency gates, and manifests.
"""

from datetime import datetime, timezone
import json
import threading
from typing import Any, Dict, List, Optional, Tuple

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

from .models import ManifestRecord
from .telemetry import PipelineMetrics, log_json


class S3Sink:
    """Manages deterministic S3 writes, ETag cache gates, zero-RAM streams, and task manifests."""

    def __init__(
        self,
        bucket_name: str,
        metrics: PipelineMetrics,
        run_id: Optional[str] = None,
        mode: str = "delta"
    ):
        self.s3 = boto3.client("s3")
        self.bucket = bucket_name
        self.metrics = metrics
        self.run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        self.manifest_entries: List[Dict[str, Any]] = []
        self._manifest_lock = threading.Lock()
        self.mode = mode.lower()

        # Dynamic TransferConfig tuning based on execution tier
        if self.mode == "heavy_worker":
            self.chunk_size = 32 * 1024 * 1024  # 32 MB multipart chunksize
            self.max_concurrency = 4
        else:
            self.chunk_size = 16 * 1024 * 1024  # 16 MB chunksize (Tier 1 fast lane)
            self.max_concurrency = 2

        self.transfer_config = TransferConfig(
            multipart_threshold=self.chunk_size,
            multipart_chunksize=self.chunk_size,
            max_concurrency=self.max_concurrency,
            use_threads=True
        )

    def check_item_sync_state(
        self,
        s3_prefix: str,
        upstream_etag: Optional[str]
    ) -> Tuple[bool, bool, Optional[Dict[str, Any]]]:
        """
        Evaluates whether binary download can be skipped via ETag comparison.
        Returns:
            is_binary_unchanged: True if ETag matches existing metadata.json
            is_update: True if item existed in S3 previously
            existing_meta: Previous sidecar metadata payload if found
        """
        meta_key = f"{s3_prefix}/metadata.json"
        try:
            resp = self.s3.get_object(Bucket=self.bucket, Key=meta_key)
            existing_meta = json.loads(resp["Body"].read().decode("utf-8"))
            cached_etag = existing_meta.get("upstream_etag")
            if cached_etag and upstream_etag and cached_etag == upstream_etag:
                return True, True, existing_meta
            return False, True, existing_meta
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return False, False, None
            log_json("warning", "Failed to retrieve existing metadata from S3", s3_key=meta_key, error=str(e))
            return False, False, None

    def stream_binary_to_s3(
        self,
        response_stream,
        s3_key: str,
        mime_type: str,
        metadata: Dict[str, str]
    ) -> int:
        """Streams binary response directly from socket into S3 with zero local RAM accumulation."""
        extra_args = {
            "ContentType": mime_type,
            "Metadata": metadata
        }
        self.s3.upload_fileobj(
            Fileobj=response_stream,
            Bucket=self.bucket,
            Key=s3_key,
            ExtraArgs=extra_args,
            Config=self.transfer_config
        )
        head = self.s3.head_object(Bucket=self.bucket, Key=s3_key)
        return head.get("ContentLength", 0)

    def write_sidecar_metadata(self, s3_prefix: str, metadata: Dict[str, Any]):
        """Writes JSON sidecar metadata.json adjacent to content.bin."""
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
        """Writes deletion tombstone marker to S3."""
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

    def get_delta_token(self, safe_site: str) -> Optional[str]:
        """Reads persisted delta token from S3 state prefix."""
        token_key = f"state/delta/sharepoint/{safe_site}/delta_token.json"
        try:
            resp = self.s3.get_object(Bucket=self.bucket, Key=token_key)
            data = json.loads(resp["Body"].read().decode("utf-8"))
            return data.get("delta_link")
        except ClientError:
            return None

    def save_delta_token(self, safe_site: str, delta_link: str):
        """Persists latest delta link to S3 state prefix."""
        token_key = f"state/delta/sharepoint/{safe_site}/delta_token.json"
        payload = {
            "delta_link": delta_link,
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            "run_id": self.run_id
        }
        self.s3.put_object(
            Bucket=self.bucket,
            Key=token_key,
            Body=json.dumps(payload, indent=2).encode("utf-8"),
            ContentType="application/json"
        )

    def record_manifest_entry(
        self,
        item_id: str,
        status: str,
        file_name: str,
        size_bytes: int = 0,
        etag: Optional[str] = None,
        s3_path: Optional[str] = None
    ):
        """Appends manifest record and flushes in batches."""
        record = ManifestRecord(
            item_id=item_id,
            status=status,
            file_name=file_name,
            size_bytes=size_bytes,
            etag=etag,
            s3_path=s3_path
        )
        with self._manifest_lock:
            self.manifest_entries.append(record.to_dict())
            if len(self.manifest_entries) >= 1000:
                self.flush_manifest()

    def flush_manifest(self):
        """Flushes buffered manifest entries to an S3 ndjson file."""
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

    def write_heavy_task_marker(self, safe_site: str, item_id: str, task_payload: Dict[str, Any]):
        """Writes S3 task marker queued for Tier 2 worker processing."""
        task_key = f"tasks/heavy/{safe_site}/{item_id}.json"
        self.s3.put_object(
            Bucket=self.bucket,
            Key=task_key,
            Body=json.dumps(task_payload, indent=2).encode("utf-8"),
            ContentType="application/json"
        )

    def delete_heavy_task_marker(self, safe_site: str, item_id: str):
        """Deletes S3 task marker upon Tier 2 worker completion."""
        task_key = f"tasks/heavy/{safe_site}/{item_id}.json"
        try:
            self.s3.delete_object(Bucket=self.bucket, Key=task_key)
        except ClientError as e:
            log_json("warning", "Failed to delete S3 task marker", key=task_key, error=str(e))

    def list_pending_heavy_tasks(self, safe_site: str) -> List[Dict[str, Any]]:
        """Scans S3 task prefix for pending heavy tasks."""
        prefix = f"tasks/heavy/{safe_site}/"
        tasks: List[Dict[str, Any]] = []
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".json"):
                    try:
                        resp = self.s3.get_object(Bucket=self.bucket, Key=key)
                        tasks.append(json.loads(resp["Body"].read().decode("utf-8")))
                    except Exception as e:
                        log_json("warning", "Failed to read task marker", key=key, error=str(e))
        return tasks
