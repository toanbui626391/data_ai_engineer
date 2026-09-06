"""
Structured JSON logging and thread-safe pipeline metrics for Confluence connector.
"""

import json
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict


logger = logging.getLogger("confluence_connector")


def log_json(level: str, message: str, **kwargs):
    """Emits structured JSON logs parsable by AWS CloudWatch Metric Filters."""
    payload = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "level": level.upper(),
        "message": message,
        **kwargs
    }
    log_func = getattr(logger, level.lower(), logger.info)
    log_func(json.dumps(payload, default=str))


class PipelineMetrics:
    """Thread-safe telemetry accumulator for Confluence ingestion execution."""

    def __init__(self):
        self._lock = threading.Lock()
        self.discovered_docs = 0
        self.discovered_bytes = 0
        self.downloaded_docs = 0
        self.inserted_docs = 0
        self.updated_docs = 0
        self.skipped_existing = 0
        self.deleted_docs = 0
        self.quarantined_docs = 0
        self.bytes_transferred = 0
        self.retries_429 = 0
        self.start_time = time.time()

    def record_discovered(self, count: int = 1, size_bytes: int = 0):
        with self._lock:
            self.discovered_docs += count
            self.discovered_bytes += size_bytes

    def record_inserted(self, size_bytes: int = 0):
        with self._lock:
            self.inserted_docs += 1
            self.downloaded_docs += 1
            self.bytes_transferred += size_bytes

    def record_updated(self, size_bytes: int = 0):
        with self._lock:
            self.updated_docs += 1
            self.downloaded_docs += 1
            self.bytes_transferred += size_bytes

    def record_skipped(self):
        with self._lock:
            self.skipped_existing += 1

    def record_deleted(self, count: int = 1):
        with self._lock:
            self.deleted_docs += count

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
