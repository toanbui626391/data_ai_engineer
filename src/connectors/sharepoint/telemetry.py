"""
Structured JSON logging and thread-safe pipeline metrics.
"""

import json
import logging
import threading
import time
from typing import Any, Dict

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
    """Thread-safe telemetry accumulator for pipeline execution metrics."""

    def __init__(self):
        self._lock = threading.Lock()
        self.inserted_items = 0
        self.inserted_bytes = 0
        self.updated_items = 0
        self.updated_bytes = 0
        self.deleted_items = 0
        self.skipped_items = 0
        self.failed_items = 0
        self.retries_429 = 0
        self.circuit_breaker_tripped = False
        self.start_time = time.time()

    def record_inserted(self, bytes_transferred: int):
        with self._lock:
            self.inserted_items += 1
            self.inserted_bytes += bytes_transferred

    def record_updated(self, bytes_transferred: int):
        with self._lock:
            self.updated_items += 1
            self.updated_bytes += bytes_transferred

    def record_deleted(self):
        with self._lock:
            self.deleted_items += 1

    def record_skipped(self):
        with self._lock:
            self.skipped_items += 1

    def record_failed(self):
        with self._lock:
            self.failed_items += 1

    def record_retry_429(self):
        with self._lock:
            self.retries_429 += 1

    def record_circuit_breaker_tripped(self):
        with self._lock:
            self.circuit_breaker_tripped = True

    def summary(self) -> Dict[str, Any]:
        with self._lock:
            elapsed = max(time.time() - self.start_time, 0.001)
            total_items = self.inserted_items + self.updated_items + self.deleted_items + self.skipped_items + self.failed_items
            total_bytes = self.inserted_bytes + self.updated_bytes
            mb_transferred = total_bytes / (1024 * 1024)

            return {
                "total_items_processed": total_items,
                "inserted_items": self.inserted_items,
                "updated_items": self.updated_items,
                "deleted_items": self.deleted_items,
                "skipped_items": self.skipped_items,
                "failed_items": self.failed_items,
                "circuit_breaker_tripped": self.circuit_breaker_tripped,
                "total_bytes_transferred": total_bytes,
                "total_mb_transferred": round(mb_transferred, 2),
                "throughput_items_sec": round(total_items / elapsed, 2),
                "throughput_mb_sec": round(mb_transferred / elapsed, 2),
                "rate_limit_retries": self.retries_429,
                "duration_seconds": round(elapsed, 2)
            }
