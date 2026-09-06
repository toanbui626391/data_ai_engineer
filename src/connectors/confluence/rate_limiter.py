"""
Token-bucket rate limiter and resilient HTTP client with full randomized jitter.
"""

import random
import threading
import time
from typing import Optional
import requests
from .telemetry import PipelineMetrics, log_json


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
                kwargs.setdefault("timeout", (15, 60))
                resp = self.session.request(method, url, **kwargs)

                if resp.status_code in (200, 201, 204, 206):
                    return resp

                # HTTP 429 (Too Many Requests) or HTTP 503 (Service Unavailable with Retry-After)
                if resp.status_code in (429, 503):
                    if resp.status_code == 429:
                        self.metrics.record_retry_429()

                    header_retry = resp.headers.get("Retry-After")
                    if header_retry and header_retry.isdigit():
                        sleep_time = int(header_retry)
                    else:
                        sleep_time = self.base_delay * (2 ** attempt)

                    # Full Randomized Jitter (Desynchronizes concurrent worker threads)
                    jitter = random.uniform(0.1, 0.5) * sleep_time
                    total_sleep = sleep_time + jitter

                    log_json(
                        "warning",
                        f"Throttled by Atlassian API (HTTP {resp.status_code})",
                        action="rate_limit_429" if resp.status_code == 429 else "service_unavailable_503",
                        status_code=resp.status_code,
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
