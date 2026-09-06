"""
Token-bucket rate limiter and resilient HTTP client with 429 jitter backoff.
"""

import random
import threading
import time
from typing import Optional

import requests
import requests.adapters

from .exceptions import RateLimitExceededError
from .telemetry import PipelineMetrics, log_json


class BoundedRateLimiter:
    """Token-bucket rate limiter to prevent exceeding Microsoft Graph tenant quotas."""

    def __init__(self, max_requests_per_sec: float = 10.0):
        self.rate = max(max_requests_per_sec, 0.1)
        self.capacity = self.rate
        self.tokens = self.rate
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
    """HTTP client with connection pooling, 429 jitter backoff, and transient error recovery."""

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
        """Executes HTTP request with rate limiting and exponential backoff on 429/5xx."""
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
                    time.sleep(total_sleep)
                    continue

                # HTTP 410: Delta link expired
                if resp.status_code == 410:
                    log_json("warning", "Upstream Graph Delta token expired (HTTP 410 Gone)", url=url)
                    return resp

                # HTTP 5xx: Transient Server Error
                if resp.status_code in (500, 502, 503, 504):
                    sleep_time = (self.base_delay * (2 ** attempt)) + random.uniform(0.1, 0.5)
                    log_json(
                        "warning",
                        "Transient server error from Graph API",
                        action="server_error_retry",
                        status_code=resp.status_code,
                        url=url,
                        attempt=attempt + 1,
                        sleep_seconds=round(sleep_time, 2)
                    )
                    time.sleep(sleep_time)
                    continue

                # Non-retriable 4xx
                log_json("error", "Unrecoverable client error from Graph API", status_code=resp.status_code, url=url)
                return resp

            except requests.RequestException as e:
                sleep_time = (self.base_delay * (2 ** attempt)) + random.uniform(0.1, 0.5)
                log_json(
                    "warning",
                    "Network error during Graph API request",
                    action="network_retry",
                    error=str(e),
                    url=url,
                    attempt=attempt + 1,
                    sleep_seconds=round(sleep_time, 2)
                )
                time.sleep(sleep_time)

        log_json("error", "Exhausted maximum retries for Graph API request", url=url, max_retries=self.max_retries)
        return None
