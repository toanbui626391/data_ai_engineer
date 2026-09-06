"""
Confluence Ingestion Connector entrypoint and orchestration façade.
Runtime: AWS Glue Python Shell (0.0625 DPU) / Local CLI

Maintains 100% backward compatibility while delegating to modular components.
"""

import sys
from typing import Any, Dict, List, Optional
from .config import ConfluenceConfig, fetch_secret, load_config, parse_job_arguments, GLUE_ENVIRONMENT
from .engine import CircuitBreaker, ConfluenceSyncEngine
from .exceptions import CircuitBreakerTrippedError
from .models import PageMetadata, PageRestrictions, PageSummary, SyncAction
from .rate_limiter import BoundedRateLimiter, ResilientHttpClient
from .s3_sink import S3Sink
from .sanitizer import ConfluenceMacroSanitizer
from .telemetry import PipelineMetrics, log_json


class ConfluenceConnector:
    """
    High-level façade orchestrating Confluence REST API v2 ingestion.
    Provides complete backward compatibility with existing tests and scripts.
    """

    def __init__(
        self,
        secrets: Dict[str, Any],
        http_client: ResilientHttpClient,
        s3_sink: S3Sink,
        max_workers: int = 8
    ):
        self.secrets = secrets
        self.http = http_client
        self.s3 = s3_sink
        self.max_workers = max_workers
        self.macro_sanitizer = ConfluenceMacroSanitizer()
        self.base_url = secrets["base_url"].rstrip("/")
        self.user_email = secrets.get("user_email")
        self.api_token = secrets["api_token"]
        raw_spaces = secrets.get("space_keys", "")
        self.space_keys = [s.strip() for s in raw_spaces.split(",") if s.strip()]

        # Circuit Breaker Governance
        self.circuit_breaker = CircuitBreaker(
            max_consecutive_failures=15,
            max_error_rate=0.10
        )

        # Build typed config
        self.config = ConfluenceConfig(
            base_url=self.base_url,
            api_token=self.api_token,
            user_email=self.user_email,
            space_keys=self.space_keys,
            landing_bucket=s3_sink.bucket,
            max_workers=max_workers,
            max_consecutive_failures=15,
            max_error_rate=0.10
        )

        # Delegate engine
        self.engine = ConfluenceSyncEngine(
            config=self.config,
            http_client=self.http,
            s3_sink=self.s3,
            circuit_breaker=self.circuit_breaker,
            sanitizer=self.macro_sanitizer,
            page_processor=self._process_page
        )

    # Backward-compatible property delegates
    @property
    def max_consecutive_failures(self) -> int:
        return self.circuit_breaker.max_consecutive_failures

    @max_consecutive_failures.setter
    def max_consecutive_failures(self, value: int):
        self.circuit_breaker.max_consecutive_failures = value

    @property
    def max_error_rate(self) -> float:
        return self.circuit_breaker.max_error_rate

    @max_error_rate.setter
    def max_error_rate(self, value: float):
        self.circuit_breaker.max_error_rate = value

    @property
    def consecutive_failures(self) -> int:
        return self.circuit_breaker.consecutive_failures

    @consecutive_failures.setter
    def consecutive_failures(self, value: int):
        self.circuit_breaker.consecutive_failures = value

    @property
    def total_processed(self) -> int:
        return self.circuit_breaker.total_processed

    @total_processed.setter
    def total_processed(self, value: int):
        self.circuit_breaker.total_processed = value

    @property
    def total_failed(self) -> int:
        return self.circuit_breaker.total_failed

    @total_failed.setter
    def total_failed(self, value: int):
        self.circuit_breaker.total_failed = value

    @property
    def circuit_broken(self) -> bool:
        return self.circuit_breaker.circuit_broken

    @circuit_broken.setter
    def circuit_broken(self, value: bool):
        self.circuit_breaker.circuit_broken = value

    def _get_auth_headers(self) -> Dict[str, str]:
        return self.engine.get_auth_headers()

    def _record_success(self):
        self.circuit_breaker.record_success()

    def _record_failure(self, page_id: str, error_msg: str):
        self.circuit_breaker.record_failure(page_id, error_msg)

    def _process_page(self, page: Dict[str, Any], space_key: str, headers: Dict[str, str]) -> Optional[str]:
        return self.engine.process_page(page, space_key, headers)

    def _sync_space(self, space_key: str, headers: Dict[str, str]):
        self.engine.page_processor = self._process_page
        self.engine.sync_space(space_key, headers)

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
            if self.circuit_broken:
                log_json("warning", "Halting further space syncs: Circuit breaker tripped", space=space_key)
                break
            self._sync_space(space_key, headers)

        self.s3.flush_batch_manifest()

        if self.circuit_broken:
            log_json(
                "critical",
                "Confluence Ingestion aborted due to tripped circuit breaker",
                action="circuit_breaker_tripped",
                consecutive_failures=self.consecutive_failures,
                total_failed=self.total_failed,
                total_processed=self.total_processed
            )
            raise RuntimeError(
                f"Confluence Ingestion aborted: Circuit breaker tripped "
                f"(consecutive_failures={self.consecutive_failures}, total_failed={self.total_failed})"
            )

        log_json("info", "Confluence Ingestion complete across all target spaces")


# Backward compatibility aliases for argument parsing
get_job_arguments = parse_job_arguments


def main():
    log_json("info", "=======================================================")
    log_json("info", "Starting Self-Contained Confluence Ingestion Connector")
    log_json("info", "=======================================================")

    args = parse_job_arguments()
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
