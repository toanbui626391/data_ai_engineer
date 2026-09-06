"""
SharePoint Connector Entry Point & Backwards-Compatible Facade.

This module exposes the unified SharePointConnector interface, coordinates dependency
injection across modular sub-packages, and serves as the primary AWS Glue job entry point.
"""

import os
import sys
from typing import Any, Dict, List, Optional, Tuple

try:
    from .auth import TokenManager
    from .config import ConnectorConfig, fetch_secret, load_config, parse_job_arguments
    from .engine import CircuitBreaker, DeltaSyncEngine, HeavyTransferWorker
    from .exceptions import SharePointConnectorError
    from .extractor import FieldSanitizer, PermissionsExtractor
    from .models import ItemMetadata, ManifestRecord, SyncAction, TaxonomyTerm
    from .rate_limiter import BoundedRateLimiter, ResilientHttpClient
    from .s3_sink import S3Sink
    from .taxonomy import TermStoreTaxonomyResolver
    from .telemetry import PipelineMetrics, log_json
except ImportError:
    # Fallback for direct script execution without -m
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from auth import TokenManager
    from config import ConnectorConfig, fetch_secret, load_config, parse_job_arguments
    from engine import CircuitBreaker, DeltaSyncEngine, HeavyTransferWorker
    from exceptions import SharePointConnectorError
    from extractor import FieldSanitizer, PermissionsExtractor
    from models import ItemMetadata, ManifestRecord, SyncAction, TaxonomyTerm
    from rate_limiter import BoundedRateLimiter, ResilientHttpClient
    from s3_sink import S3Sink
    from taxonomy import TermStoreTaxonomyResolver
    from telemetry import PipelineMetrics, log_json


class SharePointConnector:
    """
    High-level facade orchestrating Microsoft Graph Delta sync, Entra ID authentication,
    streaming, ACL security trimming, and taxonomy resolution.
    
    Provides complete backwards-compatibility for existing Glue workflows and unit tests.
    """

    def __init__(
        self,
        secrets: Dict[str, str],
        http_client: ResilientHttpClient,
        s3_sink: S3Sink,
        max_workers: int = 8,
        mode: str = "delta",
        heavy_file_threshold_bytes: Optional[int] = None,
        heavy_queue_url: Optional[str] = None
    ):
        self.secrets = secrets
        self.http = http_client
        self.s3 = s3_sink
        self.max_workers = max_workers
        self.mode = mode.lower()
        self.tenant_id = secrets["tenant_id"]
        self.client_id = secrets["client_id"]
        self.client_secret = secrets["client_secret"]
        self.site_id = secrets["site_id"]
        self.drive_id = secrets.get("drive_id")

        self.heavy_file_threshold_bytes = heavy_file_threshold_bytes or int(
            os.environ.get("HEAVY_FILE_THRESHOLD_BYTES", str(500 * 1024 * 1024))
        )
        self.heavy_queue_url = heavy_queue_url or os.environ.get("HEAVY_QUEUE_URL")
        self.max_file_size_bytes = int(
            os.environ.get("MAX_FILE_SIZE_BYTES", str(5 * 1024 * 1024 * 1024))
        )

        # 1. Config Object
        self.config = ConnectorConfig(
            s3_landing_bucket=self.s3.bucket,
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
            site_id=self.site_id,
            drive_id=self.drive_id,
            mode=self.mode,
            max_workers=self.max_workers,
            heavy_file_threshold_bytes=self.heavy_file_threshold_bytes,
            max_file_size_bytes=self.max_file_size_bytes,
            heavy_queue_url=self.heavy_queue_url,
            raw_secrets=self.secrets
        )

        # 2. Token Manager
        self.token_manager = TokenManager(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret
        )

        # 3. Circuit Breaker
        self.circuit_breaker = CircuitBreaker(
            max_consecutive_failures=20,
            max_error_rate=0.15
        )

        # 4. Taxonomy Resolver
        self.taxonomy = TermStoreTaxonomyResolver(
            site_id=self.site_id,
            http_client=self.http,
            auth_headers_provider=self.token_manager.get_auth_headers
        )
        self._init_term_store_cache()

        # 5. Extractors
        self.sanitizer = FieldSanitizer(taxonomy_resolver=self.taxonomy)
        self.permissions = PermissionsExtractor(
            site_id=self.site_id,
            http_client=self.http,
            auth_headers_provider=self.token_manager.get_auth_headers
        )

        # 6. Core Engines
        self.delta_engine = DeltaSyncEngine(
            config=self.config,
            token_manager=self.token_manager,
            http_client=self.http,
            s3_sink=self.s3,
            field_sanitizer=self.sanitizer,
            permissions_extractor=self.permissions,
            circuit_breaker=self.circuit_breaker
        )

        self.heavy_worker = HeavyTransferWorker(
            config=self.config,
            token_manager=self.token_manager,
            http_client=self.http,
            s3_sink=self.s3
        )

    # Backwards-compatible properties
    @property
    def term_store_cache(self) -> Dict[str, str]:
        return self.taxonomy.cache

    @term_store_cache.setter
    def term_store_cache(self, value: Dict[str, str]):
        self.taxonomy._cache = value

    @property
    def circuit_broken(self) -> bool:
        return self.circuit_breaker.is_tripped

    @circuit_broken.setter
    def circuit_broken(self, value: bool):
        self.circuit_breaker.is_tripped = value

    def _init_term_store_cache(self):
        """Backwards-compatible alias to initialize Term Store cache."""
        self.taxonomy.initialize()

    def get_auth_headers(self) -> Dict[str, str]:
        """Returns valid authorization headers."""
        return self.token_manager.get_auth_headers()

    def sync(self):
        """Executes full or incremental delta synchronization."""
        self.delta_engine.sync()

    def sync_delta(self):
        """Alias for sync() maintaining backwards compatibility."""
        self.sync()

    def run_heavy_worker(self, item_id: Optional[str] = None, drive_id: Optional[str] = None):
        """Executes Tier 2 bulk ingestion worker."""
        self.heavy_worker.run(item_id=item_id, drive_id=drive_id)

    def _extract_custom_fields(self, item: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        return self.sanitizer.extract_custom_fields(item)

    def _extract_item_permissions(self, item_id: str, drive_id: Optional[str] = None) -> List[str]:
        return self.permissions.extract_permissions(item_id, drive_id)

    def _process_delta_item(self, item: Dict[str, Any]):
        return self.delta_engine._process_delta_item(item)


def main():
    """Main execution entrypoint for AWS Glue and standalone runners."""
    args = parse_job_arguments()
    mode = args.get("MODE", "delta")
    bucket_name = args.get("S3_LANDING_BUCKET")
    max_workers = args.get("MAX_WORKERS", 4)
    max_req_sec = args.get("MAX_REQUESTS_PER_SEC", 10.0)

    log_json("info", "=======================================================")
    log_json("info", "Starting Refactored SharePoint Ingestion Connector", mode=mode)
    log_json("info", "=======================================================")

    if not bucket_name:
        log_json("fatal", "Missing mandatory argument: S3_LANDING_BUCKET")
        sys.exit(1)

    sp_secret_name = args.get("SHAREPOINT_SECRET_NAME", "enterprise/rag/sharepoint_auth")
    sp_secrets = fetch_secret(sp_secret_name)
    if not sp_secrets:
        log_json("fatal", "SharePoint secret missing or unparseable. Aborting.")
        sys.exit(1)

    metrics = PipelineMetrics()
    rate_limiter = BoundedRateLimiter(max_requests_per_sec=max_req_sec)
    http_client = ResilientHttpClient(metrics=metrics, rate_limiter=rate_limiter, max_retries=5, base_delay=1.0)
    s3_sink = S3Sink(bucket_name=bucket_name, metrics=metrics, mode=mode)

    connector = SharePointConnector(
        secrets=sp_secrets,
        http_client=http_client,
        s3_sink=s3_sink,
        max_workers=max_workers,
        mode=mode,
        heavy_file_threshold_bytes=args.get("HEAVY_FILE_THRESHOLD_BYTES"),
        heavy_queue_url=args.get("HEAVY_QUEUE_URL")
    )

    if mode == "heavy_worker":
        connector.run_heavy_worker(item_id=args.get("ITEM_ID"), drive_id=args.get("DRIVE_ID"))
    else:
        connector.sync()

    summary = metrics.summary()
    log_json("info", "=======================================================")
    log_json("info", "SharePoint Ingestion Job Completed Successfully", mode=mode, **summary)
    log_json("info", "=======================================================")


if __name__ == "__main__":
    main()
