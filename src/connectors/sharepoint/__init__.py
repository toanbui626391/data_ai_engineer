"""
Enterprise Microsoft SharePoint custom connector for AWS Glue and lakehouse ingestion.
"""

from .auth import TokenManager
from .config import ConnectorConfig, load_config, parse_job_arguments, fetch_secret
from .exceptions import (
    SharePointConnectorError,
    AuthenticationError,
    RateLimitExceededError,
    CircuitBreakerTrippedError,
    DeltaTokenExpiredError,
    StorageSinkError
)
from .extractor import FieldSanitizer, PermissionsExtractor, SYSTEM_FIELDS_BLOCKLIST
from .models import ItemMetadata, ManifestRecord, SyncAction, TaxonomyTerm, HeavyTaskMarker
from .rate_limiter import BoundedRateLimiter, ResilientHttpClient
from .s3_sink import S3Sink
from .taxonomy import TermStoreTaxonomyResolver
from .telemetry import PipelineMetrics, log_json
from .engine import CircuitBreaker, DeltaSyncEngine, HeavyTransferWorker
from .connector import SharePointConnector

__all__ = [
    "SharePointConnector",
    "TokenManager",
    "ConnectorConfig",
    "load_config",
    "parse_job_arguments",
    "fetch_secret",
    "SharePointConnectorError",
    "AuthenticationError",
    "RateLimitExceededError",
    "CircuitBreakerTrippedError",
    "DeltaTokenExpiredError",
    "StorageSinkError",
    "FieldSanitizer",
    "PermissionsExtractor",
    "SYSTEM_FIELDS_BLOCKLIST",
    "ItemMetadata",
    "ManifestRecord",
    "SyncAction",
    "TaxonomyTerm",
    "HeavyTaskMarker",
    "BoundedRateLimiter",
    "ResilientHttpClient",
    "S3Sink",
    "TermStoreTaxonomyResolver",
    "PipelineMetrics",
    "log_json",
    "CircuitBreaker",
    "DeltaSyncEngine",
    "HeavyTransferWorker"
]
