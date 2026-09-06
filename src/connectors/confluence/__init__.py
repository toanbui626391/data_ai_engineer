"""
Enterprise Atlassian Confluence custom connector for AWS Glue and lakehouse ingestion.
"""

from .config import ConfluenceConfig, fetch_secret, load_config, parse_job_arguments
from .connector import ConfluenceConnector
from .engine import CircuitBreaker, ConfluenceSyncEngine
from .exceptions import (
    AuthenticationError,
    CircuitBreakerTrippedError,
    ConfluenceConnectorError,
    QuarantineError,
    RateLimitExceededError,
    StorageSinkError
)
from .models import (
    ManifestRecord,
    PageMetadata,
    PageRestrictions,
    PageSummary,
    SyncAction,
    TombstoneMarker
)
from .rate_limiter import BoundedRateLimiter, ResilientHttpClient
from .s3_sink import S3Sink
from .sanitizer import ConfluenceMacroSanitizer
from .telemetry import PipelineMetrics, log_json

__all__ = [
    "ConfluenceConnector",
    "ConfluenceConfig",
    "load_config",
    "parse_job_arguments",
    "fetch_secret",
    "ConfluenceSyncEngine",
    "CircuitBreaker",
    "ConfluenceConnectorError",
    "AuthenticationError",
    "RateLimitExceededError",
    "CircuitBreakerTrippedError",
    "QuarantineError",
    "StorageSinkError",
    "ManifestRecord",
    "PageMetadata",
    "PageRestrictions",
    "PageSummary",
    "SyncAction",
    "TombstoneMarker",
    "BoundedRateLimiter",
    "ResilientHttpClient",
    "S3Sink",
    "ConfluenceMacroSanitizer",
    "PipelineMetrics",
    "log_json"
]
