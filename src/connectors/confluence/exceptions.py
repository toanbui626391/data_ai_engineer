"""
Domain-specific exception hierarchy for Atlassian Confluence connector.
"""


class ConfluenceConnectorError(Exception):
    """Base exception for all Confluence connector errors."""
    pass


class AuthenticationError(ConfluenceConnectorError):
    """Raised when authentication against Atlassian Cloud / Data Center fails."""
    pass


class RateLimitExceededError(ConfluenceConnectorError):
    """Raised when Atlassian API rate limit (HTTP 429) retries are exhausted."""
    def __init__(self, message: str, retry_after: int = 0):
        super().__init__(message)
        self.retry_after = retry_after


class CircuitBreakerTrippedError(ConfluenceConnectorError):
    """Raised when consecutive error threshold or error rate ceiling is breached."""
    def __init__(self, message: str, consecutive_failures: int = 0, total_failed: int = 0):
        super().__init__(message)
        self.consecutive_failures = consecutive_failures
        self.total_failed = total_failed


class QuarantineError(ConfluenceConnectorError):
    """Raised when an isolated corrupted item is written to dead-letter quarantine."""
    def __init__(self, message: str, item_id: str, error_type: str = ""):
        super().__init__(message)
        self.item_id = item_id
        self.error_type = error_type


class StorageSinkError(ConfluenceConnectorError):
    """Raised when an Amazon S3 persistence, watermark, or manifest operation fails."""
    pass
