"""
Domain-specific exception hierarchy for SharePoint connector.
"""

class SharePointConnectorError(Exception):
    """Base exception for all SharePoint connector errors."""
    pass


class AuthenticationError(SharePointConnectorError):
    """Raised when OAuth 2.0 token acquisition or refresh fails."""
    pass


class RateLimitExceededError(SharePointConnectorError):
    """Raised when Microsoft Graph API rate limits (HTTP 429) exhaust all retries."""
    pass


class CircuitBreakerTrippedError(SharePointConnectorError):
    """Raised when consecutive errors or overall error rate exceeds safety thresholds."""
    pass


class DeltaTokenExpiredError(SharePointConnectorError):
    """Raised when upstream delta link returns HTTP 410 Gone (resync required)."""
    pass


class StorageSinkError(SharePointConnectorError):
    """Raised when writing metadata, tombstones, or streaming binaries to S3 fails."""
    pass
