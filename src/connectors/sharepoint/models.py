"""
Strongly-typed data models and enums for SharePoint connector.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class SyncAction(str, Enum):
    """Manifest status for synchronized drive items."""
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    RENAME = "RENAME"
    ACL_REFRESH = "ACL_REFRESH"
    METADATA_REFRESH = "METADATA_REFRESH"
    SKIP = "SKIP"
    QUEUED_HEAVY = "QUEUED_HEAVY"
    HEAVY_COMPLETE = "HEAVY_COMPLETE"
    SKIPPED_OVERSIZED = "SKIPPED_OVERSIZED"
    TOMBSTONE = "TOMBSTONE"


@dataclass
class TaxonomyTerm:
    """Represents a resolved Managed Metadata taxonomy term."""
    term_guid: Optional[str]
    label: str
    path: str
    wss_id: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "term_guid": self.term_guid,
            "label": self.label,
            "path": self.path,
            "wss_id": self.wss_id
        }


@dataclass
class ItemMetadata:
    """Schema for document sidecar metadata.json."""
    doc_id: str
    file_name: str
    site_id: str
    upstream_etag: Optional[str] = None
    size_bytes: int = 0
    mime_type: str = "application/octet-stream"
    web_url: Optional[str] = None
    created_at_utc: Optional[str] = None
    modified_at_utc: Optional[str] = None
    allowed_principals: List[str] = field(default_factory=list)
    custom_fields: Dict[str, Any] = field(default_factory=dict)
    taxonomy: Dict[str, Any] = field(default_factory=dict)
    is_update: bool = False
    status: Optional[str] = None
    synced_at_utc: Optional[str] = None
    run_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        data = {
            "doc_id": self.doc_id,
            "file_name": self.file_name,
            "site_id": self.site_id,
            "upstream_etag": self.upstream_etag,
            "size_bytes": self.size_bytes,
            "mime_type": self.mime_type,
            "web_url": self.web_url,
            "created_at_utc": self.created_at_utc,
            "modified_at_utc": self.modified_at_utc,
            "allowed_principals": self.allowed_principals,
            "custom_fields": self.custom_fields,
            "taxonomy": self.taxonomy,
            "is_update": self.is_update
        }
        if self.status:
            data["status"] = self.status
        if self.synced_at_utc:
            data["synced_at_utc"] = self.synced_at_utc
        if self.run_id:
            data["run_id"] = self.run_id
        return data


@dataclass
class ManifestRecord:
    """Entry in the batch inventory manifest."""
    item_id: str
    status: str
    file_name: str
    size_bytes: int = 0
    etag: Optional[str] = None
    s3_path: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        data = {
            "item_id": self.item_id,
            "status": self.status,
            "file_name": self.file_name,
            "size_bytes": self.size_bytes
        }
        if self.etag:
            data["etag"] = self.etag
        if self.s3_path:
            data["s3_path"] = self.s3_path
        return data


@dataclass
class HeavyTaskMarker:
    """Task definition queued for Tier 2 bulk ingestion."""
    item_id: str
    drive_id: Optional[str]
    site_id: str
    safe_site: str
    file_name: str
    size_bytes: int
    upstream_etag: Optional[str]
    mime_type: str
    s3_prefix: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "item_id": self.item_id,
            "drive_id": self.drive_id,
            "site_id": self.site_id,
            "safe_site": self.safe_site,
            "file_name": self.file_name,
            "size_bytes": self.size_bytes,
            "upstream_etag": self.upstream_etag,
            "mime_type": self.mime_type,
            "s3_prefix": self.s3_prefix
        }
