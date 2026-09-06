"""
Strongly-typed domain models and enums for Atlassian Confluence connector.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class SyncAction(str, Enum):
    """Manifest status for synchronized Confluence pages."""
    INSERT = "INSERT"
    INSERTED = "INSERT"
    UPDATE = "UPDATE"
    UPDATED = "UPDATE"
    ACL_REFRESH = "ACL_REFRESH"
    SKIP = "SKIP"
    SKIPPED = "SKIP"
    DELETE = "DELETE"
    DELETED = "DELETE"
    QUARANTINE = "QUARANTINE"


@dataclass
class PageSummary:
    """Lightweight page summary extracted from /pages batch enumeration."""
    page_id: str
    space_key: str
    title: str
    version_number: int = 1
    parent_id: Optional[str] = None
    parent_type: Optional[str] = None
    author_id: Optional[str] = None
    created_at_utc: Optional[str] = None
    modified_at_utc: Optional[str] = None

    @classmethod
    def from_api_dict(cls, data: Dict[str, Any], space_key: Optional[str] = None) -> "PageSummary":
        version_obj = data.get("version", {})
        resolved_space = space_key or data.get("spaceId") or (data.get("space") or {}).get("key", "")
        return cls(
            page_id=str(data["id"]),
            space_key=str(resolved_space),
            title=data.get("title", "Untitled Page"),
            version_number=version_obj.get("number", 1),
            parent_id=data.get("parentId"),
            parent_type=data.get("parentType"),
            author_id=version_obj.get("authorId"),
            created_at_utc=data.get("createdAt"),
            modified_at_utc=version_obj.get("createdAt")
        )

    @property
    def id(self) -> str:
        return self.page_id

    @property
    def version_created_at(self) -> Optional[str]:
        return self.modified_at_utc


@dataclass
class PageRestrictions:
    """Read and update restriction lists for RAG security trimming."""
    allowed_users: List[str] = field(default_factory=list)
    allowed_groups: List[str] = field(default_factory=list)
    edit_users: List[str] = field(default_factory=list)
    edit_groups: List[str] = field(default_factory=list)

    def __init__(
        self,
        allowed_users: Optional[List[str]] = None,
        allowed_groups: Optional[List[str]] = None,
        edit_users: Optional[List[str]] = None,
        edit_groups: Optional[List[str]] = None,
        read_users: Optional[List[str]] = None,
        read_groups: Optional[List[str]] = None,
    ):
        self.allowed_users = list(allowed_users or read_users or [])
        self.allowed_groups = list(allowed_groups or read_groups or [])
        self.edit_users = list(edit_users or [])
        self.edit_groups = list(edit_groups or [])

    @property
    def has_restrictions(self) -> bool:
        return bool(self.allowed_users or self.allowed_groups or self.edit_users or self.edit_groups)

    @property
    def is_restricted(self) -> bool:
        return self.has_restrictions

    @property
    def read_users(self) -> List[str]:
        return self.allowed_users

    @property
    def read_groups(self) -> List[str]:
        return self.allowed_groups

    def to_dict(self) -> Dict[str, Any]:
        return {
            "allowed_users": self.allowed_users,
            "allowed_groups": self.allowed_groups,
            "edit_users": self.edit_users,
            "edit_groups": self.edit_groups,
            "read_users": self.read_users,
            "read_groups": self.read_groups,
            "has_restrictions": self.has_restrictions,
        }

    @classmethod
    def from_api_dict(cls, data: Dict[str, Any]) -> "PageRestrictions":
        allowed_users = []
        allowed_groups = []
        edit_users = []
        edit_groups = []

        read_restr = data.get("read", {}).get("restrictions", {})
        for u in read_restr.get("user", {}).get("results", []):
            uid = u.get("accountId") or u.get("publicName")
            if uid:
                allowed_users.append(uid)
        for g in read_restr.get("group", {}).get("results", []):
            gid = g.get("name") or g.get("id")
            if gid:
                allowed_groups.append(gid)

        update_restr = data.get("update", {}).get("restrictions", {})
        for u in update_restr.get("user", {}).get("results", []):
            uid = u.get("accountId") or u.get("publicName")
            if uid:
                edit_users.append(uid)
        for g in update_restr.get("group", {}).get("results", []):
            gid = g.get("name") or g.get("id")
            if gid:
                edit_groups.append(gid)

        return cls(
            allowed_users=allowed_users,
            allowed_groups=allowed_groups,
            edit_users=edit_users,
            edit_groups=edit_groups
        )


@dataclass
class PageMetadata:
    """Schema for document sidecar metadata.json."""
    page_id: str
    space_key: str
    title: str
    version_number: int
    parent_id: Optional[str] = None
    parent_type: Optional[str] = None
    author_id: Optional[str] = None
    created_at_utc: Optional[str] = None
    modified_at_utc: Optional[str] = None
    web_url: Optional[str] = None
    size_bytes: int = 0
    has_restrictions: bool = False
    allowed_users: List[str] = field(default_factory=list)
    allowed_groups: List[str] = field(default_factory=list)
    edit_users: List[str] = field(default_factory=list)
    edit_groups: List[str] = field(default_factory=list)
    is_update: bool = False
    is_deleted: bool = False
    status: Optional[str] = None
    deleted_at_utc: Optional[str] = None
    acl_synced_at_utc: Optional[str] = None
    synced_at_utc: Optional[str] = None
    run_id: Optional[str] = None
    storage_format_bytes: int = 0
    markdown_bytes: int = 0
    s3_storage_uri: Optional[str] = None
    s3_markdown_uri: Optional[str] = None
    content_hash: Optional[str] = None
    restrictions_hash: Optional[str] = None
    version_created_at: Optional[str] = None
    restrictions: Optional[PageRestrictions] = None

    def __post_init__(self):
        if self.restrictions:
            self.has_restrictions = self.restrictions.has_restrictions
            self.allowed_users = self.restrictions.allowed_users
            self.allowed_groups = self.restrictions.allowed_groups
            self.edit_users = self.restrictions.edit_users
            self.edit_groups = self.restrictions.edit_groups

    def to_dict(self) -> Dict[str, Any]:
        data = {
            "page_id": self.page_id,
            "space_key": self.space_key,
            "title": self.title,
            "version_number": self.version_number,
            "parent_id": self.parent_id,
            "parent_type": self.parent_type,
            "author_id": self.author_id,
            "created_at_utc": self.created_at_utc,
            "modified_at_utc": self.modified_at_utc,
            "web_url": self.web_url,
            "size_bytes": self.size_bytes,
            "has_restrictions": self.has_restrictions,
            "allowed_users": self.allowed_users,
            "allowed_groups": self.allowed_groups,
            "edit_users": self.edit_users,
            "edit_groups": self.edit_groups,
            "is_update": self.is_update,
            "is_deleted": self.is_deleted,
            "status": self.status,
            "deleted_at_utc": self.deleted_at_utc,
            "acl_synced_at_utc": self.acl_synced_at_utc,
            "synced_at_utc": self.synced_at_utc,
            "run_id": self.run_id,
            "storage_format_bytes": self.storage_format_bytes,
            "markdown_bytes": self.markdown_bytes,
            "s3_storage_uri": self.s3_storage_uri,
            "s3_markdown_uri": self.s3_markdown_uri,
            "content_hash": self.content_hash,
            "restrictions_hash": self.restrictions_hash,
            "version_created_at": self.version_created_at,
        }
        if self.restrictions:
            data["restrictions"] = self.restrictions.to_dict()
        return data


@dataclass
class ManifestRecord:
    """Schema for run manifest entries in manifest_{run_id}.jsonl."""
    item_id: str
    run_id: str = ""
    source: str = "confluence"
    status: str = "INSERT"
    action: Optional[Any] = None
    space_key: Optional[str] = None
    version_number: Optional[int] = None
    version_created_at: Optional[str] = None
    file_name: str = ""
    size_bytes: int = 0
    s3_path: str = ""
    s3_markdown_uri: Optional[str] = None
    s3_sidecar_uri: Optional[str] = None
    timestamp_utc: Optional[str] = None
    error: Optional[str] = None

    def __post_init__(self):
        if self.action is not None:
            self.status = self.action.value if hasattr(self.action, "value") else str(self.action)
        if not self.s3_path and self.s3_markdown_uri:
            self.s3_path = self.s3_markdown_uri

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "source": self.source,
            "item_id": self.item_id,
            "file_name": self.file_name,
            "status": self.status,
            "size_bytes": self.size_bytes,
            "s3_path": self.s3_path,
            "s3_markdown_uri": self.s3_markdown_uri,
            "s3_sidecar_uri": self.s3_sidecar_uri,
            "space_key": self.space_key,
            "version_number": self.version_number,
            "version_created_at": self.version_created_at,
            "timestamp_utc": self.timestamp_utc,
            "error": self.error
        }


@dataclass
class TombstoneMarker:
    """Schema for S3 soft-delete marker raw/confluence/{space}/{page_id}/DELETED."""
    page_id: str
    space_key: str
    deleted_at_utc: str
    run_id: str = ""
    status: str = "DELETE"
    reason: str = "missing_from_upstream_space_crawl"
    purge_confirmed: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "page_id": self.page_id,
            "space_key": self.space_key,
            "status": self.status,
            "deleted_at_utc": self.deleted_at_utc,
            "run_id": self.run_id,
            "reason": self.reason,
            "purge_confirmed": self.purge_confirmed
        }
