"""
Custom column field sanitization, space decoding, and Entra ID ACL permission extraction.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple

from .rate_limiter import ResilientHttpClient
from .taxonomy import TermStoreTaxonomyResolver

# Known SharePoint internal and system fields to sanitize
SYSTEM_FIELDS_BLOCKLIST = frozenset({
    "id", "ContentTypeId", "FileRef", "FileLeafRef", "FileDirRef", "FSObjType",
    "PermMask", "Modified", "Created", "AuthorLookupId", "EditorLookupId",
    "Attachments", "Edit", "DocIcon", "ItemChildCount", "FolderChildCount",
    "AppAuthorLookupId", "AppEditorLookupId", "SyncClientId", "ProgId",
    "ScopeId", "HTML_x0020_File_x0020_Type", "SMTotalSize", "SMLastModifiedDate",
    "OData__UIVersionString", "owshiddenversion"
})


class FieldSanitizer:
    """Sanitizes internal SharePoint system fields, decodes encoded column names, and resolves taxonomies."""

    def __init__(self, taxonomy_resolver: TermStoreTaxonomyResolver):
        self.taxonomy = taxonomy_resolver

    def extract_custom_fields(self, item: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Extracts custom Document Library columns and resolves Managed Metadata taxonomies.
        Returns:
            custom_fields: Dict of business columns (e.g. Department, FiscalYear, DocumentType)
            taxonomy_fields: Dict of resolved taxonomy terms (term_guid, label, hierarchical path)
        """
        raw_fields = item.get("listItem", {}).get("fields", {})
        if not raw_fields:
            return {}, {}

        custom_fields: Dict[str, Any] = {}
        taxonomy_fields: Dict[str, Any] = {}

        for key, val in raw_fields.items():
            # 1. Strip internal system fields and metadata directives
            if key.startswith(("_", "@odata")) or key in SYSTEM_FIELDS_BLOCKLIST:
                continue

            # 2. Decode SharePoint URL-encoded spaces (e.g. "Project_x0020_Name" -> "Project Name")
            clean_key = key.replace("_x0020_", " ")

            # 3. Detect and resolve Managed Metadata taxonomy fields
            if isinstance(val, dict) and ("TermGuid" in val or "Label" in val or "wssId" in val):
                term = self.taxonomy.resolve_term(
                    term_guid=val.get("TermGuid"),
                    raw_label=str(val.get("Label") or ""),
                    wss_id=val.get("wssId")
                )
                taxonomy_fields[clean_key] = term.to_dict()

            elif isinstance(val, list):
                # Multi-value taxonomy or lookup list
                parsed_list = []
                is_tax_list = False
                for sub_val in val:
                    if isinstance(sub_val, dict) and ("TermGuid" in sub_val or "Label" in sub_val):
                        is_tax_list = True
                        term = self.taxonomy.resolve_term(
                            term_guid=sub_val.get("TermGuid"),
                            raw_label=str(sub_val.get("Label") or "")
                        )
                        parsed_list.append(term.to_dict())
                    else:
                        parsed_list.append(sub_val)

                if is_tax_list:
                    taxonomy_fields[clean_key] = parsed_list
                else:
                    custom_fields[clean_key] = parsed_list
            else:
                custom_fields[clean_key] = val

        return custom_fields, taxonomy_fields


class PermissionsExtractor:
    """Queries Microsoft Graph permissions and extracts Entra ID user and group principals for RAG security."""

    def __init__(
        self,
        site_id: str,
        http_client: ResilientHttpClient,
        auth_headers_provider: Callable[[], Dict[str, str]]
    ):
        self.site_id = site_id
        self.http = http_client
        self.get_auth_headers = auth_headers_provider

    def extract_permissions(self, item_id: str, drive_id: Optional[str] = None) -> List[str]:
        """Queries /permissions endpoint and parses granted userPrincipalNames and group IDs."""
        if drive_id:
            url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/permissions"
        else:
            url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drive/items/{item_id}/permissions"

        resp = self.http.request("GET", url, headers=self.get_auth_headers())
        allowed_principals: List[str] = []

        if resp and resp.status_code == 200:
            perm_data = resp.json()
            for perm in perm_data.get("value", []):
                granted = perm.get("grantedToV2", {})
                if "user" in granted:
                    user_val = granted["user"].get("userPrincipalName") or granted["user"].get("id")
                    if user_val:
                        allowed_principals.append(f"user:{user_val}")
                if "group" in granted:
                    group_val = granted["group"].get("id")
                    if group_val:
                        allowed_principals.append(f"group:{group_val}")

        return allowed_principals
