"""
In-memory SharePoint Term Store taxonomy pre-caching and resolution.
"""

from typing import Callable, Dict, Optional

from .models import TaxonomyTerm
from .rate_limiter import ResilientHttpClient
from .telemetry import log_json


class TermStoreTaxonomyResolver:
    """Pre-caches SharePoint Term Store taxonomy terms into memory to eliminate N+1 resolution calls."""

    def __init__(
        self,
        site_id: str,
        http_client: ResilientHttpClient,
        auth_headers_provider: Callable[[], Dict[str, str]]
    ):
        self.site_id = site_id
        self.http = http_client
        self.get_auth_headers = auth_headers_provider
        self._cache: Dict[str, str] = {}
        self._initialized = False

    @property
    def cache(self) -> Dict[str, str]:
        return self._cache

    def initialize(self):
        """Discovers and caches all term sets and terms under site Term Store."""
        if self._initialized:
            return

        try:
            url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/termStore/groups"
            headers = self.get_auth_headers()
            resp = self.http.request("GET", url, headers=headers)
            if not resp or resp.status_code != 200:
                log_json("info", "Term Store groups endpoint unavailable or unconfigured, using inline label resolution")
                self._initialized = True
                return

            groups = resp.json().get("value", [])
            for group in groups:
                group_id = group.get("id")
                sets_url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/termStore/groups/{group_id}/sets"
                sets_resp = self.http.request("GET", sets_url, headers=self.get_auth_headers())
                if not sets_resp or sets_resp.status_code != 200:
                    continue

                sets = sets_resp.json().get("value", [])
                for term_set in sets:
                    set_id = term_set.get("id")
                    localized_names = term_set.get("localizedNames", [])
                    set_name = localized_names[0].get("name", "Default") if localized_names else "Default"
                    terms_url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/termStore/sets/{set_id}/terms"
                    terms_resp = self.http.request("GET", terms_url, headers=self.get_auth_headers())
                    if not terms_resp or terms_resp.status_code != 200:
                        continue

                    terms = terms_resp.json().get("value", [])
                    for term in terms:
                        term_id = term.get("id")
                        labels = term.get("labels", [])
                        term_label = labels[0].get("name") if labels else term.get("defaultLanguageTag", "")
                        if term_id and term_label:
                            self._cache[term_id] = f"{set_name}/{term_label}"

            log_json("info", "Pre-cached SharePoint Term Store taxonomy terms", cached_terms_count=len(self._cache))
        except Exception as e:
            log_json("warning", "Graceful fallback: failed to pre-cache Term Store taxonomy", error=str(e))
        finally:
            self._initialized = True

    def resolve_term(self, term_guid: Optional[str], raw_label: str, wss_id: Optional[int] = None) -> TaxonomyTerm:
        """Resolves term GUID and raw label to clean TaxonomyTerm dataclass with full hierarchical path."""
        clean_label = raw_label
        if "#" in clean_label:
            clean_label = clean_label.split("#", 1)[-1]

        resolved_path = self._cache.get(term_guid, clean_label) if term_guid else clean_label
        return TaxonomyTerm(
            term_guid=term_guid,
            label=clean_label,
            path=resolved_path,
            wss_id=wss_id
        )
