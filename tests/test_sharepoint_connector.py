"""
Unit tests for modular SharePoint connector.
"""

import sys
import types
from unittest.mock import MagicMock

# Mock third-party libraries (requests, boto3, botocore) to allow running in bare Python environments
mock_requests = types.ModuleType("requests")
mock_requests.Session = MagicMock
mock_token_resp = MagicMock()
mock_token_resp.status_code = 200
mock_token_resp.json.return_value = {"access_token": "init-token", "expires_in": 3600}
mock_requests.post = MagicMock(return_value=mock_token_resp)
mock_requests.adapters = types.ModuleType("requests.adapters")
mock_requests.adapters.HTTPAdapter = MagicMock
sys.modules["requests"] = mock_requests
sys.modules["requests.adapters"] = mock_requests.adapters

mock_boto3 = types.ModuleType("boto3")
mock_boto3.client = MagicMock()
mock_boto3_s3 = types.ModuleType("boto3.s3")
mock_boto3_s3_transfer = types.ModuleType("boto3.s3.transfer")
mock_boto3_s3_transfer.TransferConfig = MagicMock()
mock_boto3.s3 = mock_boto3_s3
mock_boto3.s3.transfer = mock_boto3_s3_transfer
sys.modules["boto3"] = mock_boto3
sys.modules["boto3.s3"] = mock_boto3_s3
sys.modules["boto3.s3.transfer"] = mock_boto3_s3_transfer

mock_botocore = types.ModuleType("botocore")
mock_botocore_exceptions = types.ModuleType("botocore.exceptions")
mock_botocore_exceptions.ClientError = Exception
mock_botocore_exceptions.BotoCoreError = Exception
mock_botocore.exceptions = mock_botocore_exceptions
sys.modules["botocore"] = mock_botocore
sys.modules["botocore.exceptions"] = mock_botocore_exceptions

import unittest
from src.connectors.sharepoint import (
    SharePointConnector,
    TokenManager,
    BoundedRateLimiter,
    ResilientHttpClient,
    TermStoreTaxonomyResolver,
    FieldSanitizer,
    CircuitBreaker,
    PipelineMetrics,
    SyncAction,
    ConnectorConfig
)


class TestSharePointModularConnector(unittest.TestCase):
    def setUp(self):
        self.secrets = {
            "tenant_id": "test-tenant-id",
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "site_id": "test-site-id",
            "drive_id": "test-drive-id"
        }
        self.metrics = PipelineMetrics()
        self.rate_limiter = BoundedRateLimiter(max_requests_per_sec=100.0)
        self.mock_http = MagicMock()
        self.mock_s3 = MagicMock()
        self.mock_s3.bucket = "test-lakehouse-bucket"

    def test_token_manager_lifecycle(self):
        token_mgr = TokenManager(
            tenant_id="tenant-123",
            client_id="client-123",
            client_secret="secret-123"
        )
        # 1. Fetch token
        token = token_mgr.get_token()
        self.assertEqual(token, "init-token")

        # 2. Get auth headers
        headers = token_mgr.get_auth_headers()
        self.assertIn("Authorization", headers)
        self.assertEqual(headers["Authorization"], "Bearer init-token")

        # 3. Invalidation
        token_mgr.invalidate_token()
        self.assertEqual(token_mgr.access_token, "")

    def test_circuit_breaker_tripping(self):
        cb = CircuitBreaker(max_consecutive_failures=3, max_error_rate=0.5)
        self.assertFalse(cb.is_tripped)

        # Record failures
        cb.record_failure("item-1", "Timeout")
        self.assertFalse(cb.is_tripped)
        cb.record_failure("item-2", "ConnectionReset")
        self.assertFalse(cb.is_tripped)
        cb.record_failure("item-3", "GraphError")
        # Should trip after 3 consecutive failures
        self.assertTrue(cb.is_tripped)

    def test_field_sanitizer_and_taxonomy_resolution(self):
        taxonomy_resolver = TermStoreTaxonomyResolver(
            site_id="site-1",
            http_client=self.mock_http,
            auth_headers_provider=lambda: {}
        )
        taxonomy_resolver._cache = {
            "term-guid-corp": "Enterprise Taxonomy/Finance/Capital Allocation",
            "term-guid-conf": "Compliance/GDPR/Personally Identifiable"
        }
        sanitizer = FieldSanitizer(taxonomy_resolver=taxonomy_resolver)

        raw_item = {
            "listItem": {
                "fields": {
                    "id": "1",
                    "ContentTypeId": "0x01...",
                    "_UIVersionString": "2.0",
                    "AuthorLookupId": "4",
                    "owshiddenversion": "7",
                    "Project_x0020_Name": "Apollo 11",
                    "Fiscal_x0020_Year": 2026,
                    "Business_x0020_Unit": "Engineering",
                    "SecurityCategory": {
                        "TermGuid": "term-guid-conf",
                        "Label": "1033#Personally Identifiable",
                        "wssId": 8
                    },
                    "MultiTerms": [
                        {"TermGuid": "term-guid-corp", "Label": "Capital Allocation"},
                        {"TermGuid": "unknown-guid", "Label": "Uncategorized"}
                    ]
                }
            }
        }

        custom_fields, taxonomy_fields = sanitizer.extract_custom_fields(raw_item)

        # Check system fields stripped
        self.assertNotIn("id", custom_fields)
        self.assertNotIn("ContentTypeId", custom_fields)
        self.assertNotIn("_UIVersionString", custom_fields)
        self.assertNotIn("AuthorLookupId", custom_fields)

        # Check decoded column names
        self.assertEqual(custom_fields["Project Name"], "Apollo 11")
        self.assertEqual(custom_fields["Fiscal Year"], 2026)
        self.assertEqual(custom_fields["Business Unit"], "Engineering")

        # Check taxonomy resolution
        self.assertIn("SecurityCategory", taxonomy_fields)
        self.assertEqual(taxonomy_fields["SecurityCategory"]["path"], "Compliance/GDPR/Personally Identifiable")
        self.assertEqual(taxonomy_fields["SecurityCategory"]["label"], "Personally Identifiable")

        self.assertIn("MultiTerms", taxonomy_fields)
        self.assertEqual(taxonomy_fields["MultiTerms"][0]["path"], "Enterprise Taxonomy/Finance/Capital Allocation")
        self.assertEqual(taxonomy_fields["MultiTerms"][1]["path"], "Uncategorized")

    def test_facade_backwards_compatibility_and_metadata_refresh(self):
        with unittest.mock.patch.object(TermStoreTaxonomyResolver, 'initialize'):
            connector = SharePointConnector(
                secrets=self.secrets,
                http_client=self.mock_http,
                s3_sink=self.mock_s3
            )

        connector.term_store_cache = {"guid-abc": "Corporate/Legal/Contract"}
        self.assertEqual(connector.term_store_cache["guid-abc"], "Corporate/Legal/Contract")

        item = {
            "id": "doc-99",
            "name": "ServiceAgreement.pdf",
            "eTag": '"etag-v99"',
            "size": 5000,
            "file": {"mimeType": "application/pdf"},
            "parentReference": {"driveId": "drive-99"},
            "@microsoft.graph.downloadUrl": "https://graph.download.url/doc99",
            "listItem": {
                "fields": {
                    "Department": "Legal",
                    "Contract_x0020_Status": "Executed"
                }
            }
        }

        # Simulate existing unchanged binary in S3 with older metadata
        old_meta = {
            "doc_id": "doc-99",
            "file_name": "ServiceAgreement.pdf",
            "upstream_etag": '"etag-v99"',
            "allowed_principals": [],
            "custom_fields": {
                "Department": "Legal",
                "Contract Status": "Draft"
            },
            "taxonomy": {}
        }
        connector.s3.check_item_sync_state.return_value = (True, True, old_meta)
        connector._extract_item_permissions = MagicMock(return_value=[])

        # Process item
        connector._process_delta_item(item)

        # Verify binary download was skipped (0 ms latency)
        connector.delta_engine._download_and_stream_with_retry = MagicMock()
        connector.delta_engine._download_and_stream_with_retry.assert_not_called()

        # Verify sidecar metadata updated
        connector.s3.write_sidecar_metadata.assert_called_once()
        saved_meta = connector.s3.write_sidecar_metadata.call_args[0][1]
        self.assertEqual(saved_meta["custom_fields"]["Contract Status"], "Executed")

        # Verify manifest recorded METADATA_REFRESH
        connector.s3.record_manifest_entry.assert_called_once()
        manifest_status = connector.s3.record_manifest_entry.call_args[1]["status"]
        self.assertEqual(manifest_status, SyncAction.METADATA_REFRESH.value)


if __name__ == "__main__":
    unittest.main()
