"""
Unit tests for self-contained Confluence connector.
Tests two-phase decoupled ETag fetch, macro sanitization, ACL drift, and circuit breakers.
"""

import sys
import json
import types
from unittest.mock import MagicMock, patch

# Mock third-party libraries (requests, boto3, botocore) to allow running in bare Python environments
mock_requests = types.ModuleType("requests")
mock_requests.Session = MagicMock
mock_requests.adapters = types.ModuleType("requests.adapters")
mock_requests.adapters.HTTPAdapter = MagicMock
sys.modules["requests"] = mock_requests
sys.modules["requests.adapters"] = mock_requests.adapters

mock_boto3 = types.ModuleType("boto3")
mock_boto3.client = MagicMock()
sys.modules["boto3"] = mock_boto3

mock_botocore = types.ModuleType("botocore")
mock_botocore_exceptions = types.ModuleType("botocore.exceptions")
mock_botocore_exceptions.ClientError = Exception
mock_botocore_exceptions.BotoCoreError = Exception
mock_botocore.exceptions = mock_botocore_exceptions
sys.modules["botocore"] = mock_botocore
sys.modules["botocore.exceptions"] = mock_botocore_exceptions

import unittest
from src.connectors.confluence.connector import (
    ConfluenceConnector,
    ConfluenceMacroSanitizer,
    PipelineMetrics,
    BoundedRateLimiter,
    ResilientHttpClient,
    S3Sink
)


class TestConfluenceConnector(unittest.TestCase):
    def setUp(self):
        self.secrets = {
            "base_url": "https://test-company.atlassian.net/wiki",
            "user_email": "bot@company.com",
            "api_token": "secret-token-xyz",
            "space_keys": "ENG"
        }
        self.metrics = PipelineMetrics()
        self.rate_limiter = BoundedRateLimiter(max_requests_per_sec=100.0)
        self.http_client = MagicMock(spec=ResilientHttpClient)
        self.s3_sink = MagicMock(spec=S3Sink)
        self.s3_sink.bucket = "test-bucket"
        self.s3_sink.s3 = MagicMock()
        self.s3_sink.metrics = self.metrics

        self.connector = ConfluenceConnector(
            secrets=self.secrets,
            http_client=self.http_client,
            s3_sink=self.s3_sink,
            max_workers=2
        )

    def test_macro_sanitizer_transformations(self):
        """Validates that Confluence XML macros convert cleanly to Markdown."""
        raw_xhtml = (
            "<p>Header intro</p>"
            '<ac:structured-macro ac:name="code">'
            '<ac:parameter ac:name="language">python</ac:parameter>'
            "<ac:plain-text-body><![CDATA[def calculate():\n    return 42]]></ac:plain-text-body>"
            "</ac:structured-macro>"
            '<ac:structured-macro ac:name="info">'
            "<ac:rich-text-body><p>Confidential doc</p></ac:rich-text-body>"
            "</ac:structured-macro>"
            '<ac:structured-macro ac:name="warning">'
            "<ac:rich-text-body><p>Do not expose credentials</p></ac:rich-text-body>"
            "</ac:structured-macro>"
            '<ac:structured-macro ac:name="toc"/>'
            '<ac:structured-macro ac:name="status">'
            '<ac:parameter ac:name="title">IN REVIEW</ac:parameter>'
            "</ac:structured-macro>"
            "<h2>Next Steps</h2>"
            "<p><strong>bold note</strong> and <em>italic term</em></p>"
        )

        sanitizer = ConfluenceMacroSanitizer()
        clean_md = sanitizer.sanitize(raw_xhtml)

        # Check code block conversion
        self.assertIn("```python", clean_md)
        self.assertIn("def calculate():", clean_md)
        self.assertIn("return 42", clean_md)
        self.assertIn("```", clean_md)

        # Check callout conversion
        self.assertIn("> [!NOTE]", clean_md)
        self.assertIn("Confidential doc", clean_md)
        self.assertIn("> [!WARNING]", clean_md)
        self.assertIn("Do not expose credentials", clean_md)

        # Check TOC removal
        self.assertNotIn("ac:name=\"toc\"", clean_md)
        self.assertNotIn("<ac:", clean_md)

        # Check status badge and headings
        self.assertIn("[IN REVIEW]", clean_md)
        self.assertIn("## Next Steps", clean_md)
        self.assertIn("**bold note**", clean_md)
        self.assertIn("*italic term*", clean_md)

    def test_two_phase_etag_skip_unchanged_page(self):
        """Unchanged pages skip full body download and S3 body writes."""
        page_summary = {
            "id": "184729103",
            "title": "Unchanged Spec",
            "version": {"number": 3, "createdAt": "2026-03-01T00:00:00Z"},
            "parentId": None,
            "parentType": "page"
        }

        # Mock cache check: body unchanged, not update, existing meta matches version 3
        existing_meta = {
            "version_number": 3,
            "allowed_users": ["user-1"],
            "allowed_groups": ["eng-team"],
            "size_bytes": 1024
        }
        self.s3_sink.check_item_sync_state.return_value = (True, False, existing_meta)

        # Mock restrictions response returning same permissions
        mock_restr_resp = MagicMock()
        mock_restr_resp.status_code = 200
        mock_restr_resp.json.return_value = {
            "read": {
                "restrictions": {
                    "user": {"results": [{"accountId": "user-1"}]},
                    "group": {"results": [{"name": "eng-team"}]}
                }
            }
        }
        self.http_client.request.return_value = mock_restr_resp

        result = self.connector._process_page(page_summary, "ENG", {})

        self.assertEqual(result, "2026-03-01T00:00:00Z")
        # Ensure detail endpoint (?body-format=storage) was NEVER called
        for call_args in self.http_client.request.call_args_list:
            url_called = call_args[0][1] if len(call_args[0]) > 1 else ""
            self.assertNotIn("?body-format=storage", url_called)

        # Ensure no body was written to S3
        self.s3_sink.s3.put_object.assert_not_called()
        self.s3_sink.record_manifest_entry.assert_called_once_with(
            item_id="184729103",
            status="SKIP",
            file_name="Unchanged Spec.xhtml"
        )

    def test_two_phase_etag_miss_fetches_body_and_writes_dual_artifacts(self):
        """Modified page triggers Phase 2 fetch, macro sanitization, and dual S3 writes."""
        page_summary = {
            "id": "184729104",
            "title": "Modified Architecture",
            "version": {"number": 4, "createdAt": "2026-03-02T10:00:00Z"},
            "parentId": "184729100",
            "parentType": "page"
        }

        # Cache check returns unchanged = False
        self.s3_sink.check_item_sync_state.return_value = (False, True, {"version_number": 3})

        # Detail response with storage XHTML
        mock_detail_resp = MagicMock()
        mock_detail_resp.status_code = 200
        mock_detail_resp.json.return_value = {
            "body": {
                "storage": {
                    "value": (
                        "<p>Updated text</p>"
                        '<ac:structured-macro ac:name="code">'
                        '<ac:parameter ac:name="language">python</ac:parameter>'
                        "<ac:plain-text-body><![CDATA[x = 100]]></ac:plain-text-body>"
                        "</ac:structured-macro>"
                    )
                }
            }
        }

        # Restrictions response
        mock_restr_resp = MagicMock()
        mock_restr_resp.status_code = 200
        mock_restr_resp.json.return_value = {"read": {"restrictions": {"user": {"results": []}, "group": {"results": []}}}}

        def side_effect_request(method, url, **kwargs):
            if "?body-format=storage" in url:
                return mock_detail_resp
            if "/restrictions" in url:
                return mock_restr_resp
            return None

        self.http_client.request.side_effect = side_effect_request

        result = self.connector._process_page(page_summary, "ENG", {})

        self.assertEqual(result, "2026-03-02T10:00:00Z")

        # Verify S3 put_object was called twice: content.xhtml (Bronze) and content.md (Silver)
        put_keys = [call[1]["Key"] for call in self.s3_sink.s3.put_object.call_args_list]
        self.assertIn("raw/confluence/ENG/184729104/content.xhtml", put_keys)
        self.assertIn("raw/confluence/ENG/184729104/content.md", put_keys)

        # Verify metadata sidecar was written
        self.s3_sink.write_sidecar_metadata.assert_called_once()
        sidecar = self.s3_sink.write_sidecar_metadata.call_args[0][1]
        self.assertEqual(sidecar["version_number"], 4)
        self.assertEqual(sidecar["parent_id"], "184729100")

    def test_acl_drift_refresh_without_body_fetch(self):
        """Restrictions updated without version change triggers ACL_REFRESH without re-downloading body."""
        page_summary = {
            "id": "184729105",
            "title": "Confidential Strategy",
            "version": {"number": 2, "createdAt": "2026-03-01T00:00:00Z"},
            "parentId": None,
            "parentType": "page"
        }

        # Body version is unchanged!
        existing_meta = {
            "version_number": 2,
            "allowed_users": ["old-user"],
            "allowed_groups": [],
            "size_bytes": 2048
        }
        self.s3_sink.check_item_sync_state.return_value = (True, False, existing_meta)

        # But restrictions API returns a new restricted user
        mock_restr_resp = MagicMock()
        mock_restr_resp.status_code = 200
        mock_restr_resp.json.return_value = {
            "read": {
                "restrictions": {
                    "user": {"results": [{"accountId": "new-executive-user"}]},
                    "group": {"results": []}
                }
            }
        }
        self.http_client.request.return_value = mock_restr_resp

        result = self.connector._process_page(page_summary, "ENG", {})

        self.assertEqual(result, "2026-03-01T00:00:00Z")

        # Ensure no body detail call was made
        for call_args in self.http_client.request.call_args_list:
            url_called = call_args[0][1] if len(call_args[0]) > 1 else ""
            self.assertNotIn("?body-format=storage", url_called)

        # Ensure metadata sidecar WAS written with new ACLs
        self.s3_sink.write_sidecar_metadata.assert_called_once()
        sidecar = self.s3_sink.write_sidecar_metadata.call_args[0][1]
        self.assertEqual(sidecar["allowed_users"], ["new-executive-user"])
        self.assertTrue(sidecar["has_restrictions"])
        self.assertIn("acl_synced_at_utc", sidecar)

        # Manifest status is ACL_REFRESH
        self.s3_sink.record_manifest_entry.assert_called_once_with(
            item_id="184729105",
            status="ACL_REFRESH",
            file_name="Confidential Strategy.xhtml",
            size_bytes=2048,
            s3_path="raw/confluence/ENG/184729105/content.xhtml"
        )

    def test_update_edit_restrictions_triggers_acl_refresh(self):
        """Update/edit restrictions change triggers ACL_REFRESH even when read restrictions are unchanged."""
        page_summary = {
            "id": "184729106",
            "title": "System Architecture Edit Lock",
            "version": {"number": 5, "createdAt": "2026-03-01T00:00:00Z"},
            "parentId": "184728000",
            "parentType": "page"
        }

        # Body version and read restrictions are unchanged
        existing_meta = {
            "version_number": 5,
            "allowed_users": ["dev-user"],
            "allowed_groups": ["developers"],
            "edit_users": [],
            "edit_groups": [],
            "size_bytes": 4096
        }
        self.s3_sink.check_item_sync_state.return_value = (True, False, existing_meta)

        # But edit restrictions now restrict editing to architects
        mock_restr_resp = MagicMock()
        mock_restr_resp.status_code = 200
        mock_restr_resp.json.return_value = {
            "read": {
                "restrictions": {
                    "user": {"results": [{"accountId": "dev-user"}]},
                    "group": {"results": [{"name": "developers"}]}
                }
            },
            "update": {
                "restrictions": {
                    "user": {"results": []},
                    "group": {"results": [{"name": "enterprise-architects"}]}
                }
            }
        }
        self.http_client.request.return_value = mock_restr_resp

        result = self.connector._process_page(page_summary, "ENG", {})

        self.assertEqual(result, "2026-03-01T00:00:00Z")

        # Verify metadata sidecar recorded edit_groups
        self.s3_sink.write_sidecar_metadata.assert_called_once()
        sidecar = self.s3_sink.write_sidecar_metadata.call_args[0][1]
        self.assertEqual(sidecar["edit_groups"], ["enterprise-architects"])
        self.assertEqual(sidecar["parent_id"], "184728000")

        # Manifest status is ACL_REFRESH
        self.s3_sink.record_manifest_entry.assert_called_once_with(
            item_id="184729106",
            status="ACL_REFRESH",
            file_name="System Architecture Edit Lock.xhtml",
            size_bytes=4096,
            s3_path="raw/confluence/ENG/184729106/content.xhtml"
        )

    def test_circuit_breaker_trips_on_consecutive_failures(self):
        """Verifies circuit breaker trips after max consecutive failures."""
        self.connector.max_consecutive_failures = 3

        for i in range(3):
            self.connector._record_failure(f"page-{i}", "Atlassian 500 Error")

        self.assertTrue(self.connector.circuit_broken)

        # When tripped, _process_page immediately returns None
        page = {"id": "page-999", "title": "Test", "version": {"number": 1}}
        result = self.connector._process_page(page, "ENG", {})
        self.assertIsNone(result)

    @patch("time.sleep", return_value=None)
    def test_resilient_http_client_429_retry_after(self, mock_sleep):
        """HTTP client respects 429 Retry-After header and applies jitter backoff."""
        metrics = PipelineMetrics()
        rate_limiter = BoundedRateLimiter(max_requests_per_sec=100.0)
        client = ResilientHttpClient(metrics=metrics, rate_limiter=rate_limiter, max_retries=3, base_delay=0.1)

        # First call returns 429 with Retry-After: 2, second call returns 200 OK
        resp_429 = MagicMock()
        resp_429.status_code = 429
        resp_429.headers = {"Retry-After": "2"}

        resp_200 = MagicMock()
        resp_200.status_code = 200

        client.session.request = MagicMock(side_effect=[resp_429, resp_200])

        final_resp = client.request("GET", "https://test-company.atlassian.net/wiki/api/v2/spaces")

        self.assertEqual(final_resp.status_code, 200)
        self.assertEqual(metrics.retries_429, 1)
        mock_sleep.assert_called_once()
        # Verify sleep duration was >= 2.0 (base 2 + positive jitter)
        sleep_duration = mock_sleep.call_args[0][0]
        self.assertGreaterEqual(sleep_duration, 2.0)
        self.assertLessEqual(sleep_duration, 3.5)

    def test_manifest_reconciliation_detects_hard_deletes_and_emits_tombstone(self):
        """Validates that pages missing from the active space crawl trigger soft tombstones."""
        self.s3_sink.get_checkpoint.return_value = "2026-01-01T00:00:00Z"
        # Previous run had pages 101, 102, 103
        self.s3_sink.get_active_page_ids.return_value = {"101", "102", "103"}
        self.s3_sink.check_item_sync_state.return_value = (False, False, {"title": "Deleted Design", "version_number": 2})

        # Today's crawl only returns pages 101 and 103 (page 102 was deleted in Confluence)
        mock_page_resp = MagicMock()
        mock_page_resp.status_code = 200
        mock_page_resp.json.return_value = {
            "results": [
                {"id": "101", "title": "Page 101", "version": {"number": 1, "createdAt": "2026-01-02T10:00:00Z"}},
                {"id": "103", "title": "Page 103", "version": {"number": 1, "createdAt": "2026-01-02T11:00:00Z"}}
            ],
            "_links": {}
        }
        self.http_client.request.return_value = mock_page_resp

        # Run space sync
        with patch.object(self.connector, "_process_page", side_effect=["2026-01-02T10:00:00Z", "2026-01-02T11:00:00Z"]):
            self.connector._sync_space("ENG", {"Authorization": "Bearer token"})

        # Verify tombstone was written for missing page 102
        self.s3_sink.write_tombstone.assert_called_once_with(
            "ENG", "102", existing_metadata={"title": "Deleted Design", "version_number": 2}
        )

        # Verify active page inventory was committed with {101, 103}
        self.s3_sink.save_active_page_ids.assert_called_once_with("ENG", {"101", "103"})

        # Verify watermark advanced to max modified_ts
        self.s3_sink.save_checkpoint.assert_called_once_with("confluence_ENG_cursor.json", "2026-01-02T11:00:00Z", {"space_key": "ENG"})

    def test_s3_sink_write_tombstone_format_and_manifest(self):
        """Directly tests S3Sink.write_tombstone payload structure, sidecar update, and manifest entry."""
        sink = S3Sink(bucket_name="test-bucket", metrics=self.metrics, run_id="run_20260906_test")
        mock_s3_client = MagicMock()
        sink.s3 = mock_s3_client

        existing_meta = {
            "title": "Legacy Architecture",
            "version_number": 5,
            "author_id": "user-123",
            "parent_id": "root-01"
        }

        sink.write_tombstone("PLATFORM", "999", existing_metadata=existing_meta)

        # 1. Verify S3 put_object for DELETED marker
        marker_call = [c for c in mock_s3_client.put_object.call_args_list if c[1]["Key"] == "raw/confluence/PLATFORM/999/DELETED"]
        self.assertEqual(len(marker_call), 1)
        marker_data = json.loads(marker_call[0][1]["Body"].decode("utf-8"))
        self.assertEqual(marker_data["page_id"], "999")
        self.assertEqual(marker_data["space_key"], "PLATFORM")
        self.assertEqual(marker_data["status"], "DELETE")
        self.assertIn("deleted_at_utc", marker_data)

        # 2. Verify S3 put_object for metadata.json sidecar update
        meta_call = [c for c in mock_s3_client.put_object.call_args_list if c[1]["Key"] == "raw/confluence/PLATFORM/999/metadata.json"]
        self.assertEqual(len(meta_call), 1)
        meta_data = json.loads(meta_call[0][1]["Body"].decode("utf-8"))
        self.assertTrue(meta_data["is_deleted"])
        self.assertEqual(meta_data["status"], "DELETE")
        self.assertEqual(meta_data["title"], "Legacy Architecture")
        self.assertEqual(meta_data["parent_id"], "root-01")

        # 3. Verify manifest entry
        self.assertEqual(len(sink.manifest_entries), 1)
        entry = sink.manifest_entries[0]
        self.assertEqual(entry["item_id"], "999")
        self.assertEqual(entry["status"], "DELETE")
        self.assertEqual(entry["s3_path"], "raw/confluence/PLATFORM/999/DELETED")

        # 4. Verify metrics
        self.assertGreaterEqual(self.metrics.deleted_docs, 1)

    def test_first_run_establishes_baseline_active_ids_without_false_deletes(self):
        """Initial space crawl establishes baseline active IDs without emitting false tombstones."""
        self.s3_sink.get_checkpoint.return_value = None
        # First run: no previous active IDs in S3
        self.s3_sink.get_active_page_ids.return_value = set()

        mock_page_resp = MagicMock()
        mock_page_resp.status_code = 200
        mock_page_resp.json.return_value = {
            "results": [
                {"id": "201", "title": "First Page", "version": {"number": 1, "createdAt": "2026-01-01T10:00:00Z"}}
            ],
            "_links": {}
        }
        self.http_client.request.return_value = mock_page_resp

        with patch.object(self.connector, "_process_page", return_value="2026-01-01T10:00:00Z"):
            self.connector._sync_space("ENG", {"Authorization": "Bearer token"})

        # Zero tombstones emitted on initial baseline run
        self.s3_sink.write_tombstone.assert_not_called()

        # Baseline committed
        self.s3_sink.save_active_page_ids.assert_called_once_with("ENG", {"201"})

    def test_circuit_breaker_prevents_premature_tombstone_emission(self):
        """Tripped circuit breaker halts sync and prevents premature tombstone generation."""
        self.s3_sink.get_checkpoint.return_value = "2026-01-01T00:00:00Z"
        self.s3_sink.get_active_page_ids.return_value = {"301", "302", "303"}

        # Simulate circuit breaker already tripped
        self.connector.circuit_broken = True

        self.connector._sync_space("ENG", {"Authorization": "Bearer token"})

        # Must not emit tombstones or commit state
        self.s3_sink.write_tombstone.assert_not_called()
        self.s3_sink.save_active_page_ids.assert_not_called()
        self.s3_sink.save_checkpoint.assert_not_called()

    def test_poison_pill_quarantine_captures_forensics_and_manifest(self):
        """Validates that S3Sink.write_quarantine stores complete forensics and records manifest."""
        sink = S3Sink(bucket_name="test-bucket", metrics=self.metrics, run_id="run_quarantine_test")
        mock_s3 = MagicMock()
        sink.s3 = mock_s3

        raw_page = {"id": "bad-999", "title": "Corrupt Doc"}
        sink.write_quarantine(
            item_id="bad-999",
            payload=raw_page,
            error_msg="XMLSyntaxError: mismatched tag",
            error_type="XMLSyntaxError",
            stack_trace="Traceback: line 42"
        )

        # Verify S3 upload to quarantine path
        quarantine_calls = [c for c in mock_s3.put_object.call_args_list if c[1]["Key"] == "quarantine/confluence/bad-999/error.json"]
        self.assertEqual(len(quarantine_calls), 1)
        body = json.loads(quarantine_calls[0][1]["Body"].decode("utf-8"))
        self.assertEqual(body["item_id"], "bad-999")
        self.assertEqual(body["error_type"], "XMLSyntaxError")
        self.assertEqual(body["error"], "XMLSyntaxError: mismatched tag")
        self.assertEqual(body["stack_trace"], "Traceback: line 42")
        self.assertEqual(body["raw_payload"]["id"], "bad-999")

        # Verify manifest entry
        self.assertEqual(len(sink.manifest_entries), 1)
        self.assertEqual(sink.manifest_entries[0]["status"], "QUARANTINE")
        self.assertEqual(sink.manifest_entries[0]["item_id"], "bad-999")

        # Verify metric
        self.assertEqual(self.metrics.quarantined_docs, 1)

    def test_space_error_rate_circuit_breaker_trips(self):
        """Validates that space error rate exceeding 10% after 10 items trips circuit breaker."""
        self.connector.consecutive_failures = 0
        self.connector.total_processed = 0
        self.connector.total_failed = 0
        self.connector.circuit_broken = False
        self.connector.max_consecutive_failures = 100  # Ensure consecutive breaker doesn't trip first

        # Process 8 successes, then 2 failures (total = 10, failed = 2 -> 20% > 10%)
        for _ in range(8):
            self.connector._record_success()
        self.assertFalse(self.connector.circuit_broken)

        self.connector._record_failure("p-fail-1", "Parser error 1")
        self.assertFalse(self.connector.circuit_broken)

        # 10th processed item with 20% failure rate -> should trip
        self.connector._record_failure("p-fail-2", "Parser error 2")
        self.assertTrue(self.connector.circuit_broken)

    def test_sync_flushes_manifest_and_raises_on_circuit_breaker(self):
        """Validates that sync() flushes partial manifest and raises RuntimeError if breaker tripped."""
        self.connector.space_keys = ["SPACE_A", "SPACE_B"]
        # Simulate circuit breaker tripped
        self.connector.circuit_broken = True

        with self.assertRaises(RuntimeError) as ctx:
            self.connector.sync()

        self.assertIn("Circuit breaker tripped", str(ctx.exception))
        # Verify batch manifest was still flushed for forensic audit
        self.s3_sink.flush_batch_manifest.assert_called_once()


class TestConfluenceModularComponents(unittest.TestCase):
    """Validates strongly typed models, configuration parsers, and custom domain exceptions."""

    def test_package_exports(self):
        """Validates that all expected modular classes and functions are exported from root package."""
        import src.connectors.confluence as conf_pkg
        self.assertTrue(hasattr(conf_pkg, "ConfluenceConnector"))
        self.assertTrue(hasattr(conf_pkg, "ConfluenceConfig"))
        self.assertTrue(hasattr(conf_pkg, "PageMetadata"))
        self.assertTrue(hasattr(conf_pkg, "ManifestRecord"))
        self.assertTrue(hasattr(conf_pkg, "PageSummary"))
        self.assertTrue(hasattr(conf_pkg, "PageRestrictions"))
        self.assertTrue(hasattr(conf_pkg, "TombstoneMarker"))
        self.assertTrue(hasattr(conf_pkg, "SyncAction"))
        self.assertTrue(hasattr(conf_pkg, "ConfluenceConnectorError"))
        self.assertTrue(hasattr(conf_pkg, "CircuitBreakerTrippedError"))
        self.assertTrue(hasattr(conf_pkg, "CircuitBreaker"))
        self.assertTrue(hasattr(conf_pkg, "ConfluenceSyncEngine"))
        self.assertTrue(hasattr(conf_pkg, "S3Sink"))
        self.assertTrue(hasattr(conf_pkg, "ConfluenceMacroSanitizer"))
        self.assertTrue(hasattr(conf_pkg, "BoundedRateLimiter"))
        self.assertTrue(hasattr(conf_pkg, "ResilientHttpClient"))
        self.assertTrue(hasattr(conf_pkg, "PipelineMetrics"))

    def test_page_summary_from_api_dict(self):
        """Validates parsing Confluence API v2 summary payload into strongly typed PageSummary."""
        from src.connectors.confluence.models import PageSummary

        raw = {
            "id": "12345",
            "title": "Architecture Guide",
            "spaceId": "ENG",
            "version": {"number": 3, "createdAt": "2026-03-01T12:00:00Z"}
        }
        summary = PageSummary.from_api_dict(raw)
        self.assertEqual(summary.id, "12345")
        self.assertEqual(summary.title, "Architecture Guide")
        self.assertEqual(summary.space_key, "ENG")
        self.assertEqual(summary.version_number, 3)
        self.assertEqual(summary.version_created_at, "2026-03-01T12:00:00Z")

    def test_page_restrictions_and_metadata_serialization(self):
        """Validates PageRestrictions and PageMetadata dictionary serialization."""
        from src.connectors.confluence.models import PageRestrictions, PageMetadata

        restr = PageRestrictions(
            read_users=["usr-1", "usr-2"],
            read_groups=["grp-sec"],
            edit_users=["usr-1"],
            edit_groups=["grp-sec"]
        )
        self.assertTrue(restr.is_restricted)
        restr_dict = restr.to_dict()
        self.assertEqual(restr_dict["read_users"], ["usr-1", "usr-2"])
        self.assertEqual(restr_dict["read_groups"], ["grp-sec"])

        meta = PageMetadata(
            page_id="12345",
            title="Design Doc",
            space_key="ENG",
            version_number=2,
            version_created_at="2026-03-01T10:00:00Z",
            storage_format_bytes=1024,
            markdown_bytes=512,
            s3_storage_uri="s3://bkt/raw/storage.xhtml",
            s3_markdown_uri="s3://bkt/clean/page.md",
            restrictions=restr,
            content_hash="abc123hash",
            restrictions_hash="restr456hash"
        )
        meta_dict = meta.to_dict()
        self.assertEqual(meta_dict["page_id"], "12345")
        self.assertEqual(meta_dict["restrictions"]["read_users"], ["usr-1", "usr-2"])
        self.assertEqual(meta_dict["content_hash"], "abc123hash")

    def test_manifest_and_tombstone_serialization(self):
        """Validates ManifestRecord and TombstoneMarker serialization."""
        from src.connectors.confluence.models import ManifestRecord, TombstoneMarker, SyncAction

        record = ManifestRecord(
            item_id="12345",
            action=SyncAction.INSERTED,
            space_key="ENG",
            version_number=1,
            version_created_at="2026-01-01T00:00:00Z",
            s3_markdown_uri="s3://bkt/clean/12345.md",
            s3_sidecar_uri="s3://bkt/sidecars/12345.json"
        )
        rec_dict = record.to_dict()
        self.assertEqual(rec_dict["status"], "INSERT")
        self.assertEqual(rec_dict["item_id"], "12345")

        tombstone = TombstoneMarker(
            page_id="9999",
            space_key="ENG",
            deleted_at_utc="2026-03-06T12:00:00Z",
            purge_confirmed=True
        )
        tomb_dict = tombstone.to_dict()
        self.assertEqual(tomb_dict["page_id"], "9999")
        self.assertTrue(tomb_dict["purge_confirmed"])

    def test_config_parser_and_validation(self):
        """Validates CLI/Glue argument parsing into ConfluenceConfig."""
        from src.connectors.confluence.config import ConfluenceConfig, parse_job_arguments

        cli_args = [
            "--JOB_NAME", "confluence_sync_job",
            "--secret_name", "prod/confluence/api_token",
            "--target_s3_bucket", "my-lake-bucket",
            "--space_keys", "ENG,PROD,DOCS",
            "--max_workers", "8",
            "--rate_limit_rps", "25.0",
            "--circuit_breaker_failures", "7",
            "--circuit_breaker_error_rate", "0.15"
        ]
        parsed = parse_job_arguments(cli_args)
        self.assertEqual(parsed["JOB_NAME"], "confluence_sync_job")
        self.assertEqual(parsed["secret_name"], "prod/confluence/api_token")
        self.assertEqual(parsed["target_s3_bucket"], "my-lake-bucket")
        self.assertEqual(parsed["space_keys"], "ENG,PROD,DOCS")
        self.assertEqual(parsed["max_workers"], "8")

        secrets = {
            "base_url": "https://example.atlassian.net/wiki/",
            "user_email": "admin@example.com",
            "api_token": "token123"
        }
        cfg = ConfluenceConfig.from_dict_and_args(secrets, parsed)
        self.assertEqual(cfg.base_url, "https://example.atlassian.net/wiki")
        self.assertEqual(cfg.user_email, "admin@example.com")
        self.assertEqual(cfg.target_s3_bucket, "my-lake-bucket")
        self.assertEqual(cfg.space_keys, ["ENG", "PROD", "DOCS"])
        self.assertEqual(cfg.max_workers, 8)
        self.assertEqual(cfg.rate_limit_rps, 25.0)
        self.assertEqual(cfg.circuit_breaker_failures, 7)
        self.assertEqual(cfg.circuit_breaker_error_rate, 0.15)

    def test_domain_exception_hierarchy(self):
        """Validates custom Confluence exception inheritance."""
        from src.connectors.confluence.exceptions import (
            ConfluenceConnectorError,
            AuthenticationError,
            RateLimitExceededError,
            CircuitBreakerTrippedError,
            QuarantineError,
            StorageSinkError
        )
        self.assertTrue(issubclass(AuthenticationError, ConfluenceConnectorError))
        self.assertTrue(issubclass(RateLimitExceededError, ConfluenceConnectorError))
        self.assertTrue(issubclass(CircuitBreakerTrippedError, ConfluenceConnectorError))
        self.assertTrue(issubclass(QuarantineError, ConfluenceConnectorError))
        self.assertTrue(issubclass(StorageSinkError, ConfluenceConnectorError))


if __name__ == "__main__":
    unittest.main()



