"""
Configuration models and argument parsing for AWS Glue and local execution.
"""

import json
import os
import sys
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

try:
    from awsglue.utils import getResolvedOptions
    GLUE_ENVIRONMENT = True
except ImportError:
    GLUE_ENVIRONMENT = False

import boto3
from botocore.exceptions import ClientError
from .exceptions import AuthenticationError
from .telemetry import log_json


@dataclass
class ConnectorConfig:
    """Strongly-typed runtime configuration for SharePoint connector."""
    s3_landing_bucket: str
    tenant_id: str
    client_id: str
    client_secret: str
    site_id: str
    drive_id: Optional[str] = None
    sharepoint_secret_name: str = "enterprise/rag/sharepoint_auth"
    mode: str = "delta"
    max_workers: int = 4
    max_requests_per_sec: float = 10.0
    heavy_file_threshold_bytes: int = 500 * 1024 * 1024
    max_file_size_bytes: int = 5 * 1024 * 1024 * 1024
    heavy_queue_url: Optional[str] = None
    item_id: Optional[str] = None
    max_consecutive_failures: int = 20
    max_error_rate: float = 0.15
    raw_secrets: Dict[str, str] = field(default_factory=dict)

    @property
    def safe_site_id(self) -> str:
        """Returns safe string representation of site_id suitable for S3 object prefixes."""
        return self.site_id.replace(",", "_").replace("/", "_")


def fetch_secret(secret_name: str) -> Dict[str, Any]:
    """Retrieves secret JSON string from AWS Secrets Manager."""
    client = boto3.client("secretsmanager")
    try:
        resp = client.get_secret_value(SecretId=secret_name)
        log_json("info", "Successfully fetched secret from AWS Secrets Manager", secret_name=secret_name)
        return json.loads(resp["SecretString"])
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            log_json("warning", "Secret not found in Secrets Manager", secret_name=secret_name)
            raise AuthenticationError(f"Secret '{secret_name}' not found in AWS Secrets Manager") from e
        log_json("error", "Failed to retrieve secret from Secrets Manager", secret_name=secret_name, error=str(e))
        raise AuthenticationError(f"Error retrieving secret '{secret_name}': {e}") from e


def parse_job_arguments() -> Dict[str, Any]:
    """Resolves CLI arguments from AWS Glue or environment variables."""
    args: Dict[str, Any] = {}
    if GLUE_ENVIRONMENT:
        resolved = getResolvedOptions(
            sys.argv,
            [
                "S3_LANDING_BUCKET",
                "SHAREPOINT_SECRET_NAME",
                "MAX_WORKERS",
                "MAX_REQUESTS_PER_SEC"
            ]
        )
        args.update(resolved)

        for opt_key in ["MODE", "ITEM_ID", "DRIVE_ID", "HEAVY_FILE_THRESHOLD_BYTES", "HEAVY_QUEUE_URL"]:
            try:
                opt_res = getResolvedOptions(sys.argv, [opt_key])
                args.update(opt_res)
            except Exception:
                pass

    args["S3_LANDING_BUCKET"] = args.get("S3_LANDING_BUCKET") or os.environ.get("S3_LANDING_BUCKET")
    args["SHAREPOINT_SECRET_NAME"] = args.get("SHAREPOINT_SECRET_NAME") or os.environ.get(
        "SHAREPOINT_SECRET_NAME", "enterprise/rag/sharepoint_auth"
    )
    args["MAX_WORKERS"] = int(args.get("MAX_WORKERS") or os.environ.get("MAX_WORKERS", 4))
    args["MAX_REQUESTS_PER_SEC"] = float(args.get("MAX_REQUESTS_PER_SEC") or os.environ.get("MAX_REQUESTS_PER_SEC", 10.0))
    args["MODE"] = (args.get("MODE") or os.environ.get("MODE", "delta")).lower()
    args["ITEM_ID"] = args.get("ITEM_ID") or os.environ.get("ITEM_ID")
    args["DRIVE_ID"] = args.get("DRIVE_ID") or os.environ.get("DRIVE_ID")
    args["HEAVY_FILE_THRESHOLD_BYTES"] = int(
        args.get("HEAVY_FILE_THRESHOLD_BYTES") or os.environ.get("HEAVY_FILE_THRESHOLD_BYTES", str(500 * 1024 * 1024))
    )
    args["MAX_FILE_SIZE_BYTES"] = int(
        args.get("MAX_FILE_SIZE_BYTES") or os.environ.get("MAX_FILE_SIZE_BYTES", str(5 * 1024 * 1024 * 1024))
    )
    args["HEAVY_QUEUE_URL"] = args.get("HEAVY_QUEUE_URL") or os.environ.get("HEAVY_QUEUE_URL")
    return args


def load_config(secrets_override: Optional[Dict[str, str]] = None) -> ConnectorConfig:
    """Loads configuration and credentials into validated ConnectorConfig instance."""
    args = parse_job_arguments()
    bucket = args.get("S3_LANDING_BUCKET")
    if not bucket:
        raise ValueError("Missing required argument: S3_LANDING_BUCKET")

    secret_name = args["SHAREPOINT_SECRET_NAME"]
    secrets = secrets_override or fetch_secret(secret_name)

    required_keys = ["tenant_id", "client_id", "client_secret", "site_id"]
    missing = [k for k in required_keys if k not in secrets]
    if missing:
        raise ValueError(f"Missing mandatory credentials in secret '{secret_name}': {', '.join(missing)}")

    return ConnectorConfig(
        s3_landing_bucket=bucket,
        tenant_id=secrets["tenant_id"],
        client_id=secrets["client_id"],
        client_secret=secrets["client_secret"],
        site_id=secrets["site_id"],
        drive_id=args.get("DRIVE_ID") or secrets.get("drive_id"),
        sharepoint_secret_name=secret_name,
        mode=args["MODE"],
        max_workers=args["MAX_WORKERS"],
        max_requests_per_sec=args["MAX_REQUESTS_PER_SEC"],
        heavy_file_threshold_bytes=args["HEAVY_FILE_THRESHOLD_BYTES"],
        max_file_size_bytes=args["MAX_FILE_SIZE_BYTES"],
        heavy_queue_url=args.get("HEAVY_QUEUE_URL"),
        item_id=args.get("ITEM_ID"),
        raw_secrets=secrets
    )
