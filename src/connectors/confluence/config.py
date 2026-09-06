"""
Configuration dataclass, AWS Secrets Manager retrieval, and argument parsing.
"""

import json
import os
import sys
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
import boto3
from botocore.exceptions import ClientError
from .telemetry import log_json

try:
    from awsglue.utils import getResolvedOptions
    GLUE_ENVIRONMENT = True
except ImportError:
    GLUE_ENVIRONMENT = False


@dataclass
class ConfluenceConfig:
    """Strongly typed runtime configuration for Confluence ingestion connector."""
    base_url: str
    api_token: str
    landing_bucket: str
    user_email: Optional[str] = None
    space_keys: List[str] = field(default_factory=list)
    max_workers: int = 8
    max_requests_per_sec: float = 10.0
    max_consecutive_failures: int = 15
    max_error_rate: float = 0.10
    batch_limit: int = 50

    def __post_init__(self):
        self.base_url = self.base_url.rstrip("/")
        if not self.base_url:
            raise ValueError("Confluence base_url cannot be empty")
        if not self.api_token:
            raise ValueError("Confluence api_token cannot be empty")
        if not self.landing_bucket:
            raise ValueError("landing_bucket cannot be empty")

    @property
    def target_s3_bucket(self) -> str:
        return self.landing_bucket

    @property
    def rate_limit_rps(self) -> float:
        return self.max_requests_per_sec

    @property
    def circuit_breaker_failures(self) -> int:
        return self.max_consecutive_failures

    @property
    def circuit_breaker_error_rate(self) -> float:
        return self.max_error_rate

    @classmethod
    def from_dict_and_args(cls, secrets: Dict[str, Any], args: Dict[str, Any]) -> "ConfluenceConfig":
        raw_spaces = args.get("space_keys") or secrets.get("space_keys", "")
        if isinstance(raw_spaces, str):
            spaces = [s.strip() for s in raw_spaces.split(",") if s.strip()]
        else:
            spaces = list(raw_spaces)

        bucket = (
            args.get("target_s3_bucket")
            or args.get("S3_LANDING_BUCKET")
            or secrets.get("landing_bucket", "")
        )

        return cls(
            base_url=secrets.get("base_url", ""),
            api_token=secrets.get("api_token", ""),
            landing_bucket=bucket,
            user_email=secrets.get("user_email"),
            space_keys=spaces,
            max_workers=int(args.get("max_workers") or args.get("MAX_WORKERS") or 8),
            max_requests_per_sec=float(args.get("rate_limit_rps") or args.get("MAX_REQUESTS_PER_SEC") or 10.0),
            max_consecutive_failures=int(args.get("circuit_breaker_failures") or 15),
            max_error_rate=float(args.get("circuit_breaker_error_rate") or 0.10),
            batch_limit=int(args.get("batch_limit") or 50)
        )


def parse_job_arguments(argv: Optional[List[str]] = None) -> Dict[str, Any]:
    """Resolves AWS Glue job arguments with OS environment fallback and CLI flag support."""
    args: Dict[str, Any] = {}
    cli_tokens = argv if argv is not None else sys.argv[1:]

    i = 0
    while i < len(cli_tokens):
        token = cli_tokens[i]
        if token.startswith("--"):
            key = token.lstrip("-")
            if i + 1 < len(cli_tokens) and not cli_tokens[i + 1].startswith("--"):
                args[key] = cli_tokens[i + 1]
                i += 2
            else:
                args[key] = "true"
                i += 1
        else:
            i += 1

    if GLUE_ENVIRONMENT and argv is None:
        try:
            resolved = getResolvedOptions(
                sys.argv,
                [
                    "S3_LANDING_BUCKET",
                    "CONFLUENCE_SECRET_NAME",
                    "MAX_WORKERS",
                    "MAX_REQUESTS_PER_SEC"
                ]
            )
            args.update(resolved)
        except Exception:
            pass

    for key, env_var in [
        ("S3_LANDING_BUCKET", "S3_LANDING_BUCKET"),
        ("CONFLUENCE_SECRET_NAME", "CONFLUENCE_SECRET_NAME"),
        ("MAX_WORKERS", "MAX_WORKERS"),
        ("MAX_REQUESTS_PER_SEC", "MAX_REQUESTS_PER_SEC")
    ]:
        if key not in args and env_var in os.environ:
            args[key] = os.environ[env_var]

    return args


def fetch_secret(secret_name: str, region_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Retrieves Confluence credentials securely from AWS Secrets Manager."""
    client = boto3.client("secretsmanager", region_name=region_name or os.environ.get("AWS_REGION", "us-east-1"))
    try:
        response = client.get_secret_value(SecretId=secret_name)
        if "SecretString" in response:
            return json.loads(response["SecretString"])
        return None
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            log_json("warning", "Secret not found in Secrets Manager, aborting", secret_name=secret_name)
            return None
        log_json("error", "Failed to retrieve secret from Secrets Manager", secret_name=secret_name, error=str(e))
        raise


def load_config(args: Optional[Dict[str, Any]] = None) -> ConfluenceConfig:
    """Constructs and validates ConfluenceConfig from job arguments and Secrets Manager."""
    job_args = args or parse_job_arguments()
    bucket_name = job_args.get("S3_LANDING_BUCKET")
    if not bucket_name:
        raise ValueError("Missing mandatory argument: S3_LANDING_BUCKET")

    secret_name = job_args.get("CONFLUENCE_SECRET_NAME")
    if not secret_name:
        raise ValueError("Missing mandatory argument: CONFLUENCE_SECRET_NAME")

    secrets = fetch_secret(secret_name)
    if not secrets:
        raise ValueError(f"Secret '{secret_name}' not found or invalid in Secrets Manager")

    raw_spaces = secrets.get("space_keys", "")
    spaces = [s.strip() for s in raw_spaces.split(",") if s.strip()]

    return ConfluenceConfig(
        base_url=secrets["base_url"],
        api_token=secrets["api_token"],
        user_email=secrets.get("user_email"),
        space_keys=spaces,
        landing_bucket=bucket_name,
        max_workers=job_args.get("MAX_WORKERS", 8),
        max_requests_per_sec=job_args.get("MAX_REQUESTS_PER_SEC", 10.0)
    )
