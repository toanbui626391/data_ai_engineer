#!/usr/bin/env bash
# ==============================================================================
# Package Dependencies as Offline Wheels (.whl) for AWS Glue Python Shell
# Solves PyPI access in strict private VPCs without internet access.
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
DIST_DIR="${ROOT_DIR}/dist/dependencies"

echo "==> 1. Preparing clean distribution directory: ${DIST_DIR}..."
rm -rf "${DIST_DIR}"
mkdir -p "${DIST_DIR}"

echo "==> 2. Downloading binary wheels for Python 3.9 (AWS Glue Python Shell runtime)..."
python3 -m pip download \
    --only-binary=:all: \
    --platform manylinux2014_x86_64 \
    --python-version 39 \
    --implementation cp \
    --dest "${DIST_DIR}" \
    requests>=2.31.0 \
    urllib3>=2.0.0 \
    requests-ntlm>=1.2.0 \
    pydantic>=2.0.0

echo "==> 3. Bundled Wheels in ${DIST_DIR}:"
ls -lh "${DIST_DIR}"

echo ""
echo "To upload these wheels to Amazon S3 for your private VPC Glue jobs:"
echo "    aws s3 sync ${DIST_DIR} s3://<S3_LANDING_BUCKET>/glue_dependencies/"
