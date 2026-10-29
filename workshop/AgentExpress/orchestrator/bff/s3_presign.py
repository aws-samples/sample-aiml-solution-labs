"""A copy of app/common/s3_presign.py: the S3 client every presigned URL and presigned POST
is made with. The BFF ships as its own zip and cannot import app.*. tests/test_s3_presign.py
holds the two to the same syntax tree, so change both. It signs with SigV4 for the bucket's
own Region and its regional virtual-hosted host (<bucket>.s3.<region>.amazonaws.com), so a
browser never meets the 307 redirect S3 can send from the global host. See the original.
"""
from __future__ import annotations

import os

_clients: dict = {}


def client(region: str = ""):
    """The presigning S3 client for buckets in `region` (default: this one's AWS_REGION)."""
    region = region or os.environ.get("AWS_REGION") or "us-east-1"
    if region not in _clients:
        import boto3
        from botocore.config import Config
        _clients[region] = boto3.client("s3", region_name=region, config=Config(
            signature_version="s3v4", retries={"mode": "standard"},
            s3={"addressing_style": "virtual", "us_east_1_regional_endpoint": "regional"}))
    return _clients[region]
