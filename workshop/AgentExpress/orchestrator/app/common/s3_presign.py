"""The S3 client every presigned URL and presigned POST is made with.
A browser opens these URLs itself, so the host in them matters. A plain
`boto3.client("s3", region_name=r)` signs with SigV2 for the GLOBAL host
`<bucket>.s3.amazonaws.com`. For a bucket outside us-east-1, and above all a new one, S3 can
answer that host with a 307 redirect to the bucket's Region while the name propagates
(https://docs.aws.amazon.com/AmazonS3/latest/developerguide/UsingRouting.html). A browser
does not follow a redirect of a cross-origin form POST, so an upload fails with "Failed to
fetch". This client signs with SigV4 for the bucket's own Region and its virtual-hosted
regional host, `<bucket>.s3.<region>.amazonaws.com`
(https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html), us-east-1
included. botocore picks the host for the partition (amazonaws.com.cn in China), so no
endpoint URL is built by hand here.
A SigV4 signature holds the Region: sign for the Region the BUCKET is in, not the caller's.
Standard library and boto3 only, because bff/s3_presign.py is this file copied (the BFF
ships as its own zip and cannot import app.*); tests/test_s3_presign.py holds the two to the
same syntax tree.
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
