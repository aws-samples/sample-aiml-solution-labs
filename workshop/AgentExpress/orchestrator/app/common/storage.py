"""The build's storage at run time (workflow.json `orchestrator.storage`).

The IaC resolves the `${storage}` token (app/vocabulary.json storageToken) where it builds
something from a value (cdk/lib/storage.ts, terraform/storage.tf). A step that reads its S3
location from workflow.json while it runs resolves it here instead, from STORAGE_BUCKET,
which both IaC paths set on the orchestrator runtime only when the build creates storage.
"""

from __future__ import annotations

import os

from app.common.vocabulary import values

#: What a value writes for the build's bucket: s3://<the bucket>.
TOKEN = values("storageToken")[0]


class NoStorage(RuntimeError):
    """A value names the build's storage, and this deployment has none."""


def bucket() -> str:
    """The build's bucket, or "" when it creates none."""
    return os.getenv("STORAGE_BUCKET", "")


def resolve(value: str) -> str:
    """`value` with the token as s3://<the bucket>; unchanged when it carries none."""
    if TOKEN not in value:
        return value
    name = bucket()
    if not name:
        raise NoStorage(f"{value!r} names the build's storage ({TOKEN}), and this deployment has none "
                        "(no STORAGE_BUCKET): set orchestrator.storage.create to true and deploy again")
    return value.replace(TOKEN, f"s3://{name}")
