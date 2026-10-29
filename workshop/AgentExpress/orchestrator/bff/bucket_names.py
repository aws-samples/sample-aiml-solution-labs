"""The S3 bucket names a deployment gives its own buckets, in both naming schemes.
Mirrors cdk/lib/bucket-names.ts and terraform/bucket_names.tf; tests/fixtures/bucket_names.json
holds all three to the same names.

A general purpose bucket name is unique across every account and Region of a partition, and
a deleted name "might not become available immediately"
(https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html). A build
destroyed in one Region and deployed in another minutes later then fails to create its
bucket (409, "A conflicting conditional operation is currently in progress").

  * "legacy": the names every deployment had before, with no Region in most of them. The
    default of both IaC paths, so upgrading never renames a bucket: CloudFormation and
    Terraform REPLACE a bucket whose name changes, which loses what is in it.
  * "regional": the Region is the last part of every name, so the same build in another
    Region never collides with its old names. The deploy runner chooses it only for a build
    with nothing deployed (deployer/runner.py bucket_naming) and records it on the build.

A regional name over 63 characters keeps its prefix, its `-<kind>-<account>-<region>` and
an 8-character digest of the agent name, and cuts the agent name to fit, so every IAM
pattern on these names still matches, and two long agent names that begin the same way
still get different names.
"""
from __future__ import annotations

import hashlib

SCHEMES = ("legacy", "regional")
#: kind -> the prefix before the agent name. Not agentcore-* for storage: that prefix is the
#: framework's own, which every code tool's boundary denies (cdk/lib/storage.ts).
KINDS = {"assets": "agentcore-", "kbdocs": "agentcore-", "schemas": "agentcore-", "ui": "agentcore-",
         "ui-logs": "agentcore-", "storage": ""}
#: https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
MAX_LEN = 63
RESERVED_PREFIXES = ("xn--", "sthree-", "amzn-s3-demo-")
DIGEST_LEN = 8


class BucketNameError(ValueError):
    """A name S3 would refuse."""


def _legacy(kind: str, d: str, account: str, region: str, tool: str) -> str:
    pre = KINDS[kind]
    # The CDK UI bucket always had the Region (lib/orchestrator-stack.ts UiBucket); the
    # Terraform one did not (ui.tf). CDK leaves the UI logs bucket's name to CloudFormation.
    if kind == "ui" and tool == "cdk":
        return f"{pre}{d}-ui-{account}-{region}"
    return f"{pre}{d}-{kind}-{account}"


def name(kind: str, agent: str, account: str, region: str, scheme: str = "legacy",
         tool: str = "cdk") -> str:
    """The bucket of `kind` for `agent` in `account` and `region`, under `scheme`."""
    if kind not in KINDS:
        raise BucketNameError(f"unknown bucket kind {kind!r}")
    if scheme not in SCHEMES:
        raise BucketNameError(f"bucketNaming must be one of {', '.join(SCHEMES)}, not {scheme!r}")
    if scheme == "legacy":
        return _legacy(kind, agent.replace("_", "-"), account, region, tool)
    pre, d = KINDS[kind], agent.replace("_", "-").lower()
    tail = f"-{kind}-{account}-{region}"
    out = f"{pre}{d}{tail}"
    if len(out) > MAX_LEN:
        h = hashlib.sha256(agent.encode()).hexdigest()[:DIGEST_LEN]
        keep = max(0, MAX_LEN - len(pre) - len(tail) - DIGEST_LEN - 1)
        t = d[:keep].rstrip("-")
        out = f"{pre}{t}-{h}{tail}" if t else f"{pre}{h}{tail}"
    if len(out) > MAX_LEN:
        raise BucketNameError(f"the bucket name {out} is over S3's {MAX_LEN} characters")
    if out.startswith(RESERVED_PREFIXES):
        raise BucketNameError(f"the bucket name {out} starts with a prefix S3 reserves "
                              f"({', '.join(RESERVED_PREFIXES)}); use another agent name")
    return out


def all_names(agent: str, account: str, region: str) -> set[str]:
    """Every name this agent's buckets can have in `account` and `region`: both schemes,
    both tools. What the deploy runner may empty when it clears a failed first create."""
    out = set()
    for kind in KINDS:
        for scheme in SCHEMES:
            for tool in ("cdk", "terraform"):
                try:
                    out.add(name(kind, agent, account, region, scheme, tool))
                except BucketNameError:
                    continue
    return out
