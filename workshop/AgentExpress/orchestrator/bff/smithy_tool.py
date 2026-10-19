"""A type="smithy" tool's own checks, for validate_build.py.

A Smithy target calls an AWS service that a Smithy model describes, signed by the build's
Gateway role (the only outbound auth AWS documents for it besides an OAuth client, and the
one that fits an AWS API). So the tool says, in `allow`, which AWS actions on which
resources that role may make for it. The rules here keep that grant narrow: named actions
only, full ARNs only, and none of the services in vocabulary.json smithyDeniedServices.

Mirrors web/src/builder/smithy.ts (same words: tests/fixtures/validation_cases.json),
cdk/lib/smithy-tool.ts validateSmithy and the preconditions in terraform/tools_smithy.tf.
https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/gateway-building-smithy-targets.html
"""
from __future__ import annotations

import re

S3_URI_RE = re.compile(r"^s3://[a-z0-9.-]{3,63}/.+")
#: One named IAM action: a service prefix and an action name, no wildcard.
ACTION_RE = re.compile(r"^[a-z0-9-]+:[A-Za-z0-9]+$")
#: A full ARN: region and account written out or left empty (S3, IAM-style globals),
#: never "*", and a resource part that is not a bare wildcard.
ARN_RE = re.compile(r"^arn:aws[a-z-]*:[a-z0-9-]+:([a-z]{2}(-[a-z]+)+-[0-9]+)?:([0-9]{12})?:[^*\s]\S*$")
MAX_ENTRIES = 20


def check(tool: dict, path: str, where: dict, add, denied: list[str]) -> None:
    """Add the issues of one smithy tool. `schema` / `schemaS3Uri` being exactly one, and
    `allow` being present, are the generic key checks' job (app/keys.json)."""
    if "schema" in tool:
        doc = tool.get("schema")
        shapes = doc.get("shapes") if isinstance(doc, dict) else None
        if not isinstance(doc, dict) or not isinstance(doc.get("smithy"), str) or not isinstance(shapes, dict):
            add("error", where, f"{path}.schema", 'must be a Smithy JSON model: {"smithy": "2.0", "shapes": {...}}')
        else:
            kinds = [s for s in shapes.values() if isinstance(s, dict)]
            services = [s for s in kinds if s.get("type") == "service"]
            if not services:
                add("error", where, f"{path}.schema.shapes", "has no service shape: the model must describe one AWS service")
            if not any(s.get("type") == "operation" for s in kinds):
                add("error", where, f"{path}.schema.shapes", "has no operation, so the target would expose no tool")
            if services and not any(isinstance(s.get("traits"), dict) and "aws.protocols#restJson1" in s["traits"]
                                    for s in services):
                add("warning", where, f"{path}.schema.shapes",
                    "names no service using aws.protocols#restJson1: the Gateway supports only the RestJson protocol")
    uri = tool.get("schemaS3Uri")
    if isinstance(uri, str) and not S3_URI_RE.search(uri):
        add("error", where, f"{path}.schemaS3Uri", "must be s3://<bucket>/<key> — the Gateway loads a Smithy model from S3 or inline")
    if "allow" not in tool:
        return
    allow = tool.get("allow")
    if not isinstance(allow, dict):
        add("error", where, f"{path}.allow", 'must be {"actions": [...], "resources": [...]}')
        return
    actions = allow.get("actions")
    if not isinstance(actions, list) or not 0 < len(actions) <= MAX_ENTRIES:
        add("error", where, f"{path}.allow.actions",
            "must list 1 to 20 IAM actions, each one named action like dynamodb:GetItem (no *)")
    else:
        for i, a in enumerate(actions):
            if not isinstance(a, str) or not ACTION_RE.match(a):
                add("error", where, f"{path}.allow.actions[{i}]", "must be one named IAM action, like dynamodb:GetItem (no *)")
            elif a.split(":", 1)[0] in denied:
                add("error", where, f"{path}.allow.actions[{i}]",
                    f"{a.split(':', 1)[0]} is not allowed: a Smithy tool's calls run as the Gateway role every tool shares")
    resources = allow.get("resources")
    if not isinstance(resources, list) or not 0 < len(resources) <= MAX_ENTRIES:
        add("error", where, f"{path}.allow.resources", "must list 1 to 20 resource ARNs")
    else:
        for i, r in enumerate(resources):
            if not isinstance(r, str) or not ARN_RE.match(r):
                add("error", where, f"{path}.allow.resources[{i}]",
                    "must be a full ARN with its region and account written out (or empty where the service has none), not a bare *")
