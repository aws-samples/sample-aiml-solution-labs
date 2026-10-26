"""Track B docgen. The Document generator (a type="lambda" tool with `source: "docgen"`):
what its config may say. For validate_build.py.

The function (app/tools/docgen/handler.py) publishes one tool, `generate_document`, and
reads three settings: `formats` (FORMATS), `templateS3Uri` (TEMPLATE_S3_URI) and
`outputPrefix` (OUTPUT_PREFIX). Each S3 location is also what its role may read or write
(cdk/lib/docgen-tool.ts, terraform/docgen_tool.tf), so a malformed one is caught here,
where the person writing it can see why. It saves every file with the run, so the
deployment must keep run files (orchestrator.files). Mirrors web/src/builder/docgenTool.ts
word for word (both run tests/fixtures/validation_cases.json).
"""
from __future__ import annotations

import re

SOURCE = "docgen"
#: The one tool the function publishes (handler.py TOOL).
TOOL = "generate_document"
KEYS = ("formats", "templateS3Uri", "outputPrefix")
#: Arguments the runtime sets on every call (app/features/gateway/client.py), which the
#: published schema must declare for the Gateway to pass them on.
RUN_ARGS = ("runId", "agent")
#: The build's own storage (Track E, orchestrator.storage), which the deploy resolves.
_WHERE = r"(?:s3://[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]|\$\{storage\})/"
#: One template (a .pptx or .docx object), or a folder of them (ending in /).
TEMPLATE_RE = re.compile(rf"^{_WHERE}(?:(?:[^*?]+/)?|[^*?]*[^/*?]\.(pptx|docx))$")
OUTPUT_RE = re.compile(rf"^{_WHERE}(?:[^*?]+/)?$")
#: The same as validate_build.FRAMEWORK_BUCKET_RE: buckets no tool may reach.
FRAMEWORK_BUCKET_RE = re.compile(r"^s3://(agentcore-|[^/]*builderbuildsbucket)")


def check(tool: dict, path: str, where: dict, add, files_on: bool) -> None:
    """Add an issue for each thing wrong with a lambda tool's Document generator settings."""
    is_docgen = tool.get("source") == SOURCE
    for k in KEYS:
        if k in tool and not is_docgen:
            add("error", where, f"{path}.{k}", f'only the Document generator (source "docgen") reads {k}')
    if not is_docgen:
        return
    formats = tool.get("formats")
    if "formats" not in tool:
        add("error", where, f"{path}.formats", "the Document generator needs `formats`: any of pdf, pptx, docx")
    elif isinstance(formats, list):
        if not formats:
            add("error", where, f"{path}.formats", "list at least one format")
        seen = set()
        for i, f in enumerate(formats):
            if isinstance(f, str):
                if f in seen:
                    add("error", where, f"{path}.formats[{i}]", f'"{f}" is listed twice')
                seen.add(f)
    template = tool.get("templateS3Uri")
    if isinstance(template, str):
        m = TEMPLATE_RE.match(template)
        if not m:
            add("error", where, f"{path}.templateS3Uri",
                "must be one template, s3://<bucket>/<key>.pptx or .docx, or a folder of them, "
                "s3://<bucket>/<prefix>/ (or ${storage}/...)")
        elif FRAMEWORK_BUCKET_RE.match(template):
            add("error", where, f"{path}.templateS3Uri",
                "is one of the framework's own buckets, which no tool may reach")
        elif m.group(1) and isinstance(formats, list) and m.group(1) not in formats:
            add("error", where, f"{path}.templateS3Uri",
                f'is a .{m.group(1)} template, so `formats` must include "{m.group(1)}"')
    output = tool.get("outputPrefix")
    if isinstance(output, str):
        if not OUTPUT_RE.match(output):
            add("error", where, f"{path}.outputPrefix",
                "must be s3://<bucket>/ or s3://<bucket>/<prefix>/, ending in / (or ${storage}/.../)")
        elif FRAMEWORK_BUCKET_RE.match(output):
            add("error", where, f"{path}.outputPrefix",
                "is one of the framework's own buckets, which no tool may reach")
    if not files_on:
        add("error", where, f"{path}.source",
            "the Document generator saves each file with its run: set orchestrator.files (even {}) "
            "so this deployment keeps run files")
    schema = tool.get("toolSchema") if isinstance(tool.get("toolSchema"), list) else []
    for i, ts in enumerate(schema):
        if isinstance(ts, dict) and isinstance(ts.get("name"), str) and ts["name"] and ts["name"] != TOOL:
            add("error", where, f"{path}.toolSchema[{i}].name", f'the Document generator publishes one tool, "{TOOL}"')
        elif isinstance(ts, dict) and ts.get("name") == TOOL:
            props = ts.get("properties") if isinstance(ts.get("properties"), dict) else {}
            missing = [p for p in RUN_ARGS if p not in props]
            if missing:
                add("error", where, f"{path}.toolSchema[{i}].properties",
                    f"declare {' and '.join(missing)} (optional strings): the Gateway passes the function only "
                    "the arguments its schema declares, and the workflow sets runId and agent on every call "
                    "so each file is saved with its run")
