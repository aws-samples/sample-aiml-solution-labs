"""The files a run was started with, for agents that set `attachments` in workflow.json.

The BFF (bff/runfiles.py) checks each upload or s3:// path when the run starts and copies
it into the run's own folder in the assets bucket, runs/<session>/attachments/. The run
then carries a list of {key, name, kind, format, size}, in the graph state (State) and,
for a dedicated agent, in its InvokeAgentRuntime payload. Here they are read back as
Converse content: a document block per document, an image block per image.

Only keys under this run's own folder are read, so a list that names anything else in
the bucket, or another run's files, reads nothing.
"""

from __future__ import annotations

import os
import re

#: Converse: at most 5 documents per message. Images share the 20-per-message limit with
#: `vision`, which caps itself; uploads are at most 5 files in total (bff/runfiles.py).
MAX_FILES = 5
_DOC_FORMATS = {"pdf", "csv", "doc", "docx", "xls", "xlsx", "html", "txt", "md"}
_IMAGE_FORMATS = {"png", "jpeg", "gif", "webp"}

_clients: dict = {}


def _bucket() -> str:
    return os.getenv("ASSETS_BUCKET", "")


def _s3():
    if "s3" not in _clients:
        import boto3
        _clients["s3"] = boto3.client("s3", region_name=os.getenv("AWS_REGION") or "us-east-1")
    return _clients["s3"]


def doc_name(name: str, taken: set) -> str:
    """A document name Converse accepts: letters, digits, spaces, hyphens, parentheses and
    brackets, no runs of spaces, unique in the message. Same rule as bff/attachments.py."""
    stem = name.rsplit(".", 1)[0]
    clean = " ".join(re.sub(r"[^A-Za-z0-9\s\-()\[\]]", " ", stem).split())[:180] or "file"
    out, n = clean, 2
    while out.lower() in taken:
        out, n = f"{clean} {n}", n + 1
    taken.add(out.lower())
    return out


def mine(files, session_id: str) -> list[dict]:
    """The entries of `files` that are this run's own, well-formed attachments."""
    own = f"runs/{session_id}/attachments/"
    out = []
    for f in files or []:
        if not isinstance(f, dict):
            continue
        key, kind, fmt = str(f.get("key") or ""), f.get("kind"), str(f.get("format") or "")
        if not key.startswith(own) or "/" in key[len(own):] or ".." in key:
            continue
        if (kind == "document" and fmt in _DOC_FORMATS) or (kind == "image" and fmt in _IMAGE_FORMATS):
            out.append(f)
    return out[:MAX_FILES]


def load_for(files, *, session_id: str, bucket: str | None = None) -> list[dict]:
    """Read this run's attachments: [{kind, format, name, bytes}], documents named so
    Converse accepts them. [] when there are none."""
    own = mine(files, session_id)
    if not own:
        return []
    bucket = bucket if bucket is not None else _bucket()
    if not bucket:
        raise RuntimeError("this deployment has no assets bucket (ASSETS_BUCKET), so there are no "
                           "run files to read; an agent with `attachments` creates it")
    taken: set = set()
    out = []
    for f in own:
        data = _s3().get_object(Bucket=bucket, Key=f["key"])["Body"].read()
        name = str(f.get("name") or f["key"].rsplit("/", 1)[-1])
        out.append({"kind": f["kind"], "format": f["format"], "name": name, "bytes": data,
                    **({"docName": doc_name(name, taken)} if f["kind"] == "document" else {})})
    return out


def blocks(files: list[dict]) -> list[dict]:
    """Converse content blocks for what load_for returned."""
    out = []
    for f in files or []:
        if f["kind"] == "image":
            out.append({"image": {"format": f["format"], "source": {"bytes": f["bytes"]}}})
        else:
            out.append({"document": {"format": f["format"], "name": f["docName"],
                                     "source": {"bytes": f["bytes"]}}})
    return out
