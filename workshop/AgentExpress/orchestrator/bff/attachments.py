"""What a file attached for a model may be, shared by the two places that take them:
the AgentExpress Assistant's messages (designer.py) and a run's start (runfiles.py).

Converse takes at most 5 documents of 4.5 MB per message, and images of 3.75 MB. A text
format Converse has no name for (JSON, YAML, code...) is sent as plain text.
"""

from __future__ import annotations

import re

ATTACH_MAX = 5
DOC_MAX_BYTES = 4_500_000
IMAGE_MAX_BYTES = 3_750_000
#: File extension -> (kind, Converse format).
ATTACH_FORMATS = {
    **{e: ("document", e) for e in ("pdf", "csv", "doc", "docx", "xls", "xlsx", "html", "txt", "md")},
    "htm": ("document", "html"), "markdown": ("document", "md"),
    **{e: ("document", "txt") for e in ("json", "yaml", "yml", "xml", "tf", "py", "ts", "js", "sql", "log")},
    "png": ("image", "png"), "jpg": ("image", "jpeg"), "jpeg": ("image", "jpeg"),
    "gif": ("image", "gif"), "webp": ("image", "webp"),
}
S3_URI_RE = re.compile(r"s3://([a-z0-9][a-z0-9.\-]{1,61}[a-z0-9])(/[^\s\"'<>)]*)?")


def format_of(name: str):
    """(kind, format) for a file name, or None when it is not one a model reads."""
    return ATTACH_FORMATS.get(name.rsplit(".", 1)[-1].lower()) if "." in name else None


def problem(name: str, size: int, where: str, reader: str = "the assistant") -> str:
    """Why this file may not be attached, or "" when it may."""
    got = format_of(name)
    if not got:
        return f"{where}: not a file type {reader} reads ({', '.join(sorted(ATTACH_FORMATS))})"
    limit = IMAGE_MAX_BYTES if got[0] == "image" else DOC_MAX_BYTES
    if size > limit:
        return f"{where} is {size / 1e6:.1f} MB; the most is {limit / 1e6:.2f} MB"
    if size <= 0:
        return f"{where} is empty"
    return ""


def kind_of(name: str) -> dict:
    """{kind, format} for a name format_of accepts."""
    got = format_of(name)
    return {"kind": got[0], "format": got[1]}


def allowed(locations, bucket: str, key: str) -> bool:
    """Whether s3://bucket/key is inside one of `locations` (a bucket, or bucket/prefix).
    A prefix is a FOLDER: `bucket/reports` allows `reports/...` and `reports` itself,
    never `reports-private/...` — a plain startswith let a sibling folder through."""
    for p in locations or []:
        p = str(p).strip().strip("/")
        if p == bucket:
            return True
        if p.startswith(bucket + "/"):
            folder = p[len(bucket) + 1:].rstrip("/")
            if key == folder or key.startswith(folder + "/"):
                return True
    return False


def doc_name(name: str, taken: set) -> str:
    """A document name Converse accepts: letters, digits, spaces, hyphens, parentheses and
    brackets, no runs of spaces, unique in the message."""
    stem = name.rsplit(".", 1)[0]
    clean = " ".join(re.sub(r"[^A-Za-z0-9\s\-()\[\]]", " ", stem).split())[:180] or "file"
    out, n = clean, 2
    while out.lower() in taken:
        out, n = f"{clean} {n}", n + 1
    taken.add(out.lower())
    return out


# --- a run's files (runfiles.py), from workflow.json -------------------------------------

def run_enabled(wf: dict) -> bool:
    """Whether some agent of `wf` reads a run's files (`attachments: true`)."""
    agents = (wf or {}).get("agents") or {}
    return any(isinstance(a, dict) and a.get("attachments") is True for a in agents.values())


def run_s3_allowed(wf: dict) -> list[str]:
    """`orchestrator.attachments.s3`: the buckets or bucket/prefix folders a request may name."""
    orch = (wf or {}).get("orchestrator") or {}
    att = orch.get("attachments") if isinstance(orch, dict) else None
    locs = (att or {}).get("s3") if isinstance(att, dict) else None
    return [str(p).strip().strip("/") for p in (locs or []) if str(p).strip()]


def run_view(wf: dict) -> dict | None:
    """What the Start run form may offer, or None: no attach button when no agent would
    read the files."""
    if not run_enabled(wf):
        return None
    return {"maxFiles": ATTACH_MAX, "maxBytes": DOC_MAX_BYTES, "imageMaxBytes": IMAGE_MAX_BYTES,
            "types": sorted(ATTACH_FORMATS), "s3": run_s3_allowed(wf)}
