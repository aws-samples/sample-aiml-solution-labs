"""A run's files, for the run page's Files tab and the app's Files page.

    GET /api/sessions/{id}/files              -> the run's inputs, outputs and code
    GET /api/sessions/{id}/files/url?key=...  -> a 5-minute link to one of them
    GET /api/files[?page=n][&scope=all]       -> the files of the caller's recent runs, a page at a time

Everything a run touches is in the assets bucket under runs/<session>/ (the layout is in
app/common/artifacts.py): the files it was started with, the images its agents drew, the
files they saved and the code the sandbox ran. The handler checks that the caller may read
the run (its owner, an admin, a service trigger's approvers: handler._own_session) BEFORE
anything here reads the bucket, and a link is signed only for a key under that run's own
folder, so a run id the caller may read never opens another run's files.

A link opens in the browser only for an image or a PDF. Everything else is sent as a
download (Content-Disposition: attachment) with a content type chosen here from the
extension, never the one stored with the object, so a file an agent named .html or saved
as text/html is never rendered as a page from the bucket's origin. The page reads text
through the link and shows it as text.

A run of a deployed build keeps its files in that build's own bucket; its own app shows
them, so this deployment lists none.
"""

from __future__ import annotations

import contextlib
import os
import re
import urllib.parse

import boto3

REGION = os.environ.get("AWS_REGION", "us-east-1")
ASSETS_BUCKET = os.environ.get("ASSETS_BUCKET", "")
ROUTES = ("GET /api/sessions/{id}/files", "GET /api/sessions/{id}/files/url", "GET /api/files")

SESSION_RE = re.compile(r"^[A-Za-z0-9_-]{6,64}$")
#: A key the page may ask a link for: printable, no `..`, no empty path part.
_KEY_RE = re.compile(r"^runs/[A-Za-z0-9_-]{6,64}(/[A-Za-z0-9._()\- ]+)+$")
LINK_TTL_S = 300
#: Objects listed per run, and output files whose agent is read (one HeadObject each).
MAX_PER_RUN = 500
MAX_HEADS = 50
RUNS_PER_PAGE = 10
MAX_PAGE = 1000

#: Extension -> (content type served, preview kind). Anything else is a plain download.
TYPES = {
    "png": ("image/png", "image"), "jpg": ("image/jpeg", "image"), "jpeg": ("image/jpeg", "image"),
    "gif": ("image/gif", "image"), "webp": ("image/webp", "image"),
    "pdf": ("application/pdf", "pdf"),
    "txt": ("text/plain; charset=utf-8", "text"), "log": ("text/plain; charset=utf-8", "text"),
    "py": ("text/plain; charset=utf-8", "text"), "js": ("text/plain; charset=utf-8", "text"),
    "ts": ("text/plain; charset=utf-8", "text"),
    "md": ("text/markdown; charset=utf-8", "markdown"),
    "csv": ("text/csv; charset=utf-8", "csv"),
    "json": ("application/json", "json"),
}
#: Shown in the browser rather than downloaded: images (never SVG, which can carry script)
#: and PDF. Text is downloaded when opened and read by the page through fetch.
INLINE = {"image", "pdf"}

_clients: dict = {}


class FilesError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status


def _s3():
    if "s3" not in _clients:
        _clients["s3"] = boto3.client("s3", region_name=REGION)
    return _clients["s3"]


def kind_of(key: str) -> tuple[str, str]:
    """(content type to serve, preview kind) for a key, from its extension."""
    ext = key.rsplit("/", 1)[-1].rsplit(".", 1)[-1].lower() if "." in key.rsplit("/", 1)[-1] else ""
    return TYPES.get(ext, ("application/octet-stream", ""))


def describe(session_id: str, key: str) -> dict | None:
    """What a key under runs/<session>/ is: {group, name, agent}, or None for something
    that is not a run file (a folder marker, another run's key)."""
    own = f"runs/{session_id}/"
    if not key.startswith(own) or key.endswith("/"):
        return None
    parts = key[len(own):].split("/")
    if len(parts) != 2 or not parts[1]:
        return None
    folder, name = parts
    if folder == "attachments":
        return {"group": "input", "name": re.sub(r"^\d+-", "", name) or name, "agent": ""}
    if folder == "outputs":
        return {"group": "output", "name": name, "agent": ""}
    if folder == "code":
        stem = name.rsplit(".", 1)[0]
        return {"group": "code", "name": name, "agent": stem.rsplit("-", 1)[0] if "-" in stem else ""}
    # An image an image agent drew: runs/<session>/<agent>/<n>.<ext> (app/common/images.py).
    return {"group": "output", "name": f"{folder}-{name}", "agent": folder}


def _iso(t) -> str:
    return t.isoformat() if hasattr(t, "isoformat") else str(t or "")


def list_run(session_id: str, bucket: str | None = None, heads: list | None = None) -> tuple[list[dict], bool]:
    """Every file of one run, oldest first, and whether the list was cut at MAX_PER_RUN.
    `heads` counts HeadObject calls across runs, so a page of runs stays bounded."""
    bucket = bucket if bucket is not None else ASSETS_BUCKET
    heads = heads if heads is not None else [0]
    out: list[dict] = []
    kw = {"Bucket": bucket, "Prefix": f"runs/{session_id}/", "MaxKeys": 1000}
    cut = False
    while True:
        page = _s3().list_objects_v2(**kw)
        for o in page.get("Contents", []):
            d = describe(session_id, o["Key"])
            if d is None:
                continue
            if len(out) >= MAX_PER_RUN:
                cut = True
                break
            ctype, preview = kind_of(o["Key"])
            out.append({"key": o["Key"], **d, "size": int(o.get("Size") or 0), "type": ctype,
                        "preview": preview, "time": _iso(o.get("LastModified")), "session": session_id})
        if cut or not page.get("IsTruncated"):
            break
        kw["ContinuationToken"] = page["NextContinuationToken"]
    # Which agent saved each output: recorded in its metadata (artifacts.put_output).
    for f in out:
        if f["group"] == "output" and not f["agent"] and heads[0] < MAX_HEADS:
            heads[0] += 1
            # Removed meanwhile (retention): it is listed without an agent.
            with contextlib.suppress(Exception):
                f["agent"] = str((_s3().head_object(Bucket=bucket, Key=f["key"]).get("Metadata") or {})
                                 .get("agent") or "")
    out.sort(key=lambda f: (f["time"], f["key"]))
    return out, cut


def _disposition(name: str, inline: bool) -> str:
    """RFC 6266: an ASCII fallback, and the real name percent-encoded."""
    ascii_name = re.sub(r'[^A-Za-z0-9._()\- ]', "_", name)[:120] or "file"
    return (f'{"inline" if inline else "attachment"}; filename="{ascii_name}"; '
            f"filename*=UTF-8''{urllib.parse.quote(name[:200], safe='')}")


def link(session_id: str, key: str, bucket: str | None = None) -> dict:
    """A 5-minute GET link to one file of THIS run."""
    bucket = bucket if bucket is not None else ASSETS_BUCKET
    key = str(key or "")
    if (len(key) > 1024 or not _KEY_RE.match(key) or ".." in key
            or not key.startswith(f"runs/{session_id}/") or describe(session_id, key) is None):
        raise FilesError(400, "not a file of this run")
    ctype, preview = kind_of(key)
    d = describe(session_id, key)
    url = _s3().generate_presigned_url("get_object", ExpiresIn=LINK_TTL_S, Params={
        "Bucket": bucket, "Key": key, "ResponseContentType": ctype,
        "ResponseContentDisposition": _disposition(d["name"], preview in INLINE)})
    return {"url": url, "expiresIn": LINK_TTL_S, "type": ctype, "preview": preview, "name": d["name"]}


def run_files(session_id: str, build: str = "", bucket: str | None = None) -> dict:
    bucket = bucket if bucket is not None else ASSETS_BUCKET
    if build:
        return {"stored": False, "files": [], "note": "This run's files are in the build's own app."}
    if not bucket:
        return {"stored": False, "files": []}
    files, cut = list_run(session_id, bucket)
    return {"stored": True, "files": files, "truncated": cut}


def page_number(qs: dict) -> int:
    raw = str((qs or {}).get("page") or "0")
    if not raw.isdigit() or int(raw) > MAX_PAGE:
        raise FilesError(400, f"page is a whole number, 0 to {MAX_PAGE}")
    return int(raw)


def recent_files(runs: list[dict], page: int, bucket: str | None = None) -> dict:
    """The files of one page of runs (newest run first): `runs` is the caller's run rows
    already filtered to what they may see, each {session_id, topic, created}."""
    bucket = bucket if bucket is not None else ASSETS_BUCKET
    if not bucket:
        return {"stored": False, "files": [], "nextPage": None}
    ordered = sorted(runs, key=lambda r: str(r.get("created", "")), reverse=True)
    chosen = ordered[page * RUNS_PER_PAGE:(page + 1) * RUNS_PER_PAGE]
    heads = [0]
    files: list[dict] = []
    for r in chosen:
        sid = str(r.get("session_id") or "")
        if not SESSION_RE.match(sid):
            continue
        got, _ = list_run(sid, bucket, heads)
        files.extend({**f, "topic": str(r.get("topic") or "")[:200]} for f in reversed(got))
    more = len(ordered) > (page + 1) * RUNS_PER_PAGE
    return {"stored": True, "files": files, "nextPage": page + 1 if more else None,
            "runs": len(chosen)}
