"""Files a run is started with, for agents that set `attachments` in workflow.json.

Like the AgentExpress Assistant's attachments (designer.py), two ways in:

  * an UPLOAD: the page asks for a presigned POST (POST /api/sessions/attachments) and
    sends the file straight to the assets bucket, under uploads/<owner>/ — a folder that
    expires after a day, so an upload that is never used is not kept;
  * an S3 PATH in the request text, s3://bucket/key (or s3://bucket/folder/ for the files
    in it), inside a location `orchestrator.attachments.s3` allows.

When the run starts (POST /api/sessions) every file is checked — type, size, at most five
— and COPIED into the run's own folder, runs/<session>/attachments/. The run carries that
list; the runtime reads only keys under its own folder (app/common/attachments.py). So a
re-run reads the same files even after the upload expired or the S3 object changed, and
the runtime needs no access to the customer's buckets: only the BFF reads those.
"""

from __future__ import annotations

import hashlib
import os
import re
import secrets

import attachments as _att
import boto3
import builds

REGION = os.environ.get("AWS_REGION", "us-east-1")
#: The assets bucket (terraform/images.tf, the AssetsBucket in orchestrator-stack.ts),
#: created whenever an agent has `attachments` or draws images.
ASSETS_BUCKET = os.environ.get("ASSETS_BUCKET", "")
UPLOAD_TTL_S = 900
_clients: dict = {}


def _s3():
    if "s3" not in _clients:
        _clients["s3"] = boto3.client("s3", region_name=REGION)
    return _clients["s3"]


def _workflow() -> dict:
    import workflow
    return workflow.RAW


def enabled() -> bool:
    """Whether some agent of this deployment's workflow reads a run's files."""
    return _att.run_enabled(_workflow())


def s3_allowed() -> list[str]:
    return _att.run_s3_allowed(_workflow())


def _folder(owner: str) -> str:
    """The caller's upload folder: a digest, so the key shows no identity."""
    return f"uploads/{hashlib.sha256(str(owner).encode()).hexdigest()[:16]}/"


def _need_bucket() -> str:
    if not enabled():
        raise builds.BuildError(404, "no agent in this workflow reads attached files")
    if not ASSETS_BUCKET:
        raise builds.BuildError(404, "this deployment stores no run files (no assets bucket)")
    return ASSETS_BUCKET


def upload(owner: str, name: str) -> dict:
    """A presigned POST for one file to attach to the next run."""
    bucket = _need_bucket()
    name = os.path.basename(str(name or "")).strip()[:200]
    if not name:
        raise builds.BuildError(400, "name required")
    why = _att.problem(name, 1, name, "an agent")
    if why:
        raise builds.BuildError(400, why)
    key = f"{_folder(owner)}{secrets.token_hex(4)}-{name}"
    post = _s3().generate_presigned_post(Bucket=bucket, Key=key, ExpiresIn=UPLOAD_TTL_S,
                                         Conditions=[["content-length-range", 1, _att.DOC_MAX_BYTES]])
    return {"url": post["url"], "fields": post["fields"], "key": key, "name": name,
            "maxBytes": _att.DOC_MAX_BYTES}


def _safe(name: str) -> str:
    """A file name safe as the last part of an S3 key."""
    return re.sub(r"[^A-Za-z0-9._()\- ]", "_", name)[:120] or "file"


def resolve(owner: str, uploads, topic: str) -> list[dict]:
    """The files a run brings: the caller's uploads, and every s3:// path in its request
    (only when some agent reads them). Checked NOW, so a wrong one is refused with the
    reason before the run starts. [] when there are none."""
    uploads = [u for u in (uploads or []) if u]
    if not enabled():
        if uploads:
            raise builds.BuildError(400, "no agent in this workflow reads attached files")
        return []
    bucket = ASSETS_BUCKET
    out: list[dict] = []
    folder = _folder(owner)
    for u in uploads:
        key = str(u.get("key") or "") if isinstance(u, dict) else ""
        if not key.startswith(folder) or "/" in key[len(folder):]:
            raise builds.BuildError(400, "an attachment is not one of your uploads")
        name = str(u.get("name") or key.rsplit("/", 1)[-1].split("-", 1)[-1])
        try:
            size = int(_s3().head_object(Bucket=bucket, Key=key)["ContentLength"])
        except Exception:  # noqa: BLE001 - absent: the upload did not finish, or expired
            raise builds.BuildError(400, f"{name} has not finished uploading") from None
        why = _att.problem(name, size, name, "an agent")
        if why:
            raise builds.BuildError(400, why)
        out.append({"source": "upload", "bucket": bucket, "srcKey": key, "name": name,
                    "size": size, **_att.kind_of(name)})
    allowed = s3_allowed()
    for m in _att.S3_URI_RE.finditer(topic or ""):
        uri, b, key = m.group(0).rstrip(".,;:"), m.group(1), (m.group(2) or "/").lstrip("/").rstrip(".,;:")
        if not allowed:
            raise builds.BuildError(400, f"{uri}: this workflow reads no S3 paths; list the bucket "
                                         "in orchestrator.attachments.s3 to allow it")
        if not _att.allowed(allowed, b, key):
            raise builds.BuildError(400, f"{uri} is not in a location this workflow may read "
                                         f"(allowed: {', '.join(allowed)})")
        try:
            if not key or key.endswith("/"):
                found = [o for o in _s3().list_objects_v2(Bucket=b, Prefix=key, MaxKeys=50)
                         .get("Contents", []) if not o["Key"].endswith("/")]
                if not found:
                    raise builds.BuildError(400, f"{uri} holds no files")
                objs = [(o["Key"], int(o["Size"])) for o in found[:_att.ATTACH_MAX + 1]]
            else:
                objs = [(key, int(_s3().head_object(Bucket=b, Key=key)["ContentLength"]))]
        except builds.BuildError:
            raise
        except Exception as e:  # noqa: BLE001 - not found, or not readable by this deployment
            code = getattr(e, "response", {}).get("Error", {}).get("Code") or type(e).__name__
            raise builds.BuildError(400, f"{uri} could not be read ({code})") from None
        for k, size in objs:
            name = k.rsplit("/", 1)[-1]
            why = _att.problem(name, size, f"s3://{b}/{k}", "an agent")
            if why:
                raise builds.BuildError(400, why)
            out.append({"source": "s3", "uri": f"s3://{b}/{k}", "bucket": b, "srcKey": k,
                        "name": name, "size": size, **_att.kind_of(name)})
    if len(out) > _att.ATTACH_MAX:
        raise builds.BuildError(400, f"at most {_att.ATTACH_MAX} files per run (this one brings {len(out)})")
    return out


def copy_in(session_id: str, files: list[dict]) -> list[dict]:
    """Copy each file into the run's own folder; return what the run carries."""
    out = []
    for n, f in enumerate(files, 1):
        key = f"runs/{session_id}/attachments/{n}-{_safe(f['name'])}"
        _s3().copy_object(Bucket=ASSETS_BUCKET, Key=key,
                          CopySource={"Bucket": f["bucket"], "Key": f["srcKey"]})
        out.append({"key": key, "name": f["name"], "kind": f["kind"], "format": f["format"],
                    "size": f["size"], **({"uri": f["uri"]} if f.get("uri") else {})})
    return out
