"""Files a run makes: what an agent saves, and the code a sandbox ran.

Everything a run touches lives in the deployment's assets bucket under the run's own
folder, so the BFF can check that the caller may see the run before it lists or links a
file (bff/files.py), and the bucket's retention rule (orchestrator.files.retentionDays)
removes them together:

    runs/<session>/attachments/<n>-<name>   the files the run was started with (bff/runfiles.py)
    runs/<session>/<agent>/<n>.png|jpg      images an image agent drew (app/common/images.py)
    runs/<session>/outputs/<name>           files an agent saved (put_output, ctx.save_file)
    runs/<session>/code/<agent>-<n>.<ext>   code the codeInterpreter tool ran (put_code)
    runs/<session>/code/<agent>-<n>.log     ... and its stdout and stderr

This layout is a contract: the BFF, the run page and other node types build on it.

Each object records the agent that made it in its metadata (x-amz-meta-agent). A name is
reduced to its last path part and to letters, digits and ._()- and space, so a name can
never climb out of the run's folder. The same name saved twice replaces the first: a
re-run of a step writes its file again, which is what a reader expects to see.
"""

from __future__ import annotations

import os
import re
import time

#: The biggest file an agent may save. put_object sends it in one request and the runtime
#: holds it in memory, so a runaway agent cannot fill the bucket or the runtime with one
#: call. Anything larger belongs in the customer's own storage.
MAX_OUTPUT_BYTES = 25_000_000
#: The code and log kept for one sandbox call. The tool itself refuses code over 50,000
#: bytes and cuts each stream at 10,000 characters (app/tools/codeInterpreter/handler.py),
#: so these only bound what a different caller might pass.
MAX_CODE_BYTES = 50_000
MAX_LOG_BYTES = 1_000_000
#: A name's length, extension included. S3 takes 1,024 bytes of key; the run's folder
#: and the agent use some of them, and a reader needs a name, not a paragraph.
MAX_NAME = 120
#: The languages the sandbox runs (the service's ProgrammingLanguage enum), and their files.
CODE_EXT = {"python": "py", "javascript": "js", "typescript": "ts"}

_SESSION_RE = re.compile(r"^[A-Za-z0-9_-]{6,64}$")
_AGENT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")
_TYPE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9!#$&^_.+-]{0,126}/[A-Za-z0-9][A-Za-z0-9!#$&^_.+-]{0,126}$")

_clients: dict = {}


class NoFileStore(RuntimeError):
    """This deployment has no assets bucket, so a run cannot keep files."""


def _bucket() -> str:
    return os.getenv("ASSETS_BUCKET", "")


def _s3():
    if "s3" not in _clients:
        import boto3
        _clients["s3"] = boto3.client("s3", region_name=os.getenv("AWS_REGION") or "us-east-1")
    return _clients["s3"]


def safe_name(name: str) -> str:
    """A file name safe as the last part of a key: its last path part, only letters,
    digits and ._()- and space, no leading dot (so never `.` or `..`), at most MAX_NAME
    characters with the extension kept. "file" when nothing is left."""
    base = re.split(r"[/\\]", str(name or ""))[-1]
    clean = re.sub(r"[^A-Za-z0-9._()\- ]", "_", base).strip(" .")
    clean = re.sub(r"\.{2,}", ".", clean)
    if not clean:
        return "file"
    if len(clean) > MAX_NAME:
        stem, dot, ext = clean.rpartition(".")
        ext = ext[:16] if dot and stem else ""
        clean = (clean[:MAX_NAME - len(ext) - 1].rstrip(" .") + "." + ext) if ext else clean[:MAX_NAME]
    return clean


def _check_ids(session_id: str, agent_id: str) -> None:
    if not _SESSION_RE.match(str(session_id or "")):
        raise ValueError(f"not a run id: {session_id!r}")
    if not _AGENT_RE.match(str(agent_id or "")):
        raise ValueError(f"not an agent id: {agent_id!r}")


def _need_bucket() -> str:
    bucket = _bucket()
    if not bucket:
        raise NoFileStore("this deployment keeps no run files (no assets bucket, ASSETS_BUCKET): "
                          "set orchestrator.files in workflow.json and deploy again")
    return bucket


def put_output(session_id: str, name: str, data: bytes, content_type: str, agent_id: str) -> str:
    """Save one file an agent made, as runs/<session>/outputs/<name>. Returns the key."""
    _check_ids(session_id, agent_id)
    if isinstance(data, str):
        data = data.encode("utf-8")
    if not isinstance(data, (bytes, bytearray)):
        raise TypeError("data must be bytes (or text)")
    if len(data) > MAX_OUTPUT_BYTES:
        raise ValueError(f"{safe_name(name)} is {len(data) / 1e6:.1f} MB; an agent may save files "
                         f"of at most {MAX_OUTPUT_BYTES / 1e6:.0f} MB")
    ctype = str(content_type or "").strip()
    if not _TYPE_RE.match(ctype):
        ctype = "application/octet-stream"
    bucket = _need_bucket()
    key = f"runs/{session_id}/outputs/{safe_name(name)}"
    _s3().put_object(Bucket=bucket, Key=key, Body=bytes(data), ContentType=ctype,
                     Metadata={"agent": agent_id})
    return key


def _log_text(stdout: str, stderr: str) -> bytes:
    text = f"=== stdout ===\n{stdout or ''}\n=== stderr ===\n{stderr or ''}\n"
    raw = text.encode("utf-8")
    if len(raw) > MAX_LOG_BYTES:
        raw = raw[:MAX_LOG_BYTES] + b"\n[... cut at 1 MB]\n"
    return raw


def put_code(session_id: str, agent_id: str, language: str, code: str, stdout: str,
             stderr: str) -> str:
    """Keep the code a sandbox ran and what it printed: runs/<session>/code/<agent>-<n>.<ext>
    and the .log beside it, n the time in milliseconds. Returns the code's key."""
    _check_ids(session_id, agent_id)
    ext = CODE_EXT.get(str(language or "").strip().lower(), "txt")
    body = str(code or "").encode("utf-8")
    if len(body) > MAX_CODE_BYTES:
        body = body[:MAX_CODE_BYTES]
    bucket = _need_bucket()
    stem = f"runs/{session_id}/code/{agent_id}-{int(time.time() * 1000)}"
    meta = {"agent": agent_id, "language": ext}
    _s3().put_object(Bucket=bucket, Key=f"{stem}.{ext}", Body=body,
                     ContentType="text/plain; charset=utf-8", Metadata=meta)
    _s3().put_object(Bucket=bucket, Key=f"{stem}.log", Body=_log_text(stdout, stderr),
                     ContentType="text/plain; charset=utf-8", Metadata=meta)
    return f"{stem}.{ext}"


#: The one tool the built-in sandbox (`source: "codeInterpreter"`) publishes.
CODE_TOOL = "execute_code"


def is_code_tool(tool: dict | None, published: str) -> bool:
    """Whether a call is to the sandbox: a `type: "lambda"` tool publishing execute_code.
    Told by the published name, because the runtime's copy of a tool entry
    (config.APP_TOOL_KEYS) carries its call shape, not its `source`. A Lambda of your own
    publishing a tool of that name is kept the same way: code it was asked to run."""
    t = tool or {}
    return (str(t.get("type") or "").lower() == "lambda"
            and str(published or "").rsplit("___", 1)[-1] == CODE_TOOL)


def _streams(result_text: str) -> tuple[str, str]:
    """stdout and stderr from what the tool returned (rendered as JSON by the Gateway
    client); the whole text as stdout when it is not that shape."""
    import json
    try:
        data = json.loads(result_text)
    except (TypeError, ValueError):
        return str(result_text or ""), ""
    if not isinstance(data, dict):
        return str(result_text or ""), ""
    err = str(data.get("stderr") or "")
    if data.get("error"):
        err = f"{err}\n{data['error']}" if err else str(data["error"])
    return str(data.get("stdout") or ""), err


_warned: set = set()


def record_code_call(session_id: str, agent_id: str, args: dict, result_text: str) -> str:
    """Keep one codeInterpreter call as a code/ artifact. Best effort: never fails the call.
    "" when this deployment keeps no files, or the write was refused."""
    if not _bucket() or not isinstance(args, dict) or not str(args.get("code") or "").strip():
        return ""
    out, err = _streams(result_text)
    try:
        return put_code(session_id, agent_id, str(args.get("language") or ""),
                        str(args["code"]), out, err)
    except Exception as e:  # noqa: BLE001 - keeping a copy must never break the tool call
        # Once per runtime and reason: a dedicated runtime may write only where
        # orchestrator.files grants it, and every call would say so again.
        why = type(e).__name__
        if why not in _warned:
            _warned.add(why)
            print(f"[artifacts] code not kept for {agent_id}: {why}: {str(e)[:200]}")
        return ""
