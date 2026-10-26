"""
Gateway Lambda target: the Document generator, as a tool.

The fourth BUILT-IN `type: "lambda"` function (`source: "docgen"`), next to
`app/tools/pricing/`, `app/tools/codeInterpreter/` and `app/tools/browser/`. It publishes ONE
tool:

  generate_document(format, data, template?, fileName?) - render a PDF, PPTX or DOCX from
  JSON `data`, into a template from S3 (PPTX, DOCX) or the built-in simple layout, save it
  with the run's files and under the tool's output prefix, and return where it is: its
  s3:// URI and a presigned link. The file is named `fileName`, else the data's title,
  else the tool's key, and never replaces another call's file of the run (_claim); a
  step's new attempt replaces the file its earlier attempt wrote under that name.

`runId`, `agent` and `attempt` are three more arguments, which the RUNTIME sets on every
call to this tool (app/features/gateway/client.py _invoke) from the run it is serving,
whatever the model sent: they decide which run's folder the file goes to, and which
earlier file is this step's own to replace. The deploy publishes `attempt` in the tool's
schema itself (cdk/lib/tool-plane.ts, terraform/tools.tf), so no workflow.json declares it.

What the function may touch is fixed by its deployment, never by a call (environment, set
by cdk/lib/docgen-tool.ts and terraform/docgen_tool.tf from workflow.json):

  FORMATS           the formats the tool's `formats` allows, comma-separated
  TEMPLATE_S3_URI   `templateS3Uri`: one template object, or a folder ending in "/"
  OUTPUT_PREFIX     `outputPrefix`: s3://bucket/prefix/ (optional)
  ASSETS_BUCKET     the deployment's assets bucket, which holds the run's Files

A template is read only from TEMPLATE_S3_URI: the one object, or a name under the folder
with no "..", "." or empty part, so a call cannot climb out of it (and the role may read
nothing else). The template rules are in render.py.

Invocation contract (identical to app/tools/pricing/handler.py): the Gateway passes the
tool arguments as `event`, and the tool name as
`context.client_context.custom["bedrockAgentCoreToolName"]`, formatted
"<targetName>___<toolName>".
  https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/gateway-add-target-lambda.html
"""
import json
import os
import re

import boto3
import render  # this folder's render.py (the zip holds this folder and its libraries)
from botocore.config import Config
from botocore.exceptions import ClientError

TOOL = "generate_document"
#: The extension, and the Content-Type the file is saved with, of each format.
CONTENT_TYPES = {
    "pdf": "application/pdf",
    "pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
}
#: The largest template read, and file written. The same bound as an agent's saved file
#: (app/common/artifacts.py MAX_OUTPUT_BYTES).
MAX_TEMPLATE_BYTES = 20_000_000
MAX_OUTPUT_BYTES = 25_000_000
#: `data`, as JSON. The Gateway's request is itself capped (Lambda: 6 MB synchronous).
MAX_DATA_BYTES = 1_000_000
#: How long the returned link works. A link signed with the function's role credentials
#: stops working when those credentials expire, which may be sooner:
#:   https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html
URL_SECONDS = 3600
#: The run and agent ids, as app/common/artifacts.py checks them.
_SESSION_RE = re.compile(r"^[A-Za-z0-9_-]{6,64}$")
_AGENT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")
#: This run of the step (app/features/observability/scope.py Scope.attempt).
_ATTEMPT_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
MAX_NAME = 120
#: How many names one call tries (<stem>.<ext>, <stem>-2.<ext>, ...). Each try is one
#: conditional PutObject (If-None-Match: *), so a file never replaces another one, even
#: when several calls of the same run ask for the same name at the same moment.
#:   https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html
MAX_NAME_TRIES = 50
#: How many times one name is looked at again when the file there changed between reading
#: who wrote it and replacing it (If-Match refused): another call of the same attempt has
#: just replaced it, and the second look says so.
MAX_REPLACE_TRIES = 3
_TAKEN = ("PreconditionFailed", "ConditionalRequestConflict")

_s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION") or "us-east-1",
                   config=Config(signature_version="s3v4", retries={"total_max_attempts": 2}))


class Refused(ValueError):
    """A call this function will not carry out; the message says why."""


def safe_name(name: str) -> str:
    """app/common/artifacts.py safe_name: a file name safe as the last part of a key."""
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


def split_s3(uri: str) -> tuple[str, str]:
    """s3://bucket/key -> (bucket, key)."""
    if not str(uri or "").startswith("s3://"):
        raise Refused(f"not an s3:// URI: {uri!r}")
    bucket, _, key = uri[5:].partition("/")
    if not bucket:
        raise Refused(f"no bucket in {uri!r}")
    return bucket, key


def _formats() -> list[str]:
    return [f.strip() for f in os.environ.get("FORMATS", "").split(",") if f.strip()]


def template_key(fmt: str, name: str) -> tuple[str, str] | None:
    """(bucket, key) of the template for this call, or None for the built-in layout.

    One object configured: it, when its extension is the format asked for. A folder: the
    name the call gives, under it, with the format's extension; no name, no template."""
    uri = os.environ.get("TEMPLATE_S3_URI", "")
    if not uri or fmt == "pdf":
        return None
    bucket, key = split_s3(uri)
    if not key.endswith("/"):
        return (bucket, key) if key.lower().endswith(f".{fmt}") else None
    name = str(name or "").strip()
    if not name:
        return None
    parts = name.split("/")
    if "\\" in name or any(p in ("", ".", "..") for p in parts):
        raise Refused(f"template {name!r} is not a name under the template folder")
    if not name.lower().endswith(f".{fmt}"):
        raise Refused(f"template {name!r} is not a .{fmt} file")
    return bucket, key + name


def _read_template(bucket: str, key: str) -> bytes:
    try:
        head = _s3.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        # Without s3:ListBucket (which this role does not have) S3 answers a missing
        # object with 403, not 404.
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("403", "404", "NoSuchKey", "NotFound", "AccessDenied"):
            raise Refused(f"cannot read the template s3://{bucket}/{key}: it is not there, "
                          f"or this tool may not read it") from e
        raise
    if int(head.get("ContentLength") or 0) > MAX_TEMPLATE_BYTES:
        raise Refused(f"the template is over {MAX_TEMPLATE_BYTES / 1e6:.0f} MB")
    return _s3.get_object(Bucket=bucket, Key=key)["Body"].read(MAX_TEMPLATE_BYTES + 1)


def _data(raw) -> dict:
    """The call's `data`: an object, or a JSON object in a string; other text is {"text": it}."""
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except ValueError:
            raw = {"text": raw}
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raw = {"items": raw} if isinstance(raw, list) else {"text": raw}
    if len(json.dumps(raw, default=str)) > MAX_DATA_BYTES:
        raise Refused(f"data is over {MAX_DATA_BYTES / 1e6:.0f} MB as JSON")
    return raw


def _default_stem(target: str, agent: str) -> str:
    """A file's name when the call gives none: the tool's key (its Gateway target), else the
    agent's id, else "document"."""
    for candidate in (target, agent):
        if candidate and _AGENT_RE.match(candidate):
            return candidate
    return "document"


def _named(base: str, fmt: str, n: int) -> str:
    """The n-th name for `base` (1: as it is, then <stem>-<n>), with the format's extension
    and within MAX_NAME."""
    name = safe_name(base)
    if name.lower().endswith(f".{fmt}"):
        name = name[:-len(fmt) - 1].rstrip(" .") or "file"
    suffix = f"-{n}" if n > 1 else ""
    return f"{name[:MAX_NAME - len(suffix) - len(fmt) - 1].rstrip(' .') or 'file'}{suffix}.{fmt}"


def _taken(e: ClientError) -> bool:
    """Whether S3 refused a conditional write because of the key's current state: 412
    PreconditionFailed (it exists, or it changed), or 409 ConditionalRequestConflict
    (another conditional write of the key is in flight)."""
    code = str(e.response.get("Error", {}).get("Code", ""))
    status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return code in _TAKEN or status in (409, 412)


def _earlier_attempt(bucket: str, key: str, meta: dict) -> str:
    """The ETag of the file at `key` when an EARLIER attempt of this same step wrote it (the
    same agent, a different attempt: a gate's revise, a rerun_from, a re-run of part of a
    stage), so this attempt replaces it; "" otherwise. A file of another step, of this same
    attempt, or with no attempt recorded is never replaced. Unreadable counts as not ours."""
    agent, attempt = meta.get("agent"), meta.get("attempt")
    if not (agent and attempt):
        return ""
    try:
        head = _s3.head_object(Bucket=bucket, Key=key)
    except ClientError:
        return ""
    theirs = {str(k).lower(): str(v) for k, v in (head.get("Metadata") or {}).items()}
    if theirs.get("agent") == agent and theirs.get("attempt") and theirs["attempt"] != attempt:
        return str(head.get("ETag") or "")
    return ""


def _claim(body: bytes, ctype: str, meta: dict, fmt: str, base: str, key_for) -> str:
    """Write the file under the first name for `base` that is free or this step's own from
    an earlier attempt, and return that name.

    Each name is tried with a conditional PutObject (If-None-Match: *): S3 refuses it when
    the key exists (412 PreconditionFailed), or while another conditional write of the same
    key is in flight (409 ConditionalRequestConflict). A taken name that an earlier attempt
    of this step wrote is replaced, with If-Match on the ETag read, so a re-run writes its
    file again (app/common/artifacts.py) and the draft it replaces is not kept beside it.
    Any other taken name moves on to the next, so parallel calls that ask for the same name
    each get one of their own and none replaces another's file. An explicit name is kept
    when it is free."""
    for n in range(1, MAX_NAME_TRIES + 1):
        name = _named(base, fmt, n)
        bucket, key = key_for(name)
        for _ in range(MAX_REPLACE_TRIES):
            try:
                _s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType=ctype, Metadata=meta,
                               IfNoneMatch="*")
                return name
            except ClientError as e:
                if not _taken(e):
                    raise
            etag = _earlier_attempt(bucket, key, meta)
            if not etag:
                break
            try:
                _s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType=ctype, Metadata=meta,
                               IfMatch=etag)
                return name
            except ClientError as e:
                if not _taken(e):
                    raise
            # Changed since it was read: look again (it may now be this attempt's own).
    raise Refused(f"{MAX_NAME_TRIES} files named like {_named(base, fmt, 1)!r} already exist for this run; "
                  "give the file another name")


def generate_document(event: dict, target: str = "") -> dict:
    fmt = str(event.get("format") or "").strip().lower().lstrip(".")
    allowed = _formats()
    if fmt not in CONTENT_TYPES or fmt not in allowed:
        raise Refused(f"format must be one of {', '.join(allowed) or '(none configured)'}, not {fmt!r}")
    data = _data(event.get("data"))
    run_id = str(event.get("runId") or "")
    agent = str(event.get("agent") or "")
    run_ok = bool(_SESSION_RE.match(run_id))
    assets = os.environ.get("ASSETS_BUCKET", "")
    output = os.environ.get("OUTPUT_PREFIX", "")
    if not output and not (assets and run_ok):
        raise Refused("this call is for no run, and the tool has no outputPrefix: there is nowhere to save the file")

    where = template_key(fmt, str(event.get("template") or ""))
    body = render.render(fmt, data, _read_template(*where) if where else None)
    if len(body) > MAX_OUTPUT_BYTES:
        raise Refused(f"the {fmt} is {len(body) / 1e6:.1f} MB; at most {MAX_OUTPUT_BYTES / 1e6:.0f} MB")

    # The name asked for (fileName, else the data's title), else this tool's own key, else
    # the agent's id: never one fixed default, which every call of a run would share.
    asked = str(event.get("fileName") or "") or render.as_text(data.get("title"))
    meta = {"agent": agent} if _AGENT_RE.match(agent) else {}
    attempt = str(event.get("attempt") or "")
    if meta and _ATTEMPT_RE.match(attempt):
        meta["attempt"] = attempt
    ctype = CONTENT_TYPES[fmt]
    output_at = split_s3(output) if output else None
    run_file = ""

    def key_for(n: str) -> tuple[str, str]:
        """(bucket, key) of the file named `n` that claims the name: the run's copy when
        there is a run, else the output prefix's (under the run's id when there is one,
        which is where the link below points)."""
        if assets and run_ok:
            # The run's Files tab (app/common/artifacts.py layout): runs/<run>/outputs/<name>.
            return assets, f"runs/{run_id}/outputs/{n}"
        return output_at[0], f"{output_at[1]}{run_id}/{n}" if run_ok else f"{output_at[1]}{n}"

    name = _claim(body, ctype, meta, fmt, asked or _default_stem(target, agent), key_for)
    if assets and run_ok:
        run_file = f"runs/{run_id}/outputs/{name}"
    if output_at:
        bucket, prefix = output_at
        key = f"{prefix}{run_id}/{name}" if run_ok else f"{prefix}{name}"
        if run_file:
            # Under the run's id and a name the run's own folder just gave this call alone,
            # so no other call writes this key.
            _s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType=ctype, Metadata=meta)
    else:
        bucket, key = assets, run_file
    url = _s3.generate_presigned_url("get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=URL_SECONDS)
    return {"fileName": name, "format": fmt, "bytes": len(body), "s3Uri": f"s3://{bucket}/{key}",
            "url": url, "expiresInSeconds": URL_SECONDS, "runFile": run_file,
            "template": f"s3://{where[0]}/{where[1]}" if where else "built-in layout"}


_TOOLS = {TOOL: generate_document}


def _tool_name(context) -> str:
    """The tool the Gateway is asking for, minus the "<target>___" prefix."""
    try:
        full = context.client_context.custom.get("bedrockAgentCoreToolName", "")
    except Exception:  # noqa: BLE001 - a direct test invoke has no client_context
        return ""
    return full.rsplit("___", 1)[-1] if full else ""


def _target_name(context) -> str:
    """The Gateway target the call came through ("<target>___<tool>"): the tool's key."""
    try:
        full = context.client_context.custom.get("bedrockAgentCoreToolName", "")
    except Exception:  # noqa: BLE001 - a direct test invoke has no client_context
        return ""
    return full.rsplit("___", 1)[0] if full and "___" in full else ""


def lambda_handler(event, context):
    tool = _tool_name(context) or TOOL
    fn = _TOOLS.get(tool)
    if fn is None:
        return {"error": f"unknown tool {tool!r}", "availableTools": sorted(_TOOLS)}
    try:
        return fn(event or {}, _target_name(context))
    except (Refused, render.RenderError) as e:
        return {"error": str(e)}
    except Exception as e:  # noqa: BLE001 - surface the reason, never a fake answer
        print(f"[docgen] {tool} failed: {type(e).__name__}: {e}")
        return {"error": f"{tool} failed: {type(e).__name__}: {e}"}
