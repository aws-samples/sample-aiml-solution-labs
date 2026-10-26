"""Deliver a run's files: a task step with `task.kind: "deliver"` (app/common/task_agent.py).

    "send": {"name": "Send the reports", "runtime": "task",
             "task": {"kind": "deliver", "targets": [
               {"type": "s3", "uri": "s3://reports-bucket/out/", "files": ["*.pdf"]},
               {"type": "email", "identityArn": "arn:aws:ses:us-east-1:111122223333:identity/example.com",
                "fromAddress": "reports@example.com", "to": ["team@example.com"],
                "when": {"field": "outputs.check.passed", "equals": true}},
               {"type": "webhook", "url": "https://hooks.example.com/in", "secret": "hook"}]}}

The files are the ones the run's steps saved (runs/<session>/outputs/, app/common/artifacts.py;
the run's Files tab). Each target picks some of them by name pattern (`files`) and by the
step that made them (`producedBy`), and may hold a condition (`when`, the operators of
branch.when) that skips it. "Deliver only if the check passed or a person approved" is
the step placed after the gate or the branch, like any step.

  s3       CopyObject to <prefix><session>/<name>. The runtime role may put only under the
           configured prefixes (deliver.tf / cdk/lib/deliver.ts).
  email    Amazon SES v2 SendEmail from the configured, verified identity, to the addresses
           the AUTHOR wrote in `to`. A step's output adds recipients only through
           `recipientsFrom`, the explicit opt-in. The files go as attachments while they fit
           (ATTACH_MAX_BYTES, under the SES v2 limit of 40 MB per message after base64,
           https://docs.aws.amazon.com/ses/latest/dg/quotas.html), else as links. In the SES
           sandbox every recipient must be verified too; SES's own error is shown as it is.
  webhook  HTTPS POST of the files' links and metadata through the SSRF guard
           (app/common/netguard.py), signed like the framework's own webhooks
           (X-AX-Timestamp, X-AX-Signature: sha256=HMAC(secret, "<ts>.<body>")) when `secret`
           names a deliverSecrets build secret.

Links are presigned GETs made with the runtime role's credentials: S3 ends them when those
credentials expire, even before `linkExpiresMinutes`
(https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html).

Every target is tried; each delivery, skip and failure is a line on the run's timeline and
an event in the activity log (run.delivered / run.delivery_failed). A failed target fails
the step after the others were tried, naming it.
"""

from __future__ import annotations

import asyncio
import fnmatch
import hashlib
import hmac
import json
import os
import re
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from urllib.parse import urlsplit

from app.common import audit, branching, clock, netguard, task_template, vocabulary

TARGET_TYPES = vocabulary.values("deliverTargetTypes")

#: Targets one step may have, files one target may send, and run files looked at.
MAX_TARGETS = 10
MAX_FILES = 20
MAX_LISTED = 200
#: SES: at most 50 recipients (To, CC and BCC) per message
#: (https://docs.aws.amazon.com/ses/latest/dg/quotas.html).
MAX_RECIPIENTS = 50
#: The files attached to one message, together, before they go as links instead. SES v2
#: takes 40 MB per message after base64 (which adds a third, plus MIME line breaks).
ATTACH_MAX_BYTES = 25_000_000
#: Links: default and longest life. SigV4 presigned URLs last at most 7 days.
LINK_DEFAULT_MINUTES = 60
LINK_MAX_MINUTES = 7 * 24 * 60
WEBHOOK_TIMEOUT_SECONDS = 10

COMMON_KEYS = ("type", "files", "producedBy", "when")
#: The keys each target type reads; any other is refused.
TARGET_KEYS = {
    "s3": (*COMMON_KEYS, "uri"),
    "email": (*COMMON_KEYS, "identityArn", "fromAddress", "to", "subject", "body", "attach",
              "recipientsFrom", "linkExpiresMinutes"),
    "webhook": (*COMMON_KEYS, "url", "secret", "linkExpiresMinutes"),
}
#: The build-storage token (orchestrator.storage) an s3 uri may start with.
STORAGE_TOKEN = "${storage}"  # noqa: S105 - a placeholder in a uri, not a secret  # nosec B105
S3_URI_RE = re.compile(r"^(s3://[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]|\$\{storage\})/([^*?]+/)?$")
IDENTITY_ARN_RE = re.compile(r"^arn:aws[a-z-]*:ses:([a-z0-9-]+):(\d{12}):identity/([^/\s]+)$")
EMAIL_RE = re.compile(r"^[^@\s,;<>\"]+@[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)+$")
SECRET_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")

APP_NAME = os.environ.get("APP_NAME", "")

_clients: dict = {}


class DeliveryError(RuntimeError):
    """One target could not be delivered; the message says why, in words a person can act on."""


def _client(service: str, region: str = ""):
    key = (service, region)
    if key not in _clients:
        import boto3
        from botocore.config import Config
        _clients[key] = boto3.client(service, region_name=region or os.getenv("AWS_REGION") or "us-east-1",
                                     config=Config(retries={"mode": "standard"}))
    return _clients[key]


# --- what the runtime refuses ------------------------------------------------------------

def problem(agent_id: str, task: dict) -> str:
    """The first thing that stops this deliver step from running, or "". The validators
    (bff/task_check.py, taskCheck.ts) check the same and more before a deploy."""
    targets = task.get("targets")
    if not isinstance(targets, list) or not targets:
        return f"agent {agent_id!r}: a deliver step needs task.targets, where to send the files"
    if len(targets) > MAX_TARGETS:
        return f"agent {agent_id!r}: task.targets may have at most {MAX_TARGETS} targets"
    for i, t in enumerate(targets):
        kind = t.get("type") if isinstance(t, dict) else None
        if kind not in TARGET_TYPES:
            return f"agent {agent_id!r}: task.targets[{i}].type is not one of {', '.join(TARGET_TYPES)}"
        extra = [k for k in t if k not in TARGET_KEYS[kind]]
        if extra:
            return f"agent {agent_id!r}: task.targets[{i}].{extra[0]} does not apply to a {kind!r} target"
    return ""


# --- the run's files ---------------------------------------------------------------------

def _assets_bucket() -> str:
    return os.getenv("ASSETS_BUCKET", "")


def run_files(session_id: str) -> list[dict]:
    """The files the run's steps saved: [{name, key, bytes, contentType, agent}], in name
    order. Each object records the step that made it (x-amz-meta-agent)."""
    bucket = _assets_bucket()
    if not bucket:
        raise DeliveryError("this deployment keeps no run files (set orchestrator.files and deploy again)")
    s3 = _client("s3")
    prefix = f"runs/{session_id}/outputs/"
    found: list[dict] = []
    token = None
    while len(found) < MAX_LISTED:
        kw = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
        if token:
            kw["ContinuationToken"] = token
        page = s3.list_objects_v2(**kw)
        found += [{"key": o["Key"], "bytes": int(o.get("Size") or 0)} for o in page.get("Contents") or []
                  if not o["Key"].endswith("/")]
        token = page.get("NextContinuationToken")
        if not page.get("IsTruncated") or not token:
            break
    out = []
    for f in found[:MAX_LISTED]:
        head = s3.head_object(Bucket=bucket, Key=f["key"])
        out.append({"name": f["key"].rsplit("/", 1)[-1], "key": f["key"], "bytes": f["bytes"],
                    "contentType": str(head.get("ContentType") or "application/octet-stream"),
                    "agent": str((head.get("Metadata") or {}).get("agent") or "")})
    return sorted(out, key=lambda f: f["name"])


def select(files: list[dict], target: dict) -> list[dict]:
    """The files a target sends: matching any of its `files` patterns (case-insensitive,
    * and ?) and made by one of its `producedBy` steps, when it gives them."""
    patterns = [str(p).lower() for p in target.get("files") or []]
    by = {str(a) for a in target.get("producedBy") or []}
    return [f for f in files
            if (not patterns or any(fnmatch.fnmatchcase(f["name"].lower(), p) for p in patterns))
            and (not by or f["agent"] in by)]


# --- conditions --------------------------------------------------------------------------

def condition(when, data: dict) -> tuple[bool, str]:
    """(holds, the condition in words). `field` is a placeholder reference into the run
    (request, session, outputs.<step>.<field>); a reference to nothing is "missing", so only
    `exists: false` holds for it. The operators are branch.when's (app/common/branching.py)."""
    if not when:
        return True, ""
    field = str(when.get("field") or "")
    try:
        value = task_template.lookup(field, data)
    except task_template.TemplateError:
        value = branching._MISSING
    ops = [op for op in branching._OPS if op in when]
    return all(branching._holds(op, value, when[op]) for op in ops), branching._describe(when)


# --- links -------------------------------------------------------------------------------

def _minutes(target: dict) -> int:
    m = target.get("linkExpiresMinutes", LINK_DEFAULT_MINUTES)
    return int(m) if isinstance(m, int) and not isinstance(m, bool) and 1 <= m <= LINK_MAX_MINUTES \
        else LINK_DEFAULT_MINUTES


def _link(f: dict, minutes: int) -> str:
    return _client("s3").generate_presigned_url(
        "get_object", Params={"Bucket": _assets_bucket(), "Key": f["key"]}, ExpiresIn=minutes * 60)


def _linked(files: list[dict], minutes: int) -> list[dict]:
    at = datetime.fromtimestamp(time.time() + minutes * 60, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    return [{"name": f["name"], "bytes": f["bytes"], "contentType": f["contentType"],
             "producedBy": f["agent"], "url": _link(f, minutes), "expiresAt": at} for f in files]


# --- s3 ----------------------------------------------------------------------------------

def resolve_uri(uri: str) -> tuple[str, str]:
    """(bucket, prefix) of an s3 target's uri. `${storage}` is the build's own storage
    (orchestrator.storage), reached through STORAGE_BUCKET."""
    if uri.startswith(STORAGE_TOKEN):
        bucket = os.getenv("STORAGE_BUCKET", "")
        if not bucket:
            raise DeliveryError(f"{uri} uses {STORAGE_TOKEN}, but this deployment has no build storage "
                                "(orchestrator.storage)")
        uri = f"s3://{bucket}{uri[len(STORAGE_TOKEN):]}"
    rest = uri.removeprefix("s3://")
    bucket, _, prefix = rest.partition("/")
    return bucket, prefix


def send_s3(session_id: str, target: dict, files: list[dict], data: dict) -> dict:
    bucket, prefix = resolve_uri(str(target.get("uri") or ""))
    folder = f"{prefix}{session_id}/"
    s3 = _client("s3")
    keys = []
    for f in files:
        key = f"{folder}{f['name']}"
        try:
            s3.copy_object(Bucket=bucket, Key=key, CopySource={"Bucket": _assets_bucket(), "Key": f["key"]})
        except Exception as e:  # said as S3 said it
            raise DeliveryError(f"S3 refused {f['name']} to s3://{bucket}/{key}: {_aws_error(e)}") from e
        keys.append(key)
    return {"destination": f"s3://{bucket}/{folder}", "keys": keys}


# --- email -------------------------------------------------------------------------------

def _addresses(value) -> list[str]:
    if isinstance(value, str):
        return [a.strip() for a in re.split(r"[,;\s]+", value) if a.strip()]
    if isinstance(value, list):
        return [a for v in value for a in _addresses(v)]
    return []


def recipients(target: dict, data: dict) -> list[str]:
    """The author's `to`, plus what `recipientsFrom` names when it is set. Each must look
    like an address; at most MAX_RECIPIENTS, each once."""
    wanted = _addresses(target.get("to"))
    if target.get("recipientsFrom"):
        try:
            wanted += _addresses(task_template.render(str(target["recipientsFrom"]), data))
        except task_template.TemplateError as e:
            raise DeliveryError(f"recipientsFrom: {e}") from e
    out: list[str] = []
    for a in wanted:
        if not EMAIL_RE.match(a):
            raise DeliveryError(f"{a!r} is not an email address")
        if a.lower() not in {x.lower() for x in out}:
            out.append(a)
    if not out:
        raise DeliveryError("no recipient: write at least one address in `to`")
    if len(out) > MAX_RECIPIENTS:
        raise DeliveryError(f"{len(out)} recipients; SES sends one message to at most {MAX_RECIPIENTS}")
    return out


def _render(text: str, data: dict) -> str:
    try:
        got = task_template.render(text, data)
    except task_template.TemplateError as e:
        raise DeliveryError(str(e)) from e
    return got if isinstance(got, str) else json.dumps(got, ensure_ascii=False, default=str)


def _ses_hint(code: str, message: str) -> str:
    if code == "MessageRejected" and "not verified" in message.lower():
        return (" — while the account is in the SES sandbox, every recipient must be a verified "
                "identity too (https://docs.aws.amazon.com/ses/latest/dg/request-production-access.html)")
    if code in ("AccessDenied", "AccessDeniedException"):
        return " — the runtime may send only from the configured identity and From address"
    return ""


def send_email(session_id: str, target: dict, files: list[dict], data: dict) -> dict:
    arn = str(target.get("identityArn") or "")
    m = IDENTITY_ARN_RE.match(arn)
    if not m:
        raise DeliveryError(f"identityArn {arn!r} is not an SES identity ARN")
    to = recipients(target, data)
    total = sum(f["bytes"] for f in files)
    attach = target.get("attach", True) is not False and total <= ATTACH_MAX_BYTES
    subject = _render(str(target.get("subject") or f"{APP_NAME or 'AgentExpress'} run {session_id}: "
                      f"{len(files)} file{'' if len(files) == 1 else 's'}"), data)
    body = _render(str(target.get("body") or "The run's files are below."), data)
    s3 = _client("s3")
    attachments, lines = [], []
    if attach:
        for f in files:
            raw = s3.get_object(Bucket=_assets_bucket(), Key=f["key"])["Body"].read()
            attachments.append({"FileName": f["name"][:255], "RawContent": raw,
                                "ContentType": f["contentType"][:78] or "application/octet-stream",
                                "ContentDisposition": "ATTACHMENT", "ContentTransferEncoding": "BASE64"})
            lines.append(f"- {f['name']} ({f['bytes']} bytes, attached)")
    else:
        minutes = _minutes(target)
        lines.extend(f"- {f['name']} ({f['bytes']} bytes): {f['url']}" for f in _linked(files, minutes))
        lines.append(f"(The links expire after {minutes} minutes at most.)")
    simple = {"Subject": {"Data": subject[:998], "Charset": "UTF-8"},
              "Body": {"Text": {"Data": body + "\n\nFiles:\n" + "\n".join(lines) + "\n", "Charset": "UTF-8"}}}
    if attachments:
        simple["Attachments"] = attachments
    ses = _client("sesv2", m.group(1))
    try:
        sent = ses.send_email(FromEmailAddress=str(target.get("fromAddress") or ""),
                              Destination={"ToAddresses": to}, Content={"Simple": simple})
    except Exception as e:  # SES's own words, so a person can act on them
        code = str(((getattr(e, "response", None) or {}).get("Error") or {}).get("Code") or "")
        raise DeliveryError(f"SES did not send to {', '.join(to)}: {_aws_error(e)}"
                            f"{_ses_hint(code, str(e))}") from e
    return {"to": to, "mode": "attached" if attach else "links", "messageId": sent.get("MessageId", "")}


# --- webhook -----------------------------------------------------------------------------

def shown_url(url: str) -> str:
    """A webhook URL as the timeline and the activity log show it: no query or fragment,
    which often carry a token."""
    p = urlsplit(url)
    return f"{p.scheme}://{p.hostname or ''}{p.path or ''}"


def _secret(name: str) -> str:
    try:
        secrets = json.loads(os.getenv("DELIVER_SECRETS") or "{}")
    except ValueError:
        secrets = {}
    value = secrets.get(name) if isinstance(secrets, dict) else None
    if not value:
        raise DeliveryError(f"the deliverSecrets secret {name!r} was not deployed with this build: "
                            "save it on the step's panel and deploy again")
    return str(value)


def signature(secret: str, body: str, ts: int) -> dict:
    """The framework's own webhook signature (bff/triggers.py sign, the "agentexpress" scheme)."""
    mac = hmac.new(secret.encode(), f"{ts}.{body}".encode(), hashlib.sha256).hexdigest()
    return {"X-AX-Timestamp": str(ts), "X-AX-Signature": f"sha256={mac}"}


def send_webhook(session_id: str, target: dict, files: list[dict], data: dict, step: str = "") -> dict:
    url = str(target.get("url") or "")
    try:
        netguard.check_url(url)
    except netguard.BlockedURL as e:
        raise DeliveryError(f"{shown_url(url)}: {e.reason}") from e
    body = json.dumps({"event": "agentexpress.delivery", "app": APP_NAME, "session": session_id,
                       "step": step, "deliveredAt": clock.now_et().isoformat(),
                       "files": _linked(files, _minutes(target))}, ensure_ascii=False, default=str)
    headers = {"Content-Type": "application/json", "User-Agent": "AgentExpress-Deliver/1"}
    if target.get("secret"):
        headers.update(signature(_secret(str(target["secret"])), body, int(time.time())))
    # https only: checked above, and again by netguard.urlopen on connect and each redirect.
    req = urllib.request.Request(url, data=body.encode("utf-8"), headers=headers, method="POST")  # noqa: S310
    try:
        with netguard.urlopen(req, timeout=WEBHOOK_TIMEOUT_SECONDS) as resp:
            status = int(getattr(resp, "status", 0) or resp.getcode())
    except urllib.error.HTTPError as e:
        raise DeliveryError(f"{shown_url(url)} answered HTTP {e.code}") from e
    except (urllib.error.URLError, OSError) as e:
        why = e.reason if isinstance(e, urllib.error.URLError) else e
        raise DeliveryError(f"{shown_url(url)} could not be reached: {why}") from e
    if not 200 <= status < 300:
        raise DeliveryError(f"{shown_url(url)} answered HTTP {status}")
    return {"url": shown_url(url), "httpStatus": status}


def _aws_error(e: Exception) -> str:
    err = (getattr(e, "response", None) or {}).get("Error") or {}
    if err:
        return f"{err.get('Code', '')}: {err.get('Message', '')}".strip(": ")
    return f"{type(e).__name__}: {e}"


# --- the step ----------------------------------------------------------------------------

def where_to(target: dict) -> str:
    """The target in words: the bucket and prefix, the recipients, or the URL's host and path."""
    kind = target.get("type")
    if kind == "s3":
        return str(target.get("uri") or "")
    if kind == "email":
        extra = " and addresses from " + str(target["recipientsFrom"]) if target.get("recipientsFrom") else ""
        return ", ".join(_addresses(target.get("to"))) + extra
    return shown_url(str(target.get("url") or ""))


async def run(agent, ctx, task: dict, data: dict) -> dict:
    """Deliver to every target in order. Returns {"deliveries": [...]}; raises TaskFailed
    naming the targets that failed, after every target was tried."""
    from app.common.task_agent import TaskFailed, _record
    files = await asyncio.to_thread(_files_or_fail, ctx.session_id)
    out, failed = [], []
    for i, target in enumerate(task.get("targets") or []):
        kind = str(target.get("type") or "")
        dest = where_to(target)
        entry = {"target": i, "type": kind, "to": dest}
        ok, said = condition(target.get("when"), data)
        if not ok:
            entry.update(status="skipped", why=f"the condition did not hold: {said}")
            await ctx.log(f"{agent.name}: skipped {kind} delivery to {dest}: {said} did not hold")
            out.append(entry)
            continue
        picked = select(files, target)
        if not picked:
            entry.update(status="skipped", why="no run file matched")
            await ctx.log(f"{agent.name}: skipped {kind} delivery to {dest}: no run file matched")
            out.append(entry)
            continue
        names = [f["name"] for f in picked]
        entry["files"] = names
        started = time.monotonic()
        try:
            if len(picked) > MAX_FILES:
                raise DeliveryError(f"{len(picked)} files match; a target sends at most {MAX_FILES}")
            if kind == "webhook":
                got = await asyncio.to_thread(send_webhook, ctx.session_id, target, picked, data, agent.id)
            else:
                got = await asyncio.to_thread(SENDERS[kind], ctx.session_id, target, picked, data)
        except DeliveryError as e:
            entry.update(status="failed", error=str(e))
            failed.append(f"{kind} to {dest}: {e}")
            _record(f"deliver:{kind}", "deliver", int((time.monotonic() - started) * 1000), dest, str(e), False)
            await ctx.log(f"{agent.name}: {kind} delivery to {dest} FAILED: {e}")
            _audit(ctx.session_id, "run.delivery_failed", agent.id, entry)
            out.append(entry)
            continue
        entry.update(status="delivered", **got)
        _record(f"deliver:{kind}", "deliver", int((time.monotonic() - started) * 1000), dest,
                json.dumps(got, default=str)[:2000], True)
        await ctx.log(f"{agent.name}: delivered {len(names)} file{'' if len(names) == 1 else 's'} "
                      f"({', '.join(names)}) by {kind} to {got.get('destination') or dest}"
                      + (f", {got['mode']}" if got.get("mode") else ""))
        _audit(ctx.session_id, "run.delivered", agent.id, entry)
        out.append(entry)
    if failed:
        raise TaskFailed(f"{agent.name}: {len(failed)} of {len(out)} deliveries failed: " + "; ".join(failed))
    return {"deliveries": out}


def _files_or_fail(session_id: str) -> list[dict]:
    from app.common.task_agent import TaskFailed
    try:
        return run_files(session_id)
    except DeliveryError as e:
        raise TaskFailed(str(e)) from e


SENDERS = {"s3": send_s3, "email": send_email, "webhook": send_webhook}


def _audit(session_id: str, action: str, step: str, entry: dict) -> None:
    to = entry.get("to")
    detail = {"step": step, "target": entry.get("type"),
              "to": (", ".join(map(str, to)) if isinstance(to, list) else str(to or ""))[:300],
              "files": ", ".join(entry.get("files") or [])[:500], "error": str(entry.get("error") or "")[:300]}
    audit.run_event(session_id, action, detail)
