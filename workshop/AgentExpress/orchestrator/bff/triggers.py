"""External triggers: start a run without anyone typing the request (orchestrator.triggers).

    "triggers": {
      "jira":    {"type": "webhook", "signature": "agentexpress",
                  "prompt": "Triage {{body.issue.key}}: {{body.issue.fields.summary}}"},
      "nightly": {"type": "schedule", "expression": "cron(0 6 * * ? *)", "prompt": "Daily report"},
      "alarms":  {"type": "eventbridge", "pattern": {"source": ["aws.cloudwatch"]},
                  "prompt": "Investigate {{detail.alarmName}}"},
      "inbox":   {"type": "s3", "bucket": "acme-docs", "prefix": "incoming/", "prompt": "Review it"},
      "queue":   {"type": "sqs", "prompt": "{{body.text}}"}
    }

How each arrives at this BFF (the IaC wires them; nothing here listens on its own):

  webhook      POST /api/hooks/{name}, the one route with no JWT: its proof is a
               signature of the body with the trigger's secret (see VERIFIERS).
  schedule     EventBridge Scheduler invokes this function with {"axTrigger": name, ...}.
  eventbridge  a rule on the bus invokes it with {"axTrigger": name, "event": <event>}.
  s3           the same, for the bucket's "Object Created" events; the object is
               attached to the run when orchestrator.attachments.s3 allows it.
  sqs          a Lambda event source mapping; one message, one run (TRIGGER_QUEUES).

Every delivery then goes through `fire`: the trigger must be on; the same delivery
twice starts one run (an idempotency key, kept a day); at most `maxRunsPerHour` runs;
the request is the trigger's `prompt` with {{placeholders}} filled from the delivery;
`attachPayload` brings the delivery as payload.json. The run is the build owner's
(`runAs: "owner"`) or the trigger's own (`runAs: "service"`, readable and decided by
the trigger's `approvers` groups). Its review gates behave as configured, unless the
trigger sets `gates: "auto"`.

THE PAYLOAD IS UNTRUSTED. It reaches the agents only as text in the request and as an
attachment, never as instructions to this code; guardrails apply as they do to a person.
"""

from __future__ import annotations

import base64
import contextlib
import hashlib
import hmac
import json
import os
import re
import secrets as _secrets
import time

import boto3
import clock
import workflow

REGION = os.environ.get("AWS_REGION", "us-east-1")
#: Where each webhook trigger's secret is kept: one JSON object, name -> secret.
SECRET_ARN = os.environ.get("TRIGGER_SECRET_ARN", "")
#: SQS queue ARN -> trigger name, for the event source mappings.
QUEUES: dict = json.loads(os.environ.get("TRIGGER_QUEUES") or "{}")
#: Whose runs `runAs: "owner"` starts: the address of the build's owner in this app.
OWNER_EMAIL = os.environ.get("RUN_OWNER_EMAIL", "")
USER_POOL_ID = os.environ.get("USER_POOL_ID", "")
EVENTS_TABLE = os.environ.get("EVENTS_TABLE", "")
MAX_BODY = 256_000
SKEW_S = 300
IDEMPOTENCY_TTL_S = 86_400
DELIVERY_TTL_S = 7 * 86_400
VALUE_MAX = 4000
PLACEHOLDER = re.compile(r"\{\{\s*([A-Za-z0-9_\-]+(?:\.[A-Za-z0-9_\-]+)*)\s*\}\}")
#: Headers never handed to a prompt: the proof of the delivery, and credentials.
SECRET_HEADERS = ("authorization", "cookie", "x-ax-signature", "x-ax-token", "x-hub-signature",
                  "x-hub-signature-256", "x-slack-signature", "stripe-signature")
_clients: dict = {}
_cache: dict = {}


class TriggerError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status


def _client(name: str):
    if name not in _clients:
        _clients[name] = boto3.client(name, region_name=REGION)
    return _clients[name]


def _table():
    if "table" not in _clients:
        _clients["table"] = boto3.resource("dynamodb", region_name=REGION).Table(EVENTS_TABLE)
    return _clients["table"]


def all_triggers() -> dict:
    orch = workflow.RAW.get("orchestrator") if isinstance(workflow.RAW.get("orchestrator"), dict) else {}
    t = orch.get("triggers")
    return t if isinstance(t, dict) else {}


def trigger(name: str) -> dict:
    t = all_triggers().get(name)
    if not isinstance(t, dict):
        raise TriggerError(404, "unknown trigger")
    return t


# --- the request -----------------------------------------------------------------------

def _lookup(ctx, path: str):
    cur = ctx
    for part in path.split("."):
        if isinstance(cur, dict):
            cur = cur.get(part)
        elif isinstance(cur, list) and part.isdigit() and int(part) < len(cur):
            cur = cur[int(part)]
        else:
            return None
    return cur


def render(template: str, ctx: dict, limit: int = 10_000) -> str:
    """The trigger's prompt with each {{dot.path}} filled from the delivery: text as is,
    anything else as JSON, a missing value as nothing. No expressions, no code."""
    def value(m):
        v = _lookup(ctx, m.group(1))
        if v is None:
            return ""
        text = v if isinstance(v, str) else json.dumps(v, ensure_ascii=False, default=str)
        return text[:VALUE_MAX]
    return PLACEHOLDER.sub(value, str(template or ""))[:limit].strip()


# --- webhook signatures ----------------------------------------------------------------

def _hex(secret: str, message: bytes) -> str:
    return hmac.new(secret.encode(), message, hashlib.sha256).hexdigest()


def _fresh(ts: str) -> bool:
    try:
        return abs(time.time() - int(float(ts))) <= SKEW_S
    except (TypeError, ValueError):
        return False


def _verify_agentexpress(secret, headers, body):
    ts = headers.get("x-ax-timestamp", "")
    sig = headers.get("x-ax-signature", "").removeprefix("sha256=")
    return _fresh(ts) and hmac.compare_digest(sig, _hex(secret, f"{ts}.".encode() + body))


def _verify_github(secret, headers, body):
    sig = headers.get("x-hub-signature-256", "").removeprefix("sha256=")
    return bool(sig) and hmac.compare_digest(sig, _hex(secret, body))


def _verify_slack(secret, headers, body):
    ts = headers.get("x-slack-request-timestamp", "")
    sig = headers.get("x-slack-signature", "").removeprefix("v0=")
    return _fresh(ts) and hmac.compare_digest(sig, _hex(secret, f"v0:{ts}:".encode() + body))


def _verify_stripe(secret, headers, body):
    parts = [p.split("=", 1) for p in headers.get("stripe-signature", "").split(",") if "=" in p]
    ts = next((v for k, v in parts if k.strip() == "t"), "")
    expected = _hex(secret, f"{ts}.".encode() + body)
    return _fresh(ts) and any(hmac.compare_digest(v.strip(), expected) for k, v in parts if k.strip() == "v1")


def _verify_token(secret, headers, body):
    return hmac.compare_digest(headers.get("x-ax-token", ""), secret)


#: How a sender proves a delivery is theirs. "agentexpress" is ours; the others are the
#: schemes GitHub, Slack and Stripe sign with, so their webhooks work unchanged; "token"
#: (a shared header) is for a system that can set a header but not sign.
VERIFIERS = {"agentexpress": _verify_agentexpress, "github": _verify_github, "slack": _verify_slack,
             "stripe": _verify_stripe, "token": _verify_token}


def _secrets_doc() -> dict:
    hit = _cache.get("secrets")
    if hit and hit[0] > time.time():
        return hit[1]
    if not SECRET_ARN:
        return {}
    try:
        doc = json.loads(_client("secretsmanager").get_secret_value(SecretId=SECRET_ARN).get("SecretString") or "{}")
    except Exception as e:
        # The secret is created with no value: until one is set it reads as none.
        if "ResourceNotFound" not in type(e).__name__ and "ResourceNotFound" not in str(e):
            raise
        doc = {}
    doc = doc if isinstance(doc, dict) else {}
    _cache["secrets"] = (time.time() + 60, doc)
    return doc


def has_secret(name: str) -> bool:
    return bool(_secrets_doc().get(name))


def set_secret(name: str, value: str = "") -> str:
    """Store a webhook trigger's secret: the one given (GitHub's, Slack's, Stripe's), or a
    new random one. Returned once; never readable again."""
    t = trigger(name)
    if t.get("type") != "webhook":
        raise TriggerError(400, "only a webhook trigger has a secret")
    if not SECRET_ARN:
        raise TriggerError(404, "this deployment keeps no trigger secrets")
    value = str(value or "").strip() or "axwh_" + _secrets.token_urlsafe(32)
    if not 16 <= len(value) <= 512:
        raise TriggerError(400, "a secret is 16 to 512 characters")
    _cache.pop("secrets", None)
    doc = dict(_secrets_doc())
    doc[name] = value
    _client("secretsmanager").put_secret_value(SecretId=SECRET_ARN, SecretString=json.dumps(doc))
    _cache.pop("secrets", None)
    return value


def _headers(event: dict) -> dict:
    return {str(k).lower(): str(v) for k, v in (event.get("headers") or {}).items()}


def _raw_body(event: dict) -> bytes:
    body = event.get("body") or ""
    return base64.b64decode(body) if event.get("isBase64Encoded") else str(body).encode()


# --- whose run -------------------------------------------------------------------------

def run_identity(name: str, t: dict) -> tuple[str, str]:
    """(owner, user) of a run this trigger starts. The owner's `sub` in this app's user
    pool, looked up by address (cached); a service run is the trigger's own."""
    if t.get("runAs") == "service":
        return f"trigger:{name}", f"trigger:{name}"
    if not (OWNER_EMAIL and USER_POOL_ID):
        raise TriggerError(409, "runAs \"owner\" needs the build's owner (RUN_OWNER_EMAIL): deploy it from "
                                "the Builder, or set runAs \"service\"")
    hit = _cache.get("owner")
    if not hit:
        try:
            got = _client("cognito-idp").admin_get_user(UserPoolId=USER_POOL_ID, Username=OWNER_EMAIL)
        except Exception as e:  # noqa: BLE001 - not invited yet, or removed
            raise TriggerError(409, f"the build's owner {OWNER_EMAIL} is not a user of this app "
                                    f"({type(e).__name__})") from None
        sub = next((a["Value"] for a in got.get("UserAttributes", []) if a["Name"] == "sub"), "")
        hit = _cache["owner"] = (sub, OWNER_EMAIL)
    return hit


def may_see(t: dict, groups: list[str]) -> bool:
    """A caller in one of a service trigger's `approvers` groups reads and decides its runs."""
    return bool(set(t.get("approvers") or []) & set(groups or []))


# --- delivery bookkeeping (in the events table, under trigger#<name>) -------------------

def _first_time(name: str, key: str) -> bool:
    digest = hashlib.sha256(key.encode()).hexdigest()[:32]
    try:
        _table().put_item(Item={"session_id": f"trigger#{name}", "ts": f"idem#{digest}",
                                "ttl": int(time.time()) + IDEMPOTENCY_TTL_S},
                          ConditionExpression="attribute_not_exists(ts)")
        return True
    except Exception as e:
        if "ConditionalCheckFailed" in type(e).__name__ or "ConditionalCheckFailed" in str(e):
            return False
        raise


def _within_rate(name: str, limit: int) -> bool:
    hour = clock.now_str()[:13]
    try:
        _table().update_item(
            Key={"session_id": f"trigger#{name}", "ts": f"rate#{hour}"},
            UpdateExpression="ADD n :one SET #t = :ttl",
            ConditionExpression="attribute_not_exists(n) OR n < :max",
            ExpressionAttributeNames={"#t": "ttl"},
            ExpressionAttributeValues={":one": 1, ":max": limit, ":ttl": int(time.time()) + 7200})
        return True
    except Exception as e:
        if "ConditionalCheckFailed" in type(e).__name__ or "ConditionalCheckFailed" in str(e):
            return False
        raise


def _record(name: str, source: str, outcome: str, **detail) -> None:
    with contextlib.suppress(Exception):
        _table().put_item(Item={"session_id": f"trigger#{name}",
                                "ts": f"delivery#{clock.now_str()}#{_secrets.token_hex(2)}",
                                "source": source, "outcome": outcome,
                                **{k: str(v)[:300] for k, v in detail.items() if v not in (None, "")},
                                "ttl": int(time.time()) + DELIVERY_TTL_S})


def deliveries(name: str, limit: int = 20) -> list[dict]:
    from boto3.dynamodb.conditions import Key
    got = _table().query(KeyConditionExpression=Key("session_id").eq(f"trigger#{name}")
                         & Key("ts").begins_with("delivery#"), ScanIndexForward=False, Limit=limit)
    return [{"at": i["ts"].split("#")[1], **{k: v for k, v in i.items()
                                            if k in ("source", "outcome", "session", "reason")}}
            for i in got.get("Items", [])]


# --- firing ------------------------------------------------------------------------------

def fire(name: str, source: str, ctx: dict, key: str, start, payload=None, s3_uri: str = "") -> dict:
    """Start one run for one delivery, or say why not. `start(topic, owner, user, extra)`
    is the BFF's run start; it returns the session id."""
    t = trigger(name)
    if t.get("enabled") is False:
        _record(name, source, "off")
        return {"started": False, "reason": "the trigger is off"}
    custom = str(t.get("idempotencyKey") or "")
    key = (render(custom, ctx, 500) if custom else "") or key
    if key and not _first_time(name, key):
        _record(name, source, "duplicate")
        return {"started": False, "duplicate": True, "reason": "already received"}
    if not _within_rate(name, int(t.get("maxRunsPerHour") or 60)):
        _record(name, source, "throttled")
        return {"started": False, "reason": f"over {int(t.get('maxRunsPerHour') or 60)} runs this hour"}
    topic = render(t.get("prompt") or "", {**ctx, "trigger": name, "now": clock.now_str()})
    if not topic:
        _record(name, source, "empty")
        return {"started": False, "reason": "the prompt rendered empty"}
    owner, user = run_identity(name, t)
    try:
        sid = start(topic, owner, user, {
            "trigger": {"name": name, "type": t.get("type"), "source": source,
                        "runAs": t.get("runAs") or "owner"},
            "payload": payload if t.get("attachPayload") else None,
            "s3": s3_uri, "gates": t.get("gates") or "inherit"})
    except Exception as e:
        _record(name, source, "failed", reason=f"{type(e).__name__}: {e}")
        raise
    _record(name, source, "started", session=sid)
    return {"started": True, "session_id": sid}


def webhook(event: dict, start) -> tuple[int, dict]:
    """POST /api/hooks/{name}: check the signature, then fire. Answers only whether a run
    started (and its id), never anything the run produces."""
    name = str((event.get("pathParameters") or {}).get("name") or "")
    try:
        t = trigger(name)
    except TriggerError:
        return 404, {"error": "unknown trigger"}
    if t.get("type") != "webhook":
        return 404, {"error": "unknown trigger"}
    body = _raw_body(event)
    if len(body) > MAX_BODY:
        return 413, {"error": f"at most {MAX_BODY // 1000} KB"}
    headers = _headers(event)
    secret = _secrets_doc().get(name)
    scheme = str(t.get("signature") or "agentexpress")
    if not secret or not VERIFIERS[scheme](secret, headers, body):
        _record(name, "webhook", "unauthorized")
        return 401, {"error": "signature missing, stale or wrong"}
    text = body.decode(errors="replace")
    try:
        parsed = json.loads(text) if text.strip() else {}
    except ValueError:
        parsed = {"text": text}
    if scheme == "slack" and isinstance(parsed, dict) and parsed.get("type") == "url_verification":
        return 200, {"challenge": parsed.get("challenge")}
    default_key = (headers.get("x-ax-delivery") or headers.get("x-github-delivery")
                   or (parsed.get("event_id") if scheme == "slack" and isinstance(parsed, dict) else "")
                   or (parsed.get("id") if scheme == "stripe" and isinstance(parsed, dict) else "")
                   or hashlib.sha256(headers.get("x-ax-timestamp", "").encode() + body).hexdigest())
    ctx = {"body": parsed, "headers": {k: v for k, v in headers.items() if k not in SECRET_HEADERS},
           "query": event.get("queryStringParameters") or {}}
    try:
        got = fire(name, "webhook", ctx, str(default_key), start, payload=parsed)
    except TriggerError as e:
        return e.status, {"error": str(e)}
    return (202 if got.get("started") else 200), got


def _sqs(event: dict, start) -> dict:
    """One run per message. A message whose run could not start is handed back, so SQS
    retries it and, past maxReceiveCount, moves it to the dead-letter queue."""
    failures = []
    for rec in event.get("Records") or []:
        name = QUEUES.get(rec.get("eventSourceARN", ""))
        try:
            if not name:
                raise TriggerError(404, f"no trigger reads {rec.get('eventSourceARN')}")
            text = rec.get("body") or ""
            try:
                parsed = json.loads(text)
            except ValueError:
                parsed = {"text": text}
            ctx = {"body": parsed, "message": {"id": rec.get("messageId"),
                                               "attributes": rec.get("messageAttributes") or {}}}
            fire(name, "sqs", ctx, str(rec.get("messageId") or ""), start, payload=parsed)
        except Exception as e:  # noqa: BLE001 - reported per message
            print(f"[triggers] sqs {name}: {type(e).__name__}: {e}")
            failures.append({"itemIdentifier": rec.get("messageId")})
    return {"batchItemFailures": failures}


def is_trigger_event(event: dict) -> bool:
    if "axTrigger" in event:
        return True
    recs = event.get("Records")
    return bool(isinstance(recs, list) and recs and recs[0].get("eventSource") == "aws:sqs")


def dispatch(event: dict, start) -> dict:
    """A delivery that did not come through the API: a schedule, an EventBridge rule
    (an AWS service, your own events, a SaaS partner, S3) or an SQS message."""
    if "Records" in event:
        return _sqs(event, start)
    name = str(event.get("axTrigger") or "")
    try:
        t = trigger(name)
    except TriggerError:
        print(f"[triggers] unknown trigger {name!r}")
        return {"started": False, "reason": "unknown trigger"}
    kind = t.get("type")
    try:
        if kind == "schedule":
            ctx = {"time": event.get("time") or clock.now_str()}
            return fire(name, "schedule", ctx, str(event.get("id") or ""), start)
        ev = event.get("event") if isinstance(event.get("event"), dict) else {}
        ctx = {"event": ev, "detail": ev.get("detail") or {}, "source": ev.get("source"),
               "detailType": ev.get("detail-type")}
        uri = ""
        if kind == "s3":
            d = ev.get("detail") or {}
            uri = f"s3://{(d.get('bucket') or {}).get('name', '')}/{(d.get('object') or {}).get('key', '')}"
        return fire(name, kind or "eventbridge", ctx, str(ev.get("id") or ""), start, payload=ev, s3_uri=uri)
    except TriggerError as e:
        print(f"[triggers] {name}: {e}")
        return {"started": False, "reason": str(e)}


def listing(domain: str) -> list[dict]:
    """What the app's Triggers page shows: each trigger, its URL, and its last deliveries."""
    out = []
    for name, t in all_triggers().items():
        if not isinstance(t, dict):
            continue
        row = {"name": name, "type": t.get("type"), "enabled": t.get("enabled") is not False,
               "runAs": t.get("runAs") or "owner", "gates": t.get("gates") or "inherit",
               "prompt": t.get("prompt") or ""}
        if t.get("type") == "webhook":
            row.update(url=f"https://{domain}/api/hooks/{name}", signature=t.get("signature") or "agentexpress",
                       hasSecret=has_secret(name))
        for k in ("expression", "timezone", "bucket", "prefix", "pattern", "bus"):
            if k in t:
                row[k] = t[k]
        try:
            row["deliveries"] = deliveries(name, 10) if EVENTS_TABLE else []
        except Exception as e:  # noqa: BLE001 - the list still shows
            print(f"[triggers] deliveries {name}: {type(e).__name__}: {e}")
            row["deliveries"] = []
        out.append(row)
    return out


def sign(secret: str, body: str, ts: int | None = None) -> dict:
    """The headers an AgentExpress-signed delivery carries (for the page's curl sample,
    and for a sender written in Python)."""
    ts = int(time.time()) if ts is None else ts
    return {"X-AX-Timestamp": str(ts), "X-AX-Signature": "sha256=" + _hex(secret, f"{ts}.{body}".encode())}
