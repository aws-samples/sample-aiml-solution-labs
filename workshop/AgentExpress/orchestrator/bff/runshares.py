"""Read-only links to ONE run, with an expiry.

A run is private to whoever started it (handler._own_session). Its owner may make a link
that lets anyone SIGNED IN to this deployment read that one run until the link expires or
is revoked. The link names the run by a random id (secrets.token_urlsafe, 192 bits) kept
here, so the id is authorization (which run) and never authentication: the API's JWT
authorizer still decides who is signed in, and the BFF refuses a caller with no identity.

    POST   /api/sessions/{id}/shares            {expiresIn}  -> {shareId, url, expires}  (owner)
    GET    /api/sessions/{id}/shares                         -> the run's active links     (owner)
    DELETE /api/sessions/{id}/shares/{shareId}               -> {ok}                       (owner)
    GET    /api/shared/{shareId}                -> the run, read-only, + {sharedBy, expires}

Stored in this deployment's events table, beside the trigger rows (bff/triggers.py), so
no table and no permission is added:

    session_id            ts      what
    share#<shareId>       link    the link: {session, owner, sharedBy, created, expiresAt, revoked?}
    shares#<session>      <id>    the run's list of its links, so listing is one Query

Expiry is checked HERE on every read. Each row also carries `ttl` (a day past its expiry)
so DynamoDB removes it in time, but TTL deletion can run days late, so it is never what
refuses a link. A revoked link keeps its row, marked, until then.

What a shared read returns is an ALLOW-list of the run's snapshot (SNAPSHOT_FIELDS): no
telemetry (it holds captured prompts), no files, no owner id, nothing added to the
status row later unless it is added here on purpose.
"""

from __future__ import annotations

import datetime as _dt
import re
import secrets
import time

from boto3.dynamodb.conditions import Key

ROUTES = ("POST /api/sessions/{id}/shares", "GET /api/sessions/{id}/shares",
          "DELETE /api/sessions/{id}/shares/{shareId}", "GET /api/shared/{shareId}")
#: What token_urlsafe(24) makes: 32 characters of the URL-safe alphabet.
SHARE_RE = re.compile(r"^[A-Za-z0-9_-]{32}$")
#: A run id as the BFF makes them (uuid4 hex), with room for older forms.
SESSION_RE = re.compile(r"^[A-Za-z0-9_-]{6,64}$")
EXPIRY_S = {"1h": 3600, "1d": 86400, "7d": 7 * 86400, "30d": 30 * 86400}
DEFAULT_EXPIRY = "7d"
#: Active links one run may have at once.
MAX_ACTIVE = 10
#: How long after expiry a row may stay before DynamoDB's TTL removes it.
TTL_GRACE_S = 86400
#: The parts of a run's status row a shared read returns.
SNAPSHOT_FIELDS = ("session_id", "topic", "overall", "nodes", "history", "hitl", "result",
                   "created", "updated_at", "workflow", "workflowVersion", "subject_id", "user")
#: The same refusal for an unknown, malformed, revoked or expired link, and for a link
#: whose run is gone: which of these it is, is nobody's business.
GONE = "this link is unknown, expired or revoked"


def _now() -> int:
    return int(time.time())


def _error(status: int, message: str):
    import builds
    return builds.BuildError(status, message)


def _iso(epoch: int) -> str:
    return _dt.datetime.fromtimestamp(int(epoch), _dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _public(share_id: str, row: dict) -> dict:
    return {"shareId": share_id, "url": f"/#shared:{share_id}", "expires": _iso(row["expiresAt"]),
            "created": _iso(row["created"])}


def hint(share_id: str) -> str:
    """What the activity log names a link by: enough to tell links apart, not to open one."""
    return str(share_id)[:6]


def active(table, session_id: str) -> list[dict]:
    """The run's links that have neither expired nor been revoked, newest first."""
    now, out = _now(), []
    kw = {"KeyConditionExpression": Key("session_id").eq(f"shares#{session_id}")}
    while True:
        page = table.query(**kw)
        out += [_public(i["ts"], i) for i in page.get("Items", [])
                if int(i.get("expiresAt") or 0) > now and not i.get("revoked")]
        if not page.get("LastEvaluatedKey"):
            break
        kw["ExclusiveStartKey"] = page["LastEvaluatedKey"]
    return sorted(out, key=lambda s: s["created"], reverse=True)


def create(table, session_id: str, owner: str, shared_by: str, expires_in) -> dict:
    """A new link to the run. The caller has checked that `owner` owns it."""
    expires_in = DEFAULT_EXPIRY if expires_in in (None, "") else expires_in
    if not isinstance(expires_in, str) or expires_in not in EXPIRY_S:
        raise _error(400, f"expiresIn is one of {', '.join(EXPIRY_S)}")
    # A soft cap: two links made at the same instant may both pass. It bounds what one
    # run can hand out, it is not a quota anyone gains from racing.
    if len(active(table, session_id)) >= MAX_ACTIVE:
        raise _error(409, f"a run has at most {MAX_ACTIVE} active links: revoke one first")
    now = _now()
    share_id = secrets.token_urlsafe(24)
    row = {"created": now, "expiresAt": now + EXPIRY_S[expires_in],
           "ttl": now + EXPIRY_S[expires_in] + TTL_GRACE_S}
    table.put_item(Item={"session_id": f"share#{share_id}", "ts": "link", "session": session_id,
                         "owner": owner, "sharedBy": shared_by, **row},
                   ConditionExpression="attribute_not_exists(session_id)")
    table.put_item(Item={"session_id": f"shares#{session_id}", "ts": share_id, **row})
    return _public(share_id, row)


def revoke(table, session_id: str, share_id: str) -> None:
    """Revoke one of the run's links. Unknown, already revoked, or another run's: 404."""
    if not SHARE_RE.match(str(share_id or "")):
        raise _error(404, GONE)
    try:
        table.update_item(
            Key={"session_id": f"share#{share_id}", "ts": "link"},
            UpdateExpression="SET revoked = :n",
            # Checked and applied as one step: only this run's link, only once.
            ConditionExpression="attribute_exists(session_id) AND #s = :sid AND attribute_not_exists(revoked)",
            ExpressionAttributeNames={"#s": "session"},
            ExpressionAttributeValues={":n": _now(), ":sid": session_id})
    except Exception as e:
        if getattr(e, "response", {}).get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            raise _error(404, GONE) from None
        raise
    table.delete_item(Key={"session_id": f"shares#{session_id}", "ts": share_id})


def open_link(table, share_id: str) -> dict:
    """The link, if it may still be opened: known, not revoked, not expired. Else 404."""
    if not SHARE_RE.match(str(share_id or "")):
        raise _error(404, GONE)
    row = table.get_item(Key={"session_id": f"share#{share_id}", "ts": "link"}).get("Item")
    if not row or row.get("revoked") or int(row.get("expiresAt") or 0) <= _now():
        raise _error(404, GONE)
    return row


def snapshot(item: dict, logs: list[dict], link: dict) -> dict:
    """What a shared read returns: the allowed parts of the run, its timeline, and the link."""
    out = {k: item[k] for k in SNAPSHOT_FIELDS if k in item}
    out["logs"] = logs
    out.update(sharedBy=str(link.get("sharedBy") or ""), expires=_iso(link["expiresAt"]), readOnly=True)
    return out
