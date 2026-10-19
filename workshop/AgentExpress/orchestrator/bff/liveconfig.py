"""Live config, the BFF half: versions of an agent's prompt and model settings, the pointer
to the active one, and the read a run makes when it starts. Off unless
`"orchestrator": {"liveConfig": true}`. The design note is app/common/liveconfig.py.

WHERE THEY LIVE
Reserved rows in the telemetry table, next to the `_limit#` counters (bff/limits.py). Run ids
are 12 hex digits and never start with `_liveconfig#`, and no row has a `date`, so the by_date
index the aggregation reads never sees them and no run route can name them (each first looks
the id up in the status table, where they do not exist).

    session_id = _liveconfig#versions   sk = v#00000001   version, at, by, note, overlay (JSON)
    session_id = _liveconfig#pointer    sk = active       pointer {"active": n}, at, by

A version is written once, with a condition that its key is new, and never changed. The pointer
is one row, changed with a condition on what the caller last saw, so two admins cannot both
move it from the same place without one of them being told. Only the BFF touches these rows,
and its IAM is held to keys that start `_liveconfig#` (LiveConfig in terraform/liveconfig.tf
and the CDK stack). The runtimes read nothing: the overlay rides in the start payload.

WHAT A VERSION MAY CHANGE
Per agent: `systemPrompt`, `model`, `temperature`, `topP`, `maxTokens`, `stopSequences`.
Anything else is refused by name, with the reason. The five model settings are checked by the
same validator a workflow.json is (bff/validate_build.py) on the deployed workflow with the
version laid over it; a model must also be one the runtimes' IAM grant reaches
(`model_allowed`) and able to do what the agent asks of it (handler.model_problems).
"""
from __future__ import annotations

import copy
import json
import re
import time

import builds
import clock
import validate_build
import workflow

PREFIX = "_liveconfig#"
VERSIONS_PK = f"{PREFIX}versions"
POINTER_PK = f"{PREFIX}pointer"
POINTER_SK = "active"
#: Mirrors FIELDS in app/common/liveconfig.py (tests/test_liveconfig.py holds the two together).
FIELDS = ("systemPrompt", "model", "temperature", "topP", "maxTokens", "stopSequences")
SETTINGS = FIELDS[1:]
PROMPT_MAX = 32000
NOTE_MAX = 500
#: The whole overlay, serialized. It rides in the run's start payload and graph state.
OVERLAY_MAX_BYTES = 64 * 1024
#: The most versions listed; the newest first.
LIST_MAX = 200
#: How long a container trusts the pointer it last read, in seconds.
POINTER_TTL_S = 5
OFF = "live config is off: set orchestrator.liveConfig to true in workflow.json and redeploy"
#: Why the rest of an agent's entry cannot change live: it is provisioned, not read.
NOT_LIVE = ("only the prompt and the model settings (model, temperature, topP, maxTokens, "
            "stopSequences) change live; `{key}` is provisioned from workflow.json by the deploy "
            "(IAM, Gateway, Policy or the graph), so it needs a redeploy")

#: The model resources the runtimes' IAM grant covers, on both IaC paths (BedrockModelInvocation
#: in terraform/main.tf and subagent_runtimes.tf, and the runtime roles in the CDK stack;
#: tests/test_liveconfig.py holds them together). Anything else would be refused by IAM on the
#: first call, so it is refused here instead.
_FOUNDATION_ARN = re.compile(r"^arn:aws:bedrock:[a-z0-9-]+::foundation-model/[A-Za-z0-9._:-]+$")
_PROFILE_ARN = re.compile(
    r"^arn:aws:bedrock:(?P<region>[a-z0-9-]+):(?P<account>\d{12}):"
    r"(?:inference-profile|application-inference-profile)/[A-Za-z0-9._:-]+$")
#: A bare id: a foundation model (`anthropic.claude-...`) or a system profile (`us.anthropic...`).
_MODEL_ID = re.compile(r"^[a-z0-9-]+(\.[A-Za-z0-9-]+)+(:[0-9a-z]+){0,2}$")

_pointer_cache: dict = {"at": 0.0, "value": None}
_version_cache: dict[int, dict] = {}


def enabled(raw: dict | None) -> bool:
    return ((raw or {}).get("orchestrator") or {}).get("liveConfig") is True


def model_allowed(model: str, region: str, account: str) -> str:
    """"" if the runtimes may call `model`, else why not."""
    m = str(model or "").strip()
    if not m or len(m) > 2048:
        return "must be a Bedrock model id or ARN"
    if m.startswith("arn:"):
        if _FOUNDATION_ARN.match(m):
            return ""
        got = _PROFILE_ARN.match(m)
        if not got:
            return (f"{m} is not a model this deployment's runtimes may call: their IAM grant covers "
                    f"foundation models and inference profiles, not custom, imported or provisioned "
                    f"models")
        if got["region"] != region or not account or got["account"] != account:
            return (f"{m} is outside this deployment: the runtimes may call inference profiles in "
                    f"{region} of this account only")
        return ""
    if not _MODEL_ID.match(m):
        return f"{m} is not a Bedrock model id"
    return ""


def editable(raw: dict) -> dict:
    """The agents a version may change, with their deployed settings (version 0)."""
    out = {}
    for aid, a in (raw.get("agents") or {}).items():
        # A task step has no model or prompt to change.
        # A harness agent's prompt and model are set on its AgentCore Harness at deploy time.
        if not isinstance(a, dict) or a.get("runtime") in ("a2a", "task", "harness"):
            continue
        out[aid] = {"name": a.get("name") or aid, "runtime": a.get("runtime") or workflow._DEFAULTS["agent"]["runtime"],
                    "deployed": {k: a[k] for k in SETTINGS if k in a}}
    return out


def _pkey(n: int) -> str:
    return f"v#{n:08d}"


def _public(item: dict, full: bool = False) -> dict:
    overlay = json.loads(item.get("overlay") or "{}")
    out = {"version": int(item["version"]), "at": item.get("at", ""), "by": item.get("by", ""),
           "note": item.get("note", ""), "agents": sorted(overlay)}
    if full:
        out["overlay"] = overlay
    return out


def pointer(table, consistent: bool = True) -> dict:
    """{"active": n, "at", "by"}; {"active": 0} before anything was activated."""
    item = table.get_item(Key={"session_id": POINTER_PK, "sk": POINTER_SK},
                          ConsistentRead=consistent).get("Item") or {}
    p = dict(item.get("pointer") or {})
    return {"active": int(p.get("active") or 0), "at": item.get("at", ""), "by": item.get("by", "")}


def versions(table) -> list[dict]:
    from boto3.dynamodb.conditions import Key
    kw = {"KeyConditionExpression": Key("session_id").eq(VERSIONS_PK), "ScanIndexForward": False}
    out: list[dict] = []
    while len(out) < LIST_MAX:
        page = table.query(**kw, Limit=LIST_MAX - len(out))
        out += [_public(i) for i in page.get("Items", [])]
        if not page.get("LastEvaluatedKey"):
            break
        kw["ExclusiveStartKey"] = page["LastEvaluatedKey"]
    return out


def version(table, n: int) -> dict:
    item = table.get_item(Key={"session_id": VERSIONS_PK, "sk": _pkey(n)},
                          ConsistentRead=True).get("Item")
    if not item:
        raise builds.BuildError(404, f"no live config version {n}")
    return _public(item, full=True)


def for_run(table) -> dict:
    """{"version": n, "agents": overlay} for a run starting now; version 0 on any failure.

    The pointer is trusted for POINTER_TTL_S; a version row never changes, so it is kept."""
    try:
        if table is None:
            raise RuntimeError("this deployment has no telemetry table")
        now = time.time()
        if _pointer_cache["value"] is None or now - _pointer_cache["at"] >= POINTER_TTL_S:
            _pointer_cache.update(at=now, value=pointer(table, consistent=False)["active"])
        n = int(_pointer_cache["value"] or 0)
        if n <= 0:
            return {"version": 0, "agents": {}}
        if n not in _version_cache:
            if len(_version_cache) > 64:
                _version_cache.clear()
            _version_cache[n] = version(table, n)["overlay"]
        return {"version": n, "agents": _version_cache[n]}
    except Exception as e:  # noqa: BLE001 - a run must not fail because this is unreadable
        print(f"[liveconfig] using deployed config: {type(e).__name__}: {e}")
        return {"version": 0, "agents": {}}


def check(raw: dict, overlay, region: str, account: str, model_problems=None) -> list[dict]:
    """Every problem with `overlay` as a version of `raw`, as {path, message}; [] if none."""
    issues: list[dict] = []

    def bad(path: str, message: str) -> None:
        issues.append({"path": path, "message": message})

    if not isinstance(overlay, dict) or not overlay:
        bad("agents", "name at least one agent and what changes for it")
        return issues
    known = raw.get("agents") or {}
    merged = copy.deepcopy(raw)
    for aid, fields in overlay.items():
        path = f"agents.{aid}"
        if aid not in known:
            bad(path, f"{aid!r} is not an agent of this deployment")
            continue
        if (known[aid] or {}).get("runtime") == "a2a":
            bad(path, "a remote (a2a) agent chooses its own model and prompt; nothing here can change them")
            continue
        if (known[aid] or {}).get("runtime") == "task":
            bad(path, "a task step makes no model call; nothing here can change it")
            continue
        if (known[aid] or {}).get("runtime") == "harness":
            bad(path, "a harness agent's instructions and model are set on its AgentCore Harness; change them in the build and deploy")
            continue
        if not isinstance(fields, dict) or not fields:
            bad(path, "must change at least one setting")
            continue
        for key, value in fields.items():
            if key not in FIELDS:
                bad(f"{path}.{key}", NOT_LIVE.format(key=key))
            elif key == "systemPrompt":
                if not isinstance(value, str) or not value.strip():
                    bad(f"{path}.systemPrompt", "must be the prompt's text")
                elif len(value) > PROMPT_MAX:
                    bad(f"{path}.systemPrompt", f"is at most {PROMPT_MAX} characters")
            else:
                if key == "model" and isinstance(value, str):
                    why = model_allowed(value, region, account)
                    if why:
                        bad(f"{path}.model", why)
                merged["agents"][aid][key] = value
    if issues:
        return issues
    # The same rules a workflow.json is held to, on the deployed one with this laid over it,
    # counting only what this version adds at the fields it sets.
    before = {(i["path"], i["message"]) for i in validate_build.errors(raw)}
    for i in validate_build.errors(merged):
        p = i["path"]
        touched = any(p == f"agents.{aid}.{k}" or p.startswith(f"agents.{aid}.{k}[")
                      for aid, fields in overlay.items() for k in fields)
        if touched and (p, i["message"]) not in before:
            bad(p, i["message"])
    if not issues and model_problems is not None and any("model" in f for f in overlay.values()):
        before = {(i["path"], i["message"]) for i in model_problems(raw)}
        for i in model_problems(merged):
            if i.get("severity") == "error" and (i["path"], i["message"]) not in before:
                bad(i["path"], i["message"])
    if not issues and len(json.dumps(overlay)) > OVERLAY_MAX_BYTES:
        bad("agents", f"a version is at most {OVERLAY_MAX_BYTES // 1024} KB")
    return issues


def create(table, raw: dict, body: dict, who: str, region: str, account: str,
           model_problems=None) -> dict:
    """Save `body` ({"agents": overlay, "note"}) as the next version. Does not activate it."""
    extra = sorted(set(body) - {"agents", "note"})
    if extra:
        raise builds.BuildError(400, f"unknown field {extra[0]!r}; a version takes agents and note")
    note = body.get("note") or ""
    if not isinstance(note, str) or len(note) > NOTE_MAX:
        raise builds.BuildError(400, f"note must be text, at most {NOTE_MAX} characters")
    overlay = body.get("agents")
    issues = check(raw, overlay, region, account, model_problems)
    if issues:
        raise _Invalid(issues)
    for _attempt in range(3):
        latest = versions_top(table)
        n = latest + 1
        try:
            table.put_item(Item={"session_id": VERSIONS_PK, "sk": _pkey(n), "version": n,
                                 "at": clock.now_str(), "by": who, "note": note.strip(),
                                 "overlay": json.dumps(overlay, sort_keys=True)},
                           ConditionExpression="attribute_not_exists(sk)")
            return version(table, n)
        except Exception as e:  # noqa: BLE001
            if _code(e) != "ConditionalCheckFailedException":
                raise
    raise builds.BuildError(409, "another version was being saved at the same time; save again")


def versions_top(table) -> int:
    from boto3.dynamodb.conditions import Key
    items = table.query(KeyConditionExpression=Key("session_id").eq(VERSIONS_PK),
                        ScanIndexForward=False, Limit=1, ConsistentRead=True).get("Items", [])
    return int(items[0]["version"]) if items else 0


def activate(table, n, who: str, expected=None) -> tuple[int, int]:
    """Point new runs at version `n` (0 = the deployed config): (before, after). Rollback is
    activating an older one. `expected` is the version the caller saw active; a pointer that
    has moved since is a 409, not a silent overwrite."""
    if not isinstance(n, int) or isinstance(n, bool) or n < 0:
        raise builds.BuildError(400, "version must be a whole number, 0 for the deployed config")
    if n:
        version(table, n)           # 404 for one that does not exist
    before = pointer(table)["active"]
    if expected is not None and expected != before:
        raise builds.BuildError(409, f"version {before} is active now, not {expected}: reload and try again")
    # Checked and written as one step: the pointer is still what was read above.
    cond = "#p.active = :was" if before else "attribute_not_exists(session_id) OR #p.active = :was"
    try:
        table.update_item(
            Key={"session_id": POINTER_PK, "sk": POINTER_SK},
            UpdateExpression="SET #p = :p, #at = :at, #by = :by",
            ConditionExpression=cond,
            ExpressionAttributeNames={"#p": "pointer", "#at": "at", "#by": "by"},
            ExpressionAttributeValues={":p": {"active": n}, ":at": clock.now_str(), ":by": who,
                                       ":was": before})
    except Exception as e:  # noqa: BLE001
        if _code(e) == "ConditionalCheckFailedException":
            raise builds.BuildError(409, "the active version changed while this was saved: reload and try again") from e
        raise
    # This container's next run reads the new pointer at once; others within POINTER_TTL_S.
    _pointer_cache.update(at=0.0, value=None)
    return before, n


class _Invalid(builds.BuildError):
    def __init__(self, issues: list[dict]):
        # Every problem in the message, which is what the page shows (web/src/api.ts).
        super().__init__(400, "; ".join(f"{i['path']}: {i['message']}" for i in issues))
        self.issues = issues


def _code(e: Exception) -> str:
    return str(getattr(e, "response", {}).get("Error", {}).get("Code", ""))


def route(route_key: str, params: dict, body: dict, event: dict, table, raw: dict, *,
          region: str, audit, model_problems=None) -> tuple[int, dict]:
    """One /api/liveconfig call, for a caller the handler has already checked is an admin."""
    claims = ((event.get("requestContext") or {}).get("authorizer") or {}).get("jwt", {}).get("claims", {}) or {}
    who = str(claims.get("email") or claims.get("sub") or "")
    account = str((event.get("requestContext") or {}).get("accountId") or "")
    if table is None:
        return 404, {"error": "this deployment has no telemetry table to keep versions in"}
    try:
        if route_key == "GET /api/liveconfig":
            return 200, {"pointer": pointer(table), "agents": editable(raw), "fields": list(FIELDS),
                         "promptMax": PROMPT_MAX, "noteMax": NOTE_MAX}
        if route_key == "GET /api/liveconfig/versions":
            return 200, {"versions": versions(table)}
        if route_key == "GET /api/liveconfig/versions/{version}":
            raw_n = str((params or {}).get("version") or "")
            if not re.fullmatch(r"[1-9]\d{0,7}", raw_n):
                return 400, {"error": "a version is a whole number from 1"}
            return 200, version(table, int(raw_n))
        if route_key == "POST /api/liveconfig/versions":
            got = create(table, raw, body if isinstance(body, dict) else {}, who, region, account,
                         model_problems)
            audit("liveconfig.created", version=got["version"], agents=got["agents"],
                  note=got["note"][:200])
            return 200, got
        if route_key == "PUT /api/liveconfig/active":
            b = body if isinstance(body, dict) else {}
            before, after = activate(table, b.get("version"), who, b.get("from"))
            audit("liveconfig.activated", before=before, after=after)
            return 200, {"before": before, "pointer": pointer(table)}
    except _Invalid as e:
        return 400, {"error": str(e), "issues": e.issues}
    except builds.BuildError as e:
        return e.status, {"error": str(e)}
    return 404, {"error": f"no route for {route_key}"}
