"""Operational analytics: how the deployment is behaving, per tool and per agent.

Read by `GET /api/analytics?from=&to=` (bff/handler.py), which is for whoever holds the
`admin` permission: the rows span every user's runs. Everything here is computed from
the telemetry rows the runtime already writes (app/features/observability/records.py);
nothing new is stored and nothing the runtime writes changes.

WHAT IS READ
------------
The by_date index, one query per day, newest day first, with a ProjectionExpression of
FIELDS only: no prompt, answer, tool query or tool result is requested, so none can be
returned. (Projection does not lower the read cost: DynamoDB bills the size of the item
read.) At most MAX_ROWS rows are read; the response says how many were read and whether
it stopped short, and when it did, the rows it kept are the NEWEST ones.

WHAT IS COUNTED
---------------
Per tool (a `tools` key): its calls (kind "tool" rows), errors (status "error"), p50/p95
latency over those calls, and the Cedar decisions on it (kind "policy" rows, whose label
is the tool key or "<key>___<tool>"): allowed, denied, log-only. The denial rate is
denied / (allowed + denied). In LOG_ONLY mode the Gateway logs its decision to CloudWatch
and the app records "log-only", so those calls are counted but no rate is claimed for them.
A call Cedar denied has no "tool" row (it never ran), only a "policy" row.

Per agent: its calls (kind llm, tool and image rows), errors among them, guardrail checks
and blocks (kind "guardrail"), Cedar decisions and denials (kind "policy"), and p50/p95
latency over its MODEL calls only (kind "llm"): a tool call and a model call take very
different times, and mixing them would make every agent look erratic. The runtime's
compute rows (kind "agentcore", agent "__session__") are not an agent and are left out of
the agent table; they are still counted in `kinds`.

A row whose kind is not one of KINDS is counted under "other", in `kinds` and on its
agent. A row of a counted kind whose status is not one this module knows is counted in
that tool's or agent's `otherStatus`. Neither is ever dropped.

Percentiles are EXACT over the rows read, by the nearest-rank method: sort the n
latencies and take the one at position ceil(p/100 * n) (1-based). So with one call, p50
and p95 are that call; with 4 calls, p95 is the slowest.

HEALTH SCORE (per agent, 0-100)
-------------------------------
    score = 100 - 60*E - 20*D - 10*G - 10*S, clamped to 0..100, rounded half up

    E = errors / calls                       (0 when it made no calls)
    D = Cedar denials / (allowed + denied)   (0 when no decision was recorded)
    G = guardrail blocks / guardrail checks  (0 when nothing was checked)
    S = 1 when the agent has at least 5 model calls and its p95 model latency is more
        than 3 times its own median (p50), else 0

Each term that takes points off is listed as a reason, with its number and the points it
cost. An agent with no calls, no checks and no decisions in the range has no score.
Example: 20 calls, 2 errors (E = 0.1), 1 of 4 Cedar decisions denied (D = 0.25), no
guardrail blocks, steady latency: 100 - 6 - 5 = 89.

The score reads telemetry only. Whether an agent HAS a guardrail is configuration, and is
reported separately by coverage(), not folded into the score.

WHAT IS NOT COVERED
-------------------
  * No alarms: telemetry is in DynamoDB, and the runtime emits no CloudWatch metric for
    errors or denials, so there is nothing for an alarm to watch.
  * A tool called through a deployment without a policy engine records no "policy" row,
    so its Cedar columns are empty, not zero.
  * Coverage is read from the deployed workflow.json; it cannot see a guardrail or a
    policy changed in the console after the deploy.
"""
from __future__ import annotations

import math
from datetime import date, timedelta

from boto3.dynamodb.conditions import Key

#: The only attributes requested. Nothing here holds prompt, answer or tool text.
FIELDS = ("agent_id", "kind", "label", "status", "latency_ms")
#: Most rows one request reads; the response says when it stopped there.
MAX_ROWS = 20000
#: The kinds records.py documents. Anything else is "other".
KINDS = ("llm", "tool", "agentcore", "memory", "guardrail", "policy", "eval", "image")
#: Kinds that are a call the agent made, and so can fail.
CALL_KINDS = ("llm", "tool", "image")
CALL_STATUSES = ("ok", "error")
GUARDRAIL_STATUSES = ("passed", "blocked", "anonymized")
POLICY_STATUSES = ("allowed", "denied", "log-only")
#: Model calls needed before latency spread is judged at all.
MIN_LATENCY_SAMPLES = 5
#: p95 above this multiple of the median counts as erratic latency.
LATENCY_SPREAD = 3
WEIGHTS = {"errors": 60, "denials": 20, "guardrail": 10, "latency": 10}
#: Who reads compute rows: the session, not an agent (meter.record_session_compute).
SESSION_AGENT = "__session__"


# --- reading ---------------------------------------------------------------------------

def date_range(frm: str | None, to: str | None, today: str, max_days: int) -> tuple[date, date]:
    """The days to read (defaults: the last 30). Raises ValueError with the message the
    aggregate route uses for the same mistake."""
    t = date.fromisoformat(today)
    try:
        d1 = date.fromisoformat(to) if to else t
        d0 = date.fromisoformat(frm) if frm else t - timedelta(days=30)
    except ValueError:
        raise ValueError("from/to must be YYYY-MM-DD") from None
    if d1 < d0:
        raise ValueError("`from` must not be after `to`")
    days = (d1 - d0).days + 1
    if days > max_days:
        raise ValueError(f"range too wide: {days} days, maximum {max_days}. Narrow `from`/`to`.")
    return d0, d1


def read_rows(tbl, d0: date, d1: date, max_rows: int = MAX_ROWS) -> tuple[list[dict], bool]:
    """The newest rows from d0 to d1 (both included), at most `max_rows`, and whether
    there were more. Newest first, so a cut keeps what an operator looks at first."""
    names = {f"#f{i}": f for i, f in enumerate(FIELDS)}
    rows: list[dict] = []
    day = d1
    while day >= d0:
        kwargs = {"IndexName": "by_date", "KeyConditionExpression": Key("date").eq(day.isoformat()),
                  "ProjectionExpression": ", ".join(names), "ExpressionAttributeNames": names,
                  "ScanIndexForward": False}
        while True:
            page = tbl.query(**kwargs)
            for it in page.get("Items", []):
                if len(rows) >= max_rows:
                    return rows, True
                rows.append({k: it[k] for k in FIELDS if k in it})
            lek = page.get("LastEvaluatedKey")
            if not lek:
                break
            kwargs["ExclusiveStartKey"] = lek
        day -= timedelta(days=1)
    return rows, False


# --- counting --------------------------------------------------------------------------

def percentile(values: list[int], p: float) -> int | None:
    """Nearest-rank percentile: the value at 1-based position ceil(p/100 * n) of the
    sorted list. None for no values."""
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, math.ceil(p * len(ordered) / 100))
    return ordered[rank - 1]


def _rate(part: int, whole: int) -> float | None:
    return part / whole if whole else None


def _ms(row: dict) -> int | None:
    """The row's latency, or None when it has none (an absent value is not a 0 ms call)."""
    try:
        return int(row["latency_ms"])
    except (KeyError, TypeError, ValueError):
        return None


def _tool_key(label: str) -> str:
    """A policy row names "<key>___<tool>" for a single-tool permit; the tool is the key."""
    return label.split("___", 1)[0]


def _new_tool(key: str) -> dict:
    return {"tool": key, "calls": 0, "errors": 0, "otherStatus": 0, "latencies": [],
            "allowed": 0, "denied": 0, "logOnly": 0}


def _new_agent(aid: str) -> dict:
    return {"agentId": aid, "rows": 0, "calls": 0, "errors": 0, "otherStatus": 0, "other": 0,
            "guardrailChecks": 0, "guardrailBlocks": 0, "allowed": 0, "denied": 0, "logOnly": 0,
            "latencies": []}


def _count_policy(bucket: dict, status: str) -> None:
    if status == "allowed":
        bucket["allowed"] += 1
    elif status == "denied":
        bucket["denied"] += 1
    elif status == "log-only":
        bucket["logOnly"] += 1
    else:
        bucket["otherStatus"] += 1


def _fmt(x: float) -> str:
    """15.0 -> "15", 2.5 -> "2.5": one decimal at most, as an operator would write it."""
    return f"{x:.1f}".rstrip("0").rstrip(".")


def health(a: dict) -> tuple[int | None, list[str]]:
    """The score and its reasons for one agent's counts (see the module docstring)."""
    decisions = a["allowed"] + a["denied"]
    if not (a["calls"] or a["guardrailChecks"] or decisions):
        return None, ["no calls in this range"]
    e = _rate(a["errors"], a["calls"]) or 0
    d = _rate(a["denied"], decisions) or 0
    g = _rate(a["guardrailBlocks"], a["guardrailChecks"]) or 0
    p50, p95 = percentile(a["latencies"], 50), percentile(a["latencies"], 95)
    s = 1 if (len(a["latencies"]) >= MIN_LATENCY_SAMPLES and p50 is not None
              and p95 > LATENCY_SPREAD * p50) else 0
    terms = [
        (WEIGHTS["errors"] * e, f"error rate {_fmt(100 * e)}% ({a['errors']} of {a['calls']} calls)"),
        (WEIGHTS["denials"] * d, f"Cedar denied {_fmt(100 * d)}% of tool calls ({a['denied']} of {decisions})"),
        (WEIGHTS["guardrail"] * g,
         f"guardrail blocked {_fmt(100 * g)}% of checks ({a['guardrailBlocks']} of {a['guardrailChecks']})"),
        (WEIGHTS["latency"] * s, f"p95 model latency {p95} ms is more than {LATENCY_SPREAD}x its median {p50} ms"),
    ]
    score = 100 - sum(pts for pts, _ in terms)
    score = math.floor(min(100, max(0, score)) + 0.5)
    return score, [f"{why} (-{_fmt(pts)})" for pts, why in terms if pts > 0]


def summarize(rows: list[dict], capped: bool = False, max_rows: int = MAX_ROWS) -> dict:
    """Per-tool and per-agent counts, rates, latencies and health over `rows`."""
    tools: dict[str, dict] = {}
    agents: dict[str, dict] = {}
    kinds: dict[str, int] = {}
    for r in rows:
        kind = str(r.get("kind") or "")
        status = str(r.get("status") or "")
        known = kind in KINDS
        kinds[kind if known else "other"] = kinds.get(kind if known else "other", 0) + 1
        if kind == "agentcore":
            continue
        aid = str(r.get("agent_id") or "") or "(unknown)"
        a = agents.setdefault(aid, _new_agent(aid))
        a["rows"] += 1
        if not known:
            a["other"] += 1
        elif kind in CALL_KINDS:
            a["calls"] += 1
            if status == "error":
                a["errors"] += 1
            elif status not in CALL_STATUSES:
                a["otherStatus"] += 1
            ms = _ms(r)
            if kind == "llm" and ms is not None:
                a["latencies"].append(ms)
            if kind == "tool":
                key = str(r.get("label") or "(unknown)")
                t = tools.setdefault(key, _new_tool(key))
                t["calls"] += 1
                if ms is not None:
                    t["latencies"].append(ms)
                if status == "error":
                    t["errors"] += 1
                elif status not in CALL_STATUSES:
                    t["otherStatus"] += 1
        elif kind == "guardrail":
            a["guardrailChecks"] += 1
            if status == "blocked":
                a["guardrailBlocks"] += 1
            elif status not in GUARDRAIL_STATUSES:
                a["otherStatus"] += 1
        elif kind == "policy":
            key = _tool_key(str(r.get("label") or "(unknown)"))
            _count_policy(tools.setdefault(key, _new_tool(key)), status)
            _count_policy(a, status)
    tool_out = []
    for t in tools.values():
        lat = t.pop("latencies")
        tool_out.append({**t, "errorRate": _rate(t["errors"], t["calls"]),
                         "p50Ms": percentile(lat, 50), "p95Ms": percentile(lat, 95),
                         "denialRate": _rate(t["denied"], t["allowed"] + t["denied"])})
    agent_out = []
    for a in agents.values():
        score, reasons = health(a)
        lat = a.pop("latencies")
        agent_out.append({**a, "errorRate": _rate(a["errors"], a["calls"]),
                          "guardrailBlockRate": _rate(a["guardrailBlocks"], a["guardrailChecks"]),
                          "denialRate": _rate(a["denied"], a["allowed"] + a["denied"]),
                          "p50Ms": percentile(lat, 50), "p95Ms": percentile(lat, 95),
                          "health": score, "reasons": reasons})
    return {"rowsRead": len(rows), "capped": capped, "maxRows": max_rows,
            "percentiles": "exact over the rows read (nearest rank)",
            "kinds": dict(sorted(kinds.items())),
            "tools": sorted(tool_out, key=lambda x: x["tool"]),
            "agents": sorted(agent_out, key=lambda x: x["agentId"])}


# --- configuration ---------------------------------------------------------------------

_GUARDRAIL_POLICY_KEYS = ("contentFilters", "deniedWords", "managedWordLists", "deniedTopics",
                          "piiEntities")


def coverage(wf: dict) -> dict:
    """From the deployed workflow: every agent's guardrail, with a warning where none
    screens it, and every tool whose calls no Cedar policy decides.

    An agent is screened when agentcore.guardrails turns on `input` and/or `output` AND a
    guardrail is there to apply: a named one (`use`, which must be in `guardrails`), its
    own `guardrailId`, or the deployment's (the `guardrail` block, which the IaC creates
    only when it enforces something: terraform/guardrail.tf gr_has_policy)."""
    wf = wf if isinstance(wf, dict) else {}
    gr = wf.get("guardrail") if isinstance(wf.get("guardrail"), dict) else {}
    deployment_guardrail = any(gr.get(k) for k in _GUARDRAIL_POLICY_KEYS)
    named = wf.get("guardrails") if isinstance(wf.get("guardrails"), dict) else {}
    agents = []
    for aid, a in (wf.get("agents") or {}).items():
        a = a if isinstance(a, dict) else {}
        g = (a.get("agentcore") or {}).get("guardrails") if isinstance(a.get("agentcore"), dict) else None
        g = g if isinstance(g, dict) else {}
        sides = [s for s in ("input", "output") if g.get(s) is True]
        source, warning = None, None
        if g.get("use"):
            source = f"named guardrail {g['use']}"
            if g["use"] not in named:
                warning = (f"names guardrail {g['use']!r} (agentcore.guardrails.use), which is not in "
                           f"`guardrails`: its checks fail")
        elif g.get("guardrailId"):
            source = "its own guardrailId"
        elif deployment_guardrail:
            source = "the deployment guardrail"
        if not sides:
            warning = "no guardrail: neither its input nor its output is screened"
        elif source is None:
            warning = (f"screens its {' and '.join(sides)}, but this deployment has no guardrail "
                       f"to apply (the `guardrail` block enforces nothing), so nothing is checked")
        agents.append({"agentId": aid, "name": str(a.get("name") or aid), "input": "input" in sides,
                       "output": "output" in sides, "guardrail": source if sides else None,
                       "warning": warning})
    policy = (wf.get("orchestrator") or {}).get("policy") if isinstance(wf.get("orchestrator"), dict) else None
    policy = policy if isinstance(policy, dict) else {}
    enabled = policy.get("enabled", True) is not False
    mode = str(policy.get("mode") or "ENFORCE").upper()
    custom = " ".join(str(c.get("statement") or "") for c in policy.get("custom") or [] if isinstance(c, dict))
    tools = []
    for key, t in (wf.get("tools") or {}).items():
        t = t if isinstance(t, dict) else {}
        own = t.get("policy") if isinstance(t.get("policy"), dict) else {}
        attached = bool(t.get("policies")) or any(
            f'AgentCore::Action::"{key}{end}' in custom for end in ('"', "___"))
        if not enabled:
            warning = ("the Cedar policy engine is off (orchestrator.policy.enabled false): every call "
                       "is allowed with no policy check")
        elif mode == "LOG_ONLY":
            warning = "Cedar runs in LOG_ONLY: decisions are logged to CloudWatch, not enforced"
        elif own.get("permit") is False and not attached:
            warning = ("no permit (policy.permit false) and no policy names it: Cedar denies every "
                       "call to it")
        else:
            warning = None
        tools.append({"tool": key, "type": str(t.get("type") or ""), "warning": warning})
    return {"agents": agents, "tools": tools,
            "policyEngine": {"enabled": enabled, "mode": mode}}


def report(tbl, wf: dict, frm: str | None, to: str | None, today: str, max_days: int,
           max_rows: int | None = None) -> tuple[int, dict]:
    """(status, body) for GET /api/analytics. Validates the range before any read."""
    max_rows = MAX_ROWS if max_rows is None else max_rows
    try:
        d0, d1 = date_range(frm, to, today, max_days)
    except ValueError as e:
        return 400, {"error": str(e)}
    rows, capped = read_rows(tbl, d0, d1, max_rows) if tbl is not None else ([], False)
    return 200, {"from": d0.isoformat(), "to": d1.isoformat(),
                 **summarize(rows, capped, max_rows), "coverage": coverage(wf)}
