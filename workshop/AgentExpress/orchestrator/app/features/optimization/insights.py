"""AgentCore Insights — cross-run failure/intent/summary analysis.

Insights is a BATCH EVALUATION over many sessions' traces. It answers "why is the
agent failing, what are users asking for, how is it solving problems" by running
the built-in insight analyzers over a window of runtime traces:
  * Builtin.Insight.FailureAnalysis  — prioritized failure-pattern clusters
  * Builtin.Insight.UserIntent       — user-intent clusters
  * Builtin.Insight.ExecutionSummary — behavior summaries

Unlike per-agent Evaluations (tied to one run), Insights is manual/periodic and
spans ALL runs in the lookback window, giving a cross-run view of quality trends.

State: one reserved row in the insights table (id="latest") holding the latest
run's id/arn/status plus a compact findings JSON. Best-effort — Insights never
affects a workflow run.

Requires CloudWatch Transaction Search (terraform/transaction_search.tf) so the
runtime traces the analyzers read are indexed.
"""

import json
import os
import time
from datetime import UTC

from app.common import defaults
from app.common.config import INSIGHTS as CONFIG_INSIGHTS
from app.common.config import REGION

_INSIGHTS_TABLE = os.getenv("INSIGHTS_TABLE", "")
_INSIGHTS_KEY = "latest"

_INSIGHT_IDS = [
    "Builtin.Insight.FailureAnalysis",
    "Builtin.Insight.UserIntent",
    "Builtin.Insight.ExecutionSummary",
]

# Timings, from `orchestrator.insights` in workflow.json. Insights was the ONE feature
# with no config block at all — evaluations, guardrails, policy, memory and the chatbot
# all have one — so a customer whose runs take longer than fifteen minutes to analyse,
# or who wants a different default window, had to edit this file.
# Defaults from app/defaults.json (generated from app/keys.json), so the numbers a
# customer would override are declared beside the key they belong to rather than here.
_INSIGHTS_CFG = {**defaults.get("orchestrator", "insights"), **CONFIG_INSIGHTS}
_POLL_TIMEOUT = int(_INSIGHTS_CFG["pollTimeoutSeconds"])
_POLL_INTERVAL = int(_INSIGHTS_CFG["pollIntervalSeconds"])
_DEFAULT_LOOKBACK_HOURS = int(_INSIGHTS_CFG["lookbackHours"])
_TERMINAL = ("COMPLETED", "COMPLETED_WITH_ERRORS", "FAILED", "STOPPED")


def _agentcore():
    import boto3
    return boto3.client("bedrock-agentcore", region_name=REGION)


def _table():
    import boto3
    return boto3.resource("dynamodb", region_name=REGION).Table(_INSIGHTS_TABLE)


def _now() -> str:
    from app.common import clock
    return clock.now_str()


def _store(fields: dict) -> str:
    """Persist the latest insights state. Returns "" on success, else the reason.

    RETURNS the failure instead of only printing it. It used to swallow its own write
    error entirely, so `run_batch` could hand the caller
    `{"status": "COMPLETED", "findings": {...}}` while the table held nothing at all and
    `get_latest()` — which the UI reads — reported `{"status": "none"}`. The run looked
    successful from the API and absent from the panel, with no way to connect the two.
    """
    if not _INSIGHTS_TABLE:
        return "INSIGHTS_TABLE is not set, so there is nowhere to persist findings"
    try:
        item = {"id": _INSIGHTS_KEY, "updated_at": _now(), **fields}
        _table().put_item(Item=item)
    except Exception as e:  # noqa: BLE001
        reason = f"{type(e).__name__}: {e}"
        print(f"[insights] store failed: {reason}")
        return reason
    return ""


def _load() -> dict:
    if not _INSIGHTS_TABLE:
        return {}
    try:
        return _table().get_item(Key={"id": _INSIGHTS_KEY}).get("Item") or {}
    except Exception as e:  # noqa: BLE001
        print(f"[insights] load failed: {type(e).__name__}: {e}")
        return {}


def _known_sessions() -> set | None:
    """Session ids that still exist in the status table (i.e. still openable in
    the UI). Insights analyzes CloudWatch runtime traces over a window that can
    outlive a run's DynamoDB rows, so some analyzed sessions are no longer
    openable — we flag each session's `available` so the UI can disable dead
    links instead of opening an empty run view.

    Returns None when we could not find out (no STATUS_TABLE, or the scan failed or was
    denied), which `_avail` treats differently from an empty set. Returning an empty set
    for a FAILED scan is what made every link look openable."""
    table = os.getenv("STATUS_TABLE", "")
    if not table:
        return None
    try:
        import boto3
        t = boto3.resource("dynamodb", region_name=REGION).Table(table)
        out, kw = set(), {"ProjectionExpression": "session_id"}
        while True:
            page = t.scan(**kw)
            for it in page.get("Items", []):
                if it.get("session_id"):
                    out.add(it["session_id"])
            lek = page.get("LastEvaluatedKey")
            if not lek:
                break
            kw["ExclusiveStartKey"] = lek
        return out
    except Exception as e:  # noqa: BLE001
        print(f"[insights] known-sessions scan failed: {type(e).__name__}: {e}")
        # None, not an empty set. "The scan failed" and "there are no runs" are
        # different facts and _avail has to tell them apart — see below.
        return None


def _avail(sid: str, known: set | None) -> bool:
    """Whether a run is still openable in the UI.

    `known is None` means we could not find out (no STATUS_TABLE, or the scan failed or
    was denied), so every link is reported available rather than greying them all out.
    `known == set()` means we DID find out and there are no runs, so nothing is
    openable.

    Those two used to collapse into one: the scan returned an empty set on any
    exception and this returned `not known` — True — so a failed or unauthorised scan
    reported every findings link as openable, and clicking one opened an empty run view
    with nothing anywhere to say the availability check had not actually run.
    """
    if known is None:
        return True
    return sid in known


def _short_sid(sid) -> str:
    """AgentCore reports the padded 33-char runtime session id; our app
    session_id is its first 12 chars (uuid4().hex[:12], zero-padded)."""
    s = str(sid or "")
    return s[:12] if len(s) > 12 else s


def _clip(v, n) -> str:
    return str(v or "")[:n]


# Caps keep the stored findings JSON well under DynamoDB's 400 KB item limit
# while preserving the console's level of detail.
_MAX_CLUSTERS = 25
_MAX_SUBCATS = 6
_MAX_ROOTCAUSES = 6
_MAX_SESSIONS = 10
_MAX_SPANS = 5


def _failure_spans(session: dict) -> list:
    """A failed session's span-level evidence: spanId + the strongest signal's
    category/evidence (what the console shows under 'Failure spans')."""
    out = []
    for fs in (session.get("failureSpans") or [])[:_MAX_SPANS]:
        sigs = fs.get("signals") or []
        # keep the highest-confidence signal's evidence
        best = max(sigs, key=lambda s: float(s.get("confidence", 0) or 0), default={})
        out.append({
            "spanId": _clip(fs.get("spanId"), 32),
            "category": _clip(best.get("category"), 80),
            "evidence": _clip(best.get("evidence"), 600),
        })
    return out


def _failure_sessions(node: dict, known: set) -> list:
    """Affected sessions on a root cause: session id + why it failed + the fix
    type + span evidence. `available` = the run still exists in the status table."""
    out = []
    for a in (node.get("affectedSessions") or [])[:_MAX_SESSIONS]:
        sid = _short_sid(a.get("sessionId"))
        out.append({
            "sid": sid,
            "available": _avail(sid, known),
            "explanation": _clip(a.get("explanation"), 700),
            "fixType": _clip(a.get("fixType"), 60),
            "recommendation": _clip(a.get("recommendation"), 500),
            "spans": _failure_spans(a),
        })
    return out


def _failure_clusters(items: list, known: set) -> list:
    """Full failure tree: cluster -> subCategories -> rootCauses -> sessions ->
    spans (mirrors the AgentCore console's Failure details view)."""
    out = []
    for c in (items or [])[:_MAX_CLUSTERS]:
        subcats = []
        for sc in (c.get("subCategories") or [])[:_MAX_SUBCATS]:
            rcs = [
                {
                    "name": _clip(rc.get("name"), 200),
                    "rootCause": _clip(rc.get("rootCause"), 900),
                    "recommendation": _clip(rc.get("recommendation"), 700),
                    "affectedSessionCount": int(rc.get("affectedSessionCount", 0) or 0),
                    "sessions": _failure_sessions(rc, known),
                }
                for rc in (sc.get("rootCauses") or [])[:_MAX_ROOTCAUSES]
            ]
            subcats.append({
                "name": _clip(sc.get("name"), 200),
                "description": _clip(sc.get("description"), 600),
                "affectedSessionCount": int(sc.get("affectedSessionCount", 0) or 0),
                "rootCauses": rcs,
            })
        out.append({
            "name": _clip(c.get("name"), 200),
            "description": _clip(c.get("description"), 600),
            "affectedSessionCount": int(c.get("affectedSessionCount", 0) or 0),
            "subCategories": subcats,
        })
    return out


def _intent_clusters(items: list, known: set) -> list:
    """User-intent clusters with a sample user message per affected session."""
    out = []
    for c in (items or [])[:_MAX_CLUSTERS]:
        sess = []
        for a in (c.get("affectedSessions") or [])[:_MAX_SESSIONS]:
            msgs = [_clip(m, 300) for m in (a.get("userMessages") or [])[:2] if m]
            sid = _short_sid(a.get("sessionId"))
            sess.append({"sid": sid, "available": _avail(sid, known), "messages": msgs})
        out.append({
            "name": _clip(c.get("name"), 200),
            "description": _clip(c.get("description"), 600),
            "affectedSessionCount": int(c.get("affectedSessionCount", 0) or 0),
            "sessions": sess,
        })
    return out


def _summary_clusters(items: list, known: set) -> list:
    """Execution-summary clusters with each session's approach + outcome."""
    out = []
    for c in (items or [])[:_MAX_CLUSTERS]:
        sess = []
        for a in (c.get("affectedSessions") or [])[:_MAX_SESSIONS]:
            sid = _short_sid(a.get("sessionId"))
            sess.append({
                "sid": sid,
                "available": _avail(sid, known),
                "approach": _clip(a.get("approachTaken"), 400),
                "outcome": _clip(a.get("finalOutcome"), 400),
            })
        out.append({
            "name": _clip(c.get("name"), 200),
            "description": _clip(c.get("description"), 600),
            "affectedSessionCount": int(c.get("affectedSessionCount", 0) or 0),
            "sessions": sess,
        })
    return out


def _findings(resp: dict) -> dict:
    """Compact the GetBatchEvaluation result into UI-friendly findings, preserving
    the console's detail: failure root causes + affected sessions + span evidence,
    user intents (sample messages), and execution summaries (approach/outcome)."""
    er = resp.get("evaluationResults") or {}
    known = _known_sessions()
    return {
        "sessions": {
            "total": int(er.get("totalNumberOfSessions", 0) or 0),
            "completed": int(er.get("numberOfSessionsCompleted", 0) or 0),
            "failed": int(er.get("numberOfSessionsFailed", 0) or 0),
        },
        "failures": _failure_clusters((resp.get("failureAnalysisResult") or {}).get("failures"), known),
        "userIntents": _intent_clusters((resp.get("userIntentResult") or {}).get("userIntents"), known),
        "executionSummaries": _summary_clusters(
            (resp.get("executionSummaryResult") or {}).get("executionSummaries"), known),
    }


def run_batch(lookback_hours: int = _DEFAULT_LOOKBACK_HOURS, user: str = "") -> dict:
    """Start an insights batch evaluation over the app's recent runtime traces,
    poll to completion, and store the findings. Returns a summary dict."""
    import uuid
    from datetime import datetime, timedelta

    from app.features.evaluations import service as ev

    src = ev.runtime_trace_sources()
    if not src.get("logGroupNames") or not src.get("serviceNames"):
        return {"status": "error",
                "error": "No runtime trace sources found (is Transaction Search on, and has a run completed?)."}

    end = datetime.now(UTC)
    hours = int(lookback_hours or _DEFAULT_LOOKBACK_HOURS)
    start = end - timedelta(hours=hours)
    name = f"insights_{uuid.uuid4().hex[:12]}"  # pattern [a-zA-Z][a-zA-Z0-9_]{0,47} (no hyphens)

    req = {
        "batchEvaluationName": name,
        "insights": [{"insightId": i} for i in _INSIGHT_IDS],
        "dataSourceConfig": {"cloudWatchLogs": {
            "serviceNames": src["serviceNames"],
            "logGroupNames": src["logGroupNames"],
            "filterConfig": {"timeRange": {"startTime": start, "endTime": end}},
        }},
        "description": f"Cross-run insights over last {hours}h",
    }

    try:
        ac = _agentcore()
        started = ac.start_batch_evaluation(**req)
    except Exception as e:  # noqa: BLE001
        print(f"[insights] StartBatchEvaluation failed: {type(e).__name__}: {e}")
        _store({"status": "FAILED", "error": f"{type(e).__name__}: {e}"})
        return {"status": "error", "error": f"StartBatchEvaluation failed: {type(e).__name__}: {e}"}

    batch_id = started.get("batchEvaluationId")
    batch_arn = started.get("batchEvaluationArn")
    _store({"status": "IN_PROGRESS", "batch_id": batch_id, "batch_arn": batch_arn,
            "started_by": user or "", "findings_json": ""})

    # Poll to completion (blocking; runs in a worker thread the runtime keeps alive).
    #
    # Every way out of this loop is now RECORDED. It used to `break` on a polling
    # exception and fall through to `status = resp.get("status") or "IN_PROGRESS"`,
    # which stored IN_PROGRESS with no error — so a run that had actually failed to poll
    # (or had timed out) sat in the panel as "in progress" forever, and there was
    # nothing anywhere saying why. The same happened on deadline expiry.
    deadline = time.time() + _POLL_TIMEOUT
    resp: dict = {}
    poll_error = ""
    timed_out = False
    while True:
        if time.time() >= deadline:
            timed_out = True
            break
        try:
            resp = ac.get_batch_evaluation(batchEvaluationId=batch_id)
        except Exception as e:  # noqa: BLE001
            poll_error = f"{type(e).__name__}: {e}"
            print(f"[insights] GetBatchEvaluation failed: {poll_error}")
            break
        if resp.get("status") in _TERMINAL:
            break
        time.sleep(_POLL_INTERVAL)

    status = resp.get("status") or "IN_PROGRESS"
    error = ""
    if poll_error:
        # The batch may well still be running AWS-side; what failed is our ability to
        # observe it. Say that, rather than implying the analysis itself failed.
        error = (f"the analysis was started but its progress could not be read: "
                 f"{poll_error}. It may still complete — reopen the panel to re-poll.")
    elif timed_out:
        error = (f"the analysis did not finish within {_POLL_TIMEOUT}s and is still "
                 f"running AWS-side. Reopen the panel to re-poll, or raise "
                 f"orchestrator.insights.pollTimeoutSeconds in workflow.json.")

    findings = _findings(resp) if status in ("COMPLETED", "COMPLETED_WITH_ERRORS") else {}
    store_error = _store({"status": status, "batch_id": batch_id, "batch_arn": batch_arn,
                          "started_by": user or "", "error": error,
                          "findings_json": json.dumps(findings)})
    result = {"status": status, "batch_id": batch_id, "findings": findings}
    if error:
        result["error"] = error
    if store_error:
        # Do not report success for findings the UI will never see: get_latest() reads
        # the table, so a failed write means the panel shows nothing.
        result["status"] = "error"
        result["error"] = (f"{error + ' ' if error else ''}the findings could not be "
                           f"persisted, so the panel will not show them: {store_error}")
    return result


def get_latest() -> dict:
    """The latest insights run for the UI. Re-polls once if still in progress so
    the panel reflects current state without starting another run."""
    item = _load()
    if not item:
        return {"status": "none"}
    status = item.get("status")
    if status not in _TERMINAL and item.get("batch_id"):
        try:
            resp = _agentcore().get_batch_evaluation(batchEvaluationId=item["batch_id"])
            status = resp.get("status") or status
            findings = _findings(resp) if status in ("COMPLETED", "COMPLETED_WITH_ERRORS") else {}
            _store({"status": status, "batch_id": item.get("batch_id"),
                    "batch_arn": item.get("batch_arn"), "started_by": item.get("started_by", ""),
                    "findings_json": json.dumps(findings) if findings else (item.get("findings_json") or "")})
            item = _load()
        except Exception as e:  # noqa: BLE001
            print(f"[insights] refresh failed: {type(e).__name__}: {e}")
    try:
        findings = json.loads(item.get("findings_json") or "{}")
    except Exception:  # noqa: BLE001
        findings = {}
    return {"status": item.get("status", "none"), "batch_id": item.get("batch_id", ""),
            "updated_at": item.get("updated_at", ""), "findings": findings}
