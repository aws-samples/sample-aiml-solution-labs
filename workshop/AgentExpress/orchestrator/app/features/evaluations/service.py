"""AgentCore Evaluations — score an agent's run with LLM-as-a-judge.

The built-in evaluators read the task input/output from the agent's
OpenTelemetry AGENT span. nodes.py stamps those as gen_ai.task.input /
gen_ai.task.output on a span emitted under the ADOT LangChain instrumentation
scope (amazon.opentelemetry.distro.instrumentation.langchain) with
aws.genai.span_kind=AGENT. That exact shape is what the Evaluate API scores.

Flow (per agent), primary path — PER PROMPT:
  1. Read this agent's persisted model-call rows for the current run version from
     the telemetry table (they carry system_prompt / user_input / output_text and
     the named `prompt` each call belongs to).
  2. For each named prompt, build a minimal AGENT span carrying that prompt's
     input/output and call bedrock-agentcore:Evaluate once per evaluator.
  3. Record each result as a kind="eval" telemetry row (value / label /
     explanation), tagged with the prompt + version so the UI can show eval per
     prompt per run version.

Fallback path (legacy runs with no per-prompt rows):
  Query the agent's AGENT span by EXACT name ("agent.<id>") scoped to the session
  from CloudWatch Logs, isolate its subtree within the trace, and score that.

Everything is best-effort: evaluation never fails a run.
"""

import contextlib
import json
import os
import time

from app.common import defaults
from app.common.config import AGENTS, REGION

# Where OTEL spans land. This deployment keeps spans in the agent runtime's own
# log group; X-Ray Transaction Search may ALSO deliver them to "aws/spans". We
# read both and combine so evaluation works regardless of delivery mode.
_SPANS_LOG_GROUP = "aws/spans"
# Prefix of the runtime log group(s) for THIS app, injected by Terraform
# (e.g. "/aws/bedrock-agentcore/runtimes/multiagent_orchestrator-"). The exact
# id-suffixed name is only known after the runtime exists, so we discover the
# matching groups at query time.
_RUNTIME_LOG_PREFIX = os.getenv("SPAN_LOG_GROUP_PREFIX", "")
#: From app/defaults.json (generated from app/keys.json), where the key is documented.
_DEFAULT_EVALUATORS = defaults.get("agentcore", "evaluations.evaluators")

# How long to keep retrying for the agent span to appear (spans reach CloudWatch
# a little after the agent finishes, and Logs Insights indexing is not instant).
_SPAN_RETRIES = 5
_SPAN_RETRY_SLEEP = 12


def _logs_client():
    import boto3
    return boto3.client("logs", region_name=REGION)


def _agentcore_client():
    import boto3
    return boto3.client("bedrock-agentcore", region_name=REGION)


# --- config helpers --------------------------------------------------------

def evaluators_for(agent_id: str) -> list[str]:
    """The judges for this agent: its `evaluators` (or the default), plus every custom
    evaluator it defines — a custom one is always run, listed or not."""
    cfg = ((AGENTS.get(agent_id) or {}).get("agentcore") or {}).get("evaluations") or {}
    chosen = list(cfg.get("evaluators") or _DEFAULT_EVALUATORS)
    for c in cfg.get("custom") or []:
        name = f"Custom.{c.get('name')}" if isinstance(c, dict) else ""
        if name and name not in chosen:
            chosen.append(name)
    return chosen


def evaluator_id(agent_id: str, evaluator: str) -> str:
    """What to pass the Evaluate API: a built-in's own id, or for Custom.<name> the id of
    the evaluator the IaC created for this agent (CUSTOM_EVALUATORS, {"<agent>.<name>":
    id}). A custom one the deploy did not create is refused, not sent as a name."""
    if not evaluator.startswith("Custom."):
        return evaluator
    try:
        ids = json.loads(os.getenv("CUSTOM_EVALUATORS") or "{}")
    except ValueError:
        ids = {}
    found = ids.get(f"{agent_id}.{evaluator[7:]}")
    if not found:
        raise ValueError(f"{evaluator} has no evaluator in this deployment; deploy again to create it")
    return str(found)


def is_enabled(agent_id: str) -> bool:
    cfg = ((AGENTS.get(agent_id) or {}).get("agentcore") or {}).get("evaluations") or {}
    return bool(cfg.get("enabled"))


def is_auto(agent_id: str) -> bool:
    cfg = ((AGENTS.get(agent_id) or {}).get("agentcore") or {}).get("evaluations") or {}
    return bool(cfg.get("enabled") and cfg.get("auto"))


def _agent_version(session_id: str, agent_id: str) -> int:
    """Current run version of an agent = number of times it has run this session.

    Read from the status table's per-agent history list. Used to tag eval rows
    with the version they scored so the UI can show eval per run version.
    Best-effort: returns 0 if unavailable (legacy/unscoped behavior).
    """
    table = os.getenv("STATUS_TABLE", "")
    if not table:
        return 0
    try:
        import boto3
        item = (
            boto3.resource("dynamodb", region_name=REGION)
            .Table(table)
            .get_item(Key={"session_id": session_id})
            .get("Item")
            or {}
        )
        history = (item.get("history") or {}).get(agent_id) or []
        return len(history)
    except Exception as exc:  # noqa: BLE001
        print(f"[evaluations] version lookup failed for {agent_id}: {exc}")
        return 0


# --- CloudWatch span sources ----------------------------------------------

def _span_log_groups(client) -> list[str]:
    """Log groups that may hold this app's spans: the agent runtime group(s)
    (this deployment) plus aws/spans (Transaction Search)."""
    groups: list[str] = []
    if _RUNTIME_LOG_PREFIX:
        try:
            resp = client.describe_log_groups(logGroupNamePrefix=_RUNTIME_LOG_PREFIX)
            groups += [g["logGroupName"] for g in resp.get("logGroups", [])]
        except Exception as e:  # noqa: BLE001
            print(f"[evaluations] describe_log_groups failed: {type(e).__name__}: {e}")
    groups.append(_SPANS_LOG_GROUP)
    return groups


def _service_name_for(log_group_name: str) -> str:
    """Derive the OTEL service.name an AgentCore runtime emits from its log group.

    Log group:  /aws/bedrock-agentcore/runtimes/<runtimeId>-DEFAULT
    runtimeId:  <agentRuntimeName>-<10charSuffix>   (e.g. multiagent_orchestrator-jspBxy2Bxn)
    service:    <agentRuntimeName>.DEFAULT          (e.g. multiagent_orchestrator.DEFAULT)
    """
    name = log_group_name.rsplit("/", 1)[-1]           # <runtimeId>-DEFAULT
    name = name.removesuffix("-DEFAULT")                # <runtimeId>
    runtime_name = name.rsplit("-", 1)[0] if "-" in name else name  # drop random suffix
    return f"{runtime_name}.DEFAULT"


# The optimization APIs require agentTraces to target EXACTLY ONE service
# (serviceNames length <= 1) with up to 5 log groups (logGroupArns/Names <= 5).
_TRACE_LOGGROUP_MAX = 5


def runtime_trace_sources(agent_id: str | None = None) -> dict:
    """The app's AgentCore runtime CloudWatch source for the optimization APIs,
    scoped to ONE runtime service (required — the APIs cap serviceNames at 1).

    `agent_id` targets that agent's OWN runtime when it is `dedicated`; omitted (the
    only way insights calls it today) targets the MAIN orchestrator runtime, where all
    `main` agents run. Returns {logGroupArns, logGroupNames, serviceNames:[one]} with
    up to 5 (newest) log groups for that service — a re-created runtime leaves stale
    groups behind. Best-effort."""
    if not _RUNTIME_LOG_PREFIX:
        return {"logGroupArns": [], "logGroupNames": [], "serviceNames": []}
    # service -> list of {name, arn, ct}
    by_service: dict[str, list] = {}
    try:
        client = _logs_client()
        token = None
        while True:
            kw = {"logGroupNamePrefix": _RUNTIME_LOG_PREFIX}
            if token:
                kw["nextToken"] = token
            resp = client.describe_log_groups(**kw)
            for g in resp.get("logGroups", []):
                lg = g["logGroupName"]
                svc = _service_name_for(lg)
                by_service.setdefault(svc, []).append({
                    "name": lg, "ct": int(g.get("creationTime", 0) or 0),
                    "arn": str(g.get("arn", "")).rstrip(":*") or g.get("arn", "")})
            token = resp.get("nextToken")
            if not token:
                break
    except Exception as e:  # noqa: BLE001
        print(f"[optimization] runtime_trace_sources failed: {type(e).__name__}: {e}")
    if not by_service:
        return {"logGroupArns": [], "logGroupNames": [], "serviceNames": []}

    # The main orchestrator service is the shortest name (base runtime); the
    # dedicated ones are "<base>_<module>.DEFAULT".
    main_service = min(by_service.keys(), key=lambda s: (len(s), s))
    target = main_service
    if agent_id:
        acfg = AGENTS.get(agent_id) or {}
        if acfg.get("runtime") == "dedicated":
            # The agent id is the module name (see registry.build_agent_module),
            # and the IaC names a dedicated runtime "<base>_<agent id>".
            match = [s for s in by_service if s.endswith(f"_{agent_id}.DEFAULT")]
            if match:
                target = match[0]

    groups = sorted(by_service.get(target, []), key=lambda x: x["ct"], reverse=True)[:_TRACE_LOGGROUP_MAX]
    return {
        "logGroupArns": [g["arn"] for g in groups if g.get("arn")],
        "logGroupNames": [g["name"] for g in groups],
        "serviceNames": [target],
    }


def _run_query(client, log_group: str, query: str, now: int, lookback_hours: int) -> list[dict]:
    """Run one Logs Insights query and return the parsed span JSON documents."""
    try:
        qid = client.start_query(logGroupName=log_group,
                                 startTime=now - lookback_hours * 3600, endTime=now,
                                 queryString=query)["queryId"]
    except Exception as e:  # noqa: BLE001 - group may not exist
        print(f"[evaluations] start_query {log_group} failed: {type(e).__name__}: {e}")
        return []
    deadline = time.time() + 30
    result = {"status": "Running"}
    while time.time() < deadline:
        result = client.get_query_results(queryId=qid)
        if result["status"] in ("Complete", "Failed", "Cancelled"):
            break
        time.sleep(1)
    spans = []
    for row in result.get("results", []):
        for field in row:
            if field["field"] == "@message" and field["value"].strip().startswith("{"):
                with contextlib.suppress(Exception):
                    spans.append(json.loads(field["value"]))
    return spans


def _query_all_groups(query: str, lookback_hours: int = 6) -> list[dict]:
    """Run a query across every candidate log group, combine and dedup by spanId."""
    client = _logs_client()
    now = int(time.time())
    seen: set = set()
    spans: list[dict] = []
    for lg in _span_log_groups(client):
        for s in _run_query(client, lg, query, now, lookback_hours):
            sid = s.get("spanId") or s.get("attributes", {}).get("otelSpanID") or id(s)
            if sid in seen:
                continue
            seen.add(sid)
            spans.append(s)
    return spans


def _fetch_agent_span(session_id: str, agent_id: str) -> dict | None:
    """Fetch the target agent's AGENT span by EXACT name, scoped to the session.

    Only reached by the fallback path — which also fires when TELEMETRY_TABLE is unset
    or the telemetry read failed, not just for a run that predates per-prompt tagging.

    Exact `name = "agent.<id>"` filtering is deterministic (unlike hunting for
    one span in a broad session download), so we reliably score the RIGHT agent.
    Retries because spans land a little after the agent completes.
    """
    query = (f'fields @message | filter attributes.session.id like "{session_id}" '
             f'and name = "agent.{agent_id}" | sort @timestamp desc | limit 3')
    for attempt in range(_SPAN_RETRIES):
        spans = _query_all_groups(query)
        if spans:
            return spans[0]
        if attempt < _SPAN_RETRIES - 1:
            time.sleep(_SPAN_RETRY_SLEEP)
    return None


def _fetch_trace_spans(session_id: str, trace_id: str) -> list[dict]:
    """All spans of one trace for this session (the agent + its LLM/tool calls)."""
    if not trace_id:
        return []
    query = (f'fields @message | filter attributes.session.id like "{session_id}" '
             f'and traceId = "{trace_id}" | sort @timestamp asc | limit 1000')
    return _query_all_groups(query)


def _agent_subtree(agent_span: dict, trace_spans: list[dict]) -> list[dict]:
    """The agent span plus all its descendant spans within the trace.

    Isolates just this agent (its input/output + the tool/LLM calls under it),
    even when the whole run shares one trace with other agents. Falls back to the
    agent span alone if descendants can't be walked — a single AGENT span with
    gen_ai.task.input/output is sufficient for the built-in evaluators.
    """
    root_id = agent_span.get("spanId")
    if not root_id or not trace_spans:
        return [agent_span]

    children_by_parent: dict = {}
    for s in trace_spans:
        children_by_parent.setdefault(s.get("parentSpanId"), []).append(s)

    subtree, stack, seen = [], [agent_span], set()
    while stack:
        span = stack.pop()
        sid = span.get("spanId")
        if sid in seen:
            continue
        seen.add(sid)
        subtree.append(span)
        stack.extend(children_by_parent.get(sid, []))
    return subtree


# --- persisted-prompt path (primary) --------------------------------------

def _telemetry_rows(session_id: str) -> list[dict]:
    """All telemetry rows for a session (best-effort, [] on failure)."""
    table = os.getenv("TELEMETRY_TABLE", "")
    if not table:
        return []
    try:
        import boto3
        from boto3.dynamodb.conditions import Key
        tbl = boto3.resource("dynamodb", region_name=REGION).Table(table)
        # PAGINATE. A query returns at most 1 MB per page, and these rows carry the
        # captured prompt/response text, so a long run exceeds one page easily. A
        # single unpaginated query dropped the newest rows — which are precisely the
        # ones evaluation needs, because _prompt_io takes the LAST match. The symptom
        # was a stale or missing score with no error anywhere.
        out, kw = [], {"KeyConditionExpression": Key("session_id").eq(session_id)}
        while True:
            page = tbl.query(**kw)
            out.extend(page.get("Items", []))
            lek = page.get("LastEvaluatedKey")
            if not lek:
                return out
            kw["ExclusiveStartKey"] = lek
    except Exception as e:  # noqa: BLE001
        print(f"[evaluations] telemetry read failed: {type(e).__name__}: {e}")
        return []


def _llm_rows(rows: list[dict], agent_id: str, version: int) -> list[dict]:
    """This agent's model-call rows for the given run version, in order."""
    out = [r for r in rows if r.get("kind") == "llm" and r.get("agent_id") == agent_id
           and int(r.get("version") or 0) == int(version or 0)]
    return sorted(out, key=lambda x: str(x.get("sk", "")))


def _list_prompts(rows: list[dict], agent_id: str, version: int) -> list[str]:
    """Distinct named prompts this agent issued in the given version, in first-seen
    order. Rows without a prompt name (legacy) are ignored."""
    seen: list[str] = []
    for r in _llm_rows(rows, agent_id, version):
        p = (r.get("prompt") or "").strip()
        if p and p not in seen:
            seen.append(p)
    return seen


def _prompt_io(rows: list[dict], agent_id: str, prompt: str, version: int) -> tuple[str, str] | None:
    """The latest (task_input, task_output) for one named prompt of this version.

    task_input pairs the system prompt with the real user input (the same shape
    nodes.py stamps on the agent span) so the evaluators judge substance."""
    matches = [r for r in _llm_rows(rows, agent_id, version)
               if (r.get("prompt") or "").strip() == prompt]
    if not matches:
        return None
    r = matches[-1]  # latest by sk
    system = str(r.get("system_prompt") or "")
    user = str(r.get("user_input") or "")
    output = str(r.get("output_text") or "")
    task_input = f"{system}\n\n=== INPUT ===\n{user}" if user else system
    return task_input, output


def _synth_span(session_id: str, agent_id: str, task_input: str, task_output: str) -> dict:
    """Build an AGENT span carrying one prompt's input/output. The built-in
    evaluators read gen_ai.task.input/output from this shape, so we can score any
    prompt's persisted I/O directly — no dependency on per-prompt spans reaching
    CloudWatch."""
    import uuid

    from app.features.observability import otel
    now_ns = int(time.time() * 1e9)  # integer nanoseconds (the API parses this)
    return {
        "name": f"agent.{agent_id}",
        "spanId": uuid.uuid4().hex[:16],
        "traceId": uuid.uuid4().hex,
        "parentSpanId": "",
        "kind": "SPAN_KIND_INTERNAL",
        "startTimeUnixNano": now_ns,
        "endTimeUnixNano": now_ns + 1_000_000,
        "status": {},
        "scope": {"name": "amazon.opentelemetry.distro.instrumentation.langchain", "version": ""},
        "attributes": {
            "session.id": otel.normalize_session_id(session_id),
            "gen_ai.agent.id": agent_id,
            "gen_ai.operation.name": "invoke_agent",
            "aws.genai.span_kind": "AGENT",
            "gen_ai.task.input": str(task_input)[:40000],
            "gen_ai.task.output": str(task_output)[:12000],
        },
    }


def _score_spans(ac, evaluators: list[str], spans: list[dict], agent_id: str,
                 prompt: str) -> list[dict]:
    """Run each evaluator over `spans` and record a kind="eval" row per result,
    tagged with `prompt`. Returns per-evaluator summaries."""
    from app.features.observability import meter
    summaries: list[dict] = []
    for evaluator in evaluators:
        t0 = time.perf_counter()
        try:
            resp = ac.evaluate(evaluatorId=evaluator_id(agent_id, evaluator),
                               evaluationInput={"sessionSpans": spans})
            latency_ms = int((time.perf_counter() - t0) * 1000)
            results = resp.get("evaluationResults") or []
            scored = next((r for r in results if r.get("value") is not None), None)
            r = scored or (results[0] if results else {})
            value = r.get("value")
            usage = r.get("tokenUsage") or {}
            if value is not None:
                meter.record_eval(evaluator=evaluator, value=value,
                                  label=r.get("label") or "", explanation=r.get("explanation") or "",
                                  agent_id=agent_id, latency_ms=latency_ms, status="ok",
                                  input_tokens=usage.get("inputTokens", 0),
                                  output_tokens=usage.get("outputTokens", 0), prompt=prompt)
                summaries.append({"evaluator": evaluator, "value": value, "prompt": prompt,
                                  "label": r.get("label"), "status": "ok"})
            else:
                msg = r.get("errorMessage") or r.get("error") or "No score returned."
                meter.record_eval(evaluator=evaluator, value=None, label="error",
                                  explanation=msg, agent_id=agent_id,
                                  latency_ms=latency_ms, status="error", prompt=prompt)
                summaries.append({"evaluator": evaluator, "status": "error", "prompt": prompt,
                                  "error": msg})
        except Exception as e:  # noqa: BLE001
            print(f"[evaluations] {evaluator} failed for {agent_id}/{prompt}: {type(e).__name__}: {e}")
            summaries.append({"evaluator": evaluator, "status": "error", "prompt": prompt,
                              "error": str(e)})
    return summaries


def evaluate_agent(session_id: str, agent_id: str, user: str = "",
                   evaluators: list[str] | None = None, prompt: str | None = None) -> list[dict]:
    """Evaluate an agent's run PER PROMPT and record kind="eval" rows.

    An agent can issue several named prompts (e.g. "analysis" and
    "analysis-scoring"); each is scored on its own persisted input/output for the
    current run version, so the UI can show eval per prompt per version.

    - prompt given  -> evaluate just that named prompt.
    - prompt None   -> evaluate every prompt the agent issued this version.

    Returns per-(prompt, evaluator) summaries. Each evaluator's failure is caught and
    logged individually (see _score_spans), but this is not a blanket guarantee —
    both callers still guard, because client construction and span synthesis can raise.
    """
    from app.features.observability.scope import set_scope

    # THE GATE, and it belongs here because this is the only chokepoint every caller
    # passes through: the UI's Evaluate button and the REST route (runtime._run_eval),
    # the in-app assistant's run_evaluation tool, and auto_evaluate_session.
    #
    # It was missing entirely. `is_enabled()` existed and had ZERO callers, so
    # `"evaluations": {"enabled": false}` only hid the button in web/observability.js
    # — a client-side gate, which is not enforcement. Anyone calling the route or
    # asking the assistant still had the agent scored and BILLED, with a kind="eval"
    # row written, under `_DEFAULT_EVALUATORS` it never declared. That is the
    # "config key that reads like a switch and controls nothing" failure this repo
    # polices everywhere else (see tests/test_config_keys.py).
    #
    # Returned rather than raised, so the caller can tell "you have not enabled this"
    # apart from "enabled, but there was nothing scorable" — two very different
    # answers that both used to arrive as an empty list.
    if not is_enabled(agent_id):
        return [{"agent_id": agent_id, "status": "disabled", "evaluator": None,
                 "reason": f"evaluations are not enabled for {agent_id!r}; set "
                           f"agentcore.evaluations.enabled on that agent in "
                           f"workflow.json"}]

    evaluators = evaluators or evaluators_for(agent_id)
    version = _agent_version(session_id, agent_id)
    # tag recorded rows with the agent + the version this eval scored
    set_scope(session_id, agent_id, user, version)
    ac = _agentcore_client()

    rows = _telemetry_rows(session_id)
    prompts = [prompt] if prompt else _list_prompts(rows, agent_id, version)

    # Fallback for runs whose model-call rows predate per-prompt tagging: score the
    # agent's AGENT span as a whole (prompt unscoped).
    if not prompts:
        agent_span = _fetch_agent_span(session_id, agent_id)
        if not agent_span:
            print(f"[evaluations] no prompts and no AGENT span for {agent_id} in {session_id}; skipping")
            return []
        trace_spans = _fetch_trace_spans(session_id, agent_span.get("traceId", ""))
        spans = _agent_subtree(agent_span, trace_spans)
        return _score_spans(ac, evaluators, spans, agent_id, prompt="")

    summaries: list[dict] = []
    for pname in prompts:
        io = _prompt_io(rows, agent_id, pname, version)
        if not io:
            print(f"[evaluations] no I/O for {agent_id}/{pname} v{version}; skipping")
            continue
        span = _synth_span(session_id, agent_id, io[0], io[1])
        summaries += _score_spans(ac, evaluators, [span], agent_id, pname)
    return summaries


def outcome_log(agent_id: str, summaries: list[dict] | None) -> str | None:
    """The timeline line owed to a reader when an evaluation produced no scores.

    Returns None when there ARE scores — the caller reports those individually.

    Lives here, not at the call site, because the three outcomes are an evaluations
    concern and telling two of them apart is the whole point: "you have not enabled
    this" and "enabled, but nothing scorable was found" used to arrive identically as
    an empty list, so a config mistake was reported as missing telemetry and sent the
    reader looking in the wrong place.
    """
    refused = next((s for s in (summaries or []) if s.get("status") == "disabled"), None)
    if refused:
        return f"Evaluation skipped: {refused['reason']}"
    if not summaries:
        return f"Evaluation: nothing scorable found for {agent_id}"
    return None


def auto_evaluate_session(session_id: str, user: str = "") -> None:
    """Evaluate every agent that has evaluations.enabled + auto (called at session
    completion, when all agents' data is present)."""
    auto_agents = [aid for aid in AGENTS if is_auto(aid)]
    for aid in auto_agents:
        try:
            evaluate_agent(session_id, aid, user=user)
        except Exception as e:  # noqa: BLE001
            print(f"[evaluations] auto-eval {aid} skipped: {type(e).__name__}: {e}")
