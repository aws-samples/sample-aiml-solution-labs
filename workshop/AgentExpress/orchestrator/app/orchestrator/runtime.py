"""AgentCore Runtime entrypoint.

Long-running / async model: each invocation registers an async task
(app.add_async_task) and runs the workflow in a background task, returning to
the caller immediately. While a task is registered, /ping reports HealthyBusy,
keeping the session alive up to the 8-hour max lifetime. Progress is written to
DynamoDB (sink) for the UI; HITL pauses persist via AgentCore Memory and resume
on a later invocation.

Invoke contract:
  {"action": "start",       "topic": "...", "session_id": "..."(optional),
                            "subject_id": "..."(optional, scopes long-term memory)}
  {"action": "resume",      "session_id": "...", "decision": "approve"|"deny"|"revise",
                            "comment": "..."(feedback, used on revise),
                            "decisions": {...}(per-agent map, parallel-group gates)}
  {"action": "rerun_from",  "session_id": "...", "agent_id": "..."|"agents": [...],
                            "comment": "..."}
  {"action": "evaluate",    "session_id": "...", "agent_id": "...", "prompt": "..."(optional)}
  {"action": "run_insights","lookback_hours": 168}
  {"action": "get_insights"}   -> synchronous read of the latest findings
All except get_insights return immediately; clients poll the status store.
"""

import asyncio
import contextlib
import uuid

from bedrock_agentcore.runtime import BedrockAgentCoreApp
from langgraph.types import Command

from app.common.config import AGENT_ORDER, DEFAULT_TOPIC, LAST_AGENT_ID, MEMORY_ID, REGION
from app.common.sink import WorkflowCancelled, emit, ensure_session, is_cancelled
from app.orchestrator.graph_builder import build_graph, group_rerun_plan, rerun_plan

if not MEMORY_ID:
    raise RuntimeError("MEMORY_ID environment variable is required")

from langgraph_checkpoint_aws import AgentCoreMemorySaver

graph = build_graph(AgentCoreMemorySaver(MEMORY_ID, region_name=REGION))
app = BedrockAgentCoreApp()

ACTOR_ID = "orchestrator"
_bg_tasks: set = set()
# 0 = "not given": insights.run_batch then uses `orchestrator.insights.lookbackHours`
# from workflow.json. This used to be a literal 168 passed explicitly on every path,
# so the configured window never took effect.
_DEFAULT_INSIGHTS_LOOKBACK = 0


def _config(session_id: str) -> dict:
    return {"configurable": {"thread_id": session_id, "actor_id": ACTOR_ID}}


def _spawn(coro, session_id: str = "") -> None:
    """Run `coro` as a background task, carrying the current trace context and
    grouping its spans under `session_id` (so a run is one CloudWatch session)."""
    from app.features.observability import otel
    _carrier = otel.carrier()

    async def _traced():
        _t = otel.attach_carrier(_carrier)
        _s = otel.set_session(session_id) if session_id else None
        try:
            await coro
        finally:
            if _s is not None:
                otel.detach(_s)
            otel.detach(_t)

    task = asyncio.create_task(_traced())
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


def _result(outputs: dict) -> str:
    """The run's deliverable: the configured last agent's output, or — when a branch
    ended the run before reaching it — the last agent that actually produced one.

    Without the fallback a branching workflow finishes with an empty result box and
    no hint that the deliverable is sitting on the agent one step back."""
    return outputs.get(LAST_AGENT_ID) or next(
        (outputs[a] for a in reversed(AGENT_ORDER) if outputs.get(a)), "")


async def _finalize(session_id: str) -> None:
    state = await graph.aget_state(_config(session_id))
    if state.next:
        return  # paused at a HITL gate; resume will continue
    values = state.values or {}
    decisions = values.get("decisions") or {}
    if "deny" in decisions.values():
        overall, result = "denied", "Denied by human. Workflow halted."
    else:
        overall, result = "done", _result(values.get("outputs") or {})
    await emit(session_id, {"type": "session_status", "status": overall,
                            "result": result, "log": f"Workflow finished ({overall})"})
    if overall == "done":
        await _reconcile_skipped(session_id)

    # Auto-evaluation (AgentCore Evaluations): when the run completed, score every
    # agent with evaluations.auto=true in workflow.json. Runs now because all
    # agents' data is present by session end. Blocking (LLM judge + CloudWatch
    # reads), so it runs in a worker thread; best-effort, never fails the run.
    # On-demand evaluation (the UI button) covers auto=false agents.
    if overall == "done":
        try:
            from app.features.evaluations import service as eval_service
            await asyncio.to_thread(eval_service.auto_evaluate_session,
                                    session_id, values.get("user", ""))
        except Exception as e:  # noqa: BLE001
            print(f"[evaluations] auto-eval skipped: {type(e).__name__}: {e}")


async def _rewind(session_id: str, rerun: dict) -> None:
    """Rewind the graph so a chosen agent (or a subset of a parallel stage)
    re-runs and cascades downstream.

    Single-agent (rerun={"agent_id","comment"}): apply rerun_plan via
    aupdate_state (seed the predecessor gate's decision to "approve" so routing
    forwards to the target, inject the reviewer feedback into the target agent,
    clear stale downstream feedback).

    Multi-agent subset (rerun={"agents":[{"agent_id","comment"}, ...]} within one
    gated parallel stage): apply group_rerun_plan — seed the stage's OWN gate to
    "revise" with group_rerun=<subset> and per-agent feedback, so ONLY the picked
    agents re-run in parallel; the gate then re-pauses for review before the
    workflow cascades downstream.

    Either way, resets the UI (re-run + downstream nodes -> pending, clear the
    prior final result). The caller invokes the graph with None afterwards.
    """
    agents = rerun.get("agents")
    if agents:
        # --- multi-agent subset of a gated parallel stage ---
        fb_in = {a["agent_id"]: (a.get("comment", "") or "") for a in agents if a.get("agent_id")}
        plan = group_rerun_plan(list(fb_in.keys()))
        downstream = plan["downstream_agents"]
        gid, subset = plan["group_id"], plan["subset"]

        # Reset feedback across the downstream slice, then set each picked agent's.
        fb = {a: "" for a in downstream}
        fb.update(fb_in)
        values = {
            "decisions": {gid: "revise"},          # group router -> loop back
            "group_rerun": {gid: subset},          # ... to exactly this subset
            "feedback": fb,
        }
        await graph.aupdate_state(_config(session_id), values, as_node=plan["as_node"])
        await emit(session_id, {
            "type": "session_status", "status": "running", "result": "",
            "log": f"Re-running {', '.join(subset)} in {gid}; "
                   f"{len(downstream) - len(subset)} downstream agent(s) will regenerate"})
        for a in downstream:
            await emit(session_id, {"type": "node_status", "node": a, "status": "pending"})
        return

    # --- single-agent rewind ---
    plan = rerun_plan(rerun["agent_id"])
    comment = rerun.get("comment", "") or ""
    downstream = plan["downstream_agents"]

    values: dict = {}
    if plan["decision_key"]:
        # Force the previous gate's router to forward to this step (not loop/halt).
        values["decisions"] = {plan["decision_key"]: "approve"}
    if plan["branch_seed"]:
        # The previous step BRANCHES, so its router decides where flow goes. Point
        # that recorded decision at this step: the reviewer asked for this agent, and
        # on a branching workflow that genuinely changes the run's path.
        values["branch"] = plan["branch_seed"]
    # Reset feedback for the whole downstream slice so a stale note from an
    # earlier revise cannot silently re-apply, then set the target's feedback.
    fb = {a: "" for a in downstream}
    fb[rerun["agent_id"]] = comment
    values["feedback"] = fb

    await graph.aupdate_state(_config(session_id), values, as_node=plan["as_node"])

    # Reset the UI: the prior final result is stale; downstream nodes are about to
    # re-run. The graph re-emits "running"/gate events as it goes.
    await emit(session_id, {
        "type": "session_status", "status": "running", "result": "",
        "log": f"Rerun from {rerun['agent_id']} (step {plan['step_index']}); "
               f"re-running {len(downstream)} agent(s) downstream"})
    for a in downstream:
        await emit(session_id, {"type": "node_status", "node": a, "status": "pending"})


async def _reconcile_incomplete(session_id: str, status: str) -> None:
    """On a failed/cancelled run, flip every node still 'running' to `status`, so
    the UI never shows agents spinning under a settled run. Best-effort — a
    reconciliation error must not mask the original failure."""
    try:
        from app.common.sink import nodes_in_status
        for nid in nodes_in_status(session_id, "running"):
            await emit(session_id, {"type": "node_status", "node": nid, "status": status,
                                    "log": f"{nid}: {status} (run {status} before this agent finished)"})
    except Exception as e:  # noqa: BLE001
        print(f"[reconcile] {type(e).__name__}: {e}")


async def _reconcile_skipped(session_id: str) -> None:
    """On a COMPLETED run, flip every node still 'pending' to 'skipped'.

    A branch node marks what it bypasses as it goes, which is what the reviewer sees
    live. This is the backstop for the paths it cannot know about — chiefly a rewind,
    where the branch node does not re-run and so re-emits nothing. A finished run
    must not leave an agent looking like it is still queued. Best-effort."""
    try:
        from app.common.sink import nodes_in_status
        for nid in nodes_in_status(session_id, "pending"):
            await emit(session_id, {"type": "node_status", "node": nid, "status": "skipped",
                                    "log": f"{nid}: skipped (the run did not reach it)"})
    except Exception as e:  # noqa: BLE001
        print(f"[reconcile] {type(e).__name__}: {e}")


async def _run(session_id: str, initial=None, resume=None, rerun=None, user: str = "") -> None:
    task_id = app.add_async_task("workflow")
    import time as _t

    from app.features.observability import otel
    _burst_start = _t.perf_counter()
    # Group every span from this run under the session id so CloudWatch GenAI
    # Observability shows one workflow run as one session.
    _sess_token = otel.set_session(session_id)
    try:
        if rerun is not None:
            # Rewind first, then continue with no new input (runs the pending
            # tasks the rewind created and cascades forward).
            await _rewind(session_id, rerun)
            payload = None
        else:
            payload = Command(resume=resume) if resume is not None else initial
        await graph.ainvoke(payload, _config(session_id))
        await _finalize(session_id)
    except WorkflowCancelled:
        await emit(session_id, {"type": "session_status", "status": "cancelled",
                                "result": "Stopped by user.", "log": "Workflow cancelled by user"})
        await _reconcile_incomplete(session_id, "cancelled")
    except Exception as e:  # noqa: BLE001
        # A node may raise WorkflowCancelled wrapped by the graph runtime; if the
        # user asked to stop, report it as cancelled rather than a failure.
        if is_cancelled(session_id):
            await emit(session_id, {"type": "session_status", "status": "cancelled",
                                    "result": "Stopped by user.",
                                    "log": "Workflow cancelled by user"})
            await _reconcile_incomplete(session_id, "cancelled")
        else:
            await emit(session_id, {"type": "session_status", "status": "failed",
                                    "log": f"Workflow error: {type(e).__name__}: {e}"})
            # Flip any agent still "running" to "failed" so a settled (failed) run
            # never shows in-flight nodes. The specific agent that raised already
            # emits its own "failed" via make_agent_node; this catches its
            # siblings in the same parallel superstep.
            await _reconcile_incomplete(session_id, "failed")
    finally:
        # Record the AgentCore Runtime compute consumed by this active burst
        # (best-effort; isolated in app/features/observability).
        with contextlib.suppress(Exception):
            from app.features.observability import meter
            meter.record_session_compute(
                session_id=session_id, user=user,
                active_seconds=_t.perf_counter() - _burst_start,
            )
        otel.detach(_sess_token)
        # Push buffered spans to the exporter BEFORE marking the task complete —
        # once complete, AgentCore may freeze/reclaim the idle container and the
        # BatchSpanProcessor's buffered spans would never be exported.
        otel.force_flush()
        app.complete_async_task(task_id)


async def _run_eval(session_id: str, agent_id: str, user: str = "", prompt: str = "") -> None:
    """On-demand AgentCore Evaluation for one agent (UI "Evaluate" button).

    Scores each named prompt the agent used (or one `prompt` if given) on its own
    persisted input/output. Registers an async task so the container stays alive
    through the (blocking) scoring, which runs in a worker thread. Results land as
    kind="eval" telemetry rows (tagged with prompt + version) the UI reads."""
    task_id = app.add_async_task("evaluate")
    try:
        from app.features.evaluations import service as eval_service
        await emit(session_id, {
            "type": "log",
            "log": f"Evaluating {agent_id}{(' · ' + prompt) if prompt else ''} (AgentCore Evaluations)"})
        summaries = await asyncio.to_thread(eval_service.evaluate_agent, session_id, agent_id,
                                           user, None, prompt or None)
        # One line when nothing was scored, and it must say WHICH kind of nothing —
        # "you have not enabled this" is not "enabled, but nothing scorable was found".
        no_scores = eval_service.outcome_log(agent_id, summaries)
        if no_scores:
            await emit(session_id, {"type": "log", "log": no_scores})
        for s in (summaries or []):
            if s.get("status") == "ok":
                await emit(session_id, {
                    "type": "log",
                    "log": f"Eval {agent_id} {s['evaluator']}: {s.get('label')} ({s.get('value')})"})
            else:
                await emit(session_id, {
                    "type": "log",
                    "log": f"Eval {agent_id} {s['evaluator']}: ERROR {str(s.get('error'))[:180]}"})
        await emit(session_id, {"type": "log", "log": f"Evaluation complete for {agent_id}"})
    except Exception as e:  # noqa: BLE001
        await emit(session_id, {"type": "log",
                                "log": f"Evaluation failed for {agent_id}: {type(e).__name__}: {e}"})
    finally:
        app.complete_async_task(task_id)


async def _run_insights(lookback_hours: int = _DEFAULT_INSIGHTS_LOOKBACK, user: str = "") -> None:
    """Cross-run AgentCore Insights (batch evaluation). Runs the
    failure/intent/summary analyzers over recent traces and stores the findings
    (best-effort; blocking scoring in a worker thread with an async task alive)."""
    task_id = app.add_async_task("insights")
    try:
        from app.features.optimization import insights
        await asyncio.to_thread(insights.run_batch, lookback_hours, user)
    except Exception as e:  # noqa: BLE001
        print(f"[insights] run failed: {type(e).__name__}: {e}")
    finally:
        app.complete_async_task(task_id)


@app.entrypoint
async def invoke(payload, context=None):
    action = payload.get("action", "start")

    # Insights: read the latest cross-run findings (synchronous).
    if action == "get_insights":
        try:
            from app.features.optimization import insights
            return insights.get_latest()
        except Exception as e:  # noqa: BLE001
            return {"error": f"{type(e).__name__}: {e}"}

    # Insights: start a cross-run batch analysis in the background.
    if action == "run_insights":
        try:
            lookback = max(0, int(payload.get("lookback_hours") or _DEFAULT_INSIGHTS_LOOKBACK))
        except (TypeError, ValueError):
            lookback = _DEFAULT_INSIGHTS_LOOKBACK
        _spawn(_run_insights(lookback, payload.get("user", "")), session_id="insights")
        return {"status": "insights_started"}

    if action == "start":
        session_id = payload.get("session_id") or uuid.uuid4().hex[:12]
        # Default from workflow.json (ui.defaultTopic), never a literal.
        topic = payload.get("topic") or DEFAULT_TOPIC
        user = payload.get("user", "")
        await ensure_session(session_id, topic)
        initial = {"topic": topic, "user": user,
                   "subject_id": payload.get("subject_id", ""),
                   "status": {}, "outputs": {}, "decisions": {}}
        _spawn(_run(session_id, initial=initial, user=user), session_id=session_id)
        return {"session_id": session_id, "status": "started"}

    if action == "resume":
        session_id = payload.get("session_id")
        if not session_id:
            return {"error": "resume requires 'session_id'"}
        decision = payload.get("decision", "approve")
        # `decisions` (a per-agent map) is used by parallel-group gates; when it
        # is present the single `decision` is not required to be one of the three.
        decisions = payload.get("decisions")
        if not decisions and decision not in ("approve", "deny", "revise"):
            return {"error": "decision must be 'approve', 'deny' or 'revise'"}
        resume = {"decision": decision, "comment": payload.get("comment", ""),
                  "decisions": decisions}
        _spawn(_run(session_id, resume=resume, user=payload.get("user", "")),
               session_id=session_id)
        return {"session_id": session_id, "status": "resuming"}

    # Rewind to a chosen agent and re-run it + everything downstream, with the
    # reviewer's comment injected as feedback for that agent. Downstream HITL
    # gates re-pause for review. Use when an earlier decision was a mistake or the
    # final deliverable needs a change that must flow through the whole pipeline.
    if action == "rerun_from":
        session_id = payload.get("session_id")
        if not session_id:
            return {"error": "rerun_from requires 'session_id'"}
        # Multi-agent subset of a parallel stage: agents=[{agent_id, comment}].
        agents = payload.get("agents")
        if agents:
            agents = [{"agent_id": a.get("agent_id") or a.get("agentId"),
                       "comment": a.get("comment", "")}
                      for a in agents if (a.get("agent_id") or a.get("agentId"))]
            try:
                group_rerun_plan([a["agent_id"] for a in agents])  # validate before spawning
            except ValueError as e:
                return {"error": str(e)}
            _spawn(_run(session_id, rerun={"agents": agents}, user=payload.get("user", "")),
                   session_id=session_id)
            return {"session_id": session_id, "status": "rerunning"}
        # Single-agent rewind.
        agent_id = payload.get("agent_id") or payload.get("agentId")
        if not agent_id:
            return {"error": "rerun_from requires 'agent_id' or 'agents'"}
        try:
            rerun_plan(agent_id)  # validate the target before spawning
        except ValueError as e:
            return {"error": str(e)}
        rerun = {"agent_id": agent_id, "comment": payload.get("comment", "")}
        _spawn(_run(session_id, rerun=rerun, user=payload.get("user", "")),
               session_id=session_id)
        return {"session_id": session_id, "status": "rerunning"}

    # On-demand AgentCore Evaluation: score one agent's run now (UI button).
    if action == "evaluate":
        session_id = payload.get("session_id")
        agent_id = payload.get("agent_id") or payload.get("agentId")
        if not session_id or not agent_id:
            return {"error": "evaluate requires 'session_id' and 'agent_id'"}
        _spawn(_run_eval(session_id, agent_id, payload.get("user", ""), payload.get("prompt", "")),
               session_id=session_id)
        return {"session_id": session_id, "status": "evaluating"}

    return {"error": f"unknown action '{action}'; use 'start', 'resume', 'rerun_from', "
                     f"'evaluate', 'run_insights' or 'get_insights'"}


if __name__ == "__main__":
    app.run()
