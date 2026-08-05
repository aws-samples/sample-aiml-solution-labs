"""Strands adapter — the ONLY framework-aware part of the goal fence.

It plugs the pure GoalFenceEngine into a Strands Agent as a HookProvider:
  - BeforeInvocationEvent  -> reset the trajectory (fresh run)
  - BeforeToolCallEvent    -> translate the tool-call into an Action, ask the engine,
                              apply the Verdict to the event (cancel / allow / escalate)
  - BeforeModelCallEvent   -> record a "model invoked" signal (drained by the server so the
                              flow panel can show model calls — AG-UI has no native event)

Everything that DECIDES lives behind the engine boundary (pure, unit-tested). This file
only does translation + enforcement, so it stays thin.

Enforcement mapping:
  ALLOW    -> tool runs; observe() folds it into the trajectory
  DENY     -> event.cancel_tool (SOFT block: this tool is skipped with an error result, but
              the model keeps reasoning and can explain "I can't do that")
  HALT     -> event.cancel_tool AND event.agent.cancel() (HARD stop: cancels this tool AND
              the whole invocation. Strands' agent.cancel() is a thread-safe stop-at-next-
              checkpoint; the loop ends with stop_reason="cancelled" and the model never
              gets another turn — a true kill, no UI/HITL needed)
  ESCALATE -> treated as DENY here (HITL is intentionally out of scope for this demo). The
              engine still emits ESCALATE so a future build can wire human approval; today
              the value rule blocks + explains that approval isn't available.
The engine DECISION KIND (DENY/HALT/ESCALATE) + rule are preserved on each verdict record
so the UI can label the outcome correctly.
"""
from __future__ import annotations

import collections
import logging

from strands.hooks import (
    HookProvider, HookRegistry, BeforeToolCallEvent, BeforeModelCallEvent,
)

try:  # BeforeInvocationEvent exists in current Strands; degrade gracefully if not.
    from strands.hooks import BeforeInvocationEvent
except Exception:  # pragma: no cover
    BeforeInvocationEvent = None

from .engine import GoalFenceEngine
from .entities import Action, Phase

log = logging.getLogger("iris-goalfence")


class GoalFenceHook(HookProvider):
    """Attaches a GoalFenceEngine to a Strands Agent.

    Args:
        engine: the pure decision engine (holds the charter + rules).
        verified_caller: the caller identity resolved from a TRUSTED source (the OBO/JWT
            sub), NEVER from tool args. The scope rule compares targets against this.
        on_verdict: optional callback(record: dict) invoked for every tool verdict, so the
            server can stream a CUSTOM goal_fence frame live. record shape:
            {tool, decision: "ALLOWED"|"CANCELLED", kind: "ALLOW|DENY|HALT|ESCALATE",
             rule, reason, detail}.
        drift_score: optional precomputed semantic-drift score for THIS invoke (the adapter
            computes it once per turn via DriftScorer; the engine only compares it).
    """

    def __init__(self, engine: GoalFenceEngine, verified_caller: str | None,
                 on_verdict=None, drift_score: float | None = None):
        self.engine = engine
        self.verified_caller = verified_caller
        self.on_verdict = on_verdict
        self.drift_score = drift_score
        self.state = engine.new_state()
        self.verdicts: list[dict] = []          # every tool verdict (for post-run summary)
        self.halted = False                      # any HALT verdict fired this run
        # Model-call signals — AG-UI has no native "model invoked" event, so we record each
        # BeforeModelCallEvent and let the server drain this deque between AG-UI frames.
        self._model_calls: collections.deque = collections.deque()

    def register_hooks(self, registry: HookRegistry, **kwargs):
        registry.add_callback(BeforeToolCallEvent, self._gate)
        registry.add_callback(BeforeModelCallEvent, self._on_model_call)
        if BeforeInvocationEvent is not None:
            registry.add_callback(BeforeInvocationEvent, self._reset)

    # -- lifecycle ----------------------------------------------------------------------
    def _reset(self, event):
        self.state = self.engine.new_state()
        self.verdicts = []
        self.halted = False

    def _on_model_call(self, event: BeforeModelCallEvent):
        try:
            self._model_calls.append(True)
        except Exception:
            pass

    # -- the veto -----------------------------------------------------------------------
    def _gate(self, event: BeforeToolCallEvent):
        tu = getattr(event, "tool_use", None) or {}
        name = (tu.get("name") or "").split("___").pop()   # bare name (strip gateway prefix)
        args = tu.get("input") or {}
        action = Action(tool_name=name, args=args, verified_caller=self.verified_caller,
                        phase=Phase.BEFORE_TOOL, drift_score=self.drift_score)
        verdict = self.engine.evaluate(self.state, action)

        record = {
            "tool": name,
            "decision": "CANCELLED" if verdict.blocked else "ALLOWED",  # legacy UI contract
            "kind": verdict.decision.value,                              # ALLOW|DENY|HALT|ESCALATE
            "rule": verdict.rule,
            "reason": verdict.reason if verdict.blocked else "within charter",
            "detail": verdict.detail,
        }
        self.verdicts.append(record)
        # ALERTING CONTRACT — do not change the "GOAL FENCE <decision>" prefix. A
        # CloudWatch Logs metric filter (filterPattern 'GOAL FENCE CANCELLED') turns every
        # block into the Iris/GoalFence·GoalFenceBlocks metric, which drives the Layer 7
        # incident alarm. The decision word, the DENY/HALT kind, the rule and the tool are
        # all kept so the alarm and log-based triage keep working.
        # `reason` is deliberately NOT logged: for the cross-actor rule it embeds the
        # target and caller customer ids. It still travels in the verdict record below
        # (SSE -> console UI -> threat engine), which is in-band to the authorized
        # operator rather than a downstream log sink.
        log.info("GOAL FENCE %s (%s/%s) tool=%s",
                 record["decision"], verdict.decision.value, verdict.rule, name)
        if self.on_verdict:
            try:
                self.on_verdict(record)
            except Exception:
                pass

        if verdict.blocked:
            kind = verdict.decision.value
            if kind == "ESCALATE":
                prefix = ("Blocked by the goal fence (Layer 6) — this high-impact action "
                          "needs human approval, which isn't available in this session")
            else:
                prefix = "Blocked by the goal fence (Layer 6)"
            # Always skip THIS tool with an error result.
            event.cancel_tool = (
                f"{prefix}. {verdict.reason} The agent's chartered goal bounds it to the "
                f"caller's own records at small scale; this action is out of scope.")
            # HALT is a HARD stop: also cancel the whole invocation so the agent can't
            # continue reasoning or emit any further response. Strands' agent.cancel() is
            # thread-safe and ends the loop with stop_reason="cancelled" at the next
            # checkpoint. The event exposes the Agent instance (HookEvent.agent).
            if kind == "HALT":
                self.halted = True
                agent = getattr(event, "agent", None)
                if agent is not None and hasattr(agent, "cancel"):
                    try:
                        agent.cancel()
                        log.info("GOAL FENCE HALT -> agent.cancel() issued (rule=%s)",
                                 verdict.rule)
                    except Exception as e:
                        log.warning("agent.cancel() failed (%s) — tool still cancelled", e)
        else:
            self.engine.observe(self.state, action)
