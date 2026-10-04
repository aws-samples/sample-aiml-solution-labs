"""`agentcore.evaluations.enabled` is enforced where it cannot be bypassed.

The defect this file exists for: `is_enabled()` was defined in
`app/features/evaluations/service.py` and had **zero callers**. The only thing reading
the config key was `web/observability.js`, which hides the Evaluate button — and a
client-side gate is not enforcement. Anyone who called the REST route, or asked the
in-app assistant, or was reached by `auto_evaluate_session`, had the agent scored and
BILLED against the AgentCore Evaluations API, with a `kind="eval"` telemetry row
written, under `_DEFAULT_EVALUATORS` the agent never declared.

That is the exact failure `tests/test_config_keys.py` was written to prevent — a key
that reads like a switch and controls nothing — one level deeper, in the enforcement
rather than in the vocabulary.

The gate is in `evaluate_agent` because that is the single function EVERY path goes
through: the UI button and REST route (via `runtime._run_eval`), the assistant's
`run_evaluation` tool, and `auto_evaluate_session`. Gating at the callers is what
produced the bug — one caller was gated and the others were not.
"""
import json

import pytest
from conftest import workflow


def _wf(enabled: bool | None, *, auto: bool = False, evaluators=None) -> dict:
    """A one-agent workflow whose evaluations block is exactly as given.

    `enabled=None` omits the block entirely, which is how most agents are written.
    """
    agentcore: dict = {}
    if enabled is not None:
        block: dict = {"enabled": enabled}
        if auto:
            block["auto"] = True
        if evaluators:
            block["evaluators"] = evaluators
        agentcore["evaluations"] = block
    return {
        "orchestrator": {"defaultModel": "m"},
        "agents": {"solo": {"name": "Solo", "maxTokens": 100,
                            **({"agentcore": agentcore} if agentcore else {})}},
        "steps": [{"agent": "solo"}],
    }


def _evaluate(imp, agent_id="solo", **kw):
    """Call the real evaluate_agent. Anything that reaches AWS explodes loudly, so a
    test that gets past the gate cannot pass by accident."""
    service = imp("app.features.evaluations.service")

    def _boom(*_a, **_k):
        raise AssertionError(
            "evaluate_agent built an AgentCore client, i.e. it got PAST the gate and "
            "would have scored and billed this agent")

    service._agentcore_client = _boom
    return service.evaluate_agent("s1", agent_id, **kw)


# ---------------------------------------------------------------------------
# The gate
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("spec,why", [
    (None, "no evaluations block at all — how most agents are written"),
    (False, "explicitly disabled"),
])
def test_an_agent_without_evaluations_enabled_is_never_scored(spec, why):
    with workflow(_wf(spec)) as imp:
        summaries = _evaluate(imp)
    assert len(summaries) == 1, f"{why}: {summaries}"
    assert summaries[0]["status"] == "disabled", why
    assert "not enabled" in summaries[0]["reason"]
    assert "workflow.json" in summaries[0]["reason"]


def test_the_refusal_is_distinguishable_from_nothing_scorable():
    """Both used to arrive as an empty list, so the timeline said "nothing scorable
    found" for a config mistake and sent the reader looking for missing telemetry."""
    with workflow(_wf(False)) as imp:
        summaries = _evaluate(imp)
    assert summaries and summaries[0]["status"] == "disabled"
    assert summaries[0]["status"] != "ok"
    assert summaries[0]["evaluator"] is None


def test_an_explicit_evaluators_list_does_not_bypass_the_gate():
    """A caller may pass evaluators directly (the signature allows it). That must not
    be a way around the agent's own config."""
    with workflow(_wf(False)) as imp:
        summaries = _evaluate(imp, evaluators=["Builtin.Faithfulness"])
    assert summaries[0]["status"] == "disabled"


def test_a_named_prompt_does_not_bypass_the_gate():
    with workflow(_wf(False)) as imp:
        summaries = _evaluate(imp, prompt="solo")
    assert summaries[0]["status"] == "disabled"


def test_an_enabled_agent_gets_past_the_gate():
    """The other half: the gate must not be a blanket off switch. Getting past it trips
    the exploding client, which is how we know it proceeded."""
    with workflow(_wf(True)) as imp, pytest.raises(AssertionError, match="PAST the gate"):
        _evaluate(imp)


# ---------------------------------------------------------------------------
# The auto path
# ---------------------------------------------------------------------------

def test_auto_evaluation_only_covers_agents_that_enabled_both_flags():
    """`is_auto` requires enabled AND auto. Pinned because it is the one path that was
    already gated, and the fix must not have loosened it."""
    with workflow(_wf(True, auto=True)) as imp:
        service = imp("app.features.evaluations.service")
        assert service.is_auto("solo") is True
        assert service.is_enabled("solo") is True

    with workflow(_wf(True)) as imp:  # enabled, but not auto
        service = imp("app.features.evaluations.service")
        assert service.is_auto("solo") is False
        assert service.is_enabled("solo") is True

    with workflow(_wf(False, auto=True)) as imp:  # auto without enabled means nothing
        service = imp("app.features.evaluations.service")
        assert service.is_auto("solo") is False
        assert service.is_enabled("solo") is False


def test_auto_evaluate_session_scores_nothing_when_no_agent_opted_in():
    with workflow(_wf(None)) as imp:
        service = imp("app.features.evaluations.service")
        called = []
        service.evaluate_agent = lambda *a, **k: called.append(a)
        service.auto_evaluate_session("s1")
    assert called == []


# ---------------------------------------------------------------------------
# is_enabled has a caller now — which is the whole point
# ---------------------------------------------------------------------------

def test_is_enabled_is_actually_wired_into_evaluate_agent():
    """The defect was a correct function nobody called. Asserted against the source so
    it cannot silently become dead again."""
    import inspect

    from app.features.evaluations import service

    src = inspect.getsource(service.evaluate_agent)
    assert "is_enabled(agent_id)" in src, (
        "evaluate_agent no longer consults is_enabled, so evaluations.enabled is back "
        "to gating only the UI button")


def test_the_shipped_workflow_still_has_evaluable_agents():
    """A gate that accidentally refused everything would also pass the tests above."""
    from app.features.evaluations.service import is_enabled

    wf = json.loads((__import__("conftest").ORCH_ROOT / "app" / "workflow.json").read_text())
    enabled = [aid for aid in wf["agents"] if is_enabled(aid)]
    # AT LEAST ONE, not at least five. The invariant is that the gate has something to
    # gate; five was the shipped sample's count, and asserting it told a customer with
    # four evaluable agents that the framework was broken.
    assert enabled, ("no agent in this workflow enables evaluations, so the gate above "
                     "would pass while refusing everything")


# ---------------------------------------------------------------------------
# What the reader is told
# ---------------------------------------------------------------------------
# Three outcomes, and two of them used to be indistinguishable. `outcome_log` is the
# one place that decides, so the timeline cannot report a config mistake as missing
# telemetry — which sent a reader hunting for absent spans instead of looking at their
# own workflow.json.

def test_a_refusal_is_reported_as_a_config_problem():
    from app.features.evaluations.service import outcome_log

    refused = [{"agent_id": "solo", "status": "disabled", "evaluator": None,
                "reason": "evaluations are not enabled for 'solo'; set "
                          "agentcore.evaluations.enabled on that agent in workflow.json"}]
    line = outcome_log("solo", refused)
    assert line is not None
    assert "skipped" in line
    assert "workflow.json" in line
    assert "nothing scorable" not in line


def test_an_empty_result_is_reported_as_nothing_scorable():
    from app.features.evaluations.service import outcome_log

    for empty in ([], None):
        line = outcome_log("solo", empty)
        assert line == "Evaluation: nothing scorable found for solo"


def test_real_scores_get_no_summary_line_because_each_is_reported_individually():
    from app.features.evaluations.service import outcome_log

    scored = [{"evaluator": "Builtin.Faithfulness", "status": "ok", "label": "high",
               "value": 0.9, "prompt": "solo"}]
    assert outcome_log("solo", scored) is None


def test_an_evaluator_error_is_not_mistaken_for_a_refusal():
    """A failed evaluator is a real attempt that produced a row; it must not be
    reported as "skipped", which would read as though nothing was billed."""
    from app.features.evaluations.service import outcome_log

    errored = [{"evaluator": "Builtin.Faithfulness", "status": "error",
                "prompt": "solo", "error": "ValidationException"}]
    assert outcome_log("solo", errored) is None


def test_the_runtime_uses_that_decision_rather_than_its_own():
    """The logic was inline in runtime._run_eval, where nothing could reach it."""
    import inspect
    from pathlib import Path

    from conftest import ORCH_ROOT

    src = Path(ORCH_ROOT / "app" / "orchestrator" / "runtime.py").read_text()
    assert "outcome_log(agent_id, summaries)" in src, (
        "runtime._run_eval no longer asks evaluations.outcome_log, so the timeline "
        "message is back to being untestable inline logic")
    # And the helper must still be the thing that distinguishes the two cases.
    from app.features.evaluations import service
    assert '"disabled"' in inspect.getsource(service.outcome_log)
