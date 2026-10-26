"""AgentExpress Assistant (bff/designer.py): a turn that runs out of model calls still says what
it saved.

Seen on a live session: a long first brief was built in several apply_changes calls, the
turn used all of its model calls before the model wrote a reply, and the reply was only
"Now the OpenAPI tool and the agents. I made several changes but ran out of steps." The
changes had landed, but the user was told none of them, nor the defaults used. The ending
now comes from the change records: what was saved, the defaults, and what is still needed.

Scenarios: a research-report build and a device-alert build; the model is scripted.
"""
# ruff: noqa: F811 - `env` and `model` are fixtures, imported so every test can take them
from __future__ import annotations

import pytest
from test_builds import env, save  # noqa: F401 - env is the builds fixture, taken by name
from test_designer import conversation, draft, model, run_background, send, tool_use  # noqa: F401

moto = pytest.importorskip("moto")

AGENT = {"runtime": "main", "maxTokens": 1500, "agentcore": {"guardrails": {"input": True, "output": True}}}


def add(agent_id: str, summary: str, version: str, defaults: list[str], needs=None, after=()) -> tuple[str, dict]:
    """A change that adds `agent_id`, run after the agents in `after` (added earlier)."""
    return ("apply_changes", {
        "summary": summary, "version_name": version, "defaults_used": defaults,
        **({"needs_input": needs} if needs else {}),
        "ops": [{"op": "set_agent", "id": agent_id, "agent": {**AGENT, "name": agent_id.title()},
                 "prompt": {"systemPrompt": f"You are the {agent_id}.", "schema": "{}"}},
                {"op": "set_steps", "steps": [{"agent": "claims_intake", "hitl": True},
                                              *[{"agent": a} for a in (*after, agent_id)],
                                              {"agent": "partner"}]}]})


@pytest.mark.parametrize("calls,expect", [
    # A research-report build: two changes, the second with a value only the user can give.
    ([add("source_finder", "Added the source finder", "sources", ["model: Claude Haiku 4.5"]),
      add("report_writer", "Added the report writer", "writer", ["maxTokens 1500", "model: Claude Haiku 4.5"],
          [{"path": "tools.journals.endpoint", "question": "Which journal search API?"}],
          after=("source_finder",))],
     ["- Added the source finder (sources)", "- Added the report writer (writer)", "Defaults used:",
      "- model: Claude Haiku 4.5", "- maxTokens 1500", "Still needed from you:",
      "- tools.journals.endpoint: Which journal search API?"]),
    # A device-alert build: one change, then a call that never ends the turn.
    ([add("alert_triage", "Added alert triage", "triage", ["temperature 0"]),
      ("search_registry", {"query": "pager"})],
     ["- Added alert triage (triage)", "Defaults used:", "- temperature 0"]),
])
def test_out_of_steps_the_reply_lists_what_was_saved_and_the_defaults(env, model, monkeypatch, calls, expect):
    save(env)
    monkeypatch.setattr(env.handler.designer, "MAX_CALLS", len(calls))
    monkeypatch.setattr(env.handler.designer, "search_registry", lambda args: {"results": []})
    model(*[tool_use(c, text="Working on it." if i == 0 else "") for i, c in enumerate(calls)])
    send(env, "Build it")
    run_background(env)
    reply = conversation(env)[1]["turns"][-1]
    assert reply["status"] == "done"
    assert "I ran out of steps before I could finish this reply. What I saved:" in reply["text"]
    for line in expect:
        assert line in reply["text"].split("\n"), line
    # Each default once, and the changes are really saved.
    assert reply["text"].count("- model: Claude Haiku 4.5") <= 1
    assert len(reply["changes"]) == sum(1 for c in calls if c[0] == "apply_changes")
    assert set(draft(env)["workflow"]["agents"]) >= {c[1]["ops"][0]["id"] for c in calls if c[0] == "apply_changes"}


def test_out_of_steps_with_nothing_saved_says_so(env, model, monkeypatch):
    save(env)
    monkeypatch.setattr(env.handler.designer, "MAX_CALLS", 1)
    monkeypatch.setattr(env.handler.designer, "search_registry", lambda args: {"results": []})
    model(tool_use(("search_registry", {"query": "invoice parser"})))
    send(env, "Find an invoice parser")
    run_background(env)
    reply = conversation(env)[1]["turns"][-1]
    assert reply["text"].endswith("I ran out of steps before I changed anything. Tell me what to do next.")
    assert not reply.get("changes")
