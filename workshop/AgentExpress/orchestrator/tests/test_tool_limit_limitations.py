"""An agent that runs out of tool calls is told which tools it did not call (app/common/tool_loop.py).

Seen on a live run: an agent spent its maxToolCalls on look-ups, never called the tool
that publishes, and still answered "published successfully" with a made-up link. The
limit was only on the timeline. Now, when the limit is reached, every call refused for it
and every bound tool never called is a limitation the agent's answer is built with ("was
not called ... so there is no result from it. Do not report one."), the same channel a
refused or failed call already uses.

Scenarios: invoice payment (a look-up tool, then a payment tool past the limit) and a
research digest (a search tool, and an archiving tool never reached).
"""
from __future__ import annotations

from conftest import workflow
from test_tool_loop import AI, FakeCtx, FakeTool, _run

ARN = "arn:aws:lambda:us-east-1:123456789012:function:f"


def tool(name: str, op: str) -> dict:
    return {"type": "lambda", "description": name, "lambdaArn": ARN,
            "toolSchema": [{"name": op, "properties": {"claim": {"type": "string"}}}]}


def wf(tools: dict, bound: list[str], limit: int) -> dict:
    return {"orchestrator": {}, "tools": tools,
            "agents": {"triage": {"name": "Worker", "runtime": "dedicated", "maxTokens": 100, "tool": bound,
                                  "toolMode": "model", "maxToolCalls": limit}},
            "steps": [{"agent": "triage"}]}


def test_invoice_payment_asked_for_past_the_limit_is_a_limitation(monkeypatch):
    tools = {"ledgerLookup": tool("Ledger", "find"), "payments": tool("Payments", "pay")}
    with workflow(wf(tools, list(tools), 2)) as imp:
        find = FakeTool("ledgerLookup___find", answer="invoice INV-1 open, 120.00")
        pay = FakeTool("payments___pay", answer="paid")
        ctx = FakeCtx(list(tools), max_tool_calls=2)
        script = [AI(("ledgerLookup___find", {"claim": "INV-1"}), ("ledgerLookup___find", {"claim": "INV-2"})),
                  AI(("payments___pay", {"claim": "INV-1"})),
                  AI(text="done")]
        _text, gaps, _turns = _run(monkeypatch, imp, ctx, script, {"ledgerLookup": [find], "payments": [pay]})
    assert pay.seen == []
    assert ("'payments___pay' was not called for {\"claim\": \"INV-1\"}: this agent reached its limit of 2 "
            "tool calls (maxToolCalls), so there is no result from it. Do not report one.") in gaps
    # The tool it asked for is named once, not again as "never called".
    assert sum("payments" in g for g in gaps) == 1
    assert not any("'ledgerLookup' was not called" in g for g in gaps)


def test_research_archive_never_reached_is_a_limitation(monkeypatch):
    tools = {"paperSearch": tool("Papers", "search"), "archive": tool("Archive", "store")}
    with workflow(wf(tools, list(tools), 1)) as imp:
        search = FakeTool("paperSearch___search", answer="3 papers")
        store = FakeTool("archive___store", answer="stored")
        ctx = FakeCtx(list(tools), max_tool_calls=1)
        script = [AI(("paperSearch___search", {"claim": "battery recycling"})), AI(text="Digest written.")]
        _text, gaps, _turns = _run(monkeypatch, imp, ctx, script, {"paperSearch": [search], "archive": [store]})
    assert ("'archive' was not called: this agent reached its limit of 1 tool calls (maxToolCalls) first, so "
            "there is no result from it. Do not report one.") in gaps
    assert ctx.logs[-1].startswith("Tool calls: reached this agent's limit of 1")


def test_under_the_limit_an_uncalled_tool_is_not_reported(monkeypatch):
    tools = {"paperSearch": tool("Papers", "search"), "archive": tool("Archive", "store")}
    with workflow(wf(tools, list(tools), 6)) as imp:
        search = FakeTool("paperSearch___search", answer="3 papers")
        ctx = FakeCtx(list(tools), max_tool_calls=6)
        script = [AI(("paperSearch___search", {"claim": "battery recycling"})), AI(text="Digest written.")]
        _text, gaps, _turns = _run(monkeypatch, imp, ctx, script,
                                   {"paperSearch": [search], "archive": [FakeTool("archive___store")]})
    assert not any("was not called" in g for g in gaps)
