"""Prompt-caching insights (bff/cache_insights.py) over recorded telemetry rows, and their
place on GET /api/telemetry/aggregate. Costs are the rows' recorded ones; the model minimum
comes from app/vocabulary.json promptCacheModels."""
import importlib.util
import json
from decimal import Decimal
from pathlib import Path

import cache_insights

HAIKU45 = "us.anthropic.claude-haiku-4-5-20251001-v1:0"
NOVA = "us.amazon.nova-lite-v1:0"


def _llm(agent, read=0, write=0, read_cost="0", write_cost="0", saving="0",
         requested="system", model=HAIKU45, known=True, **extra):
    return {"agent_id": agent, "kind": "llm", "label": model, "cache_requested": requested,
            "cache_read_tokens": read, "cache_write_tokens": write,
            "cache_read_cost_usd": Decimal(read_cost), "cache_write_cost_usd": Decimal(write_cost),
            "cache_saving_usd": Decimal(saving), "cache_cost_known": known, "rates_known": True,
            **extra}


def _insights(rows):
    r = cache_insights.Rollup()
    for row in rows:
        r.add(row)
    return r.insights()


def test_written_but_rarely_read():
    out = _insights([_llm("draft", write=5000, write_cost="0.00625", saving="-0.00125"),
                     _llm("draft", read=1000, write=5000, read_cost="0.0001",
                          write_cost="0.00625", saving="-0.00035")])
    assert len(out) == 1
    i = out[0]
    assert i["kind"] == "rarely_read" and i["agentId"] == "draft"
    assert (i["cacheReadTokens"], i["cacheWriteTokens"]) == (1000, 10000)
    assert i["cacheReadCostUsd"] == Decimal("0.0001")
    assert i["cacheWriteCostUsd"] == Decimal("0.0125")
    assert i["netSavingUsd"] == Decimal("-0.0016")
    assert i["rule"] == cache_insights.RARELY_READ_RULE


def test_requested_but_nothing_cached_shows_the_table_minimum():
    out = _insights([_llm("intake"), _llm("intake", model=NOVA)])
    assert len(out) == 1
    i = out[0]
    assert i["kind"] == "nothing_cached" and i["requestedCalls"] == 2
    mins = {m["model"]: m["minTokens"] for m in i["models"]}
    vocab = json.loads((Path(cache_insights.__file__).resolve().parent.parent
                        / "app" / "vocabulary.json").read_text())["promptCacheModels"]
    assert mins[HAIKU45] == vocab["byModel"]["anthropic.claude-haiku-4-5-20251001-v1:0"]["minTokensText"]
    assert mins[NOVA] == vocab["byModel"]["amazon.nova-lite-v1:0"]["minTokensText"]
    assert i["source"] == vocab["source"]


def test_paying_off_with_the_net_saving():
    out = _insights([_llm("report", write=4000, write_cost="0.005", saving="-0.001"),
                     _llm("report", read=4000, read_cost="0.0004", saving="0.0036"),
                     _llm("report", read=4000, read_cost="0.0004", saving="0.0036")])
    assert [i["kind"] for i in out] == ["paying_off"]
    assert out[0]["netSavingUsd"] == Decimal("0.0062")


def test_reads_below_writes_but_a_positive_saving_counts_as_paying_off():
    """Writes replace input billed at the normal rate anyway, so a few reads can already pay."""
    out = _insights([_llm("a", read=2000, write=4000, read_cost="0.0002",
                          write_cost="0.005", saving="0.0008")])
    assert [i["kind"] for i in out] == ["paying_off"]


def test_unknown_cost_is_reported_as_unknown_never_estimated():
    out = _insights([_llm("gpt", write=3000, known=False),
                     _llm("gpt", read=100, known=False)])
    i = out[0]
    assert i["kind"] == "rarely_read" and i["costKnown"] is False
    assert i["cacheReadCostUsd"] is None and i["cacheWriteCostUsd"] is None
    assert i["netSavingUsd"] is None
    # A fallback-priced model is just as unknown, and never "paying off".
    out = _insights([_llm("x", read=9000, write=1000, saving="0.01", rates_known=False)])
    assert out == []


def test_no_caching_at_all_gives_no_cache_insights():
    rows = [{"agent_id": "a", "kind": "llm", "label": HAIKU45, "input_tokens": 100},
            _llm("b", requested=""), {"agent_id": "b", "kind": "tool", "label": "search"}]
    assert _insights(rows) == []
    assert _insights([]) == []


def test_aggregate_route_carries_the_cache_insights(monkeypatch):
    path = Path(__file__).resolve().parent.parent / "bff" / "handler.py"
    monkeypatch.setenv("WORKFLOW_JSON", json.dumps({"orchestrator": {}, "agents": {}}))
    spec = importlib.util.spec_from_file_location("_cache_insights_bff_handler", path)
    handler = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(handler)
    rows = [dict(_llm("intake"), date="2026-10-20", session_id="s1")]
    monkeypatch.setattr(handler, "_query_all", lambda tbl, **kw: [dict(r) for r in rows])
    out = handler._telemetry_aggregate("date", "2026-10-20", "2026-10-20", tbl=object())
    body = json.loads(out["body"])
    assert [i["kind"] for i in body["cacheInsights"]] == ["nothing_cached"]
    assert body["cacheInsights"][0]["agentId"] == "intake"
