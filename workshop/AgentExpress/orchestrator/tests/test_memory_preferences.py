"""A recalled preference reaches the agent whatever the new run is about (app/common/context.py).

Seen on a live run: a person stated a tone and a format in one run; the next run recalled
"Tone should be short and upbeat, formatted with bullet points." from the user-preference
strategy and then dropped it, because the off-topic filter (_on_topic) keeps only records
that share a subject word with the new request. A preference is about the person, not the
subject, so it is kept; every other strategy's records are still filtered as before.

Scenarios: an invoice-processing run and a staff-onboarding run.
"""
from __future__ import annotations

import asyncio

import pytest


def _ctx(strategies, topic, recorded):
    from app.common.context import AgentContext
    c = AgentContext.__new__(AgentContext)
    c.agent_id, c.subject_id, c.topic, c.session_id = "writer", "", topic, "s"
    c._agentcore = {"memory": {"longTerm": strategies}}
    c._record_memory = lambda op, ns, q, res, ms: recorded.append((op, ns, res))
    return c


@pytest.fixture()
def store(monkeypatch):
    """A fake long-term store: namespace prefix -> what a recall there returns."""
    from app.features import memory as memory_pkg
    by_prefix: dict[str, list[str]] = {}

    async def fake_recall(query, namespace="", memory_id="", namespace_path=""):
        where = namespace or namespace_path
        return next((v for k, v in by_prefix.items() if where.startswith(f"/{k}/")), [])
    monkeypatch.setattr(memory_pkg, "recall", fake_recall)
    return by_prefix


@pytest.mark.parametrize("request_text,preference,off_topic,on_topic", [
    ("Code invoice INV-2207 from the office supplier and post it to the ledger",
     "Prefers replies in a formal tone, with numbered steps.",
     "Quarterly webinar attendance grew after the reminder emails.",
     "Office supplier invoices go to cost centre 410."),
    ("Prepare the welcome pack for a new hire starting Monday",
     "Tone should be short and upbeat, formatted with bullet points.",
     "Database migrations need a rollback script.",
     "A welcome pack includes the laptop checklist."),
])
def test_a_preference_is_kept_for_any_subject_and_off_topic_insights_are_still_dropped(store, request_text,
                                                                                     preference, off_topic, on_topic):
    store.update({"preferences": [preference], "insights": [off_topic, on_topic]})
    recorded: list = []
    ctx = _ctx(["userPreference", "semantic"], request_text, recorded)
    got = asyncio.run(ctx.memory_recall(request_text))
    assert got == [preference, on_topic]
    note = next(r[2] for r in recorded if r[0] == "recall-filtered")
    assert note.startswith("2 of 3 recalled insight(s) kept (preferences always")


def test_with_only_preferences_nothing_is_dropped_and_no_filter_row_is_written(store):
    store.update({"preferences": ["Prefers tables over prose."]})
    recorded: list = []
    ctx = _ctx(["userPreference"], "Summarise the overnight device alerts", recorded)
    assert asyncio.run(ctx.memory_recall("Summarise the overnight device alerts")) == ["Prefers tables over prose."]
    assert [r[0] for r in recorded] == ["recall"]
