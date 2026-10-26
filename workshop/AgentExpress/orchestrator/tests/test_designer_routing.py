"""AgentExpress Assistant (bff/designer.py): sending each file to its own destination.

Seen on a live session: asked to send each of several documents to its own destination,
the Assistant built one target for everything and said the deliver step "can't filter by
artifact", then asked whether to add per-destination delivery. Each deliver target has
always taken `files` and `producedBy` filters (app/common/deliver.py `select`); the guide
now says so, and a change built that way is applied as it is.

Scenarios: expense reports and research publishing; the model is scripted.
"""

# ruff: noqa: F811 - `env` and `model` are fixtures, imported so every test can take them
from __future__ import annotations

import pytest
from test_builds import env, save  # noqa: F401 - env is the builds fixture, taken by name
from test_designer import draft, model, run_background, says, send, tool_use  # noqa: F401

from app.common import deliver

moto = pytest.importorskip("moto")

AGENT = {"runtime": "main", "maxTokens": 1500, "agentcore": {"guardrails": {"input": True, "output": True}}}


def guide(env) -> str:
    return " ".join(env.handler.designer.system_prompt()[0]["text"].split())


def test_the_guide_says_each_target_takes_its_own_files(env):
    text = guide(env)
    for part in (("Each target can take only some of those files: `files` (name patterns such as [\"*.pdf\"]) "
                  "and/or `producedBy` (the ids of the earlier steps that saved them)"),
                 "ONE deliver step with one target per destination, each filtered to its own files",
                 '"producedBy": ["<step id>"]',
                 "When the user names destinations but not how to reach them, give each one an s3 target",
                 ("Never say the deliver step cannot route particular files to particular targets or filter "
                  "by step or file"),
                 "never ask whether to add routing the user already asked for"):
        assert part in text, part


@pytest.mark.parametrize("producers,destinations", [
    # Expense reports: the summary to finance, the receipts bundle to audit.
    ({"expense_summary": "summary.pdf", "receipt_bundle": "receipts.pdf"}, ["finance", "audit"]),
    # Research publishing: the paper to the editors, the dataset sheet to the archive.
    ({"paper_writer": "paper.docx", "dataset_sheet": "dataset.pdf"}, ["editors", "archive"]),
])
def test_one_deliver_step_routes_each_producers_file_to_its_own_target(env, model, producers, destinations):
    save(env)
    targets = [{"type": "s3", "uri": f"${{storage}}/delivered/{dest}/", "producedBy": [step]}
               for step, dest in zip(producers, destinations, strict=True)]
    ops = [{"op": "set_agent", "id": step, "agent": {**AGENT, "name": step.replace("_", " ").title()},
            "prompt": {"systemPrompt": f"You write the {name}.", "schema": '{"title": ""}'}}
           for step, name in producers.items()]
    ops += [{"op": "set_agent", "id": "send_files",
             "agent": {"name": "Send Files", "runtime": "task", "task": {"kind": "deliver", "targets": targets}}},
            {"op": "set_block", "name": "orchestrator", "value": {"files": {}, "storage": {"create": True}}},
            {"op": "set_steps", "steps": [{"agent": "claims_intake", "hitl": True},
                                          {"parallel": list(producers), "gateId": "write", "gateName": "Write"},
                                          {"agent": "send_files"}, {"agent": "partner"}]}]
    script = model(tool_use(("apply_changes", {"summary": "Each file goes to its own destination",
                                               "version_name": "routed delivery", "ops": ops})),
                   says("Each document goes to its own destination."))
    send(env, "Send each document to its own team's folder.")
    run_background(env)
    result = script.calls[1]["messages"][-1]["content"][0]["toolResult"]["content"][0]["json"]
    assert result["status"] == "applied", result
    saved = draft(env)["workflow"]["agents"]["send_files"]["task"]["targets"]
    assert saved == targets
    # At run time each target sends only its producer's file (each file records its step).
    files = [{"name": name, "agent": step} for step, name in producers.items()]
    for target, name in zip(saved, producers.values(), strict=True):
        assert [f["name"] for f in deliver.select(files, target)] == [name]
