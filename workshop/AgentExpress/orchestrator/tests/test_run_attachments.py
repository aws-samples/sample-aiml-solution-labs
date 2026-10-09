"""The files a run is started with (workflow.json `attachments` on an agent): the BFF
copies them into the run's folder, and an agent that sets `attachments` gets them as
Converse document or image content on each of its model calls.

What has to hold, on the runtime side:
  * only this run's own attachment keys are read, at most 5, well-formed only;
  * documents go as `document` blocks with a name Converse accepts, images as `image`;
  * an agent without `attachments` sends exactly what it sent before;
  * the logs and telemetry name the files, never carry their bytes;
  * a dedicated agent receives the list in its payload, and only if it reads them.
"""
from __future__ import annotations

import asyncio
import sys
import types

import boto3
import pytest

moto = pytest.importorskip("moto")

SID = "abc123def456"
BUCKET = "agentcore-x-assets-1"
OWN = f"runs/{SID}/attachments/"


def _f(name, kind="document", fmt="pdf", key=None):
    return {"key": key or f"{OWN}1-{name}", "name": name, "kind": kind, "format": fmt, "size": 3}


@pytest.fixture()
def s3(monkeypatch):
    for k, v in {"AWS_ACCESS_KEY_ID": "t", "AWS_SECRET_ACCESS_KEY": "t",
                 "AWS_DEFAULT_REGION": "us-east-1", "AWS_REGION": "us-east-1"}.items():
        monkeypatch.setenv(k, v)
    with moto.mock_aws():
        client = boto3.client("s3")
        client.create_bucket(Bucket=BUCKET)
        from app.common import attachments
        attachments._clients.clear()
        yield client, attachments


def test_only_this_runs_own_well_formed_files_are_listed():
    from app.common import attachments
    files = [_f("a.pdf"), _f("b.png", "image", "png"),
             _f("x.pdf", key="runs/otherrun0001/attachments/1-x.pdf"),     # another run
             _f("y.pdf", key=f"{OWN}sub/1-y.pdf"),                        # not directly in it
             _f("z.pdf", key=f"runs/{SID}/poster/1.png"),                  # not an attachment
             _f("w.exe", fmt="exe"), "junk", None]
    assert [f["name"] for f in attachments.mine(files, SID)] == ["a.pdf", "b.png"]
    many = [_f(f"{i}.pdf", key=f"{OWN}{i}-{i}.pdf") for i in range(9)]
    assert len(attachments.mine(many, SID)) == 5


def test_they_are_read_as_document_and_image_blocks(s3):
    client, attachments = s3
    client.put_object(Bucket=BUCKET, Key=f"{OWN}1-Q3 report (final).pdf", Body=b"%PDF")
    client.put_object(Bucket=BUCKET, Key=f"{OWN}2-chart.png", Body=b"\x89PNG")
    client.put_object(Bucket=BUCKET, Key=f"{OWN}3-Q3 report (final).pdf", Body=b"%PDF2")
    got = attachments.load_for([_f("Q3 report (final).pdf", key=f"{OWN}1-Q3 report (final).pdf"),
                                _f("chart.png", "image", "png", key=f"{OWN}2-chart.png"),
                                _f("Q3 report (final).pdf", key=f"{OWN}3-Q3 report (final).pdf")],
                               session_id=SID, bucket=BUCKET)
    blocks = attachments.blocks(got)
    assert blocks[0] == {"document": {"format": "pdf", "name": "Q3 report (final)",
                                      "source": {"bytes": b"%PDF"}}}
    assert blocks[1] == {"image": {"format": "png", "source": {"bytes": b"\x89PNG"}}}
    assert blocks[2]["document"]["name"] == "Q3 report (final) 2"      # unique in the message
    assert attachments.load_for([], session_id=SID, bucket=BUCKET) == []
    with pytest.raises(RuntimeError, match="no assets bucket"):
        attachments.load_for([_f("a.pdf")], session_id=SID, bucket="")


class _Msg:
    def __init__(self):
        self.content = "Read it."
        self.response_metadata = {"stopReason": "end_turn"}
        self.usage_metadata = {"input_tokens": 10, "output_tokens": 2}


def test_a_call_with_files_sends_them_after_the_text_and_names_them_in_telemetry(monkeypatch):
    from app.common import llm as llm_mod
    sent, metered = [], []

    class _FakeLLM:
        def __init__(self, **_kw):
            pass

        async def ainvoke(self, messages):
            sent.append(messages)
            return _Msg()
    fake = types.ModuleType("langchain_aws")
    fake.ChatBedrockConverse = _FakeLLM
    monkeypatch.setitem(sys.modules, "langchain_aws", fake)
    monkeypatch.setattr(llm_mod, "_meter_llm", lambda *a, **k: metered.append(a[3]))
    files = [{"kind": "document", "format": "pdf", "name": "a.pdf", "docName": "a", "bytes": b"%PDF"}]
    asyncio.run(llm_mod.run_llm("read", "sys", "Summarise", files=files))
    _system, human = sent[0]
    assert human.content == [{"type": "text", "text": "Summarise"},
                             {"document": {"format": "pdf", "name": "a", "source": {"bytes": b"%PDF"}}}]
    assert metered[0] == "Summarise\n\n[1 attached file(s): a.pdf]"


def _ctx(reads: bool, files: list):
    from app.common.context import AgentContext
    c = AgentContext.__new__(AgentContext)
    c.agent_id, c.model, c.temperature, c.max_tokens = "reader", None, 0, 1000
    c.recalled_memory, c.truncated_calls, c.session_id = [], [], SID
    c.state = {"outputs": {}, "attachments": files}
    c.vision, c._seen_images = {}, None
    c.reads_attachments, c._files = reads, None
    c.logged = []

    async def _log(msg):
        c.logged.append(msg)
    c.log = _log
    return c


def test_an_agent_with_attachments_reads_them_once_and_others_never_do(monkeypatch):
    from app.common import attachments
    from app.common import context as ctx_mod
    calls, loads = [], []

    async def fake_run_llm(name, system, user, **kw):
        calls.append((user, kw.get("files")))
        return "ok", False

    def fake_load(files, *, session_id):
        loads.append((len(files), session_id))
        return [{"kind": "document", "format": "pdf", "name": f["name"], "docName": "a", "bytes": b"x"}
                for f in files]
    monkeypatch.setattr(ctx_mod, "run_llm", fake_run_llm)
    monkeypatch.setattr(attachments, "load_for", fake_load)

    c = _ctx(True, [_f("brief.pdf")])
    asyncio.run(c.llm("sys", "Plan it"))
    asyncio.run(c.llm("sys", "again"))
    assert loads == [(1, SID)]                                     # once per run of the agent
    assert "attached to this message: brief.pdf" in calls[0][0] and len(calls[0][1]) == 1
    assert c.logged == ["Reading 1 attached file: brief.pdf"]

    calls.clear()
    loads.clear()
    plain = _ctx(False, [_f("brief.pdf")])
    asyncio.run(plain.llm("sys", "user"))
    assert loads == [] and calls == [("user", [])]


def test_the_registry_and_a_dedicated_payload_carry_it():
    from app.common.agentcore_agent import AgentCoreRuntimeAgent
    from app.orchestrator import registry

    class A:
        pass
    a, b = A(), A()
    registry._configure(a, "reader", {"name": "R", "attachments": True})
    registry._configure(b, "plain", {"name": "P"})
    assert a.attachments is True and b.attachments is False
    from pathlib import Path
    src = Path(sys.modules[AgentCoreRuntimeAgent.__module__].__file__).read_text()
    assert '"attachments": (ctx.state or {}).get("attachments")' in src
    assert 'if getattr(self, "attachments", False)' in src
