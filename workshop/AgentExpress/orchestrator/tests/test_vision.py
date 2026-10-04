"""An agent that READS images (workflow.json `vision`): the images earlier agents of the
same run produced go to its model as Converse image content, next to its text.

What has to hold:
  * only keys under THIS run's folder are read, at most `maxImages`, in `from` order;
  * a model call with images carries them as Bedrock image blocks; one without is
    sent exactly as before (so no existing agent changes);
  * the logs and telemetry name the images, never carry their bytes;
  * an agent with `vision` loads the images once per run, and says so when there are none.
"""
from __future__ import annotations

import asyncio
import json
import sys
import types

import boto3
import pytest

moto = pytest.importorskip("moto")

SID = "abc123def456"
BUCKET = "agentcore-x-assets-1"


@pytest.fixture()
def images(monkeypatch):
    for k, v in {"AWS_ACCESS_KEY_ID": "t", "AWS_SECRET_ACCESS_KEY": "t",
                 "AWS_DEFAULT_REGION": "us-east-1", "AWS_REGION": "us-east-1"}.items():
        monkeypatch.setenv(k, v)
    with moto.mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket=BUCKET)
        sys.modules.pop("app.common.images", None)
        from app.common import images as mod
        mod._clients.clear()
        yield mod, s3


def _out(*keys) -> str:
    """An image agent's output, as scaffold.py IMAGE_REASON writes it."""
    return json.dumps({"summary": "s", "brief": {}, "images": [{"imageKey": k} for k in keys]})


def test_it_reads_only_this_runs_images_in_order_and_at_most_max(images):
    mod, s3 = images
    for k in (f"runs/{SID}/poster/1.png", f"runs/{SID}/poster/2.jpg", f"runs/{SID}/logo/3.png",
              "runs/otherrun0001/poster/9.png", "secrets/x.png"):
        s3.put_object(Bucket=BUCKET, Key=k, Body=b"IMG:" + k.encode())
    outputs = {"poster": _out(f"runs/{SID}/poster/1.png", f"runs/{SID}/poster/2.jpg",
                              "runs/otherrun0001/poster/9.png", "secrets/x.png"),
               "logo": {"images": [{"imageKey": f"runs/{SID}/logo/3.png"}, {"nope": 1}]},
               "writer": "not json at all"}
    got = mod.load_for(outputs, ["logo", "poster", "writer", "missing"], session_id=SID, bucket=BUCKET)
    assert [(i["key"], i["format"]) for i in got] == [
        (f"runs/{SID}/logo/3.png", "png"), (f"runs/{SID}/poster/1.png", "png"),
        (f"runs/{SID}/poster/2.jpg", "jpeg")]
    assert got[0]["bytes"] == f"IMG:runs/{SID}/logo/3.png".encode()
    assert len(mod.load_for(outputs, ["poster", "logo"], session_id=SID, max_images=1, bucket=BUCKET)) == 1
    assert mod.load_for({}, ["poster"], session_id=SID, bucket=BUCKET) == []


def test_a_too_large_image_or_no_bucket_is_an_error_with_the_reason(images):
    mod, s3 = images
    key = f"runs/{SID}/poster/1.png"
    s3.put_object(Bucket=BUCKET, Key=key, Body=b"x" * (mod.IMAGE_MAX_BYTES + 1))
    with pytest.raises(ValueError, match=r"at most 3\.75 MB"):
        mod.load_for({"poster": _out(key)}, ["poster"], session_id=SID, bucket=BUCKET)
    with pytest.raises(RuntimeError, match="no assets bucket"):
        mod.load_for({"poster": _out(key)}, ["poster"], session_id=SID, bucket="")


class _Msg:
    def __init__(self):
        self.content = "Looks right."
        self.response_metadata = {"stopReason": "end_turn"}
        self.usage_metadata = {"input_tokens": 1500, "output_tokens": 5}


def _fake_converse(monkeypatch):
    sent: list = []

    class _FakeLLM:
        def __init__(self, **_kw):
            pass

        async def ainvoke(self, messages):
            sent.append(messages)
            return _Msg()

    fake = types.ModuleType("langchain_aws")
    fake.ChatBedrockConverse = _FakeLLM
    monkeypatch.setitem(sys.modules, "langchain_aws", fake)
    return sent


def test_a_call_with_images_sends_them_as_image_blocks_and_one_without_is_unchanged(monkeypatch):
    from app.common import llm as llm_mod
    sent = _fake_converse(monkeypatch)
    metered: list = []
    monkeypatch.setattr(llm_mod, "_meter_llm", lambda *a, **k: metered.append(a[3]))
    img = [{"key": f"runs/{SID}/poster/1.png", "format": "png", "bytes": b"\x89PNG"}]
    text, _ = asyncio.run(llm_mod.run_llm("check", "sys", "Is it right?", images=img))
    assert text == "Looks right."
    system, human = sent[0]
    assert system.content == "sys"
    assert human.content == [{"type": "text", "text": "Is it right?"},
                             {"image": {"format": "png", "source": {"bytes": b"\x89PNG"}}}]
    # Telemetry names the image; it never carries its bytes.
    assert metered[0] == f"Is it right?\n\n[1 image(s): runs/{SID}/poster/1.png]"
    # No images: the exact messages every agent sent before.
    asyncio.run(llm_mod.run_llm("plain", "sys", "user"))
    assert sent[1] == [("system", "sys"), ("human", "user")] and metered[1] == "user"


def _ctx(vision: dict | None, outputs: dict):
    from app.common.context import AgentContext
    c = AgentContext.__new__(AgentContext)
    c.agent_id, c.model, c.temperature, c.max_tokens = "checker", None, 0, 1000
    c.recalled_memory, c.truncated_calls, c.session_id = [], [], SID
    c.state = {"outputs": outputs}
    if vision is not None:
        c.vision, c._seen_images = vision, None
    c.logged = []

    async def _log(msg):
        c.logged.append(msg)
    c.log = _log
    return c


def test_an_agent_with_vision_reads_the_images_once_and_says_when_there_are_none(monkeypatch):
    from app.common import context as ctx_mod
    from app.common import images as img_mod
    calls, loads = [], []

    async def fake_run_llm(name, system, user, **kw):
        calls.append((user, kw.get("images")))
        return "ok", False

    def fake_load(outputs, sources, *, session_id, max_images):
        loads.append((sources, session_id, max_images))
        return [{"key": "k", "format": "png", "bytes": b"x"}] if outputs else []
    monkeypatch.setattr(ctx_mod, "run_llm", fake_run_llm)
    monkeypatch.setattr(img_mod, "load_for", fake_load)

    c = _ctx({"from": ["poster"], "maxImages": 2}, {"poster": "{}"})
    asyncio.run(c.llm("sys", "check it"))
    asyncio.run(c.llm("sys", "again", name="checker.repair"))
    assert loads == [(["poster"], SID, 2)]                       # once per run of the agent
    assert [len(i) for _, i in calls] == [1, 1] and calls[0][0] == "check it"
    assert c.logged == ["Reading 1 image from poster"]

    calls.clear()
    none = _ctx({"from": ["poster"]}, {})
    asyncio.run(none.llm("sys", "check it"))
    assert calls[0][1] == [] and "No images were available from: poster" in calls[0][0]
    assert none.logged == ["No images from poster to read"]

    # No `vision` (every existing agent): no load, no images, the prompt untouched.
    calls.clear()
    loads.clear()
    plain = _ctx(None, {"poster": "{}"})
    asyncio.run(plain.llm("sys", "user"))
    assert loads == [] and calls == [("user", [])]


def test_the_registry_puts_vision_on_the_agent():
    from app.orchestrator import registry

    class A:
        pass
    a = A()
    registry._configure(a, "checker", {"name": "C", "vision": {"from": ["poster"], "maxImages": 3}})
    assert a.vision == {"from": ["poster"], "maxImages": 3}
    b = A()
    registry._configure(b, "plain", {"name": "P"})
    assert b.vision == {}
