"""An agent's sampling settings — temperature, topP, stopSequences — reach the model,
and a model that refuses one of them still answers.

Claude Sonnet 5 rejects any temperature ("temperature is deprecated for this model"),
and several Claude 4.x models take a temperature or a top_p but not both. Every agent
carries a temperature (the default is 0), so without the retry, choosing one of those
models for an agent would fail its every call.
"""

from __future__ import annotations

import asyncio
import sys
import types

from conftest import agents, workflow


class _Msg:
    def __init__(self):
        self.content = "ok"
        self.usage_metadata: dict = {}
        self.response_metadata = {"stopReason": "end_turn"}


def _run(monkeypatch, refuse: list[str], **kw):
    """run_llm against a fake model that refuses the named settings; returns the
    settings each attempt was made with."""
    from app.common import llm as llm_mod
    attempts: list[dict] = []

    class _FakeLLM:
        def __init__(self, **k):
            self.k = k
            attempts.append({x: k[x] for x in ("temperature", "top_p", "stop_sequences") if x in k})

        async def ainvoke(self, _m):
            if "temperature" in refuse and "temperature" in self.k:
                if "top_p" in self.k and refuse == ["temperature", "both"]:
                    raise ValueError("ValidationException: `temperature` and `top_p` cannot both "
                                     "be specified for this model. Please use only one.")
                if refuse == ["temperature"]:
                    raise ValueError("ValidationException: The model returned the following "
                                     "errors: `temperature` is deprecated for this model.")
            return _Msg()

    fake = types.ModuleType("langchain_aws")
    fake.ChatBedrockConverse = _FakeLLM
    monkeypatch.setitem(sys.modules, "langchain_aws", fake)
    text, _ = asyncio.run(llm_mod.run_llm("a", "sys", "user", max_tokens=10, **kw))
    assert text == "ok"
    return attempts


def test_settings_reach_the_model(monkeypatch):
    attempts = _run(monkeypatch, [], temperature=0.2, top_p=0.9, stop_sequences=["END"])
    assert attempts == [{"temperature": 0.2, "top_p": 0.9, "stop_sequences": ["END"]}]


def test_a_model_that_refuses_a_temperature_is_called_without_one(monkeypatch):
    attempts = _run(monkeypatch, ["temperature"], temperature=0)
    assert attempts == [{"temperature": 0}, {}]


def test_a_model_that_takes_one_of_temperature_and_top_p_keeps_the_top_p(monkeypatch):
    attempts = _run(monkeypatch, ["temperature", "both"], temperature=0, top_p=0.8)
    assert attempts[-1] == {"top_p": 0.8}


def test_any_other_error_still_fails_the_call(monkeypatch):
    from app.common import llm as llm_mod
    from app.common.errors import ModelUnavailable

    class _Down:
        def __init__(self, **_k):
            pass

        async def ainvoke(self, _m):
            raise RuntimeError("AccessDeniedException")

    fake = types.ModuleType("langchain_aws")
    fake.ChatBedrockConverse = _Down
    monkeypatch.setitem(sys.modules, "langchain_aws", fake)
    try:
        asyncio.run(llm_mod.run_llm("a", "sys", "user"))
    except ModelUnavailable as e:
        assert "AccessDeniedException" in str(e)
    else:
        raise AssertionError("a real failure was swallowed")


def test_the_registry_reads_them_from_workflow_json():
    defn = {"orchestrator": {}, "tools": {},
            "agents": agents("a", topP=0.7, stopSequences=["###"], temperature=0.3),
            "steps": [{"agent": "a"}]}
    with workflow(defn) as imp:
        agent = imp("app.orchestrator.registry").load_agents()["a"]
        assert (agent.top_p, agent.stop_sequences, agent.temperature) == (0.7, ["###"], 0.3)


def _run_refusing(monkeypatch, refusal, **kw):
    """run_llm against a fake model that refuses once with `refusal`, then answers;
    returns what each attempt sent."""
    from app.common import llm as llm_mod
    attempts: list[dict] = []

    class _FakeLLM:
        def __init__(self, **k):
            self.k = k

        async def ainvoke(self, messages):
            attempts.append({"max_tokens": self.k["max_tokens"], "roles": [r for r, _ in messages],
                             "temperature" in self.k: True})
            if len(attempts) == 1:
                raise ValueError(refusal)
            return _Msg()

    fake = types.ModuleType("langchain_aws")
    fake.ChatBedrockConverse = _FakeLLM
    monkeypatch.setitem(sys.modules, "langchain_aws", fake)
    asyncio.run(llm_mod.run_llm("a", "sys", "user", **kw))
    return attempts


def test_a_budget_over_the_models_ceiling_is_retried_at_the_ceiling(monkeypatch):
    # Llama 3 70B/8B: "The maximum tokens you requested exceeds the model limit of 2048."
    a = _run_refusing(monkeypatch, "ValidationException: The maximum tokens you requested "
                      "exceeds the model limit of 2048. Try again with a maximum tokens value "
                      "that is lower than 2048.", max_tokens=4000)
    assert [x["max_tokens"] for x in a] == [4000, 2048]


def test_a_model_without_system_messages_gets_its_instructions_in_the_user_turn(monkeypatch):
    # Mistral 7B Instruct and Mixtral 8x7B.
    a = _run_refusing(monkeypatch, "ValidationException: This model doesn't support system "
                      "messages. Try again without a system message.", max_tokens=100)
    assert [x["roles"] for x in a] == [["system", "human"], ["human"]]


def test_the_other_temperature_wording_is_understood_too(monkeypatch):
    # OpenAI GPT-5.4 / GPT-6, xAI Grok 4.6, Kimi K3.
    a = _run_refusing(monkeypatch, "ValidationException: This model doesn't support the "
                      "temperature field. Remove temperature and try again.", max_tokens=100)
    assert [x.get(True) for x in a] == [True, None]
