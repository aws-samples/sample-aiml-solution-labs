"""The model picker (GET /api/models) lists only models an agent can actually call.

Probed live against every text model a console account lists, through the framework's
own call (app.common.llm.run_llm). These list TEXT output but cannot answer an agent's
Converse call, so offering them is offering a deploy that fails at the first run.
"""

from __future__ import annotations

import importlib
import json
import sys

import pytest
from conftest import ORCH_ROOT


@pytest.fixture(scope="module")
def handler():
    mp = pytest.MonkeyPatch()
    for k, v in {"WORKFLOW_JSON": json.dumps({"agents": {}, "steps": []}), "STATUS_TABLE": "s",
                 "EVENTS_TABLE": "e", "RUNTIME_ARN": "arn:aws:bedrock-agentcore:us-east-1:1:runtime/x",
                 "AWS_REGION": "us-east-1", "AWS_DEFAULT_REGION": "us-east-1"}.items():
        mp.setenv(k, v)
    mp.syspath_prepend(str(ORCH_ROOT / "bff"))
    for mod in ("workflow", "authz", "chatbot", "handler", "builds", "buildstore", "accounts", "designer"):
        sys.modules.pop(mod, None)
    yield importlib.import_module("handler")
    for mod in ("workflow", "authz", "chatbot", "handler", "builds", "buildstore", "accounts", "designer"):
        sys.modules.pop(mod, None)
    mp.undo()

NOT_CALLABLE = ["amazon.rerank-v1:0", "amazon.nova-2-sonic-v1:0", "twelvelabs.pegasus-1-2-v1:0",
                "writer.palmyra-vision-7b", "amazon.titan-embed-text-v2:0"]
#: From the live tool probe: Bedrock refuses tool use for these, or they take the tools
#: and never call one.
NO_TOOLS = ["us.deepseek.r1-v1:0", "meta.llama3-8b-instruct-v1:0", "meta.llama3-70b-instruct-v1:0",
            "us.meta.llama3-3-70b-instruct-v1:0", "mistral.mistral-7b-instruct-v0:2",
            "mistral.mixtral-8x7b-instruct-v0:1", "google.gemma-3-27b-it", "mistral.magistral-small-2509"]
TOOLS = ["us.meta.llama3-1-70b-instruct-v1:0", "us.anthropic.claude-sonnet-5-5", "openai.gpt-oss-120b-1:0",
         "deepseek.v3.2", "us.xai.grok-4.7", "google.gemma-4", "mistral.mistral-large-3-675b-instruct"]


@pytest.mark.parametrize("model", NO_TOOLS)
def test_a_model_that_cannot_choose_tools_is_marked(model, handler):
    assert handler.NO_TOOL_MODELS.search(model)


@pytest.mark.parametrize("model", TOOLS)
def test_a_model_the_tool_probe_passed_is_not_marked(model, handler):
    assert not handler.NO_TOOL_MODELS.search(model)


CALLABLE = ["amazon.nova-pro-v1:0", "meta.llama3-70b-instruct-v1:0", "openai.gpt-oss-120b-1:0",
            "mistral.mistral-7b-instruct-v0:2", "anthropic.claude-sonnet-5", "deepseek.v3.2",
            "qwen.qwen3-32b-v1:0", "writer.palmyra-x5-v1:0"]


@pytest.mark.parametrize("model", NOT_CALLABLE)
def test_a_model_that_cannot_answer_a_converse_call_is_not_offered(model, handler):
    assert handler.NOT_CHAT_MODELS.search(model)


@pytest.mark.parametrize("model", CALLABLE)
def test_every_chat_model_the_probe_passed_is_offered(model, handler):
    assert not handler.NOT_CHAT_MODELS.search(model)
