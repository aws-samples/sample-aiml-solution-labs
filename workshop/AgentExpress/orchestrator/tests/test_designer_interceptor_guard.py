"""AgentExpress Assistant (bff/designer.py): a response interceptor it writes keeps MCP protocol
fields as the Gateway sent them.

The guide says what a response interceptor may change (a tool's result only) and the
server enforces it: set_tool_code on interceptor-response is refused when handler.py does
not keep the generated lambda_handler and _keep_protocol (tests/test_interceptor_protocol.py
runs the code). A build saved before the guard existed is not taken for hand-edited code,
so its next template change regenerates it with the guard.

Scenarios: invoice processing and staff onboarding masks (from test_interceptor_protocol).
"""
# ruff: noqa: F811 - `env` is the builds fixture, imported so every test can take it
from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest
from test_builds import env  # noqa: F401 - env is the builds fixture, taken by name
from test_designer import BASE
from test_interceptor_protocol import CUSTOM_STUB, INITIALIZE, SCENARIOS, event
from test_interceptors import answered, load

pytest.importorskip("moto")
ORCH = Path(__file__).resolve().parent.parent
LEGACY = {c["name"]: c for c in json.loads(
    (ORCH / "tests" / "fixtures" / "interceptor_legacy_cases.json").read_text())["cases"]}


def ops(e):
    return e.handler.designer.apply_ops


def test_the_guide_says_mask_only_a_tools_result(env):
    text = " ".join(env.handler.designer.system_prompt()[0]["text"].split())
    for part in ("A response interceptor sees EVERY MCP answer (initialize, tools/list, tools/call)",
                 "Mask, cut or rewrite only what a tools/call answer returns in `result`",
                 ("never a JSON-RPC or MCP protocol field (jsonrpc, id, method, protocolVersion, capabilities, "
                  "serverInfo, tool names and schemas in tools/list)"),
                 "never write a whole response handler of your own: change only its step functions"):
        assert part in text, part


@pytest.mark.parametrize("domain", sorted(SCENARIOS))
def test_the_assistant_fills_in_custom_but_cannot_replace_the_response_handler(env, domain):
    code, secret_text, mask, secret = SCENARIOS[domain]
    d = ops(env)
    first, _ = d(BASE, [{"op": "set_interceptor", "point": "response", "templates": {"custom": {}}}])
    generated = first["toolCode"]["interceptor-response"]["handler.py"]
    # A whole handler of its own, masking every answer: refused, with how to do it instead.
    whole = "import json\nimport re\n\n" + code + (
        "\n\ndef lambda_handler(event, context):\n    resp = (event.get('mcp') or {}).get('gatewayResponse') or {}\n"
        "    return {'interceptorOutputVersion': '1.0', 'mcp': {'transformedGatewayResponse': "
        "{'body': custom(resp.get('body'), {})}}}\n")
    with pytest.raises(env.handler.designer.OpError, match="must keep the generated lambda_handler"):
        d(first, [{"op": "set_tool_code", "key": "interceptor-response", "files": {"handler.py": whole}}])
    # custom() filled in, the rest kept: applied, and the deployed code keeps initialize whole.
    filled = generated.replace("import json\n", "import json\nimport re\n", 1).replace(CUSTOM_STUB, code)
    done, _ = d(first, [{"op": "set_tool_code", "key": "interceptor-response", "files": {"handler.py": filled}}])
    handler = load(done["toolCode"]["interceptor-response"]["handler.py"])
    assert answered(handler(event(INITIALIZE, "initialize"), None))["body"] == INITIALIZE
    answer = {"jsonrpc": "2.0", "id": 7, "result": {"content": [{"type": "text", "text": secret_text}]}}
    got = json.dumps(answered(handler(event(answer, "tools/call"), None))["body"]["result"])
    assert secret not in got and mask in got


def test_a_request_interceptors_code_is_not_held_to_the_response_guard(env):
    d = ops(env)
    first, _ = d(BASE, [{"op": "set_interceptor", "point": "request", "templates": {"custom": {}}}])
    mine = first["toolCode"]["interceptor-request"]["handler.py"] + "\n# reviewed\n"
    done, _ = d(first, [{"op": "set_tool_code", "key": "interceptor-request", "files": {"handler.py": mine}}])
    assert done["toolCode"]["interceptor-request"]["handler.py"] == mine


def test_a_build_saved_before_the_guard_is_regenerated_with_it_on_its_next_change(env):
    legacy = LEGACY["response: redact"]
    base = copy.deepcopy(BASE)
    base["workflow"].setdefault("orchestrator", {})["interceptors"] = {
        "response": {"code": {}, "templates": legacy["templates"]}}
    base["toolCode"] = {**(base.get("toolCode") or {}),
                        "interceptor-response": {"handler.py": legacy["handler"], "requirements.txt": ""}}
    new, changed = ops(env)(base, [{"op": "set_interceptor", "point": "response",
                                    "templates": {**legacy["templates"], "capResult": {"maxChars": 900}}}])
    handler = new["toolCode"]["interceptor-response"]["handler.py"]
    assert "def _keep_protocol(" in handler and "def cap_result(" in handler
    assert not changed.get("notes")                  # not reported as edited by hand
