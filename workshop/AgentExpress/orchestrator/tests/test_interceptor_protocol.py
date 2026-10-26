"""A Gateway response interceptor changes a tool's result and never an MCP protocol field.

Seen on a live run: the Assistant wrote a response interceptor that masked every string
matching its account and phone patterns, in every answer. The MCP initialize result's
protocolVersion ("2025-03-26") matched, came back "[REDACTED]", the agents' MCP client
could not initialize, and every run failed "Could not list tools on the Gateway". The
code check and the sample tools/list test passed, because neither looks at initialize.

Now the generated response handler always keeps a protocol guard (response.py
`_keep_protocol`): whatever its steps do (custom included), only a tools/call answer's
result changes and only which tools tools/list lists; initialize and everything else is
delivered as the Gateway sent it. The Assistant may fill in the step functions but not
drop or alter the guard (designer set_tool_code, interceptor_code.guard_problem). A build
saved before the guard existed is still taken for generated code, and gets it on its next
change.

Masking scenarios are from two unrelated domains: invoice processing (bank account
numbers) and staff onboarding (every number in an employee record).
"""
from __future__ import annotations

import copy
import json
import re
from pathlib import Path

import interceptor_code  # bff/ is on sys.path (conftest)
import pytest
from test_interceptors import answered, load, res

ORCH = Path(__file__).resolve().parent.parent
LEGACY = json.loads((ORCH / "tests" / "fixtures" / "interceptor_legacy_cases.json").read_text())["cases"]
WF = {"agents": {"intake": {"name": "Intake"}}, "steps": [{"agent": "intake"}],
      "tools": {"ledger": {"type": "lambda", "toolSchema": [{"name": "lookup"}], "code": {}}}}

INITIALIZE = {"jsonrpc": "2.0", "id": 0, "result": {
    "protocolVersion": "2025-03-26", "capabilities": {"tools": {"listChanged": False}},
    "serverInfo": {"name": "gateway-1234567890", "version": "1.0.0"}}}
TOOLS_LIST = {"jsonrpc": "2.0", "id": 1, "result": {"tools": [
    {"name": "ledger___lookup", "description": "Look up account 1234-5678-9012 for 2025-03-26",
     "inputSchema": {"type": "object", "properties": {"account": {"type": "string"}}}},
    {"name": "hr___record", "description": "Employee record 55501", "inputSchema": {"type": "object"}}]},
    "nextCursor": "2025-03-26"}

CUSTOM_STUB = ('def custom(body: dict, request: dict) -> dict:\n'
               '    """Your own change to the answer: return the body to deliver."""\n'
               '    return body\n')

# What an Assistant fills custom() in with, per domain: deliberately careless masks that
# walk the WHOLE body, keys and numbers too, as the live one did.
INVOICE_MASK = '''_ACCOUNT = re.compile(r"\\d[\\d -]{6,}\\d")


def _mask(value):
    if isinstance(value, str):
        return _ACCOUNT.sub("[ACCOUNT]", value)
    if isinstance(value, list):
        return [_mask(v) for v in value]
    if isinstance(value, dict):
        return {_mask(k): _mask(v) for k, v in value.items()}
    return value


def custom(body: dict, request: dict) -> dict:
    """Mask bank account numbers."""
    return _mask(body)
'''
ONBOARDING_MASK = '''def _numbers(value):
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, (int, float)):
        return "[NUMBER]"
    if isinstance(value, str):
        return re.sub(r"\\d+", "#", value)
    if isinstance(value, list):
        return [_numbers(v) for v in value]
    if isinstance(value, dict):
        return {k: _numbers(v) for k, v in value.items()}
    return value


def custom(body: dict, request: dict) -> dict:
    """Hide every number in an employee record."""
    body = _numbers(body)
    body["jsonrpc"] = "masked"
    return body
'''
SCENARIOS = {
    "invoice": (INVOICE_MASK, "Pay to account 1234-5678-9012 by Friday.", "[ACCOUNT]", "1234-5678-9012"),
    "onboarding": (ONBOARDING_MASK, "Employee 55501 starts on desk 42.", "#", "55501"),
}


def generated(templates: dict) -> str:
    return interceptor_code.files("response", templates, WF)["handler.py"]


def with_custom(templates: dict, code: str) -> str:
    """The generated handler with custom() filled in, as the Assistant is told to do."""
    source = generated({**templates, "custom": {}})
    assert CUSTOM_STUB in source
    if "import re\n" not in source:
        source = source.replace("import json\n", "import json\nimport re\n", 1)
    return source.replace(CUSTOM_STUB, code)


def event(body: dict, method: str) -> dict:
    e = res(None, method=method, tool="ledger___lookup")
    e["mcp"]["gatewayResponse"]["body"] = copy.deepcopy(body)
    return e


# --- the guard holds whatever the steps do -----------------------------------------------

@pytest.mark.parametrize("domain", sorted(SCENARIOS))
@pytest.mark.parametrize("templates", [{}, {"redactPii": {}}, {"audit": {}, "redactPii": {}, "capResult": {}}],
                         ids=["custom only", "custom+redact", "custom+audit+redact+cap"])
def test_initialize_and_tools_list_pass_untouched_while_the_tool_result_is_masked(domain, templates):
    code, secret_text, mask, secret = SCENARIOS[domain]
    handler = load(with_custom(templates, code))
    # initialize: protocolVersion, capabilities and serverInfo exactly as sent.
    out = answered(handler(event(INITIALIZE, "initialize"), None))
    assert out == {"statusCode": 200, "body": INITIALIZE}
    # tools/list: every tool, name, description and schema as sent, and the cursor.
    out = answered(handler(event(TOOLS_LIST, "tools/list"), None))
    assert out["body"] == TOOLS_LIST
    # tools/call: the tool's own result is masked; jsonrpc and id are not.
    answer = {"jsonrpc": "2.0", "id": 7, "result": {"content": [{"type": "text", "text": secret_text}]}}
    out = answered(handler(event(answer, "tools/call"), None))
    text = json.dumps(out["body"]["result"])
    assert secret not in text and mask in text
    assert out["body"]["jsonrpc"] == "2.0" and out["body"]["id"] == 7


@pytest.mark.parametrize("domain", sorted(SCENARIOS))
def test_a_step_that_rewrites_the_requests_method_cannot_unlock_initialize(domain):
    """The guard decides by the request as the Gateway sent it, not as a step left it: a
    step that sets the method to tools/call (or empties it) still cannot change initialize."""
    code, secret_text, mask, secret = SCENARIOS[domain]
    sneaky = code.replace('def custom(body: dict, request: dict) -> dict:\n',
                          'def custom(body: dict, request: dict) -> dict:\n'
                          '    request["method"] = "tools/call"\n    request.pop("id", None)\n', 1)
    assert sneaky != code
    handler = load(with_custom({"redactPii": {}}, sneaky))
    assert answered(handler(event(INITIALIZE, "initialize"), None)) == {"statusCode": 200, "body": INITIALIZE}
    assert answered(handler(event(TOOLS_LIST, "tools/list"), None))["body"] == TOOLS_LIST
    answer = {"jsonrpc": "2.0", "id": 7, "result": {"content": [{"type": "text", "text": secret_text}]}}
    text = json.dumps(answered(handler(event(answer, "tools/call"), None))["body"]["result"])
    assert secret not in text and mask in text


@pytest.mark.parametrize("domain", sorted(SCENARIOS))
def test_a_hand_written_handler_that_masks_everything_is_refused_for_the_assistant(domain):
    """The live failure: a whole handler of the Assistant's own, with no guard."""
    code, *_ = SCENARIOS[domain]
    whole = ("import json\nimport re\n\n" + code + "\n\ndef lambda_handler(event, context):\n"
             "    mcp = event.get('mcp') or {}\n    resp = mcp.get('gatewayResponse') or {}\n"
             "    return {'interceptorOutputVersion': '1.0', 'mcp': {'transformedGatewayResponse': "
             "{'statusCode': resp.get('statusCode', 200), 'body': custom(resp.get('body'), {})}}}\n")
    # It really does break initialize...
    out = answered(load(whole)(event(INITIALIZE, "initialize"), None))
    assert out["body"]["result"]["protocolVersion"] != "2025-03-26" or out["body"]["id"] != 0
    # ...so the Assistant may not install it.
    why = interceptor_code.guard_problem("response", whole, {"custom": {}})
    assert "must keep the generated lambda_handler() exactly as it is" in why
    # Filling in custom() alone is accepted.
    assert interceptor_code.guard_problem("response", with_custom({}, code), {"custom": {}}) == ""


@pytest.mark.parametrize("edit,why", [
    (lambda s: s.replace("body = _keep_protocol(sent, body, asked)", "pass"), "lambda_handler"),
    (lambda s: s.replace("body = _keep_protocol(sent, body, asked)", "body = _keep_protocol(sent, body, request)"),
     "lambda_handler"),
    (lambda s: s.replace('if method == "tools/call":', 'if True:'), "_keep_protocol"),
    (lambda s: s + "\n\ndef _keep_protocol(sent, changed, request):\n    return changed\n", "_keep_protocol"),
    (lambda s: s + "\n_keep_protocol = lambda sent, changed, request: changed\n", "rebinds '_keep_protocol'"),
    (lambda s: s + "\nimport json as copy\n", "rebinds 'copy'"),
    (lambda s: s.replace("def lambda_handler(", "def lambda_handler_(", 1), "lambda_handler"),
])
def test_any_change_to_the_guard_is_refused(edit, why):
    source = with_custom({}, INVOICE_MASK)
    assert why in interceptor_code.guard_problem("response", edit(source), {"custom": {}})


def test_every_generated_response_handler_has_the_guard_and_request_handlers_are_unchanged():
    for templates in ({}, {"audit": {}}, {"redactPii": {}}, {"hideTools": {"tools": ["hr"]}}, {"capResult": {}},
                      {"audit": {}, "redactPii": {}, "hideTools": {"tools": ["hr"]}, "capResult": {}, "custom": {}}):
        source = generated(templates)
        assert "def _keep_protocol(" in source and "body = _keep_protocol(sent, body, asked)" in source
        assert "sent, asked = copy.deepcopy(body), copy.deepcopy(request)" in source
        assert interceptor_code.guard_problem("response", source, templates) == ""
        handler = load(source)
        assert answered(handler(event(INITIALIZE, "initialize"), None))["body"] == INITIALIZE
    assert interceptor_code.guard_problem("request", "anything at all", {}) == ""


def test_hide_tools_still_drops_a_tool_from_tools_list_and_nothing_else():
    handler = load(generated({"hideTools": {"tools": ["hr"]}}))
    out = answered(handler(event(TOOLS_LIST, "tools/list"), None))["body"]
    assert [t["name"] for t in out["result"]["tools"]] == ["ledger___lookup"]
    assert out["result"]["tools"][0] == TOOLS_LIST["result"]["tools"][0] and out["nextCursor"] == "2025-03-26"


# --- builds saved before the guard ----------------------------------------------------------

@pytest.mark.parametrize("case", LEGACY, ids=[c["name"] for c in LEGACY])
def test_a_handler_generated_before_the_guard_is_still_taken_for_generated_code(case):
    assert case["handler"] != interceptor_code.files(case["point"], case["templates"], WF)["handler.py"]
    assert interceptor_code.is_generated(case["point"], case["templates"], case["handler"])
    assert not interceptor_code.is_generated(case["point"], case["templates"], case["handler"] + "# mine\n")
    # And it still runs as it did: initialize untouched (its built-in steps never touched it).
    assert answered(load(case["handler"])(event(INITIALIZE, "initialize"), None))["body"] == INITIALIZE


def test_the_page_and_the_server_keep_the_same_sections():
    ts = (ORCH / "web" / "src" / "builder" / "interceptorCode.ts").read_text()
    assert re.search(r'const ALWAYS = \["protocol"\];', ts)
    assert interceptor_code.ALWAYS == ["protocol"]
