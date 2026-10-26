"""AgentExpress Assistant (bff/designer.py): it writes ONLY the custom() function of a
template-generated Gateway interceptor (set_interceptor_custom), sees that function in its
server context, and still can't replace the whole handler. The response handler it leaves
keeps the protocol guard (interceptor_code.guard_problem), so initialize passes untouched.

Scenarios: a library-catalog lookup whose results carry patron email addresses, and a
weather-station feed whose results carry station serials. The model is scripted.
"""
# ruff: noqa: F811 - `env` and `model` are fixtures, imported so every test can take them
from __future__ import annotations

import json

import pytest
from test_builds import env, save  # noqa: F401 - env is the builds fixture, taken by name
from test_designer import BASE, conversation, draft, model, run_background, says, send, tool_use  # noqa: F401
from test_interceptor_protocol import INITIALIZE, event
from test_interceptors import answered, load

pytest.importorskip("moto")

EMAIL_MASK = '''def custom(body: dict, request: dict) -> dict:
    """Mask email addresses in a tool's result."""
    email = re.compile(r"[\\w.+-]+@[\\w-]+(\\.[\\w-]+)+")

    def walk(v):
        if isinstance(v, str):
            return email.sub("[EMAIL]", v)
        if isinstance(v, list):
            return [walk(x) for x in v]
        if isinstance(v, dict):
            return {k: walk(x) for k, x in v.items()}
        return v
    return walk(body)
'''
SERIAL_MASK = '''_SERIAL = re.compile(r"WX-\\d{4,}")


def _mask(v):
    return json.loads(_SERIAL.sub("[SERIAL]", json.dumps(v)))


def custom(body: dict, request: dict) -> dict:
    return _mask(body)
'''
SCENARIOS = {
    "library catalog": (EMAIL_MASK, "Hold for patron jo.reader@example.org, due 2026-04-01.",
                        "jo.reader@example.org", "[EMAIL]"),
    "weather station": (SERIAL_MASK, "Station WX-40917 reports 12.5 C.", "WX-40917", "[SERIAL]"),
}


def ops(e):
    return e.handler.designer.apply_ops


def result_of(call) -> dict:
    return call["messages"][-1]["content"][0]["toolResult"]["content"][0]["json"]


def test_the_guide_and_ops_offer_custom_only(env):
    text = " ".join(env.handler.designer.system_prompt()[0]["text"].split())
    for part in ("write its custom() function with set_interceptor_custom {point, code}",
                 "never ask the user to paste handler.py"):
        assert part in text, part
    assert "- set_interceptor_custom {point, code}: write the custom() function" in env.handler.designer._OPS_DOC


def test_asked_to_mask_emails_it_writes_custom_and_the_handler_keeps_the_guard(env, model):
    import interceptor_code
    save(env)
    # A build with a template response interceptor (audit only), as the tab makes it.
    model(tool_use(("apply_changes", {"summary": "Audit every tool answer", "version_name": "audit",
                                      "ops": [{"op": "set_interceptor", "point": "response",
                                               "templates": {"audit": {}}}]})), says("Added."))
    send(env, "Log every tool answer through the Gateway.")
    run_background(env)
    script = model(tool_use(("apply_changes", {
        "summary": "Mask email addresses in tool results", "version_name": "mask emails",
        "ops": [{"op": "set_interceptor_custom", "point": "response", "code": EMAIL_MASK}]})),
        says("custom() now masks email addresses in tool results."))
    send(env, "Mask email addresses in tool results.")
    run_background(env)
    # The context showed it where custom() stands now.
    context = script.calls[0]["messages"][-1]["content"][0]["text"]
    assert "The response interceptor's custom() function as it is now" in context
    assert result_of(script.calls[1])["status"] == "applied", result_of(script.calls[1])
    project = draft(env)
    ic = project["workflow"]["orchestrator"]["interceptors"]["response"]
    assert set(ic["templates"]) == {"audit", "custom"}            # custom checked in the same change
    source = project["toolCode"]["interceptor-response"]["handler.py"]
    assert interceptor_code.guard_problem("response", source, ic["templates"]) == ""
    assert interceptor_code.custom_function(source).strip() == EMAIL_MASK.strip()
    handler = load(source)
    assert answered(handler(event(INITIALIZE, "initialize"), None))["body"] == INITIALIZE
    answer = {"jsonrpc": "2.0", "id": 4, "result": {"content": [
        {"type": "text", "text": "Hold for patron jo.reader@example.org and a.b@lib.example.com."}]}}
    got = answered(handler(event(answer, "tools/call"), None))["body"]
    assert got["id"] == 4 and got["jsonrpc"] == "2.0"
    assert "@" not in json.dumps(got["result"]) and got["result"]["content"][0]["text"].count("[EMAIL]") == 2
    assert conversation(env)[1]["turns"][-1]["changes"][0]["versionName"] == "mask emails"


@pytest.mark.parametrize("domain", sorted(SCENARIOS))
def test_custom_is_written_alone_and_a_whole_handler_is_still_refused(env, domain):
    import interceptor_code
    code, text, secret, mask = SCENARIOS[domain]
    d = ops(env)
    first, _ = d(BASE, [{"op": "set_interceptor", "point": "response", "templates": {"capResult": {"maxChars": 900}}}])
    done, _ = d(first, [{"op": "set_interceptor_custom", "point": "response", "code": code}])
    source = done["toolCode"]["interceptor-response"]["handler.py"]
    templates = done["workflow"]["orchestrator"]["interceptors"]["response"]["templates"]
    assert interceptor_code.guard_problem("response", source, templates) == ""
    assert "def cap_result(" in source
    handler = load(source)
    assert answered(handler(event(INITIALIZE, "initialize"), None))["body"] == INITIALIZE
    answer = {"jsonrpc": "2.0", "id": 9, "result": {"content": [{"type": "text", "text": text}]}}
    got = json.dumps(answered(handler(event(answer, "tools/call"), None))["body"]["result"])
    assert secret not in got and mask in got
    # Written again: replaced in place, the rest unchanged.
    again, _ = d(done, [{"op": "set_interceptor_custom", "point": "response", "code": EMAIL_MASK}])
    assert interceptor_code.custom_function(again["toolCode"]["interceptor-response"]["handler.py"]).strip() \
        == EMAIL_MASK.strip()
    # A whole handler is still refused, by either op.
    whole = source.replace("def lambda_handler(event, context):", "def lambda_handler(event, context):\n    pass\n\n"
                           "def _unused(event, context):", 1)
    with pytest.raises(env.handler.designer.OpError, match="must keep the generated lambda_handler"):
        d(done, [{"op": "set_tool_code", "key": "interceptor-response", "files": {"handler.py": whole}}])
    for bad, why in [
        (code + "\n\ndef lambda_handler(event, context):\n    return {}\n", "redefines 'lambda_handler'"),
        (code + "\n\ndef _keep_protocol(sent, changed, request):\n    return changed\n", "redefines '_keep_protocol'"),
        ("def custom(body):\n    return body\n", "must keep its signature"),
        ("x = 1\n", "exactly one top-level `def custom"),
    ]:
        with pytest.raises(env.handler.designer.OpError, match=why):
            d(done, [{"op": "set_interceptor_custom", "point": "response", "code": bad}])


def test_a_request_interceptors_custom_is_written_too_and_an_own_function_has_none(env):
    import interceptor_code
    d = ops(env)
    code = ('def custom(body: dict, run: dict) -> str | None:\n'
            '    if "drop table" in json.dumps(body).lower():\n'
            '        return "refused: looks like SQL"\n'
            '    return None\n')
    done, _ = d(BASE, [{"op": "set_interceptor_custom", "point": "request", "code": code}])
    assert done["workflow"]["orchestrator"]["interceptors"]["request"]["templates"] == {"custom": {}}
    assert interceptor_code.custom_function(done["toolCode"]["interceptor-request"]["handler.py"]).strip() \
        == code.strip()
    arn = "arn:aws:lambda:us-east-1:123456789012:function:mine"
    own, _ = d(BASE, [{"op": "set_interceptor", "point": "response", "lambdaArn": arn}])
    with pytest.raises(env.handler.designer.OpError, match="has no generated custom"):
        d(own, [{"op": "set_interceptor_custom", "point": "response", "code": EMAIL_MASK}])
