"""Gateway interceptors written in the build (orchestrator.interceptors.<point>.code).

What has to hold:
  * the code the Builder generates RUNS: each case in tests/fixtures/interceptor_cases.json
    (written by web/src/builder/interceptorCode.test.ts from the real generator) is executed
    on Gateway payloads, and refuses, rewrites, redacts or passes through as its checked
    templates say — and returns the Gateway's output shape every time;
  * an interceptor's files travel with the build like a code tool's: in the bundle, to
    app/tools/_code/interceptor-<point>/, checked before a deploy;
  * the runtime tells the interceptor which run is calling (x-ax-* headers), and a
    refusal reaches the agent as a refusal, not as an outage.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

import codecheck  # bff/ is on sys.path (conftest)
import pytest
from test_scaffold_apply import bundle, claims_workflow, run
from test_scaffold_apply import tree as tree  # the temp framework fixture

ORCH = Path(__file__).resolve().parent.parent
CASES = {c["name"]: c for c in json.loads(
    (ORCH / "tests" / "fixtures" / "interceptor_cases.json").read_text())["cases"]}
TEMPLATES = ORCH / "web" / "src" / "builder" / "interceptor-templates"


def load(source: str):
    """The handler module of a generated interceptor."""
    ns: dict = {"__name__": "interceptor"}
    exec(compile(source, "handler.py", "exec"), ns)  # noqa: S102 - the code under test
    return ns["lambda_handler"]


def case(name: str):
    return load(CASES[name]["handler"])


HEADERS = {"x-ax-session": "s-1", "x-ax-agent": "intake", "x-ax-user": "ana@example.com",
           "Authorization": "Bearer secret-token"}


def req(method="tools/call", tool="refunds___issueRefund", args=None, headers=HEADERS):
    body = {"jsonrpc": "2.0", "id": 7, "method": method}
    if method == "tools/call":
        body["params"] = {"name": tool, "arguments": {"amount": 20} if args is None else args}
    gr = {"path": "/mcp", "httpMethod": "POST", "body": body}
    if headers is not None:
        gr["headers"] = headers
    return {"interceptorInputVersion": "1.0", "mcp": {"rawGatewayRequest": {"body": json.dumps(body)},
                                                       "gatewayRequest": gr}}


def res(result, method="tools/call", tool="refunds___issueRefund", status=200):
    event = req(method, tool)
    event["mcp"]["gatewayResponse"] = {"statusCode": status, "body": {"jsonrpc": "2.0", "id": 7, "result": result}}
    return event


def passed(out):
    assert out["interceptorOutputVersion"] == "1.0"
    return out["mcp"]["transformedGatewayRequest"]["body"]


def refused(out):
    assert out["interceptorOutputVersion"] == "1.0" and "transformedGatewayRequest" not in out["mcp"]
    answer = out["mcp"]["transformedGatewayResponse"]
    assert answer["statusCode"] == 200 and answer["body"]["id"] == 7
    return answer["body"]["error"]["message"]


def answered(out):
    assert out["interceptorOutputVersion"] == "1.0"
    return out["mcp"]["transformedGatewayResponse"]


# --- the shipped templates, every section on ---------------------------------------------

@pytest.mark.parametrize("point", ["request", "response"])
def test_the_full_templates_load_and_pass_the_static_checks(point):
    source = (TEMPLATES / f"{point}.py").read_text()
    load(source)
    errors = [p for p in codecheck.static({"handler.py": source}, []) if p["severity"] == "error"]
    assert errors == []


@pytest.mark.parametrize("name", sorted(CASES))
def test_every_generated_case_passes_the_static_checks(name):
    problems = codecheck.static({"handler.py": CASES[name]["handler"]}, [])
    assert [p for p in problems if p["severity"] == "error"] == []
    assert "# >>>" not in CASES[name]["handler"] and "# <<<" not in CASES[name]["handler"]


# --- request -----------------------------------------------------------------------------

def test_with_nothing_checked_a_request_passes_unchanged():
    event = req()
    assert passed(case("request: nothing checked")(event, None)) == event["mcp"]["gatewayRequest"]["body"]


def test_block_a_tool_refuses_only_that_tool():
    h = case("request: block a tool")
    assert "refunds___issueRefund is blocked" in refused(h(req(), None))
    assert passed(h(req(tool="refunds___status"), None))["params"]["name"] == "refunds___status"
    assert passed(h(req("tools/list"), None))["method"] == "tools/list"
    assert passed(h(req("initialize"), None))["method"] == "initialize"


def test_block_for_listed_agents_and_refuse_a_call_that_does_not_say_who():
    h = case("request: block for one agent")
    assert "blocked for agent intake" in refused(h(req(tool="refunds___any"), None))
    other = {**HEADERS, "x-ax-agent": "report"}
    assert passed(h(req(headers=other), None))
    assert "does not say which agent" in refused(h(req(headers=None), None))


def test_argument_guard_refuses_patterns_and_size():
    h = case("request: argument guard")
    assert "denied pattern" in refused(h(req(args={"q": "x; DROP  TABLE users"}), None))
    assert "over the limit of 50" in refused(h(req(args={"q": "y" * 60}), None))
    assert passed(h(req(args={"q": "fine"}), None))


def test_inject_context_overwrites_what_the_model_sent():
    h = case("request: inject context")
    body = passed(h(req(args={"tenant": "someone-else", "q": 1}), None))
    assert body["params"]["arguments"] == {"tenant": "ana@example.com", "runId": "s-1", "q": 1}
    # Without the headers there is nothing to set, and the model's claim is removed.
    body = passed(h(req(args={"tenant": "someone-else"}, headers=None), None))
    assert body["params"]["arguments"] == {}


def test_audit_logs_the_decision_but_never_values_or_headers(caplog):
    h = case("request: everything")
    with caplog.at_level(logging.INFO):
        refused(h(req(), None))
        passed(h(req(tool="refunds___status", args={"card": "4111111111111111"}), None))
    lines = [json.loads(r.getMessage()) for r in caplog.records if r.getMessage().startswith("{")]
    assert [ln["decision"] for ln in lines] == ["refused", "allowed"]
    assert lines[1]["arguments"] == ["card", "runId"] and lines[1]["agent"] == "intake"
    text = caplog.text
    assert "4111111111111111" not in text and "secret-token" not in text


# --- response ----------------------------------------------------------------------------

def test_with_nothing_checked_an_answer_passes_unchanged():
    event = res({"content": [{"type": "text", "text": "hello"}]})
    out = answered(case("response: nothing checked")(event, None))
    assert out == {"statusCode": 200, "body": event["mcp"]["gatewayResponse"]["body"]}


def test_redact_masks_only_the_types_checked():
    h = case("response: redact")
    text = ("Mail jane.doe@example.com, call 555-123-4567, card 4111 1111 1111 1111, "
            "order 1234567890123456.")
    out = answered(h(res({"content": [{"type": "text", "text": text}]}), None))
    got = out["body"]["result"]["content"][0]["text"]
    assert "jane.doe" not in got and "4111" not in got
    assert "555-123-4567" in got                      # phone is not checked in this case
    assert "1234567890123456" in got                  # fails the Luhn check: not a card
    assert got.count("***") == 2


def test_redact_everything_by_default():
    out = answered(case("response: everything")(res({"content": [{"type": "text",
        "text": "ssn 123-45-6789 phone (555) 123-4567"}]}), None))
    assert out["body"]["result"]["content"][0]["text"] == "ssn [REDACTED] phone [REDACTED]"


def test_hide_tools_filters_tools_list_only():
    h = case("response: hide a tool")
    listed = {"tools": [{"name": "refunds___issueRefund"}, {"name": "refunds___status"}, {"name": "kb___retrieve"}]}
    out = answered(h(res(listed, method="tools/list"), None))
    assert [t["name"] for t in out["body"]["result"]["tools"]] == ["kb___retrieve"]
    call = {"content": [{"type": "text", "text": "x"}]}
    assert answered(h(res(call), None))["body"]["result"] == call


def test_cap_cuts_long_text_and_says_so():
    out = answered(case("response: cap")(res({"content": [{"type": "text", "text": "z" * 150}]}), None))
    text = out["body"]["result"]["content"][0]["text"]
    assert text.startswith("z" * 100) and "50 more characters cut" in text


def test_a_streamed_later_event_carries_no_status():
    event = res({"content": []})
    del event["mcp"]["gatewayResponse"]["statusCode"]
    out = answered(case("response: everything")(event, None))
    assert "statusCode" not in out


def test_a_refusal_answer_passes_through_the_response_side():
    event = req()
    event["mcp"]["gatewayResponse"] = {"statusCode": 200, "body": {"jsonrpc": "2.0", "id": 7,
                                       "error": {"code": -32001, "message": "Refused by interceptor: no"}}}
    out = answered(case("response: everything")(event, None))
    assert out["body"]["error"]["message"] == "Refused by interceptor: no"


# --- the files' journey ------------------------------------------------------------------

def _wf(**ics):
    wf = claims_workflow()
    wf["tools"] = {"refunds": {"type": "lambda", "description": "R.", "lambdaArn":
                               "arn:aws:lambda:us-east-1:123456789012:function:r", "toolSchema": [{"name": "x"}]}}
    wf.setdefault("orchestrator", {})["interceptors"] = ics
    return wf


def test_apply_writes_an_interceptor_where_the_iac_reads_it(tree, tmp_path):
    handler = CASES["request: block a tool"]["handler"]
    wf = _wf(request={"code": {}, "templates": {"blockTools": {"tools": ["refunds"]}}},
             response={"lambdaArn": "arn:aws:lambda:us-east-1:123456789012:function:mine"})
    r = run(tree, str(bundle(tmp_path, wf, toolCode={"interceptor-request": {"handler.py": handler},
                                                    "interceptor-response": {"handler.py": "x"}})), "--exact")
    assert r.returncode == 0, r.stderr
    code = tree / "app" / "tools" / "_code"
    assert (code / "interceptor-request" / "handler.py").read_text() == handler
    # The response side is yours (lambdaArn): nothing is written for it.
    assert not (code / "interceptor-response").exists()


def test_apply_refuses_an_interceptor_without_its_handler(tree, tmp_path):
    r = run(tree, str(bundle(tmp_path, _wf(response={"code": {}}))), "--exact")
    assert r.returncode == 2 and "orchestrator.interceptors.response is written in the build" in r.stderr


def test_the_bundle_and_the_deploy_check_include_interceptors():
    import builds
    import buildstore
    wf = _wf(request={"code": {}})
    p = {"id": "p1", "name": "n", "workflow": wf, "prompts": {},
         "toolCode": {"interceptor-request": {"handler.py": "def lambda_handler(event, context)\n"},
                      "interceptor-response": {"handler.py": "x"}}}
    assert list(buildstore.bundle_of(p, 1)["toolCode"]) == ["interceptor-request"]
    assert buildstore.code_functions(wf) == ["interceptor-request"]
    errors = builds.code_errors(p)
    assert errors and errors[0].startswith("orchestrator.interceptors.request handler.py")


# --- the runtime's side ------------------------------------------------------------------

def test_the_runtime_names_the_run_in_headers(monkeypatch):
    from app.features.gateway import client
    from app.features.observability.scope import set_scope
    set_scope("sess-9", "intake", "ana@example.com")
    assert client._run_headers() == {"x-ax-session": "sess-9", "x-ax-agent": "intake",
                                     "x-ax-user": "ana@example.com"}
    set_scope("", "", "")
    assert client._run_headers() == {}


def test_an_interceptor_refusal_is_a_denial_not_an_outage():
    import asyncio

    from app.common.errors import ToolDenied
    from app.features.gateway import client

    class Tool:
        name = "refunds___issueRefund"

        async def ainvoke(self, args):
            raise RuntimeError("McpError: Refused by interceptor: refunds___issueRefund is blocked in this build")

    with pytest.raises(ToolDenied) as got:
        asyncio.run(client._invoke(Tool(), {"amount": 1}))
    assert got.value.by == "interceptor" and "blocked in this build" in str(got.value)


def test_the_library_takes_an_interceptor_with_its_files(monkeypatch):
    import library

    class Err(Exception):
        def __init__(self, status, msg):
            super().__init__(msg)
    monkeypatch.setattr(library, "_t", lambda: (None, Err))
    files = {"handler.py": CASES["response: redact"]["handler"]}
    spec = {"point": "response", "code": {}, "templates": {"redactPii": {}}}
    assert library._check("interceptor", "piiRedactor", spec, files) == ("piiRedactor", spec)
    with pytest.raises(Err, match="point"):
        library._check("interceptor", "x", {"code": {}}, files)
    with pytest.raises(Err, match=r"handler\.py"):
        library._check("interceptor", "x", spec, None)
    with pytest.raises(Err, match="lambdaArn"):
        library._check("interceptor", "x", {"point": "request", "lambdaArn": "nope"}, None)
    assert library._check("interceptor", "mine", {"point": "request", "lambdaArn":
                          "arn:aws:lambda:us-east-1:123456789012:function:f"}, None)[0] == "mine"
