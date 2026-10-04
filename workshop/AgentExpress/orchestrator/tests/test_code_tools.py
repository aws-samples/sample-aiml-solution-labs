"""A Lambda tool written in the build (tools.<key>.code): bff/codecheck.py, the files'
journey (bundle -> scaffold.py apply -> the runner's pip install), and the BFF routes.

What has to hold:
  * a deploy is refused while the code has an error (no handler, a syntax error, a
    written-in key, a requirement from a URL); warnings never block;
  * the sandbox runs each test event and reports what came back, and a requirement it
    lacks is "installed at deploy", not a failure;
  * the files travel with the build and land in app/tools/_code/<key>/, nowhere else,
    and a tool no longer written in the build loses its folder;
  * every change to the code is in the owner's log, and Test tool calls the deployed
    function of the caller's own build only.
"""
from __future__ import annotations

import io
import json
import sys

import codecheck  # bff/ is on sys.path (conftest)
import pytest
from test_scaffold_apply import bundle, claims_workflow, run
from test_scaffold_apply import tree as tree  # the temp framework fixture

moto = pytest.importorskip("moto")
from test_builds import call, mark_deployed, project  # noqa: E402
from test_builds import env as env  # noqa: E402

SCHEMA = [{"name": "issueRefund", "description": "Refund an order.",
           "properties": {"amount": {"type": "integer", "required": True, "description": "USD."}}}]
HANDLER = "def lambda_handler(event, context):\n    return {'refunded': event['amount']}\n"
FILES = {"handler.py": HANDLER, "requirements.txt": "requests==2.32.4\n",
         "events.json": json.dumps([{"name": "twenty", "tool": "issueRefund", "event": {"amount": 20}}])}
TOOL = {"type": "lambda", "description": "Refunds.", "toolSchema": SCHEMA, "arg": "amount",
        "code": {"grants": {"table": "orders"}}}


# --- codecheck.static ---------------------------------------------------------------------

def test_good_code_has_no_errors():
    assert [p for p in codecheck.static(FILES, ["issueRefund"]) if p["severity"] == "error"] == []


@pytest.mark.parametrize("files, fragment", [
    ({}, "is missing"),
    ({"handler.py": "def lambda_handler(event, context)\n    return 1\n"}, "does not parse"),
    ({"handler.py": "def handler(event, context):\n    return 1\n"}, "no top-level `def lambda_handler"),
    ({"handler.py": "async def lambda_handler(event, context):\n    return 1\n"}, "must not be async"),
    ({"handler.py": "def lambda_handler(event):\n    return 1\n"}, "exactly (event, context)"),
    ({"handler.py": HANDLER + "K = 'AKIA" + "ABCDEFGHIJKLMNOP'\n"}, "an AWS access key"),
    ({"handler.py": HANDLER + "P = '''-----BEGIN RSA PRIVATE KEY-----'''\n"}, "a private key"),
    ({"handler.py": HANDLER, "requirements.txt": "git+https://github.com/x/y.git\n"}, "only `name==version`"),
    ({"handler.py": HANDLER, "requirements.txt": "--index-url https://evil.example.com\n"}, "only `name==version`"),
    ({"handler.py": HANDLER, "requirements.txt": "-e .\n"}, "only `name==version`"),
    ({"handler.py": HANDLER, "../escape.py": ""}, "not a file a code tool may hold"),
    ({"handler.py": HANDLER, "events.json": "{}"}, "must be a list of test events"),
])
def test_what_would_fail_or_leak_is_an_error(files, fragment):
    errors = [p["message"] for p in codecheck.static(files, ["issueRefund"]) if p["severity"] == "error"]
    assert any(fragment in m for m in errors), errors


@pytest.mark.parametrize("source, fragment", [
    ("import os\n" + HANDLER, "`os` is imported and never used"),
    (HANDLER + "def f(x):\n    return eval(x)\n", "`eval` runs text as code"),
    ("import subprocess\n" + HANDLER + "def f(x):\n    subprocess.run(x, shell=True)\n", "runs a shell command"),
    ("import pickle\n" + HANDLER + "def f(x):\n    return pickle.loads(x)\n", "can run code hidden"),
    ("import yaml\n" + HANDLER + "def f(x):\n    return yaml.load(x)\n", "yaml.safe_load"),
    ("import requests\n" + HANDLER + "def f():\n    requests.get('https://x', verify=False)\n", "verify=False"),
    (HANDLER + "PASSWORD = 'hunter2hunter2'\n", "password or token"),
    (HANDLER + "URL = 'http://api.example.com'\n", "plain http://"),
])
def test_lint_and_security_findings_warn(source, fragment):
    warnings = [p["message"] for p in codecheck.static({"handler.py": source}, ["issueRefund"])
                if p["severity"] == "warning"]
    assert any(fragment in m for m in warnings), warnings


def test_requirements_and_several_tools_warn():
    found = codecheck.static({"handler.py": HANDLER, "requirements.txt": "requests\nboto3==1.0\n"},
                             ["a", "b"])
    msgs = [p["message"] for p in found]
    assert any("is not pinned" in m for m in msgs)
    assert any("already has boto3" in m for m in msgs)
    assert any("never reads which one was called" in m for m in msgs)
    assert all(p["severity"] == "warning" for p in found)


# --- the sandbox ------------------------------------------------------------------------

class FakeInterpreter:
    def __init__(self, stdout):
        self.stdout, self.calls, self.stopped = stdout, [], False

    def start_code_interpreter_session(self, **kw):
        self.calls.append(("start", kw))
        return {"sessionId": "s1"}

    def invoke_code_interpreter(self, **kw):
        self.calls.append((kw["name"], kw["arguments"]))
        text = "ok" if kw["name"] == "writeFiles" else self.stdout
        return {"stream": [{"result": {"content": [{"type": "text", "text": text}],
                                       "structuredContent": {"stdout": text, "stderr": ""}}}]}

    def stop_code_interpreter_session(self, **kw):
        self.stopped = True


def test_the_sandbox_runs_each_event_with_the_functions_environment():
    out = codecheck.RESULT_MARK + json.dumps({"results": [{"name": "twenty", "ok": True, "output": "{}", "ms": 1}]})
    fake = FakeInterpreter("noise\n" + out)
    got = codecheck.check("refunds", FILES, {"grants": {"table": "orders"}, "environment": {"CURRENCY": "USD"}},
                          ["issueRefund"], client=fake)
    assert got["ok"] and got["sandbox"]["ran"] and got["sandbox"]["results"][0]["name"] == "twenty"
    written = dict(fake.calls)["writeFiles"]["content"]
    assert [c["path"] for c in written] == ["tool/handler.py"]          # code only, not the events
    code = dict(fake.calls)["executeCode"]["code"]
    assert '\\"TABLE_NAME\\": \\"orders\\"' in code and '\\"CURRENCY\\": \\"USD\\"' in code
    assert "refunds___" in code and fake.stopped
    compile(code, "harness", "exec")                                     # it is valid Python


def test_a_missing_requirement_is_installed_at_deploy_not_a_failure():
    fake = FakeInterpreter(codecheck.RESULT_MARK + json.dumps(
        {"import": "ModuleNotFoundError: No module named 'stripe'"}))
    got = codecheck.check("refunds", FILES, {}, ["issueRefund"], client=fake)
    assert got["ok"] and not got["sandbox"]["ran"] and "installed from requirements.txt" in got["sandbox"]["note"]


def test_no_events_no_run_and_errors_never_reach_the_sandbox():
    assert codecheck.check("r", {"handler.py": HANDLER}, {}, ["t"])["sandbox"]["ran"] is False
    fake = FakeInterpreter("")
    got = codecheck.check("r", {"handler.py": "def x(:"}, {}, ["t"], client=fake)
    assert not got["ok"] and "sandbox" not in got and fake.calls == []


def test_invoke_calls_as_the_gateway_does():
    class FakeLambda:
        def invoke(self, **kw):
            self.kw = kw
            return {"StatusCode": 200, "Payload": io.BytesIO(b'{"refunded": 20}'),
                    "LogResult": "U1RBUlQ="}
    fake = FakeLambda()
    got = codecheck.invoke(fake, "ToolLambda-ax_1-refunds", "refunds", "issueRefund", {"amount": 20})
    assert got["ok"] and got["output"] == '{"refunded": 20}' and got["log"] == "START"
    import base64
    ctx = json.loads(base64.b64decode(fake.kw["ClientContext"]))
    assert ctx == {"custom": {"bedrockAgentCoreToolName": "refunds___issueRefund"}}


# --- the files' journey -----------------------------------------------------------------

def test_apply_writes_the_code_where_the_iac_reads_it(tree, tmp_path):
    wf = claims_workflow()
    wf["tools"] = {"refunds": TOOL}
    wf["agents"]["claims_intake"]["tool"] = "refunds"
    (tree / "app" / "tools" / "_code" / "gone").mkdir(parents=True)
    r = run(tree, str(bundle(tmp_path, wf, toolCode={"refunds": FILES, "stranger": {"handler.py": "x"}})), "--exact")
    assert r.returncode == 0, r.stderr
    folder = tree / "app" / "tools" / "_code" / "refunds"
    assert (folder / "handler.py").read_text() == HANDLER
    assert (folder / "requirements.txt").exists() and (folder / ".from-builder").exists()
    assert not (tree / "app" / "tools" / "_code" / "gone").exists()
    assert not (tree / "app" / "tools" / "_code" / "stranger").exists()
    # The sample's own tool folders go (--exact), _code stays: the workflow uses it.
    assert sorted(d.name for d in (tree / "app" / "tools").iterdir() if d.is_dir()) == ["_code"]


def test_apply_refuses_a_code_tool_without_its_handler(tree, tmp_path):
    wf = claims_workflow()
    wf["tools"] = {"refunds": TOOL}
    r = run(tree, str(bundle(tmp_path, wf, toolCode={"refunds": {"requirements.txt": ""}})), "--exact")
    assert r.returncode == 2 and "handler.py is missing" in r.stderr


def test_apply_without_exact_keeps_a_folder_you_took_over(tree, tmp_path):
    wf = claims_workflow()
    wf["tools"] = {"refunds": TOOL}
    path = bundle(tmp_path, wf, toolCode={"refunds": FILES})
    assert run(tree, str(path)).returncode == 0
    folder = tree / "app" / "tools" / "_code" / "refunds"
    (folder / ".from-builder").unlink()
    (folder / "handler.py").write_text("# mine\n" + HANDLER)
    assert run(tree, str(path)).returncode == 0
    assert (folder / "handler.py").read_text().startswith("# mine")


def test_the_runner_installs_requirements_for_lambdas_python(monkeypatch):
    sys.path.insert(0, str(codecheck.__file__).rsplit("/bff/", 1)[0] + "/deployer")
    try:
        import runner
    finally:
        sys.path.pop(0)
    seen = []
    monkeypatch.setattr(runner, "run", lambda cmd, cwd, env=None, keep=60: seen.append(cmd))
    real = runner.ROOT / "app" / "tools" / "_code" / "zzTestTool"
    real.mkdir(parents=True)
    try:
        (real / "requirements.txt").write_text("# none yet\n")
        assert runner.install_code_requirements({"tools": {"zzTestTool": TOOL}}) == []
        (real / "requirements.txt").write_text("requests==2.32.4\n")
        assert runner.install_code_requirements({"tools": {"zzTestTool": TOOL}}) == ["zzTestTool"]
    finally:
        import shutil
        shutil.rmtree(real)
        if not any((runner.ROOT / "app" / "tools" / "_code").iterdir()):
            (runner.ROOT / "app" / "tools" / "_code").rmdir()
    cmd = seen[0]
    assert cmd[1:4] == ["-m", "pip", "install"] and "--only-binary=:all:" in cmd
    assert cmd[cmd.index("--platform") + 1] == "manylinux2014_x86_64"
    assert cmd[cmd.index("--python-version") + 1] == "3.12"


# --- the BFF ----------------------------------------------------------------------------

def code_project(files=FILES, **kw):
    p = project(tools={"refunds": TOOL}, **kw)
    p["workflow"]["agents"]["claims_intake"]["tool"] = "refunds"
    p["toolCode"] = {"refunds": files}
    return p


def put(e, p, sub="u1"):
    return call(e, "PUT /api/builds/{id}", params={"id": "pclaims01"}, body={"project": p}, sub=sub)


def test_every_change_to_the_code_is_in_the_owners_log(env):
    assert put(env, code_project())[0] == 200
    put(env, code_project())                                   # unchanged: no new entry
    put(env, code_project(files={**FILES, "handler.py": HANDLER + "# v2\n"}))
    logged = [e for e in call(env, "GET /api/audit")[1] if e["action"] == "lambda.code.updated"]
    assert len(logged) == 2 and logged[0]["detail"]["tools"] == ["refunds"]
    put(env, project())                                        # the tool is gone
    gone = next(e for e in call(env, "GET /api/audit")[1] if e["action"] == "lambda.code.updated")
    assert gone["detail"]["removed"] == ["refunds"]


def test_the_bundle_carries_the_code_of_code_tools_only(env):
    import buildstore
    p = code_project()
    p["toolCode"]["old"] = {"handler.py": "x"}
    b = buildstore.bundle_of(p, 1)
    assert list(b["toolCode"]) == ["refunds"]
    assert "toolCode" not in buildstore.bundle_of(project(), 1)


def test_a_deploy_is_refused_while_the_code_has_an_error(env):
    put(env, code_project(files={"handler.py": "def lambda_handler(event, context)\n"}))
    status, body = call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"}, body={"tool": "cdk"})
    assert status == 400 and "does not parse" in body["error"]
    put(env, code_project())
    assert call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"}, body={"tool": "cdk"})[0] == 202


def test_the_check_route_runs_the_checks(env, monkeypatch):
    monkeypatch.setattr(codecheck, "sandbox", lambda *a, **k: {"ran": True, "results": [], "note": "n"})
    status, got = call(env, "POST /api/code/check", body={"key": "refunds", "files": FILES, "tool": TOOL})
    assert status == 200 and got["ok"] and got["sandbox"]["ran"]
    assert call(env, "POST /api/code/check", body={"key": "no-good", "files": FILES})[0] == 400


def test_test_tool_calls_the_callers_own_deployed_function(env, monkeypatch):
    put(env, code_project())
    call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"}, body={"tool": "cdk"})
    agent = mark_deployed(env, "pclaims01")
    seen = {}

    def fake_invoke(client, fn, key, tool, event):
        seen.update(fn=fn, key=key, tool=tool, event=event)
        return {"ok": True, "status": 200, "error": "", "output": "{}", "log": "", "ms": 3}
    monkeypatch.setattr(codecheck, "invoke", fake_invoke)
    status, got = call(env, "POST /api/builds/{id}/test-tool", params={"id": "pclaims01"},
                       body={"key": "refunds", "event": {"amount": 20}})
    assert status == 200 and got["ok"]
    assert seen == {"fn": f"ToolLambda-{agent}-refunds", "key": "refunds", "tool": "issueRefund",
                    "event": {"amount": 20}}
    assert "tool.tested" in [e["action"] for e in call(env, "GET /api/audit")[1]]
    # Not someone else's build, not a tool that is not code, not without `deploy`.
    assert call(env, "POST /api/builds/{id}/test-tool", params={"id": "pclaims01"},
                body={"key": "refunds", "event": {}}, sub="u2")[0] == 404
    assert call(env, "POST /api/builds/{id}/test-tool", params={"id": "pclaims01"},
                body={"key": "other", "event": {}})[0] == 404
    assert call(env, "POST /api/builds/{id}/test-tool", params={"id": "pclaims01"},
                body={"key": "refunds", "event": {}}, groups=("members",))[0] == 403
