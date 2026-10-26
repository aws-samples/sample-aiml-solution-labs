"""Task steps (runtime "task", app/common/task_agent.py): a step with no model.

What has to hold:
  * placeholders take an earlier output's value as it is when they are the whole string,
    its text inside a longer one, and name what is missing when they point at nothing;
  * a tool step sends the arguments it built (the tool's fixed `args` win) and keeps the
    result as data;
  * a code step gives the sandbox the request, earlier outputs and the run's files, runs
    the script once, keeps what it wrote to out/ and its code and log, always stops the
    session, and fails the step when the script fails;
  * an extract step reads an image synchronously and a PDF through the asynchronous job
    (one page is the synchronous limit), pages through the results, and turns the blocks
    into text, tables and form fields;
  * an output step writes csv / json / markdown / txt from the outputs it names;
  * the registry builds a TaskAgent for runtime "task" and refuses a task it cannot run.

Every AWS call goes to a fake whose requests are checked against the installed botocore
model, so a request the real service would refuse fails here.
"""
from __future__ import annotations

import asyncio
import base64
import json

import botocore.session
import botocore.validate
import pytest

from app.common import task_agent as ta
from app.common import task_template as tt

SID = "sess123456"
_SESSION = botocore.session.get_session()


def _check(service: str, op: str, kw: dict) -> None:
    shape = _SESSION.get_service_model(service).operation_model(op).input_shape
    botocore.validate.validate_parameters(kw, shape)


class Ctx:
    """What a task reads from its AgentContext."""

    def __init__(self, outputs=None, files=None, attachments=None):
        self.topic = "price the cluster"
        self.session_id = SID
        self.agent_id = "step"
        self.tools = []
        self.state = {"outputs": dict(outputs or {}), "attachments": list(attachments or [])}
        self.logs: list[str] = []
        self.saved: list[tuple] = []
        self._files = list(files or [])

    async def log(self, msg):
        self.logs.append(msg)

    async def _run_files(self):
        return self._files

    async def save_file(self, name, data, content_type):
        self.saved.append((name, data, content_type))
        return f"runs/{SID}/outputs/{name}"


def _agent(task: dict, **extra) -> ta.TaskAgent:
    a = ta.TaskAgent()
    a.id, a.name = "step", "A step"
    for k, v in extra.items():
        setattr(a, k, v)
    return a.use(task)


def run(agent, ctx) -> dict:
    return json.loads(asyncio.run(agent.run(ctx)))


# --- placeholders -----------------------------------------------------------------------

DATA = {"request": "r", "session": SID,
        "outputs": {"intake": json.dumps({"service": "EKS", "nodes": 3, "regions": ["us-east-1", "eu-west-1"]}),
                    "notes": "plain text"}}


def test_a_whole_placeholder_keeps_its_type_and_an_embedded_one_becomes_text():
    got = tt.render({"n": "{{outputs.intake.nodes}}", "q": "{{ outputs.intake.service }} for {{request}}",
                     "r": ["{{outputs.intake.regions.1}}"], "all": "{{outputs.intake}}", "s": "{{session}}",
                     "t": "{{outputs.notes}}", "kept": 4}, DATA)
    assert got == {"n": 3, "q": "EKS for r", "r": ["eu-west-1"], "s": SID, "t": "plain text", "kept": 4,
                   "all": {"service": "EKS", "nodes": 3, "regions": ["us-east-1", "eu-west-1"]}}
    assert tt.render("regions: {{outputs.intake.regions}}", DATA) == 'regions: ["us-east-1", "eu-west-1"]'


@pytest.mark.parametrize("text, why", [
    ("{{topic}}", "is not a placeholder"),
    ("{{outputs.later}}", "later has no output in this run yet"),
    ("{{outputs.intake.missing}}", "intake has no 'missing'"),
    ("{{outputs.intake.regions.5}}", "intake.regions has no '5'"),
])
def test_a_placeholder_that_names_nothing_says_what(text, why):
    with pytest.raises(tt.TemplateError, match=why):
        tt.render(text, DATA)


def test_refs_finds_every_placeholder():
    assert tt.refs({"a": ["{{request}}", {"b": "x {{outputs.y.z}}"}], "c": 1}) == ["request", "outputs.y.z"]


# --- tool -------------------------------------------------------------------------------

def test_a_tool_step_sends_the_arguments_it_built_and_keeps_the_result_as_data(monkeypatch):
    from app.features.gateway import client
    sent = {}

    async def invoke_task(key, args):
        sent.update(key=key, args=args)
        return [{"price": 0.1}, "price 0.1"]
    monkeypatch.setattr(client, "invoke_task", invoke_task)
    agent = _agent({"kind": "tool", "tool": "pricing", "arguments": {"service": "{{outputs.intake.service}}"}})
    assert agent.tools == ["pricing"]           # so a call waiting for approval is journaled
    out = run(agent, Ctx(outputs=DATA["outputs"]))
    assert sent == {"key": "pricing", "args": {"service": "EKS"}}
    assert out["task"] == "tool" and out["result"] == {"price": 0.1} and out["createdByAgent"] == "step"


class FakeTool:
    def __init__(self, name):
        self.name = name
        self.calls = []

    async def ainvoke(self, args):
        self.calls.append(args)
        return [{"type": "text", "text": json.dumps({"rows": [1, 2]})}]


def test_invoke_task_lets_the_tools_fixed_args_win_and_refuses_an_ambiguous_target(monkeypatch):
    from app.common.errors import ToolUnavailable
    from app.features.gateway import client
    tool = FakeTool("pricing___get")
    monkeypatch.setattr(client, "TOOLS", {"pricing": {"type": "lambda", "args": {"region": "us-east-1"}}})
    monkeypatch.setattr(client, "_record_tool", lambda *a, **k: None)
    monkeypatch.setattr(client, "_record_policy", lambda *a, **k: None)

    async def one(keys):
        return {"pricing": [tool]}
    monkeypatch.setattr(client, "published_for", one)
    data, text = asyncio.run(client.invoke_task("pricing", {"region": "eu-west-1", "q": "x"}))
    assert tool.calls == [{"region": "us-east-1", "q": "x"}]
    assert data == {"rows": [1, 2]} and "1" in text

    async def two(keys):
        return {"pricing": [tool, FakeTool("pricing___other")]}
    monkeypatch.setattr(client, "published_for", two)
    with pytest.raises(ToolUnavailable, match="exactly one tool"):
        asyncio.run(client.invoke_task("pricing", {}))


# --- code -------------------------------------------------------------------------------

class FakeAgentCore:
    """bedrock-agentcore's Code Interpreter calls, each checked against botocore."""

    def __init__(self, exit_code=0, listed=None, read=None):
        self.calls: list[tuple[str, dict]] = []
        self.exit_code = exit_code
        self.listed = listed if listed is not None else [
            {"type": "resource_link", "uri": "file:///workspace/out/table.csv", "name": "table.csv"},
            {"type": "resource_link", "uri": "file:///workspace/out/.keep", "name": ".keep"}]
        self.read = read if read is not None else [
            {"type": "resource", "resource": {"type": "text", "uri": "file:///workspace/out/table.csv",
                                              "mimeType": "text/csv", "text": "a,b\n1,2\n"}}]

    def start_code_interpreter_session(self, **kw):
        _check("bedrock-agentcore", "StartCodeInterpreterSession", kw)
        self.calls.append(("start", kw))
        return {"sessionId": "s01ABC", "codeInterpreterIdentifier": kw["codeInterpreterIdentifier"]}

    def stop_code_interpreter_session(self, **kw):
        _check("bedrock-agentcore", "StopCodeInterpreterSession", kw)
        self.calls.append(("stop", kw))
        return {}

    def invoke_code_interpreter(self, **kw):
        _check("bedrock-agentcore", "InvokeCodeInterpreter", kw)
        self.calls.append((kw["name"], kw))
        if kw["name"] == "executeCode":
            return {"stream": [{"result": {"content": [], "isError": self.exit_code != 0, "structuredContent": {
                "stdout": json.dumps({"total": 3}) + "\n", "stderr": "warn\n" if self.exit_code == 0 else "Traceback: boom",
                "exitCode": self.exit_code}}}]}
        if kw["name"] == "listFiles":
            return {"stream": [{"result": {"content": self.listed}}]}
        if kw["name"] == "readFiles":
            return {"stream": [{"result": {"content": self.read}}]}
        return {"stream": [{"result": {"content": [{"type": "text", "text": "ok"}]}}]}


@pytest.fixture()
def sandbox(monkeypatch):
    fake = FakeAgentCore()
    monkeypatch.setitem(ta._clients, "bedrock-agentcore", fake)
    kept = {"outputs": [], "code": []}
    monkeypatch.setattr(ta.artifacts, "_bucket", lambda: "bucket")
    monkeypatch.setattr(ta.artifacts, "put_output", lambda s, n, d, t, a: kept["outputs"].append((n, d, t)) or f"runs/{s}/outputs/{n}")
    monkeypatch.setattr(ta.artifacts, "put_code", lambda s, a, lang, c, o, e: kept["code"].append((lang, c, o, e)) or f"runs/{s}/code/{a}-1.py")
    monkeypatch.setattr(ta, "_record", lambda *a, **k: None)
    return fake, kept


def test_a_code_step_gives_the_sandbox_its_inputs_and_keeps_what_it_wrote(sandbox):
    fake, kept = sandbox
    files = [{"name": "sales data.xlsx", "bytes": b"PK\x03\x04", "kind": "document", "format": "xlsx"}]
    out = run(_agent({"kind": "code", "code": "print(1)"}), Ctx(outputs={"intake": '{"a": 1}'}, files=files))
    names = [c[0] for c in fake.calls]
    assert names == ["start", "writeFiles", "executeCode", "listFiles", "readFiles", "stop"]
    written = {b["path"]: b for b in fake.calls[1][1]["arguments"]["content"]}
    assert written["in/request.txt"]["text"] == "price the cluster"
    assert json.loads(written["in/outputs.json"]["text"]) == {"intake": {"a": 1}}
    assert written["in/sales data.xlsx"]["blob"] == b"PK\x03\x04" and "out/.keep" in written
    run_args = fake.calls[2][1]["arguments"]
    assert run_args == {"language": "python", "code": "print(1)", "clearContext": True}
    assert fake.calls[4][1]["arguments"] == {"paths": ["out/table.csv"]}
    assert kept["outputs"] == [("table.csv", b"a,b\n1,2\n", "text/csv")]
    assert kept["code"][0][:2] == ("python", "print(1)")
    assert out["output"] == {"total": 3} and out["files"][0]["name"] == "table.csv" and out["stderr"] == "warn\n"


def test_a_failing_script_fails_the_step_and_still_stops_the_session(sandbox):
    fake, kept = sandbox
    fake.exit_code = 1
    with pytest.raises(ta.TaskFailed, match=r"exit status 1.*boom"):
        run(_agent({"kind": "code", "code": "raise SystemExit(1)"}), Ctx())
    assert fake.calls[-1][0] == "stop" and kept["code"], "the script and its log are kept even when it fails"
    assert kept["outputs"] == []


def test_files_read_back_as_a_base64_blob_are_decoded(sandbox):
    fake, kept = sandbox
    fake.read = [{"type": "resource", "resource": {"type": "blob", "uri": "out/chart.png", "mimeType": "image/png",
                                                   "blob": base64.b64encode(b"\x89PNG").decode()}}]
    run(_agent({"kind": "code", "code": "x"}), Ctx())
    assert kept["outputs"] == [("chart.png", b"\x89PNG", "image/png")]


# --- extract ----------------------------------------------------------------------------

def _blocks():
    return [
        {"BlockType": "PAGE", "Id": "p", "Page": 1},
        {"BlockType": "LINE", "Id": "l1", "Text": "Invoice 42", "Page": 1},
        {"BlockType": "WORD", "Id": "w1", "Text": "Total"}, {"BlockType": "WORD", "Id": "w2", "Text": "12.50"},
        {"BlockType": "WORD", "Id": "w3", "Text": "Item"}, {"BlockType": "WORD", "Id": "w4", "Text": "Paid"},
        {"BlockType": "SELECTION_ELEMENT", "Id": "x1", "SelectionStatus": "SELECTED"},
        {"BlockType": "TABLE", "Id": "t", "Page": 2, "Relationships": [{"Type": "CHILD", "Ids": ["c1", "c2"]}]},
        {"BlockType": "CELL", "Id": "c1", "RowIndex": 1, "ColumnIndex": 1, "Relationships": [{"Type": "CHILD", "Ids": ["w3"]}]},
        {"BlockType": "CELL", "Id": "c2", "RowIndex": 1, "ColumnIndex": 2, "Relationships": [{"Type": "CHILD", "Ids": ["w2"]}]},
        {"BlockType": "KEY_VALUE_SET", "Id": "k", "EntityTypes": ["KEY"], "Page": 1,
         "Relationships": [{"Type": "CHILD", "Ids": ["w1"]}, {"Type": "VALUE", "Ids": ["v"]}]},
        {"BlockType": "KEY_VALUE_SET", "Id": "v", "EntityTypes": ["VALUE"], "Relationships": [{"Type": "CHILD", "Ids": ["w2"]}]},
        {"BlockType": "KEY_VALUE_SET", "Id": "k2", "EntityTypes": ["KEY"], "Page": 1,
         "Relationships": [{"Type": "CHILD", "Ids": ["w4"]}, {"Type": "VALUE", "Ids": ["v2"]}]},
        {"BlockType": "KEY_VALUE_SET", "Id": "v2", "EntityTypes": ["VALUE"], "Relationships": [{"Type": "CHILD", "Ids": ["x1"]}]},
    ]


def test_blocks_become_text_tables_and_form_fields():
    got = ta.blocks_to_json(_blocks())
    assert got["pages"] == 2 and got["text"] == "Invoice 42"
    assert got["tables"] == [{"page": 2, "rows": [["Item", "12.50"]]}]
    assert got["forms"] == [{"key": "Total", "value": "12.50", "page": 1}, {"key": "Paid", "value": "X", "page": 1}]


class FakeTextract:
    def __init__(self):
        self.calls: list[tuple[str, dict]] = []
        self.polls = 0

    def analyze_document(self, **kw):
        _check("textract", "AnalyzeDocument", kw)
        self.calls.append(("analyze", kw))
        return {"Blocks": _blocks()[:2]}

    def start_document_analysis(self, **kw):
        _check("textract", "StartDocumentAnalysis", kw)
        self.calls.append(("start", kw))
        return {"JobId": "job1"}

    def get_document_analysis(self, **kw):
        _check("textract", "GetDocumentAnalysis", kw)
        self.calls.append(("get", kw))
        self.polls += 1
        if self.polls == 1:
            return {"JobStatus": "IN_PROGRESS"}
        if "NextToken" not in kw:
            return {"JobStatus": "SUCCEEDED", "Blocks": _blocks()[:7], "NextToken": "n2"}
        return {"JobStatus": "SUCCEEDED", "Blocks": _blocks()[7:]}


def test_an_image_is_read_at_once_and_a_pdf_through_the_job(monkeypatch):
    fake = FakeTextract()
    monkeypatch.setitem(ta._clients, "textract", fake)
    monkeypatch.setattr(ta.attachments, "_bucket", lambda: "assets")
    monkeypatch.setattr(ta.time, "sleep", lambda s: None)
    monkeypatch.setattr(ta, "_record", lambda *a, **k: None)
    files = [{"key": f"runs/{SID}/attachments/1-scan.png", "name": "scan.png", "kind": "image", "format": "png"},
             {"key": f"runs/{SID}/attachments/2-invoice.pdf", "name": "invoice.pdf", "kind": "document", "format": "pdf"},
             {"key": f"runs/{SID}/attachments/3-notes.txt", "name": "notes.txt", "kind": "document", "format": "txt"}]
    out = run(_agent({"kind": "extract", "features": ["TABLES", "FORMS"]}), Ctx(attachments=files))
    assert [f["name"] for f in out["files"]] == ["scan.png", "invoice.pdf"]
    assert fake.calls[0] == ("analyze", {"Document": {"S3Object": {"Bucket": "assets", "Name": files[0]["key"]}},
                                         "FeatureTypes": ["TABLES", "FORMS"]})
    start = fake.calls[1][1]
    assert start["DocumentLocation"] == {"S3Object": {"Bucket": "assets", "Name": files[1]["key"]}}
    assert len(start["ClientRequestToken"]) == 64      # the same job for a re-run of the step
    assert [c[0] for c in fake.calls[2:]] == ["get", "get", "get"] and fake.calls[-1][1]["NextToken"] == "n2"
    assert out["files"][1]["tables"] and out["files"][1]["forms"][0]["key"] == "Total"


def test_an_extract_step_with_nothing_to_read_says_so(monkeypatch):
    with pytest.raises(ta.TaskFailed, match="no PDF, PNG or JPEG"):
        run(_agent({"kind": "extract"}), Ctx())
    files = [{"key": f"runs/{SID}/attachments/1-a.pdf", "name": "a.pdf", "kind": "document", "format": "pdf"}]
    with pytest.raises(ta.TaskFailed, match=r"no file named 'b\.pdf'"):
        run(_agent({"kind": "extract", "file": "b.pdf"}), Ctx(attachments=files))


def test_a_failed_textract_job_fails_the_step(monkeypatch):
    class Failed(FakeTextract):
        def get_document_analysis(self, **kw):
            return {"JobStatus": "FAILED", "StatusMessage": "password protected"}
    monkeypatch.setitem(ta._clients, "textract", Failed())
    with pytest.raises(ta.TaskFailed, match="password protected"):
        ta._analyze("assets", "runs/x/attachments/1-a.pdf", "pdf", ["TABLES"])


# --- output -----------------------------------------------------------------------------

OUTS = {"intake": json.dumps({"service": "EKS", "rows": [{"sku": "a", "usd": 1}, {"sku": "b", "usd": 2, "note": "x"}]}),
        "report": json.dumps({"title": "T", "score": 9})}


def test_a_csv_is_written_from_the_list_of_rows_an_output_holds():
    assert ta.render_file("csv", {"intake": json.loads(OUTS["intake"])}) == "sku,usd,note\na,1,\nb,2,x\n"
    both = ta.render_file("csv", {"intake.rows.0": {"sku": "a"}, "report": {"title": "T"}})
    assert both == "from,sku,title\nintake.rows.0,a,\nreport,,T\n"


def test_json_markdown_and_txt():
    one = {"report": {"title": "T", "score": 9}}
    assert json.loads(ta.render_file("json", one)) == {"title": "T", "score": 9}
    assert "## report" in ta.render_file("markdown", one) and "- **score**: 9" in ta.render_file("markdown", one)
    assert ta.render_file("txt", {"a": "hello", "b": [1]}) == "hello\n\n[1]\n"


def test_an_output_step_saves_its_file_with_the_run():
    ctx = Ctx(outputs=OUTS)
    out = run(_agent({"kind": "output", "format": "csv", "from": ["intake.rows"], "name": "prices-{{session}}"}), ctx)
    name, data, ctype = ctx.saved[0]
    assert name == f"prices-{SID}.csv" and ctype == "text/csv" and data.startswith(b"sku,usd,note")
    assert out["file"]["name"] == f"prices-{SID}.csv" and out["from"] == ["intake.rows"]
    ctx = Ctx(outputs=OUTS)
    run(_agent({"kind": "output", "format": "json"}), ctx)
    assert ctx.saved[0][0] == "step.json" and set(json.loads(ctx.saved[0][1])) == {"intake", "report"}


# --- registry ---------------------------------------------------------------------------

def _wf(task: dict, **extra) -> dict:
    return {"agents": {"first": {"name": "F", "runtime": "dedicated", "maxTokens": 100},
                       "step": {"name": "S", "runtime": "task", "task": task, **extra}},
            "tools": {"pricing": {"type": "lambda", "lambdaArn": "arn:aws:lambda:us-east-1:123456789012:function:p",
                                  "toolSchema": [{"name": "get", "properties": {"q": {"type": "string"}}}]}},
            "steps": [{"agent": "first"}, {"agent": "step"}]}


def test_the_registry_builds_a_task_agent_with_no_folder():
    from conftest import workflow
    with workflow(_wf({"kind": "tool", "tool": "pricing"})) as imp:
        agents = imp("app.orchestrator.registry").load_agents()
        assert type(agents["step"]).__name__ == "TaskAgent"
        assert agents["step"].task == {"kind": "tool", "tool": "pricing"} and agents["step"].tools == ["pricing"]
        assert agents["step"].recall_in_orchestrator is False and agents["step"].store_in_orchestrator is False


@pytest.mark.parametrize("task, why", [
    ({"kind": "lambda"}, "is not one of tool, code, extract, output, deliver"),
    ({"kind": "tool", "tool": "nope"}, "is not a key in the `tools` block"),
    ({"kind": "tool", "tool": "pricing", "code": "x"}, "task.code does not apply"),
    ({"kind": "code"}, "needs task.code"),
    ({"kind": "output"}, "needs task.format"),
])
def test_the_registry_refuses_a_task_it_cannot_run(task, why):
    from conftest import workflow
    with workflow(_wf(task)) as imp, pytest.raises(ValueError, match=why):
        imp("app.orchestrator.registry").load_agents()


def test_a_task_block_on_an_agent_is_refused():
    from conftest import workflow
    wf = _wf({"kind": "code", "code": "1"})
    wf["agents"]["first"]["task"] = {"kind": "code", "code": "1"}
    with workflow(wf) as imp, pytest.raises(ValueError, match="set runtime \"task\""):
        imp("app.orchestrator.registry").load_agents()


def test_a_task_steps_figures_are_not_flagged_as_ungrounded(monkeypatch):
    from app.orchestrator import nodes
    said = []

    async def emit(sid, ev):
        said.append(ev)
    monkeypatch.setattr(nodes, "emit", emit)
    ctx = Ctx()
    asyncio.run(nodes._check_grounding(ctx, _agent({"kind": "code", "code": "1"}), {"outputs": {}}, '{"total": 912345}'))
    assert said == []


def test_the_browser_sees_a_task_steps_kind():
    import importlib
    import sys

    from conftest import ORCH_ROOT
    sys.path.insert(0, str(ORCH_ROOT / "bff"))
    sys.modules.pop("workflow", None)
    bff_workflow = importlib.import_module("workflow")
    sys.modules.pop("workflow", None)
    got = bff_workflow.project(_wf({"kind": "code", "code": "print(1)"}))["agents"]
    assert got["step"]["task"] == "code" and got["step"]["source"] == "Task \u00b7 Code"
    assert "task" not in got["first"]


def test_scaffold_apply_writes_no_folder_for_a_task_step(tmp_path):
    """A task step's `task` block is the whole step: `scaffold.py apply --exact` must not
    create app/subagents/<id>/ for it (the deploy would ship an agent nothing runs)."""
    import shutil
    import subprocess
    import sys

    from conftest import ORCH_ROOT
    root = tmp_path / "orchestrator"
    root.mkdir()
    for name in ("scaffold.py", "format_workflow.py", "build_schema.py", "VERSION"):
        shutil.copy2(ORCH_ROOT / name, root / name)
    shutil.copytree(ORCH_ROOT / "app", root / "app", ignore=shutil.ignore_patterns("__pycache__"))
    sample = json.loads((ORCH_ROOT / "app" / "workflow.json").read_text())
    wf = {**{k: sample[k] for k in ("$schema", "$comment", "orchestrator", "ui", "guardrail", "authorization")},
          "tools": {},
          "agents": {"claims_intake": {"name": "Claims Intake", "runtime": "main", "maxTokens": 2000,
                                       "agentcore": {"evaluations": {"enabled": True}}},
                     "export": {"name": "Export", "runtime": "task", "task": {"kind": "code", "code": "print(1)"}}},
          "steps": [{"agent": "claims_intake"}, {"agent": "export"}]}
    path = tmp_path / "b.agentexpress.json"
    path.write_text(json.dumps({"format": "agentexpress-bundle", "version": 1, "project": {"name": "C"},
                                "workflow": wf, "prompts": {}}))
    result = subprocess.run([sys.executable, str(root / "scaffold.py"), "apply", str(path), "--exact"],  # noqa: S603 - argv list, no shell  # nosec B603
                            capture_output=True, text=True, cwd=root, check=False)
    assert result.returncode == 0, result.stderr
    assert (root / "app" / "subagents" / "claims_intake").is_dir()
    assert not (root / "app" / "subagents" / "export").exists()
    assert "app/subagents/export/" not in result.stdout


def test_a_sandbox_or_textract_call_is_a_tool_row_in_observability(monkeypatch):
    from app.features.observability import scope, store
    rows = []
    monkeypatch.setattr(store, "put", rows.append)
    scope.set_scope(SID, "step", "alice", 2)
    ta._record("codeInterpreter", "agentcore", 1234, "print(1)", "1\n", True)
    ta._record("textract", "textract", 50, "a.pdf", "", False)
    assert [(r.kind, r.label, r.mode, r.status, r.agent_id, r.version) for r in rows] == [
        ("tool", "codeInterpreter", "agentcore", "ok", "step", 2), ("tool", "textract", "error", "error", "step", 2)]
    assert rows[0].latency_ms == 1234 and rows[0].user_input == "print(1)"
