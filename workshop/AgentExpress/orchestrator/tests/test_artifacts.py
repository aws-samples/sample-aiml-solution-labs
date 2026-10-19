"""Files a run makes (app/common/artifacts.py): what an agent saves, and the code the
codeInterpreter tool ran.

What has to hold:
  * the key layout is the contract other code builds on: runs/<session>/outputs/<name>,
    runs/<session>/code/<agent>-<n>.<ext> and the .log beside it, the agent in metadata;
  * a name can never leave the run's folder, and is capped; a file over 25 MB is refused;
  * a deployment with no assets bucket says why when an agent saves, and skips code
    keeping silently, never failing the tool call;
  * a codeInterpreter call the model makes is kept once, with what it printed; other
    tools are not.
"""
from __future__ import annotations

import asyncio
import json
import sys

import boto3
import pytest

moto = pytest.importorskip("moto")

BUCKET = "agentcore-x-assets-1"
SID = "abc123def456"


@pytest.fixture()
def art(monkeypatch):
    for k, v in {"AWS_ACCESS_KEY_ID": "t", "AWS_SECRET_ACCESS_KEY": "t", "AWS_DEFAULT_REGION": "us-east-1",
                 "AWS_REGION": "us-east-1", "ASSETS_BUCKET": BUCKET}.items():
        monkeypatch.setenv(k, v)
    with moto.mock_aws():
        boto3.client("s3").create_bucket(Bucket=BUCKET)
        sys.modules.pop("app.common.artifacts", None)
        from app.common import artifacts as mod
        mod._clients.clear()
        mod._warned.clear()
        yield mod


def _get(key):
    return boto3.client("s3").get_object(Bucket=BUCKET, Key=key)


def test_an_output_is_kept_under_the_runs_outputs_folder_with_its_agent(art):
    key = art.put_output(SID, "report.csv", b"a,b\n1,2\n", "text/csv", "writer")
    assert key == f"runs/{SID}/outputs/report.csv"
    got = _get(key)
    assert got["Body"].read() == b"a,b\n1,2\n" and got["ContentType"] == "text/csv"
    assert got["Metadata"] == {"agent": "writer"}
    # Text is accepted and stored as UTF-8; a nonsense content type becomes a download.
    key = art.put_output(SID, "notes.md", "café", "<script>", "writer")
    assert _get(key)["Body"].read() == "café".encode() and _get(key)["ContentType"] == "application/octet-stream"


@pytest.mark.parametrize("name, safe", [
    ("../../other/run.txt", "run.txt"),
    ("..\\..\\x.txt", "x.txt"),
    ("..", "file"),
    (".hidden", "hidden"),
    ("a..b.txt", "a.b.txt"),
    ("résumé <final>.pdf", "r_sum_ _final_.pdf"),
    ("", "file"),
])
def test_a_name_can_never_leave_the_runs_folder(art, name, safe):
    assert art.safe_name(name) == safe
    key = art.put_output(SID, name, b"x", "text/plain", "writer")
    assert key == f"runs/{SID}/outputs/{safe}"


def test_a_long_name_is_cut_and_keeps_its_extension(art):
    out = art.safe_name("x" * 300 + ".xlsx")
    assert len(out) == art.MAX_NAME and out.endswith(".xlsx")


def test_size_and_ids_are_checked_before_anything_is_written(art):
    with pytest.raises(ValueError, match="at most 25 MB"):
        art.put_output(SID, "big.bin", b"x" * (art.MAX_OUTPUT_BYTES + 1), "application/octet-stream", "writer")
    with pytest.raises(ValueError, match="not a run id"):
        art.put_output("../x", "a.txt", b"x", "text/plain", "writer")
    with pytest.raises(ValueError, match="not an agent id"):
        art.put_output(SID, "a.txt", b"x", "text/plain", "../writer")
    assert boto3.client("s3").list_objects_v2(Bucket=BUCKET).get("KeyCount") == 0


def test_code_and_its_output_are_kept_side_by_side(art):
    key = art.put_code(SID, "analyst", "Python", "print(1)", "1\n", "warn")
    assert key.startswith(f"runs/{SID}/code/analyst-") and key.endswith(".py")
    n = key.rsplit("-", 1)[1].split(".")[0]
    assert n.isdigit()
    assert _get(key)["Body"].read() == b"print(1)"
    log = _get(key[:-3] + ".log")
    assert log["Body"].read().decode() == "=== stdout ===\n1\n\n=== stderr ===\nwarn\n"
    assert log["Metadata"] == {"agent": "analyst", "language": "py"}
    assert art.put_code(SID, "analyst", "cobol", "x", "", "").endswith(".txt")


def test_code_and_logs_are_capped(art):
    key = art.put_code(SID, "analyst", "javascript", "x" * 60_000, "y" * 2_000_000, "")
    assert key.endswith(".js") and len(_get(key)["Body"].read()) == art.MAX_CODE_BYTES
    assert _get(key[:-3] + ".log")["Body"].read().endswith(b"[... cut at 1 MB]\n")


def test_without_a_bucket_saving_says_why_and_code_keeping_is_skipped(art, monkeypatch):
    monkeypatch.setenv("ASSETS_BUCKET", "")
    with pytest.raises(art.NoFileStore, match=r"orchestrator\.files"):
        art.put_output(SID, "a.txt", b"x", "text/plain", "writer")
    assert art.record_code_call(SID, "analyst", {"language": "python", "code": "print(1)"}, "{}") == ""


def test_a_code_call_is_kept_with_what_it_printed_and_a_failure_never_breaks_it(art, capsys):
    result = json.dumps({"language": "python", "exitCode": 1, "stdout": "half", "stderr": "Traceback",
                         "error": "the code failed (exit status 1). See `stderr`."}, indent=2)
    key = art.record_code_call(SID, "analyst", {"language": "python", "code": "1/0"}, result)
    log = _get(key[:-3] + ".log")["Body"].read().decode()
    assert "half" in log and "Traceback\nthe code failed" in log
    # Text that is not the tool's JSON is kept as stdout.
    key = art.record_code_call(SID, "analyst", {"language": "python", "code": "print(2)"}, "2")
    assert _get(key[:-3] + ".log")["Body"].read().startswith(b"=== stdout ===\n2\n")
    # No code: nothing kept. A refused write: logged once, "" returned, no exception.
    assert art.record_code_call(SID, "analyst", {"language": "python"}, "x") == ""
    assert art.record_code_call("s1", "analyst", {"code": "x"}, "x") == ""
    assert art.record_code_call("s1", "analyst", {"code": "y"}, "x") == ""
    assert capsys.readouterr().out.count("[artifacts] code not kept") == 1


def test_only_a_lambda_publishing_execute_code_is_a_code_tool(art):
    """The runtime's tool entries keep the call shape only (config.APP_TOOL_KEYS), not
    `source`, so the sandbox is told by the one tool it publishes."""
    assert art.is_code_tool({"type": "lambda"}, "sandbox___execute_code")
    assert not art.is_code_tool({"type": "lambda"}, "pricing___get_price")
    assert not art.is_code_tool({"type": "mcp"}, "docs___execute_code")
    assert not art.is_code_tool(None, "sandbox___execute_code")


# --- the model's sandbox calls, from the tool loop ----------------------------------------

def test_the_tool_loop_keeps_each_sandbox_call_once_and_nothing_else(monkeypatch):
    from conftest import workflow
    from test_tool_loop import AI, FakeCtx, FakeTool, _run
    tools = {"sandbox": {"type": "lambda", "source": "codeInterpreter", "description": "Run code"},
             "claimsDb": {"type": "lambda", "description": "Claims",
                          "lambdaArn": "arn:aws:lambda:us-east-1:123456789012:function:f",
                          "toolSchema": [{"name": "q", "properties": {"query": {"type": "string"}}}]}}
    defn = {"orchestrator": {}, "tools": tools,
            "agents": {"triage": {"name": "Triage", "runtime": "dedicated", "maxTokens": 100,
                                  "tool": ["sandbox", "claimsDb"], "toolMode": "model"}},
            "steps": [{"agent": "triage"}]}
    with workflow(defn) as imp:
        loop = imp("app.common.tool_loop")
        kept: list[tuple] = []
        monkeypatch.setattr(loop.artifacts, "record_code_call", lambda *a: kept.append(a) or "k")
        run = FakeTool("sandbox___execute_code", "Run code",
                       schema={"type": "object", "properties": {"language": {"type": "string"},
                                                                 "code": {"type": "string"}}},
                       answer=[{"type": "text", "text": json.dumps({"stdout": "4\n", "stderr": ""})}])
        db = FakeTool("claimsDb___q", "Look up a claim", answer="claim 1")
        script = [AI(("sandbox___execute_code", {"language": "python", "code": "print(2+2)"})),
                  AI(("claimsDb___q", {"claim": "1"})),
                  AI(text="done")]
        _run(monkeypatch, imp, FakeCtx(["sandbox", "claimsDb"]), script,
             {"sandbox": [run], "claimsDb": [db]})
    assert len(kept) == 1
    sid, agent, args, found = kept[0]
    assert (sid, agent, args) == ("s1", "triage", {"language": "python", "code": "print(2+2)"})
    assert json.loads(found)["stdout"] == "4\n"


def test_ctx_save_file_keeps_the_file_for_the_current_agent(monkeypatch):
    from conftest import agents, workflow
    calls: list[tuple] = []
    with workflow({"agents": agents("writer"), "steps": [{"agent": "writer"}]}) as imp:
        ctx_mod = imp("app.common.context")
        art = imp("app.common.artifacts")
        monkeypatch.setattr(art, "put_output", lambda *a: calls.append(a) or f"runs/{a[0]}/outputs/{a[1]}")
        logs: list[str] = []

        class Ctx:
            session_id, agent_id = SID, "writer"

            async def log(self, msg):
                logs.append(msg)
        key = asyncio.run(ctx_mod.AgentContext.save_file(Ctx(), "out.csv", b"a", "text/csv"))
    assert calls == [(SID, "out.csv", b"a", "text/csv", "writer")]
    assert key == f"runs/{SID}/outputs/out.csv" and logs == ["Saved file out.csv"]
