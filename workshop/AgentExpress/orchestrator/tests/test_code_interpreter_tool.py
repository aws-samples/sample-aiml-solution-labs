"""The built-in code interpreter tool (orchestrator/app/tools/codeInterpreter/handler.py).

`source: "codeInterpreter"` runs model-written code in the AWS-managed AgentCore Code
Interpreter and returns stdout, stderr and the exit status. The function runs against a
faked boto3 client here, like tests/test_code_tools.py does for bff/codecheck.py; nothing
in this file reaches AWS, so what these tests establish is the function's own logic and
the shape of its IAM, not that a deployed sandbox behaves this way.

What has to hold:

  * a normal run returns what the program printed and its exit status;
  * the session is stopped on EVERY path: success, a sandbox error, a timeout, an
    exception out of the client, or an exception while reading the result stream;
  * a refused call (no language, a bad language, no code, too much code) starts no
    session at all, so it costs nothing;
  * the run time, the code size and the output size are limited in code, and a cut
    output SAYS it was cut;
  * a program that fails is an error RESULT the model can read, not a crashed function;
  * the function never retries an invoke (a retry would run the code twice);
  * the role may do three actions on one interpreter, identically on both IaC paths.
"""
from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from botocore.exceptions import ReadTimeoutError

ORCH = Path(__file__).resolve().parent.parent
HANDLER_PATH = ORCH / "app" / "tools" / "codeInterpreter" / "handler.py"

ARN_RE = re.compile(r"arn:aws:bedrock-agentcore:[^\"`]*:aws:code-interpreter/aws\.codeinterpreter\.v1")
ACTIONS = {"bedrock-agentcore:StartCodeInterpreterSession",
           "bedrock-agentcore:InvokeCodeInterpreter",
           "bedrock-agentcore:StopCodeInterpreterSession"}


class FakeClient:
    """The three Code Interpreter calls, with the session rules that matter.

    A session must be started before it can be used, and a stopped session is gone: using
    it raises, as the service does. A fake that accepted any session id would let a
    function that forgot to start (or kept using a stopped) session pass.
    """

    def __init__(self, events=None, invoke_error=None, stream_error=None, stop_error=None,
                 start_error=None):
        self.events = events if events is not None else [_result("hi\n", "", 0)]
        self.invoke_error = invoke_error
        self.stream_error = stream_error
        self.stop_error = stop_error
        self.start_error = start_error
        self.open: set[str] = set()
        self.started = 0
        self.stopped = 0
        self.invokes: list[dict] = []
        self.start_kwargs: dict = {}

    def start_code_interpreter_session(self, **kw):
        if self.start_error:
            raise self.start_error
        self.started += 1
        self.start_kwargs = kw
        sid = f"sess-{self.started}"
        self.open.add(sid)
        return {"sessionId": sid, "codeInterpreterIdentifier": kw["codeInterpreterIdentifier"]}

    def invoke_code_interpreter(self, **kw):
        if kw["sessionId"] not in self.open:
            raise RuntimeError(f"ResourceNotFoundException: session {kw['sessionId']} is not open")
        self.invokes.append(kw)
        if self.invoke_error:
            raise self.invoke_error
        return {"sessionId": kw["sessionId"], "stream": self._stream()}

    def _stream(self):
        for i, ev in enumerate(self.events):
            yield ev
            if self.stream_error and i == 0:
                raise self.stream_error

    def stop_code_interpreter_session(self, **kw):
        self.stopped += 1
        if self.stop_error:
            raise self.stop_error
        self.open.discard(kw["sessionId"])
        return {"sessionId": kw["sessionId"]}


def _result(stdout="", stderr="", exit_code=0, is_error=False):
    """One stream event in the real shape (see bff/codecheck.py _stream_text)."""
    return {"result": {"isError": is_error, "content": [],
                       "structuredContent": {"stdout": stdout, "stderr": stderr,
                                             "exitCode": exit_code, "executionTime": 0.1}}}


@pytest.fixture()
def ci(monkeypatch):
    """The handler module, loaded under its own name (pricing's is also `handler`)."""
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    spec = importlib.util.spec_from_file_location("ax_code_interpreter_handler", HANDLER_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules["ax_code_interpreter_handler"] = module
    spec.loader.exec_module(module)
    yield module
    sys.modules.pop("ax_code_interpreter_handler", None)


def _run(ci, fake, **args):
    ci._client = fake
    return ci.execute_code(args)


# --- a normal run ----------------------------------------------------------------------

def test_a_normal_run_returns_stdout_and_the_exit_status(ci):
    fake = FakeClient([_result("hello\n", "", 0)])
    out = _run(ci, fake, language="python", code="print('hello')")
    assert out["stdout"] == "hello\n"
    assert out["stderr"] == ""
    assert out["exitCode"] == 0
    assert out["language"] == "python"
    assert "error" not in out
    assert "truncated" not in out


def test_the_code_goes_to_the_system_interpreter_in_a_clean_state(ci):
    fake = FakeClient()
    _run(ci, fake, language="python", code="print(1)")
    assert fake.start_kwargs["codeInterpreterIdentifier"] == "aws.codeinterpreter.v1"
    call = fake.invokes[0]
    assert call["codeInterpreterIdentifier"] == "aws.codeinterpreter.v1"
    assert call["name"] == "executeCode"
    assert call["arguments"] == {"language": "python", "code": "print(1)", "clearContext": True}
    assert call["sessionId"] == "sess-1"


@pytest.mark.parametrize("language", ["javascript", "typescript", "Python", "  python  "])
def test_every_language_the_interpreter_runs_is_accepted(ci, language):
    fake = FakeClient()
    out = _run(ci, fake, language=language, code="1")
    assert "error" not in out
    assert fake.invokes[0]["arguments"]["language"] == language.strip().lower()


def test_text_only_results_are_read_as_output(ci):
    event = {"result": {"isError": False, "structuredContent": None,
                        "content": [{"type": "text", "text": "plain text"}]}}
    out = _run(ci, FakeClient([event]), language="python", code="1")
    assert out["stdout"] == "plain text"
    assert "error" not in out


def test_each_call_gets_its_own_session(ci):
    fake = FakeClient()
    _run(ci, fake, language="python", code="a = 1")
    _run(ci, fake, language="python", code="print(a)")
    assert fake.started == 2
    assert [c["sessionId"] for c in fake.invokes] == ["sess-1", "sess-2"]


# --- the session is always stopped -------------------------------------------------------

def test_the_session_is_stopped_after_a_normal_run(ci):
    fake = FakeClient()
    _run(ci, fake, language="python", code="1")
    assert (fake.started, fake.stopped, fake.open) == (1, 1, set())


def test_the_session_is_stopped_when_the_sandbox_reports_an_error(ci):
    fake = FakeClient([_result("", "Traceback...", 1, is_error=True)])
    _run(ci, fake, language="python", code="1/0")
    assert (fake.started, fake.stopped, fake.open) == (1, 1, set())


def test_the_session_is_stopped_when_invoking_raises(ci):
    fake = FakeClient(invoke_error=RuntimeError("service unavailable"))
    ci._client = fake
    out = ci.lambda_handler({"language": "python", "code": "1"}, None)
    assert "RuntimeError" in out["error"]
    assert (fake.started, fake.stopped, fake.open) == (1, 1, set())


def test_the_session_is_stopped_when_reading_the_result_stream_raises(ci):
    fake = FakeClient([_result("partial", "", None), _result("more", "", 0)],
                      stream_error=ValueError("stream broke"))
    ci._client = fake
    out = ci.lambda_handler({"language": "python", "code": "1"}, None)
    assert "ValueError" in out["error"]
    assert (fake.started, fake.stopped, fake.open) == (1, 1, set())


def test_a_session_that_will_not_stop_does_not_hide_the_result(ci):
    fake = FakeClient([_result("done\n", "", 0)], stop_error=RuntimeError("stop refused"))
    out = _run(ci, fake, language="python", code="print('done')")
    assert out["stdout"] == "done\n"
    assert "error" not in out
    assert fake.stopped == 1


def test_a_session_that_cannot_start_is_an_error_and_nothing_is_stopped(ci):
    fake = FakeClient(start_error=RuntimeError("throttled"))
    ci._client = fake
    out = ci.lambda_handler({"language": "python", "code": "1"}, None)
    assert "throttled" in out["error"]
    assert fake.stopped == 0


# --- a program that fails is a result the model can read ---------------------------------

def test_a_sandbox_error_comes_back_as_an_error_result_with_the_traceback(ci):
    fake = FakeClient([_result("before\n", "Traceback (most recent call last):\nZeroDivisionError", 1,
                               is_error=True)])
    out = _run(ci, fake, language="python", code="print('before'); 1/0")
    assert "the code failed" in out["error"]
    assert out["exitCode"] == 1
    assert "ZeroDivisionError" in out["stderr"]
    assert out["stdout"] == "before\n"


def test_a_non_zero_exit_is_an_error_even_if_the_sandbox_did_not_flag_it(ci):
    out = _run(ci, FakeClient([_result("", "boom", 2, is_error=False)]), language="python", code="x")
    assert "the code failed" in out["error"]
    assert out["exitCode"] == 2


def test_an_unknown_exit_status_with_no_error_flag_is_not_an_error(ci):
    out = _run(ci, FakeClient([_result("ok", "", None)]), language="python", code="x")
    assert "error" not in out
    assert out["exitCode"] is None


# --- limits ------------------------------------------------------------------------------

def test_the_client_never_retries_because_a_retry_would_run_the_code_twice(ci):
    retries = ci._client.meta.config.retries
    assert retries["total_max_attempts"] == 1
    assert ci._client.meta.config.read_timeout == ci.EXEC_TIMEOUT_SECONDS


def test_a_run_with_no_output_that_hits_the_read_timeout_is_a_timeout(ci):
    err = ReadTimeoutError(endpoint_url="https://bedrock-agentcore.us-east-1.amazonaws.com")
    fake = FakeClient(invoke_error=err)
    ci._client = fake
    out = ci.lambda_handler({"language": "python", "code": "import time; time.sleep(999)"}, None)
    assert "timed out after" in out["error"]
    assert str(ci.EXEC_TIMEOUT_SECONDS) in out["error"]
    assert (fake.started, fake.stopped, fake.open) == (1, 1, set())


def test_a_run_that_keeps_printing_past_the_deadline_is_stopped_with_its_partial_output(ci, monkeypatch):
    # Three chunks; the clock jumps past the deadline after the first is read.
    ticks = iter([1000.0, 1000.0, 1000.0 + ci.EXEC_TIMEOUT_SECONDS + 1, 2000.0, 2000.0, 2000.0])
    monkeypatch.setattr(ci.time, "monotonic", lambda: next(ticks))
    fake = FakeClient([_result("one\n", "", None), _result("two\n", "", None), _result("three\n", "", 0)])
    out = _run(ci, fake, language="python", code="while True: print(1)")
    assert "timed out after" in out["error"]
    assert out["stdout"] == "one\n", "reading stopped at the deadline, so later chunks are not included"
    assert fake.stopped == 1 and fake.open == set()


def test_a_run_inside_the_deadline_is_not_a_timeout(ci):
    out = _run(ci, FakeClient([_result("a", "", None), _result("b", "", 0)]), language="python", code="1")
    assert out["stdout"] == "ab"
    assert "error" not in out


def test_output_over_the_limit_is_cut_and_says_so(ci):
    big = "x" * (ci.MAX_OUTPUT_CHARS + 5000)
    out = _run(ci, FakeClient([_result(big, "", 0)]), language="python", code="print('x' * 99999)")
    assert len(out["stdout"]) == ci.MAX_OUTPUT_CHARS
    assert out["truncated"] == ["stdout"]
    assert "stdout cut at" in out["note"]
    assert str(ci.MAX_OUTPUT_CHARS) in out["note"]


def test_stderr_is_limited_on_its_own(ci):
    big = "e" * (ci.MAX_OUTPUT_CHARS + 1)
    out = _run(ci, FakeClient([_result("small", big, 1, is_error=True)]), language="python", code="x")
    assert len(out["stderr"]) == ci.MAX_OUTPUT_CHARS
    assert out["stdout"] == "small"
    assert out["truncated"] == ["stderr"]


def test_output_exactly_at_the_limit_is_not_cut(ci):
    exact = "y" * ci.MAX_OUTPUT_CHARS
    out = _run(ci, FakeClient([_result(exact, "", 0)]), language="python", code="x")
    assert out["stdout"] == exact
    assert "truncated" not in out and "note" not in out


def test_code_over_the_limit_is_refused_before_any_session_starts(ci):
    fake = FakeClient()
    out = _run(ci, fake, language="python", code="#" * (ci.MAX_CODE_BYTES + 1))
    assert "the limit is" in out["error"]
    assert (fake.started, fake.stopped, fake.invokes) == (0, 0, [])


def test_code_exactly_at_the_limit_is_accepted(ci):
    fake = FakeClient()
    out = _run(ci, fake, language="python", code="#" * ci.MAX_CODE_BYTES)
    assert "error" not in out
    assert fake.started == 1


def test_the_size_limit_counts_bytes_not_characters(ci):
    fake = FakeClient()
    # 30,000 characters, 60,000 bytes.
    out = _run(ci, fake, language="python", code="é" * 30_000)
    assert "the limit is" in out["error"]
    assert fake.started == 0


# --- bad input starts nothing --------------------------------------------------------------

@pytest.mark.parametrize("args, fragment", [
    ({"code": "1"}, "`language` is required"),
    ({"language": "", "code": "1"}, "`language` is required"),
    ({"language": None, "code": "1"}, "`language` is required"),
    ({"language": 7, "code": "1"}, "`language` is required"),
    ({"language": "python"}, "`code` is required"),
    ({"language": "python", "code": ""}, "`code` is required"),
    ({"language": "python", "code": "   \n"}, "`code` is required"),
    ({"language": "python", "code": 5}, "`code` is required"),
    ({"language": "ruby", "code": "puts 1"}, "unsupported language"),
    ({"language": "bash", "code": "ls"}, "unsupported language"),
])
def test_a_missing_or_bad_argument_is_refused_without_starting_a_session(ci, args, fragment):
    fake = FakeClient()
    out = _run(ci, fake, **args)
    assert fragment in out["error"]
    assert (fake.started, fake.stopped, fake.invokes) == (0, 0, [])


def test_a_bad_language_names_the_languages_that_work(ci):
    out = _run(ci, FakeClient(), language="ruby", code="1")
    assert out["supportedLanguages"] == ["python", "javascript", "typescript"]


# --- the Gateway entry point ---------------------------------------------------------------

def _context(tool):
    return SimpleNamespace(client_context=SimpleNamespace(
        custom={"bedrockAgentCoreToolName": f"sandbox___{tool}"}))


def test_the_gateway_names_the_tool_in_the_client_context(ci):
    ci._client = FakeClient([_result("7\n", "", 0)])
    out = ci.lambda_handler({"language": "python", "code": "print(7)"}, _context("execute_code"))
    assert out["stdout"] == "7\n"


def test_an_unknown_tool_is_named_not_guessed_at(ci):
    fake = FakeClient()
    ci._client = fake
    out = ci.lambda_handler({"language": "python", "code": "1"}, _context("delete_everything"))
    assert "unknown tool" in out["error"]
    assert out["availableTools"] == ["execute_code"]
    assert fake.started == 0


def test_a_direct_invoke_without_a_client_context_runs_the_one_tool(ci):
    ci._client = FakeClient()
    out = ci.lambda_handler({"language": "python", "code": "1"}, None)
    assert "error" not in out


def test_the_interpreter_is_not_taken_from_the_environment(ci, monkeypatch):
    """The role grants one interpreter; letting config name another could only fail."""
    monkeypatch.setenv("CODE_INTERPRETER_ID", "someone-elses")
    fake = FakeClient()
    _run(ci, fake, language="python", code="1")
    assert fake.start_kwargs["codeInterpreterIdentifier"] == "aws.codeinterpreter.v1"


# --- IAM: three actions on one interpreter, the same on both IaC paths ---------------------

TF = (ORCH / "terraform" / "tools.tf").read_text()
TS = (ORCH / "cdk" / "lib" / "tool-plane.ts").read_text()


def _tf_policy_block():
    start = TF.index('resource "aws_iam_role_policy" "tool_lambda_code_interpreter"')
    end = TF.index('\nresource "', start + 10)
    return TF[start:end]


def _ts_ci_block():
    start = TS.index("if (isCodeInterpreter) {")
    return TS[start:TS.index("} else {", start)]


def _quoted(text):
    return re.findall(r'"([^"\n]+)"', text)


def test_terraform_grants_exactly_three_actions_on_the_system_interpreter():
    block = _tf_policy_block()
    actions = {a for a in _quoted(block) if a.startswith("bedrock-agentcore:")}
    assert actions == ACTIONS
    assert "bedrock-agentcore:*" not in block
    resources = ARN_RE.findall(block)
    assert len(resources) == 1
    assert resources[0].endswith(":aws:code-interpreter/aws.codeinterpreter.v1")
    # No Resource anywhere in this policy is a bare wildcard.
    assert not re.search(r'Resource\s*=\s*(\[\s*)?"\*"', block)


def test_terraform_attaches_the_policy_to_the_code_interpreter_function_and_only_it():
    block = _tf_policy_block()
    assert "for_each = local.builtin_lambda_code_interpreter_tools" in block
    assert re.search(r'builtin_lambda_code_interpreter_tools\s*=\s*\{ for n, t in local\.builtin_lambda_tools : '
                     r'n => t if t\.source == "codeInterpreter" \}', TF)
    assert "role     = aws_iam_role.tool_lambda[each.key].id" in block


def test_terraform_log_access_is_to_its_own_group_and_cannot_create_groups():
    block = _tf_policy_block()
    log_actions = {a for a in _quoted(block) if a.startswith("logs:")}
    assert log_actions == {"logs:CreateLogStream", "logs:PutLogEvents"}
    assert 'aws_cloudwatch_log_group.tool[each.key].arn' in block


def test_cdk_grants_exactly_three_actions_on_the_system_interpreter():
    block = _ts_ci_block()
    actions = {a for a in _quoted(block) if a.startswith("bedrock-agentcore:")}
    assert actions == ACTIONS
    assert "bedrock-agentcore:*" not in block
    assert len(re.findall(r"arn:aws:bedrock-agentcore:\$\{[^}]*\}:aws:code-interpreter/aws\.codeinterpreter\.v1", block)) == 1
    assert 'resources: ["*"]' not in block


def test_both_paths_name_the_same_interpreter_and_actions():
    tf_actions = {a for a in _quoted(_tf_policy_block()) if a.startswith("bedrock-agentcore:")}
    ts_actions = {a for a in _quoted(_ts_ci_block()) if a.startswith("bedrock-agentcore:")}
    assert tf_actions == ts_actions
    handler = HANDLER_PATH.read_text()
    assert 'INTERPRETER = "aws.codeinterpreter.v1"' in handler
    assert TF.count(":aws:code-interpreter/aws.codeinterpreter.v1") >= 1
    assert TS.count(":aws:code-interpreter/aws.codeinterpreter.v1") >= 1


def test_both_paths_give_the_function_the_same_timeout_and_no_environment():
    assert 'timeout     = contains(["codeInterpreter", "browser"], each.value.source) ? 60 : 30' in TF
    assert 'cdk.Duration.seconds(isCodeInterpreter ? 60 : 30)' in TS
    # 60 s must leave room for the 30 s run plus starting and stopping a session.
    handler = HANDLER_PATH.read_text()
    assert "EXEC_TIMEOUT_SECONDS = 30" in handler


def test_the_pricing_function_keeps_its_policy_name_and_address():
    """An upgrade from v1.2 must plan no change to the pricing function's policy."""
    assert 'resource "aws_iam_role_policy" "tool_lambda" {' in TF
    assert 'name     = "ToolLambdaPricingRead"' in TF
    assert "for_each = local.builtin_lambda_pricing_tools" in TF


def test_the_source_is_in_the_vocabulary_and_the_shipped_sample_does_not_use_it():
    import json  # noqa: PLC0415
    vocab = json.loads((ORCH / "app" / "vocabulary.json").read_text())
    assert vocab["builtinLambdaSource"]["values"] == ["pricing", "codeInterpreter", "browser"]
    workflow = json.loads((ORCH / "app" / "workflow.json").read_text())
    sources = {t.get("source") for t in workflow["tools"].values()}
    assert "codeInterpreter" not in sources, "the shipped sample must not enable the sandbox"
    assert (ORCH / "app" / "tools" / "codeInterpreter" / "handler.py").is_file()
