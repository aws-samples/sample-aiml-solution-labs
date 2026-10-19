"""
Gateway Lambda target: run code in an AgentCore Code Interpreter sandbox, as a tool.

This is the second BUILT-IN `type: "lambda"` function (`source: "codeInterpreter"`),
next to `app/tools/pricing/`. It publishes ONE tool:

  execute_code(language, code) - run `code` and return its stdout, stderr and exit status.

WHAT THIS EXECUTES
Code a MODEL wrote, or that a person pasted into a prompt. That is the whole point of
the tool and also its risk, so the facts are here rather than only in the docs:

  * It runs in the AWS-managed interpreter `aws.codeinterpreter.v1`: an isolated
    sandbox AWS operates, with no access to this deployment's tables, buckets,
    secrets or credentials. This function's own role cannot reach any of them either;
    it may only start, use and stop a session on that one interpreter.
  * The system interpreter's network setting is AWS's default for it, not something
    this function chooses. The framework does not restrict or widen it.
  * Nothing persists. Every call starts a session, runs once and stops it (see
    `execute_code`), so one call cannot see another's variables or files.

Hard limits live in this file, not in a prompt: the code size, the run time and the
size of what comes back. A model cannot argue its way past a constant.

Invocation contract (identical to app/tools/pricing/handler.py): the Gateway passes the
tool arguments as `event`, and the tool name as
`context.client_context.custom["bedrockAgentCoreToolName"]`, formatted
"<targetName>___<toolName>".
"""

import os
import time

import boto3
from botocore.config import Config
from botocore.exceptions import ConnectTimeoutError, ReadTimeoutError

REGION = os.environ.get("AWS_REGION", "us-east-1")

# The AWS-managed interpreter, which every account has and nothing has to provision.
# A constant and not an environment variable on purpose: the execution role grants
# access to exactly this interpreter (terraform/tools.tf, cdk/lib/tool-plane.ts), so
# a different value could only ever be refused.
INTERPRETER = "aws.codeinterpreter.v1"

# What the interpreter can run (the service's ProgrammingLanguage enum).
LANGUAGES = ("python", "javascript", "typescript")

# --- Hard limits ------------------------------------------------------------
MAX_CODE_BYTES = 50_000          # of UTF-8, so a multi-byte script is not undercounted
EXEC_TIMEOUT_SECONDS = 30        # for the run itself; the Lambda's own timeout is 60
MAX_OUTPUT_CHARS = 10_000        # per stream (stdout and stderr each)
# How long the service keeps a session whose stop call failed. The finally below
# stops it in every normal case; this only bounds the abnormal one.
SESSION_TTL_SECONDS = 120

_client = boto3.client(
    "bedrock-agentcore",
    region_name=REGION,
    # NO RETRIES. A retried invoke runs the code a second time, and code is not
    # idempotent: a script that appends to a file or calls an API would act twice.
    # (`total_max_attempts` counts the first try; `max_attempts` would not.)
    # read_timeout bounds a run that produces no output at all; the deadline in
    # `_run` bounds one that keeps printing.
    config=Config(retries={"total_max_attempts": 1}, connect_timeout=5,
                  read_timeout=EXEC_TIMEOUT_SECONDS),
)


def _tool_name(context) -> str:
    """The tool the Gateway is asking for, minus the "<target>___" prefix."""
    try:
        full = context.client_context.custom.get("bedrockAgentCoreToolName", "")
    except Exception:  # noqa: BLE001 - a direct test invoke has no client_context
        return ""
    return full.rsplit("___", 1)[-1] if full else ""


def _cut(text: str, name: str, cut: list[str]) -> str:
    """`text` limited to MAX_OUTPUT_CHARS, recording `name` in `cut` when it was cut."""
    if len(text) <= MAX_OUTPUT_CHARS:
        return text
    cut.append(name)
    return text[:MAX_OUTPUT_CHARS]


def _run(sid: str, language: str, code: str) -> dict:
    """Run `code` in session `sid`; read the result stream until it ends or time is up.

    Returns {"stdout", "stderr", "exitCode", "isError", "timedOut"}.
    """
    deadline = time.monotonic() + EXEC_TIMEOUT_SECONDS
    out: list[str] = []
    err: list[str] = []
    exit_code = None
    is_error = False
    timed_out = False
    try:
        resp = _client.invoke_code_interpreter(
            codeInterpreterIdentifier=INTERPRETER, sessionId=sid, name="executeCode",
            # clearContext: a fresh interpreter state, so nothing from a previous
            # call in the same session (there is never one) could leak into this.
            arguments={"language": language, "code": code, "clearContext": True})
        for event in resp.get("stream", []):
            result = event.get("result") or {}
            is_error = is_error or bool(result.get("isError"))
            structured = result.get("structuredContent") or {}
            if structured:
                out.append(str(structured.get("stdout") or ""))
                err.append(str(structured.get("stderr") or ""))
                if structured.get("exitCode") is not None:
                    exit_code = structured["exitCode"]
            else:
                # A result with only text blocks: that text is the output.
                out.extend(c.get("text", "") for c in result.get("content") or []
                           if c.get("type") == "text")
            if time.monotonic() > deadline:
                timed_out = True
                break
    except (ReadTimeoutError, ConnectTimeoutError):
        timed_out = True
    return {"stdout": "".join(out), "stderr": "".join(err), "exitCode": exit_code,
            "isError": is_error, "timedOut": timed_out}


def execute_code(args: dict) -> dict:
    """Run code in a fresh sandbox session and say what happened."""
    language = args.get("language")
    code = args.get("code")
    # Validated BEFORE a session starts: a refused call must cost nothing.
    if not isinstance(language, str) or not language.strip():
        return {"error": "`language` is required.", "supportedLanguages": list(LANGUAGES)}
    language = language.strip().lower()
    if language not in LANGUAGES:
        return {"error": f"unsupported language {language!r}.",
                "supportedLanguages": list(LANGUAGES)}
    if not isinstance(code, str) or not code.strip():
        return {"error": "`code` is required: the program to run."}
    size = len(code.encode("utf-8"))
    if size > MAX_CODE_BYTES:
        return {"error": f"`code` is {size} bytes; the limit is {MAX_CODE_BYTES}. "
                         "Send a shorter program."}

    started = time.monotonic()
    sid = _client.start_code_interpreter_session(
        codeInterpreterIdentifier=INTERPRETER, name="ax-execute-code",
        sessionTimeoutSeconds=SESSION_TTL_SECONDS)["sessionId"]
    try:
        ran = _run(sid, language, code)
    finally:
        # ALWAYS, including when the run raised: a session left open is billed until
        # it expires, and one that is still running code is how a runaway loop
        # outlives its timeout. Stopping it is also what ends that code.
        try:
            _client.stop_code_interpreter_session(
                codeInterpreterIdentifier=INTERPRETER, sessionId=sid)
        except Exception as e:  # noqa: BLE001 - it expires by itself (SESSION_TTL_SECONDS)
            print(f"[codeInterpreter] stop session failed: {type(e).__name__}: {e}")

    cut: list[str] = []
    result = {
        "language": language,
        "exitCode": ran["exitCode"],
        "stdout": _cut(ran["stdout"], "stdout", cut),
        "stderr": _cut(ran["stderr"], "stderr", cut),
        "seconds": round(time.monotonic() - started, 2),
    }
    if cut:
        result["truncated"] = cut
        result["note"] = (f"{' and '.join(cut)} cut at {MAX_OUTPUT_CHARS} characters: "
                          "print less, or print a summary instead of the data.")
    if ran["timedOut"]:
        result["error"] = (f"timed out after {EXEC_TIMEOUT_SECONDS} seconds and was "
                           "stopped. Output up to that point is included.")
    elif ran["isError"] or (ran["exitCode"] not in (None, 0)):
        # The sandbox reporting that the PROGRAM failed (an exception, a non-zero
        # exit). That is a normal outcome of running model-written code, returned as
        # an error result so the model can read the traceback in `stderr` and fix it.
        result["error"] = (f"the code failed (exit status {ran['exitCode']}). "
                           "See `stderr`.")
    return result


# Tool name -> implementation. `call` in workflow.json says which one the bound agent invokes.
_TOOLS = {"execute_code": execute_code}


def lambda_handler(event, context):
    tool = _tool_name(context) or ("execute_code" if len(_TOOLS) == 1 else "")
    fn = _TOOLS.get(tool)
    if fn is None:
        return {"error": f"unknown tool {tool!r}", "availableTools": sorted(_TOOLS)}
    try:
        return fn(event or {})
    except Exception as e:  # noqa: BLE001 - surface the reason, never a fake answer
        print(f"[codeInterpreter] {tool} failed: {type(e).__name__}: {e}")
        return {"error": f"{tool} failed: {type(e).__name__}: {e}"}
