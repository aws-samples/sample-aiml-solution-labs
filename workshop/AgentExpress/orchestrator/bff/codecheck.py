"""Checks for a tool whose function is written in the build (tools.<key>.code).

Before a deploy, in three layers, all reported file by file and line by line:

  * static — each file parses; handler.py defines lambda_handler(event, context); the
    requirements are plain `name==version` lines (no URLs, no other index, no editable
    installs: nothing the build could pull from somewhere unreviewed); no AWS access key
    or private key is written into the code. These are ERRORS: a deploy is refused.
  * lint and security — an import nothing uses; eval/exec; a shell command; unpickling;
    yaml.load without a safe loader; TLS verification turned off; a hard-coded password
    or token; plain http://; an unpinned requirement; with several tools, a handler that
    never reads which one was called. WARNINGS: shown, never blocking.
  * a sandbox run — the handler is run on each test event in an AgentCore Code Interpreter
    session (a managed, isolated sandbox with no access to this console or the build's
    account), with the environment the function will have. What it returns, what it
    raised and how long it took come back per event. Requirements are not installed
    there, so a missing import is reported as "installed at deploy", not as a failure.

After a deploy, `invoke` calls the deployed function with one event, the way the Gateway
does (the tool's name in the client context), in the build's own account.
"""

from __future__ import annotations

import ast
import base64
import json
import os
import re
import time

import boto3

REGION = os.environ.get("AWS_REGION", "us-east-1")
#: The AWS-managed Code Interpreter: every account has it, nothing to provision.
INTERPRETER = os.environ.get("CODE_INTERPRETER_ID", "aws.codeinterpreter.v1")
FILE_RE = re.compile(r"^([a-z_][a-z0-9_]*\.py|requirements\.txt|events\.json)$")
MAX_FILES = 20
MAX_BYTES = 500_000
MAX_EVENTS = 10
OUTPUT_MAX = 4000
#: A requirement the deploy installs: a name, extras, and an optional version constraint.
REQ_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*(\[[A-Za-z0-9,._-]+\])?\s*((==|>=|<=|~=|!=|<|>)\s*[A-Za-z0-9.*+!-]+"
                    r"(\s*,\s*(==|>=|<=|~=|!=|<|>)\s*[A-Za-z0-9.*+!-]+)*)?$")
AWS_KEY_RE = re.compile(r"(?<![A-Z0-9])(AKIA|ASIA)[A-Z0-9]{16}(?![A-Z0-9])")
PRIVATE_KEY_RE = re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----")
SECRET_ASSIGN_RE = re.compile(r"(?i)\b(password|passwd|secret|token|api_?key)\b\s*[:=]\s*[\"'][^\"'\s]{8,}[\"']")
TOOL_NAME_KEY = "bedrockAgentCoreToolName"
RESULT_MARK = "__AX_RESULT__"


def _issue(severity: str, file: str, line: int, message: str) -> dict:
    return {"severity": severity, "file": file, "line": line, "message": message}


def _dotted(node) -> str:
    parts = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return ".".join(reversed(parts))


def _lint(name: str, tree: ast.AST, source: str) -> list[dict]:
    out = []
    imported: dict[str, int] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                imported[(a.asname or a.name).split(".")[0]] = node.lineno
        elif isinstance(node, ast.ImportFrom):
            for a in node.names:
                if a.name != "*":
                    imported[a.asname or a.name] = node.lineno
    used = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
    exported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "__all__" for t in node.targets):
            exported |= {e.value for e in getattr(node.value, "elts", []) if isinstance(e, ast.Constant)}
    for n, line in sorted(imported.items(), key=lambda x: x[1]):
        if n not in used and n not in exported:
            out.append(_issue("warning", name, line, f"`{n}` is imported and never used"))
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        fn = _dotted(node.func)
        kwargs = {k.arg: k.value for k in node.keywords if k.arg}
        line = node.lineno
        if fn in ("eval", "exec", "compile"):
            out.append(_issue("warning", name, line, f"`{fn}` runs text as code: an input could run anything"))
        elif fn in ("os.system", "os.popen") or (fn.startswith("subprocess.") and isinstance(
                kwargs.get("shell"), ast.Constant) and kwargs["shell"].value is True):
            out.append(_issue("warning", name, line, f"`{fn}` runs a shell command: never build one from an input"))
        elif fn in ("pickle.load", "pickle.loads", "marshal.loads", "shelve.open"):
            out.append(_issue("warning", name, line, f"`{fn}` can run code hidden in the data it reads"))
        elif fn == "yaml.load" and "Loader" not in kwargs:
            out.append(_issue("warning", name, line,
                              "`yaml.load` without a Loader can build any object: use yaml.safe_load"))
        elif fn == "tempfile.mktemp":
            out.append(_issue("warning", name, line, "`tempfile.mktemp` is racy: use tempfile.mkstemp"))
        v = kwargs.get("verify")
        if isinstance(v, ast.Constant) and v.value is False:
            out.append(_issue("warning", name, line, "`verify=False` turns TLS certificate checks off"))
    for i, text in enumerate(source.splitlines(), 1):
        if SECRET_ASSIGN_RE.search(text):
            out.append(_issue("warning", name, i, "looks like a password or token written into the code: "
                                                  "grant a secret and read it from SECRET_NAME instead"))
        if re.search(r"[\"']http://(?!localhost|127\.0\.0\.1)", text):
            out.append(_issue("warning", name, i, "a plain http:// address: its traffic is not encrypted"))
    return out


def static(files, tool_names: list[str]) -> list[dict]:
    """Every problem with a code tool's files, errors first."""
    if not isinstance(files, dict):
        return [_issue("error", "handler.py", 0, "the tool has no files")]
    out = []
    for n in files:
        if not FILE_RE.match(str(n)):
            out.append(_issue("error", str(n), 0,
                              "not a file a code tool may hold: *.py, requirements.txt, events.json"))
        elif not isinstance(files[n], str):
            out.append(_issue("error", str(n), 0, "is not text"))
    if len(files) > MAX_FILES:
        out.append(_issue("error", "", 0, f"at most {MAX_FILES} files"))
    if sum(len(str(b).encode()) for b in files.values()) > MAX_BYTES:
        out.append(_issue("error", "", 0, f"the files are over {MAX_BYTES // 1000} KB together"))
    handler = files.get("handler.py")
    if not isinstance(handler, str) or not handler.strip():
        out.append(_issue("error", "handler.py", 0, "is missing: it holds lambda_handler(event, context)"))
    for name, body in files.items():
        if not isinstance(body, str):
            continue
        for i, text in enumerate(body.splitlines(), 1):
            if AWS_KEY_RE.search(text):
                out.append(_issue("error", name, i, "an AWS access key is written here: remove it, and "
                                                   "deactivate it — the function's role is its credentials"))
            if PRIVATE_KEY_RE.search(text):
                out.append(_issue("error", name, i, "a private key is written here: put it in a secret instead"))
        if not name.endswith(".py"):
            continue
        try:
            tree = ast.parse(body, filename=name)
        except SyntaxError as e:
            out.append(_issue("error", name, e.lineno or 0, f"does not parse: {e.msg}"))
            continue
        if name == "handler.py":
            fn = next((n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
                       and n.name == "lambda_handler"), None)
            if fn is None:
                out.append(_issue("error", name, 0, "has no top-level `def lambda_handler(event, context)`"))
            elif isinstance(fn, ast.AsyncFunctionDef):
                out.append(_issue("error", name, fn.lineno,
                                  "lambda_handler must not be async: Lambda calls it directly"))
            elif len(fn.args.args) + len(fn.args.posonlyargs) != 2 and not fn.args.vararg:
                out.append(_issue("error", name, fn.lineno, "lambda_handler takes exactly (event, context)"))
            if len(tool_names) > 1 and TOOL_NAME_KEY not in body:
                out.append(_issue("warning", name, fn.lineno if fn else 0,
                                  f"it publishes {len(tool_names)} tools but never reads which one was called: "
                                  f"context.client_context.custom['{TOOL_NAME_KEY}'] is '<key>___<tool>'"))
        out += _lint(name, tree, body)
    req = files.get("requirements.txt")
    for i, line in enumerate((req or "").splitlines() if isinstance(req, str) else [], 1):
        text = line.split("#", 1)[0].strip()
        if not text:
            continue
        if text.startswith(("-", ".", "/")) or "://" in text or "@" in text:
            out.append(_issue("error", "requirements.txt", i, "only `name==version` lines: no options, URLs, "
                                                             "paths or other indexes"))
        elif not REQ_RE.match(text):
            out.append(_issue("error", "requirements.txt", i, f"`{text}` is not a requirement pip reads"))
        else:
            if "==" not in text:
                out.append(_issue("warning", "requirements.txt", i, f"`{text}` is not pinned: pin it (==) so "
                                                                   "every deploy installs the same code"))
            if re.match(r"(?i)^(boto3|botocore)\b", text):
                out.append(_issue("warning", "requirements.txt", i, "Lambda's Python already has boto3 and botocore"))
    events = files.get("events.json")
    if isinstance(events, str) and events.strip():
        try:
            parsed = json.loads(events)
            if not isinstance(parsed, list) or not all(isinstance(e, dict) for e in parsed):
                raise ValueError
        except ValueError:
            out.append(_issue("error", "events.json", 0, 'must be a list of test events: [{"name": "...", '
                                                         '"tool": "<toolName>", "event": {...}}]'))
    return sorted(out, key=lambda i: (i["severity"] != "error", i["file"], i["line"]))


def events_of(files) -> list[dict]:
    try:
        parsed = json.loads((files or {}).get("events.json") or "[]")
    except ValueError:
        return []
    return [e for e in parsed if isinstance(e, dict)][:MAX_EVENTS] if isinstance(parsed, list) else []


def environment_of(key: str, code: dict) -> dict:
    """What the function will find in os.environ (mirrors codeToolEnvironment)."""
    g = (code or {}).get("grants") or {}
    env = {k: str(v) for k, v in ((code or {}).get("environment") or {}).items()}
    for name, grant in (("SECRET_NAME", "secret"), ("TABLE_NAME", "table"), ("S3_PREFIX", "s3Prefix")):
        if g.get(grant):
            env[name] = str(g[grant])
    return env


def _harness(key: str, events: list[dict], default_tool: str, env: dict) -> str:
    """Runs in the sandbox: import the handler from tool/, call it per event."""
    return f'''
import json, os, sys, time, traceback, types
os.environ.update(json.loads({json.dumps(json.dumps(env))}))
sys.path.insert(0, "tool")
results, handler = [], None
try:
    import handler
except Exception as e:
    print({RESULT_MARK!r} + json.dumps({{"import": f"{{type(e).__name__}}: {{e}}"}}))
for ev in ([] if handler is None else json.loads({json.dumps(json.dumps(events))})):
    tool = ev.get("tool") or {json.dumps(default_tool)}
    ctx = types.SimpleNamespace(function_name="ToolLambda-sandbox-{key}", aws_request_id="sandbox",
        memory_limit_in_mb=256, get_remaining_time_in_millis=lambda: 30000,
        client_context=types.SimpleNamespace(custom={{"{TOOL_NAME_KEY}": "{key}___" + tool}}))
    t = time.monotonic()
    try:
        out = handler.lambda_handler(ev.get("event") or {{}}, ctx)
        results.append({{"name": ev.get("name") or tool, "ok": True,
                         "output": json.dumps(out, default=str)[:{OUTPUT_MAX}],
                         "ms": round((time.monotonic() - t) * 1000)}})
    except Exception as e:
        results.append({{"name": ev.get("name") or tool, "ok": False, "error": f"{{type(e).__name__}}: {{e}}",
                         "trace": traceback.format_exc()[-1500:], "ms": round((time.monotonic() - t) * 1000)}})
if handler is not None:
    print({RESULT_MARK!r} + json.dumps({{"results": results}}))
'''


def _stream_text(resp) -> tuple[str, bool]:
    text, error = [], False
    for ev in resp.get("stream", []):
        r = ev.get("result") or {}
        error = error or bool(r.get("isError"))
        sc = r.get("structuredContent") or {}
        if sc.get("stdout") is not None or sc.get("stderr"):
            text += [str(sc.get("stdout") or ""), str(sc.get("stderr") or "")]
        else:
            text += [c.get("text", "") for c in r.get("content") or [] if c.get("type") == "text"]
    return "\n".join(text), error


def sandbox(key: str, files: dict, code: dict, tool_names: list[str], client=None) -> dict:
    """Run the handler on each test event in a Code Interpreter session."""
    events = events_of(files)
    if not events:
        return {"ran": False, "note": "No test events: add some to events.json to run it in the sandbox."}
    client = client or boto3.client("bedrock-agentcore", region_name=REGION)
    started = time.monotonic()
    sid = client.start_code_interpreter_session(codeInterpreterIdentifier=INTERPRETER, name=f"ax-check-{key}"[:48],
                                                sessionTimeoutSeconds=120)["sessionId"]
    try:
        content = [{"path": f"tool/{n}", "text": b} for n, b in files.items()
                   if n.endswith(".py") and isinstance(b, str)]
        _stream_text(client.invoke_code_interpreter(codeInterpreterIdentifier=INTERPRETER, sessionId=sid,
                                                    name="writeFiles", arguments={"content": content}))
        text, _ = _stream_text(client.invoke_code_interpreter(
            codeInterpreterIdentifier=INTERPRETER, sessionId=sid, name="executeCode",
            arguments={"language": "python", "clearContext": True,
                       "code": _harness(key, events, tool_names[0] if tool_names else "", environment_of(key, code))}))
    finally:
        try:
            client.stop_code_interpreter_session(codeInterpreterIdentifier=INTERPRETER, sessionId=sid)
        except Exception as e:  # noqa: BLE001 - it times out by itself
            print(f"[codecheck] stop session: {type(e).__name__}: {e}")
    line = next((ln for ln in text.splitlines() if ln.startswith(RESULT_MARK)), "")
    if not line:
        return {"ran": False, "note": "The sandbox did not finish the run.", "output": text[-OUTPUT_MAX:]}
    got, _ = json.JSONDecoder().raw_decode(line[len(RESULT_MARK):])
    ms = round((time.monotonic() - started) * 1000)
    if "import" in got:
        missing = re.match(r"ModuleNotFoundError: No module named '([^']+)'", got["import"])
        if missing:
            return {"ran": False, "ms": ms, "note": f"{missing.group(1)} is not in the sandbox. It is installed from "
                    "requirements.txt when the build deploys: test it then, with Test tool."}
        return {"ran": False, "ms": ms, "note": f"handler.py failed to import: {got['import']}"}
    return {"ran": True, "ms": ms, "results": got["results"],
            "note": "Run in an isolated sandbox with no AWS credentials: a call to AWS fails here and works "
                    "once deployed, with the function's own role."}


def check(key: str, files, code: dict, tool_names: list[str], run: bool = True, client=None) -> dict:
    problems = static(files, tool_names)
    errors = [p for p in problems if p["severity"] == "error"]
    result: dict = {"problems": problems, "ok": not errors}
    if run and not errors:
        try:
            result["sandbox"] = sandbox(key, files, code, tool_names, client)
        except Exception as e:  # noqa: BLE001 - the static checks still stand
            print(f"[codecheck] sandbox: {type(e).__name__}: {e}")
            result["sandbox"] = {"ran": False, "note": f"The sandbox could not run it just now ({type(e).__name__})."}
    return result


def invoke(lambda_client, fn_name: str, key: str, tool: str, event: dict) -> dict:
    """Call a deployed code tool as the Gateway would, and say what came back."""
    ctx = base64.b64encode(json.dumps({"custom": {TOOL_NAME_KEY: f"{key}___{tool}"}}).encode()).decode()
    t = time.monotonic()
    resp = lambda_client.invoke(FunctionName=fn_name, Payload=json.dumps(event).encode(), LogType="Tail",
                                ClientContext=ctx)
    body = resp["Payload"].read().decode(errors="replace")
    log = base64.b64decode(resp.get("LogResult") or "").decode(errors="replace")
    return {"ok": not resp.get("FunctionError"), "status": resp.get("StatusCode"),
            "error": resp.get("FunctionError") or "", "output": body[:OUTPUT_MAX],
            "log": log[-OUTPUT_MAX:], "ms": round((time.monotonic() - t) * 1000)}
