"""TaskAgent — a step with no model (workflow.json `runtime: "task"` and a `task` block).

A task sits in `steps` like any agent (alone, in a parallel group or a sequence, gated or
not, re-run from a gate), runs in the orchestrator's own runtime, and returns JSON that a
later agent reads with ctx.input(<id>). It never calls a model. Four kinds:

  tool     call one of the build's tools through the Gateway, with `arguments` built from
           the run (app/common/task_template.py). The Cedar policy, interceptors and a
           tool's `approval` apply exactly as for an agent's call.
  code     run `code` in the AWS-managed AgentCore Code Interpreter sandbox
           (aws.codeinterpreter.v1). The script finds the request in in/request.txt,
           earlier outputs in in/outputs.json and, with `attachments`, the run's files in
           in/. What it prints is the output (JSON when it parses); what it writes to out/
           is kept with the run (runs/<session>/outputs/), and the script and its log under
           runs/<session>/code/ (app/common/artifacts.py).
  extract  read the run's PDFs and images with Amazon Textract: text, tables and form
           fields as JSON. Images go to AnalyzeDocument; a PDF goes to the asynchronous
           StartDocumentAnalysis, because the synchronous call reads one page only
           (https://docs.aws.amazon.com/textract/latest/dg/limits-document.html).
  output   write a csv, json, markdown or txt file from earlier outputs (needs
           orchestrator.files, which keeps it).

The node wrapper (app/orchestrator/nodes.py) still emits the step's status, versions and
timeline lines and applies a guardrail the step asks for; memory is never recalled or
stored, because there is no model call to give it to.
"""

from __future__ import annotations

import asyncio
import base64
import csv
import hashlib
import io
import json
import os
import time

from app.common import artifacts, attachments, clock, task_template, vocabulary
from app.common.base import Agent

REGION = os.getenv("AWS_REGION", "us-east-1")

KINDS = vocabulary.values("taskKinds")
LANGUAGES = vocabulary.values("taskLanguages")
FORMATS = vocabulary.values("taskOutputFormats")
FEATURES = vocabulary.values("taskExtractFeatures")

#: The keys each kind reads; any other is refused (it would look like a setting and do nothing).
#: A code step's `codeSource` (git or S3) is read by the deployer, which deploys the script
#: as `code` in its place (deployer/code_fetch.py read_script, runner.materialise_code_sources).
KEYS_OF = {"tool": ("kind", "tool", "arguments"), "code": ("kind", "language", "code", "codeSource"),
           "extract": ("kind", "file", "features"), "output": ("kind", "format", "name", "from")}

# --- code -------------------------------------------------------------------------------
#: The AWS-managed interpreter every account has; the runtime role may use only this one.
INTERPRETER = "aws.codeinterpreter.v1"
MAX_CODE_BYTES = 50_000
#: A data-processing script may take a while; the session ends after SESSION_TTL_SECONDS
#: even if the stop call fails.
CODE_TIMEOUT_SECONDS = 120
SESSION_TTL_SECONDS = 300
#: What is kept of the script's output in the step's own output (the log keeps 1 MB).
MAX_STDOUT_CHARS = 50_000
#: Files read back from out/: how many, and how big each (artifacts caps it again).
MAX_OUT_FILES = 20
#: Where the script finds what it is given, and where it leaves files to keep.
IN_DIR, OUT_DIR = "in", "out"

# --- extract ----------------------------------------------------------------------------
#: What Textract reads (limits-document.html); TIFF is not among the files a run accepts.
EXTRACT_FORMATS = ("pdf", "png", "jpeg")
#: How long an asynchronous analysis may take before the step gives up, and the polling
#: steps. The docs advise an SNS notification for many documents; one step polls one job.
EXTRACT_TIMEOUT_SECONDS = 300
_POLL = (2, 3, 5, 5, 10)
#: Text kept per file in the output; the tables and fields are kept whole.
MAX_TEXT_CHARS = 50_000

_CONTENT_TYPES = {"csv": "text/csv", "json": "application/json", "markdown": "text/markdown",
                  "txt": "text/plain"}
_EXT = {"csv": "csv", "json": "json", "markdown": "md", "txt": "txt"}

_clients: dict = {}


class TaskFailed(RuntimeError):
    """The step's work failed (the script's own error, Textract's, a missing file)."""


def _client(service: str):
    if service not in _clients:
        import boto3
        from botocore.config import Config
        # NO RETRIES on the sandbox: a retried invoke runs the script twice, and a script
        # is not idempotent (same reason as app/tools/codeInterpreter/handler.py).
        config = (Config(retries={"total_max_attempts": 1}, connect_timeout=5,
                         read_timeout=CODE_TIMEOUT_SECONDS)
                  if service == "bedrock-agentcore" else Config(retries={"mode": "standard"}))
        _clients[service] = boto3.client(service, region_name=REGION, config=config)
    return _clients[service]


def _record(label: str, mode: str, latency_ms: int, query: str, result: str, ok: bool) -> None:
    """A `tool` telemetry row for the call, so Observability and the Flow tab show it ("<step>
    called codeInterpreter"). Not priced: the sandbox bills by session time and Textract by
    page, which no call here reports."""
    try:
        from app.features.observability import store
        from app.features.observability.meter import _next_seq, _truncate
        from app.features.observability.records import CallRecord
        from app.features.observability.scope import get_scope
        s = get_scope()
        store.put(CallRecord(session_id=s.session_id, agent_id=s.agent_id, user=s.user, kind="tool",
                             label=label, mode=mode if ok else "error",
                             latency_ms=int(latency_ms), seq=_next_seq(),
                             status="ok" if ok else "error", user_input=_truncate(query),
                             output_text=_truncate(result), version=s.version))
    except Exception as e:  # noqa: BLE001 - telemetry must never break a run
        print(f"[task] telemetry skipped: {type(e).__name__}: {e}")


def problem(agent_id: str, spec: dict, tools: dict) -> str:
    """The first thing that stops this task from running, or "". The runtime's own check;
    bff/task_check.py and taskCheck.ts check the same and more before a deploy."""
    task = spec.get("task")
    if not isinstance(task, dict):
        return f"agent {agent_id!r} has runtime \"task\" and needs a `task` block saying what it does"
    kind = task.get("kind")
    if kind not in KINDS:
        return f"agent {agent_id!r}: task.kind {kind!r} is not one of {', '.join(KINDS)}"
    extra = [k for k in task if k not in KEYS_OF[kind]]
    if extra:
        return f"agent {agent_id!r}: task.{extra[0]} does not apply to a {kind!r} step"
    if kind == "tool" and task.get("tool") not in (tools or {}):
        return f"agent {agent_id!r}: task.tool {task.get('tool')!r} is not a key in the `tools` block"
    if kind == "code":
        code = task.get("code")
        if "codeSource" in task and not isinstance(code, str):
            return (f"agent {agent_id!r}: task.codeSource was not fetched at deploy, so the step "
                    "has no script; deploy the build again")
        if not isinstance(code, str) or not code.strip():
            return f"agent {agent_id!r}: a code step needs task.code, the script to run"
        if len(code.encode("utf-8")) > MAX_CODE_BYTES:
            return f"agent {agent_id!r}: task.code is over {MAX_CODE_BYTES} bytes"
        if task.get("language", "python") not in LANGUAGES:
            return f"agent {agent_id!r}: task.language is not one of {', '.join(LANGUAGES)}"
    if kind == "extract" and any(f not in FEATURES for f in task.get("features") or []):
        return f"agent {agent_id!r}: task.features may name {', '.join(FEATURES)}"
    if kind == "output" and task.get("format") not in FORMATS:
        return f"agent {agent_id!r}: an output step needs task.format, one of {', '.join(FORMATS)}"
    return ""


def validate(agents: dict, tools: dict) -> None:
    """Refuse, at container start, a task that cannot run."""
    for agent_id, spec in (agents or {}).items():
        if str((spec or {}).get("runtime") or "") == "task":
            why = problem(agent_id, spec, tools)
        else:
            # Read only for runtime "task": anywhere else it would look like a setting.
            why = (f"agent {agent_id!r} has a `task` block but runtime "
                   f"{(spec or {}).get('runtime') or 'main'!r}; set runtime \"task\"") if "task" in (spec or {}) else ""
        if why:
            raise ValueError(why)


class TaskAgent(Agent):
    """Runs a `task` block. Configured by the registry like any agent, plus `task`."""

    # No model call, so nothing to give a recalled memory to, and no insight to store.
    recall_in_orchestrator = False
    store_in_orchestrator = False
    task: dict = None

    def use(self, task: dict) -> TaskAgent:
        """Take the workflow's `task` block (called by the registry after _configure)."""
        self.task = dict(task or {})
        # A tool step's tool is the one it is bound to, so ctx.tools journals a call that
        # waits for a person's approval (asks.journaling) as it does for an agent's.
        if self.task.get("kind") == "tool" and self.task.get("tool"):
            self.tools = [str(self.task["tool"])]
            self.tool = self.tools[0]
        return self

    async def run(self, ctx) -> str:
        task = dict(self.task or {})
        kind = task.get("kind")
        runner = {"tool": _run_tool, "code": _run_code, "extract": _run_extract,
                  "output": _run_output}.get(kind)
        if runner is None:
            raise TaskFailed(f"{self.id}: task.kind {kind!r} is not one of {', '.join(KINDS)}")
        result = await runner(self, ctx, task)
        from app.common.config import AGENTS
        produces = str((AGENTS.get(self.id) or {}).get("produces") or "").strip()
        envelope = {**({"assetType": produces} if produces else {}), "createdByAgent": self.id,
                    "createdAt": clock.now_et().isoformat(), "task": kind}
        return json.dumps({**envelope, **result}, indent=2, ensure_ascii=False, default=str)


def _data(ctx) -> dict:
    """What placeholders read: the request, the run's id and the outputs so far."""
    return {"request": ctx.topic or "", "session": ctx.session_id,
            "outputs": dict((ctx.state or {}).get("outputs") or {})}


# --- tool -------------------------------------------------------------------------------

async def _run_tool(agent: TaskAgent, ctx, task: dict) -> dict:
    from app.common import asks
    from app.common.errors import ToolDenied
    from app.features.gateway import client
    key = str(task.get("tool") or "")
    args = task_template.render(task.get("arguments") or {}, _data(ctx))
    if not isinstance(args, dict):
        raise TaskFailed(f"{agent.id}: task.arguments must give an object, got {type(args).__name__}")
    if asks.approval_of(key):
        # A tool that needs a person's approval waits for it, as for an agent's call.
        async def published() -> str:
            found = (await client.published_for([key])).get(key) or []
            return found[0].name if len(found) == 1 else ""
        name = await asks.once(ctx, published)
        if name:
            ok, told = await asks.approve(ctx, key, name, args)
            if not ok:
                raise ToolDenied(told, by="person")
    await ctx.log(f"{agent.name}: calling {key}")
    # Journaled, so a run that waited for an approval does not call the tool twice.
    data, text = await asks.once(ctx, lambda: client.invoke_task(key, args))
    return {"tool": key, "arguments": args, "result": task_template.parsed(data)
            if isinstance(data, str) else data, "text": text}


# --- code -------------------------------------------------------------------------------

def _invoke(sid: str, name: str, arguments: dict) -> dict:
    """One InvokeCodeInterpreter call, its stream read to the end: {stdout, stderr,
    exitCode, isError, content}. Raises on an exception event in the stream."""
    resp = _client("bedrock-agentcore").invoke_code_interpreter(
        codeInterpreterIdentifier=INTERPRETER, sessionId=sid, name=name, arguments=arguments)
    out, err, content = [], [], []
    exit_code, is_error = None, False
    for event in resp.get("stream", []):
        for problem_key in ("accessDeniedException", "conflictException", "internalServerException",
                            "resourceNotFoundException", "serviceQuotaExceededException",
                            "throttlingException", "validationException"):
            if event.get(problem_key):
                raise TaskFailed(f"Code Interpreter {name}: {problem_key}: "
                                 f"{(event[problem_key] or {}).get('message', '')}")
        result = event.get("result") or {}
        is_error = is_error or bool(result.get("isError"))
        sc = result.get("structuredContent") or {}
        if sc:
            out.append(str(sc.get("stdout") or ""))
            err.append(str(sc.get("stderr") or ""))
            if sc.get("exitCode") is not None:
                exit_code = sc["exitCode"]
        blocks = result.get("content") or []
        content.extend(blocks)
        if not sc:
            out.extend(str(b.get("text") or "") for b in blocks if b.get("type") == "text")
    return {"stdout": "".join(out), "stderr": "".join(err), "exitCode": exit_code,
            "isError": is_error, "content": content}


def _listed(content: list) -> list[str]:
    """The file paths a listFiles answer names, under out/ (its blocks carry `uri` or
    `name`; a resource block its `resource.uri`)."""
    paths = []
    for b in content or []:
        raw = str(b.get("uri") or (b.get("resource") or {}).get("uri") or b.get("name") or "")
        path = raw.split("://", 1)[-1].lstrip("/")
        at = path.find(f"{OUT_DIR}/")
        if at >= 0:
            path = path[at:]
        elif "/" not in path and path:
            path = f"{OUT_DIR}/{path}"
        if path.startswith(f"{OUT_DIR}/") and not path.endswith(("/", "/.keep")):
            paths.append(path)
    return list(dict.fromkeys(paths))[:MAX_OUT_FILES]


def _read_back(content: list) -> list[dict]:
    """The files a readFiles answer holds: [{name, bytes, type}]. A block carries its file
    as `resource` (text or base64 blob) or, flat, as `text` / `data`."""
    out = []
    for b in content or []:
        res = b.get("resource") or {}
        uri = str(res.get("uri") or b.get("uri") or b.get("name") or "")
        if res.get("blob") is not None or b.get("data") is not None:
            raw = res.get("blob") if res.get("blob") is not None else b.get("data")
            data = raw if isinstance(raw, (bytes, bytearray)) else base64.b64decode(raw or b"")
        elif res.get("text") is not None or (b.get("type") == "resource" and b.get("text") is not None):
            data = str(res.get("text") if res.get("text") is not None else b.get("text")).encode("utf-8")
        else:
            continue
        name = uri.rsplit("/", 1)[-1]
        if name and name != ".keep":
            out.append({"name": name, "bytes": bytes(data),
                        "type": str(res.get("mimeType") or b.get("mimeType") or "application/octet-stream")})
    return out


def _sandbox(sid: str, language: str, code: str, inputs: list[dict]) -> dict:
    """Write the inputs, run the script, read out/ back. Runs in a worker thread."""
    _invoke(sid, "writeFiles", {"content": inputs})
    ran = _invoke(sid, "executeCode", {"language": language, "code": code, "clearContext": True})
    files: list[dict] = []
    try:
        listed = _listed(_invoke(sid, "listFiles", {"directoryPath": OUT_DIR})["content"])
        if listed:
            files = _read_back(_invoke(sid, "readFiles", {"paths": listed})["content"])
    except TaskFailed as e:
        # The script's output stands; its files are reported missing rather than failing it.
        ran["stderr"] += f"\n[framework] could not read {OUT_DIR}/ back: {e}"
    ran["files"] = files[:MAX_OUT_FILES]
    return ran


def _code_inputs(ctx, files: list[dict]) -> list[dict]:
    """What the script is given: writeFiles content blocks (path + text or blob)."""
    outputs = {k: task_template.parsed(v) for k, v in ((ctx.state or {}).get("outputs") or {}).items()}
    blocks = [{"path": f"{IN_DIR}/request.txt", "text": ctx.topic or ""},
              {"path": f"{IN_DIR}/outputs.json", "text": json.dumps(outputs, ensure_ascii=False, default=str)},
              # writeFiles creates the folder, so the script can write to out/ straight away.
              {"path": f"{OUT_DIR}/.keep", "text": ""}]
    blocks.extend({"path": f"{IN_DIR}/{artifacts.safe_name(f['name'])}", "blob": f["bytes"]} for f in files)
    return blocks


async def _run_code(agent: TaskAgent, ctx, task: dict) -> dict:
    language = str(task.get("language") or "python")
    code = str(task.get("code") or "")
    files = await ctx._run_files()
    c = _client("bedrock-agentcore")
    started = time.monotonic()
    sid = (await asyncio.to_thread(
        c.start_code_interpreter_session, codeInterpreterIdentifier=INTERPRETER,
        name=f"ax-task-{agent.id}"[:100], sessionTimeoutSeconds=SESSION_TTL_SECONDS))["sessionId"]
    await ctx.log(f"{agent.name}: running {language} in the Code Interpreter sandbox")
    try:
        ran = await asyncio.to_thread(_sandbox, sid, language, code, _code_inputs(ctx, files))
    finally:
        # ALWAYS: an open session is billed until it expires, and stopping it is what ends
        # a script that is still running.
        try:
            await asyncio.to_thread(c.stop_code_interpreter_session,
                                    codeInterpreterIdentifier=INTERPRETER, sessionId=sid)
        except Exception as e:  # noqa: BLE001 - it expires by itself (SESSION_TTL_SECONDS)
            print(f"[task] stop session failed: {type(e).__name__}: {e}")
    ms = int((time.monotonic() - started) * 1000)
    failed = ran["isError"] or ran["exitCode"] not in (None, 0)
    _record("codeInterpreter", "agentcore", ms, code, ran["stdout"] + ran["stderr"], not failed)
    kept = await _keep_code(ctx, agent, language, code, ran)
    if failed:
        tail = (ran["stderr"] or ran["stdout"]).strip()[-1500:]
        raise TaskFailed(f"{agent.name}: the script failed (exit status {ran['exitCode']}): {tail}")
    saved = []
    for f in ran["files"]:
        try:
            key = await asyncio.to_thread(artifacts.put_output, ctx.session_id, f["name"], f["bytes"],
                                          f["type"], agent.id)
            saved.append({"name": key.rsplit("/", 1)[-1], "key": key, "bytes": len(f["bytes"])})
        except artifacts.NoFileStore:
            await ctx.log(f"{agent.name}: {f['name']} was not kept: this deployment keeps no run files "
                          f"(set orchestrator.files)")
            break
        except ValueError as e:
            await ctx.log(f"{agent.name}: {f['name']} was not kept: {e}")
    stdout = ran["stdout"]
    out = {"language": language, "seconds": round(ms / 1000, 2),
           "output": task_template.parsed(stdout.strip()) if len(stdout) <= MAX_STDOUT_CHARS
           else stdout[:MAX_STDOUT_CHARS] + f"\n[... cut at {MAX_STDOUT_CHARS} characters]",
           "files": saved}
    if ran["stderr"].strip():
        out["stderr"] = ran["stderr"][-4000:]
    if kept:
        out["code"] = kept
    return out


async def _keep_code(ctx, agent: TaskAgent, language: str, code: str, ran: dict) -> str:
    """The script and its log, as code/ artifacts, when the deployment keeps files."""
    if not artifacts._bucket():
        return ""
    try:
        return await asyncio.to_thread(artifacts.put_code, ctx.session_id, agent.id, language, code,
                                       ran["stdout"], ran["stderr"])
    except Exception as e:  # noqa: BLE001 - keeping a copy must never decide the step
        print(f"[task] code not kept for {agent.id}: {type(e).__name__}: {e}")
        return ""


# --- extract ----------------------------------------------------------------------------

def _words(block: dict, by_id: dict) -> str:
    """The text of a block's CHILD words (a selected checkbox reads as "X")."""
    parts = []
    for rel in block.get("Relationships") or []:
        if rel.get("Type") != "CHILD":
            continue
        for cid in rel.get("Ids") or []:
            child = by_id.get(cid) or {}
            if child.get("BlockType") == "WORD":
                parts.append(str(child.get("Text") or ""))
            elif child.get("BlockType") == "SELECTION_ELEMENT" and child.get("SelectionStatus") == "SELECTED":
                parts.append("X")
    return " ".join(p for p in parts if p)


def blocks_to_json(blocks: list[dict]) -> dict:
    """Textract Blocks as {pages, text, tables, forms}: the LINE text in reading order, each
    TABLE as rows of cell text, each KEY_VALUE_SET key with its value."""
    by_id = {b.get("Id"): b for b in blocks or [] if b.get("Id")}
    pages = max([int(b.get("Page") or 1) for b in blocks or []] or [0])
    lines = [str(b.get("Text") or "") for b in blocks or [] if b.get("BlockType") == "LINE"]
    tables = []
    for b in blocks or []:
        if b.get("BlockType") != "TABLE":
            continue
        cells = [by_id[c] for rel in b.get("Relationships") or [] if rel.get("Type") == "CHILD"
                 for c in rel.get("Ids") or [] if by_id.get(c, {}).get("BlockType") == "CELL"]
        rows = max([int(c.get("RowIndex") or 0) for c in cells] or [0])
        cols = max([int(c.get("ColumnIndex") or 0) for c in cells] or [0])
        grid = [["" for _ in range(cols)] for _ in range(rows)]
        for c in cells:
            r, k = int(c.get("RowIndex") or 0) - 1, int(c.get("ColumnIndex") or 0) - 1
            if 0 <= r < rows and 0 <= k < cols:
                grid[r][k] = _words(c, by_id)
        tables.append({"page": int(b.get("Page") or 1), "rows": grid})
    forms = []
    for b in blocks or []:
        if b.get("BlockType") != "KEY_VALUE_SET" or "KEY" not in (b.get("EntityTypes") or []):
            continue
        value = ""
        for rel in b.get("Relationships") or []:
            if rel.get("Type") == "VALUE":
                value = " ".join(_words(by_id.get(v) or {}, by_id) for v in rel.get("Ids") or []).strip()
        forms.append({"key": _words(b, by_id), "value": value, "page": int(b.get("Page") or 1)})
    text = "\n".join(lines)
    out = {"pages": pages, "text": text[:MAX_TEXT_CHARS], "tables": tables, "forms": forms}
    if len(text) > MAX_TEXT_CHARS:
        out["textCut"] = f"the text is cut at {MAX_TEXT_CHARS} characters"
    return out


def _analyze(bucket: str, key: str, fmt: str, features: list[str]) -> list[dict]:
    """Textract Blocks for one file in the assets bucket. Runs in a worker thread."""
    t = _client("textract")
    location = {"S3Object": {"Bucket": bucket, "Name": key}}
    if fmt != "pdf":
        # An image is one page: the synchronous call takes it straight from S3.
        return t.analyze_document(Document=location, FeatureTypes=features).get("Blocks") or []
    # The same token for the same file and features: a re-run of the step gets the job it
    # already started (for 7 days) instead of paying for the document again.
    token = hashlib.sha256(f"{key}|{','.join(features)}".encode()).hexdigest()[:64]
    job = t.start_document_analysis(DocumentLocation=location, FeatureTypes=features,
                                    ClientRequestToken=token)["JobId"]
    deadline = time.monotonic() + EXTRACT_TIMEOUT_SECONDS
    n = 0
    while True:
        page = t.get_document_analysis(JobId=job)
        status = page.get("JobStatus")
        if status != "IN_PROGRESS":
            break
        if time.monotonic() > deadline:
            raise TaskFailed(f"Textract did not finish {key.rsplit('/', 1)[-1]} in {EXTRACT_TIMEOUT_SECONDS} seconds")
        time.sleep(_POLL[min(n, len(_POLL) - 1)])
        n += 1
    if status == "FAILED":
        raise TaskFailed(f"Textract could not read {key.rsplit('/', 1)[-1]}: {page.get('StatusMessage') or 'FAILED'}")
    blocks = list(page.get("Blocks") or [])
    while page.get("NextToken"):
        page = t.get_document_analysis(JobId=job, NextToken=page["NextToken"])
        blocks += page.get("Blocks") or []
    return blocks


async def _run_extract(agent: TaskAgent, ctx, task: dict) -> dict:
    features = list(task.get("features") or FEATURES)
    wanted = str(task.get("file") or "")
    files = [f for f in attachments.mine((ctx.state or {}).get("attachments"), ctx.session_id)
             if f.get("format") in EXTRACT_FORMATS
             and (not wanted or str(f.get("name") or "") == wanted)]
    if not files:
        what = f"file named {wanted!r}" if wanted else "PDF, PNG or JPEG"
        raise TaskFailed(f"{agent.name}: the run has no {what} "
                         f"to read. Start the run with one attached.")
    bucket = attachments._bucket()
    out = []
    for f in files:
        name = str(f.get("name") or f["key"].rsplit("/", 1)[-1])
        await ctx.log(f"{agent.name}: reading {name} with Textract")
        started = time.monotonic()
        try:
            blocks = await asyncio.to_thread(_analyze, bucket, f["key"], f["format"], features)
        except TaskFailed:
            _record("textract", "textract", int((time.monotonic() - started) * 1000), name, "", False)
            raise
        found = blocks_to_json(blocks)
        _record("textract", "textract", int((time.monotonic() - started) * 1000), name,
                found["text"][:2000], True)
        out.append({"name": name, **found})
    return {"files": out}


# --- output -----------------------------------------------------------------------------

def _pick(ref: str, outputs: dict):
    """`agent` or `agent.field...` of the outputs so far."""
    head, _, rest = ref.partition(".")
    return task_template.lookup(f"outputs.{head}{'.' + rest if rest else ''}", {"outputs": outputs})


def _rows(value) -> list[dict]:
    """Rows for a CSV: a list of objects as it is; an object holding one, that list; another
    object, one row; anything else, one row with a `value` column."""
    if isinstance(value, list):
        return [r if isinstance(r, dict) else {"value": r} for r in value]
    if isinstance(value, dict):
        lists = [v for v in value.values() if isinstance(v, list) and v and all(isinstance(r, dict) for r in v)]
        return list(lists[0]) if len(lists) == 1 else [value]
    return [{"value": value}]


def _cell(v) -> str:
    return v if isinstance(v, str) else json.dumps(v, ensure_ascii=False, default=str)


def render_file(fmt: str, picked: dict) -> str:
    """The file's text, from {ref: value} in the order given."""
    if fmt == "json":
        body = next(iter(picked.values())) if len(picked) == 1 else picked
        return json.dumps(body, indent=2, ensure_ascii=False, default=str) + "\n"
    if fmt == "txt":
        return "\n\n".join(_cell(v) for v in picked.values()) + "\n"
    if fmt == "markdown":
        parts = []
        for ref, v in picked.items():
            parts.append(f"## {ref}\n")
            if isinstance(v, dict):
                parts += [f"- **{k}**: {x}" if not isinstance(x, (dict, list))
                          else f"- **{k}**:\n\n```json\n{json.dumps(x, indent=2, ensure_ascii=False, default=str)}\n```"
                          for k, x in v.items()]
            elif isinstance(v, list):
                parts.append(f"```json\n{json.dumps(v, indent=2, ensure_ascii=False, default=str)}\n```")
            else:
                parts.append(str(v))
            parts.append("")
        return "\n".join(parts)
    rows: list[dict] = []
    for ref, v in picked.items():
        got = _rows(v)
        rows += [{"from": ref, **r} for r in got] if len(picked) > 1 else got
    columns: list[str] = list(dict.fromkeys(k for r in rows for k in r))
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=columns, extrasaction="ignore", lineterminator="\n")
    writer.writeheader()
    for r in rows:
        writer.writerow({k: _cell(r.get(k, "")) for k in columns})
    return buf.getvalue()


async def _run_output(agent: TaskAgent, ctx, task: dict) -> dict:
    fmt = str(task.get("format") or "")
    if fmt not in FORMATS:
        raise TaskFailed(f"{agent.name}: task.format must be one of {', '.join(FORMATS)}")
    outputs = dict((ctx.state or {}).get("outputs") or {})
    refs = [str(r) for r in task.get("from") or []] or [k for k in outputs if k != agent.id]
    picked = {ref: _pick(ref, outputs) for ref in refs}
    text = render_file(fmt, picked)
    name = task_template.render(str(task.get("name") or f"{agent.id}.{_EXT[fmt]}"), _data(ctx))
    name = artifacts.safe_name(str(name))
    if not name.lower().endswith(f".{_EXT[fmt]}"):
        name = f"{name}.{_EXT[fmt]}"
    try:
        key = await ctx.save_file(name, text.encode("utf-8"), _CONTENT_TYPES[fmt])
    except artifacts.NoFileStore as e:
        raise TaskFailed(f"{agent.name}: {e}") from e
    return {"file": {"name": key.rsplit("/", 1)[-1], "key": key, "format": fmt,
                     "bytes": len(text.encode("utf-8"))}, "from": refs}
