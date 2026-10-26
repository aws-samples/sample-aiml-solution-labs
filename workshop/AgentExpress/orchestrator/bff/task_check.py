"""A runtime "task" step's own checks, for validate_build.py.

A task is a step with no model: it calls a tool, runs a script in the Code Interpreter
sandbox, reads the run's documents with Textract, or writes a file (app/common/task_agent.py).
The generic key checks (app/keys.json) already say that `task` is required, that it is an
object, and that the model keys do not apply. These are the rest: what each kind needs, and
that every placeholder names something that exists by the time the step runs.

Mirrors web/src/builder/taskCheck.ts word for word (both run
tests/fixtures/validation_cases.json) and, for what the runtime itself refuses,
app/common/task_agent.problem.
"""
from __future__ import annotations

import re

import code_source

#: The keys each kind reads (app/common/task_agent.KEYS_OF).
KEYS_OF = {"tool": ("kind", "tool", "arguments"), "code": ("kind", "language", "code", "codeSource"),
           "extract": ("kind", "file", "features"), "output": ("kind", "format", "name", "from"),
           # v139 Track C deliver
           "deliver": ("kind", "targets")}
ALL_KEYS = {k for keys in KEYS_OF.values() for k in keys}
MAX_CODE_BYTES = 50_000
PLACEHOLDER = re.compile(r"\{\{\s*([^{}]*?)\s*\}\}")
REF = re.compile(r"^(request|session|outputs\.[A-Za-z][A-Za-z0-9_]*(\.[A-Za-z0-9_-]+)*)$")
FROM_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*(\.[A-Za-z0-9_-]+)*$")


def _not_one_of(v, allowed: list[str]) -> str:
    """The words for a value outside its set (a non-string is not echoed: Python and
    JavaScript would print it differently)."""
    names = ", ".join(allowed)
    return f'"{v}" is not one of: {names}' if isinstance(v, str) else f"must be one of: {names}"


def _refs(value) -> list[str]:
    if isinstance(value, str):
        return [m.group(1) for m in PLACEHOLDER.finditer(value)]
    if isinstance(value, dict):
        return [r for v in value.values() for r in _refs(v)]
    if isinstance(value, list):
        return [r for v in value for r in _refs(v)]
    return []


def _check_refs(value, before: list[str], where: dict, p: str, add) -> None:
    for ref in _refs(value):
        if not REF.match(ref):
            add("error", where, p, f"{{{{{ref}}}}} is not a placeholder this step knows: use "
                                   "{{request}}, {{session}} or {{outputs.<agent>.<field>}}")
        elif ref.startswith("outputs."):
            src = ref.split(".")[1]
            if src not in before:
                add("error", where, p, f'{{{{{ref}}}}}: "{src}" does not run before this step')


def check(agents: dict, tools: dict, steps: list, orch, add, vocab, upstream_ids) -> None:
    """Add the issues of every task step. `vocab` and `upstream_ids` are validate_build's."""
    kinds = vocab("taskKinds")
    for aid, a in agents.items():
        if not isinstance(a, dict) or str(a.get("runtime") or "").lower() != "task":
            continue
        task = a.get("task")
        if not isinstance(task, dict):
            continue          # required, and an object: the generic checks say so
        where = {"kind": "agent", "id": aid}
        path = f"agents.{aid}"
        tp = f"{path}.task"
        kind = task.get("kind")
        if "kind" not in task:
            add("error", where, f"{tp}.kind", f"say what this step does: one of {', '.join(kinds)}")
            continue
        if not isinstance(kind, str) or kind not in kinds:
            add("error", where, f"{tp}.kind", _not_one_of(kind, kinds))
            continue
        for k in task:
            # A key no kind reads is the generic checks' "unknown key"; this is the one that
            # belongs to another kind.
            if k in ALL_KEYS and k not in KEYS_OF[kind]:
                add("error", where, f"{tp}.{k}", f'does not apply to a "{kind}" step')
        before = upstream_ids(steps, aid)
        core = a.get("agentcore")
        for feature in (core if isinstance(core, dict) else {}):
            if feature != "guardrails":
                add("error", where, f"{path}.agentcore.{feature}",
                    f"a task step makes no model call, so {feature} has nothing to work on — remove it")
        if kind == "tool":
            key = task.get("tool")
            if not isinstance(key, str) or not key:
                add("error", where, f"{tp}.tool", "pick the tool this step calls")
            elif not isinstance(tools.get(key), dict):
                add("error", where, f"{tp}.tool",
                    f'"{key}" is not a tool. Declare it under Tools, or pick one of: '
                    f'{", ".join(tools) or "(none yet)"}')
            _check_refs(task.get("arguments"), before, where, f"{tp}.arguments", add)
        elif kind == "code":
            code = task.get("code")
            if "codeSource" in task:
                # The script is fetched at deploy time (deployer/code_fetch.py read_script).
                if "code" in task:
                    add("error", where, f"{tp}.codeSource",
                        "use one of code (written or uploaded here) or codeSource, not both")
                for cp, msg in code_source.problems(task["codeSource"], f"{tp}.codeSource"):
                    add("error", where, cp, msg)
            elif not isinstance(code, str) or not code.strip():
                add("error", where, f"{tp}.code", "write the script this step runs")
            elif len(code.encode("utf-8")) > MAX_CODE_BYTES:
                add("error", where, f"{tp}.code",
                    f"is {len(code.encode('utf-8'))} bytes; a script may be at most {MAX_CODE_BYTES}")
            lang = task.get("language")
            if "language" in task and lang not in vocab("taskLanguages"):
                add("error", where, f"{tp}.language", _not_one_of(lang, vocab("taskLanguages")))
        elif kind == "extract":
            if a.get("attachments") is not True:
                add("error", where, f"{path}.attachments",
                    "an extract step reads the run's files: turn on attachments")
            feats = task.get("features")
            if isinstance(feats, list):
                if not feats:
                    add("error", where, f"{tp}.features",
                        f"name at least one of {', '.join(vocab('taskExtractFeatures'))}, or leave it out for both")
                for i, f in enumerate(feats):
                    if f not in vocab("taskExtractFeatures"):
                        add("error", where, f"{tp}.features[{i}]", _not_one_of(f, vocab("taskExtractFeatures")))
        elif kind == "output":
            fmt = task.get("format")
            if "format" not in task:
                add("error", where, f"{tp}.format",
                    f"pick the file format: {', '.join(vocab('taskOutputFormats'))}")
            elif fmt not in vocab("taskOutputFormats"):
                add("error", where, f"{tp}.format", _not_one_of(fmt, vocab("taskOutputFormats")))
            if not (isinstance(orch, dict) and isinstance(orch.get("files"), dict)):
                add("error", where, tp,
                    "an output step saves a file, which needs orchestrator.files so the run keeps it")
            src = task.get("from")
            for i, ref in enumerate(src if isinstance(src, list) else []):
                if not isinstance(ref, str) or not FROM_RE.match(ref):
                    add("error", where, f"{tp}.from[{i}]", "must be an agent id, or <agent>.<field>")
                elif ref.split(".")[0] not in before:
                    add("error", where, f"{tp}.from[{i}]", f'"{ref.split(".")[0]}" does not run before this step')
            _check_refs(task.get("name"), before, where, f"{tp}.name", add)
        elif kind == "deliver":
            _check_deliver(task, before, orch, where, tp, add, vocab)


# --- begin v139 Track C deliver -----------------------------------------------------------
# A deliver step (app/common/deliver.py): where the run's files go. Mirrors checkDeliver in
# web/src/builder/taskCheck.ts word for word.

MAX_TARGETS = 10
MAX_RECIPIENTS = 50
LINK_MAX_MINUTES = 7 * 24 * 60
COMMON_TARGET_KEYS = ("type", "files", "producedBy", "when")
#: The keys each target type reads (app/common/deliver.TARGET_KEYS).
TARGET_KEYS = {
    "s3": (*COMMON_TARGET_KEYS, "uri"),
    "email": (*COMMON_TARGET_KEYS, "identityArn", "fromAddress", "to", "subject", "body", "attach",
              "recipientsFrom", "linkExpiresMinutes"),
    "webhook": (*COMMON_TARGET_KEYS, "url", "secret", "linkExpiresMinutes"),
}
ALL_TARGET_KEYS = {k for keys in TARGET_KEYS.values() for k in keys}
#: branch.when's operators (app/common/branching.py _OPS).
WHEN_OPS = ("equals", "notEquals", "in", "contains", "exists", "gt", "gte", "lt", "lte")
S3_URI_RE = re.compile(r"^(s3://[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]|\$\{storage\})/([^*?]+/)?$")
#: The framework's own buckets (validate_build.FRAMEWORK_BUCKET_RE).
FRAMEWORK_BUCKET_RE = re.compile(r"^s3://(agentcore-|[^/]*builderbuildsbucket)")
IDENTITY_ARN_RE = re.compile(r"^arn:aws[a-z-]*:ses:[a-z0-9-]+:[0-9]{12}:identity/([^/\s]+)$")
EMAIL_RE = re.compile(r"^[^@\s,;<>\"]+@[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)+$")
ONE_OUTPUT_RE = re.compile(r"^\{\{\s*(outputs\.[A-Za-z][A-Za-z0-9_]*(\.[A-Za-z0-9_-]+)*)\s*\}\}$")
SECRET_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")
HTTPS_RE = re.compile(r"^https://[^/@\s]+(/\S*)?$")


def _num(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _check_when(when, before: list[str], where: dict, p: str, add) -> None:
    if not isinstance(when, dict):
        add("error", where, p, "must be an object: {\"field\": \"outputs.<step>.<field>\", \"equals\": ...}")
        return
    for k in when:
        if k != "field" and k not in WHEN_OPS:
            add("error", where, f"{p}.{k}", f"unknown key; a condition takes field and one of: {', '.join(WHEN_OPS)}")
    field = when.get("field")
    if not isinstance(field, str) or not REF.match(field):
        add("error", where, f"{p}.field", "name what to test: request, or outputs.<step>.<field>")
    elif field.startswith("outputs.") and field.split(".")[1] not in before:
        add("error", where, f"{p}.field", f'"{field.split(".")[1]}" does not run before this step')
    if not any(op in when for op in WHEN_OPS):
        add("error", where, p, f"needs one comparison: {', '.join(WHEN_OPS)}")
    if "in" in when and not isinstance(when["in"], list):
        add("error", where, f"{p}.in", "takes a list of alternatives")
    if "exists" in when and not isinstance(when["exists"], bool):
        add("error", where, f"{p}.exists", "takes true or false")
    for op in ("gt", "gte", "lt", "lte"):
        if op in when and not _num(when[op]):
            add("error", where, f"{p}.{op}", "takes a number")


def _check_links(t: dict, where: dict, p: str, add) -> None:
    m = t.get("linkExpiresMinutes")
    whole = isinstance(m, int) and not isinstance(m, bool)
    if "linkExpiresMinutes" in t and not (whole and 1 <= m <= LINK_MAX_MINUTES):
        add("error", where, f"{p}.linkExpiresMinutes", f"must be a whole number of minutes, 1 to {LINK_MAX_MINUTES}")


def _check_email(t: dict, before: list[str], where: dict, p: str, add) -> None:
    arn = t.get("identityArn")
    m = IDENTITY_ARN_RE.match(arn) if isinstance(arn, str) else None
    if "identityArn" not in t:
        add("error", where, f"{p}.identityArn",
            "pick the SES identity it is sent from: arn:aws:ses:<region>:<account>:identity/<domain or address>")
    elif not m:
        add("error", where, f"{p}.identityArn",
            "must be an SES identity ARN: arn:aws:ses:<region>:<account>:identity/<domain or address>")
    sender = t.get("fromAddress")
    if "fromAddress" not in t:
        add("error", where, f"{p}.fromAddress", "write the From address, at the identity's domain")
    elif not (isinstance(sender, str) and EMAIL_RE.match(sender)):
        add("error", where, f"{p}.fromAddress", "must be an email address")
    elif m:
        ident = m.group(1).lower()
        if "@" in ident and sender.lower() != ident:
            add("error", where, f"{p}.fromAddress", f"must be the identity's own address, {m.group(1)}")
        elif "@" not in ident and sender.lower().rsplit("@", 1)[1] != ident:
            add("error", where, f"{p}.fromAddress", f"must be an address at {m.group(1)}, the identity's domain")
    to = t.get("to")
    if not isinstance(to, list) or not to:
        add("error", where, f"{p}.to", "write at least one recipient")
    else:
        if len(to) > MAX_RECIPIENTS:
            add("error", where, f"{p}.to", f"at most {MAX_RECIPIENTS} recipients (SES's limit per message)")
        for j, a in enumerate(to):
            if isinstance(a, str) and PLACEHOLDER.search(a):
                add("error", where, f"{p}.to[{j}]",
                    "recipients are written here, not taken from a step's output: use recipientsFrom for that")
            elif not (isinstance(a, str) and EMAIL_RE.match(a)):
                add("error", where, f"{p}.to[{j}]", "must be an email address")
    for k in ("subject", "body"):
        if k in t and not isinstance(t[k], str):
            add("error", where, f"{p}.{k}", "must be a string")
        else:
            _check_refs(t.get(k), before, where, f"{p}.{k}", add)
    if "attach" in t and not isinstance(t["attach"], bool):
        add("error", where, f"{p}.attach", "must be true or false")
    if "recipientsFrom" in t:
        rf = t["recipientsFrom"]
        one = ONE_OUTPUT_RE.match(rf) if isinstance(rf, str) else None
        if not one:
            add("error", where, f"{p}.recipientsFrom", "must be one placeholder, {{outputs.<step>.<field>}}")
        elif one.group(1).split(".")[1] not in before:
            add("error", where, f"{p}.recipientsFrom", f'"{one.group(1).split(".")[1]}" does not run before this step')
    _check_links(t, where, p, add)


def _check_deliver(task: dict, before: list[str], orch, where: dict, tp: str, add, vocab) -> None:
    if not (isinstance(orch, dict) and isinstance(orch.get("files"), dict)):
        add("error", where, tp,
            "a deliver step sends the run's files, which needs orchestrator.files so the run keeps them")
    targets = task.get("targets")
    if "targets" not in task or (isinstance(targets, list) and not targets):
        add("error", where, f"{tp}.targets", "add at least one target: where to send the files")
        return
    if not isinstance(targets, list):
        return                # must be an array: the generic checks say so
    if len(targets) > MAX_TARGETS:
        add("error", where, f"{tp}.targets", f"at most {MAX_TARGETS} targets")
    types = vocab("deliverTargetTypes")
    for i, t in enumerate(targets):
        p = f"{tp}.targets[{i}]"
        if not isinstance(t, dict):
            add("error", where, p, "must be an object")
            continue
        kind = t.get("type")
        if "type" not in t:
            add("error", where, f"{p}.type", f"pick where it goes: one of {', '.join(types)}")
            continue
        if not isinstance(kind, str) or kind not in types:
            add("error", where, f"{p}.type", _not_one_of(kind, types))
            continue
        for k in t:
            if k in ALL_TARGET_KEYS and k not in TARGET_KEYS[kind]:
                add("error", where, f"{p}.{k}", f'does not apply to type "{kind}"')
            elif k not in ALL_TARGET_KEYS:
                add("error", where, f"{p}.{k}", f'unknown key; type "{kind}" takes: {", ".join(TARGET_KEYS[kind])}')
        files = t.get("files")
        if "files" in t and not isinstance(files, list):
            add("error", where, f"{p}.files", "must be a list of file-name patterns, e.g. *.pdf")
        for j, f in enumerate(files if isinstance(files, list) else []):
            if not isinstance(f, str) or not f.strip() or "/" in f or "\\" in f:
                add("error", where, f"{p}.files[{j}]", "must be a file-name pattern, e.g. *.pdf (no folder)")
        by = t.get("producedBy")
        if "producedBy" in t and not isinstance(by, list):
            add("error", where, f"{p}.producedBy", "must be a list of step ids")
        for j, s in enumerate(by if isinstance(by, list) else []):
            if not isinstance(s, str) or s not in before:
                add("error", where, f"{p}.producedBy[{j}]", f'"{s}" does not run before this step'
                    if isinstance(s, str) else "must be a step id")
        if "when" in t:
            _check_when(t["when"], before, where, f"{p}.when", add)
        if kind == "s3":
            uri = t.get("uri")
            if "uri" not in t:
                add("error", where, f"{p}.uri",
                    "write the bucket and prefix: s3://<bucket>/<prefix>/ or ${storage}/<prefix>/")
            elif not (isinstance(uri, str) and S3_URI_RE.match(uri)):
                add("error", where, f"{p}.uri",
                    "must be s3://<bucket>/ or s3://<bucket>/<prefix>/, ending in /, or ${storage}/<prefix>/")
            elif FRAMEWORK_BUCKET_RE.match(uri):
                add("error", where, f"{p}.uri", "is one of the framework's own buckets, which no delivery may write to")
        elif kind == "email":
            _check_email(t, before, where, p, add)
        elif kind == "webhook":
            url = t.get("url")
            if "url" not in t:
                add("error", where, f"{p}.url", "write the https URL to post to")
            elif not (isinstance(url, str) and HTTPS_RE.match(url)):
                add("error", where, f"{p}.url", "must be an https:// URL, with no user:password@")
            sec = t.get("secret")
            if "secret" in t and not (isinstance(sec, str) and SECRET_NAME_RE.match(sec)):
                add("error", where, f"{p}.secret",
                    "must be the name of a deliverSecrets build secret (letters, digits, . _ -), "
                    "never the secret itself")
            _check_links(t, where, p, add)
# --- end v139 Track C deliver -------------------------------------------------------------
