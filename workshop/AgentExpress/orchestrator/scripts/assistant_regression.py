#!/usr/bin/env python3
"""Regression checks for the AgentExpress Assistant against a DEPLOYED console (Control Plane).

Each check makes a fresh build, sends the Assistant one or more messages, waits for each reply,
then asserts on the build's workflow.json and on the reply. One check per behaviour the Assistant
got wrong in a production session, plus that session's brief replayed whole:

    parallel      "run them in parallel" gives one parallel stage, never "cannot run in parallel"
    tools         four listed tools become four tools, not one catch-all
    files         a PDF output uses the document tool (docgen), or a Lambda tool it wrote
    scope         the first reply says what is out of scope; the next one does not repeat it
    no-request    pasted content with no ask gets ONE question and no change
    versions      changes are named versions; "go back to v1" restores v1 and says so
    storage       a tool that needs S3, with no bucket named, gets build storage (no question)
    replay        the production brief: parallel docgen tools, a decision gate, a deliver step
                  and created storage, built in the first reply; with no choice made the run
                  reaches the deliver step and Abort ends it (routed by app/common/branching.py,
                  so run it with the orchestrator's requirements installed)

    export AX_ID_TOKEN='eyJ...'        # an ID token of a console user; never a command-line value
    python3 scripts/assistant_regression.py --url https://dxxxx.cloudfront.net
    python3 scripts/assistant_regression.py --url ... --only parallel,versions --keep
    python3 scripts/assistant_regression.py --url ... --dry-run

Sign-in, redaction and the HTTP client are scripts/e2e_smoke.py's: it talks only to --url, follows
no redirect and never prints a token. The builds it makes are named "v139 regression <check>" and
deleted afterwards unless --keep (they are never deployed). Each check costs a few Assistant turns.

Prints a Markdown table `| check | result | evidence |` with PASS, FAIL or NOT-TESTABLE (reason).
Exit codes: 0 every check passed (or was not testable), 1 a check failed, 2 it could not run.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import secrets
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from e2e_smoke import (
    EXIT_CANNOT_RUN,
    EXIT_FAIL,
    EXIT_PASS,
    Api,
    ApiError,
    CannotRun,
    Console,
    Redactor,
    Unreachable,
    _Parser,
    normalise_base,
    srp_token_provider,
)

PASS, FAIL, NOT_TESTABLE = "PASS", "FAIL", "NOT-TESTABLE"
BUILD_NAME = "v139 regression {check}"

REPLAY_BRIEF = ("Build a pipeline that parses an uploaded CSV, generates a PDF report, a PPTX deck and a DOCX "
                "summary from templates in parallel, checks them against the CSV, and delivers them to S3 only "
                "if the check passes, otherwise asks the operator to override or abort")

#: What each check sends, in order. "Just build it" keeps the Assistant from asking first
#: (GUIDE rule 1), so each check sees a draft.
PROMPTS: dict[str, list[str]] = {
    "parallel": [("Just build it: three writer agents (a headline writer, a summary writer and a tagline "
                  "writer), each given the same product description, and run the three in parallel.")],
    "tools": [("Just build it: one agent that cleans an order record, with four tools: a CSV parser, a "
               "currency converter, a date normaliser and a unit converter. Write each as a Lambda tool.")],
    "files": ["Just build it: an agent that turns a sales request into a one-page PDF report file."],
    "scope": ["Build a support ticket triage workflow, plus a React web UI for it and unit tests for that UI.",
              "Add a review gate after triage."],
    "no-request": ["region,units,revenue\nNorth,120,4300\nSouth,98,3900\nEast,143,5120"],
    "versions": ["Just build it: a research workflow with two agents, a researcher then a writer.",
                 "Make it a single agent that does both.",
                 "Go back to v1."],
    "storage": [("Just build it: an agent that writes a daily summary and saves it as a text file in S3 "
                 "with a Lambda tool.")],
    "replay": [REPLAY_BRIEF],
}
CHECKS = list(PROMPTS)


def _starter(name: str) -> dict:
    """A new build as the console makes one: one placeholder agent behind a review gate."""
    return {"name": name, "workflow": {
        "agents": {"first_agent": {"name": "First Agent", "runtime": "main", "maxTokens": 4000}},
        "steps": [{"agent": "first_agent", "hitl": True}],
        "ui": {"title": name, "heading": name}},
        "prompts": {"first_agent": {"systemPrompt": "You are the first agent.", "schema": "{}"}}}


# --- what is checked: pure functions of the build and the replies (offline-testable) ----------

def _wf(project: dict) -> dict:
    return (project or {}).get("workflow") or {}


def _replies(doc: dict) -> list[dict]:
    return [t for t in (doc or {}).get("turns") or [] if t.get("role") == "assistant"]


def _tools_of(agent: dict) -> list[str]:
    t = (agent or {}).get("tool")
    return [t] if isinstance(t, str) else [x for x in t or [] if isinstance(x, str)]


def _is_docgen(tool) -> bool:
    """The Document generator: a lambda tool with source "docgen" (bff/docgen_tool.py)."""
    return isinstance(tool, dict) and tool.get("type") == "lambda" and tool.get("source") == "docgen"


def _files_on(wf: dict) -> bool:
    """orchestrator.files is set: the run keeps its files, which docgen and deliver need."""
    return isinstance((wf.get("orchestrator") or {}).get("files"), dict)


#: The build-storage token, and where it is read (app/vocabulary.json storageToken.fields).
STORAGE_TOKEN = "${storage}"  # noqa: S105 - a placeholder in an S3 location, not a secret  # nosec B105


def _storage_uses(wf: dict) -> list[str]:
    """Each place the workflow points at the build's storage: a tool's s3Uri, templateS3Uri,
    outputPrefix or code grant, or a deliver step's s3 target uri."""
    out = []
    for k, t in (wf.get("tools") or {}).items():
        if not isinstance(t, dict):
            continue
        code = t.get("code") if isinstance(t.get("code"), dict) else {}
        grants = code.get("grants") if isinstance(code.get("grants"), dict) else {}
        for field, v in (("s3Uri", t.get("s3Uri")), ("templateS3Uri", t.get("templateS3Uri")),
                         ("outputPrefix", t.get("outputPrefix")), ("code.grants.s3Prefix", grants.get("s3Prefix"))):
            if isinstance(v, str) and v.startswith(STORAGE_TOKEN):
                out.append(f"tools.{k}.{field}")
    for a, e in _deliver_steps(wf).items():
        for i, t in enumerate((e.get("task") or {}).get("targets") or []):
            if isinstance(t, dict) and isinstance(t.get("uri"), str) and t["uri"].startswith(STORAGE_TOKEN):
                out.append(f"agents.{a}.task.targets[{i}].uri")
    return out


def _step_index(wf: dict, agent: str) -> int:
    """The index of the stage that runs `agent` (an agent, sequence or parallel stage), or -1."""
    for i, s in enumerate(wf.get("steps") or []):
        if isinstance(s, dict) and (s.get("agent") == agent or agent in (s.get("sequence") or [])
                                    or agent in (s.get("parallel") or [])):
            return i
    return -1


def _deliver_steps(wf: dict) -> dict[str, dict]:
    """The deliver steps: runtime "task" agents whose task.kind is "deliver" (app/common/deliver.py)."""
    return {a: e for a, e in (wf.get("agents") or {}).items() if isinstance(e, dict)
            and e.get("runtime") == "task" and isinstance(e.get("task"), dict) and e["task"].get("kind") == "deliver"}


def _parallel_groups(wf: dict) -> list[list[str]]:
    return [list(s["parallel"]) for s in wf.get("steps") or [] if isinstance(s, dict)
            and isinstance(s.get("parallel"), list)]


_CANNOT_PARALLEL = re.compile(r"(can ?not|can't|cannot|no way to|doesn't support|does not support)\s+"
                              r"(run\s+)?(\w+\s+){0,3}(in\s+)?parallel", re.IGNORECASE)
_NO_RENDER = re.compile(r"no (built-in )?(rendering|render|document|file)[- ](tool|generation)|"
                        r"(can ?not|cannot|can't) (render|generate) (a |the )?(pdf|pptx|docx|file)", re.IGNORECASE)


def check_parallel(project: dict, doc: dict) -> tuple[str, str]:
    groups = [g for g in _parallel_groups(_wf(project)) if len(g) >= 3]
    said = " ".join(t.get("text") or "" for t in _replies(doc))
    if _CANNOT_PARALLEL.search(said):
        return FAIL, f"reply says parallel is not possible: {_CANNOT_PARALLEL.search(said).group(0)!r}"
    if not groups:
        return FAIL, f"no parallel stage of 3 or more agents; steps: {json.dumps(_wf(project).get('steps'))[:300]}"
    return PASS, f"parallel stage {groups[0]}"


def check_tools(project: dict, doc: dict) -> tuple[str, str]:
    tools = _wf(project).get("tools") or {}
    if len(tools) >= 4:
        return PASS, f"{len(tools)} tools: {', '.join(sorted(tools))}"
    why = re.search(r"[^.]*\b(merge|combine|one tool|single tool)\b[^.]*\.", " ".join(
        t.get("text") or "" for t in _replies(doc)), re.IGNORECASE)
    return FAIL, (f"{len(tools)} tool(s) for 4 listed: {', '.join(sorted(tools)) or 'none'}"
                  + (f"; reply: {why.group(0).strip()[:200]!r}" if why else ""))


def check_files(project: dict, doc: dict) -> tuple[str, str]:
    wf, said = _wf(project), " ".join(t.get("text") or "" for t in _replies(doc))
    if _NO_RENDER.search(said):
        return FAIL, f"reply says it cannot render: {_NO_RENDER.search(said).group(0)!r}"
    tools = wf.get("tools") or {}
    docgen = [k for k, t in tools.items() if _is_docgen(t)
              and "pdf" in [str(f).lower() for f in t.get("formats") or []]]
    if docgen and not _files_on(wf):
        return FAIL, f"docgen tool {docgen[0]} without orchestrator.files (it saves each file with the run)"
    if docgen:
        return PASS, f"docgen tool {docgen[0]} with pdf"
    code = (project or {}).get("toolCode") or {}
    written = [k for k, t in tools.items() if isinstance(t, dict) and t.get("type") == "lambda"
               and "code" in t and code.get(k, {}).get("handler.py")]
    unpinned = [k for k in written if any(ln.strip() and not ln.strip().startswith("#") and "==" not in ln
                                          for ln in str(code[k].get("requirements.txt") or "").splitlines())]
    if written and not unpinned:
        return PASS, f"Lambda tool {written[0]} written in the build (no docgen tool)"
    return FAIL, ("no docgen tool with pdf and no written Lambda tool" if not written
                  else f"Lambda tool(s) with unpinned requirements: {unpinned}")


_SCOPE = re.compile(r"\b(UI|user interface|front[- ]?end|unit tests?)\b", re.IGNORECASE)
_OUT = re.compile(r"\b(not|doesn't|does not|outside|isn't|won't|cannot|can't)\b", re.IGNORECASE)


def check_scope(project: dict, doc: dict) -> tuple[str, str]:
    replies = _replies(doc)
    if len(replies) < 2:
        return FAIL, f"{len(replies)} replies, expected 2"
    first, second = replies[0].get("text") or "", replies[1].get("text") or ""
    if not (_SCOPE.search(first) and _OUT.search(first)):
        return FAIL, "the first reply does not say the UI or unit tests are outside what it builds"
    if re.search(r"not covered", second, re.IGNORECASE) or re.search(r"\breact\b", second, re.IGNORECASE):
        return FAIL, "the second reply repeats the scope ('Not covered' or the UI again)"
    return PASS, "scope stated in reply 1 only"


def check_no_request(project: dict, doc: dict, before: dict) -> tuple[str, str]:
    reply = (_replies(doc) or [{}])[-1]
    text = reply.get("text") or ""
    if reply.get("changes"):
        return FAIL, f"it changed the build: {[c.get('summary') for c in reply['changes']]}"
    if _wf(project) != _wf(before):
        return FAIL, "the workflow changed"
    if text.count("?") != 1:
        return FAIL, f"expected one question, the reply has {text.count('?')} question marks"
    return PASS, f"one question: {text.strip()[:160]!r}"


def check_versions(project: dict, doc: dict, v1_agents: list[str] | None) -> tuple[str, str]:
    versions = (doc or {}).get("versions") or []
    named = [v for v in versions if int(v.get("v") or 0) >= 1]
    if len(named) < 2:
        return FAIL, f"{len(named)} named versions after two changes: {versions}"
    last = (_replies(doc) or [{}])[-1]
    summary = " ".join(c.get("summary") or "" for c in last.get("changes") or [])
    if int((doc or {}).get("version") or -1) != 1:
        return FAIL, f"current version is {(doc or {}).get('version')}, not 1"
    if "v1" not in (last.get("text") or "") or not summary.startswith("Went back to v1"):
        return FAIL, f"the reply does not say it restores v1: {(last.get('text') or '')[:160]!r}; change {summary!r}"
    if v1_agents is not None and sorted(_wf(project).get("agents") or {}) != sorted(v1_agents):
        return FAIL, f"agents {sorted(_wf(project).get('agents') or {})} are not v1's {sorted(v1_agents)}"
    labels = ["v{}: {}".format(v["v"], v.get("name")) for v in named]
    return PASS, f"versions {labels}; back on v1"


def _storage_on(wf: dict) -> bool:
    st = (wf.get("orchestrator") or {}).get("storage")
    return isinstance(st, dict) and st.get("create") is True


_BUCKET_Q = re.compile(r"bucket", re.IGNORECASE)


def _bucket_questions(doc: dict) -> list[str]:
    """needsInput items that ask for a bucket. A template to upload (a docgen tool's
    templateS3Uri) is a real input the brief leaves out, even when it names the bucket."""
    return [n.get("question") or n.get("path") or "" for t in _replies(doc) for c in t.get("changes") or []
            for n in c.get("needsInput") or []
            if not str(n.get("path") or "").endswith(".templateS3Uri")
            and _BUCKET_Q.search(f"{n.get('path')} {n.get('question')}")]


def check_storage(project: dict, doc: dict) -> tuple[str, str]:
    wf = _wf(project)
    if not _storage_on(wf):
        return FAIL, f"orchestrator.storage is {json.dumps((wf.get('orchestrator') or {}).get('storage'))}"
    uses = _storage_uses(wf)
    if not uses:
        return FAIL, f"nothing points at {STORAGE_TOKEN} (a tool's S3 location or a deliver target's uri)"
    asked = _bucket_questions(doc)
    if asked:
        return FAIL, f"it still asked for a bucket: {asked[:2]}"
    return PASS, f"orchestrator.storage.create true; {STORAGE_TOKEN} in {', '.join(uses[:3])}"


def _option_labels(hitl) -> list[str]:
    """A gate's named options (steps[].hitl.options): a list of label strings."""
    opts = hitl.get("options") if isinstance(hitl, dict) else None
    return [o.lower() for o in opts or [] if isinstance(o, str) and o]


def _routing_problems(wf: dict, gate: dict, deliver: list[str]) -> list[str]:
    """Where the decision gate's branch sends a run, routed as the runtime does
    (app/common/branching.choose): with no choice made (a passing check, the gate did not
    stop) the run must reach a deliver step, and Abort must end it."""
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from app.common import branching  # the orchestrator's own router, so the same semantics
    at = wf["steps"].index(gate)
    spec = gate.get("branch") or {}

    def reaches_deliver(target: str | None) -> bool:
        if target == "END":
            return False
        start = at + 1 if target is None else _step_index(wf, target)
        return start > at and any(_step_index(wf, a) >= start for a in deliver)

    problems = []
    target, _ = branching.choose(spec, "{}")
    if not reaches_deliver(target):
        problems.append(f"with no choice made the decision gate's branch goes to {target or 'the next stage'}, "
                        "not to the deliver step")
    abort = next((o for o in gate["hitl"]["options"] if isinstance(o, str) and o.lower() == "abort"), "Abort")
    target, _ = branching.choose(spec, "{}", {"decision": abort})
    if target != "END":
        problems.append(f"{abort} goes to {target or 'the next stage'}, not END")
    return problems


def check_replay(project: dict, doc: dict) -> tuple[str, str]:
    wf = _wf(project)
    agents, tools = wf.get("agents") or {}, wf.get("tools") or {}
    problems = []
    formats_by_group = []
    for group in _parallel_groups(wf):
        fmts = set()
        keys = set()
        for aid in group:
            for k in _tools_of(agents.get(aid) or {}):
                t = tools.get(k) or {}
                if _is_docgen(t):
                    keys.add(k)
                    fmts |= {str(f).lower() for f in t.get("formats") or []}
        formats_by_group.append((group, keys, fmts))
    if not any({"pdf", "pptx", "docx"} <= f and len(k) >= 3 for _g, k, f in formats_by_group):
        problems.append(f"no parallel stage of three docgen tools covering pdf/pptx/docx ({formats_by_group})")
    gates = [s for s in wf.get("steps") or [] if isinstance(s, dict)
             and {"override", "abort"} <= set(_option_labels(s.get("hitl")))]
    if not gates:
        problems.append("no gate with the options Override and Abort")
    elif not any(any(isinstance(r, dict) and r.get("field") == "decision"
                     for r in (g.get("branch") or {}).get("when") or []) for g in gates):
        problems.append("the decision gate has no branch on `decision`")
    steps = _deliver_steps(wf)
    deliver = [a for a, e in steps.items()
               if any(isinstance(t, dict) and t.get("type") == "s3" for t in e["task"].get("targets") or [])]
    if not deliver:
        problems.append("no deliver step (runtime task, task.kind deliver) with an s3 target in task.targets")
    elif gates and not any(_step_index(wf, a) > wf["steps"].index(gates[0]) for a in deliver):
        problems.append("the deliver step does not come after the decision gate")
    elif gates:
        problems += _routing_problems(wf, gates[0], deliver)
    if not _files_on(wf):
        problems.append("orchestrator.files is not set (docgen and deliver need the run's files)")
    if not _storage_on(wf):
        problems.append("orchestrator.storage.create is not true")
    elif not _storage_uses(wf):
        problems.append(f"storage is created but nothing points at {STORAGE_TOKEN}")
    replies = _replies(doc)
    if not replies or not replies[0].get("changes"):
        problems.append("the first reply built nothing (it asked first)")
    asked = _bucket_questions(doc)
    if asked:
        problems.append(f"it asked for a bucket: {asked[:2]}")
    if problems:
        return FAIL, "; ".join(problems)[:600]
    gate = gates[0].get("agent") or gates[0].get("gateId")
    return PASS, f"docgen in parallel, gate {gate}, deliver {deliver[0]}, storage on"


# --- talking to the console --------------------------------------------------------------------

class Session:
    def __init__(self, api: Api, console: Console, timeout: float, review: str) -> None:
        self.api, self.console, self.timeout, self.review = api, console, timeout, review

    def create(self, check: str) -> str:
        bid = "v139reg" + secrets.token_hex(6)
        self.api.call("PUT", f"/api/builds/{bid}", {"project": _starter(BUILD_NAME.format(check=check))})
        return bid

    def project(self, bid: str) -> dict:
        return (self.api.call("GET", f"/api/builds/{bid}") or {}).get("project") or {}

    def say(self, bid: str, message: str) -> dict:
        """Send one message and wait for the reply to finish; the conversation as it then is."""
        self.api.call("POST", f"/api/builds/{bid}/design", {"message": message, "review": self.review})
        began = last_note = time.monotonic()
        wait = 2.0
        while True:
            doc = self.api.call("GET", f"/api/builds/{bid}/design") or {}
            if doc.get("status") == "idle":
                return doc
            now = time.monotonic()
            if now - began > self.timeout:
                raise TimeoutError(f"no reply within {self.timeout:g}s")
            if now - last_note >= 60:
                self.console.log(f"  still waiting for the reply ({now - began:.0f}s)")
                last_note = now
            time.sleep(wait)
            wait = min(wait * 1.5, 8.0)

    def delete(self, bid: str) -> None:
        self.api.call("DELETE", f"/api/builds/{bid}")


def run_check(s: Session, check: str, keep: bool) -> tuple[str, str]:
    bid = s.create(check)
    s.console.log(f"{check}: build {bid}")
    try:
        before = s.project(bid)
        doc: dict = {}
        v1_agents = None
        for i, message in enumerate(PROMPTS[check]):
            doc = s.say(bid, message)
            last = (_replies(doc) or [{}])[-1]
            if last.get("status") == "error":
                return FAIL, f"reply {i + 1} failed: {(last.get('text') or '')[:200]!r}"
            if check == "versions" and i == 0:
                v1_agents = sorted(_wf(s.project(bid)).get("agents") or {})
        project = s.project(bid)
        if check == "no-request":
            return check_no_request(project, doc, before)
        if check == "versions":
            return check_versions(project, doc, v1_agents)
        return {"parallel": check_parallel, "tools": check_tools, "files": check_files,
                "scope": check_scope, "storage": check_storage, "replay": check_replay}[check](project, doc)
    finally:
        if keep:
            s.console.log(f"{check}: kept build {bid}")
        else:
            try:
                s.delete(bid)
            except (ApiError, Unreachable) as e:
                s.console.log(f"{check}: could not delete build {bid}: {e}")


def table(rows: list[tuple[str, str, str]]) -> str:
    def cell(s: str) -> str:
        return " ".join(str(s).split()).replace("|", "\\|")
    return "\n".join(["| check | result | evidence |", "|---|---|---|",
                      *(f"| {cell(c)} | {cell(r)} | {cell(e)} |" for c, r, e in rows)])


def build_parser() -> argparse.ArgumentParser:
    p = _Parser(prog="assistant_regression.py", description=(__doc__ or "").split("\n\n")[0],
                allow_abbrev=False, epilog="The password is read from the environment variable AX_PASSWORD only.")
    p.add_argument("--url", required=True, help="the console's CloudFront URL (https; http only for localhost)")
    p.add_argument("--id-token", default=None, help="a Cognito ID token (or set AX_ID_TOKEN; the variable is safer)")
    p.add_argument("--pool-id", default=None, help="Cognito user pool id, to sign in with SRP")
    p.add_argument("--client-id", default=None, help="Cognito app client id, to sign in with SRP")
    p.add_argument("--username", default=None, help="the user to sign in as (the password is in AX_PASSWORD)")
    p.add_argument("--only", default="", help=f"comma-separated checks to run (default all: {', '.join(CHECKS)})")
    p.add_argument("--timeout", type=float, default=900.0, help="seconds to wait for each reply (default 900)")
    p.add_argument("--review", default="auto", choices=("off", "auto", "always"),
                   help="the Assistant's Review changes setting for every message (default auto)")
    p.add_argument("--keep", action="store_true", help="do not delete the builds afterwards")
    p.add_argument("--dry-run", action="store_true", help="print what would be sent and call nothing")
    p.add_argument("-v", "--verbose", action="store_true", help="log each request's method, path and status")
    return p


def run(argv: list[str], env: dict | None = None) -> int:
    env = os.environ if env is None else env
    redact = Redactor()
    try:
        args = build_parser().parse_args(argv)
    except SystemExit as e:
        return int(e.code) if isinstance(e.code, int) else EXIT_CANNOT_RUN
    token = (args.id_token or env.get("AX_ID_TOKEN") or "").strip().removeprefix("Bearer ").strip()
    password = env.get("AX_PASSWORD") or ""
    redact.add(token, password, args.id_token)
    console = Console(redact, args.verbose)

    def cannot(msg: str) -> int:
        console.err(f"assistant_regression: cannot run: {msg}")
        return EXIT_CANNOT_RUN
    try:
        base = normalise_base(args.url)
    except ValueError as e:
        return cannot(str(e))
    checks = [c.strip() for c in args.only.split(",") if c.strip()] or CHECKS
    unknown = [c for c in checks if c not in PROMPTS]
    if unknown:
        return cannot(f"unknown check(s) {unknown}; the checks are {', '.join(CHECKS)}")
    if args.dry_run:
        console.out(f"DRY RUN against {base}: for each check, PUT /api/builds/<new id>, then for each message "
                    f"POST /api/builds/<id>/design and poll GET /api/builds/<id>/design until idle, then GET "
                    f"/api/builds/<id>" + ("" if args.keep else " and DELETE /api/builds/<id>"))
        for c in checks:
            for m in PROMPTS[c]:
                console.out(f"  {c}: {json.dumps(m)}")
        return EXIT_PASS
    srp = [bool(args.pool_id), bool(args.client_id), bool(args.username)]
    if any(srp) and not all(srp):
        return cannot("--pool-id, --client-id and --username go together")
    if not token and not all(srp):
        return cannot("no credentials: pass AX_ID_TOKEN (or --id-token), or --pool-id --client-id --username "
                      "with AX_PASSWORD")
    try:
        get_token = (srp_token_provider(args.pool_id, args.client_id, args.username, password) if all(srp)
                     else (lambda: token))
    except CannotRun as e:
        return cannot(str(e))
    except Exception as e:  # noqa: BLE001 - whatever the sign-in library raised, without a traceback
        return cannot(f"sign-in failed ({type(e).__name__}: {redact(e)})")
    s = Session(Api(base, get_token, console, timeout=60.0), console, args.timeout, args.review)
    try:
        me = s.api.call("GET", "/api/me") or {}
    except (ApiError, Unreachable) as e:
        return cannot(f"the console did not answer /api/me: {e}")
    if me.get("consoleMode") not in (None, "builder"):
        return cannot("this URL is not a Control Plane console (the Assistant runs there)")
    rows: list[tuple[str, str, str]] = []
    for c in checks:
        try:
            rows.append((c, *run_check(s, c, args.keep)))
        except TimeoutError as e:
            rows.append((c, FAIL, str(e)))
        except (ApiError, Unreachable) as e:
            rows.append((c, NOT_TESTABLE, f"the console call failed ({e})"))
        console.log(f"{c}: {rows[-1][1]} {rows[-1][2][:200]}")
    console.out()
    console.out(table(rows))
    return EXIT_FAIL if any(r == FAIL for _c, r, _e in rows) else EXIT_PASS


def main(argv: list[str] | None = None) -> int:
    try:
        return run(sys.argv[1:] if argv is None else argv)
    except KeyboardInterrupt:
        print("assistant_regression: interrupted (builds made so far may be left)", file=sys.stderr)
        return EXIT_CANNOT_RUN


if __name__ == "__main__":
    sys.exit(main())
