#!/usr/bin/env python3
"""A scripted end-to-end release test for a DEPLOYED AgentExpress app.

Before a release someone deploys the app, signs in, runs the workflow through every review gate and
checks that telemetry came out. This does that, against any deployed app, with one command:

    export AX_PASSWORD='...'                      # never a command-line argument
    python3 scripts/e2e_smoke.py --url https://dxxxx.cloudfront.net \\
        --pool-id us-east-1_AbCdEf --client-id 1abc... --username you@example.com
    # or, with an ID token you already have:
    AX_ID_TOKEN=eyJ... python3 scripts/e2e_smoke.py --url https://dxxxx.cloudfront.net

What it does: checks the app answers and who it is, reads the workflow (so it needs no agent names),
starts a run, polls with backoff, approves every review gate that waits (refusing to loop on one),
stops at done / failed / cancelled / timeout, then checks the run and prints one PASS/FAIL table.

Exit codes:   0  every check passed (warnings do not fail)
              1  a check failed
              2  it could not run: bad arguments, cannot sign in, app unreachable, or the app refused
                 to start a run (HTTP 429 from `orchestrator.limits`, or this URL is not a runnable app)

Safety. It talks only to `--url`, follows no redirect (a redirect would carry the token to another
host), accepts plain http only for localhost, and prints no token, password or Authorization header:
every line it writes passes through one redaction step, with or without `-v`, and an unexpected
exception is reported by type and redacted message, without a traceback. An HTTP error is shown as its
status and the server's `error` text, never the body.

It was built against a local fake of the app's API (tests/test_e2e_smoke.py). It has NOT been run
against a real deployment; scripts/README.md says what that leaves unproven.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

EXIT_PASS, EXIT_FAIL, EXIT_CANNOT_RUN = 0, 1, 2

TERMINAL = ("done", "failed", "cancelled", "denied")
#: Node states that are fine at the end of a run.
OK_NODE = ("done", "skipped")
#: The agent runtimes whose model calls the app meters itself. An `a2a` agent runs in its own Lambda,
#: and nothing in this repo says it writes `llm` telemetry rows, so none is required of it.
METERED_RUNTIMES = ("main", "dedicated")
SESSION_ID = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
MAX_BODY = 10_000_000
DEFAULT_TOPIC = "End-to-end smoke test: produce a short, sourced brief on a topic of your choosing."

PASS, FAIL, WARN, SKIP = "PASS", "FAIL", "WARN", "SKIP"


# --- Redaction ------------------------------------------------------------------------------------
class Redactor:
    """Everything this script prints goes through here. The known secrets are replaced by value, and
    anything shaped like a bearer header or a JWT is replaced by shape, in case a server echoes one."""

    SHAPES = (re.compile(r"(?i)\bbearer\s+[^\s'\"]+"), re.compile(r"\beyJ[\w-]+\.[\w-]+\.[\w-]*"))

    def __init__(self) -> None:
        self._secrets: list[str] = []

    def add(self, *values: str | None) -> None:
        for v in values:
            if v and len(v) >= 4 and v not in self._secrets:
                self._secrets.append(v)
        self._secrets.sort(key=len, reverse=True)

    def __call__(self, text: object) -> str:
        s = str(text)
        for v in self._secrets:
            s = s.replace(v, "***")
        for rx in self.SHAPES:
            s = rx.sub("***", s)
        return s


class Console:
    def __init__(self, redact: Redactor, verbose: bool) -> None:
        self.redact, self.verbose = redact, verbose

    def out(self, text: str = "") -> None:
        print(self.redact(text), flush=True)

    def err(self, text: str) -> None:
        print(self.redact(text), file=sys.stderr, flush=True)

    def log(self, text: str) -> None:
        self.out(f"[{time.strftime('%H:%M:%S')}] {text}")

    def debug(self, text: str) -> None:
        if self.verbose:
            self.log(f"  {text}")


# --- HTTP -----------------------------------------------------------------------------------------
class ApiError(Exception):
    """The app answered with an HTTP error. Carries the status and the server's `error` text only."""

    def __init__(self, status: int, text: str) -> None:
        super().__init__(f"HTTP {status}" + (f": {text}" if text else ""))
        self.status, self.text = status, text


class Unreachable(Exception):
    """No HTTP answer at all (DNS, refused, timeout, TLS)."""


class CannotRun(Exception):
    """Something that stops the script before or instead of a run: exit code 2."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):  # noqa: D401, ANN002
        return None


def _error_text(raw: bytes) -> str:
    try:
        err = json.loads(raw.decode("utf-8", "replace")).get("error")
    except (ValueError, AttributeError):
        return ""
    return " ".join(err.split())[:300] if isinstance(err, str) else ""


class Api:
    """The app's `/api/*`, nothing else. `token` is a callable so a signed-in session can renew."""

    def __init__(self, base: str, token, console: Console, timeout: float = 30.0) -> None:
        self.base, self._token, self.console, self.timeout = base, token, console, timeout
        self._opener = urllib.request.build_opener(_NoRedirect)

    def call(self, method: str, path: str, body: dict | None = None):
        url = self.base + path
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(url, data=data, method=method, headers={
            "Authorization": f"Bearer {self._token()}", "Accept": "application/json",
            **({"Content-Type": "application/json"} if data is not None else {})})
        try:
            with self._opener.open(req, timeout=self.timeout) as r:
                status, raw = r.status, r.read(MAX_BODY)
        except urllib.error.HTTPError as e:
            raw = e.read(MAX_BODY) if e.fp else b""
            self.console.debug(f"{method} {path} -> {e.code}")
            raise ApiError(e.code, _error_text(raw)) from None
        except (urllib.error.URLError, OSError, ValueError) as e:
            self.console.debug(f"{method} {path} -> no answer ({type(e).__name__})")
            raise Unreachable(f"{type(e).__name__}: {getattr(e, 'reason', e)}") from None
        self.console.debug(f"{method} {path} -> {status}")
        if not raw:
            return None
        try:
            return json.loads(raw.decode("utf-8", "replace"))
        except ValueError:
            raise ApiError(status, "the answer was not JSON") from None


# --- Sign in --------------------------------------------------------------------------------------
def srp_token_provider(pool_id: str, client_id: str, username: str, password: str):
    """Sign in with Cognito SRP (the pool allows nothing else) and return a callable that gives a
    current ID token. `pycognito` is imported here, and only here, so everything else is stdlib."""
    try:
        from pycognito import Cognito  # type: ignore[import-not-found]
    except ImportError:
        raise CannotRun(
            "pycognito is needed to sign in with --pool-id, and it is not installed here. Install it in "
            "a separate virtualenv and run this script with that Python:\n"
            "    python3 -m venv /tmp/pc && /tmp/pc/bin/pip install pycognito\n"
            "    /tmp/pc/bin/python scripts/e2e_smoke.py ...\n"
            "or pass an ID token you already have with --id-token / AX_ID_TOKEN.") from None
    user = Cognito(pool_id, client_id, user_pool_region=pool_id.split("_")[0], username=username)
    user.authenticate(password=password)

    def token() -> str:
        user.check_token()  # renews from the refresh token when the ID token has expired
        return user.id_token
    return token


# --- Arguments ------------------------------------------------------------------------------------
class _Parser(argparse.ArgumentParser):
    """argparse quotes the offending value in its messages ("unrecognized arguments: --password
    hunter2"). A mistyped option can have a secret after it, so only option NAMES are ever echoed."""

    def parse_args(self, args=None, namespace=None):
        ns, extra = self.parse_known_args(args, namespace)
        if extra:
            names = sorted({a.split("=")[0] for a in extra if a.startswith("-")})
            self.error("unrecognized option: " + (", ".join(names) or "(a stray value)"))
        return ns

    def error(self, message: str):
        self.print_usage(sys.stderr)
        # a quoted value in the message (invalid float value: 'abc') is not repeated either
        shown = re.sub(r"'[^']*'", "'...'", message)
        self.exit(EXIT_CANNOT_RUN, f"{self.prog}: error: {shown}\n")


def build_parser() -> argparse.ArgumentParser:
    # allow_abbrev off: `--pass` must not quietly mean something. argparse exits 2 on a bad argument,
    # which is this script's "could not run".
    p = _Parser(prog="e2e_smoke.py", description=(__doc__ or "").split("\n\n")[0],
                allow_abbrev=False,
                epilog="The password is read from the environment variable AX_PASSWORD only.")
    p.add_argument("--url", required=True, help="the app's CloudFront URL or API base (https; http only for localhost)")
    p.add_argument("--id-token", default=None, help="a Cognito ID token (or set AX_ID_TOKEN; the variable is safer)")
    p.add_argument("--pool-id", default=None, help="Cognito user pool id, to sign in with SRP")
    p.add_argument("--client-id", default=None, help="Cognito app client id, to sign in with SRP")
    p.add_argument("--username", default=None, help="the user to sign in as (the password is in AX_PASSWORD)")
    p.add_argument("--topic", default=None, help="the request to run (default: the workflow's own default topic)")
    p.add_argument("--timeout", type=float, default=1200.0, help="seconds to wait for the run in all (default 1200)")
    p.add_argument("--approve-as", default="e2e-smoke", help="a label put in the comment of every approval")
    p.add_argument("--keep", action="store_true", help="do not delete the run afterwards")
    p.add_argument("--dry-run", action="store_true", help="print what would be called and call nothing")
    p.add_argument("-v", "--verbose", action="store_true", help="log each request's method, path and status")
    p.add_argument("--poll", type=float, default=2.0, help="first wait between polls, seconds (default 2)")
    p.add_argument("--poll-max", type=float, default=10.0, help="longest wait between polls (default 10)")
    p.add_argument("--gate-grace", type=float, default=20.0,
                   help="seconds to wait for an approved gate to clear before approving it again (default 20)")
    p.add_argument("--max-approvals", type=int, default=3,
                   help="approvals sent to one gate before it counts as stuck (default 3)")
    return p


def normalise_base(url: str) -> str:
    u = urllib.parse.urlsplit(url.strip())
    if u.scheme not in ("http", "https") or not u.hostname:
        raise ValueError("--url must be an http(s) address, such as https://dxxxx.cloudfront.net")
    if u.scheme == "http" and u.hostname not in ("localhost", "127.0.0.1", "::1"):
        raise ValueError("--url must be https: a token is sent with every call (http is allowed for localhost only)")
    if u.username or u.password or u.query or u.fragment:
        raise ValueError("--url takes no credentials, query or fragment")
    path = u.path.rstrip("/")
    if path.endswith("/api"):
        path = path[: -len("/api")]
    return f"{u.scheme}://{u.netloc}{path}"


# --- Checks ---------------------------------------------------------------------------------------
class Results:
    def __init__(self) -> None:
        self.rows: list[tuple[str, str, str]] = []

    def add(self, name: str, status: str, evidence: str) -> None:
        self.rows.append((name, status, evidence))

    def worst(self) -> int:
        return EXIT_FAIL if any(s == FAIL for _, s, _ in self.rows) else EXIT_PASS

    def table(self) -> str:
        w = max([len(n) for n, _, _ in self.rows] + [5])
        lines = [f"{'CHECK'.ljust(w)}  {'RESULT':<6}  EVIDENCE", f"{'-' * w}  {'-' * 6}  {'-' * 40}"]
        lines += [f"{n.ljust(w)}  {s:<6}  {e}" for n, s, e in self.rows]
        counts = {k: sum(1 for _, s, _ in self.rows if s == k) for k in (PASS, FAIL, WARN, SKIP)}
        lines.append("")
        lines.append("  ".join(f"{k}: {v}" for k, v in counts.items()))
        return "\n".join(lines)


def _num(v) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _signature(snap: dict) -> tuple:
    nodes = snap.get("nodes") or {}
    return (snap.get("overall"), (snap.get("hitl") or {}).get("node"),
            tuple(sorted((k, (v or {}).get("status")) for k, v in nodes.items())))


def drive_run(api: Api, sid: str, args, console: Console) -> tuple[dict, str, list[str], str]:
    """Poll the run to the end, approving gates. Returns (last snapshot, how it ended, gates approved,
    detail). `how` is one of done / failed / cancelled / denied / timeout / stuck / lost."""
    deadline = time.monotonic() + args.timeout
    wait, last_sig = args.poll, None
    approved: list[str] = []
    sent: dict[str, int] = {}          # approvals sent per gate
    sent_at: dict[str, float] = {}     # when the last one went
    snap: dict = {}
    bad_polls = 0
    while True:
        try:
            snap = api.call("GET", f"/api/sessions/{sid}") or {}
            bad_polls = 0
        except (Unreachable, ApiError) as e:
            if isinstance(e, ApiError) and e.status in (401, 403, 404):
                return snap, "lost", approved, f"polling the run: {e}"
            bad_polls += 1
            if bad_polls >= 3:
                return snap, "lost", approved, f"polling the run failed {bad_polls} times in a row: {e}"
            console.log(f"poll failed ({e}); retrying")
        else:
            overall = str(snap.get("overall") or "")
            sig = _signature(snap)
            if sig != last_sig:
                nodes = snap.get("nodes") or {}
                console.log(f"run {overall or '?'}: " + (", ".join(
                    f"{k}={(v or {}).get('status')}" for k, v in nodes.items()) or "no nodes yet"))
                last_sig, wait = sig, args.poll
            if overall in TERMINAL:
                return snap, overall, approved, ""
            gate = str((snap.get("hitl") or {}).get("node") or "") if snap.get("hitl") else ""
            if gate:
                now = time.monotonic()
                if sent.get(gate, 0) == 0 or now - sent_at[gate] >= args.gate_grace:
                    if sent.get(gate, 0) >= args.max_approvals:
                        return snap, "stuck", approved, (
                            f"gate '{gate}' still waiting after {sent[gate]} approvals")
                    try:
                        api.call("POST", f"/api/sessions/{sid}/decision", {
                            "decision": "approve", "comment": f"approved by e2e-smoke ({args.approve_as})"})
                    except (Unreachable, ApiError) as e:
                        return snap, "lost", approved, f"approving gate '{gate}': {e}"
                    sent[gate] = sent.get(gate, 0) + 1
                    sent_at[gate] = now
                    if gate not in approved:
                        approved.append(gate)
                    console.log(f"gate '{gate}' is waiting: approved (approval {sent[gate]} of at most "
                                f"{args.max_approvals})")
                    wait = args.poll
        if time.monotonic() + wait > deadline:
            return snap, "timeout", approved, f"no end after {args.timeout:g}s"
        time.sleep(wait)
        wait = min(wait * 1.5, args.poll_max)


def check_nodes(res: Results, snap: dict, ended: str, agents: dict) -> None:
    nodes = {k: (v or {}).get("status") for k, v in (snap.get("nodes") or {}).items()
             if not agents or k in agents}
    bad = {k: s for k, s in nodes.items() if s not in OK_NODE and s != "pending"}
    unreached = sorted(k for k, s in nodes.items() if s == "pending")
    summary = ", ".join(f"{k}={s}" for k, s in nodes.items()) or "no agent nodes in the snapshot"
    if not nodes:
        res.add("every agent ended done or skipped", FAIL, summary)
    elif bad:
        res.add("every agent ended done or skipped", FAIL, "not done: " + ", ".join(f"{k}={s}" for k, s in bad.items()))
    elif unreached and ended == "done":
        res.add("every agent ended done or skipped", WARN,
                f"{summary} (not reached: a branch may have ended the run early)")
    elif unreached:
        res.add("every agent ended done or skipped", FAIL, f"never started: {', '.join(unreached)}")
    else:
        res.add("every agent ended done or skipped", PASS, summary)


def check_telemetry(res: Results, tel: dict | None, tel_error: str, snap: dict, agents: dict) -> None:
    if tel is None:
        for n in ("telemetry returned rows", "run cost is above zero", "each model-calling agent has rows"):
            res.add(n, FAIL if n == "telemetry returned rows" else SKIP, tel_error)
        return
    calls = tel.get("calls") or []
    totals = tel.get("totals") or {}
    if not calls:
        res.add("telemetry returned rows", FAIL, "GET telemetry returned no rows for this run")
    else:
        res.add("telemetry returned rows", PASS,
                f"{len(calls)} rows, {len(tel.get('byAgent') or [])} agents, {int(_num(totals.get('inputTokens')))} "
                f"input / {int(_num(totals.get('outputTokens')))} output tokens")
    cost = _num(totals.get("costUsd"))
    if cost > 0:
        res.add("run cost is above zero", PASS, f"${cost:.4f}")
    else:
        res.add("run cost is above zero", WARN if calls else SKIP,
                "cost is zero: prices unknown?" if calls else "no rows, so no cost")
    nodes = snap.get("nodes") or {}
    expected = sorted(k for k, v in nodes.items() if (v or {}).get("status") == "done"
                      and (not agents or k in agents)
                      and str((agents.get(k) or {}).get("runtime") or "main") in METERED_RUNTIMES)
    seen = {str(c.get("agent_id")) for c in calls if c.get("kind") == "llm"}
    missing = [k for k in expected if k not in seen]
    skipped_rt = sorted(k for k in nodes if k in agents and (agents[k] or {}).get("runtime") not in (None, *METERED_RUNTIMES))
    note = f" ({', '.join(skipped_rt)}: runtime not metered by the app, not required)" if skipped_rt else ""
    if not expected:
        res.add("each model-calling agent has rows", SKIP, "no finished agent that the app meters" + note)
    elif missing:
        res.add("each model-calling agent has rows", FAIL, f"no model-call row for: {', '.join(missing)}")
    else:
        res.add("each model-calling agent has rows", PASS, f"{len(expected)} agents with a model-call row" + note)


def check_limits(res: Results, api: Api, run_cost: float) -> None:
    name = "limits reachable and consistent"
    try:
        lim = api.call("GET", "/api/limits")
    except ApiError as e:
        res.add(name, SKIP if e.status == 404 else FAIL, f"{e}" + (" (this app predates /api/limits)" if e.status == 404 else ""))
        return
    except Unreachable as e:
        res.add(name, FAIL, str(e))
        return
    if not isinstance(lim, dict) or "limits" not in lim:
        res.add(name, FAIL, "GET /api/limits did not answer with a `limits` field")
        return
    if not lim["limits"]:
        res.add(name, PASS, "no limit is set (nothing to check)")
        return
    problems, notes = [], []
    for key in ("daily", "monthly", "hourly"):
        e = lim.get(key)
        if e is None:
            continue
        lm, used = (e.get("limit"), e.get("used")) if isinstance(e, dict) else (None, None)
        if not isinstance(lm, (int, float)) or not isinstance(used, (int, float)) or lm <= 0 or used < 0 \
                or not isinstance(e.get("resets"), str) or not isinstance(e.get("warn"), bool) \
                or not isinstance(e.get("reached"), bool):
            problems.append(f"{key}: malformed {e!r}"[:120])
            continue
        share = used / lm
        if e["warn"] != (share >= 0.8) or e["reached"] != (share >= 1):
            problems.append(f"{key}: warn/reached disagree with {used:g} of {lm:g}")
        if key == "hourly" and used < 1:
            problems.append("hourly: shows 0 runs this hour right after this run started")
        if key in ("daily", "monthly") and run_cost > 0 and used + 1e-6 < run_cost:
            problems.append(f"{key}: used {used:g} is below this run's own cost {run_cost:.4f}")
        notes.append(f"{key} {used:g} of {lm:g}" + (" REACHED" if e["reached"] else ""))
    if not notes and not problems:
        res.add(name, FAIL, "limits is true but no limit is listed")
    elif problems:
        res.add(name, FAIL, "; ".join(problems))
    elif any("REACHED" in n for n in notes):
        res.add(name, WARN, "; ".join(notes) + " (the next run would be refused)")
    else:
        res.add(name, PASS, "; ".join(notes))


# --- Main -----------------------------------------------------------------------------------------
def dry_run(args, base: str, via_srp: bool, console: Console) -> None:
    console.out("DRY RUN: nothing is called. This is what a real run would do against " + base)
    if via_srp:
        console.out(f"  sign in    Cognito SRP, pool {args.pool_id}, client {args.client_id}, user {args.username} "
                    f"(password from AX_PASSWORD)")
    elif args.id_token:
        console.out("  sign in    use the given ID token")
    else:
        console.out("  sign in    (no credentials were given; a real run needs --id-token or --pool-id)")
    h = "the ID token goes in an Authorization header on every call; it is never printed"
    topic = json.dumps({"topic": args.topic} if args.topic else {"topic": "<the workflow's default topic>"})
    for line in (
            f"GET    {base}/api/me                     [{h}]",
            f"GET    {base}/api/workflow",
            f"POST   {base}/api/sessions               {topic}",
            f"GET    {base}/api/sessions/<id>          every {args.poll:g}s, backing off to {args.poll_max:g}s, "
            f"until done / failed / cancelled / {args.timeout:g}s",
            f"POST   {base}/api/sessions/<id>/decision {{\"decision\": \"approve\"}} whenever a gate waits "
            f"(at most {args.max_approvals} times per gate)",
            f"GET    {base}/api/sessions/<id>/telemetry",
            f"GET    {base}/api/limits",
            (f"(kept) the run is not deleted" if args.keep else f"DELETE {base}/api/sessions/<id>")):
        console.out("  " + line)


def run(argv: list[str], env: dict | None = None) -> int:
    env = os.environ if env is None else env
    redact = Redactor()
    try:
        args = build_parser().parse_args(argv)
    except SystemExit as e:  # argparse printed its usage error; 2 is "could not run"
        return int(e.code) if isinstance(e.code, int) else EXIT_CANNOT_RUN
    # Secrets are registered before anything can print them.
    token = (args.id_token or env.get("AX_ID_TOKEN") or "").strip()
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    password = env.get("AX_PASSWORD") or ""
    redact.add(token, password, args.id_token)
    console = Console(redact, args.verbose)
    try:
        return _run(args, token, password, console)
    except KeyboardInterrupt:
        console.err("e2e_smoke: interrupted (a run that was started may still be going)")
        return EXIT_CANNOT_RUN
    except Exception as e:  # noqa: BLE001 - last resort; never a traceback, which could carry a header
        console.err(f"e2e_smoke: unexpected {type(e).__name__}: {e}")
        return EXIT_CANNOT_RUN


def _run(args, token: str, password: str, console: Console) -> int:
    def cannot(msg: str) -> int:
        console.err(f"e2e_smoke: cannot run: {msg}")
        return EXIT_CANNOT_RUN

    try:
        base = normalise_base(args.url)
    except ValueError as e:
        return cannot(str(e))
    srp_given = [bool(args.pool_id), bool(args.client_id), bool(args.username)]
    via_srp = all(srp_given)
    if any(srp_given) and not via_srp:
        return cannot("--pool-id, --client-id and --username go together")
    if token and via_srp:
        return cannot("give either an ID token or --pool-id/--client-id/--username, not both")
    if not (token or via_srp) and not args.dry_run:
        return cannot("no credentials: pass --id-token (or AX_ID_TOKEN), or --pool-id --client-id --username "
                      "with AX_PASSWORD set")
    if via_srp and not password and not args.dry_run:
        return cannot("AX_PASSWORD is not set (the password is read from that environment variable only)")
    if re.search(r"\s", token):
        return cannot("the ID token contains whitespace; paste the bare token")
    if args.timeout <= 0 or args.poll <= 0 or args.poll_max < args.poll or args.gate_grace < 0 \
            or args.max_approvals < 1:
        return cannot("--timeout and --poll must be above zero, --poll-max not below --poll, "
                      "--max-approvals at least 1")
    if args.dry_run:
        dry_run(args, base, via_srp, console)
        return EXIT_PASS

    try:
        get_token = srp_token_provider(args.pool_id, args.client_id, args.username, password) if via_srp \
            else (lambda: token)
    except CannotRun as e:
        return cannot(str(e))
    except Exception as e:  # noqa: BLE001 - whatever the sign-in library raised; reported without a traceback
        return cannot(f"sign-in failed ({type(e).__name__}: {e})")
    api = Api(base, get_token, console)
    res = Results()

    # 1. The app answers, and who it is.
    try:
        me = api.call("GET", "/api/me") or {}
        wf = api.call("GET", "/api/workflow") or {}
    except ApiError as e:
        return cannot(f"the app refused the sign-in or the first call: {e}" if e.status in (401, 403)
                      else f"the app answered badly: {e}")
    except Unreachable as e:
        return cannot(f"the app is unreachable at {base}: {e}")
    if me.get("consoleMode") == "builder":
        return cannot("this URL is a Control Plane console: it runs no workflow of its own")
    agents = wf.get("agents") if isinstance(wf.get("agents"), dict) else {}
    console.log(f"signed in as {me.get('user') or '?'}; workflow has {len(agents)} agents, "
                f"{len(wf.get('steps') or [])} steps")
    res.add("app answers and who it is", PASS, f"user {me.get('user') or '?'}, {len(agents)} agents")

    # 2. Start a run.
    topic = args.topic or str((wf.get("ui") or {}).get("defaultTopic") or "").strip() or DEFAULT_TOPIC
    try:
        started = api.call("POST", "/api/sessions", {"topic": topic}) or {}
    except ApiError as e:
        if e.status == 429:
            return cannot(f"the app refused to start a run: it is over its limit (HTTP 429): {e.text}")
        return cannot(f"could not start a run: {e}")
    except Unreachable as e:
        return cannot(f"the app is unreachable at {base}: {e}")
    sid = str(started.get("session_id") or "")
    if not SESSION_ID.match(sid):
        return cannot("the app did not return a usable session id")
    console.log(f"run {sid} started")

    # 3. Drive it to the end.
    snap, ended, approved, detail = drive_run(api, sid, args, console)
    if ended in ("timeout", "stuck"):
        # A run left going keeps spending, and deleting a running one would orphan it.
        try:
            api.call("POST", f"/api/sessions/{sid}/cancel")
            console.log("run cancelled (it did not finish)")
        except (Unreachable, ApiError) as e:
            console.log(f"could not cancel the run: {e}")
    res.add("run reached done", PASS if ended == "done" else FAIL,
            f"run {sid} ended '{ended}'" + (f": {detail}" if detail else ""))
    gate_stuck = ended == "stuck" or (ended == "timeout" and bool(snap.get("hitl")))
    res.add("review gates cleared", FAIL if gate_stuck else PASS,
            (f"approved: {', '.join(approved)}" if approved else "no gate waited")
            + (f"; {detail}" if gate_stuck else ""))
    check_nodes(res, snap, ended, agents)

    # 4. Telemetry and limits, read before the run is deleted.
    tel, tel_err = None, ""
    try:
        tel = api.call("GET", f"/api/sessions/{sid}/telemetry")
        if not isinstance(tel, dict):
            tel, tel_err = None, "the telemetry answer was not an object"
    except (ApiError, Unreachable) as e:
        tel_err = str(e)
    check_telemetry(res, tel, tel_err, snap, agents)
    check_limits(res, api, _num(((tel or {}).get("totals") or {}).get("costUsd")))

    # 5. Clean up.
    if args.keep:
        res.add("run deleted", SKIP, f"--keep: run {sid} left in place")
    else:
        try:
            api.call("DELETE", f"/api/sessions/{sid}")
            res.add("run deleted", PASS, f"run {sid} removed")
        except (ApiError, Unreachable) as e:
            res.add("run deleted", WARN, f"run {sid} was NOT removed: {e}")

    console.out()
    console.out(res.table())
    code = res.worst()
    console.out(f"\nexit {code}: " + ("all checks passed" if code == EXIT_PASS else "a check failed"))
    return code


def main(argv: list[str] | None = None) -> int:
    return run(sys.argv[1:] if argv is None else argv)


if __name__ == "__main__":
    sys.exit(main())
