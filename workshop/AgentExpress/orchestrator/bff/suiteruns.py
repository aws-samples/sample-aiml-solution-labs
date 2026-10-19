"""Running a build's test suites, and keeping what each run found (the engine is suites.py).

The suites belong to the build's PROJECT: a `suites` list beside `workflow` and `prompts`, in
the draft, so they are saved, exported and imported with the build and frozen into each
deployed version's bundle (bff/buildstore.py bundle_of). Nothing here adds a workflow.json key.

    GET  /api/builds/{id}/suites            the suites, and the trend of each (?name= for one)
    PUT  /api/builds/{id}/suites            replace them, validated (what the assistant or a script calls;
                                            the Builder's Tests tab saves them with the build's autosave)
    POST /api/builds/{id}/suite-runs        run one suite {"suite": name}: one run per case
    GET  /api/builds/{id}/suite-runs/{run}  where a suite run stands, case by case

A SUITE RUN starts one run per case through the same `_begin_run` a person and a trigger use, with
`gates: "auto"` so the gates approve themselves and an unattended run can finish. So it is held to
`orchestrator.limits` like any run; a case the limits refuse is recorded as "not started: <why>" and
is not counted against the build. Nothing is scheduled and no worker waits: the RESULT IS COMPUTED
WHEN IT IS READ, from the runs' own snapshots, so a build that never runs a suite costs nothing.

WHAT IS STORED (the builds table, in the build's own partition, which a delete of the build removes)

    pk BUILD#<id>   sk SUITERUN#<suiteKey>-<time>-<rand>   one item per suite run

The suite run id IS the sort key after `SUITERUN#`, so a run is one GetItem. `suiteKey` is a hash of
the suite's name, so one query lists a suite's runs oldest to newest (the trend), and renaming a suite
starts a new trend. The item carries a few plain attributes for the trend (`suite`, `owner`,
`buildVersion`, `startedAt`, `state`, and the counts once it has finished) and a JSON `body` with
the suite exactly as it was run, the run id of each case, and the outcome once it has finished.

A suite run FINISHES when no case is still running, and the outcome is then written once and never
recomputed: deleting a run afterwards, or destroying the build, no longer changes it. Until then every
read grades the runs again. A run that has not ended after `SETTLE_AFTER_SECONDS` counts as failed, so
a run that died without writing its end cannot keep a suite open forever.

The newest `TREND_KEEP` runs of each suite are kept. A run is private to whoever started it (like any
run): only they read its cases. Everyone who can open the build sees the trend, which holds counts only.

NOT COVERED
  * A tool that acts as the person (auth "user") needs that person's consent in the middle of a run; a
    suite run has no person to ask, and passes no sign-in on to the runs.
  * All the cases start at once. The suite cap (suites.MAX_CASES) bounds how many runs that is; a
    deployment that throttles concurrent runs will fail some of them, and the case says why.
  * A suite run that nobody reads stays open (shown as running in the trend) until someone does.
  * Deleting a suite, or renaming it, leaves its old runs in the table until the build is deleted.
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import json
import re
import secrets

import buildstore
import suites

#: Runs of one suite kept for its trend.
TREND_KEEP = 20
#: A case whose run has not ended this long after the suite run started counts as failed.
SETTLE_AFTER_SECONDS = 3 * 3600
#: Failed checks kept per case in the stored outcome, and the length of each message.
FAILED_SHOWN = 5
MESSAGE_MAX = 400
PREFIX = buildstore.SUITE_RUN_PREFIX
#: <suiteKey: 12 hex>-<UTC time to the microsecond>-<6 hex>. Sorts by time within a suite.
RUN_ID_RE = re.compile(r"^[0-9a-f]{12}-\d{8}T\d{12}-[0-9a-f]{6}$")
#: The routes handler.py sends here (a parity test reads the route lists of both IaC paths).
ROUTES = ("GET /api/builds/{id}/suites", "PUT /api/builds/{id}/suites",
          "POST /api/builds/{id}/suite-runs", "GET /api/builds/{id}/suite-runs/{run}")


class SuitesInvalid(Exception):
    """The suites (or the one asked to run) have errors; `issues` is the validator's list."""

    def __init__(self, message: str, issues: list[dict]):
        super().__init__(message)
        self.issues = issues


def _b():
    """bff/builds.py, imported when used: the tests import it afresh for each fixture, and a
    module-level import here would keep the first one's tables."""
    import builds
    return builds


def suite_key(name: str) -> str:
    return hashlib.sha256(str(name).strip().encode()).hexdigest()[:12]


run_key = buildstore.suite_run_key


def new_run_id(name: str, now: _dt.datetime | None = None) -> str:
    stamp = (now or _dt.datetime.now(_dt.UTC)).strftime("%Y%m%dT%H%M%S%f")
    return f"{suite_key(name)}-{stamp}-{secrets.token_hex(3)}"


def _parse(stamp: str) -> _dt.datetime:
    return _dt.datetime.strptime(stamp, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=_dt.UTC)


def _clip(text: str, n: int = MESSAGE_MAX) -> str:
    text = str(text)
    return text if len(text) <= n else text[: n - 1] + "…"


# --- the suites of a build ----------------------------------------------------------------------

def _agents(project: dict) -> set[str]:
    agents = (project.get("workflow") or {}).get("agents")
    return set(agents) if isinstance(agents, dict) else set()


def stored(project: dict) -> list:
    got = project.get("suites")
    return got if isinstance(got, list) else []


def _find(project: dict, name: str) -> dict:
    for s in stored(project):
        if isinstance(s, dict) and str(s.get("name") or "").strip() == str(name or "").strip():
            return s
    raise _b().BuildError(404, f"this build has no suite named {str(name or '')[:80]!r}")


def put_suites(build_id: str, who, email: str, given) -> dict:
    """Replace the build's suites. Errors refuse the whole list; warnings come back with it."""
    b = _b()
    b._meta(build_id, who)
    if not isinstance(given, list):
        raise b.BuildError(400, 'body must be {"suites": [...]}')
    project = b._draft(build_id)
    if not isinstance(project.get("workflow"), dict):
        raise b.BuildError(409, "this build has no saved draft to put suites in")
    issues = suites.validate_suites(given, _agents(project))
    bad = suites.errors(issues)
    if bad:
        raise SuitesInvalid(f"the suites have {len(bad)} error{'s' if len(bad) != 1 else ''}: "
                            f"{bad[0]['path']}: {bad[0]['message']}", issues)
    b.save(build_id, who, {**project, "suites": given}, email)
    return {"suites": given, "issues": issues}


def listing(build_id: str, who, name: str | None = None) -> dict:
    """The build's suites with the trend of each; `name` narrows it to one suite."""
    b = _b()
    b._meta(build_id, who, write=False)
    project = b._draft(build_id)
    have = stored(project)
    trends = _trends(build_id)
    if name is not None:
        s = _find(project, name)
        return {"suite": s, "trend": trends.get(str(s["name"]).strip(), [])}
    return {"suites": have, "trends": trends}


# --- the trend ------------------------------------------------------------------------------------

_TREND_FIELDS = ("runId", "suite", "buildVersion", "startedAt", "state", "passed", "failed", "total", "notStarted")


def _trend_entry(item: dict) -> dict:
    finished = item.get("state") == "finished"
    out = {"suiteRunId": item["runId"], "startedAt": item.get("startedAt", ""),
           "buildVersion": int(item["buildVersion"]) if item.get("buildVersion") is not None else None,
           "finished": finished}
    if finished:
        out.update(passed=int(item.get("passed") or 0), failed=int(item.get("failed") or 0),
                   total=int(item.get("total") or 0), notStarted=int(item.get("notStarted") or 0))
    return out


def _query(build_id: str, begins: str, fields: tuple[str, ...]) -> list[dict]:
    names = {f"#{i}": f for i, f in enumerate(fields)}
    kwargs = {"KeyConditionExpression": "pk = :p AND begins_with(sk, :s)",
              "ExpressionAttributeValues": {":p": f"BUILD#{build_id}", ":s": begins},
              "ProjectionExpression": ", ".join(names), "ExpressionAttributeNames": names}
    items: list[dict] = []
    while True:
        page = _b()._table.query(**kwargs)
        items.extend(page.get("Items", []))
        if not page.get("LastEvaluatedKey"):
            return items
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def _trends(build_id: str) -> dict[str, list[dict]]:
    """Suite name -> its newest `TREND_KEEP` runs, oldest first."""
    by_suite: dict[str, list[dict]] = {}
    for item in sorted(_query(build_id, PREFIX, _TREND_FIELDS), key=lambda i: str(i["runId"]).split("-", 1)[1]):
        by_suite.setdefault(str(item.get("suite") or ""), []).append(item)
    return {name: [_trend_entry(i) for i in suites.keep_newest(items, TREND_KEEP)[0]]
            for name, items in by_suite.items()}


def _prune(build_id: str, key: str) -> None:
    """Drop all but the newest `TREND_KEEP` runs of one suite."""
    ids = sorted((i["runId"] for i in _query(build_id, f"{PREFIX}{key}-", ("runId",))),
                 key=lambda r: r.split("-", 1)[1])
    _, dropped = suites.keep_newest(ids, TREND_KEEP)
    for run_id in dropped:
        _b()._table.delete_item(Key=run_key(build_id, run_id))


# --- starting -------------------------------------------------------------------------------------

def start(build_id: str, who, name: str, version: int, begin, now: _dt.datetime | None = None) -> dict:
    """Run one suite: `begin(case)` starts a run for a case and returns its session id (it raises
    the limits' BuildError when the case may not start, and returns None when the run could not be
    created). The deployed `version` is recorded with the suite run for the trend."""
    b = _b()
    b._meta(build_id, who)
    project = b._draft(build_id)
    suite = _find(project, name)
    issues = suites.validate_suite(suite, "", _agents(project))
    bad = suites.errors(issues)
    if bad:
        raise SuitesInvalid(f"suite {suite.get('name')!r} has {len(bad)} error{'s' if len(bad) != 1 else ''}: "
                            f"{bad[0]['path']}: {bad[0]['message']}", issues)
    if not suite["cases"]:
        raise b.BuildError(400, f"suite {suite['name']!r} has no cases to run")
    started_at = buildstore.now()
    run_id = new_run_id(suite["name"], now)
    cases: list[dict] = []
    refused = ""
    for case in suite["cases"]:
        entry = {"case": case["name"].strip(), "sessionId": None, "notStarted": None}
        if refused:
            # Spend only grows and runs are only counted up, so once one case is refused the
            # rest would be: say so without asking again.
            entry["notStarted"] = refused
        else:
            try:
                sid = begin(case, run_id)
            except b.BuildError as e:
                refused = entry["notStarted"] = str(e)
            else:
                if sid is None:
                    entry["notStarted"] = "the run could not be created"
                else:
                    entry["sessionId"] = sid
        cases.append(entry)
    body = {"definition": suite, "cases": cases}
    b._table.put_item(Item={
        **run_key(build_id, run_id), "runId": run_id, "suite": suite["name"].strip(), "owner": str(who),
        "buildVersion": int(version), "startedAt": started_at, "state": "running",
        "body": json.dumps(body, ensure_ascii=False)})
    _prune(build_id, suite_key(suite["name"]))
    return {"suiteRunId": run_id, "suite": suite["name"].strip(), "buildVersion": int(version),
            "startedAt": started_at, "cases": cases,
            "started": sum(1 for c in cases if c["sessionId"]), "notStarted": sum(1 for c in cases if c["notStarted"])}


# --- reading the result ---------------------------------------------------------------------------

def _outcome(entry: dict, case: dict, snapshot, totals, expired: bool) -> dict:
    """One case of a suite run, in the shape the result is stored and returned in."""
    if entry.get("notStarted") or not entry.get("sessionId"):
        why = entry.get("notStarted") or "the suite run was interrupted before this case started"
        return {"case": entry["case"], "sessionId": None, "status": "not_started",
                "message": _clip(f"Not started: {why}"), "checks": {"passed": 0, "total": 0}, "failed": []}
    g = suites.grade(case, snapshot, totals, expired=expired)
    done = g["checks"]
    return {"case": entry["case"], "sessionId": entry["sessionId"], "status": g["status"],
            "message": _clip(g["message"]),
            "checks": {"passed": sum(1 for c in done if c["passed"]), "total": len(done)},
            "failed": [{"expected": _clip(c["expected"]), "actual": _clip(c["actual"]), "message": _clip(c["message"]),
                        **({"implicit": True} if c.get("implicit") else {})}
                       for c in done if not c["passed"]][:FAILED_SHOWN]}


def result(build_id: str, who, run_id: str, reader, now: _dt.datetime | None = None) -> dict:
    """Where one suite run stands, case by case, with the trend of its suite.

    `reader()` returns the function that fetches a run: `read(session_id) -> (snapshot | None,
    totals | None)`. It is called only if the suite run is still open, so a finished one is
    answered from what was stored and needs the build to be deployed no more."""
    b = _b()
    b._meta(build_id, who)
    if not RUN_ID_RE.match(run_id or ""):
        raise b.BuildError(400, "invalid suite run id")
    item = b._table.get_item(Key=run_key(build_id, run_id), ConsistentRead=True).get("Item")
    # A run is private to whoever started it; for anyone else it is not there.
    if not item or str(item.get("owner")) != str(who):
        raise b.BuildError(404, "unknown suite run")
    body = json.loads(item["body"])
    definition = body["definition"]
    if item.get("state") == "finished":
        outcomes = body["outcome"]
        summary = {k: int(item.get(k) or 0) for k in ("passed", "failed", "total", "notStarted")} | {"running": 0}
    else:
        read = None
        age = ((now or _dt.datetime.now(_dt.UTC)) - _parse(item["startedAt"])).total_seconds()
        outcomes = []
        for entry, case in zip(body["cases"], definition["cases"], strict=True):
            snapshot = totals = None
            if entry.get("sessionId"):
                read = read or reader()
                snapshot, totals = read(entry["sessionId"])
            outcomes.append(_outcome(entry, case, snapshot, totals, age > SETTLE_AFTER_SECONDS))
        summary = suites.summarize(outcomes)
        if summary["running"] == 0:
            _finish(build_id, run_id, body, outcomes, summary, item)
    return {"suiteRunId": run_id, "suite": item["suite"], "buildVersion": int(item["buildVersion"]),
            "startedAt": item["startedAt"], "state": "finished" if summary["running"] == 0 else "running",
            "summary": summary, "cases": outcomes,
            "trend": _trends(build_id).get(str(item["suite"]), [])}


def _finish(build_id: str, run_id: str, body: dict, outcomes: list[dict], summary: dict, item: dict) -> None:
    """Write the outcome once. A second reader finishing the same run at the same moment finds
    it already written, and the two agree anyway: the runs it read had ended."""
    names = {"#st": "state", "#p": "passed", "#f": "failed", "#t": "total", "#n": "notStarted", "#b": "body",
             "#a": "finishedAt"}
    try:
        _b()._table.update_item(
            Key=run_key(build_id, run_id),
            UpdateExpression="SET #st = :fin, #p = :p, #f = :f, #t = :t, #n = :n, #b = :b, #a = :a",
            ConditionExpression="#st = :open", ExpressionAttributeNames=names,
            ExpressionAttributeValues={":fin": "finished", ":open": "running", ":p": summary["passed"],
                                       ":f": summary["failed"], ":t": summary["total"],
                                       ":n": summary["notStarted"], ":a": buildstore.now(),
                                       ":b": json.dumps({**body, "outcome": outcomes}, ensure_ascii=False)})
    except Exception as e:  # only the lost race is expected
        if getattr(e, "response", {}).get("Error", {}).get("Code", "") != "ConditionalCheckFailedException":
            raise
