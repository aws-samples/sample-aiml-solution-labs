"""A build's test suites over the API (bff/suiteruns.py, the routes in bff/handler.py).

What has to hold:
  * suites live in the build's draft, are frozen into each version's bundle, and a build
    without any has a bundle exactly as before;
  * another user's build is a 404 on every suite route, and a suite run is read only by the
    person who started it, even by someone the build is shared with;
  * a suite run starts one run per case through the one run start, with the gates approving
    themselves, and a case the limits refuse is "not started", not a failure;
  * the result is computed from the runs of THAT suite run, is final once every case has
    ended, and the trend keeps only the newest runs of each suite.

DynamoDB and S3 are moto's, as in test_builds.py: a conditional write and a query on a
sort-key prefix are what a hand-rolled fake would get wrong.
"""
from __future__ import annotations

import decimal
import json
import re

import boto3
import pytest
from conftest import ORCH_ROOT
from test_builds import CTX, call, mark_deployed, project, version_object
from test_builds import env as base_env  # noqa: F401  (the fixture, under another name)

B = "pclaims01"
SUITE = {"name": "Triage", "cases": [
    {"name": "urgent", "request": "Claim 1: hospitalised", "subject": "acme",
     "expect": [{"agent": "claims_intake", "field": "riskScore", "gte": 80}, {"cost": {"lt": 0.5}}]},
    {"name": "routine", "request": "Claim 2: a dent",
     "expect": [{"agent": "claims_intake", "field": "riskScore", "lt": 30}]},
]}
OTHER = {"name": "Smoke", "cases": [{"name": "runs", "request": "Claim 3", "expect": []}]}
OWN = "/api/builds/{id}/suites"
RUNS = "/api/builds/{id}/suite-runs"
ONE = "/api/builds/{id}/suite-runs/{run}"


def put_project(e, suites=None, sub="u1", bid=B, **wf_over):
    body = project(**wf_over)
    if suites is not None:
        body["suites"] = suites
    return call(e, "PUT /api/builds/{id}", params={"id": bid}, body={"project": body}, sub=sub)


def deploy(e, bid=B):
    call(e, "POST /api/builds/{id}/deploy", params={"id": bid}, body={"tool": "cdk"})
    agent = mark_deployed(e, bid)
    boto3.client("dynamodb").create_table(
        TableName=f"{agent}_telemetry", BillingMode="PAY_PER_REQUEST",
        KeySchema=[{"AttributeName": "session_id", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
        AttributeDefinitions=[{"AttributeName": "session_id", "AttributeType": "S"},
                              {"AttributeName": "sk", "AttributeType": "S"}])
    return agent


def start(e, name="Triage", sub="u1", bid=B, **kw):
    return call(e, f"POST {RUNS}", params={"id": bid}, body={"suite": name}, sub=sub, **kw)


def result(e, run, sub="u1", bid=B):
    return call(e, f"GET {ONE}", params={"id": bid, "run": run}, path=f"/api/builds/{bid}/suite-runs/{run}", sub=sub)


def finish(agent, sid, overall="done", output=None, cost=None, latency=1000):
    """What the runtime writes when a run ends: the node outputs, the overall status, the calls' cost."""
    status = boto3.resource("dynamodb").Table(f"{agent}_status")
    item = status.get_item(Key={"session_id": sid})["Item"]
    for node, out in (output or {}).items():
        item["nodes"][node] = {"status": "done", "pct": 100, "output": out if isinstance(out, str) else json.dumps(out)}
    item["overall"] = overall
    status.put_item(Item=item)
    if cost is not None:
        boto3.resource("dynamodb").Table(f"{agent}_telemetry").put_item(Item={
            "session_id": sid, "sk": "1", "agent_id": "claims_intake", "kind": "llm",
            "cost_usd": decimal.Decimal(str(cost)), "latency_ms": latency})


def sessions(started):
    return [c["sessionId"] for c in started["cases"]]


@pytest.fixture()
def env(base_env, monkeypatch):  # noqa: F811 - overrides test_builds' env fixture on purpose
    """test_builds' fixture, with one module it leaves stale: bff/limits.py is not re-imported
    with the rest, so it would raise the refusal of a build module no handler uses any more."""
    monkeypatch.setattr(base_env.handler.limits, "builds", base_env.builds)
    return base_env


@pytest.fixture()
def built(env):
    """A deployed build with two suites."""
    assert put_project(env, [SUITE, OTHER])[0] == 200
    return env, deploy(env)


# --- the suites are part of the build ----------------------------------------------------------

def test_suites_are_saved_with_the_build_and_read_back(env):
    put_project(env, [SUITE, OTHER])
    status, got = call(env, f"GET {OWN}", params={"id": B})
    assert status == 200 and got["suites"] == [SUITE, OTHER] and got["trends"] == {}
    # They are in the project the Build view opens, beside the workflow and the prompts.
    assert call(env, "GET /api/builds/{id}", params={"id": B})[1]["project"]["suites"] == [SUITE, OTHER]
    # One suite by name, with its trend.
    status, one = call(env, f"GET {OWN}", params={"id": B}, qs={"name": "Smoke"})
    assert status == 200 and one == {"suite": OTHER, "trend": []}
    assert call(env, f"GET {OWN}", params={"id": B}, qs={"name": "nope"})[0] == 404


def test_a_build_without_suites_lists_none(env):
    put_project(env)
    assert call(env, f"GET {OWN}", params={"id": B})[1] == {"suites": [], "trends": {}}


def test_suites_are_frozen_into_the_version_that_deploys_and_an_old_build_has_none(env):
    put_project(env, [SUITE])
    call(env, "POST /api/builds/{id}/deploy", params={"id": B}, body={"tool": "cdk"})
    assert version_object(env, B, 1)["suites"] == [SUITE]
    # Edited afterwards: the deployed version keeps the suites it was frozen with.
    put_project(env, [OTHER])
    assert version_object(env, B, 1)["suites"] == [SUITE]
    # A build with no suites has a bundle exactly as it was before this feature: no key.
    put_project(env, None, bid="pnosuite01")
    call(env, "POST /api/builds/{id}/deploy", params={"id": "pnosuite01"}, body={"tool": "cdk"})
    assert "suites" not in version_object(env, "pnosuite01", 1)
    put_project(env, [], bid="pempty0001")
    call(env, "POST /api/builds/{id}/deploy", params={"id": "pempty0001"}, body={"tool": "cdk"})
    assert "suites" not in version_object(env, "pempty0001", 1)


def test_the_suites_put_route_replaces_them_and_leaves_the_rest_of_the_build_alone(env):
    put_project(env, [SUITE])
    before = call(env, "GET /api/builds/{id}", params={"id": B})[1]["project"]
    status, body = call(env, f"PUT {OWN}", params={"id": B}, body={"suites": [OTHER]})
    assert status == 200 and body["suites"] == [OTHER]
    after = call(env, "GET /api/builds/{id}", params={"id": B})[1]["project"]
    assert after["suites"] == [OTHER]
    assert {k: v for k, v in after.items() if k != "suites"} == {k: v for k, v in before.items() if k != "suites"}


def test_suites_with_errors_are_refused_with_the_issues_and_change_nothing(env):
    put_project(env, [SUITE])
    broken = {"name": "Bad", "cases": [{"name": "c", "request": "  ", "expect": [{"nonsense": 1}]}]}
    status, body = call(env, f"PUT {OWN}", params={"id": B}, body={"suites": [broken]})
    assert status == 400 and "2 errors" in body["error"]
    assert sorted(i["path"] for i in body["issues"]) == ["suites.0.cases.0.expect.0", "suites.0.cases.0.request"]
    assert {"severity", "where", "path", "message"} == set(body["issues"][0])
    assert call(env, f"GET {OWN}", params={"id": B})[1]["suites"] == [SUITE]
    assert call(env, f"PUT {OWN}", params={"id": B}, body={"suites": "x"})[0] == 400
    assert call(env, f"PUT {OWN}", params={"id": B}, body={})[0] == 400


def test_a_check_on_an_agent_the_build_lacks_is_a_warning_that_comes_back_with_the_suites(env):
    put_project(env)
    ghost = {"name": "G", "cases": [{"name": "c", "request": "r", "expect": [{"agent": "ghost", "contains": "x"}]}]}
    status, body = call(env, f"PUT {OWN}", params={"id": B}, body={"suites": [ghost]})
    assert status == 200 and [i["severity"] for i in body["issues"]] == ["warning"]


def test_deleting_a_build_deletes_its_suite_runs(built):
    env, _ = built
    for _ in range(2):
        start(env)
    table = env.table
    assert len(table.query(KeyConditionExpression="pk = :p AND begins_with(sk, :s)",
                           ExpressionAttributeValues={":p": f"BUILD#{B}", ":s": "SUITERUN#"})["Items"]) == 2
    env.builds.buildstore.record_destroyed(table, B)
    assert call(env, "DELETE /api/builds/{id}", params={"id": B})[0] == 200
    left = table.scan()["Items"]
    assert [i for i in left if i["pk"].startswith(f"BUILD#{B}")] == []


# --- whose they are --------------------------------------------------------------------------------

ROUTES = [("GET", OWN, None), ("PUT", OWN, {"suites": []}), ("POST", RUNS, {"suite": "Triage"}),
          ("GET", ONE, None)]


@pytest.mark.parametrize(("method", "template", "body"), ROUTES)
def test_another_users_build_is_a_404_on_every_suite_route(built, method, template, body):
    env, _ = built
    run = start(env)[1]["suiteRunId"]
    status, answer = call(env, f"{method} {template}", params={"id": B, "run": run}, body=body, sub="u2",
                          path=template.replace("{id}", B).replace("{run}", run))
    assert status == 404, answer
    # Nothing leaked: the same answer a build that does not exist gives.
    ghost = call(env, f"{method} {template}", params={"id": "pghost00001", "run": run}, body=body, sub="u2",
                 path=template.replace("{id}", "pghost00001").replace("{run}", run))
    assert ghost == (404, answer)


def test_another_users_attempt_to_put_suites_changes_nothing(built):
    env, _ = built
    call(env, f"PUT {OWN}", params={"id": B}, body={"suites": [OTHER]}, sub="u2")
    assert call(env, f"GET {OWN}", params={"id": B})[1]["suites"] == [SUITE, OTHER]


def test_a_build_shared_with_someone_gives_them_the_suites_and_the_trend_but_not_the_owners_results(built):
    env, _ = built
    call(env, "PUT /api/builds/{id}/shares", params={"id": B}, body={"emails": ["u2@example.com"]})
    mine = start(env)[1]
    assert call(env, f"GET {OWN}", params={"id": B}, sub="u2")[1]["suites"] == [SUITE, OTHER]
    trend = call(env, f"GET {OWN}", params={"id": B}, sub="u2")[1]["trends"]["Triage"]
    assert [t["suiteRunId"] for t in trend] == [mine["suiteRunId"]]
    # Their own runs are theirs; the owner's are not theirs to read.
    assert result(env, mine["suiteRunId"], sub="u2")[0] == 404
    theirs = start(env, sub="u2")
    assert theirs[0] == 202
    assert result(env, theirs[1]["suiteRunId"], sub="u2")[0] == 200
    assert result(env, theirs[1]["suiteRunId"], sub="u1")[0] == 404


def test_a_suite_run_is_found_only_under_its_own_build(built):
    env, _ = built
    put_project(env, [SUITE], bid="pother00001")
    run = start(env)[1]["suiteRunId"]
    assert result(env, run, bid="pother00001")[0] == 404
    assert result(env, "not-a-run-id")[0] == 400
    assert result(env, "0" * 12 + "-20260101T000000000000-aaaaaa")[0] == 404


# --- starting ----------------------------------------------------------------------------------------

def test_a_suite_run_starts_one_run_per_case_through_the_normal_start_with_the_gates_automatic(built):
    env, agent = built
    status, started = start(env)
    assert status == 202
    assert [c["case"] for c in started["cases"]] == ["urgent", "routine"]
    assert started["started"] == 2 and started["notStarted"] == 0 and started["buildVersion"] == 1
    status_table = boto3.resource("dynamodb").Table(f"{agent}_status")
    rows = [status_table.get_item(Key={"session_id": s})["Item"] for s in sessions(started)]
    assert [r["topic"] for r in rows] == ["Claim 1: hospitalised", "Claim 2: a dent"]
    assert rows[0]["subject_id"] == "acme" and "subject_id" not in rows[1]
    assert all(r["owner"] == "u1" and r["overall"] == "running" for r in rows)
    assert all(r["build"] == {"id": B, "name": "Claims", "version": 1} for r in rows)
    # The runtime is told to approve the gates itself, and which build to run in.
    starts = [p for p in env.invoked if p.get("action") == "start"]
    assert [p["session_id"] for p in starts] == sessions(started)
    assert all(p["gates"] == "auto" and p["build"] == B for p in starts)
    # Each case's run is linked to the build, like a run started from the Start run box.
    assert env.builds.build_of_run(sessions(started)[0]) == B


def test_a_suite_run_is_not_started_without_the_start_permission(built, monkeypatch):
    env, _ = built
    monkeypatch.setitem(env.handler.authz._RULES, "start", ["testers"])
    status, body = start(env, groups=("operators",))
    assert status == 403 and body["error"] == "not authorized to perform 'start'"
    assert env.invoked == []
    assert start(env, groups=("testers",))[0] == 202


def test_a_build_that_is_not_deployed_cannot_run_a_suite_but_its_suites_can_be_written(env):
    put_project(env, [SUITE])
    status, body = start(env)
    assert status == 409 and "not deployed" in body["error"]
    assert call(env, f"PUT {OWN}", params={"id": B}, body={"suites": [SUITE, OTHER]})[0] == 200


def test_a_suite_that_does_not_exist_or_is_broken_or_empty_starts_nothing(built):
    env, _ = built
    assert start(env, "Nope")[0] == 404
    # Put through the draft save, which does not validate suites (a draft is allowed to be unfinished).
    put_project(env, [{"name": "Broken", "cases": [{"name": "c", "request": "", "expect": []}]},
                      {"name": "Empty", "cases": []}])
    status, body = start(env, "Broken")
    assert status == 400 and body["issues"][0]["path"] == "cases.0.request" and "is empty" in body["error"]
    status, body = start(env, "Empty")
    assert status == 400 and "no cases" in body["error"]
    assert [p for p in env.invoked if p.get("action") == "start"] == []


def test_a_control_plane_console_runs_nothing_and_says_so_but_still_holds_the_suites(built):
    env, _ = built
    env.handler.CONSOLE_MODE = "builder"
    try:
        status, body = start(env)
        assert status == 404 and "runs live in each build's own app" in body["error"]
        assert result(env, "0" * 12 + "-20260101T000000000000-aaaaaa")[0] == 404
        assert call(env, f"GET {OWN}", params={"id": B})[0] == 200
        assert call(env, f"PUT {OWN}", params={"id": B}, body={"suites": [SUITE]})[0] == 200
    finally:
        env.handler.CONSOLE_MODE = "app"


def test_a_case_the_limits_refuse_is_not_started_and_not_a_failure(env):
    limits = {"orchestrator": {"limits": {"userRunsPerHour": 1}}}
    put_project(env, [{"name": "Three", "cases": [
        {"name": f"c{i}", "request": f"r{i}", "expect": []} for i in range(3)]}], **limits)
    agent = deploy(env)
    status, started = start(env, "Three")
    assert status == 202 and started["started"] == 1 and started["notStarted"] == 2
    assert started["cases"][0]["sessionId"] and started["cases"][0]["notStarted"] is None
    reason = started["cases"][1]["notStarted"]
    assert "You have started 1 runs this hour" in reason and started["cases"][2]["notStarted"] == reason
    # Only the one that started left a run.
    assert len([p for p in env.invoked if p.get("action") == "start"]) == 1
    finish(agent, started["cases"][0]["sessionId"], "done")
    status, got = result(env, started["suiteRunId"])
    assert status == 200 and got["state"] == "finished"
    assert got["summary"] == {"passed": 1, "failed": 0, "running": 0, "notStarted": 2, "total": 1}
    assert [c["status"] for c in got["cases"]] == ["passed", "not_started", "not_started"]
    assert got["cases"][1]["message"].startswith("Not started: You have started 1 runs this hour")
    assert got["trend"][-1]["passed"] == 1 and got["trend"][-1]["total"] == 1 and got["trend"][-1]["notStarted"] == 2


def test_once_the_limits_refuse_a_case_the_rest_are_not_asked_again(env, monkeypatch):
    """Spend only grows and runs are only counted up, so the rest would be refused too."""
    put_project(env, [{"name": "Three", "cases": [
        {"name": f"c{i}", "request": f"r{i}", "expect": []} for i in range(3)]}],
        orchestrator={"limits": {"userRunsPerHour": 1}})
    deploy(env)
    asked = []
    real = env.handler._begin_run
    monkeypatch.setattr(env.handler, "_begin_run", lambda *a, **k: asked.append(a[2]) or real(*a, **k))
    start(env, "Three")
    assert asked == ["r0", "r1"]


def test_someone_a_build_is_no_longer_shared_with_cannot_read_the_suite_runs_they_started(built):
    env, agent = built
    call(env, "PUT /api/builds/{id}/shares", params={"id": B}, body={"emails": ["u2@example.com"]})
    started = start(env, "Smoke", sub="u2")[1]
    theirs = started["suiteRunId"]
    # Ended and read once, so the stored outcome could answer without touching the runs.
    finish(agent, sessions(started)[0], "done")
    assert result(env, theirs, sub="u2")[1]["state"] == "finished"
    call(env, "PUT /api/builds/{id}/shares", params={"id": B}, body={"emails": []})
    assert result(env, theirs, sub="u2")[0] == 404
    assert call(env, f"GET {OWN}", params={"id": B}, sub="u2")[0] == 404


def test_a_run_that_could_not_be_created_is_not_started_either(built, monkeypatch):
    env, _ = built
    monkeypatch.setattr(env.handler, "_begin_run", lambda *a, **k: None)
    status, started = start(env)
    assert status == 202 and started["started"] == 0
    assert {c["notStarted"] for c in started["cases"]} == {"the run could not be created"}


# --- the result --------------------------------------------------------------------------------------

def test_a_result_follows_the_runs_while_they_go_and_judges_each_case_when_it_ends(built):
    env, agent = built
    _, started = start(env)
    urgent, routine = sessions(started)
    status, got = result(env, started["suiteRunId"])
    assert status == 200 and got["state"] == "running" and got["summary"]["running"] == 2
    assert [c["status"] for c in got["cases"]] == ["running", "running"]
    finish(agent, urgent, "done", {"claims_intake": {"riskScore": 91}}, cost=0.02)
    got = result(env, started["suiteRunId"])[1]
    assert [c["status"] for c in got["cases"]] == ["passed", "running"] and got["state"] == "running"
    assert got["trend"] == [{"suiteRunId": started["suiteRunId"], "startedAt": started["startedAt"],
                             "buildVersion": 1, "finished": False}]
    finish(agent, routine, "done", {"claims_intake": {"riskScore": 55}}, cost=0.02)
    got = result(env, started["suiteRunId"])[1]
    assert got["state"] == "finished" and got["suite"] == "Triage" and got["buildVersion"] == 1
    assert got["summary"] == {"passed": 1, "failed": 1, "running": 0, "notStarted": 0, "total": 2}
    passed, failed = got["cases"]
    assert passed["status"] == "passed" and passed["sessionId"] == urgent
    assert passed["message"] == "All 3 checks passed."
    assert failed["status"] == "failed" and failed["sessionId"] == routine
    assert failed["checks"] == {"passed": 1, "total": 2}
    assert failed["failed"] == [{
        "expected": "agent claims_intake riskScore lt 30", "actual": "55",
        "message": "Agent claims_intake, field riskScore was 55, expected lt 30."}]
    assert failed["message"] == "1 of 2 checks failed: Agent claims_intake, field riskScore was 55, expected lt 30."
    assert got["trend"][-1] == {"suiteRunId": started["suiteRunId"], "startedAt": started["startedAt"],
                                "buildVersion": 1, "finished": True, "passed": 1, "failed": 1, "total": 2,
                                "notStarted": 0}


@pytest.mark.parametrize("ended", ["failed", "cancelled", "denied"])
def test_a_run_that_did_not_finish_done_is_a_failed_case_even_if_its_outputs_look_right(built, ended):
    env, agent = built
    _, started = start(env, "Smoke")
    finish(agent, sessions(started)[0], ended)
    got = result(env, started["suiteRunId"])[1]
    assert got["summary"]["failed"] == 1 and got["cases"][0]["status"] == "failed"
    assert got["cases"][0]["failed"][0]["implicit"] is True
    assert f"ended {ended}" in got["cases"][0]["message"]


def test_the_cost_of_a_run_is_read_from_its_telemetry(built):
    env, agent = built
    _, started = start(env)
    urgent, routine = sessions(started)
    finish(agent, urgent, "done", {"claims_intake": {"riskScore": 91}}, cost=0.75)
    finish(agent, routine, "done", {"claims_intake": {"riskScore": 5}}, cost=0.01)
    got = result(env, started["suiteRunId"])[1]
    assert [c["status"] for c in got["cases"]] == ["failed", "passed"]
    assert got["cases"][0]["failed"] == [{"expected": "cost lt $0.5000", "actual": "$0.7500",
                                          "message": "The run cost $0.7500, expected lt $0.5000."}]


def test_a_run_with_no_telemetry_fails_its_cost_check_instead_of_costing_nothing(built):
    env, agent = built
    _, started = start(env)
    finish(agent, sessions(started)[0], "done", {"claims_intake": {"riskScore": 91}})   # no telemetry row
    got = result(env, started["suiteRunId"])[1]
    assert got["cases"][0]["status"] == "failed"
    assert got["cases"][0]["failed"] == [{
        "expected": "cost lt $0.5000", "actual": "(not recorded)",
        "message": "No cost was recorded for this run, so the limit cannot be checked."}]


def test_a_run_that_was_deleted_is_a_failed_case(built):
    env, agent = built
    _, started = start(env, "Smoke")
    boto3.resource("dynamodb").Table(f"{agent}_status").delete_item(Key={"session_id": sessions(started)[0]})
    got = result(env, started["suiteRunId"])[1]
    assert got["cases"][0]["status"] == "failed" and "no longer exists" in got["cases"][0]["message"]
    assert got["state"] == "finished"


def test_a_run_that_never_ends_fails_its_case_once_the_suite_run_gives_up(built, monkeypatch):
    env, _ = built
    _, started = start(env, "Smoke")
    assert result(env, started["suiteRunId"])[1]["state"] == "running"
    monkeypatch.setattr(env.handler.suiteruns, "SETTLE_AFTER_SECONDS", -1)
    got = result(env, started["suiteRunId"])[1]
    assert got["state"] == "finished" and got["cases"][0]["status"] == "failed"
    assert "gave up waiting" in got["cases"][0]["message"]


def test_a_finished_result_is_final_whatever_happens_to_the_runs_or_the_build_afterwards(built):
    env, agent = built
    _, started = start(env, "Smoke")
    sid = sessions(started)[0]
    finish(agent, sid, "done")
    first = result(env, started["suiteRunId"])[1]
    assert first["cases"][0]["status"] == "passed"
    # The run is deleted, and the build is destroyed: the answer does not change, and needs neither.
    boto3.resource("dynamodb").Table(f"{agent}_status").delete_item(Key={"session_id": sid})
    env.builds.buildstore.record_destroyed(env.table, B)
    assert result(env, started["suiteRunId"])[1] == first


def test_an_open_suite_run_of_a_build_that_is_no_longer_deployed_says_so(built):
    env, _ = built
    _, started = start(env, "Smoke")
    env.builds.buildstore.record_destroyed(env.table, B)
    status, body = result(env, started["suiteRunId"])
    assert status == 409 and "not deployed" in body["error"]


def test_two_suite_runs_of_the_same_suite_never_mix_their_cases(built):
    env, agent = built
    _, first = start(env)
    _, second = start(env)
    a1, a2 = sessions(first)
    b1, b2 = sessions(second)
    assert len({a1, a2, b1, b2}) == 4
    finish(agent, a1, "done", {"claims_intake": {"riskScore": 91}}, cost=0.01)
    finish(agent, a2, "done", {"claims_intake": {"riskScore": 10}}, cost=0.01)     # run one: both pass
    finish(agent, b1, "done", {"claims_intake": {"riskScore": 10}}, cost=0.01)     # run two: both fail
    finish(agent, b2, "done", {"claims_intake": {"riskScore": 99}}, cost=0.01)
    r1, r2 = result(env, first["suiteRunId"])[1], result(env, second["suiteRunId"])[1]
    assert [c["status"] for c in r1["cases"]] == ["passed", "passed"]
    assert [c["sessionId"] for c in r1["cases"]] == [a1, a2]
    assert [c["status"] for c in r2["cases"]] == ["failed", "failed"]
    assert [c["sessionId"] for c in r2["cases"]] == [b1, b2]
    assert [t["suiteRunId"] for t in r2["trend"]] == [first["suiteRunId"], second["suiteRunId"]]
    assert [(t["passed"], t["total"]) for t in r2["trend"]] == [(2, 2), (0, 2)]


def test_the_trend_is_per_suite_oldest_first_and_carries_the_version(built):
    env, _ = built
    _, a = start(env, "Smoke")
    _, b = start(env, "Triage")
    _, c = start(env, "Smoke")
    trends = call(env, f"GET {OWN}", params={"id": B})[1]["trends"]
    assert [t["suiteRunId"] for t in trends["Smoke"]] == [a["suiteRunId"], c["suiteRunId"]]
    assert [t["suiteRunId"] for t in trends["Triage"]] == [b["suiteRunId"]]
    assert {t["buildVersion"] for t in trends["Smoke"]} == {1}


def test_only_the_newest_runs_of_a_suite_are_kept(built, monkeypatch):
    env, _ = built
    monkeypatch.setattr(env.handler.suiteruns, "TREND_KEEP", 3)
    ids = [start(env, "Smoke")[1]["suiteRunId"] for _ in range(5)]
    other = start(env, "Triage")[1]["suiteRunId"]
    left = env.table.query(KeyConditionExpression="pk = :p AND begins_with(sk, :s)",
                           ExpressionAttributeValues={":p": f"BUILD#{B}", ":s": "SUITERUN#"})["Items"]
    kept = sorted(i["runId"] for i in left)
    assert [i for i in kept if i in ids] == sorted(ids[-3:]) and other in kept
    trend = call(env, f"GET {OWN}", params={"id": B})[1]["trends"]["Smoke"]
    assert [t["suiteRunId"] for t in trend] == ids[-3:]
    # An older run is gone for good.
    assert result(env, ids[0])[0] == 404


def test_the_trend_never_holds_more_than_the_cap_even_if_more_are_stored(built, monkeypatch):
    env, _ = built
    ids = [start(env, "Smoke")[1]["suiteRunId"] for _ in range(4)]
    monkeypatch.setattr(env.handler.suiteruns, "TREND_KEEP", 2)
    trend = call(env, f"GET {OWN}", params={"id": B})[1]["trends"]["Smoke"]
    assert [t["suiteRunId"] for t in trend] == ids[-2:]


def test_two_readers_finishing_the_same_suite_run_leave_one_outcome(built):
    env, agent = built
    _, started = start(env, "Smoke")
    finish(agent, sessions(started)[0], "done")
    suiteruns = env.handler.suiteruns

    def reader():
        return env.handler._suite_reader(env.handler._build_target(B, "u1"))

    one = suiteruns.result(B, env.builds.Who("u1"), started["suiteRunId"], reader)
    two = suiteruns.result(B, env.builds.Who("u1"), started["suiteRunId"], reader)
    assert one == two and one["state"] == "finished"


def test_the_runs_say_which_suite_run_started_them_in_the_audit_log(built):
    env, _ = built
    _, started = start(env, "Smoke")
    mine = env.builds.activity("u1")
    ev = next(e for e in mine if e["action"] == "run.started")
    assert ev["detail"]["suite"] == "Smoke" and ev["detail"]["suiteRun"] == started["suiteRunId"]
    assert ev["detail"]["case"] == "runs" and ev["detail"]["build"] == B


def test_the_json_the_routes_return_has_no_decimals_left_in_it(built):
    env, agent = built
    _, started = start(env, "Smoke")
    finish(agent, sessions(started)[0], "done", cost=0.5)
    status, got = result(env, started["suiteRunId"])
    assert status == 200 and json.dumps(got) and CTX.function_name == "bff-fn"


def test_every_route_the_handler_sends_to_suites_is_in_both_route_lists():
    """A route only the handler knows is a 404 on a deployed API. (cdk/test/parity.test.ts holds the two
    lists to each other; this holds them to the handler.)"""
    from importlib import import_module
    routes = import_module("suiteruns").ROUTES
    tf = (ORCH_ROOT / "terraform" / "bff.tf").read_text()
    cdk = (ORCH_ROOT / "cdk" / "lib" / "orchestrator-stack.ts").read_text()
    in_cdk = {f"{m} {p}" for p, methods in re.findall(r'\{\s*path:\s*"(/api/[^"]*)",\s*methods:\s*\[([^\]]*)\]', cdk)
              for m in re.findall(r"HttpMethod\.(\w+)", methods)}
    for route in routes:
        assert f'"{route}"' in tf, f"{route} is not in terraform/bff.tf"
        assert route in in_cdk, f"{route} is not in cdk/lib/orchestrator-stack.ts"
