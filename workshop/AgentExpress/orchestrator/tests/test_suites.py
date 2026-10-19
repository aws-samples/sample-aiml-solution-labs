"""The assertion engine for a build's test suites (bff/suites.py): a pure function, no AWS.

A suite is only worth having if a green result means the build is fine, so these tests are
mostly about the ways a check could pass when it should not: a boundary off by one, a failed
run counted as passed, a missing agent or field read as a match, a check this version does not
know being skipped. Each is a case here, so a change that opens one fails a test by name.
"""
from __future__ import annotations

import ast
import json
import re
import sys

import pytest
from conftest import ORCH_ROOT

sys.path.insert(0, str(ORCH_ROOT / "bff"))
import suites


def snap(overall="done", **agents):
    """A run snapshot, as the status table holds it: agent -> (status, output)."""
    nodes = {}
    for aid, spec in agents.items():
        status, output = spec if isinstance(spec, tuple) else ("done", spec)
        nodes[aid] = {"status": status, "pct": 100, "output": output if isinstance(output, str)
                      else json.dumps(output)}
    return {"session_id": "abc123", "overall": overall, "nodes": nodes, "result": None}


def one(check, snapshot=None, totals=None):
    return suites.evaluate_check(check, snapshot if snapshot is not None else snap(), totals)


TRIAGE = snap(triage={"disposition": "Escalate ", "riskScore": 80, "notes": "URGENT: call back",
                      "openItems": ["a", "b"], "empty": [], "scope": {"inScope": True},
                      "findings": [{"claim": "water"}]},
              summary=("done", "Draft summary. TODO: add dates"),
              review=("skipped", ""))


# --- the copied comparisons are the originals ---------------------------------------------------

def _functions(path, names):
    tree = ast.parse(path.read_text())
    found = {}
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in names:
            body = node.body
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) \
                    and isinstance(body[0].value.value, str):
                node.body = body[1:] or [ast.Pass()]
            found[node.name] = ast.dump(node, indent=1)
    return found


def _assignments(path, names):
    return {t.id: ast.dump(n.value) for n in ast.parse(path.read_text()).body if isinstance(n, ast.Assign)
            for t in n.targets if isinstance(t, ast.Name) and t.id in names}


def test_the_comparisons_are_the_branch_rules_comparisons():
    """The BFF cannot import app.*, so suites.py holds copies. They are held to the originals
    as syntax trees (docstrings aside): a change to a branch operator that is not copied here
    would make a suite judge a value differently from the workflow that branched on it."""
    names = {"_dig", "_norm", "_magnitude", "_is_present", "_contains", "_holds"}
    original = _functions(ORCH_ROOT / "app" / "common" / "branching.py", names)
    copy = _functions(ORCH_ROOT / "bff" / "suites.py", names)
    assert set(original) == names == set(copy)
    for name in sorted(names):
        assert copy[name] == original[name], f"bff/suites.py {name} differs from app/common/branching.py"
    consts = {"_OPS", "_TEXT_OPS", "_NUM_OPS", "_MISSING"}
    assert _assignments(ORCH_ROOT / "bff" / "suites.py", consts) \
        == _assignments(ORCH_ROOT / "app" / "common" / "branching.py", consts)


def test_the_json_salvage_is_the_one_every_agent_output_goes_through():
    names = {"extract_json", "_complete_objects", "_balance_close", "_repair_json"}
    original = _functions(ORCH_ROOT / "app" / "common" / "assets.py", names)
    copy = _functions(ORCH_ROOT / "bff" / "suites.py", names)
    assert set(original) == names == set(copy)
    for name in sorted(names):
        assert copy[name] == original[name], f"bff/suites.py {name} differs from app/common/assets.py"


def test_a_request_may_be_as_long_as_the_start_run_box_takes():
    handler = (ORCH_ROOT / "bff" / "handler.py").read_text()
    assert int(re.search(r"^TOPIC_MAX = (\d+)", handler, re.MULTILINE).group(1)) == suites.MAX_REQUEST


def test_the_run_outcomes_are_the_statuses_the_runtime_writes():
    runtime = (ORCH_ROOT / "app" / "orchestrator" / "runtime.py").read_text()
    assert '"denied"' in runtime and '"done"' in runtime
    assert set(suites.TERMINAL) == {"done", "failed", "denied", "cancelled"}


# --- the run ---------------------------------------------------------------------------------

def test_a_run_that_ended_as_expected_passes():
    r = one({"run": "done"})
    assert r["passed"] and r["actual"] == "done" and r["expected"] == "run done"
    assert r["message"] == "The run ended done."


@pytest.mark.parametrize("overall", ["failed", "cancelled", "denied"])
def test_a_run_that_did_not_finish_done_fails_a_done_check(overall):
    r = one({"run": "done"}, snap(overall))
    assert not r["passed"] and r["actual"] == overall
    assert f"ended {overall}" in r["message"] and "not done" in r["message"]


def test_a_failed_run_names_the_agents_that_failed():
    r = one({"run": "done"}, snap("failed", triage=("done", "{}"), review=("failed", "")))
    assert not r["passed"] and "failed at: review" in r["message"]


def test_a_case_may_expect_a_run_to_end_any_way_it_can():
    for ended in suites.RUN_OUTCOMES:
        assert one({"run": ended}, snap(ended))["passed"]
    assert not one({"run": "denied"}, snap("done"))["passed"]


def test_a_run_check_for_an_outcome_a_run_cannot_have_fails_instead_of_passing():
    r = one({"run": "running"}, snap("running"))
    assert not r["passed"] and "not a way a run ends" in r["message"]


# --- an agent's status -------------------------------------------------------------------------

def test_an_agents_status():
    assert one({"agent": "triage", "status": "done"}, TRIAGE)["passed"]
    assert one({"agent": "review", "status": "skipped"}, TRIAGE)["passed"]
    r = one({"agent": "review", "status": "done"}, TRIAGE)
    assert not r["passed"] and r["actual"] == "skipped"
    assert "ended skipped, not done" in r["message"]


def test_an_agent_that_is_not_in_the_run_fails_every_kind_of_check_and_lists_the_ones_that_are():
    for check in ({"agent": "ghost", "status": "done"}, {"agent": "ghost", "contains": "x"},
                  {"agent": "ghost", "field": "a", "exists": False}, {"agent": "ghost", "notContains": "x"}):
        r = one(check, TRIAGE)
        assert not r["passed"], check
        assert "'ghost' is not in this run's workflow" in r["message"] and "triage" in r["message"]


# --- a field of an agent's JSON output: every operator, both ways ---------------------------------

OPERATOR_CASES = [
    # (check, passes)
    ({"field": "disposition", "equals": "escalate"}, True),        # case and space are ignored
    ({"field": "disposition", "equals": "route"}, False),
    ({"field": "disposition", "notEquals": "route"}, True),
    ({"field": "disposition", "notEquals": "ESCALATE"}, False),
    ({"field": "disposition", "in": ["route", "escalate"]}, True),
    ({"field": "disposition", "in": ["route", "close"]}, False),
    ({"field": "notes", "contains": "urgent"}, True),
    ({"field": "notes", "contains": "calm"}, False),
    ({"field": "notes", "notContains": "calm"}, True),
    ({"field": "notes", "notContains": "URGENT"}, False),
    ({"field": "openItems", "contains": "a"}, True),               # membership of a list
    ({"field": "scope", "contains": "inScope"}, True),             # a key of an object
    ({"field": "riskScore", "exists": True}, True),
    ({"field": "riskScore", "exists": False}, False),
    ({"field": "empty", "exists": True}, False),                   # present but empty is not "there"
    ({"field": "empty", "exists": False}, True),
    ({"field": "nothing", "exists": False}, True),
    ({"field": "nothing", "exists": True}, False),
    ({"field": "scope.inScope", "equals": True}, True),            # a dot path, a boolean
    ({"field": "findings.0.claim", "equals": "water"}, True),      # a number indexes a list
    ({"field": "findings.-1.claim", "equals": "water"}, True),
    ({"field": "findings.3.claim", "exists": False}, True),
]


@pytest.mark.parametrize(("check", "passes"), OPERATOR_CASES)
def test_each_operator_on_a_field(check, passes):
    r = one({"agent": "triage", **check}, TRIAGE)
    assert r["passed"] is passes, r["message"]


# riskScore is 80: the boundary of each numeric operator, both sides.
@pytest.mark.parametrize(("op", "limit", "passes"), [
    ("gt", 79, True), ("gt", 80, False), ("gt", 81, False),
    ("gte", 79, True), ("gte", 80, True), ("gte", 81, False),
    ("lt", 79, False), ("lt", 80, False), ("lt", 81, True),
    ("lte", 79, False), ("lte", 80, True), ("lte", 81, True),
])
def test_each_numeric_operator_at_its_boundary(op, limit, passes):
    r = one({"agent": "triage", "field": "riskScore", op: limit}, TRIAGE)
    assert r["passed"] is passes, r["message"]


def test_a_list_compares_by_its_length_and_a_numeric_string_by_its_value():
    assert one({"agent": "triage", "field": "openItems", "gt": 1}, TRIAGE)["passed"]
    assert not one({"agent": "triage", "field": "openItems", "gt": 2}, TRIAGE)["passed"]
    s = snap(a={"score": "85"})
    assert one({"agent": "a", "field": "score", "gte": 85}, s)["passed"]


def test_several_comparisons_in_one_check_must_all_hold():
    both = {"agent": "triage", "field": "riskScore", "gte": 80, "lt": 90}
    assert one(both, TRIAGE)["passed"]
    r = one({**both, "lt": 80}, TRIAGE)
    assert not r["passed"] and "lt 80" in r["message"] and "gte" not in r["message"].split("expected")[1]


def test_a_failure_says_what_was_found_and_what_was_expected():
    r = one({"agent": "triage", "field": "riskScore", "gte": 90}, TRIAGE)
    assert r["actual"] == "80" and r["expected"] == "agent triage riskScore gte 90"
    assert r["message"] == "Agent triage, field riskScore was 80, expected gte 90."
    r = one({"agent": "triage", "field": "disposition", "equals": "close"}, TRIAGE)
    assert r["actual"] == '"Escalate "' and "expected equals \"close\"" in r["message"]


def test_a_missing_field_fails_every_comparison_but_exists_false():
    for op, want in (("equals", ""), ("notEquals", "x"), ("in", ["x"]), ("contains", ""), ("gt", -1),
                     ("lt", 99), ("notContains", "x"), ("exists", True)):
        r = one({"agent": "triage", "field": "ghost.field", op: want}, TRIAGE)
        assert not r["passed"], op
        assert r["actual"] == "(missing)" and "is not in the output" in r["message"]


def test_a_field_of_output_that_is_not_json_says_so():
    s = snap(a=("done", "Just a sentence, no braces."))
    r = one({"agent": "a", "field": "x", "equals": "1"}, s)
    assert not r["passed"] and "did not return JSON" in r["message"]
    r = one({"agent": "a", "field": "x", "equals": "1"}, snap(a=("pending", "")))
    assert not r["passed"] and "has no output (status pending)" in r["message"]


def test_json_in_a_fence_with_prose_around_it_or_cut_off_is_still_read():
    fenced = snap(a=("done", 'Here you go:\n```json\n{"risk": 7}\n```'))
    assert one({"agent": "a", "field": "risk", "equals": 7}, fenced)["passed"]
    cut = snap(a=("done", '{"risk": 7, "notes": "the answer was cut o'))
    assert one({"agent": "a", "field": "risk", "equals": 7}, cut)["passed"]


# --- an agent's output as text --------------------------------------------------------------------

def test_text_checks_read_the_agents_raw_output():
    assert one({"agent": "summary", "contains": "draft"}, TRIAGE)["passed"]
    assert not one({"agent": "summary", "contains": "final"}, TRIAGE)["passed"]
    assert one({"agent": "summary", "notContains": "final"}, TRIAGE)["passed"]
    r = one({"agent": "summary", "notContains": "TODO"}, TRIAGE)
    assert not r["passed"] and 'expected notContains "TODO"' in r["message"]
    assert one({"agent": "summary", "equals": "draft summary. todo: add dates"}, TRIAGE)["passed"]


def test_a_text_check_of_an_agent_that_produced_nothing_says_so():
    r = one({"agent": "review", "contains": "ok"}, TRIAGE)
    assert not r["passed"] and r["actual"] == "(no output)" and "(status skipped)" in r["message"]
    # Nothing contains nothing: "does not contain" is true of an empty output.
    assert one({"agent": "review", "notContains": "error"}, TRIAGE)["passed"]


# --- cost and time --------------------------------------------------------------------------------

def test_cost_is_compared_to_the_limit_at_its_boundary():
    totals = {"costUsd": 0.05, "latencyMs": 9000}
    assert one({"cost": {"lt": 0.06}}, totals=totals)["passed"]
    assert not one({"cost": {"lt": 0.05}}, totals=totals)["passed"]
    assert one({"cost": {"lte": 0.05}}, totals=totals)["passed"]
    assert one({"cost": {"gt": 0.01, "lt": 1}}, totals=totals)["passed"]
    r = one({"cost": {"lt": 0.04}}, totals=totals)
    assert r["message"] == "The run cost $0.0500, expected lt $0.0400." and r["actual"] == "$0.0500"


def test_latency_is_the_sum_of_the_calls_and_is_compared_at_its_boundary():
    totals = {"costUsd": 0, "latencyMs": 9000}
    assert not one({"latencyMs": {"lt": 9000}}, totals=totals)["passed"]
    assert one({"latencyMs": {"lte": 9000}}, totals=totals)["passed"]
    r = one({"latencyMs": {"lt": 5000}}, totals=totals)
    assert not r["passed"] and "9,000 ms" in r["message"] and "model and tool calls" in r["message"]


def test_a_cost_that_was_never_recorded_fails_instead_of_passing_as_zero():
    for totals in (None, {}, {"costUsd": None}):
        r = one({"cost": {"lt": 100}}, totals=totals)
        assert not r["passed"] and r["actual"] == "(not recorded)"
    assert one({"cost": {"lt": 1}}, totals={"costUsd": 0})["passed"]


def test_cost_arrives_as_a_decimal_a_string_or_a_float():
    import decimal
    for v in (decimal.Decimal("0.05"), "0.05", 0.05):
        assert one({"cost": {"lte": 0.05}}, totals={"costUsd": v})["passed"]


# --- a check this version cannot read never passes ------------------------------------------------

@pytest.mark.parametrize("check", [
    {"nonsense": 1}, {"agent": "triage"}, {"agent": "triage", "field": "riskScore"},
    {"agent": "triage", "field": "riskScore", "gte": 1, "frobnicate": 2},
    {"run": "done", "cost": {"lt": 1}}, {}, "run done", None, ["run"], {"cost": {"approx": 1}},
    {"cost": 5}, {"cost": {}},
])
def test_an_unknown_or_malformed_check_fails_and_says_it_was_not_run(check):
    r = one(check, TRIAGE, {"costUsd": 0, "latencyMs": 0})
    assert r["passed"] is False
    assert set(r) == {"passed", "expected", "actual", "message"} and r["message"]


def test_a_check_that_raises_fails_and_does_not_hide_the_others():
    class Bad:
        def __str__(self):
            raise RuntimeError("boom")
    case = {"expect": [{"agent": "triage", "field": "riskScore", "equals": Bad()},
                       {"agent": "triage", "field": "riskScore", "gte": 80}]}
    graded = suites.grade(case, TRIAGE)
    assert [c["passed"] for c in graded["checks"]] == [True, False, True]
    assert "could not be evaluated" in graded["checks"][1]["message"]


# --- a case ----------------------------------------------------------------------------------------

def test_a_case_passes_when_every_check_does_and_the_run_finished_done():
    case = {"name": "n", "request": "r", "expect": [
        {"agent": "triage", "field": "disposition", "equals": "escalate"},
        {"agent": "summary", "notContains": "final"}]}
    g = suites.grade(case, TRIAGE, {"costUsd": 0.01, "latencyMs": 1})
    assert g["status"] == "passed" and g["message"] == "All 3 checks passed."
    assert g["checks"][0]["implicit"] is True and "implicit" not in g["checks"][1]


def test_one_failed_check_fails_the_case_and_the_message_names_it():
    case = {"expect": [{"agent": "triage", "field": "riskScore", "gte": 99}, {"agent": "summary", "contains": "draft"}]}
    g = suites.grade(case, TRIAGE)
    assert g["status"] == "failed"
    assert g["message"].startswith("1 of 3 checks failed: Agent triage, field riskScore was 80")
    assert [c["passed"] for c in g["checks"]] == [True, False, True]


@pytest.mark.parametrize("overall", ["failed", "cancelled", "denied"])
def test_a_case_that_only_looks_at_an_agents_output_still_fails_when_the_run_did_not_finish(overall):
    """The agent's output is fine, the run is not: without the implicit `run: done` this passed."""
    case = {"expect": [{"agent": "triage", "field": "riskScore", "gte": 80}]}
    s = {**TRIAGE, "overall": overall}
    g = suites.grade(case, s)
    assert g["status"] == "failed" and g["checks"][0]["implicit"] is True
    assert not g["checks"][0]["passed"] and g["checks"][1]["passed"]


def test_a_case_with_no_checks_passes_when_the_run_finishes():
    assert suites.grade({"expect": []}, snap("done"))["status"] == "passed"
    assert suites.grade({}, snap("done"))["status"] == "passed"
    assert suites.grade({"expect": []}, snap("failed"))["status"] == "failed"


def test_a_case_may_expect_the_run_to_be_refused_or_to_fail():
    case = {"expect": [{"run": "denied"}]}
    g = suites.grade(case, snap("denied"))
    assert g["status"] == "passed" and len(g["checks"]) == 1 and "implicit" not in g["checks"][0]
    assert suites.grade({"expect": [{"run": "failed"}]}, snap("done"))["status"] == "failed"


def test_a_cancelled_run_is_finished_and_judged_not_left_running():
    g = suites.grade({"expect": []}, snap("cancelled"))
    assert g["status"] == "failed" and "ended cancelled" in g["checks"][0]["message"]


@pytest.mark.parametrize("overall", ["running", "waiting_human", "", None])
def test_a_run_that_has_not_ended_is_not_judged(overall):
    g = suites.grade({"expect": [{"agent": "triage", "contains": "x"}]}, {"overall": overall, "nodes": {}})
    assert g["status"] == "running" and g["checks"] == []


def test_a_run_that_never_ends_fails_once_the_suite_run_gives_up_on_it():
    g = suites.grade({"expect": []}, snap("running"), expired=True)
    assert g["status"] == "failed" and "gave up waiting" in g["message"]
    assert suites.grade({"expect": []}, snap("done"), expired=True)["status"] == "passed"


def test_a_deleted_run_is_a_failed_case_with_the_reason():
    g = suites.grade({"expect": []}, None)
    assert g["status"] == "failed" and "no longer exists" in g["message"]


def test_summary_counts_cases_and_keeps_the_ones_that_never_started_apart():
    results = [{"status": "passed"}, {"status": "passed"}, {"status": "failed"},
               {"status": "running"}, {"status": "not_started"}, {"status": "not_started"}]
    assert suites.summarize(results) == {"passed": 2, "failed": 1, "running": 1, "notStarted": 2, "total": 4}
    assert suites.summarize([]) == {"passed": 0, "failed": 0, "running": 0, "notStarted": 0, "total": 0}


def test_keep_newest():
    assert suites.keep_newest([1, 2, 3, 4, 5], 3) == ([3, 4, 5], [1, 2])
    assert suites.keep_newest([1, 2], 3) == ([1, 2], [])
    assert suites.keep_newest([1, 2], 0) == ([], [1, 2])


# --- validating a suite ----------------------------------------------------------------------------

def good_case(**over):
    return {"name": "c1", "request": "Claim 1", "expect": [{"run": "done"}], **over}


def good_suite(**over):
    return {"name": "Triage", "cases": [good_case()], **over}


def paths(issues, severity="error"):
    return sorted(i["path"] for i in issues if i["severity"] == severity)


def test_a_good_suite_has_no_problems():
    assert suites.validate_suite(good_suite()) == []
    assert suites.validate_suites([good_suite(), good_suite(name="Other")]) == []
    assert suites.validate_suites(None) == [] and suites.validate_suites([]) == []


def test_every_check_kind_validates():
    expect = [{"run": "failed"}, {"agent": "a", "status": "skipped"},
              {"agent": "a", "field": "x.y", "gte": 1, "lt": 9}, {"agent": "a", "contains": "t"},
              {"agent": "a", "notContains": "t"}, {"agent": "a", "field": "x", "in": ["a", "b"]},
              {"agent": "a", "field": "x", "exists": False}, {"cost": {"lt": 1.5}}, {"latencyMs": {"lte": 100}}]
    assert suites.validate_suite(good_suite(cases=[good_case(expect=expect)])) == []


def test_issues_have_the_shape_the_other_validators_use():
    [issue] = suites.validate_suite(good_suite(cases=[good_case(request="  ")]), "suites.2.")
    assert set(issue) == {"severity", "where", "path", "message"}
    assert issue["severity"] == "error" and issue["where"] == {"kind": "suite", "id": "Triage"}
    assert issue["path"] == "suites.2.cases.0.request" and "is empty" in issue["message"]


def test_a_case_with_no_request_or_an_empty_one_is_an_error():
    for request in ("", "   \n", None, 5):
        s = good_suite(cases=[good_case(request=request)])
        assert paths(suites.validate_suite(s)) == ["cases.0.request"], request
    s = good_suite(cases=[{"name": "no request", "expect": []}])
    assert paths(suites.validate_suite(s)) == ["cases.0.request"]


def test_a_request_longer_than_the_start_run_box_takes_is_an_error():
    s = good_suite(cases=[good_case(request="x" * (suites.MAX_REQUEST + 1))])
    assert paths(suites.validate_suite(s)) == ["cases.0.request"]
    assert suites.validate_suite(good_suite(cases=[good_case(request="x" * suites.MAX_REQUEST)])) == []


def test_too_many_cases_or_checks_or_suites_are_errors_and_the_boundary_is_allowed():
    ok = good_suite(cases=[good_case(name=f"c{i}") for i in range(suites.MAX_CASES)])
    assert suites.validate_suite(ok) == []
    over = good_suite(cases=[good_case(name=f"c{i}") for i in range(suites.MAX_CASES + 1)])
    assert paths(suites.validate_suite(over)) == ["cases"]
    many = [{"run": "done"}] * suites.MAX_CHECKS
    assert suites.validate_suite(good_suite(cases=[good_case(expect=many)])) == []
    assert paths(suites.validate_suite(good_suite(cases=[good_case(expect=[*many, {"run": "done"}])]))) \
        == ["cases.0.expect"]
    lots = [good_suite(name=f"s{i}") for i in range(suites.MAX_SUITES + 1)]
    assert paths(suites.validate_suites(lots)) == ["suites"]
    assert suites.validate_suites(lots[:-1]) == []


def test_text_is_capped():
    big = "y" * (suites.MAX_TEXT + 1)
    checks = [{"agent": "a", "contains": big}, {"agent": "a", "field": big, "exists": True},
              {"agent": big, "contains": "x"}]
    s = good_suite(cases=[good_case(expect=checks)])
    assert paths(suites.validate_suite(s)) == ["cases.0.expect.0.contains", "cases.0.expect.1.field",
                                               "cases.0.expect.2.agent"]


def test_a_suite_over_the_size_cap_is_an_error():
    # Each part is inside its own cap; only the whole is too big.
    cases = [good_case(name=f"c{i}", expect=[{"agent": "a", "contains": "z" * suites.MAX_TEXT}] * 6)
             for i in range(suites.MAX_CASES)]
    [issue] = suites.validate_suite(good_suite(cases=cases))
    assert issue["path"] == "" and "at most 200 KB" in issue["message"]


def test_names_are_required_bounded_and_unique():
    assert paths(suites.validate_suite(good_suite(name=""))) == ["name"]
    assert paths(suites.validate_suite(good_suite(name="n" * (suites.MAX_NAME + 1)))) == ["name"]
    twin = good_suite(cases=[good_case(), good_case()])
    assert paths(suites.validate_suite(twin)) == ["cases.1.name"]
    assert paths(suites.validate_suite(good_suite(cases=[good_case(name=" ")]))) == ["cases.0.name"]
    assert paths(suites.validate_suites([good_suite(), good_suite()])) == ["suites.1.name"]


def test_a_suite_without_cases_is_a_warning_not_an_error():
    issues = suites.validate_suite(good_suite(cases=[]))
    assert paths(issues) == [] and paths(issues, "warning") == ["cases"]
    assert paths(suites.validate_suite(good_suite(cases="x"))) == ["cases"]


@pytest.mark.parametrize(("check", "path"), [
    ({"nonsense": 1}, "cases.0.expect.0"),
    ({"run": "done", "agent": "a", "status": "done"}, "cases.0.expect.0"),
    ({"run": "sleeping"}, "cases.0.expect.0.run"),
    ({"run": "running"}, "cases.0.expect.0.run"),
    ({"agent": "a", "status": "waiting"}, "cases.0.expect.0.status"),
    ({"agent": "a", "status": "done", "contains": "x"}, "cases.0.expect.0"),
    ({"agent": "a"}, "cases.0.expect.0"),
    ({"agent": "a", "field": "f"}, "cases.0.expect.0"),
    ({"agent": "a", "field": "f", "equals": 1, "approx": 2}, "cases.0.expect.0"),
    ({"agent": "", "contains": "x"}, "cases.0.expect.0.agent"),
    ({"agent": "a", "field": "", "exists": True}, "cases.0.expect.0.field"),
    ({"agent": "a", "field": "f", "in": "abc"}, "cases.0.expect.0.in"),
    ({"agent": "a", "field": "f", "exists": "yes"}, "cases.0.expect.0.exists"),
    ({"agent": "a", "field": "f", "gt": "many"}, "cases.0.expect.0.gt"),
    ({"agent": "a", "field": "f", "equals": ["a"]}, "cases.0.expect.0.equals"),
    ({"agent": "a", "notContains": {"k": 1}}, "cases.0.expect.0.notContains"),
    ({"cost": 5}, "cases.0.expect.0.cost"),
    ({"cost": {}}, "cases.0.expect.0.cost"),
    ({"cost": {"approx": 1}}, "cases.0.expect.0.cost.approx"),
    ({"cost": {"lt": -1}}, "cases.0.expect.0.cost.lt"),
    ({"cost": {"lt": "cheap"}}, "cases.0.expect.0.cost.lt"),
    ({"cost": {"lt": True}}, "cases.0.expect.0.cost.lt"),
    ({"cost": {"lt": 1}, "latencyMs": {"lt": 1}}, "cases.0.expect.0"),
    ({"latencyMs": {"equals": 5}}, "cases.0.expect.0.latencyMs.equals"),
    ("done", "cases.0.expect.0"),
])
def test_each_way_a_check_can_be_wrong_is_named(check, path):
    issues = suites.validate_suite(good_suite(cases=[good_case(expect=[check])]))
    assert path in paths(issues), issues


def test_unknown_keys_are_errors_so_a_typo_does_not_silently_drop_a_check():
    assert paths(suites.validate_suite(good_suite(extra=1))) == [""]
    assert paths(suites.validate_suite(good_suite(cases=[good_case(expects=[])]))) == ["cases.0"]


def test_a_check_on_an_agent_the_workflow_lacks_is_a_warning_when_the_workflow_is_known():
    s = good_suite(cases=[good_case(expect=[{"agent": "ghost", "contains": "x"}, {"agent": "real", "contains": "x"}])])
    assert suites.validate_suite(s) == []
    issues = suites.validate_suite(s, agents={"real"})
    assert paths(issues) == [] and paths(issues, "warning") == ["cases.0.expect.0.agent"]


def test_a_suite_that_is_not_an_object_is_one_error():
    for bad in ("x", 5, None, []):
        [issue] = suites.validate_suite(bad)
        assert issue["severity"] == "error"
    [issue] = suites.validate_suites("nope")
    assert issue["path"] == "suites"


def test_what_validates_evaluates_and_what_does_not_never_passes():
    """The two halves agree: every check the validator accepts is one the evaluator runs
    (so it can pass or fail on its merits), and every one it rejects the evaluator fails."""
    accepted = [{"run": "done"}, {"agent": "triage", "status": "done"},
                {"agent": "triage", "field": "riskScore", "gte": 1}, {"agent": "summary", "contains": "x"},
                {"agent": "summary", "notContains": "x"}, {"cost": {"lt": 9}}, {"latencyMs": {"lt": 9}}]
    for check in accepted:
        assert suites.validate_suite(good_suite(cases=[good_case(expect=[check])])) == [], check
        assert "could not" not in one(check, TRIAGE, {"costUsd": 0, "latencyMs": 0})["message"]
    rejected = [{"nonsense": 1}, {"agent": "triage"}, {"run": "sleeping"}, {"cost": {"approx": 1}}]
    for check in rejected:
        assert paths(suites.validate_suite(good_suite(cases=[good_case(expect=[check])])))
        assert one(check, TRIAGE, {"costUsd": 0, "latencyMs": 0})["passed"] is False
