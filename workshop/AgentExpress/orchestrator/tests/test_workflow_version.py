"""Each run records the deployed version of the workflow it ran on.

The Control Plane deploys a build's version N with the tag agentexpress:version=N; both IaC
paths pass it to the BFF as WORKFLOW_VERSION. The BFF shows it with the workflow ("Workflow
v3" in the app), stamps it on every run it starts, and returns it in the Runs list, so an
output can be traced back to the workflow that produced it.
"""
import importlib
import json
import sys
from pathlib import Path

import pytest
from test_bff_routes import CTX, WORKFLOW, FakeTable, call

MODS = ("workflow", "authz", "chatbot", "handler")


class RecordingTable(FakeTable):
    """Keeps what a run start writes, and lists it back."""

    def __init__(self):
        super().__init__()
        self.puts = []

    def put_item(self, **kw):
        self.puts.append(kw["Item"])
        return {}

    def scan(self, **kw):
        keep = [k.strip() for k in kw["ProjectionExpression"].replace("#o", "owner").replace(
            "#tr", "trigger").replace("#u", "user").split(",")]
        return {"Items": [{k: p[k] for k in keep if k in p} for p in self.puts]}


def load(monkeypatch, version):
    monkeypatch.setenv("WORKFLOW_JSON", json.dumps(WORKFLOW))
    monkeypatch.setenv("STATUS_TABLE", "t-status")
    monkeypatch.setenv("EVENTS_TABLE", "t-events")
    monkeypatch.setenv("RUNTIME_ARN", "arn:aws:bedrock-agentcore:us-east-1:1:runtime/x")
    if version is None:
        monkeypatch.delenv("WORKFLOW_VERSION", raising=False)
    else:
        monkeypatch.setenv("WORKFLOW_VERSION", version)
    for mod in MODS:
        sys.modules.pop(mod, None)
    # bff/ first: other tests put a tool's own handler.py on the path.
    bff = str(Path(__file__).resolve().parent.parent / "bff")
    if sys.path[0] != bff:
        sys.path.insert(0, bff)
    handler = importlib.import_module("handler")
    status = RecordingTable()
    monkeypatch.setattr(handler, "status_tbl", status)
    monkeypatch.setattr(handler, "events_tbl", FakeTable())
    monkeypatch.setattr(handler, "_self_invoke", lambda fn, payload: None)
    return handler, status


@pytest.fixture(autouse=True)
def _clean():
    yield
    for mod in MODS:
        sys.modules.pop(mod, None)


def test_a_run_records_the_version_and_the_runs_list_returns_it(monkeypatch):
    handler, status = load(monkeypatch, "3")
    sid = handler._begin_run(CTX, handler._default_target(), "a request", "user@example.com",
                             "user@example.com")
    assert sid and status.puts[0]["workflowVersion"] == 3
    code, runs = call(handler, "GET", "/api/sessions", groups=[])
    assert code == 200 and runs[0]["workflowVersion"] == 3


def test_the_page_is_told_which_version_is_deployed(monkeypatch):
    handler, _ = load(monkeypatch, "3")
    code, wf = call(handler, "GET", "/api/workflow", groups=[])
    assert code == 200 and wf["deployment"] == {"version": 3}


def test_an_app_deployed_outside_the_control_plane_records_none(monkeypatch):
    handler, status = load(monkeypatch, None)
    handler._begin_run(CTX, handler._default_target(), "a request", "user@example.com",
                       "user@example.com")
    assert "workflowVersion" not in status.puts[0]
    assert "deployment" not in call(handler, "GET", "/api/workflow", groups=[])[1]


def test_a_malformed_value_is_ignored(monkeypatch):
    handler, _ = load(monkeypatch, "v2; drop")
    assert handler.WORKFLOW_VERSION is None
