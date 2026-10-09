"""Starting a run with files (bff/runfiles.py): uploads and s3:// paths, checked when the
run starts and copied into the run's own folder, for agents that set `attachments`.

What has to hold:
  * the Start run form is offered files only when some agent reads them;
  * an upload is a presigned POST into the caller's own upload folder;
  * a run may bring only the caller's own uploads, and s3:// paths inside an allowed
    location, of a type and size a model reads, at most five;
  * each file is copied to runs/<session>/attachments/, and the run carries that list
    (on its status item and in the runtime payload);
  * a workflow whose agents read no files ignores s3:// text, as before, and refuses uploads.
"""
from __future__ import annotations

import importlib
import json
import sys
import types

import boto3
import pytest

moto = pytest.importorskip("moto")

ASSETS = "agentcore-console-assets-1"
DOCS = "customer-docs"
WF = {"agents": {"reader": {"name": "Reader", "attachments": True}, "other": {"name": "Other"}},
      "steps": [{"agent": "reader"}, {"agent": "other"}],
      "orchestrator": {"attachments": {"s3": [f"{DOCS}/contracts"]}}}
MODULES = ("workflow", "authz", "chatbot", "handler", "builds", "buildstore", "accounts",
           "designer", "policies", "audit", "runfiles", "attachments")
CTX = types.SimpleNamespace(function_name="bff-fn")


def _env(monkeypatch, wf):
    for k, v in {"AWS_ACCESS_KEY_ID": "testing", "AWS_SECRET_ACCESS_KEY": "testing",
                 "AWS_DEFAULT_REGION": "us-east-1", "AWS_REGION": "us-east-1",
                 "WORKFLOW_JSON": json.dumps(wf), "STATUS_TABLE": "app_status",
                 "RUNTIME_ARN": "arn:aws:bedrock-agentcore:us-east-1:1:runtime/app",
                 "ASSETS_BUCKET": ASSETS}.items():
        monkeypatch.setenv(k, v)
    for k in ("BUILDS_TABLE", "BUILDS_BUCKET", "EVENTS_TABLE"):
        monkeypatch.delenv(k, raising=False)


@pytest.fixture(params=[WF])
def app(monkeypatch, request):
    _env(monkeypatch, request.param)
    with moto.mock_aws():
        boto3.client("dynamodb").create_table(
            TableName="app_status", BillingMode="PAY_PER_REQUEST",
            KeySchema=[{"AttributeName": "session_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "session_id", "AttributeType": "S"}])
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket=ASSETS)
        s3.create_bucket(Bucket=DOCS)
        for mod in MODULES:
            sys.modules.pop(mod, None)
        handler = importlib.import_module("handler")
        handler.runfiles._clients.clear()
        invoked: list[dict] = []
        monkeypatch.setattr(handler, "_self_invoke", lambda fn, p: invoked.append(p))
        yield types.SimpleNamespace(handler=handler, s3=s3, invoked=invoked,
                                    status=boto3.resource("dynamodb").Table("app_status"))
    for mod in MODULES:
        sys.modules.pop(mod, None)


def call(e, route_key, body=None, sub="u1"):
    method, path = route_key.split(" ", 1)
    claims = {"sub": sub, "email": f"{sub}@example.com"}
    event = {"requestContext": {"http": {"method": method}, "authorizer": {"jwt": {"claims": claims}}},
             "rawPath": path, "routeKey": route_key, "pathParameters": {}}
    if body is not None:
        event["body"] = json.dumps(body)
    resp = e.handler.handler(event, CTX)
    return resp["statusCode"], json.loads(resp["body"])


def _upload(e, name, data=b"%PDF-1.7 hello", sub="u1"):
    """What the page does: ask for a presigned POST, then put the file where it says."""
    status, post = call(e, "POST /api/sessions/attachments", {"name": name}, sub=sub)
    assert status == 200, post
    e.s3.put_object(Bucket=ASSETS, Key=post["key"], Body=data)
    return {"key": post["key"], "name": post["name"]}


def test_the_form_is_offered_files_only_when_an_agent_reads_them(app):
    status, view = call(app, "GET /api/workflow")
    assert status == 200
    att = view["attachments"]
    assert att["maxFiles"] == 5 and att["s3"] == [f"{DOCS}/contracts"] and "pdf" in att["types"]
    from attachments import run_view
    assert run_view({"agents": {"a": {"name": "A"}}}) is None


def test_an_upload_is_a_presigned_post_into_the_callers_own_folder(app):
    status, post = call(app, "POST /api/sessions/attachments", {"name": "../../brief.pdf"})
    assert status == 200 and post["key"].startswith("uploads/") and post["key"].endswith("-brief.pdf")
    assert post["url"] and post["fields"]["key"] == post["key"]
    other = call(app, "POST /api/sessions/attachments", {"name": "brief.pdf"}, sub="u2")[1]
    assert other["key"].split("/")[1] != post["key"].split("/")[1]
    assert call(app, "POST /api/sessions/attachments", {"name": "tool.exe"})[0] == 400
    assert call(app, "POST /api/sessions/attachments", {"name": ""})[0] == 400


def test_a_run_copies_its_files_into_its_own_folder_and_carries_the_list(app):
    up = _upload(app, "brief.pdf")
    app.s3.put_object(Bucket=DOCS, Key="contracts/acme.docx", Body=b"PK docx")
    app.s3.put_object(Bucket=DOCS, Key="contracts/2026/terms.md", Body=b"# terms")
    status, body = call(app, "POST /api/sessions", {
        "topic": f"Review s3://{DOCS}/contracts/acme.docx and s3://{DOCS}/contracts/2026/.",
        "attachments": [up]})
    assert status == 200, body
    sid = body["session_id"]
    item = app.status.get_item(Key={"session_id": sid})["Item"]
    files = item["attachments"]
    assert [f["name"] for f in files] == ["brief.pdf", "acme.docx", "terms.md"]
    assert [f["kind"] for f in files] == ["document"] * 3
    assert [f["format"] for f in files] == ["pdf", "docx", "md"]
    assert all(f["key"].startswith(f"runs/{sid}/attachments/") for f in files)
    assert files[1]["uri"] == f"s3://{DOCS}/contracts/acme.docx" and "uri" not in files[0]
    assert app.s3.get_object(Bucket=ASSETS, Key=files[0]["key"])["Body"].read() == b"%PDF-1.7 hello"
    assert app.invoked[-1]["attachments"] == files
    # The runtime payload carries them too.
    sent: list = []

    class _RT:
        def invoke_agent_runtime(self, **kw):
            sent.append(json.loads(kw["payload"]))
    app.handler.agentcore = _RT()
    app.handler._run(app.invoked[-1])
    assert sent[0]["attachments"] == files and sent[0]["topic"].startswith("Review")


@pytest.mark.parametrize("bad, why", [
    ({"key": "uploads/someoneelse/1234-x.pdf", "name": "x.pdf"}, "not one of your uploads"),
    ({"key": "runs/abc/attachments/1-x.pdf", "name": "x.pdf"}, "not one of your uploads"),
])
def test_a_run_may_bring_only_the_callers_own_uploads(app, bad, why):
    status, body = call(app, "POST /api/sessions", {"topic": "go", "attachments": [bad]})
    assert status == 400 and why in body["error"]


def test_an_unfinished_upload_or_another_users_is_refused(app):
    status, post = call(app, "POST /api/sessions/attachments", {"name": "a.pdf"})
    status, body = call(app, "POST /api/sessions", {"topic": "go", "attachments": [
        {"key": post["key"], "name": "a.pdf"}]})
    assert status == 400 and "has not finished uploading" in body["error"]
    mine = _upload(app, "b.pdf")
    status, body = call(app, "POST /api/sessions", {"topic": "go", "attachments": [mine]}, sub="u2")
    assert status == 400 and "not one of your uploads" in body["error"]


def test_s3_paths_must_be_inside_an_allowed_location_and_readable(app):
    app.s3.put_object(Bucket=DOCS, Key="private/payroll.pdf", Body=b"%PDF")
    for topic, why in ((f"read s3://{DOCS}/private/payroll.pdf", "not in a location"),
                       (f"read s3://{DOCS}/contracts-old/x.pdf", "not in a location"),
                       (f"read s3://{DOCS}/contracts/missing.pdf", "could not be read"),
                       (f"read s3://{DOCS}/contracts/empty/", "holds no files")):
        status, body = call(app, "POST /api/sessions", {"topic": topic})
        assert status == 400 and why in body["error"], (topic, body)


def test_type_size_and_count_are_checked(app):
    app.s3.put_object(Bucket=DOCS, Key="contracts/tool.exe", Body=b"MZ")
    assert "not a file type" in call(app, "POST /api/sessions",
                                     {"topic": f"s3://{DOCS}/contracts/tool.exe"})[1]["error"]
    app.s3.put_object(Bucket=DOCS, Key="contracts/huge.png", Body=b"x" * 3_800_000)
    assert "the most is 3.75 MB" in call(app, "POST /api/sessions",
                                         {"topic": f"s3://{DOCS}/contracts/huge.png"})[1]["error"]
    for i in range(6):
        app.s3.put_object(Bucket=DOCS, Key=f"contracts/many/{i}.txt", Body=b"t")
    assert "at most 5 files" in call(app, "POST /api/sessions",
                                     {"topic": f"s3://{DOCS}/contracts/many/"})[1]["error"]


@pytest.mark.parametrize("app", [{"agents": {"a": {"name": "A"}}, "steps": [{"agent": "a"}]}],
                         indirect=True)
def test_a_workflow_that_reads_no_files_is_unchanged(app):
    status, body = call(app, "POST /api/sessions", {"topic": f"note s3://{DOCS}/contracts/x.pdf"})
    assert status == 200
    item = app.status.get_item(Key={"session_id": body["session_id"]})["Item"]
    assert "attachments" not in item and "attachments" not in app.invoked[-1]
    assert call(app, "POST /api/sessions/attachments", {"name": "a.pdf"})[0] == 404
    status, body = call(app, "POST /api/sessions", {"topic": "go", "attachments": [{"key": "k"}]})
    assert status == 400 and "reads attached files" in body["error"]
    assert call(app, "GET /api/workflow")[1]["attachments"] is None
