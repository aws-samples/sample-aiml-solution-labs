"""A run's files from the BFF (bff/runartifacts.py): the Files tab and the Files page.

What has to hold:
  * a run's files go only to whoever may read the run (404 for anyone else, before the
    bucket is read); an admin reads anyone's; everyone's list needs `admin`;
  * the list groups inputs, outputs (saved files and drawn images) and code, with the
    agent that made each;
  * a link is signed only for a key under THAT run's folder, for 5 minutes, opens in the
    browser only for an image or a PDF, with a type chosen from the extension;
  * a deployment with no assets bucket, and a build's run, list nothing and say why;
  * every route is declared on both IaC paths, behind the JWT authorizer.
"""
# ruff: noqa: F811 - `env` is the builds fixture, imported so a test can take it
from __future__ import annotations

import pathlib
import sys
import urllib.parse

import boto3
import pytest
from test_builds import call, env, mark_deployed, save  # noqa: F401

moto = pytest.importorskip("moto")

BUCKET = "agentcore-console-assets-1"
ORCH = pathlib.Path(__file__).resolve().parent.parent
SID, OTHER = "abc123def456", "fff000eee111"


@pytest.fixture()
def app(env, monkeypatch):
    monkeypatch.setattr(env.handler, "ASSETS_BUCKET", BUCKET)
    ra = env.handler.runartifacts
    ra._clients.clear()
    s3 = boto3.client("s3")
    s3.create_bucket(Bucket=BUCKET)
    status = boto3.resource("dynamodb").Table("console_status")
    status.put_item(Item={"session_id": SID, "owner": "u1", "overall": "done", "topic": "Price the plan",
                          "created": "2026-10-08 10:00:00"})
    status.put_item(Item={"session_id": OTHER, "owner": "u2", "overall": "done", "topic": "Someone else's",
                          "created": "2026-10-08 11:00:00"})
    for key, body, meta in (
            (f"runs/{SID}/attachments/1-brief.pdf", b"%PDF-1.7", {}),
            (f"runs/{SID}/outputs/report.csv", b"a,b\n1,2\n", {"agent": "writer"}),
            (f"runs/{SID}/outputs/page.html", b"<script>alert(1)</script>", {"agent": "writer"}),
            (f"runs/{SID}/hero/1700000000000.png", b"\x89PNG", {}),
            (f"runs/{SID}/code/analyst-1700000000001.py", b"print(1)", {"agent": "analyst"}),
            (f"runs/{SID}/code/analyst-1700000000001.log", b"=== stdout ===\n1\n", {"agent": "analyst"}),
            (f"runs/{OTHER}/outputs/secret.txt", b"not yours", {"agent": "writer"})):
        s3.put_object(Bucket=BUCKET, Key=key, Body=body, Metadata=meta)
    yield env
    ra._clients.clear()


def _files(e, sid=SID, **kw):
    return call(e, "GET /api/sessions/{id}/files", params={"id": sid}, **kw)


def _link(e, key, sid=SID, **kw):
    return call(e, "GET /api/sessions/{id}/files/url", params={"id": sid}, qs={"key": key}, **kw)


def test_a_runs_files_are_grouped_with_the_agent_that_made_each(app):
    status, body = _files(app)
    assert status == 200 and body["stored"] is True and body["truncated"] is False
    by_name = {f["name"]: f for f in body["files"]}
    assert set(by_name) == {"brief.pdf", "report.csv", "page.html", "hero-1700000000000.png",
                            "analyst-1700000000001.py", "analyst-1700000000001.log"}
    assert by_name["brief.pdf"]["group"] == "input" and by_name["brief.pdf"]["agent"] == ""
    assert by_name["report.csv"] == {**by_name["report.csv"], "group": "output", "agent": "writer",
                                     "preview": "csv", "size": 8, "session": SID}
    assert by_name["hero-1700000000000.png"]["agent"] == "hero"
    assert by_name["hero-1700000000000.png"]["preview"] == "image"
    assert by_name["analyst-1700000000001.py"]["group"] == "code"
    assert by_name["analyst-1700000000001.py"]["agent"] == "analyst"
    assert by_name["page.html"]["preview"] == "" and by_name["page.html"]["type"] == "application/octet-stream"
    assert all(f["key"].startswith(f"runs/{SID}/") for f in body["files"])


def test_only_whoever_may_read_the_run_sees_its_files(app):
    assert _files(app, sub="u2")[0] == 404
    assert _link(app, f"runs/{SID}/outputs/report.csv", sub="u2")[0] == 404
    assert _files(app, sid="../../x")[0] == 404
    # An admin reads anyone's run, as on the run itself.
    status, body = _files(app, sid=OTHER, groups=("admins",))
    assert status == 200 and [f["name"] for f in body["files"]] == ["secret.txt"]


def test_a_link_is_only_for_a_file_of_that_run(app):
    status, got = _link(app, f"runs/{SID}/outputs/report.csv")
    assert status == 200 and got["expiresIn"] == 300 and got["preview"] == "csv"
    q = urllib.parse.parse_qs(urllib.parse.urlparse(got["url"]).query)
    # SigV4 says how long in X-Amz-Expires; the older query form gives the moment in Expires.
    assert q.get("X-Amz-Expires", ["300"]) == ["300"] and ("X-Amz-Expires" in q or "Expires" in q)
    assert q["response-content-type"] == ["text/csv; charset=utf-8"]
    assert q["response-content-disposition"][0].startswith('attachment; filename="report.csv"')
    for bad in (f"runs/{OTHER}/outputs/secret.txt", f"runs/{SID}/../{OTHER}/outputs/secret.txt",
                f"runs/{SID}/", f"runs/{SID}/outputs/a/b.txt", "uploads/x/1-a.pdf", "",
                f"runs/{SID}/outputs/" + "x" * 1100):
        assert _link(app, bad)[0] == 400, bad


def test_only_images_and_pdfs_open_in_the_browser(app):
    def disposition(key):
        url = _link(app, key)[1]["url"]
        return urllib.parse.parse_qs(urllib.parse.urlparse(url).query)["response-content-disposition"][0]
    assert disposition(f"runs/{SID}/hero/1700000000000.png").startswith("inline")
    assert disposition(f"runs/{SID}/attachments/1-brief.pdf").startswith("inline")
    # An agent's .html is a download served as bytes, never a page from the bucket's origin.
    url = _link(app, f"runs/{SID}/outputs/page.html")[1]["url"]
    q = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
    assert q["response-content-disposition"][0].startswith("attachment")
    assert q["response-content-type"] == ["application/octet-stream"]


def test_the_files_page_lists_the_callers_runs_and_everyones_only_for_admin(app):
    status, body = call(app, "GET /api/files", path="/api/files")
    assert status == 200 and body["stored"] and body["nextPage"] is None
    assert {f["session"] for f in body["files"]} == {SID}
    assert {f["topic"] for f in body["files"]} == {"Price the plan"}
    assert call(app, "GET /api/files", path="/api/files", qs={"scope": "all"})[0] == 403
    status, body = call(app, "GET /api/files", path="/api/files", qs={"scope": "all"}, groups=("admins",))
    assert status == 200 and {f["session"] for f in body["files"]} == {SID, OTHER}
    assert call(app, "GET /api/files", path="/api/files", qs={"page": "-1"})[0] == 400
    assert call(app, "GET /api/files", path="/api/files", qs={"page": "x"})[0] == 400


def test_the_files_page_is_paged_by_run(app):
    status_tbl = boto3.resource("dynamodb").Table("console_status")
    for i in range(12):
        status_tbl.put_item(Item={"session_id": f"run{i:09d}", "owner": "u1", "topic": f"t{i}",
                                  "created": f"2026-10-0{1 + i % 7} 09:00:{i:02d}"})
    first = call(app, "GET /api/files", path="/api/files")[1]
    assert first["runs"] == 10 and first["nextPage"] == 1
    second = call(app, "GET /api/files", path="/api/files", qs={"page": "1"})[1]
    assert second["runs"] == 3 and second["nextPage"] is None


def test_no_bucket_and_a_builds_run_list_nothing_and_say_why(app, monkeypatch):
    save(app)
    call(app, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"}, body={"tool": "cdk"})
    mark_deployed(app, "pclaims01")
    sid = call(app, "POST /api/sessions", body={"build": "pclaims01"})[1]["session_id"]
    status, body = _files(app, sid=sid)
    assert status == 200 and body["stored"] is False and "build's own app" in body["note"]
    assert _link(app, f"runs/{sid}/outputs/x.txt", sid=sid)[0] == 404
    monkeypatch.setattr(app.handler, "ASSETS_BUCKET", "")
    assert _files(app)[1] == {"stored": False, "files": []}
    assert call(app, "GET /api/files", path="/api/files")[1]["stored"] is False


def test_a_control_plane_console_runs_nothing_so_lists_no_files(app, monkeypatch):
    monkeypatch.setattr(app.handler, "CONSOLE_MODE", "builder")
    assert call(app, "GET /api/files", path="/api/files")[0] == 404
    assert _files(app)[0] == 404


def test_every_route_is_declared_on_both_paths_behind_the_authorizer():
    tf = (ORCH / "terraform" / "bff.tf").read_text()
    cdk = (ORCH / "cdk" / "lib" / "orchestrator-stack.ts").read_text()
    sys.path.insert(0, str(ORCH / "bff"))
    import runartifacts
    for route in runartifacts.ROUTES:
        method, path = route.split(" ", 1)
        assert f'"{route}"' in tf
        assert f'{{ path: "{path}", methods: [HttpMethod.{method}] }}' in cdk
