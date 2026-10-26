"""Deliver steps (v139 Track C deliver): `task.kind: "deliver"`, app/common/deliver.py.

What has to hold:
  * a target picks the run's saved files by name pattern and by the step that made them,
    and a `when` that does not hold skips it (on the timeline, not a failure);
  * s3: each file is copied to <prefix><session>/<name> in the configured bucket;
  * email: SES v2 SendEmail from the configured identity's address to the AUTHOR's `to`;
    a step's output adds recipients only through `recipientsFrom`; files are attached while
    they fit and linked above it; SES's own error (the sandbox's "not verified") fails the
    step after the other targets were tried, in SES's words;
  * webhook: an https POST through the SSRF guard with each file's link, signed exactly as
    the framework's own webhooks (bff/triggers.py) when `secret` names a deployed secret;
  * every delivery and failure goes to the activity log, and the runtime refuses a target
    it cannot run;
  * the runtime, bff/task_check.py and taskCheck.ts read the same keys.

Every AWS call goes to a fake whose requests are checked against the installed botocore
model, so a request the real service would refuse fails here.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import sys
import urllib.error

import botocore.session
import botocore.validate
import pytest
from conftest import ORCH_ROOT

from app.common import audit, deliver
from app.common import task_agent as ta

#: The modules this file patches, as imported. Other tests purge app.* from sys.modules
#: (conftest.workflow), and the runtime imports deliver and task_agent lazily (task_agent
#: _run_deliver, deliver.run), so each test pins these back (_same_modules) or a lazy
#: import would load fresh, unpatched copies.
_PINNED = {name: sys.modules[name] for name in ("app", "app.common", "app.common.audit",
                                               "app.common.deliver", "app.common.task_agent")}


@pytest.fixture(autouse=True)
def _same_modules(monkeypatch):
    for name, module in _PINNED.items():
        monkeypatch.setitem(sys.modules, name, module)

SID = "sess123456"
_SESSION = botocore.session.get_session()


def _check(service: str, op: str, kw: dict) -> None:
    shape = _SESSION.get_service_model(service).operation_model(op).input_shape
    botocore.validate.validate_parameters(kw, shape)


class ClientError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(f"An error occurred ({code}) when calling the operation: {message}")
        self.response = {"Error": {"Code": code, "Message": message}}


class FakeS3:
    def __init__(self, files: dict[str, tuple[bytes, str, str]]):
        # name -> (body, content type, agent)
        self.files = files
        self.copies: list[dict] = []
        self.refuse: str = ""

    def list_objects_v2(self, **kw):
        _check("s3", "ListObjectsV2", kw)
        assert kw["Prefix"] == f"runs/{SID}/outputs/"
        return {"Contents": [{"Key": f"{kw['Prefix']}{n}", "Size": len(b)} for n, (b, _, _) in self.files.items()],
                "IsTruncated": False}

    def head_object(self, **kw):
        _check("s3", "HeadObject", kw)
        body, ctype, agent = self.files[kw["Key"].rsplit("/", 1)[-1]]
        return {"ContentType": ctype, "Metadata": {"agent": agent}, "ContentLength": len(body)}

    def get_object(self, **kw):
        _check("s3", "GetObject", kw)
        body = self.files[kw["Key"].rsplit("/", 1)[-1]][0]

        class Body:
            def read(self):
                return body
        return {"Body": Body()}

    def copy_object(self, **kw):
        # boto3 takes CopySource as {"Bucket", "Key"} and encodes it before validation
        # (botocore.handlers.handle_copy_source_param), as the real client does.
        from botocore.handlers import handle_copy_source_param
        wire = dict(kw)
        handle_copy_source_param(wire)
        _check("s3", "CopyObject", wire)
        if self.refuse and kw["Bucket"] == self.refuse:
            raise ClientError("AccessDenied", "Access Denied")
        self.copies.append(kw)
        return {}

    def generate_presigned_url(self, op, Params, ExpiresIn):  # boto3's names
        assert op == "get_object"
        return f"https://assets.s3.amazonaws.com/{Params['Key']}?X-Amz-Expires={ExpiresIn}"


class FakeSES:
    def __init__(self, error: ClientError | None = None):
        self.sent: list[dict] = []
        self.error = error

    def send_email(self, **kw):
        _check("sesv2", "SendEmail", kw)
        if self.error:
            raise self.error
        self.sent.append(kw)
        return {"MessageId": "m-1"}


class Ctx:
    def __init__(self, outputs=None):
        self.topic = "make the reports"
        self.session_id = SID
        self.agent_id = "send"
        self.state = {"outputs": dict(outputs or {})}
        self.logs: list[str] = []

    async def log(self, msg):
        self.logs.append(msg)


FILES = {"report.pdf": (b"%PDF-1.7 report", "application/pdf", "render_pdf"),
         "deck.pptx": (b"PK deck", "application/vnd.openxmlformats-officedocument.presentationml.presentation", "render_deck"),
         "notes.txt": (b"notes", "text/plain", "check")}
OUTPUTS = {"check": json.dumps({"passed": True, "contacts": ["owner@example.com", "lead@example.com"]})}
IDENTITY = "arn:aws:ses:eu-west-1:111122223333:identity/example.com"


@pytest.fixture()
def aws(monkeypatch):
    s3, ses = FakeS3(dict(FILES)), FakeSES()
    regions: list[str] = []

    def client(service, region=""):
        if service == "sesv2":
            regions.append(region)
            return ses
        return s3
    monkeypatch.setattr(deliver, "_client", client)
    monkeypatch.setenv("ASSETS_BUCKET", "agentcore-app-assets-111122223333")
    events: list[tuple] = []
    monkeypatch.setattr(audit, "run_event", lambda sid, action, detail: events.append((sid, action, detail)))
    return s3, ses, regions, events


def _agent(targets: list[dict]) -> ta.TaskAgent:
    a = ta.TaskAgent()
    a.id, a.name = "send", "Send"
    return a.use({"kind": "deliver", "targets": targets})


def run(agent, ctx) -> dict:
    return json.loads(asyncio.run(agent.run(ctx)))


# --- picking files and conditions -------------------------------------------------------

def test_a_target_picks_files_by_pattern_and_by_the_step_that_made_them(aws):
    files = [{"name": n, "agent": a} for n, (_, _, a) in FILES.items()]
    assert [f["name"] for f in deliver.select(files, {"files": ["*.PDF", "deck.*"]})] == ["report.pdf", "deck.pptx"]
    assert [f["name"] for f in deliver.select(files, {"producedBy": ["check"]})] == ["notes.txt"]
    assert [f["name"] for f in deliver.select(files, {"files": ["*.pdf"], "producedBy": ["check"]})] == []
    assert len(deliver.select(files, {})) == 3


@pytest.mark.parametrize("when, holds", [
    ({"field": "outputs.check.passed", "equals": True}, True),
    ({"field": "outputs.check.passed", "equals": "true"}, True),
    ({"field": "outputs.check.passed", "equals": False}, False),
    ({"field": "outputs.check.contacts", "gte": 2}, True),
    ({"field": "outputs.check.missing", "exists": False}, True),
    ({"field": "outputs.nobody.x", "equals": 1}, False),
    ({"field": "request", "contains": "REPORTS"}, True),
])
def test_a_condition_uses_the_branch_operators_on_the_run(when, holds):
    data = {"request": "make the reports", "session": SID, "outputs": OUTPUTS}
    assert deliver.condition(when, data)[0] is holds


def test_a_target_whose_condition_does_not_hold_is_skipped_and_said_so(aws):
    s3, _, _, events = aws
    out = run(_agent([{"type": "s3", "uri": "s3://amzn-s3-demo-bucket/out/",
                       "when": {"field": "outputs.check.passed", "equals": False}}]), Ctx(OUTPUTS))
    assert out["deliveries"][0]["status"] == "skipped" and not s3.copies and not events
    assert out["task"] == "deliver"


# --- s3 ---------------------------------------------------------------------------------

def test_s3_copies_each_picked_file_under_the_prefix_and_the_run(aws):
    s3, _, _, events = aws
    ctx = Ctx(OUTPUTS)
    out = run(_agent([{"type": "s3", "uri": "s3://amzn-s3-demo-bucket/reports/", "files": ["*.pdf", "*.pptx"]}]), ctx)
    assert [(c["Bucket"], c["Key"], c["CopySource"]["Key"]) for c in s3.copies] == [
        ("amzn-s3-demo-bucket", f"reports/{SID}/deck.pptx", f"runs/{SID}/outputs/deck.pptx"),
        ("amzn-s3-demo-bucket", f"reports/{SID}/report.pdf", f"runs/{SID}/outputs/report.pdf")]
    d = out["deliveries"][0]
    assert d["status"] == "delivered" and d["destination"] == f"s3://amzn-s3-demo-bucket/reports/{SID}/"
    assert any("delivered 2 files (deck.pptx, report.pdf) by s3" in m for m in ctx.logs)
    assert events == [(SID, "run.delivered", {"step": "send", "target": "s3", "to": "s3://amzn-s3-demo-bucket/reports/",
                                              "files": "deck.pptx, report.pdf", "error": ""})]


def test_the_storage_token_needs_the_builds_storage(aws, monkeypatch):
    monkeypatch.delenv("STORAGE_BUCKET", raising=False)
    with pytest.raises(ta.TaskFailed, match=r"uses \$\{storage\}, but this deployment has no build storage"):
        run(_agent([{"type": "s3", "uri": "${storage}/out/"}]), Ctx())
    monkeypatch.setenv("STORAGE_BUCKET", "app-storage-111122223333")
    s3 = aws[0]
    run(_agent([{"type": "s3", "uri": "${storage}/out/", "files": ["notes.txt"]}]), Ctx())
    assert s3.copies[-1]["Bucket"] == "app-storage-111122223333" and s3.copies[-1]["Key"] == f"out/{SID}/notes.txt"


def test_a_run_with_no_file_store_fails_the_step(monkeypatch):
    monkeypatch.delenv("ASSETS_BUCKET", raising=False)
    with pytest.raises(ta.TaskFailed, match="keeps no run files"):
        run(_agent([{"type": "s3", "uri": "s3://amzn-s3-demo-bucket/out/"}]), Ctx())


# --- email ------------------------------------------------------------------------------

EMAIL = {"type": "email", "identityArn": IDENTITY, "fromAddress": "reports@example.com",
         "to": ["team@example.com"], "subject": "Reports for {{session}}", "files": ["*.pdf", "*.pptx"]}


def test_email_attaches_the_files_from_the_identity_to_the_authors_recipients(aws):
    _, ses, regions, _ = aws
    out = run(_agent([EMAIL]), Ctx(OUTPUTS))
    sent = ses.sent[0]
    assert regions == ["eu-west-1"]                      # the identity's Region
    assert sent["FromEmailAddress"] == "reports@example.com"
    assert sent["Destination"] == {"ToAddresses": ["team@example.com"]}
    simple = sent["Content"]["Simple"]
    assert simple["Subject"]["Data"] == f"Reports for {SID}"
    assert [(a["FileName"], a["RawContent"], a["ContentDisposition"]) for a in simple["Attachments"]] == [
        ("deck.pptx", b"PK deck", "ATTACHMENT"), ("report.pdf", b"%PDF-1.7 report", "ATTACHMENT")]
    assert out["deliveries"][0]["mode"] == "attached" and out["deliveries"][0]["messageId"] == "m-1"


def test_a_steps_output_adds_recipients_only_when_the_author_allows_it(aws):
    _, ses, _, _ = aws
    run(_agent([EMAIL]), Ctx(OUTPUTS))
    assert ses.sent[-1]["Destination"]["ToAddresses"] == ["team@example.com"]
    run(_agent([{**EMAIL, "recipientsFrom": "{{outputs.check.contacts}}"}]), Ctx(OUTPUTS))
    assert ses.sent[-1]["Destination"]["ToAddresses"] == ["team@example.com", "owner@example.com", "lead@example.com"]


def test_a_bad_or_excess_recipient_is_refused_before_ses_is_called(aws):
    _, ses, _, _ = aws
    bad = {"check": json.dumps({"contacts": ["x@example.com", "not an address"]})}
    with pytest.raises(ta.TaskFailed, match="'not' is not an email address"):
        run(_agent([{**EMAIL, "recipientsFrom": "{{outputs.check.contacts}}"}]), Ctx(bad))
    many = {"check": json.dumps({"contacts": [f"p{i}@example.com" for i in range(60)]})}
    with pytest.raises(ta.TaskFailed, match="at most 50"):
        run(_agent([{**EMAIL, "recipientsFrom": "{{outputs.check.contacts}}"}]), Ctx(many))
    assert ses.sent == []


def test_files_over_the_attachment_limit_go_as_links(aws, monkeypatch):
    _, ses, _, _ = aws
    monkeypatch.setattr(deliver, "ATTACH_MAX_BYTES", 10)
    out = run(_agent([{**EMAIL, "linkExpiresMinutes": 30}]), Ctx())
    simple = ses.sent[0]["Content"]["Simple"]
    assert "Attachments" not in simple
    text = simple["Body"]["Text"]["Data"]
    assert f"runs/{SID}/outputs/report.pdf?X-Amz-Expires=1800" in text and "30 minutes" in text
    assert out["deliveries"][0]["mode"] == "links"


def test_the_ses_sandbox_error_is_shown_and_the_other_targets_still_run(aws):
    s3, ses, _, events = aws
    ses.error = ClientError("MessageRejected", "Email address is not verified. The following identities failed "
                                               "the check in region EU-WEST-1: team@example.com")
    ctx = Ctx()
    with pytest.raises(ta.TaskFailed) as e:
        run(_agent([EMAIL, {"type": "s3", "uri": "s3://amzn-s3-demo-bucket/out/"}]), ctx)
    msg = str(e.value)
    assert "1 of 2 deliveries failed" in msg and "MessageRejected: Email address is not verified" in msg
    assert "SES sandbox, every recipient must be a verified identity too" in msg
    assert len(s3.copies) == 3                           # the s3 target was still delivered
    assert [a for _, a, _ in events] == ["run.delivery_failed", "run.delivered"]
    assert any("FAILED" in m and "team@example.com" in m for m in ctx.logs)


# --- webhook ----------------------------------------------------------------------------

class FakeResponse:
    def __init__(self, status=200):
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def getcode(self):
        return self.status


def _bff(name):
    sys.path.insert(0, str(ORCH_ROOT / "bff"))
    try:
        return __import__(name)
    finally:
        sys.path.pop(0)


def test_a_webhook_gets_the_links_signed_like_the_apps_own_webhooks(aws, monkeypatch):
    posted = {}

    def urlopen(req, timeout=10, allow_private=False):
        posted.update(url=req.full_url, body=req.data.decode(), headers=dict(req.header_items()),
                      method=req.get_method(), allow_private=allow_private, timeout=timeout)
        return FakeResponse(204)
    monkeypatch.setattr(deliver.netguard, "urlopen", urlopen)
    monkeypatch.setenv("DELIVER_SECRETS", json.dumps({"hook": "s3cret"}))
    out = run(_agent([{"type": "webhook", "url": "https://hooks.example.com/in?token=abc", "secret": "hook",
                       "files": ["*.pdf"]}]), Ctx())
    body = json.loads(posted["body"])
    assert posted["method"] == "POST" and posted["allow_private"] is False
    assert body["event"] == "agentexpress.delivery" and body["session"] == SID and body["step"] == "send"
    assert [(f["name"], f["producedBy"]) for f in body["files"]] == [("report.pdf", "render_pdf")]
    assert body["files"][0]["url"].endswith("X-Amz-Expires=3600")
    ts = posted["headers"]["X-ax-timestamp"]
    expected = hmac.new(b"s3cret", f"{ts}.{posted['body']}".encode(), hashlib.sha256).hexdigest()
    assert posted["headers"]["X-ax-signature"] == f"sha256={expected}"
    triggers = _bff("triggers")
    assert triggers.sign("s3cret", posted["body"], int(ts)) == deliver.signature("s3cret", posted["body"], int(ts))
    # The token in the query is never shown.
    d = out["deliveries"][0]
    assert d["url"] == "https://hooks.example.com/in" and "abc" not in json.dumps(out)


def test_a_webhook_to_a_private_address_or_over_http_is_refused(aws, monkeypatch):
    import socket
    monkeypatch.setattr(socket, "getaddrinfo", lambda *a, **k: [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.5", 443))])
    with pytest.raises(ta.TaskFailed, match="resolves to a non-public address"):
        run(_agent([{"type": "webhook", "url": "https://internal.example.com/in"}]), Ctx())
    with pytest.raises(ta.TaskFailed, match="only https:// URLs"):
        run(_agent([{"type": "webhook", "url": "http://hooks.example.com/in"}]), Ctx())


def test_a_webhook_that_answers_an_error_or_names_a_missing_secret_fails(aws, monkeypatch):
    def urlopen(req, timeout=10, allow_private=False):
        raise urllib.error.HTTPError(req.full_url, 500, "boom", {}, None)
    monkeypatch.setattr(deliver.netguard, "urlopen", urlopen)
    monkeypatch.delenv("DELIVER_SECRETS", raising=False)
    with pytest.raises(ta.TaskFailed, match="answered HTTP 500"):
        run(_agent([{"type": "webhook", "url": "https://hooks.example.com/in"}]), Ctx())
    with pytest.raises(ta.TaskFailed, match="deliverSecrets secret 'hook' was not deployed"):
        run(_agent([{"type": "webhook", "url": "https://hooks.example.com/in", "secret": "hook"}]), Ctx())


# --- audit ------------------------------------------------------------------------------

def test_a_delivery_is_written_to_the_run_owners_activity_log(monkeypatch):
    moto = pytest.importorskip("moto")
    import boto3
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "x")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "x")
    with moto.mock_aws():
        ddb = boto3.resource("dynamodb", region_name="us-east-1")
        ddb.create_table(TableName="s", KeySchema=[{"AttributeName": "session_id", "KeyType": "HASH"}],
                         AttributeDefinitions=[{"AttributeName": "session_id", "AttributeType": "S"}],
                         BillingMode="PAY_PER_REQUEST")
        ddb.create_table(TableName="a", KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"},
                                                   {"AttributeName": "sk", "KeyType": "RANGE"}],
                         AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"},
                                               {"AttributeName": "sk", "AttributeType": "S"}],
                         BillingMode="PAY_PER_REQUEST")
        ddb.Table("s").put_item(Item={"session_id": SID, "owner": "u1", "user": "u1@example.com"})
        monkeypatch.setenv("STATUS_TABLE", "s")
        monkeypatch.setattr(audit, "AUDIT_TABLE", "a")
        monkeypatch.setattr(audit, "_table", None)
        audit.run_event(SID, "run.delivered", {"step": "send", "target": "s3", "to": "s3://b/p/", "error": ""})
        items = ddb.Table("a").scan()["Items"]
        assert min(i["pk"] for i in items) == "AUDIT#u1"
        assert {i["action"] for i in items} == {"run.delivered"}
        assert items[0]["detail"] == {"session": SID, "step": "send", "target": "s3", "to": "s3://b/p/"}
    buildstore = _bff("buildstore")
    assert {"run.delivered", "run.delivery_failed"} <= set(buildstore.AUDIT_ACTIONS)


# --- what the runtime refuses, and one set of keys ----------------------------------------

@pytest.mark.parametrize("task, why", [
    ({"kind": "deliver"}, "needs task.targets"),
    ({"kind": "deliver", "targets": []}, "needs task.targets"),
    ({"kind": "deliver", "targets": [{"type": "fax"}]}, r"targets\[0\]\.type is not one of s3, email, webhook"),
    ({"kind": "deliver", "targets": [{"type": "s3", "url": "https://x"}]}, r"targets\[0\]\.url does not apply"),
    ({"kind": "deliver", "targets": [{"type": "s3"}] * 11}, "at most 10 targets"),
])
def test_the_runtime_refuses_a_deliver_step_it_cannot_run(task, why):
    with pytest.raises(ValueError, match=why):
        ta.validate({"send": {"runtime": "task", "task": task}}, {})


def test_the_runtime_and_both_validators_read_the_same_keys():
    task_check = _bff("task_check")
    assert task_check.KEYS_OF == ta.KEYS_OF
    assert task_check.TARGET_KEYS == deliver.TARGET_KEYS
    assert task_check.MAX_TARGETS == deliver.MAX_TARGETS and task_check.MAX_RECIPIENTS == deliver.MAX_RECIPIENTS
    assert task_check.LINK_MAX_MINUTES == deliver.LINK_MAX_MINUTES
    assert task_check.S3_URI_RE.pattern == deliver.S3_URI_RE.pattern
    assert task_check.EMAIL_RE.pattern == deliver.EMAIL_RE.pattern
    assert tuple(task_check.WHEN_OPS) == tuple(sorted(deliver.branching._OPS, key=task_check.WHEN_OPS.index))
    ts = (ORCH_ROOT / "web" / "src" / "builder" / "taskCheck.ts").read_text()
    for k in ("identityArn", "fromAddress", "recipientsFrom", "linkExpiresMinutes", "producedBy"):
        assert f'"{k}"' in ts
    assert 'deliver: ["kind", "targets"]' in ts
