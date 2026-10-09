"""How a run ended goes in the activity log (app/common/audit.py), written by the
runtime through the progress sink, once per end — the BFF logs only the start."""
import importlib
import sys
from pathlib import Path

import boto3
import pytest

moto = pytest.importorskip("moto")

ORCH = Path(__file__).resolve().parent.parent


def _forget():
    """Drop the modules AND their attributes on the package: `from app.common import audit`
    returns the package attribute when there is one, so clearing sys.modules alone kept a
    module imported earlier (with another AUDIT_TABLE) whenever an earlier test had one."""
    import app.common as pkg
    for m in ("config", "audit", "sink"):
        sys.modules.pop(f"app.common.{m}", None)
        pkg.__dict__.pop(m, None)


@pytest.fixture()
def sink(monkeypatch):
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
        monkeypatch.setenv("STATUS_TABLE", "s")
        monkeypatch.setenv("AUDIT_TABLE", "a")
        monkeypatch.delenv("EVENTS_TABLE", raising=False)
        _forget()
        mod = importlib.import_module("app.common.sink")
        mod._ddb = ddb
        yield mod, ddb.Table("s"), ddb.Table("a")
        _forget()


def _log(audit, owner="u1"):
    from boto3.dynamodb.conditions import Key
    return [i["action"] for i in audit.query(KeyConditionExpression=Key("pk").eq(f"AUDIT#{owner}"))["Items"]]


def test_a_run_that_ends_is_logged_once_with_its_owner(sink):
    mod, status, audit = sink
    status.put_item(Item={"session_id": "r1", "overall": "running", "owner": "u1",
                          "user": "u1@example.com", "topic": "Add 2 and 3"})
    mod._write_ddb("r1", {"type": "session_status", "status": "done", "result": "5"})
    mod._write_ddb("r1", {"type": "session_status", "status": "done"})      # the same end, again
    assert _log(audit) == ["run.completed"]
    item = audit.scan()["Items"][0]
    assert item["email"] == "u1@example.com" and item["detail"] == {"session": "r1", "topic": "Add 2 and 3"}
    # A re-run that fails is a new end; a cancel is the BFF's to log.
    mod._write_ddb("r1", {"type": "session_status", "status": "running"})
    mod._write_ddb("r1", {"type": "session_status", "status": "failed", "log": "Workflow error: GuardrailBlocked: no"})
    mod._write_ddb("r1", {"type": "session_status", "status": "cancelled"})
    assert sorted(_log(audit)) == ["run.completed", "run.failed"]
    failed = next(i for i in audit.scan()["Items"] if i["action"] == "run.failed" and i["pk"].startswith("AUDIT#"))
    assert failed["detail"]["error"] == "GuardrailBlocked: no"


def test_the_runtime_writes_events_exactly_as_the_bff_does():
    sys.path.insert(0, str(ORCH / "bff"))
    try:
        import buildstore
    finally:
        sys.path.pop(0)
    from app.common import audit
    args = ("u1", "u1@example.com", "run.completed", {"session": "s", "empty": ""})
    assert audit.audit_items(*args, ts="2026-09-09T10:00:00Z", rand="1234abcd") == \
        buildstore.audit_items(*args, ts="2026-09-09T10:00:00Z", rand="1234abcd")
    assert set(audit.FINISHED.values()) <= set(buildstore.AUDIT_ACTIONS)
