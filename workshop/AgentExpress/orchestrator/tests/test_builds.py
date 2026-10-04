"""Builder builds: stored per user, deployed one stack each, run from the console.

What these hold the BFF (bff/builds.py, the /api/builds routes in bff/handler.py) and
the deploy runner (deployer/runner.py) to:

  * a build is private to its owner, and its AWS name is fixed at creation;
  * a deploy freezes an immutable VERSION, and edits made afterwards cannot change it;
  * one job at a time per build, and destroy always uses the tool it was deployed with;
  * deploy and destroy are guarded by their own permissions;
  * a run started against a build lives in THAT build's tables, carries a snapshot of
    the workflow it ran with, and every later per-run call finds it by its id.

DynamoDB and S3 are moto's in-memory versions, because conditional writes, nested-map
updates and the owner GSI are exactly what a hand-rolled fake would get wrong.
CodeBuild and the AgentCore runtime are stubbed; nothing here touches AWS.
"""

from __future__ import annotations

import importlib
import json
import sys
import types
from pathlib import Path

import boto3
import pytest

moto = pytest.importorskip("moto")

RULES = {"groupsClaim": "cognito:groups",
         "actions": {"deploy": ["operators"], "destroy": ["operators"],
                     "insights": ["operators"], "audit": ["auditors"],
                     "admin": ["admins"]}}
CONSOLE_WORKFLOW = {"agents": {"a": {"name": "A"}}, "steps": [{"agent": "a"}],
                    "authorization": RULES}
MODULES = ("workflow", "authz", "chatbot", "handler", "builds", "buildstore", "accounts",
           "designer", "policies", "audit")
CTX = types.SimpleNamespace(function_name="bff-fn")


class FakeCodeBuild:
    def __init__(self):
        self.started: list[dict] = []
        self.status: dict[str, str] = {}

    def start_build(self, **kw):
        job_id = f"agentexpress-deploy:{len(self.started) + 1}"
        self.started.append({"id": job_id, **kw})
        self.status[job_id] = "IN_PROGRESS"
        return {"build": {"id": job_id}}

    def batch_get_builds(self, ids):
        return {"builds": [{"id": i, "buildStatus": self.status.get(i, "IN_PROGRESS"),
                            "logs": {"deepLink": f"https://logs/{i}"}} for i in ids]}

    def env(self, n=-1) -> dict:
        return {e["name"]: e["value"] for e in self.started[n]["environmentVariablesOverride"]}


def _status_table(ddb, name):
    ddb.create_table(TableName=name, BillingMode="PAY_PER_REQUEST",
                     KeySchema=[{"AttributeName": "session_id", "KeyType": "HASH"}],
                     AttributeDefinitions=[{"AttributeName": "session_id",
                                            "AttributeType": "S"}])


def _events_table(ddb, name):
    ddb.create_table(TableName=name, BillingMode="PAY_PER_REQUEST",
                     KeySchema=[{"AttributeName": "session_id", "KeyType": "HASH"},
                                {"AttributeName": "ts", "KeyType": "RANGE"}],
                     AttributeDefinitions=[{"AttributeName": "session_id", "AttributeType": "S"},
                                           {"AttributeName": "ts", "AttributeType": "S"}])


def create_builds_table(ddb, name="console_builds"):
    ddb.create_table(
        TableName=name, BillingMode="PAY_PER_REQUEST",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"},
                   {"AttributeName": "sk", "KeyType": "RANGE"}],
        AttributeDefinitions=[{"AttributeName": a, "AttributeType": "S"}
                              for a in ("pk", "sk", "owner", "updated")],
        GlobalSecondaryIndexes=[{
            "IndexName": "by_owner",
            "KeySchema": [{"AttributeName": "owner", "KeyType": "HASH"},
                          {"AttributeName": "updated", "KeyType": "RANGE"}],
            "Projection": {"ProjectionType": "ALL"}}])


@pytest.fixture()
def env(monkeypatch):
    for k, v in {"AWS_ACCESS_KEY_ID": "testing", "AWS_SECRET_ACCESS_KEY": "testing",
                 "AWS_DEFAULT_REGION": "us-east-1", "AWS_REGION": "us-east-1",
                 "WORKFLOW_JSON": json.dumps(CONSOLE_WORKFLOW),
                 "STATUS_TABLE": "console_status", "EVENTS_TABLE": "console_events",
                 "RUNTIME_ARN": "arn:aws:bedrock-agentcore:us-east-1:1:runtime/console",
                 "BUILDS_TABLE": "console_builds", "BUILDS_BUCKET": "console-builds",
                 "DEPLOY_PROJECT": "agentexpress-deploy",
                 "SECRETS_PREFIX": "agentexpress/console/builds/",
                 "DEPLOY_ROLE_ARN": "arn:aws:iam::123456789012:role/console-deploy",
                 "BFF_ROLE_ARN": "arn:aws:iam::123456789012:role/console-bff"}.items():
        monkeypatch.setenv(k, v)
    with moto.mock_aws():
        ddb = boto3.client("dynamodb")
        create_builds_table(ddb)
        _status_table(ddb, "console_status")
        _events_table(ddb, "console_events")
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket="console-builds")
        s3.put_bucket_versioning(Bucket="console-builds",
                                 VersioningConfiguration={"Status": "Enabled"})
        for mod in MODULES:
            sys.modules.pop(mod, None)
        handler = importlib.import_module("handler")
        cb = FakeCodeBuild()
        monkeypatch.setattr(handler.builds, "_codebuild", cb)
        invoked: list[dict] = []
        monkeypatch.setattr(handler, "_self_invoke", lambda fn, p: invoked.append(p))
        yield types.SimpleNamespace(handler=handler, builds=handler.builds, cb=cb,
                                    invoked=invoked, ddb=ddb, s3=s3,
                                    table=boto3.resource("dynamodb").Table("console_builds"))
    for mod in MODULES:
        sys.modules.pop(mod, None)


def call(e, route_key, *, path=None, params=None, body=None, sub="u1",
         groups=("operators",), qs=None):
    method, template = route_key.split(" ", 1)
    claims = {"sub": sub, "email": f"{sub}@example.com", "cognito:groups": list(groups)}
    event = {"requestContext": {"http": {"method": method},
                                "authorizer": {"jwt": {"claims": claims}}},
             "rawPath": path or template.replace("{id}", (params or {}).get("id", "")),
             "routeKey": route_key, "pathParameters": params or {}}
    if body is not None:
        event["body"] = json.dumps(body)
    if qs:
        event["queryStringParameters"] = qs
    resp = e.handler.handler(event, CTX)
    return resp["statusCode"], json.loads(resp["body"])


def project(name="Claims", prompt="You triage claims.", **wf_over):
    # Valid as the Build view judges it: the deploy route refuses anything else.
    card = "https://partner.example.com/.well-known/agent-card.json"
    wf = {"agents": {"claims_intake": {"name": "Claims Intake", "runtime": "main",
                                       "maxTokens": 2000},
                     "partner": {"name": "Partner", "runtime": "a2a", "agentCard": card}},
          "steps": [{"agent": "claims_intake", "hitl": True}, {"agent": "partner"}],
          "tools": {}, "ui": {"defaultTopic": "claim 88213"}, **wf_over}
    return {"name": name, "workflow": wf,
            "prompts": {"claims_intake": {"systemPrompt": prompt, "schema": "{}"},
                        "partner": {"systemPrompt": "not mine", "schema": "{}"},
                        "gone": {"systemPrompt": "deleted agent", "schema": "{}"}}}


def save(e, bid="pclaims01", **kw):
    return call(e, "PUT /api/builds/{id}", params={"id": bid}, body={"project": project(**kw)})


def version_object(e, bid, n):
    return json.loads(e.s3.get_object(Bucket="console-builds",
                                      Key=f"builds/{bid}/versions/{n}.json")["Body"].read())


def mark_deployed(e, bid, version=1, tool="cdk"):
    """What the runner records on success (deployer/runner.py -> record_deployed)."""
    item = e.table.get_item(Key={"pk": f"BUILD#{bid}", "sk": "META"})["Item"]
    agent = item["agentName"]
    for suffix, make in (("_status", _status_table), ("_events", _events_table)):
        make(e.ddb, agent + suffix)
    e.builds.buildstore.record_deployed(e.table, bid, {
        "version": version, "tool": tool, "agentName": agent,
        "runtimeArn": f"arn:aws:bedrock-agentcore:us-east-1:1:runtime/{agent}-X",
        "statusTable": f"{agent}_status", "eventsTable": f"{agent}_events",
        "telemetryTable": f"{agent}_telemetry", "uiUrl": "", "apiUrl": ""})
    return agent


# --- storing -------------------------------------------------------------------

def test_a_build_is_stored_server_side_with_a_fixed_aws_name(env):
    status, build = save(env)
    assert status == 200
    assert env.builds.buildstore.AGENT_RE.match(build["agentName"])
    status, listed = call(env, "GET /api/builds")
    assert status == 200 and [b["name"] for b in listed] == ["Claims"]
    # Renaming changes the label, never the resource name.
    _, renamed = save(env, name="Claims triage")
    assert renamed["name"] == "Claims triage"
    assert renamed["agentName"] == build["agentName"]
    status, got = call(env, "GET /api/builds/{id}", params={"id": "pclaims01"})
    assert status == 200 and got["project"]["workflow"]["steps"][0]["agent"] == "claims_intake"


def test_a_build_is_private_to_its_owner(env):
    save(env)
    assert call(env, "GET /api/builds", sub="u2") == (200, [])
    assert call(env, "GET /api/builds/{id}", params={"id": "pclaims01"}, sub="u2")[0] == 404
    # Another user cannot overwrite it by reusing the id.
    status, _ = call(env, "PUT /api/builds/{id}", params={"id": "pclaims01"},
                     body={"project": project(name="hijack")}, sub="u2")
    assert status == 404
    assert call(env, "GET /api/builds")[1][0]["name"] == "Claims"


@pytest.mark.parametrize("name", ["", "x" * 65])
def test_a_build_name_is_one_to_sixty_four_characters(env, name):
    assert save(env, name=name)[0] == 400


def test_a_build_id_is_validated(env):
    status, _ = call(env, "PUT /api/builds/{id}", params={"id": "../../etc"},
                     body={"project": project()})
    assert status == 400


# --- deploying -------------------------------------------------------------------

def test_deploy_needs_the_deploy_permission(env):
    save(env)
    status, body = call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"},
                        body={"tool": "cdk"}, groups=("interns",))
    assert status == 403 and body["error"] == "not authorized to perform 'deploy'"
    assert env.cb.started == []


def test_deploy_freezes_an_immutable_version_and_starts_one_job(env):
    _, build = save(env)
    status, job = call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"},
                       body={"tool": "terraform"})
    assert status == 202 and job["version"] == 1 and job["status"] == "QUEUED"
    assert env.cb.env() == {"ACTION": "deploy", "TOOL": "terraform", "BUILD_ID": "pclaims01",
                            "VERSION": "1", "AGENT_NAME": build["agentName"],
                            "DELETE_AFTER": "0"}
    frozen = version_object(env, "pclaims01", 1)
    assert frozen["format"] == "agentexpress-bundle"
    # Prompts only for agents that exist and are not remote — what toBundle() sends.
    assert set(frozen["prompts"]) == {"claims_intake"}

    # A second click while it runs is refused rather than starting a second job.
    assert call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"},
                body={"tool": "terraform"})[0] == 409
    assert len(env.cb.started) == 1

    # Editing the build afterwards changes the draft, not the deployed version.
    save(env, prompt="edited later")
    assert version_object(env, "pclaims01", 1)["prompts"]["claims_intake"]["systemPrompt"] \
        == "You triage claims."


def test_a_deployed_build_cannot_switch_tools_without_a_destroy(env):
    save(env)
    call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"}, body={"tool": "cdk"})
    mark_deployed(env, "pclaims01", tool="cdk")
    status, body = call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"},
                        body={"tool": "terraform"})
    assert status == 409 and "deployed with cdk" in body["error"]
    # Same tool again is a new version.
    status, job = call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"},
                       body={"tool": "cdk"})
    assert status == 202 and job["version"] == 2


def test_destroy_uses_the_tool_the_build_was_deployed_with(env):
    save(env)
    call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"},
         body={"tool": "terraform"})
    mark_deployed(env, "pclaims01", tool="terraform")
    status, body = call(env, "POST /api/builds/{id}/destroy", params={"id": "pclaims01"},
                        groups=("interns",))
    assert status == 403 and "destroy" in body["error"]
    status, _ = call(env, "POST /api/builds/{id}/destroy", params={"id": "pclaims01"},
                       body={"tool": "cdk"})   # ignored: the recorded tool wins
    assert status == 202
    assert env.cb.env()["ACTION"] == "destroy" and env.cb.env()["TOOL"] == "terraform"
    assert env.cb.env()["VERSION"] == "1"


def test_destroying_a_build_with_nothing_deployed_is_refused(env):
    save(env)
    assert call(env, "POST /api/builds/{id}/destroy", params={"id": "pclaims01"})[0] == 409


def test_a_job_codebuild_ended_is_marked_failed_on_the_next_read(env):
    save(env)
    call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"}, body={"tool": "cdk"})
    env.cb.status["agentexpress-deploy:1"] = "FAILED"
    _, got = call(env, "GET /api/builds/{id}", params={"id": "pclaims01"})
    assert got["build"]["job"]["status"] == "FAILED"
    assert got["build"]["job"]["logsUrl"] == "https://logs/agentexpress-deploy:1"


# --- deleting --------------------------------------------------------------------

def test_deleting_a_never_deployed_build_removes_it_at_once(env):
    save(env)
    assert call(env, "DELETE /api/builds/{id}", params={"id": "pclaims01"}) == (200, {"ok": True})
    assert call(env, "GET /api/builds") == (200, [])
    assert "Contents" not in env.s3.list_objects_v2(Bucket="console-builds")


def test_deleting_a_deployed_build_destroys_its_stack_first(env):
    save(env)
    call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"}, body={"tool": "cdk"})
    mark_deployed(env, "pclaims01")
    assert call(env, "DELETE /api/builds/{id}", params={"id": "pclaims01"},
                groups=("interns",))[0] == 403
    status, body = call(env, "DELETE /api/builds/{id}", params={"id": "pclaims01"})
    assert status == 202 and body["destroying"] is True
    assert env.cb.env()["ACTION"] == "destroy" and env.cb.env()["DELETE_AFTER"] == "1"
    # Still listed until the destroy succeeds: a failed destroy must not orphan a stack.
    assert [b["name"] for b in call(env, "GET /api/builds")[1]] == ["Claims"]


# --- running a deployed build ----------------------------------------------------

def test_a_run_against_a_build_lives_in_that_builds_tables_with_a_snapshot(env, monkeypatch):
    save(env)
    call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"}, body={"tool": "cdk"})
    agent = mark_deployed(env, "pclaims01")

    status, body = call(env, "POST /api/sessions", body={"build": "pclaims01",
                                                         "subject_id": "acme"})
    assert status == 200
    sid = body["session_id"]
    item = boto3.resource("dynamodb").Table(f"{agent}_status").get_item(
        Key={"session_id": sid})["Item"]
    assert item["topic"] == "claim 88213"          # the BUILD's default topic
    assert set(item["nodes"]) == {"claims_intake", "partner"}
    assert item["subject_id"] == "acme"             # the spelling the console sends
    assert item["build"] == {"id": "pclaims01", "name": "Claims", "version": 1}
    # The snapshot is the browser projection of the version that ran, not the draft.
    assert item["workflow"]["steps"][0]["agent"] == "claims_intake"
    assert "prompts" not in item["workflow"]
    assert env.invoked[-1]["build"] == "pclaims01"

    # Every per-run call now finds it by id alone.
    status, got = call(env, "GET /api/sessions/{id}", params={"id": sid},
                       path=f"/api/sessions/{sid}")
    assert status == 200 and got["session_id"] == sid
    status, runs = call(env, "GET /api/sessions", qs={"build": "pclaims01"})
    assert [r["session_id"] for r in runs] == [sid]
    assert call(env, "GET /api/sessions")[1] == []   # not in the console's own list

    # Runner mode invokes the BUILD's runtime.
    seen = {}
    monkeypatch.setattr(env.handler.agentcore, "invoke_agent_runtime",
                        lambda **kw: seen.update(kw))
    env.handler.handler({"action": "start", "session_id": sid, "build": "pclaims01"}, CTX)
    assert seen["agentRuntimeArn"].endswith(f"runtime/{agent}-X")

    # Deleting the run removes it from the build's table and from the index.
    assert call(env, "DELETE /api/sessions/{id}", params={"id": sid},
                path=f"/api/sessions/{sid}")[0] == 200
    assert env.builds.build_of_run(sid) == ""


def test_a_run_of_the_consoles_own_workflow_also_carries_its_snapshot(env):
    status, body = call(env, "POST /api/sessions", body={"topic": "t"})
    assert status == 200
    item = boto3.resource("dynamodb").Table("console_status").get_item(
        Key={"session_id": body["session_id"]})["Item"]
    assert item["workflow"] == json.loads(json.dumps(env.handler.WORKFLOW))
    assert "build" not in item


def test_only_a_deployed_build_of_your_own_can_be_run(env):
    save(env)
    assert call(env, "POST /api/sessions", body={"build": "pclaims01"})[0] == 409
    call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"}, body={"tool": "cdk"})
    mark_deployed(env, "pclaims01")
    assert call(env, "POST /api/sessions", body={"build": "pclaims01"}, sub="u2")[0] == 404


def test_the_workflow_route_serves_a_builds_deployed_version(env):
    save(env)
    call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"}, body={"tool": "cdk"})
    mark_deployed(env, "pclaims01")
    save(env, prompt="x", steps=[{"agent": "claims_intake"}])   # a later, undeployed edit
    status, view = call(env, "GET /api/workflow", qs={"build": "pclaims01"})
    assert status == 200 and len(view["steps"]) == 2


def test_me_says_whether_the_builder_is_available(env):
    assert call(env, "GET /api/me")[1]["builder"] is True


# --- the deploy runner -----------------------------------------------------------

@pytest.fixture()
def runner(monkeypatch):
    sys.modules.pop("runner", None)
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "deployer"))
    try:
        mod = importlib.import_module("runner")
    finally:
        sys.path.pop(0)
    monkeypatch.setattr(mod, "WORK", mod.Path(pytest.importorskip("tempfile").mkdtemp()))
    yield mod
    sys.modules.pop("runner", None)


def runner_env(e, action="deploy", tool="cdk", delete_after="0"):
    item = e.table.get_item(Key={"pk": "BUILD#pclaims01", "sk": "META"})["Item"]
    return {"ACTION": action, "TOOL": tool, "BUILD_ID": "pclaims01", "VERSION": "1",
            "AGENT_NAME": item["agentName"], "BUILDS_TABLE": "console_builds",
            "BUILDS_BUCKET": "console-builds", "DELETE_AFTER": delete_after,
            "AWS_REGION": "us-east-1"}


def test_the_runner_applies_the_version_exactly_then_records_the_deploy(env, runner, monkeypatch):
    save(env)
    call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"}, body={"tool": "cdk"})
    commands = []
    monkeypatch.setattr(runner, "run", lambda cmd, cwd, env=None, keep=60: commands.append(cmd))
    monkeypatch.setattr(runner, "deploy_cdk", lambda action, agent, gw, *a: {
        "stack": runner.buildstore.stack_name(agent), "runtimeArn": "arn:rt", "uiUrl": "https://ui",
        "apiUrl": "https://api"})
    assert runner.main(runner_env(env), env.table, env.s3) == 0
    assert commands[0][1:] == ["scaffold.py", "apply", str(runner.WORK / "bundle.json"), "--exact"]
    build = call(env, "GET /api/builds/{id}", params={"id": "pclaims01"})[1]["build"]
    assert build["job"]["status"] == "SUCCEEDED"
    assert build["deployed"]["runtimeArn"] == "arn:rt" and build["deployed"]["version"] == 1
    assert build["deployed"]["statusTable"] == f"{build['agentName']}_status"
    # Each version says which framework it was frozen on, and which one deployed it.
    version_file = (Path(__file__).resolve().parent.parent / "VERSION").read_text().strip()
    assert version_object(env, "pclaims01", 1)["framework"] == {"version": version_file}
    assert build["deployed"]["frameworkVersion"] == version_file


def test_a_failed_deploy_is_recorded_with_the_tail_of_its_output(env, runner, monkeypatch):
    save(env)
    call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"}, body={"tool": "cdk"})
    monkeypatch.setattr(runner, "run", lambda *a, **k: None)

    def boom(*a):
        raise runner.StepFailed("`npx cdk deploy` failed (exit 1):\nResource limit exceeded")
    monkeypatch.setattr(runner, "deploy_cdk", boom)
    assert runner.main(runner_env(env), env.table, env.s3) == 1
    job = call(env, "GET /api/builds/{id}", params={"id": "pclaims01"})[1]["build"]["job"]
    assert job["status"] == "FAILED" and "Resource limit exceeded" in job["error"]
    assert "deployed" not in call(env, "GET /api/builds/{id}",
                                  params={"id": "pclaims01"})[1]["build"]


def test_a_destroy_with_delete_after_removes_every_trace(env, runner, monkeypatch):
    save(env)
    call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"}, body={"tool": "cdk"})
    mark_deployed(env, "pclaims01")
    sid = call(env, "POST /api/sessions", body={"build": "pclaims01"})[1]["session_id"]
    monkeypatch.setattr(runner, "run", lambda *a, **k: None)
    monkeypatch.setattr(runner, "deploy_cdk", lambda *a: {})
    assert runner.main(runner_env(env, "destroy", delete_after="1"), env.table, env.s3) == 0
    assert call(env, "GET /api/builds") == (200, [])
    assert env.builds.build_of_run(sid) == ""
    assert not env.s3.list_object_versions(Bucket="console-builds").get("Versions")


def test_a_plain_destroy_forgets_the_stack_and_the_tool(env, runner, monkeypatch):
    save(env)
    call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"},
         body={"tool": "terraform"})
    mark_deployed(env, "pclaims01", tool="terraform")
    monkeypatch.setattr(runner, "run", lambda *a, **k: None)
    monkeypatch.setattr(runner, "deploy_terraform", lambda *a: {})
    assert runner.main(runner_env(env, "destroy", "terraform"), env.table, env.s3) == 0
    build = call(env, "GET /api/builds/{id}", params={"id": "pclaims01"})[1]["build"]
    assert "deployed" not in build and "tool" not in build
    # ...so it may now be deployed with the other tool.
    assert call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"},
                body={"tool": "cdk"})[0] == 202


def test_the_runner_refuses_a_malformed_request(runner):
    assert runner.main({"ACTION": "deploy", "TOOL": "pulumi", "BUILD_ID": "b", "VERSION": "1",
                        "AGENT_NAME": "ax_12345678", "BUILDS_TABLE": "t",
                        "BUILDS_BUCKET": "b"}, object(), object()) == 2
    assert runner.main({"ACTION": "deploy", "TOOL": "cdk", "BUILD_ID": "b", "VERSION": "1",
                        "AGENT_NAME": "prod_stack", "BUILDS_TABLE": "t",
                        "BUILDS_BUCKET": "b"}, object(), object()) == 2


def test_build_stacks_get_no_builder_plane_of_their_own(runner):
    """A build is a workflow, not another console."""
    assert "builder=false" in runner.cdk_context("ax_12345678", False)
    assert runner.tf_vars("ax_12345678", True, "us-east-1")["enable_builder"] is False


def test_terraform_outputs_are_read_into_the_deploy_record(runner):
    raw = json.dumps({"agent_runtime_arn": {"value": "arn:rt"}, "ui_url": {"value": "https://u"},
                      "api_endpoint": {"value": "https://a"}})
    assert runner.tf_outputs(raw) == {"runtimeArn": "arn:rt", "uiUrl": "https://u",
                                      "apiUrl": "https://a", "userPoolId": ""}


def test_a_destroy_deletes_only_the_log_groups_carrying_the_builds_id(runner):
    class Logs:
        def __init__(self):
            self.deleted: list[str] = []

        def describe_log_groups(self, logGroupNamePattern, **kw):
            assert logGroupNamePattern == "27dc5302"
            return {"logGroups": [{"logGroupName": n} for n in (
                "/aws/bedrock-agentcore/runtimes/ax_27dc5302-bw03fLA26L-DEFAULT",
                "/aws/lambda/ax-27dc5302-stack-CustomS3AutoDeleteObjectsCustomR-914TQq",
                "ax-27dc5302-stack-UiDeployLogGroupAF2120AA-9xrpGJ",
                "/aws/lambda/someone-else-27dc5302",          # the hex alone is not enough
            )]}

        def delete_log_group(self, logGroupName):
            self.deleted.append(logGroupName)

    logs = Logs()
    runner.delete_leftover_logs("ax_27dc5302", logs)
    assert len(logs.deleted) == 3
    assert "/aws/lambda/someone-else-27dc5302" not in logs.deleted


# --- per-user privacy ---------------------------------------------------------------
# Anyone can sign up to a hosted console, so a run is private to the user who started it:
# a run id alone must never be enough to read, act on or delete someone else's run.

def test_a_run_is_invisible_and_untouchable_to_every_other_user(env, monkeypatch):
    sid = call(env, "POST /api/sessions", body={"topic": "mine"}, sub="alice")[1]["session_id"]
    assert [r["session_id"] for r in call(env, "GET /api/sessions", sub="alice")[1]] == [sid]
    assert call(env, "GET /api/sessions", sub="bob")[1] == []

    p = {"id": sid}
    for route, path, body in [
        ("GET /api/sessions/{id}", f"/api/sessions/{sid}", None),
        ("POST /api/sessions/{id}/decision", f"/api/sessions/{sid}/decision", {"decision": "approve"}),
        ("POST /api/sessions/{id}/rerun", f"/api/sessions/{sid}/rerun", {"agentId": "a"}),
        ("POST /api/sessions/{id}/evaluate", f"/api/sessions/{sid}/evaluate", {"agentId": "a"}),
        ("POST /api/sessions/{id}/cancel", f"/api/sessions/{sid}/cancel", None),
        ("GET /api/sessions/{id}/telemetry", f"/api/sessions/{sid}/telemetry", None),
        ("DELETE /api/sessions/{id}", f"/api/sessions/{sid}", None),
    ]:
        status, resp = call(env, route, params=p, path=path, body=body, sub="bob")
        # 404, not 403: whether the run exists is not bob's business either.
        assert status == 404 and resp["error"] == "unknown session", route
    assert env.invoked[-1]["action"] == "start"          # nothing reached the runtime
    # ...and it is all still there for its owner.
    assert call(env, "GET /api/sessions/{id}", params=p, path=f"/api/sessions/{sid}",
                sub="alice")[0] == 200


def test_the_cost_rollup_counts_only_the_callers_runs(env):
    ddb = boto3.client("dynamodb")
    ddb.create_table(
        TableName="console_telemetry", BillingMode="PAY_PER_REQUEST",
        KeySchema=[{"AttributeName": "session_id", "KeyType": "HASH"},
                   {"AttributeName": "sk", "KeyType": "RANGE"}],
        AttributeDefinitions=[{"AttributeName": a, "AttributeType": "S"}
                              for a in ("session_id", "sk", "date")],
        GlobalSecondaryIndexes=[{"IndexName": "by_date",
                                 "KeySchema": [{"AttributeName": "date", "KeyType": "HASH"},
                                               {"AttributeName": "sk", "KeyType": "RANGE"}],
                                 "Projection": {"ProjectionType": "ALL"}}])
    tel = boto3.resource("dynamodb").Table("console_telemetry")
    env.handler.telemetry_tbl = tel
    mine = call(env, "POST /api/sessions", body={"topic": "a"}, sub="alice")[1]["session_id"]
    theirs = call(env, "POST /api/sessions", body={"topic": "b"}, sub="bob")[1]["session_id"]
    today = env.handler.clock.today_str()
    for sid, cost in ((mine, 1), (theirs, 100)):
        tel.put_item(Item={"session_id": sid, "sk": "x", "date": today, "cost_usd": cost,
                           "kind": "llm"})
    _, agg = call(env, "GET /api/telemetry/aggregate", qs={"by": "date"}, sub="alice")
    assert agg["totalSessions"] == 1 and int(agg["buckets"][0]["costUsd"]) == 1


def test_the_assistant_cannot_reach_another_users_run(env, monkeypatch):
    chat = env.handler.chatbot
    sid = call(env, "POST /api/sessions", body={"topic": "secret"}, sub="alice")[1]["session_id"]
    monkeypatch.setattr(chat, "_status", boto3.resource("dynamodb").Table("console_status"))
    chat._OWNER = "bob"
    assert chat._owns(sid) is False
    assert chat._t_sessions({}, "", None)[0]["runs"] == []
    chat._OWNER = "alice"
    assert chat._owns(sid) is True
    assert [r["session_id"] for r in chat._t_sessions({}, "", None)[0]["runs"]] == [sid]


def test_insights_findings_are_behind_the_insights_permission(env):
    """Insights analyse every run in the window, whoever started it, so READING the
    findings needs the same permission as producing them."""
    status, resp = call(env, "GET /api/insights", groups=("interns",))
    assert status == 403 and resp["error"] == "not authorized to perform 'insights'"


# --- Terraform state: per user and build, gone after every destroy ------------------

def test_terraform_state_lives_under_the_owner_and_build(runner):
    assert runner.buildstore.state_prefix("alice-sub", "pclaims01") == "tfstate/alice-sub/pclaims01/"
    # An owner value can never climb out of its own prefix.
    assert runner.buildstore.state_prefix("../bob", "p1") == "tfstate/.._bob/p1/"


def test_a_plain_destroy_deletes_the_builds_terraform_state_and_nothing_else(env, runner, monkeypatch):
    save(env)
    call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"}, body={"tool": "terraform"})
    mark_deployed(env, "pclaims01", tool="terraform")
    mine = "tfstate/u1/pclaims01/terraform.tfstate"
    other = "tfstate/u2/pother001/terraform.tfstate"
    for key in (mine, other):
        env.s3.put_object(Bucket="console-builds", Key=key, Body=b"{}")
        env.s3.put_object(Bucket="console-builds", Key=key, Body=b"{v2}")   # a second version
    seen = {}
    monkeypatch.setattr(runner, "run", lambda *a, **k: None)
    monkeypatch.setattr(runner, "deploy_terraform",
                        lambda action, agent, gw, e, *a: seen.update(prefix=e["STATE_PREFIX"]) or {})
    assert runner.main(runner_env(env, "destroy", "terraform"), env.table, env.s3) == 0
    assert seen["prefix"] == "tfstate/u1/pclaims01/"
    keys = {v["Key"] for v in env.s3.list_object_versions(Bucket="console-builds").get("Versions", [])}
    assert mine not in keys                        # every version of it
    assert other in keys                           # another user's build is untouched


# --- secrets: write-only ------------------------------------------------------------

def test_a_builds_secrets_are_write_only_and_go_with_the_build(env):
    save(env)
    status, names = call(env, "PUT /api/builds/{id}/secrets", params={"id": "pclaims01"},
                         body={"toolApiKeys": {"billing": "sk-live-123"}, "a2aTokens": {"partner": "tok"}})
    assert status == 200 and names == {"toolApiKeys": ["billing"], "a2aTokens": ["partner"], "identitySecrets": []}
    status, got = call(env, "GET /api/builds/{id}/secrets", params={"id": "pclaims01"})
    assert got == names and "sk-live-123" not in json.dumps(got)   # names out, never values
    assert call(env, "GET /api/builds/{id}/secrets", params={"id": "pclaims01"}, sub="u2")[0] == 404
    # "" clears one.
    call(env, "PUT /api/builds/{id}/secrets", params={"id": "pclaims01"}, body={"toolApiKeys": {"billing": ""}})
    assert call(env, "GET /api/builds/{id}/secrets", params={"id": "pclaims01"})[1]["toolApiKeys"] == []
    call(env, "DELETE /api/builds/{id}", params={"id": "pclaims01"})
    sm = boto3.client("secretsmanager")
    with pytest.raises(sm.exceptions.ResourceNotFoundException):
        sm.get_secret_value(SecretId="agentexpress/console/builds/pclaims01")


def test_the_runner_hands_secrets_to_both_tools(env, runner):
    save(env)
    call(env, "PUT /api/builds/{id}/secrets", params={"id": "pclaims01"},
         body={"toolApiKeys": {"billing": "k"}})
    out = runner.read_secrets({"SECRETS_PREFIX": "agentexpress/console/builds/"}, "pclaims01")
    assert json.loads(out["TOOL_API_KEYS"]) == {"billing": "k"}
    assert out["TF_VAR_tool_api_keys"] == out["TOOL_API_KEYS"]
    assert "A2A_TOKENS" not in out


# --- knowledge-base documents ---------------------------------------------------------

def test_documents_are_uploaded_per_corpus_and_copied_into_kb_docs(env, runner, tmp_path, monkeypatch):
    save(env)
    status, up = call(env, "POST /api/builds/{id}/docs", params={"id": "pclaims01"},
                      body={"corpus": "claims", "name": "policy.pdf"})
    assert status == 200 and up["fields"]["key"] == "builds/pclaims01/kb/claims/policy.pdf"
    env.s3.put_object(Bucket="console-builds", Key=up["fields"]["key"], Body=b"%PDF-1.4")
    assert call(env, "GET /api/builds/{id}/docs", params={"id": "pclaims01"})[1][0]["corpus"] == "claims"
    for bad in ({"corpus": "../x", "name": "a.pdf"}, {"corpus": "claims", "name": "../a.pdf"},
                {"corpus": "claims", "name": "run.sh"}):
        assert call(env, "POST /api/builds/{id}/docs", params={"id": "pclaims01"}, body=bad)[0] == 400
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    # The source ships sample documents; a corpus the user uploaded to holds only theirs,
    # and a corpus they did not upload to keeps what the source has.
    for corpus in ("claims", "reference"):
        (tmp_path / "kb_docs" / corpus).mkdir(parents=True)
        (tmp_path / "kb_docs" / corpus / "sample.md").write_text("shipped sample")
    assert runner.sync_documents(env.s3, "console-builds", "pclaims01") == 1
    assert (tmp_path / "kb_docs" / "claims" / "policy.pdf").read_bytes() == b"%PDF-1.4"
    assert sorted(p.name for p in (tmp_path / "kb_docs" / "claims").iterdir()) == ["policy.pdf"]
    assert (tmp_path / "kb_docs" / "reference" / "sample.md").exists()
    call(env, "DELETE /api/builds/{id}/docs", params={"id": "pclaims01"},
         qs={"corpus": "claims", "name": "policy.pdf"})
    assert call(env, "GET /api/builds/{id}/docs", params={"id": "pclaims01"})[1] == []


def test_a_build_whose_corpus_has_no_documents_can_still_be_destroyed(runner, tmp_path, monkeypatch):
    """Observed live: a destroy failed in the synth's "corpus folder does not exist"
    check, so a build whose documents were never uploaded could not be deleted."""
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    (tmp_path / "kb_docs" / "reference").mkdir(parents=True)
    wf = {"tools": {"guide": {"type": "kb", "corpora": ["brand_style_guide", "reference"]},
                    "theirs": {"type": "kb", "corpora": ["x"], "knowledgeBaseId": "KB123"}}}
    assert runner.stand_in_corpora(wf) == ["brand_style_guide"]
    assert (tmp_path / "kb_docs" / "brand_style_guide").is_dir()
    assert not (tmp_path / "kb_docs" / "x").exists()       # their own KB: nothing to stand in for


# --- connected accounts ---------------------------------------------------------------

def test_connecting_an_account_gives_a_role_only_this_console_can_assume(env):
    status, c = call(env, "POST /api/accounts", body={"accountId": "123456789012", "region": "eu-west-1"})
    assert status == 200 and c["status"] == "pending"
    assert c["roleArn"].startswith("arn:aws:iam::123456789012:role/AgentExpressDeploy-")
    assert "quickcreate" in c["launchUrl"] and "eu-west-1" in c["launchUrl"]
    key = next(o["Key"] for o in env.s3.list_objects_v2(Bucket="console-builds", Prefix="connect/")["Contents"])
    tpl = json.loads(env.s3.get_object(Bucket="console-builds", Key=key)["Body"].read())
    trust = tpl["Resources"]["DeployRole"]["Properties"]["AssumeRolePolicyDocument"]["Statement"][0]
    assert trust["Principal"]["AWS"] == ["arn:aws:iam::123456789012:role/console-deploy",
                                         "arn:aws:iam::123456789012:role/console-bff"]
    external_id = trust["Condition"]["StringEquals"]["sts:ExternalId"]
    assert len(external_id) == 32 and key == f"connect/{external_id}.json"
    # Private: another user sees none of it.
    assert call(env, "GET /api/accounts", sub="u2")[1] == []
    assert call(env, "POST /api/accounts", body={"accountId": "12", "region": "eu-west-1"})[0] == 400


def test_a_connection_is_usable_only_once_verified(env):
    save(env)
    call(env, "POST /api/accounts", body={"accountId": "123456789012", "region": "us-east-1"})
    status, body = call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"},
                        body={"tool": "cdk", "account": "123456789012"})
    assert status == 409 and "not connected" in body["error"]
    status, v = call(env, "POST /api/accounts/{id}/verify", params={"id": "123456789012"})
    assert status == 200 and v["status"] == "connected"
    status, job = call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"},
                       body={"tool": "cdk", "account": "123456789012"})
    assert status == 202 and job["account"] == "123456789012"
    # The runner reads the account from the job it was started with, never an override.
    assert "TARGET_ROLE_ARN" not in env.cb.env()


def test_a_build_deployed_in_a_connected_account_stays_there_until_destroyed(env, runner, monkeypatch):
    save(env)
    call(env, "POST /api/accounts", body={"accountId": "123456789012", "region": "us-east-1"})
    call(env, "POST /api/accounts/{id}/verify", params={"id": "123456789012"})
    call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"},
         body={"tool": "cdk", "account": "123456789012"})
    env.cb.status["agentexpress-deploy:1"] = "SUCCEEDED"
    env.builds.buildstore.record_deployed(env.table, "pclaims01", {
        "version": 1, "tool": "cdk", "agentName": "ax_00000000", "account": "123456789012",
        "runtimeArn": "arn:rt", "statusTable": "t", "eventsTable": "e", "telemetryTable": "x"})
    status, body = call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"},
                        body={"tool": "cdk"})
    assert status == 409 and "123456789012" in body["error"]
    # It runs in its own app there, not through this console.
    assert call(env, "POST /api/sessions", body={"build": "pclaims01"})[0] == 409
    # Nor can the account be disconnected while it is there.
    assert call(env, "DELETE /api/accounts/{id}", params={"id": "123456789012"})[0] == 409
    # A destroy goes to the same account, taken from the build.
    call(env, "POST /api/builds/{id}/destroy", params={"id": "pclaims01"})
    item = env.table.get_item(Key={"pk": "BUILD#pclaims01", "sk": "META"})["Item"]
    assert item["job"]["account"] == "123456789012"
    target = runner.resolve_target(env.table, item, "u1", "pclaims01")
    assert target.account == "123456789012" and target.role_arn.endswith(
        env.table.get_item(Key={"pk": "USER#u1", "sk": "ACCOUNT#123456789012"})["Item"]["externalId"][:12])


def test_the_target_profile_assumes_the_role_with_the_external_id(runner, tmp_path, monkeypatch):
    monkeypatch.setattr(runner, "WORK", tmp_path)
    t = runner.Target("111122223333", "eu-west-1", "arn:aws:iam::111122223333:role/AgentExpressDeploy-abc",
                      "ext-123", "pclaims01")
    env = t.env("us-east-1")
    cfg = (tmp_path / "aws-config").read_text()
    assert "role_arn = arn:aws:iam::111122223333:role/AgentExpressDeploy-abc" in cfg
    assert "external_id = ext-123" in cfg and "credential_source = EcsContainer" in cfg
    assert env["AWS_PROFILE"] == "target" and env["CDK_DEFAULT_ACCOUNT"] == "111122223333"
    assert env["AWS_REGION"] == "eu-west-1"
    # Terraform's SDK (Go) refuses credential_source without a role_arn, so on that path
    # the console profile reads the container's credentials through credential_process.
    t.env("us-east-1", terraform=True)
    cfg = (tmp_path / "aws-config").read_text()
    assert "credential_source" not in cfg
    assert "credential_process = " in cfg and "deployer/container_creds.py" in cfg


def test_the_credential_process_prints_the_containers_credentials(monkeypatch, capsys):
    import io
    import urllib.request
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "deployer"))
    try:
        creds = importlib.import_module("container_creds")
    finally:
        sys.path.pop(0)
    seen = {}

    def fake_open(req, timeout):
        seen["url"] = req.full_url
        return io.StringIO(json.dumps({"AccessKeyId": "AK", "SecretAccessKey": "SK",
                                       "Token": "TK", "Expiration": "2030-01-01T00:00:00Z"}))
    monkeypatch.setattr(urllib.request, "urlopen", fake_open)
    monkeypatch.setenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/v2/credentials/abc")
    assert creds.main() == 0
    assert seen["url"] == "http://169.254.170.2/v2/credentials/abc"
    assert json.loads(capsys.readouterr().out) == {
        "Version": 1, "AccessKeyId": "AK", "SecretAccessKey": "SK", "SessionToken": "TK",
        "Expiration": "2030-01-01T00:00:00Z"}


def test_terraform_keeps_its_state_in_this_console_even_for_a_connected_account(runner, tmp_path, monkeypatch):
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setattr(runner, "WORK", tmp_path)
    (tmp_path / "terraform").mkdir()
    monkeypatch.setattr(runner, "ensure_terraform", lambda v: "terraform")
    monkeypatch.setattr(runner, "run", lambda *a, **k: None)
    monkeypatch.setattr(runner.subprocess, "run", lambda *a, **k: type("R", (), {"stdout": "{}"})())
    t = runner.Target("111122223333", "eu-west-1", "arn:r", "x", "p1")
    runner.deploy_terraform("deploy", "ax_12345678", False,
                            {"BUILDS_BUCKET": "b", "STATE_PREFIX": "tfstate/u1/p1/", "AWS_REGION": "us-east-1"},
                            t.env("us-east-1"), {"agentexpress:build": "p1"}, t)
    override = (tmp_path / "terraform" / "zz_builder_backend_override.tf").read_text()
    assert 'profile = "console"' in override and 'region = "us-east-1"' in override
    tfvars = json.loads((tmp_path / "terraform" / "builder.auto.tfvars.json").read_text())
    assert tfvars["region"] == "eu-west-1" and tfvars["tags"] == {"agentexpress:build": "p1"}


def test_every_build_resource_is_tagged_with_its_build_version_and_owner(runner):
    tags = runner.build_tags("pclaims01", 3, "sub-1", "me@example.com", "agentexpress")
    assert tags == {"agentexpress:build": "pclaims01", "agentexpress:version": "3",
                    "agentexpress:owner": "me@example.com", "agentexpress:console": "agentexpress"}
    flags = runner.cdk_context("ax_12345678", False, tags)
    assert json.loads(flags[flags.index("-c", len(flags) - 2) + 1][len("tags="):]) == tags


def test_the_owner_is_invited_into_the_builds_own_app(runner):
    calls = []

    class Idp:
        class exceptions:
            class UsernameExistsException(Exception):
                pass

        def admin_create_user(self, **kw):
            calls.append(("create", kw["Username"], kw["DesiredDeliveryMediums"]))

        def list_groups(self, **kw):
            return {"Groups": [{"GroupName": "approvers"}, {"GroupName": "operators"}]}

        def admin_add_user_to_group(self, **kw):
            calls.append(("group", kw["GroupName"]))

    session = types.SimpleNamespace(client=lambda name: Idp())
    assert runner.invite_owner(session, "us-east-1_X", "me@example.com") == "created"
    assert calls == [("create", "me@example.com", ["EMAIL"]), ("group", "approvers"), ("group", "operators")]
    assert runner.invite_owner(session, "", "me@example.com") == ""


# --- the assistant and Insights on a deployed build ------------------------------------

def test_the_assistant_works_on_a_deployed_builds_runs(env, monkeypatch):
    save(env)
    call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"}, body={"tool": "cdk"})
    agent = mark_deployed(env, "pclaims01")
    seen = {}

    def fake_chat(body, ctx, si, permitted=None, owner=None, target=None):
        seen["status"] = target["status"].name
        si({"action": "resume", "session_id": "s"})
        return {"reply": "ok", "actions": []}
    monkeypatch.setattr(env.handler.chatbot, "is_enabled", lambda: True)
    monkeypatch.setattr(env.handler.chatbot, "handle_chat", fake_chat)
    assert call(env, "POST /api/chat", body={"message": "hi", "build": "pclaims01"})[0] == 200
    assert seen["status"] == f"{agent}_status"
    assert env.invoked[-1]["build"] == "pclaims01"          # its actions go to its runtime
    assert call(env, "POST /api/chat", body={"message": "hi", "build": "pclaims01"}, sub="u2")[0] == 404


def test_insights_on_a_build_are_its_owners_without_the_console_wide_permission(env, monkeypatch):
    save(env)
    call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"}, body={"tool": "cdk"})
    agent = mark_deployed(env, "pclaims01")
    seen = {}
    monkeypatch.setattr(env.handler, "_insights_latest",
                        lambda arn=None: seen.update(arn=arn) or env.handler._resp(200, {}))
    assert call(env, "GET /api/insights", qs={"build": "pclaims01"}, groups=("interns",))[0] == 200
    assert seen["arn"].endswith(f"runtime/{agent}-X")
    assert call(env, "POST /api/insights/run", qs={"build": "pclaims01"}, groups=("interns",))[0] == 200
    assert env.invoked[-1]["build"] == "pclaims01"
    assert call(env, "GET /api/insights", qs={"build": "pclaims01"}, sub="u2")[0] == 404
    assert call(env, "GET /api/insights", groups=("interns",))[0] == 403    # the console's own


def test_a_failed_deploy_does_not_use_up_a_version_number(env, runner, monkeypatch):
    save(env)
    call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"}, body={"tool": "cdk"})
    env.builds.buildstore.record_failed(env.table, "pclaims01", "boom")
    save(env, prompt="fixed it")
    status, job = call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"}, body={"tool": "cdk"})
    assert status == 202 and job["version"] == 1           # the retry is still version 1...
    frozen = version_object(env, "pclaims01", 1)
    assert frozen["prompts"]["claims_intake"]["systemPrompt"] == "fixed it"   # ...of the new draft
    mark_deployed(env, "pclaims01", version=1)
    assert call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"},
                body={"tool": "cdk"})[1]["version"] == 2


def test_a_build_the_build_view_would_flag_is_refused_before_anything_starts(env):
    # Sent straight to the API, around the page: an agent in no stage, and a tool that
    # does not exist. The deploy says why, in the page's words, and starts nothing.
    save(env, steps=[{"agent": "claims_intake", "hitl": True}],
         agents={"claims_intake": {"name": "Claims Intake", "runtime": "main",
                                   "maxTokens": 2000, "tool": "nope"},
                 "partner": {"name": "Partner", "runtime": "a2a",
                             "agentCard": "https://partner.example.com/card.json"}})
    status, body = call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"},
                        body={"tool": "cdk"})
    assert status == 400
    assert body["error"].startswith("this build has 2 errors to fix before it can deploy")
    assert 'agents.claims_intake.tool: "nope" is not a tool' in body["error"]
    assert "agents.partner: not in any stage" in body["error"]
    assert env.cb.started == []
    assert "Contents" not in env.s3.list_objects_v2(Bucket="console-builds",
                                                   Prefix="builds/pclaims01/versions/")


def test_a_connected_accounts_role_can_deploy_a_build_but_is_not_an_administrator(env):
    import accounts
    call(env, "POST /api/accounts", body={"accountId": "123456789012", "region": "eu-west-1"})
    key = next(o["Key"] for o in env.s3.list_objects_v2(Bucket="console-builds", Prefix="connect/")["Contents"])
    tpl = json.loads(env.s3.get_object(Bucket="console-builds", Key=key)["Body"].read())
    role = tpl["Resources"]["DeployRole"]["Properties"]
    assert "arn:aws:iam::aws:policy/AdministratorAccess" not in json.dumps(tpl)
    assert role["ManagedPolicyArns"][0] == "arn:aws:iam::aws:policy/ReadOnlyAccess"
    policies = {r["Ref"] for r in role["ManagedPolicyArns"][1:]}
    docs = [tpl["Resources"][p]["Properties"]["PolicyDocument"] for p in sorted(policies)]
    assert all(tpl["Resources"][p]["Type"] == "AWS::IAM::ManagedPolicy" for p in policies)
    # IAM refuses a managed policy over 6,144 characters, and a role takes 10 of them.
    assert all(accounts._size(d["Statement"]) <= accounts.POLICY_LIMIT for d in docs)
    assert len(role["ManagedPolicyArns"]) <= 10
    sids = [st["Sid"] for d in docs for st in d["Statement"]]
    # Everything the Terraform path documents, less the console-only Builder plane...
    shipped = json.loads((Path(__file__).resolve().parent.parent / "terraform"
                          / "deploy-role-policy.json").read_text())["Statement"]
    assert [s["Sid"] for s in shipped if s["Sid"] != "BuilderPlane"] == sids[:len(shipped) - 1]
    # ...plus the CDK bootstrap hand-off, the owner's invite and leftover-log cleanup.
    for sid in ("CdkBootstrapStack", "CdkDeployThroughBootstrapRoles", "InviteOwner",
                "DeleteLeftoverBuildLogs"):
        assert sid in sids
    assert not any(st.get("Action") == "*" for d in docs for st in d["Statement"])


# --- the audit log ------------------------------------------------------------------

def activity(e, sub="u1", **qs):
    return call(e, "GET /api/audit", sub=sub, qs=qs or None, groups=qs.pop("groups", ("operators",)))


def test_the_audit_log_records_who_deployed_and_destroyed_what_where_and_when(env, runner, monkeypatch):
    save(env)
    call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"}, body={"tool": "cdk"})
    monkeypatch.setattr(runner, "run", lambda *a, **k: None)
    monkeypatch.setattr(runner, "deploy_cdk", lambda *a: {"uiUrl": "https://ui", "apiUrl": "x"})
    assert runner.main(runner_env(env), env.table, env.s3) == 0
    call(env, "POST /api/builds/{id}/destroy", params={"id": "pclaims01"})
    monkeypatch.setattr(runner, "deploy_cdk", lambda *a: (_ for _ in ()).throw(
        runner.StepFailed("`npx cdk destroy` failed (exit 1): stack is busy")))
    assert runner.main(runner_env(env, action="destroy"), env.table, env.s3) == 1

    status, events = activity(env)
    assert status == 200
    # Newest first; each names the build, the version, the tool and the account.
    assert [e["action"] for e in events] == [
        "destroy.failed", "destroy.requested", "deploy.succeeded", "deploy.requested",
        "build.created"]
    requested, succeeded = events[3], events[2]
    assert events[4]["detail"]["name"] == "Claims"
    assert requested["email"] == "u1@example.com" and requested["owner"] == "u1"
    assert requested["detail"] == {"build": "pclaims01", "name": "Claims",
                                   "agentName": requested["detail"]["agentName"],
                                   "version": 1, "tool": "cdk", "account": "console",
                                   "region": "us-east-1"}
    assert succeeded["detail"]["region"] == "us-east-1"
    assert succeeded["detail"]["uiUrl"] == "https://ui"
    assert "stack is busy" in events[0]["detail"]["error"]
    assert all(e["ts"].endswith("Z") for e in events)
    # Private, like builds and runs.
    assert activity(env, sub="u2")[1] == []


def test_everyones_activity_takes_the_audit_permission(env):
    save(env)
    call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"}, body={"tool": "cdk"})
    call(env, "POST /api/audit/logout", sub="u2")
    day = env.builds.buildstore.now()[:10]
    status, body = call(env, "GET /api/audit", qs={"scope": "all", "day": day})
    assert status == 403 and body["error"] == "not authorized to perform 'audit'"
    status, events = call(env, "GET /api/audit", qs={"scope": "all", "day": day},
                          groups=("auditors",))
    assert status == 200
    assert {(e["owner"], e["action"]) for e in events} == {("u1", "build.created"),
                                                           ("u1", "deploy.requested"),
                                                           ("u2", "logout")}
    assert call(env, "GET /api/audit", qs={"scope": "all", "day": "yesterday"},
                groups=("auditors",))[0] == 400


def test_everyones_activity_over_a_range_of_days_newest_first(env):
    import datetime as dt
    bs = env.builds.buildstore
    for day, who in (("2026-09-01", "u1"), ("2026-09-03", "u2"), ("2026-09-05", "u3")):
        for item in bs.audit_items(who, f"{who}@example.com", "login", {}, ts=f"{day}T10:00:00.000000Z"):
            env.table.put_item(Item=item)
    auditor = {"groups": ("auditors",)}
    status, events = call(env, "GET /api/audit",
                          qs={"scope": "all", "from": "2026-09-01", "to": "2026-09-04"}, **auditor)
    assert status == 200 and [e["owner"] for e in events] == ["u2", "u1"]
    # Only an auditor, the range in order, and at most 31 days.
    assert call(env, "GET /api/audit", qs={"scope": "all", "from": "2026-09-01", "to": "2026-09-04"})[0] == 403
    assert call(env, "GET /api/audit", qs={"scope": "all", "from": "2026-09-04", "to": "2026-09-01"},
                **auditor)[0] == 400
    far = (dt.date(2026, 9, 1) + dt.timedelta(days=31)).isoformat()
    assert call(env, "GET /api/audit", qs={"scope": "all", "from": "2026-09-01", "to": far},
                **auditor)[0] == 400


def test_the_audit_log_names_changed_secrets_never_their_values(env):
    save(env)
    call(env, "PUT /api/builds/{id}/secrets", params={"id": "pclaims01"},
         body={"tools": {"docs": "sk-live-XYZ"}})
    event = activity(env)[1][0]
    assert event["action"] == "secrets.updated"
    assert event["detail"]["changed"] == {"tools": ["docs"]}
    assert "sk-live-XYZ" not in json.dumps(event)


def test_an_audit_write_that_fails_never_fails_the_action(env, monkeypatch):
    save(env)

    class Broken:
        def put_item(self, **kw):
            raise RuntimeError("throttled")
    monkeypatch.setattr(env.builds, "_table", env.table)
    real = env.builds.buildstore.record_audit
    monkeypatch.setattr(env.builds.buildstore, "record_audit",
                        lambda _t, *a, **k: real(Broken(), *a, **k))
    status, job = call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"},
                       body={"tool": "cdk"})
    assert status == 202 and job["status"] == "QUEUED"


def test_the_sign_in_trigger_writes_the_same_items_as_the_bff(env, monkeypatch):
    monkeypatch.setenv("AUDIT_TABLE", "console_builds")
    monkeypatch.delenv("DEFAULT_GROUP", raising=False)
    spec = importlib.util.spec_from_file_location(
        "cognito_trigger", Path(__file__).resolve().parent.parent / "signup_lambda" / "handler.py")
    trigger = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(trigger)
    args = ("u1", "u1@example.com", "login", {"client": "abc", "empty": ""})
    assert trigger.audit_items(*args, ts="2026-09-09T10:00:00Z", rand="1234abcd") == \
        env.builds.buildstore.audit_items(*args, ts="2026-09-09T10:00:00Z", rand="1234abcd")
    event = {"triggerSource": "PostAuthentication_Authentication", "userName": "u1",
             "request": {"userAttributes": {"sub": "u1", "email": "u1@example.com"}},
             "callerContext": {"clientId": "spa-client"}}
    assert trigger.handler(event, None) is event
    # A sign-up confirmation is not a sign-in, and with no DEFAULT_GROUP adds no group.
    trigger.handler({"triggerSource": "PostConfirmation_ConfirmSignUp", "userName": "u1",
                     "userPoolId": "p"}, None)
    events = activity(env)[1]
    assert [(e["action"], e["detail"]) for e in events] == [("login", {"client": "spa-client"})]


def test_the_launch_link_brings_a_connected_accounts_role_up_to_date(env, monkeypatch):
    call(env, "POST /api/accounts", body={"accountId": "123456789012", "region": "us-east-1"})
    key = next(o["Key"] for o in env.s3.list_objects_v2(Bucket="console-builds", Prefix="connect/")["Contents"])
    env.s3.put_object(Bucket="console-builds", Key=key, Body=b'{"stale": true}')
    status, link = call(env, "GET /api/accounts/{id}/launch", params={"id": "123456789012"})
    assert status == 200 and "templateUrl" in link
    tpl = json.loads(env.s3.get_object(Bucket="console-builds", Key=key)["Body"].read())
    assert "DeployRole" in tpl["Resources"]


def test_connecting_a_connected_account_again_updates_its_role_and_stays_connected(env):
    call(env, "POST /api/accounts", body={"accountId": "123456789012", "region": "us-east-1"})
    call(env, "POST /api/accounts/{id}/verify", params={"id": "123456789012"})
    first = call(env, "GET /api/accounts")[1][0]
    status, again = call(env, "POST /api/accounts", body={"accountId": "123456789012", "region": "eu-west-1"})
    assert status == 200 and again["update"] is True
    assert again["stackName"] == first["stackName"] and again["roleArn"] == first["roleArn"]
    now = call(env, "GET /api/accounts")[1][0]
    assert now["status"] == "connected" and now["region"] == "us-east-1"


def test_a_terraform_redeploy_rebuilds_the_ui_its_fresh_checkout_does_not_have(runner, tmp_path, monkeypatch):
    # Otherwise the build step, already in state, is skipped; ui.tf finds no files and
    # the apply deletes the build's whole UI from its bucket.
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setattr(runner, "WORK", tmp_path)
    (tmp_path / "terraform").mkdir()
    monkeypatch.setattr(runner, "ensure_terraform", lambda v: "terraform")
    commands = []
    monkeypatch.setattr(runner, "run", lambda cmd, *a, **k: commands.append(cmd))

    def fake_run(cmd, **kw):
        out = "null_resource.ui_build\naws_s3_bucket.ui\n" if cmd[1:3] == ["state", "list"] else "{}"
        return type("R", (), {"stdout": out})()
    monkeypatch.setattr(runner.subprocess, "run", fake_run)
    env = {"BUILDS_BUCKET": "b", "STATE_PREFIX": "tfstate/u1/p1/", "AWS_REGION": "us-east-1"}
    runner.deploy_terraform("deploy", "ax_12345678", False, env, {}, {}, None)
    ui = next(c for c in commands if "-target=null_resource.ui_build" in c)
    assert "-replace=null_resource.ui_build" in ui
    # With the bundle already built (a local checkout), nothing is rebuilt needlessly.
    commands.clear()
    (tmp_path / "web" / "dist").mkdir(parents=True)
    (tmp_path / "web" / "dist" / "index.html").write_text("<html>")
    runner.deploy_terraform("deploy", "ax_12345678", False, env, {}, {}, None)
    ui = next(c for c in commands if "-target=null_resource.ui_build" in c)
    assert "-replace=null_resource.ui_build" not in ui


# --- region per deploy, and the owner's login ----------------------------------------

def _connected(env, account="123456789012", region="eu-west-1"):
    call(env, "POST /api/accounts", body={"accountId": account, "region": region})
    call(env, "POST /api/accounts/{id}/verify", params={"id": account})


def test_a_connected_account_takes_any_region_and_the_build_stays_there(env):
    save(env)
    _connected(env)
    status, job = call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"},
                       body={"tool": "cdk", "account": "123456789012", "region": "ap-southeast-2"})
    assert status == 202 and job["region"] == "ap-southeast-2"
    env.builds.buildstore.record_failed(env.table, "pclaims01", "boom")
    # Deployed (or part-deployed) there, it stays there until destroyed.
    status, body = call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"},
                        body={"tool": "cdk", "account": "123456789012", "region": "us-west-2"})
    assert status == 409 and "ap-southeast-2" in body["error"]
    # No region named: the connection's.
    save(env, bid="pother01")
    job = call(env, "POST /api/builds/{id}/deploy", params={"id": "pother01"},
               body={"tool": "cdk", "account": "123456789012"})[1]
    assert job["region"] == "eu-west-1"


def test_this_consoles_account_deploys_to_its_own_region_only(env):
    save(env)
    status, body = call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"},
                        body={"tool": "cdk", "region": "us-west-2"})
    assert status == 400 and "connect an AWS account" in body["error"]
    assert call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"},
                body={"tool": "cdk"})[1]["region"] == "us-east-1"


def test_a_region_that_cannot_run_a_tool_is_refused_before_the_deploy(env):
    save(env, tools={"search": {"type": "websearch"}},
         agents={"claims_intake": {"name": "Claims Intake", "runtime": "main", "maxTokens": 2000,
                                   "tool": "search"},
                 "partner": {"name": "Partner", "runtime": "a2a",
                             "agentCard": "https://partner.example.com/card.json"}})
    _connected(env)
    status, body = call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"},
                        body={"tool": "cdk", "account": "123456789012", "region": "us-west-2"})
    assert status == 400 and "web search (search) is only available in" in body["error"]
    assert env.cb.started == []


def test_the_runner_deploys_to_the_region_the_job_names(env, runner):
    save(env)
    _connected(env)
    call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"},
         body={"tool": "terraform", "account": "123456789012", "region": "ap-southeast-2"})
    meta = env.table.get_item(Key={"pk": "BUILD#pclaims01", "sk": "META"})["Item"]
    target = runner.resolve_target(env.table, meta, meta["owner"], "pclaims01")
    assert (target.account, target.region) == ("123456789012", "ap-southeast-2")


def test_a_region_without_agentcore_fails_the_deploy_with_the_reason(runner):
    class Session:
        def client(self, name, region_name=None):
            class C:
                def list_agent_runtimes(self, **_):
                    raise RuntimeError("EndpointConnectionError: Could not connect to the endpoint URL")
            return C()
    with pytest.raises(runner.StepFailed, match="AgentCore is not available in sa-east-1"):
        runner.agentcore_available(Session(), "sa-east-1")


def test_the_owner_sees_their_builds_temporary_password_and_nobody_else_does(env, runner, monkeypatch):
    save(env)
    call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"}, body={"tool": "cdk"})
    monkeypatch.setattr(runner, "run", lambda *a, **k: None)
    monkeypatch.setattr(runner, "deploy_cdk", lambda *a: {"uiUrl": "https://ui", "userPoolId": "p"})
    seen = {}
    monkeypatch.setattr(runner, "invite_owner",
                        lambda s, pool, email, pw="": seen.update(pw=pw, email=email) or "created")
    renv = {**runner_env(env), "SECRETS_PREFIX": "agentexpress/console/builds/"}
    assert runner.main(renv, env.table, env.s3) == 0
    pw = seen["pw"]
    assert len(pw) == 16 and any(c.isupper() for c in pw) and any(c.isdigit() for c in pw)
    status, login = call(env, "GET /api/builds/{id}/login", params={"id": "pclaims01"})
    assert status == 200 and login["password"] == pw and login["temporary"] is True
    assert login["user"] == seen["email"]
    assert call(env, "GET /api/builds/{id}/login", params={"id": "pclaims01"}, sub="u2")[0] == 404
    assert activity(env)[1][0]["action"] == "login.viewed"
    # A destroy takes the login with the console it opened.
    call(env, "POST /api/builds/{id}/destroy", params={"id": "pclaims01"})
    assert runner.main({**runner_env(env, action="destroy"), "SECRETS_PREFIX": "agentexpress/console/builds/"},
                       env.table, env.s3) == 0
    assert call(env, "GET /api/builds/{id}/login", params={"id": "pclaims01"})[0] == 404


def test_a_connection_has_a_name_a_default_region_and_lists_the_builds_deployed_there(env):
    save(env)
    status, c = call(env, "POST /api/accounts", body={"accountId": "123456789012",
                                                      "region": "eu-west-1", "label": "  Team   sandbox "})
    assert status == 200 and c["label"] == "Team sandbox"
    call(env, "POST /api/accounts/{id}/verify", params={"id": "123456789012"})
    call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"},
         body={"tool": "cdk", "account": "123456789012", "region": "us-west-2"})
    a = call(env, "GET /api/accounts")[1][0]
    assert a["label"] == "Team sandbox" and a["region"] == "eu-west-1"
    assert a["builds"] == [{"id": "pclaims01", "name": a["builds"][0]["name"], "region": "us-west-2"}]
    # Renamed and re-defaulted; the deployed build keeps the region it is in.
    status, u = call(env, "PUT /api/accounts/{id}", params={"id": "123456789012"},
                     body={"label": "Prod", "region": "ap-southeast-2"})
    assert status == 200 and (u["label"], u["region"]) == ("Prod", "ap-southeast-2") and u["updatedAt"]
    a = call(env, "GET /api/accounts")[1][0]
    assert a["status"] == "connected" and a["builds"][0]["region"] == "us-west-2"
    assert call(env, "PUT /api/accounts/{id}", params={"id": "123456789012"},
                body={"region": "mars"})[0] == 400
    assert call(env, "PUT /api/accounts/{id}", params={"id": "123456789012"}, body={})[0] == 400
    assert call(env, "PUT /api/accounts/{id}", params={"id": "123456789012"},
                body={"label": "x" * 65})[0] == 400
    # Another user's connection is not theirs to rename.
    assert call(env, "PUT /api/accounts/{id}", params={"id": "123456789012"},
                body={"label": "mine"}, sub="u2")[0] == 404
    acts = [e["action"] for e in call(env, "GET /api/audit")[1]]
    assert "account.updated" in acts


def test_the_audit_log_records_creating_designing_and_every_run_action(env, monkeypatch):
    save(env)
    save(env)                                       # autosave again: created only once
    monkeypatch.setattr(env.handler, "_self_invoke", lambda *a, **k: None)
    call(env, "POST /api/builds/{id}/design", params={"id": "pclaims01"},
         body={"message": "Add   a fraud\ncheck " + "x" * 300})
    call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"}, body={"tool": "cdk"})
    mark_deployed(env, "pclaims01")
    sid = call(env, "POST /api/sessions", body={"build": "pclaims01"})[1]["session_id"]
    at = {"params": {"id": sid}}
    call(env, "POST /api/sessions/{id}/decision", path=f"/api/sessions/{sid}/decision",
         body={"decision": "revise", "comment": "tighten it"}, **at)
    call(env, "POST /api/sessions/{id}/rerun", path=f"/api/sessions/{sid}/rerun",
         body={"agentId": "partner", "comment": "again"}, **at)
    call(env, "POST /api/sessions/{id}/cancel", path=f"/api/sessions/{sid}/cancel", **at)
    call(env, "DELETE /api/sessions/{id}", path=f"/api/sessions/{sid}", **at)
    events = activity(env)[1]
    acts = [e["action"] for e in events]
    assert acts == ["run.deleted", "run.cancelled", "run.rerun", "run.decided", "run.started",
                    "deploy.requested", "design.message", "build.created"]
    by = {e["action"]: e["detail"] for e in events}
    assert by["design.message"]["message"].startswith("Add a fraud check x")
    assert len(by["design.message"]["message"]) == 200 and by["design.message"]["name"] == "Claims"
    assert by["run.started"]["session"] == sid and by["run.started"]["build"] == "pclaims01"
    assert by["run.decided"]["decision"] == "revise" and by["run.decided"]["comment"] == "tighten it"
    assert by["run.rerun"]["agents"] == ["partner"]
    assert all(e["owner"] == "u1" for e in events)


def test_a_run_takes_a_long_detailed_request_but_not_an_unbounded_one(env):
    request = "Triage claim 88213.\n\nInstructions:\n" + "- check the policy wording\n" * 300
    status, body = call(env, "POST /api/sessions", body={"topic": request})
    assert status == 200
    item = boto3.resource("dynamodb").Table("console_status").get_item(
        Key={"session_id": body["session_id"]})["Item"]
    assert item["topic"] == request.strip()                 # kept whole, line breaks and all
    assert env.invoked[-1]["topic"] == request.strip()
    status, body = call(env, "POST /api/sessions", body={"topic": "x" * 10001})
    assert status == 400 and "10000" in body["error"]


# --- admins -------------------------------------------------------------------------

def test_an_admin_reads_every_users_builds_and_runs_but_changes_nothing(env):
    save(env)                                               # u1's build
    call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"}, body={"tool": "cdk"})
    mark_deployed(env, "pclaims01")
    sid = call(env, "POST /api/sessions", body={"build": "pclaims01"})[1]["session_id"]
    own = call(env, "POST /api/sessions", body={"topic": "u1 console run"})[1]["session_id"]
    boss = {"sub": "boss", "groups": ("admins",)}
    # Not an admin: nothing of anyone else's, and no scope=all.
    assert call(env, "GET /api/builds", qs={"scope": "all"}, sub="u2")[0] == 403
    assert call(env, "GET /api/builds/{id}", params={"id": "pclaims01"}, sub="u2")[0] == 404
    assert call(env, "GET /api/sessions/{id}", params={"id": own}, path=f"/api/sessions/{own}",
                sub="u2")[0] == 404
    status, runs = call(env, "GET /api/sessions", qs={"scope": "all"}, sub="u2")
    assert status == 200 and runs == []                     # ?scope=all is ignored for them
    # An admin sees every build, with its owner, and can open it, its chat and its log.
    status, every = call(env, "GET /api/builds", qs={"scope": "all"}, **boss)
    assert status == 200 and [(b["id"], b["owner"], b["ownerEmail"]) for b in every] == [
        ("pclaims01", "u1", "u1@example.com")]
    assert call(env, "GET /api/builds", **boss)[1] == []    # their OWN list is still theirs
    assert call(env, "GET /api/builds/{id}", params={"id": "pclaims01"}, **boss)[1]["project"]["name"] == "Claims"
    assert call(env, "GET /api/builds/{id}/design", params={"id": "pclaims01"}, **boss)[0] == 200
    # ...and every run, the console's and the build's, read-only.
    status, runs = call(env, "GET /api/sessions", qs={"scope": "all"}, **boss)
    assert status == 200 and {r["session_id"]: r["owner"] for r in runs} == {own: "u1"}
    status, runs = call(env, "GET /api/sessions", qs={"build": "pclaims01", "scope": "all"}, **boss)
    assert [r["session_id"] for r in runs] == [sid]
    assert call(env, "GET /api/sessions/{id}", params={"id": sid}, path=f"/api/sessions/{sid}",
                **boss)[1]["session_id"] == sid
    # Read-only: no saving, deploying, chatting, deciding, re-running, cancelling,
    # deleting, and no reading the owner's app password.
    assert call(env, "PUT /api/builds/{id}", params={"id": "pclaims01"},
                body={"project": project()}, **boss)[0] == 404
    assert call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"},
                body={"tool": "cdk"}, sub="boss", groups=("admins", "operators"))[0] == 404
    assert call(env, "POST /api/builds/{id}/design", params={"id": "pclaims01"},
                body={"message": "hi"}, **boss)[0] == 404
    assert call(env, "GET /api/builds/{id}/login", params={"id": "pclaims01"}, **boss)[0] == 404
    for action in ("decision", "rerun", "cancel"):
        assert call(env, f"POST /api/sessions/{{id}}/{action}", params={"id": sid},
                    path=f"/api/sessions/{sid}/{action}", body={"decision": "approve", "agentId": "a"},
                    sub="boss", groups=("admins", "operators", "approvers"))[0] == 404
    assert call(env, "DELETE /api/sessions/{id}", params={"id": sid}, path=f"/api/sessions/{sid}",
                sub="boss", groups=("admins", "operators"))[0] == 404
    assert call(env, "DELETE /api/builds/{id}", params={"id": "pclaims01"},
                sub="boss", groups=("admins", "operators"))[0] == 404
    # Every look is in the admin's log — once, however often the page polls.
    call(env, "GET /api/builds/{id}", params={"id": "pclaims01"}, **boss)
    viewed = [e["detail"] for e in call(env, "GET /api/audit", **boss)[1] if e["action"] == "admin.viewed"]
    assert sorted((d["kind"], d.get("what", "")) for d in viewed) == [
        ("build", "build"), ("build", "design"), ("session", "")]
    assert all(d["forUser"] == "u1" for d in viewed)


def test_an_admin_can_destroy_anyones_build_and_the_owner_is_told(env):
    save(env)
    call(env, "POST /api/builds/{id}/deploy", params={"id": "pclaims01"}, body={"tool": "cdk"})
    mark_deployed(env, "pclaims01")
    # Destroy still takes the destroy permission as well.
    assert call(env, "POST /api/builds/{id}/destroy", params={"id": "pclaims01"},
                sub="boss", groups=("admins",))[0] == 403
    assert call(env, "POST /api/builds/{id}/destroy", params={"id": "pclaims01"},
                sub="u2", groups=("operators",))[0] == 404
    status, job = call(env, "POST /api/builds/{id}/destroy", params={"id": "pclaims01"},
                       sub="boss", groups=("admins", "operators"))
    assert status == 202 and job["action"] == "destroy"
    mine = call(env, "GET /api/audit", sub="boss", groups=("admins",))[1][0]
    assert mine["action"] == "destroy.requested" and mine["detail"]["forUser"] == "u1"
    assert mine["detail"]["asAdmin"] is True
    theirs = call(env, "GET /api/audit")[1][0]
    assert theirs["action"] == "destroy.requested" and theirs["detail"]["byAdmin"] == "boss@example.com"


def test_admin_and_audit_are_closed_unless_granted_and_insights_whenever_rbac_is_on(env, monkeypatch):
    authz = env.handler.authz
    ev = lambda *g: {"requestContext": {"authorizer": {"jwt": {"claims": {  # noqa: E731
        "sub": "x", "cognito:groups": list(g)}}}}}
    assert not authz.permitted("admin", ev("operators", "auditors"))
    assert authz.permitted("admin", ev("admins"))
    monkeypatch.setattr(authz, "_RULES", {"deploy": ["operators"]})
    assert not authz.permitted("insights", ev("operators"))  # RBAC on, insights not granted
    assert not authz.permitted("admin", ev("admins"))
    monkeypatch.setattr(authz, "_RULES", {})
    monkeypatch.setattr(authz, "ENABLED", False)
    assert authz.permitted("insights", ev())                 # single-tenant: open as before
    assert not authz.permitted("admin", ev()) and not authz.permitted("audit", ev())


def test_a_failed_first_create_is_deleted_before_the_retry_and_nothing_else_is(runner, monkeypatch):
    """Live: a create that failed left ROLLBACK_FAILED (a Memory still CREATING when
    rolled back), and `cdk deploy` refuses that. Only first-create failures are cleared."""
    class Cfn:
        def __init__(self, states):
            self.states, self.deleted = list(states), 0

        def describe_stacks(self, StackName):
            if not self.states:
                from botocore.exceptions import ClientError
                raise ClientError({"Error": {"Code": "ValidationError",
                                             "Message": f"Stack with id {StackName} does not exist"}}, "Describe")
            return {"Stacks": [{"StackStatus": self.states[0]}]}

        def delete_stack(self, StackName):
            self.deleted += 1

        def get_waiter(self, _):
            cfn = self

            class W:
                def wait(self, **_):
                    cfn.states.pop(0)
                    if cfn.states and cfn.states[0] == "DELETE_FAILED":
                        raise RuntimeError("still settling")
            return W()

    class Sts:
        def get_caller_identity(self):
            return {"Account": "123456789012"}

        def assume_role(self, RoleArn, **_):
            assert RoleArn == "arn:aws:iam::123456789012:role/cdk-hnb659fds-deploy-role-123456789012-us-east-1"
            return {"Credentials": {"AccessKeyId": "a", "SecretAccessKey": "b", "SessionToken": "c"}}

    session = types.SimpleNamespace(client=lambda _: Sts())
    for states, deletes in ((["ROLLBACK_FAILED"], 1), (["ROLLBACK_FAILED", "DELETE_FAILED"], 2),
                            (["CREATE_COMPLETE"], 0), (["UPDATE_ROLLBACK_FAILED"], 0), ([], 0)):
        cfn = Cfn(states)
        monkeypatch.setattr(boto3, "client", lambda *a, _c=cfn, **k: _c)
        runner.clear_failed_create(session, "ax-1-stack", "us-east-1", pause=0)
        assert cfn.deleted == deletes, states
