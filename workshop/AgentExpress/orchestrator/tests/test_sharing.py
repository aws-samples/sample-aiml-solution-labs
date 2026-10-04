"""Sharing builds and library items, admin-defined groups, and builds that use library
items live (bff/sharing.py, bff/library.py).

What has to hold:
  * nothing is visible to anyone else until its owner shares it: another user gets 404;
  * a share (email, group or everyone) lets that person do everything the owner can —
    list, open, save, deploy, share on, delete — and the owner stays the owner;
  * unsharing, or leaving a group, takes access away again;
  * only an admin defines groups; anyone may pick one by name;
  * two people editing one build cannot silently overwrite each other (409);
  * a build using a library item sees it as it is NOW, and deploys that snapshot; an
    item that is gone or no longer shared stops the deploy with a reason.
"""
from __future__ import annotations

import pytest

moto = pytest.importorskip("moto")
from test_builds import call, project, save, version_object  # noqa: E402
from test_builds import env as env  # noqa: E402

B = "pshare001"
TOOL = {"type": "mcp", "description": "Docs server.", "endpoint": "https://docs.example.com/mcp"}


def _share(e, bid=B, sub="u1", **shares):
    return call(e, "PUT /api/builds/{id}/shares", params={"id": bid}, sub=sub,
                body={"emails": [], "groups": [], "everyone": False, **shares})


def _get(e, sub, bid=B):
    return call(e, "GET /api/builds/{id}", params={"id": bid}, sub=sub)


def test_a_build_shared_by_email_can_be_co_built_and_the_owner_stays_the_owner(env):
    assert save(env, B)[0] == 200
    assert _get(env, "u2")[0] == 404
    assert _share(env, emails=["U2@example.com"])[0] == 200
    status, got = _get(env, "u2")
    assert status == 200 and got["build"]["shared"] is True
    assert [b["id"] for b in call(env, "GET /api/builds", sub="u2")[1]] == [B]
    assert all("shared" not in b for b in call(env, "GET /api/builds", sub="u1")[1])
    # u2 edits and deploys; the build is still u1's, and invites u1 into its app.
    p = project(name="Claims v2")
    assert call(env, "PUT /api/builds/{id}", params={"id": B}, sub="u2", body={"project": p})[0] == 200
    meta = env.table.get_item(Key={"pk": f"BUILD#{B}", "sk": "META"})["Item"]
    assert meta["owner"] == "u1" and meta["ownerEmail"] == "u1@example.com" and meta["lastEditor"] == "u2@example.com"
    assert call(env, "POST /api/builds/{id}/deploy", params={"id": B}, sub="u2", body={"tool": "cdk"})[0] == 202
    # Someone else still sees nothing.
    assert _get(env, "u3")[0] == 404
    assert call(env, "GET /api/builds", sub="u3")[1] == []
    # Unshare: gone again.
    assert _share(env, sub="u2")[0] == 200
    assert _get(env, "u2")[0] == 404 and call(env, "GET /api/builds", sub="u2")[1] == []


def test_groups_are_an_admins_and_a_group_share_follows_its_members(env):
    assert save(env, B)[0] == 200
    team = {"id": "claims team"}
    assert call(env, "PUT /api/groups/{id}", params=team, body={"members": ["u3@example.com"]})[0] == 403
    status, g = call(env, "PUT /api/groups/{id}", params={"id": "claims team"}, groups=("admins",),
                     body={"members": ["U3@example.com", "u4@example.com"]})
    assert status == 200 and g["members"] == ["u3@example.com", "u4@example.com"]
    assert call(env, "GET /api/groups")[1] == [{"name": "claims team"}]
    assert call(env, "GET /api/groups", groups=("admins",))[1][0]["members"] == ["u3@example.com", "u4@example.com"]
    assert _share(env, groups=["nope"])[0] == 400
    assert _share(env, groups=["claims team"])[0] == 200
    assert _get(env, "u3")[0] == 200 and _get(env, "u4")[0] == 200 and _get(env, "u5")[0] == 404
    # u4 leaves the group: no access any more.
    call(env, "PUT /api/groups/{id}", params=team, groups=("admins",), body={"members": ["u3@example.com"]})
    assert _get(env, "u4")[0] == 404 and _get(env, "u3")[0] == 200
    assert call(env, "DELETE /api/groups/{id}", params={"id": "claims team"}, groups=("admins",))[0] == 200
    assert _get(env, "u3")[0] == 404


def test_everyone_means_everyone_who_signs_in(env):
    assert save(env, B)[0] == 200
    assert _share(env, everyone=True)[0] == 200
    assert _get(env, "anyone")[0] == 200
    assert call(env, "DELETE /api/builds/{id}", params={"id": B}, sub="anyone")[0] == 200
    assert _get(env, "u1")[0] == 404          # deleted, for everyone
    assert call(env, "GET /api/builds", sub="u9")[1] == []


def test_two_editors_cannot_silently_overwrite_each_other(env):
    assert save(env, B)[0] == 200
    rev = env.table.get_item(Key={"pk": f"BUILD#{B}", "sk": "META"})["Item"]["rev"]
    _share(env, emails=["u2@example.com"])
    p = {**project(name="From u2"), "rev": int(rev)}
    assert call(env, "PUT /api/builds/{id}", params={"id": B}, sub="u2", body={"project": p})[0] == 200
    stale = {**project(name="From u1, stale"), "rev": int(rev)}
    status, err = call(env, "PUT /api/builds/{id}", params={"id": B}, body={"project": stale})
    assert status == 409 and "u2@example.com" in err["error"]
    assert _get(env, "u1")[1]["project"]["name"] == "From u2"


# --- the library, and builds that use it live --------------------------------------------

def _item(e, sub="u1", **body):
    return call(e, "POST /api/library", sub=sub, body={"kind": "tool", "name": "docs", "definition": TOOL, **body})


def test_a_library_item_is_private_until_shared(env):
    status, it = _item(env)
    assert status == 200 and it["mine"] is True
    assert _item(env)[0] == 409                        # same kind and name
    assert call(env, "GET /api/library", sub="u2")[1] == []
    assert call(env, "GET /api/library/{id}", params={"id": it["id"]}, sub="u2")[0] == 404
    assert call(env, "PUT /api/library/{id}/shares", params={"id": it["id"]},
                body={"emails": ["u2@example.com"]})[0] == 200
    got = call(env, "GET /api/library", sub="u2", qs={"kind": "tool"})[1]
    assert [(i["name"], i["mine"]) for i in got] == [("docs", False)]
    # Shared means co-owned: u2 may change it.
    status, changed = call(env, "PUT /api/library/{id}", params={"id": it["id"]}, sub="u2",
                           body={"definition": {**TOOL, "description": "Changed by u2."}})
    assert status == 200 and changed["definition"]["description"] == "Changed by u2."


@pytest.mark.parametrize("kind, definition, bad", [
    ("guardrail", {"contentFilters": {"HATE": "HIGH"}}, {"contentFilters": {"HATE": "LOUD"}}),
    ("memory", {"strategies": ["semantic"], "expiryDays": 30}, {"strategies": ["telepathy"]}),
    ("evaluator", {"instructions": "Score it."}, {"scale": 3}),
    ("identity", {"type": "apikey"}, {"type": "saml"}),
    ("policy", {"statement": 'forbid(principal, action in AgentCore::Action::"{{tool}}", '
                             'resource == AgentCore::Gateway::"{{gateway}}");'}, {"statement": "allow all"}),
])
def test_every_kind_is_checked_like_the_build_would_check_it(env, kind, definition, bad):
    assert call(env, "POST /api/library", body={"kind": kind, "name": "x", "definition": definition})[0] == 200
    assert call(env, "POST /api/library", body={"kind": kind, "name": "y", "definition": bad})[0] == 400
    assert call(env, "POST /api/library", body={"kind": "nope", "name": "z", "definition": definition})[0] == 400


def test_numbers_in_an_item_come_back_as_the_numbers_they_were(env):
    """Live: a memory's expiryDays came back from DynamoDB as Decimal and the build's
    validator refused it as "not a whole number"."""
    d = {"strategies": ["semantic"], "expiryDays": 45}
    _, it = call(env, "POST /api/library", body={"kind": "memory", "name": "m", "definition": d})
    ev = {"instructions": "x", "scale": [{"value": 0.5, "label": "Half", "definition": "h"},
                                          {"value": 1, "label": "Full", "definition": "f"}]}
    assert call(env, "POST /api/library", body={"kind": "evaluator", "name": "e", "definition": ev})[0] == 200
    got = {i["kind"]: i["definition"] for i in call(env, "GET /api/library")[1]}
    assert got["memory"]["expiryDays"] == 45 and type(got["memory"]["expiryDays"]) is int
    assert got["evaluator"]["scale"][0]["value"] == 0.5
    wf = project()["workflow"]
    wf["memories"] = {"m": {"library": it["id"]}}
    wf["agents"]["claims_intake"]["agentcore"] = {"memory": {"use": "m"}}
    call(env, "PUT /api/builds/{id}", params={"id": B}, body={"project": {**project(), "workflow": wf}})
    assert call(env, "POST /api/builds/{id}/deploy", params={"id": B}, body={"tool": "cdk"})[0] == 202


def test_a_build_uses_a_library_item_live_and_deploys_it_as_it_is_now(env):
    _, it = _item(env)
    wf = project()["workflow"]
    wf["tools"] = {"docs": {"library": it["id"]}}
    wf["agents"]["claims_intake"]["tool"] = "docs"
    body = {"project": {**project(), "workflow": wf}}
    assert call(env, "PUT /api/builds/{id}", params={"id": B}, body=body)[0] == 200
    got = _get(env, "u1")[1]
    assert got["project"]["workflow"]["tools"]["docs"] == {"library": it["id"]}
    assert got["refs"][it["id"]]["definition"] == TOOL
    # A change to the item shows in the build at once, and is what deploys.
    call(env, "PUT /api/library/{id}", params={"id": it["id"]},
         body={"definition": {**TOOL, "endpoint": "https://docs2.example.com/mcp"}})
    assert _get(env, "u1")[1]["refs"][it["id"]]["definition"]["endpoint"] == "https://docs2.example.com/mcp"
    assert call(env, "POST /api/builds/{id}/deploy", params={"id": B}, body={"tool": "cdk"})[0] == 202
    frozen = version_object(env, B, 1)["workflow"]["tools"]["docs"]
    assert frozen["endpoint"] == "https://docs2.example.com/mcp" and "library" not in frozen


def test_someone_a_build_is_shared_with_sees_the_items_it_uses(env):
    _, it = _item(env)                    # u1's, not shared with u2
    wf = project()["workflow"]
    wf["tools"] = {"docs": {"library": it["id"]}}
    call(env, "PUT /api/builds/{id}", params={"id": B}, body={"project": {**project(), "workflow": wf}})
    _share(env, emails=["u2@example.com"])
    assert it["id"] in _get(env, "u2")[1]["refs"]


def test_an_item_that_is_gone_stops_the_deploy_with_a_reason(env):
    wf = project()["workflow"]
    wf["tools"] = {"docs": {"library": "deadbeef"}}
    call(env, "PUT /api/builds/{id}", params={"id": B}, body={"project": {**project(), "workflow": wf}})
    status, err = call(env, "POST /api/builds/{id}/deploy", params={"id": B}, body={"tool": "cdk"})
    assert status == 400 and "could not be loaded" in err["error"]


def test_deleting_an_item_leaves_every_build_that_used_it_its_own_copy(env):
    """Live: unpublishing (deleting from the library) took the tool out of the build too."""
    files = {"handler.py": "def lambda_handler(e, c):\n    return 1\n"}
    _, it = _item(env, files=files)
    call(env, "PUT /api/library/{id}/shares", params={"id": it["id"]}, body={"emails": ["u2@example.com"]})
    wf = project()["workflow"]
    wf["tools"] = {"docs": {"library": it["id"]}}
    wf["agents"]["claims_intake"]["tool"] = "docs"
    body = {"project": {**project(), "workflow": wf}}
    assert call(env, "PUT /api/builds/{id}", params={"id": B}, body=body)[0] == 200
    assert call(env, "PUT /api/builds/{id}", params={"id": "pother01"}, sub="u2",
                body={"project": {**project(), "workflow": wf}})[0] == 200
    assert save(env, "pplain01")[0] == 200            # uses nothing: left alone
    rev = _get(env, "u1")[1]["build"]["rev"]
    status, gone = call(env, "DELETE /api/library/{id}", params={"id": it["id"]})
    assert status == 200 and sorted(b["id"] for b in gone["keptIn"]) == sorted([B, "pother01"])
    for bid, sub in ((B, "u1"), ("pother01", "u2")):
        got = _get(env, sub, bid)[1]
        assert got["project"]["workflow"]["tools"]["docs"] == TOOL
        assert got["project"]["toolCode"]["docs"] == files and got["refs"] == {}
    # Someone with the build open from before is asked to reload, not allowed to save the
    # link back over the copy.
    stale = {"project": {**project(), "workflow": wf, "rev": rev}}
    assert call(env, "PUT /api/builds/{id}", params={"id": B}, body=stale)[0] == 409
    assert call(env, "POST /api/builds/{id}/deploy", params={"id": B}, body={"tool": "cdk"})[0] == 202
    assert version_object(env, B, 1)["workflow"]["tools"]["docs"] == TOOL


def test_policies_saved_before_the_library_are_moved_into_it(env):
    env.table.put_item(Item={"pk": "USER#u1", "sk": "POLICY#abcd1234", "id": "abcd1234", "owner": "u1",
                             "name": "old", "statement": 'forbid(principal, action in AgentCore::Action::"kb", '
                                                         'resource == AgentCore::Gateway::"{{gateway}}");',
                             "source": "cedar", "created": "2026-01-01T00:00:00Z"})
    got = call(env, "GET /api/policies")[1]
    assert [(p["id"], p["name"]) for p in got] == [("abcd1234", "old")]
    assert [i["kind"] for i in call(env, "GET /api/library")[1]] == ["policy"]
    assert not env.table.get_item(Key={"pk": "USER#u1", "sk": "POLICY#abcd1234"}).get("Item")


def test_the_new_routes_are_declared_on_both_iac_paths():
    from conftest import ORCH_ROOT
    tf = (ORCH_ROOT / "terraform" / "bff.tf").read_text()
    cdk = (ORCH_ROOT / "cdk" / "lib" / "orchestrator-stack.ts").read_text()
    for route in ("PUT /api/builds/{id}/shares", "GET /api/library", "POST /api/library", "GET /api/library/{id}",
                  "PUT /api/library/{id}", "DELETE /api/library/{id}", "PUT /api/library/{id}/shares",
                  "GET /api/groups", "PUT /api/groups/{id}", "DELETE /api/groups/{id}"):
        method, path = route.split(" ")
        assert f'"{route}"' in tf, route
        assert f'path: "{path}"' in cdk and f"HttpMethod.{method}" in cdk, route


def test_a_collaborators_app_login_is_shown_again_until_they_set_their_own(env):
    """Live: the first GET /login made qa-two's user and showed the password once; the
    second showed nothing, so the password was lost. It is kept like the owner's now."""
    import boto3
    assert save(env, B)[0] == 200
    idp = boto3.client("cognito-idp", region_name="us-east-1")
    pool = idp.create_user_pool(PoolName="app")["UserPool"]["Id"]
    idp.create_group(UserPoolId=pool, GroupName="operators")
    env.table.update_item(Key={"pk": f"BUILD#{B}", "sk": "META"}, UpdateExpression="SET deployed = :d",
                          ExpressionAttributeValues={":d": {"version": 1, "tool": "cdk", "userPoolId": pool}})
    assert _share(env, emails=["u2@example.com"])[0] == 200
    s1, first = call(env, "GET /api/builds/{id}/login", params={"id": B}, sub="u2")
    s2, again = call(env, "GET /api/builds/{id}/login", params={"id": B}, sub="u2")
    assert s1 == s2 == 200 and first["password"] and again["password"] == first["password"]
    assert first["user"] == "u2@example.com" and again["temporary"] is True
    groups = idp.admin_list_groups_for_user(UserPoolId=pool, Username="u2@example.com")["Groups"]
    assert [g["GroupName"] for g in groups] == ["operators"]
    # The owner, whose own login the deploy never stored here, is told so rather than
    # handed an empty one from the collaborators' secret.
    assert call(env, "GET /api/builds/{id}/login", params={"id": B})[0] == 404
