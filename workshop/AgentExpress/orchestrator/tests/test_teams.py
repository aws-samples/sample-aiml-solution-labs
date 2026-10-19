"""Teams and their workspaces (bff/teams.py, the /api/teams routes and the build routes in
bff/handler.py).

What has to hold:
  * only an admin creates or deletes a team, and only an admin adds, removes or changes an
    owner; a team's owners manage its editors and viewers;
  * a team's owners and editors change and deploy its builds (deploy still takes `deploy`),
    its viewers only read them, and anyone else gets 404, as for any build they may not know;
  * the creator of a build keeps full access wherever it is, as through any share;
  * a member named by a sign-in (IdP) group gets that group's role;
  * a build moves into a team only by someone who may edit it and the team, and back to its
    creator's own builds only by its creator;
  * a team with builds is not deleted; every change is in the activity log;
  * for someone in no team, nothing changes.
"""
from __future__ import annotations

import pytest

moto = pytest.importorskip("moto")
from test_builds import call, project, save  # noqa: E402
from test_builds import env as env  # noqa: E402

B = "pteam0001"
ADMIN = ("admins",)


def _team(e, name="Claims", members=(), groups=(), sub="admin1"):
    return call(e, "POST /api/teams", sub=sub, groups=ADMIN,
                body={"name": name, "members": list(members), "groups": list(groups)})


def _members(*pairs):
    return [{"email": f"{s}@example.com", "role": r} for s, r in pairs]


def _put_team(e, tid, sub, members=None, groups=None, admin=False, **extra):
    body = {**({"members": members} if members is not None else {}),
            **({"groups": groups} if groups is not None else {}), **extra}
    return call(e, "PUT /api/teams/{id}", params={"id": tid}, sub=sub,
                groups=ADMIN if admin else ("operators",), body=body)


def _get(e, sub, bid=B, groups=("operators",)):
    return call(e, "GET /api/builds/{id}", params={"id": bid}, sub=sub, groups=groups)


def _put(e, sub, bid=B, name="Claims v2", groups=("operators",), **body):
    return call(e, "PUT /api/builds/{id}", params={"id": bid}, sub=sub, groups=groups,
                body={"project": project(name=name), **body})


def _move(e, sub, to, bid=B):
    return call(e, "PUT /api/builds/{id}/team", params={"id": bid}, sub=sub, body={"team": to})


def _workspace(e, sub, tid, groups=("operators",)):
    return call(e, "GET /api/builds", sub=sub, groups=groups, qs={"team": tid})


@pytest.fixture()
def team(env):
    """A team: u1 its owner, u2 an editor, u3 a viewer; u4 is in no team."""
    status, t = _team(env, members=_members(("u1", "owner"), ("u2", "editor"), ("u3", "viewer")))
    assert status == 200
    return t["id"]


def test_only_an_admin_creates_a_team_and_its_members_see_it(env):
    assert call(env, "POST /api/teams", sub="u1", body={"name": "Claims", "members": []})[0] == 403
    status, t = _team(env, members=_members(("u1", "owner"), ("u2", "viewer")))
    assert status == 200 and t["name"] == "Claims" and len(t["id"]) == 8
    assert _team(env, name="claims")[0] == 409                    # names are unique, any case
    assert _team(env, name="")[0] == 400
    assert call(env, "POST /api/teams", sub="admin1", groups=ADMIN,
                body={"name": "X", "members": [{"email": "u9@example.com", "role": "boss"}]})[0] == 400
    mine = call(env, "GET /api/teams", sub="u2")[1]
    assert [(x["name"], x["role"]) for x in mine] == [("Claims", "viewer")]
    assert call(env, "GET /api/teams", sub="u4")[1] == []
    assert call(env, "GET /api/teams/{id}", params={"id": t["id"]}, sub="u2")[0] == 200
    # Someone not in it cannot tell it exists.
    assert call(env, "GET /api/teams/{id}", params={"id": t["id"]}, sub="u4")[0] == 404
    assert call(env, "GET /api/teams/{id}", params={"id": t["id"]}, sub="admin1", groups=ADMIN)[0] == 200
    assert call(env, "GET /api/teams", sub="u4", qs={"scope": "all"})[0] == 403
    every = call(env, "GET /api/teams", sub="admin1", groups=ADMIN, qs={"scope": "all"})[1]
    assert [x["name"] for x in every] == ["Claims"]


def test_team_owners_manage_editors_and_viewers_but_only_an_admin_manages_owners(env, team):
    members = _members(("u1", "owner"), ("u2", "editor"), ("u3", "viewer"), ("u5", "editor"))
    status, t = _put_team(env, team, "u1", members=members)
    assert status == 200 and {m["email"] for m in t["members"]} >= {"u5@example.com"}
    # An owner cannot make an owner, nor demote one, nor rename the team.
    assert _put_team(env, team, "u1", members=members + _members(("u6", "owner")))[0] == 403
    assert _put_team(env, team, "u1", members=_members(("u1", "editor"), ("u2", "editor")))[0] == 403
    assert _put_team(env, team, "u1", name="Renamed")[0] == 403
    # Editors and viewers manage nobody; a stranger is told there is no such team.
    assert _put_team(env, team, "u2", members=members)[0] == 403
    assert _put_team(env, team, "u3", members=members)[0] == 403
    assert _put_team(env, team, "u4", members=members)[0] == 404
    # An admin does all of it.
    status, t = _put_team(env, team, "admin1", admin=True, name="Claims EU",
                          members=members + _members(("u6", "owner")))
    assert status == 200 and t["name"] == "Claims EU"
    assert {m["email"]: m["role"] for m in t["members"]}["u6@example.com"] == "owner"


def test_editors_change_and_deploy_viewers_read_and_strangers_see_nothing(env, team):
    assert _put(env, "u2", name="Team build", team=team)[0] == 200      # an editor creates it there
    meta = env.table.get_item(Key={"pk": f"BUILD#{B}", "sk": "META"})["Item"]
    assert meta["team"] == team and meta["owner"] == "u2"
    # Owner and editor: open, change, deploy.
    for sub in ("u1", "u2"):
        status, got = _get(env, sub)
        assert status == 200 and "readOnly" not in got["build"]
        assert _put(env, sub, name=f"by {sub}")[0] == 200
    assert call(env, "POST /api/builds/{id}/deploy", params={"id": B}, sub="u1", body={"tool": "cdk"})[0] == 202
    # Viewer: reads it and what it holds, changes nothing.
    status, got = _get(env, "u3")
    assert status == 200 and got["build"]["readOnly"] is True
    assert call(env, "GET /api/builds/{id}/secrets", params={"id": B}, sub="u3")[0] == 200
    assert call(env, "GET /api/builds/{id}/deployments", params={"id": B}, sub="u3")[0] == 200
    assert call(env, "GET /api/builds/{id}/docs", params={"id": B}, sub="u3")[0] == 200
    assert _put(env, "u3")[0] == 403
    for route, body in (("PUT /api/builds/{id}/secrets", {"toolApiKeys": {"t": "k"}}),
                        ("POST /api/builds/{id}/destroy", {}), ("POST /api/builds/{id}/deploy", {"tool": "cdk"}),
                        ("PUT /api/builds/{id}/shares", {"emails": ["u3@example.com"]}),
                        ("POST /api/builds/{id}/docs", {"corpus": "c", "name": "a.pdf"}),
                        ("DELETE /api/builds/{id}", None)):
        assert call(env, route, params={"id": B}, sub="u3", body=body)[0] == 403, route
    # Someone in no team: 404 for all of it, read or write.
    assert _get(env, "u4")[0] == 404
    assert _put(env, "u4")[0] == 404
    assert call(env, "GET /api/builds/{id}/secrets", params={"id": B}, sub="u4")[0] == 404
    assert call(env, "POST /api/builds/{id}/deploy", params={"id": B}, sub="u4", body={"tool": "cdk"})[0] == 404


def test_an_editor_still_needs_the_deploy_action(env, team):
    assert _put(env, "u2", team=team)[0] == 200
    status, err = call(env, "POST /api/builds/{id}/deploy", params={"id": B}, sub="u1", groups=(),
                       body={"tool": "cdk"})
    assert status == 403 and "deploy" in err["error"]


def test_an_admin_reads_a_team_build_but_does_not_edit_it(env, team):
    assert _put(env, "u2", team=team)[0] == 200
    assert _get(env, "admin1", groups=ADMIN)[0] == 200
    assert _put(env, "admin1", groups=ADMIN)[0] == 404
    assert any(b["id"] == B and b["team"] == team
               for b in call(env, "GET /api/builds", sub="admin1", groups=ADMIN, qs={"scope": "all"})[1])


def test_only_editors_create_in_a_team(env, team):
    assert _put(env, "u3", bid="pviewer01", team=team)[0] == 403
    assert _put(env, "u4", bid="pstrange1", team=team)[0] == 404
    assert _put(env, "u2", bid="pbadteam1", team="nope")[0] == 400
    # Neither left anything behind.
    for bid in ("pviewer01", "pstrange1", "pbadteam1"):
        assert not env.table.get_item(Key={"pk": f"BUILD#{bid}", "sk": "META"}).get("Item")
    # `team` on a save of an EXISTING build moves nothing: moving is its own route.
    assert save(env, "pmine0001")[0] == 200
    assert _put(env, "u1", bid="pmine0001", team=team)[0] == 200
    assert "team" not in env.table.get_item(Key={"pk": "BUILD#pmine0001", "sk": "META"})["Item"]


def test_the_workspace_lists_the_teams_builds_and_my_builds_do_not(env, team):
    assert save(env, "pmine0001")[0] == 200                                 # u1's own
    assert _put(env, "u2", team=team)[0] == 200                             # the team's, made by u2
    ids = lambda r: [b["id"] for b in r[1]]                                 # noqa: E731
    for sub in ("u1", "u2", "u3"):
        assert ids(_workspace(env, sub, team)) == [B], sub
    assert [b.get("readOnly") for b in _workspace(env, "u3", team)[1]] == [True]
    assert _workspace(env, "u4", team)[0] == 404
    assert ids(call(env, "GET /api/builds", sub="u1")) == ["pmine0001"]
    assert ids(call(env, "GET /api/builds", sub="u2")) == []                # it is in the team's
    # u2 leaves the team: their build is in their own list again, and they keep access.
    _put_team(env, team, "u1", members=_members(("u1", "owner"), ("u3", "viewer")))
    assert ids(call(env, "GET /api/builds", sub="u2")) == [B]
    assert _get(env, "u2")[0] == 200 and _put(env, "u2")[0] == 200


def test_removing_a_member_takes_their_access_away(env, team):
    assert _put(env, "u1", team=team)[0] == 200
    assert _get(env, "u3")[0] == 200
    _put_team(env, team, "u1", members=_members(("u1", "owner"), ("u2", "editor")))
    assert _get(env, "u3")[0] == 404 and _workspace(env, "u3", team)[0] == 404
    # Promoted to editor: may change it now.
    assert _put(env, "u2")[0] == 200
    _put_team(env, team, "u1", members=_members(("u1", "owner"), ("u2", "viewer")))
    assert _put(env, "u2")[0] == 403


def test_a_sign_in_group_gets_its_role(env):
    _, t = _team(env, members=_members(("u1", "owner")), groups=[{"name": "claims-readers", "role": "viewer"},
                                                                {"name": "claims-devs", "role": "editor"}])
    assert _put(env, "u1", team=t["id"])[0] == 200
    assert _get(env, "u7", groups=("claims-readers",))[0] == 200
    assert _put(env, "u7", groups=("claims-readers",))[0] == 403
    # The highest role of everything that names them.
    assert _put(env, "u7", groups=("claims-readers", "claims-devs"))[0] == 200
    assert _get(env, "u7", groups=("other",))[0] == 404
    assert _team(env, name="Bad", groups=[{"name": "has space", "role": "viewer"}])[0] == 400


def test_moving_a_build_in_and_out_of_a_team(env, team):
    _, other = _team(env, name="Other", members=_members(("u2", "editor")))
    assert save(env, B)[0] == 200                                    # u1's own
    assert _move(env, "u1", "deadbeef")[0] == 404                    # no such team
    assert _move(env, "u4", team)[0] == 404                          # not u4's build
    status, moved = _move(env, "u1", team)
    assert status == 200 and moved["team"] == team
    assert env.table.get_item(Key={"pk": f"TEAM#{team}", "sk": f"BUILD#{B}"}).get("Item")
    assert _move(env, "u3", "")[0] == 403                            # a viewer moves nothing
    assert _move(env, "u1", other["id"])[0] == 404                   # u1 is not in Other
    assert _move(env, "u2", "")[0] == 403                            # only its creator takes it home
    # u2 edits both: team to team.
    status, moved = _move(env, "u2", other["id"])
    assert status == 200 and moved["team"] == other["id"]
    assert not env.table.get_item(Key={"pk": f"TEAM#{team}", "sk": f"BUILD#{B}"}).get("Item")
    assert [b["id"] for b in _workspace(env, "u2", other["id"])[1]] == [B]
    assert _workspace(env, "u3", team)[1] == []
    # Its creator takes it back.
    status, moved = _move(env, "u1", "")
    assert status == 200 and "team" not in moved
    assert [b["id"] for b in call(env, "GET /api/builds", sub="u1")[1]] == [B]
    assert _get(env, "u2")[0] == 404
    assert env.table.get_item(Key={"pk": f"BUILD#{B}", "sk": "META"})["Item"]["owner"] == "u1"


def test_a_team_with_builds_is_not_deleted(env, team):
    assert _put(env, "u1", team=team)[0] == 200
    assert call(env, "DELETE /api/teams/{id}", params={"id": team}, sub="u1")[0] == 403
    assert call(env, "DELETE /api/teams/{id}", params={"id": team}, sub="admin1", groups=ADMIN)[0] == 409
    assert call(env, "DELETE /api/builds/{id}", params={"id": B}, sub="u2")[0] == 200
    assert not env.table.get_item(Key={"pk": f"TEAM#{team}", "sk": f"BUILD#{B}"}).get("Item")
    assert call(env, "DELETE /api/teams/{id}", params={"id": team}, sub="admin1", groups=ADMIN)[0] == 200
    assert call(env, "GET /api/teams", sub="u1")[1] == []


def test_a_pointer_left_by_a_build_deleted_elsewhere_is_dropped(env, team):
    """The deploy runner deletes a build after a destroy (buildstore.purge), and knows
    nothing of teams: the workspace forgets the pointer it left."""
    assert _put(env, "u1", team=team)[0] == 200
    env.table.delete_item(Key={"pk": f"BUILD#{B}", "sk": "META"})
    assert _workspace(env, "u1", team)[1] == []
    assert not env.table.get_item(Key={"pk": f"TEAM#{team}", "sk": f"BUILD#{B}"}).get("Item")
    assert call(env, "DELETE /api/teams/{id}", params={"id": team}, sub="admin1", groups=ADMIN)[0] == 200


def test_team_changes_are_in_the_activity_log(env, team):
    _put_team(env, team, "u1", members=_members(("u1", "owner"), ("u2", "editor"), ("u5", "viewer")))
    assert save(env, B)[0] == 200
    _move(env, "u1", team)
    actions = lambda sub: [(e["action"], e["detail"]) for e in call(env, "GET /api/audit", sub=sub)[1]]  # noqa: E731
    admin = dict(actions("admin1"))
    assert admin["team.created"]["team"] == team and admin["team.created"]["members"] == 3
    mine = dict(actions("u1"))
    assert mine["team.updated"]["added"] == ["u5@example.com"]
    assert mine["team.updated"]["removed"] == ["u3@example.com"] and "roleChanged" not in mine["team.updated"]
    assert mine["build.moved"]["build"] == B and mine["build.moved"]["team"] == team
    assert "fromTeam" not in mine["build.moved"]          # it came from u1's own builds
    call(env, "PUT /api/builds/{id}/team", params={"id": B}, sub="u1", body={"team": ""})
    call(env, "DELETE /api/teams/{id}", params={"id": team}, sub="admin1", groups=ADMIN)
    assert dict(actions("admin1"))["team.deleted"]["teamName"] == "Claims"


def test_nothing_changes_for_someone_in_no_team(env, team):
    assert save(env, B)[0] == 200
    status, got = _get(env, "u1")
    assert status == 200 and "readOnly" not in got["build"] and "team" not in got["build"]
    assert [b["id"] for b in call(env, "GET /api/builds", sub="u1")[1]] == [B]
    assert _get(env, "u4")[0] == 404
    assert call(env, "PUT /api/builds/{id}/shares", params={"id": B},
                body={"emails": ["u4@example.com"], "groups": [], "everyone": False})[0] == 200
    assert _get(env, "u4")[0] == 200 and _put(env, "u4")[0] == 200


def test_the_design_worker_keeps_the_callers_groups(env):
    """The Assistant answers in a second invocation; a team role from a sign-in group must
    reach it, or the worker sees a 404 for a build the caller may edit."""
    _, t = _team(env, members=[], groups=[{"name": "claims-devs", "role": "editor"}])
    assert _put(env, "u7", groups=("claims-devs",), team=t["id"])[0] == 200
    status, _ = call(env, "POST /api/builds/{id}/design", params={"id": B}, sub="u8", groups=("claims-devs",),
                     body={"message": "add a step"})
    assert status == 202
    payload = env.invoked[-1]
    assert payload["action"] == "design" and payload["groups"] == ["claims-devs"]
    who = env.builds.Who(payload["owner"], payload["email"], payload["groups"])
    assert env.builds._meta(B, who)["id"] == B


def test_the_new_routes_are_declared_on_both_iac_paths():
    from conftest import ORCH_ROOT
    tf = (ORCH_ROOT / "terraform" / "bff.tf").read_text()
    cdk = (ORCH_ROOT / "cdk" / "lib" / "orchestrator-stack.ts").read_text()
    for route in ("GET /api/teams", "POST /api/teams", "GET /api/teams/{id}", "PUT /api/teams/{id}",
                  "DELETE /api/teams/{id}", "PUT /api/builds/{id}/team"):
        method, path = route.split(" ")
        assert f'"{route}"' in tf, route
        assert f'path: "{path}"' in cdk and f"HttpMethod.{method}" in cdk, route
