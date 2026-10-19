"""Teams, and the workspace each one has: builds that belong to the team, not to one user.

A team has a name and members, each with a role:

    owner    manages the team's members (editors and viewers), and edits and deploys
    editor   edits, deploys and destroys the team's builds (still subject to the `deploy` and
             `destroy` actions of workflow.json `authorization`, like everyone else)
    viewer   reads the team's builds; changes nothing

A member is named by email, or by a group of the sign-in provider (the JWT groups claim,
bff/authz.py GROUPS_CLAIM), so a whole IdP group can be given a role. Someone in several
entries gets the highest role among them. Only an admin (the `admin` action) creates or
deletes a team, and only an admin adds, removes or changes an owner; a team's owners manage
everyone else.

A build is in a workspace by its `team` attribute (absent: its creator's own builds, as
before). Its `owner` never changes when it moves: the Terraform state path, the connected
account a deploy uses and the deployed app's invitations are all derived from it, and the
creator keeps full access wherever the build is, as an owner keeps it through any share
(bff/sharing.py). A team role is one more way to pass the check every build route makes
(builds._meta); without one, a team's build answers 404 like any build the caller may not
know exists.

Stored in the builds table, under keys of their own (the BFF's existing grants on the table
cover them; no attribute here is a key of the by_owner index, so none enters it):

    TEAMS        <team id>     the team: {id, name, members: [{email, role}],
                               groups: [{name, role}], created, updatedAt, createdBy}
    TEAM#<id>    BUILD#<bid>   a pointer: build <bid> is in this team's workspace, so the
                               workspace is one Query

Out of scope: per-team billing, connected AWS accounts per team (a team build deploys to its
creator's connected accounts, or this console's), library items in a workspace, and the
execution plane (a build's own app).
"""

from __future__ import annotations

import re
import secrets

from boto3.dynamodb.conditions import Key

ID_RE = re.compile(r"^[0-9a-f]{8}$")
NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 ._-]{0,63}$")
# What bff/authz.py groups_of can hand back as one group: never spaces, commas, brackets or
# quotes, which it splits on. Cognito allows up to 128 characters in a group name.
IDP_GROUP_RE = re.compile(r"^[^\s,\[\]\"]{1,128}$")
ROLES = ("viewer", "editor", "owner")
_RANK = {r: i + 1 for i, r in enumerate(ROLES)}
EDIT_ROLES = ("editor", "owner")
MAX_MEMBERS = 200
MAX_GROUPS = 50
MAX_TEAMS = 500


def _t():
    import builds
    return builds._table, builds.BuildError


def _email(who) -> str:
    return str(getattr(who, "email", "") or "").strip().lower()


# --- reading teams ------------------------------------------------------------------------

def list_teams() -> list[dict]:
    table, _ = _t()
    items, kw = [], {"KeyConditionExpression": Key("pk").eq("TEAMS")}
    while True:
        page = table.query(**kw)
        items += page.get("Items", [])
        if not page.get("LastEvaluatedKey"):
            break
        kw["ExclusiveStartKey"] = page["LastEvaluatedKey"]
    return sorted(items, key=lambda i: str(i.get("name") or "").lower())


def get_team(team_id: str) -> dict | None:
    table, _ = _t()
    if not ID_RE.match(str(team_id or "")):
        return None
    return table.get_item(Key={"pk": "TEAMS", "sk": str(team_id)}).get("Item")


def role_of(team: dict | None, who) -> str:
    """The caller's role in the team: the highest of their email's and their groups'. ""
    for someone not in it."""
    if not team:
        return ""
    email, groups = _email(who), set(getattr(who, "groups", ()) or ())
    best = ""
    for m in team.get("members") or []:
        if email and str(m.get("email") or "") == email and _RANK.get(m.get("role"), 0) > _RANK.get(best, 0):
            best = str(m["role"])
    for g in team.get("groups") or []:
        if str(g.get("name") or "") in groups and _RANK.get(g.get("role"), 0) > _RANK.get(best, 0):
            best = str(g["role"])
    return best


def public(team: dict, who=None) -> dict:
    out = {k: team[k] for k in ("id", "name", "members", "groups", "created", "updatedAt", "createdBy")
           if k in team}
    if who is not None:
        out["role"] = role_of(team, who)
    return out


def mine(who) -> list[dict]:
    """The teams the caller is in, each with their role."""
    return [public(t, who) for t in list_teams() if role_of(t, who)]


def visible(team_id: str, who, admin: bool) -> dict:
    """A team its members (and admins) may see; anyone else gets 404, as for a build."""
    _, BuildError = _t()
    team = get_team(team_id)
    if not team or not (admin or role_of(team, who)):
        raise BuildError(404, "no such team")
    return team


def require_role(team_id: str, who, roles=EDIT_ROLES) -> dict:
    """The team, if the caller holds one of `roles` in it: 404 when they are not in it (or it
    does not exist), 403 when their role is too low."""
    _, BuildError = _t()
    team = get_team(team_id)
    role = role_of(team, who)
    if not role:
        raise BuildError(404, "no such team")
    if role not in roles:
        raise BuildError(403, f"your role in {team.get('name')} is {role}: it cannot change the team's builds")
    return team


# --- access to a build ----------------------------------------------------------------------

def access(item: dict | None, who) -> str:
    """"edit", "read" or "" — what the caller may do with a build. Its creator and anyone it
    is shared with may do everything, as before; a team's editors and owners edit it and its
    viewers read it."""
    import sharing
    if not item:
        return ""
    if sharing.can_access(item, who):
        return "edit"
    team_id = str(item.get("team") or "")
    if not team_id:
        return ""
    role = role_of(get_team(team_id), who)
    return "edit" if role in EDIT_ROLES else "read" if role == "viewer" else ""


# --- managing teams -------------------------------------------------------------------------

def _clean_name(name) -> str:
    _, BuildError = _t()
    name = " ".join(str(name or "").split())
    if not NAME_RE.match(name):
        raise BuildError(400, "a team name is 1 to 64 letters, digits, spaces, dots, - and _")
    return name


def _clean_members(members, groups) -> tuple[list[dict], list[dict]]:
    import sharing
    _, BuildError = _t()
    if not isinstance(members, list) or not isinstance(groups, list):
        raise BuildError(400, "members and groups are lists of {email, role} and {name, role}")
    out_m: dict[str, str] = {}
    for m in members:
        if not isinstance(m, dict):
            raise BuildError(400, "a member is {email, role}")
        email = str(m.get("email") or "").strip().lower()
        if not sharing.EMAIL_RE.match(email):
            raise BuildError(400, f"{m.get('email')!r} is not an email address")
        if m.get("role") not in ROLES:
            raise BuildError(400, f"a role is one of: {', '.join(ROLES)}")
        out_m[email] = str(m["role"])
    out_g: dict[str, str] = {}
    for g in groups:
        if not isinstance(g, dict):
            raise BuildError(400, "a group is {name, role}")
        name = str(g.get("name") or "").strip()
        if not IDP_GROUP_RE.match(name):
            raise BuildError(400, f"{g.get('name')!r} is not a sign-in group name")
        if g.get("role") not in ROLES:
            raise BuildError(400, f"a role is one of: {', '.join(ROLES)}")
        out_g[name] = str(g["role"])
    if len(out_m) > MAX_MEMBERS:
        raise BuildError(400, f"a team has at most {MAX_MEMBERS} members")
    if len(out_g) > MAX_GROUPS:
        raise BuildError(400, f"a team has at most {MAX_GROUPS} groups")
    return ([{"email": e, "role": r} for e, r in sorted(out_m.items())],
            [{"name": n, "role": r} for n, r in sorted(out_g.items())])


def _owners(members: list[dict], groups: list[dict]) -> set:
    return ({("email", m["email"]) for m in members if m.get("role") == "owner"}
            | {("group", g["name"]) for g in groups if g.get("role") == "owner"})


def _name_taken(name: str, but: str = "") -> bool:
    return any(str(t.get("name") or "").lower() == name.lower() and t.get("sk") != but for t in list_teams())


def create(body: dict, who, stamp: str) -> dict:
    """An admin's: a new team, with its first owners (and any other members)."""
    table, BuildError = _t()
    name = _clean_name(body.get("name"))
    members, groups = _clean_members(body.get("members") or [], body.get("groups") or [])
    teams = list_teams()
    if len(teams) >= MAX_TEAMS:
        raise BuildError(400, f"this console holds at most {MAX_TEAMS} teams")
    if _name_taken(name):
        raise BuildError(409, f"there is already a team named {name}")
    tid = secrets.token_hex(4)
    item = {"pk": "TEAMS", "sk": tid, "id": tid, "name": name, "members": members, "groups": groups,
            "created": stamp, "updatedAt": stamp, "createdBy": _email(who) or str(who)}
    table.put_item(Item=item, ConditionExpression="attribute_not_exists(pk)")
    return public(item, who)


def update(team_id: str, body: dict, who, admin: bool, stamp: str) -> tuple[dict, dict]:
    """Change a team's name (admins), or its members and groups (its owners, and admins). An
    owner manages editors and viewers; only an admin adds, removes or changes an owner.
    Returns (the team, what changed) for the audit log."""
    table, BuildError = _t()
    team = visible(team_id, who, admin)
    if not admin and role_of(team, who) != "owner":
        raise BuildError(403, "only the team's owners, or an admin, change who is in it")
    name = team["name"]
    if "name" in body and body.get("name") != team["name"]:
        if not admin:
            raise BuildError(403, "only an admin renames a team")
        name = _clean_name(body.get("name"))
        if _name_taken(name, but=team_id):
            raise BuildError(409, f"there is already a team named {name}")
    members, groups = _clean_members(body.get("members", team.get("members") or []),
                                     body.get("groups", team.get("groups") or []))
    before = _owners(team.get("members") or [], team.get("groups") or [])
    if not admin and _owners(members, groups) != before:
        raise BuildError(403, "only an admin adds, removes or changes a team's owners")
    was_m = {m["email"]: m["role"] for m in team.get("members") or []}
    now_m = {m["email"]: m["role"] for m in members}
    changed = {"added": sorted(set(now_m) - set(was_m)), "removed": sorted(set(was_m) - set(now_m)),
               "roleChanged": sorted(e for e in set(now_m) & set(was_m) if now_m[e] != was_m[e])}
    try:
        # Conditional on the copy just read, so two owners saving at once cannot silently
        # drop each other's change: the second is told to reload.
        table.update_item(
            Key={"pk": "TEAMS", "sk": team_id},
            UpdateExpression="SET #n = :n, members = :m, #g = :g, updatedAt = :t",
            ConditionExpression="updatedAt = :was",
            ExpressionAttributeNames={"#n": "name", "#g": "groups"},
            ExpressionAttributeValues={":n": name, ":m": members, ":g": groups, ":t": stamp,
                                       ":was": team.get("updatedAt", "")})
    except table.meta.client.exceptions.ConditionalCheckFailedException:
        raise BuildError(409, "this team was changed by someone else since you opened it: reload") from None
    return public({**team, "name": name, "members": members, "groups": groups, "updatedAt": stamp}, who), \
        {k: v for k, v in changed.items() if v}


def delete(team_id: str) -> dict:
    """An admin's. A team that still has builds is not deleted: they would be left in no
    workspace anyone but their creators could reach."""
    table, BuildError = _t()
    team = get_team(team_id)
    if not team:
        raise BuildError(404, "no such team")
    left = build_ids(team_id)
    if left:
        raise BuildError(409, f"{team['name']} still has {len(left)} build{'s' if len(left) != 1 else ''}: "
                              "move or delete them first")
    table.delete_item(Key={"pk": "TEAMS", "sk": team_id})
    return team


# --- the workspace ------------------------------------------------------------------------

def _pointer(team_id: str, build_id: str) -> dict:
    return {"pk": f"TEAM#{team_id}", "sk": f"BUILD#{build_id}"}


def add_build(team_id: str, build_id: str) -> None:
    table, _ = _t()
    table.put_item(Item={**_pointer(team_id, build_id), "id": build_id})


def drop_build(team_id: str, build_id: str) -> None:
    table, _ = _t()
    if team_id:
        table.delete_item(Key=_pointer(team_id, build_id))


def _metas(team_id: str) -> list[dict]:
    """The META of each build in the workspace. A pointer whose build is gone (the deploy
    runner deletes a build after a destroy, and knows nothing of teams) or has moved since is
    dropped here."""
    import buildstore
    table, _ = _t()
    out, kw = [], {"KeyConditionExpression": Key("pk").eq(f"TEAM#{team_id}") & Key("sk").begins_with("BUILD#")}
    while True:
        page = table.query(**kw)
        for p in page.get("Items", []):
            bid = p["sk"][len("BUILD#"):]
            item = table.get_item(Key=buildstore.meta_key(bid)).get("Item")
            if item and str(item.get("team") or "") == team_id:
                out.append(item)
            else:
                table.delete_item(Key={"pk": p["pk"], "sk": p["sk"]})
        if not page.get("LastEvaluatedKey"):
            return out
        kw["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def build_ids(team_id: str) -> list[str]:
    return [str(i.get("id") or i["pk"][len("BUILD#"):]) for i in _metas(team_id)]


def workspace(team_id: str, who) -> list[dict]:
    """The team's builds, newest first, for its members: each `readOnly` for a viewer."""
    import builds
    team = get_team(team_id)
    role = role_of(team, who)
    if not role:
        _, BuildError = _t()
        raise BuildError(404, "no such team")
    items = _metas(team_id)
    items.sort(key=lambda i: str(i.get("updated") or ""), reverse=True)
    builds._refresh(items)
    return [builds._public(i, who, access(i, who)) for i in items]


def move(build_id: str, who, to: str) -> tuple[dict, str]:
    """Put a build in a team's workspace (`to` = its id), or back in its creator's own builds
    (`to` = ""). The caller must be able to edit the build and the destination: an editor or
    owner of the team, or, for a creator's own builds, the creator. Returns (the build, the
    team it came from)."""
    import builds
    import buildstore
    table, BuildError = _t()
    to = str(to or "")
    if to and not ID_RE.match(to):
        raise BuildError(400, "invalid team id")
    item = builds._meta(build_id, who)               # 404, or 403 for a viewer
    frm = str(item.get("team") or "")
    if to == frm:
        return builds._public(item, who), frm
    if to:
        require_role(to, who)
    elif str(item.get("owner") or "") != str(who):
        raise BuildError(403, "only the person who created this build can take it back to their own builds")
    names, values = {"#t": "team"}, {}
    if frm:
        cond, values[":from"] = "#t = :from", frm
    else:
        cond = "attribute_not_exists(#t)"
    if to:
        expr, values[":to"] = "SET #t = :to", to
    else:
        expr = "REMOVE #t"
    try:
        # Conditional on where it was, so two moves at once cannot leave a pointer behind.
        res = table.update_item(Key=buildstore.meta_key(build_id), UpdateExpression=expr,
                                ConditionExpression=cond, ExpressionAttributeNames=names,
                                ReturnValues="ALL_NEW", **({"ExpressionAttributeValues": values} if values else {}))
    except table.meta.client.exceptions.ConditionalCheckFailedException:
        raise BuildError(409, "this build was moved by someone else just now: reload") from None
    drop_build(frm, build_id)
    if to:
        add_build(to, build_id)
    return builds._public(res["Attributes"], who), frm
