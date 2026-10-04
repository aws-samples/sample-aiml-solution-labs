"""Sharing: who besides its owner may see and change a build or a library item.

A share names people by email, admin-defined groups of emails, or everyone who signs
in to this console. Whoever it is shared with may do everything the owner may — edit,
deploy, destroy, delete, share on — and the owner always keeps access. Anything not
shared stays as private as before (another user gets 404, not 403).

Stored in the builds table:

    GROUPS            <name>            an admin's group: {name, members: [emails]}
    SHARE#<who>       <KIND>#<id>       a pointer, so "what is shared with me" is a Query
                                        per principal: EMAIL#<email>, GROUP#<name> or ALL

The object itself carries `shares` = {emails, groups, everyone}, which access is decided
on; the pointers only make listing cheap, and are rewritten with it.
"""

from __future__ import annotations

import re

from boto3.dynamodb.conditions import Key

EMAIL_RE = re.compile(r"^[^@\s]{1,64}@[^@\s]{1,255}$")
GROUP_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 ._-]{0,63}$")
MAX_SHARES = 200


def _t():
    import builds
    return builds._table, builds.BuildError


def email_of(who) -> str:
    return str(getattr(who, "email", "") or "").strip().lower()


# --- groups (admins define them) --------------------------------------------------------

def list_groups() -> list[dict]:
    table, _ = _t()
    items, kw = [], {"KeyConditionExpression": Key("pk").eq("GROUPS")}
    while True:
        page = table.query(**kw)
        items += page.get("Items", [])
        if not page.get("LastEvaluatedKey"):
            break
        kw["ExclusiveStartKey"] = page["LastEvaluatedKey"]
    return [{"name": i["sk"], "members": sorted(i.get("members") or []), "updatedAt": i.get("updatedAt", "")}
            for i in sorted(items, key=lambda i: i["sk"].lower())]


def groups_of(email: str) -> list[str]:
    email = (email or "").lower()
    return [g["name"] for g in list_groups() if email in g["members"]] if email else []


def put_group(name: str, members, stamp: str) -> dict:
    table, BuildError = _t()
    name = str(name or "").strip()
    if not GROUP_RE.match(name):
        raise BuildError(400, "a group name is 1 to 64 letters, digits, spaces, dots, - and _")
    clean = _emails(members)
    table.put_item(Item={"pk": "GROUPS", "sk": name, "members": clean, "updatedAt": stamp})
    return {"name": name, "members": clean, "updatedAt": stamp}


def delete_group(name: str) -> None:
    table, BuildError = _t()
    if not table.get_item(Key={"pk": "GROUPS", "sk": str(name)}).get("Item"):
        raise BuildError(404, "no such group")
    table.delete_item(Key={"pk": "GROUPS", "sk": str(name)})


def _emails(values) -> list[str]:
    _, BuildError = _t()
    if not isinstance(values, list):
        raise BuildError(400, "members / emails is a list of email addresses")
    out: list[str] = []
    for v in values:
        e = str(v or "").strip().lower()
        if not EMAIL_RE.match(e):
            raise BuildError(400, f"{v!r} is not an email address")
        if e not in out:
            out.append(e)
    if len(out) > MAX_SHARES:
        raise BuildError(400, f"at most {MAX_SHARES} addresses")
    return out


# --- access -------------------------------------------------------------------------------

def normalise(shares) -> dict:
    """{emails, groups, everyone}, checked. Groups must exist."""
    _, BuildError = _t()
    shares = shares if isinstance(shares, dict) else {}
    groups = shares.get("groups") or []
    if not isinstance(groups, list):
        raise BuildError(400, "groups is a list of group names")
    known = {g["name"] for g in list_groups()}
    missing = [g for g in groups if g not in known]
    if missing:
        raise BuildError(400, f"no such group: {', '.join(map(str, missing))}")
    return {"emails": _emails(shares.get("emails") or []), "groups": sorted(set(map(str, groups))),
            "everyone": bool(shares.get("everyone"))}


def can_access(item: dict, who) -> bool:
    """The owner, or anyone the item is shared with."""
    if not item:
        return False
    if str(item.get("owner") or "") == str(who):
        return True
    s = item.get("shares") or {}
    if s.get("everyone"):
        return True
    email = email_of(who)
    if email and email in (s.get("emails") or []):
        return True
    return bool(email and set(s.get("groups") or []) & set(groups_of(email)))


def _principals(shares: dict) -> list[str]:
    return ([f"EMAIL#{e}" for e in shares.get("emails") or []]
            + [f"GROUP#{g}" for g in shares.get("groups") or []]
            + (["ALL"] if shares.get("everyone") else []))


def write_pointers(kind: str, obj_id: str, owner: str, old: dict | None, new: dict) -> None:
    """Replace the share pointers of one object (kind BUILD or LIB)."""
    table, _ = _t()
    before, after = set(_principals(old or {})), set(_principals(new))
    for p in before - after:
        table.delete_item(Key={"pk": f"SHARE#{p}", "sk": f"{kind}#{obj_id}"})
    for p in after - before:
        table.put_item(Item={"pk": f"SHARE#{p}", "sk": f"{kind}#{obj_id}", "owner": owner,
                             "id": obj_id, "kind": kind})


def drop_pointers(kind: str, obj_id: str, shares: dict | None) -> None:
    write_pointers(kind, obj_id, "", shares, {})


def shared_with(who, kind: str) -> list[dict]:
    """Pointers to the objects of one kind (BUILD or LIB) shared with this caller."""
    table, _ = _t()
    email = email_of(who)
    principals = ["ALL"] + ([f"EMAIL#{email}"] + [f"GROUP#{g}" for g in groups_of(email)] if email else [])
    seen: set = set()
    out: list[dict] = []
    for p in principals:
        kw = {"KeyConditionExpression": Key("pk").eq(f"SHARE#{p}") & Key("sk").begins_with(f"{kind}#")}
        while True:
            page = table.query(**kw)
            for i in page.get("Items", []):
                if i["id"] not in seen and str(i.get("owner")) != str(who):
                    seen.add(i["id"])
                    out.append(i)
            if not page.get("LastEvaluatedKey"):
                break
            kw["ExclusiveStartKey"] = page["LastEvaluatedKey"]
    return out
