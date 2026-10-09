"""The library: tools, guardrails, memories, evaluators, identities and policies a user
keeps to use in any of their builds — and shares (bff/sharing.py).

A build uses one LIVE: its workflow holds `{"library": "<id>"}` where the entry would be
(`tools.<key>`, `guardrails.<name>`, ...), and every read of the build resolves it to the
item as it is now. So a change to a shared item shows in every build that uses it at
once; a DEPLOYED build changes when it is next deployed (the deploy freezes a snapshot,
like the rest of the build). A secret is never part of an item: each build sets its own.

Stored in the builds table:

    LIB#<id>      META        the item: kind, name, description, definition, files (a code
                              tool's), owner, ownerEmail, shares, created, updatedAt
    USER#<sub>    LIB#<id>    a pointer, so "mine" is one Query
"""

from __future__ import annotations

import copy
import decimal
import json
import re
import secrets

import buildstore
import sharing
from boto3.dynamodb.conditions import Key

#: kind -> (the workflow map it goes in, the keys.json block its definition follows)
KINDS = {"tool": ("tools", "tool"), "guardrail": ("guardrails", "guardrail"),
         "memory": ("memories", "memory"), "evaluator": ("evaluators", "evaluator"),
         "identity": ("identities", "identity"), "policy": ("policies", "policy"),
         "skill": ("skills", "skill")}
MAP_KIND = {m: k for k, (m, _) in KINDS.items()}
#: Kinds a build takes a COPY of, never a live {"library": id} link: a Gateway
#: interceptor (orchestrator.interceptors.<point>, with its files) is one per build.
COPY_KINDS = ("interceptor",)
ALL_KINDS = (*KINDS, *COPY_KINDS)
ID_RE = re.compile(r"^[0-9a-f]{8}$")
NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9]{0,31}$")
LIMIT = 300
ITEM_MAX = 350_000


def _t():
    import builds
    return builds._table, builds.BuildError


def _public(item: dict, who=None) -> dict:
    out = {k: item[k] for k in ("id", "kind", "name", "description", "definition", "files", "source",
                                "ownerEmail", "shares", "created", "updatedAt") if k in item}
    if who is not None:
        out["mine"] = str(item.get("owner") or "") == str(who)
    return out


def _plain(v):
    """What DynamoDB returns, as the JSON it was: its numbers come back as Decimal, and a
    build must see 45, not Decimal("45") — which no validator counts as a whole number."""
    if isinstance(v, decimal.Decimal):
        return int(v) if v == v.to_integral_value() else float(v)
    if isinstance(v, dict):
        return {k: _plain(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_plain(x) for x in v]
    return v


def _dynamo(v):
    """JSON as DynamoDB takes it: floats as Decimal (it refuses a float)."""
    if isinstance(v, float):
        return decimal.Decimal(str(v))
    if isinstance(v, dict):
        return {k: _dynamo(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_dynamo(x) for x in v]
    return v


def _get(item_id: str) -> dict | None:
    table, _ = _t()
    if not ID_RE.match(item_id or ""):
        return None
    item = table.get_item(Key={"pk": f"LIB#{item_id}", "sk": "META"}).get("Item")
    return _plain(item) if item else None


def get(item_id: str, who) -> dict:
    _, BuildError = _t()
    item = _get(item_id)
    if not item or not sharing.can_access(item, who):
        raise BuildError(404, "no such library item")
    return item


def list_items(who, kind: str = "") -> list[dict]:
    """The caller's own items and those shared with them, of one kind or all."""
    table, BuildError = _t()
    if kind and kind not in ALL_KINDS:
        raise BuildError(400, f"kind is one of: {', '.join(ALL_KINDS)}")
    _migrate_policies(who)
    ids, kw = [], {"KeyConditionExpression": Key("pk").eq(f"USER#{who}") & Key("sk").begins_with("LIB#")}
    while True:
        page = table.query(**kw)
        ids += [i["sk"][4:] for i in page.get("Items", [])]
        if not page.get("LastEvaluatedKey"):
            break
        kw["ExclusiveStartKey"] = page["LastEvaluatedKey"]
    ids += [p["id"] for p in sharing.shared_with(who, "LIB") if p["id"] not in ids]
    out = []
    for i in ids:
        item = _get(i)
        if item and sharing.can_access(item, who) and (not kind or item.get("kind") == kind):
            out.append(_public(item, who))
    return sorted(out, key=lambda x: (x["kind"], str(x.get("name", "")).lower()))


def _check(kind: str, name, definition, files) -> tuple[str, dict]:
    """The definition as a build would carry it, checked with the build's own validator."""
    import cedar
    import validate_build
    _, BuildError = _t()
    if kind not in ALL_KINDS:
        raise BuildError(400, f"kind is one of: {', '.join(ALL_KINDS)}")
    name = str(name or "")
    if not NAME_RE.match(name):
        raise BuildError(400, "a name is a letter, then letters and digits (32 at most)")
    if not isinstance(definition, dict) or "library" in definition:
        raise BuildError(400, "definition is the entry itself: an object")
    if kind == "interceptor":
        return name, _check_interceptor(definition, files, BuildError)
    block = KINDS[kind][1]
    problems: list = []
    validate_build._check_entry(block, definition, KINDS[kind][0], {"kind": "library"}, problems)
    errors = [p for p in problems if p["severity"] == "error"]
    if kind == "policy":
        errors += [{"path": "statement", "message": m} for m in cedar.problems(definition.get("statement"), None)]
    if errors:
        raise BuildError(400, f"{errors[0]['path']}: {errors[0]['message']}")
    if files is not None and (kind not in ("tool", "interceptor") or not isinstance(files, dict) or not all(
            isinstance(k, str) and isinstance(v, str) for k, v in files.items())):
        raise BuildError(400, "files are a code tool's {filename: text}")
    return name, definition


def _check_interceptor(definition: dict, files, BuildError) -> dict:
    """{"point": "request" | "response", ...orchestrator.interceptors.<point>}, checked
    with the build's own validator; written here, it brings its files."""
    import validate_build
    point = definition.get("point")
    if point not in ("request", "response"):
        raise BuildError(400, 'point: "request" or "response"')
    ic = {k: v for k, v in definition.items() if k != "point"}
    problems: list = []
    validate_build._check_interceptors({"interceptors": {point: ic}}, {"t": {}}, {},
                                       lambda sev, where, path, msg: problems.append((sev, path, msg)))
    errors = [p for p in problems if p[0] == "error"]
    if errors:
        raise BuildError(400, f"{errors[0][1]}: {errors[0][2]}")
    if "code" in ic and not (isinstance(files, dict) and isinstance(files.get("handler.py"), str)
                             and all(isinstance(k, str) and isinstance(v, str) for k, v in files.items())):
        raise BuildError(400, "an interceptor written here brings its files, with handler.py")
    return definition


def create(who, body: dict) -> dict:
    table, BuildError = _t()
    kind = str(body.get("kind") or "")
    name, definition = _check(kind, body.get("name"), body.get("definition"), body.get("files"))
    mine = [i for i in list_items(who) if i.get("mine")]
    if len(mine) >= LIMIT:
        raise BuildError(400, f"a library holds at most {LIMIT} items: delete one first")
    if any(i["kind"] == kind and i["name"] == name for i in mine):
        raise BuildError(409, f'you already have a {kind} named "{name}"')
    iid, now = secrets.token_hex(4), buildstore.now()
    item = {"pk": f"LIB#{iid}", "sk": "META", "id": iid, "kind": kind, "name": name,
            "description": " ".join(str(body.get("description") or "").split())[:1000],
            "definition": definition, "owner": str(who), "ownerEmail": getattr(who, "email", ""),
            "shares": {"emails": [], "groups": [], "everyone": False}, "created": now, "updatedAt": now}
    if body.get("files") is not None:
        item["files"] = body["files"]
    if body.get("source"):
        item["source"] = str(body["source"])[:20]     # how a policy was written (english/form/cedar)
    if len(json.dumps(item, default=str)) > ITEM_MAX:
        raise BuildError(413, "this item is too large to store")
    table.put_item(Item=_dynamo(item), ConditionExpression="attribute_not_exists(pk)")
    table.put_item(Item={"pk": f"USER#{who}", "sk": f"LIB#{iid}", "kind": kind})
    return _public(item, who)


def update(who, item_id: str, body: dict) -> dict:
    """Anyone it is shared with may change it; every build using it sees the change."""
    table, BuildError = _t()
    item = get(item_id, who)
    name, definition = _check(item["kind"], body.get("name", item["name"]),
                              body.get("definition", item.get("definition")),
                              body.get("files", item.get("files")))
    item = {**item, "name": name, "definition": definition, "updatedAt": buildstore.now(),
            "description": " ".join(str(body.get("description", item.get("description")) or "").split())[:1000]}
    if body.get("files", item.get("files")) is not None:
        item["files"] = body.get("files", item.get("files"))
    if body.get("source"):
        item["source"] = str(body["source"])[:20]
    if len(json.dumps(item, default=str)) > ITEM_MAX:
        raise BuildError(413, "this item is too large to store")
    table.put_item(Item=_dynamo(item))
    return _public(item, who)


def _builds_using(item_id: str):
    """(meta, draft) of every build whose draft uses the item. A build saved since the
    library existed lists what it uses in `libraryUses` (builds.save), so only those
    that name the item — or predate the list — have their draft read."""
    import builds
    from boto3.dynamodb.conditions import Attr
    kwargs = {"FilterExpression": Attr("sk").eq("META") & Attr("pk").begins_with("BUILD#")
              & (Attr("libraryUses").not_exists() | Attr("libraryUses").contains(item_id))}
    while True:
        page = builds._table.scan(**kwargs)
        for meta in page.get("Items", []):
            draft = builds._draft(str(meta.get("id") or meta["pk"][6:]))
            if item_id in ids_in(draft):
                yield meta, draft
        if not page.get("LastEvaluatedKey"):
            break
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def detach(item: dict, email: str = "") -> list[dict]:
    """Every build that uses the item gets its own copy of it, as it is now (a code tool
    with its files), so deleting it from the library never takes it out of a build. Each
    such save counts as an edit (`rev`), so someone with the build open is asked to reload
    rather than saving the link back over the copy."""
    import builds
    iid, kind = str(item["id"]), str(item.get("kind") or "")
    kept = []
    for meta, draft in _builds_using(iid):
        wf = draft.get("workflow") or {}
        for m, k in MAP_KIND.items():
            entries = wf.get(m)
            if k != kind or not isinstance(entries, dict):
                continue
            for key, entry in list(entries.items()):
                if isinstance(entry, dict) and entry.get("library") == iid:
                    entries[key] = copy.deepcopy(_plain(item.get("definition") or {}))
                    if kind == "tool" and item.get("files"):
                        draft.setdefault("toolCode", {}).setdefault(key, copy.deepcopy(item["files"]))
        bid = str(meta.get("id") or meta["pk"][6:])
        builds._s3.put_object(Bucket=builds.BUILDS_BUCKET, Key=buildstore.draft_key(bid),
                              Body=json.dumps(draft, ensure_ascii=False).encode(),
                              ContentType="application/json")
        builds._table.update_item(
            Key=buildstore.meta_key(bid),
            UpdateExpression="SET rev = if_not_exists(rev, :z) + :one, lastEditor = :e, updated = :t, libraryUses = :u",
            ExpressionAttributeValues={":z": 0, ":one": 1, ":e": email or "the library", ":t": buildstore.now(),
                                       ":u": ids_in(draft)})
        kept.append({"id": bid, "name": str(meta.get("name") or "")})
    return kept


def delete(who, item_id: str) -> dict:
    table, _ = _t()
    item = get(item_id, who)
    kept = detach(item, getattr(who, "email", ""))
    sharing.drop_pointers("LIB", item_id, item.get("shares"))
    table.delete_item(Key={"pk": f"USER#{item['owner']}", "sk": f"LIB#{item_id}"})
    table.delete_item(Key={"pk": f"LIB#{item_id}", "sk": "META"})
    return {"ok": True, "name": item.get("name", ""), "kind": item.get("kind", ""), "keptIn": kept}


def set_shares(who, item_id: str, shares) -> dict:
    table, _ = _t()
    item = get(item_id, who)
    clean = sharing.normalise(shares)
    table.update_item(Key={"pk": f"LIB#{item_id}", "sk": "META"}, UpdateExpression="SET shares = :s",
                      ExpressionAttributeValues={":s": clean})
    sharing.write_pointers("LIB", item_id, str(item["owner"]), item.get("shares"), clean)
    return clean


def _migrate_policies(who) -> None:
    """Policies saved before the library had kinds (USER#<sub> / POLICY#<id>) become
    library items, once, keeping their ids."""
    table, _ = _t()
    old = table.query(KeyConditionExpression=Key("pk").eq(f"USER#{who}") & Key("sk").begins_with("POLICY#")
                      ).get("Items", [])
    for p in old:
        iid = str(p.get("id") or p["sk"][7:])
        if not _get(iid):
            table.put_item(Item={
                "pk": f"LIB#{iid}", "sk": "META", "id": iid, "kind": "policy", "name": p.get("name", ""),
                "description": p.get("description", ""), "definition": {"statement": p.get("statement", "")},
                "source": p.get("source", "cedar"),
                "owner": str(who), "ownerEmail": getattr(who, "email", ""),
                "shares": {"emails": [], "groups": [], "everyone": False},
                "created": p.get("created", buildstore.now()), "updatedAt": p.get("updatedAt", buildstore.now())})
            table.put_item(Item={"pk": f"USER#{who}", "sk": f"LIB#{iid}", "kind": "policy"})
        table.delete_item(Key={"pk": p["pk"], "sk": p["sk"]})


# --- builds that use items live ----------------------------------------------------------

def ids_in(project: dict) -> list[str]:
    wf = (project or {}).get("workflow") or {}
    out = []
    for m in MAP_KIND:
        entries = wf.get(m) if isinstance(wf.get(m), dict) else {}
        out.extend(e["library"] for e in entries.values()
                   if isinstance(e, dict) and isinstance(e.get("library"), str))
    return list(dict.fromkeys(out))


def refs_for(project: dict, who, build_owner: str = "") -> dict:
    """{id: item} for the shared items a build uses that the caller — or the build's
    owner, for someone the build is shared with — may use."""
    out = {}
    for iid in ids_in(project):
        item = _get(iid)
        if item and (sharing.can_access(item, who) or (build_owner and sharing.can_access(item, _Owner(build_owner)))):
            out[iid] = _public(item)
    return out


class _Owner(str):
    email = ""


def resolve(project: dict, refs: dict) -> dict:
    """The project with each live entry replaced by its item's definition (and a code
    tool's files), as it deploys and exports. An entry whose item is gone is left as it
    is, and the validator names it."""
    out = copy.deepcopy(project or {})
    wf = out.get("workflow") or {}
    for m, kind in MAP_KIND.items():
        entries = wf.get(m)
        if not isinstance(entries, dict):
            continue
        for key, entry in list(entries.items()):
            ref = refs.get(entry.get("library")) if isinstance(entry, dict) else None
            if not ref or ref.get("kind") != kind:
                continue
            entries[key] = copy.deepcopy(ref.get("definition") or {})
            if kind == "tool" and ref.get("files"):
                out.setdefault("toolCode", {})[key] = copy.deepcopy(ref["files"])
    return out
