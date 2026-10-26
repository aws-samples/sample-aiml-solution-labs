"""Build storage checks (`orchestrator.storage` and the `${storage}` token), for
validate_build.py.

The generic key checks (app/keys.json) already say `storage` takes `create` and `retain`,
both true or false. These are the rest. The token (app/vocabulary.json storageToken) stands
for s3://<the build's bucket> and is resolved by the IaC only in the fields it lists
(cdk/lib/storage.ts resolveStorage, terraform/storage.tf), so a value carrying it is refused:
  - anywhere else in the tools, the orchestrator block or a task step: it would deploy as
    the literal text;
  - when the build creates no storage (orchestrator.storage.create is not true);
  - unless it starts the value, once, followed by / or nothing.
`retain` without `create` keeps nothing, which is said rather than ignored.

Mirrors web/src/builder/storageCheck.ts word for word (both run
tests/fixtures/storage_cases.json).
"""

from __future__ import annotations

#: A valid bucket name the token's place is checked as, by the S3 patterns that do not know
#: the token (validate_build.py _check_code, the kb tool's s3Uri).
STAND_IN = "s3://build-storage"


def _is_obj(v) -> bool:
    return isinstance(v, dict)


def creates(wf) -> bool:
    """Does the workflow create the build's storage?"""
    orch = wf.get("orchestrator") if _is_obj(wf) and _is_obj(wf.get("orchestrator")) else {}
    st = orch.get("storage") if _is_obj(orch.get("storage")) else {}
    return st.get("create") is True


def as_checked(value, token: str):
    """The value with a leading token in the stand-in's place, for a check of its shape."""
    return STAND_IN + value[len(token):] if isinstance(value, str) and value.startswith(token) else value


def _strings(node, at: list):
    """Every string under node, as (path segments, value), in document order."""
    if isinstance(node, str):
        yield at, node
    elif isinstance(node, dict):
        for k, v in node.items():
            yield from _strings(v, [*at, k])
    elif isinstance(node, list):
        for i, v in enumerate(node):
            yield from _strings(v, [*at, i])


def _dotted(at: list) -> str:
    return "".join(f"[{k}]" if isinstance(k, int) else (f".{k}" if i else str(k)) for i, k in enumerate(at))


def _matches(field: str, at: list) -> bool:
    parts = field.split(".")
    return len(parts) == len(at) and all(p == "*" or p == str(k) for p, k in zip(parts, at, strict=True))


def check(wf: dict, add, vocab, vocab_extra) -> None:
    token = (vocab("storageToken") or [""])[0]
    fields = list(vocab_extra("storageToken", "fields") or [])
    orch = wf.get("orchestrator") if _is_obj(wf.get("orchestrator")) else {}
    st = orch.get("storage") if _is_obj(orch.get("storage")) else None
    block = {"kind": "block", "name": "orchestrator"}
    if st is not None and st.get("retain") is True and st.get("create") is not True:
        add("warning", block, "orchestrator.storage.retain",
            "keeps nothing: the build creates no storage unless orchestrator.storage.create is true")
    on = creates(wf)
    roots: list = [(["tools"], wf.get("tools") if _is_obj(wf.get("tools")) else {}),
                   (["orchestrator"], {k: v for k, v in orch.items() if k != "storage"})]
    agents = wf.get("agents") if _is_obj(wf.get("agents")) else {}
    for aid, a in agents.items():
        if _is_obj(a) and "task" in a:
            roots.append((["agents", aid, "task"], a["task"]))
    for base, node in roots:
        for at, value in _strings(node, base):
            if token not in value:
                continue
            where = ({"kind": "tool", "id": at[1]} if at[0] == "tools" and len(at) > 1
                     else {"kind": "agent", "id": at[1]} if at[0] == "agents" else block)
            p = _dotted(at)
            if not any(_matches(f, at) for f in fields):
                add("error", where, p, f"{token} is read only in: {', '.join(fields)}")
            elif not on:
                add("error", where, p, f"uses {token}, but this build creates no storage: "
                    "set orchestrator.storage.create to true")
            elif not (value.startswith(token) and value.count(token) == 1
                      and (len(value) == len(token) or value[len(token)] == "/")):
                add("error", where, p, f"must start with {token}/ (it stands for s3://<the build's bucket>), once")
