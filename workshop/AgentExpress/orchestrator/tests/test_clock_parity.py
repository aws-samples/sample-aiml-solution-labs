"""bff/clock.py duplicates app/common/clock.py. Enforce it, don't ask for it.

Every stored and displayed timestamp in this project comes from one of these two
modules, and they have to agree: the runtime writes a run's timestamps, the BFF
Lambda reads and renders them, and a run whose events are stamped an hour apart from
its status row is a debugging trap.

They cannot share an import — the BFF ships as its own zip and has no `app` package —
so the code is duplicated on purpose. What was NOT working is how that was managed:
a comment in `app/common/clock.py` claimed the two were "byte-for-byte" copies and
asked the reader to keep them in sync. They were not byte-for-byte (the docstrings
had already diverged), so the claim was false, and nothing would have caught the
logic diverging either.

This compares the two as ASTs with docstrings stripped: the LOGIC must be identical,
the prose may differ.
"""

from __future__ import annotations

import ast

from conftest import ORCH_ROOT


def _code_only(path) -> str:
    """The module's AST, docstrings removed, as a comparable string."""
    tree = ast.parse(path.read_text())
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef,
                             ast.ClassDef)):
            body = node.body
            if (body and isinstance(body[0], ast.Expr)
                    and isinstance(body[0].value, ast.Constant)
                    and isinstance(body[0].value.value, str)):
                node.body = body[1:]
    return ast.dump(tree, indent=1)


def test_the_bff_clock_is_the_same_code_as_the_runtime_clock():
    runtime = _code_only(ORCH_ROOT / "app" / "common" / "clock.py")
    bff = _code_only(ORCH_ROOT / "bff" / "clock.py")
    assert runtime == bff, (
        "app/common/clock.py and bff/clock.py have diverged. They must stay identical "
        "in behaviour: the runtime stamps a run's timestamps and the BFF renders them, "
        "so a difference shows up as a run whose events and status disagree about the "
        "time. Copy the changed function across (docstrings may differ, code may not)."
    )


def test_both_expose_the_same_public_surface():
    """A caller of either must find the same names — the BFF imports `clock.now_str`
    and `clock.today_str` the same way the runtime does."""
    names = {}
    for label, path in (("runtime", ORCH_ROOT / "app" / "common" / "clock.py"),
                        ("bff", ORCH_ROOT / "bff" / "clock.py")):
        tree = ast.parse(path.read_text())
        names[label] = {n.name for n in tree.body
                        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
                        and not n.name.startswith("_")}
    assert names["runtime"] == names["bff"], names
    assert {"now_str", "today_str"} <= names["runtime"]
