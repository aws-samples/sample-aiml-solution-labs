"""Gateway interceptors written in the build, generated from their templates on the server
(for the Assistant, which edits the draft without the page). A port of
web/src/builder/interceptorCode.ts `interceptorFiles`: the same template sources and the
same output byte for byte, so code generated here is not taken for code edited by hand
when the Interceptors tab compares the two. Checked against what the page generates:
tests/fixtures/interceptor_cases.json (handler.py, written by interceptorCode.test.ts)
and interceptor_files_cases.json (every file, by interceptorCode.parity.test.ts).

The sources are web/src/builder/interceptor-templates/{request,response}.py, staged into
the BFF package as interceptor_templates/ (cdk stageBffPackage, terraform/bff.tf)."""
from __future__ import annotations

import ast
import json
import os
import re
import sys
import textwrap

POINTS = ("request", "response")
#: TEMPLATES in interceptorCode.ts: each point's templates, in that order.
TEMPLATES = {
    "request": ["audit", "blockTools", "argumentGuard", "injectContext", "custom"],
    "response": ["audit", "redactPii", "hideTools", "capResult", "custom"],
}
#: Templates that read the x-ax-* headers (TemplateDef.headers), so need passRequestHeaders.
HEADERS = {"injectContext"}

_HERE = os.path.dirname(os.path.abspath(__file__))
_DIRS = (os.path.join(_HERE, "interceptor_templates"),
         os.path.join(_HERE, "..", "web", "src", "builder", "interceptor-templates"))
_OPEN = re.compile(r"^\s*# >>> (\w+)\s*$")
_CLOSE = re.compile(r"^\s*# <<< (\w+)\s*$")


def source(point: str) -> str:
    for d in _DIRS:
        path = os.path.join(d, f"{point}.py")
        if os.path.isfile(path):
            with open(path, encoding="utf-8") as f:
                return f.read()
    raise FileNotFoundError(f"interceptor template {point}.py")


def _js(v):
    """A value as JSON.stringify writes it: a whole float is an integer."""
    if isinstance(v, float) and v.is_integer():
        return int(v)
    if isinstance(v, list):
        return [_js(x) for x in v]
    if isinstance(v, dict):
        return {k: _js(x) for k, x in v.items()}
    return v


def _stringify(v, indent: int) -> str:
    return json.dumps(_js(v), indent=indent, ensure_ascii=False)


def _canonical(v):
    if isinstance(v, list):
        return [_canonical(x) for x in v]
    if isinstance(v, dict):
        return {k: _canonical(v[k]) for k in sorted(v)}
    return v


def render(text: str, keep: list[str], settings: dict) -> str:
    """A template source with only the `keep` sections, and SETTINGS set to `settings`."""
    out: list[str] = []
    opened: list[str] = []
    for line in text.split("\n"):
        o = _OPEN.match(line)
        if o:
            opened.append(o.group(1))
            if o.group(1) == "settings":
                out.append(f'SETTINGS = json.loads(r"""\n{_stringify(settings, 4)}\n""")')
            continue
        if _CLOSE.match(line):
            if opened:
                opened.pop()
            continue
        if "settings" in opened:
            continue
        if all(s in keep for s in opened):
            out.append(line)
    body = re.sub(r"\n{4,}", "\n\n\n", "\n".join(out))
    return re.sub(r"\n+$", "", body) + "\n"


def _first_name(tool) -> str | None:
    schema = tool.get("toolSchema") if isinstance(tool, dict) and isinstance(tool.get("toolSchema"), list) else []
    first = schema[0] if schema else None
    return first["name"] if isinstance(first, dict) and isinstance(first.get("name"), str) else None


def _sample_tool(wf: dict, prefer) -> str:
    if isinstance(prefer, list) and prefer and isinstance(prefer[0], str) and prefer[0]:
        p = prefer[0]
        return p if "___" in p else f"{p}___{_first_name((wf.get('tools') or {}).get(p)) or 'search'}"
    key, tool = next(iter((wf.get("tools") or {}).items()), ("mytool", {}))
    return f"{key}___{_first_name(tool) or 'search'}"


def _setting(templates: dict, tid: str, k: str):
    t = templates.get(tid)
    return t.get(k) if isinstance(t, dict) else None


def sample_events(point: str, wf: dict, templates: dict) -> list:
    """Test events for the sandbox: what the Gateway sends at this point."""
    agent = next(iter(wf.get("agents") or {}), "intake")
    headers = {"x-ax-session": "sample-session", "x-ax-agent": agent, "x-ax-user": "user@example.com"}
    prefer = _setting(templates, "blockTools", "tools")
    tool = _sample_tool(wf, prefer if prefer is not None else _setting(templates, "hideTools", "tools"))

    def request(body):
        return {"path": "/mcp", "httpMethod": "POST", "headers": headers, "body": body}
    list_body = {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}
    call_body = {"jsonrpc": "2.0", "id": 2, "method": "tools/call",
                 "params": {"name": tool, "arguments": {"query": "example"}}}
    if point == "request":
        return [
            {"name": "tools/list", "event": {"interceptorInputVersion": "1.0",
                                             "mcp": {"gatewayRequest": request(list_body)}}},
            {"name": f"a call to {tool}", "event": {"interceptorInputVersion": "1.0",
                                                    "mcp": {"gatewayRequest": request(call_body)}}},
        ]
    return [
        {"name": "tools/list answer", "event": {"interceptorInputVersion": "1.0", "mcp": {
            "gatewayRequest": request(list_body),
            "gatewayResponse": {"statusCode": 200, "body": {"jsonrpc": "2.0", "id": 1, "result": {"tools": [
                {"name": tool, "description": "A tool", "inputSchema": {"type": "object"}},
                {"name": "other___tool", "description": "Another", "inputSchema": {"type": "object"}}]}}}}}},
        {"name": f"{tool} answer", "event": {"interceptorInputVersion": "1.0", "mcp": {
            "gatewayRequest": request(call_body),
            "gatewayResponse": {"statusCode": 200, "body": {"jsonrpc": "2.0", "id": 2, "result": {"content": [
                {"type": "text",
                 "text": "Contact jane.doe@example.com or 202-555-0123 about card 4111 1111 1111 1111."}]}}}}}},
    ]


#: ALWAYS in interceptorCode.ts: sections every generated handler keeps, whatever is
#: checked. response.py's `protocol` guard lets the steps change only a tool's result,
#: never an MCP protocol field (initialize's protocolVersion, capabilities, serverInfo;
#: jsonrpc, id), and GUARDED names what in it the Assistant may not rewrite.
ALWAYS = ["protocol"]
GUARDED = {"response": ("lambda_handler", "_keep_protocol")}


def _handler(point: str, templates: dict, sections: list[str]) -> str:
    templates = templates if isinstance(templates, dict) else {}
    keep = [t for t in TEMPLATES[point] if t in templates]
    settings = {t: _canonical(templates[t]) for t in keep}
    return render(source(point), [*keep, *sections], settings)


def files(point: str, templates: dict, wf: dict) -> dict:
    """The files of an interceptor generated from its checked templates."""
    templates = templates if isinstance(templates, dict) else {}
    return {"handler.py": _handler(point, templates, ALWAYS),
            "requirements.txt": "",
            "events.json": _stringify(sample_events(point, wf, templates), 2) + "\n"}


def _custom_node(tree):
    found = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "custom"]
    return found[0] if len(found) == 1 else None


def custom_function(handler: str) -> str | None:
    """The source of handler.py's top-level custom() function, or None when it has none."""
    try:
        tree = ast.parse(handler)
    except SyntaxError:
        return None
    node = _custom_node(tree)
    if node is None:
        return None
    lines = handler.splitlines(keepends=True)
    start = min([node.lineno, *(d.lineno for d in node.decorator_list)])
    return "".join(lines[start - 1:node.end_lineno]).rstrip("\n") + "\n"


def _defines(n) -> list[str] | None:
    """The names a top-level statement binds, or None when it is not a definition."""
    if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        return [n.name]
    if isinstance(n, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
        targets = n.targets if isinstance(n, ast.Assign) else [n.target]
        return [x.id for t in targets for x in ast.walk(t) if isinstance(x, ast.Name)]
    if isinstance(n, (ast.Import, ast.ImportFrom)):
        return [a.asname or a.name.split(".")[0] for a in n.names]
    return None


def with_custom(handler: str, code: str, generated: str | None = None) -> str:
    """`handler` with its custom() function replaced by `code` (a whole `def custom(...)`,
    with helpers of its own if it needs them); the rest of the file as it was. `generated`
    is what its templates generate: a helper an earlier custom() added (not in it) is
    replaced by one of the same name. Raises ValueError when `code` is not that, or would
    redefine anything else in the handler."""
    try:
        tree = ast.parse(handler)
    except SyntaxError as e:
        raise ValueError(f"handler.py does not parse: {e.msg} (line {e.lineno})") from None
    old = _custom_node(tree)
    if old is None:
        raise ValueError("handler.py has no custom() function (it was edited by hand: "
                         "generate it again from its templates first)")
    code = textwrap.dedent(str(code or "")).strip("\n")
    try:
        mine = ast.parse(code)
    except SyntaxError as e:
        raise ValueError(f"custom() does not parse: {e.msg} (line {e.lineno})") from None
    new = _custom_node(mine)
    if new is None:
        raise ValueError("`code` must define exactly one top-level `def custom(...)`")
    if [a.arg for a in new.args.args] != [a.arg for a in old.args.args]:
        raise ValueError("custom() must keep its signature: custom("
                         + ", ".join(a.arg for a in old.args.args) + ")")
    mine_names: set[str] = set()
    for n in mine.body:
        names = _defines(n)
        if names is None:
            raise ValueError("`code` may hold only custom(), helper functions, imports and constants")
        mine_names |= set(names)
    base = ast.parse(generated) if generated else tree
    fixed = {x for n in base.body if not (isinstance(n, ast.FunctionDef) and n.name == "custom")
             and not isinstance(n, (ast.Import, ast.ImportFrom)) for x in (_defines(n) or [])}
    clash = sorted((mine_names - {"custom"}) & fixed)
    if clash:
        raise ValueError(f"`code` redefines {clash[0]!r}, which the rest of handler.py defines")
    # Helpers an earlier custom() added, defined again now: the old ones go.
    drop = [old] + [n for n in tree.body if n is not old and not isinstance(n, (ast.Import, ast.ImportFrom))
                    and (names := _defines(n)) and set(names) <= mine_names and not set(names) & fixed]
    # A standard-library module the code uses (re, json...) that neither it nor the
    # handler imports is imported just before it, so it never fails on a NameError.
    bound = {a.asname or a.name.split(".")[0] for t in (tree, mine) for n in ast.walk(t)
             if isinstance(n, (ast.Import, ast.ImportFrom)) for a in n.names}
    used = {n.id for n in ast.walk(mine) if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)}
    need = sorted(m for m in used - bound - fixed - mine_names if m in sys.stdlib_module_names)
    if need:
        code = "".join(f"import {m}\n" for m in need) + "\n\n" + code
    lines = handler.splitlines(keepends=True)
    spans = sorted((min([n.lineno, *(d.lineno for d in getattr(n, "decorator_list", []))]), n.end_lineno, n is old)
                   for n in drop)
    out, at = [], 0
    for start, end, is_old in spans:
        out.append("".join(lines[at:start - 1]))
        if is_old:
            out.append(code + "\n")
        at = end
    out.append("".join(lines[at:]))
    return "".join(out)


def is_generated(point: str, templates: dict, handler: str) -> bool:
    """isGenerated in interceptorCode.ts: `handler` is what these templates generate, now or
    before the ALWAYS sections existed (a build saved then)."""
    return handler in (_handler(point, templates, ALWAYS), _handler(point, templates, []))


def guard_problem(point: str, handler: str, templates: dict) -> str:
    """Why `handler` (code the Assistant wrote for this point) would let the steps change
    MCP protocol fields, or "" when it keeps the generated guard.

    The functions in GUARDED must be in it once each, exactly as generated, and nothing
    else at the top level may bind those names or `copy`: then whatever the other
    functions (custom included) do to an answer, lambda_handler hands back a tool's result
    and nothing else changed."""
    import ast
    names = GUARDED.get(point)
    if not names:
        return ""
    try:
        tree = ast.parse(handler)
    except SyntaxError as e:
        return f"handler.py does not parse: {e.msg} (line {e.lineno})"
    want = {n.name: ast.dump(n) for n in ast.parse(_handler(point, templates, ALWAYS)).body
            if isinstance(n, ast.FunctionDef) and n.name in names}
    defs: dict[str, list[str]] = {}
    for node in tree.body:                       # the module's top level
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            defs.setdefault(node.name, []).append(ast.dump(node))
            continue
        bound: list[tuple[str, bool]] = []       # (name, is it `import copy` itself)
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            bound = [(a.asname or a.name.split(".")[0], isinstance(node, ast.Import) and a.name == "copy"
                      and not a.asname) for a in node.names]
        elif isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            bound = [(n.id, False) for t in targets for n in ast.walk(t) if isinstance(n, ast.Name)]
        for name, plain_import in bound:
            if name in names or (name == "copy" and not plain_import):
                return f"handler.py rebinds {name!r}, which the generated protocol guard needs"
    for name in names:
        if defs.get(name) != [want[name]]:
            return (f"handler.py must keep the generated {name}() exactly as it is: it lets the steps change "
                    "only a tool's result and never an MCP protocol field (initialize's protocolVersion, "
                    "capabilities, serverInfo; jsonrpc, id)")
    return ""


def needs_headers(point: str, templates: dict) -> bool:
    """Whether a checked template reads the x-ax-* headers (Interceptors.tsx needsHeaders)."""
    templates = templates if isinstance(templates, dict) else {}
    if any(t in templates for t in HEADERS):
        return True
    agents = _setting(templates, "blockTools", "agents")
    return point == "request" and isinstance(agents, list) and len(agents) > 0
