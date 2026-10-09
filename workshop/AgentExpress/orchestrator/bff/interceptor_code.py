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

import json
import os
import re

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
                 "text": "Contact jane.doe@example.com or 555-123-4567 about card 4111 1111 1111 1111."}]}}}}}},
    ]


def files(point: str, templates: dict, wf: dict) -> dict:
    """The files of an interceptor generated from its checked templates."""
    templates = templates if isinstance(templates, dict) else {}
    keep = [t for t in TEMPLATES[point] if t in templates]
    settings = {t: _canonical(templates[t]) for t in keep}
    return {"handler.py": render(source(point), keep, settings),
            "requirements.txt": "",
            "events.json": _stringify(sample_events(point, wf, templates), 2) + "\n"}


def needs_headers(point: str, templates: dict) -> bool:
    """Whether a checked template reads the x-ax-* headers (Interceptors.tsx needsHeaders)."""
    templates = templates if isinstance(templates, dict) else {}
    if any(t in templates for t in HEADERS):
        return True
    agents = _setting(templates, "blockTools", "agents")
    return point == "request" and isinstance(agents, list) and len(agents) > 0
