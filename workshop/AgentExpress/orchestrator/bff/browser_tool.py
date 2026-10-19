"""The Browser tool (a type="lambda" tool with `source: "browser"`): what its config may say.
For validate_build.py.

The function (app/tools/browser/handler.py) publishes one tool, `open_page`, and reads one
setting, `allowDomains`, which reaches it as ALLOW_DOMAINS. A malformed domain makes the
function refuse every call (it fails closed), so the list is checked here first, where the
person writing it can see why. Mirrors web/src/builder/browserTool.ts word for word (both
run tests/fixtures/validation_cases.json).
"""
from __future__ import annotations

import re

SOURCE = "browser"
#: The one tool the function publishes (handler.py _TOOLS).
TOOL = "open_page"
MAX_DOMAINS = 50
#: The same expression as app/tools/browser/handler.py DOMAIN_RE and browserTool.ts.
DOMAIN_RE = re.compile(r"^(?=.{1,253}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+(?:[a-z]{2,63}|xn--[a-z0-9-]{1,59})$")


def check(tool: dict, path: str, where: dict, add) -> None:
    """Add an issue for each thing wrong with a lambda tool's Browser settings."""
    is_browser = tool.get("source") == SOURCE
    domains = tool.get("allowDomains")
    if "allowDomains" in tool and not is_browser:
        add("error", where, f"{path}.allowDomains", 'only the Browser tool (source "browser") reads allowDomains')
    if not is_browser:
        return
    if isinstance(domains, list):
        if len(domains) > MAX_DOMAINS:
            add("error", where, f"{path}.allowDomains", f"at most {MAX_DOMAINS} domains")
        for i, d in enumerate(domains):
            if isinstance(d, str) and not DOMAIN_RE.match(d):
                add("error", where, f"{path}.allowDomains[{i}]",
                    f'"{d}" is not a domain name: write it like docs.aws.amazon.com, with no https://, path, port or *')
    schema = tool.get("toolSchema") if isinstance(tool.get("toolSchema"), list) else []
    for i, ts in enumerate(schema):
        if isinstance(ts, dict) and isinstance(ts.get("name"), str) and ts["name"] and ts["name"] != TOOL:
            add("error", where, f"{path}.toolSchema[{i}].name", f'the Browser function publishes one tool, "{TOOL}"')
