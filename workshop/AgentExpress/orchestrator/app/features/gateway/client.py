"""MCP access through the AgentCore Gateway.

All MCP tools are reached through one authenticated Gateway endpoint. The runtime
fetches a short-lived client-credentials access token from the configured identity
provider and connects to the Gateway's MCP URL over Streamable HTTP.

There is NO simulated fallback: an unreachable or unpublished tool raises
ToolUnavailable, and a Cedar refusal raises ToolDenied (app/common/errors.py). The
framework never substitutes invented evidence for a tool that did not run.

The token request differs per IdP (GATEWAY_AUTH_FLOW, set by Terraform from the
`idp` variable) — see _token_request() below.

A successful call returns (text, "gateway"); anything else raises. The mode is kept
in the return shape because the telemetry rows and the UI record how the tool was
reached.
"""

import asyncio
import base64
import contextlib
import json
import os
import time
import urllib.parse
import urllib.request

from app.common import defaults
from app.common.config import (
    GATEWAY_AUDIENCE,
    GATEWAY_AUTH_FLOW,
    GATEWAY_CLIENT_ID,
    GATEWAY_CLIENT_SECRET,
    GATEWAY_SCOPE,
    GATEWAY_TOKEN_URL,
    GATEWAY_URL,
    GATEWAY_USER_URL,
    MCP_TIMEOUT,
    PERSON_TOOLS,
    TOOLS,
)
from app.common.errors import ToolDenied, ToolNeedsConsent, ToolUnavailable
from app.features.gateway import person

#: One cached token per client: with orchestrator.gatewayIdentity "perAgent" each agent
#: signs in as itself, so one shared slot would hand agent A's token to agent B.
_token_cache: dict[str, dict] = {}


def _credentials() -> tuple[str, str]:
    """(client id, secret) for the agent making this call: its own client when the
    deployment gives each agent one (GATEWAY_AGENT_CLIENTS), else the shared client."""
    try:
        per_agent = json.loads(os.getenv("GATEWAY_AGENT_CLIENTS") or "{}")
    except ValueError:
        per_agent = {}
    if isinstance(per_agent, dict) and per_agent:
        from app.features.observability.scope import get_scope
        own = per_agent.get(get_scope().agent_id)
        if isinstance(own, dict) and own.get("id") and own.get("secret"):
            return str(own["id"]), str(own["secret"])
    return GATEWAY_CLIENT_ID, GATEWAY_CLIENT_SECRET


def _token_request(client_id: str = "", client_secret: str = "") -> urllib.request.Request:
    """Build the client-credentials request for the configured IdP.

    The providers differ in BOTH how the client authenticates and what it asks
    for, which is why this is a branch rather than one shared request:

      cognito — the client authenticates with HTTP Basic (client_id:secret) and
                requests an OAuth2 `scope`. The resulting token carries
                `client_id` + `scope` and NO `aud`.
      auth0   — the client credentials go in the form body and it requests an
                `audience` (the API identifier). The resulting token carries
                `aud` + `azp` and no `client_id`.
      okta    — HTTP Basic, and a custom `scope` of the authorization server
                (GATEWAY_SCOPE; Okta refuses client credentials without one). The
                token carries the server's `aud` and the client as `cid`.
      entra   — the credentials in the form body, and `scope` "<api>/.default"
                (GATEWAY_SCOPE, else built from GATEWAY_AUDIENCE). A v2 token
                carries the API app's client id as `aud` and the caller as `azp`.

    The Gateway's authorizer is configured to match (see terraform/gateway.tf).
    To add another OIDC provider, add a branch here and one in identity.tf.

    HTTPS is REQUIRED, and checked rather than assumed. This request carries the
    Gateway client secret — in the Authorization header (Cognito, Okta) or in the
    form body (Auth0, Entra) — so a token URL that arrived misconfigured as `http://` would
    put those credentials on the wire in plaintext, and a `file://` one would make
    this read a local path instead. The URL comes from the IaC, so this should never
    fire; it raises rather than warns because there is no safe way to continue.
    """
    if not GATEWAY_TOKEN_URL.lower().startswith("https://"):
        raise ToolUnavailable(
            "GATEWAY_TOKEN_URL must be an https:// URL — this request carries the "
            f"Gateway client secret. Got: {GATEWAY_TOKEN_URL.split('://')[0]!r}://…")
    client_id = client_id or GATEWAY_CLIENT_ID
    client_secret = client_secret or GATEWAY_CLIENT_SECRET
    if GATEWAY_AUTH_FLOW in ("auth0", "entra"):
        asked = ({"audience": GATEWAY_AUDIENCE} if GATEWAY_AUTH_FLOW == "auth0"
                 else {"scope": GATEWAY_SCOPE or f"{GATEWAY_AUDIENCE}/.default"})
        body = urllib.parse.urlencode({
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            **asked,
        }).encode()
        return urllib.request.Request(  # noqa: S310 - https enforced above
            GATEWAY_TOKEN_URL, data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

    # Cognito (the default) and Okta: HTTP Basic and a scope. Okta's is its own
    # setting, since the audience there is the authorization server's, not a scope.
    body = urllib.parse.urlencode({
        "grant_type": "client_credentials",
        "scope": GATEWAY_SCOPE if GATEWAY_AUTH_FLOW == "okta" else GATEWAY_AUDIENCE,
    }).encode()
    credentials = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    return urllib.request.Request(  # noqa: S310 - https enforced above
        GATEWAY_TOKEN_URL, data=body,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {credentials}",
        },
    )


def _gateway_token() -> str:
    """Client-credentials token for the Gateway, for the calling agent's client (cached
    per client until expiry)."""
    now = time.time()
    client_id, client_secret = _credentials()
    slot = _token_cache.get(client_id) or {}
    if slot.get("value") and slot.get("exp", 0) - 60 > now:
        return slot["value"]
    with urllib.request.urlopen(_token_request(client_id, client_secret), timeout=10) as resp:  # noqa: S310 - https enforced in _token_request  # nosec B310
        tok = json.loads(resp.read())
    _token_cache[client_id] = {"value": tok["access_token"],
                               "exp": now + float(tok.get("expires_in", 3600))}
    return tok["access_token"]


def _error_text(e: BaseException) -> str:
    """An MCP error with everything it carries: its message, and its `data` (where the
    Gateway puts a URL elicitation) — str() alone drops the data."""
    parts = [str(e)]
    err = getattr(e, "error", None)
    data = getattr(err, "data", None) if err is not None else None
    if data is not None:
        parts.append(json.dumps(data, default=str))
    code = getattr(err, "code", None) if err is not None else None
    if code is not None:
        parts.append(f"code {code}")
    # An ExceptionGroup from the client carries the real error inside.
    parts.extend(_error_text(sub) for sub in getattr(e, "exceptions", None) or [])
    return " ".join(parts)


def _run_headers() -> dict[str, str]:
    """Which run, agent and user this call is for, as x-ax-* headers. The Gateway
    ignores them; an interceptor with passRequestHeaders on reads them (the token
    names only the machine client, never the person). Printable ASCII, capped, so a
    value can never break the header it rides in."""
    from app.features.observability.scope import get_scope
    s = get_scope()
    out = {}
    for name, value in (("x-ax-session", s.session_id), ("x-ax-agent", s.agent_id), ("x-ax-user", s.user)):
        clean = "".join(c for c in str(value or "") if " " <= c <= "~")[:256]
        if clean:
            out[name] = clean
    return out


async def _gateway_tools(as_person: bool = False):
    """Connect to a Gateway MCP endpoint and list its tools: the machine Gateway with a
    client-credentials token (shaped per GATEWAY_AUTH_FLOW, _token_request), or, for
    tools that act as the person, the person Gateway with the caller's own sign-in."""
    from langchain_mcp_adapters.client import MultiServerMCPClient

    if as_person:
        if not GATEWAY_USER_URL:
            raise ToolUnavailable("This tool acts as the person using the app, but this deployment has no "
                                  "person Gateway (GATEWAY_USER_URL): deploy it again.")
        token = person.token()
        if not token:
            raise ToolUnavailable("This tool acts as the person who started the run, and this step was "
                                  "not started by them in this app (a reviewer, a trigger, a schedule, or "
                                  "a run from the Builder console): ask them to run it in the build's own "
                                  "app, or re-run this step themselves there.")
        url = GATEWAY_USER_URL
    else:
        token = await asyncio.to_thread(_gateway_token)
        url = GATEWAY_URL
    client = MultiServerMCPClient(
        {
            "gateway": {
                "url": url,
                "transport": "streamable_http",
                "headers": {"Authorization": f"Bearer {token}", **_run_headers()},
            }
        }
    )
    return await client.get_tools()


async def _tools_for(tool_keys) -> list:
    """The published tools these workflow.json keys need, from whichever Gateway(s)
    hold them: a person tool is never listed with the machine client, nor the
    reverse."""
    keys = list(tool_keys)
    out: list = []
    if any(k not in PERSON_TOOLS for k in keys):
        out += await _gateway_tools()
    if any(k in PERSON_TOOLS for k in keys):
        out += await _gateway_tools(as_person=True)
    return out


# How much evidence one tool call may contribute, in characters. Generous by default
# (roughly 5k tokens) and tunable per deployment: too small and agents reason from
# truncated sources and report it as a limitation of the SOURCE.
MAX_EVIDENCE_CHARS = int(os.getenv("MAX_EVIDENCE_CHARS", "20000"))
# Per-result cap, so one enormous document cannot crowd out the other results.
MAX_RESULT_CHARS = int(os.getenv("MAX_RESULT_CHARS", "4000"))

# Where a result list hides, by tool. The Gateway does not normalise this: the KB
# Lambda, the Web Search connector and a Lambda target return {"results": [...]},
# while the AWS Knowledge MCP server returns {"content": {"result": [...]}}.
_RESULT_PATHS = (("results",), ("content", "result"), ("content", "results"),
                 ("result",), ("items",), ("documents",))
# The field holding a result's prose, by tool. Web Search and the KB use `text`;
# the AWS Knowledge MCP server uses `context`.
_TEXT_FIELDS = ("text", "context", "content", "snippet", "passage", "body")
_TITLE_FIELDS = ("title", "name", "documentTitle")
_URL_FIELDS = ("url", "uri", "link", "source")
_DATE_FIELDS = ("publishedDate", "published_date", "published", "lastUpdated")


def _unwrap_mcp(result):
    """Peel the MCP content envelope off a tool result.

    A tool result does NOT arrive as the payload the tool returned. MCP wraps it as a
    list of content blocks, with the real payload as a JSON *string* one level down:

        [{"type": "text", "text": "{\\"results\\": [...]}"}]

    Code that looks for a top-level dict with a "results" key therefore matches
    nothing and falls through to a generic stringify — which is how citations end up
    buried in JSON text instead of reaching the model as fields.
    """
    # A JSON string at any level: parse and recurse.
    if isinstance(result, str):
        s = result.strip()
        if s.startswith(("{", "[")):
            try:
                return _unwrap_mcp(json.loads(s))
            except ValueError:
                return result
        return result

    # The MCP envelope: a list of content blocks. Merge every text block, since a
    # large answer can be split across several.
    if isinstance(result, list):
        texts = [b.get("text") for b in result
                 if isinstance(b, dict) and b.get("type") == "text" and b.get("text")]
        if texts:
            unwrapped = [_unwrap_mcp(t) for t in texts]
            # One block is the common case; several means a genuine multi-part body.
            return unwrapped[0] if len(unwrapped) == 1 else unwrapped
        return result

    return result


def _result_list(data, row_path: str = ""):
    """The list of results inside `data`, wherever this tool chose to put it.

    `row_path` is the tool's `rowPath` from workflow.json: an explicit dot path, for a
    target that nests its records somewhere _RESULT_PATHS does not guess. It is tried
    FIRST and, when it is set, it is the only answer accepted — probing on after an
    explicit path failed would silently hand back a different list than the one the
    config named, and an agent computing over the wrong list cannot tell.
    """
    if row_path:
        node = data
        for key in row_path.split("."):
            if not isinstance(node, dict) or key not in node:
                return None
            node = node[key]
        return node if isinstance(node, list) else None
    if isinstance(data, list):
        return data
    if not isinstance(data, dict):
        return None
    for path in _RESULT_PATHS:
        node = data
        for key in path:
            if not isinstance(node, dict) or key not in node:
                node = None
                break
            node = node[key]
        if isinstance(node, list):
            return node
    return None


def _first(d: dict, fields) -> str:
    for f in fields:
        v = d.get(f)
        if v:
            return str(v).strip()
    return ""


def _extract_chunks(result) -> str:
    """Flatten a tool result into evidence text, RETAINING its citations.

    Every result becomes a numbered block carrying whichever of title / url /
    published date it actually has, so the model can attribute a finding to a real
    source. Fields a tool does not set are simply absent.

    Citations must survive for two reasons:

      1. Web Search's acceptable-use terms REQUIRE that the citations and links
         returned with each result are retained and displayed in anything shown to
         an end user.
      2. Correctness. Hand the model snippet prose with no URLs and any URL that
         appears in its output is one it INVENTED — indistinguishable from a real
         citation to whoever reads the report.

    Tools disagree about shape, so both the result-list location and the prose
    field name are looked up rather than assumed (see _RESULT_PATHS / _TEXT_FIELDS).
    A shape nothing recognises is returned as pretty JSON rather than dropped: the
    model can still read it, and truncating to nothing would hide real evidence.
    """
    data = _unwrap_mcp(result)

    # A multi-part body unwraps to a LIST OF PAYLOADS, each with its own result
    # list. Flatten them, or the outer list is mistaken for the results themselves
    # and every item looks textless.
    if (isinstance(data, list)
            and any(isinstance(d, dict) and _result_list(d) is not None for d in data)):
        results = []
        for part in data:
            results.extend(_result_list(part) or [])
    else:
        results = _result_list(data)

    if results is None:
        # Unrecognised shape. Keep it readable and keep it whole (up to the cap)
        # instead of silently cutting it to a couple of thousand characters.
        #
        # ensure_ascii=False because this string BECOMES the evidence a research
        # agent is shown, and the grounding rules test the agent's output against
        # it (app/common/rules.py). Escaping non-ASCII put "\u2013" in the evidence
        # where the source had an en dash, so an agent quoting "2–4 weeks" straight
        # off the page was told the figure was unsupported.
        text = (data if isinstance(data, str)
                else json.dumps(data, indent=2, default=str, ensure_ascii=False))
        return _clip(text, MAX_EVIDENCE_CHARS)

    # Records with no prose field at all ({"word", "score"} from a keyword API, rows
    # of figures): there is no passage to cite, so the records themselves are the
    # evidence. Dropping each as "textless" turned a full answer into nothing.
    if results and all(isinstance(r, dict) and not _first(r, _TEXT_FIELDS) for r in results):
        text = json.dumps(results, indent=1, default=str, ensure_ascii=False)
        return _clip(text, MAX_EVIDENCE_CHARS)

    blocks: list[str] = []
    used = 0
    for i, r in enumerate(results, 1):
        if isinstance(r, str):
            text, head = r.strip(), [f"[{i}]"]
        elif isinstance(r, dict):
            text = _first(r, _TEXT_FIELDS)
            head = [f"[{i}]"]
            title = _first(r, _TITLE_FIELDS)
            url = _first(r, _URL_FIELDS)
            date = _first(r, _DATE_FIELDS)
            if title:
                head.append(f"title: {title}")
            if url:
                head.append(f"url: {url}")
            if date:
                head.append(f"published: {date}")
        else:
            continue
        if not text:
            continue
        block = " | ".join(head) + "\n" + _clip(text, MAX_RESULT_CHARS)
        # Stop at the budget rather than emitting a half block, and say how many
        # results were dropped so the agent can name it as a limitation.
        if used + len(block) > MAX_EVIDENCE_CHARS:
            remaining = len(results) - (i - 1)
            blocks.append(f"[... {remaining} further result(s) omitted: evidence "
                          f"budget of {MAX_EVIDENCE_CHARS} characters reached]")
            break
        blocks.append(block)
        used += len(block)

    return "\n---\n".join(blocks) or "(no matching context)"


def _clip(s: str, limit: int) -> str:
    """Cut to `limit`, on a word boundary where possible, and SAY it was cut.

    The annotation matters: without it an agent sees evidence ending mid-word and
    cannot tell whether the SOURCE was incomplete or the framework trimmed it — and
    it reports the wrong one as a data limitation.
    """
    if len(s) <= limit:
        return s
    cut = s[:limit]
    space = cut.rfind(" ")
    if space > limit * 0.8:
        cut = cut[:space]
    return cut + f"\n[... truncated by the framework at {limit} characters]"


# Error text that means "the Gateway's Cedar policy engine refused this call",
# as opposed to a plain outage — so the UI can show "denied by policy".
_DENIED_MARKERS = ("denied", "forbidden", "not authorized", "unauthorized",
                   "accessdenied", "policy")
#: How an interceptor the Builder generated words a refusal (its _refuse()).
_INTERCEPTOR_MARKER = "refused by interceptor"


def _tool_arguments(tool_key: str, query: str) -> dict:
    """Build the call arguments for a tool, entirely from its config in TOOLS.

    Every MCP server names its parameters differently, so the parameter name is
    CONFIG, not code. A tools entry may set:

      arg   — the parameter the query goes into            (default "query")
      args  — fixed extra arguments sent on every call     (default none)

    So a server whose search tool takes `question` plus a required `repoName` is
    reachable without touching this file:

        "wiki": { "type": "mcp", "endpoint": "...", "call": "ask_question",
                  "arg": "question", "args": { "repoName": "aws/aws-cdk" } }

    Type-specific handling on top of that:
      websearch — the managed connector's documented schema: `query` (clamped to
                  200 chars) plus optional `maxResults` and a `filters` object.
      kb        — the corpus `filter` is added by retrieve(), which knows the
                  agent's corpus.
    """
    spec = TOOLS.get(tool_key) or {}
    kind = str(spec.get("type", "mcp")).lower()
    arg_name = str(spec.get("arg") or defaults.get("tool", "arg"))
    fixed = dict(spec.get("args") or {})

    if kind == "websearch":
        # The connector's parameter IS `query`, and the limit is documented.
        args: dict = {"query": query[:200]}
        max_results = spec.get("maxResults")
        if max_results:
            args["maxResults"] = int(max_results)
        # NO DOMAIN FILTER IS SENT FROM HERE, deliberately. The connector accepts one
        # per request, and this used to build it from `includeDomains`/`excludeDomains`
        # alongside a second, target-level pair named `targetIncludeDomains`/
        # `targetExcludeDomains`. Four keys for one intent, and the pair with the
        # obvious name was the weaker one: a request-level filter is supplied by the
        # caller, so the Gateway treats it as scoping rather than as a boundary, and it
        # additionally needs connector v1.2.0+. The target-level form is enforced on
        # every request, invisible to the agent, and has no version requirement — there
        # was no case where statically configuring the request-level pair bought a
        # customer anything. So `domains` is now one key, applied at the target
        # (terraform/tools.tf, cdk/lib/tool-plane.ts), and nothing about which domains
        # are allowed travels in a request this process builds.
        filters: dict = {}
        # Published-date bounds, inclusive, ISO-8601 UTC. Web results only. These stay
        # request-level because the connector target has no equivalent — there is no
        # stronger place to put them.
        date_filter = {
            k: v for k, v in (
                ("from", spec.get("publishedFrom") or ""),
                ("to", spec.get("publishedTo") or ""),
            ) if v
        }
        if date_filter:
            filters["publishedDateFilter"] = date_filter
        if filters:
            args["filters"] = filters
        args.update(fixed)
        return args

    out = {arg_name: query}
    out.update(fixed)
    return out


def _select_tool(tools, tool_key: str):
    """Pick which published tool to invoke for a workflow.json tool label.

    The Gateway names every tool "<targetName>___<toolName>". A target can publish
    SEVERAL tools (the AWS Documentation MCP server publishes five), so which to call is
    config: set `call` on the tools entry. Without it, and with more than one
    candidate, we refuse rather than guess — picking arbitrarily would silently
    call the wrong tool with the wrong arguments.
    """
    prefix = f"{tool_key.lower()}___"
    candidates = [t for t in tools if t.name.lower().startswith(prefix)]
    if not candidates:  # a target that doesn't prefix its tools
        candidates = [t for t in tools if tool_key.lower() in t.name.lower()]

    wanted = str((TOOLS.get(tool_key) or {}).get("call") or "").lower()
    if wanted:
        exact = next((t for t in candidates if t.name.lower() == f"{prefix}{wanted}"), None)
        return exact or next((t for t in candidates if wanted in t.name.lower()), None)

    if len(candidates) == 1:
        return candidates[0]
    return None


async def _query_tool_impl(tool_key: str, query: str,
                           extra_args: dict | None = None,
                           *, raw: bool = False) -> tuple[str, str]:
    """Call a Gateway tool by its workflow.json label. Returns (text, "gateway").

    `raw=True` returns the UNFLATTENED tool result instead of rendered text, for
    `query_tool_rows`. One call path either way — the auth, tool resolution, Cedar
    permit, timeout and error handling below must not be duplicated for the sake of
    a different return shape.

    Raises rather than returning placeholder text — see app/common/errors.py:
      ToolUnavailable — no Gateway configured, the tool isn't published, the call
                        failed, or the target publishes several tools and the
                        config didn't say which to use.
      ToolDenied      — the Cedar policy engine refused the call.
    """
    if not GATEWAY_URL:
        raise ToolUnavailable(
            f"Agent needs tool '{tool_key}' but no Gateway is configured (GATEWAY_URL is "
            f"empty). Deploy with enable_gateway = true / -c enableGateway=true, or remove "
            f"the `tool` binding from the agents that use it in workflow.json.")

    try:
        tools = await asyncio.wait_for(_tools_for([tool_key]), timeout=MCP_TIMEOUT)
    except ToolUnavailable:
        raise
    except Exception as e:
        raise ToolUnavailable(
            f"Could not list tools on the Gateway while calling '{tool_key}': "
            f"{type(e).__name__}: {e}") from e

    tool = _select_tool(tools, tool_key)
    if tool is None:
        available = ", ".join(sorted(t.name for t in tools)) or "(none)"
        prefix = f"{tool_key.lower()}___"
        matching = [t.name for t in tools if t.name.lower().startswith(prefix)]
        if len(matching) > 1:
            raise ToolUnavailable(
                f"Target '{tool_key}' publishes {len(matching)} tools ({', '.join(matching)}), "
                f"so workflow.json must say which to call: add \"call\": \"<toolName>\" to "
                f"tools.{tool_key}.")
        raise ToolUnavailable(
            f"The Gateway publishes no '{tool_key}' tool, so the agent has no evidence to "
            f"work from. Tools available: {available}. Check that the target reached READY, "
            f"that no policy forbids it with no condition (the Gateway hides a tool that is "
            f"always blocked), and that `call` in workflow.json matches a published name minus the "
            f"'{tool_key}___' prefix. Note the Gateway composes names as "
            f"'<target>___<tool>' and AWS also prefixes its own managed tools with "
            f"'aws___', so a name may be doubly prefixed.")

    args = _tool_arguments(tool_key, query)
    if extra_args:
        args.update(extra_args)
    result = await _invoke(tool, args)
    if raw:
        return result
    return (_extract_chunks(result), "gateway")


async def _invoke(tool, args: dict):
    """Invoke one published tool with these arguments: the timeout, and a Cedar deny
    told apart from an outage. The one invoke path for direct and model-chosen calls."""
    try:
        return await asyncio.wait_for(tool.ainvoke(args), timeout=MCP_TIMEOUT)
    except Exception as e:
        # The Gateway's request interceptor answered instead of the tool (the code
        # the Builder generates says "Refused by interceptor: <reason>").
        text = _error_text(e)
        # Each person's own account (auth "user"), not connected yet: the person
        # Gateway answers with where to connect it (URL elicitation, -32042). Checked
        # first: the words in that answer ("authorization") would read as a denial.
        url = person.consent_url(text)
        if url or "-32042" in text:
            raise ToolNeedsConsent(
                f"Connect your account for '{tool.name.split('___')[0]}' first, then run this step again: {url}",
                url=url, tool=tool.name.split("___")[0]) from e
        at = text.lower().find(_INTERCEPTOR_MARKER)
        if at >= 0:
            reason = text[at + len(_INTERCEPTOR_MARKER):].strip(" :") or "no reason given"
            raise ToolDenied(f"Tool '{tool.name}' was refused by the Gateway's request interceptor: "
                             f"{reason[:300]}", by="interceptor") from e
        # A Cedar DENY (ENFORCE mode) surfaces as an authorization error from the
        # Gateway — distinguish it from an outage so the operator knows the policy
        # is working as configured rather than something being broken.
        if any(k in text.lower() for k in _DENIED_MARKERS):
            raise ToolDenied(
                f"Tool '{tool.name}' was denied by the Cedar policy engine for this call "
                f"(arguments: {sorted(args)}). Widen the permit in the workflow.json `tools` "
                f"entry if this should be allowed.") from e
        raise ToolUnavailable(
            f"Tool '{tool.name}' call failed: {type(e).__name__}: {e}") from e


# --- tools the MODEL chooses (toolMode "model", app/common/tool_loop.py) ---------------

async def published_for(tool_keys: list[str]) -> dict[str, list]:
    """For each workflow.json tool key, the Gateway tools it publishes — narrowed to
    `call` when the entry names one, so config still decides what an agent may reach."""
    if not GATEWAY_URL:
        raise ToolUnavailable("The agent's tools need a Gateway, and GATEWAY_URL is empty.")
    try:
        tools = await asyncio.wait_for(_tools_for(tool_keys), timeout=MCP_TIMEOUT)
    except ToolUnavailable:
        raise
    except Exception as e:
        raise ToolUnavailable(f"Could not list tools on the Gateway: {type(e).__name__}: {e}") from e
    out: dict[str, list] = {}
    for key in tool_keys:
        prefix = f"{key.lower()}___"
        found = [t for t in tools if t.name.lower().startswith(prefix)]
        wanted = str((TOOLS.get(key) or {}).get("call") or "").lower()
        if wanted:
            found = ([t for t in found if t.name.lower() == prefix + wanted]
                     or [t for t in found if wanted in t.name.lower()])
        out[key] = found
    return out


def input_schema_of(tool) -> dict:
    """A published tool's JSON input schema, for the model's tool spec."""
    schema = getattr(tool, "args_schema", None)
    found = None
    if isinstance(schema, dict):
        found = schema
    elif hasattr(schema, "model_json_schema"):
        with contextlib.suppress(Exception):
            found = schema.model_json_schema()
    if not isinstance(found, dict) or found.get("type") != "object":
        return {"type": "object", "properties": {"query": {"type": "string"}}, "required": ["query"]}
    return {k: v for k, v in found.items() if k in ("type", "properties", "required", "$defs")}


async def invoke_chosen(tool_key: str, tool, model_args: dict) -> tuple[str, str]:
    """Call a tool the model chose, with the arguments it chose — except that the tool's
    fixed `args` in workflow.json win over the model's, so a page size or a scope set in
    config cannot be widened by a prompt. Same span, telemetry and policy record as a
    direct call; raises ToolDenied / ToolUnavailable like one."""
    from app.features.observability import otel
    kind = str((TOOLS.get(tool_key) or {}).get("type", "mcp")).lower()
    args = {**(model_args if isinstance(model_args, dict) else {}),
            **dict((TOOLS.get(tool_key) or {}).get("args") or {})}
    shown = json.dumps(args, ensure_ascii=False, default=str)[:1000]
    start = time.perf_counter()
    with otel.span(f"tool.{tool_key}",
                   **{"gen_ai.tool.name": tool.name, "gen_ai.operation.name": kind,
                      "gen_ai.tool.call.arguments": shown}) as sp:
        try:
            result = (_extract_chunks(await _invoke(tool, args)), "gateway")
        except ToolDenied:
            _record_policy(tool_key, "denied", int((time.perf_counter() - start) * 1000))
            raise
        if sp is not None:
            with contextlib.suppress(Exception):
                sp.set_attribute("gen_ai.tool.mode", result[1])
    latency_ms = int((time.perf_counter() - start) * 1000)
    _record_tool(tool_key, shown, latency_ms, result)
    _record_policy(tool_key, result[1], latency_ms)
    return result


def extract_rows(result, row_path: str = "") -> list[dict]:
    """A tool result as its LIST OF RECORDS, before it is flattened to prose.

    `_extract_chunks` renders a result for a model to read. This returns the same
    result as data, for an agent that COMPUTES its output instead of generating it
    (see app/subagents/cost_research/agent.py). A model handed a table of numbers
    will eventually do arithmetic nobody asked for — multiplying a real price by an
    assumed volume, or subtracting two timestamps — and report the result as though
    it had been measured. Where the output is a transformation of the rows, doing
    the transformation in code removes the opportunity instead of forbidding it.

    Such an agent needs the tool's fields, not its rendering. Reading them back out
    of the prose would couple the agent to one tool's exact sentence shape, which is
    the opposite of this framework's premise: a tool is a config entry, so pointing
    it at a different Lambda, warehouse or API must not require an agent code
    change. The FIELD NAMES are config too — see the tool's `rowFields` block in
    workflow.json.

    Returns [] when the tool returned no recognisable list, which the caller must
    treat as "nothing to report", never as "there is nothing".
    """
    data = _unwrap_mcp(result)
    if (isinstance(data, list)
            and any(isinstance(d, dict) and _result_list(d, row_path) is not None
                    for d in data)):
        rows: list = []
        for part in data:
            rows.extend(_result_list(part, row_path) or [])
    else:
        rows = _result_list(data, row_path) or []
    return [r for r in rows if isinstance(r, dict)]


async def query_tool_rows(tool_key: str, query: str) -> tuple[list[dict], str]:
    """Call a tool and return (rows, mode) instead of (text, mode).

    Same call, same Cedar permit, same telemetry as `query_tool` — only the shape
    handed back differs. Raises ToolUnavailable / ToolDenied identically, so a
    deterministic agent still fails loudly rather than reporting an empty result
    as though the source were empty.
    """
    from app.features.observability import otel
    kind = str((TOOLS.get(tool_key) or {}).get("type", "mcp")).lower()
    start = time.perf_counter()
    # No `gen_ai.tool.mode` attribute here: this path only ever reaches the Gateway
    # (it raises otherwise), so the value would be a constant. `query_tool` sets it
    # because its result carries a real mode.
    with otel.span(f"tool.{tool_key}",
                   **{"gen_ai.tool.name": tool_key,
                      "gen_ai.operation.name": kind,
                      "gen_ai.tool.mode": "gateway"}):
        result = await _query_tool_impl(tool_key, query, raw=True)
    latency_ms = int((time.perf_counter() - start) * 1000)
    rows = extract_rows(result, str((TOOLS.get(tool_key) or {}).get("rowPath") or ""))
    # Record the rendered text so the timeline/observability view of this call looks
    # the same as any other tool call — a reviewer should not have to know which
    # agents compute and which generate to read the trace.
    _record_tool(tool_key, query, latency_ms, (_extract_chunks(result), "gateway"))
    _record_policy(tool_key, "gateway", latency_ms)
    return rows, "gateway"


def _kb_tool_key() -> str:
    """The workflow.json label of the tool declared with type="kb".

    RAISES when there is none, rather than falling back to a label. The fallback was
    the literal "kb" — which is THIS SAMPLE's key for it, so a customer who called
    theirs `policies` and then called `ctx.retrieve()` from an agent with no kb tool
    declared got a Gateway call for a tool named "kb" that does not exist, and an
    empty retrieval reported as a successful one.

    Both IaC paths already reject a `corpus` on an agent whose tool is not a kb, so
    reaching here without one means the agent called `ctx.retrieve()` without being
    bound to a Knowledge Base at all. That is an agent-code mistake and it deserves
    to say so.
    """
    key = next((k for k, v in TOOLS.items() if str(v.get("type", "")).lower() == "kb"), "")
    if not key:
        raise ToolUnavailable(
            "ctx.retrieve() needs a tool declared with type=\"kb\" in workflow.json, and "
            "this workflow has none. Declare one (its key is yours to choose) and bind "
            "the agent to it with `tool`, or gather evidence with ctx.call_tool() "
            "against a tool that does exist.")
    return key


# --- observability wrappers -------------------------------------------------
# Public entry points: open an OTEL span, time the call, and record a telemetry
# row (both best-effort), then return exactly what the impl returned. Metering
# never alters behaviour.

def _record_tool(provider: str, query: str, latency_ms: int, result: tuple[str, str]) -> None:
    with contextlib.suppress(Exception):  # metering must never break a call
        from app.features.observability import meter
        text = result[0] if isinstance(result, tuple) and result else ""
        # The tool's declared TYPE as well as its label: the embedding charge for a
        # retrieval depends on the kind of tool, and the label is whatever the
        # customer named it. See meter.record_tool.
        tool_type = str((TOOLS.get(provider) or {}).get("type") or "").lower()
        meter.record_tool(provider=provider, tool_type=tool_type,
                          query=query, latency_ms=latency_ms,
                          mode=(result[1] if isinstance(result, tuple) and len(result) > 1 else "gateway"),
                          result_text=text)


def _record_policy(tool: str, result_mode: str, latency_ms: int,
                   filter_value: str | None = None) -> None:
    """Record the Gateway Cedar-policy decision for a tool call. Only when a real
    Gateway (and thus a policy engine) is in play."""
    pmode = os.getenv("GATEWAY_POLICY_MODE", "")
    if not pmode:
        return  # no policy engine attached
    if result_mode == "denied":
        decision = "denied"
    elif result_mode == "gateway":
        decision = "log-only" if pmode.upper() == "LOG_ONLY" else "allowed"
    else:
        return  # gateway not used -> no policy evaluation happened
    with contextlib.suppress(Exception):
        from app.features.observability import meter
        meter.record_policy(tool=tool, decision=decision,
                            filter_value=filter_value or "", mode=pmode,
                            latency_ms=latency_ms)


async def query_tool(tool_key: str, query: str) -> tuple[str, str]:
    """Call the tool an agent is bound to (its `tool` label in workflow.json)."""
    from app.features.observability import otel
    kind = str((TOOLS.get(tool_key) or {}).get("type", "mcp")).lower()
    start = time.perf_counter()
    with otel.span(f"tool.{tool_key}",
                   **{"gen_ai.tool.name": tool_key, "gen_ai.operation.name": kind}) as sp:
        try:
            result = await _query_tool_impl(tool_key, query)
        except ToolDenied:
            # Recorded like a model-chosen call's refusal (invoke_chosen): without it a
            # refused web search left no trace in Observability, so a policy that was
            # working looked like one that had matched nothing.
            _record_policy(tool_key, "denied", int((time.perf_counter() - start) * 1000))
            raise
        if sp is not None:
            with contextlib.suppress(Exception):
                sp.set_attribute("gen_ai.tool.mode", result[1])
    latency_ms = int((time.perf_counter() - start) * 1000)
    _record_tool(tool_key, query, latency_ms, result)
    # Cedar action id for a target-level permit is the target name itself; for a
    # tool-specific permit it is "<target>___<tool>". Report the target, which is
    # what the generated policy is keyed on.
    _record_policy(tool_key, result[1], latency_ms)
    return result


async def retrieve(query: str, doc_type: str | None = None) -> tuple[str, str]:
    """Retrieve grounded context from the Knowledge Base tool via the Gateway.

    `doc_type` scopes retrieval to one corpus (a top-level folder under kb_docs/).
    The generated Cedar permit restricts which corpora are allowed, so a call with
    a corpus outside the declared list is denied server-side.
    """
    from app.features.observability import otel
    tool_key = _kb_tool_key()
    extra = {"filter": doc_type} if doc_type else None
    start = time.perf_counter()
    with otel.span(f"tool.{tool_key}.retrieve",
                   **{"gen_ai.tool.name": tool_key, "gen_ai.operation.name": "retrieve",
                      "kb.doc_type": doc_type}) as sp:
        result = await _query_tool_impl(tool_key, query, extra_args=extra)
        if sp is not None:
            with contextlib.suppress(Exception):
                sp.set_attribute("gen_ai.tool.mode", result[1])
    latency_ms = int((time.perf_counter() - start) * 1000)
    _record_tool(tool_key, query, latency_ms, result)
    _record_policy(f"{tool_key}___retrieve", result[1], latency_ms, filter_value=doc_type)
    return result
