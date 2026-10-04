"""A2AAgent — the node body for a step run by an agent THIS WORKFLOW DOES NOT OWN
(workflow.json `runtime: "a2a"`).

`main` and `dedicated` both run code that ships in this image. This is the third
placement and the only one where the agent is somebody else's service: a partner's,
another team's, or a managed one. It is reached over the Agent2Agent protocol at its
Agent Card URL, so nothing about it lives in this repo.

    "credit_check": {
      "name": "Partner Credit Check",
      "runtime": "a2a",
      "agentCard": "https://agents.partner.example/credit",
      "auth": "bearer",
      "produces": "credit-assessment"
    }

WHAT A2A ACTUALLY GIVES US, AND WHAT IT DOES NOT
It is transport and discovery: how to find an agent and how to hand it a task. It
carries no opinion about WHICH agent to call — that stays this framework's job
(`steps` for the fixed order, `branch` for a decision from an output, a review gate
for a decision from a human). So an a2a agent slots into the topology exactly like
any other and every one of those mechanisms works on it unchanged.

THE WIRE
  1. GET  <card>/.well-known/agent-card.json   discovery; tells us where to POST
  2. POST <url>  {"jsonrpc":"2.0","method":"message/send","params":{"message":…}}
  3. the result is either a Message (answered inline) or a Task, and a Task may
     still be running — so poll `tasks/get` until it reaches a terminal state
  4. read the text parts out of the artifacts

Two shapes of the same thing have to be tolerated throughout, because the spec
renamed them and deployed agents lag: task states arrive as either `completed` or
`TASK_STATE_COMPLETED`, and a text part as either `{"kind":"text","text":…}` or bare
`{"text":…}`. Normalising on the way in is three lines; guessing wrong is a run that
hangs on a task that already finished.

WHAT DEGRADES, AND IT IS WORTH KNOWING
The framework wraps this call in OUR container, so guardrails and long-term memory
still apply to it. Two things cannot:
  * evaluations fall back to a role descriptor, because there is no local model call
    to capture a prompt from — the reasoning happened on their side;
  * any tool the remote agent uses is outside this deployment's Cedar policy. You are
    trusting their boundary, not enforcing yours.

NO NEW DEPENDENCY. stdlib urllib, https enforced, the blocking call moved to a
thread — the same shape as app/features/gateway/client.py, for the same reason: this
request can carry a bearer token.
"""

from __future__ import annotations

import asyncio
import json
import os
import urllib.error
import urllib.request
import uuid

from app.common.base import Agent
from app.common.config import A2A_INVOKE, AGENTS
from app.common.errors import RemoteAgentUnavailable

#: agent_id -> bearer token, injected by the IaC (never written in workflow.json).
_TOKENS: dict = json.loads(os.getenv("A2A_TOKENS", "{}"))

#: agent_id -> endpoint, for an agent whose URL is only known AFTER a deploy (a Lambda
#: Function URL). Injected by the IaC, exactly like AGENT_RUNTIME_ARNS. See `source`.
_ENDPOINTS: dict = json.loads(os.getenv("A2A_ENDPOINTS", "{}"))

_CARD_PATH = "/.well-known/agent-card.json"
#: Sent so a version-aware agent can reject us explicitly instead of misreading the
#: request. Agents that predate the header ignore it.
_PROTOCOL_VERSION = "0.3"

# Task lifecycle. The spec moved from lower-case-hyphen names to a TASK_STATE_*
# enum, and both are in the wild, so compare on a normalised form.
_TERMINAL_OK = frozenset({"completed"})
_TERMINAL_BAD = frozenset({"failed", "canceled", "cancelled", "rejected", "unknown"})
#: States that will never advance on their own — the agent is waiting on somebody.
#: Polling through these is how a run silently burns its whole timeout budget.
_BLOCKED = frozenset({"input-required", "inputrequired", "auth-required", "authrequired"})


def _state(status: object) -> str:
    """A task's state, normalised: `TASK_STATE_COMPLETED` and `completed` both -> completed."""
    raw = status.get("state") if isinstance(status, dict) else status
    s = str(raw or "").strip().lower()
    return s.removeprefix("task_state_").replace("_", "-")


def _require_https(url: str, what: str) -> str:
    """Reject a non-https endpoint rather than sending a token over plaintext.

    Checked, not assumed. The URL comes from `workflow.json`, which is a file a
    customer edits by hand, and a mistyped `http://` here would put the bearer token
    for someone else's agent on the wire. `file://` would make urllib read a local
    path instead of making a request at all.
    """
    if not str(url or "").lower().startswith("https://"):
        raise RemoteAgentUnavailable(
            f"{what} must be an https:// URL — the request may carry a bearer token. "
            f"Got: {str(url).split('://')[0]!r}://…")
    _require_public_host(url, what)
    return url


#: Set to "true" to let a remote agent live on a private address (a runtime in a VPC
#: calling a partner over a private link). Off by default: see _require_public_host.
_ALLOW_PRIVATE = os.getenv("A2A_ALLOW_PRIVATE_ENDPOINTS", "").lower() == "true"


def _require_public_host(url: str, what: str) -> None:
    """Refuse an endpoint whose host is, or resolves to, a non-public address.

    The URL is workflow.json config, and the request carries a bearer token or a SigV4
    signature made with the runtime's own role. Pointed at 127.0.0.1, a link-local
    metadata address or a private range, it would send those to something inside the
    network rather than to a partner's agent (server-side request forgery). A host that
    does not resolve is left to fail on the request itself.
    """
    if _ALLOW_PRIVATE:
        return
    import ipaddress
    import socket
    from urllib.parse import urlsplit
    host = urlsplit(url).hostname or ""
    try:
        infos = socket.getaddrinfo(host, 443, proto=socket.IPPROTO_TCP)
    except (socket.gaierror, UnicodeError):
        return
    for info in infos:
        address = ipaddress.ip_address(info[4][0].split("%", 1)[0])
        if not address.is_global:
            raise RemoteAgentUnavailable(
                f"{what} resolves to {address}, which is not a public address. A remote "
                f"agent is reached over the internet; set A2A_ALLOW_PRIVATE_ENDPOINTS=true "
                f"on the runtime if this one is deliberately private.")


#: Hosts a request may be SigV4-signed for: AWS endpoints (`*.amazonaws.com`) and Lambda
#: Function URLs (`*.on.aws`). A signature is made with the runtime's own role, so it is
#: never sent to anyone else's server.
_SIGV4_HOST_SUFFIXES = (".amazonaws.com", ".on.aws")


def _text_parts(parts: object) -> list[str]:
    """The text out of a `parts` array, whichever part shape the agent used."""
    out: list[str] = []
    for p in parts if isinstance(parts, list) else []:
        if not isinstance(p, dict):
            continue
        # `kind` is the current discriminator, `type` the legacy one; a part with
        # neither but carrying `text` is still text.
        kind = str(p.get("kind") or p.get("type") or "").lower()
        if kind in ("", "text") and isinstance(p.get("text"), str):
            out.append(p["text"])
        elif kind == "data" and p.get("data") is not None:
            # A structured part. Keep it as JSON rather than dropping it: a remote
            # agent that answers with data instead of prose is answering.
            out.append(json.dumps(p["data"], ensure_ascii=False))
    return out


def _result_text(result: dict) -> str:
    """The answer inside a `message/send` or `tasks/get` result.

    Three shapes, all legal: a bare Message, a Task with artifacts, or a Task whose
    only content is the final status message. Artifacts win when present because they
    are the deliverable; the status message is usually just progress narration.
    """
    chunks: list[str] = []
    for artifact in result.get("artifacts") or []:
        if isinstance(artifact, dict):
            chunks += _text_parts(artifact.get("parts"))
    if chunks:
        return "\n".join(chunks)
    # A Message reply, or a Task carrying its answer in the status message.
    for holder in (result, (result.get("status") or {}).get("message") or {}):
        if isinstance(holder, dict) and holder.get("parts"):
            chunks = _text_parts(holder.get("parts"))
            if chunks:
                return "\n".join(chunks)
    return ""


class A2AAgent(Agent):
    """Runs this step by delegating it to a remote A2A agent."""

    # Long-term memory works on a remote agent, in BOTH directions, and that is a
    # deliberate change from how this class started.
    #
    # It began as `recall_in_orchestrator = False`, on the reasoning that applies to a
    # `dedicated` agent: the recall would be computed, billed, written as a telemetry
    # row, and then discarded, because the payload sent onward does not carry it. True
    # for InvokeAgentRuntime, which has a fixed payload shape. NOT true here — A2A
    # carries opaque text, so `_message` can hand the recollections over with the task,
    # and it does.
    #
    # The alternative was to leave it off and accept that `agentcore.memory.longTerm`
    # on an a2a agent means store-only. That reads as configured and half works: a
    # customer declares memory, the insights accumulate run after run, and nothing ever
    # reads them back. Silent, and in the direction that looks fine.
    #
    # WORTH KNOWING BEFORE YOU DECLARE IT: this sends your deployment's accumulated
    # recollections to an agent outside it. That is the customer's call, which is why it
    # follows `agentcore.memory` rather than happening unconditionally — declaring
    # memory on an agent you have chosen to outsource IS the decision. The caveat block
    # travels with them (app/common/context.py RECALL_CAVEAT), so the remote agent is
    # told they are recollections and not evidence.
    recall_in_orchestrator = True

    # Set by the registry from workflow.json.
    agent_card: str = ""
    auth: str = "none"

    # --- transport ---------------------------------------------------------

    @property
    def endpoint_base(self) -> str:
        """Where this agent lives: the committed `agentCard`, or the URL the IaC
        injected for a framework-deployed one (`source`).

        Both exist because a URL is not always knowable at commit time. An external
        partner's card is a stable value you write down; a Lambda Function URL is
        generated by the deploy, so committing it is impossible and hardcoding a
        placeholder would be worse. Same split as a `tools` entry's `lambdaArn` vs
        `source`.
        """
        return self.agent_card or str(_ENDPOINTS.get(self.id, ""))

    def _headers(self, token: str) -> dict:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "A2A-Version": _PROTOCOL_VERSION,
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    def _auth_hint(self, status: int) -> str:
        """Extra diagnosis for a 403, which on this path is almost always ours and not
        theirs — and says nothing by itself.

        A SigV4 403 means the signature was rejected, and the only question that matters
        is WHICH principal signed: an IAM-authed endpoint has to name it. Resolving that
        needs a call, so it only happens on the failure path, and it is best-effort
        because a diagnostic must never replace the error it is explaining.
        """
        if status != 403 or self.auth != "sigv4":
            return ""
        try:
            import boto3
            arn = boto3.client("sts").get_caller_identity()["Arn"]
        except Exception:  # noqa: BLE001 - a hint that cannot be produced is just absent
            return " (signed with SigV4; could not resolve the calling identity)"
        return (f" (signed with SigV4 as {arn} — that principal needs "
                f"lambda:InvokeFunctionUrl on the target)")

    def _sign(self, request: urllib.request.Request) -> None:
        """SigV4-sign an outgoing request in place, for `auth: "sigv4"`.

        The mode that exists because the best-secured A2A agent has NO bearer token to
        steal and NO public endpoint: an AWS-hosted one, authorized by IAM. The caller
        is the orchestrator's own execution role, so there is no credential in config,
        nothing to rotate, and nothing to leak in a log.

        Signed over the body, so it must be called after the body is set. `lambda` is
        the signing service for a Function URL; override it for another AWS-hosted
        A2A server with `orchestrator.a2aInvoke.sigv4Service`.
        """
        from urllib.parse import urlsplit

        import boto3
        from botocore.auth import SigV4Auth
        from botocore.awsrequest import AWSRequest

        host = (urlsplit(request.full_url).hostname or "").lower()
        if not host.endswith(_SIGV4_HOST_SUFFIXES):
            raise RemoteAgentUnavailable(
                f"agent '{self.id}' uses auth \"sigv4\", which signs with this runtime's own "
                f"AWS role, so it is only sent to an AWS endpoint (*.amazonaws.com or a "
                f"Lambda Function URL, *.on.aws) — not to {host}.")
        session = boto3.Session()
        credentials = session.get_credentials()
        if credentials is None:
            raise RemoteAgentUnavailable(
                f"agent '{self.id}' uses auth \"sigv4\" but no AWS credentials are available to "
                f"sign with. In the runtime this is the execution role; locally, configure a "
                f"profile.")
        service = str(A2A_INVOKE.get("sigv4Service") or "lambda")
        region = session.region_name or os.getenv("AWS_REGION", "us-east-1")
        signed = AWSRequest(method=request.get_method(), url=request.full_url,
                            data=request.data, headers=dict(request.headers))
        SigV4Auth(credentials.get_frozen_credentials(), service, region).add_auth(signed)
        for header, value in signed.headers.items():
            # urllib title-cases header names it already holds, so replace rather than
            # add — otherwise an unsigned duplicate can shadow the signed one.
            request.add_unredirected_header(header, value)

    def _post(self, url: str, body: dict, token: str) -> dict:
        """One JSON-RPC call. Returns the `result`, raising on a transport or RPC error."""
        request = urllib.request.Request(  # noqa: S310 - https enforced by _require_https
            _require_https(url, f"agent '{self.id}' endpoint"),
            data=json.dumps(body).encode(),
            headers=self._headers(token),
        )
        if self.auth == "sigv4":
            self._sign(request)
        try:
            with urllib.request.urlopen(  # noqa: S310 - https enforced above  # nosec B310
                    request, timeout=float(A2A_INVOKE["timeoutSeconds"])) as resp:
                payload = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            # A2A says deliver JSON-RPC errors over HTTP 200, but real servers
            # (AgentCore Runtime among them) return the true status code with the
            # error in the body. Read the body before giving up, or the actual
            # reason is replaced by a bare "HTTP 409".
            detail = ""
            try:
                detail = _rpc_error(json.loads(e.read())) or ""
            except Exception:  # noqa: BLE001 - a non-JSON error body is still a failure
                detail = ""
            raise RemoteAgentUnavailable(
                f"remote agent '{self.id}' returned HTTP {e.code}"
                f"{f': {detail}' if detail else ''}{self._auth_hint(e.code)}") from e
        except (urllib.error.URLError, TimeoutError, ValueError) as e:
            raise RemoteAgentUnavailable(
                f"remote agent '{self.id}' could not be reached: {type(e).__name__}: {e}") from e

        problem = _rpc_error(payload)
        if problem:
            raise RemoteAgentUnavailable(f"remote agent '{self.id}' rejected the request: {problem}")
        result = payload.get("result")
        if not isinstance(result, dict):
            raise RemoteAgentUnavailable(
                f"remote agent '{self.id}' returned no result object (got "
                f"{type(result).__name__})")
        return result

    def _endpoint(self, token: str) -> str:
        """Where to POST, from the Agent Card.

        The card is the protocol's discovery document, so it is read rather than
        assumed: an agent may serve its RPC endpoint somewhere other than the card's
        own origin, and `supportedInterfaces` is how it says so. Falling back to the
        configured base URL keeps a minimal agent working.
        """
        configured = self.endpoint_base
        if not configured:
            raise RemoteAgentUnavailable(
                f"agent '{self.id}' has no endpoint. It declares neither `agentCard` nor a "
                f"`source` the IaC could inject a URL for (A2A_ENDPOINTS is "
                f"{sorted(_ENDPOINTS) or 'empty'}).")
        base = _require_https(configured, f"agent '{self.id}' endpoint").rstrip("/")
        # A card URL may be given either as the agent's base URL or as the card itself.
        card_url = base if base.endswith(".json") else base + _CARD_PATH
        request = urllib.request.Request(  # noqa: S310 - https enforced above
            card_url, headers=self._headers(token))
        if self.auth == "sigv4":
            self._sign(request)
        try:
            with urllib.request.urlopen(  # noqa: S310 - https enforced above  # nosec B310
                    request, timeout=float(A2A_INVOKE["timeoutSeconds"])) as resp:
                card = json.loads(resp.read())
        except (urllib.error.URLError, TimeoutError, ValueError) as e:
            raise RemoteAgentUnavailable(
                f"could not read the Agent Card for '{self.id}' at {card_url}: "
                f"{type(e).__name__}: {e}") from e
        if not isinstance(card, dict):
            raise RemoteAgentUnavailable(
                f"the Agent Card for '{self.id}' at {card_url} is not a JSON object")
        # Preference order is the card's, not ours: take the first interface whose
        # transport we speak. Everything here is JSON-RPC over HTTP.
        for interface in card.get("supportedInterfaces") or []:
            if not isinstance(interface, dict):
                continue
            transport = str(interface.get("transport") or interface.get("protocol") or "").lower()
            if interface.get("url") and ("json" in transport or not transport):
                return str(interface["url"])
        return str(card.get("url") or base)

    # --- the task ----------------------------------------------------------

    def _message(self, ctx) -> dict:
        """The task handed over, as a single text part.

        A2A carries opaque text, so the remote agent gets the same inputs an
        in-process agent reads — the request, the approved upstream outputs, and any
        reviewer feedback — serialised. It is deliberately not this framework's asset
        contract: the remote agent has its own, and demanding ours would make it
        un-integrable.
        """
        upstream = {k: v for k, v in ((ctx.state or {}).get("outputs") or {}).items()
                    if k != self.id}
        # `produces` is read from config the same way nodes.py reads it, rather than
        # carried on the Agent object, so this adds no field to the author contract.
        produces = str((AGENTS.get(self.id) or {}).get("produces") or "").strip()
        task = {
            "request": ctx.topic,
            "producing": produces or None,
            "upstreamOutputs": upstream,
            "reviewerFeedback": (getattr(ctx, "feedback", "") or "") or None,
            "recalledContext": self._recalled(ctx),
        }
        return {
            "role": "user",
            "messageId": uuid.uuid4().hex,
            "parts": [{"kind": "text",
                       "text": json.dumps({k: v for k, v in task.items() if v is not None},
                                          ensure_ascii=False, default=str)}],
        }

    def _recalled(self, ctx) -> dict | None:
        """Long-term recollections for this step, with the caveat that governs them.

        None when memory is not configured for this agent, so the key is simply absent
        from the task rather than present and empty.

        THE CAVEAT IS NOT OPTIONAL AND IS NOT PARAPHRASED HERE. In-process, `ctx.llm`
        appends it to the system prompt; a remote agent has no system prompt of ours,
        so it has to arrive as part of the task. Sending bare `items` would hand a
        third-party agent a list of unverified assertions from earlier runs with nothing
        saying so — and the predictable result is a recollection cited as a source.
        """
        from app.common.context import RECALL_CAVEAT

        items = [str(i) for i in (getattr(ctx, "recalled_memory", None) or []) if str(i).strip()]
        return {"items": items, "caveat": RECALL_CAVEAT} if items else None

    def _send(self, url: str, ctx, token: str) -> dict:
        return self._post(url, {
            "jsonrpc": "2.0",
            "id": uuid.uuid4().hex,
            "method": "message/send",
            # A context id shared across this run's calls lets a stateful remote
            # agent correlate them, which is what makes a revise cycle coherent
            # on their side rather than looking like unrelated requests.
            "params": {"message": self._message(ctx),
                       "contextId": f"{ctx.session_id}-{self.id}"},
        }, token)

    def _poll(self, url: str, task_id: str, token: str) -> dict:
        return self._post(url, {"jsonrpc": "2.0", "id": uuid.uuid4().hex,
                                "method": "tasks/get", "params": {"id": task_id}}, token)

    async def run(self, ctx) -> str:
        if self.auth == "bearer":
            token = _TOKENS.get(self.id, "")
            if not token:
                raise RemoteAgentUnavailable(
                    f"agent '{self.id}' declares auth \"bearer\" but no token was injected for "
                    f"it. Supply it to the IaC (Terraform var.a2a_tokens / CDK -c a2aTokens), "
                    f"keyed by the agent id — never in workflow.json.")
        elif self.auth == "sigv4":
            # Nothing to fetch: each request is signed with the runtime's own role.
            token = ""
        elif self.auth == "oauth2":
            # Reuses the per-agent outbound identity already declared in
            # `agentcore.identity.outbound`, so an OAuth-protected remote agent needs
            # no new config concept. Empty when identity is not configured, which
            # then fails below as a 401 from their side rather than silently here.
            token = await ctx.get_identity_token()
        else:
            token = ""

        url = await asyncio.to_thread(self._endpoint, token)
        # The recollection count is ON THE TIMELINE, not just in the telemetry, because
        # this is the one step where a reviewer cannot look at the prompt to see what the
        # agent was told. Long-term memory leaving this deployment is also exactly the
        # kind of thing an operator should be able to SEE happening rather than infer
        # from the fact that the config asks for it.
        carried = self._recalled(ctx)
        await ctx.log(
            f"Delegating '{self.id}' to remote A2A agent at {url}"
            + (f", carrying {len(carried['items'])} recalled insight(s)" if carried else ""))
        result = await asyncio.to_thread(self._send, url, ctx, token)

        task_id = result.get("id") if isinstance(result, dict) else None
        state = _state(result.get("status"))
        # A Message reply has no task to poll; a Task might already be terminal.
        deadline = asyncio.get_running_loop().time() + float(A2A_INVOKE["maxPollSeconds"])
        while task_id and state and state not in _TERMINAL_OK | _TERMINAL_BAD:
            if state in _BLOCKED:
                # Waiting on input or on a credential we cannot supply mid-task.
                # Polling would burn the whole budget to reach the same answer.
                raise RemoteAgentUnavailable(
                    f"remote agent '{self.id}' is waiting on '{state}' and this workflow cannot "
                    f"supply it mid-task. Its review gate is where a human joins a run — see "
                    f"`hitl` in workflow.json.")
            if asyncio.get_running_loop().time() > deadline:
                raise RemoteAgentUnavailable(
                    f"remote agent '{self.id}' did not finish task {task_id} within "
                    f"{A2A_INVOKE['maxPollSeconds']}s (last state: {state or 'unknown'}). Raise "
                    f"`orchestrator.a2aInvoke.maxPollSeconds` if their agent is legitimately "
                    f"this slow.")
            await asyncio.sleep(float(A2A_INVOKE["pollIntervalSeconds"]))
            result = await asyncio.to_thread(self._poll, url, task_id, token)
            state = _state(result.get("status"))

        if state in _TERMINAL_BAD:
            raise RemoteAgentUnavailable(
                f"remote agent '{self.id}' ended task {task_id} as '{state}'"
                f"{_status_detail(result)}")

        text = _result_text(result)
        if not text.strip():
            # An empty answer is not an answer. Returning "" would put a gap into
            # every downstream agent's inputs looking exactly like a real finding of
            # nothing — the same reason ModelOutputUnusable exists.
            raise RemoteAgentUnavailable(
                f"remote agent '{self.id}' completed but returned no text content")
        return self._as_asset(ctx, text)

    def _upstream_asset_ids(self, ctx) -> list[str]:
        """The assetIds of the approved upstream assets this step was given.

        Asset-level provenance, and the framework is the only party that can state it:
        the remote agent was handed the upstream CONTENT (`_message`), so it can quote
        it, but the ids are this graph's bookkeeping. Same field, same meaning, as the
        `sourceAssetIds` a local synthesis agent stamps.
        """
        from app.common import assets
        from app.common.config import upstream_of

        found: list[str] = []
        for upstream_id in upstream_of(self.id):
            asset_id = assets.parse(ctx.input(upstream_id)).get("assetId")
            if asset_id:
                found.append(str(asset_id))
        return found

    def _as_asset(self, ctx, text: str) -> str:
        """Give a structured remote reply the same asset envelope a local agent's has.

        WHY THE FRAMEWORK DOES THIS AND NOT THE REMOTE AGENT. The envelope is
        BOOKKEEPING ABOUT THIS RUN, and a remote agent cannot know it: `version` is how
        many times this agent has run in THIS session (read from the graph's history),
        `createdByAgent` is the id THIS workflow gave the step, `assetType` is the
        `produces` THIS workflow declared, and `sourceAssetIds` are the ids of the
        upstream assets this graph chose to hand over. A remote agent asked to invent
        them would get the version wrong on every re-run — silently, because a wrong
        integer still validates.

        So the division is: the remote agent supplies the CONTENT, which is the only
        part it can legitimately know, and the framework supplies the envelope. That
        makes a `runtime: "a2a"` agent a first-class contract producer — it gets a real
        `assetId`, so a downstream agent can trace a claim to it and the UI renders it
        as a structured asset rather than a wall of text. Without this a remote agent's
        output had no assetId at all, and `synthesis.upstream_context` — which collects
        exactly that field — passed it to the model as an unattributable block, so a
        report could mention it in prose but never cite it.

        A PROSE reply is returned untouched. A reviewer agent that answers in sentences
        is a legitimate remote agent, and wrapping its text in a JSON envelope would
        make the timeline and the report worse, not better. Only a JSON OBJECT is
        treated as contract content.

        WHAT IS NOT DONE HERE, deliberately: the merged asset is not validated against
        a pydantic contract, because there is no local contract class for an agent whose
        code is somebody else's — `produces` names the type, it does not import a model.
        That is the real cost of the trust boundary, and it is the reason the UI renders
        an asset by SHAPE rather than by field name. A remote agent you need schema
        enforcement over belongs behind a `dedicated` wrapper that validates its reply.

        Fields the remote DID supply are kept. If a remote agent is contract-aware
        enough to send its own `executiveSummary`, `sources` or even `assetId`, that is
        its business — this fills the gaps rather than overwriting.
        """
        from app.common import assets, clock
        from app.common.config import AGENTS
        from app.common.contracts.base import AssetStatus

        produces = str((AGENTS.get(self.id) or {}).get("produces") or "").strip()
        if not produces:
            # Nothing to call the asset. `produces` is what names the contract, so
            # without it there is no envelope to build — return the reply as given.
            return text

        payload = assets.extract_json(text)
        if not isinstance(payload, dict) or not payload:
            return text

        version = assets.prior_version(ctx)
        title = assets.brief_title(assets.brief(ctx), ctx)
        envelope = {
            "assetId": f"asset-{assets.slug(produces)}-{assets.slug(str(title))}-v{version}",
            "assetType": produces,
            "version": version,
            # in-review, not approved: a remote agent's answer is exactly the kind of
            # thing a human gate exists to look at, and the local agents use the same
            # status for the same reason.
            "status": AssetStatus.IN_REVIEW.value,
            "createdAt": clock.now_et().isoformat(),
            "createdByAgent": self.id,
            "sourceAssetIds": self._upstream_asset_ids(ctx),
        }
        # The remote's own fields win; the envelope only fills what is absent.
        return json.dumps({**envelope, **payload}, indent=2)


def _rpc_error(payload: object) -> str:
    """A JSON-RPC error rendered for a human, or "" when there is none."""
    err = payload.get("error") if isinstance(payload, dict) else None
    if not isinstance(err, dict):
        return ""
    return f"{err.get('message') or 'error'} (code {err.get('code')})".strip()


def _status_detail(result: dict) -> str:
    """Whatever the agent said about why it stopped. Their words are the only
    diagnostic available from this side of the boundary, so they are not discarded."""
    said = _text_parts(((result.get("status") or {}).get("message") or {}).get("parts"))
    return f": {' '.join(said)[:300]}" if said else ""


agent = A2AAgent()
