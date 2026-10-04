"""A real A2A server, so the `runtime: "a2a"` placement ships with something live.

Every other capability in this sample has a working agent proving it: `kb` has one,
`websearch` has one, `mcp` has one, `lambda` has one. `a2a` had documentation and a
test suite, and nothing you could watch run — because that placement needs an agent
THIS DEPLOYMENT DOES NOT OWN, and a committed placeholder URL would have failed every
run. So the framework deploys a stand-in: a genuine A2A server, in its own Lambda,
behind its own Function URL, reached over the protocol exactly as a third party's
agent would be.

It is a STAND-IN, not a mock. It speaks real JSON-RPC 2.0, publishes a real Agent
Card, and answers by really calling Bedrock. The only fiction is the org chart: it
happens to live in your account. Cross that out and nothing about the client changes
— which is the property worth demonstrating.

    "analysis": {                      <- the shipped sample's own stage 3
      "name": "Analysis",
      "runtime": "a2a",
      "source": "a2a_lambda",          <- the framework deploys me and injects my URL
      "skill": "analysis",             <- which of my skills; a PATH segment, see below
      "auth": "sigv4",                 <- required with `source`: my URL is AWS_IAM
      "produces": "analysis"
    }

Point an agent at a real partner instead and this function stops being deployed:
`"agentCard": "https://theirs.example/agent"` and `source` goes away, the same way a
`tools` entry swaps `source` for `lambdaArn`.

THE THREE ENDPOINTS, per the A2A spec and the AgentCore A2A contract
  GET  /.well-known/agent-card.json  discovery: identity, skills, where to POST
  POST /                             JSON-RPC 2.0: `message/send`, `tasks/get`
  GET  /ping                         health

WHY IT ANSWERS SYNCHRONOUSLY. A2A lets a server return a Task that is still `working`
and be polled. This one finishes inside the request because a Lambda has nowhere to
keep a task between invocations — there is no store here, and inventing one would be
inventing infrastructure the demonstration does not need. The client handles both, and
`tests/test_a2a.py` covers the polling path; a real partner with a queue behind it will
exercise it.

SECURITY. The Function URL is `AWS_IAM`, so this endpoint is not public: a caller must
present a SigV4 signature from a principal allowed to invoke it, which is the
orchestrator's execution role and nothing else. That is why the client has an
`auth: "sigv4"` mode — no bearer token exists to leak, nothing to rotate, and no
secret in `workflow.json`. A Function URL with `authType: NONE` guarded by a shared
token would have been less code and materially worse.
"""

from __future__ import annotations

import json
import os
import re

import boto3

# Always set by both IaC paths, already resolved to the deployment's default model in its
# region's inference-profile geography. No literal fallback: a copy here is how a model
# default drifts, and a missing value is reported by Bedrock on the first call.
MODEL_ID = os.getenv("BEDROCK_MODEL_ID", "")
REGION = os.getenv("AWS_REGION", "us-east-1")
AGENT_NAME = os.getenv("A2A_AGENT_NAME", "Partner Agent")
MAX_TOKENS = int(os.getenv("A2A_MAX_TOKENS", "2000"))

CARD_PATH = "/.well-known/agent-card.json"
PROTOCOL_VERSION = "0.3"

# JSON-RPC error codes. The two standard ones plus the A2A code for an unknown task,
# so a client that switches on them sees what it expects.
PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
TASK_NOT_FOUND = -32051

_bedrock = None


def _client():
    global _bedrock
    if _bedrock is None:
        _bedrock = boto3.client("bedrock-runtime", region_name=REGION)
    return _bedrock


# ---------------------------------------------------------------------------
# Skills
# ---------------------------------------------------------------------------
#
# A catalogue, not a fixed roster. `workflow.json` picks which of these are actually
# wired up as agents, so adding a remote step to a pipeline is a workflow.json edit and
# nothing else — and a skill nobody names simply is not reached. Each skill is a role
# plus a shape, which is exactly what an agent under app/subagents/ carries in its
# prompts.py: from the orchestrator's side there is no way to tell this one lives
# somewhere else.
#
# TWO KINDS OF SKILL HERE, on purpose.
#
#   `compliance` / `resilience` — REVIEWERS, and their shape is their own. This is the
#   normal case for a third party: they have an output format, it is not ours, and
#   demanding ours is what makes a partner agent un-integrable.
#
#   `analysis` / `recommendation` — CONTRACT PRODUCERS. Their shape is the CONTENT of
#   this sample's Analysis and Recommendation contracts, because these two are steps in
#   the deliverable rather than commentary on it: the report agent traces its sections
#   back to them. Note what is NOT in these shapes — assetId, version, status,
#   createdAt, createdByAgent, sourceAssetIds. Those are bookkeeping about the
#   orchestrator's run, the remote agent cannot know them (it has no idea how many
#   times it has been re-run in this session), and A2AAgent._as_asset stamps them on
#   the way back. Asking for them here is how you get `"version": 1` on a revise.
#
# WHY THE GROUNDING RULES ARE SPELLED OUT AT LENGTH BELOW. An in-process synthesis
# agent gets them for free: app/subagents/_shared/synthesis.py appends nine rules to
# every prompt. That code does not cross the boundary, so a remote agent that is not
# told "no figure that is not in an upstream asset" will invent one. The rules are the
# same rules, in the place that can still apply them — which is the honest shape of
# outsourcing a step: the standard travels with the work.

SKILLS: dict[str, dict] = {
    "compliance": {
        "id": "compliance",
        "name": "Compliance review",
        "description": "Reviews a proposed design against common regulatory and data-handling "
                       "obligations, and names the controls that would have to exist.",
        "tags": ["compliance", "governance", "risk"],
        "prompt": (
            "You are an independent compliance reviewer at a firm the requester does not "
            "control. Review the request and the evidence supplied for regulatory and "
            "data-handling exposure.\n\n"
            "RULES\n"
            "1. Name concrete obligations that plausibly apply (data residency, retention, "
            "audit trail, access control, personal-data handling) and say WHY each applies to "
            "THIS design. Do not list a compliance regime you cannot tie to something in the "
            "inputs.\n"
            "2. For each obligation, name the control that would satisfy it.\n"
            "3. Separate what the inputs ESTABLISH from what you are INFERRING. You are an "
            "outside reviewer; say when you are assuming.\n"
            "4. If the inputs are too thin to review, say so plainly and say what you would "
            "need. Do not pad.\n"
            "5. No preamble about being an AI or about the request's quality."
        ),
        "shape": ('{"reviewer": "compliance", "verdict": "one sentence", '
                  '"obligations": [{"obligation": "...", "whyItApplies": "...", '
                  '"control": "...", "basis": "established|inferred"}], '
                  '"assumptions": ["..."], "notReviewable": ["..."]}'),
    },
    "resilience": {
        "id": "resilience",
        "name": "Resilience review",
        "description": "Stress-tests a proposed design for failure modes, blast radius and "
                       "recovery, and names what is missing.",
        "tags": ["resilience", "reliability", "operations"],
        "prompt": (
            "You are an independent resilience reviewer at a firm the requester does not "
            "control. Stress-test the proposed design.\n\n"
            "RULES\n"
            "1. Name failure modes that follow from what the inputs actually describe — a "
            "dependency that can be slow or absent, a step with no retry, state that cannot be "
            "rebuilt. Tie each to something in the inputs.\n"
            "2. For each, give the blast radius and the recovery path.\n"
            "3. Name what the design does NOT say that you would need to judge it.\n"
            "4. Separate what the inputs establish from what you are inferring.\n"
            "5. If the inputs are too thin, say so plainly. Do not pad.\n"
            "6. No preamble about being an AI or about the request's quality."
        ),
        "shape": ('{"reviewer": "resilience", "verdict": "one sentence", '
                  '"failureModes": [{"mode": "...", "blastRadius": "...", "recovery": "...", '
                  '"basis": "established|inferred"}], '
                  '"unanswered": ["..."], "notReviewable": ["..."]}'),
    },
    "analysis": {
        "id": "analysis",
        "name": "Evidence analysis",
        "description": "Synthesizes a request and its approved research findings into one "
                       "analysis, with every material claim traced to the asset that supports "
                       "it.",
        "tags": ["analysis", "synthesis", "provenance"],
        # An analysis over four research findings does not fit in the reviewers' budget.
        # Declared per skill rather than raised for everyone, because a compliance review
        # given 8000 tokens does not get better, it gets longer.
        #
        # 8000 and not 6000, which is what the local versions of these agents used. 6000
        # turned out to be the edge rather than the fit: a measured run produced 21.4 KB
        # of recommendation JSON and the NEXT run failed outright, because asking each
        # item to carry the analysis assetId alongside its research ids (see the prompt)
        # added enough to cross the ceiling. That failure was correct and legible — the
        # stand-in refuses a cut-off answer and names the budget — but a budget that a
        # normal run sits on the edge of is a budget that is too small.
        "maxTokens": 8000,
        "prompt": (
            "You are an independent analysis agent. Synthesize the request and the approved "
            "research findings you are given into ONE cohesive analysis.\n\n"
            "The inputs arrive as JSON: `request` is what the run is for, `upstreamOutputs` "
            "holds the approved assets from earlier steps, and `reviewerFeedback` — when "
            "present — is a human's instruction for THIS revision, which takes precedence.\n\n"
            "EVERY UPSTREAM ASSET CARRIES AN `assetId`. That id is how a claim is traced: put "
            "the ids of the assets that support a claim in its `tracedToAssetIds`. A claim "
            "with an empty list is an assertion nobody can check.\n\n"
            "RULES\n"
            "1. Use ONLY the upstream assets you were given. Invent no evidence.\n"
            "2. No figure that is not in an upstream asset — no cost, threshold, percentage, "
            "duration, cadence or timeline. Not even as an illustration, and not hedged with "
            "\"e.g.\": a reader takes a number in a deliverable for one somebody agreed to.\n"
            "3. Separate what the evidence ESTABLISHES from your own interpretation. Mark a "
            "claim `medium` or `low` confidence when the evidence is thin, and say why in "
            "`rationale`.\n"
            "4. `rationale` EXPLAINS HOW YOU WEIGHED THE EVIDENCE and nothing else: which "
            "findings agreed, which conflicted and how you settled it, what you set aside. It "
            "is NOT a description of the request. If your rationale would read the same for "
            "any request on this topic, you have written about the wrong thing.\n"
            "5. Write about the SUBJECT, never about the request, the requester, or this "
            "system's own run history. \"The request brief identifies seven key questions\" "
            "and \"Eight prior runs completed\" are both true sentences about the wrong "
            "subject. `recalledContext`, when you are given it, orients you and is never a "
            "claim, a rationale or a source — it says so itself.\n"
            "6. What the request leaves unsettled goes in `limitations`, ONCE. It is never a "
            "claim, and it does not need repeating in every claim and rationale that touches "
            "it. Measured on the in-house version of this agent: the same gap restated 21 "
            "times in one deliverable.\n"
            "7. If an input says something is unavailable, that is a limitation, not a "
            "finding.\n"
            "8. Counts must match the lists they count.\n"
            "9. No preamble about being an AI, and no commentary on the request's quality.\n"
            "10. RETURN EXACTLY THE FIELDS IN THE SHAPE AND NO OTHERS, at every level "
            "including inside each claim. The caller feeds this into a strict contract that "
            "rejects an unknown field, and it has no way to tell a useful addition from a "
            "typo. Observed live: one claim out of twenty-one carried its own `rationale`, "
            "which invalidated the whole asset. If you have per-claim reasoning to give, it "
            "belongs in the top-level `rationale`, which is FOR that."
        ),
        # The CONTENT of app/subagents/_shared/contracts/analysis.py. No envelope
        # fields: the orchestrator stamps assetId/version/status/createdAt/
        # createdByAgent/sourceAssetIds, because they describe its run and not this
        # agent's answer.
        "shape": ('{"executiveSummary": "1-2 sentences on the SUBJECT, for a reviewer", '
                  '"summary": "the core analysis", '
                  '"rationale": "how you weighed the evidence: what agreed, what conflicted '
                  'and how you settled it, what you set aside", '
                  '"claims": [{"statement": "a material claim", '
                  '"tracedToAssetIds": ["the asset-... ids from upstreamOutputs that support '
                  'it"], "confidence": "high|medium|low"}], '
                  '"assumptions": ["..."], '
                  '"limitations": ["named evidence gaps, or []"], '
                  '"sources": [{"sourceId": "s1", '
                  '"sourceType": "research-finding|request-brief|other", '
                  '"sourceName": "which input", "sourceAssetId": "asset-... or omit"}]}'),
    },
    "recommendation": {
        "id": "recommendation",
        "name": "Recommendation",
        "description": "Turns an approved analysis into a prioritized set of recommended "
                       "actions, each traced to the asset that justifies it.",
        "tags": ["recommendation", "prioritization", "provenance"],
        # See the note on the analysis skill: 6000 was the ceiling this one actually hit.
        "maxTokens": 8000,
        "prompt": (
            "You are an independent recommendation agent. Turn the approved analysis you are "
            "given into a prioritized set of actionable recommendations.\n\n"
            "The inputs arrive as JSON: `request` is what the run is for, `upstreamOutputs` "
            "holds the approved assets from earlier steps, and `reviewerFeedback` — when "
            "present — is a human's instruction for THIS revision, which takes precedence.\n\n"
            "THE ANALYSIS IS YOUR PRIMARY INPUT, AND EVERY ITEM MUST TRACE TO IT. "
            "`upstreamOutputs` carries the whole run, including the raw research the analysis "
            "was built FROM — so it is easy to reach past the analysis and cite the research "
            "directly. Do not. Your job is to act on the analysis's conclusions, not to redo "
            "them, and an item that cites only research assets is doing the previous agent's "
            "work again while hiding whether its analysis was used at all. Put the `assetId` of "
            "the asset whose `assetType` is \"analysis\" in every item's `tracedToAssetIds`, "
            "and add a research `assetId` ALONGSIDE it when a specific figure or citation in "
            "that item comes from one. Measured on a live run before this paragraph existed: "
            "seven items out of seven cited research only, and the analysis appeared nowhere "
            "except in `sources`.\n\n"
            "ONE DECISION, ONE ITEM. Before you answer, read your own list and merge every "
            "item that rests on the SAME missing input or the SAME underlying decision. "
            "Measured on the in-house version of this agent: four separate items — defer the "
            "compute choice, defer the queue choice, defer the cost estimate, look up one "
            "service's price — which are one fact wearing four titles, namely that the "
            "workload profile is unknown. Four slots spent, one thing said.\n"
            "  EVERYTHING THE REQUESTER HAS NOT TOLD YOU IS *ONE* ITEM. \"Specify the data "
            "source\", \"Specify the latency requirement\", \"Specify the budget\" is one "
            "item split three ways. Write it once, name what is needed inside it, and say "
            "which decisions each part unblocks — that is MORE useful than three, because a "
            "reader learns that one answer unblocks three choices.\n\n"
            "PREFER FEWER, LARGER ITEMS. Beyond roughly eight you are almost certainly "
            "splitting decisions that belong together. Merge before you cut, so nothing is "
            "lost.\n\n"
            "LARGER DOES NOT MEAN INVENTING NUMBERS, and this is the trap that comes with the "
            "instruction above. A bigger item has room for specifics, and the specifics that "
            "come to mind are thresholds nobody gave you: \"alarm above a 1% failure rate\", "
            "\"alert past a 1-hour lag\". Name the DIMENSION and say the value has to be set: "
            "\"alarm on error rate and on processing lag; the thresholds depend on the latency "
            "requirement, which has not been supplied\". That tells a reader what to "
            "instrument AND what they still owe you.\n\n"
            "RULES\n"
            "1. Use ONLY the upstream assets you were given.\n"
            "2. No figure that is not in an upstream asset — no cost, threshold, percentage, "
            "duration, size or count. Not even as an illustration.\n"
            "3. `executiveSummary` and `summary` are about the SUBJECT: what should be done "
            "and what it turns on. Not about the request, and not a list of what is missing — "
            "that is the one item above and `assumptions`.\n"
            "4. If an input says something is unavailable, it cannot become an action. Name "
            "the gap and what closing it would unblock.\n"
            "5. No preamble about being an AI, and no commentary on the request's quality.\n"
            "6. RETURN EXACTLY THE FIELDS IN THE SHAPE AND NO OTHERS, at every level "
            "including inside each item. The caller feeds this into a strict contract that "
            "rejects an unknown field, and it has no way to tell a useful addition from a "
            "typo."
        ),
        # The CONTENT of app/subagents/_shared/contracts/recommendation.py.
        "shape": ('{"executiveSummary": "1-2 sentences on the SUBJECT, for a reviewer", '
                  '"summary": "overview of the recommendations", '
                  '"items": [{"title": "the recommended action", "detail": "what to do", '
                  '"priority": "high|medium|low", '
                  '"rationale": "why, grounded in the analysis", '
                  '"tracedToAssetIds": ["the analysis asset-... id, ALWAYS, plus any research '
                  'asset-... id a specific figure in this item came from"]}], '
                  '"risks": ["risks to weigh, or []"], "assumptions": ["...", "or []"], '
                  '"sources": [{"sourceId": "s1", '
                  '"sourceType": "analysis|request-brief|other", '
                  '"sourceName": "which input", "sourceAssetId": "asset-... or omit"}]}'),
    },
}

DEFAULT_SKILL = "compliance"


def _skill(name: str) -> dict:
    return SKILLS.get(str(name or "").strip().lower()) or SKILLS[DEFAULT_SKILL]


def _max_tokens(skill: dict) -> int:
    """This skill's output budget.

    Per skill because they are not the same size of job: a compliance review given 6000
    tokens does not get better, it gets longer, and an analysis over four research
    findings does not fit in 2000. `A2A_MAX_TOKENS` stays the default for a skill that
    does not declare one, so the IaC needs to know nothing about any of this.
    """
    try:
        return int(skill.get("maxTokens") or MAX_TOKENS)
    except (TypeError, ValueError):
        return MAX_TOKENS


# ---------------------------------------------------------------------------
# The Agent Card
# ---------------------------------------------------------------------------

def agent_card(base_url: str, skill_name: str) -> dict:
    """Discovery document. `url` is absolute and points back at this Function URL,
    because a client is entitled to POST wherever the card says — including a
    different host from the card's own origin."""
    skill = _skill(skill_name)
    return {
        "protocolVersion": PROTOCOL_VERSION,
        # The SKILL's name, not the function's. Each path is a distinct agent as far as
        # the protocol is concerned, so a single `name` for all of them would advertise
        # an analysis agent as a reviewer. Who OPERATES them is the card's `provider`,
        # which is where A2A puts it and the honest place for the one fact the stand-in
        # is pretending about.
        "name": skill["name"],
        "provider": {"organization": AGENT_NAME, "url": base_url},
        "description": skill["description"],
        "version": "1.0.0",
        "url": base_url,
        "preferredTransport": "JSONRPC",
        "supportedInterfaces": [{"transport": "JSONRPC", "url": base_url}],
        "defaultInputModes": ["text/plain"],
        "defaultOutputModes": ["application/json"],
        "capabilities": {"streaming": False, "pushNotifications": False},
        "skills": [{k: skill[k] for k in ("id", "name", "description", "tags")}],
    }


# ---------------------------------------------------------------------------
# The work
# ---------------------------------------------------------------------------

def _task_text(params: dict) -> str:
    """The text the client sent, out of `params.message.parts`.

    Mirrors the client's own tolerance: `kind`, the legacy `type`, or neither. A server
    that accepts only the current form rejects clients that are merely older.
    """
    message = params.get("message") if isinstance(params, dict) else None
    parts = (message or {}).get("parts") if isinstance(message, dict) else None
    out: list[str] = []
    for part in parts if isinstance(parts, list) else []:
        if not isinstance(part, dict):
            continue
        kind = str(part.get("kind") or part.get("type") or "").lower()
        if kind in ("", "text") and isinstance(part.get("text"), str):
            out.append(part["text"])
        elif kind == "data" and part.get("data") is not None:
            out.append(json.dumps(part["data"], ensure_ascii=False))
    return "\n".join(out).strip()


def review(task_text: str, skill_name: str) -> str:
    """Do the actual work: one Bedrock call, in the reviewer's voice.

    Raises on failure rather than returning a placeholder — the same standard the rest
    of this sample holds. A stand-in that fabricates when the model is unavailable
    would teach exactly the wrong lesson about remote agents.
    """
    skill = _skill(skill_name)
    response = _client().converse(
        modelId=MODEL_ID,
        system=[{"text": f"{skill['prompt']}\n\nReturn ONLY valid JSON of this shape:\n"
                         f"{skill['shape']}"}],
        messages=[{"role": "user", "content": [{"text": task_text or "(no inputs supplied)"}]}],
        inferenceConfig={"maxTokens": _max_tokens(skill), "temperature": 0},
    )
    blocks = response.get("output", {}).get("message", {}).get("content", [])
    text = "\n".join(b.get("text", "") for b in blocks if isinstance(b, dict)).strip()
    if not text:
        raise RuntimeError("the model returned no content")
    if str(response.get("stopReason") or "").lower() == "max_tokens":
        # A CUT-OFF ANSWER IS REFUSED, not returned. In-process, the orchestrator
        # detects its own truncation and stamps a `limitations` entry on the asset
        # (synthesis.truncation_limitation) so the reviewer is told. That signal cannot
        # cross this boundary: from the client's side a truncated reply is just a reply,
        # and the orchestrator's JSON repair turns it into a complete-LOOKING asset with
        # the end of the longest list missing. Silent partial content in front of an
        # approver is worse than a failed step, so this is a failed task with the fix
        # named in it.
        raise RuntimeError(
            f"the '{skill['id']}' answer hit its {_max_tokens(skill)}-token output limit and "
            f"was cut off; raise maxTokens for that skill in a2a_lambda/handler.py rather "
            f"than returning a partial asset")
    # Unwrap a fenced block if the model added one, so the client gets JSON and not
    # markdown. Not a parse — just the fence.
    fenced = re.match(r"^```(?:json)?\s*\n(.*)\n```\s*$", text, re.DOTALL)
    return fenced.group(1).strip() if fenced else text


# ---------------------------------------------------------------------------
# JSON-RPC
# ---------------------------------------------------------------------------

def _ok(rpc_id, result: dict) -> dict:
    return {"jsonrpc": "2.0", "id": rpc_id, "result": result}


def _err(rpc_id, code: int, message: str) -> dict:
    return {"jsonrpc": "2.0", "id": rpc_id, "error": {"code": code, "message": message}}


def _completed_task(task_id: str, context_id: str, text: str) -> dict:
    """A finished Task carrying its answer as an artifact.

    An artifact rather than a status message on purpose: the artifact is the
    deliverable, the status message is narration, and the client prefers the former.
    """
    return {
        "id": task_id,
        "contextId": context_id,
        "status": {"state": "completed"},
        "artifacts": [{
            "artifactId": f"{task_id}-review",
            "name": "review",
            "parts": [{"kind": "text", "text": text}],
        }],
    }


def handle_rpc(body: dict, skill_name: str) -> dict:
    rpc_id = body.get("id")
    method = str(body.get("method") or "")
    params = body.get("params") if isinstance(body.get("params"), dict) else {}

    if body.get("jsonrpc") != "2.0":
        return _err(rpc_id, INVALID_REQUEST, 'Invalid request - "jsonrpc" must be "2.0"')

    if method == "message/send":
        task_text = _task_text(params)
        context_id = str(params.get("contextId") or "")
        task_id = str((params.get("message") or {}).get("messageId") or "task")
        try:
            text = review(task_text, skill_name)
        except Exception as e:  # noqa: BLE001 - any failure is reported as a failed task
            # A failed Task, not a JSON-RPC error: the request was valid, the work did
            # not succeed. The client distinguishes these, and only this one carries a
            # reason the reviewer can read.
            return _ok(rpc_id, {
                "id": task_id, "contextId": context_id,
                "status": {"state": "failed", "message": {"parts": [
                    {"kind": "text", "text": f"{type(e).__name__}: {e}"}]}},
            })
        return _ok(rpc_id, _completed_task(task_id, context_id, text))

    if method == "tasks/get":
        # Answering synchronously means a task is finished before the client could ask
        # about it, and there is no store to look one up in. Say that, rather than
        # inventing a `working` state that would be polled forever.
        return _err(rpc_id, TASK_NOT_FOUND,
                    "Task not found - this agent completes synchronously and keeps no task store")

    return _err(rpc_id, METHOD_NOT_FOUND,
                f'Method not found - "{method}". This agent implements message/send.')


# ---------------------------------------------------------------------------
# Function URL entry point
# ---------------------------------------------------------------------------

def _response(status: int, payload: dict) -> dict:
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json", "A2A-Version": PROTOCOL_VERSION},
        "body": json.dumps(payload, ensure_ascii=False),
    }


def _origin(event: dict) -> str:
    """This function's own https origin, from the request.

    Taken from the Host header rather than configured, so the card advertises the URL
    the client actually reached — which makes it correct under an alias, and avoids a
    second place to keep the URL in sync.
    """
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
    host = headers.get("host") or ""
    return f"https://{host}" if host else ""


def _skill_from_path(path: str) -> str:
    """The skill is the FIRST PATH SEGMENT — `/compliance`, `/resilience`.

    A path and not a query string, because a client appends the well-known card path to
    whatever base URL it was given. `https://host/?skill=x` + `/.well-known/...` puts
    the path after the query and resolves to nothing; `https://host/compliance` +
    `/.well-known/...` is a real URL. Found by reading back the card this function
    generated.
    """
    for segment in str(path or "").split("/"):
        if segment and segment.lower() in SKILLS:
            return segment.lower()
    return DEFAULT_SKILL


def lambda_handler(event, _context=None):
    """Route on method + path, the way a Function URL delivers them.

    The skill comes from the path, so ONE deployed function can back several
    workflow.json agents that are genuinely different reviewers — `/compliance` and
    `/resilience`. The alternative was a Lambda per skill: more infrastructure to
    demonstrate the same protocol.
    """
    http = (event.get("requestContext") or {}).get("http") or {}
    method = str(http.get("method") or event.get("httpMethod") or "GET").upper()
    path = str(event.get("rawPath") or http.get("path") or "/")
    skill_name = _skill_from_path(path)

    if method == "GET" and path.rstrip("/").endswith(CARD_PATH.rstrip("/")):
        return _response(200, agent_card(f"{_origin(event)}/{skill_name}", skill_name))

    if method == "GET" and path.rstrip("/").endswith("/ping"):
        return _response(200, {"status": "Healthy"})

    if method != "POST":
        return _response(404, {"error": f"Not found - {method} {path}"})

    raw = event.get("body") or ""
    if event.get("isBase64Encoded"):
        import base64
        raw = base64.b64decode(raw).decode()
    try:
        body = json.loads(raw)
    except ValueError:
        return _response(200, _err(None, PARSE_ERROR, "Parse error - body is not valid JSON"))
    if not isinstance(body, dict):
        return _response(200, _err(None, INVALID_REQUEST, "Invalid request - expected an object"))

    result = handle_rpc(body, skill_name)
    # HTTP 200 with the error in the body, which is what the A2A spec prescribes. (The
    # client also copes with servers that return the real status code, because
    # AgentCore Runtime does.)
    return _response(200, result)
