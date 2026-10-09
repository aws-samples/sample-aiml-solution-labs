"""Bedrock Guardrails client — ApplyGuardrail API wrapper."""

import asyncio
import json
import os

import boto3

from app.common.config import REGION

_GUARDRAIL_ID = os.getenv("GUARDRAIL_ID", "")
_VERSION = os.getenv("GUARDRAIL_VERSION", "DRAFT")
_client = None


def _named() -> dict:
    """The named guardrails (workflow.json `guardrails`): {name: {id, version}}, from
    GUARDRAILS, which both IaC paths set. Read per call, so a test can set it."""
    try:
        got = json.loads(os.getenv("GUARDRAILS") or "{}")
    except ValueError:
        got = {}
    return got if isinstance(got, dict) else {}


def named(name: str) -> tuple[str, str]:
    """(id, version) of a named guardrail. Raises when this deployment has none by that
    name: an agent told to use a guardrail must not run unchecked."""
    found = _named().get(name)
    if not isinstance(found, dict) or not found.get("id"):
        raise RuntimeError(f"guardrail {name!r} (agentcore.guardrails.use) is not deployed here: "
                           f"deploy again after adding it to `guardrails`")
    return str(found["id"]), str(found.get("version") or "DRAFT")


def _bedrock():
    global _client
    if _client is None:
        _client = boto3.client("bedrock-runtime", region_name=REGION)
    return _client


class GuardrailBlocked(Exception):
    """Raised when a guardrail blocks the content. `reasons` names what blocked it (a
    denied topic, a content filter, a word or a PII type), for the timeline: the
    guardrail's own message never says which rule it was."""

    def __init__(self, message: str, reasons: list[str] | None = None):
        self.message = message
        self.reasons = list(reasons or [])
        super().__init__(message)


def _reasons(resp: dict) -> list[str]:
    """What BLOCKED, by policy: "denied topic FinancialAdvice", "content filter INSULTS"."""
    out: list[str] = []
    for a in resp.get("assessments") or []:
        if not isinstance(a, dict):
            continue
        words = a.get("wordPolicy") or {}
        found = [
            *((f"denied topic {t.get('name')}", t) for t in (a.get("topicPolicy") or {}).get("topics") or []),
            *((f"content filter {f.get('type')}", f) for f in (a.get("contentPolicy") or {}).get("filters") or []),
            *((f"word {w.get('match')}", w)
              for w in [*(words.get("customWords") or []), *(words.get("managedWordLists") or [])]),
            *((f"sensitive information {p.get('type')}", p)
              for p in (a.get("sensitiveInformationPolicy") or {}).get("piiEntities") or []),
        ]
        out.extend(label for label, item in found if isinstance(item, dict) and item.get("action") == "BLOCKED")
    return list(dict.fromkeys(out))


async def check(text: str, source: str = "INPUT",
                guardrail_id: str = "", version: str = "") -> str:
    """Apply a guardrail to text. Returns the text unchanged if allowed, raises
    GuardrailBlocked if filtered. Uses the per-agent `guardrail_id` when given
    (from workflow.json), else the GUARDRAIL_ID env default. No-op if neither
    is set, so the feature is genuinely optional."""
    gid = guardrail_id or _GUARDRAIL_ID
    if not gid or not text:
        return text

    resp = await asyncio.to_thread(
        _bedrock().apply_guardrail,
        guardrailIdentifier=gid,
        guardrailVersion=version or _VERSION,
        source=source,
        content=[{"text": {"text": text}}],
    )

    if resp.get("action") == "GUARDRAIL_INTERVENED":
        outputs = resp.get("outputs", [])
        masked = outputs[0].get("text") if outputs else None
        # A guardrail that only ANONYMIZED (sensitive information set to mask, nothing
        # blocked) hands back the text with the PII masked: that is what to carry on
        # with. Treating it as a block stopped every run whose request held an email
        # address, where the guardrail was configured to mask it (observed live).
        if masked and not _blocked(resp):
            return masked
        raise GuardrailBlocked(masked or "Blocked by guardrail", _reasons(resp))

    return text


def _blocked(resp: dict) -> bool:
    """Whether any policy in an ApplyGuardrail assessment BLOCKED (rather than only
    anonymized). Unknown shapes count as a block: failing closed is the safe reading."""
    assessments = resp.get("assessments")
    if not isinstance(assessments, list) or not assessments:
        return True

    def walk(v) -> bool:
        if isinstance(v, dict):
            if v.get("action") == "BLOCKED" and v.get("detected", True) is not False:
                return True
            return any(walk(x) for x in v.values())
        if isinstance(v, list):
            return any(walk(x) for x in v)
        return False

    if walk(assessments):
        return True
    # Intervened with nothing blocked: it must have anonymized something to count as a mask.
    return not _anonymized(assessments)


def _anonymized(v) -> bool:
    if isinstance(v, dict):
        return v.get("action") == "ANONYMIZED" or any(_anonymized(x) for x in v.values())
    if isinstance(v, list):
        return any(_anonymized(x) for x in v)
    return False
