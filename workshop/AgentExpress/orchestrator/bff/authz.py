"""Authorization for the actions a human can take on a run.

The JWT authorizer on /api/* answers "is this a valid user?". It does NOT answer
"may THIS user approve a review gate?". Without that second question, any
authenticated user can approve, deny, re-run or cancel any run — which for a
human-in-the-loop product is the wrong default.

This module answers it, driven entirely by the `authorization` block in
app/workflow.json:

    "authorization": {
      "groupsClaim": "cognito:groups",
      "actions": {
        "decision": ["approvers"],
        "rerun":    ["approvers"],
        "cancel":   ["approvers", "operators"],
        "evaluate": ["operators"],
        "insights": ["operators"]
      }
    }

Semantics, chosen so the default stays backwards compatible:

  * An action NOT listed in `actions` is unrestricted — any authenticated caller
    may perform it. So an absent or empty `authorization` block behaves exactly as
    before this module existed.
  * An action listed with a non-empty group list requires the caller to hold at
    least ONE of those groups.
  * An action listed with an EMPTY list is denied to everyone. That is deliberate:
    it is how you switch a capability off entirely.

`groupsClaim` is configurable because providers differ: Cognito issues
`cognito:groups`, while Auth0 needs a namespaced custom claim added by an action
(e.g. "https://your-app/roles"), Okta a `groups` claim on its authorization server, and
Entra ID `roles` (app roles). Read-only endpoints are not gated here — they are
already behind the authorizer, and hiding a run from a reviewer who can see the UI
buys nothing.
"""

import json
from pathlib import Path

import workflow

# The framework's closed value sets, from app/vocabulary.json — the same file the
# runtime, the CDK stack and the Terraform preconditions read. This list lived in three
# places, and a copy that drifted meant an action name one plane accepted and another
# rejected. Bundled next to this module by both IaC paths (see the BFF package staging).
_VOCAB_PATH = Path(__file__).resolve().parent / "vocabulary.json"
if not _VOCAB_PATH.exists():  # a source checkout, where nothing has staged a copy yet
    _VOCAB_PATH = Path(__file__).resolve().parent.parent / "app" / "vocabulary.json"
_VOCAB = json.loads(_VOCAB_PATH.read_text())
# Every key's DEFAULT, from app/defaults.json — generated from app/keys.json and read by
# all three planes. Staged beside this module by both IaC paths, same as the vocabulary
# above, because a Lambda bundle cannot reach app/.
_DEFAULTS_PATH = Path(__file__).resolve().parent / "defaults.json"
if not _DEFAULTS_PATH.exists():  # a source checkout
    _DEFAULTS_PATH = Path(__file__).resolve().parent.parent / "app" / "defaults.json"
_DEFAULTS = json.loads(_DEFAULTS_PATH.read_text())

# The mutating actions this module knows about. Keys are what workflow.json uses.
# `delete` (removing a completed run and its timeline) is included because it is
# irreversible; it is not an approval, but leaving it open while gating `cancel`
# would be inconsistent.
ACTIONS = tuple(_VOCAB["authorizationActions"]["values"])
# Actions that are CLOSED unless workflow.json names groups for them — the opposite of
# every other action. `audit` shows every user's activity and `admin` every user's builds
# and runs, so a workflow.json written before they existed (or with no authorization
# block at all) must not open them to all. Grant them to a group filled only from the
# backend (the sample's `admins`): the IaC refuses to make it the self-sign-up group.
CLOSED_UNLESS_GRANTED = frozenset({"audit", "admin"})
# Closed unless granted whenever workflow.json restricts anything, i.e. whenever the
# console has users to keep apart: Insights analyse EVERY user's runs. A deployment with
# no authorization block is single-tenant, and there it stays open as before.
CLOSED_WHEN_RESTRICTED = frozenset({"insights"})

# From the RAW workflow, not the browser projection: these rules are enforced
# server-side, and the projection exists to limit what LEAVES the server. Reading
# them from the projection would couple enforcement to a display concern.
_AUTHZ = workflow.RAW.get("authorization") or {}
GROUPS_CLAIM: str = _AUTHZ.get("groupsClaim") or _DEFAULTS["authorization"]["groupsClaim"]
# action -> list of groups permitted. Absent key = unrestricted.
_RULES: dict = _AUTHZ.get("actions") or {}

# True when workflow.json actually restricts something. When it does not, this
# module is a no-op and every authenticated caller is allowed.
ENABLED: bool = bool(_RULES)


def claims(event: dict) -> dict:
    """The validated JWT claims API Gateway attached to the request.

    Empty when idp = "none" (no authorizer), which is why deploying RBAC together
    with idp = "none" is rejected at plan/synth time — there would be no identity
    to authorize against.
    """
    return (event.get("requestContext", {}).get("authorizer", {})
            .get("jwt", {}).get("claims", {}) or {})


def groups_of(event: dict) -> list[str]:
    """The caller's groups, tolerating the several shapes this claim arrives in.

    A list already (defaults.get("authorization", "groupsClaim") via a JWT authorizer), a JSON array encoded as
    a string, or a space/comma-separated string — API Gateway and the two providers
    are not consistent about this, so normalise rather than assume.
    """
    raw = claims(event).get(GROUPS_CLAIM)
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(g).strip() for g in raw if str(g).strip()]
    s = str(raw).strip()
    if not s:
        return []
    if s.startswith("["):
        # Either real JSON, or Cognito's bracketed form: [admins approvers]
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                return [str(g).strip() for g in parsed if str(g).strip()]
        except ValueError:
            pass
        s = s.strip("[]")
    return [p for p in (x.strip() for x in s.replace(",", " ").split()) if p]


def permitted(action: str, event: dict) -> bool:
    """May this caller perform `action`?"""
    if action not in _RULES:                      # not restricted by config
        if action in CLOSED_UNLESS_GRANTED:
            return False
        return not (ENABLED and action in CLOSED_WHEN_RESTRICTED)
    allowed = _RULES.get(action) or []
    if not allowed:
        return False  # explicitly closed to everyone
    return bool(set(allowed) & set(groups_of(event)))


def permitted_actions(event: dict) -> list[str]:
    """Every action this caller may perform — for /api/me and to filter the
    assistant's action tools, so it cannot be used to route around these checks."""
    return [a for a in ACTIONS if permitted(a, event)]


def denial(action: str, event: dict) -> dict:
    """The body for a 403. Names the groups required and what the caller has, so a
    misconfigured group mapping is diagnosable from the response alone."""
    return {
        "error": f"not authorized to perform '{action}'",
        "requiredGroups": _RULES.get(action) or [],
        "yourGroups": groups_of(event),
        "groupsClaim": GROUPS_CLAIM,
    }
