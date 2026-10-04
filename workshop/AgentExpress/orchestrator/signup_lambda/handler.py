"""Cognito triggers: the default group for a self-registered user, and the sign-in log.

Two jobs, each switched on by its own environment variable:

* DEFAULT_GROUP — post-confirmation: put a self-registered user in the default group.
  Set only when self sign-up is on (CDK `-c selfSignUp=true`, Terraform
  `cognito.self_signup = true`). Without it a new user would sign in with no groups, and
  every gated action in workflow.json `authorization` — deploy, destroy, decide, delete —
  would 403 on their own builds and runs.

  The group grants actions on the caller's OWN builds and runs only: the BFF scopes every
  build and run to the user who created it (bff/builds.py, bff/handler.py _own_session),
  so membership never reaches anyone else's. Cross-user features (Insights, everyone's
  activity) are left to groups this trigger never grants.

* AUDIT_TABLE — post-authentication: record each sign-in in the audit log (the builds
  table; layout in bff/buildstore.py). Set whenever the console has a Builder and its
  own user pool. Sign-outs are recorded by the BFF (POST /api/audit/logout), which the
  page calls before it signs out.
"""

import os
import secrets
from datetime import UTC, datetime

import boto3

GROUP = os.environ.get("DEFAULT_GROUP", "")
AUDIT_TABLE = os.environ.get("AUDIT_TABLE", "")
_cognito = boto3.client("cognito-idp") if GROUP else None
_table = boto3.resource("dynamodb").Table(AUDIT_TABLE) if AUDIT_TABLE else None


def audit_items(owner: str, email: str, action: str, detail: dict,
                ts: str = "", rand: str = "") -> list[dict]:
    """bff/buildstore.audit_items, copied: this function ships alone and cannot import
    it. tests/test_audit.py holds the two to the same output."""
    ts = ts or datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    sk = f"{ts}#{rand or secrets.token_hex(4)}"
    body = {"ts": ts, "owner": owner, "email": email, "action": action,
            "detail": {k: v for k, v in detail.items() if v not in (None, "")}}
    return [{"pk": f"AUDIT#{owner}", "sk": sk, **body},
            {"pk": f"AUDIT_DAY#{ts[:10]}", "sk": sk, **body}]


def _log_sign_in(event: dict) -> None:
    attrs = (event.get("request") or {}).get("userAttributes") or {}
    owner = attrs.get("sub") or event.get("userName", "")
    try:
        client = (event.get("callerContext") or {}).get("clientId", "")
        for item in audit_items(owner, attrs.get("email", ""), "login", {"client": client}):
            _table.put_item(Item=item)
    except Exception as e:  # noqa: BLE001 - a failed log line must never block a sign-in
        print(f"[audit] could not record a sign-in: {type(e).__name__}: {e}")


def handler(event, context):
    source = event.get("triggerSource", "")
    # Only a completed SELF sign-up. PostConfirmation also fires for a confirmed
    # forgotten-password reset, which must not change anyone's groups.
    if GROUP and source == "PostConfirmation_ConfirmSignUp":
        _cognito.admin_add_user_to_group(
            UserPoolId=event["userPoolId"], Username=event["userName"], GroupName=GROUP)
        print(f"[signup] added a new user to {GROUP!r}")
    if _table is not None and source == "PostAuthentication_Authentication":
        _log_sign_in(event)
    # A Cognito trigger must hand the event back unchanged.
    return event
