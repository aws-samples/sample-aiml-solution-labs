"""The person a run acts as, for tools that act as the PERSON (auth "user" / "obo").

Such a tool is on the person Gateway (GATEWAY_USER_URL), which trusts the app's sign-in,
not the agents' machine client: each call carries the sign-in token of the person who
started the run (or resumed it, when that is the same person). The BFF sends it with
the invocation (bff/handler.py `user_token`), the runtime holds it here for the
invocation's tasks, and nothing else ever sees it:

  * a context variable, not run state: it is never checkpointed, logged or returned,
    so it does not outlive the invocation that brought it;
  * set before the run's task is spawned, so the agents' tasks (copies of that
    context) see it, and a later invocation by someone else does not.

A person tool called with no token (a reviewer who is not the run's owner approved a
gate, a trigger started the run) fails with PersonNeeded rather than acting as anyone.
"""
from __future__ import annotations

import contextvars
import re

_token: contextvars.ContextVar[str] = contextvars.ContextVar("person_token", default="")

#: Where AgentCore Identity sends a person to connect their account (URL elicitation).
CONSENT_URL = re.compile(r"https://bedrock-agentcore\.[a-z0-9-]+\.amazonaws\.com/identities/oauth2/authorize\?[^\s\"'<>\\]+")


def set_token(token: str) -> None:
    """The caller's sign-in token for this invocation ("" when there is none)."""
    _token.set(token if isinstance(token, str) and token.count(".") == 2 else "")


def token() -> str:
    return _token.get()


def consent_url(text: str) -> str:
    """The connect-your-account address in a Gateway refusal, or ""."""
    clean = (text or "").replace("\\u0026", "&").replace("&amp;", "&")
    m = CONSENT_URL.search(clean)
    return m.group(0) if m else ""
