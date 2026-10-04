"""Credentials a runtime needs, read from Secrets Manager at container start.

The IaC no longer puts the Gateway client secret(s) or the A2A bearer tokens in the
runtime's environment variables, where anyone allowed GetAgentRuntime could read them.
It stores them in one secret per runtime and passes only its ARN, RUNTIME_SECRET_ARN.
This module copies the secret's keys into os.environ BEFORE app.common.config and the
modules after it read them, so the code that uses them is unchanged:

    {"GATEWAY_CLIENT_SECRET": "...", "GATEWAY_AGENT_CLIENTS": {...}, "A2A_TOKENS": {...}}

A key already in the environment wins (local development sets them directly). A JSON
object is re-encoded as the JSON string the readers expect.
"""

from __future__ import annotations

import json
import os

#: The only keys copied: a secret cannot set anything else in the environment.
KEYS = ("GATEWAY_CLIENT_SECRET", "GATEWAY_AGENT_CLIENTS", "A2A_TOKENS")

_loaded = False


def load(client=None) -> list[str]:
    """Copy the runtime secret into the environment once. Returns the keys it set.

    Raises if RUNTIME_SECRET_ARN is set and the secret cannot be read: a runtime that
    silently started without its Gateway credentials would fail every tool call later,
    with an error far from the cause."""
    global _loaded
    arn = os.getenv("RUNTIME_SECRET_ARN", "")
    if _loaded or not arn:
        return []
    if client is None:
        import boto3
        client = boto3.client("secretsmanager")
    raw = client.get_secret_value(SecretId=arn)["SecretString"]
    values = json.loads(raw or "{}")
    if not isinstance(values, dict):
        raise ValueError("RUNTIME_SECRET_ARN must hold a JSON object")
    done = []
    for key in KEYS:
        if key not in values or os.getenv(key):
            continue
        v = values[key]
        os.environ[key] = v if isinstance(v, str) else json.dumps(v)
        done.append(key)
    _loaded = True
    return done
