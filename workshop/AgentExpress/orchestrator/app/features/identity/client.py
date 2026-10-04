"""AgentCore Identity — a credential for an API an agent calls DIRECTLY.

An agent names identities in agentcore.identity.outbound. One from workflow.json
`identities` is created by the IaC as a credential provider (terraform/identities.tf,
cdk/lib/orchestrator-stack.ts) and listed in IDENTITY_PROVIDERS as
{name: {provider, type, scopes}}; any other name is taken to be a provider that already
exists in the account (OAuth2, client credentials).

The exchange is AgentCore Identity's own: this runtime's workload identity
(WORKLOAD_IDENTITY) gets a workload access token, and the token vault returns the
provider's OAuth access token or API key for it. Nothing here holds a secret.

Both calls return "" when the agent has no such identity or the exchange fails, so a
caller degrades to "no credential" rather than breaking — and the reason is printed.
"""

import asyncio
import json
import os

from app.common.config import REGION

_client = None


def _data_plane():
    global _client
    if _client is None:
        import boto3
        _client = boto3.client("bedrock-agentcore", region_name=REGION)
    return _client


def _providers() -> dict:
    try:
        got = json.loads(os.getenv("IDENTITY_PROVIDERS") or "{}")
    except ValueError:
        got = {}
    return got if isinstance(got, dict) else {}


def _workload_token() -> str:
    """This runtime's workload access token: the one AgentCore Runtime passed with the
    invocation when there is one, else minted for the build's workload identity."""
    try:
        from bedrock_agentcore.runtime.context import BedrockAgentCoreContext
        token = BedrockAgentCoreContext.get_workload_access_token()
        if token:
            return str(token)
    except Exception as e:  # noqa: BLE001 - an older SDK, or no request context
        print(f"[identity] no workload token in the request context ({type(e).__name__}); minting one")
    name = os.getenv("WORKLOAD_IDENTITY", "")
    if not name:
        raise RuntimeError("no workload identity: this deployment has no identity an agent uses directly")
    return _data_plane().get_workload_access_token(workloadName=name)["workloadAccessToken"]


def _resolve(name: str) -> dict:
    """{provider, type, scopes} for an identity name."""
    return _providers().get(name) or {"provider": name, "type": "oauth2", "scopes": []}


def _oauth_token(name: str, scopes: list[str] | None) -> str:
    p = _resolve(name)
    resp = _data_plane().get_resource_oauth2_token(
        workloadIdentityToken=_workload_token(), resourceCredentialProviderName=p["provider"],
        scopes=list(scopes or p.get("scopes") or []), oauth2Flow="M2M")
    return str(resp.get("accessToken") or "")


def _api_key(name: str) -> str:
    p = _resolve(name)
    resp = _data_plane().get_resource_api_key(
        workloadIdentityToken=_workload_token(), resourceCredentialProviderName=p["provider"])
    return str(resp.get("apiKey") or "")


async def get_token(provider_name: str, scopes: list[str] | None = None) -> str:
    """An OAuth access token from the named identity (or existing provider)."""
    if not provider_name:
        return ""
    try:
        return await asyncio.to_thread(_oauth_token, provider_name, scopes)
    except Exception as e:  # noqa: BLE001
        print(f"[identity] no token from {provider_name!r}: {type(e).__name__}: {e}")
        return ""


async def get_api_key(name: str) -> str:
    """The API key held by an apikey identity."""
    if not name:
        return ""
    try:
        return await asyncio.to_thread(_api_key, name)
    except Exception as e:  # noqa: BLE001
        print(f"[identity] no API key from {name!r}: {type(e).__name__}: {e}")
        return ""
