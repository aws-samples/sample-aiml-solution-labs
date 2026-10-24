"""Checks for a hosted MCP server, `tools.<key> = {"type": "mcp", "hosted": {code?, network?}}`,
for validate_build.py. The generic key checks (app/keys.json) already say `hosted` applies to
type "mcp", holds only `code` and `network`, and that exactly one of `endpoint` / `hosted` is
set; network_check.py checks `hosted.network`. These are the rest:

  - the key is part of the Runtime name ax_<build>_mcp_<key>; AgentRuntimeName is
    [a-zA-Z][a-zA-Z0-9_]{0,47}, so with the 11-character build name the key is at most 32
    https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-bedrockagentcore-runtime.html
  - the Gateway reaches it with SigV4 as its role, signing for "bedrock-agentcore"
    https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/gateway-building-adding-targets-authorization.html
  - `code` is a code source, checked by bff/code_source.py; only here may it be an `image`.
    The containerUri pattern also admits public.ecr.aws, .amazonaws.com.cn and tag+digest
    https://docs.aws.amazon.com/bedrock-agentcore-control/latest/APIReference/API_ContainerConfiguration.html
    but the CDK and Terraform pull grants only parse a private, commercial-partition ECR URI
    with a tag or a sha256 digest, so an image must also match ECR_IMAGE_RE.

Mirrors web/src/builder/hostedMcpCheck.ts word for word (both run
tests/fixtures/hosted_mcp_cases.json).
"""

from __future__ import annotations

import re

import code_source  # bff/code_source.py

#: 48 (AgentRuntimeName) - len("ax_12345678_mcp_").
MAX_KEY = 32
#: What cdk/lib/hosted-mcp.ts ecrRepositoryArn and terraform/tools_hosted_mcp.tf parse.
ECR_IMAGE_RE = re.compile(
    r"^\d{12}\.dkr\.ecr\.[a-z0-9-]+\.amazonaws\.com/[a-z0-9._/-]+(:[\w.-]+|@sha256:[0-9a-f]{64})?$", re.ASCII)
VARIANTS = ("git", "s3", "image")


def _code_problems(code, p: str) -> list[tuple[str, str]]:
    out = list(code_source.problems(code, p, allow=VARIANTS))
    image = code.get("image") if isinstance(code, dict) and len(code) == 1 else None
    if not out and isinstance(image, str) and not ECR_IMAGE_RE.fullmatch(image):
        out.append((f"{p}.image", ("a hosted MCP server's image must be a private ECR image URI, "
                                   "<account>.dkr.ecr.<region>.amazonaws.com/<repo>[:tag | @sha256:<digest>]")))
    return out


def check(key: str, tool: dict, path: str, where: dict, add) -> None:
    hosted = tool.get("hosted")
    if not isinstance(hosted, dict):
        return
    if len(key) > MAX_KEY:
        add("error", where, path, f"a hosted MCP server's key is part of its Runtime name "
            f"ax_<build>_mcp_<key>, which AgentCore caps at 48 characters: use at most {MAX_KEY}")
    if tool.get("auth") not in (None, "sigv4"):
        add("error", where, f"{path}.auth",
            'a hosted MCP server is reached with SigV4 as the Gateway role: leave auth unset or set "sigv4"')
    if tool.get("service") not in (None, "bedrock-agentcore"):
        add("error", where, f"{path}.service",
            'a hosted MCP server is signed for "bedrock-agentcore": leave service unset')
    if hosted.get("code") is not None:
        for p, message in _code_problems(hosted["code"], f"{path}.hosted.code"):
            add("error", where, p, message)
