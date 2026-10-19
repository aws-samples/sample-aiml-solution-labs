"""The deployed AWS resource behind each component the run graph shows.

A component row (workflow.components) says what is attached in workflow.json words: the
tool key, "strict", "agent memory". A reader clicking it in the execution plane wants the
thing in their account: the Gateway target, the Bedrock Guardrail and its version, the
AgentCore Memory, the Cedar policy, the evaluator, the credential provider.

NO NEW READS. Every name here is the one both IaC paths give the resource, computed from
workflow.json and the deployment's agent name, so the BFF needs no AWS call and no new
grant. Each formula names the Terraform and CDK lines it mirrors; a name the BFF cannot
know (a Knowledge Base the deploy names from a storage digest) is "not available" rather
than guessed. IDs come only from what the BFF is already given (SEMANTIC_MEMORY_ID and
MEMORIES, for the memory inspector).

Names and IDs only: never a statement, prompt, endpoint, secret or credential.
"""
from __future__ import annotations

import hashlib
import json
import os

#: Both IaC paths name the BFF `AgentCoreBFF-<agentName>` (terraform/bff.tf
#: aws_lambda_function.bff, cdk/lib/orchestrator-stack.ts bffFunctionName), and Lambda
#: always sets AWS_LAMBDA_FUNCTION_NAME (a reserved variable), so the agent name needs no
#: environment value of its own.
_FUNCTION_PREFIX = "AgentCoreBFF-"

NOT_AVAILABLE = "not available"

#: The tool types deployed as an AgentCore Gateway target named after the tool key
#: (terraform/tools.tf, tools_smithy.tf, kb.tf; cdk/lib/tool-plane.ts CfnGatewayTarget `name`).
GATEWAY_TYPES = frozenset({"kb", "mcp", "openapi", "lambda", "websearch", "apigateway", "smithy"})


def own_agent_name() -> str:
    """This deployment's agent name, from its own function name; "" outside Lambda."""
    fn = os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "")
    return fn[len(_FUNCTION_PREFIX):] if fn.startswith(_FUNCTION_PREFIX) else ""


def own_memory_ids() -> dict:
    """{"": the semantic memory id, <name>: a named memory's id}, from the env the memory
    inspector already has (terraform/memories.tf, cdk bff.addEnvironment)."""
    try:
        named = json.loads(os.environ.get("MEMORIES") or "{}")
    except ValueError:
        named = {}
    out = {k: v for k, v in (named.items() if isinstance(named, dict) else []) if isinstance(v, str) and v}
    if os.environ.get("SEMANTIC_MEMORY_ID"):
        out[""] = os.environ["SEMANTIC_MEMORY_ID"]
    return out


def _hex(algo: str, s: str, n: int) -> str:
    return hashlib.new(algo, s.encode()).hexdigest()[:n]


def _obj(v) -> dict:
    return v if isinstance(v, dict) else {}


def _row(label: str, value: str) -> dict:
    return {"label": label, "value": value or NOT_AVAILABLE}


def _guardrail_has_policy(g: dict) -> bool:
    """Whether the build-wide guardrail exists: terraform/guardrail.tf gr_has_policy,
    cdk grHasPolicy (Bedrock refuses a guardrail with no policy)."""
    return any(g.get(k) for k in ("contentFilters", "deniedWords", "managedWordLists", "deniedTopics", "piiEntities"))


def _policy_enabled(workflow: dict, defaults: dict) -> bool:
    got = _obj(_obj(workflow.get("orchestrator")).get("policy")).get("enabled")
    return got if isinstance(got, bool) else bool(_obj(_obj(defaults.get("orchestrator")).get("policy")).get("enabled"))


def resources(workflow: dict, agent_id: str, comp: dict, agent_name: str,
              memory_ids: dict | None = None, defaults: dict | None = None) -> list[dict]:
    """[{label, value}] for one component of one agent: what it is deployed as. [] for a
    kind with no AWS resource of its own (a skill, asking HITL)."""
    a = agent_name
    dash = a.replace("_", "-")
    agent = _obj(_obj(workflow.get("agents")).get(agent_id))
    core = _obj(agent.get("agentcore"))
    kind = comp.get("kind")

    if kind == "tool":
        key = str(comp.get("tool") or comp.get("name") or "")
        t = _obj(_obj(workflow.get("tools")).get(key))
        if str(t.get("type") or "") not in GATEWAY_TYPES:
            return []
        # A tool that acts as the person is on the person Gateway (gateway.tf `-gwu`).
        person = t.get("auth") in ("user", "obo")
        out = [_row("Gateway target", key), _row("Gateway", f"{dash}-{'gwu' if person else 'gw'}" if a else "")]
        if t.get("type") == "kb":
            # An existing KB is named by its id; one this deploy creates is named from a
            # digest of its storage settings (kb.tf kb_name), which the BFF does not have.
            out.append(_row("Knowledge Base ID", str(t.get("knowledgeBaseId") or "")))
        return out

    if kind == "guardrail":
        g = _obj(core.get("guardrails"))
        # Every guardrail is applied at DRAFT (GUARDRAIL_VERSION in both IaC paths;
        # guardrails_env for the named ones).
        if isinstance(g.get("use"), str) and g["use"]:
            return [_row("Bedrock Guardrail", f"{a}-gr-{g['use']}" if a else ""), _row("Version", "DRAFT")]
        if isinstance(g.get("guardrailId"), str) and g["guardrailId"]:
            return [_row("Bedrock Guardrail ID", g["guardrailId"]), _row("Version", "DRAFT")]
        if not _guardrail_has_policy(_obj(workflow.get("guardrail"))):
            return [_row("Bedrock Guardrail", "none deployed: the guardrail block sets no policy, so this check does nothing")]
        return [_row("Bedrock Guardrail", f"{a}-guardrail" if a else ""), _row("Version", "DRAFT")]

    if kind == "memory":
        use = _obj(core.get("memory")).get("use")
        use = use if isinstance(use, str) else ""
        # terraform evaluations.tf memory "named" / main.tf "semantic"; cdk `${agentName}_m_${n}` / `_semantic`.
        name = (f"{a}_m_{use}" if use else f"{a}_semantic") if a else ""
        return [_row("AgentCore Memory", name), _row("Memory ID", (memory_ids or {}).get(use, ""))]

    if kind == "policy":
        p, key = str(comp.get("name") or ""), str(comp.get("tool") or "")
        if not _policy_enabled(workflow, defaults or {}):
            return [_row("Cedar policy", "not deployed: orchestrator.policy is off")]
        if not a:
            return [_row("Policy engine", ""), _row("Cedar policy", "")]
        suffix = _hex("sha1", a, 8)
        stmt = str(_obj(_obj(workflow.get("policies")).get(p)).get("statement") or "")
        # A "{{tool}}" template is deployed once per tool (policy.tf named_policy_list,
        # cdk namedPolicyName); any other once, as named (customPolicyName).
        deployed = f"{p[:20]}{_hex('sha1', f'{p}.{key}', 8)}" if "{{tool}}" in stmt else p
        return [_row("Policy engine", f"{a}_policy"), _row("Cedar policy", f"custom_{deployed}_{suffix}")]

    if kind == "evaluation":
        e = str(comp.get("evaluator") or "")
        if not e:
            return []
        if e.startswith("Builtin."):
            return [_row("Evaluator ID", e)]
        name = e[len("Custom."):] if e.startswith("Custom.") else ""
        own = [c.get("name") for c in (_obj(core.get("evaluations")).get("custom") or []) if isinstance(c, dict)]
        if not a or not name:
            return [_row("Evaluator", "")]
        # evaluations.tf custom_evaluators / shared; cdk evaluatorName.
        if name in own:
            return [_row("Evaluator", f"{a}_{_hex('sha256', f'{agent_id}.{name}', 6)}_{name}"[:48])]
        if name in _obj(workflow.get("evaluators")):
            return [_row("Evaluator", f"{a}_{_hex('sha256', f'*.{name}', 6)}_{name}"[:48])]
        return [_row("Evaluator", "")]

    if kind == "identity":
        n = str(comp.get("name") or "")
        # identities.tf identity_provider_name; cdk `bedrock-agentcore-${dash}-id-${n}`.
        made = a and n in _obj(workflow.get("identities"))
        return [_row("Credential provider", f"bedrock-agentcore-{dash}-id-{n}" if made else "")]

    return []
