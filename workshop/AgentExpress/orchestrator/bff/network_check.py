"""Checks for an AgentCore Runtime's network: `orchestrator.network` (the shared orchestrator
Runtime, where every main and task agent runs) and `agents.<id>.network` (a runtime
"dedicated" agent's own Runtime), for validate_build.py.

The generic key checks (app/keys.json) already say both are objects with the keys mode,
subnetIds and securityGroupIds, and that `network` on an agent applies to runtime
"dedicated" only. These are the rest, and only what the AgentCore docs state:
  - mode: NetworkConfiguration networkMode, Valid Values PUBLIC | VPC, Required
    https://docs.aws.amazon.com/bedrock-agentcore-control/latest/APIReference/API_NetworkConfiguration.html
  - subnets / securityGroups: VpcConfig, each Required, 1-16 items,
    subnet-[0-9a-zA-Z]{8,17} and sg-[0-9a-zA-Z]{8,17}
    https://docs.aws.amazon.com/bedrock-agentcore-control/latest/APIReference/API_VpcConfig.html
Ids on mode PUBLIC would be sent nowhere (the IaC writes NetworkModeConfig for VPC only), so
they are rejected like any setting that controls nothing.

Mirrors web/src/builder/networkCheck.ts word for word (both run
tests/fixtures/validation_cases.json).
"""

from __future__ import annotations

import re


def check(orch, agents: dict, add, vocab_get, tools: dict | None = None) -> None:
    spec = vocab_get("networkModes") or {}
    modes = list(spec.get("values") or [])
    most = int(spec.get("maxIds") or 16)
    ids = (("subnetIds", re.compile(str(spec.get("subnetPattern"))), "subnet-"),
           ("securityGroupIds", re.compile(str(spec.get("securityGroupPattern"))), "sg-"))

    def one(net, where: dict, p: str) -> None:
        if not isinstance(net, dict):
            return
        mode = net.get("mode")
        if mode is None:
            add("error", where, f"{p}.mode", f"is required: one of {', '.join(modes)}")
            return
        if not isinstance(mode, str):
            return
        if mode not in modes:
            add("error", where, f"{p}.mode", f'"{mode}" is not one of: {", ".join(modes)}')
            return
        for k, pattern, prefix in ids:
            v = net.get(k)
            if mode != "VPC":
                if k in net:
                    add("error", where, f"{p}.{k}", 'applies to mode "VPC" only')
                continue
            if v is None or (isinstance(v, list) and not v):
                add("error", where, f"{p}.{k}", f'mode "VPC" needs 1-{most} ids, each {prefix}...')
            elif isinstance(v, list) and (len(v) > most or not all(isinstance(x, str) and pattern.match(x) for x in v)):
                add("error", where, f"{p}.{k}", f"1-{most} ids, each {prefix}...")

    one(orch.get("network") if isinstance(orch, dict) else None,
        {"kind": "block", "name": "orchestrator"}, "orchestrator.network")
    for aid, a in agents.items():
        if isinstance(a, dict) and str(a.get("runtime") or "").lower() == "dedicated":
            one(a.get("network"), {"kind": "agent", "id": aid}, f"agents.{aid}.network")
    # A hosted MCP server's own Runtime (tools.<key>.hosted.network), the same shape.
    for key, t in (tools or {}).items():
        if isinstance(t, dict) and isinstance(t.get("hosted"), dict):
            one(t["hosted"].get("network"), {"kind": "tool", "id": key}, f"tools.{key}.hosted.network")
