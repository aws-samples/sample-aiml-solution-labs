/** Checks for an AgentCore Runtime's network: `orchestrator.network` (the shared orchestrator
 *  Runtime, where every main and task agent runs) and `agents.<id>.network` (a runtime
 *  "dedicated" agent's own Runtime). The generic key checks (keys.json) say both are objects
 *  with mode, subnetIds and securityGroupIds, and that an agent's `network` applies to
 *  "dedicated" only; these are the rest, and only what the AgentCore docs state:
 *  - mode: NetworkConfiguration networkMode, PUBLIC | VPC, Required
 *    https://docs.aws.amazon.com/bedrock-agentcore-control/latest/APIReference/API_NetworkConfiguration.html
 *  - subnets / securityGroups: VpcConfig, each Required, 1-16 items,
 *    subnet-[0-9a-zA-Z]{8,17} and sg-[0-9a-zA-Z]{8,17}
 *    https://docs.aws.amazon.com/bedrock-agentcore-control/latest/APIReference/API_VpcConfig.html
 *  Ids on mode PUBLIC would be sent nowhere, so they are rejected. Mirrors
 *  bff/network_check.py word for word (both run tests/fixtures/validation_cases.json). */
import { vocab, vocabExtra } from "./meta";
import type { Issue, Where } from "./validate";

type Add = (severity: Issue["severity"], where: Where, path: string, message: string) => void;
const isObj = (v: unknown): v is Record<string, unknown> => typeof v === "object" && v !== null && !Array.isArray(v);

export const NETWORK_MODES = vocab("networkModes");
export const MAX_NETWORK_IDS = Number(vocabExtra<number>("networkModes", "maxIds") ?? 16);
export const SUBNET_ID_RE = new RegExp(String(vocabExtra<string>("networkModes", "subnetPattern")));
export const SG_ID_RE = new RegExp(String(vocabExtra<string>("networkModes", "securityGroupPattern")));
const IDS = [["subnetIds", SUBNET_ID_RE, "subnet-"], ["securityGroupIds", SG_ID_RE, "sg-"]] as const;

function checkOne(net: unknown, where: Where, p: string, add: Add): void {
  if (!isObj(net)) return;
  const mode = net.mode;
  if (mode === undefined || mode === null) {
    add("error", where, `${p}.mode`, `is required: one of ${NETWORK_MODES.join(", ")}`);
    return;
  }
  if (typeof mode !== "string") return;
  if (!NETWORK_MODES.includes(mode)) {
    add("error", where, `${p}.mode`, `"${mode}" is not one of: ${NETWORK_MODES.join(", ")}`);
    return;
  }
  for (const [k, pattern, prefix] of IDS) {
    const v = net[k];
    if (mode !== "VPC") {
      if (k in net) add("error", where, `${p}.${k}`, 'applies to mode "VPC" only');
      continue;
    }
    if (v === undefined || v === null || (Array.isArray(v) && !v.length)) {
      add("error", where, `${p}.${k}`, `mode "VPC" needs 1-${MAX_NETWORK_IDS} ids, each ${prefix}...`);
    } else if (Array.isArray(v) && (v.length > MAX_NETWORK_IDS || !v.every((x) => typeof x === "string" && pattern.test(x)))) {
      add("error", where, `${p}.${k}`, `1-${MAX_NETWORK_IDS} ids, each ${prefix}...`);
    }
  }
}

export function checkNetwork(orch: unknown, agents: Record<string, unknown>, add: Add,
  tools: Record<string, unknown> = {}): void {
  checkOne(isObj(orch) ? orch.network : undefined, { kind: "block", name: "orchestrator" }, "orchestrator.network", add);
  for (const [aid, a] of Object.entries(agents)) {
    if (isObj(a) && String(a.runtime || "").toLowerCase() === "dedicated") {
      checkOne(a.network, { kind: "agent", id: aid }, `agents.${aid}.network`, add);
    }
  }
  // A hosted MCP server's own Runtime (tools.<key>.hosted.network), the same shape.
  for (const [key, t] of Object.entries(tools)) {
    if (isObj(t) && isObj(t.hosted)) {
      checkOne(t.hosted.network, { kind: "tool", id: key }, `tools.${key}.hosted.network`, add);
    }
  }
}
