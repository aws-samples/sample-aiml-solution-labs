/** The "Network" sections: where an AgentCore Runtime runs, PUBLIC or in your VPC.
 *  `orchestrator.network` (Settings) is the shared orchestrator Runtime, where every main and
 *  task agent runs; `agents.<id>.network` (a dedicated agent's panel) is that agent's own
 *  Runtime. Unset is PUBLIC, and choosing Public removes the key, so the deploy is what it was.
 *  Modes and limits are the API's (app/vocabulary.json networkModes):
 *  https://docs.aws.amazon.com/bedrock-agentcore-control/latest/APIReference/API_VpcConfig.html */
import Box from "@cloudscape-design/components/box";
import ExpandableSection from "@cloudscape-design/components/expandable-section";
import FormField from "@cloudscape-design/components/form-field";
import Input from "@cloudscape-design/components/input";
import Link from "@cloudscape-design/components/link";
import SegmentedControl from "@cloudscape-design/components/segmented-control";
import SpaceBetween from "@cloudscape-design/components/space-between";
import { MAX_NETWORK_IDS, NETWORK_MODES } from "./networkCheck";
import type { Issue } from "./validate";

type Obj = Record<string, unknown>;
const isObj = (v: unknown): v is Obj => typeof v === "object" && v !== null && !Array.isArray(v);

export const VPC_DOC = "https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/agentcore-vpc.html";
const LABEL: Record<string, string> = { PUBLIC: "Public", VPC: "VPC" };

/** Why an agent's own network does not apply, by runtime ("" for dedicated). */
export function agentNetworkNote(runtime: string): string {
  if (runtime === "dedicated") return "";
  if (runtime === "a2a") return "Not available for a remote (a2a) agent: it runs on its owner's network, outside this build.";
  if (runtime === "harness") return "A harness agent's network is set under Harness settings, Environment.";
  return "Runs inside the shared orchestrator Runtime; switch this agent to Dedicated to give it its own network.";
}

/** The mode and ids for one Runtime. `value` is the `network` object, or undefined for PUBLIC. */
export function NetworkFields({ value, onChange, issues = [], path, disabledReason }: {
  value: unknown;
  onChange: (next: Obj | undefined) => void;
  issues?: Issue[];
  /** Where it is written, e.g. `orchestrator.network`, to show its issues. */
  path: string;
  disabledReason?: string;
}) {
  const net = isObj(value) ? value : {};
  const mode = typeof net.mode === "string" ? net.mode : "PUBLIC";
  const err = (k: string) => issues.filter((i) => i.path === `${path}.${k}` || (k === "mode" && i.path === path))
    .map((i) => i.message).join(" ") || undefined;
  const ids = (k: string) => (Array.isArray(net[k]) ? (net[k] as unknown[]).map(String).join(", ") : "");
  const setIds = (k: string, text: string) =>
    onChange({ ...net, [k]: text.split(",").map((x) => x.trim()).filter(Boolean) });
  const off = Boolean(disabledReason);
  return (
    <SpaceBetween size="m">
      {disabledReason ? <Box variant="small" color="text-status-inactive" data-testid="network-reason">{disabledReason}</Box> : null}
      <FormField label="Network mode" errorText={err("mode")}
        description="Public: the Runtime reaches the internet and AWS endpoints directly. VPC: it runs in your subnets, with no internet unless they route to a NAT gateway.">
        <SegmentedControl label="Network mode" selectedId={NETWORK_MODES.includes(mode) ? mode : null}
          options={NETWORK_MODES.map((m) => ({ id: m, text: LABEL[m] ?? m, disabled: off }))}
          onChange={({ detail }) => onChange(detail.selectedId === "VPC"
            ? { mode: "VPC", subnetIds: Array.isArray(net.subnetIds) ? net.subnetIds : [],
              securityGroupIds: Array.isArray(net.securityGroupIds) ? net.securityGroupIds : [] }
            : undefined)} />
      </FormField>
      {mode === "VPC" && !off ? (
        <>
          <FormField label="Subnet ids" errorText={err("subnetIds")}
            description={`Comma-separated, 1-${MAX_NETWORK_IDS}, in Availability Zones AgentCore supports. Two private subnets in different AZs are recommended.`}>
            <Input ariaLabel="Subnet ids" value={ids("subnetIds")} placeholder="subnet-0abc…, subnet-0def…"
              onChange={({ detail }) => setIds("subnetIds", detail.value)} />
          </FormField>
          <FormField label="Security group ids" errorText={err("securityGroupIds")}
            description={`Comma-separated, 1-${MAX_NETWORK_IDS}. Their outbound rules must allow what the agents call.`}>
            <Input ariaLabel="Security group ids" value={ids("securityGroupIds")} placeholder="sg-0abc…"
              onChange={({ detail }) => setIds("securityGroupIds", detail.value)} />
          </FormField>
          <Box variant="small" color="text-body-secondary">
            The VPC needs VPC endpoints or a NAT gateway for the AWS services the agents call.{" "}
            <Link href={VPC_DOC} external externalIconAriaLabel="Opens in a new tab">AgentCore VPC requirements</Link>
          </Box>
        </>
      ) : null}
    </SpaceBetween>
  );
}

/** The agent panel's Network section. Editable for a dedicated agent; for any other runtime, says why not. */
export function AgentNetworkSettings({ agent, runtime, onAgent, issues, path }: {
  agent: Obj;
  runtime: string;
  onAgent: (next: Obj) => void;
  issues?: Issue[];
  path: string;
}) {
  const note = agentNetworkNote(runtime);
  return (
    <ExpandableSection headerText="Network" variant="container"
      headerDescription="Where this agent's own AgentCore Runtime runs: Public (default) or your VPC.">
      {runtime === "dedicated" ? (
        <NetworkFields value={agent.network} path={path} issues={issues}
          onChange={(net) => {
            const out = { ...agent };
            if (net) out.network = net; else delete out.network;
            onAgent(out);
          }} />
      ) : runtime === "a2a" ? (
        <NetworkFields value={undefined} path={path} disabledReason={note} onChange={() => undefined} />
      ) : (
        <Box variant="small" color="text-body-secondary" data-testid="network-note">{note}</Box>
      )}
    </ExpandableSection>
  );
}
