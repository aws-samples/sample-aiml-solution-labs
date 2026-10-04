/** "What is this?" — the AppLayout tools drawer.
 *
 *  Two audiences, so two texts:
 *    - THE CONSOLE (consoleMode "builder"): what AgentExpress lets you build, deploy and
 *      govern. Fixed prose, short, one line per capability.
 *    - A BUILD'S OWN APP: what the framework does for a run, plus "This deployment",
 *      derived from the workflow actually loaded (stages, agents, gates, tools) so it
 *      never drifts from workflow.json. */

import Box from "@cloudscape-design/components/box";
import HelpPanel from "@cloudscape-design/components/help-panel";
import KeyValuePairs from "@cloudscape-design/components/key-value-pairs";
import SpaceBetween from "@cloudscape-design/components/space-between";
import { useMemo } from "react";

import type { Workflow } from "../types";
import { toolsOf } from "../lib/tools";
import { FRAMEWORK_VERSION } from "../builder/meta";

type Item = { title: string; body: string };

/** The console: what a builder can do here. */
export const CONSOLE_FEATURES: Item[] = [
  { title: "Design by chatting",
    body: "Describe the workflow to the AgentExpress Assistant, attach documents or S3 files, and it drafts the agents, tools and steps. Every change shows on the canvas." },
  { title: "Or build it visually",
    body: "Drag agents onto the canvas, run them in sequence or in parallel, add review gates and branches, and set each agent's model and prompt." },
  { title: "Connect agents to tools",
    body: "Web search, knowledge bases from your own documents, MCP servers, OpenAPI and API Gateway APIs, and Lambda code tools you write and test in the console." },
  { title: "Built-in AgentCore features",
    body: "Guardrails, memory, custom evaluators, identities (Cognito, OAuth, API keys), Cedar policies, image generation and image reading." },
  { title: "A shared library",
    body: "Save tools, guardrails, memory, evaluators, identities and policies once and reuse them in any build. Share them with people, groups or everyone." },
  { title: "Deploy in one click",
    body: "Deploy with AWS CDK or Terraform into this account or any AWS account you connect. Each build gets its own app, sign-in and API." },
  { title: "Share and collaborate",
    body: "Share a build with colleagues or groups. They see its design, its chat and its app." },
];

/** A build's own app: what the framework does for a run. */
export const APP_FEATURES: Item[] = [
  { title: "Review gates",
    body: "A stage can pause for you to approve, revise or deny. Revise sends your note back to the agent and re-runs just that agent." },
  { title: "Runs that wait",
    body: "A paused run is saved. Close the tab and come back later; it is still waiting at its gate." },
  { title: "Re-run any step",
    body: "Re-run a step and everything after it. Earlier versions of each output are kept for comparison." },
  { title: "Safe by design",
    body: "Guardrails, Cedar policies and group permissions are enforced on the server, not only hidden in the UI." },
  { title: "Cost and quality",
    body: "Observability shows cost, tokens and latency per call, the exact prompts, and evaluation scores." },
  { title: "An assistant that can act",
    body: "The chat bubble answers questions about a run and can take the same actions as the buttons, with the same permissions." },
];

function Features({ items }: { items: Item[] }) {
  return (
    <SpaceBetween size="s">
      {items.map((f) => (
        <div key={f.title}>
          <Box variant="awsui-key-label">{f.title}</Box>
          <Box variant="p">{f.body}</Box>
        </div>
      ))}
    </SpaceBetween>
  );
}

export function AboutPanel({ workflow, heading, console: isConsole = false }: {
  workflow: Workflow; heading: string; console?: boolean;
}) {
  const facts = useMemo(() => {
    const steps = workflow.steps ?? [];
    const agents = workflow.agents ?? {};
    const ids = steps.flatMap((s) => s.parallel ?? s.sequence ?? (s.agent ? [s.agent] : []));
    const gates = steps.filter((s) => s.hitl);
    const branches = steps.filter((s) => s.branch);
    const placements = new Map<string, number>();
    for (const id of ids) {
      const where = agents[id]?.runtime ?? "main";
      placements.set(where, (placements.get(where) ?? 0) + 1);
    }
    const tools = new Set(ids.flatMap((id) => toolsOf(agents[id]?.tool)));
    return { steps, ids, gates, branches, placements, tools };
  }, [workflow]);

  if (isConsole) {
    return (
      <HelpPanel header={<h2>{`About ${heading}`}</h2>}>
        <SpaceBetween size="l">
          <Box variant="p">
            AgentExpress turns a description of a business process into a working
            multi-agent application on Amazon Bedrock AgentCore, with no code to write.
            Design it, deploy it to your AWS account, and share it.
          </Box>
          <div>
            <Box variant="h3">What you can do</Box>
            <Features items={CONSOLE_FEATURES} />
          </div>
          <div>
            <Box variant="h3">Get started</Box>
            <Box variant="p">
              1. Under <b>Build</b>, press <b>+</b> for a new build.<br />
              2. Tell the Assistant what the workflow should do, or drag agents onto the canvas.<br />
              3. Fix anything listed under Problems, then choose <b>Deploy</b>.<br />
              4. Open the build&apos;s app and start a run.
            </Box>
          </div>
          <Box variant="small" color="text-body-secondary">Framework {FRAMEWORK_VERSION || "—"}</Box>
        </SpaceBetween>
      </HelpPanel>
    );
  }

  const placementText = [...facts.placements.entries()]
    .map(([where, n]) => `${n} ${where}`)
    .join(", ") || "—";

  return (
    <HelpPanel header={<h2>{`About ${heading}`}</h2>}>
      <SpaceBetween size="l">
        <Box variant="p">
          Give it one request and a team of AI agents works through it, pausing where a
          person has to sign off, and keeping every output, cost and decision.
        </Box>

        <div>
          <Box variant="h3">This deployment</Box>
          <KeyValuePairs
            columns={1}
            items={[
              { label: "Framework", value: FRAMEWORK_VERSION || "—" },
              { label: "Stages", value: String(facts.steps.length) },
              { label: "Agents", value: String(facts.ids.length) },
              { label: "Placement", value: placementText },
              {
                label: "Review gates",
                // A gate on a single-agent step has no gateName, so it is named by the
                // agent it guards.
                value: facts.gates.length
                  ? facts.gates
                    .map((g) => g.gateName
                      ?? (g.agent ? (workflow.agents[g.agent]?.name ?? g.agent) : null)
                      ?? g.gateId
                      ?? "a stage")
                    .join(", ")
                  : "none",
              },
              {
                label: "Conditional stages",
                value: facts.branches.length ? String(facts.branches.length) : "none",
              },
              {
                label: "Data sources",
                value: facts.tools.size ? [...facts.tools].join(", ") : "none declared",
              },
            ]}
          />
        </div>

        <div>
          <Box variant="h3">Key features</Box>
          <Features items={APP_FEATURES} />
        </div>

        <div>
          <Box variant="h3">Get started</Box>
          <Box variant="p">
            Choose <b>Start run</b> and describe what you want. Open the run to watch the
            graph fill in. When a stage turns amber it is waiting for you: read the outputs
            and approve, revise or deny.
          </Box>
        </div>
      </SpaceBetween>
    </HelpPanel>
  );
}
