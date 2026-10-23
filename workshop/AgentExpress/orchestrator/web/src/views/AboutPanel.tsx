/** "What is this?" — the AppLayout tools drawer.
 *
 *  Two audiences, so two texts:
 *    - THE CONSOLE (consoleMode "builder"): what AgentExpress lets you build, deploy and
 *      govern. Fixed prose, short, one line per capability.
 *    - A BUILD'S OWN APP: what the framework does for a run, plus "This deployment",
 *      derived from the workflow actually loaded (stages, agents, gates, tools) so it
 *      never drifts from workflow.json.
 *  The console's text opens with a link to the demo video when one is configured. */

import Box from "@cloudscape-design/components/box";
import HelpPanel from "@cloudscape-design/components/help-panel";
import KeyValuePairs from "@cloudscape-design/components/key-value-pairs";
import Link from "@cloudscape-design/components/link";
import SpaceBetween from "@cloudscape-design/components/space-between";
import { useMemo } from "react";

import type { Workflow } from "../types";
import { toolsOf } from "../lib/tools";
import { FRAMEWORK_VERSION } from "../builder/meta";
import { workflowVersionLabel } from "../lib/workflowVersion";
import { InfoLink } from "../docs/InfoLink";

type Item = { title: string; body: string };

/** The console: what a builder can do here. */
export const CONSOLE_FEATURES: Item[] = [
  { title: "Design by chatting",
    body: "Describe the workflow to the AgentExpress Assistant, attach documents or S3 files, and it drafts the agents, tools and steps. A short brief gets a few questions first. Every change shows on the canvas." },
  { title: "Or build it visually",
    body: "Drag agents onto the canvas, run them in sequence or in parallel, add review gates and branches, and set each agent's model, prompt and framework (plain, Strands or LangGraph)." },
  { title: "Connect agents to tools",
    body: "Web search, knowledge bases from your own documents, MCP servers, OpenAPI and API Gateway APIs, Lambda code tools you write and test in the console, and remote A2A agents." },
  { title: "Built-in AgentCore features",
    body: "Guardrails, memory, skills, custom evaluators, identities (OAuth and API keys), Cedar policies, Gateway interceptors, image generation and image reading." },
  { title: "Start runs automatically",
    body: "Triggers start a run from a webhook, a schedule, an EventBridge event, an S3 upload or an SQS message, and gates can approve by rule or after a timeout." },
  { title: "A shared library",
    body: "Save tools, skills, memory, guardrails, identities, policies, interceptors and evaluators once and reuse them in any build. Share them with people, groups or everyone." },
  { title: "Your organization's registry",
    body: "Publish a build, its tools and its skills to an AWS Agent Registry, and add skills, MCP servers and A2A agents that others published." },
  { title: "Deploy in one click",
    body: "Deploy with AWS CDK or Terraform into this account or any AWS account you connect. Each build gets its own app, API and sign-in: Cognito, Entra ID, Okta or Auth0." },
  { title: "Take it to code",
    body: "Export a build as one file, change it in Kiro or Claude Code, and import it back, or deploy it from your own pipeline." },
  { title: "Share and govern",
    body: "Share a build with colleagues or groups; they see its design, its chat and its app. Every deploy is in its history, and every action in Activity." },
];

/** A build's own app: what the framework does for a run. */
export const APP_FEATURES: Item[] = [
  { title: "HITL gates",
    body: "A stage can pause for you to approve, revise or deny. Revise sends your note back to the agent and re-runs just that agent. A gate can also decide by rule or after a timeout." },
  { title: "Runs that wait",
    body: "A paused run is saved. Close the tab and come back later; it is still waiting at its gate." },
  { title: "Runs that start themselves",
    body: "Where the workflow has triggers, a webhook, a schedule, an event, an S3 upload or a queue message starts a run without anyone pressing Start." },
  { title: "Files with a request",
    body: "Attach documents or name S3 files when you start a run, for the agents that read them." },
  { title: "Re-run any step",
    body: "Re-run a step and everything after it. Earlier versions of each output are kept for comparison." },
  { title: "Why a run ended",
    body: "A run that stops early, by a branch, a deny, a guardrail or a failure, says why at the top, and the Timeline has every step, tool call and decision." },
  { title: "Safe by design",
    body: "Guardrails (which can mask personal data), Cedar policies and group permissions are enforced on the server, not only hidden in the UI." },
  { title: "Cost and quality",
    body: "Observability shows cost, tokens and latency per call, the exact prompts, and evaluation scores." },
  { title: "An assistant that can act",
    body: "The chat bubble answers questions about a run and can take the same actions as the buttons, with the same permissions." },
  { title: "Activity",
    body: "Who started, decided, re-ran or deleted which run, and when." },
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

export function AboutPanel({ workflow, heading, console: isConsole = false, demoUrl }: {
  workflow: Workflow; heading: string; console?: boolean;
  /** The console's demo video (lib/demoVideo.ts). Shown on the console only. */
  demoUrl?: string;
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
      <HelpPanel header={<h2>{`About ${heading}`}<InfoLink feature="about.console" /></h2>}>
        <SpaceBetween size="l">
          {demoUrl ? <Link href={demoUrl} external variant="primary">Watch the 10-minute demo</Link> : null}
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
    <HelpPanel header={<h2>{`About ${heading}`}<InfoLink feature="about" /></h2>}>
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
              { label: "Workflow", value: workflowVersionLabel(workflow.deployment?.version) },
              { label: "Framework", value: FRAMEWORK_VERSION || "—" },
              { label: "Stages", value: String(facts.steps.length) },
              { label: "Agents", value: String(facts.ids.length) },
              { label: "Placement", value: placementText },
              {
                label: "HITL gates",
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
