import { createContext, useContext } from "react";

import type { Me } from "../types";

/** In-product documentation: every feature's page and section in the deployment's
 *  documentation, with the short description its info popover shows.
 *
 *  The base URL is the deployment's `-c docsUrl` / var.docs_url, reaching the page as
 *  GET /api/me `docsUrl`. By default it is the public user guide on GitHub at the release
 *  tag (orchestrator/docs_default.json); a deployment can point it at an XWiki copy of the
 *  same guide instead. With `-c docsLinks=false` / var.docs_links = false, or an empty
 *  docsUrl, the BFF names no docsUrl, and with a person's own "Show help icons" off
 *  (readHelpIcons), nothing documentation-related renders: <InfoLink> and the top bar's
 *  Documentation link are both absent. This file is the only place a page or a section
 *  is named; no documentation URL is written anywhere else.
 *
 *  `page` is the page's slug ("" is the guide's home page). `heading` is the section's
 *  heading text, exactly as the page writes it, and `n` which occurrence of it (0, the
 *  default, for the first) when the page repeats it. docsHref turns them into the
 *  target's link (docsTarget): an XWiki page and its "H…" id, or a GitHub Markdown file
 *  and its slug. docsLinks.test.tsx checks every entry against both sources, and
 *  scripts/check_docs_links.py against the live site. */
export interface DocsEntry {
  page: string;
  heading: string;
  n?: number;
  /** The popover's header: the feature's name as the UI shows it. */
  title: string;
  /** One or two plain sentences, at most 160 characters. */
  description: string;
}

export const DOCS = {
  // --- the Build page ------------------------------------------------------------------
  "builder": { page: "Builder_Console", heading: "Read the build page", title: "Build page",
    description: "Design a workflow, give agents tools and gate the stages a person signs off. Builds save automatically, and deploy from here." },
  "builder.problems": { page: "Builder_Console", heading: "Check problems before you deploy", title: "Problems",
    description: "Every error and warning in this build. Errors block deploy and export; warnings do not." },
  "assistant": { page: "AgentExpress_Assistant", heading: "What the Assistant does", title: "AgentExpress Assistant",
    description: "Say what you want to build. It drafts the workflow, asks what it needs, and updates the build as you talk." },
  "assistant.review": { page: "AgentExpress_Assistant", heading: "Have a second model review changes", title: "Review changes",
    description: "A second model checks each change the Assistant makes before it is saved to the build." },
  "export": { page: "Export_and_Import", heading: "Open the Export menu", title: "Export",
    description: "Download the build as workflow.json only, or as a bundle with its prompts and code." },
  "import": { page: "Export_and_Import", heading: "Import a file", title: "Import a file",
    description: "Open a workflow.json or an exported bundle as a new build, or load it into this build, replacing it." },
  "sharing": { page: "Sharing_a_Build", heading: "Share a build", title: "Share a build",
    description: "Share this build with people, groups or everyone. Whoever you share it with can do everything you can with it." },
  "teams.move": { page: "Teams", heading: "Move a build into a team", title: "Move to team",
    description: "Put this build in a team's workspace, where the team's editors can change and deploy it." },
  "deploy": { page: "Deploying_a_Build", heading: "Read the Deployment panel", title: "Deployment",
    description: "Deploy this build, follow the job, open its app, see the deploy history, or destroy what it deployed." },
  "deploy.dialog": { page: "Deploying_a_Build", heading: "Deploy a build", title: "Deploy",
    description: "Pick CDK or Terraform and where it goes, then deploy the next version. A deploy takes 15 to 30 minutes." },
  "deploy.history": { page: "Deploying_a_Build", heading: "See the deploy history", title: "Deployment history",
    description: "Every deploy and destroy of this build: when, what, the result, the tool, where and by whom, with the end of any error." },
  "assistant.workflow": { page: "AgentExpress_Assistant", heading: "What you see while it works", title: "Workflow",
    description: "The build as the conversation shapes it. It reloads as each change is saved and highlights the agents that changed." },
  "assistant.secrets": { page: "AgentExpress_Assistant", heading: "The cards on the right", title: "Secrets",
    description: "API keys, client secrets or tokens a tool or agent needs. Type them here, never in the chat: each is stored encrypted and never shown again." },
  "assistant.inputs": { page: "AgentExpress_Assistant", heading: "The cards on the right", title: "Needs your input",
    description: "Values only you can give, such as an endpoint, an ARN or a key. Reply in the chat, or fill them in under Build manually. Deploy waits for them." },
  "assistant.defaults": { page: "AgentExpress_Assistant", heading: "The cards on the right", title: "Defaults used",
    description: "What the Assistant chose where you did not say, such as temperature, token limits or tool mode. Ask for something else, or change it by hand." },
  "assistant.versions": { page: "AgentExpress_Assistant", heading: "Go back to a version", title: "Versions",
    description: "Each change the Assistant saved is a named version. Say \"go back to v1\" to return to one; it says which version it restores before it does." },
  "registry": { page: "Agent_Registry", heading: "Add records from the registry", title: "Add from registry",
    description: "Add agents, tools or skills published in an AWS Agent Registry, and optionally keep them in sync." },
  "registry.publish": { page: "Agent_Registry", heading: "Publish to the registry", title: "Publish to registry",
    description: "Publish this deployed build, or its skills, as records in an AWS Agent Registry." },

  // --- the Builder's tabs --------------------------------------------------------------
  "tab.design": { page: "Canvas", heading: "Open the canvas", title: "Design",
    description: "Drag agents onto the canvas and drop tools and features onto them. Select anything to configure it in the panel." },
  "tab.tools": { page: "Tools_and_Gateway", heading: "Read the Tools tab", title: "Tools",
    description: "The build's data sources and APIs. An agent uses the tools it lists; every call goes through the build's Gateway." },
  "tab.skills": { page: "Skills", heading: "Read the Skills tab", title: "Skills",
    description: "Know-how an agent opens when a task needs it. An agent uses the skills it lists in its Skills setting." },
  "tab.memory": { page: "Memory", heading: "Read the Memory tab", title: "Memory",
    description: "Memories agents use to remember across runs: their strategies, retention and who shares them." },
  "tab.guardrails": { page: "Guardrails", heading: "Read the Guardrails tab", title: "Guardrails",
    description: "Checks on each agent's model input and output: the guardrail it names, else the deployment guardrail." },
  "tab.identity": { page: "Identity", heading: "Choose how people sign in", title: "Identity",
    description: "How people sign in to the app, and the credentials agents and tools use to reach APIs." },
  "tab.policies": { page: "Policies_and_Cedar", heading: "Know where Cedar applies", title: "Policies",
    description: "Cedar rules the Gateway checks on every tool call, for every agent. Anything no policy permits is denied." },
  "tab.interceptors": { page: "Interceptors", heading: "Read the Interceptors tab", title: "Interceptors",
    description: "Functions that run before each tool request and after each answer, for every agent." },
  "tab.triggers": { page: "Triggers", heading: "Read the Triggers tab", title: "Triggers",
    description: "What starts a run without anyone typing it: a webhook, a schedule or an EventBridge event." },
  "tab.evals": { page: "Evaluations", heading: "Write your own judge on the Evals tab", title: "Evals",
    description: "Judges that score the output of the agents that use them, on every run or on demand." },
  "tab.tests": { page: "Test_Suites", heading: "Open the Tests tab", title: "Tests",
    description: "Saved requests with fixed checks, run against the deployed build after each change." },
  "tab.settings": { page: "Settings", heading: "Read the Settings tab", title: "Settings",
    description: "The app's text, the runtime settings, who may do what, where runs are exported and the runtime's network." },
  "tab.json": { page: "Workflow_JSON_Tab", heading: "Read the file", title: "workflow.json",
    description: "Exactly what the export writes, in canonical order. Copy it, or check a file you edited." },

  // --- Settings tab sections -----------------------------------------------------------
  "settings.ui": { page: "Settings", heading: "Set the execution plane's text", title: "ui",
    description: "The title, heading and other text people see in this build's app." },
  "settings.orchestrator": { page: "Settings", heading: "Set the runtime settings", title: "orchestrator",
    description: "Engine-wide options for every run of this build, such as the default model and run limits." },
  "settings.authorization": { page: "Settings", heading: "Decide who may do what", title: "authorization",
    description: "Which groups may start runs, decide gates, re-run, deploy and take the other actions." },
  "settings.export": { page: "Settings", heading: "Export runs to another tool", title: "Export runs",
    description: "Send every run of this build to the observability tool your team uses, as it happens." },
  "settings.exportDestination": { page: "Observability_Export", heading: "Pick a destination", title: "Where to send runs",
    description: "Langfuse, Datadog, Grafana Cloud, Honeycomb or any OTLP backend, and the region or host and keys each one needs." },
  "settings.exportConnect": { page: "Observability_Export", heading: "Set it up in the control plane", title: "Connect",
    description: "Fill in only what the tool needs, or use an existing Secrets Manager secret. The keys are saved write-only and never shown again." },
  "settings.exportTest": { page: "Observability_Export", heading: "Set it up in the control plane", title: "Test the connection",
    description: "Sends one small test trace and says what the tool answered, such as accepted, or the key refused." },
  "settings.exportWhat": { page: "Observability_Export", heading: "What is sent", title: "What to send",
    description: "One trace per run, sent when it ends. Traces always go; scores and metrics are on by default, prompts and answers are off." },
  "settings.network": { page: "Runtimes", heading: "Run agents in your VPC", title: "Network",
    description: "Where the shared runtime runs, with every main and task agent: public, or in your VPC." },
  // --- begin Track E storage ---
  "settings.storage": { page: "Settings", heading: "Create storage for the build", title: "Storage",
    description: "A private S3 bucket the deploy creates for this build. Tools reach it as ${storage}, for example ${storage}/templates/." },
  // --- end Track E storage ---
  "guardrails.deployment": { page: "Guardrails", heading: "Set the deployment guardrail", title: "Deployment guardrail",
    description: "Applied to an agent that turns guardrails on without naming one." },

  // --- named items: the tables, their editors and the editors' Advanced sections -------
  "named.table": { page: "Skills", heading: "Use the named-item tables", title: "This build's items",
    description: "What this build defines or uses from the library. Add, edit, publish to the library, share, or make a library item this build's copy." },
  "library.add": { page: "Library", heading: "Move items between a build and the library", title: "Add from library",
    description: "Use library items in this build, live: a change in the library shows in every build that uses it, until you make a copy." },
  "guardrail.define": { page: "Guardrails", heading: "Add a named guardrail", title: "Define a guardrail",
    description: "A named Bedrock guardrail with its own filters, topics and PII rules, for agents whose rules differ from the deployment guardrail." },
  "guardrail.words": { page: "Guardrails", heading: "Set the deployment guardrail", title: "Words",
    description: "Exact custom terms to block, and AWS-managed word lists such as PROFANITY." },
  "guardrail.pii": { page: "Guardrails", heading: "Set the deployment guardrail", title: "Sensitive information",
    description: "PII entity types to block or anonymize, such as email addresses or US social security numbers." },
  "guardrail.messages": { page: "Guardrails", heading: "Set the deployment guardrail", title: "Messages",
    description: "What people see when their input, or the generated content, is blocked." },
  "memory.define": { page: "Memory", heading: "Define a memory", title: "Define a memory",
    description: "A memory's strategies (what is kept from each stored turn), how long events are kept, and whose memories they are." },
  "memory.retention": { page: "Memory", heading: "Define a memory", title: "Retention",
    description: "Days the stored events are kept, 3 to 365. Extracted long-term records are not affected." },
  "memory.sharing": { page: "Memory", heading: "Define a memory", title: "Sharing",
    description: "Whose memories they are: each user's own (the default), shared per subject, everyone who runs the agent, or one run only." },
  "memory.about": { page: "Memory", heading: "Define a memory", title: "About this memory",
    description: "What the memory is for, for whoever picks it." },
  "evaluator.define": { page: "Evaluations", heading: "Write your own judge on the Evals tab", title: "Write a judge",
    description: "Your own judge: instructions that may use {context} and {assistant_turn}, an optional judge model and a rating scale." },
  "evaluator.judge": { page: "Evaluations", heading: "Write your own judge on the Evals tab", title: "Judge",
    description: "The judge model. Leave it empty for the workflow's default model. Judge models must be Bedrock models." },
  "evaluator.scoring": { page: "Evaluations", heading: "Write your own judge on the Evals tab", title: "Scoring",
    description: "The rating scale: 2 to 20 points, each with a value, a label and a definition. The default is five points from 0 to 1." },
  "evaluator.about": { page: "Evaluations", heading: "Write your own judge on the Evals tab", title: "About this evaluator",
    description: "What the judge scores, for whoever picks it." },
  "skill.define": { page: "Skills", heading: "Add a skill", title: "Add a skill",
    description: "A skill's name, when to use it, its instructions and optional reference files. Or load a SKILL.md file." },

  // --- Identity tab --------------------------------------------------------------------
  "identity.cognito": { page: "Identity", heading: "Built-in sign-in", title: "Built-in sign-in",
    description: "Amazon Cognito: the deploy creates a user pool for the app. Nothing to set up." },
  "identity.provider": { page: "Identity", heading: "Your own provider", title: "Your own provider",
    description: "Sign people in with your Okta, Entra ID or Auth0 app instead of the built-in sign-in." },
  "identity.toolAccess": { page: "Identity", heading: "Review how each tool connects", title: "Tool access",
    description: "How each tool connects to its service. Set on each tool; the agent never sees a key or a secret." },
  "identity.savedLogins": { page: "Identity", heading: "Save a login several tools share", title: "Saved logins",
    description: "An API key or OAuth client several tools can share. Rotate it here and every tool using it follows." },
  "identity.oauth": { page: "Identity", heading: "Save a login several tools share", title: "OAuth",
    description: "The scopes to request, and the issuer when it differs from the token URL's origin." },
  "identity.about": { page: "Identity", heading: "Save a login several tools share", title: "About this identity",
    description: "What the identity signs in to, for whoever picks it." },
  "identity.secrets": { page: "Identity", heading: "Enter secrets", title: "Secrets",
    description: "Each identity's API key or OAuth client secret: stored encrypted with this build, vaulted in AgentCore Identity at deploy, never shown again." },

  // --- Policies, Interceptors, Triggers, Tests -----------------------------------------
  "policies.engine": { page: "Policies_and_Cedar", heading: "Turn the engine on and pick a mode", title: "Policy engine",
    description: "Turn the Gateway's Cedar engine on, and enforce decisions or only log them while you roll a policy out." },
  "policies.write": { page: "Policies_and_Cedar", heading: "Write a policy", title: "Write a policy",
    description: "Say what to allow or block in plain words, pick it on a form, or write Cedar." },
  "policies.generated": { page: "Policies_and_Cedar", heading: "Manage generated permits and build-wide policies", title: "Generated permits",
    description: "Declaring a tool permits it. Turn one off to allow only what your own policies permit for it." },
  "policies.buildWide": { page: "Policies_and_Cedar", heading: "Manage generated permits and build-wide policies", title: "Build-wide policies",
    description: "Policies written before policies were attached to tools. Each deploys as written." },
  "interceptors.point": { page: "Interceptors", heading: "Set up an interceptor", title: "Interceptor",
    description: "Your code on every tool call: before the request goes out, or after the answer comes back." },
  "interceptors.function": { page: "Interceptors", heading: "Set up an interceptor", title: "The function",
    description: "Write the interceptor here, or use a Lambda function of yours by its ARN. The Gateway's role is allowed to invoke it." },
  "interceptors.templates": { page: "Interceptors", heading: "Pick the templates", title: "What it does",
    description: "Templates such as an audit log, blocked tools or redacted personal data. Each one you check becomes a section of handler.py." },
  "interceptors.code": { page: "Interceptors", heading: "Set up an interceptor", title: "The code",
    description: "Read or edit the handler.py the templates generate, or take the code from an upload, Git or S3." },
  "interceptors.reach": { page: "Interceptors", heading: "Set up an interceptor", title: "What it may reach",
    description: "Grant only what the function needs: a secret, a DynamoDB table, an S3 prefix or a private network. With none, it has no AWS access." },
  "interceptors.check": { page: "Interceptors", heading: "Set up an interceptor", title: "Check and run",
    description: "A syntax check, lint and a security scan, then each test event, run in an AgentCore Code Interpreter sandbox." },
  "interceptors.test": { page: "Interceptors", heading: "Set up an interceptor", title: "Test the deployed interceptor",
    description: "After a deploy, send the deployed interceptor a test event and see what it does with it." },
  "interceptors.share": { page: "Interceptors", heading: "Share an interceptor", title: "Interceptors in the library",
    description: "Publish an interceptor to the library for other builds. A build that adds one takes a copy, so a library change never changes it." },
  "triggers.add": { page: "Triggers", heading: "Add a trigger", title: "Add a trigger",
    description: "Name a trigger and pick its type, then write the request it starts a run with." },
  "tests.suites": { page: "Test_Suites", heading: "Write a suite", title: "Test suites",
    description: "Requests with checks on what each run should produce. Run a suite after a change and compare it with the last run." },

  // --- the agent panel -----------------------------------------------------------------
  "agent": { page: "Agents", heading: "Open the settings panel", title: "Agent",
    description: "Everything about one agent: its stage, prompt, model, tools and the AgentCore features it uses." },
  "agent.attached": { page: "Agents", heading: "Read an agent card", title: "Attached to this agent",
    description: "Everything this agent uses: tools, guardrails, memory, policies, skills, evals and identities. Click one to see and change its settings." },
  "agent.prompt": { page: "Agents", heading: "Write the prompt", title: "Prompt",
    description: "The agent's system prompt, and the JSON its model must return." },
  "agent.instructions": { page: "Runtimes", heading: "AgentCore Harness", title: "Instructions",
    description: "The system prompt this agent's AgentCore Harness is created with. A change takes effect when you deploy." },
  // The agent form's Advanced sections (formLayout.ts).
  "agent.tuning": { page: "Models", heading: "Pick a model", title: "Model tuning",
    description: "The output budget per call (maxTokens), temperature, top P and stop sequences. Raise maxTokens first when the output is cut mid-JSON." },
  "agent.toolUse": { page: "Adding_Tools", heading: "Pick how the agent uses tools", title: "Tool use",
    description: "Who picks the tool calls: every tool once before the model reasons (direct), or the model itself, up to maxToolCalls (model)." },
  "agent.images": { page: "Models", heading: "Draw and read images", title: "Images",
    description: "Make this an image agent (output: image) and set how it renders, or let it read images earlier agents made in this run (vision)." },
  "agent.code": { page: "Runtimes", heading: "Frameworks", title: "Code",
    description: "How the agent's reasoning step is written: no framework, Strands Agents or LangGraph. All three call the model through ctx.llm." },
  "agent.labels": { page: "Agents", heading: "Label long-running agents and data sources", title: "Labels",
    description: "kind marks a long-running step with a chip on its card, and produces names this agent's output, such as claim-decision." },
  "agent.other": { page: "Workflow_JSON_Reference", heading: "Describe agents", title: "Other",
    description: "The agent's other settings, such as skills, model routing, reading the run's files and asking a person. Each field says what it does." },
  "agent.model": { page: "Models", heading: "Pick a model", title: "Model",
    description: "The model this agent calls. The list shows the models this account can use that fit the agent's settings." },
  "agent.routing": { page: "Models", heading: "Route calls by rule", title: "Model routing",
    description: "Send a call to another model by rule, and try fallback models when the model is throttled or unavailable." },
  "agent.runtime": { page: "Runtimes", heading: "Compare the runtimes", title: "Runtime",
    description: "Where the agent runs: the shared runtime, a runtime of its own, a managed harness, or a remote A2A agent." },
  "agent.tools": { page: "Adding_Tools", heading: "Give an agent a tool", title: "Tools",
    description: "The tools this agent uses. Every call goes through the build's Gateway." },
  "agent.skills": { page: "Skills", heading: "How an agent uses a skill", title: "Skills",
    description: "Know-how from the Skills tab or the library that this agent opens when a task needs it." },
  "agent.features": { page: "Agents", heading: "Turn on AgentCore features", title: "AgentCore features",
    description: "Turn on guardrails, long-term memory, evaluations or outbound identity for this agent." },
  "agent.guardrails": { page: "Guardrails", heading: "Turn a guardrail on for an agent", title: "Guardrail",
    description: "Check this agent's model input, output or both with a guardrail from the Guardrails tab." },
  "agent.memory": { page: "Memory", heading: "Give an agent memory", title: "Long-term memory",
    description: "Let this agent remember across runs with a memory defined in the Memory tab." },
  "agent.evaluations": { page: "Evaluations", heading: "Turn evaluations on for an agent", title: "Evaluations",
    description: "Score this agent's output with built-in or custom judges, on every run or on demand." },
  "agent.identity": { page: "Identity", heading: "Let an agent call an API itself", title: "Outbound identity",
    description: "Let this agent call an API directly, with credentials defined in the Identity tab." },
  "agent.promptCache": { page: "Prompt_Caching", heading: "Turn it on for an agent", title: "Prompt caching",
    description: "Cache points for this agent's model calls, so a repeated prefix is billed at the cache-read rate. Off unless set." },
  "agent.network": { page: "Runtimes", heading: "Run agents in your VPC", title: "Network",
    description: "Where this agent's own AgentCore Runtime runs: public (the default) or your VPC." },
  "agent.harness": { page: "Runtimes", heading: "AgentCore Harness", title: "AgentCore Harness",
    description: "An agent with no code of yours, run by a managed AgentCore Harness with the instructions, model and tools set here." },
  "stage": { page: "Stages_and_Steps", heading: "Change stage settings", title: "Stage",
    description: "How the stage's agents run (one, side by side or in sequence), its review gate and its branch." },
  "stage.gate": { page: "Human_in_the_Loop", heading: "Add a review gate", title: "HITL gate",
    description: "Pause after this stage for a person to approve, revise or deny." },
  "stage.branch": { page: "Human_in_the_Loop", heading: "Route with branch rules", title: "Branch",
    description: "Route on this stage's output: continue, jump to a later stage, or end the run." },
  // --- begin v139 Track D (decide) ---
  "stage.gateOptions": { page: "Human_in_the_Loop", heading: "Offer named options at a gate", title: "Named options",
    description: "Let the person choose one of your options, such as Override or Abort, and route on the choice as the field decision." },
  "agent.askPerson": { page: "Human_in_the_Loop", heading: "Let an agent request a decision", title: "Ask a person",
    description: "Let this agent ask a person a question, or ask them to choose one of your options, in the middle of its step." },
  // --- end v139 Track D (decide) ---
  "task": { page: "Stages_and_Steps", heading: "Add a generic step", title: "Generic step",
    description: "A step with no model that calls a tool, runs code, extracts fields or shapes the output." },
  // v139 Track C deliver
  "task.deliver": { page: "Stages_and_Steps", heading: "Deliver a run's files", title: "Deliver files",
    description: "Send the files a run saved to S3, by email with Amazon SES, or to a webhook, only when a condition holds if you set one." },
  "component": { page: "Agents", heading: "Work with rows", title: "Agent card row",
    description: "Something attached to an agent. Configure it here, move it to another agent, or remove it." },

  // --- tools ---------------------------------------------------------------------------
  "tool": { page: "Tool_Types", heading: "Know the keys every tool shares", title: "Tool",
    description: "A data source or API that agents call through the build's Gateway." },
  "tool.add": { page: "Adding_Tools", heading: "Add a tool", title: "Add a tool",
    description: "Name the tool and pick its type, then set how it connects in its panel." },
  "tool.connects": { page: "Adding_Tools", heading: "Choose how a tool connects", title: "How it connects",
    description: "How the Gateway signs in to this tool's service: an API key, OAuth, an IAM role, the signed-in person, or nothing." },
  "connectors": { page: "Adding_Tools", heading: "Add a connector", title: "Connectors",
    description: "Ready-made tools for major services such as Salesforce, Slack and Jira. Pick one, then set its client secret." },
  "tool.kb": { page: "Tool_Types", heading: "Knowledge base and web search", title: "Knowledge Base",
    description: "Retrieves passages from documents you upload, an S3 bucket, a connector, or a Knowledge Base of yours." },
  "tool.websearch": { page: "Tool_Types", heading: "Knowledge base and web search", title: "Web search",
    description: "Searches the web and gives the agent the results to cite." },
  "tool.mcp": { page: "Tool_Types", heading: "MCP and OpenAPI", title: "MCP server",
    description: "Calls the tools an MCP server lists, at a URL or hosted by this build." },
  "tool.hostedMcp": { page: "Tool_Types", heading: "Host an MCP server here", title: "Hosted MCP server",
    description: "Run your MCP server's code in this build instead of pointing at a URL." },
  "tool.openapi": { page: "Tool_Types", heading: "MCP and OpenAPI", title: "REST API (OpenAPI)",
    description: "Calls a REST API described by an OpenAPI 3 document." },
  "tool.lambda": { page: "Tool_Types", heading: "Lambda", title: "Lambda function",
    description: "Calls a Lambda function: one you name, or code you write here." },
  "tool.code": { page: "Tools_and_Gateway", heading: "Write a code tool", title: "Code tool",
    description: "Write the tool's code here, say what it may reach, then check it and run it." },
  "tool.codeSource": { page: "Tool_Types", heading: "Choose where code comes from", title: "Code source",
    description: "Where code comes from: written or uploaded here and kept in the build, or fetched from Git or S3 when the build deploys." },
  "tool.apigateway": { page: "Tool_Types", heading: "API Gateway and Smithy", title: "REST API (API Gateway)",
    description: "Calls a REST API you run on Amazon API Gateway." },
  "tool.smithy": { page: "Tool_Types", heading: "API Gateway and Smithy", title: "AWS API (Smithy)",
    description: "Calls AWS service operations from a Smithy model, limited to the operations you allow." },
  "tool.codeInterpreter": { page: "Tool_Types", heading: "Code Interpreter and Browser", title: "Code Interpreter",
    description: "Runs a short program in an AWS sandbox and returns what it printed." },
  "tool.browser": { page: "Tool_Types", heading: "Code Interpreter and Browser", title: "Browser",
    description: "Opens a web page in an AWS browser and returns its text and links." },
  // Track B docgen
  "tool.docgen": { page: "Tool_Types", heading: "Document generator", title: "Document generator",
    description: "Writes a PDF, PPTX or DOCX from data, into a template from S3 or a simple layout, and saves it with the run." },
  // The tool form's Advanced sections (formLayout.ts), and the tool panel's other sections.
  "tool.calling": { page: "Tool_Types", heading: "Know the keys every tool shares", title: "Calling",
    description: "Which operation to invoke (call), the parameter the agent's query goes into (arg), and fixed arguments the agent cannot change (args)." },
  "tool.search": { page: "Tool_Types", heading: "Knowledge base and web search", title: "Search and filters",
    description: "How many results come back, the domains and dates web search may use, and the corpus match and filter a knowledge base applies." },
  "tool.kbProcessing": { page: "Tool_Types", heading: "Knowledge base and web search", title: "Document processing",
    description: "How documents are read (parsing) and split into chunks (chunking) before they are embedded. Leave both out for Bedrock's defaults." },
  "tool.kbDataSource": { page: "Tool_Types", heading: "Knowledge base and web search", title: "Where documents come from",
    description: "Take the documents from a web crawl, Confluence, SharePoint or Salesforce instead of S3. Needs an OpenSearch Serverless vector store." },
  "tool.kbVectorStore": { page: "Tool_Types", heading: "Knowledge base and web search", title: "Vector store",
    description: "Use a vector store you already have instead of the S3 Vectors index the framework creates." },
  "tool.kbDocuments": { page: "Tool_Types", heading: "Knowledge base and web search", title: "Documents",
    description: "Upload files for each corpus, or point the tool at your own S3 bucket or an existing Knowledge Base." },
  "tool.policy": { page: "Policies_and_Cedar", heading: "Manage generated permits and build-wide policies", title: "Policy",
    description: "Narrow the tool's generated Cedar permit to one operation or to allowed arguments, or attach policies from the Policies tab." },
  "tool.other": { page: "Human_in_the_Loop", heading: "When a tool call waits for approval", title: "Other",
    description: "The tool's other settings, such as approval: every call waits for a person to approve it, for up to approvalTimeout." },
  "tool.lambdaFunction": { page: "Tool_Types", heading: "Lambda", title: "The function",
    description: "Call a function you deployed, by its ARN, or write the code here: the framework deploys it with only the grants you give." },
  "tool.codeCheck": { page: "Tools_and_Gateway", heading: "Write a code tool", title: "Check and run",
    description: "A syntax check, lint and a security scan, then the test events, run in an AgentCore Code Interpreter sandbox." },
  "tool.codeTest": { page: "Tools_and_Gateway", heading: "Write a code tool", title: "Test the deployed tool",
    description: "After a deploy, call the deployed function with an event and see its log." },
  "tool.openapiUpload": { page: "Tool_Types", heading: "MCP and OpenAPI", title: "Upload an OpenAPI document",
    description: "A local OpenAPI 3 JSON file becomes this tool's inline schema. The schema is the allow list: describe only the operations agents need." },
  "gateway": { page: "Tools_and_Gateway", heading: "What the Gateway does", title: "Gateway",
    description: "Agents never call a tool directly: every call goes through the build's Gateway, which checks it against the Cedar policies." },

  // --- the control plane's other pages -------------------------------------------------
  "library": { page: "Library", heading: "Find the library pages", title: "Library",
    description: "Tools, skills, guardrails and more you keep to reuse. A build uses an item live, so a change here shows in every build." },
  "accounts": { page: "AWS_Accounts", heading: "Connect an account", title: "AWS accounts",
    description: "AWS accounts of yours that builds can deploy into, so their data, cost and resources stay yours." },
  "accounts.manage": { page: "AWS_Accounts", heading: "Manage a connected account", title: "Manage an account",
    description: "Rename a connected account or change its default region, check the connection, update its role, or disconnect it." },
  "teams": { page: "Teams", heading: "Open the Teams page", title: "Teams",
    description: "A team shares a workspace of builds. Editors change and deploy them; viewers only look." },
  "teams.edit": { page: "Teams", heading: "Create a team", title: "Team",
    description: "A team's name, its people with their roles, and the sign-in groups whose members get a role. Add at least one owner." },
  "admin": { page: "Users_Groups_and_RBAC", heading: "The Admin page", title: "Admin",
    description: "Every user's builds and runs, read-only, and Destroy to remove a build's resources from AWS." },
  "admin.runs": { page: "Users_Groups_and_RBAC", heading: "The Admin page", title: "Console runs",
    description: "Runs of this console's own workflow. A build's runs are under the build." },
  "about.console": { page: "", heading: "What is AgentExpress", title: "About AgentExpress",
    description: "AgentExpress turns a description of a business process into a working multi-agent application on AgentCore, with no code to write." },
  "admin.groups": { page: "Users_Groups_and_RBAC", heading: "How groups grant actions", title: "Groups",
    description: "Anyone can share a build or a library item with a group; only an admin changes who is in it." },
  "activity": { page: "Activity", heading: "Find an entry", title: "Activity",
    description: "Who signed in, and who started, decided, cancelled, re-ran or deleted what, and when." },

  // --- the execution plane -------------------------------------------------------------
  "runs": { page: "Execution_Plane", heading: "The Runs home page", title: "Runs",
    description: "Every run you can see, newest first. Open one to follow it, or start a new run." },
  "startRun": { page: "Starting_a_Run", heading: "Start a run with a prompt", title: "Start a run",
    description: "Type the request, attach files if the workflow takes them, and start the run." },
  "startRun.advanced": { page: "Starting_a_Run", heading: "Use the Advanced options", title: "Advanced",
    description: "Options for this run only: a subject that scopes long-term memory, and the models its agents use when you may pick them." },
  "limits": { page: "Cost_and_Limits", heading: "Set spend and run limits", title: "Limits",
    description: "Spend and run limits per person, and how much of them you have used." },
  "about": { page: "Execution_Plane", heading: "Read the About panel", title: "About this application",
    description: "What this app's workflow does, this deployment's version, stages, agents and data sources, and how to get started." },
  "run": { page: "Run_Page", heading: "Read the run details", title: "Run",
    description: "One run: its status, progress and who started it, with a view of every step." },
  "run.details": { page: "Run_Page", heading: "Read the run details", title: "Details",
    description: "The run's status and progress, when it started and how long it ran, its workflow version, who started it, its files and what it awaits." },
  "run.graph": { page: "Run_Page", heading: "Graph view", title: "Graph view",
    description: "The workflow as a graph with each step's status. Select a step to see its output and history." },
  "run.table": { page: "Run_Page", heading: "Table view", title: "Table view",
    description: "Every step in a table: its status, placement, data source and output size." },
  "run.timeline": { page: "Run_Page", heading: "Timeline", title: "Timeline",
    description: "When each step started and finished, as a chart or as the event log." },
  "run.flow": { page: "Run_Page", heading: "Flow", title: "Flow",
    description: "What each agent did with the Gateway, memory, policy and guardrails, in order." },
  "run.outputs": { page: "Run_Page", heading: "Outputs", title: "Outputs",
    description: "The output each step produced." },
  "run.files": { page: "Run_Page", heading: "Files", title: "Files",
    description: "The files this run brought or made, to preview and download." },
  "run.step": { page: "Run_Page", heading: "The step panel", title: "Step",
    description: "One step's output and its versions, and re-running it with a comment." },
  "run.gate": { page: "Human_in_the_Loop", heading: "Approve, revise or deny", title: "Review gate",
    description: "The run waits for a person: approve to continue, revise with a comment, or deny to stop it." },
  "run.ask": { page: "Human_in_the_Loop", heading: "When an agent asks a person", title: "Question",
    description: "An agent asked a person a question. The run continues when it is answered." },
  // v139 Track D (decide)
  "run.decision": { page: "Human_in_the_Loop", heading: "Choose an option", title: "Decision",
    description: "The run waits for a person to choose one of the options. The choice is recorded and decides what runs next." },
  "run.share": { page: "Sharing_a_Run", heading: "Create a link", title: "Share a run",
    description: "Create a read-only link to this run for a colleague, and revoke it when it is no longer needed." },
  "run.shareLinks": { page: "Sharing_a_Run", heading: "Manage and revoke links", title: "Active links",
    description: "This run's links, with when each was created and expires. Revoke ends a link at once; a run has at most 10 active links." },
  "run.export": { page: "Observability_Export", heading: "Re-send and pause in the execution plane", title: "Run export",
    description: "Where this app's runs are sent as they end. Re-send a run, or pause sending." },
  "runAssistant": { page: "Run_Assistant", heading: "Open the Run Assistant", title: "Run Assistant",
    description: "Ask about runs in plain words, or ask it to act, within what you are allowed to do." },
  "observability": { page: "Observability", heading: "Open the Observability page", title: "Observability",
    description: "Cost, latency, tokens and quality for every run, recorded with nothing to switch on." },
  "observability.overview": { page: "Observability", heading: "Overview tab", title: "Overview",
    description: "Totals by date, model and user over a date range, as a chart and a table." },
  "observability.runDetail": { page: "Run_Traces", heading: "Load a run", title: "Run detail",
    description: "One run's cost, latency and tokens per agent, with its prompts and I/O." },
  "observability.insights": { page: "Observability", heading: "Insights tab", title: "Insights",
    description: "Cross-run analysis of failure patterns, and what prompt caching saves per agent." },
  "evaluations": { page: "Evaluations", heading: "Read the scores", title: "Evaluations",
    description: "The scores judges gave each agent's output, and scoring an agent on demand." },
  "cost": { page: "Cost_and_Limits", heading: "Read the cost estimates", title: "Cost",
    description: "How run costs are estimated from recorded usage and prices, and what costs extra." },
  "analytics": { page: "Analytics", heading: "Open the Analytics page", title: "Analytics",
    description: "Every user's errors, denials and latency for each agent and tool, with a health score." },
  "analytics.warnings": { page: "Analytics", heading: "Read the configuration warnings", title: "Configuration warnings",
    description: "From the deployed workflow.json, not from telemetry: agents no guardrail screens, and tools no Cedar policy decides." },
  "analytics.agents": { page: "Analytics", heading: "Read the Agents and Tools tables", title: "Agents",
    description: "Each agent's health score and why it is low, its calls, error rate, guardrail blocks, Cedar denials and model latency." },
  "analytics.tools": { page: "Analytics", heading: "Read the Agents and Tools tables", title: "Tools",
    description: "Each tool's calls, error rate, latency, Cedar denial rate, and how many calls were allowed, denied or only logged." },
  "memoryRecords": { page: "Memory_Records", heading: "Open the Memory records page", title: "Memory records",
    description: "What agents remembered across runs, by agent and namespace. Search records or delete one." },
  "memoryRecords.namespace": { page: "Memory_Records", heading: "Browse an agent's records", title: "Namespace records",
    description: "The records in one namespace: the text, when it was created and the strategy that wrote it. Load more reads the next page." },
  "memoryRecords.delete": { page: "Memory_Records", heading: "Delete a record", title: "Delete memory record",
    description: "Deletes the record named here, for good. Every delete is in the activity log." },
  "files": { page: "Files_Page", heading: "Open the Files page", title: "Files",
    description: "Every file your runs brought or made, to preview and download." },
  "liveConfig": { page: "Live_Config", heading: "Open the Live config page", title: "Live config",
    description: "Change an agent's prompt and model settings without a redeploy, as versions you activate or roll back." },
  "liveConfig.active": { page: "Live_Config", heading: "What a run does with it", title: "Active",
    description: "The version new runs use. A run keeps the version that was active when it started, for every step." },
  "liveConfig.versions": { page: "Live_Config", heading: "Roll back", title: "Versions",
    description: "Every saved version. Activate an older one to roll back, or use the deployed config to go back to version 0." },
  "liveConfig.new": { page: "Live_Config", heading: "Make and activate a version", title: "New version",
    description: "Change agents' prompts and model settings and save them as a version. Nothing changes for runs until you activate it." },
  "triggersPage": { page: "Triggers", heading: "Manage triggers in the execution plane", title: "Triggers",
    description: "This app's trigger URLs, secrets and recent deliveries, and a test delivery." },
} as const satisfies Record<string, DocsEntry>;

export type DocsFeature = keyof typeof DOCS;

/** The base URL from GET /api/me, with a trailing slash, or undefined when none is set,
 *  it is not an https URL, or the deployment turned documentation links off
 *  (`docsLinks: false`). The IaC already refuses anything else; this keeps a bad value
 *  from ever becoming a link (as lib/demoVideo.ts does). */
export function docsBase(me: Pick<Me, "docsUrl" | "docsLinks">): string | undefined {
  if (me.docsLinks === false || !me.docsUrl) return undefined;
  try {
    if (new URL(me.docsUrl).protocol !== "https:") return undefined;
  } catch {
    return undefined;
  }
  return me.docsUrl.endsWith("/") ? me.docsUrl : `${me.docsUrl}/`;
}

/** Where the documentation lives, which decides the link's shape: "wiki" for an XWiki
 *  site (its pages are under /bin/view/), "github" for anything else (the public user
 *  guide's Markdown files, on github.com or a copy of them). */
export type DocsTarget = "wiki" | "github";

export function docsTarget(base: string): DocsTarget {
  try {
    return new URL(base).pathname.includes("/bin/view/") ? "wiki" : "github";
  } catch {
    return "github";
  }
}

/** XWiki's id for a heading: "H" plus its text with spaces removed, letters, digits and
 *  - . _ : kept, any other character as its hex code ("," is "2C", "'" is "27"). A repeat
 *  of the same id on the page gets -1, -2. */
export function xwikiAnchor(heading: string, n = 0): string {
  let id = "H";
  for (const c of heading) {
    if (c === " ") continue;
    id += /^[A-Za-z0-9\-._:]$/.test(c) ? c : c.codePointAt(0)!.toString(16).toUpperCase();
  }
  return n ? `${id}-${n}` : id;
}

/** GitHub's id for a Markdown heading (github-slugger): lower case, every character but
 *  letters, digits, marks, "-", "_" and spaces removed, each space a "-". A repeat of the
 *  same slug on the page gets -1, -2. `heading` is the text as rendered (no Markdown). */
export function githubSlug(heading: string, n = 0): string {
  const slug = heading.toLowerCase().replace(/[^\p{L}\p{M}\p{N}\p{Pc}\- ]/gu, "").replace(/ /g, "-");
  return n ? `${slug}-${n}` : slug;
}

/** The feature's section of the documentation: `${base}${page}#H…` on a wiki, where the
 *  home page ("") is the base URL itself; `${base}${page}.md#slug` on GitHub, where it is
 *  Home.md (README.md there is the guide's table of contents). */
export function docsHref(base: string, feature: DocsFeature): string {
  const { page, heading, n }: DocsEntry = DOCS[feature];
  if (docsTarget(base) === "wiki") return `${base}${page}#${xwikiAnchor(heading, n)}`;
  return `${base}${page || "Home"}.md#${githubSlug(heading, n)}`;
}

/** A person's own choice to show the info icons and the Documentation link ("Show help
 *  icons" in the top bar's menu), kept in this browser. On unless they turned it off. */
const HELP_ICONS_KEY = "agentexpress.docs.helpIcons";

export function readHelpIcons(): boolean {
  try { return localStorage.getItem(HELP_ICONS_KEY) !== "off"; } catch { return true; }
}

export function writeHelpIcons(on: boolean): void {
  try { localStorage.setItem(HELP_ICONS_KEY, on ? "on" : "off"); } catch { /* private mode: not remembered */ }
}

/** The documentation base URL, or "" (none: render nothing). App.tsx provides it. */
export const DocsContext = createContext("");

export function useDocsBase(): string {
  return useContext(DocsContext);
}
