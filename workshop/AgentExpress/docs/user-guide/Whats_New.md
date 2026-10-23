# What's new

Coming back to AgentExpress after a while? Here's what you can now do, newest first, each with a link to its page. The sections after **Newest** are the features of release `agentexpress-v1.3.2`.

# Newest

## Ask the Assistant, on every feature page

- A new page, [Ask the Assistant](Ask_the_Assistant.md), lists what you can ask the AgentExpress Assistant to set for each feature, with requests you can paste.
- Each feature page now has an **Ask the Assistant** section: what it can set there, and where to go for what it can't.

## The Assistant reads pages you link

- Paste a public page's address, for example "Build this based on &lt;the page's address>", and the Assistant reads it before it changes anything.
- Up to 5 pages per message, about 10 seconds each. Pages behind a sign-in, private addresses and PDFs are not read: attach the file instead.
- See [Build with the AgentExpress Assistant](AgentExpress_Assistant.md).

## Review changes

- A **Review changes** setting in the Assistant panel: **Off**, **Auto** (default) or **Always**. A second model checks the Assistant's change before it's saved.
- **Auto** reviews new drafts, added or removed parts, code and security-relevant changes, and changes of more than 8 items. It only advises, and never blocks a change.
- See [Build with the AgentExpress Assistant](AgentExpress_Assistant.md).

## Prompt caching

- Per agent, cache the system prompt, the tool definitions or the conversation so far, so repeated calls cost less and return sooner.
- Works on Claude, Amazon Nova and OpenAI models, within each model's rules: a 5 minute or 1 hour cache, or 30 minutes on OpenAI GPT-5.6 and later.
- See [Prompt caching](Prompt_Caching.md).

## Cache insights

- **Overview** and **Run detail** on the Observability page show a **Cache saving** card: tokens read and written, cache cost and the net saving.
- A **Prompt caching** card on **Insights** gives each caching agent one finding, such as **Caching is paying off** or **Cache written but rarely read**, with what to do.
- See [Observability](Observability.md).

## Hosted MCP servers

- Have MCP server code but nowhere to run it? Pick **Host it here** on an MCP server tool. It runs on its own AgentCore Runtime and is added to the build's Gateway.
- The code can be written in the build, uploaded, taken from Git or S3, or a private Amazon ECR image.
- See [Tool types in depth](Tool_Types.md).

## Code from Git or S3

- A Lambda tool, an interceptor, a **Run code** step and a hosted MCP server can each take code you already keep: **Written here**, **Upload**, **Git repository** or **Amazon S3**.
- Code in Git or S3 is fetched at each deploy.
- See [Tool types in depth](Tool_Types.md).

## Runtimes in your VPC

- Run the shared orchestrator runtime (main agents and task steps), a dedicated agent, or a hosted MCP server in your VPC. Each is **Public** by default.
- A dedicated agent has its own **Network** setting, not taken from the orchestrator's.
- See [Runtimes](Runtimes.md).

## More reliable destroy

- A destroy started from the control plane tries again by itself, up to 3 more times, when an AgentCore Runtime's delete times out.
- It reads every resource of the build's stack, however many there are. On CDK it also empties a UI logs bucket that filled up again.
- See [Clean up](Clean_Up.md).

## Questions and feedback on GitHub

- Ask questions and share feedback in [GitHub Issues](https://github.com/aws-samples/sample-aiml-solution-labs/issues). The link is at the top of the [index of this guide](README.md).

# Run views

## Flow tab: Story and Summary

A run's **Flow** tab tells what each agent did with the Gateway, memory, Cedar policy and guardrails, one line per event. **Story** tells the run stage by stage, with filters such as **Problems only** and **Cedar policy**. **Summary** shows one card per agent and counts the run's events by kind. A replay bar plays the run back event by event. See [The run page and its tabs](Run_Page.md).

## Timeline: Chart and Log

A run's **Timeline** tab has a **Chart** view, a Gantt chart with each step's model, tool, guardrail, memory, policy and evaluation calls inside its bar, the critical path and the waits at gates. The **Log** view lists every timeline line, newest first. See [The run page and its tabs](Run_Page.md).

# Observability export

Every run can be sent, when it ends, to Langfuse, Datadog, Grafana Cloud, Honeycomb or any OTLP backend. Pick the tool on **Settings → Export** in the control plane, paste the key, and click **Test connection**. In the execution plane, a run links to its trace, can be re-sent, and admins can pause sending. See [Export to Langfuse, Datadog and others](Observability_Export.md).

# Building

## Connectors

**Tools → Add a connector** opens a gallery of ready-made OAuth tools for services such as Salesforce, Slack, Jira, GitHub and Microsoft 365. Each one adds an ordinary OpenAPI tool you can edit. See [Add tools and connectors](Adding_Tools.md).

## Generic task steps

**+ New generic step** adds a step with no model: it calls one tool, runs a script in Code Interpreter, reads a document with Textract, or writes a file. You pick the kind in its panel. See [Stages and steps](Stages_and_Steps.md).

## Custom Cedar policy rows on agent cards

A custom Cedar policy shows as a **Policy** row on every agent that calls a tool it names, on the canvas in the control plane and on the run graph in the execution plane. You change it on the **Policies** tab, not on one agent. See [Policies (Cedar)](Policies_and_Cedar.md).

# Agents and models

## AgentCore Harness, including your own

`runtime: "harness"` runs an agent with no code on AgentCore Harness: Gateway tools, the managed Code Interpreter and Browser, a named endpoint, other model providers, skills, memory, MCP servers and more. You can also bring your own harness by ARN. See [Runtimes](Runtimes.md).

## Code Interpreter and Browser tools

A **Code Interpreter** tool lets an agent run code it wrote in the managed sandbox. A **Browser** tool opens a public page and reads its text and links. See [Tool types in depth](Tool_Types.md).

## Per-run model override and routing

Per-agent routing rules pick a model by input size or by a pattern in the request, with up to 3 fallbacks. People granted the `override` action can pick a run's models on **Start run**. See [Models, routing and LiteLLM](Models.md).

## LiteLLM

A model named `litellm/<model>` is called through your own LiteLLM proxy, for models outside Bedrock. See [Models, routing and LiteLLM](Models.md).

# Working together

## Teams and workspaces

Team workspaces share builds with owner, editor and viewer roles. A switcher above the build list moves between **My builds** and your teams, and **Project → Move to team…** moves a build. See [Teams and workspaces](Teams.md).

# Related pages

- [Ask the Assistant](Ask_the_Assistant.md): what to ask the AgentExpress Assistant, feature by feature.
- [Prompt caching](Prompt_Caching.md): cache what agents send on every call.
- [The run page and its tabs](Run_Page.md): Flow and Timeline in detail.
- [Export to Langfuse, Datadog and others](Observability_Export.md): set up the export.
- [Runtimes](Runtimes.md): harness agents and the other runtimes.
- [Teams and workspaces](Teams.md): share builds with a team.
- [FAQ](FAQ.md): short answers by topic.
