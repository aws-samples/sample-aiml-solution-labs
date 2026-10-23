# Agents

An agent is one step of the workflow that calls a model: it reads the request and earlier outputs, may call tools, and returns its output. Use this when you add an agent or change what one does: read its card, then set its prompt, model, AgentCore features and where it runs.

# Read an agent card

Each agent on the canvas is a card. Its head shows the agent's initials (or a symbol for a generic step), its name, and one line under the name: its tools, _remote agent (A2A)_, the kind of generic step, the image model of an image agent, or where it runs. A red badge counts its errors.

Each row under the head is something attached to the agent:

| Row tag | What it is | Read |
|---|---|---|
| **Tool** | A tool the agent may call | [Add tools and connectors](Adding_Tools.md) |
| **Guardrail** | A Bedrock guardrail that checks the agent's input or output | [Guardrails](Guardrails.md) |
| **Memory** | AgentCore Memory: what the agent remembers across runs | [Memory](Memory.md) |
| **Policy** | A Cedar policy on one of the agent's tools. It shows on every agent that uses that tool. | [Policies (Cedar)](Policies_and_Cedar.md) |
| **Skill** | SKILL.md know-how the agent may open | [Skills](Skills.md) |
| **Eval** | An AgentCore evaluator that scores the agent's answers | [Evaluations](Evaluations.md) |
| **Identity** | Outbound credentials from AgentCore Identity | [Identity](Identity.md) |
| **HITL** | The agent can ask a person questions mid-step | [Human in the loop](Human_in_the_Loop.md) |

## Work with rows

- Click a row's name to open its settings.
- Click × on a row, or select it and press Delete, to take it off the agent. The notice says _Undo puts it back_.
- Drag a row to another agent to move it there, or drop it off its agent to take it off.
- Drag a **Policy** row onto another tool's row to move the policy to that tool.
- **+ Add tool, memory, guardrail…** at the bottom of a card adds anything the build has that the agent does not use yet. Its **Create new** group opens the tab where that kind is made.

A policy governs its tool for every agent that calls it, so taking it off one card asks you to confirm when other agents use the same tool. A build-wide policy cannot be taken off a card; change it on the Policies tab.

# Open the settings panel

Click an agent's card to open its settings on the right.

The example build's **Letter drafter** card selected on the canvas, with the top of its settings panel on the right:

![The Design tab: the palette on the left, the canvas with the Letter drafter card selected and its rows for claimsDb, customerMemory, noOtherCustomers, claimLetter and Asks questions, and on the right the settings panel: Letter drafter with Remove from canvas and Delete, Runs in this deployment, Attached to this agent with Tool claimsDb, Memory customerMemory, Policy noOtherCustomers on claimsDb, Skill claimLetter and HITL Asks questions, then Agent id, Stage, and Earlier in group and Later in group](images/cp-agent-inspector.png)

| Part | What it does |
|---|---|
| Name and buttons | The agent's name, where it runs, **Remove from canvas** and **Delete** |
| Problems | This agent's problems, if any |
| **Attached to this agent** | The same rows as the card, each with ×, and the **Add a tool, memory, guardrail…** menu |
| **Agent id** | Letters, digits and underscores, starting with a letter. Renaming updates every reference to it. |
| **Stage** | Which stage it runs in, **Not on the canvas**, or **A new stage at the end** |
| **Prompt** | **System prompt** and **Output shape**. A harness agent has **Instructions** instead. |
| Agent fields | Runtime, model, routing and fallbacks, framework, tools, tool mode, skills, knowledge-base corpus and more. Rarely used fields are folded. |
| **Prompt caching** | What Bedrock caches for this agent's model calls: the system prompt, tool definitions and conversation so far. Off unless set. See [Prompt caching](Prompt_Caching.md). |
| **Network** | Where a dedicated agent's own AgentCore Runtime runs: **Public** (default) or **VPC**. Other agents say where their network is set. See [Runtimes](Runtimes.md). |
| **AgentCore features** | Switches for guardrails, long-term memory, evaluations and outbound identity |

# Write the prompt

Under **Prompt**:

- **System prompt**: who the agent is, what it does with the request and earlier outputs, and what it must not do.
- **Output shape**: the JSON the model must return, field by field. Later steps, branch rules and test checks read these fields.

The control plane writes the agent's `prompts.py` from these fields when it deploys. Every agent is told today's date. To change a prompt in the running execution plane without a deploy, use [Live config](Live_Config.md).

# Choose a model

The model list is read live from Bedrock and shows only models this account can call. When the agent reads images or picks its own tool calls, the list shows only models that can do that. You can also type a model id the list does not show. An agent with no model uses the build's default model, set on the [Settings tab](Settings.md).

Set **maxTokens**, the output budget per call, high enough for the whole JSON output. Too low, and the output is cut and the step reports that the model returned no parseable JSON.

Routing rules, fallbacks, per-run overrides and LiteLLM models are on [Models, routing and LiteLLM](Models.md).

# Turn on AgentCore features

The **AgentCore features** section has one switch per feature. Turn one on, then pick what it uses.

- **Guardrail: check input** and **Guardrail: check output**: pick the **Guardrail**, or leave it on **The deployment guardrail**. New agents start with the build's guardrail on.
- **Long-term memory**: pick a memory defined on the **Memory** tab.
- **Evaluations**: pick built-in evaluators and your own. Turn on **Score every run automatically (otherwise on demand)** to score each run.
- **Outbound identity (calls an API directly)**: pick the identities the agent signs in with to call an API itself, not through the Gateway.

There is no Cedar switch here. Cedar policies apply at the Gateway to every tool call, whichever agent makes it, and you attach them to tools. See [Policies (Cedar)](Policies_and_Cedar.md).

# Choose where the agent runs

**Runtime** in the agent fields says where the agent runs: `main` (inside the orchestrator, the default), `dedicated` (its own AgentCore Runtime), `harness` (an AgentCore Harness run from instructions) or `a2a` (someone else's agent). **Agent framework** picks how a `main` or `dedicated` agent's reasoning step is written: plain, Strands Agents or LangGraph. Both are explained on [Runtimes](Runtimes.md).

An agent can also draw images (`output: "image"`) or read earlier agents' images (`vision`); see [Models, routing and LiteLLM](Models.md).

# Label long-running agents and data sources

Two agent fields only change what the card shows. Neither changes how the agent runs.

- **kind** (under **Advanced → Labels**): `sync` (default) or `async`. Set `async` for a step that is long-running; the card then shows a chip.
- **access** (under **Advanced → Tool use**): a short label for the data source the agent reads, shown as a chip. It is read only when the agent has no tool, because a tool's chip comes from its type. Picking a tool for the agent in the control plane removes **access**, and an agent with both is an error in Problems.

# Ask the Assistant

The AgentExpress Assistant can add, change, rename or delete any agent. It writes the prompt and output shape, and sets the runtime, model (only models this account can use), model tuning, tools and tool mode, knowledge-base corpus, skills, the AgentCore features, images, labels and a dedicated agent's network. It can also find an approved remote agent in your AWS Agent Registry and add it. Requests you can paste:

- "Add a Risk scorer agent after intake that returns a score from 1 to 5 and its reasons"
- "Give the policy checker the warranty corpus of the policyDocs knowledge base"
- "Run the fraud checker on its own dedicated runtime in my VPC, subnets subnet-0abc and subnet-0def, security group sg-0123"
- "Find a currency conversion agent in our registry and add it after intake"

* A change to a network, guardrail, identity or tool connection is checked by a second model first when **Review changes** is on **Auto** or **Always**.
* A remote agent's bearer token is not typed in the chat: enter it in the **Secrets** card or the agent's settings.

More requests are on [Ask the Assistant](Ask_the_Assistant.md).

# Related pages

- [Stages and steps](Stages_and_Steps.md): where the agent runs in the order
- [Add tools and connectors](Adding_Tools.md): give the agent tools
- [Runtimes](Runtimes.md): main, dedicated, task, harness and A2A
- [Models, routing and LiteLLM](Models.md): model choice per call
