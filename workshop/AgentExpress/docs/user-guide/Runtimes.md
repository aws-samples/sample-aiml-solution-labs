# Runtimes

Every step of a workflow is an agent entry in `workflow.json`, and its **runtime** says where it runs and who operates it: inside the orchestrator, in its own AgentCore Runtime, as a step with no model, in an AgentCore Harness, or as somebody else's agent over A2A. Read on to pick a runtime for each agent, run agents in your VPC, and choose an agent framework. In the control plane, pick the runtime under **Runtime** in the agent settings panel.

# Compare the runtimes

| `runtime` | Where it runs | Code you write | Use it for |
|---|---|---|---|
| `main` (default) | Inside the orchestrator's AgentCore Runtime | An agent folder (the control plane writes it) | Most agents |
| `dedicated` | Its own AgentCore Runtime, same image | The same agent folder | An agent that needs its own scaling, role or isolation |
| `task` | Inside the orchestrator, no model | None | Calling one tool, running a script, reading a document, writing a file |
| `harness` | An AgentCore Harness the deploy creates, or yours by ARN | None: instructions only | An agent with no code, other model providers, a shell, the managed browser and sandbox |
| `a2a` | Someone else's service | None | Handing a step to an agent you do not operate |

Every runtime is an ordinary step: put it in a parallel or sequence stage, gate it, branch on it and re-run it. On a run's graph, a chip on each card names the runtime when it is not `main`.

# Main agents

A `main` agent runs as a LangGraph node inside the orchestrator container. An agent with no `runtime` is `main`. It has a folder `app/subagents/<id>/` with its prompt and its `agent.py`, which the control plane writes from the agent's **Prompt** section when it deploys.

Around each agent, the framework applies the input and output guardrails, recalls and stores long-term memory, records telemetry for every call, and warns on the timeline about figures that appear in no earlier output. Inside its code the agent calls `ctx.llm` for a model call, `ctx.call_tool` for a tool call through the Gateway, `ctx.retrieve` for knowledge-base retrieval, and `ctx.input` for an earlier step's approved output.

# Dedicated agents

A `dedicated` agent runs the same code as a `main` agent, but in its own AgentCore Runtime with its own IAM role. Moving an agent there is a one-word change: `"runtime": "dedicated"`.

- The orchestrator calls it and waits, up to `orchestrator.runtimeInvoke.readTimeoutSeconds` (default 600 seconds). Keep it above your slowest agent.
- The call is not retried by default (`maxAttempts` 1), because a retry would run and bill the agent twice.
- A dedicated agent cannot wait for a person: `askPerson` is refused, and so is a tool with `approval`.
- Live config still applies to it.
- It can run in your VPC, apart from the orchestrator. See the next section.

# Run agents in your VPC

Each AgentCore Runtime of a build runs **Public** by default: it reaches the internet and AWS endpoints directly. Choose **VPC** when agents must reach resources inside your VPC. Where you set it depends on the runtime:

| Runtime | Where to set the network | `workflow.json` |
|---|---|---|
| `main` and `task` | **Network** on the Settings tab. It is the shared orchestrator Runtime, so every main agent and task step runs there. | `orchestrator.network` |
| `dedicated` | **Network** in the agent's settings panel. It is not taken from the orchestrator's. | `agents.<id>.network` |
| `harness` | **Environment** in the harness settings: **VPC subnets (optional)** and **Security groups** | `harness.network` |
| `a2a` | Not available: the agent runs on its owner's network, outside this build | None |

A hosted MCP server has its own **Network** setting too; see [Tool types in depth](Tool_Types.md).

To put a Runtime in your VPC:

1. Pick **VPC** under **Network mode**.
1. In **Subnet ids**, list 1 to 16 subnets, separated by commas. AWS recommends two private subnets in different Availability Zones.
1. In **Security group ids**, list 1 to 16 security groups. Their outbound rules must allow everything the agents call.

```json
"network": { "mode": "VPC", "subnetIds": ["subnet-0abc1234def567890", "subnet-0fed9876cba543210"],
             "securityGroupIds": ["sg-0abc1234def567890"] }
```

Choosing **Public** removes the setting, so the deploy is as before. Subnet and security group ids on a Public Runtime are refused, because they would set nothing.

Plan the VPC before you deploy. The full rules are on the AWS page [AgentCore VPC requirements](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/agentcore-vpc.html):

- A Runtime in a VPC has no internet access by default. It reaches only what is inside the VPC.
- For AWS services, add VPC endpoints. The AWS page lists the ones a Runtime needs, and the agents also call services such as Amazon Bedrock.
- For the internet, use private subnets that route to a NAT gateway in a public subnet. In AgentExpress that covers, for example, a LiteLLM proxy or an export to another tool.
- Subnets must be in Availability Zones AgentCore supports. The AWS page lists them per Region.
- AgentCore creates the network interfaces with the service-linked role `AWSServiceRoleForBedrockAgentCoreNetwork` the first time a Runtime is created with a VPC. The deploy role is allowed to create that role.
- After an agent's Runtime is deleted, its network interfaces can stay for up to 8 hours. Until they are gone, they block deleting the subnets and security groups.

The AgentExpress Assistant can set any of these networks from the chat.

# Task steps

A `task` step does one fixed thing with no model call: call a tool, run code, read a document with Amazon Textract, write a file, or deliver the run's files. In the control plane it is a **generic step**, added with **+ New generic step**. Its five kinds and their fields are on [Stages and steps](Stages_and_Steps.md). On a run's graph it shows as "Task · Code", "Task · Tool", "Task · Extract", "Task · Output" or "Task · Deliver".

# AgentCore Harness

A harness agent has no code. You write its instructions, and the deploy creates an AgentCore Harness for it.

1. Set **Runtime** to **harness**. Settings a harness does not read are dropped, and **Instructions** start from the prompt you wrote.
1. Write the **Instructions**: who the agent is, what it does with the request and earlier stages, and the JSON it returns. A change takes effect when you deploy; live config does not change a harness agent.
1. Fill in the harness sections.

| Section | What you set |
|---|---|
| **Model** | **Provider**: Amazon Bedrock (default), OpenAI, Google Gemini or LiteLLM, its **API key** (an API key identity) and an optional **API base URL** |
| **Tools** | The build's Gateway tools, remote MCP servers (**Add MCP server**, up to 10), Browser, Code Interpreter, and a shell |
| **Skills** | From the AWS skills catalog, a Git repository or an S3 prefix (**Add skill**), up to 20 |
| **Memory** | One of the build's memories |
| **Environment** | **Container image (optional)**, storage mounts (**Add storage**), **VPC subnets (optional)** and **Security groups** |
| **Version and endpoint** | An **Endpoint name (optional)** |
| **Limits** | **Most model turns** (1 to 100 per step) and **Longest step (seconds)** (1 to 900). Empty uses the harness's own. |

The example build's **Sandbox harness** card selected on the canvas, with the lower part of its settings on the right:

![The Design tab: the palette on the left, the canvas with the Sandbox harness card selected, and on the right the lower part of its settings: Storage with Add storage, VPC subnets (optional), Security groups, Version and endpoint with Endpoint name live, Limits with Most model turns 20 and Longest step 600 seconds, then the start of the AgentCore features section](images/cp-harness-settings.png)

- The harness gets its own execution role with only what its settings need. There is no separate harness charge: you pay for the model calls and the runtime session.
- Guardrails, evaluations and the agent's memory switch work as for any agent. Outbound identity and tools that act as the person are refused.
- A model of another provider is recorded with its tokens and no price.

## Bring your own harness

Turn on **Use an existing harness (ARN)** to call a harness made elsewhere in the same account and Region. Give its **Harness ARN**, an optional **Endpoint**, and the limits. In `workflow.json`: `"harness": { "arn": "<harness ARN>" }`. The deploy creates nothing for that agent, and the orchestrator's role may invoke exactly that ARN. Instructions, model and tools are refused, because they are set on your harness.

# A2A agents

An A2A agent is somebody else's agent, reached over the Agent2Agent protocol. Nothing of it lives in your build, so it is a trust boundary. Set **Runtime** to **a2a** and give its Agent Card URL (https only), or import an approved one from the [AWS Agent Registry](Agent_Registry.md).

```json
"credit_check": { "name": "Vendor Credit Check", "runtime": "a2a",
  "agentCard": "https://agents.vendor.example/credit", "auth": "bearer", "produces": "credit-assessment" }
```

- **auth**: `none` (default), `bearer` (a token from the build's secrets), `oauth2` (the agent's outbound identity) or `sigv4` (the orchestrator's role).
- **You keep** guardrails, long-term memory, gates, branches, re-runs and version history.
- **You lose** local prompt capture (so Faithfulness cannot be scored), Cedar over the remote agent's own tool calls, and contract validation of its reply.
- `model`, `temperature`, `maxTokens`, `tool` and `corpus` are refused. If the remote agent fails, the step fails with what it said.
- `orchestrator.a2aInvoke` sets the request timeout (default 30 seconds), the polling gap (2 seconds) and the most time to wait (300 seconds).

## Try A2A with the framework's stand-in

The framework ships a stand-in A2A server, so you can try an A2A step without a third party's agent:

- `"source": "a2a_lambda"` reaches the A2A server the framework deploys for you, instead of an Agent Card URL. The deploy fills in its address, so there is nothing to commit.
- Set `auth` to `sigv4`: the stand-in is called with the orchestrator's role. `source` and `agentCard` cannot be used together.
- `skill` picks which of the stand-in's skills this agent is: `compliance` (default), `resilience`, `analysis` or `recommendation`. An external agent lists its own skills in its Agent Card instead.

```json
"compliance": { "name": "Outside compliance review", "runtime": "a2a", "produces": "compliance-review",
  "source": "a2a_lambda", "skill": "compliance", "auth": "sigv4" }
```

# Frameworks

A `main` or `dedicated` agent's reasoning step can be written three ways. Pick **Agent framework** in the agent settings panel:

| Option | `framework` | What agent.py does |
|---|---|---|
| No framework, one `ctx.llm` call | `plain` (default) | Calls the model directly |
| Strands Agents | `strands` | Reasons in a Strands agent |
| LangGraph | `langgraph` | Reasons in a nested LangGraph |

All three call the model through `ctx.llm`, where guardrails, telemetry, memory and truncation detection live. The choice decides how the generated `agent.py` is written; an `agent.py` you wrote yourself is kept.

# Ask the Assistant

The AgentExpress Assistant can set any runtime on this page: move an agent to `dedicated`, `task`, `harness` or `a2a`, fill in every harness section, set A2A agents and their timeouts, put any Runtime in your VPC, and pick the framework. Requests you can paste:

- "Move contract_review to its own dedicated runtime and let the orchestrator wait 900 seconds for it"
- "Put the main runtime in our VPC: subnets subnet-0abc1234def567890 and subnet-0fed9876cba543210, security group sg-0abc1234def567890"
- "Make the research agent a harness agent with the shell and Code Interpreter, at most 20 model turns and 600 seconds per step"
- "Add the framework's A2A stand-in as an analysis agent after the intake stage"

* It never invents subnet, security group or harness ids, or an Agent Card URL. It asks, or lists the value under **Needs your input**.
* It can't enter an A2A bearer token or a harness provider's API key. It names the secret; fill it in on the **Secrets** card beside the chat, the agent's panel, or the Identity tab.
* It can't create the VPC, its endpoints or a NAT gateway. Set those up in AWS first.
* With **Review changes** on **Auto**, a second model checks network, harness shell, container, browser and Code Interpreter changes before they are saved.

More requests are on [Ask the Assistant](Ask_the_Assistant.md).

# Related pages

- [Agents](Agents.md): the agent settings panel
- [Models, routing and LiteLLM](Models.md): which model each agent calls
- [Tool types in depth](Tool_Types.md): what agents can reach, and hosted MCP servers
- [workflow.json settings](Workflow_JSON_Reference.md): the `agents` keys
