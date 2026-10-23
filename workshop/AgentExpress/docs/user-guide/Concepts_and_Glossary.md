# Concepts and glossary

New to the terms? Start with the ideas the rest of the manual builds on: builds and versions, stages and steps, runtimes, tools and the Gateway, and review gates. Then use the A to Z glossary of the terms you meet in the control plane and the execution plane, each linked to the page that explains it.

# Builds and versions

A **build** is one workflow in the control plane: its `workflow.json`, prompts, tool code, documents and test suites. It saves as you type and follows you to any browser. A build is private to its creator until shared, and can live in **My builds** or in a team's workspace.

A **version** is a build frozen by **Deploy**. Versions count successful deploys only. Library items the build uses are frozen into the version, so a later library change reaches the execution plane only on its next deploy. Each run records the workflow version it ran on.

A **deployment** is a build's stack in an AWS account and Region, created with AWS CDK or Terraform. See [Deploy a build](Deploying_a_Build.md).

Live config versions are different: they change an agent's prompt or model on the running execution plane without a deploy. See [Live config](Live_Config.md).

# Stages and steps

A **workflow** is the whole process. In `workflow.json` it is the `agents` block (what each step is) and the `steps` block (the order).

Each entry in `steps` is a **stage**, and each agent in it is a **step**. A stage holds one agent, a **parallel** group whose agents run at the same time, or a **sequence** whose agents run one after another.

- Every step reads the run's **request**: what the person typed on **Start run**.
- Each step reads the approved outputs of earlier steps.
- A **review gate** after a stage lets a person approve, revise or deny.
- A **branch** on a stage lets its output pick the next stage, skip one, or end the run.

See [Stages and steps](Stages_and_Steps.md).

# Runtimes

An **agent** is one entry in `agents`. Its **runtime** says where it runs: `main` (inside the orchestrator, the default), `dedicated` (its own AgentCore Runtime), `task` (a step with no model), `harness` (an AgentCore Harness run from instructions) or `a2a` (someone else's agent over the A2A protocol). Around every agent with a model, the framework applies its guardrails, memory, telemetry and skills. See [Runtimes](Runtimes.md).

# Tools and the Gateway

A **tool** is anything an agent may call: a knowledge base, web search, an MCP server, a REST API, a Lambda function, an AWS service API, a code sandbox or a browser. Every tool is a target on the build's **AgentCore Gateway**, and agents call tools only through it. The Gateway's Cedar policy engine decides every call, for every agent: a declared tool is permitted, anything else is denied, and your own rules narrow it further. See [Tools tab and the Gateway](Tools_and_Gateway.md) and [Policies (Cedar)](Policies_and_Cedar.md).

# Review gates

A **review gate** (HITL, human in the loop) pauses a run after a stage until a person decides: **Approve** continues, **Revise** re-runs the step with the reviewer's note, **Deny** ends the run. The run's state is checkpointed, so it can wait as long as it needs. People also come in mid-step: an agent can ask a person a question, and a tool can make each call wait for approval. See [Human in the loop](Human_in_the_Loop.md).

# Glossary

| Term | Meaning | Read |
|---|---|---|
| A2A agent | An agent you do not operate, reached over the Agent2Agent protocol | [Runtimes](Runtimes.md) |
| Activity log | The record of sign-ins, run actions, deploys and changes | [Activity](Activity.md) |
| Agent card | A card on the canvas or the run graph for one step, listing its components as rows | [Agents](Agents.md) |
| AgentCore | Amazon Bedrock AgentCore, the AWS service AgentExpress runs on | [Control plane and execution plane](Architecture.md) |
| AgentCore Harness | An agent with no code, run by AgentCore from its instructions | [Runtimes](Runtimes.md) |
| AgentCore rail | The canvas strip listing the deployment-wide pieces | [Build manually on the canvas](Canvas.md) |
| AgentExpress Assistant | The control plane's chat that drafts and edits a build. It cannot deploy or publish. | [Build with the AgentExpress Assistant](AgentExpress_Assistant.md), [Ask the Assistant](Ask_the_Assistant.md) |
| AWS Agent Registry | Your organisation's catalogue of approved MCP servers, A2A agents and skills | [AWS Agent Registry](Agent_Registry.md) |
| Branch | Rules on a step's output that pick the next stage, skip one, or end the run | [Stages and steps](Stages_and_Steps.md) |
| Bundle | One JSON file with a build's workflow, prompts and code | [Export, edit and import](Export_and_Import.md) |
| Canvas | The control plane's drag-and-drop view of the workflow | [Build manually on the canvas](Canvas.md) |
| Cedar policy | A rule the Gateway applies to every tool call | [Policies (Cedar)](Policies_and_Cedar.md) |
| Checkpoint | The saved state of a run, kept in AgentCore Memory | [Memory](Memory.md) |
| Connector | A ready-made OAuth tool for a SaaS service | [Add tools and connectors](Adding_Tools.md) |
| Control plane | Where you design, configure, share and deploy workflows. One per account. | [Control plane and execution plane](Architecture.md) |
| Corpus | A named set of documents in the knowledge base | [Tool types in depth](Tool_Types.md) |
| Evaluator | An LLM judge that scores an agent's output | [Evaluations](Evaluations.md) |
| Execution plane | A deployed build's own web application, where runs happen. One per deployed workflow. | [The execution plane](Execution_Plane.md) |
| Fallback model | A model tried when the agent's model is busy or not ready | [Models, routing and LiteLLM](Models.md) |
| Framework | How an agent's reasoning step is written: plain, Strands Agents or LangGraph | [Runtimes](Runtimes.md) |
| Gateway | The AgentCore Gateway: one MCP endpoint in front of every tool | [Tools tab and the Gateway](Tools_and_Gateway.md) |
| Generic step | The control plane's name for a task step: no model | [Stages and steps](Stages_and_Steps.md) |
| Group | A sign-in group that grants actions | [Users, groups and RBAC](Users_Groups_and_RBAC.md) |
| Guardrail | A Bedrock guardrail that screens an agent's input or output | [Guardrails](Guardrails.md) |
| HITL | Human in the loop: a review gate, a question to a person, or a tool approval | [Human in the loop](Human_in_the_Loop.md) |
| Identity | A named OAuth client or API key, kept in AgentCore Identity | [Identity](Identity.md) |
| Interceptor | A Lambda the Gateway calls before each tool request or after each answer | [Interceptors](Interceptors.md) |
| Library | Pieces shared across builds | [Library](Library.md) |
| LiteLLM | A proxy through which an agent can use non-Bedrock models | [Models, routing and LiteLLM](Models.md) |
| Live config | Versioned prompt and model changes on a running execution plane, with rollback | [Live config](Live_Config.md) |
| MCP | Model Context Protocol. The Gateway speaks it to agents. | [Tool types in depth](Tool_Types.md) |
| Memory record | One insight AgentCore extracted into long-term memory | [Memory records](Memory_Records.md) |
| Model routing | Per-agent rules that pick a model per call | [Models, routing and LiteLLM](Models.md) |
| Named block | A guardrail, memory, evaluator, identity or policy defined once and used by name | [workflow.json settings](Workflow_JSON_Reference.md) |
| Orchestrator | The LangGraph program on AgentCore Runtime that runs a workflow's steps | [Runtimes](Runtimes.md) |
| Policy engine | The Gateway's Cedar engine, in Enforce or Log only mode | [Policies (Cedar)](Policies_and_Cedar.md) |
| RBAC | Role-based access: which groups may start, decide, re-run, deploy and so on | [Users, groups and RBAC](Users_Groups_and_RBAC.md) |
| Re-run | Running a step again, with a note, and everything after it | [Resume, restart and re-run](Resume_Restart_Rerun.md) |
| Request | What a person types on Start run. Every agent in the run reads it. | [Start a run: prompt, file or trigger](Starting_a_Run.md) |
| Run | One execution of a deployed workflow | [The run page and its tabs](Run_Page.md) |
| Run Assistant | The execution plane's chat that answers about runs | [Chat with the Run Assistant](Run_Assistant.md) |
| Skill | Know-how (a SKILL.md and files) an agent opens when a task needs it | [Skills](Skills.md) |
| Subject | An optional name on Start run that scopes long-term memory | [Memory](Memory.md) |
| Team | People who share a workspace of builds | [Teams and workspaces](Teams.md) |
| Test suite | Stored requests with fixed checks, run against the deployed build | [Test suites](Test_Suites.md) |
| Tool mode | `direct` (every tool once, up front) or `model` (the model picks) | [Add tools and connectors](Adding_Tools.md) |
| Trigger | A webhook, schedule, EventBridge rule, S3 upload or SQS message that starts a run | [Triggers](Triggers.md) |
| workflow.json | The one file that describes a workflow | [workflow.json settings](Workflow_JSON_Reference.md) |

# Related pages

- [Control plane and execution plane](Architecture.md): the two planes
- [The lifecycle](Lifecycle.md): the ten stages from design to refine
- [workflow.json settings](Workflow_JSON_Reference.md): the file, section by section
- [FAQ](FAQ.md): common questions
