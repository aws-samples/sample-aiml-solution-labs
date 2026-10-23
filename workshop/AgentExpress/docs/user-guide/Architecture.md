# Control plane and execution plane

AgentExpress is split into a control plane, where you design and deploy workflows, and an execution plane, where people run them. Read this to learn which plane does what, what a deploy creates, and where Amazon Bedrock AgentCore fits.

# Two planes

AgentExpress is a configuration-driven framework. Every workflow is one file, `workflow.json`, and the framework turns that file into a running application on Amazon Bedrock AgentCore. The work is split into two planes.

The control plane, one per account, and the execution plane, one per deployed workflow:

![The AgentExpress control plane, one per account, with Design, Build, Configure, Connect tools, Test and evaluate, Library and registry, Teams and sharing, Work in code, AWS accounts, and Version and deploy; a Deploy arrow to the execution plane, one per deployed workflow, with Start a run, Follow it, Review, Resume and re-run, Files, Observe, Export, Monitor, Live config and Chat; and a Refine arrow back](images/design-planes.png)

| Plane | What it is | Who uses it |
|---|---|---|
| Control plane | The site where you keep builds and deploy them | People who design, share and deploy workflows |
| Execution plane | Each deployed build's own web application | Everyone who runs the workflow, reviews its gates and reads its results |

# What lives in the control plane

The control plane is where you design and deploy. It holds:

- **Builds**: each workflow as a draft, with its prompts, tool code, documents and test suites. See [The control plane](Builder_Console.md).
- **Design tools**: the [AgentExpress Assistant](AgentExpress_Assistant.md) and the [canvas](Canvas.md), plus one configuration tab per AgentCore feature.
- **The library and the registry**: pieces shared across builds, and your organisation's approved MCP servers, A2A agents and skills. See [Library](Library.md) and [AWS Agent Registry](Agent_Registry.md).
- **People**: teams, sharing, and the admin page. See [Teams and workspaces](Teams.md).
- **AWS accounts** a build can deploy into, and the deploy itself. See [Set up AWS accounts](AWS_Accounts.md).

The control plane stores each workflow as `workflow.json`. A control plane deployed in **Builder only** mode never runs a workflow itself. With the default settings, your control plane also includes one sample workflow of its own, and can start a deployed build's runs with **Run here**.

# What lives in the execution plane

Each deployed build is its own execution plane, with its own address, API, sign-in, runtimes, Gateway and tables. Its pages are:

- **Runs**: start a run with a prompt, attached files or a trigger, follow it on a graph, table, timeline or flow story, decide review gates, and re-run steps. See [The execution plane](Execution_Plane.md).
- **Observability**: cost, tokens, traces, prompts and I/O for each run, with export to other tools. See [Observability](Observability.md).
- **Files**, **Activity**, **Analytics**, **Memory records** and **Live config**: the pages for operating the execution plane. See [Analytics](Analytics.md) and [Live config](Live_Config.md).
- The **Run Assistant**: a chat that answers questions about runs. See [Chat with the Run Assistant](Run_Assistant.md).

An execution plane has no canvas and no configuration tabs. Its title in the top bar starts with "AgentExpress -" followed by the build's title.

# One control plane deploys many execution planes

- **Deploy** freezes the build as its next version and deploys exactly that version as its own stack, with AWS CDK or Terraform.
- Each build gets its own execution plane, in the control plane's own account or in an AWS account you connected. Other builds and the control plane are not touched.
- Deploying again ships version 2, 3 and so on to the same execution plane. Old runs keep the graph of the version they ran on.
- Destroying a build removes its stack and its execution plane.

See [Deploy a build](Deploying_a_Build.md) and [Refine and redeploy](Refine_and_Redeploy.md).

# Where AgentCore fits

The execution plane runs on Amazon Bedrock AgentCore. The framework turns each feature on from configuration, so you never write CDK, Terraform or Cedar plumbing for it.

| AgentCore feature | What it does in a build | Read |
|---|---|---|
| Runtime | Runs the orchestrator and any dedicated agents | [Runtimes](Runtimes.md) |
| Harness | Runs agents written as instructions only | [Runtimes](Runtimes.md) |
| Gateway | One MCP endpoint in front of every tool | [Tools tab and the Gateway](Tools_and_Gateway.md) |
| Policy (Cedar) | Decides every tool call at the Gateway | [Policies (Cedar)](Policies_and_Cedar.md) |
| Memory | Checkpoints for pauses, and long-term memory across runs | [Memory](Memory.md) |
| Identity | Credentials tools and agents use to reach APIs | [Identity](Identity.md) |
| Code Interpreter and Browser | Sandboxed code and web page reading | [Tool types in depth](Tool_Types.md) |
| Observability and Evaluations | Traces of every call, and LLM judges that score answers | [Observability](Observability.md), [Evaluations](Evaluations.md) |

Amazon Bedrock Guardrails screens agents' input and output; see [Guardrails](Guardrails.md).

# Inside one execution plane

The AWS resources of one execution plane:

![Architecture of one execution plane: the end user signs in through an identity provider; CloudFront and S3 serve the UI; API Gateway with a JWT authorizer fronts a Lambda back end that writes to DynamoDB and calls the orchestrator on AgentCore Runtime; the runtime uses AgentCore Memory, AgentCore Identity, Bedrock Guardrails and Claude, and calls tools through AgentCore Gateway with AgentCore Policy (Cedar), reaching an MCP server and a Lambda that queries a Bedrock Knowledge Base on S3 Vectors; CloudWatch, AgentCore Evaluations and Insights read the run traces](images/architecture.png)

What happens when you start a run:

1. The browser sends your sign-in token with each API call. API Gateway checks it before anything else runs.
1. A Lambda function (the back end) checks your permissions, records the run and calls the orchestrator on AgentCore Runtime.
1. The orchestrator runs the workflow as a LangGraph graph built from `steps`. Agents call Bedrock models and, through the AgentCore Gateway, their tools.
1. Progress is written to DynamoDB as it happens, and the run page polls it every 2 seconds.
1. At a review gate the graph pauses and its state is saved in AgentCore Memory. Your decision resumes it.

# Related pages

- [The lifecycle](Lifecycle.md): the ten stages from design to refine
- [Concepts and glossary](Concepts_and_Glossary.md): builds, versions, stages and steps
- [The control plane](Builder_Console.md): the control plane in detail
- [The execution plane](Execution_Plane.md): the execution plane in detail
