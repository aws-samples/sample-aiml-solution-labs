# The lifecycle

A workflow's life in AgentExpress has ten stages, from design to refine. Each stage below links to the manual pages that cover it, so use it as a map when you are not sure which page you need next.

The lifecycle: stages 1 to 5 happen in the control plane, stages 6 to 9 in the build's execution plane, and stage 10 loops back:

![The AgentExpress lifecycle: in the control plane, 1 Design, 2 Build, 3 Configure, 4 Test and 5 Deploy; in the execution plane, 6 Run, 7 Review, 8 Observe and 9 Evaluate; and 10 Refine, which changes a prompt or model with live config or edits the build and deploys the next version](images/design-lifecycle.png)

# Design and build

**1 Design.** Decide the agents, the stages they run in, and where a person signs off. Start from what comes in, what should come out, where the facts come from, and who approves.

- [Control plane and execution plane](Architecture.md)
- [Concepts and glossary](Concepts_and_Glossary.md)

**2 Build.** Describe the workflow to the AgentExpress Assistant, or lay it out yourself on the canvas. Both edit the same build.

- [Build with the AgentExpress Assistant](AgentExpress_Assistant.md)
- [Build manually on the canvas](Canvas.md)
- [Stages and steps](Stages_and_Steps.md) and [Agents](Agents.md)
- [Export, edit and import](Export_and_Import.md), to work on the build in code

# Configure and test

**3 Configure.** Give agents their tools, and turn on the AgentCore features each one needs: guardrails, memory, Cedar policies, identity, interceptors, skills and triggers.

- [Add tools and connectors](Adding_Tools.md)
- [Tools tab and the Gateway](Tools_and_Gateway.md), [Policies (Cedar)](Policies_and_Cedar.md), [Guardrails](Guardrails.md), [Memory](Memory.md)
- [Runtimes](Runtimes.md) and [Models, routing and LiteLLM](Models.md) for advanced settings
- [Library](Library.md), to reuse pieces across builds

**4 Test.** Write test suites with fixed checks and evaluators that score answers, so you can tell whether a change helped before you ship it.

- [Test suites](Test_Suites.md)
- [Evaluations](Evaluations.md)

# Deploy

**5 Deploy.** Freeze the build as its next version and deploy it with AWS CDK or Terraform, into the control plane's own account or one of yours. Each build gets its own execution plane.

- [Set up AWS accounts](AWS_Accounts.md)
- [Deploy a build](Deploying_a_Build.md)
- [Open the execution plane](Opening_the_App.md)

# Run and review

**6 Run.** Start a run with a prompt, attached files, or a trigger such as a webhook or a schedule. Follow it step by step.

- [Start a run: prompt, file or trigger](Starting_a_Run.md)
- [The run page and its tabs](Run_Page.md)
- [Triggers](Triggers.md)

**7 Review.** People approve, revise or deny at review gates, answer an agent's questions, and approve tool calls. Re-run a step with a note when it needs another try.

- [Human in the loop](Human_in_the_Loop.md)
- [Resume, restart and re-run](Resume_Restart_Rerun.md)

# Observe and evaluate

**8 Observe.** Read each run's cost, tokens, traces, prompts and I/O, and watch error, denial and block rates across runs.

- [Observability](Observability.md) and [One run in detail](Run_Traces.md)
- [Analytics](Analytics.md) and [Activity](Activity.md)
- [Cost and spend limits](Cost_and_Limits.md)

**9 Evaluate.** Score agents' answers with built-in or custom LLM judges, on every run or on demand.

- [Evaluations](Evaluations.md)

# Refine

**10 Refine.** Change a prompt or a model on the running execution plane with live config, or edit the build and deploy the next version. Version history, test pass rates and evaluation scores show whether each change helped.

- [Live config](Live_Config.md)
- [Refine and redeploy](Refine_and_Redeploy.md)

# Related pages

- [Your first build and run](First_Build_and_Run.md): the lifecycle in ten minutes
- [Control plane and execution plane](Architecture.md): where each stage happens
- [Concepts and glossary](Concepts_and_Glossary.md): the terms used on each page
