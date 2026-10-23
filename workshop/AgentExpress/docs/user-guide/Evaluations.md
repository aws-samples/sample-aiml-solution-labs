# Evaluations

Evaluations score an agent's output with an LLM judge, using AgentCore Evaluations. Use them to track answer quality as you change prompts and models. You'll pick built-in judges or write your own on the **Evals** tab in the control plane, turn evaluations on for an agent (automatic or on demand), and read the scores. For fixed pass or fail checks on whole runs, use [Test suites](Test_Suites.md) instead.

# Know the built-in judges

AgentCore has built-in evaluators, each named `Builtin.<Name>`. The framework scores these:

- Coherence, Conciseness, Correctness, Faithfulness;
- Harmfulness, Helpfulness, InstructionFollowing;
- Refusal, ResponseRelevance, Stereotyping.

Pick a built-in when it already measures what you care about. Faithfulness, for example, checks an answer against the material the agent was given.

# Write your own judge on the Evals tab

The **Evals** tab of a build holds your own judges. It sits among the build's tabs under **Build manually**.

The Evals tab of a build with no custom evaluators yet:

![The Evals tab: Agent level. Judges that score the output of the agents that use them. This build's evals (0), Custom LLM-as-a-judge evaluators an agent is scored with; Publish to library, Share, Add from library and Add evaluator buttons; a table with Name, From, Used by, Problems and Actions, and None in this build. Add one here, or from your library.](images/cp-tab-evals.png)

To add one:

1. Click **Add evaluator**.
1. Give it a name, a description and **instructions** for the judge. The instructions may use `{context}` (the request and earlier steps) and `{assistant_turn}` (this agent's output).
1. Optionally pick the judge **model** (the workflow's default model otherwise). Judge models must be Bedrock models.
1. Optionally set a **scale** of 2 to 20 points, each with a value, a label and a definition. The default is five points from 0 to 1.
1. Save it. An agent uses it as `Custom.<name>`.

**Add from library** brings in a judge someone published, and **Publish to library** shares yours. See [Library](Library.md).

The deploy creates each custom evaluator in AgentCore and deletes it on destroy.

# Turn evaluations on for an agent

1. Select the agent on the canvas.
1. In its settings panel, under **AgentCore features**, turn on **Evaluations**.
1. Pick **Evaluators**: built-in ones, your own from the **Evals** tab, or both.
1. Choose when to score: tick **Score every run automatically (otherwise on demand)** to score at the end of each run (`auto: true`), or leave it off to score on demand only (`auto: false`).
1. Deploy.

Each evaluator is a billed model call, so on demand is the cheaper choice. In `workflow.json`:

```json
"evaluations": { "enabled": true, "auto": true, "evaluators": ["Builtin.Faithfulness", "Custom.toneCheck"] }
```

An A2A agent makes its own model call, so it is scored from a role description. Prefer a narrow evaluator list there, and skip Faithfulness.

# Score on demand

When scoring is not automatic, or you want a fresh score:

- On the Observability page, open **Run detail**, load the run, and click the agent's **Prompts & I/O**. Click **Evaluate** to score it now, or **Re-evaluate** to score again.
- Or ask the Run Assistant to run an evaluation.

You need the `evaluate` permission. On-demand evaluations do not count against the spend and run limits.

# Read the scores

- On the run's graph, the agent's **Eval** row shows the number of scores.
- On the run's **Flow** tab, a line such as "Scored by Helpfulness". The **Evaluations** chip filters to them.
- In the **Prompts & I/O** inspector, each evaluator's score with its label and the judge's explanation.
- When runs are exported, the scores go with them (on by default). See [Export to Langfuse, Datadog and others](Observability_Export.md).

Compare scores for the same kind of request before and after a change to see whether it helped. See [Refine and redeploy](Refine_and_Redeploy.md).

# Ask the Assistant

In the control plane, the AgentExpress Assistant can turn evaluations on for an agent, pick its judges, choose automatic or on-demand scoring, and write your own judges. Requests you can paste:

- "Turn on evaluations for the letter agent with Faithfulness and Helpfulness, scored on demand"
- "Add a judge called toneCheck that scores whether a reply is polite and plain, and use it on the reply agent"
- "Score the summary agent automatically after every run, with Correctness"

It offers only the built-in judges this framework can score. To base a judge on your own rubric, attach it, or paste the address of the page that holds it.

What it can't do here:

- Score a run now or read scores. Use **Evaluate** in the **Prompts & I/O** inspector on the execution plane's Observability page, under **Run detail**, or ask the Run Assistant in the execution plane.
- Write or run test suites. Use the **Tests** tab.
- Deploy. Judges are created and scoring starts once you choose **Deploy**.

All requests by feature: [Ask the Assistant](Ask_the_Assistant.md).

# Related pages

- [Agents](Agents.md): the agent settings panel and AgentCore features.
- [One run in detail](Run_Traces.md): the inspector with Evaluate and Re-evaluate.
- [Test suites](Test_Suites.md): fixed checks, with a pass rate by version.
- [Library](Library.md): share judges across builds.
- [Refine and redeploy](Refine_and_Redeploy.md): act on the scores.
