# Stages and steps

A stage is one step of the workflow on the canvas. Stages run from top to bottom, and each holds one agent or several. Use this when you lay out the order of a workflow: pick a stage shape, add a review gate or a branch, and add generic steps, which are steps with no model.

# Pick a stage shape

| Shape | Runs | On the run graph |
|---|---|---|
| **Single** | One agent | One card, numbered 1, 2… |
| **Parallel** | Several agents at the same time, joining before the next stage | A dashed group, cards 3a, 3b… |
| **Sequence** | Several agents left to right, each seeing the output of the ones before it | Cards 4a, 4b… with one gate after the last |

On the canvas, dropping an agent onto a stage runs it in parallel with the others there, and dropping it between stages makes a new stage. A stage's title says what it is: _Stage_ for one agent, or its gate name (or _Parallel_ or _Sequence_) and how many agents run _in parallel_ or _in order_.

In `workflow.json` each stage is one entry in `steps`:

```json
"steps": [
  { "agent": "intake", "hitl": true },
  { "parallel": ["policyCheck", "research"], "hitl": true, "gateId": "evidence", "gateName": "Evidence" },
  { "sequence": ["drafter", "reviewer"] },
  { "agent": "report" }
]
```

# Change stage settings

Click a stage's title to open its settings.

| Setting | What it does |
|---|---|
| **Shape** | **Single**, **Parallel** or **Sequence**. Parallel and Sequence need two or more agents. |
| **HITL gate** | Pause after this stage so a person can approve, revise or deny |
| **Gate id** | The stage's name, used by branch rules. Letters, digits and underscores. |
| **Gate name** | What reviewers see on the gate |
| **How the gate decides** | **Who decides**, **Rules that call for HITL**, **Where it is answered** and **Timeout** |
| **Branch** | Rules on this stage's output that pick the next stage, skip one, or end the run |

# Add a review gate

1. Select the stage on the canvas.
1. Turn on **HITL gate**. A **HITL gate** pill appears on the stage.
1. Optionally give it a **Gate id** and a **Gate name**.
1. Optionally open **How the gate decides**: a person always, only when rules match the output, or automatically; answered in the execution plane only or also by an EventBridge event; and a **Timeout** such as `30m`, `24h` or `2d`.

A gate on one agent decides that agent; on a sequence, the whole chain; on a parallel stage, each agent separately. What reviewers see and do is on [Human in the loop](Human_in_the_Loop.md).

# Add a branch

A branch lets a stage's output pick what runs next, with no agent code. A triage step can send a high-risk case to a deeper review, or end the run when the request cannot be acted on.

1. Select the stage and open **Branch** in its settings.
1. Add rules. Each names a **field** of the output, an operator and a target. The first match wins.
1. Set a default target, or leave it empty to continue to the next stage.

```json
{ "agent": "triage",
  "branch": {
    "when": [
      { "field": "disposition", "equals": "escalate", "goto": "investigator" },
      { "field": "claimId", "exists": false, "goto": "END" }
    ],
    "default": "adjuster" } }
```

- Operators: `equals`, `notEquals`, `in`, `contains`, `exists`, `gt`, `gte`, `lt` and `lte`. Text comparisons ignore case and surrounding spaces.
- A branch goes on a single-agent or sequence stage, not on a parallel stage and not on the last stage.
- A target is a later stage (an agent id or a `gateId`) or `END`. Going backwards is what a gate's Revise is for.
- On the canvas, a **Branch** pill marks the stage; point at it to read the rules. Branch arrows are dashed and labelled _if_ and _otherwise_. Steps a branch bypassed show as **Skipped** in a run.

# Add a generic step

A generic step (`runtime: "task"`) is a step with no model. Use one for work that needs no reasoning: call a tool with fixed arguments, run a script, read a document, or write a file. It runs in the orchestrator and costs no model call.

1. Drag **+ New generic step** from the palette onto the canvas, or click it to add it at the end.
1. In its settings, pick **What it does**. Until you pick one, the step is an error and the build cannot deploy.
1. Fill in the fields for that kind.

| What it does | Fields | Good to know |
|---|---|---|
| **Call a tool** | **Tool**, **Arguments** | Calls one tool through the Gateway. Cedar policy, interceptors and the tool's approval apply. |
| **Run code** | **Language**, **Script** | Python, JavaScript or TypeScript in the AgentCore Code Interpreter sandbox. Up to 50,000 bytes of code and 120 seconds. Each run is a billed session. |
| **Read a document** | **File**, **Also read** | Reads text, tables and forms from the run's PDF, PNG and JPEG files with Amazon Textract. Billed per page; not in every Region. |
| **Write a file** | **Format**, **File name**, **From** | Writes CSV, JSON, Markdown or text from earlier steps' output and keeps it with the run |

Every kind also has **Name**, **Step id** (later steps read this step's output by it) and **Stage**.

The example build's **Claim intake** step, a Run code step, selected on the canvas, with its settings on the right:

![The Design tab: the palette on the left, the canvas with stage 1 holding the Claim intake card (Task · Code) and stage 2 holding Fraud review, and on the right the generic step's settings: Claim intake, Task · Code, with Remove from canvas and Delete, Name, Step id intake, Stage 1, What it does set to Run code, Language python, and the Script with a note on in/request.txt, in/outputs.json and out/](images/cp-task-step.png)

A Run code script reads the request from `in/request.txt`, earlier outputs from `in/outputs.json`, and the run's files from `in/`. What it prints is its output (JSON is read as data), and files it writes to `out/` are kept.

A Call a tool step's arguments and a Write a file step's file name can use the request, the run ID and earlier outputs as placeholders, each written in double curly braces, for example `outputs.intake.service` inside them. Each step a placeholder names must run earlier.

# Ask the Assistant

The AgentExpress Assistant can lay out and change the whole stage list: shapes and order, review gates with their mode, rules, timeout and where they are answered, gate ids and names, and branch rules. It can also add any of the four generic steps, including a Run code step that points at a script you keep in Git or S3. Requests you can paste:

- "Run policyCheck and research in parallel, with a review gate named Evidence after them"
- "After triage, go to investigator if disposition equals escalate, and end the run if there is no claimId"
- "If nobody answers the Evidence gate within 24 hours, approve it"
- "Add a generic step at the end that writes the report as a Markdown file"

Answering a gate (approve, revise or deny) happens in the execution plane, on the run page. See [Human in the loop](Human_in_the_Loop.md).

More requests are on [Ask the Assistant](Ask_the_Assistant.md).

# Related pages

- [Build manually on the canvas](Canvas.md): drag agents onto stages
- [Agents](Agents.md): what each agent does
- [Human in the loop](Human_in_the_Loop.md): gates, questions and approvals in a run
- [Runtimes](Runtimes.md): task steps and other runtimes
