# Human in the loop

AgentExpress lets people into a run at set points. A **review gate** (HITL, human in the loop) pauses the run after a step until someone approves, revises or denies it. An agent can **ask a person** a question in the middle of its step. A tool can make each call **wait for approval**. **Branch rules** let a step's output choose what runs next. If you set up a build, you'll add these in the control plane. If you review runs, you'll see what each box asks of you in the execution plane.

# Add a review gate

A review gate pauses a run after a step and waits for a person. Use one wherever a person has to sign off before the work goes on: after an intake brief, after research, before a letter goes out.

1. On the canvas, select the stage or the agent the gate should follow.
1. In the settings panel, turn on **HITL gate**.
1. Optionally give it a **Gate id** and a **Gate name**. The name is what reviewers see.
1. Optionally expand **How the gate decides** to set who decides, where it is answered and a timeout.

In `workflow.json` a gate is `"hitl": true` on a step:

```json
"steps": [
  { "agent": "intake", "hitl": true },
  { "parallel": ["policyCheck", "research"], "hitl": true, "gateId": "evidence", "gateName": "Evidence" },
  { "agent": "report" }
]
```

| Gate on | One decision for | Revise re-runs |
|---|---|---|
| One agent | That agent | That agent |
| A sequence | The whole chain | The whole chain, from its first agent |
| A parallel stage | Each agent separately | Only the agents the reviewer marked Revise |

At a gate, the run's status turns to **Awaiting review**, the step badge turns amber, and **Awaiting** names the gate. The run is saved in a checkpoint, so you can close the tab and come back later.

# Approve, revise or deny

When a run waits at a gate, the run page shows a **Review required** box above the details. Expand each agent's output to read it, then decide.

- **Approve** continues the run. A note is optional.
- **Revise** re-runs the step with your note as its feedback. Write the note in **Reviewer note** first: **Revise** is disabled without one.
- **Deny** stops the run. It ends as **Denied**, and the run page quotes your note.

On a parallel stage, each agent has its own **Approve** and **Revise** switch. Set **Revise** on any agent to send back, write **What should it change?** for it, and click **Submit decisions**. **Deny run** stops the whole run.

## Who can decide

- You need a group granted the `decision` action. Without it the buttons are greyed out. See [Users, groups and RBAC](Users_Groups_and_RBAC.md).
- Runs are private, so only the run's owner decides its gates. A run a trigger started as the service is decided by the trigger's `approvers` groups.
- An admin can read another user's run but not decide it.
- The Run Assistant can approve, revise or deny for you, under the same permission.

Every decision is a line on the run's timeline, with the reviewer's note, and an event in the Activity log.

# Let a gate decide for itself

A gate does not have to wait for a person every time. In the control plane, open **How the gate decides** under the gate.

| Who decides | `mode` | What happens |
|---|---|---|
| Always: HITL decides | `always` (default) | A person decides every time |
| Only when a rule matches | `threshold` | A person decides only when one of the **Rules that call for HITL** matches the step's output. No match: the gate approves itself. |
| Approves itself (logged) | `auto` | The gate approves itself and records that it did |

For example, ask a person only when the risk score is 80 or more, and deny after a day:

```json
{ "agent": "triage", "hitl": {
    "mode": "threshold",
    "when": [{ "field": "riskScore", "gte": 80 }],
    "timeout": { "after": "24h", "action": "deny" } } }
```

**Timeout** decides a gate nobody decided in time: give a length (30m, 24h, 2d) and pick **Deny** or **Approve**. Leave it empty to wait for ever. The execution plane checks for timed-out gates every five minutes, so a decision can come up to five minutes late.

# Answer a gate from another system

A gate can also be answered by an event, so Slack, ServiceNow or your own code can decide it.

1. Under **How the gate decides**, set **Where it is answered** to **In the app, or by EventBridge event** (`"approval": "event"`).
1. Deploy.

When the run reaches the gate, the execution plane puts an "AgentExpress Approval Requested" event on the account's default EventBridge bus. Your system answers with an "AgentExpress Approval Decision" event whose `detail` names the `app`, the `session` (the run ID), the `gate`, the `decision` (approve, revise or deny), a `comment` and `by` (who decided). The gate can still be decided in the execution plane.

> [!WARNING]
> Whoever may put events on the account's default bus may decide these gates. Keep that in mind when you grant access to the bus.

# When an agent asks a person

An agent can stop in the middle of its step, ask a question, and carry on with the answer. Use it when an agent may need a fact only a person has.

- Turn it on with `agents.<id>.askPerson` set to `true`, or `{"maxQuestions": 3, "timeout": "24h"}`. The agent must run in the orchestrator (`runtime: "main"`) with `toolMode: "model"`.
- The run page shows a box titled "&lt;agent> asks", with the question and **Your answer**. **Answer** sends it; **Decline** tells the agent nobody answered, and it carries on with what it has.
- At most `maxQuestions` per step (1 to 10, default 3). With `timeout`, an unanswered question lapses and the agent carries on.
- Questions are answered on the run page only, by people who hold `decision`. Events and the Run Assistant cannot answer them.
- When the run resumes, the model turns and tool calls made before the question are replayed from the checkpoint, not made or billed again.

# When a tool call waits for approval

A tool can make every call wait for a person, who sees the exact arguments before it runs. Use it on tools that change something: issuing a refund, sending mail, writing a record.

- Set `tools.<name>.approval` to `"person"`, or `"event"` to also allow an EventBridge answer. `approvalTimeout`, for example `{"after": "1h", "action": "deny"}`, decides a call nobody decided in time.
- It works on `mcp`, `openapi`, `lambda`, `apigateway` and `smithy` tools. A `dedicated` agent cannot wait, so binding it to such a tool is refused.
- A box titled "Approve a tool call: &lt;agent>" shows the **Tool**, its **Arguments** and an optional **Note**. **Approve** makes the call. **Deny** tells the agent the call was refused, and it carries on.
- A run started unattended refuses a call that needs approval.

# Route with branch rules

A branch lets a step's own output pick what runs next: send a high-risk case to a deeper review, or end the run when a request cannot be acted on. In the control plane, use **Branch** in the step's settings panel.

```json
{ "agent": "triage",
  "branch": {
    "when": [
      { "field": "disposition", "equals": "escalate", "goto": "investigator" },
      { "field": "claimId", "exists": false, "goto": "END" }
    ],
    "default": "adjuster" } }
```

- Rules are tried in order and the first match wins. `field` is a dot path into the step's JSON output. `goto` names a later step, or `END`.
- Operators: `equals`, `notEquals`, `in`, `contains`, `exists`, `gt`, `gte`, `lt`, `lte`. Text comparisons ignore case and surrounding spaces.
- A branch goes on a single-agent or sequence step, not on a parallel step or the last step. With a gate on the same step, the person approves first and the branch then reads the approved output.
- In a run you see a **Branch** pill on the graph, a timeline line naming the rule that matched, and **Skipped** on the steps it bypassed.

# Ask the Assistant

In the control plane, the AgentExpress Assistant can add and change review gates, with their rules, timeouts and EventBridge answers. It can also let an agent ask a person, make a tool's calls wait for approval, add branch rules, and set who may decide. Requests you can paste:

- "Put a review gate after the evidence stage, named Evidence"
- "At the triage gate, ask a person only when riskScore is 80 or more, and deny after 24 hours"
- "Let the intake agent ask a person up to 2 questions, and carry on after 1 hour with no answer"
- "Make every call to the refunds tool wait for a person's approval, and deny it after an hour"

With **Review changes** on **Auto** or **Always**, a second model checks approval settings and new stages before they're saved.

What it can't do here:

- Approve, revise or deny a gate, answer an agent's question, or decide a tool call. Do that on the run page in the execution plane. The Run Assistant can decide gates for you, but not answer questions.
- Deploy. Gates and questions work in runs once you choose **Deploy**.

All requests by feature: [Ask the Assistant](Ask_the_Assistant.md).

# Related pages

- [Stages and steps](Stages_and_Steps.md): where gates and branches are set on the canvas.
- [The run page and its tabs](Run_Page.md): where the review boxes appear.
- [Resume, restart and re-run](Resume_Restart_Rerun.md): what happens after a decision.
- [Chat with the Run Assistant](Run_Assistant.md): decide a gate by chat.
- [Users, groups and RBAC](Users_Groups_and_RBAC.md): the decision permission.
