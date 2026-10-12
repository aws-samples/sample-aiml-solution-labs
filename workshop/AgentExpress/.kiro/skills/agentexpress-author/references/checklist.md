# Readiness checklist

What the framework needs from a customer before a workflow can be built, and what each
item turns into. Ten items. Every one of them ends up in `workflow.json` or in an agent
folder — nothing here is asked for its own sake.

Use this in step 0 of the SOP: score the customer's description against it, ask once for
the gaps, and restate the whole set back before designing anything.

| # | Item | Becomes |
|---|---|---|
| 1 | **The request.** What does a user type to start a run? | `ui.defaultTopic`, `ui.topicPlaceholder` |
| 2 | **The deliverables.** What does the workflow produce, and what do you call those things? | `produces` per agent, the asset types in `_shared/contracts/` |
| 3 | **The stages.** What has to happen, in order? | `steps` |
| 4 | **Concurrency.** Which of those don't depend on each other? | `parallel` vs `sequence` |
| 5 | **Human approval.** Where must a person sign off, and what are they deciding? | `hitl`, `gateId`, `gateName` |
| 6 | **Conditional paths.** Is there a case where the workflow should skip ahead or stop? What output field decides it? | `branch.when`, `goto` |
| 7 | **Data sources.** What must each agent read, and how is it reached today? | `tools`, and which of the five types |
| 8 | **Field meanings.** For anything returning rows, which field is the name, the value, the unit? | `rowFields` |
| 9 | **Permissions.** Who may start a run, approve a gate, re-run, delete? Which identity provider? | `authorization`, `groupsClaim`, the `idp` variable |
| 10 | **Quality and safety.** Which outputs need scoring? Anything the guardrail must block? | `agentcore.evaluations`, `guardrail` |

## How to judge each item

An item is **answered** when the design can be written from it without a guess. Prose that
names a goal but no mechanism is not an answer.

| Item | Not enough | Enough |
|---|---|---|
| 1 | "users submit a claim" | "the FNOL reference and a free-text description of the damage" |
| 2 | "a report" | "fnol-summary, coverage-finding, repair-estimate, settlement-decision" |
| 3 | "it researches and decides" | "read the FNOL → check coverage and price the repair → issue a settlement" |
| 4 | "in parallel where possible" | "coverage and pricing don't read each other, so both" |
| 5 | "a human reviews it" | "an adjuster approves each assessment; a manager approves the settlement" |
| 6 | "sometimes we reject early" | "if `coverageStatus` equals `not-covered`, go to END" |
| 7 | "our policy documents" | "PDFs in SharePoint, which we'll export to a KB corpus"; "rates from an internal API, no MCP server, so a Lambda we deploy" |
| 8 | "it returns prices" | "`part` is the name, `unitPrice` the value, `uom` the unit" |
| 9 | "only adjusters" | "Cognito group `adjusters` may decide; `ops` may re-run; nobody but `admin` may delete" |
| 10 | "it should be accurate" | "score the settlement for faithfulness; the guardrail must block medical detail" |

## Defaults to offer rather than block on

Items 9 and 10 stall a session more often than they inform it, because most customers have
not decided yet and do not need to in order to see their workflow run. Offer a default,
**state it out loud**, and move on:

- **9** — "no restrictions: anyone who can log in can do anything. Add groups later; the
  backend enforces them the moment you do." The `idp` choice is a deploy-time variable, not
  a workflow decision, so it can wait.
- **10** — "evaluations on the final deliverable only, scored on demand rather than
  automatically. The guardrail stays as shipped."

Never default items 1–8 silently. Each one changes the shape of what gets built, so a wrong
guess there is work the customer has to unpick rather than a setting they flip.

## Things customers volunteer that are NOT needed yet

Say so, so they don't spend the session on them:

- **Model choice and token budgets** — every agent inherits the deployment default;
  `model` and `maxTokens` are per-agent overrides added when something proves too long or
  too weak.
- **Which agents run in their own AgentCore Runtime** — `runtime: "dedicated"` is a scaling
  and isolation decision, and `main` is right until there is evidence otherwise.
- **Long-term memory** — `subjectId` scopes it, and it earns its place once there are
  repeat runs on the same customer or project to recall.
- **Cost** — visible per call the moment the thing runs. Nothing to configure.

## The one question worth asking that is not in the config

**"When this goes wrong, who finds out and how?"**

It has no key. But the answer decides where the gates go, whether grounding warnings need
to block or merely flag, and whether the final deliverable should be scored automatically
rather than on demand. A customer who says "the settlement is sent to the policyholder
automatically" needs a gate in a different place from one who says "an adjuster reads
everything anyway".
