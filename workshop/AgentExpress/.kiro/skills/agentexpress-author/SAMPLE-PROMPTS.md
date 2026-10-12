# Sample prompts

Open Kiro in this repository and paste one of these. The `agentexpress-author` skill
loads automatically because your request matches its description, then follows
[author-workflow.sop.md](author-workflow.sop.md): it confirms the topology with you,
writes the config, scaffolds the agents, writes the prompts, and runs the gates.

You can also just describe what you want in your own words — "build me a claims
settlement workflow with AgentExpress" — and it will ask for whatever it still needs in a
single prompt.

---

## 1. Rebuild this project's own workflow

Useful as a rehearsal: you can diff the result against the shipped
`orchestrator/app/workflow.json` and see exactly where it drifts.

```
Use the agentexpress-author skill to build this workflow.

use_case_description: Take a high-level AWS architecture request from a user,
research it from multiple sources in parallel, analyse the findings, produce a
recommendation, and assemble a final sectioned report. A human reviews the request
brief before research starts, reviews all research findings together, and reviews the
analysis and recommendation before the report is written.

domain_vocabulary: request-brief, research-finding, analysis, recommendation-package,
final-report

data_sources:
- internal reference docs as a Bedrock Knowledge Base corpus
- web search
- AWS documentation via a remote MCP server
- AWS pricing via the framework's built-in pricing Lambda
- product lifecycle dates via a REST API we have an OpenAPI spec for

review_points: a solutions architect approves the request brief; the same reviewer
approves the five research findings, deciding on each one separately; a lead approves
the analysis and recommendation together before the report is written

branching: intake decides — if the request has no clear objective, end the run; if it
has fewer than one key question, skip straight to the analysis stage

field_meanings: the pricing Lambda returns rows where `service` is the name,
`pricePerUnit` the value and `unit` the unit; the lifecycle API returns releases
newest-first, with `isMaintained` marking the maintenance track rather than current
support

permissions: leave unrestricted for now

quality: evaluations on the recommendation and the report, scored on demand

Notes: analysis and recommendation run as a sequence and are agents we do not operate,
so use A2A.
```

---

## 2. Insurance claims settlement

A different domain, exercising a Knowledge Base corpus and a customer-owned Lambda.
This is the one that was used to test the skill.

```
Use the agentexpress-author skill to build this workflow.

use_case_description: Settle household insurance claims. A handler submits the claim
reference and a free-text description of the damage. The workflow reads that into a
summary, checks the policy wording for coverage, prices the repair, and produces a
settlement decision a human approves.

domain_vocabulary: fnol-summary, coverage-finding, repair-estimate, settlement-decision

data_sources:
- household policy wordings as a Bedrock Knowledge Base corpus called `wordings`
- parts and labour rates from a Lambda we deploy ourselves. It returns rows where
  `part` is the item name, `unitPrice` the value, `uom` the unit and `ccy` the currency

review_points: an adjuster approves the FNOL summary; an adjuster approves each of the
two assessments separately; a manager approves the settlement before it is issued

branching: if the coverage check comes back with `coverageStatus` equal to
`not-covered`, end the run rather than pricing a repair we will not pay for

permissions: Cognito group `adjusters` may decide and re-run, `managers` may also
approve settlements, nobody but `admin` may delete a run

quality: score the settlement decision for faithfulness automatically when a run
completes; the guardrail must block medical detail about claimants
```

Produces four agents — `intake`, then `coverage_check` and `repair_pricing` in parallel
behind an Assessment gate, then `settle`.

**This brief answers all ten checklist items**, so the skill goes straight to proposing a
topology. A shorter one works too — it will just ask you for the gaps first.

---

## 3. Minimal — reasoning only, no external data

Touches two surfaces: `workflow.json` and the agent folders. No tools, no KB.

```
Use the agentexpress-author skill to build this workflow.

use_case_description: Review an architecture proposal against our internal engineering
standards and produce a decision record a principal engineer signs off.

domain_vocabulary: proposal-summary, standards-finding, decision-record

review_points: one gate before the decision record is written
```

---

## 4. Add one agent to an existing workflow

You do not have to start over to change something.

```
Use the agentexpress-author skill to add an agent.

I want a new agent called `regulatory_check` in the parallel research stage. It should
produce a `compliance-finding` and read our regulatory guidance, which I will add as a
new Knowledge Base corpus called `regulations`. It must be covered by the existing
review gate on that stage.
```

---

## What it will ask you

**You do not need a complete brief to start.** Say what you want in whatever detail you
have and the skill scores it against a ten-item readiness checklist, shows you the score,
and asks for the gaps in one grouped message. "Build me a claims system" is a perfectly
good opening — you will just answer more questions before it designs anything.

The ten items, and what each becomes:

| # | It needs to know | Becomes |
|---|---|---|
| 1 | What a user types to start a run | the topic field and its placeholder |
| 2 | What the workflow produces, in your words | `produces`, your asset types |
| 3 | The stages, in order | `steps` |
| 4 | Which stages don't depend on each other | `parallel` vs `sequence` |
| 5 | Where a human signs off, and who | the review gates |
| 6 | Any case where it should skip ahead or stop, and the field that decides | `branch` |
| 7 | What each agent must read, and how it is reached today | `tools` |
| 8 | For anything returning rows, which field means what | `rowFields` |
| 9 | Who may start, approve, re-run, delete | `authorization` |
| 10 | Which outputs need scoring, and anything the guardrail must block | evaluations, guardrail |

Items 9 and 10 have sensible defaults it will offer rather than block on — unrestricted
permissions, and evaluations on the final deliverable only. It will say which default it
took rather than assuming silently. Items 1 to 8 it will not guess, because each one
changes the shape of what gets built.

It will also tell you what you **don't** need to decide yet: model choice, token budgets,
which agents get their own runtime, long-term memory, and cost. All have working defaults.

## What it will NOT do

It stops and asks you to confirm the topology before writing any file, because every
later step derives from that shape.

It will not deploy. When the gates pass it tells you the next command —
`terraform apply` or `cdk deploy` — and stops there, because passing gates prove the
config is valid, not that it runs.
