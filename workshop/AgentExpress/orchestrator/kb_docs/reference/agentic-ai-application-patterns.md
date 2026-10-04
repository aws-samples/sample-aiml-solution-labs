# Agentic AI Application Patterns (Sample Reference)

This reference document is sample content for the Knowledge Base used by the
`knowledge_research` (RAG) agent. Replace it with your own reference material.

It exists because the other two sample documents cover serverless data pipelines
and well-architected principles, so a demo asking about agentic AI found nothing
relevant and the agent — correctly — reported an empty retrieval. Keep at least one
document that matches whatever you plan to demo.

## What makes an application "agentic"

A standard LLM application maps one input to one output: a prompt goes in, text
comes out, and the control flow is fixed by the code around it.

An agentic application adds three things:

- **Goal-directed control flow.** The system decides what to do next, rather than
  following a path the developer hardcoded.
- **Tool use.** It can act on the world — query a database, call an API, retrieve a
  document — instead of only producing text.
- **State across steps.** It carries context between actions, so a later step can
  build on an earlier one.

An orchestrated multi-agent system adds a fourth: several specialised agents with
distinct prompts, data sources and permissions, coordinated by a topology.

## Core components

| Component | Role |
|---|---|
| Model | The reasoning engine. Different steps may warrant different models. |
| Prompt / instructions | The agent's identity: what it does and what it must not do. |
| Tools | How the agent reaches data or takes action. |
| Memory | Short-term (this run's state) and long-term (recall across runs). |
| Orchestration | The topology: which agents run, in what order, and where they join. |
| Guardrails | Content safety on input and output. |
| Authorization | Which tools an agent may call, and which humans may act on a run. |
| Observability | Traces, token counts, cost and latency per step. |
| Evaluation | Scoring output quality, usually with a model as judge. |

## Orchestration patterns

- **Sequential** — each agent consumes the previous agent's output. Use when a step
  genuinely depends on the one before it.
- **Parallel (fan-out / fan-in)** — independent agents run concurrently and join.
  Use for research from several sources. Members must not depend on each other, or
  the result becomes order-dependent.
- **Router** — a classifier picks which agent or path handles the request.
- **Hierarchical (supervisor / subagent)** — a coordinator decomposes a goal and
  delegates to specialists.
- **Human-in-the-loop gate** — the run pauses for approval, revision or denial
  before continuing. Requires durable state so a pause can outlive the process.

## Planning and reasoning

- **Structured output** — constrain the model to a schema so the next step can
  consume it programmatically instead of parsing prose.
- **Decomposition** — split a goal into named subtasks, each with its own prompt
  and its own success condition.
- **Tool-use loop** — the model proposes a tool call, the framework executes it and
  returns the result, and the loop continues until the model answers.
- **Evidence classification** — label every claim by provenance (sourced fact,
  calculation, assumption, interpretation) so a reviewer can see which parts rest
  on retrieved evidence.
- **Reflection** — a second pass critiques the first. Costs another model call, so
  reserve it for steps where quality matters more than latency.

## Failure modes and mitigations

| Failure mode | Mitigation |
|---|---|
| Fabricated facts or citations | Ground every claim in retrieved evidence and verify each citation against what the model was actually shown. Drop what cannot be verified. |
| Silent truncation of evidence | Cap evidence explicitly and label the cut, so an agent can report the gap instead of reasoning from a fragment. |
| Prompt injection from retrieved content | Treat all tool output as untrusted. Enforce tool permissions server-side, not by prompt wording. |
| Over-broad tool access | Default-deny. Permit only declared tools, and restrict arguments where the values are known. |
| Truncated model output | Size the output budget per agent. A step emitting a large structured payload needs more than one emitting a sentence. |
| Runaway loops or cost | Cap iterations, meter every call, and set an explicit budget. |
| Non-determinism in review | Version each agent's output and record the feedback that triggered a re-run. |
| Stale or irrelevant memory | Namespace long-term memory per agent and per subject, or recalled context bleeds across unrelated topics. |

## Practices worth adopting early

- Make the workflow **configuration**, not code, so the topology can change without
  a rewrite.
- **Fail loudly.** If a tool or model call fails, stop and attach the reason to the
  step that failed. A plausible-looking answer built on no evidence is worse than
  an error.
- Separate the **config plane** (topology, permissions, tool wiring — deterministic,
  testable) from the **data plane** (prompts and model output — judged, not
  asserted).
- **Meter everything** from the first day. Retro-fitting cost and latency capture is
  far harder than building it in.
- Put a **human gate** wherever a wrong answer is expensive, and make the gate able
  to re-run only the part that was wrong.
