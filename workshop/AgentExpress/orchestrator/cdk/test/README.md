# Config-plane tests (CDK path)

```bash
cd orchestrator/cdk
npm install
npm test
```

269 tests across six files. No AWS credentials: `Template.fromStack` renders the
template without staging the cloud assembly, which is the step that would build the
runtime's Docker image. Synthesizing the stack does build the UI bundle
(`npm ci && npm run build` in `../web`), locally when `npm` is on the PATH and in a
`node:22-alpine` container otherwise, so `stack.test.ts` and `named.test.ts` take a few
minutes. `named.test.ts` passes with `CDK_DOCKER=/nonexistent/builder`; with no local
`npm`, set a real engine (`CDK_DOCKER=finch` works).

This is the TypeScript half of the config-plane suite. The Python half is
[`orchestrator/tests/`](../../tests/README.md), which covers the runtime; this covers the
IaC.

## The files

| File | Tests | Covers |
|---|---|---|
| `config-plane.test.ts` | 77 | the pure projections and validators: `validateTools`, `validateWorkflow` (which runs `validateRuntimes`), `validateBranches`, `authzGroups`, `buildGuardrail`, `cedarStatement`, and the `openapi` / `lambda` tool shapes |
| `tool-plane.test.ts` | 87 | the ToolPlane construct with tools blocks the sample does not ship: one Gateway target per `tools` entry (`websearch`, `lambda` incl. cross-account and multi-tool, `openapi`, `apigateway` with an inline schema and OAuth client credentials, a tool written in the build), the Knowledge Base config and document sources (the digest that lets an immutable property be replaced rather than fail the deploy), custom Cedar policies, and a tool × policy-engine matrix |
| `stack.test.ts` | 68 | the synthesized template: Cognito groups and self-signup, the route set and its authorizer, the BFF deployment package and environment, the tool plane, `idp=none`, a2a agents and the stand-in, dedicated-agent roles, the Builder control plane, connected accounts, image agents, long-term memory strategies and custom evaluators, that inline Lambda code compiles, and the baseline hardening (PITR, TLS-only buckets, API logs and throttle, security headers, Cognito policy, no credential in a runtime's environment) |
| `named.test.ts` | 13 | the build-level named blocks (`guardrails`, `memories`, `evaluators`, `identities`, `policies`) and `orchestrator.gatewayIdentity`, synthesized |
| `regional-model.test.ts` | 12 | model ids made regional (inference-profile prefixes) for the deploy region |
| `nag.test.ts` | 3 | cdk-nag's AwsSolutions pack over a console and a build stack: no finding that `lib/nag.ts` has not acknowledged with a reason |
| `parity.test.ts` | 22 | Terraform ↔ CDK agreement: the API route list, the framework vocabulary having one home, web-search domain filtering, the `kb` tool's retrieval settings, the environment and IAM grants, Cedar policy names, and the shipped `workflow.json` being read the same way by both |

## Why parity.test.ts exists

The project promises both IaC paths deploy the same thing from the same `workflow.json`, and
nothing structural enforces that — they are two independent implementations of one
projection. They have drifted more than once, and the failure is silent: a CDK deployment
that lost the data-source chips, the Evaluate buttons and the in-app assistant while
Terraform kept them, and a Terraform deployment that granted only
`lambda:InvokeFunctionUrl` where CDK granted both invoke actions, so every A2A stage
returned 403. Both were found by comparing two live deployments, which is not a repeatable
way to find that class of bug.

So `parity.test.ts` reads `terraform/*.tf` as text and compares shapes. Regex over HCL is
crude; the mitigation is that every extraction asserts it found something before comparing,
so a regex that stops matching fails loudly instead of passing vacuously. Anchor on
declarations rather than bare names, too — prose elsewhere in a file can match first and
silently move the window.

Closed value sets are no longer duplicated: they live in `app/vocabulary.json`, which
`bff/authz.py`, `terraform/*.tf` (`jsondecode(file(...))`) and `lib/vocabulary.ts` all
read. "the framework vocabulary has ONE home" asserts each plane reaches the file and
that no plane has written a set out as a literal again — the eleven RBAC action names
among them, which the BFF enforces and both IaC paths need at plan/synth time to reject
a typo'd action key.

## Verified by mutation

Each of these was reintroduced and the suite confirmed to fail, then reverted:

| Mutation | Tests that caught it |
|---|---|
| emit `mcp`/`rag` instead of `tool`/`corpus` | 2 |
| drop `evalAgents` + `chatbot` from the projection | 5 |
| drop `authorization` from the projection | 3 |
| remove `/api/me` from the CDK route list only | 3 |
| typo the RBAC action list in TypeScript only | 19 |
| re-enable Cognito self-signup | 1 |
| stop creating the Cognito groups | 2 |
| permit web search at target level instead of by name | 2 |

The last is a live `ToolDenied`: a target-level Cedar permit does not authorize the managed
web-search connector's tool, so it must be permitted by name. An MCP target is the opposite
— its tool names are unknown at deploy time, so it must stay target-level.

## Adding a test

Projection and validator assertions go in `config-plane.test.ts`, anything comparing against
the HCL in `parity.test.ts`, anything needing the rendered template in `stack.test.ts`.
`stack.test.ts` synthesizes once in `beforeAll` and shares the `Template` — keep it that
way, since synth is the only slow part.
