# Iris — Agentic AI Security & Governance Demo

An interactive demo that **deploys real AWS resources** to show how a layered set of
controls hardens an AI agent against prompt injection, data exfiltration, memory
poisoning, model misuse, goal hijacking, and confused-deputy attacks across agents.

You deploy the full secure agent from a local web console in **two steps** (a CDK
stack, then the AgentCore control plane), log in with Okta, run attack prompts, and
watch each control block what an unprotected agent allows. A **baseline** (deliberately
unprotected) agent is deployed alongside so you can see the before/after.

> ⚠️ **Deploys real AWS resources.** Point your environment credentials at a
> **sandbox / dev account**, never production. Everything created is tagged
> `demo:iris-security` and tracked in `state.json` for one-click cleanup.

---

## What gets deployed

The console deploys **one consolidated stack** (not five separate layers). Every
control below is stood up together and demonstrated through the per-layer nav tabs.

| Layer | Control | What it stops |
|-------|---------|---------------|
| **Baseline** | none (broad IAM, open egress) — the unprotected reference agent | Prompt injection exfiltrates the customer DB to the attacker collector. |
| **L1 · Network** | VPC + private subnets + VPC endpoints + DNS Firewall (default-deny egress allowlist) | The exfil POST is NXDOMAIN'd — nothing reaches the collector. |
| **L2 · Identity** | Okta OIDC login; `customer_id` bound from the verified JWT | The agent only ever acts for the logged-in user, whatever the prompt says. |
| **L3 · Tools** | AgentCore Gateway (MCP) + Cedar policy (default-deny) + RFC 8693 OBO + REQUEST interceptor | Typed tools only; identity is injected from the token; unauthorized tool calls are denied. |
| **L4 · Memory** | CMK-encrypted per-actor memory + self-managed **pre-write gate** | Ungrounded "pre-approved refund" premises are dropped before they poison long-term memory. |
| **L5 · Models** | IAM model allowlist + mandatory Bedrock Guardrail (via the `bedrock:GuardrailIdentifier` IAM condition; attached per-request, tags the user turn) | Off-allowlist models are `AccessDenied`; the guardrail blocks rule-override / prompt-attack inputs, and there is no ungoverned model path. |
| **L6 · Agents** | Goal fence (charter: scope · scale · value · sequence · drift · fail-closed) + A2A peer identity propagation | Out-of-charter/bulk actions are cancelled or the run is halted; the confused-deputy A2A read is blocked when the peer uses the verified caller. |
| **L7 · Observe & Contain** | Threat engine over the goal-fence blocks (rate / retry-after-block / critical) → incident → surgical kill (`StopRuntimeSession`) + CloudWatch metric/alarm | A rogue *instance* that keeps retrying after each block is detected as a pattern, attributed to its agent × session × user tuple, and the session is killed — while the rest of the fleet keeps running. |

Layers **2–6 use Okta** for identity (see [Okta setup](#okta-setup)). The baseline
and L1 need no IdP. **L7** reuses the L6 stack — it launches a disposable `iris_rogue`
runtime on demand and never touches the real agents.

---

## Repository layout

```
AgentSecruityDemo/
  README.md                  <- you are here
  app/
    docs/                     the three console-served "How it works" explainer pages
                              (obo-flow, memory-flow, goal-fence) + the Okta setup guide
                              (okta-setup.md) ARE committed; personal aids stay ignored
    cdk/                      THE consolidated CDK stack (Python) — infra + network +
                              endpoints + tools + memory + models + collector
    iris_goalfence/           standalone goal-fence engine (pure) + Strands adapter +
                              drift scorer + tests (the Layer 6 control)
    agent-baseline/           Strands agent — unprotected reference (before)
    agent-layer6/             Strands agent — goal-fenced superset (after)
    agent-peer/               A2A Orders peer (confused-deputy demo)
    server/
      app.py                  FastAPI control plane — Okta login, run, state tracker
      layer6_deploy.py        the two deploy panels: /api/deploy/stack + /agentcore
      layers/                 per-phase deploy helpers used by the deploy flow
      rogue_ops.py            Layer 7 control plane — rogue launch/detect/contain
      threat_engine.py        pure threat engine (rate / retry / critical → incident)
    web/index.html            console UI (deploy panels, login, flow diagrams, L7)
    data/                     seed data (customers, orders, shipments)
    scripts/
      reset-demo-state.sh     reset mutable demo state (s3 | memory | data | all)
    run.sh                    start the local console
    state.json                runtime resource tracker (for teardown)

(../okta-info.md)              concrete Okta values for THIS org (no secrets) —
                              at the repo root, one level above AgentSecruityDemo/
```

> `cdk/` is the single, consolidated CDK project the console uses. (The older per-layer
> CDK project and its per-layer deploy endpoints have been removed.)
>
> `app/docs/` ships the three explainer pages the console serves (`/docs/obo-flow`,
> `/docs/memory-flow`, `/docs/goal-fence`) plus `okta-setup.md` (generic guide, placeholders
> only). Anything else there (personal reference pages) is git-ignored via `app/docs/*`
> with per-file `!` exceptions.

---

## Prerequisites

- **AWS credentials** for a sandbox account in your environment
  (`aws sts get-caller-identity` must work). Region defaults to `us-east-1`
  (override with `AWS_REGION`).
- **Python 3.10+**.
- **Node.js** — for the AWS CDK CLI (the CDK *stacks* are Python; the CLI is Node).
  `run.sh` installs the CDK CLI if missing.
- **Docker** — the agent/peer/baseline images are built and pushed to ECR.
- One-time per account/region: `npx cdk bootstrap` (run inside `app/cdk`).
- An **Okta org** for Layers 2–6 — see [Okta setup](#okta-setup).

---

## Environment variables

### AWS

The app uses your ambient AWS credentials (the same ones `aws` and `boto3` read).
Set them however you normally do — env vars, a profile, or SSO:

| Variable | Meaning |
|----------|---------|
| `AWS_REGION` | Target region (default `us-east-1`). |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN` | Static/temporary keys. |
| `AWS_PROFILE` | Named profile in `~/.aws/config` (alternative to keys). |

The console's top bar shows the resolved **account id** — confirm it's the sandbox
before deploying.

### Okta

Non-secret IDs default to the demo org baked into `server/app.py` (the `OKTA` dict)
and mirrored in [`../okta-info.md`](../okta-info.md). **Secrets must come from
environment variables — they are never stored in any file.** If you set up your own
Okta org, override the IDs too.

| Variable | Meaning | Default |
|----------|---------|---------|
| `OKTA_DOMAIN` | Your Okta org URL | `https://<your-okta-org>.okta.com` |
| `OKTA_AGENT_AUTH_SERVER_ID` | PROVIDER auth server (inbound token, `aud=iris-agent`) | baked-in |
| `OKTA_GATEWAY_AUTH_SERVER_ID` | RESOURCE auth server (OBO token, `aud=iris-gateway`) | baked-in |
| `OKTA_LOGIN_CLIENT_ID` | `iris-login` app client id | baked-in |
| **`OKTA_LOGIN_CLIENT_SECRET`** | `iris-login` secret — **required to log in** | — |
| `OKTA_SUPPORT_DELEGATE_CLIENT_ID` | support delegate app client id | baked-in |
| **`OKTA_SUPPORT_DELEGATE_CLIENT_SECRET`** | support delegate secret (OBO) | — |
| `OKTA_ADMIN_DELEGATE_CLIENT_ID` | admin delegate app client id | baked-in |
| **`OKTA_ADMIN_DELEGATE_CLIENT_SECRET`** | admin delegate secret (OBO) | — |
| `OKTA_REDIRECT_URI` | OAuth redirect | `http://localhost:8000/callback` |

Get each secret from Okta: **Applications → [app] → General → CLIENT SECRETS →
eye/copy icon**. Set `http://localhost:8000/callback` as a Sign-in redirect URI on
the `iris-login` app.

```bash
export OKTA_LOGIN_CLIENT_SECRET='<iris-login secret>'
export OKTA_SUPPORT_DELEGATE_CLIENT_SECRET='<iris-support-delegate secret>'
export OKTA_ADMIN_DELEGATE_CLIENT_SECRET='<iris-admin-delegate secret>'
```

---

## Okta setup

Layers 2–6 authenticate users against Okta, and (L3+) perform a real RFC 8693
On-Behalf-Of token exchange through AgentCore Identity.

1. **Follow the step-by-step guide:** [`app/docs/okta-setup.md`](app/docs/okta-setup.md).
   It creates the two custom authorization servers (`iris-agent`, `iris-gateway`),
   the login app (`iris-login`), the two delegate apps (`iris-support-delegate`,
   `iris-admin-delegate`), demo users (C-1001…C-1003, A-001), scopes, claims, and the
   Trusted Server relationship.
2. **This org's concrete non-secret values** are in [`../okta-info.md`](../okta-info.md).
   For a new org, update those and/or override via the env vars above.
3. **See the OBO exchange live** once deployed: in the console, click the account badge
   (top-right) → **"How it works · OAuth & OBO token exchange"**. It logs you in for real
   and steps through JWT → WorkloadAccessToken → RFC 8693 OBO exchange → Gateway, using
   live config + your real token claims (inbound `customer_id=C-1001` → OBO token
   `aud=iris-gateway`, `customer_id` preserved).

---

## Deploy & run

```bash
cd AgentSecruityDemo/app

# 1) Okta secrets (see above)
export OKTA_LOGIN_CLIENT_SECRET='...'
export OKTA_SUPPORT_DELEGATE_CLIENT_SECRET='...'
export OKTA_ADMIN_DELEGATE_CLIENT_SECRET='...'

# 2) one-time per account/region
( cd cdk && npx cdk bootstrap )

# 3) start the console
./run.sh            # installs deps, serves http://localhost:8000
```

Then, on the **Deploy** screen (http://localhost:8000/#deploy), run the two panels
in order — each has **Deploy** and **View code** buttons:

1. **1 · CDK stack** — all CloudFormation: VPC + Aurora + ECR + roles + tool Lambdas +
   memory gate + model allowlist + guardrail + the shipment service + the attacker
   collector. Seeds Aurora and builds the DNS-Firewall allowlist.
2. **2 · AgentCore deploy** (enabled once the stack is up) — builds the agent images
   and creates the runtimes: the **baseline** (PUBLIC, unprotected), the **Gateway +
   Cedar + OBO**, **Memory**, the **A2A Orders peer**, and the **one consolidated
   goal-fenced runtime**.

Then explore each nav tab (Baseline, Layer 1 … Full Stack). **Log in with Okta**
(e.g. `ada.lovelace@example.com`) — one login carries across every layer tab. Pick a
prompt and Invoke; each tab's flow diagram shows the controls firing live (click any
block for details). The **Full Stack** tab is the complete secure agent, with two
per-invoke toggles: the **goal fence** ON/OFF and the A2A peer's
**verified-caller / trust-argument** mode (the confused-deputy demo).

---

## Cleanup

From the console: the **Delete resources** screen — **Delete All Resources** (everything),
or **Delete AgentCore only** (runtimes/gateway/policy/OBO/memory, keeping the CDK stack so
you can re-run just the AgentCore deploy). Both stream a live teardown log.

Teardown order: AgentCore runtimes / gateways / policy engines / credential providers
/ memory → CDK stacks (`cdk destroy`) → sweep orphan ENIs → clear `state.json`.

Deletion is driven **entirely by `state.json`** — the record this app writes when it
creates each resource. AgentCore resources are deleted only by their tracked ids, and
infra is deleted by its tracked CloudFormation stack names. The teardown never lists the
account and never deletes by name, so it can only ever remove resources **this app
created** — a resource from another app that happens to share the account (or the
`iris`/`iris_` prefix) is never touched. If `state.json` drifts and leaves an orphan,
delete it by hand rather than having the app guess.

> Okta objects (auth servers, apps, users) are **not** deleted by teardown — they
> live in your Okta org and are reused across deploys.

### Reset demo state (no teardown)

Between demo runs you often want a **clean slate without redeploying**. The
`app/scripts/reset-demo-state.sh` script resets mutable state only — it never deletes
agents, runtimes, gateways, or infra:

```bash
cd app
./scripts/reset-demo-state.sh <s3|memory|data|all>
# region defaults to us-east-1; override with AWS_REGION=…
```

| target | what it resets |
|--------|----------------|
| `s3` | Empties the exfil/collector bucket (objects + all versions/delete-markers). |
| `memory` | Deletes **all** AgentCore memory — short-term events + long-term `/facts/` records — for **every actor**, across every deployed memory (L4/L5/L6). |
| `data` | Drops + reseeds the Aurora tables (customers/orders/shipments/refunds) from `data/*.json`. |
| `all` | s3 → memory → data. |

Resource ids are resolved from CloudFormation outputs + `state.json` (nothing
hardcoded). Requires valid AWS credentials (fails fast if expired).

---

## Security notes

- Nothing deploys or deletes on its own — only when you click a button or run a script.
- **Secrets** (Okta client secrets) are read from env vars only and never written to
  `state.json`, logs, or committed files. Rotate them if they were ever pasted into a
  shell/transcript.
- `state.json`, `logs/`, `cdk-outputs.json`, `cdk.out*/`, `.env`, and local aids
  (`demo-script/`, the `redeploy_layer*.py` operator scripts, personal `app/docs/` pages)
  are runtime artifacts or presenter aids — kept out of commits (see `.gitignore`).
- The **collector** (attacker Lambda + S3) and the **baseline** agent are intentionally
  insecure — they exist only to demonstrate the attack the other controls stop. Deploy
  to a sandbox.
- Deploy targets whatever account your env creds resolve to. Use a sandbox.
```