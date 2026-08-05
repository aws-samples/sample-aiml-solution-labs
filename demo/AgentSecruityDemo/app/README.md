# Iris Security Demo — live control plane

An interactive demo that **actually deploys** the layered-security story into an AWS
account and tears it back down. This is the **baseline (pre-Layer-1)** phase: it stands
up the deliberately-unprotected starting point, which later layers harden.

> ⚠️ **Deploys real AWS resources.** Point your environment credentials at a **sandbox /
> dev account**, never production. Everything created is tagged `demo:iris-security` and
> tracked in `state.json` for one-click cleanup.

## What the baseline creates
- **DynamoDB** table `iris-demo-customers` seeded with 1000 **fake** customer records (no real data).
- **IAM exec role** `iris-demo-exec-role` — intentionally broad (reads the whole table). Layers scope this later.
- **AgentCore Runtime** running the **Iris** Strands agent with a table-read tool and an open
  `http_request` tool — **no VPC controls** yet.

## Layout
```
app/
  cdk/       aws-cdk (Python) — iris_demo/baseline_stack.py (table + role + runtime), collector_stack.py
  agent/     iris_agent.py — Strands agent (read_customers + http_request), Dockerized
  server/    FastAPI control plane — deploy / destroy / resources, state.json tracker
  scripts/   destroy-all.sh (idempotent), build-agent-image.sh
  web/        console UI (deploy button, resource tracker, Delete-all)
  run.sh     start it all locally
```

## Prerequisites
- AWS credentials in the environment for a **sandbox** account (`aws sts get-caller-identity` works)
- Python 3.10+ (stacks + server + agent are all Python)
- The AWS CDK **CLI** (Node-based): `npm install -g aws-cdk` — the CLI is Node even though the stacks are Python; `run.sh` installs it if missing. No Docker needed (agent uses CodeZip).
- One-time per account/region: `npx cdk bootstrap` (run inside `app/cdk`)

## Run
```bash
cd demo-code/app
./run.sh                 # installs deps, starts the server on http://localhost:8000
```
Then in the browser:
1. **Baseline Setup → Deploy baseline** — streams the CDK deploy log, creates the table + role,
   registers the AgentCore Runtime, and fills the **Created resources** tracker.
2. **View CDK & agent code** — shows exactly what is being deployed.
3. **Delete all** (left nav) — idempotent teardown of everything created.

### Build/push the agent image (optional, for a real Runtime)
The Runtime needs a container image. Build + push, then re-deploy:
```bash
export IRIS_IMAGE_URI=$(./scripts/build-agent-image.sh | tail -1 | cut -d= -f2)
```
If the image URI isn't set, the backend still deploys the table + role and records
the runtime step as skipped — the demo stays consistent and teardown still works.

## Cleanup (also works without the server)
```bash
./scripts/destroy-all.sh    # safe to run any time; no error if nothing exists
```
Order: AgentCore runtimes → `cdk destroy --all --force` → sweep the well-known table/role → clear `state.json`.

## Safety notes
- Nothing deploys or deletes on its own — only when you click the button or run a script.
- Deploy targets whatever account your env creds resolve to; the UI shows that account id up top — **check it before deploying**.
- Teardown is idempotent and best-effort: individual "already gone" failures are logged, not fatal.
