# Code bundle and a coding agent

An exported bundle is plain JSON, so a coding agent can change it for you. The framework's code repository ships a Kiro skill, `agentexpress-author`, that knows the bundle's shapes and the framework's rules. You'll export the bundle, edit it with a coding agent, then import it back or write it into a checkout with `scaffold.py apply`. Use it for changes that are faster to describe than to click, or to keep a build under version control.

# Export the bundle

1. Fix every error on the build. **Bundle** is greyed out while the build has errors.
1. Click **Export → Bundle (workflow, prompts and code)**.
1. Save the file. Its name ends `.agentexpress.json`.

The bundle holds the workflow, each agent's prompt and output schema, and code tools written in the build. It holds no secrets and no knowledge base documents. See [Export, edit and import](Export_and_Import.md).

# Get the Kiro skill

The skill lives in the framework's code repository, in `.kiro/skills/agentexpress-author/`:

| File | What it holds |
|---|---|
| `SKILL.md` | What the skill is for, the files a customer owns, what it must not edit, and the checks to run |
| `author-workflow.sop.md` | The step-by-step procedure the agent follows, including a short path for editing a bundle exported from the control plane |
| `references/contract.md` | The legal shapes: tool types, step forms, agent keys, and the bundle format the control plane exports |
| `references/checklist.md` | The things the framework needs to know about a use case |
| `SAMPLE-PROMPTS.md` | Ready-to-paste prompts |

Open the repository in Kiro. The skill is picked up from `.kiro/`. A hook in `.kiro/hooks/` blocks writes outside the files a workflow author owns. Other coding agents, such as Claude Code, can use the same skill files.

# Edit the bundle with the agent

1. Put the bundle somewhere the agent can read it, for example in the repository folder.
1. Ask the agent, naming the skill and the file. For example: _Use the agentexpress-author skill to edit my-workflow.agentexpress.json: add a review gate after the intake step and make the letter agent write in plain English._
1. The agent reads the bundle and changes its `workflow`, `prompts` or `toolCode`.
1. Check the result before you use it.

What the skill does with a bundle:

- It keeps `{"library": …}` entries as they are.
- It adds no secrets.
- It saves the file as valid JSON.

To build a whole new workflow from a description instead, ask: _Use the agentexpress-author skill to build &lt;your use case>._ The skill asks for what is missing, confirms the stages, then writes the workflow.

# Import it back

The round trip for a build made in the control plane:

1. In the control plane, open the build.
1. Open **Project → Import a file…** and choose **Into this build, replacing it**.
1. Click **Choose file…** and pick the edited bundle. Confirm.

**Undo** takes the import back. The Problems list checks the build with the same rules as a deploy. Then click **Deploy** as usual. See [Deploy a build](Deploying_a_Build.md).

# Write it into a checkout

To deploy the edited bundle yourself instead, write it into a checkout of the framework with `scaffold.py`:

```bash
cd orchestrator
python3 scaffold.py apply my-workflow.agentexpress.json --dry-run   # preview, write nothing
python3 scaffold.py apply my-workflow.agentexpress.json --exact
```

What `apply` does:

- It writes `workflow.json` and a folder per new agent under `app/subagents/`, and code tools under `app/tools/_code/<key>/`.
- It never overwrites code you edited: an existing `agent.py` is kept, and `prompts.py` is rewritten only while it carries the control plane's marker line.
- It warns when the bundle came from a different framework release.
- `--exact` makes the tree mirror the bundle: prompts written by the control plane are regenerated, and agent, `app/tools/` and `kb_docs/` folders the bundle does not name are deleted.

A deploy from the control plane runs the same `apply --exact` on a fresh copy of the framework.

Knowledge base documents are not in the bundle. Put them under `kb_docs/<corpus>/` yourself.

## Check the tree

Prove the applied bundle with the IaC checks: `cdk synth` from `orchestrator/cdk`, or `terraform validate` from `orchestrator/terraform`. On this path `pytest` also checks the shipped sample's own conventions, so some of its tests can fail on a control plane bundle that deploys fine. Then deploy as in [Deploy from your laptop](Deploy_from_Your_Laptop.md).

# Ask the Assistant

The AgentExpress Assistant is not the coding agent on this page, and it can't export a bundle or run `scaffold.py`. Use **Export → Bundle (workflow, prompts and code)** and your own checkout.

- For a change that is faster to describe than to click, you don't have to leave the control plane: ask the Assistant in plain words. For example: "Add a review gate after the intake step and make the letter agent write in plain English".
- It also takes code you attach as a tool's or interceptor's files, as they are.

More requests are on [Ask the Assistant](Ask_the_Assistant.md).

# Related pages

- [Export, edit and import](Export_and_Import.md): the Export menu and Import a file….
- [workflow.json settings](Workflow_JSON_Reference.md): every key the agent may change.
- [Deploy from your laptop](Deploy_from_Your_Laptop.md): deploy the tree with CDK or Terraform.
- [Deploy a build](Deploying_a_Build.md): deploy from the control plane after importing.
