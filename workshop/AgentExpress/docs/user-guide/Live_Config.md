# Live config

**Live config** lets an admin change an agent's system prompt and model settings in the running execution plane, without a redeploy, as numbered versions with instant rollback. Use it to fix a prompt quickly, try a different model, or roll back a change that made things worse. A builder turns it on once. After that, admins make, activate and roll back versions in the execution plane.

# Turn it on

Live config is off by default. A builder turns it on in `workflow.json` and deploys once:

```json
"orchestrator": { "liveConfig": true },
"authorization": { "actions": { "admin": ["admins"] } }
```

In the control plane, it is **Live config** under **Settings → orchestrator**. The **Live config** page then appears in the execution plane's left pane for admins. Anyone else gets 403 from its routes.

# Open the Live config page

The Live config page before any version is saved:

![Live config page: Active says New runs use the config as deployed (version 0); Versions (0) lists v0 (active), As deployed; New version starts from the active version, with an Agent picker set to Fraud review, a System prompt box that replaces the prompt in this agent's code, and a Model field showing Deployed: (as deployed); Temperature below](images/ep-live-config.png)

The page has three parts:

- **Active:** which version new runs use.
- **Versions:** every saved version, with **Saved**, **By**, **Note**, **Changes** and **Actions**. Version 0 is the config as deployed.
- **New version:** the form for the next version. It starts from the active version.

# Make and activate a version

1. Under **New version**, pick the **Agent**.
1. Change what you need: **System prompt**, **Model**, **Temperature**, top P, max tokens or stop sequences. An empty prompt keeps the deployed one.
1. Repeat for other agents. **Changes against the active version** lists every change.
1. Add a **Note** saying why, for whoever reads the list later.
1. Click **Save as a new version**. The page says "Saved as version N. It is not active yet." Saving changes nothing that runs. **Discard changes** throws the edits away.
1. In **Versions**, click **Activate** on the version and confirm.

Runs that start from then on use it. A change reaches new runs within a few seconds.

# Roll back

- Activate an older version: click **Activate** on its row and confirm.
- Or click **Use the deployed config** to go back to version 0.

**View** on a version shows what it sets. Each save and each activation is in the Activity log, with who did it and the version before and after.

# What a run does with it

- A run uses the version that was active when it started, for every step: after a gate, on a re-run, and in a dedicated agent's own runtime.
- The run page shows **Config: config vN** when the run did not use the deployed config.
- If the versions cannot be read, the run starts as deployed. A run never fails because of this.

To compare two versions, run the same request under each, then compare the runs on the Observability page, or run a test suite after each activation. See [Refine and redeploy](Refine_and_Redeploy.md).

# Know its limits

- **Only six settings change:** system prompt, model, temperature, top P, max tokens and stop sequences. Tools, gates, guardrails, memory, Cedar policies, identity and the topology need a redeploy.
- **Agents in this deployment only.** It does not apply to A2A agents. A harness agent's prompt and model are set on the harness at deploy, so live config does not change them.
- **Models the runtimes may call.** A model must be a foundation model or an inference profile in this Region and account. Custom, imported and provisioned models are refused.
- **The deployed prompt is not shown.** It lives in the agent's code. A version's prompt replaces it in full.

Live config versions are kept in the execution plane, not in the build. To make a change part of the build, make it in the control plane too.

# Ask the Assistant

In the control plane, the AgentExpress Assistant can turn live config on and choose which groups hold the `admin` permission the page needs. It can also make a change for good in the build, so it survives the next deploy. Requests you can paste:

- "Turn on live config, and let the admins group manage it"
- "Make the fraud review prompt from live config version 3 part of the build: &lt;paste the prompt>"

What it can't do here:

- Save, activate or roll back a version. Admins do that on the **Live config** page in the execution plane.
- Read the live config versions. Paste the text you want to keep.
- Deploy. Live config appears once you choose **Deploy**.

All requests by feature: [Ask the Assistant](Ask_the_Assistant.md).

# Related pages

- [Agents](Agents.md): an agent's prompt and model in the control plane.
- [Models, routing and LiteLLM](Models.md): which models an agent may use.
- [Resume, restart and re-run](Resume_Restart_Rerun.md): re-run a step after a change.
- [Refine and redeploy](Refine_and_Redeploy.md): when to use live config and when to redeploy.
