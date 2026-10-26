# Settings tab

The **Settings** tab holds what no other tab edits: the execution plane's text, the orchestrator's runtime settings for the whole build, the build's own storage, who may do what in the execution plane, and where runs are exported. Each block is a form generated from the framework's key list, with a description under every field. Use it to rebrand the execution plane, set the default model and limits, or open and close actions to groups.

# Read the Settings tab

The Settings tab, with the **ui**, **orchestrator**, **authorization** and **Export** sections:

![The Settings tab: the ui block with Title, Heading and the placeholders, the orchestrator block with Default model, Runtime invoke, Limits, Live config, Models, A2A invoke, Insights, Model rates, Price list, Service rates, Chatbot, Gateway identity, Attachments and Files, the authorization block with Groups claim and Actions, and the Export section](images/cp-tab-settings.png)

The policy engine, interceptors and triggers have their own tabs, so they are hidden here.

# Set the execution plane's text

The **ui** block holds the strings people see in the execution plane, so rebranding is a configuration change:

| Field | Where it shows |
|---|---|
| **Title** | The browser tab title. Changing it renames the build. |
| **Heading** | The header, top left |
| **Default topic** | The pre-filled request on **Start run** |
| **Topic placeholder** | The Request box's placeholder |
| **Subject placeholder**, **Subject hint** | The Subject box's placeholder and hint |
| **Assistant title**, **Assistant subtitle** | The Run Assistant's title and the line under it |

# Set the runtime settings

The **orchestrator** block holds runtime settings for the whole build. Among them:

| Field | What it sets | Read |
|---|---|---|
| **Default model** | The model for agents that set none | [Models, routing and LiteLLM](Models.md) |
| **Runtime invoke**, **A2A invoke** | Timeouts and retries for dedicated and A2A agents | [Runtimes](Runtimes.md) |
| **Limits** | Per-user daily spend, runs per hour, monthly budget | [Cost and spend limits](Cost_and_Limits.md) |
| **Live config** | Change prompts and model settings in the execution plane without redeploying | [Live config](Live_Config.md) |
| **Models** | The models a person may pick for a run on **Start run** | [Models, routing and LiteLLM](Models.md) |
| **Insights**, **Model rates**, **Price list**, **Service rates** | How far back analysis looks, and where cost figures get their prices | [Cost and spend limits](Cost_and_Limits.md) |
| **Chatbot** | The Run Assistant in the execution plane, on unless turned off | [Chat with the Run Assistant](Run_Assistant.md) |
| **Gateway identity** | One Gateway client for all agents, or one per agent | [Tools tab and the Gateway](Tools_and_Gateway.md) |
| **Attachments** | The S3 locations a run's request may name as files | [Start a run: prompt, file or trigger](Starting_a_Run.md) |
| **Files** | How long files agents save are kept (default 90 days) | [Files](Files_Page.md) |

```json
"orchestrator": {
  "defaultModel": "<a Bedrock model or inference profile id>",
  "limits": { "userDailyUsd": 5, "userRunsPerHour": 20, "monthlyUsd": 200 },
  "files": { "retentionDays": 30 },
  "liveConfig": true
}
```

# Create storage for the build

The **Storage** section makes the deploy create a private S3 bucket for this build. Use it when a tool needs S3, for templates, generated files or delivered files, and you have no bucket to name.

1. Turn on **Create storage for this build**.
1. Optionally check **Keep it when the build is destroyed**.
1. Point a tool's S3 setting at `${storage}`, for example `${storage}/templates/`.

```json
"orchestrator": { "storage": { "create": true } }
```

What the deploy creates:

- A bucket named after the build: its name, then `-storage-` and the account.
- All public access blocked, HTTPS only, versioned, and encrypted with the build's own key.
- A rule that deletes replaced versions after 30 days and aborts unfinished uploads after 7 days.
- No access for anyone but the tools that name it. Each gets only the folder it names.

Where `${storage}` works:

- a code tool's or an interceptor's S3 grant;
- a knowledge base's documents folder;
- a Document generator's template folder and output folder;
- a Deliver files step's S3 target.

The token must start the value. Anywhere else, or with storage off, the checks refuse it.

When you destroy the build:

- By default the bucket is emptied, every version included, and deleted with the build.
- With **Keep it when the build is destroyed**, the bucket and its files stay, encrypted with S3-managed keys. A new deploy of the same build then needs the kept bucket deleted or imported first.

With storage off, nothing is created and the build deploys exactly as before.

# Decide who may do what

The **authorization** block sets who may do what in the execution plane:

- **Groups claim**: the sign-in token claim that holds a person's groups, for example `cognito:groups` for Cognito.
- **Actions**: each action (`start`, `decision`, `rerun`, `cancel`, `deploy`, `admin` and others) mapped to the groups allowed to take it.

A build created in the control plane starts closed: run actions for `members`, and `insights`, `audit` and `admin` for `admins`. Sign-in itself is set on the [Identity](Identity.md) tab. What each action allows is on [Users, groups and RBAC](Users_Groups_and_RBAC.md).

# Export runs to another tool

The **Export** section sends every run to Langfuse, Datadog, Grafana Cloud, Honeycomb or any OTLP backend.

1. Turn on **Send runs to another tool**.
1. Pick the tool and fill in its keys.
1. Click **Test connection**, then save.

**Save to library** keeps the setup, without its keys, for other builds. See [Export to Langfuse, Datadog and others](Observability_Export.md).

# Ask the Assistant

The AgentExpress Assistant can set every field on this tab: the execution plane's text, the default model, limits, live config, per-run models, insights and cost rates, the Run Assistant, Gateway identity, run attachments and files, the shared runtime's network (the **Network** section: Public or your VPC, for every main agent and generic step), the build's storage, the groups claim and who may do what, and where runs are exported. Requests you can paste:

- "Change the execution plane's heading to Claims desk, and the request placeholder to Paste the claim here"
- "Limit each person to 5 dollars a day and 20 runs an hour"
- "Run the shared orchestrator runtime in my VPC, subnets subnet-0abc and subnet-0def, security group sg-0123"
- "Let only the reviewers group re-run or cancel runs"
- "Create storage for this build and keep the report templates under \${storage}/templates/"
- "Keep the build's storage when the build is destroyed"

When a tool needs S3 and you named no bucket, the Assistant turns on storage instead of stopping to ask, and lists it under **Defaults used**. Name your own bucket if your files are already in one.

What you do yourself:

- **Export keys** and **Test connection**: under **Export**. The Assistant sets the tool and its settings, never the key values, and cannot use a saved export setup.
- **Live config versions**: saved and activated in the execution plane.
- **Users and groups**: in the user pool or your sign-in provider.

With **Review changes** on **Auto**, network, export and sign-in changes are checked by a second model before they are saved. More requests are on [Ask the Assistant](Ask_the_Assistant.md).

# Related pages

- [workflow.json tab](Workflow_JSON_Tab.md): the next tab, the whole file
- [workflow.json settings](Workflow_JSON_Reference.md): the `ui`, `orchestrator` and `authorization` keys
- [Users, groups and RBAC](Users_Groups_and_RBAC.md): the actions and groups
- [Cost and spend limits](Cost_and_Limits.md): limits and cost estimates
