# Start a run: prompt, file or trigger

A run starts in one of three ways: a person types a request, a person attaches files or names files in S3 along with the request, or a trigger starts it with nobody typing. If you start runs, you'll fill in the **Start run** form, attach files and use the **Advanced** options. If you set up a build, you'll see how triggers start runs and why a run can be refused.

# Start a run with a prompt

1. Click **Start run** on the Runs page, or the **+** beside **Runs** in the left pane.
1. In **Request**, describe what the workflow should work on. Write as much detail as you like. Every agent in the run reads it.
1. Optionally attach files (see below).
1. Click **Start run**, or press Ctrl+Enter (⌘+Enter on a Mac).

The run opens and its graph fills in as each step runs. See [The run page and its tabs](Run_Page.md).

The Start run form of the example build, with a request that names a file in S3:

![The Start run dialog: the usage line This month, everyone: \$0.8992 of \$250.00, Today: \$0.8991 of \$25.00, This hour: 0 of 10 runs; the Request box with a warranty claim that names s3://…/inputs/invoice.pdf; 292 / 10,000 characters; Files (optional) with Attach files and the allowed S3 path; Advanced collapsed; Cancel and Start run](images/ep-start-run.png)

## What the form shows

| Field | When it appears | What it does |
|---|---|---|
| Usage line | When the workflow sets spend or run limits | Shows what you have used of each limit. It warns from 80% of a limit. When a limit is reached it says which one and when it resets, and **Start run** is disabled. |
| Workflow | When there is more than one deployed workflow to choose from | Picks which deployed workflow runs this request |
| Request | Always | What the workflow should work on, up to 10,000 characters |
| Files (optional) | When at least one agent reads run files | **Attach files**, and the S3 locations the request may name |
| Advanced | Always | Subject, and model overrides when you may pick models |

On a control plane that holds runs, a build deployed in the control plane's own account can also be started from the control plane with **Run here** on its Deployment panel.

# Start a run with files

When an agent in the workflow reads run files (`attachments: true`), the form shows **Files (optional)**.

- Click **Attach files** and pick up to 5 files. Documents can be up to 4.5 MB and images up to 3.75 MB. Files upload as soon as you pick them.
- Or name files in S3 inside the request, as `s3://bucket/key` for one file or `s3://bucket/folder/` for every file in a folder. Only the S3 locations listed on the form are accepted. A builder sets them in `orchestrator.attachments.s3`.
- Accepted types include PDF, Word, Excel, CSV, HTML, text, Markdown, JSON, code, PNG, JPEG, GIF and WebP.
- The files are checked and copied into the run's own folder when it starts, so a re-run reads the same files.
- A file that fails to upload is marked in red. Remove it or pick it again before you start.

The files appear in the run's **Details** under **Files** and on the run's **Files** tab, under **Inputs**.

# Use the Advanced options

Expand **Advanced** for more settings.

The Advanced section of Start run:

![Start run with Advanced expanded: Subject (optional) with the hint Scopes long-term memory, so agents recall insights from earlier runs on the same subject; Model for every agent (optional) set to Each agent's own model, with the hint No fallback model is tried; and Model per agent collapsed](images/ep-start-run-advanced.png)

- **Subject (optional).** A customer, product or project name. Long-term memory is scoped by it, so agents recall insights from earlier runs on the same subject.
- **Model for every agent (optional)** and **Model per agent.** Shown only to people in a group granted the `override` action, and only when the workflow lists models in `orchestrator.models.overrides`. The run uses the picked model instead of each agent's own, for this run only, with no routing and no fallback. The run page then shows **Model override**. See [Models, routing and LiteLLM](Models.md).

# Start a run with a trigger

A trigger starts a run without anyone typing it. A builder adds triggers on the **Triggers** tab in the control plane, and an admin manages their secrets and tests them on the **Triggers** page in the execution plane.

| Type | Starts a run when |
|---|---|
| Webhook | GitHub, Jira, ServiceNow, Slack, Stripe or your own system posts a signed delivery to its URL |
| Schedule | An EventBridge Scheduler expression fires (cron or rate) |
| EventBridge | An event matches a pattern on a bus |
| S3 | An object is created in a bucket, under a prefix |
| SQS | A message arrives on a queue |

Each trigger fills the run's request from the delivery, for example the Jira issue key and summary. A trigger runs as the build's owner, or as its own service user whose runs its approver groups decide. Spend and run limits apply to trigger runs too.

In the Runs table, a trigger's runs show the trigger's name and type under **Started by**. For setting up each type, see [Triggers](Triggers.md).

# When a run is refused

- **A spend or run limit is reached.** The server refuses the run with the limit and when it resets. See [Cost and spend limits](Cost_and_Limits.md).
- **You lack the `start` permission.** **Start run** is greyed out. Ask for the group that grants it. See [Users, groups and RBAC](Users_Groups_and_RBAC.md).

# Ask the Assistant

In the control plane, the AgentExpress Assistant can set how runs of your build start: which agents read run files, which S3 locations a request may name, the triggers, and the models people may pick per run. Requests you can paste:

- "Let the intake agent read the files people attach when they start a run"
- "Allow run requests to name files under `s3://amzn-s3-demo-bucket/contracts/`"
- "Start a run every weekday at 8:00 New York time that summarises yesterday's tickets"
- "Let the reviewers group pick Claude Haiku 4.5 or Amazon Nova Pro for every agent on Start run"

Good to know:

- It uses only the buckets, models and groups you name. It never makes up a bucket.
- It writes the whole trigger list each time, so name every trigger you want to keep when you change one.
- To build a webhook's request from what the sender posts, attach a sample delivery or paste the address of the sender's webhook docs.
- With **Review changes** on **Auto** or **Always**, a second model checks new triggers and S3 locations before they're saved.

What it can't do here:

- Start a run, attach a run's files or pick a run's model. Use **Start run** in the execution plane.
- See a webhook's signing secret or send a test delivery. Admins do that on the execution plane's **Triggers** page.
- Deploy. Its changes reach runs once you choose **Deploy**.

All requests by feature: [Ask the Assistant](Ask_the_Assistant.md).

# Related pages

- [The execution plane](Execution_Plane.md): the Runs page and who sees which runs.
- [Triggers](Triggers.md): webhooks, schedules, EventBridge, S3 and SQS in detail.
- [The run page and its tabs](Run_Page.md): what you see once the run starts.
- [Human in the loop](Human_in_the_Loop.md): when the run stops for a person.
- [Cost and spend limits](Cost_and_Limits.md): the limits on the usage line.
