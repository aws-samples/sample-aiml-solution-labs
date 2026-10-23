# The execution plane

The execution plane is where a workflow runs: each deployed build's own web application. A run is one execution of the deployed workflow. You give it a request, the agents work through it stage by stage, and the run pauses wherever a person has to sign off. If you run workflows, you'll find your runs here, find your way around, and see who can read which runs.

A control plane deployed in **Builder only** mode has no Runs or Observability pages of its own: runs live in each build's execution plane. To open one, see [Open the execution plane](Opening_the_App.md).

# The Runs home page

The Runs page is where a build's execution plane opens. It lists every run you started, newest first.

The Runs page of the example build: the left pane, the Runs table in the middle, and the About panel on the right.

![The Runs page of AgentExpress - Acme warranty claims: the Runs and Observability lists in the left pane with Files, Activity, Analytics, Memory records, Live config and About this application below; the Runs table with three succeeded warranty claim runs, Find runs and Start run; and the About panel with This deployment details](images/ep-runs-home.png)

## The Runs table

| Column | What it shows |
|---|---|
| Request | The first line of what the run was asked to do. Click it to open the run. |
| Status | Running, Awaiting review, Succeeded, Failed, Denied, Cancelling or Cancelled |
| Run ID | The run's short id. You can search for it. |
| Workflow | The deployed version the run used (v1, v2, ...). Each run keeps its own workflow, so old runs show their own graph after a redeploy. |
| Started | When the run started. The table sorts on it by default, newest first. |
| Started by | A person, or the trigger that started it, with its type |
| (trash icon) | Deletes the run. Shown only if you hold the `delete` permission. |

To find a run:

1. Type in **Find runs**. The filter matches the request text, the run ID and the status.
1. Click a column heading to sort by it.
1. Use the page arrows at the top right. The table shows 12 runs per page.

When the About panel is open, the table has less room and shows fewer columns.

# Find your way around

- **Left pane.** The **Runs** group lists your runs with a status icon. The **+** beside it starts a new run. The **Observability** group lists the same runs and opens each one on the Observability page. Below them are links to **Files**, **Activity**, and, for admins, **Analytics**, **Memory records**, **Live config** and **Triggers**, then **About this application**.
- **Top bar.** **Info** opens the About panel. The region button shows the AWS Region the execution plane runs in. Your user menu shows your sign-in groups and **Sign out**.
- **Chat bubble** at the bottom right. Opens the Run Assistant. See [Chat with the Run Assistant](Run_Assistant.md).

| Page | Who sees it | Read more |
|---|---|---|
| Runs, Observability | Everyone, for their own runs | [The run page](Run_Page.md), [Observability](Observability.md) |
| Files | Everyone, for their own runs; admins can switch to everyone's | [Files](Files_Page.md) |
| Activity | Everyone, for their own activity; `audit` for everyone's | [Activity](Activity.md) |
| Analytics, Memory records | Admins | [Analytics](Analytics.md), [Memory records](Memory_Records.md) |
| Live config | Admins, when `orchestrator.liveConfig` is on | [Live config](Live_Config.md) |
| Triggers | Admins, when the workflow has triggers | [Triggers](Triggers.md) |

An admin-only page is not shown to anyone else, and its routes answer 403 to them.

# Read the About panel

**Info** in the top bar, or **About this application** in the left pane, opens a panel on the right that explains the execution plane. It opens by itself the first time you visit.

- A one-line description of what the workflow does.
- **This deployment:** the workflow version, the framework release, the number of stages and agents, where the agents run (for example "4 task, 7 main, 1 dedicated, 2 harness, 1 a2a"), the HITL gates, the conditional stages and the data sources. These come from the deployed `workflow.json`, so they always match it.
- **Key features** and **Get started**: choose **Start run**, open the run, and decide when a stage turns amber.

# Who sees which runs

- A run is private to the person who started it.
- A member of a group granted `admin` can read anyone's run but cannot act on it. The gate shows "This is another user's run: an admin can read it, not act on it."
- Runs started by a trigger with `runAs: "service"` belong to the trigger. People in its `approvers` groups see and decide them.
- A run shared with a link can be read by people signed in to the same execution plane who have the link. See [Share a run](Sharing_a_Run.md).
- On a control plane you deployed yourself with the default settings, once it has deployed builds, a **Show the runs of** picker appears under **Runs** so you can switch between the control plane's own workflow and each build.

# Related pages

- [Open the execution plane](Opening_the_App.md): its address and your login.
- [Start a run: prompt, file or trigger](Starting_a_Run.md): the three ways a run starts.
- [The run page and its tabs](Run_Page.md): everything you see in one run.
- [Human in the loop](Human_in_the_Loop.md): when a run waits for you.
- [Control plane and execution plane](Architecture.md): how the two planes fit together.
