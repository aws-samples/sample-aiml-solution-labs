# Activity

The **Activity** page is the log of who did what, newest first. The control plane has one, for builds, deploys, library and account changes. Each build's execution plane has its own, for sign-ins and run actions. Anyone can use it to see what happened to a build or run, who approved a gate, or why a deploy failed. People with the `audit` permission can also see everyone's activity.

# The control plane's Activity page

Click **Activity** in the left pane of the control plane.

The control plane's Activity page, with sign-ins, builds created and messages to the Assistant:

![The control plane's Activity page: Activity (384), Your sign-ins, and what you built, deployed, ran, approved or deleted, newest first; a search box, an All actions filter and pages; rows with When, What (Signed in, Asked the designer, Build created), Build, Where and Details](images/cp-activity.png)

What the control plane records:

- sign-ins and sign-outs;
- builds created, deleted, shared and moved, and messages to the AgentExpress Assistant;
- secrets changed (their names only, never their values) and execution plane sign-in passwords viewed;
- deploys and destroys requested, succeeded and failed, with the build, version, tool, account and Region;
- AWS accounts connected, edited and disconnected;
- Cedar policies drafted, saved, edited and deleted;
- library items, groups and teams added, changed, shared and deleted; tools tested and tool code changed;
- export tests;
- each time an admin looked at someone else's build or run (_Admin viewed_).

# The execution plane's Activity page

Click **Activity** in a build's execution plane.

The execution plane's Activity page, showing everyone's activity for the last 7 days:

![The execution plane's Activity page with Mine and Everyone (selected), Last 7 days and a date range: Activity (32), rows with When, Who, What, Build, Where and Details, such as Signed in, Run completed, Gate decided, Question answered and Run started, each run row naming its run ID and request](images/ep-activity.png)

What the execution plane records:

- sign-ins and sign-outs;
- run actions: started, gate decided, question answered, tool call decided, re-run, evaluation requested, cancelled, deleted;
- how each run ended: _Run completed_, _Run failed_ (with the error) or _Run ended (denied)_;
- share links made, opened and revoked;
- memory records viewed and deleted;
- live config saves and activations;
- export re-sends, pauses and resumes.

# Find an entry

- Type in the search box to find a user, a build, a run ID or any text in the row.
- Pick a kind in the actions filter: **All actions**, **Sign-in and sign-out**, **Builds and design**, **Deploy and destroy**, **Runs**, **AWS accounts**, **Policies**, **Library, tools and groups** or **Failures**.
- The columns are **When**, **What**, **Build**, **Where** (the source address) and **Details** (for example the run ID and request, or the decision and note).
- The list shows 50 rows a page. Use the page numbers at the top right.
- Click the refresh button to load new entries.

# See everyone's activity

With the `audit` permission, a switch shows **Mine** or **Everyone**. Everyone's activity adds a **Who** column and a date range:

- **Today**;
- **Last 7 days** (the default);
- **Last 14 days**;
- **Last 31 days**;
- **Custom range**. A range covers at most 31 days.

Without `audit` you see only your own activity. See [Users, groups and RBAC](Users_Groups_and_RBAC.md).

# Use it to answer common questions

| Question | Where to look |
|---|---|
| Who approved this gate, and with what note? | The execution plane's Activity, filter **Runs**, search the run ID |
| Why did my deploy fail? | The control plane's Activity, filter **Failures** or **Deploy and destroy** |
| Who changed a secret? | The control plane's Activity, search the secret's name |
| Did an admin open my build? | The control plane's Activity, search _Admin viewed_ |
| Who opened a shared run link? | The execution plane's Activity of the run's owner |

# Ask the Assistant

The Activity pages are logs, with nothing to set. The AgentExpress Assistant can't read, search or filter them. Each message you send it is a line in the control plane's log.

In the control plane it can set who sees everyone's activity in a build's execution plane:

- "Let the auditors group see everyone's activity"

That change applies once you choose **Deploy**. All requests by feature: [Ask the Assistant](Ask_the_Assistant.md).

# Related pages

- [Users, groups and RBAC](Users_Groups_and_RBAC.md): the audit permission and the Admin page.
- [Deploy a build](Deploying_a_Build.md): the deploy history of one build.
- [Human in the loop](Human_in_the_Loop.md): the decisions the log records.
- [Analytics](Analytics.md): how the deployment behaves across runs.
