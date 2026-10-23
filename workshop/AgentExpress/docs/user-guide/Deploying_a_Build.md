Deploying turns a build into its own execution plane on Amazon Bedrock AgentCore. Each build deploys as its own stack, with AWS CDK or Terraform, into the control plane's AWS account or one of yours. Deploying one build never touches another build or the control plane. You'll read the Deployment panel, deploy with the Deploy dialog, choose CDK or Terraform, follow a deploy, check the history and redeploy.

> [!WARNING]
> Deploying needs the `deploy` permission and destroying needs `destroy`. The deploy can create IAM roles, so treat both as admin-level actions. See [Users, groups and RBAC](Users_Groups_and_RBAC.md).

# Read the Deployment panel

The **Deployment** panel sits under the build's name on every build page. It says where the build stands and holds the buttons that act on its deployment.

The Deployment panel of a build deployed with AWS CDK:

![The Deployment panel: Version 3 deployed with AWS CDK, framework 1.0.0 and the date; the History, Destroy and Deploy buttons and the button that opens the execution plane; and the line Deploys as the stack ax-1a2b3c4d-stack. Every AWS name in it starts with ax_1a2b3c4d.](images/cp-deploy-panel.png)

| Status | Meaning |
|---|---|
| _Not deployed_ | The build has never deployed, or was destroyed. |
| _Deploying version N with AWS CDK_ (or Terraform) and a phase | A deploy is running: _waiting for the deploy project_, _generating the code for this version_, _creating and updating resources_, then _done_. |
| _Destroying with_ the tool | A destroy is running. |
| _Version N deployed with AWS CDK_ and the date | The last deploy succeeded. |
| _Deploying version N with … failed_ | The last deploy failed. The error is shown in the panel. |

Every AWS name the build creates starts with the build's `ax_` id, so you can find its resources in the AWS console.

## Buttons

| Button | What it does | Shown when |
|---|---|---|
| **History** | Every deploy and destroy of the build. | Always |
| **Open app** | Opens the build's execution plane in a new tab. | Deployed, and nothing running |
| **Publish to registry** | Submits the deployed build to the AWS Agent Registry. | Deployed, and you are an admin |
| **Run here** | Starts a run of the build from this control plane. | Deployed in this control plane's account, on a control plane that holds runs |
| **Destroy** | Removes everything the build deployed. | The build has been deployed |
| **Deploy** | Opens the Deploy dialog. | Always; greyed out with the reason when it cannot run |

**Deploy** is greyed out while the build is saving, while a deploy or destroy of it is running, while it has errors (_Fix the N errors first_), or when you lack the `deploy` permission. Point at it to read why.

When a deploy is running or has failed, open **Log** in the panel to read its output. **Refresh** reloads it; **Full log in CloudWatch** opens the whole log. Click the panel's title to fold it to one line.

# Deploy a build

1. Fix every error. The status next to the build's name must not show an error count.
1. Enter any secrets the build needs (the Assistant's **Secrets** card, the Identity tab, or the tool's settings).
1. Click **Deploy** on the Deployment panel. The dialog **Deploy version N** opens.
1. Under **Deploy to**, pick **This console's account**, a connected account, or **Connect an AWS account…**.
1. For a connected account, pick the **Region**. Its default region is filled in.
1. Under **Deploy with**, pick **AWS CDK** or **Terraform**.
1. Click **Deploy with AWS CDK** (or **Deploy with Terraform**).

The dialog says what happens: the build is saved as version N, and exactly that version deploys as its own stack. It takes 15 to 30 minutes and keeps going if you close the page. You get your own login to the execution plane, shown on the panel and emailed to you.

The Deploy dialog for a redeploy of a build already deployed with AWS CDK, where the account and the tool are locked:

![The Deploy version 4 dialog: it saves the build as version 4 and deploys it as the stack ax-1a2b3c4d-stack, taking 15–30 minutes; Deploy to shows the control plane's own account greyed out; Deploy with shows AWS CDK selected and Terraform greyed out; Cancel and Deploy with AWS CDK buttons](images/cp-deploy-dialog.png)

# Choose CDK or Terraform

Both tools create the same resources from the same `workflow.json`. Pick the one your team knows or your account's rules require.

|  | AWS CDK | Terraform |
|---|---|---|
| Deploys as | A CloudFormation stack | A Terraform deployment (no stack) |
| State | In CloudFormation | In the control plane's builds bucket, per user and build; removed on destroy |
| Runs as | The CDK bootstrap roles of the target account | The deploy role's own permissions |
| Setup | The control plane's own account and Region must be CDK-bootstrapped. A connected account is bootstrapped on its first deploy. | Nothing extra |

Once a build is deployed, its tool is locked, because both tools would create resources with the same names. To switch, destroy the build first.

# What happens during a deploy

1. The control plane saves what is on screen and checks the build with the same rules as the Build view. A Region that cannot run something the build uses is refused here, before anything starts.
1. The build is frozen as its next version, with library items and registry items resolved as they are now.
1. The control plane's deploy project (AWS CodeBuild, with a 2-hour limit) generates the code for that version on a fresh copy of the framework and deploys it.
1. A notice appears when it ends: _Deploy of version N finished_, or that it failed, with the error and the log on the Deployment panel.

Version numbers count successful deploys only. A failed attempt retries the same number next time. A CDK stack whose first create failed is deleted and created again on the next try. A stack that ever deployed is never deleted this way.

# See the deploy history

Click **History** on the Deployment panel to see every deploy and destroy of the build.

The deployment history of the example build, with one failed deploy and three that succeeded:

![Deployment history dialog: four rows with When, What (Deploy version 1 to 3), Result (three Succeeded, one Failed), Tool AWS CDK, Where (the control plane's own account, us-east-1), By, and Details showing the end of the failure's reason](images/cp-deploy-history.png)

The columns are **When**, **What**, **Result** (**Succeeded**, **Failed** or **Running**), **Tool**, **Where**, **By** and **Details** (for a failure, the end of the error). Everyone the build is shared with, and everyone in its team, sees the same history. Every deploy and destroy is also in the [Activity](Activity.md) log.

# Redeploy a change

1. Change the build: by chat, on the canvas, or on a tab.
1. Click **Deploy**. The dialog offers the next version number.
1. The account, Region and tool are locked to where the build is already deployed. Click **Deploy with** the same tool.

The deploy updates the same stack, and your login to the execution plane is kept. A redeploy also picks up changes to library items the build uses, newer approved registry versions of items kept in sync, and secrets you changed.

Prompts and model settings can change in the execution plane without a redeploy if the build has live config on. See [Live config](Live_Config.md). To move a build to another account, Region or tool, destroy it first, then deploy it again.

# Ask the Assistant

The AgentExpress Assistant can't deploy, redeploy or destroy, and it can't read the deploy log. Click **Deploy** on the Deployment panel yourself. What it does before a deploy:

- It fixes the problems its own changes cause, so **Deploy** is not greyed out by them.
- A value only you can give, such as an endpoint or a bucket, goes on the **Needs your input** card with its exact place in the build. **Deploy** waits until you fill it in: reply in the chat, or set it under **Build manually**.
- A secret it needs goes on the **Secrets** card beside the chat. Enter the value there.

Requests you can paste:

- "What still needs my input before I can deploy?"
- "Explain this build"

More requests are on [Ask the Assistant](Ask_the_Assistant.md).

# Related pages

- [Set up AWS accounts](AWS_Accounts.md): connect an account to deploy into.
- [Open the execution plane](Opening_the_App.md): its address and your login.
- [Clean up](Clean_Up.md): destroy a build and check what is left.
- [Refine and redeploy](Refine_and_Redeploy.md): the loop of change, deploy and compare.
- [Troubleshooting](Troubleshooting.md): when a deploy fails.
