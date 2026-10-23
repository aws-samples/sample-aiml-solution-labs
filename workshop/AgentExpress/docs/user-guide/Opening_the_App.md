# Open the execution plane

Every deployed build gets its own execution plane: a web application with its own address and its own sign-in, where people run the workflow, review its gates and see its cost. You'll find its address, sign in with your login and temporary password, see what it contains, and give other people access.

# Find the address

After a successful deploy, the Deployment panel on the build page shows the execution plane's address.

The build page of a deployed build, with the Deployment panel open:

![A build page: the Deployment panel says Version 3 deployed with AWS CDK, Deploys as the stack ax-1a2b3c4d-stack, then the execution plane's address, https://dexample1234.cloudfront.net, Sign in as user@example.com, Show the temporary password; the History, Destroy and Deploy buttons and the button that opens the execution plane; below, the AgentExpress Assistant and Build manually tabs](images/cp-build-overview.png)

To open the execution plane, do one of these:

- Click **Open app** on the Deployment panel. It opens in a new tab.
- Follow the address after **Its app:**. When the build is in a connected account, the panel also names the AWS account and Region.

The address is a CloudFront address. Bookmark it, or send it to the people who will use the execution plane.

# Sign in the first time

How you sign in depends on the build's **Identity → Sign-in** setting.

## Built-in sign-in

With **Built in** (an Amazon Cognito user pool the deploy creates for this execution plane):

1. On the Deployment panel, note the email after **Sign in as**.
1. Click **Show the temporary password** and copy it.
1. Open the execution plane and sign in with that email and password.
1. Set your own password when asked.

The temporary password was also emailed to you. It stops working 7 days after it was issued. Viewing it is recorded in your activity log. The deploy puts you in every group of the execution plane, so you can do everything there.

## Okta, Entra ID or Auth0

There is no invitation. People sign in with that provider. After the first deploy, register the execution plane's address at the provider; the Identity tab shows it. See [Identity](Identity.md).

# What the execution plane has

The execution plane's title is _AgentExpress_ followed by the build's title. It opens on the **Runs** page.

The execution plane of the example build, on its Runs page with the About panel open:

![The execution plane titled AgentExpress - Acme warranty claims: the Runs list and Observability list in the left pane, with Files, Activity, Analytics, Memory records, Live config and About this application below; the Runs table with three succeeded runs and a Start run button; and the About panel listing Workflow v3, Framework 1.0.0, Stages 6, Agents 15, Placement, HITL gates, Conditional stages and Data sources](images/ep-runs-home.png)

- **Runs** and **Observability**, with your runs listed in the left pane.
- The Run Assistant, from the chat bubble at the bottom right.
- **Files** and **Activity**.
- For admins: **Analytics**, **Memory records**, **Live config** (when turned on) and **Triggers** (when the workflow has triggers).
- **About this application**, which describes the deployed workflow.

The execution plane has no Build view, no AWS accounts page and no library. Each run records the workflow version it ran with, so old runs keep their own graph after you redeploy. See [The execution plane](Execution_Plane.md).

## Run from the control plane

On a control plane that holds runs, a build deployed in the control plane's own account can also be run from the control plane: click **Run here** on the Deployment panel. A build in a connected account runs from its own execution plane only.

# Give other people access

- **People you share the build with** get their own login to its execution plane. See [Share a build](Sharing_a_Build.md).
- **Anyone else**, with built-in sign-in: add them to the execution plane's user pool and its groups. The build's **Identity → Sign-in** tab shows the pool ID and a link to it in the Amazon Cognito console. See [Users, groups and RBAC](Users_Groups_and_RBAC.md).
- **With Okta, Entra ID or Auth0:** assign the people and their groups in your provider.

What each person may do in the execution plane is set by the groups in the build's `authorization` settings. Someone with no group can sign in but sees most actions greyed out.

# Ask the Assistant

The AgentExpress Assistant can set what the execution plane shows and how people sign in to it. A change takes effect on the next deploy. Requests you can paste:

- "Set the execution plane's title and heading to Warranty claims, and make the request box on Start run say Paste the customer's claim email"
- "Sign people in to the execution plane with our Auth0, and ask me for the domain and client ID"
- "Let the approvers group approve gates, and everyone signed in start runs"

* It can't open the execution plane, show your temporary password or add people to its user pool. Use **Open app** and **Show the temporary password** on the Deployment panel, and the user pool or your identity provider for people.

More requests are on [Ask the Assistant](Ask_the_Assistant.md).

# Related pages

- [Deploy a build](Deploying_a_Build.md): the Deployment panel and the Deploy dialog.
- [The execution plane](Execution_Plane.md): its pages.
- [Start a run: prompt, file or trigger](Starting_a_Run.md): your first run in the execution plane.
- [Sign-in and security](Security.md): identity providers and passwords.
