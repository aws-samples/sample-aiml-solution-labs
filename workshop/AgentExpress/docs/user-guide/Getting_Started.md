# Sign in and find your way

Opening AgentExpress for the first time? Sign in to the control plane and to a deployed build's execution plane, and find out what each part of the two screens is for. To go straight to a working workflow, follow [Your first build and run](First_Build_and_Run.md) next.

AgentExpress builds multi-agent workflows on Amazon Bedrock AgentCore. You design and deploy a workflow in the control plane. Each deployed workflow then runs in its own execution plane.

# The two places you work

The control plane and the execution plane have the same top bar, a left pane and a main area. They do different jobs.

|  | Control plane | Execution plane |
|---|---|---|
| What you do there | Design, test, share and deploy workflows | Start runs, approve review gates, read results and cost |
| How many | One for your team or organisation | One for each deployed build, at its own address |
| How you open it | The address your control plane's deploy printed | **Open app** on the build's **Deployment** panel |
| Title in the top bar | AgentExpress | AgentExpress followed by the build's title |

How the two fit together is explained on [Control plane and execution plane](Architecture.md).

# Sign in to the control plane

## Deploy your own control plane

To get your control plane, deploy AgentExpress into your AWS account. Follow the [Quick start in the code repository](https://github.com/aws-samples/sample-aiml-solution-labs/tree/main/workshop/AgentExpress#quick-start). The deploy prints the control plane's address and creates a user for you.

1. Open the address the deploy printed.
1. Sign in with the user the deploy created. The first sign-in asks you to set a password of at least 12 characters, with upper and lower case letters, a digit and a symbol.

Your control plane uses Cognito sign-in by default, and can use Okta, Microsoft Entra ID or Auth0 instead.

## What you can do after you sign in

What you may do depends on your sign-in groups. A person in no group can look around, but buttons such as **Deploy** are greyed out or hidden. To see your groups, open the user menu at the right of the top bar. If an admin has just added you to a group, sign out and sign in again, because groups are part of the sign-in token. Groups are explained on [Users, groups and RBAC](Users_Groups_and_RBAC.md).

# Find your way in the control plane

The left pane lists your builds and the libraries. The main area shows the build you open. This screenshot shows the build "Warranty claim triage" from [Your first build and run](First_Build_and_Run.md):

- At the top, the **Deployment** panel shows version 1 deployed, with **History**, **Open app**, **Destroy** and **Deploy**.
- On the left of the **AgentExpress Assistant** tab is the conversation that built the workflow.
- On the right is the workflow it built: three stages, **Claim Intake**, **Warranty Policy Check** with a review gate, and **Customer Reply**.

![The build Warranty claim triage: the name with Valid and 6 warnings, the Share, Project and Export buttons, the Deployment panel with version 1 deployed, its buttons and the link to the execution plane, then the AgentExpress Assistant tab with the conversation on the left and the three-stage workflow on the right: Claim Intake, Warranty Policy Check with a HITL gate, and Customer Reply. The left pane lists the builds, AWS accounts, Teams, the libraries, Activity and About this application.](images/cp-first-deployed.png)

## The left pane

| Item | What it is | Read |
|---|---|---|
| **Build** (_Preview_) | Your builds, each with a status icon. **+** starts a new build. **Find build** filters a long list. | [The control plane](Builder_Console.md) |
| **AWS accounts** | AWS accounts of yours that builds can deploy into | [Set up AWS accounts](AWS_Accounts.md) |
| **Teams** | Teams you belong to, and their workspaces | [Teams and workspaces](Teams.md) |
| **Agent library** | **Tools**, **Skills**, **Memory**, **Guardrails** | [Library](Library.md) |
| **Gateway and access library** | **Identity**, **Policies**, **Interceptors** | [Library](Library.md) |
| **Quality library** | **Evals**: your own LLM judges | [Library](Library.md) |
| **Activity** | Your sign-ins, builds, deploys and run actions | [Activity](Activity.md) |
| **Admin** | Every user's builds, read-only. Shown to admins only. | [Users, groups and RBAC](Users_Groups_and_RBAC.md) |
| **About this application** | What the control plane can do. **Info** in the top bar opens the same panel. |  |

Collapse the left pane with the arrow at its top right to give the build page more room.

## The top bar

The top bar shows **Info**, the control plane's AWS Region, and your user name. The user menu lists your sign-in groups and has **Sign out**.

## A build page

Click a build in the list to open it. The build page has the build's name and status, the **Deployment** panel, and two tabs: **AgentExpress Assistant** (build by chat) and **Build manually** (the canvas and the configuration tabs). Each part is explained on [The control plane](Builder_Console.md).

# Sign in to an execution plane

Every deployed build has its own execution plane with its own sign-in.

1. On the build page, open the **Deployment** panel and click **Open app**.
1. With the built-in sign-in, the panel shows **Sign in as** your email and a **Show the temporary password** link. The same password is emailed to you.
1. Sign in. The first sign-in asks you to set your own password. The temporary password stops working after 7 days.

With Okta, Entra ID or Auth0 there is no temporary password: you sign in with that provider. People you share the build with get their own login to its execution plane. See [Share a build](Sharing_a_Build.md).

# Find your way in an execution plane

An execution plane has no canvas, no library and no AWS accounts page. Its left pane lists **Runs** and **Observability**, and lower down **Files**, **Activity**, **Analytics**, **Memory records**, **Live config** and **About this application**. Some pages are shown only to people with the right permission. The **About** panel lists what is deployed: the workflow version, the framework version, the stages and agents, where each agent runs, the review gates and the data sources.

This screenshot shows the execution plane of the same build, "Warranty claim triage", after a run:

- The left pane lists the run under **Runs** and under **Observability**.
- The main area shows the run's **Graph view**: the same three stages as the workflow in the control plane, each finished, with the review gate before **Warranty Policy Check**.
- On the right, the **About** panel lists what is deployed: workflow v1, framework 1.0.0, 3 stages, 3 agents, and the review gate on **Warranty Policy Check**.

![The execution plane of Warranty claim triage: the left pane with the run under Runs and Observability, then Files, Activity and About this application; a succeeded run with 3 of 3 steps complete and its Graph view showing Claim Intake, Warranty Policy Check with a HITL gate, and Customer Reply; and the About panel listing workflow v1, framework 1.0.0, 3 stages, 3 agents, placement, the HITL gate on Warranty Policy Check and the data sources](images/ep-first-run-about.png)

Each page of the execution plane is explained under [The execution plane](Execution_Plane.md).

## Where runs happen

Runs happen in an execution plane. Each deployed build has its own, and you open it with **Open app**.

A control plane that you deploy yourself comes in one of two modes:

- **Builder only** designs and deploys workflows. It does not run them. Each build runs in its own execution plane.
- **App** also runs one sample workflow of its own, so it has its own **Runs** and **Observability** pages. A build deployed into the same AWS account then shows a **Run here** button on its Deployment panel.

# Related pages

- [Your first build and run](First_Build_and_Run.md): a ten-minute walkthrough
- [Control plane and execution plane](Architecture.md): how the two planes fit together
- [Concepts and glossary](Concepts_and_Glossary.md): the words used in this manual
- [The control plane](Builder_Console.md): the build page in detail
- [The execution plane](Execution_Plane.md): the pages of a deployed build
