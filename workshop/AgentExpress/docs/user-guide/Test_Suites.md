# Test suites

A test suite is a list of requests (cases), each with fixed checks on what the run should produce. Run it after you change a prompt or a model, and you get a pass rate and the list of what broke, instead of a feeling. On the **Tests** tab in the control plane, you'll write cases and checks, run a suite against the deployed build, read the results, and compare the pass rate by version.

Every check is a fixed comparison. Nothing here asks a model to judge: for quality scored by a model, see [Evaluations](Evaluations.md).

# Open the Tests tab

The **Tests** tab is one of the build's tabs under **Build manually**. You can write suites before the build is deployed. Running them needs a deployed build.

The Tests tab on a control plane deployed in **Builder only** mode, which can store suites but not run them:

![The Tests tab: Build level. Saved requests with fixed checks, run against the deployed build after each change. A note that this control plane builds and deploys and holds no runs, so a suite cannot be run from it. You can still write suites here. Test suites (0) with a New suite button and the text No suites yet. A suite starts with a case: a request, and what the run should do with it.](images/cp-tab-tests.png)

# Write a suite

1. Click **New suite**.
1. Click **Add a case**. Give it a **Case name**, an optional **Subject**, and the **Request**, written as you would on **Start run**.
1. Click **Add check** and pick what to check (see below). Add as many checks as the case needs.
1. Add more cases the same way.

A build can keep up to 20 suites, a suite up to 50 cases, and a case up to 20 checks.

# Pick the checks

| Check | Passes when |
|---|---|
| **The run ends** | The run ends as you say: done, failed, denied or cancelled. |
| **An agent ends** | An agent ends done or skipped. |
| **A field of an agent's output** | A JSON field equals, is in, contains, exists, or is greater or less than a value, with the same words and comparison as a branch rule. |
| **An agent's output as text** | The text does or does not contain a phrase. |
| **The run costs (USD)** | The run costs less than an amount. |
| **The calls take (ms)** | The sum of every call's time is less than a number of milliseconds. |

A case with no **The run ends** check must also end done, so a run that failed at its second step does not pass a case that only reads the first agent's output.

# Run a suite

Click **Run** on the suite. One run starts per case, in the **deployed** build, as you, the same way **Start run** starts a run.

- Running needs the `start` permission and is held to the build's spend and run limits.
- Gates approve themselves, as for a trigger, so an unattended run can finish.
- All cases (at most 50) start at once. A case the limits refuse shows as _not started_ with the reason, and is not counted against the build.
- A tool that acts as the person cannot be used in a suite: it needs that person in the run.
- Suites run from a control plane you deployed yourself with the default settings. A build deployed into your own AWS account runs its tests from its own execution plane.

# Read the results

While a suite runs, its summary says how many cases are done. When it ends, it says how many passed, for example "4 of 5 passed", with the build version it ran on.

| Column | What it shows |
|---|---|
| **Result** | **Passed**, **Failed**, **Running** or **Not started** |
| **Case** | The case name |
| **What was found** | What the run did, and each check that failed |
| **Run** | **Open run**, to open the case's run in the execution plane |

A suite run is final once every case has ended. A run still going three hours after it started counts as failed.

# Compare pass rates by version

The last 20 runs of each suite are kept with the build version they ran on. **Pass rate by run** draws them as bars, with a line such as "v3: 4/5 · v4: 5/5" below. Hover a bar for its passed count, version and start time.

Use it after each change: deploy the next version, run the suite, and check that the pass rate did not drop. See [Refine and redeploy](Refine_and_Redeploy.md).

## Where suites live

- Suites are saved, exported and imported with the build, but do not deploy.
- Only the person who started a suite run reads its cases. Anyone who can open the build sees the trend.
- Results go with the build when it is deleted.

# Ask the Assistant

The AgentExpress Assistant can't write, run or compare test suites. Use the **Tests** tab: **New suite**, **Add a case**, **Add check**, then **Run**.

For quality scored by a model, it can set up evaluations instead, for example:

- "Turn on evaluations for the report agent with Correctness and Faithfulness, scored on demand"

All requests by feature: [Ask the Assistant](Ask_the_Assistant.md).

# Related pages

- [Evaluations](Evaluations.md): quality scored by an LLM judge.
- [Start a run: prompt, file or trigger](Starting_a_Run.md): how each case starts its run.
- [Deploy a build](Deploying_a_Build.md): the version numbers the trend uses.
- [Refine and redeploy](Refine_and_Redeploy.md): the loop of change, deploy and compare.
