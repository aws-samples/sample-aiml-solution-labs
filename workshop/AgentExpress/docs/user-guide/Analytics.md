# Analytics

**Analytics** shows how a deployed build is behaving across every user's runs: per agent and per tool error rates, latency, Cedar denials, guardrail blocks, and a health score per agent. It also warns about agents no guardrail screens and tools no Cedar policy decides. Use it to find the agent or tool that needs attention. The page is for admins.

# Open the Analytics page

Click **Analytics** in the execution plane's left pane. It appears only for people in a group granted the `admin` action. Anyone else does not see the link, and the page's route answers 403 to them.

There is nothing to turn on. The page reads the telemetry the runtime already writes, and makes no AWS call until someone opens it.

The Analytics page of the example build, with configuration warnings and the Agents table:

![Analytics page: Last 7 days, 2026/10/13 to 2026/10/19 and refresh; a line saying every user's calls in that range, 151 telemetry rows read; a note that 6 rows are of a kind this execution plane does not know and are counted as other; Configuration warnings (13), each an agent with no guardrail; and the Agents (12) table with Health, Why and Calls, where Letter drafter scores 80 because Cedar denied 50% of its tool calls and its p95 model latency is more than 3 times its median](images/ep-analytics.png)

# Pick a date range

1. Pick a range: **Last 7 days**, **Last 30 days** or **Custom range** (type **From** and **To**). A range can be up to 92 days.
1. Read **Configuration warnings**, then the **Agents** and **Tools** tables.
1. Click the refresh button to read again.

The line under the title says how many telemetry rows were read. A request reads up to 20,000 rows, newest first, and the page says when it stopped there.

# Read the configuration warnings

**Configuration warnings** come from the deployed `workflow.json`, not from telemetry. They say what is configured:

- agents no guardrail screens: neither their input nor their output is checked;
- tools no Cedar policy decides.

When there are none, the page says that every agent is screened by a guardrail and every tool is decided by Cedar. To fix a warning, see [Guardrails](Guardrails.md) or [Policies (Cedar)](Policies_and_Cedar.md), then redeploy.

# Read the Agents and Tools tables

| Table | Columns |
|---|---|
| **Agents** | **Health**, **Why** (the reasons the score is low), **Calls**, **Error rate**, **Guardrail blocks**, **Cedar denials**, **p50 model** and **p95 model** latency, and **Other** |
| **Tools** | **Calls**, **Error rate**, **p50** and **p95** latency, **Cedar denial rate**, and **Allowed / denied / log-only** |

Latency percentiles are exact over the rows read. Sort a table by clicking a heading, for example **Health** to put the weakest agents first.

# How the health score works

Health = 100 − 60 × error rate − 20 × Cedar denial rate − 10 × guardrail block rate − 10 if the p95 model latency is more than 3 times its median (with at least 5 model calls). It is clamped to 0 to 100.

- The error rate is over the agent's model, tool and image calls.
- The Cedar denial rate is denials divided by allowed plus denied.
- The guardrail block rate is blocks divided by guardrail checks.

Each term that costs points is listed under **Why** with its number. For example, 2 errors in 20 calls and 1 denial in 4 decisions gives 100 − 6 − 5 = 89.

## Good to know

- In `LOG_ONLY` mode Cedar decisions go to CloudWatch, so those calls count as log-only, with no rate.
- A row of a kind the page does not know is counted as "other", never dropped. The page says how many.
- A low score from Cedar denials can be expected: a policy that refuses some calls on purpose costs points. Read **Why** before you change anything.
- For CloudWatch alarms, use AgentCore's own runtime and policy metrics.

# Ask the Assistant

Analytics reads telemetry in the execution plane, so the AgentExpress Assistant has nothing to set on this page and can't read its figures. In the control plane it can fix what **Configuration warnings** report:

- "Turn on the guardrail for input and output on every agent"
- "Attach a Cedar policy to the refunds tool that refuses refunds over 500"

With **Review changes** on **Auto** or **Always**, a second model checks guardrail and policy changes before they're saved. The warnings clear once you choose **Deploy**. All requests by feature: [Ask the Assistant](Ask_the_Assistant.md).

# Related pages

- [Observability](Observability.md): cost and tokens across runs and per run.
- [Activity](Activity.md): who did what, and when.
- [Evaluations](Evaluations.md): score the quality of answers, not just errors.
- [Refine and redeploy](Refine_and_Redeploy.md): act on what you find.
