# Observability

Every run in AgentExpress is recorded call by call: each model call, tool call, memory read and write, guardrail check, Cedar policy decision and evaluation, with tokens, latency and an estimated cost. The **Observability** page in a build's execution plane shows these figures across runs and for one run. Anyone who runs a build uses it to see what runs cost and how they behaved, what prompt caching saves, and, for admins, whether runs reach an export tool.

# What is recorded

Observability is always on. There is no switch, per agent or for the deployment.

| Record | Where it goes | What it holds |
|---|---|---|
| Telemetry row per call | The deployment's telemetry table | One row for each model, tool, memory, guardrail, policy, evaluation, image and compute call: the agent, the run version, tokens, latency, cost and outcome. Model rows also keep the prompt and answer. |
| OpenTelemetry spans | CloudWatch (Transaction Search) | One `session.id` per run, with agent and tool spans. AgentCore Evaluations and Insights read these. |
| Timeline lines | The deployment's events table | What you see on the run's **Timeline** tab |
| Run status | The deployment's status table | Each step's status and output |

The run page's **Flow** tab and **Timeline** chart show the events. The **Observability** page shows the figures and the prompts.

> [!WARNING]
> Telemetry stores prompts and outputs. If your inputs are sensitive, lower `OBS_MAX_CAPTURE_CHARS` on the runtime. Spans indexed above 1% in Transaction Search cost extra.

# Open the Observability page

Click **Observability** in the left pane. The page is titled **Observability, Cost & Quality** and has three tabs: **Overview**, **Run detail** and **Insights**. A note at the top says that costs and token counts are estimates for planning, and names the price book.

The **Observability** group in the left pane lists your runs. Click one to open it on **Run detail**.

# Overview tab

**Overview** shows totals over a date range.

The Overview tab of the example build:

![Observability, Cost & Quality, Overview tab: the note that costs are estimates for planning; From, To, Group by Date, Apply, CSV and JSON; cards Total cost \$0.899237, Runs 3, Total tokens 878,785, Calls 151, Active days 1; a Cost projection with \$0.299746 average cost per run, 3 runs measured, a planned volume of 10 runs per day and \$89.923655 projected per month; and the Cost by date chart](images/ep-observability.png)

1. Set **From** and **To**.
1. Pick **Group by**: **Date**, **Model** or **User**.
1. Click **Apply**.

You see:

- **Totals:** total cost, runs, total tokens (in and out), calls and active days.
- **Cost projection:** the average cost per run over the runs measured. Type a planned volume in runs per day to get a projected cost for a 30-day month. It is a planning estimate.
- **Cost chart** and a **Breakdown** table by the group you picked.
- **CSV** and **JSON** download the breakdown.

# Run detail tab

**Run detail** shows one run's cost, tokens, calls and latency, split by agent and by call, and opens each agent's prompts.

The Run detail tab with one run of the example build loaded:

![Run detail for session 50e1e52d2d16: Run date, Run cost \$0.299395, Tokens 293,093, Calls 50, Model+tool latency 216.7 s; a Model rates table in USD per 1M tokens for three models; a Cost by agent bar chart; and the Per-agent breakdown with each agent's models, input and output tokens, tools, latency and cost, each with a Prompts & I/O button, and a row for AgentCore Runtime · compute](images/observability.png)

Pick a run in **Session** and click **Load**. How to read each part is on [One run in detail](Run_Traces.md).

# Insights tab

**Insights** shows AgentCore's cross-run analysis: failure patterns, user intents and behaviour across recent runs. It needs a group granted the `insights` action, because it spans every user's runs.

The tab notes that AWS has stopped accepting new Insights jobs while it redesigns the feature, so new analyses cannot be started. Findings from earlier runs are still shown. **Refresh** reads them again.

Below them, the **Prompt caching** card is the framework's own advice, per agent, over the Overview tab's **From** and **To** range. See the next section.

# See what prompt caching saves

When agents use prompt caching (see [Prompt caching](Prompt_Caching.md)), every model call records the tokens read from and written to the cache, what they cost, and the saving. The saving is the cached tokens at the model's normal input rate, minus what the cache reads and writes cost. It is negative when writes outweigh reads.

- **Overview** and **Run detail** add a **Cache saving** card when the range or run has cache tokens. It shows tokens read and written, and the cache cost, which is already inside the total cost.
- Per-agent and per-call cache figures for one run: see [One run in detail](Run_Traces.md).
- **CSV** and **JSON** downloads include the cache columns.

The **Prompt caching** card on **Insights** gives each agent that asked for caching at most one finding:

| Finding | Means | What to do |
|---|---|---|
| **Caching is paying off** | The recorded net saving is above zero | Nothing |
| **Cache written but rarely read** | Fewer tokens were read from the cache than written to it, with no net saving | Run the agent more often within the cache TTL, use a longer TTL where the model allows it, or turn caching off for the agent |
| **Caching requested but nothing cached** | Calls asked for caching, and nothing was read or written | The cached part is probably shorter than the model's minimum, which the card shows with a link to the AWS model table. Make the system prompt or tools longer than it, or turn caching off. |

Each finding shows the tokens read and written, the read and write cost, and the net saving.

Cache costs are recorded, never guessed. When a model has no published price, its call cost is an estimate (its rate is marked with `~`, and `orchestrator.modelRates` can give it a price), and its cache cost and saving show as _unknown_. See [Cost and spend limits](Cost_and_Limits.md).

# The Export card

When the build sends runs to another tool, admins see an **Export** card at the top of the page. It shows:

- **Destination**, and whether **Sending** is on or paused (and by whom).
- **Sent, last 24 hours** and **Failed, last 24 hours**, with the last error.
- **Send test trace**, to check the connection from the execution plane.
- **Pause** and **Resume**, for the whole execution plane. While paused nothing leaves.

See [Export to Langfuse, Datadog and others](Observability_Export.md).

# Ask the Assistant

Observability is always on, so there's no switch for the AgentExpress Assistant to turn. In the control plane it can change the build settings behind some figures: how far back Insights looks, the prices behind the cost estimates, and prompt caching, which the **Cache saving** and **Prompt caching** cards report on. Requests you can paste:

- "Look back 72 hours for Insights"
- "Price nova-pro at \$0.80 input and \$3.20 output per million tokens"
- "Turn on prompt caching for the system prompt on every agent"

What it can't do here:

- Open this page or read its figures. Ask the [Run Assistant](Run_Assistant.md) in the execution plane about a run's cost, latency or scores.
- Pause, resume or test the export. Admins use the **Export** card above.
- Deploy. New settings apply once you choose **Deploy**.

All requests by feature: [Ask the Assistant](Ask_the_Assistant.md).

# Related pages

- [The run page and its tabs](Run_Page.md): the Flow and Timeline views of one run.
- [One run in detail](Run_Traces.md): prompts, tokens, latency and cost per call.
- [Export to Langfuse, Datadog and others](Observability_Export.md): send runs to your own tool.
- [Analytics](Analytics.md): error, latency and denial rates across runs.
- [Cost and spend limits](Cost_and_Limits.md): where the prices come from.
