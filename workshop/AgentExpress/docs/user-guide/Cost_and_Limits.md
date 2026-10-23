# Cost and spend limits

AgentExpress estimates the cost of every call it makes, shows it per run and across runs, and can cap what people spend. If you own a build, you'll see where the cost figures come from, what costs money beyond model tokens, how to find one deployment's real AWS bill with the `agentexpress:app` tag, and how to set spend and run limits.

# Read the cost estimates

Costs are estimates for planning. The Observability page says so at the top: they will not match your AWS bill, because private pricing, Region and caching discounts differ. Token counts are exact.

You see a cost in these places:

- the **Observability** page: **Overview** for a date range, **Run detail** for one run, split by agent and call;
- a run's **Timeline** chart and **Flow** tab, in their totals;
- the usage line on **Start run**, when limits are set.

## Where a price comes from

Each model call is priced from the first of these that has a rate:

1. `orchestrator.modelRates` in `workflow.json`, per model, in US dollars per million tokens.
1. The AWS Price List for the deployment's Region. It is on by default; `orchestrator.priceList: false` turns it off. Prices are cached for a day.
1. A built-in snapshot of prices.
1. A fallback guess. The figure is shown with a tilde before the dollar sign, and its tooltip says no rate was found.

Hover a cost to see which source priced it. Image renders are priced per image the same way (`orchestrator.imageRates`). Runtime compute, Gateway calls and embeddings use `orchestrator.serviceRates`.

## Models outside Bedrock

- A LiteLLM model's cost comes from the proxy's own cost header when it tracks spend. Without it the row shows \$0, never a guess.
- A harness agent on another provider, or a harness you brought by ARN, has its tokens recorded with no price.

# Know what costs extra

Beyond model tokens, these cost money and are metered on the Observability page:

- evaluations (each evaluator is a model call);
- guardrails;
- Code Interpreter and Browser sessions;
- knowledge base parsing with a model;
- Transaction Search span indexing above 1%.

The rest of the stack is serverless. To plan a monthly figure, use the **Cost projection** on the Observability **Overview** tab: it takes your average run cost and a planned number of runs per day, and gives a 30-day figure.

# See the real bill with the cost tag

Every resource a deployment creates carries the tag `agentexpress:app` with its agent name, and each build's name is its own. To scope Lambda, DynamoDB, CloudFront, AgentCore and the rest to one deployment:

1. In the AWS Billing console, activate `agentexpress:app` as a cost allocation tag. It is an account-level billing action that AgentExpress does not take for you.
1. In Cost Explorer, group or filter by that tag.

The tag does not scope model spend. Amazon Bedrock tokens are not billed to a resource the deployment tags. Use an application inference profile, a Bedrock project or IAM principal attribution for those: each deployment's runtime runs under its own role. Until you set one up, Cost Explorer shows Bedrock for the whole account, and the Observability page is the per-deployment figure.

# Set spend and run limits

Anyone who can start a run spends model money. A build can set three optional limits, all off until you set them, on **Settings → orchestrator → Limits** (`orchestrator.limits`):

```json
"orchestrator": { "limits": { "userDailyUsd": 5, "userRunsPerHour": 20, "monthlyUsd": 200 } }
```

| Setting | Refuses | Resets |
|---|---|---|
| `monthlyUsd` | Any new run or re-run once the execution plane has spent this much this month, everyone together | The 1st of the month, Eastern time |
| `userDailyUsd` | A new run or re-run once that user has spent this much today | Midnight, Eastern time |
| `userRunsPerHour` | A run past this many started in the current clock hour | The top of the hour |

What people see: the **Start run** box shows what the signed-in user has used of each limit, for example "Today: \$1.20 of \$5.00 · This hour: 3 of 20 runs". It warns from 80%. When a limit is reached, it says which one and when it resets, and disables **Start**. A refused request says the reason, for example that today's spend limit is reached.

## Before you rely on the limits

- **A run stops at its next step, not mid-step.** A step already running finishes, so the overshoot is about one step. Set `userRunsPerHour` too when cost matters.
- **They fail open.** If the counters cannot be read, the run starts. They are a cost guard, not a security control.
- **The hour is a fixed clock hour**, not a sliding window.
- **Only starting and re-running are covered.** The Run Assistant, on-demand evaluations and Insights are not.
- **Triggers count** against whoever they run as: the build's owner, or the trigger's own service user.
- **A re-run counts against the daily spend**, but not as a new run.
- **The figure is the one the observability views show**, so it is only as exact as the model prices behind it.
- With no sign-in, everyone shares one budget.

# Ask the Assistant

In the control plane, the AgentExpress Assistant can set the spend and run limits and the prices behind the cost estimates. It can also explain what a build is likely to cost. Requests you can paste:

- "Cap each user at \$5 a day and 20 runs an hour, and the whole execution plane at \$200 a month"
- "Add a price for our custom model: \$0.50 input and \$1.50 output per million tokens"
- "Price models offline from the built-in prices, not the AWS Price List"
- "Which agents in this build will cost the most per run, and why?"

What it can't do here:

- Show what runs have cost so far. Use the Observability page in the execution plane, or ask the Run Assistant there.
- Turn on the cost allocation tag. Do that in the AWS Billing console.
- Deploy. Limits and prices apply once you choose **Deploy**.

All requests by feature: [Ask the Assistant](Ask_the_Assistant.md).

# Related pages

- [Observability](Observability.md): totals, projections and the cost chart.
- [One run in detail](Run_Traces.md): one run's cost by agent and call.
- [Models, routing and LiteLLM](Models.md): choose cheaper models and routes.
- [Settings tab](Settings.md): the Limits and rates settings.
- [Start a run: prompt, file or trigger](Starting_a_Run.md): the usage line people see.
