# Prompt caching

If your agents send the same long instructions or tool list on every model call, prompt caching makes those calls cheaper and faster. You turn it on per agent in the control plane, pick what to cache, and then check the savings in the execution plane's Observability page.

# What prompt caching does

- Many model calls start with the same text: the agent's system prompt, its tool list, the earlier turns of a tool-calling loop.
- With caching on, Amazon Bedrock stores that repeated start (the prefix) at a cache point. The next call within the cache TTL reads it from the cache instead of processing it again.
- Tokens read from the cache are billed at the cache-read rate, below the normal input rate, and the call returns sooner. Tokens written to the cache are billed at the cache-write rate.
- Caching is off unless you turn it on. Below the model's minimum size the call still runs, uncached.

The AWS details are on [Amazon Bedrock prompt caching](https://docs.aws.amazon.com/bedrock/latest/userguide/prompt-caching.html).

# Turn it on for an agent

In the control plane, select the agent on the canvas and expand **Prompt caching** in its settings panel:

- **System prompt**: caches the agent's instructions and skills. Recalled memory and today's date come after the cache point, so they are never cached.
- **Tool definitions**: caches the agent's tool list.
- **Conversation so far**: caches the earlier turns of the agent's tool-calling loop.
- **Cache TTL**: how long a cached prefix lives after its last use: **5 minutes** (default), or **1 hour** where the model allows it. The OpenAI models that take explicit caching show **30 minutes** only.

The section shows the minimum size for the agent's model, and disables what that model cannot cache, with the reason.

Where it applies:

- Prompt caching is per agent only. There is no build-wide default yet: turn it on for each agent that should cache.
- `main` and `dedicated` agents only. Harness agents have no setting (the harness makes its own model calls), and task steps and A2A agents have none either.
- **Tool definitions** and **Conversation so far** apply only to agents that call tools themselves (`toolMode: "model"`).
- LiteLLM models get no cache points.

# Which models can cache

What each model supports comes from the AWS table of supported models and caching limits on the [Amazon Bedrock prompt caching](https://docs.aws.amazon.com/bedrock/latest/userguide/prompt-caching.html) page, copied into the framework. An inference profile (`us.`, `eu.`, `global.`) is matched by its base model. A model that is not in the table gets no cache point.

| Models | What can be cached | TTL | Minimum tokens per cache checkpoint |
|---|---|---|---|
| Claude Haiku 5.5, Sonnet 5.5, Opus 5.5, Fable 5.1, Mythos 5.1, Fable 5, Mythos 5, Opus 5 | System prompt, tool definitions, conversation | 5 minutes or 1 hour | 512 |
| Claude Opus 4.8, Sonnet 5, Sonnet 4.6, Sonnet 4.5 | System prompt, tool definitions, conversation | 5 minutes or 1 hour | 1,024 |
| Claude 3.7 Sonnet, Claude 3.5 Sonnet v2 | System prompt, tool definitions, conversation | 5 minutes only | 1,024 |
| Claude Opus 4.7, Opus 4.6, Opus 4.5, Haiku 4.5 | System prompt, tool definitions, conversation | 5 minutes or 1 hour | 4,096 |
| Amazon Nova 2 Lite, Nova Pro, Nova Lite, Nova Micro | System prompt and conversation | 5 minutes only | 1K, as AWS prints it |
| OpenAI GPT-6.1 Sol, GPT-6 Astra, GPT-5.6 Sol, GPT-5.6 Terra, GPT-5.6 Luna | System prompt of single model calls | 30 minutes | 1,024 per cache breakpoint |

- The minimum counts tools, system prompt and messages together. Below it, the call still runs, uncached.
- A setting the model cannot do, such as **Tool definitions** on Amazon Nova or **1 hour** on a 5-minute model, is an error in Problems.
- Caching on a model the table does not list, such as Claude 3 Haiku, is a warning in Problems: its calls are sent without cache points, and the run is not affected.
- If Bedrock refuses a cache point, the call is tried once more without it.

## OpenAI models

- **GPT-5.6 and later in the table** (GPT-6.1 Sol, GPT-6 Astra, GPT-5.6 Sol, Terra and Luna): AgentExpress caches the system prompt of a single model call, through InvokeModel, for 30 minutes. Tool-calling turns go through Converse, which takes no cache point for these models, so **Tool definitions** and **Conversation so far** are not offered. AWS bills their cache writes at 1.25 times the input rate.
- **Other OpenAI models**: nothing to set. They cache by themselves for prefixes of 1,024 tokens or more. Any cache tokens they report are still recorded and priced.

# Ask the Assistant

The AgentExpress Assistant can turn caching on or off for you and pick what to cache. It refuses what the agent's model cannot cache, and says why. Requests you can paste:

- "Turn on prompt caching for the system prompt and tools on every agent"
- "Cache the conversation so far for the triage agent, with a 1 hour TTL"
- "Turn off prompt caching for the summary agent"

* It can't show what caching saved. Read the **Cache saving** and **Prompt caching** cards on the execution plane's **Observability** page.

More requests are on [Ask the Assistant](Ask_the_Assistant.md).

# Set it in workflow.json

Caching is `agents.<id>.promptCache`. Leave it out to turn caching off:

```json
"triage": { "name": "Triage", "toolMode": "model", "tool": ["policyDocs"],
            "promptCache": { "system": true, "tools": true, "messages": true, "ttl": "5m" } }
```

- `system`, `tools`, `messages`: `true` caches that part. With all three false or absent, caching is off.
- `ttl`: `"5m"` (default), `"1h"` or `"30m"`, and only a value the model allows.

All agent keys are on [workflow.json settings](Workflow_JSON_Reference.md).

# See what it saves

Every model call records the tokens read from and written to the cache, what they cost, and the saving. The saving is the cached tokens at the model's normal input rate, minus what the cache reads and writes cost. It is negative when writes outweigh reads.

- **Overview** and **Run detail** on the Observability page show a **Cache saving** card when the range or run has cache tokens.
- The **Prompt caching** card on **Insights** gives each agent that asked for caching one finding: **Caching is paying off**, **Cache written but rarely read** or **Caching requested but nothing cached**, with what to do.
- For one run, each model call shows **Cache read**, **Cache write**, **Cache cost**, **Cache saving** and **Cache points**, and a _cache hit_ or _cache miss_ badge.
- For a model with no published price, the cache cost and saving show as _unknown_. Cache costs are recorded, never guessed.

Details are on [Observability](Observability.md) and [One run in detail](Run_Traces.md).

# Related pages

- [Models, routing and LiteLLM](Models.md): which model each agent calls
- [Agents](Agents.md): the agent settings panel
- [Observability](Observability.md): cache hits, cost and savings
- [Cost and spend limits](Cost_and_Limits.md): how model calls are priced
- [workflow.json settings](Workflow_JSON_Reference.md): the `agents` keys
