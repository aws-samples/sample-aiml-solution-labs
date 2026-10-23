# Chat with the Run Assistant

The **Run Assistant** is a chat in each build's execution plane that answers questions about your runs and can act on them. Use it to ask what a run cost, why a step failed or what an agent wrote, or to approve a gate or re-run an agent without hunting for the button. It's a different chat from the AgentExpress Assistant, which edits builds in the control plane.

# Open the Run Assistant

Click the chat bubble at the bottom right of any page in the execution plane. The panel opens beside the page, so you can read the run while you ask. Click the bubble again, or the close icon, to hide it.

The Run Assistant open over the Runs page of the example build:

![The Assistant panel over the Runs page: Ask about a run, or tell me to do something; Ask about status, cost, latency, outputs, guardrails or evaluations, or tell me to approve a gate, re-run an agent, or run an evaluation; and an Ask about a run… input with a send button](images/ep-run-assistant.png)

Type in **Ask about a run…** and press Enter. Replies are formatted as Markdown.

# Ask about runs

You can ask about:

- a run's status and where it is waiting;
- its outputs, step by step;
- its cost and latency;
- its guardrail results and evaluation scores;
- which runs you have.

For example: _Why did the latest run end early?_, _What did the letter agent write?_, _Which step cost the most?_

On the Observability page, "this run" means the run shown in **Run detail**.

The assistant answers from the same records the pages show. To check an answer, open the page it comes from:

| Topic | Page to check |
|---|---|
| Status, outputs, why a run ended | [The run page and its tabs](Run_Page.md) |
| Cost and latency | [One run in detail](Run_Traces.md) |
| Evaluation scores | [Evaluations](Evaluations.md) |

# Ask it to act

The assistant can take these actions for you:

- approve, revise or deny a review gate;
- re-run agents;
- run an evaluation.

It works while a run is paused at a gate.

# Rules it follows

- It sees only your runs.
- It is held to the same permissions as the buttons. An action your groups do not permit is not offered to it, so it is not a way around a gate.
- It cannot answer an agent's question to a person. Answer those on the run page.

# Set it up as a builder

- The title comes from `ui.assistantTitle` ("Assistant" when unset). The greeting and placeholder come from `orchestrator.chatbot`.
- It is on unless `orchestrator.chatbot.enabled` is `false`.
- Turn off single abilities under `orchestrator.chatbot.tools`, for example `rerun`, `review` or `runEval` set to `false`.
- Its use does not count against the spend and run limits. Only starting and re-running runs are covered.

For a production execution plane where the assistant should not act, turn off its action tools. See [Sign-in and security](Security.md).

# Ask the Assistant

In the control plane, the AgentExpress Assistant can set up the Run Assistant for your build: turn it on or off, pick its model, change its title, greeting and placeholder, and turn single abilities off. Requests you can paste:

- "Call the Run Assistant Claims helper, and greet people with: Ask me about any claim run"
- "Stop the Run Assistant from re-running agents or deciding gates"
- "Use Claude Haiku 4.5 for the Run Assistant"
- "Turn the Run Assistant off"

It picks only models this account can use.

What it can't do here:

- Chat about runs for you. The Run Assistant is a separate chat in the execution plane, and the AgentExpress Assistant can't read runs.
- Deploy. Its changes reach the execution plane once you choose **Deploy**.

All requests by feature: [Ask the Assistant](Ask_the_Assistant.md).

# Related pages

- [The run page and its tabs](Run_Page.md): the same run, read on the page.
- [Human in the loop](Human_in_the_Loop.md): the gates it can decide.
- [Evaluations](Evaluations.md): the scores it can start.
- [Settings tab](Settings.md): the chatbot settings in the control plane.
