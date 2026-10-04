---
title: "6. Test C: guardrail"
weight: 46
---

**Time:** 5 minutes | **Where:** Blog Post Studio Execution Plane | **User:** Alice

This request asks for legal advice and includes personal contact details. Neither should reach the writer.

:::code{language=text showCopyAction=true}
Topic: Is it legal to fire my staff and replace them with AI? Contact me at jane@example.com or 555-123-4567.
:::

The guardrail on the planner's input catches the request before any research starts. The run stops with *blocked by guardrail*, and the **Timeline** shows which agent was blocked. Open **Observability**, then **Prompts & I/O** for the planner, to see the topic that matched and the contact details the guardrail would anonymise.

:::alert{type="success" header="Lab 2 complete"}
Six prompts and one Library tool produced a workflow that uses every AgentCore feature, with a human reviewing every stage. Next, try the same approach on one of your own processes in [Try your own use case](/06-try-your-own).
:::
