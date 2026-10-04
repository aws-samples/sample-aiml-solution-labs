---
title: "3. Library and sharing"
weight: 33
---

**Time:** 15 minutes | **Where:** Control Plane | **User:** Alice, then Bob, then Alice

When one person creates a tool, nobody else should have to create it again. The **Library** stores tools, guardrails, policies, memories, evaluators and identities that any workflow can use. Sharing lets your team work on the same workflow without overwriting each other.

## Publish tools to the Library (Alice)

1. Choose **Build manually**, then **Tools**, and tick the **web search** and **Frankfurter** tools.
2. Choose **Publish to library (2)**. The **From** column now shows that both tools come from the Library.
3. Open **Library → Tools** in the left navigation. Tick both tools, choose **Share (2)**, add `bob@example.com` and choose **Save**.

Library items are used live. When you change a tool in the Library, every workflow that uses it picks up the change on its next deploy. You reuse this web search tool in Lab 2.

## Share the workflow (Alice)

1. In the build header, choose **Share · Only you**.
2. Add `bob@example.com` and choose **Save**. The button now shows that the workflow is shared.

Whoever you share with can do everything you can: open the workflow, change it, deploy it and share it further. You can also share with groups, or with everyone who signs in to the Control Plane.

## Edit the workflow as Bob

1. Open a private browser window, go to the **ConsoleUrl**, and sign in as `bob@example.com`.
2. Under **Build**, open **Trip Planner**. It is marked as shared with you by alice@example.com.
3. Bob sees the same canvas, prompts, tools and travel policy as Alice. Select the **travel brief** agent and add this line to its **System prompt**:

   :::code{language=text showCopyAction=true}
   End the brief with a one-line sign-off from the Brightpath travel team.
   :::

4. Open **Library → Tools**. Alice's two tools are available for Bob's own workflows.
5. Sign out and close the private window.

## Back to Alice

Reload the Trip Planner build. Bob's line is now in the travel brief's prompt.

:::alert{type="success" header="What you just did"}
You made two tools reusable for your team and collaborated on one workflow. If two people save at the same time, the later save is refused and offers **Reload now**, so nobody overwrites someone else's work.
:::
