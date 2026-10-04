---
title: "4. Test A: full run"
weight: 44
---

**Time:** 20 minutes | **Where:** Blog Post Studio Execution Plane | **User:** Alice

This run exercises every feature at once. Run Tests A, B and C in order, as Alice, because Test B uses the memory that Test A creates.

Start a run with this request. It mentions AWS services on purpose, so the fact researcher uses the AWS Knowledge MCP server.

:::code{language=text showCopyAction=true}
Topic: Which AWS services can help a small shop start using AI agents in 2026? Audience: owners of small retail and service businesses with no technical team. Tone: casual, short sentences, lots of everyday examples.
:::

1. At the **planner gate** (the first review gate, after the planner writes the brief), choose **Approve**.
2. At the **research gate** (the second review gate, after the two researchers):
   - For the SEO researcher, choose **Revise** with this note:

     :::code{language=text showCopyAction=true}
     Focus on long-tail keywords
     :::

   - For the fact researcher, choose **Approve**.
   - Choose **Submit decisions**, then approve the SEO researcher when it comes back.
3. At the **draft and cover gate** (the third review gate, after the writer and illustrator), read the draft and look at the generated cover image, then choose **Approve**.
4. The editor polishes the post, checks it with the Lambda function, writes the alt text and publishes it through cmsPublish.
5. When the run finishes, open **Observability**, then **Run detail**, select the **editor** and choose **Evaluate**. The brandVoice judge scores the post against the style guide and explains its score.

## What to check

| Feature | What to look for |
|---|---|
| Library tool | Both researchers' web searches use the tool from Lab 1. |
| MCP on a dedicated runtime | The fact researcher's `aws___search_documentation` calls. |
| OpenAPI | The Datamuse keyword results from the SEO researcher. |
| Knowledge base | Style guide retrievals by the writer and the editor. |
| Lambda | The word count, reading time and keyword counts in the editor's output. |
| Image generation and reading | The cover image in the illustrator's output, at the draft and cover gate, and in the editor's final output, with alt text that describes it. |
| Identity | The cmsPublish echo shows `X-Api-Key: demo-cms-key-123`. httpbin.org only echoes the request, so there is no real published URL; the editor reports the echo instead of inventing one. |
| Memory | The writer stores the "casual, everyday examples" preference. |
| Evaluations | The brandVoice score and its explanation. |
| Observability | Cost per agent, the prompts, the tool calls, and the Cedar *allowed* decisions. |

Wait about a minute before Test B, so that memory has time to process this run.
