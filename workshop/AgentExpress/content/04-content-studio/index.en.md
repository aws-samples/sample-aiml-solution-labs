---
title: "Lab 2: Blog Post Studio"
weight: 40
---

**Time:** 85 minutes

Your marketing team wants a **Blog Post Studio**. Someone types a topic and an audience. The workflow researches the topic, writes a post in the brand's voice, generates a cover image, edits the post and publishes it, with a human approving each stage.

Lab 1 showed how to build and run a workflow. This lab shows how to make one **safe, measurable and connected to your own systems**, using every AgentCore feature, still without writing code.

![Blog Post Studio workflow](/static/images/blog-post-studio.png)

## What you add, and why

| Feature | What it does in the Blog Post Studio |
|---|---|
| **Library reuse** | Reuses the web search tool you published in Lab 1. |
| **Gateway tools** | Connects your style guide (knowledge base), the Datamuse keyword API (OpenAPI), a word-count Lambda function and the AWS Knowledge MCP server. |
| **Memory** | Lets the writer remember each user's preferred tone across runs. |
| **Images** | The illustrator creates a cover image, and the editor reads it to write alt text. |
| **Guardrails** | Blocks harmful content, refuses legal, medical and financial advice, and anonymises contact details. |
| **Cedar policy** | Refuses any web search about a competitor, enforced outside the model. |
| **Evaluations** | A custom judge scores each post against the brand style guide. |
| **Identity** | Gives each agent its own credentials, and keeps the publishing API key in a vault. |
| **Dedicated runtime** | Runs the fact researcher in its own AgentCore Runtime. |

## Steps

| Step | Where | What you do | Time |
|---|---|---|---|
| [1. Build with the Assistant](/04-content-studio/01-build) | Control Plane | Build the workflow with six prompts, one feature group at a time. | 30 min |
| [2. Deploy](/04-content-studio/02-deploy) | Control Plane | Deploy it with Terraform. | 10 min |
| [3. Under the hood](/04-content-studio/03-under-the-hood) | Control Plane | See where each feature lives while the deploy runs. | 10 min |
| [4. Test A: full run](/04-content-studio/04-test-a) | Execution Plane | Run once and see every feature at work. | 20 min |
| [5. Test B: policy and memory](/04-content-studio/05-test-b) | Execution Plane | Watch Cedar refuse a search and memory recall your tone. | 10 min |
| [6. Test C: guardrail](/04-content-studio/06-test-c) | Execution Plane | Watch the guardrail stop an unsafe request. | 5 min |

## File you need

You upload the brand style guide to a knowledge base in step 1. Download it now:

:button[Download brand-style-guide.md]{href="/static/kb/brand-style-guide.md" action=download}

This is what the file contains:

:::code{language=markdown showCopyAction=false}
# Brightpath Marketing - Brand Style Guide

## Voice
- Friendly, practical and confident. We talk like a helpful expert, never like a salesperson.
- Write for busy people: short sentences, plain words, no jargon. Explain any technical term the first time it appears.
- Use "you" and "your business". Avoid "users", "leverage", "synergy", "revolutionary" and "game-changer".

## Structure of a blog post
1. A headline under 70 characters that names a concrete benefit.
2. An opening of 2-3 sentences that states the reader's problem.
3. 3 to 5 sections with descriptive H2 headings.
4. At least one numbered, step-by-step section the reader can act on today.
5. End with a short "Next step" paragraph with one clear call to action. Never more than one.

## Rules
- Target length: 900 to 1,300 words (about 4-6 minutes of reading).
- Every statistic needs a named source in the text, e.g. "according to Gartner".
- Use the primary keyword in the headline and in the first 100 words.
- British or American spelling is fine, but be consistent within a post.
- Never promise specific results ("double your sales"). Say what is typical and why.

## Images
- Cover images are bright and optimistic, with real-world settings (a shop, a cafe, an office).
- No text inside images, no logos of other companies, no stock-photo handshakes.
:::
