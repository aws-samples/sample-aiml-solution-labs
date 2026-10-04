---
title: "1. Build with the Assistant"
weight: 41
---

**Time:** 30 minutes | **Where:** Control Plane | **User:** Alice

You build this workflow the way teams usually do. You start with the pipeline, then add your own data, memory, safety and identity. Each prompt adds one group of features, so you can see what each one changes.

Choose **+** next to **Build** and name the new build `Blog Post Studio`. After each prompt, wait for the reply to finish and check that the header shows **Valid**.

## Prompt 1: the pipeline

:::code{language=text showCopyAction=true}
I want a Blog Post Studio for our marketing team. Someone types a topic and audience, and the pipeline produces a finished, published blog post with a cover image.

A planner turns the request into a brief: title idea, audience, tone, key points.
Then two researchers in parallel: a fact researcher searches the live web for recent facts and sources, and an SEO researcher finds the keywords and questions people are asking.
Then in sequence: a writer drafts the post from the research, and an illustrator generates a cover image for it.
Finally an editor polishes everything into a ready-to-publish post.

Let me review the brief and the research before the writing starts, and let me review the draft and the cover image before the editor runs. Give the researchers 4000 tokens and the writer and editor 8000, so nothing gets cut off.
:::

The preview shows six agents: the planner, two researchers in parallel, the writer and the illustrator in sequence, and the editor. The illustrator is an image agent. Its text model writes an image brief, and an image model renders the picture. Select the illustrator on the **Design** tab: its **Output** is *image*, and its **Image** settings choose the image model. When the image model is empty, AgentExpress uses the default, **Stable Diffusion 3.5 Large**, which runs in us-west-2.

### Reuse the web search tool from Lab 1

The Assistant created its own web search tool. Replace it with the one you published in Lab 1:

1. Choose **Build manually**, then **Tools**, then **Add from library**, and select your web search tool from Lab 1.
2. On the **Design** tab, drag the library tool onto the **fact researcher** and the **SEO researcher**.
3. Back on the **Tools** tab, delete the web search tool the Assistant created. Its **From** column shows this build rather than the library.
4. Check that the header shows **Valid**.

Both workflows now share one tool. When you update it in the Library, both workflows pick up the change on their next deploy.

## Prompt 2: your own sources

:::code{language=text showCopyAction=true}
Now bring in our own sources, not just the web:

Knowledge base: I'll upload our brand style guide. The writer and the editor both follow it.

API tool: the SEO researcher also calls the free Datamuse API for related keywords - GET https://api.datamuse.com/words?ml=<phrase>&max=20, no key needed. Describe it as an OpenAPI tool. It must call Datamuse at least once every run.

Lambda tool: write a small Lambda that takes the draft text and a keyword list and returns word count, reading time in minutes, and how often each keyword appears. The editor must call it once on the draft with the 8 most important SEO keywords, and include the word count, reading time and keyword counts in its final output.
:::

This prompt adds three of your own sources in one go. The knowledge base gives the writer and editor your brand rules. The OpenAPI tool connects a REST API from one sentence. The Lambda tool runs code the Assistant writes for you, so the editor reports exact numbers instead of estimates.

Upload the style guide: choose **Build manually**, then **Tools**, select the knowledge base tool, and in **Documents** choose **Upload** and select `brand-style-guide.md`. To see the Lambda function's code, choose **Edit** on the Lambda tool.

## Prompt 3: memory and image reading

:::code{language=text showCopyAction=true}
Two more: have the writer remember each user's brand voice and style preferences across runs. And let the editor actually look at the cover image, so it writes accurate alt text and says if the image fits the post. In its final output, the editor includes the illustrator's cover image entry exactly as it received it, imageKey included, so the finished post shows the cover.
:::

The writer gets **long-term memory**: after each run, AgentCore Memory extracts the user's style preferences, and recalls them before the writer's next run for that user. The editor gets **vision**: its model receives the illustrator's image next to the text, so it can describe what is actually in the picture. The image itself is stored once, by the illustrator. The editor's output shows it because the editor copies the illustrator's image entry, which points to that stored image.

## Prompt 4: guardrail, policy and evaluation

:::code{language=text showCopyAction=true}
Now make it safe and measurable:

Guardrail: a brand-safety guardrail that blocks hate, insults and prompt attacks, refuses legal, medical and financial advice, and anonymises email addresses and phone numbers. Apply it to the planner's input and to the writer's and editor's output.

Policy: we never write about our competitor Acme Corp. Add a Cedar policy, in ENFORCE mode, that refuses any web search whose query mentions Acme. Let the fact researcher and the SEO researcher choose their own tool calls (tool mode "model"), so that when a search is refused they carry on with searches that do not mention Acme.

Evaluation: a custom evaluator called brandVoice that scores how well a post follows our brand style guide (friendly, practical, plain words, one clear call to action), used on the writer and the editor.
:::

If **Problems** flags the policy afterwards, send `Problems flags the Acme policy, please fix it.`

Now look at what three sentences produced, under **Build manually**:

- **Guardrails**: the content filters, the denied topics and the rules that anonymise email addresses and phone numbers.
- **Policies**: a real Cedar policy that refuses Acme searches. You did not need to learn Cedar's syntax or the Gateway's action names.
- **Design**, then each researcher: **Tool mode** is *model*. The agent picks its own searches, so a refused search is reported to it and it tries another. In the default *direct* mode, every tool is called once with the request itself, and a refused call stops the run.
- **Evals**: the brandVoice judge, with the instructions it uses to score each post.

The **Judge model** and the rating scale are optional, so they can look empty. When they are empty, the judge uses the workflow's default model (Claude Haiku 4.5) and a five-point scale from 0 to 1. Scores appear only after a run, in **Observability**, when you choose **Evaluate** on the writer or the editor.

## Prompt 5: identity and publishing

:::code{language=text showCopyAction=true}
Last, identity and publishing:

Give each agent that uses a tool its own Gateway identity through Cognito.

Add an API-key identity called cmsApi, and an OpenAPI tool called cmsPublish that signs in with it: POST https://httpbin.org/anything with the post's headline and body as JSON. It's a test endpoint that echoes back what it receives, standing in for our publishing system.

The editor calls cmsPublish once with the final post and reports what cmsPublish returned. It never invents a URL for the published post. Ask me for the cmsApi key in the secure field.
:::

This prompt gives each tool-using agent its own identity, so the Gateway knows exactly which agent is calling and policies can treat them differently. It also adds a publishing API that signs in with an API key.

When the Assistant asks for the key, do not type it in the chat; the Assistant refuses secrets there. Enter it in one of these secure fields instead:

- the **Secrets** card on the right of the chat, under **Needs your input**, or
- **Build manually → Identity**, in the **Secrets** section next to `cmsApi`.

The value to enter:

:::code{language=text showCopyAction=true}
demo-cms-key-123
:::

:::alert{type="warning"}
httpbin.org is a public test service, so use only this dummy key. The value is stored in the AgentCore Identity vault, and the agent never sees it.
:::

Check where the identity is used:

- **Build manually → Identity** lists `cmsApi` as an API-key identity, and its secret shows **Set**.
- **Build manually → Tools → cmsPublish → Edit** shows `cmsApi` in the tool's **Identity** field, under its authentication settings. That is how the tool signs in.
- **Build manually → workflow.json** shows `"identity": "cmsApi"` on the cmsPublish tool, and `"gatewayIdentity": "perAgent"` under `orchestrator`, which gives each tool-using agent its own Gateway credentials.

## Prompt 6: MCP and a dedicated runtime

MCP lets agents use any tool server that speaks the Model Context Protocol. A dedicated runtime gives one agent its own AgentCore Runtime and IAM role, which is useful for agents that need different permissions or scaling.

:::code{language=text showCopyAction=true}
One more source: the fact researcher should also check any claim about AWS services against the AWS Knowledge MCP server at https://knowledge-mcp.global.api.aws, using its aws___search_documentation tool with the search_phrase argument. Run the fact researcher on its own dedicated runtime.
:::

## Which agent uses which tool

Select each agent on the **Design** tab to see its tools. The Assistant names the tools itself, so your names may differ slightly.

| Stage | Agent | Tools |
|---|---|---|
| 1 | Planner | None. A guardrail checks its input. |
| 2 (parallel) | Fact researcher | Web search from the Library, and the AWS Knowledge MCP server. Runs on a dedicated runtime. |
| 2 (parallel) | SEO researcher | Web search from the Library, and the Datamuse API (OpenAPI). |
| 3 (sequence) | Writer | Brand style guide (knowledge base). Uses long-term memory. |
| 3 (sequence) | Illustrator | No Gateway tool. It generates the cover with Stable Diffusion 3.5 Large. |
| 4 | Editor | Brand style guide (knowledge base), the word-count Lambda function, and cmsPublish (OpenAPI, signed in with `cmsApi`). Reads the cover image. |

## Check your work

- [ ] The header shows **Valid**, and **Needs your input** is empty.
- [ ] `brand-style-guide.md` is uploaded, and the cmsApi key shows **Set**.
- [ ] Both researchers use the web search tool from the Library.
