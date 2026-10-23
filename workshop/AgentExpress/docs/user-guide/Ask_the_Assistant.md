# Ask the Assistant

Would you rather say what you want than click through settings? Tell the AgentExpress Assistant, and it changes the build for you. Below, area by area, is what you can ask it to set, with requests you can paste and change to your own names.

- The Assistant is the chat on the **AgentExpress Assistant** tab of a build in your control plane, behind your sign-in (Cognito by default; Okta, Entra ID or Auth0). To get one, deploy your own with the [Quick start](https://github.com/aws-samples/sample-aiml-solution-labs/tree/main/workshop/AgentExpress#quick-start).
- It edits the same draft as the canvas. Everything here can also be done by hand under **Build manually**.
- Each change is checked with the deploy's own rules, and the Assistant fixes what it broke before it replies.
- It never invents an endpoint, ARN, bucket or ID. It asks under **Needs your input**, and **Deploy** waits for the answer.
- To take a change back, click **Undo** in the build's header, or say "undo that".
- How to start a chat and what the cards mean: [The AgentExpress Assistant](AgentExpress_Assistant.md).

# Start a build

It drafts a whole workflow from a short brief, lists every default it chose under **Defaults used**, and reuses items from your library.

- "Build a workflow that triages support emails: read the email, check it against our product documents, draft a reply, and let me approve it before it is sent"
- "Just build it with your defaults"
- "Rename this build to Claims triage"
- "Explain this build, and list what still needs my input"

It cannot click a starting point, **Start over**, or make a new build: use the starting points under the message box, **Start over** in the Assistant panel, and **Project → New build**.

See [The AgentExpress Assistant](AgentExpress_Assistant.md) and [Build manually on the canvas](Canvas.md).

# Agents and models

It adds, changes, renames and removes agents, and sets each agent's:

- display name, stage, runtime (main or dedicated) and framework (plain, Strands or LangGraph);
- model, temperature, top P, stop sequences and max tokens, picked only from the models this account can use;
- routing rules and fallback models;
- image output (model, aspect ratio, format), or looking at images an earlier agent made;
- whether it reads the files a run starts with;
- the name of what it produces, a long-running label, and a short label for its data source.

It also sets the build's default model, and lets people pick a model per run.

- "Add an agent called risk_scorer after intake that scores each claim from 0 to 100"
- "Switch the letter writer to Claude Haiku 4.5 with temperature 0.2 and 4000 max tokens, and fall back to Amazon Nova Pro"
- "Make hero_image draw a 16:9 PNG, and have the editor agent look at it"
- "Let admins pick the model for a run when they start it"

* LiteLLM models are not in its list: pick a `litellm/` model ("via LiteLLM") in the agent's model picker.
* Picking the model for one run is done in the execution plane, under **Start run → Advanced**.

See [Agents](Agents.md), [Models, routing and LiteLLM](Models.md) and [Start a run: prompt, file or trigger](Starting_a_Run.md).

# Prompts and output schemas

It writes and rewrites each agent's system prompt and output schema (a JSON example of what the agent returns), and a harness agent's instructions.

- "Rewrite the fraud checker's prompt: it must never guess, and must list the evidence for each flag"
- "Change the letter writer's output to a subject, a body and a tone"
- "Add to the research harness agent's instructions: always cite the file you read"

See [Agents](Agents.md) and [Runtimes](Runtimes.md).

# Stages, steps, branches and review gates

## Stages and branches

It orders the stages, groups agents to run in parallel or in sequence, and routes forward on a field of a stage's output.

- "Run the policy checker and the fraud checker in parallel after intake"
- "If the decision is reject, skip the letter writer and end the run"
- "Move the summary agent to the last stage"

## Review gates and approvals

It sets a review gate on a stage: always, only when rules match, or automatic. It sets how the gate is answered (in the execution plane or by an EventBridge event), its timeout, its ID and name. It also lets an agent ask a person mid-step, and makes a tool call wait for approval.

- "Add a review gate after the two checkers, but only stop when the fraud score is above 70"
- "If nobody answers the gate in 4 hours, deny it"
- "Let the intake agent ask a person for a missing policy number"
- "Require approval before the refunds tool is called"

## Steps that are not agents

It adds steps that call a tool, run code (written in the build or kept in Git or S3), extract text with Textract, or write a file (CSV, JSON, Markdown or text).

- "Add a step that writes the final report as a Markdown file"
- "Add a step that extracts the text of the uploaded claim form with Textract"

Approving, revising or denying a gate, and answering an agent's question, happen on the run page in the execution plane.

See [Stages and steps](Stages_and_Steps.md) and [Human in the loop](Human_in_the_Loop.md).

# Tools

## Any tool

It adds, changes and removes knowledge base, web search, MCP, OpenAPI, Lambda, API Gateway and Smithy tools, gives them to agents, and sets how each agent uses them (its model picks, or every tool once up front) and how many calls it may make. Per tool, it sets:

- the description, which of the server's tools to call and with which argument, and how result rows are read;
- how it connects: none, API key, SigV4 (and the service name to sign for), OAuth client credentials, a user's own login, or on behalf of the user;
- a saved login (identity) to sign in with, and approval before each call.

* "Give the researcher the web search tool and let its model decide when to call it, at most 10 times"
* "Sign the inventory tool's calls with SigV4"
* "Remove the legacy pricing tool"

## Knowledge bases

It sets corpora, an S3 location or an existing Knowledge Base, a data source connector (web, Confluence, SharePoint, Salesforce), the vector store, chunking and parsing, the embedding model, result count, reranking, the corpus attribute, and a fixed filter on every call. It also sets which corpus each agent searches.

- "Add a knowledge base for our warranty documents, with corpora policies and faqs"
- "Have the claims agent search only the policies corpus"
- "Index our Confluence space into the knowledge base, with semantic chunking"
- "Store the vectors in Aurora and return the top 8 results, reranked"
- "Only ever return documents whose region is EU"

Uploading documents is done in the tool's **Documents** section.

## Web search

It sets the allowed domains, the result count, date bounds on results, and a pinned connector version. Date bounds and the pinned version deploy with Terraform only.

- "Limit web search to our two vendor domains and return 5 results"
- "Only return web results published since January 2026"
- "Pin the web search connector to version 1.2.0"

## MCP servers

It adds an MCP server by endpoint, writes and hosts an MCP server in the build, runs a hosted one from Git, S3 or an ECR image, and puts it in your VPC.

- "Add our ticketing MCP server and call its search_tickets tool"
- "Write an MCP server that converts currencies, and host it in this build"
- "Run the hosted MCP server from our Git repository, and put it in our VPC"

## OpenAPI, API Gateway and Smithy

It writes an OpenAPI document from your description, or uses one you attach or keep in S3. It adds a REST API on API Gateway with its stage and tool filters, and a Smithy tool for an AWS API with an allow list.

- "Write an OpenAPI tool for our orders API: get an order by ID, and create an order"
- "Add our orders API on API Gateway, stage prod, with only the GetOrder tool"
- "Add a Smithy tool from the model at s3://&lt;bucket>/&lt;service>.json, allowed only &lt;service>:&lt;Action> on &lt;resource ARN>"
- "Add the lifecycle OpenAPI tool the framework ships"

A Smithy model must describe a service that uses the RestJson protocol, and its allow list names full resource ARNs. For an OpenAPI or Lambda tool, it can also use an OpenAPI document or function code the framework ships, so the build names no bucket or ARN.

Uploading an openapi.json in the tool panel is done by hand. Attaching it to the chat works too.

## Lambda and code tools

It writes a tool's code (handler.py, requirements.txt, test events), takes code you attach as is, points at code in Git or S3, and sets what the code may reach (a secret, a table, an S3 prefix, a VPC), its timeout and memory. It also adds your own deployed Lambda function.

- "Write a tool that computes a claim's payout from the policy limits"
- "Let the payout tool read the claims table, with 512 MB and 30 seconds"
- "Use our existing Lambda function for address lookups"

**Check and run** and **Test the deployed tool** are in the tool's panel.

## Browser, Code Interpreter and pricing

- "Give the researcher a browser that may only visit our documentation domain"
- "Add a code interpreter so the analyst can run Python on the data"
- "Add the AWS pricing tool"

## Connectors

It cannot add a tool from the connector catalog: use **Add a connector** on the **Tools** tab. It can write an OpenAPI tool for the same service instead:

- "Write an OpenAPI tool for the Jira issue search API, signed in with OAuth"

API keys and client secrets are never typed in the chat. The Assistant names each one in the **Secrets** card; enter it there or under the tool's **How it connects**.

See [Add tools and connectors](Adding_Tools.md), [Tools and the Gateway](Tools_and_Gateway.md) and [Tool types](Tool_Types.md).

# Skills

It writes skills (a description, instructions and up to 20 text files), gives them to agents, adds approved skills from your registry, and gives a harness agent skills from the AWS catalog, Git or S3.

- "Write a skill called refundPolicy with our refund rules, and give it to the letter writer"
- "Add the approved brand voice skill from our registry"
- "Give the harness agent the skills in our Git repository"

Publishing a skill to the registry is an admin's **Publish to registry** on the **Skills** tab.

See [Skills](Skills.md).

# Memory

It adds named memories (semantic, summary, user preference, episodic or a custom strategy), sets how long they last and whose they are (per user, subject, agent or run), and gives them to agents and harness agents.

- "Give the support agent memory of each user's preferences, kept for 30 days"
- "Add a memory that summarises each customer's history, shared by intake and triage"
- "Add a custom memory strategy that only keeps order numbers and product names"

Reading and deleting stored records is done on the execution plane's **Memory records** page (admin).

See [Memory](Memory.md) and [Memory records](Memory_Records.md).

# Guardrails

It sets the build's guardrail (content filters, denied topics and words, managed word lists, PII, blocked messages), adds named guardrails, turns input and output checks on or off per agent, and points one agent at a Bedrock guardrail you already have.

- "Block medical and legal advice, and mask email addresses and phone numbers"
- "Add a guardrail named strictPii and use it on the letter writer's output only"
- "Use our existing Bedrock guardrail for the medical review agent"

Organisation guardrails, such as a permissions boundary or WAF, are set by whoever deploys the control plane.

See [Guardrails](Guardrails.md) and [Security](Security.md).

# Identity and Cedar policies

## Identity, sign-in and access

It adds named identities (OAuth or API key), lets a tool or an agent sign in with one, and sets the execution plane's sign-in provider (Cognito, Okta, Entra ID or Auth0), the groups claim, who may do what, and whether agents share one Gateway identity.

- "Add an OAuth identity called crmLogin, and make the crm tool sign in with it"
- "Let the planner agent call our calendar API itself with the calendarApi identity"
- "Sign people in to the execution plane with Okta"
- "Only the admins group may see Insights and the audit log"

Secret values go in the **Secrets** section of the **Identity** tab, or the **Secrets** card. Users and groups are managed in the user pool or your identity provider, and on the **Admin** page.

## Cedar policies

It turns the policy engine on (enforce or log only) for the build, which checks every agent's tool calls at the Gateway. It writes named Cedar policies, attaches them to tools, writes build-wide policies, and narrows what a tool may be called with.

- "Turn on the policy engine in log-only mode"
- "Write a policy that blocks refunds over 500 dollars, and attach it to the refunds tool"
- "Use the Cedar policy noBigRefunds from my library on the refunds tool"

The **Policies** tab has its own writers (Describe it, Guided, Cedar). The Assistant writes the Cedar itself.

See [Identity](Identity.md), [Users, groups and RBAC](Users_Groups_and_RBAC.md) and [Policies and Cedar](Policies_and_Cedar.md).

# Interceptors

It turns on the interceptor before each tool call or after each answer, from templates (audit, block tools, argument guard, inject context, redact PII, hide tools, cap result, custom) or your own Lambda function. It writes custom code, sets what the code may reach or where it comes from, passes request headers, regenerates code, and turns an interceptor off. The build needs at least one tool.

- "Before each tool call, log it, and block the refunds tool for the intake agent"
- "After each answer, redact emails and card numbers, and cap results at 5,000 characters"
- "Use our own Lambda function as the request interceptor, and pass it the request headers"
- "Turn off the response interceptor"

**Check and run**, **Test the deployed interceptor** and the library picker are on the **Interceptors** tab.

See [Interceptors](Interceptors.md).

# Triggers

It adds webhook (signed the AgentExpress, GitHub, Slack or Stripe way, or with a token), schedule, EventBridge, S3 object and SQS triggers. It sets each one's prompt, who it runs as, approvers, gates, runs per hour and whether the payload is attached.

- "Start a run for every new GitHub issue, with a webhook signed the GitHub way"
- "Run the weekly report every Monday at 08:00 Berlin time"
- "Start a run when a file lands in our intake bucket under claims/, and attach the file"
- "Start a run for each message on our SQS queue, at most 20 runs an hour"

A webhook's address and secret, and **Send a test delivery**, are on the execution plane's **Triggers** page (admin).

See [Triggers](Triggers.md).

# Runtimes, harness and VPC networking

It moves agents to a dedicated runtime, sets call retries and timeouts, adds harness agents (model provider, tools, container, storage mounts, VPC, limits, or your own harness), and adds A2A agents (from an agent card, or the stand-in A2A server the framework deploys) with their timeouts. It puts the shared runtime, a dedicated agent, a hosted MCP server or a code tool in your VPC.

- "Move the analyst to its own dedicated runtime, and run it in our VPC"
- "Run the main runtime in our VPC"
- "Add a harness agent with a shell and a browser, and an S3 Files storage mount"
- "Add a remote A2A agent from its agent card, signed with SigV4"

See [Runtimes](Runtimes.md) and [Settings](Settings.md).

# Prompt caching

It turns caching on or off per agent and picks what to cache and for how long. It refuses what the agent's model cannot cache, and says why.

- "Turn on prompt caching for the system prompt and tools on every agent"
- "Cache the conversation so far for the triage agent, with a 1 hour TTL"
- "Turn off prompt caching for the summary agent"

The savings are on the execution plane's **Observability** page.

See [Prompt caching](Prompt_Caching.md).

# Library and registry

It reuses your library items as they are, searches your organisation's AWS Agent Registry, imports MCP servers, A2A agents and skills from it, updates them, and keeps them in sync.

- "Find approved MCP servers for tickets in our registry, and add the one for support"
- "Is there a newer version of the ticketing tool in the registry? Take it"
- "Keep the brand voice skill in sync with the registry"
- "Make a copy of the shared policyDocs tool for this build"

Publishing, unpublishing and sharing library items is on the library pages in the left pane. Publishing a deployed agent to the registry is an admin's **Publish to registry** on the Deployment panel.

See [Library](Library.md) and [Agent Registry](Agent_Registry.md).

# Evaluations and test suites

It turns evaluations on for an agent, picks built-in judges, writes your own judges, and sets whether every run is scored.

- "Score the letter writer for faithfulness, helpfulness and correctness on every run"
- "Add a judge that checks each reply cites a policy section, and use it on the letter writer"

It cannot write or run test suites: use the **Tests** tab. To score one run on demand, use **Evaluate** in the **Prompts & I/O** inspector on the execution plane's Observability page, under **Run detail**, or ask the Run Assistant in the execution plane. After it refines a deployed build, redeploy with **Deploy**.

See [Evaluations](Evaluations.md), [Test suites](Test_Suites.md) and [Refine and redeploy](Refine_and_Redeploy.md).

# Observability export

It sends every run to Langfuse, Datadog, Grafana Cloud, Honeycomb or an OTLP backend, with the region, endpoint, whether prompt and output text go too, or a secret ARN you already have. It also sets how far back Insights look.

- "Send every run to Langfuse in the EU region, without the prompt and output text"
- "Export runs to our OTLP collector"
- "Have Insights look back 48 hours"

Export keys, **Test connection** and **Save to library** are in the **Export** section of the **Settings** tab. Re-sending and pausing are on the **Export** card of the execution plane's **Observability** page.

See [Export to Langfuse, Datadog and others](Observability_Export.md) and [Observability](Observability.md).

# Limits and cost settings

It sets spend and run limits, and the rates used to price model calls and services.

- "Limit each user to 10 dollars a day and 20 runs an hour, and the build to 500 dollars a month"
- "Price our LiteLLM model at 1 dollar per million input tokens"

See [Cost and spend limits](Cost_and_Limits.md).

# UI strings

It sets the execution plane's text: title, heading, default topic, placeholders, the subject hint, and the Run Assistant's title and subtitle.

- "Title the execution plane Claims desk, with the heading Triage a claim"
- "Change the request box placeholder to Paste the claim email here"

See [Settings](Settings.md).

# Run Assistant, live config and run files

It turns on the Run Assistant and sets its model, greeting and placeholder. It turns live config on, sets how long run files are kept, and lists the S3 locations a run may name.

- "Turn on the Run Assistant with a greeting that offers to explain a run"
- "Turn on live config so admins can change prompts without a redeploy"
- "Keep run files for 30 days"
- "Let runs name files in our intake bucket's claims folder"

Live config versions are saved, activated and rolled back on the execution plane's **Live config** page (admin).

See [Run Assistant](Run_Assistant.md), [Live config](Live_Config.md) and [Files](Files_Page.md).

# Give it context

- **Paste an address.** It reads up to 5 public pages per message before it designs: "Build this based on &lt;page address>". Pages behind a sign-in, pages drawn by JavaScript, PDFs and private addresses are not read.
- **Attach files.** Click **Attach files** under the message box: up to 5 per message. Documents up to 4.5 MB, images up to 3.75 MB. Attached code becomes a tool's or interceptor's files as is.
- **Name S3 paths.** Write `s3://bucket/file` or `s3://bucket/folder/` in the message. Only the buckets listed under the message box work.
- **Point at code in Git or S3.** It writes where the code is (a Git repository with branch, tag or commit and folder, or an S3 location), and the deploy fetches it. A hosted MCP server can also use an ECR image. A private repository's token goes in the **Secrets** card.

Requests:

- "Build this based on &lt;page address>"
- "Use the handler.py I attached as the code for the payout tool"
- "Read the sample claims in s3://&lt;bucket>/samples/ and shape the intake agent's output from them"
- "Run the payout tool's code from &lt;repository address>, tag v1.2, folder tools/payout"

See [The AgentExpress Assistant](AgentExpress_Assistant.md).

# Review changes

A second model can check the Assistant's change before it is saved. Pick **Off**, **Auto** or **Always** in the **Review changes** setting of the Assistant panel. Your choice is kept in this browser.

- **Off**: no review.
- **Auto** (default): only larger or sensitive changes are reviewed. A prompt wording change or a different model is not.
- **Always**: the first change of every message is reviewed.

What **Auto** reviews:

- a new build drafted from scratch;
- agents, stages or tools added or removed, or named blocks added or removed;
- tool or interceptor code changed;
- a security setting changed: guardrails, identities, policies, sign-in and access, network and VPC, where code comes from, secrets, endpoints, addresses and ARNs, harness shell, container or browser, a tool's data source, vector store, hosting, allow list, domains or sign-in, triggers, export, interceptors, run attachments, approvals, grants, registry items;
- more than 8 changes at once.

The review only advises. Its notes go back to the Assistant once, and a failed or slow review never blocks the change. See [The AgentExpress Assistant](AgentExpress_Assistant.md) for the result lines under each reply.

# What the Assistant can't do

- **Deploy or redeploy**: **Deploy** on the Deployment panel.
- **Destroy**: **Destroy** on the Deployment panel.
- **Run, resume, re-run or cancel a run, or answer a gate**: the run page in the execution plane.
- **Create, delete, import or export a build**: **Project → New build**, **Project → Delete this build**, **Project → Import a file…**, and the **Export** menu.
- **Share a build or a run**: **Share** in the build's header, or **Share** on the run page.
- **Teams**: **Project → Move to team…**, and the **Teams** page.
- **Connect AWS accounts**: the **AWS accounts** page.
- **Enter secret values**: the **Secrets** card, the tool's **How it connects**, the **Identity** tab, and the **Export** section of **Settings**. It only names them.
- **Add a connector from the catalog**: **Add a connector** on the **Tools** tab.
- **Upload knowledge base documents or an openapi.json**: the tool's panel.
- **Check and run code, or test a deployed tool or interceptor**: the tool's panel, and the **Interceptors** tab.
- **Write or run test suites**: the **Tests** tab.
- **Publish, unpublish or edit a library item, or use library interceptors and saved export setups**: the library pages, the **Interceptors** tab, and **Settings → Export**.
- **Publish to the Agent Registry**: **Publish to registry** on the Deployment panel or the **Skills** tab (admin).
- **Live config versions, or the model for one run**: **Live config** and **Start run → Advanced** in the execution plane.
- **Users, groups and admins**: the user pool or your identity provider, and the **Admin** page.
- **Its own settings**: **Review changes** and **Start over** in the Assistant panel. Its server settings belong to whoever deploys the control plane.
- **Anything off topic**: it only discusses this workflow's design.

# Related pages

- [The AgentExpress Assistant](AgentExpress_Assistant.md): start a chat, the cards, files and web pages
- [Build manually on the canvas](Canvas.md): the same changes by hand
- [workflow.json settings](Workflow_JSON_Reference.md): every key the Assistant can set
- [Deploy a build](Deploying_a_Build.md): when the draft is ready
