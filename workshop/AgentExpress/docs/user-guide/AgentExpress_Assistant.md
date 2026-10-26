# Build with the AgentExpress Assistant

The AgentExpress Assistant is a chat on the build page that writes the workflow for you. You describe the process in plain words; it drafts the agents, prompts, tools, stages and review gates, shows the result as it builds it, and lists every default it chose. Use it to start any new build, and keep talking to it to change one. It edits the same build the canvas edits, so you can switch between chat and hand edits at any time.

# What the Assistant does

A build opens on the **AgentExpress Assistant** tab. The left column is the conversation. The right column is the build: a **Workflow** preview with its validation status, and cards for **Secrets**, **Needs your input**, **Defaults used** and **Versions** when there is something to show.

By talking to it you can change:

- agents and their prompts, models, frameworks and AgentCore features;
- stages, parallel groups, review gates with named options, and branch rules;
- tools, including the code of a Lambda tool or a hosted MCP server it writes for you, and code fetched from Git or S3;
- Document generator tools for PDF, PPTX and DOCX files, and generic steps that deliver a run's files;
- the build's own storage, when a tool needs S3 and you named no bucket;
- each agent's prompt caching;
- where each AgentCore Runtime runs: Public or in your VPC;
- skills, triggers and interceptors;
- the named guardrails, memories, evaluators, identities and Cedar policies.

It can also read a public web page you link in a message, and have a second model review its larger changes before they are saved.

It reuses items from your library as they are, and imports MCP servers, A2A agents and skills from your organisation's AWS Agent Registry when you ask.

Every change is checked with the same rules as the deploy. If a change breaks a rule, the Assistant fixes it before it replies. If the only fix is a value you must give, it lists that value under **Needs your input**.

> [!NOTE]
> A change you make by hand while the Assistant is thinking is kept. Its edit is applied to the build as it is at that moment.

# Start from a prompt

To draft a new workflow:

1. Create a build with **+** next to **Build** in the left pane, or open an empty build.
1. On the **AgentExpress Assistant** tab, type what the workflow should do in the box that says _What would you like to build?_
1. Press Enter to send. Shift+Enter adds a new line.

Or click a starting point under the box.

| Starting point | What it asks for |
|---|---|
| **Claims triage** | Read a claim, check it against policy documents, flag possible fraud, and have a person approve the decision. |
| **Research and report** | Research a topic from internal documents and the web, analyse the findings, and write a report a manager can review. |
| **Content with images** | Draft a product launch blog post with a matching hero image, with an editor reviewing both. |
| **Explain this build** | Explain what each agent does, how they connect, and what still needs your input. |

## Draft now, or questions first

Four things shape a workflow: the **input** (what a person gives it), the **output** (what comes back), the **sources** (where its facts come from) and the **steps or sign-off** (what happens, and who approves). If your message gives at least three of the four, the Assistant drafts the whole workflow at once. Otherwise it asks up to three short questions, each with the answer it would assume. Reply with your answers, or reply **go**. Say _just build it_ to get a draft at once, or _ask me first_ to get questions before any change.

## What you see while it works

- The reply streams in. A status line says _Thinking…_, _Writing…_, _Updating the build…_ or _Checking the change…_.
- Each finished change shows as a green line in the reply.
- The **Workflow** preview reloads as each change is saved and highlights the agents that changed. Its description shows _Last change:_ and a summary.

A finished first draft, with the conversation so far: the request and reply on the left, the drafted workflow and the defaults it chose on the right:

![The Assistant after a claims triage request: the reply lists the stages it built, the Workflow preview shows Intake, then Policy Checker and Fraud Checker in parallel with a HITL gate, then Letter Writer, and Defaults used lists the model, temperature, maxTokens, guardrails, evaluations, tool mode and framework](images/assistant.png)

## The cards on the right

| Card | What it shows | What to do |
|---|---|---|
| **Defaults used** | What it chose where you did not say: model, temperature, token limits, guardrails, evaluations, tool mode, framework | Ask for something else, or change it on the canvas. |
| **Needs your input** | Values only you can give, such as an endpoint, an ARN or a key | Reply in the chat, or fill it in under **Build manually**. **Deploy** waits for them. |
| **Secrets** | API keys, OAuth client secrets or bearer tokens a tool or agent needs | Type them here, never in the chat. Each is stored encrypted with the build and never shown again. |
| **Versions** | Each change the Assistant saved, as a named version, newest first, with the current one marked | Say "go back to v1" to return to one. |

# Edit a build by chat

Once a draft exists, every message is a change request, and the Assistant applies it without asking first. For example:

- _Add a review gate after the fraud checker._
- _Give the policy checker the warranty knowledge base, and set its tool mode to model._
- _Run the two research agents in parallel._
- _Add a webhook trigger for new Jira issues._
- _Use the Cedar policy noBigRefunds from my library on the refunds tool._

To take a change back, click **Undo** in the build's header. Each Assistant change is one undo step. You can also ask it to undo its last change.

How it reads your requests:

- **Parallel work.** "Run X, Y and Z in parallel" gives each its own agent in one parallel stage. A check, gate or branch goes on the stage after it.
- **Listed tools.** When you list several tools, it makes one tool for each. If two must be one, for example because one API serves both, it says why.
- **Files.** PDF, PPTX and DOCX files come from the Document generator. For any other file it writes a Lambda tool with pinned libraries.
- **Storage.** When a tool needs S3 and you named no bucket, it turns on the build's storage and lists it under **Defaults used**, instead of stopping to ask.

## Go back to a version

Each change the Assistant saves is a named version, such as _v1: multi-agent draft_ or _v2: single agent_. v0 is the build before its first change. The **Versions** card lists them, newest first, and marks the current one.

- Say "go back to v1" to return to a version. Before it does, it says which version it restores, for example _This goes back to v1: multi-agent draft._
- "Undo that" takes back its last change, and it says which version that puts back.
- Going back is saved as a change of its own, so **Undo** takes it back too.
- When it is unclear which version you mean, it lists them and asks.

Requests you can paste:

- "List the versions"
- "Go back to v1"
- "Undo that, and tell me which version it restores"

## When it asks instead of changing

- **A message with no request.** If you paste only content, such as a spec, a list or a log, it changes nothing and asks one short question about what to do with it.
- **What it builds.** In its first reply to a brief that also asks for application code, such as a custom UI, SDK code, unit tests or a server of your own, it says once what it builds and what stays outside. Later replies mention only new gaps.

Requests you can paste for each feature, and what it cannot set, are on [Ask the Assistant](Ask_the_Assistant.md).

## Conversation controls

- **Start over** clears the conversation. The build stays as it is, and Undo still works.
- The copy icon on a reply copies its text. **Jump to latest** appears when you scroll up while a reply is coming in.
- In a long conversation, older messages are kept as a summary of what was decided.

The conversation is stored with the build and is private to its owner, like the build. An admin can read it on the Admin page.

# Attach files and S3 paths

Files give the Assistant context: a specification, sample data, a schema, an API document or a screenshot. It reads them and uses what they say in the design.

1. Click the upload icon at the bottom left of the message box (**Attach files**).
1. Pick one or more files. Each shows as a token under the box; click its × to remove it.
1. Type your message and send it.

To point at files in Amazon S3, write their path in the message: `s3://bucket/file` for one object, or `s3://bucket/folder/` for the files in a folder.

Attached code is used as it is:

- Attach your `handler.py`, a hosted MCP server's `server.py`, or an interceptor's code, and say where it goes, for example "Use the attached handler.py as the code of the orderLookup tool".
- The Assistant copies the file into the tool's or interceptor's files unchanged, rather than retyping it. The file must be UTF-8 text.
- Code you keep in Git or S3 is not attached: ask the Assistant to point the tool at it, and it is fetched at each deploy. See [Tool types in depth](Tool_Types.md#choose-where-code-comes-from).

| Limit | Value |
|---|---|
| Files per message | Up to 5, uploads and S3 objects together |
| Documents | PDF, CSV, Word, Excel, HTML, text and Markdown, plus JSON, YAML, XML, Terraform, Python, TypeScript, JavaScript, SQL and log files read as text. Up to 4.5 MB each. |
| Images | PNG, JPEG, GIF and WebP, up to 3.75 MB each |
| S3 locations | Only the buckets or folders the control plane allows, listed under the message box |

These files are for the design conversation. Files a run starts with are set up separately; see [Start a run: prompt, file or trigger](Starting_a_Run.md).

# Build from a web page

Give the Assistant the address of any public page, such as a product page, an API guide or a process description, and it reads the page before it designs anything. For example: _Based on &lt;the page's address>, build a workflow that triages these requests._

- While it reads, the reply's progress list shows _Reading_ and the site's name.
- Its reply says which pages it read. If a page could not be read, it says why and asks you to paste the text or attach the file.
- It reads up to 5 pages per message, and gives each page about 10 seconds. It reads up to 2 MB of a page and keeps up to 40,000 characters of its text, with the page's title and links.
- It reads HTML, plain text, Markdown, JSON, XML and CSV. PDFs are not read yet: attach the PDF instead.

What it does not read:

- **Private or internal addresses.** Every address the host name points to must be public. Loopback, link-local, private-range and cloud metadata addresses are refused, and so is a redirect to one.
- **Pages behind a sign-in.** Pages are read without your sign-in, cookies or credentials, so a page that needs a login is not read.
- **Addresses with a user name or password in them**, and anything but http and https.
- **Scripts.** Pages are read without a browser, so JavaScript does not run. A page drawn by JavaScript gives almost no text, and the Assistant says so.

The page's text is treated as untrusted data: reference material for the design, never instructions. It reaches the Assistant marked as page content, and the Assistant is told to ignore any instructions in it.

# Have a second model review changes

The Assistant's **Review changes** setting decides whether a second model checks the Assistant's change before it is saved. It has three choices, and yours is remembered in this browser:

- **Off**: no review.
- **Auto** (default): only larger or sensitive changes are reviewed. These are a new build drafted from scratch; agents, stages, tools or named blocks added or removed; code changed; a security-relevant setting changed, such as a guardrail, identity, policy, network, code source, secret or endpoint; or more than 8 items changed at once. A wording change to a prompt, or a different model, is not reviewed.
- **Always**: the first change of every message is reviewed.

How a review works:

1. The reviewer reads your request, the parts of the build the change touches, before and after, and the problems the checks report.
1. If it has notes, the Assistant gets them once as advice, and may revise its change. That version is saved without a second review.
1. The reviewer only advises. If it fails or takes too long, the change is saved anyway.

While it works, the progress list shows _Reviewing changes…_. Under the reply, one line says how it went:

| Line | Means |
|---|---|
| _Reviewed: no issues_ | The reviewer found nothing to change |
| _Reviewed: N suggestions applied_ | The Assistant revised its change after the notes |
| _Reviewed: N suggestions noted_ | The Assistant kept its change as it was |
| _Review skipped (small change)_ | **Auto** found the change too small to review |
| _Review unavailable_ | The review could not run, and the change was saved without it |

# What it cannot do

- **It cannot deploy, run, delete or publish anything.** You click **Deploy** yourself.
- **It does not take secrets in the chat.** Keys go in the **Secrets** card, the Identity tab, or the tool's settings.
- **It does not invent tool types.** Every data source becomes a real tool type or a remote A2A agent.
- **It does not write application code.** It builds a deployable workflow and its tools, not a custom UI, SDK code, unit tests or a server of your own.
- **It does not edit library items a build uses.** Edit them in the library, or ask for a copy for this build.
- **It stays on topic.** It discusses only the design of this workflow.

Every change it can make, you can also make by hand under **Build manually**. A control plane that keeps drafts only in the browser has no Assistant tab.

# Tips for good prompts

| Tip | Example |
|---|---|
| Say what comes in and what comes out. | _Input: a customer email. Output: a reply draft and a priority._ |
| Name the sources. | _Check it against our warranty policy documents and the claims database._ |
| Say where a person signs off. | _Let me review both checks before the letter is written._ |
| Say what runs side by side. | _The policy check and the fraud check can run in parallel._ |
| Attach a sample. | A synthetic claim, an API document or a CSV of sample rows. |
| Change one thing at a time. | _Add a guardrail that blocks legal advice to the letter writer._ |
| Read the defaults. | Check **Defaults used** after each draft. |

# Server settings for the Assistant

These are for whoever deploys your control plane. They are set at deploy time, as CDK context (`-c`) or Terraform variables, not in the chat or the build.

- `designerModel` (Terraform `designer_model`): the Assistant's models, a comma-separated list of `model[:effort]`. It uses the first one the account may call, and skips a model the account is refused.
- `designerEffort` (`designer_effort`): how hard it reasons before it answers: `low`, `medium` (default) or `high`, or `model` to take the model's own default.
- `designerSummaryModel` (`designer_summary_model`): the model that condenses older turns of a long conversation. Default: the deployment's default model.
- `designerS3Buckets` (`designer_s3_buckets`): the buckets, or bucket and prefix, a message may name as S3 paths. Empty means no S3 paths at all.
- The model that reviews changes is a server setting too.

# Related pages

- [Ask the Assistant](Ask_the_Assistant.md): what to ask for each feature
- [The control plane](Builder_Console.md): undo, validation and the Project menu
- [Build manually on the canvas](Canvas.md): review and change the draft by hand
- [Library](Library.md): items the Assistant reuses
- [Deploy a build](Deploying_a_Build.md): when the draft is ready
