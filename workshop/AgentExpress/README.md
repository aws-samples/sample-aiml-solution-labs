# AgentExpress Workshop

A 4-hour Workshop Studio workshop: build multi-agent workflows on Amazon Bedrock AgentCore with
[AgentExpress](https://github.com/praven80/AgentExpress/tree/UI).

## Repo structure

```bash
.
├── contentspec.yaml                 <-- Workshop Studio spec: accounts, regions, the CloudFormation template
├── FACILITATOR_GUIDE.md             <-- Agenda, delivery tips, troubleshooting
├── assets
│   └── agentexpress-source.zip      <-- Framework source (UI branch); upload to the workshop assets bucket
├── content                          <-- Workshop pages (Markdown)
│   ├── index.en.md                  <-- Welcome, architecture overview, what you build
│   ├── 01-setup                     <-- Get the console, sign in
│   ├── 02-architecture              <-- Multi-agent architecture on AgentCore
│   ├── 03-trip-planner              <-- Lab 1: Assistant, canvas, library, sharing, deploy, runs, observability
│   ├── 04-content-studio            <-- Lab 2: every AgentCore feature
│   ├── 05-cleanup
│   ├── 06-try-your-own              <-- Sample use cases and test prompts
│   └── 07-summary
└── static
    ├── agentexpress-console.yaml    <-- Deploys the console with CDK from CodeBuild
    ├── kb/                          <-- travel-policy.md, brand-style-guide.md (knowledge base uploads)
    └── images/                      <-- Diagrams (.svg sources and .png used by the pages)
```

## How the console is deployed

`static/agentexpress-console.yaml` creates a CodeBuild project (ARM, privileged for Docker). The build:

1. Gets the framework source: `agentexpress-source.zip` from the assets bucket when `AssetsBucketName` is
   set (Workshop Studio passes the event assets bucket), otherwise a `git clone` of `RepoUrl` / `RepoBranch`.
   The GitHub repository is private, so events must use the zip.
2. Calls each Bedrock model the labs use once (Claude Sonnet 5.5, Claude Haiku 4.5 and Claude Sonnet 5 from the `ClaudeModels` parameter, Titan Text Embeddings V2,
   Stable Diffusion 3.5 Large in us-west-2). Failures are logged, not fatal.
3. Runs:

   ```bash
   npx cdk deploy -c agentName=agentexpress -c idp=cognito -c createCognito=true \
     -c enableGateway=true -c consoleMode=builder --require-approval never
   ```

4. Creates `alice@example.com` and `bob@example.com` in the `members` group with one generated password.

A wait condition holds the stack until the build finishes; the outputs `ConsoleUrl`, `UserAlice`,
`UserBob` and `WorkshopPassword` are shown to participants. Start the same project with `ACTION=destroy`
to tear the console down.

## Refreshing the framework source

`assets/` is git-ignored. Rebuild the zip from the UI branch and upload it with the workshop's assets:

```bash
git clone --depth 1 --branch UI https://github.com/praven80/AgentExpress.git /tmp/agentexpress
git -C /tmp/agentexpress archive --format=zip -o "$PWD/assets/agentexpress-source.zip" HEAD
```

## Editing content

Each folder under `content/` needs an `index.en.md` with `title` and `weight` front matter; `weight`
sets the order in the navigation. Directives (`:::alert`, `:::code`, `::::expand`, `::::::tabs`,
`:button`) follow the Workshop Studio syntax; an outer directive needs more colons than the ones nested
in it. Diagrams are generated as SVG, then exported to PNG (`rsvg-convert -w 2400 x.svg -o x.png`).
