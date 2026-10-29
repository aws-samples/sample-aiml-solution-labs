# Tool types in depth

Use this when you connect a real data source or API. You'll pick a tool type, see what it reaches, the keys it needs in `workflow.json` and what to watch for, host an MCP server here if you need one, and choose where a tool's code comes from. Every tool is declared once in the `tools` block and reached through the build's AgentCore Gateway, which checks every call against Cedar. To add a tool in the control plane, see [Add tools and connectors](Adding_Tools.md).

# Know the keys every tool shares

| Type | Reaches | Required keys |
|---|---|---|
| `kb` | A Bedrock Knowledge Base over your documents | `corpora` |
| `websearch` | AgentCore Web Search | None |
| `mcp` | Any remote MCP server, or one AgentExpress hosts for the build | `endpoint`, or `hosted` |
| `openapi` | A REST API from an OpenAPI 3 document | `schemaS3Uri`, `source` or `schema` |
| `apigateway` | A stage of an API Gateway REST API | `restApiId`, `stage`, `toolFilters` |
| `lambda` | Your function, code written in the build, or a shipped one (Code Interpreter, Browser) | `lambdaArn`, `code` or `source`, plus `toolSchema` |
| `smithy` | An AWS service API from its Smithy model | `schemaS3Uri` or `schema`, plus `allow` |

Keys most tools share:

- `description`: what the tool does, cut to 190 characters. Agents read it.
- `call`: which operation to invoke. Required when the tool publishes more than one, unless the agent picks for itself.
- `arg` (default `query`) and `args`: the parameter the agent's query goes into, and fixed arguments the agent cannot change.
- `auth` (`none`, `apikey`, `sigv4`, `oauth2`, `user` or `obo`) or `identity`: how the Gateway signs in. See [Identity](Identity.md).
- `service`: for `sigv4` on an `mcp` or `openapi` tool, the AWS service name to sign for. It is worked out from the endpoint; set it only when that guess is wrong.
- `approval`: every call waits for a person. See [Human in the loop](Human_in_the_Loop.md).
- `policy`, `policies`: narrow the generated Cedar permit, or attach named policies. See [Policies (Cedar)](Policies_and_Cedar.md).

Secrets never go in `workflow.json`. If a tool cannot be reached, the agent fails with `ToolUnavailable` and the run says why.

# Knowledge base and web search

A `kb` tool is a Bedrock Knowledge Base over your documents. Use it when agents should answer from your policies, manuals or contracts and cite them. A build has one `kb` tool.

1. Add a **Knowledge Base** tool and name its corpora. A corpus is a set of documents, such as `policies`.
1. Under **Documents**, upload files per corpus. Or point it at your own S3 bucket (`s3Uri`), or an existing Knowledge Base (`knowledgeBaseId`, never deleted by the deploy).
1. Give it to agents, and give each agent a **corpus** to search.

```json
"kb": { "type": "kb", "corpora": ["policies", "runbooks"], "maxResults": 5 }
```

- The generated Cedar permit allows retrieval only with a filter that names a declared corpus.
- `maxResults` (default 5), `rerank`, and `embeddingModel` with `dimensions`. Changing the embedding model replaces the index and re-ingests.
- **Advanced** sets `chunking`, `parsing` (a foundation model reads PDFs and images), a `vectorStore` you already have instead of the default S3 Vectors index, and a `dataSource` such as a web crawl, Confluence, SharePoint or Salesforce (OpenSearch Serverless only).
- A deploy from the control plane of a build whose uploaded corpus has no document is refused. A bundle never carries documents.

To narrow what agents retrieve:

- `corpusKey`: the metadata attribute an agent's `corpus` is matched against. Default `doc_type`, which the framework's own ingestion writes. Set it when your documents are already tagged with something else, such as a product line or a classification.
- `corpusOperator`: how the corpus is compared: `equals` (default), `notEquals`, `startsWith` or `stringContains`. Only single values, because the Cedar permit that enforces the corpus matches one value.
- `filter`: a Bedrock retrieval filter (`andAll`, `orAll`, `equals`, `greaterThan` and so on) applied to every call. It is set on the Gateway target, so the agent cannot see or change it. It is combined with the corpus filter, so it only narrows. Put rules with several conditions here.

```json
"kb": { "type": "kb", "corpora": ["retail", "wholesale"], "corpusKey": "product_line",
        "filter": { "equals": { "key": "region", "value": "EU" } } }
```

A `websearch` tool is AgentCore Web Search, for current public information. It needs no key, and a build has one.

```json
"websearch": { "type": "websearch", "maxResults": 10, "domains": { "include": ["docs.aws.amazon.com"] } }
```

- Available in us-east-1, us-east-2, us-west-2, eu-west-1, ap-southeast-2, ap-northeast-1 and us-gov-west-1. A deploy to another Region is refused.
- `domains` is an allow list and a deny list the agent cannot see or widen. `maxResults` is 1 to 25.
- `publishedFrom` and `publishedTo`: ISO-8601 dates. Results published before or after them are dropped, for questions where old guidance is worse than none. They need connector version 1.2.0, so they work with Terraform only: a CDK deploy refuses them.
- `connectorVersion`: pins the managed web-search connector's version. Terraform pins 1.2.0 when it is left out. CDK cannot pin a version, so a CDK deploy refuses it.
- Results keep each source's title, URL and date, and cited URLs not in the results are dropped. In a custom Cedar policy, name the tool `<key>___WebSearch`.

# MCP and OpenAPI

An `mcp` tool is any remote MCP server over Streamable HTTP. You can also add an approved one from the [AWS Agent Registry](Agent_Registry.md).

```json
"wiki": { "type": "mcp", "endpoint": "https://mcp.deepwiki.com/mcp",
          "call": "ask_question", "arg": "question", "args": { "repoName": "aws/aws-cdk" } }
```

- `auth`: `none`, `apikey` (an X-API-Key header), `sigv4` or `oauth2`.
- `listingMode`: `DEFAULT` caches the server's tool list; `DYNAMIC` asks the server on each call.
- AWS-managed servers add their own `aws___` prefix to tool names.

## Host an MCP server here

If you have MCP server code but nowhere to run it, AgentExpress can host it. The server runs in its own AgentCore Runtime (a container for ARM64, on the MCP protocol) and is added as a target on the build's Gateway. Agents call it like any other MCP tool, and every call goes through Cedar.

1. Add an **MCP server** tool, and under **The server** pick **Host it here** instead of **Yours, by URL**.
1. Under **Its code**, choose where the code comes from: **Written here**, **Upload**, **Git repository**, **Amazon S3** or **Container image**. See [Choose where code comes from](#choose-where-code-comes-from).
1. With **Written here**, edit the starter's `server.py` and `requirements.txt`, and add a `Dockerfile` only if you need one.
1. Optionally set its **Network** to **VPC**. See [Runtimes](Runtimes.md).

```json
"facts": { "type": "mcp", "description": "Looks up product facts.", "hosted": {} }
```

- **What the server must do.** Serve MCP over streamable HTTP on `0.0.0.0`, port 8000, path `/mcp`, as the [AgentCore MCP server contract](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/runtime-mcp-protocol-contract.html) requires. The starter `server.py` does this with the MCP Python SDK.
- **The code's folder** needs a `server.py` or a `Dockerfile` at its root. Without a `Dockerfile`, an ARM64 Python 3.13 image installs `requirements.txt` and runs `server.py`.
- **Pinned packages.** Each line of `requirements.txt` is `name==version`. The starter, and code the Assistant writes, pin `mcp==1.30.0` and `pydantic==2.13.5`, the versions the framework is tested with.
- **A container image** you built is used as it is. It must be a private Amazon ECR image, `<account>.dkr.ecr.<region>.amazonaws.com/<repository>`, with an optional tag or `sha256` digest, built for ARM64 and serving the same port and path.
- **Sign-in.** The Gateway calls the server with SigV4 as its own role, so there is no **How it connects** setting. Leave `auth` unset.
- **Tool names.** The Gateway publishes each of the server's tools as `<key>___<tool>`, for example `facts___lookup_fact`. Use these names in a custom Cedar policy.
- **Limits.** The tool key is part of the Runtime's name, so it is at most 32 characters. The server gets no AWS permissions in this version.

The AgentExpress Assistant can write a hosted server's `server.py` and `requirements.txt` for you.

An `openapi` tool is a REST API described by an OpenAPI 3 document. The Gateway publishes each `operationId` as a tool; there is no function to write. Give the schema by upload in the control plane, `schemaS3Uri`, `source` (a folder under `app/tools/`) or inline `schema`.

- The schema is the allow list: describe only the operations agents need.
- Set `rowPath` when the API nests its records somewhere unusual.
- The ready-made connectors are OpenAPI tools.

# Lambda

A `lambda` tool calls an AWS Lambda function. Use it to reach what the Gateway cannot reach directly: a data warehouse, a relational database, anything inside a VPC.

- `lambdaArn`: your own function. The Gateway may invoke exactly that ARN; your code and role are not touched.
- `code`: code written or uploaded in the build, or fetched from Git or S3 at deploy, deployed as its own function with its own role. See [Tools tab and the Gateway](Tools_and_Gateway.md) and the next section.
- `source`: a shipped function: `pricing`, `codeInterpreter` or `browser`.

A Lambda has no tool list of its own, so `toolSchema` declares each tool it publishes:

```json
"claims": { "type": "lambda", "lambdaArn": "arn:aws:lambda:us-east-1:111122223333:function:query-claims",
  "call": "query_claims", "arg": "question",
  "toolSchema": [{ "name": "query_claims", "description": "Answer a question from the claims warehouse.",
    "properties": { "question": { "type": "string", "required": true, "description": "The question." } } }] }
```

# Choose where code comes from

Code you run in a build does not have to be typed into it. A Lambda tool's code, an interceptor, a **Run code** step and a hosted MCP server each have a **Where the code comes from** choice (**Where the script comes from** for a Run code step):

| Choice | What you give | When the code is read |
|---|---|---|
| **Written here** | The code, typed in the build | Kept in the build and exported with it |
| **Upload** | Files or a zip. For a function: `handler.py` and its other `.py` files, `requirements.txt` and `events.json`, up to 20 files and 500 KB in all. For a script: one file, or a zip holding one, up to 50,000 bytes. For a hosted MCP server, its `.py` files, `requirements.txt` and `Dockerfile` are kept. | Unpacked in the page, then kept in the build like written code |
| **Git repository** | **Repository URL** (https only, with no user name or password in it), **Branch, tag or commit**, an optional **Folder** (**Path of the script** for a script), and an optional **Access token name** | Fetched at each deploy |
| **Amazon S3** | **S3 location**: `s3://bucket/prefix/` for every object under it, or `s3://bucket/key.zip`, and an optional **Folder** | Fetched at each deploy |
| **Container image** | A hosted MCP server only: a private Amazon ECR image, used as it is | Pulled by the Runtime |

```json
"zones": { "type": "lambda", "code": { "codeSource": { "s3": { "uri": "s3://amzn-s3-demo-bucket/tools/zones.zip" } } } }
```

In `workflow.json` a code source has exactly one of `git` (`url`, `ref`, optional `path` and `token`), `s3` (`uri`, optional `path`) or `image`. It goes in `tools.<key>.code.codeSource`, `orchestrator.interceptors.<point>.code.codeSource`, `agents.<id>.task.codeSource` or `tools.<key>.hosted.code`.

- **A private repository.** Give the token a name in **Access token name**. The token itself is saved as a build secret of kind `gitTokens`, never in the workflow, and only the deploy uses it. Use a token with read access, or `user:token`.
- **Pin the version.** Pin a tag or a commit, so a deploy does not change by itself.
- **Limits.** What is fetched is at most 250 MB unpacked.
- **Bundles.** An exported bundle carries Git and S3 sources as references only, never the fetched code.

## S3 buckets the deploy may read

- **In a connected AWS account**, the deploy reads S3 with that account's deploy role, narrowed to the location you gave.
- **In the control plane's own account**, a build may read only the buckets on an allow list, set by whoever deploys the control plane: `-c codeSourceBuckets=bucket1,bucket2` (CDK) or `code_source_buckets = ["bucket1", "bucket2"]` (Terraform). The list is empty by default, so no bucket is read there until it is set.
- The control plane's own buckets are always refused: its builds bucket, the CDK staging bucket, and any bucket tagged `agentexpress:*`.

A deploy into the control plane's own account that names a bucket not on the list is refused before it starts, with the reason. One that names a control plane bucket fails when the code is fetched. Deploy into a connected account instead, or ask whoever deploys the control plane to list the bucket. See [Set up AWS accounts](AWS_Accounts.md).

# API Gateway and Smithy

An `apigateway` tool calls a stage of an Amazon API Gateway REST API in the deploy's account and Region. `toolFilters` is the allow list of paths and methods; `toolOverrides` names an operation that has no `operationId`. With `sigv4`, the Gateway may invoke exactly that API and stage.

A `smithy` tool is an AWS service API, described by its Smithy JSON model, called with the build's Gateway role.

- `allow` is required: 1 to 20 named actions (no wildcards) and 1 to 20 full ARNs. That is exactly what the role gets.
- Actions of identity, code-running and secret-holding services (IAM, STS, Organizations, KMS, Secrets Manager, Lambda, CloudFormation and others) are refused.
- RestJson protocol only, up to 10 MB, no streaming operations.

# Code Interpreter and Browser

A **Code Interpreter** tool lets an agent write a short program and run it in the AWS-managed AgentCore Code Interpreter sandbox, for calculations and data checks.

- Python, JavaScript and TypeScript. Up to 50,000 bytes of code, 30 seconds, 10,000 characters each of output and error.
- The sandbox runs in AWS, not in your account. It cannot read your tables, buckets or secrets.
- Each call is a fresh, billed session. It runs model-written code, so give it only to agents that need it.

A **Browser** tool lets an agent open a public web page and read its title, text and links, in the AWS-managed AgentCore Browser.

- `allowDomains` (optional, up to 50) limits where it may go, checked on the page asked for and the page a redirect ends on.
- It refuses anything but http and https, URLs with credentials, URLs over 2,048 characters, and private or internal hosts.
- It reads pages only: no clicking, typing or pages behind a sign-in. Page text is untrusted input to the model.

# Document generator

A **Document generator** tool lets an agent write a PDF, PPTX or DOCX file from JSON data. Use it when a workflow must hand over a report, a deck or a summary as a file. Add it with **Add a tool**, **Type**, **Document generator**. In `workflow.json` it is a `lambda` tool with `source: "docgen"`.

```json
"documents": {
  "type": "lambda", "source": "docgen",
  "call": "generate_document", "arg": "data",
  "formats": ["pdf", "pptx", "docx"],
  "templateS3Uri": "${storage}/templates/",
  "outputPrefix": "${storage}/outputs/"
}
```

The tool also lists its one tool, `generate_document`, in `toolSchema`. The control plane writes that for you. See the full entry in [workflow.json settings](Workflow_JSON_Reference.md).

- `formats` (required): any of `pdf`, `pptx` and `docx`.
- `templateS3Uri` (optional): one `.pptx` or `.docx` template, or a folder of them ending in `/`. Without one, every file uses a simple built-in layout. PDF always uses the built-in layout.
- `outputPrefix` (optional): an S3 folder ending in `/` that each file is also written to.
- Both S3 settings take an `s3://` address or the build's storage, `${storage}/<folder>/`.
- Each file is always saved with the run, on its **Files** tab, so the build needs run files turned on (`orchestrator.files`).

## Write a template

A field in a PPTX or DOCX template is its name in double curly braces. For the data `{"title": "Q3", "account": {"name": "Example Corp"}, "rows": [{"item": "A"}, {"item": "B"}]}`:

```text
{{title}}            becomes  Q3
{{account.name}}     becomes  Example Corp
{{rows.item}}        repeats its table row, DOCX paragraph or PPTX slide: A, then B
```

- A list repeats the table rows, DOCX paragraphs and PPTX slides that use one of its fields. A list of plain values uses the field `value`.
- An empty list removes the row, paragraph or slide. A field the data does not have renders as nothing.
- DOCX headers and footers are filled too. A filled paragraph takes the formatting of its first run.

## What it may reach

- Its role may read only the template object or folder, and write only the run's files and the output folder.
- It returns the file's S3 address and a download link that works for up to an hour.
- **Test run with sample data** in the tool panel calls the deployed tool once with sample data you can edit, and shows the link.
- A harness agent cannot use it.
- A build without a Document generator deploys exactly as before.

# Tools that act as the person

Most tools sign in with the build's own identity. An `mcp` or `openapi` tool can instead act as the person who started the run, so it sees only what that person may see.

- `user` (**User login**): each person connects their own account once. The first call fails the step with a **Connect** button; they sign in at the provider, return, and click **Run the step again**.
- `obo` (**On behalf of user**): the person's sign-in token is exchanged for a token to the tool. Not available with Entra ID sign-in.

These tools sit on a second Gateway that trusts the execution plane's sign-in. They work only for the person who started the run, in the build's own execution plane: a trigger run, a step approved by someone else, a test suite or a harness agent cannot use them. For **User login**, deploy once, then register the tool's callback URL at the provider.

# Ask the Assistant

The AgentExpress Assistant can add any tool type on this page and set any of its keys: corpora and filters, domains and dates, endpoints, schemas, `toolSchema`, the shipped Code Interpreter, Browser and Pricing tools, how it signs in, `service`, approval and the Cedar `policy` narrowing. It writes a hosted MCP server's or a code tool's files, or points them at code you keep in Git or S3. Requests you can paste:

- "Add a knowledge base with corpora retail and wholesale, match the corpus on the product_line attribute, and only ever return EU documents"
- "Add an OpenAPI tool for our orders API from the attached openapi.json, signed with SigV4"
- "Use the attached handler.py as the code of a new zones Lambda tool, with read access to the zones table"
- "Limit web search to [docs.aws.amazon.com](https://docs.aws.amazon.com/) and to results published since 2025-01-01"
- "Add a Document generator that writes the report as a PDF and the deck as a PPTX from the templates in the build's storage"
- "Give the report writer a tool that writes a DOCX summary, and save each file under \${storage}/outputs/"
- "Add three document tools, one each for the PDF report, the PPTX deck and the DOCX summary"

When a brief needs PDF, PPTX or DOCX files, the Assistant uses the Document generator. For any other file, such as a chart, it writes a Lambda tool with pinned libraries. When you list several tools, it makes one tool for each, or says why two must be one.

- Paste a link to an API's documentation ("Build this tool based on &lt;url>"), or attach the spec or code. Up to 5 pages and 5 files per message. Attached code becomes the tool's files as it is.
- It never invents an endpoint, ARN, bucket, Git URL or ref. It asks, or lists the value under **Needs your input**.
- It can't enter an API key, client secret or Git token: it names the secret, and you fill it in on the **Secrets** card beside the chat or under **How it connects** in the tool panel.
- It can't upload knowledge-base documents (tool panel **Documents**), upload an openapi.json in the tool panel, or run **Check and run** and **Test the deployed tool**. Do those in the tool panel.
- With **Review changes** on **Auto**, a second model checks new tools, code, data sources, vector stores, domains, sign-in and approval changes before they are saved.

More requests are on [Ask the Assistant](Ask_the_Assistant.md).

# Related pages

- [Add tools and connectors](Adding_Tools.md): add tools in the control plane
- [Tools tab and the Gateway](Tools_and_Gateway.md): the Gateway every call goes through
- [Runtimes](Runtimes.md): put a hosted MCP server's Runtime in your VPC
- [Policies (Cedar)](Policies_and_Cedar.md): rules on each tool
- [workflow.json settings](Workflow_JSON_Reference.md): the `tools` block in context
