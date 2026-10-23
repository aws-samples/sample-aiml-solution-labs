# Troubleshooting

Fixes for common problems with signing in, deploying, tools, knowledge bases and destroying, following the Troubleshooting part of `DEPLOYMENT.md` in the code repository. Find your symptom in the tables, or search the repository for the error text. For general questions, see [FAQ](FAQ.md).

# Look up a common symptom

| Symptom | Fix |
|---|---|
| Deploy or ingestion fails when invoking a model | Enable Bedrock model access for the model and the embedding model. |
| Login redirect mismatch | Add the execution plane's URL to the sign-in client's callback and sign-out URLs. Auth0: also web origins. Entra ID: as a single-page application redirect URI. |
| "Could not sign you in" page | The provider refused the sign-in, and its reason is shown. For example, Entra ID `AADSTS50105`: the user is not assigned to the Entra ID application. |
| Gate buttons greyed out, API answers 403 | Add the user to a group, then sign out and back in. |
| Every gated action is denied | Auth0, Okta or Entra ID: set `authorization.groupsClaim`. No sign-in (`idp = "none"`): remove `authorization.actions`. |
| Agents with tools fail with `ToolUnavailable` | Turn on the tool plane (the Gateway), or check what the tool's target lists. |
| Plan or synth names a missing value or secret | Supply it in the deploy settings or the build's secrets. |

# Fix sign-in and access

## A button is greyed out

Point at it to read why. Common reasons:

- **Deploy:** the build has errors (_Fix the N errors first_), it is saving, a deploy or destroy is running, or you lack `deploy`.
- **Approve, Revise, Deny:** you lack `decision`, or the run is someone else's.
- **Start run:** you lack `start`, or a spend or run limit is reached. The usage line says which limit and when it resets.

Groups travel in your sign-in token. After an admin changes your groups, sign out and sign in again. The user menu in the top bar shows your current groups. See [Users, groups and RBAC](Users_Groups_and_RBAC.md).

## The temporary password does not work

A temporary password stops working 7 days after it was issued. See [Open the execution plane](Opening_the_App.md).

# Fix a deploy

- **Read the error.** The Deployment panel shows it, and **Log** holds the deploy's output. **Full log in CloudWatch** opens the whole log. **History** shows the end of each failure's reason.
- **Refused for its Region.** Something the build uses is not offered there. AgentCore Web Search is offered in us-east-1, eu-west-1 and ap-northeast-1 only. Image agents outside the US need `image.allowCrossRegion`.
- **A CDK stack whose first create failed** is deleted and created again on the next deploy. A stack that ever deployed is never deleted this way.
- **A connected account cannot be verified.** Verifying shows the reason from AWS STS: no such role, a trust or External ID mismatch, or a service control policy. See [Set up AWS accounts](AWS_Accounts.md).
- **A grant fails at run time, not at deploy.** With a permissions boundary set, the boundary still caps every role. Widen the boundary or narrow the grant.
- **An S3 code source's bucket is refused.** In the control plane's own account, a build may read only the buckets on the control plane's allow list. Deploy into a connected account, or ask whoever deploys the control plane to list the bucket. See [Tool types in depth](Tool_Types.md).
- **A hosted MCP server fails to deploy.** Its code needs a `server.py` or a `Dockerfile` at its root, and every `requirements.txt` line pinned as `name==version`. See [Tool types in depth](Tool_Types.md).
- **Resources left from a failed deploy block the next one.** List them with `leftovers.sh`. See [Clean up](Clean_Up.md).

# Fix a knowledge base that returns nothing

S3 Vectors caps filterable metadata at 2,048 bytes per vector, so one large document can fail to ingest while the deploy succeeds. Check the last ingestion job. Expect `COMPLETE` and `numberOfDocumentsFailed: 0`:

```bash
KB=$(terraform output -raw knowledge_base_id)   # CDK: the knowledgeBaseId output
DS=$(aws bedrock-agent list-data-sources --knowledge-base-id $KB \
       --query 'dataSourceSummaries[0].dataSourceId' --output text)
aws bedrock-agent list-ingestion-jobs --knowledge-base-id $KB --data-source-id $DS \
  --query 'sort_by(ingestionJobSummaries,&startedAt)[-1]'
```

The index and knowledge base names carry a digest of their fixed properties. Changing the embedding model or dimension replaces them: expect a new knowledge base ID and a fresh ingest.

# Fix a tool that returns nothing

## An MCP tool looks empty

The MCP `tools/list` answer is paginated. A target whose tools are on page two looks empty if only page one is read. AgentExpress follows `nextCursor`; ad hoc scripts often do not. If a target really lists nothing, the agent raises `ToolUnavailable` and the run fails with that reason.

## An agent sees no rows from an API that returns data

The API nests its records somewhere the framework does not look. Set `rowPath` on the tool to the dot path of the list, and `rowFields` for its field names. See [Tool types in depth](Tool_Types.md).

## A tool call was refused

Open the run's **Timeline** in **Log** view. "Refused by Cedar policy: ..." means a Cedar rule denied it; "Refused by interceptor: ..." means an interceptor did. The **Flow** tab with the **Cedar policy** filter lists every decision. See [Policies (Cedar)](Policies_and_Cedar.md).

# Fix a run that goes wrong

- **The model returned no parseable JSON.** The agent's output was cut off by its `maxTokens` budget. Raise `maxTokens` for that agent and redeploy, or change it with live config.
- **A step asks you to Connect.** The step uses a tool that acts as you. Click **Connect**, sign in at the tool's provider, then **Run the step again**.

Where to look first:

1. The box at the top of the run page that says why the run stopped or ended.
1. The **Timeline** tab, **Log** view, filtered with **Find events**.
1. The **Flow** tab with **Problems only**.
1. The Observability page, **Prompts & I/O**, to see exactly what the agent was given and returned.

# Fix a destroy that fails

| Symptom | Fix |
|---|---|
| CDK: `DELETE_FAILED` on the orchestrator, `NotStabilized` | Run the destroy again: AgentCore was still draining the runtime. |
| CDK: `DELETE_FAILED` on a runtime, "Access denied for operation", after a `NotStabilized` retry | Run the destroy again: AgentCore had already deleted the runtime. |
| CDK: `DELETE_FAILED` on the UI logs bucket, "bucket not empty" | Empty the bucket, then destroy again. |
| Terraform: `NotStabilized` on the runtime, then "Invalid for_each argument" on the retry | Run `terraform apply -target=null_resource.ui_build`, then destroy again. |

A destroy started from the control plane retries these cases itself, up to 3 more times, on stacks of any size. You only see them when they happen on a stack you destroy yourself, or on the last attempt. More in [Clean up](Clean_Up.md).

# Related pages

- [FAQ](FAQ.md): short answers by topic.
- [Deploy a build](Deploying_a_Build.md): the Deployment panel and its log.
- [The run page and its tabs](Run_Page.md): the Timeline and Flow tabs.
- [Clean up](Clean_Up.md): teardown and leftovers.
