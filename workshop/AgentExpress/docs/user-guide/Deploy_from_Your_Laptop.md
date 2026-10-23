# Deploy from your laptop

You can deploy a build without the control plane: write its bundle into a checkout of the framework on your laptop and deploy it with AWS CDK or Terraform. Both create the same resources a deploy from the control plane creates. You'll check what you need, run the commands for your tool, pass secrets, and bring the build back into AgentExpress with **Import a file…**. Use it when your account rules require deploying from your own pipeline, or when you want the build under version control.

# What you need

- A clone of the framework's code repository.
- A container engine that builds `linux/arm64` images: Finch, Docker or Podman.
- Node.js 20 or later.
- AWS credentials for the target account, and Bedrock model access for the models the build uses.
- The build's bundle, exported with **Export → Bundle (workflow, prompts and code)**. See [Export, edit and import](Export_and_Import.md).

# Write the bundle into the checkout

```bash
cd orchestrator
python3 scaffold.py apply ~/Downloads/my-workflow.agentexpress.json --dry-run
python3 scaffold.py apply ~/Downloads/my-workflow.agentexpress.json --exact
```

`--dry-run` shows what would change and writes nothing. `--exact` makes the tree mirror the bundle, and deletes agent, tool and `kb_docs/` folders the bundle does not name. Put any knowledge base documents under `kb_docs/<corpus>/`: the bundle does not carry them. More in [Code bundle and a coding agent](Code_Bundle_and_Coding_Agent.md).

# Pass the secrets

Secrets never go in a file. Pass them in the environment as JSON maps, keyed by tool, identity or agent id:

| Secret | CDK | Terraform |
|---|---|---|
| Tool API keys | `TOOL_API_KEYS` | `TF_VAR_tool_api_keys` |
| Identity secrets (OAuth client secret or API key) | `IDENTITY_SECRETS` | `TF_VAR_identity_secrets` |
| Bearer tokens for A2A agents | `A2A_TOKENS` | `TF_VAR_a2a_tokens` |
| Your own M2M Gateway client secret | `GATEWAY_CLIENT_SECRET` | `TF_VAR_gateway_client_secret` |

For example: `export TF_VAR_tool_api_keys='{"internalTools":"your-key"}'`. Plan or synth fails when an identity in use has no secret.

# Deploy with CDK

From `orchestrator/cdk`:

```bash
npm install
export CDK_DOCKER=finch        # only if you use Finch instead of Docker
npx cdk bootstrap              # once per account and Region
npx cdk deploy -c agentName=my_workflow -c idp=cognito -c createCognito=true \
  -c enableGateway=true -c builder=false
```

- `builder=false` deploys the build's execution plane without a control plane alongside it, as a deploy from the control plane does.
- Without `enableGateway=true` there is no Gateway, knowledge base or policy engine, and agents with tools fail with `ToolUnavailable`.
- The stack is named from `agentName` with dashes. Outputs include `uiUrl` (the execution plane address) and `cognitoUserPoolId`.

# Deploy with Terraform

From `orchestrator/terraform`:

1. Create remote state once per account: `./bootstrap-state.sh`.
1. Run `cp terraform.tfvars.example terraform.tfvars` and set at least `agent_name`, `region`, `idp = "cognito"`, `cognito = { create = true }`, `enable_gateway = true` and `enable_builder = false`.
1. Apply. The targeted apply is needed only on the first apply of a new stack.

```bash
terraform apply -target=null_resource.ui_build
terraform apply
```

Outputs include `ui_url` (the execution plane address) and `cognito_user_pool_id`. `terraform validate` (or `cdk synth`) checks the workflow before a deploy.

# Sign in to the execution plane

With a Cognito pool the stack created, nobody has a login yet. Create a user and add their groups with `aws cognito-idp admin-create-user` and `admin-add-user-to-group`, then open the `uiUrl` or `ui_url` output. See [Users, groups and RBAC](Users_Groups_and_RBAC.md).

# Bring it back into AgentExpress

To continue in the control plane:

1. Open a build in the control plane, or create one.
1. Open **Project → Import a file…**.
1. Choose **As a new build**, or **Into this build, replacing it**.
1. Click **Choose file…** and pick the bundle, or the `workflow.json` from the checkout.
1. Enter the build's secrets in the control plane, then click **Deploy**.

A deploy from the control plane creates the build's own stack, named from its `ax_` id. The stack you deployed from your laptop is separate: remove it yourself when you no longer need it, with `cdk destroy` or `terraform destroy`. See [Clean up](Clean_Up.md).

# Ask the Assistant

The AgentExpress Assistant can't run CDK, Terraform or `scaffold.py`, and it can't deploy. Run the commands above yourself.

- Before you export, it can fix the build's problems and tell you what still needs your input: **Bundle** is greyed out while the build has errors. For example: "Fix the problems on this build, and tell me what still needs my input".
- It never puts secrets in the build. Pass them in the environment, as above.

More requests are on [Ask the Assistant](Ask_the_Assistant.md).

# Related pages

- [Code bundle and a coding agent](Code_Bundle_and_Coding_Agent.md): edit the bundle before you deploy it.
- [workflow.json settings](Workflow_JSON_Reference.md): the keys checked by plan and synth.
- [Deploy a build](Deploying_a_Build.md): the control plane's own deploy.
- [Clean up](Clean_Up.md): destroy, teardown.sh and leftovers.sh.
- [Troubleshooting](Troubleshooting.md): fixes for common deploy problems.
