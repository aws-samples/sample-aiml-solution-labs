# Sign-in and security

Read this before you use AgentExpress beyond a sandbox. You'll choose how people sign in to the control plane and to every build's execution plane, see the organisation controls a deployment can carry, what stays private to each person, and how secrets are kept.

> [!WARNING]
> AgentExpress is a reference sample, not a hardened product. Review it before you use it beyond a sandbox, and replace the sample guardrail and Cedar rules with your own.

# Choose an identity provider

One setting, the identity provider (`idp`), drives three things at once: how people sign in to the control plane or execution plane, the token check on every API call, and the machine token agents use to call the AgentCore Gateway.

| Provider | What you set up | Groups come from |
|---|---|---|
| Amazon Cognito, created by the deployment (recommended) | Nothing: the stack creates the user pool, sign-in page and clients, and one group per group named in `authorization.actions`. | Cognito groups (`cognito:groups`) |
| Amazon Cognito, your own pool | The pool, a sign-in domain, a client for the web page, and the groups. | Cognito groups |
| Okta | An OIDC single-page application and a custom authorization server, with a groups claim in the ID token. | The `groups` claim |
| Microsoft Entra ID | A single-tenant application registration with the single-page application platform, and application roles assigned to people. | The `roles` claim |
| Auth0 | A single-page application, and a post-login Action that adds a namespaced roles claim. | Your namespaced claim |
| None (sandbox only) | Nothing. The web page and API are open to anyone with the address. | None: tools and restricted actions cannot be used |

## Signing in

- Cognito passwords need at least 12 characters with upper and lower case letters, a digit and a symbol. An authenticator application can be added as a second factor.
- A Cognito pool the deployment creates is admin-create-only by default. Self sign-up can be turned on.
- With Okta, Entra ID, Auth0 or your own pool, register the web address as an allowed sign-in redirect and sign-out address at the provider after the first deploy.
- If the provider refuses a sign-in, the _Could not sign you in_ page shows its reason.

## A build's own execution plane

Each deployed build has its own sign-in, chosen on its **Identity → Sign-in** tab: **Built in** (a Cognito user pool the deploy creates for that execution plane) or your **Okta**, **Microsoft Entra ID** or **Auth0**. With built-in sign-in, the owner is invited with a temporary password and put in every group of the execution plane's pool. Destroying the build removes its user pool and every login in it. See [Identity](Identity.md).

## Other ways in

- **Agents to tools.** Agents sign in to the Gateway as a machine, with a token pinned to their own client, never as a person.
- **Tools that act as the person.** A tool set to **User login** or **On behalf of user** calls the service with the signed-in person's own access.
- **Webhook triggers.** The one API route without a sign-in token. Each delivery must carry an HMAC signature made with the trigger's secret, within a freshness window, and is capped per hour.

# Organisation guardrails

For accounts whose rules require them, a deployment of the control plane can carry these controls. All are off by default. They are set by whoever deploys the control plane (Terraform variables or CDK context), not in a build.

| Control | What it does | Good to know |
|---|---|---|
| Permissions boundary | Puts a managed policy as the boundary on every IAM role the stack creates. | The control plane passes it to every build it deploys. A build in a connected account uses the policy of the same name there. |
| Role path | Creates every role under an IAM path. | Set it on a new deployment: changing it replaces every role. |
| Custom domain | Your own host name on CloudFront, with an ACM certificate from us-east-1, serving TLS 1.2 or later. | You create the DNS record. Builds keep their own CloudFront addresses. |
| AWS WAF | A web ACL on CloudFront: a per-IP rate limit and AWS managed rule groups. | us-east-1 stacks only. |
| Request guard | Every request the runtime makes to an address from the workflow is checked: HTTPS only, and no private, loopback or link-local addresses. | Always on. |

Details are in the "Organisation guardrails" section of `DEPLOYMENT.md` in the code repository.

## Built in for every deployment

- Cedar default-deny at the Gateway, on every tool call, so an injected prompt cannot widen tool access. See [Policies (Cedar)](Policies_and_Cedar.md).
- RBAC on run and build actions, with the assistants held to the same rules.
- Scoped IAM roles per component; connected-account roles without AdministratorAccess.
- No secrets in the repository; runtimes read theirs from AWS Secrets Manager.
- Point-in-time recovery on tables, encrypted TLS-only buckets, API access logs and throttling, CloudFront security headers, pinned dependencies and an immutable image repository.

## Harden before production

- Restrict who holds `deploy` and `destroy`: the deploy project can create IAM roles.
- Add a custom domain and WAF; turn on bucket versioning and retention for real data.
- Replace the sample guardrail and Cedar rules.
- Lower how much of each prompt and output telemetry keeps if inputs are sensitive.
- Turn off the Run Assistant's action tools if it should not act.
- Treat model, MCP and knowledge-base output as untrusted.

# What stays private

| Thing | Who can see it |
|---|---|
| Your builds | You, people you share with, your team (by role), and admins (read-only) |
| Your runs | You and admins (read-only). A run a service trigger started belongs to the trigger, and its approver groups read and decide it. |
| Library items | You, until you share them |
| Connected AWS accounts | Only you |
| Secrets (API keys, client secrets, tokens) | Nobody: write-only, stored in AWS Secrets Manager, never shown again |
| Your activity | You, and people with `audit` |
| A shared run link | People signed in to the same execution plane who have the link, read-only, until it expires or is revoked |

Someone without access to a build or run gets "not found", as if it did not exist. There is no tenant boundary beyond this: one deployment serves everyone who can sign in to it.

## Data in runs

- Telemetry stores prompts and outputs, so observability can show them.
- One user's long-term memory never feeds another user's runs.
- Files uploaded for a run and not used within a day are deleted.
- Sending runs to another tool sends prompts and answers only if you turn that on. It is off by default.
- A guardrail's PII filters are a control, not a compliance programme. Obligations under HIPAA, PCI DSS or GDPR remain yours under the AWS shared responsibility model.

# Keep secrets out of files

- Secrets never go in `workflow.json`, an exported bundle or a library item.
- Enter them in write-only fields: the Assistant's **Secrets** card, the Identity tab, or the tool's settings.
- A tool's API key is kept in an AgentCore credential provider and sent by the Gateway. The agent never sees it.
- The activity log records that a secret changed, by name only.
- When you deploy a bundle yourself, pass secrets as JSON maps in the environment. See [Deploy from your laptop](Deploy_from_Your_Laptop.md).

# Ask the Assistant

The AgentExpress Assistant can set who signs people in to a build's execution plane (`authorization.signIn`: Cognito, or your Okta, Entra ID or Auth0) and the groups claim. It can also explain how the build signs in to its tools and which secrets it needs. Requests you can paste:

- "Sign people in to the execution plane with our Okta, and ask me for the domain and client ID"
- "Switch sign-in to Entra ID and read groups from the roles claim"
- "Which tools in this build act as the person, and which secrets does it still need?"

* Secrets never go through the chat. It names each secret it needs on the **Secrets** card beside the chat; enter the value there, on the Identity tab, or in the tool's settings. If you paste one anyway, it won't repeat it and asks you to rotate it.
* It can't set organisation guardrails such as a permissions boundary, role path, custom domain or WAF. Those are set by whoever deploys the control plane.
* With **Review changes** on **Auto**, a second model checks sign-in, identity, policy, network and secret changes before they are saved.

More requests are on [Ask the Assistant](Ask_the_Assistant.md).

# Related pages

- [Identity](Identity.md): a build's sign-in and outbound credentials.
- [Policies (Cedar)](Policies_and_Cedar.md): the default-deny rules at the Gateway.
- [Users, groups and RBAC](Users_Groups_and_RBAC.md): what each sign-in group may do.
- [Teams and workspaces](Teams.md): who shares which builds.
- [Cost and spend limits](Cost_and_Limits.md): caps on what people can spend.
