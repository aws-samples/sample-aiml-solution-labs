# AWS Agent Registry

The AWS Agent Registry is your organisation's catalogue of approved agents and tools: MCP servers, A2A agents and skills. Use it to build with pieces a curator has already approved instead of wiring them up yourself, and, as an admin, to publish a deployed build back for others to find. You'll see which registry the control plane uses, add records to a build, keep them in sync and, as an admin, publish.

# Know which registry you use

The control plane works with the registries in its own AWS account and Region. There is nothing to configure.

- With one registry, it is used without asking.
- With several, you pick one under **Registry**.
- A control plane with none says _No Agent Registry here_.

# Add records from the registry

**Add from registry** is on the **Tools** tab and the **Skills** tab, and the canvas palette has **Add an agent from the registry**.

1. Click **Add from registry** (or **Add an agent from the registry**).
1. Type what you are looking for and click **Search**. Only approved records are listed.
1. Pick the records to add. The **Becomes** column says what each will be in the build.
1. Tick **Keep in sync** if the build should follow newer approved versions.
1. Click **Add (n)**.

| Record | Becomes | Then |
|---|---|---|
| MCP server | An MCP tool with its endpoint and its list of tools | Set **How it connects**. A record carries no credentials. |
| A2A agent card | A remote (A2A) agent | Drag it onto a stage. |
| Skill (SKILL.md) | A skill | Pick it in an agent's **Skills**. |
| A published AgentExpress workflow | Nothing: it is listed with the reason | It runs as its own execution plane and serves no A2A endpoint. |

You can also ask the AgentExpress Assistant to find and import a record: name it or describe it, and it searches the registry. See [Build with the AgentExpress Assistant](AgentExpress_Assistant.md).

# Keep records in sync

- **With Keep in sync**, the item is brought to each newer approved version when the build is opened and before each deploy. A notice names what was updated.
- **Without it**, the item stays as you took it. When a newer approved version exists, the build page shows _Newer versions in the registry_. Click **Update** next to an item to take it, or close the message.

# Publish to the registry

Publishing speaks for the organisation, so only admins (people with the `admin` action) can do it.

- **A deployed build**: **Publish to registry** on the Deployment panel. It submits an agent record describing the workflow and its execution plane, and an MCP record for its Gateway's tools.
- **Skills**: **Publish to registry** on the Skills tab, then pick the skills.

Then click **Submit for approval**.

- Records wait for a curator to approve them. The control plane never approves its own.
- Publishing again submits the next version of the same record, and the registry keeps serving the approved one meanwhile.
- Destroying the build deprecates its records.
- A name and version another build already published is refused. Rename the skill, or publish from the build that owns the record.

The AgentExpress Assistant cannot publish. Who counts as an admin is on [Users, groups and RBAC](Users_Groups_and_RBAC.md).

# Choose the registry or the library

|  | AWS Agent Registry | Library |
|---|---|---|
| Holds | Approved MCP servers, A2A agents and skills | Tools, skills, memories, guardrails, identities, policies, interceptors and evaluators |
| Lives in | The control plane's AWS account and Region | The control plane |
| Adding from AgentExpress | Admins submit records; a curator approves them | Anyone makes items for themselves and shares them |
| How a build uses it | Imports a record, then follows newer approved versions with **Keep in sync** | Uses the item live, until you make a copy |
| Carries secrets | No | No |

# Ask the Assistant

The AgentExpress Assistant can search the registry for approved MCP servers, agents and skills, add them to the build, bring them to their newest approved version, and turn **Keep in sync** on or off. It asks whether to keep an item in sync when you don't say. Requests you can paste:

- "Find an approved MCP server for our ticketing system in the registry and give it to the support agent"
- "Add the vendor credit-check agent from the registry after the intake stage, and keep it in sync"
- "Update the imported skills to their newest approved versions"
- "Stop keeping the creditCheck tool in sync with the registry"

* An imported MCP tool comes without sign-in. It asks how the tool connects, and you enter any key on the **Secrets** card beside the chat.
* It can't publish. An admin uses **Publish to registry** on the Deployment panel, or on the Skills tab for skills.

More requests are on [Ask the Assistant](Ask_the_Assistant.md).

# Related pages

- [Library](Library.md): pieces you and your colleagues share
- [Add tools and connectors](Adding_Tools.md): How it connects, for an imported MCP tool
- [Runtimes](Runtimes.md): A2A agents
- [Skills](Skills.md): how agents use an imported skill
