Share a build to let other people open it, change it and deploy it. Use it when a colleague should work on the same workflow with you, or should get their own login to its execution plane. You'll share a build, see what that grants, and decide when to share pieces or use a team instead.

# Share a build

1. Click **Share** in the build's header. The button says who it is shared with now, for example **Share · Only you**.
1. Under **People**, type email addresses, one per line.
1. Under **Groups**, pick groups an admin defined on the Admin page.
1. Or turn on **Everyone**.
1. Click **Save**.

The Share dialog for the example build:

![The Share dialog for Demo: everything in one run (CDK): a note that whoever you share with can do everything you can, People as email addresses one per line, Groups defined by an admin on the Admin page, an Everyone switch set to Only the people and groups above, and Cancel and Save](images/cp-share-build.png)

To stop sharing, open **Share** again, remove the people or groups, and click **Save**.

# Know what sharing grants

- Whoever you share with can do everything you can: open the build, change it, deploy it, share it on or delete it. You always keep access.
- They still need the `deploy` and `destroy` permissions of their own groups to deploy or destroy.
- Each of them gets their own login to the build's execution plane.
- They see the build's deployment **History**, including your deploys.
- A build shared with you shows **Shared by** and the owner's email on the **Share** button.

Until you share it, a build is private: someone without access gets "not found", as if it did not exist. An admin can read every build on the Admin page, read-only.

## When two people edit at once

Each change saves a moment after you stop typing. When two people save changes that started from the same version, the later save is refused, and the save line offers **Reload now** to load the other person's version. See [The control plane](Builder_Console.md).

# Share pieces instead of the whole build

To share one tool, skill, guardrail, policy or other piece rather than the whole build, publish it to the library and share it there. Every build that uses it then stays in step. See [Library](Library.md).

To move a build to another control plane, export it as a bundle and import it there. See [Export, edit and import](Export_and_Import.md).

# Use a team for a fixed group

Sharing suits a few people on one build. For a fixed group of people who work on many builds together, a team workspace may suit better: its members are owners, editors or viewers of every build in it. Move a build into a team with **Project → Move to team…**. See [Teams and workspaces](Teams.md).

# Pick the right way to share

| You want to | Use | What the other person can do |
|---|---|---|
| Work on one build with a few colleagues | **Share** on the build | Everything you can, within their own groups' permissions |
| Work on many builds with a fixed group | A team workspace | What their team role allows: owner, editor or viewer |
| Reuse one tool, skill or rule in other builds | The library | Use the item live in their own builds |
| Show one run's result | A run's share link | Read that run only |

# Ask the Assistant

The AgentExpress Assistant can't share a build or change who it is shared with. Click **Share** in the build's header instead.

- To reuse a piece in other builds, ask it to add a library item someone shared with you, for example: "Add the shared policy documents knowledge base from the library to the research agent".

More requests are on [Ask the Assistant](Ask_the_Assistant.md).

# Related pages

- [The control plane](Builder_Console.md): the build header and saving
- [Library](Library.md): share pieces across builds
- [Teams and workspaces](Teams.md): owner, editor and viewer roles
- [Users, groups and RBAC](Users_Groups_and_RBAC.md): the groups you can share with
- [Share a run](Sharing_a_Run.md): a read-only link to one run
