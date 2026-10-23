# Share a run

The owner of a run can make a read-only link to it for a colleague, with an expiry. Use it to show someone a result, ask for a second opinion, or hand over a case, without giving them access to your other runs. You'll create a link, see what the person with it sees, and manage or revoke it.

# Create a link

1. Open the run and click **Share** at the top right of the run page.
1. Under **Expires after**, pick **1 hour**, **1 day**, **7 days** (the default) or **30 days**.
1. Click **Create link**, then copy the link from the **Link created** box.
1. Send the link to your colleague.

The Share this run dialog, with the expiry choices and the list of active links:

![Share this run dialog over a run page: text saying anyone signed in to this application who has the link can read this run until the link expires or you revoke it, read-only, without its cost, prompts or files; Expires after with 1 hour, 1 day, 7 days selected and 30 days; Create link; Active links (0 of 10) with Link, Created and Expires columns and No active links; Close](images/ep-share-run.png)

Only the run's owner makes and manages its links. An execution plane with no sign-in (`idp = "none"`) cannot make links.

# What the person with the link sees

- They must sign in to the same execution plane. A link is never anonymous.
- They see the run's request, steps, outputs and timeline, under a **Shared run** banner that names who shared it and when the link expires.
- They do not see its cost, tokens, prompts, the Flow tab or the files. Images an image agent drew do not load.
- On the Timeline chart they see the bars and waits, but not the calls inside them.
- They cannot approve, re-run, stop, evaluate or delete the run.
- An expired or revoked link shows "This link cannot be opened".

To let someone act on runs, they need their own access and groups. See [Users, groups and RBAC](Users_Groups_and_RBAC.md).

# Manage and revoke links

- **Active links** in the Share dialog lists the run's links, with when each was created and when it expires.
- **Revoke** ends a link at once, even on a page that is already open.
- A run can have at most 10 active links.
- Deleting the run ends its links.

Expiry and revocation are checked by the server on every read, so a link stops working at the next read after it expires.

# Send links with care

Anyone signed in to the same execution plane who has the link can open it. Treat a link like the run itself when you send it:

- Send it only to the people who should read the run.
- Pick the shortest expiry that works.
- Revoke a link as soon as it is no longer needed.

There is nothing to turn on: sharing needs no setting, resource or IAM permission. You share runs from each build's execution plane.

## A link or something else

| You want them to | Use |
|---|---|
| Read one run | A share link |
| Run the workflow themselves | Their own login to the execution plane, with the groups they need |
| Change or deploy the build | [Share a build](Sharing_a_Build.md) or a team |

# What is logged

Making, opening (by whom) and revoking a link are events in the run owner's Activity log. The log names a link by its first 6 characters, never the whole link. Opening is logged once an hour per viewer and link. See [Activity](Activity.md).

# Ask the Assistant

Share links are made by the run's owner in the execution plane, and there is nothing to set for them in the build. The AgentExpress Assistant can't make, list or revoke a link: use **Share** on the run page.

To let people act on runs instead of reading one, the Assistant can set who may do what in the execution plane, for example:

- "Only the support group may start runs and decide gates"

All requests by feature: [Ask the Assistant](Ask_the_Assistant.md).

# Related pages

- [The run page and its tabs](Run_Page.md): what the shared view is based on.
- [Share a build](Sharing_a_Build.md): give someone the build itself, in the control plane.
- [Sign-in and security](Security.md): what stays private to each person.
- [Activity](Activity.md): the log of link events.
