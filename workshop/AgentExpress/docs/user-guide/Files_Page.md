# Files

A run has files: the files it started with, the files its agents saved, and the code the Code Interpreter sandbox ran. The **Files** page lists the files of all your runs in one place. Each run's **Files** tab lists the files of that run. You'll find, preview and download files, see who else can read them, and, if you set up the build, choose how long they are kept.

# Open the Files page

Click **Files** in the execution plane's left pane. It lists what your runs started with and what their agents made, newest runs first.

The Files page with **Mine** selected:

![Files (21) page with Mine selected and Everyone: a Find by name, agent or run box; rows for claim-report….md (Output), coder….py and .log (Code), claim-facts.json (Output), intake….py and .log (Code) and invoice.pdf (Input), each with its Run link, Agent, Size and Time](images/ep-files-page.png)

| Column | What it shows |
|---|---|
| **Name** | The file name |
| **Kind** | **Input**, **Output** or **Code** |
| **Run** | The run's request. Click it to open the run on its **Files** tab. |
| **Agent** | The agent that saved it, or a dash for an input |
| **Size**, **Time** | Its size, and when it was saved |
| **Actions** | **Preview** or **Open**, and **Download** |

- **Find by name, agent or run** filters the list.
- The page shows ten runs at a time. **Show older runs** reads more.
- Admins can switch **Mine** to **Everyone**. Viewing everyone's files is recorded in the admin's Activity log.

# Use a run's Files tab

On a run page, the **Files** tab groups the run's files in three tables.

The Files tab of one run:

![A run's Files tab: Inputs (invoice.pdf, 634 B, Open and Download), Outputs (claim-facts.json from Claim intake and claim-report….md from Claim report, Preview and Download), and Code (.log and .py files from Claim intake and Cover calculator, Preview and Download)](images/ep-run-files.png)

| Group | Holds |
|---|---|
| **Inputs** | The files the run was started with, attached or named in S3 |
| **Outputs** | Files an agent saved, and images an image agent drew |
| **Code** | Code the Code Interpreter sandbox ran, and a .log of what it printed |

# Preview and download

- **Download** saves the file.
- **Preview** shows text, Markdown, JSON and CSV in the page. CSV shows the first 200 rows, as a table. Text previews show the first 256 KB.
- **Open** opens an image or a PDF in a new tab.

Only images and PDFs open in the browser. Every other file is a download, so an agent's .html file is never served as a page.

# Who sees a file

- Whoever may read the run: its owner, an admin, or a service trigger's approvers.
- A shared run link shows no files.
- A build deployed from the control plane lists its runs' files in the build's own execution plane.

# Turn on saved files

Agents save files, and the sandbox's code is kept, when the builder turns on `orchestrator.files` in `workflow.json` (`{}` is enough):

```json
"orchestrator": { "files": { "retentionDays": 90 } }
```

- Files under the run folders are deleted after `retentionDays` (1 to 3650, default 90).
- An agent can save a file of up to 25 MB.
- The same file name saved twice in one run replaces the first.
- Without `orchestrator.files`, inputs and images still show on the tab, with no expiry.

See [Settings tab](Settings.md) to set it in the control plane.

# Ask the Assistant

In the control plane, the AgentExpress Assistant can turn on saved files, set how long they're kept, and add steps that write files. Requests you can paste:

- "Keep the files runs make for 30 days"
- "Add a last step that writes the claim report as a Markdown file"
- "Write the reviewed claims as a CSV file after the review stage"

What it can't do here:

- List, preview or download files, or switch to everyone's files. Use this page or the run's **Files** tab in the execution plane.
- Deploy. New settings apply once you choose **Deploy**.

All requests by feature: [Ask the Assistant](Ask_the_Assistant.md).

# Related pages

- [Start a run: prompt, file or trigger](Starting_a_Run.md): attach files or name them in S3.
- [The run page and its tabs](Run_Page.md): the run's other tabs.
- [Tool types in depth](Tool_Types.md): the Code Interpreter that writes the code files.
- [Sign-in and security](Security.md): what stays private to each person.
