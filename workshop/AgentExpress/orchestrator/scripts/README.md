# scripts/

## `e2e_smoke.py`: a scripted end-to-end release test

Runs a deployed AgentExpress app through one whole run (every review gate approved) and checks the
result and the telemetry. It works against any deployed app: it reads the agents from `GET /api/workflow`
and assumes no names. Python standard library only; `pycognito` is imported only when you sign in with a
user pool.

### Get a token

The user pool allows the SRP flow only, so there is no way to post a username and password.

* **Easiest:** sign in to the app in a browser, open the developer tools, and copy the ID token from any
  `/api/*` request's `Authorization: Bearer ...` header (not the access token). An ID token lasts one hour.
* **Or let the script sign in** with SRP. This needs `pycognito`, which is deliberately not in
  `requirements*.txt`:

  ```bash
  python3 -m venv /tmp/pc && /tmp/pc/bin/pip install pycognito==2024.5.1
  export AX_PASSWORD='...'          # read from the environment only; there is no password option
  /tmp/pc/bin/python scripts/e2e_smoke.py --url https://dxxxx.cloudfront.net \
      --pool-id us-east-1_AbCdEf --client-id 1abc23... --username you@example.com
  ```

### Run it

```bash
export AX_ID_TOKEN='eyJ...'         # safer than --id-token, which shows in `ps`
python3 scripts/e2e_smoke.py --url https://dxxxx.cloudfront.net            # whole run, then clean up
python3 scripts/e2e_smoke.py --url https://dxxxx.cloudfront.net --dry-run  # print the calls, make none
```

| Option | Meaning |
|---|---|
| `--url` | CloudFront URL or API base. `https` only (`http` for localhost). Only this address is ever called. |
| `--topic` | The request to run. Default: the workflow's own default topic. |
| `--timeout` | Seconds to wait for the run in all (default 1200). |
| `--approve-as` | A label written in the comment of each approval. It does not change who approves: the decision is made as the signed-in user, so that user needs the `decision` permission. |
| `--keep` | Leave the run in place (default: delete it). |
| `--dry-run` | Print what would be called, with no token, and call nothing. |
| `-v` | Log each request's method, path and status. Never a header or a body. |
| `--poll`, `--poll-max`, `--gate-grace`, `--max-approvals` | Polling starts at `--poll` seconds and backs off to `--poll-max`. A gate still waiting `--gate-grace` seconds after an approval is approved again, at most `--max-approvals` times, then the run counts as stuck. |

A run that times out or is stuck at a gate is cancelled, then deleted, so it does not keep spending.

### Exit codes

| Code | Meaning |
|---|---|
| `0` | Every check passed. Warnings (cost is zero, a limit is reached, not every node was reached) do not fail. |
| `1` | A check failed: the run did not reach `done`, an agent failed, a gate would not clear, telemetry is missing, or `/api/limits` is inconsistent. |
| `2` | It could not run: bad arguments, no credentials, sign-in refused (401/403), app unreachable, the URL is a Control Plane console, or the app refused to start the run (HTTP 429 from `orchestrator.limits`, reported with its reason and reset time). |

### What it checks

The app answers and who you are; the run reached `done`; every review gate cleared; every agent node
ended `done` or `skipped`; telemetry returned rows and a non-zero cost (zero is a warning); each finished
`main` or `dedicated` agent has a model-call row (`a2a` agents run in their own Lambda and are not
required to); if `/api/limits` says a limit is set, its figures are well formed, its `warn`/`reached`
flags agree with the numbers, the run's own cost and run count are in the counters; the run was deleted.

### What it does not check

* The quality of the agents' output: only that each step finished and was metered.
* Whether prices are right: only that the cost is not zero.
* Other gate decisions: it approves every gate, so `revise`, `deny`, timeouts, event-based approval
  and triggers are outside it.
* Load, other users' access, or groups other than the signed-in user's.
* An agent a branch skipped is a warning, not a failure.
* Telemetry and limit counters are written by the runtime and can lag a little; if a check on them
  fails, run it again.

## `check_docs_links.py`: check the in-product documentation links

Checks every info popover's **Documentation** link (`web/src/docs/docsLinks.ts`): it renders each
popover and reads its link (vitest, so run `npm ci` in `web/` first), then checks the link's `#anchor`
names a section of its page. Not part of CI. Two targets, matching the two link shapes:

* `--target github`: the public user guide's Markdown (`<page>.md#<slug>`). Checks each slug against
  the headings of the local `docs/user-guide/*.md`, computed the way GitHub does. With `--live` it also
  fetches each page from github.com (public, no sign-in) and looks for GitHub's rendered id,
  `user-content-<slug>`, in the raw HTML, quoted or escaped in the page's JSON payload.
* `--target wiki` (the default): an XWiki copy of the guide (a base URL with `/bin/view/` in its path).
  Opens each page in Chromium and checks the element the `#H...` id names is there. Needs
  `pip install playwright==1.64.0 && playwright install chromium`.

```bash
python3 scripts/check_docs_links.py --target github
python3 scripts/check_docs_links.py --target github --live
python3 scripts/check_docs_links.py --target wiki --base-url https://wiki.example.com/bin/view/AgentExpress/ \
    --cookies ~/cookies.txt --signin-hint "sign in again to refresh ~/cookies.txt"
```

| Option | Meaning |
|---|---|
| `--target` | `wiki` (default) or `github`. |
| `--base-url` | The documentation base URL the deployment sets as `docsUrl` (or `$DOCS_URL`). `https` only. Required for `wiki`; for `github`, defaults to the deployment default in `orchestrator/docs_default.json`. |
| `--cookies` | `wiki`: a Netscape-format cookie file with a signed-in session, for a site that needs one. |
| `--signin-hint` | `wiki`: printed when a page lands on another host (a sign-in page): what refreshes the cookie file. |
| `--guide-dir` | `github`: the generated Markdown to check against (default `<repo>/docs/user-guide`). |
| `--live` | `github`: also check each page on github.com. |
| `--timeout` | Seconds per page (default 120). |

Exit `0`: every anchor found. `1`: an anchor or a page is missing (each is listed). `2`: it could not run
(bad arguments, a base URL of the other target's shape, no guide directory, no Playwright, the popovers
could not be read, or a page landed on a sign-in page).
