# Export to Langfuse, Datadog and others

Each run can be sent, when it ends, to the observability tool your team already uses: Langfuse, Datadog, Grafana Cloud, Honeycomb, or any backend that accepts OTLP over HTTP. Export is off until a build turns it on. If you set up a build, you'll pick a destination and set it up on **Settings → Export** in the control plane. If you run it, you'll re-send runs and, as an admin, pause sending in the execution plane.

> [!NOTE]
> DEPLOYMENT.md notes that the export is checked by unit tests and is pending a live test by the owner. Try it on a test build first.

# Pick a destination

| Tool | `destination` | Where | Key |
|---|---|---|---|
| Langfuse | `langfuse` | `region`: `eu` (default), `us`, or `self-hosted` with your host | Public and secret key |
| Datadog | `datadog` | `region`: your Datadog site, default `datadoghq.com` | API key |
| Grafana Cloud | `grafana` | `endpoint`: your stack's OTLP endpoint | Instance ID and token |
| Honeycomb | `honeycomb` | `region`: `us` (default) or `eu` | Ingest API key |
| Any OTLP backend | `otlp` | `endpoint`: your collector's https address | Whatever headers it checks |

# Set it up in the control plane

A build sets up its export on the **Settings** tab, in the **Export** section.

1. Turn on **Send runs to another tool**.
1. Pick **Langfuse**, **Datadog**, **Grafana Cloud**, **Honeycomb** or **Any OTLP backend**.
1. Fill in only what that tool needs. Each field says where to find its value in the tool. For Langfuse, an optional project ID turns on **View in Langfuse** links.
1. Or turn on **Use an existing secret (ARN)** and give a Secrets Manager secret that holds the headers as a JSON object.
1. Click **Test connection**. The control plane sends one small test trace and says what the tool answered, for example "Langfuse accepted a test trace" or "Langfuse refused the key (HTTP 401)".
1. Choose what to send. Traces are always sent. Evaluation scores and metrics are on by default. **Prompts and answers** is off by default.
1. Click **Save**, then deploy the build.

The keys are saved as one of the build's write-only secrets, and the page never shows them again. After the deploy, the **Deployment** panel shows where runs go, for example "Export: Langfuse (EU)".

## Reuse a setup in another build

- Once a setup is saved, **Keep this setup in your library** appears under it. Type a name (a letter, then letters and digits, up to 32) and click **Save to library**.
- In another build, pick it under **Or use a saved setup** on **Settings → Export**. The destination, address and what to send are filled in.
- Keys are not copied. Enter this build's keys, then **Test connection** and **Save**. A setup that used an existing secret's ARN brings that ARN with it, so there are no keys to enter.

In `workflow.json` the setup is `orchestrator.export`:

```json
"orchestrator": { "export": { "destination": "langfuse", "region": "eu", "secret": "langfuse" } }
```

Good to know:

- The control plane's test needs outbound HTTPS from the control plane. It refuses a private address and allows at most 10 tests a minute per person.
- An existing secret's ARN can only be tested from the execution plane, with **Send test trace**.
- Deploying yourself, pass the headers in the environment: `TF_VAR_export_headers` (Terraform) or `EXPORT_HEADERS` (CDK).

# What is sent

One trace per run, sent once the run ends (done, failed, denied or cancelled). A run waiting at a gate is sent when it ends. Agents on their own runtimes are included.

- The run: run ID, user ID, workflow name and version, total tokens and cost.
- Each agent run, with a second span for a re-run.
- Each model call: model, provider, input and output tokens, cost, and why that model was chosen.
- Each tool call: tool, target, latency, status.
- Guardrail checks, Cedar decisions, memory reads and writes, and questions to a person, with their outcome.
- Evaluation scores (on by default). An evaluation run later sends its scores then.
- Metrics (on by default) to Datadog, Honeycomb and OTLP: runs, failures, duration, cost and tokens per agent and model. Not to Langfuse or Grafana Cloud.

The export runs after the run's final status is written. Each request waits at most 5 seconds and the whole export at most 20, so a failure never touches a run. There is no retry queue: send the run again instead.

# Keep prompts private

By default no prompt, answer, document, tool input or output, request or result is sent. The user ID (the sign-in name) and the figures above are sent.

Turning on **Prompts and answers** sends that content to a third party, under its retention and access rules, and it may hold personal data. Under the AWS shared responsibility model you decide whether that is allowed. Leave it off when in doubt. The key is kept in Secrets Manager and appears in no log, span or environment variable.

# Re-send and pause in the execution plane

Each run page shows whether the run reached the tool: **Sent**, **Failed**, **Off**, **Paused**, **Not sent**, or "Sends when it ends" while it is running.

- **View in Langfuse** or **View in Datadog** opens the run's trace. The Langfuse link needs the project ID. Grafana Cloud, Honeycomb and OTLP have no link.
- **Re-send to &lt;tool>** sends the run again with the same trace and span IDs, so Langfuse updates the trace instead of adding one. Metrics are not sent twice. The run's owner or an admin can re-send.

Admins see an **Export** card at the top of the **Observability** page with the destination, sent and failed counts for the last 24 hours and the last error, **Send test trace**, and **Pause** and **Resume** for the whole execution plane. Every one of these actions is in the Activity log.

# Ask the Assistant

In the control plane, the AgentExpress Assistant can set where runs go and what is sent. It never takes the keys. Requests you can paste:

- "Send every run to Langfuse in the US region, with evaluation scores and without prompts and answers"
- "Send runs to Honeycomb in the EU, and stop sending metrics"
- "Export runs to our OTLP collector, with the headers in the Secrets Manager secret &lt;its ARN>"

With **Review changes** on **Auto** or **Always**, a second model checks every export change before it's saved.

What it can't do here:

- Enter or read the keys, or click **Test connection**. Do that on **Settings → Export**.
- Keep a setup in your library or use a saved one. Use **Keep this setup in your library** and **Or use a saved setup** on **Settings → Export**.
- Re-send a run, or pause and resume sending. Use the run page and the **Export** card in the execution plane.
- Deploy. Runs start going to the tool once you choose **Deploy**.

All requests by feature: [Ask the Assistant](Ask_the_Assistant.md).

# Related pages

- [Settings tab](Settings.md): the Export section in the control plane.
- [Observability](Observability.md): the figures the export is built from.
- [One run in detail](Run_Traces.md): the same run inside the execution plane.
- [Sign-in and security](Security.md): what data leaves your account.
