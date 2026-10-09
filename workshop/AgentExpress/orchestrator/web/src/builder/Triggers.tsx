/** The Triggers tab: what starts a run without anyone typing it (orchestrator.triggers).
 *
 *  Designed here; once deployed, the build's own app has a Triggers page (admins) with
 *  each webhook's URL, its secret and a "Send test event" button (bff/triggers.py). */
import Alert from "@cloudscape-design/components/alert";
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import Checkbox from "@cloudscape-design/components/checkbox";
import ColumnLayout from "@cloudscape-design/components/column-layout";
import Container from "@cloudscape-design/components/container";
import FormField from "@cloudscape-design/components/form-field";
import Header from "@cloudscape-design/components/header";
import Input from "@cloudscape-design/components/input";
import Select from "@cloudscape-design/components/select";
import SpaceBetween from "@cloudscape-design/components/space-between";
import Textarea from "@cloudscape-design/components/textarea";
import Toggle from "@cloudscape-design/components/toggle";
import { useState } from "react";

import { IssueList } from "./IssueList";
import type { Json, Project, Workflow } from "./model";
import { TRIGGER_NAME_RE, type Issue } from "./validate";

export type Trigger = Record<string, Json>;

const TYPES: { value: string; label: string; description: string; starter: Trigger }[] = [
  { value: "webhook", label: "Webhook", description: "A signed POST from GitHub, Jira, ServiceNow, Slack, Stripe or your own system.",
    starter: { type: "webhook", signature: "agentexpress", prompt: "Handle this request: {{body}}" } },
  { value: "schedule", label: "Schedule", description: "A cron or rate expression (EventBridge Scheduler).",
    starter: { type: "schedule", expression: "cron(0 8 ? * MON-FRI *)", timezone: "UTC", prompt: "Write today's summary" } },
  { value: "eventbridge", label: "EventBridge event", description: "An event pattern: AWS services, your own applications, SaaS partners.",
    starter: { type: "eventbridge", pattern: { source: ["aws.cloudwatch"], "detail-type": ["CloudWatch Alarm State Change"] },
      prompt: "Investigate alarm {{detail.alarmName}}: {{detail.state.reason}}" } },
  { value: "s3", label: "S3 object", description: "An object created in a bucket (send the bucket's events to EventBridge).",
    starter: { type: "s3", bucket: "", prefix: "incoming/", prompt: "Review the new file {{detail.object.key}}" } },
  { value: "sqs", label: "SQS message", description: "A message on a queue: yours, or one made for you with a dead-letter queue.",
    starter: { type: "sqs", prompt: "Handle this message: {{body}}" } },
];
const SIGNATURES = ["agentexpress", "github", "slack", "stripe", "token"].map((v) => ({ value: v, label: v }));

export function triggersOf(wf: Workflow): Record<string, Trigger> {
  const orch = (wf.orchestrator ?? {}) as Record<string, unknown>;
  const t = orch.triggers;
  return t && typeof t === "object" && !Array.isArray(t) ? t as Record<string, Trigger> : {};
}

export function setTrigger(project: Project, name: string, t: Trigger | undefined): Project {
  const orch = { ...((project.workflow.orchestrator ?? {}) as Record<string, Json>) };
  const next = { ...triggersOf(project.workflow) };
  if (t) next[name] = t; else delete next[name];
  if (Object.keys(next).length) orch.triggers = next as Json; else delete orch.triggers;
  return { ...project, workflow: { ...project.workflow, orchestrator: orch } };
}

function JsonField({ label, description, value, onChange }: {
  label: string; description: string; value: Json | undefined; onChange: (v: Json) => void;
}) {
  const [text, setText] = useState(JSON.stringify(value ?? {}, null, 1));
  const [err, setErr] = useState<string | null>(null);
  return (
    <FormField label={label} description={description} errorText={err} stretch>
      <div className="axp-cedar"><Textarea rows={4} value={text} spellcheck={false} ariaLabel={label}
        onChange={({ detail }) => setText(detail.value)}
        onBlur={() => { try { onChange(JSON.parse(text)); setErr(null); } catch (e) { setErr(`Not valid JSON: ${(e as Error).message}`); } }} />
      </div>
    </FormField>
  );
}

function TriggerForm({ name, t, onChange, onRemove }: {
  name: string; t: Trigger; onChange: (t: Trigger) => void; onRemove: () => void;
}) {
  const set = (k: string, v: Json | undefined) => {
    const { [k]: _old, ...rest } = t;
    onChange(v === undefined || v === "" ? rest : { ...rest, [k]: v });
  };
  const str = (k: string) => (typeof t[k] === "string" ? t[k] as string : "");
  const type = String(t.type);
  const def = TYPES.find((x) => x.value === type);
  return (
    <Container header={<Header variant="h3" description={def?.description}
      actions={<SpaceBetween direction="horizontal" size="xs">
        <Toggle checked={t.enabled !== false} onChange={({ detail }) => set("enabled", detail.checked ? undefined : false)}>
          {t.enabled === false ? "Off" : "On"}</Toggle>
        <Button variant="inline-link" ariaLabel={`Remove ${name}`} onClick={() => {
          if (window.confirm(`Remove the trigger ${name}?`)) onRemove();
        }}>Remove</Button>
      </SpaceBetween>}>{name} · {def?.label ?? type}</Header>}>
      <SpaceBetween size="s">
        <FormField label="Request" stretch
          description="The run's request. {{placeholders}} are filled from the delivery: {{body.x}} and {{headers.x}} (webhook, SQS), {{detail.x}} and {{event.x}} (EventBridge, S3), {{time}} (schedule). The delivery is untrusted text: it never runs as code.">
          <Textarea rows={3} value={str("prompt")} ariaLabel={`${name} request`} onChange={({ detail }) => set("prompt", detail.value)} />
        </FormField>
        {type === "webhook" ? (
          <FormField label="Signature" description="How the sender proves it is them: ours (X-AX-Signature), GitHub's, Slack's, Stripe's, or a shared token header (X-AX-Token) for a sender that cannot sign.">
            <Select selectedOption={SIGNATURES.find((s) => s.value === (str("signature") || "agentexpress"))!} options={SIGNATURES}
              onChange={({ detail }) => set("signature", detail.selectedOption.value === "agentexpress" ? undefined : detail.selectedOption.value)} />
          </FormField>
        ) : null}
        {type === "schedule" ? (
          <ColumnLayout columns={2}>
            <FormField label="Expression" description="cron(minutes hours day month weekday year) or rate(5 minutes).">
              <Input value={str("expression")} onChange={({ detail }) => set("expression", detail.value.trim())} />
            </FormField>
            <FormField label="Time zone" description="IANA, e.g. Europe/Paris. Default UTC.">
              <Input value={str("timezone")} placeholder="UTC" onChange={({ detail }) => set("timezone", detail.value.trim())} />
            </FormField>
          </ColumnLayout>
        ) : null}
        {type === "eventbridge" ? (
          <>
            <JsonField label="Event pattern" value={t.pattern} onChange={(v) => set("pattern", v)}
              description='What to listen for, e.g. {"source": ["aws.guardduty"]} or a SaaS partner source.' />
            <FormField label="Event bus" description="Its name or ARN (a custom or partner bus). Default: the default bus.">
              <Input value={str("bus")} placeholder="default" onChange={({ detail }) => set("bus", detail.value.trim())} />
            </FormField>
          </>
        ) : null}
        {type === "s3" ? (
          <ColumnLayout columns={2}>
            <FormField label="Bucket" description="It must send its events to EventBridge (bucket properties, Amazon EventBridge: on).">
              <Input value={str("bucket")} onChange={({ detail }) => set("bucket", detail.value.trim())} />
            </FormField>
            <FormField label="Prefix" description="Only objects under it. The object is attached when orchestrator.attachments.s3 allows its bucket.">
              <Input value={str("prefix")} placeholder="incoming/" onChange={({ detail }) => set("prefix", detail.value.trim())} />
            </FormField>
          </ColumnLayout>
        ) : null}
        {type === "sqs" ? (
          <FormField label="Queue ARN" description="Yours (its visibility timeout above 300 s), or empty for a queue the deployment makes, with a dead-letter queue.">
            <Input value={str("queueArn")} placeholder="arn:aws:sqs:us-east-1:123456789012:orders"
              onChange={({ detail }) => set("queueArn", detail.value.trim())} />
          </FormField>
        ) : null}
        <ColumnLayout columns={3}>
          <FormField label="Runs as" description="The build's owner, or the trigger itself (its runs are read and decided by its approvers).">
            <Select selectedOption={{ value: str("runAs") || "owner", label: str("runAs") === "service" ? "The trigger (service)" : "The build's owner" }}
              options={[{ value: "owner", label: "The build's owner" }, { value: "service", label: "The trigger (service)" }]}
              onChange={({ detail }) => set("runAs", detail.selectedOption.value === "owner" ? undefined : detail.selectedOption.value)} />
          </FormField>
          <FormField label="Review gates" description="Approve every gate automatically for runs this trigger starts (logged).">
            <Checkbox checked={t.gates === "auto"} onChange={({ detail }) => set("gates", detail.checked ? "auto" : undefined)}>
              Approve automatically</Checkbox>
          </FormField>
          <FormField label="Most runs an hour" description="Default 60. Deliveries past it start nothing.">
            <Input type="number" value={t.maxRunsPerHour === undefined ? "" : String(t.maxRunsPerHour)} placeholder="60"
              onChange={({ detail }) => set("maxRunsPerHour", detail.value === "" ? undefined : Number(detail.value))} />
          </FormField>
        </ColumnLayout>
        {str("runAs") === "service" ? (
          <FormField label="Approvers" description="Groups that read and decide this trigger's runs in the app, comma-separated.">
            <Input value={Array.isArray(t.approvers) ? (t.approvers as string[]).join(", ") : ""} placeholder="reviewers"
              onChange={({ detail }) => set("approvers", detail.value.split(/[\s,]+/).filter(Boolean))} />
          </FormField>
        ) : null}
        <ColumnLayout columns={2}>
          <FormField label="Bring the delivery" description="Attach it to the run as payload.json, for agents that read files.">
            <Checkbox checked={t.attachPayload === true} onChange={({ detail }) => set("attachPayload", detail.checked || undefined)}>
              Attach payload.json</Checkbox>
          </FormField>
          <FormField label="Same delivery key" description="A template; a key seen in the last day starts no second run. Default: the delivery's own id.">
            <Input value={str("idempotencyKey")} placeholder="{{headers.x-request-id}}" onChange={({ detail }) => set("idempotencyKey", detail.value)} />
          </FormField>
        </ColumnLayout>
      </SpaceBetween>
    </Container>
  );
}

export function BuildTriggers({ project, setProject, issues }: {
  project: Project; setProject: (p: Project) => void; issues: Issue[];
}) {
  const triggers = triggersOf(project.workflow);
  const [name, setName] = useState("");
  const [type, setType] = useState(TYPES[0].value);
  const mine = issues.filter((i) => i.path.startsWith("orchestrator.triggers"));
  const valid = TRIGGER_NAME_RE.test(name) && !(name in triggers);
  return (
    <SpaceBetween size="l">
      <Box color="text-body-secondary">
        Start a run without anyone typing the request: from a webhook, a schedule, an EventBridge event, a new S3 object
        or an SQS message. Once deployed, the app&apos;s Triggers page has each webhook&apos;s URL and secret, and sends test events.
      </Box>
      {mine.length ? <IssueList issues={mine} /> : null}
      <Container header={<Header variant="h2">Add a trigger</Header>}>
        <SpaceBetween direction="horizontal" size="xs">
          <Input value={name} placeholder="jiraTickets" ariaLabel="Trigger name" onChange={({ detail }) => setName(detail.value.trim())} />
          <Select selectedOption={{ value: type, label: TYPES.find((t) => t.value === type)!.label }} ariaLabel="Trigger type"
            options={TYPES.map((t) => ({ value: t.value, label: t.label, description: t.description }))}
            onChange={({ detail }) => setType(String(detail.selectedOption.value))} />
          <Button disabled={!valid} onClick={() => {
            setProject(setTrigger(project, name, structuredClone(TYPES.find((t) => t.value === type)!.starter)));
            setName("");
          }}>Add</Button>
        </SpaceBetween>
        {name && !valid ? <Box color="text-status-error" variant="small">
          Letters and digits, starting with a letter, at most 32, and not already used.</Box> : null}
      </Container>
      {Object.keys(triggers).length ? Object.entries(triggers).map(([n, t]) => (
        <TriggerForm key={n} name={n} t={t} onChange={(next) => setProject(setTrigger(project, n, next))}
          onRemove={() => setProject(setTrigger(project, n, undefined))} />
      )) : <Alert type="info">No triggers: runs start when someone types a request.</Alert>}
    </SpaceBetween>
  );
}
