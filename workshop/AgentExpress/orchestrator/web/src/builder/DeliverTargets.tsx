/** v139 Track C deliver: the targets of a deliver step (task.kind "deliver",
 *  app/common/deliver.py), edited in its TaskPanel. Each target sends some of the run's
 *  files to S3, by email with Amazon SES, or to a webhook; checked as you type by
 *  taskCheck.ts checkDeliver. */
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import Container from "@cloudscape-design/components/container";
import FormField from "@cloudscape-design/components/form-field";
import Header from "@cloudscape-design/components/header";
import Input from "@cloudscape-design/components/input";
import Multiselect from "@cloudscape-design/components/multiselect";
import Select from "@cloudscape-design/components/select";
import SpaceBetween from "@cloudscape-design/components/space-between";
import Textarea from "@cloudscape-design/components/textarea";
import Toggle from "@cloudscape-design/components/toggle";

import { SecretField } from "./BuildResources";
import { vocab } from "./meta";
import type { Entry, Json } from "./model";
import { MAX_TARGETS, WHEN_OPS } from "./taskCheck";

const opt = (v: string, label = v) => ({ value: v, label });
const TYPE_LABELS: Record<string, string> = { s3: "Amazon S3", email: "Email (Amazon SES)", webhook: "Webhook" };
const list = (v: unknown) => (Array.isArray(v) ? v.map(String).join(", ") : "");
const split = (s: string) => s.split(",").map((x) => x.trim()).filter(Boolean);
/** A condition's value as typed: JSON when it parses (true, 3, ["a"]), else the text. */
function parseValue(s: string): Json {
  try { return JSON.parse(s) as Json; } catch { return s; }
}

export function DeliverTargets({ targets, before, errorAt, onChange, buildId }: {
  targets: Entry[]; before: string[]; errorAt: (path: string) => string | undefined;
  onChange: (targets: Entry[]) => void; buildId?: string;
}) {
  const set = (i: number, patch: Record<string, Json | undefined>) => {
    const next = targets.map((t, j) => {
      if (j !== i) return t;
      const t2: Entry = { ...t };
      for (const [k, v] of Object.entries(patch)) { if (v === undefined) delete t2[k]; else t2[k] = v; }
      return t2;
    });
    onChange(next);
  };
  const types = vocab("deliverTargetTypes");
  return (
    <SpaceBetween size="m">
      {targets.map((t, i) => {
        const p = `task.targets[${i}]`;
        const kind = String(t.type ?? "");
        const when = (t.when && typeof t.when === "object" && !Array.isArray(t.when) ? t.when : undefined) as Entry | undefined;
        const op = when ? WHEN_OPS.find((o) => o in when) ?? "equals" : "equals";
        return (
          <Container key={i} header={
            <Header variant="h3" actions={<Button onClick={() => onChange(targets.filter((_, j) => j !== i))}>Remove</Button>}>
              {`Target ${i + 1}${TYPE_LABELS[kind] ? ` — ${TYPE_LABELS[kind]}` : ""}`}
            </Header>}>
            <SpaceBetween size="s">
              <FormField label="Send to" errorText={errorAt(`${p}.type`)}>
                <Select selectedOption={kind ? opt(kind, TYPE_LABELS[kind] ?? kind) : null} placeholder="Pick where it goes"
                  options={types.map((v) => opt(v, TYPE_LABELS[v] ?? v))}
                  onChange={({ detail }) => onChange(targets.map((x, j) => (j === i ? { type: detail.selectedOption.value! } : x)))} />
              </FormField>
              {kind === "s3" ? (
                <FormField label="Bucket and prefix" errorText={errorAt(`${p}.uri`)}
                  description="s3://<bucket>/<prefix>/ (ending in /), or ${storage}/<prefix>/ for the build's storage. Each file goes to <prefix><run id>/<name>; the runtime may write only there.">
                  <Input value={String(t.uri ?? "")} placeholder="s3://amzn-s3-demo-bucket/reports/"
                    onChange={({ detail }) => set(i, { uri: detail.value || undefined })} />
                </FormField>
              ) : null}
              {kind === "email" ? (
                <SpaceBetween size="s">
                  <FormField label="SES identity ARN" errorText={errorAt(`${p}.identityArn`)}
                    description="A domain or address verified in Amazon SES. The runtime may send only from it. In the SES sandbox, every recipient must be verified too.">
                    <Input value={String(t.identityArn ?? "")} placeholder="arn:aws:ses:us-east-1:111122223333:identity/example.com"
                      onChange={({ detail }) => set(i, { identityArn: detail.value || undefined })} />
                  </FormField>
                  <FormField label="From address" errorText={errorAt(`${p}.fromAddress`)}>
                    <Input value={String(t.fromAddress ?? "")} placeholder="reports@example.com"
                      onChange={({ detail }) => set(i, { fromAddress: detail.value || undefined })} />
                  </FormField>
                  <FormField label="To" errorText={errorAt(`${p}.to`)}
                    description="Addresses separated by commas, at most 50. Only what you write here is sent to.">
                    <Input value={list(t.to)} placeholder="team@example.com"
                      onChange={({ detail }) => { const v = split(detail.value); set(i, { to: v.length ? v : undefined }); }} />
                  </FormField>
                  <FormField label="Subject" errorText={errorAt(`${p}.subject`)} description="Optional. It may hold {{session}} or {{outputs.<step>.<field>}}.">
                    <Input value={String(t.subject ?? "")} onChange={({ detail }) => set(i, { subject: detail.value || undefined })} />
                  </FormField>
                  <FormField label="Message" errorText={errorAt(`${p}.body`)} stretch description="Optional. The files are listed under it.">
                    <Textarea rows={3} value={String(t.body ?? "")} ariaLabel="Message"
                      onChange={({ detail }) => set(i, { body: detail.value || undefined })} />
                  </FormField>
                  <Toggle checked={t.attach !== false} onChange={({ detail }) => set(i, { attach: detail.checked ? undefined : false })}>
                    Attach the files (links instead when they are over 25 MB together)
                  </Toggle>
                  <FormField label="Also send to addresses from a step's output" errorText={errorAt(`${p}.recipientsFrom`)}
                    description="Optional, and off unless you set it: one placeholder such as {{outputs.<step>.emails}}. A model's output then chooses recipients.">
                    <Input value={String(t.recipientsFrom ?? "")} placeholder="Only the addresses above"
                      onChange={({ detail }) => set(i, { recipientsFrom: detail.value || undefined })} />
                  </FormField>
                </SpaceBetween>
              ) : null}
              {kind === "webhook" ? (
                <SpaceBetween size="s">
                  <FormField label="URL" errorText={errorAt(`${p}.url`)}
                    description="https only. Gets a JSON POST with each file's name, size and a link. Private and internal addresses are refused.">
                    <Input value={String(t.url ?? "")} placeholder="https://hooks.example.com/in"
                      onChange={({ detail }) => set(i, { url: detail.value || undefined })} />
                  </FormField>
                  <FormField label="Signing secret name" errorText={errorAt(`${p}.secret`)}
                    description="Optional. Signs the body as X-AX-Timestamp and X-AX-Signature (sha256=HMAC of <timestamp>.<body>).">
                    <Input value={String(t.secret ?? "")} placeholder="Not signed"
                      onChange={({ detail }) => set(i, { secret: detail.value || undefined })} />
                  </FormField>
                  {buildId && t.secret ? (
                    <SecretField buildId={buildId} kind="deliverSecrets" name={String(t.secret)} label="Signing secret"
                      description="Stored encrypted with this build and deployed to the runtime only; never shown again." />
                  ) : null}
                </SpaceBetween>
              ) : null}
              {kind === "email" || kind === "webhook" ? (
                <FormField label="Links last (minutes)" errorText={errorAt(`${p}.linkExpiresMinutes`)}
                  description="Default 60, at most 10080. A link also ends when the runtime's credentials do.">
                  <Input type="number" value={t.linkExpiresMinutes === undefined ? "" : String(t.linkExpiresMinutes)} placeholder="60"
                    onChange={({ detail }) => set(i, { linkExpiresMinutes: detail.value ? Number(detail.value) : undefined })} />
                </FormField>
              ) : null}
              {kind ? (
                <SpaceBetween size="s">
                  <FormField label="Files" errorText={errorAt(`${p}.files`)} description="Name patterns separated by commas, such as *.pdf. Empty sends every file the run saved.">
                    <Input value={list(t.files)} placeholder="Every file"
                      onChange={({ detail }) => { const v = split(detail.value); set(i, { files: v.length ? v : undefined }); }} />
                  </FormField>
                  <FormField label="Made by" errorText={errorAt(`${p}.producedBy`)} description="Only files these earlier steps saved. Empty: any step.">
                    <Multiselect ariaLabel="Made by" placeholder="Any step"
                      selectedOptions={(Array.isArray(t.producedBy) ? t.producedBy.map(String) : []).map((s) => opt(s))}
                      options={before.map((s) => opt(s))}
                      onChange={({ detail }) => { const v = detail.selectedOptions.map((o) => o.value!); set(i, { producedBy: v.length ? v : undefined }); }} />
                  </FormField>
                  <FormField label="Only when" errorText={errorAt(`${p}.when`)}
                    description="Optional. A field of an earlier output, e.g. outputs.check.passed equals true. Put the step after a gate to send only what a person approved.">
                    <SpaceBetween direction="horizontal" size="xs">
                      <Input ariaLabel="Condition field" value={String(when?.field ?? "")} placeholder="outputs.<step>.<field>"
                        onChange={({ detail }) => set(i, { when: detail.value ? { ...(when ?? {}), field: detail.value, ...(when ? {} : { equals: true }) } : undefined })} />
                      <Select ariaLabel="Condition" selectedOption={opt(op)} options={WHEN_OPS.map((o) => opt(o))} disabled={!when}
                        onChange={({ detail }) => set(i, { when: { field: String(when?.field ?? ""), [detail.selectedOption.value!]: when?.[op] ?? true } })} />
                      <Input ariaLabel="Condition value" disabled={!when} value={when && op in when ? JSON.stringify(when[op]).replace(/^"|"$/g, "") : ""}
                        onChange={({ detail }) => set(i, { when: { field: String(when?.field ?? ""), [op]: parseValue(detail.value) } })} />
                    </SpaceBetween>
                  </FormField>
                </SpaceBetween>
              ) : null}
            </SpaceBetween>
          </Container>
        );
      })}
      {targets.length < MAX_TARGETS ? (
        <Button iconName="add-plus" onClick={() => onChange([...targets, { type: "s3" }])}>Add a target</Button>
      ) : <Box variant="small" color="text-body-secondary">{`At most ${MAX_TARGETS} targets.`}</Box>}
    </SpaceBetween>
  );
}
