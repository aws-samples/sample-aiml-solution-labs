/** A Lambda tool whose function is written in the build (tools.<key>.code).
 *
 *  Its files (handler.py, requirements.txt, other *.py, events.json) live in the
 *  project's toolCode, beside the workflow like prompts, and travel with the build. Its
 *  grants are a fixed menu — a secret, a table, an S3 prefix, a VPC — and the framework
 *  builds its role (and a permissions boundary of the same) from them; nothing else.
 *  Before a deploy it is checked (syntax, lint, security) and run on its test events in
 *  an AgentCore Code Interpreter sandbox; after one, Test tool calls the real function. */
import Alert from "@cloudscape-design/components/alert";
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import ColumnLayout from "@cloudscape-design/components/column-layout";
import ExpandableSection from "@cloudscape-design/components/expandable-section";
import FormField from "@cloudscape-design/components/form-field";
import Input from "@cloudscape-design/components/input";
import SegmentedControl from "@cloudscape-design/components/segmented-control";
import Select from "@cloudscape-design/components/select";
import SpaceBetween from "@cloudscape-design/components/space-between";
import StatusIndicator from "@cloudscape-design/components/status-indicator";
import Tabs from "@cloudscape-design/components/tabs";
import Textarea from "@cloudscape-design/components/textarea";
import Toggle from "@cloudscape-design/components/toggle";
import { useState } from "react";

import type { Entry, Json, Project, ToolFiles } from "./model";
import { codeTool, type CodeCheck, type ToolTest } from "./storage";

type Grants = {
  secret?: string; table?: string; tableAccess?: string; s3Prefix?: string; s3Access?: string;
  vpc?: { subnetIds: string[]; securityGroupIds: string[] };
};
export interface Code { grants?: Grants; timeoutSeconds?: number; memoryMB?: number; environment?: Record<string, string> }

const ACCESS = [{ value: "read", label: "Read" }, { value: "readwrite", label: "Read and write" }];

export function toolNamesOf(tool: Entry): string[] {
  return (Array.isArray(tool.toolSchema) ? tool.toolSchema : [])
    .map((s) => (s && typeof s === "object" && !Array.isArray(s) ? String((s as Record<string, Json>).name ?? "") : ""))
    .filter(Boolean);
}

/** The files a new code tool starts with: a handler for each of its tools, and events. */
export function starterFiles(key: string, tool: Entry): ToolFiles {
  const names = toolNamesOf(tool);
  const first = names[0] ?? "myTool";
  const branches = (names.length ? names : [first]).map((n) =>
    `    if tool == ${JSON.stringify(n)}:\n        return {"tool": tool, "received": event}\n`).join("");
  return {
    "handler.py": `"""${String(tool.description ?? key).replace(/"""/g, "'''")}\n\nA tool written in the build. \`event\` is the tool's arguments, as a dict; return\nJSON-serialisable data. With several tools in toolSchema, the one called is\ncontext.client_context.custom["bedrockAgentCoreToolName"] ("${key}___<tool>").\n"""\n\n\ndef lambda_handler(event, context):\n    tool = context.client_context.custom["bedrockAgentCoreToolName"].split("___")[-1]\n${branches}    raise ValueError(f"unknown tool {tool}")\n`,
    "requirements.txt": "",
    "events.json": JSON.stringify([{ name: `a ${first} call`, tool: first, event: {} }], null, 2) + "\n",
  };
}

export function GrantsForm({ code, onChange }: { code: Code; onChange: (c: Code) => void }) {
  const g = code.grants ?? {};
  const set = (patch: Partial<Grants>) => {
    const next: Grants = { ...g, ...patch };
    for (const k of Object.keys(next) as (keyof Grants)[]) if (next[k] === undefined || next[k] === "") delete next[k];
    if (!next.table) delete next.tableAccess;
    if (!next.s3Prefix) delete next.s3Access;
    const { grants: _g, ...rest } = code;
    onChange(Object.keys(next).length ? { ...rest, grants: next } : rest);
  };
  const ids = (s: string) => s.split(/[\s,]+/).map((x) => x.trim()).filter(Boolean);
  return (
    <SpaceBetween size="s">
      <Box variant="small" color="text-body-secondary">
        Its role gets logs and only what you grant here, and a permissions boundary of the same: nothing added to the role
        later can reach further, and it can never reach this console&apos;s own tables, secrets or buckets. Nothing granted:
        no AWS access at all.
      </Box>
      <ColumnLayout columns={2}>
        <FormField label="A secret it may read" description="Secrets Manager name, in the build's account. Read as SECRET_NAME.">
          <Input value={g.secret ?? ""} placeholder="payments/api-key" onChange={({ detail }) => set({ secret: detail.value.trim() || undefined })} />
        </FormField>
        <FormField label="A DynamoDB table" description="Its name, in the build's account. Read as TABLE_NAME.">
          <SpaceBetween size="xs" direction="horizontal">
            <Input value={g.table ?? ""} placeholder="orders" onChange={({ detail }) => set({ table: detail.value.trim() || undefined })} />
            {g.table ? (
              <Select selectedOption={ACCESS.find((a) => a.value === (g.tableAccess ?? "read"))!} options={ACCESS}
                onChange={({ detail }) => set({ tableAccess: detail.selectedOption.value === "read" ? undefined : detail.selectedOption.value })} />
            ) : null}
          </SpaceBetween>
        </FormField>
        <FormField label="An S3 prefix" description="s3://bucket/prefix/, ending in /. Read as S3_PREFIX.">
          <SpaceBetween size="xs" direction="horizontal">
            <Input value={g.s3Prefix ?? ""} placeholder="s3://my-bucket/refunds/" onChange={({ detail }) => set({ s3Prefix: detail.value.trim() || undefined })} />
            {g.s3Prefix ? (
              <Select selectedOption={ACCESS.find((a) => a.value === (g.s3Access ?? "read"))!} options={ACCESS}
                onChange={({ detail }) => set({ s3Access: detail.selectedOption.value === "read" ? undefined : detail.selectedOption.value })} />
            ) : null}
          </SpaceBetween>
        </FormField>
        <FormField label="A private network (VPC)" description="For a database or API only reachable inside your VPC.">
          <Toggle checked={Boolean(g.vpc)} onChange={({ detail }) => set({ vpc: detail.checked ? { subnetIds: [], securityGroupIds: [] } : undefined })}>
            {g.vpc ? "In your VPC" : "Not in a VPC"}
          </Toggle>
        </FormField>
        {g.vpc ? (
          <>
            <FormField label="Subnet ids"><Input value={g.vpc.subnetIds.join(", ")} placeholder="subnet-0abc…, subnet-0def…"
              onChange={({ detail }) => set({ vpc: { ...g.vpc!, subnetIds: ids(detail.value) } })} /></FormField>
            <FormField label="Security group ids"><Input value={g.vpc.securityGroupIds.join(", ")} placeholder="sg-0abc…"
              onChange={({ detail }) => set({ vpc: { ...g.vpc!, securityGroupIds: ids(detail.value) } })} /></FormField>
          </>
        ) : null}
        <FormField label="Timeout (seconds)" description="1 to 300. Default 30.">
          <Input type="number" value={code.timeoutSeconds === undefined ? "" : String(code.timeoutSeconds)} placeholder="30"
            onChange={({ detail }) => {
              const { timeoutSeconds: _t, ...rest } = code;
              onChange(detail.value === "" ? rest : { ...rest, timeoutSeconds: Number(detail.value) });
            }} />
        </FormField>
        <FormField label="Memory (MB)" description="128 to 10240. Default 256.">
          <Input type="number" value={code.memoryMB === undefined ? "" : String(code.memoryMB)} placeholder="256"
            onChange={({ detail }) => {
              const { memoryMB: _m, ...rest } = code;
              onChange(detail.value === "" ? rest : { ...rest, memoryMB: Number(detail.value) });
            }} />
        </FormField>
      </ColumnLayout>
    </SpaceBetween>
  );
}

export function Files({ files, onChange }: { files: ToolFiles; onChange: (f: ToolFiles) => void }) {
  const names = ["handler.py", "requirements.txt", "events.json",
    ...Object.keys(files).filter((n) => !["handler.py", "requirements.txt", "events.json"].includes(n)).sort()];
  const [active, setActive] = useState("handler.py");
  const [adding, setAdding] = useState("");
  const addable = /^[a-z_][a-z0-9_]*\.py$/.test(adding) && !(adding in files);
  const HINTS: Record<string, string> = {
    "handler.py": "lambda_handler(event, context) — Python 3.12.",
    "requirements.txt": "One name==version per line, installed at deploy. Lambda already has boto3.",
    "events.json": "Test events for the sandbox and Test tool: [{\"name\", \"tool\", \"event\"}]. Not deployed.",
  };
  return (
    <SpaceBetween size="xs">
      <Tabs activeTabId={active} onChange={({ detail }) => setActive(detail.activeTabId)}
        tabs={names.map((n) => ({
          id: n, label: n,
          ...(!HINTS[n] ? { dismissible: true, dismissLabel: `Remove ${n}`, onDismiss: () => {
            const { [n]: _gone, ...rest } = files;
            onChange(rest);
            setActive("handler.py");
          } } : {}),
          content: (
            <FormField stretch description={HINTS[n] ?? "A module handler.py can import."}>
              <div className="axp-cedar">
                <Textarea value={files[n] ?? ""} rows={n === "handler.py" ? 16 : 8} spellcheck={false} ariaLabel={n}
                  onChange={({ detail }) => onChange({ ...files, [n]: detail.value })} />
              </div>
            </FormField>
          ),
        }))} />
      <SpaceBetween size="xs" direction="horizontal">
        <Input value={adding} placeholder="helpers.py" ariaLabel="New file name" onChange={({ detail }) => setAdding(detail.value.trim())} />
        <Button disabled={!addable} onClick={() => { onChange({ ...files, [adding]: "" }); setActive(adding); setAdding(""); }}>Add a file</Button>
      </SpaceBetween>
    </SpaceBetween>
  );
}

export function CheckResult({ result }: { result: CodeCheck }) {
  const errors = result.problems.filter((p) => p.severity === "error");
  const sb = result.sandbox;
  return (
    <SpaceBetween size="s">
      {result.problems.length ? (
        <ul className="axd-list">
          {result.problems.map((p, i) => (
            <li key={i}><StatusIndicator type={p.severity === "error" ? "error" : "warning"}>
              {p.file}{p.line ? `:${p.line}` : ""} — {p.message}</StatusIndicator></li>
          ))}
        </ul>
      ) : <StatusIndicator type="success">No problems in the code.</StatusIndicator>}
      {errors.length ? <Box color="text-status-error">Fix the errors: a deploy refuses them.</Box> : null}
      {sb ? (
        <Box>
          <Box variant="h4">Sandbox run{sb.ms ? ` (${(sb.ms / 1000).toFixed(1)} s)` : ""}</Box>
          {sb.results?.map((r) => (
            <SpaceBetween key={r.name} size="xxs">
              <StatusIndicator type={r.ok ? "success" : "error"}>{r.name} · {r.ms} ms</StatusIndicator>
              <pre className="axb-code">{r.ok ? r.output : `${r.error}\n\n${r.trace ?? ""}`}</pre>
            </SpaceBetween>
          ))}
          <Box variant="small" color="text-body-secondary">{sb.note}</Box>
        </Box>
      ) : null}
    </SpaceBetween>
  );
}

function TestTool({ buildId, id, tool, files }: { buildId: string; id: string; tool: Entry; files: ToolFiles }) {
  const names = toolNamesOf(tool);
  const first = (() => {
    try {
      const evs = JSON.parse(files["events.json"] || "[]");
      return Array.isArray(evs) && evs[0] ? evs[0] : null;
    } catch { return null; }
  })();
  const [name, setName] = useState<string>(first?.tool ?? names[0] ?? "");
  const [event, setEvent] = useState(JSON.stringify(first?.event ?? {}, null, 2));
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<ToolTest | null>(null);
  const [error, setError] = useState("");
  let parsed: Record<string, unknown> | null = null;
  try {
    const v = JSON.parse(event);
    parsed = v && typeof v === "object" && !Array.isArray(v) ? v : null;
  } catch { parsed = null; }
  return (
    <SpaceBetween size="s">
      <Box variant="small" color="text-body-secondary">
        Calls the deployed function once, as the Gateway does, in the account it runs in — so it can change things there.
        Your Cedar policies are not applied to it.
      </Box>
      <ColumnLayout columns={2}>
        <FormField label="Tool">
          <Select selectedOption={{ value: name, label: name }} options={names.map((n) => ({ value: n, label: n }))}
            onChange={({ detail }) => setName(detail.selectedOption.value ?? "")} />
        </FormField>
      </ColumnLayout>
      <FormField label="Event" stretch errorText={parsed ? undefined : "A JSON object"}>
        <div className="axp-cedar"><Textarea value={event} rows={5} spellcheck={false} ariaLabel="Test event"
          onChange={({ detail }) => setEvent(detail.value)} /></div>
      </FormField>
      <Button loading={busy} disabled={!parsed || !name} onClick={async () => {
        setBusy(true); setError(""); setResult(null);
        try { setResult(await codeTool.test(buildId, id, name, parsed!)); } catch (e) { setError((e as Error).message); }
        finally { setBusy(false); }
      }}>Test tool</Button>
      {error ? <Alert type="error">{error}</Alert> : null}
      {result ? (
        <SpaceBetween size="xxs">
          <StatusIndicator type={result.ok ? "success" : "error"}>{result.ok ? "Returned" : `Failed: ${result.error}`} · {result.ms} ms</StatusIndicator>
          <pre className="axb-code">{result.output}</pre>
          {result.log ? <ExpandableSection headerText="Log"><pre className="axb-code axb-log">{result.log}</pre></ExpandableSection> : null}
        </SpaceBetween>
      ) : null}
    </SpaceBetween>
  );
}

/** Where a Lambda tool's function comes from, and — when it is written here — its files,
 *  grants, checks and test. */
export function CodeTool({ project, id, onChange, server }: {
  project: Project; id: string; onChange: (p: Project) => void; server?: boolean;
}) {
  const tool = project.workflow.tools[id];
  const written = tool.code !== undefined;
  const files = project.toolCode?.[id] ?? {};
  const [checking, setChecking] = useState(false);
  const [result, setResult] = useState<CodeCheck | null>(null);
  const [error, setError] = useState("");
  const setTool = (t: Entry, code?: ToolFiles) => onChange({
    ...project,
    workflow: { ...project.workflow, tools: { ...project.workflow.tools, [id]: t } },
    ...(code ? { toolCode: { ...(project.toolCode ?? {}), [id]: code } } : {}),
  });
  const setFiles = (f: ToolFiles) => onChange({ ...project, toolCode: { ...(project.toolCode ?? {}), [id]: f } });
  return (
    <SpaceBetween size="m">
      <FormField label="The function" description={written ? "Written here, deployed by the framework with only the grants below."
        : tool.source ? "One the framework ships." : "One you deployed yourself, by its ARN."}>
        <SegmentedControl selectedId={written ? "code" : "arn"}
          options={[{ id: "arn", text: tool.source ? "Framework's" : "Yours, by ARN" }, { id: "code", text: "Write it here" }]}
          onChange={({ detail }) => {
            if (detail.selectedId === "code") {
              const { lambdaArn: _a, source: _s, ...rest } = tool as Record<string, Json>;
              setTool({ ...rest, code: {} } as Entry, Object.keys(files).length ? undefined : starterFiles(id, tool));
            } else {
              const { code: _c, ...rest } = tool as Record<string, Json>;
              setTool({ ...rest, lambdaArn: "" } as Entry);
            }
          }} />
      </FormField>
      {written ? (
        <>
          <Files files={files} onChange={setFiles} />
          <ExpandableSection headerText="What it may reach" variant="container" defaultExpanded>
            <GrantsForm code={(tool.code ?? {}) as Code}
              onChange={(c) => setTool({ ...tool, code: c as unknown as Json } as Entry)} />
          </ExpandableSection>
          {server ? (
            <ExpandableSection headerText="Check and run" variant="container" defaultExpanded
              headerDescription="Syntax, lint and a security scan, then each test event in an AgentCore Code Interpreter sandbox.">
              <SpaceBetween size="s">
                <Button loading={checking} onClick={async () => {
                  setChecking(true); setError(""); setResult(null);
                  try { setResult(await codeTool.check(id, files, tool as Record<string, unknown>)); }
                  catch (e) { setError((e as Error).message); }
                  finally { setChecking(false); }
                }}>Check and run in the sandbox</Button>
                {error ? <Alert type="error">{error}</Alert> : null}
                {result ? <CheckResult result={result} /> : null}
              </SpaceBetween>
            </ExpandableSection>
          ) : null}
          {server ? (
            <ExpandableSection headerText="Test the deployed tool" variant="container">
              <TestTool buildId={project.id} id={id} tool={tool} files={files} />
            </ExpandableSection>
          ) : null}
        </>
      ) : null}
    </SpaceBetween>
  );
}
