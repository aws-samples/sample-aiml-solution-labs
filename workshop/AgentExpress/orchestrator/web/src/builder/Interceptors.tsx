/** The Interceptors tab: AgentCore Gateway interceptors (orchestrator.interceptors).
 *
 *  At most one Lambda before each MCP request (request) and one after each tool's
 *  answer (response). Each is written here, generated from the templates checked
 *  below and then editable, or is a function the user already owns (lambdaArn). A
 *  Cedar policy (the Policies tab) allows or denies a call by its principal, tool and
 *  arguments; an interceptor is code, for what a policy cannot say: redact, audit,
 *  rewrite arguments, hide tools. */
import Alert from "@cloudscape-design/components/alert";
import AttributeEditor from "@cloudscape-design/components/attribute-editor";
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import Checkbox from "@cloudscape-design/components/checkbox";
import ColumnLayout from "@cloudscape-design/components/column-layout";
import Container from "@cloudscape-design/components/container";
import ExpandableSection from "@cloudscape-design/components/expandable-section";
import FormField from "@cloudscape-design/components/form-field";
import Header from "@cloudscape-design/components/header";
import Input from "@cloudscape-design/components/input";
import Modal from "@cloudscape-design/components/modal";
import Multiselect from "@cloudscape-design/components/multiselect";
import Table from "@cloudscape-design/components/table";
import SegmentedControl from "@cloudscape-design/components/segmented-control";
import Select from "@cloudscape-design/components/select";
import SpaceBetween from "@cloudscape-design/components/space-between";
import StatusIndicator from "@cloudscape-design/components/status-indicator";
import Textarea from "@cloudscape-design/components/textarea";
import Toggle from "@cloudscape-design/components/toggle";
import { useEffect, useRef, useState } from "react";

import { CheckResult, Files, GrantsForm, type Code } from "./CodeTool";
import { TEMPLATES, interceptorFiles, type Point } from "./interceptorCode";
import { IssueList } from "./IssueList";
import { vocab } from "./meta";
import type { Json, Project, ToolFiles, Workflow } from "./model";
import { ShareItems } from "./Library";
import { codeTool, library, type CodeCheck, type LibraryItem, type ToolTest } from "./storage";
import type { Issue } from "./validate";

type Settings = Record<string, Json>;
export interface Interceptor {
  code?: Code;
  lambdaArn?: string;
  passRequestHeaders?: boolean;
  templates?: Record<string, Settings>;
}

const POINTS: { id: Point; title: string; description: string }[] = [
  { id: "request", title: "Before each request",
    description: "Runs before a call reaches a tool (initialize, tools/list, tools/call). It may change the request, or refuse it: the agent is told why." },
  { id: "response", title: "After each answer",
    description: "Runs after a tool answers and before the agent reads it. It may change the answer: redact, trim, hide tools." },
];

export function interceptorsOf(wf: Workflow): Partial<Record<Point, Interceptor>> {
  const orch = (wf.orchestrator ?? {}) as Record<string, unknown>;
  const ics = orch.interceptors;
  return ics && typeof ics === "object" && !Array.isArray(ics) ? ics as Partial<Record<Point, Interceptor>> : {};
}

/** The project with one point's interceptor set (or removed), and its files with it. */
export function setInterceptor(project: Project, point: Point, ic: Interceptor | undefined, files?: ToolFiles): Project {
  const wf = project.workflow;
  const orch = { ...((wf.orchestrator ?? {}) as Record<string, Json>) };
  const next = { ...interceptorsOf(wf) } as Record<string, Interceptor>;
  if (ic) next[point] = ic; else delete next[point];
  if (Object.keys(next).length) orch.interceptors = next as unknown as Json; else delete orch.interceptors;
  const key = `interceptor-${point}`;
  const toolCode = { ...(project.toolCode ?? {}) };
  if (files) toolCode[key] = files;
  if (!ic || ic.code === undefined) delete toolCode[key];
  return { ...project, workflow: { ...wf, orchestrator: orch }, toolCode };
}

const list = (v: Json | undefined): string[] => (Array.isArray(v) ? v.map(String) : []);
const csv = (s: string) => s.split(/[\s,]+/).map((x) => x.trim()).filter(Boolean);

function TemplateSettings({ id, s, wf, onChange }: { id: string; s: Settings; wf: Workflow; onChange: (s: Settings) => void }) {
  const set = (k: string, v: Json | undefined) => {
    const { [k]: _old, ...rest } = s;
    onChange(v === undefined ? rest : { ...rest, [k]: v });
  };
  const toolsHelp = `"<toolKey>" for all of a tool's tools, or "<toolKey>___<toolName>" for one. This build's tools: ${Object.keys(wf.tools ?? {}).join(", ") || "none yet"}.`;
  if (id === "blockTools" || id === "hideTools") {
    const agents = Object.keys(wf.agents ?? {}).map((a) => ({ value: a, label: a }));
    return (
      <ColumnLayout columns={id === "blockTools" ? 2 : 1}>
        <FormField label="Tools" description={toolsHelp}>
          <Input value={list(s.tools).join(", ")} placeholder="refunds___issueRefund" ariaLabel={`${id} tools`}
            onChange={({ detail }) => set("tools", csv(detail.value))} />
        </FormField>
        {id === "blockTools" ? (
          <FormField label="Only for these agents" description="None: refused for every agent. Needs the request headers on.">
            <Multiselect selectedOptions={agents.filter((o) => list(s.agents).includes(o.value))} options={agents}
              placeholder="Every agent" ariaLabel="blockTools agents"
              onChange={({ detail }) => set("agents", detail.selectedOptions.length
                ? detail.selectedOptions.map((o) => String(o.value)) : undefined)} />
          </FormField>
        ) : null}
      </ColumnLayout>
    );
  }
  if (id === "argumentGuard") {
    return (
      <ColumnLayout columns={2}>
        <FormField label="Denied patterns" description="Regular expressions, one per line, matched against the arguments as JSON.">
          <div className="axp-cedar"><Textarea value={list(s.denyPatterns).join("\n")} rows={3} spellcheck={false}
            placeholder={"(?i)drop\\s+table"} ariaLabel="argumentGuard patterns"
            onChange={({ detail }) => set("denyPatterns", detail.value.split("\n").filter((l) => l.trim()))} /></div>
        </FormField>
        <FormField label="Largest arguments (characters)" description="Default 20000.">
          <Input type="number" value={s.maxArgumentChars === undefined ? "" : String(s.maxArgumentChars)} placeholder="20000"
            ariaLabel="argumentGuard size" onChange={({ detail }) => set("maxArgumentChars", detail.value === "" ? undefined : Number(detail.value))} />
        </FormField>
      </ColumnLayout>
    );
  }
  if (id === "injectContext") {
    const args = (s.arguments && typeof s.arguments === "object" && !Array.isArray(s.arguments) ? s.arguments : {}) as Record<string, Json>;
    const items = Object.entries(args).map(([name, source]) => ({ name, source: String(source) }));
    const sources = vocab("interceptorContextSources").map((v) => ({ value: v, label: v }));
    const write = (rows: { name: string; source: string }[]) =>
      set("arguments", Object.fromEntries(rows.map((r) => [r.name, r.source])));
    return (
      <AttributeEditor items={items} addButtonText="Add an argument" removeButtonText="Remove"
        empty="No arguments set yet."
        onAddButtonClick={() => write([...items, { name: `arg${items.length + 1}`, source: "session" }])}
        onRemoveButtonClick={({ detail }) => write(items.filter((_, i) => i !== detail.itemIndex))}
        definition={[
          { label: "Argument", control: (it, i) => <Input value={it.name} ariaLabel={`argument ${i + 1}`}
            onChange={({ detail }) => write(items.map((r, j) => (j === i ? { ...r, name: detail.value.trim() } : r)))} /> },
          { label: "Set from the run's", control: (it, i) => <Select selectedOption={{ value: it.source, label: it.source }} options={sources}
            onChange={({ detail }) => write(items.map((r, j) => (j === i ? { ...r, source: String(detail.selectedOption.value) } : r)))} /> },
        ]} />
    );
  }
  if (id === "redactPii") {
    const types = vocab("interceptorPiiTypes").map((v) => ({ value: v, label: v }));
    const chosen = s.types === undefined ? types.map((t) => t.value) : list(s.types);
    return (
      <ColumnLayout columns={2}>
        <FormField label="What to mask">
          <Multiselect selectedOptions={types.filter((t) => chosen.includes(t.value))} options={types} ariaLabel="redactPii types"
            onChange={({ detail }) => set("types", detail.selectedOptions.map((o) => String(o.value)))} />
        </FormField>
        <FormField label="Replace with">
          <Input value={typeof s.mask === "string" ? s.mask : ""} placeholder="[REDACTED]" ariaLabel="redactPii mask"
            onChange={({ detail }) => set("mask", detail.value || undefined)} />
        </FormField>
      </ColumnLayout>
    );
  }
  if (id === "capResult") {
    return (
      <FormField label="Largest text (characters)" description="Default 20000.">
        <Input type="number" value={s.maxChars === undefined ? "" : String(s.maxChars)} placeholder="20000" ariaLabel="capResult size"
          onChange={({ detail }) => set("maxChars", detail.value === "" ? undefined : Number(detail.value))} />
      </FormField>
    );
  }
  return null;
}

function TestDeployed({ buildId, point, files }: { buildId: string; point: Point; files: ToolFiles }) {
  const first = (() => {
    try {
      const evs = JSON.parse(files["events.json"] || "[]");
      return Array.isArray(evs) && evs[0] ? evs[0].event : {};
    } catch { return {}; }
  })();
  const [event, setEvent] = useState(JSON.stringify(first, null, 2));
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
      <Box variant="small" color="text-body-secondary">Calls the deployed interceptor once with this Gateway payload, in the account it runs in.</Box>
      <FormField label="Event" stretch errorText={parsed ? undefined : "A JSON object"}>
        <div className="axp-cedar"><Textarea value={event} rows={8} spellcheck={false} ariaLabel={`${point} test event`}
          onChange={({ detail }) => setEvent(detail.value)} /></div>
      </FormField>
      <Button loading={busy} disabled={!parsed} onClick={async () => {
        setBusy(true); setError(""); setResult(null);
        try { setResult(await codeTool.test(buildId, `interceptor-${point}`, "", parsed!)); } catch (e) { setError((e as Error).message); }
        finally { setBusy(false); }
      }}>Test the interceptor</Button>
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


type Notify = (type: "success" | "error" | "info", msg: string) => void;

/** Publish this point's interceptor to the library, then share it. A build that adds
 *  it from the library takes a copy with its code (bff/library.py COPY_KINDS). */
function PublishBar({ project, point, notify }: { project: Project; point: Point; notify: Notify }) {
  const ic = interceptorsOf(project.workflow)[point];
  const files = project.toolCode?.[`interceptor-${point}`];
  const [open, setOpen] = useState(false);
  const [name, setName] = useState("");
  const [about, setAbout] = useState("");
  const [busy, setBusy] = useState(false);
  const [published, setPublished] = useState<LibraryItem | null>(null);
  const [sharing, setSharing] = useState<LibraryItem[] | null>(null);
  const ready = Boolean(ic && (ic.code !== undefined ? files?.["handler.py"] : ic.lambdaArn));
  const publish = async () => {
    setBusy(true);
    try {
      const made = await library.create({ kind: "interceptor", name, description: about,
        definition: { point, ...(ic as Record<string, unknown>) }, ...(ic?.code !== undefined && files ? { files } : {}) });
      notify("success", `${made.name} is in your library (Library, Interceptors).`);
      setPublished(made); setOpen(false); setName(""); setAbout("");
    } catch (e) { notify("error", `Could not publish: ${(e as Error).message}`); } finally { setBusy(false); }
  };
  return (
    <>
      <SpaceBetween direction="horizontal" size="xs">
        <Button disabled={!ready} onClick={() => setOpen(true)}>Publish to library</Button>
        {published ? <Button onClick={() => setSharing([published])}>Share {published.name}</Button> : null}
      </SpaceBetween>
      <Modal visible={open} onDismiss={() => setOpen(false)} header={`Publish the ${point} interceptor`}
        footer={<Box float="right"><SpaceBetween direction="horizontal" size="xs">
          <Button variant="link" onClick={() => setOpen(false)}>Cancel</Button>
          <Button variant="primary" loading={busy} disabled={!/^[A-Za-z][A-Za-z0-9]{0,31}$/.test(name)} onClick={() => void publish()}>Publish</Button>
        </SpaceBetween></Box>}>
        <SpaceBetween size="s">
          <FormField label="Name" description="A letter, then letters and digits.">
            <Input value={name} placeholder="piiRedactor" ariaLabel="Library name" onChange={({ detail }) => setName(detail.value.trim())} />
          </FormField>
          <FormField label="Description">
            <Input value={about} placeholder="Masks email and card numbers in tool results" ariaLabel="Library description"
              onChange={({ detail }) => setAbout(detail.value)} />
          </FormField>
          <Box variant="small" color="text-body-secondary">A build that adds it takes a copy, with its code: changing one never changes another.</Box>
        </SpaceBetween>
      </Modal>
      <ShareItems items={sharing} onDismiss={() => setSharing(null)} notify={notify}
        onSaved={(sh) => { setSharing(null); setPublished((p) => (p ? { ...p, shares: sh } : p)); }} />
    </>
  );
}

const POINT_LABEL: Record<string, string> = { request: "Before each request", response: "After each answer" };

/** Add from library: every interceptor the caller has or was shared, to pick one from.
 *  It goes to its own point (request or response), as a copy with its code. */
export function InterceptorPicker({ project, visible, onDismiss, onChange, notify }: {
  project: Project; visible: boolean; onDismiss: () => void; onChange: (p: Project) => void; notify: Notify;
}) {
  const [items, setItems] = useState<LibraryItem[] | null>(null);
  const [selected, setSelected] = useState<LibraryItem[]>([]);
  const said = useRef(notify);
  said.current = notify;
  useEffect(() => {
    if (!visible) return;
    let live = true;
    setSelected([]); setItems(null);
    library.list("interceptor").then((xs) => { if (live) setItems(xs); }, (e) => {
      said.current("error", `Could not load the library: ${(e as Error).message}`);
      if (live) setItems([]);
    });
    return () => { live = false; };
  }, [visible]);
  const pick = selected[0];
  const add = () => {
    if (!pick) return;
    const { point, ...spec } = pick.definition as Record<string, unknown>;
    if (point !== "request" && point !== "response") { notify("error", `${pick.name} does not say where it runs.`); return; }
    const p = point as Point;
    if (interceptorsOf(project.workflow)[p] && !window.confirm(
      `This build already has a ${p} interceptor. Replace it with ${pick.name}?`)) return;
    onChange(setInterceptor(project, p, spec as Interceptor, pick.files ? structuredClone(pick.files) : undefined));
    notify("success", `${pick.name} added (${POINT_LABEL[p].toLowerCase()}): a copy, so changing it here does not change the library's.`);
    onDismiss();
  };
  return (
    <Modal visible={visible} onDismiss={onDismiss} size="large" header="Add an interceptor from the library"
      footer={<Box float="right"><SpaceBetween direction="horizontal" size="xs">
        <Button variant="link" onClick={onDismiss}>Cancel</Button>
        <Button variant="primary" disabled={!pick} onClick={add}>Add</Button>
      </SpaceBetween></Box>}>
      <SpaceBetween size="m">
        <Table selectionType="single" items={items ?? []} loading={items === null} loadingText="Loading" trackBy="id"
          selectedItems={selected} onSelectionChange={({ detail }) => setSelected(detail.selectedItems)}
          ariaLabels={{ selectionGroupLabel: "Interceptors", itemSelectionLabel: (_, i) => `Select ${i.name}` }}
          columnDefinitions={[
            { id: "name", header: "Name", isRowHeader: true, cell: (i) => i.name },
            { id: "point", header: "Runs", cell: (i) => POINT_LABEL[String(i.definition.point)] ?? "—" },
            { id: "what", header: "Does", cell: (i) => (i.definition.lambdaArn ? "Your Lambda"
              : Object.keys((i.definition.templates ?? {}) as object).join(", ") || "Custom code") },
            { id: "desc", header: "Description", cell: (i) => i.description || "—" },
            { id: "owner", header: "Owner", cell: (i) => (i.mine ? "You" : i.ownerEmail || "—") },
          ]}
          empty={<Box textAlign="center" color="inherit">No interceptors in your library, or shared with you. Publish one from a build first.</Box>} />
        {pick?.files?.["handler.py"] ? (
          <ExpandableSection headerText={`${pick.name}: handler.py`} defaultExpanded>
            <pre className="axb-code">{pick.files["handler.py"]}</pre>
          </ExpandableSection>
        ) : null}
      </SpaceBetween>
    </Modal>
  );
}

function PointEditor({ project, point, onChange, server, deployed, notify }: {
  project: Project; point: (typeof POINTS)[number]; onChange: (p: Project) => void; server: boolean; deployed: boolean;
  notify?: Notify;
}) {
  const wf = project.workflow;
  const ic = interceptorsOf(wf)[point.id];
  const key = `interceptor-${point.id}`;
  const files = project.toolCode?.[key] ?? {};
  const [checking, setChecking] = useState(false);
  const [result, setResult] = useState<CodeCheck | null>(null);
  const [error, setError] = useState("");
  const templates = ic?.templates ?? {};
  const generated = (t: Record<string, Settings>) => interceptorFiles(point.id, t, wf);
  // The code was changed by hand when it is not what its current templates generate.
  const edited = ic?.code !== undefined && files["handler.py"] !== undefined
    && files["handler.py"] !== generated(templates)["handler.py"];

  const setTemplates = (next: Record<string, Settings>) => {
    const nic: Interceptor = { ...ic!, templates: next };
    // Untouched code follows the boxes; edited code is kept until Generate is pressed.
    onChange(setInterceptor(project, point.id, nic, edited ? files : { ...files, ...generated(next) }));
  };
  const needsHeaders = TEMPLATES[point.id].some((t) => t.headers && t.id in templates)
    || (point.id === "request" && list(templates.blockTools?.agents).length > 0);

  return (
    <Container header={<Header variant="h2" description={point.description}
      actions={<Toggle checked={Boolean(ic)} onChange={({ detail }) => {
        if (!detail.checked) { onChange(setInterceptor(project, point.id, undefined)); return; }
        const start: Interceptor = { code: {}, templates: { audit: {} } };
        onChange(setInterceptor(project, point.id, start, generated(start.templates!)));
      }}>{ic ? "On" : "Off"}</Toggle>}>{point.title}</Header>}>
      {server && notify && ic ? <Box padding={{ bottom: "s" }}><PublishBar project={project} point={point.id} notify={notify} /></Box> : null}
      {!ic ? <Box color="text-body-secondary">No {point.id} interceptor: the Gateway calls the tools directly.</Box> : (
        <SpaceBetween size="m">
          <FormField label="The function">
            <SegmentedControl selectedId={ic.code !== undefined ? "code" : "arn"}
              options={[{ id: "code", text: "Write it here" }, { id: "arn", text: "Yours, by ARN" }]}
              onChange={({ detail }) => {
                if (detail.selectedId === "code") {
                  const { lambdaArn: _a, ...rest } = ic;
                  const nic = { ...rest, code: {}, templates: rest.templates ?? { audit: {} } };
                  onChange(setInterceptor(project, point.id, nic, Object.keys(files).length ? files : generated(nic.templates)));
                } else {
                  const { code: _c, templates: _t, ...rest } = ic;
                  onChange(setInterceptor(project, point.id, { ...rest, lambdaArn: "" }));
                }
              }} />
          </FormField>
          {ic.code === undefined ? (
            <FormField label="Lambda function ARN" description="A function you deployed. The Gateway's role is allowed to invoke it; one in another account also needs a resource policy there for bedrock-agentcore.amazonaws.com.">
              <Input value={ic.lambdaArn ?? ""} placeholder="arn:aws:lambda:us-east-1:123456789012:function:my-interceptor"
                onChange={({ detail }) => onChange(setInterceptor(project, point.id, { ...ic, lambdaArn: detail.value.trim() }))} />
            </FormField>
          ) : null}
          <FormField label="Pass the request headers" description="The x-ax-session, x-ax-agent and x-ax-user headers say which run, agent and user called. They include the agent's Gateway token too: never log them.">
            <Toggle checked={ic.passRequestHeaders === true} onChange={({ detail }) => {
              const { passRequestHeaders: _p, ...rest } = ic;
              onChange(setInterceptor(project, point.id, detail.checked ? { ...rest, passRequestHeaders: true } : rest,
                ic.code !== undefined ? files : undefined));
            }}>{ic.passRequestHeaders ? "Passed" : "Not passed"}</Toggle>
          </FormField>
          {needsHeaders && !ic.passRequestHeaders ? (
            <Alert type="warning">A checked template reads the x-ax-* headers: turn on Pass the request headers.</Alert>
          ) : null}
          {ic.code !== undefined ? (
            <>
              <FormField label="What it does" description="Each checked template becomes a section of handler.py, with its settings.">
                <SpaceBetween size="s">
                  {TEMPLATES[point.id].map((t) => (
                    <div key={t.id}>
                      <Checkbox checked={t.id in templates} description={t.description}
                        onChange={({ detail }) => {
                          const { [t.id]: _gone, ...rest } = templates;
                          setTemplates(detail.checked ? { ...templates, [t.id]: structuredClone(t.defaults) } : rest);
                        }}>{t.label}</Checkbox>
                      {t.id in templates ? (
                        <Box padding={{ left: "xl", top: "xs" }}>
                          <TemplateSettings id={t.id} s={templates[t.id]} wf={wf}
                            onChange={(s) => setTemplates({ ...templates, [t.id]: s })} />
                        </Box>
                      ) : null}
                    </div>
                  ))}
                </SpaceBetween>
              </FormField>
              {edited ? (
                <Alert type="info" action={<Button onClick={() => {
                  if (window.confirm(`Replace ${point.id} handler.py with the code the checked templates generate? Your edits are lost.`)) {
                    onChange(setInterceptor(project, point.id, ic, { ...files, ...generated(templates) }));
                  }
                }}>Generate code</Button>}>
                  handler.py was edited by hand, so the boxes no longer change it. Generate it again to apply them.
                </Alert>
              ) : null}
              <ExpandableSection headerText="The code" variant="container">
                <Files files={files} onChange={(f) => onChange(setInterceptor(project, point.id, ic, f))} />
              </ExpandableSection>
              <ExpandableSection headerText="What it may reach" variant="container">
                <GrantsForm code={ic.code} onChange={(c) => onChange(setInterceptor(project, point.id, { ...ic, code: c }, files))} />
              </ExpandableSection>
              {server ? (
                <ExpandableSection headerText="Check and run" variant="container"
                  headerDescription="Syntax, lint and a security scan, then each test event in an AgentCore Code Interpreter sandbox.">
                  <SpaceBetween size="s">
                    <Button loading={checking} onClick={async () => {
                      setChecking(true); setError(""); setResult(null);
                      try { setResult(await codeTool.check(key, files, { code: ic.code } as Record<string, unknown>)); }
                      catch (e) { setError((e as Error).message); }
                      finally { setChecking(false); }
                    }}>Check and run in the sandbox</Button>
                    {error ? <Alert type="error">{error}</Alert> : null}
                    {result ? <CheckResult result={result} /> : null}
                  </SpaceBetween>
                </ExpandableSection>
              ) : null}
              {server && deployed ? (
                <ExpandableSection headerText="Test the deployed interceptor" variant="container">
                  <TestDeployed buildId={project.id} point={point.id} files={files} />
                </ExpandableSection>
              ) : null}
            </>
          ) : null}
        </SpaceBetween>
      )}
    </Container>
  );
}

export function BuildInterceptors({ project, setProject, issues, server, deployed = false, notify }: {
  project: Project; setProject: (p: Project) => void; issues: Issue[]; server: boolean; deployed?: boolean; notify?: Notify;
}) {
  const mine = issues.filter((i) => i.path.startsWith("orchestrator.interceptors"));
  const [picking, setPicking] = useState(false);
  return (
    <SpaceBetween size="l">
      <Header variant="h2" description="Published interceptors, yours and shared with you, are in Library, Interceptors."
        actions={server && notify ? <Button onClick={() => setPicking(true)}>Add from library</Button> : undefined}>
        Interceptors</Header>
      {server && notify ? <InterceptorPicker project={project} visible={picking} onDismiss={() => setPicking(false)}
        onChange={setProject} notify={notify} /> : null}
      <Box color="text-body-secondary">
        AgentCore Gateway interceptors run your code on every tool call: one Lambda before the request, one after the
        answer. A Cedar policy on the Policies tab allows or denies a call; an interceptor can also change it, redact it
        or log it. The Gateway may call one twice for the same request, so keep them free of side effects you would not
        want repeated. Both need the Gateway, so a build with no tools has none.
      </Box>
      {mine.length ? <IssueList issues={mine} /> : null}
      {POINTS.map((p) => (
        <PointEditor key={p.id} project={project} point={p} onChange={setProject} server={server} deployed={deployed} notify={notify} />
      ))}
    </SpaceBetween>
  );
}
