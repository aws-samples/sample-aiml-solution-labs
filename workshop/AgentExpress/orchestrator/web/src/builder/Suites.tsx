/** The Tests tab: a build's test suites, written with forms and run against the deployed build.
 *
 *  A suite is a list of requests (cases), each with checks on what the run should produce. It
 *  is saved with the build (project.suites, autosaved like the prompts). Running starts one
 *  run per case in the DEPLOYED build, so authoring works before there is one and running
 *  says plainly what it needs. Nothing here judges quality with a model: every check compares
 *  a value in the finished run with a value written here (the Evals tab is the LLM judge).
 *  The server validates and grades (bff/suites.py); this page only builds checks it accepts. */
import Alert from "@cloudscape-design/components/alert";
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import ColumnLayout from "@cloudscape-design/components/column-layout";
import Container from "@cloudscape-design/components/container";
import ExpandableSection from "@cloudscape-design/components/expandable-section";
import FormField from "@cloudscape-design/components/form-field";
import Header from "@cloudscape-design/components/header";
import Input from "@cloudscape-design/components/input";
import Link from "@cloudscape-design/components/link";
import Select from "@cloudscape-design/components/select";
import SpaceBetween from "@cloudscape-design/components/space-between";
import StatusIndicator from "@cloudscape-design/components/status-indicator";
import Table from "@cloudscape-design/components/table";
import Textarea from "@cloudscape-design/components/textarea";
import Toggle from "@cloudscape-design/components/toggle";
import { useCallback, useEffect, useRef, useState, type ReactNode } from "react";
import type { Project } from "./model";
import {
  AGENT_OUTCOMES, MAX_CASES, MAX_CHECKS, MAX_NAME, MAX_REQUEST, MAX_SUITES, OPERATORS, OPERATOR_LABEL, RUN_OUTCOMES,
  blankCheck, describeCheck, kindOf, newCase, newSuite, operatorOf, passRate,
  type CaseResult, type Check, type CheckKind, type Operator, type Suite, type SuiteCase, type SuiteRunResult,
  type TrendEntry,
} from "./suiteSpec";
import { suitesApi } from "./suitesApi";
import { useDocsBase } from "../docs/docsLinks";
import { infoSlot } from "../docs/InfoLink";

/** How often an open suite run is read again. */
export const POLL_MS = 4000;

const KINDS: { value: CheckKind; label: string }[] = [
  { value: "run", label: "The run ends" },
  { value: "agentStatus", label: "An agent ends" },
  { value: "agentField", label: "A field of an agent's output" },
  { value: "agentText", label: "An agent's output as text" },
  { value: "cost", label: "The run costs (USD)" },
  { value: "latencyMs", label: "The calls take (ms)" },
];
const opts = (values: readonly string[]) => values.map((v) => ({ value: v, label: v }));
const LIMIT_OPS = ["lt", "lte", "gt", "gte"] as const;
const num = (s: string) => (s.trim() !== "" && Number.isFinite(Number(s)) ? Number(s) : s);

/** The check with one comparison replaced by another, keeping what the check is about. */
function withOperator(check: Check, op: Operator, raw: string | boolean): Check {
  if ("run" in check || "cost" in check || "latencyMs" in check) return check;
  const { agent, field } = check;
  const value = op === "exists" ? raw === true || raw === "true"
    : op === "in" ? String(raw).split(",").map((s) => s.trim()).filter(Boolean)
    : ["gt", "gte", "lt", "lte"].includes(op) ? num(String(raw)) : String(raw);
  return { agent, ...(field !== undefined ? { field } : {}), [op]: value } as Check;
}

function valueText(check: Check): string {
  const op = "run" in check || "cost" in check || "latencyMs" in check ? undefined : operatorOf(check);
  const v = op ? (check as Record<string, unknown>)[op] : undefined;
  return Array.isArray(v) ? v.join(", ") : v === undefined ? "" : String(v);
}

function CheckRow({ check, agents, onChange, onRemove, n }: {
  check: Check; agents: string[]; n: number; onChange: (c: Check) => void; onRemove: () => void;
}) {
  const kind = kindOf(check);
  const agent = "agent" in check ? check.agent : "";
  const agentOptions = opts(agents.includes(agent) || !agent ? agents : [agent, ...agents]);
  const label = (what: string) => `Check ${n} ${what}`;
  const status = (check as { status?: string }).status;
  // ColumnLayout lays out its direct children, so the cells are an array, not a fragment.
  const cells: ReactNode[] = [
    <Select key="kind" ariaLabel={label("kind")} selectedOption={KINDS.find((k) => k.value === kind)!} options={KINDS}
      onChange={({ detail }) => onChange(blankCheck(detail.selectedOption.value as CheckKind, agent || agents[0] || ""))} />,
  ];
  if (kind === "run") {
    const run = (check as { run: string }).run;
    cells.push(<Select key="outcome" ariaLabel={label("outcome")} selectedOption={{ value: run, label: run }}
      options={opts(RUN_OUTCOMES)} onChange={({ detail }) => onChange({ run: detail.selectedOption.value as never })} />);
  } else if (kind === "cost" || kind === "latencyMs") {
    cells.push(...limitCells(check, kind, label, onChange));
  } else {
    cells.push(<Select key="agent" ariaLabel={label("agent")}
      selectedOption={agent ? { value: agent, label: agent } : null} placeholder="Agent" options={agentOptions}
      onChange={({ detail }) => onChange({ ...check, agent: detail.selectedOption.value ?? "" } as Check)} />);
    if (kind === "agentStatus") {
      cells.push(<Select key="status" ariaLabel={label("status")} selectedOption={{ value: String(status), label: String(status) }}
        options={opts(AGENT_OUTCOMES)} onChange={({ detail }) => onChange({ agent, status: detail.selectedOption.value as never })} />);
    } else if (kind === "agentField") {
      cells.push(<Input key="field" ariaLabel={label("field")} placeholder="field, or a.b.0 for a path"
        value={(check as { field?: string }).field ?? ""} onChange={({ detail }) => onChange({ ...check, field: detail.value } as Check)} />);
    }
  }
  cells.push(<Button key="remove" variant="inline-link" ariaLabel={`Remove check ${n}`} onClick={onRemove}>Remove</Button>);
  return (
    <div data-testid={`check-${n}`}>
      <ColumnLayout columns={4} variant="text-grid">{cells}</ColumnLayout>
      {kind === "agentField" || kind === "agentText" ? <CompareFields check={check} label={label} onChange={onChange} /> : null}
    </div>
  );
}

function limitCells(check: Check, kind: "cost" | "latencyMs", label: (w: string) => string, onChange: (c: Check) => void): ReactNode[] {
  const spec = (check as Record<string, Record<string, number>>)[kind];
  const op = (LIMIT_OPS.find((o) => o in spec) ?? "lt") as (typeof LIMIT_OPS)[number];
  const bad = typeof spec[op] !== "number" || spec[op] < 0;
  return [
    <Select key="comparison" ariaLabel={label("comparison")} selectedOption={{ value: op, label: OPERATOR_LABEL[op] }}
      options={LIMIT_OPS.map((o) => ({ value: o, label: OPERATOR_LABEL[o] }))}
      onChange={({ detail }) => onChange({ [kind]: { [detail.selectedOption.value as string]: spec[op] } } as Check)} />,
    <FormField key="limit" errorText={bad ? "A number, 0 or more" : undefined}>
      <Input ariaLabel={label("limit")} type="number" value={String(spec[op])}
        onChange={({ detail }) => onChange({ [kind]: { [op]: num(detail.value) } } as Check)} />
    </FormField>,
  ];
}

function CompareFields({ check, label, onChange }: { check: Check; label: (w: string) => string; onChange: (c: Check) => void }) {
  const op = operatorOf(check) ?? "contains";
  const text = valueText(check);
  const numeric = ["gt", "gte", "lt", "lte"].includes(op);
  return (
    <ColumnLayout columns={4} variant="text-grid">
      <Select ariaLabel={label("comparison")} selectedOption={{ value: op, label: OPERATOR_LABEL[op] }}
        options={OPERATORS.map((o) => ({ value: o, label: OPERATOR_LABEL[o] }))}
        onChange={({ detail }) => onChange(withOperator(check, detail.selectedOption.value as Operator,
          detail.selectedOption.value === "exists" ? true : text))} />
      {op === "exists" ? (
        <Toggle checked={(check as Record<string, unknown>).exists !== false}
          onChange={({ detail }) => onChange(withOperator(check, "exists", detail.checked))}>
          {(check as Record<string, unknown>).exists !== false ? "present" : "absent"}</Toggle>
      ) : (
        <FormField errorText={numeric && typeof (check as Record<string, unknown>)[op] !== "number" ? "A number" : undefined}>
          <Input ariaLabel={label("value")} value={text} placeholder={op === "in" ? "one, two, three" : "value"}
            onChange={({ detail }) => onChange(withOperator(check, op, detail.value))} />
        </FormField>
      )}
    </ColumnLayout>
  );
}

function CaseEditor({ c, agents, onChange, onRemove, n, result }: {
  c: SuiteCase; agents: string[]; n: number; onChange: (c: SuiteCase) => void; onRemove: () => void; result?: CaseResult;
}) {
  const [adding, setAdding] = useState<CheckKind>("agentField");
  return (
    <ExpandableSection variant="footer" defaultExpanded={!c.request}
      headerText={<span data-testid={`case-${n}`}>{c.name || "(unnamed)"}{result ? <> {" "}<CaseStatus s={result.status} /></> : null}</span>}
      headerDescription={`${c.expect.length} check${c.expect.length === 1 ? "" : "s"} · the run must also end done unless you say otherwise`}>
      <SpaceBetween size="s">
        <ColumnLayout columns={2}>
          <FormField label="Case name">
            <Input ariaLabel={`Case ${n} name`} value={c.name} onChange={({ detail }) => onChange({ ...c, name: detail.value.slice(0, MAX_NAME) })} />
          </FormField>
          <FormField label="Subject (optional)" description="The run's subject id, as in Start run.">
            <Input ariaLabel={`Case ${n} subject`} value={c.subject ?? ""}
              onChange={({ detail }) => { const { subject: _s, ...rest } = c; onChange(detail.value ? { ...rest, subject: detail.value } : rest); }} />
          </FormField>
        </ColumnLayout>
        <FormField label="Request" stretch errorText={c.request.trim() ? undefined : "A case needs the request to run."}
          description="Exactly what you would type in Start run.">
          <Textarea ariaLabel={`Case ${n} request`} rows={3} value={c.request}
            onChange={({ detail }) => onChange({ ...c, request: detail.value.slice(0, MAX_REQUEST) })} />
        </FormField>
        <Box variant="awsui-key-label">Checks</Box>
        {c.expect.map((check, i) => (
          <CheckRow key={i} n={i + 1} check={check} agents={agents}
            onChange={(next) => onChange({ ...c, expect: c.expect.map((x, j) => (j === i ? next : x)) })}
            onRemove={() => onChange({ ...c, expect: c.expect.filter((_, j) => j !== i) })} />
        ))}
        <SpaceBetween direction="horizontal" size="xs">
          <Select ariaLabel={`Case ${n} kind of check to add`} selectedOption={KINDS.find((k) => k.value === adding)!} options={KINDS}
            onChange={({ detail }) => setAdding(detail.selectedOption.value as CheckKind)} />
          <Button iconName="add-plus" disabled={c.expect.length >= MAX_CHECKS}
            onClick={() => onChange({ ...c, expect: [...c.expect, blankCheck(adding, agents[0] ?? "")] })}>Add check</Button>
          <Button variant="inline-link" onClick={onRemove}>{`Remove case ${n}`}</Button>
        </SpaceBetween>
        {c.expect.length ? (
          <Box variant="small" color="text-body-secondary">
            {c.expect.map((x) => describeCheck(x)).join(" · ")}
          </Box>
        ) : null}
      </SpaceBetween>
    </ExpandableSection>
  );
}

function CaseStatus({ s }: { s: CaseResult["status"] }) {
  return s === "passed" ? <StatusIndicator type="success">Passed</StatusIndicator>
    : s === "failed" ? <StatusIndicator type="error">Failed</StatusIndicator>
    : s === "running" ? <StatusIndicator type="in-progress">Running</StatusIndicator>
    : <StatusIndicator type="stopped">Not started</StatusIndicator>;
}

function Trend({ trend }: { trend: TrendEntry[] }) {
  if (!trend.length) return null;
  return (
    <div data-testid="suite-trend">
      <Box variant="awsui-key-label">Pass rate by run</Box>
      <div style={{ display: "flex", gap: 4, alignItems: "flex-end", height: 48, marginTop: 4 }}>
        {trend.map((e) => {
          const rate = passRate(e);
          return (
            <div key={e.suiteRunId} style={{ textAlign: "center" }}
              title={rate === null ? "not finished, or nothing started" : `${e.passed}/${e.total} passed · version ${e.buildVersion} · ${e.startedAt}`}>
              <div aria-hidden style={{ width: 14, height: Math.max(3, Math.round((rate ?? 0) * 40)), background: rate === null ? "#aab7b8" : rate === 1 ? "#1d8102" : rate >= 0.5 ? "#f89256" : "#d13212" }} />
            </div>
          );
        })}
      </div>
      <Box variant="small" color="text-body-secondary">
        {trend.map((e) => (e.finished && e.total ? `v${e.buildVersion}: ${e.passed}/${e.total}` : `v${e.buildVersion}: …`)).join(" · ")}
      </Box>
    </div>
  );
}

function Result({ r }: { r: SuiteRunResult }) {
  const s = r.summary;
  return (
    <SpaceBetween size="xs">
      <Box data-testid="suite-summary">
        {r.state === "running"
          ? <StatusIndicator type="in-progress">Running: {s.total - s.running} of {s.total} cases done</StatusIndicator>
          : s.total === 0 ? <StatusIndicator type="stopped">Nothing started</StatusIndicator>
          : <StatusIndicator type={s.failed ? "error" : "success"}>{s.passed} of {s.total} passed</StatusIndicator>}
        {s.notStarted ? ` · ${s.notStarted} not started` : ""} · build version {r.buildVersion}
      </Box>
      <Table<CaseResult> variant="embedded" trackBy="case" items={r.cases} wrapLines
        columnDefinitions={[
          { id: "status", header: "Result", cell: (c) => <CaseStatus s={c.status} />, width: 130 },
          { id: "case", header: "Case", cell: (c) => c.case },
          { id: "message", header: "What was found", cell: (c) => (
            <SpaceBetween size="xxs">
              <span>{c.message}</span>
              {c.failed.map((f, i) => <Box key={i} variant="small" color="text-status-error">{f.message}</Box>)}
            </SpaceBetween>
          ) },
          { id: "run", header: "Run", cell: (c) => (c.sessionId ? <Link href={`#run:${c.sessionId}`}>Open run</Link> : "–"), width: 110 },
        ]} />
    </SpaceBetween>
  );
}

export function BuildSuites({ project, setProject, server, buildId, deployed, runBlock, flush, notify }: {
  project: Project;
  setProject: (p: Project) => void;
  /** The console holds this build (a build kept in this browser has nothing to run against). */
  server: boolean;
  buildId?: string;
  deployed: boolean;
  /** Why running is not possible here, when it is not (a permission, a control-plane console). */
  runBlock?: string;
  /** Save what is on screen now: a suite run reads the saved build. */
  flush: () => Promise<void>;
  notify: (type: "success" | "error" | "info", msg: string) => void;
}) {
  const docs = useDocsBase();
  const suites = project.suites ?? [];
  const agents = Object.keys(project.workflow.agents);
  const setSuites = (next: Suite[]) => setProject({ ...project, suites: next });
  const [results, setResults] = useState<Record<string, SuiteRunResult>>({});
  const [trends, setTrends] = useState<Record<string, TrendEntry[]>>({});
  const [starting, setStarting] = useState<string | null>(null);
  /** suite name -> the suite run being followed */
  const [following, setFollowing] = useState<Record<string, string>>({});
  const live = useRef(true);
  useEffect(() => { live.current = true; return () => { live.current = false; }; }, []);

  // A different build: nothing of the last one's runs.
  useEffect(() => { setResults({}); setFollowing({}); setTrends({}); }, [buildId]);
  useEffect(() => {
    if (!server || !buildId) return;
    suitesApi.trends(buildId).then((t) => { if (live.current) setTrends(t); }, () => { /* no history to show */ });
  }, [server, buildId]);

  const read = useCallback(async (name: string, run: string) => {
    if (!buildId) return;
    try {
      const r = await suitesApi.result(buildId, run);
      if (!live.current) return;
      setResults((cur) => ({ ...cur, [name]: r }));
      setTrends((cur) => ({ ...cur, [r.suite]: r.trend }));
      if (r.state === "finished") setFollowing((cur) => { const { [name]: _d, ...rest } = cur; return rest; });
    } catch (e) {
      if (live.current) notify("error", `Could not read the suite run: ${(e as Error).message}`);
      setFollowing((cur) => { const { [name]: _d, ...rest } = cur; return rest; });
    }
  }, [buildId, notify]);

  // Follow the runs that are going, until each ends.
  useEffect(() => {
    const open = Object.entries(following);
    if (!open.length) return;
    const t = setInterval(() => open.forEach(([name, run]) => void read(name, run)), POLL_MS);
    return () => clearInterval(t);
  }, [following, read]);

  const run = async (name: string) => {
    if (!buildId) return;
    setStarting(name);
    try {
      await flush();
      const started = await suitesApi.start(buildId, name);
      notify("info", started.notStarted
        ? `Started ${started.started} of ${started.started + started.notStarted} cases; the rest were not allowed to start.`
        : `Started ${started.started} run${started.started === 1 ? "" : "s"}.`);
      setFollowing((cur) => ({ ...cur, [name]: started.suiteRunId }));
      await read(name, started.suiteRunId);
    } catch (e) {
      notify("error", `Could not run ${name}: ${(e as Error).message}`);
    } finally {
      if (live.current) setStarting(null);
    }
  };

  const why = !server ? "Running tests needs the console: this build is kept in your browser."
    : !deployed ? "Deploy the build to run its tests: each case is a run of the deployed build. You can write them now."
    : runBlock;
  const names = suites.map((s) => s.name);

  return (
    <SpaceBetween size="l">
      {why ? <Alert type="info" data-testid="tests-cannot-run">{why}</Alert> : null}
      <Container header={<Header variant="h2" counter={`(${suites.length})`} info={infoSlot(docs, "tests.suites")}
        description="A suite is a list of requests with checks on what each run should produce. Run it after a change to a prompt or a model, and compare it with the last time. Every check is a fixed comparison: nothing here asks a model to judge (that is the Evals tab)."
        actions={<Button iconName="add-plus" disabled={suites.length >= MAX_SUITES}
          onClick={() => setSuites([...suites, newSuite(names)])}>New suite</Button>}>Test suites</Header>}>
        {suites.length ? null : <Box color="text-body-secondary">No suites yet. A suite starts with a case: a request, and what the run should do with it.</Box>}
      </Container>
      {suites.map((s, si) => (
        <Container key={si} data-testid={`suite-${si}`}
          header={<Header variant="h3" description={`${s.cases.length} case${s.cases.length === 1 ? "" : "s"}`}
            actions={<SpaceBetween direction="horizontal" size="xs">
              <Button variant="inline-link" ariaLabel={`Remove suite ${s.name}`} onClick={() => {
                if (window.confirm(`Remove the suite ${s.name}? Its past results stay with the build.`)) setSuites(suites.filter((_, j) => j !== si));
              }}>Remove</Button>
              <Button variant="primary" ariaLabel={`Run ${s.name}`} loading={starting === s.name} disabled={Boolean(why) || !s.cases.length || Boolean(following[s.name])}
                onClick={() => void run(s.name)}>Run</Button>
            </SpaceBetween>}>
            <Input ariaLabel={`Suite ${si + 1} name`} value={s.name}
              onChange={({ detail }) => setSuites(suites.map((x, j) => (j === si ? { ...x, name: detail.value.slice(0, MAX_NAME) } : x)))} />
          </Header>}>
          <SpaceBetween size="m">
            {results[s.name] ? <Result r={results[s.name]} /> : null}
            <Trend trend={trends[s.name] ?? []} />
            {s.cases.map((c, ci) => (
              <CaseEditor key={ci} n={ci + 1} c={c} agents={agents} result={results[s.name]?.cases.find((r) => r.case === c.name)}
                onChange={(next) => setSuites(suites.map((x, j) => (j === si ? { ...x, cases: x.cases.map((y, k) => (k === ci ? next : y)) } : x)))}
                onRemove={() => setSuites(suites.map((x, j) => (j === si ? { ...x, cases: x.cases.filter((_, k) => k !== ci) } : x)))} />
            ))}
            <Button iconName="add-plus" disabled={s.cases.length >= MAX_CASES}
              onClick={() => setSuites(suites.map((x, j) => (j === si ? { ...x, cases: [...x.cases, newCase(x.cases.map((y) => y.name))] } : x)))}>
              Add a case</Button>
          </SpaceBetween>
        </Container>
      ))}
    </SpaceBetween>
  );
}
