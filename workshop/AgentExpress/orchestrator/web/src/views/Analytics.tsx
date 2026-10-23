/** The Analytics view: how this deployment is behaving, not just what it cost.
 *
 *  Per tool: calls, error rate, p50/p95 latency and the Cedar denial rate. Per agent: the
 *  same, guardrail blocks, and a health score with the reasons it is low. And the
 *  configuration warnings: agents no guardrail screens, tools no Cedar policy decides.
 *
 *  Everything comes from GET /api/analytics (bff/analytics.py), which reads every user's
 *  telemetry and so needs the `admin` permission; App only offers this view to an admin,
 *  and the server refuses everyone else. The formula for the score is written out in
 *  bff/analytics.py and DEPLOYMENT.md; this view shows the number and the reasons it was
 *  given, and computes nothing of its own. No prompt or answer text is ever returned. */
import { useCollection } from "@cloudscape-design/collection-hooks";
import Alert from "@cloudscape-design/components/alert";
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import Container from "@cloudscape-design/components/container";
import DatePicker from "@cloudscape-design/components/date-picker";
import Header from "@cloudscape-design/components/header";
import Select from "@cloudscape-design/components/select";
import SpaceBetween from "@cloudscape-design/components/space-between";
import StatusIndicator, { type StatusIndicatorProps } from "@cloudscape-design/components/status-indicator";
import Table, { type TableProps } from "@cloudscape-design/components/table";
import { type ReactNode, useCallback, useEffect, useState } from "react";
import { api } from "../api";
import { dayBefore, daysIn } from "./Activity";
import { useDocsBase } from "../docs/docsLinks";
import { infoSlot } from "../docs/InfoLink";

export interface ToolStats {
  tool: string;
  calls: number;
  errors: number;
  otherStatus: number;
  allowed: number;
  denied: number;
  logOnly: number;
  errorRate: number | null;
  denialRate: number | null;
  p50Ms: number | null;
  p95Ms: number | null;
}

export interface AgentStats {
  agentId: string;
  rows: number;
  calls: number;
  errors: number;
  otherStatus: number;
  other: number;
  guardrailChecks: number;
  guardrailBlocks: number;
  allowed: number;
  denied: number;
  logOnly: number;
  errorRate: number | null;
  guardrailBlockRate: number | null;
  denialRate: number | null;
  p50Ms: number | null;
  p95Ms: number | null;
  health: number | null;
  reasons: string[];
}

export interface Coverage {
  agents: { agentId: string; name: string; input: boolean; output: boolean; guardrail: string | null; warning: string | null }[];
  tools: { tool: string; type: string; warning: string | null }[];
  policyEngine: { enabled: boolean; mode: string };
}

export interface AnalyticsReport {
  from: string;
  to: string;
  rowsRead: number;
  capped: boolean;
  maxRows: number;
  percentiles: string;
  kinds: Record<string, number>;
  tools: ToolStats[];
  agents: AgentStats[];
  coverage: Coverage;
}

/** The server reads at most this many days at once (handler.py _MAX_AGGREGATE_DAYS). */
export const MAX_DAYS = 92;

const RANGES = [
  { value: "6", label: "Last 7 days" },
  { value: "29", label: "Last 30 days" },
  { value: String(MAX_DAYS - 1), label: `Last ${MAX_DAYS} days` },
  { value: "custom", label: "Custom range" },
];

/** "12.5%", or "—" where there was nothing to divide by. */
export function pct(rate: number | null | undefined): string {
  if (rate === null || rate === undefined) return "—";
  return `${(Math.round(rate * 1000) / 10).toString()}%`;
}

export function ms(v: number | null | undefined): string {
  return v === null || v === undefined ? "—" : `${v} ms`;
}

/** How a score reads at a glance. The bands are display only; the score is the server's. */
export function healthType(score: number | null): StatusIndicatorProps.Type {
  if (score === null) return "stopped";
  return score >= 90 ? "success" : score >= 70 ? "warning" : "error";
}

export function analyticsPath(from: string, to: string, build = ""): string {
  const q = new URLSearchParams({ from, to });
  if (build) q.set("build", build);
  return `/api/analytics?${q.toString()}`;
}

/** Every configuration warning, agents first, each naming what it is about. */
export function warningsOf(c: Coverage | undefined): string[] {
  if (!c) return [];
  return [
    ...c.agents.filter((a) => a.warning).map((a) => `Agent ${a.name} (${a.agentId}): ${a.warning}`),
    ...c.tools.filter((t) => t.warning).map((t) => `Tool ${t.tool}: ${t.warning}`),
  ];
}

/** Sorts a column that may be empty, with the empty ones last either way round. */
function nullable<T>(get: (x: T) => number | null): (a: T, b: T) => number {
  return (a, b) => {
    const x = get(a), y = get(b);
    if (x === null && y === null) return 0;
    if (x === null) return 1;
    if (y === null) return -1;
    return x - y;
  };
}

/** One comparator instance per column for the life of the page: the Table finds the
 *  sorted column by comparator identity, so a fresh one per render loses the sort. */
const SORT = {
  agentByErrorRate: nullable((x: AgentStats) => x.errorRate),
  agentByHealth: nullable((x: AgentStats) => x.health),
  agentByP50Ms: nullable((x: AgentStats) => x.p50Ms),
  agentByP95Ms: nullable((x: AgentStats) => x.p95Ms),
  toolByDenialRate: nullable((x: ToolStats) => x.denialRate),
  toolByErrorRate: nullable((x: ToolStats) => x.errorRate),
  toolByP50Ms: nullable((x: ToolStats) => x.p50Ms),
  toolByP95Ms: nullable((x: ToolStats) => x.p95Ms),
};

/** A sortable table, sorted at first by the column `sortBy` names. The default state is
 *  the column definition itself, so its header shows as the sorted one. */
function SortedTable<T>({ items, columns, header, empty, trackBy, sortBy }: {
  items: T[]; columns: TableProps.ColumnDefinition<T>[]; header: ReactNode; empty: string;
  trackBy: string; sortBy: string;
}) {
  const { items: shown, collectionProps } = useCollection(items, {
    sorting: { defaultState: { sortingColumn: columns.find((c) => c.id === sortBy) ?? columns[0] } },
  });
  return (
    <Table
      {...collectionProps}
      variant="container"
      items={shown}
      trackBy={trackBy}
      header={header}
      columnDefinitions={columns}
      empty={<Box textAlign="center" color="inherit">{empty}</Box>}
    />
  );
}

export function Analytics({ build = "" }: { build?: string }) {
  const docs = useDocsBase();
  const [range, setRange] = useState("6");
  const [from, setFrom] = useState(dayBefore(6));
  const [to, setTo] = useState(dayBefore(0));
  const [data, setData] = useState<AnalyticsReport | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const days = daysIn(from, to);
  const rangeError = !days ? "“To” must be on or after “From”."
    : days > MAX_DAYS ? `Pick at most ${MAX_DAYS} days.` : "";

  const load = useCallback(async () => {
    if (rangeError) { setData(null); setLoading(false); return; }
    setLoading(true);
    setError("");
    try {
      setData(await api.get<AnalyticsReport>(analyticsPath(from, to, build)));
    } catch (e) {
      setError((e as Error).message);
      setData(null);
    } finally {
      setLoading(false);
    }
  }, [from, to, build, rangeError]);
  useEffect(() => { void load(); }, [load]);

  const pickRange = (value: string) => {
    setRange(value);
    if (value !== "custom") { setFrom(dayBefore(Number(value))); setTo(dayBefore(0)); }
  };
  const names = Object.fromEntries((data?.coverage.agents ?? []).map((a) => [a.agentId, a.name]));
  const warnings = warningsOf(data?.coverage);
  const other = data?.kinds.other ?? 0;

  return (
    <SpaceBetween size="l">
      <Header
        variant="h1"
        info={infoSlot(docs, "analytics")}
        description={rangeError || (data
          ? `Every user's calls from ${data.from} to ${data.to}: ${data.rowsRead} telemetry rows read. `
            + `Latency percentiles are ${data.percentiles}.`
          : "Every user's calls over a date range.")}
        actions={
          <SpaceBetween direction="horizontal" size="xs">
            <Select
              ariaLabel="Date range"
              selectedOption={RANGES.find((r) => r.value === range) ?? RANGES[0]}
              options={RANGES}
              onChange={({ detail }) => pickRange(detail.selectedOption.value ?? "6")}
            />
            <DatePicker
              value={from}
              onChange={({ detail }) => { if (detail.value) { setFrom(detail.value); setRange("custom"); } }}
              placeholder="From YYYY/MM/DD"
              openCalendarAriaLabel={(d) => `Choose the first day${d ? `, selected ${d}` : ""}`}
            />
            <DatePicker
              value={to}
              onChange={({ detail }) => { if (detail.value) { setTo(detail.value); setRange("custom"); } }}
              placeholder="To YYYY/MM/DD"
              openCalendarAriaLabel={(d) => `Choose the last day${d ? `, selected ${d}` : ""}`}
            />
            <Button iconName="refresh" ariaLabel="Refresh analytics" onClick={() => void load()} />
          </SpaceBetween>
        }
      >
        Analytics
      </Header>

      {error ? <Alert type="error" header="Could not load analytics">{error}</Alert> : null}
      {data?.capped ? (
        <Alert type="warning" header="Only part of the range was read">
          {`The newest ${data.maxRows} rows were read and the figures cover those only. Narrow the range to see all of it.`}
        </Alert>
      ) : null}
      {other ? (
        <Alert type="info">
          {`${other} row${other === 1 ? " is" : "s are"} of a kind this console does not know; `
            + "they are counted as “other” on their agent."}
        </Alert>
      ) : null}

      <Container header={<Header variant="h2" counter={data ? `(${warnings.length})` : undefined}
        description="From the deployed workflow.json, not from telemetry." info={infoSlot(docs, "analytics.warnings")}>Configuration warnings</Header>}>
        {loading ? <StatusIndicator type="loading">Loading</StatusIndicator>
          : !data ? <Box color="text-status-inactive">—</Box>
          : warnings.length
            ? <ul>{warnings.map((w) => <li key={w}><StatusIndicator type="warning">{w}</StatusIndicator></li>)}</ul>
            : <StatusIndicator type="success">Every agent is screened by a guardrail and every tool is decided by Cedar.</StatusIndicator>}
      </Container>

      <SortedTable<AgentStats>
        items={data?.agents ?? []}
        trackBy="agentId"
        empty={loading ? "Loading" : "No agent made a call in this range."}
        sortBy="health"
        header={<Header variant="h2" counter={data ? `(${data.agents.length})` : undefined} info={infoSlot(docs, "analytics.agents")}
          description="Health = 100 − 60 × error rate − 20 × Cedar denial rate − 10 × guardrail block rate − 10 if p95 model latency is over 3× its median (5+ calls).">
          Agents</Header>}
        columns={[
          { id: "agent", header: "Agent", sortingField: "agentId", isRowHeader: true,
            cell: (a) => names[a.agentId] ? `${names[a.agentId]} (${a.agentId})` : a.agentId },
          { id: "health", header: "Health", sortingComparator: SORT.agentByHealth,
            cell: (a) => <StatusIndicator type={healthType(a.health)}>{a.health === null ? "—" : String(a.health)}</StatusIndicator> },
          { id: "reasons", header: "Why", cell: (a) => a.reasons.join("; ") || "—", minWidth: 260 },
          { id: "calls", header: "Calls", sortingField: "calls", cell: (a) => a.calls },
          { id: "errorRate", header: "Error rate", sortingComparator: SORT.agentByErrorRate,
            cell: (a) => pct(a.errorRate) },
          { id: "blocks", header: "Guardrail blocks", sortingField: "guardrailBlocks",
            cell: (a) => `${a.guardrailBlocks} of ${a.guardrailChecks}` },
          { id: "denials", header: "Cedar denials", sortingField: "denied",
            cell: (a) => `${a.denied} of ${a.allowed + a.denied}` },
          { id: "p50", header: "p50 model", sortingComparator: SORT.agentByP50Ms, cell: (a) => ms(a.p50Ms) },
          { id: "p95", header: "p95 model", sortingComparator: SORT.agentByP95Ms, cell: (a) => ms(a.p95Ms) },
          { id: "other", header: "Other", sortingField: "other",
            cell: (a) => (a.other || a.otherStatus ? `${a.other} kind, ${a.otherStatus} status` : "—") },
        ]}
      />

      <SortedTable<ToolStats>
        items={data?.tools ?? []}
        trackBy="tool"
        empty={loading ? "Loading" : "No tool was called in this range."}
        sortBy="tool"
        header={<Header variant="h2" counter={data ? `(${data.tools.length})` : undefined} info={infoSlot(docs, "analytics.tools")}
          description="Cedar denial rate is denied ÷ (allowed + denied). In LOG_ONLY mode the decision is in CloudWatch, so those calls show as log-only with no rate.">
          Tools</Header>}
        columns={[
          { id: "tool", header: "Tool", sortingField: "tool", isRowHeader: true, cell: (t) => t.tool },
          { id: "calls", header: "Calls", sortingField: "calls", cell: (t) => t.calls },
          { id: "errorRate", header: "Error rate", sortingComparator: SORT.toolByErrorRate,
            cell: (t) => pct(t.errorRate) },
          { id: "p50", header: "p50", sortingComparator: SORT.toolByP50Ms, cell: (t) => ms(t.p50Ms) },
          { id: "p95", header: "p95", sortingComparator: SORT.toolByP95Ms, cell: (t) => ms(t.p95Ms) },
          { id: "denialRate", header: "Cedar denial rate", sortingComparator: SORT.toolByDenialRate,
            cell: (t) => pct(t.denialRate) },
          { id: "decisions", header: "Allowed / denied / log-only",
            cell: (t) => `${t.allowed} / ${t.denied} / ${t.logOnly}` },
        ]}
      />
    </SpaceBetween>
  );
}
