/** The audit log: who signed in and out, and who created, designed, deployed, destroyed
 *  or deleted what, where and when — and every action on a run (start, gate decision,
 *  re-run, evaluate, cancel, delete). Filter by kind of action and by text (a user,
 *  a build, a run id).
 *
 *  Your own activity, or with the `audit` permission (workflow.json
 *  authorization.actions) everyone's over a range of up to 31 days, the last 7 by
 *  default — the server enforces that; the toggle is only hidden from those who would
 *  get a 403. */
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import DatePicker from "@cloudscape-design/components/date-picker";
import Header from "@cloudscape-design/components/header";
import SegmentedControl from "@cloudscape-design/components/segmented-control";
import Pagination from "@cloudscape-design/components/pagination";
import Select from "@cloudscape-design/components/select";
import SpaceBetween from "@cloudscape-design/components/space-between";
import TextFilter from "@cloudscape-design/components/text-filter";
import StatusIndicator, { type StatusIndicatorProps } from "@cloudscape-design/components/status-indicator";
import Table from "@cloudscape-design/components/table";
import { useCallback, useEffect, useMemo, useState } from "react";
import { api } from "../api";

export interface AuditEvent {
  id: string;
  ts: string;
  owner: string;
  email: string;
  action: string;
  detail: Record<string, unknown>;
}

const LABELS: Record<string, [string, StatusIndicatorProps.Type]> = {
  "login": ["Signed in", "info"],
  "logout": ["Signed out", "info"],
  "deploy.requested": ["Deploy requested", "pending"],
  "deploy.succeeded": ["Deployed", "success"],
  "deploy.failed": ["Deploy failed", "error"],
  "destroy.requested": ["Destroy requested", "pending"],
  "destroy.succeeded": ["Destroyed", "success"],
  "destroy.failed": ["Destroy failed", "error"],
  "build.created": ["Build created", "success"],
  "design.message": ["Asked the designer", "info"],
  "build.deleted": ["Build deleted", "stopped"],
  "account.connected": ["Account connected", "success"],
  "account.updated": ["Account edited", "info"],
  "account.disconnected": ["Account disconnected", "stopped"],
  "secrets.updated": ["Secrets changed", "info"],
  "login.viewed": ["Viewed app password", "info"],
  "run.started": ["Run started", "in-progress"],
  "run.completed": ["Run completed", "success"],
  "run.failed": ["Run failed", "error"],
  "run.denied": ["Run ended (denied)", "stopped"],
  "run.decided": ["Gate decided", "success"],
  "run.rerun": ["Re-ran agents", "in-progress"],
  "run.evaluated": ["Evaluation requested", "info"],
  "run.cancelled": ["Run cancelled", "stopped"],
  "run.deleted": ["Run deleted", "stopped"],
  "policy.generated": ["Drafted a policy", "info"],
  "policy.created": ["Policy saved", "success"],
  "policy.updated": ["Policy edited", "info"],
  "policy.deleted": ["Policy deleted", "stopped"],
  "build.shared": ["Build shared", "info"],
  "lambda.code.updated": ["Tool code changed", "info"],
  "tool.tested": ["Tool tested", "info"],
  "library.created": ["Library item added", "success"],
  "library.updated": ["Library item edited", "info"],
  "library.deleted": ["Library item deleted", "stopped"],
  "library.shared": ["Library item shared", "info"],
  "group.saved": ["Group saved", "success"],
  "group.deleted": ["Group deleted", "stopped"],
  "admin.viewed": ["Admin viewed", "info"],
};
/** The kinds the filter offers, each a prefix set of actions. */
export const KINDS: { value: string; label: string; match: (a: string) => boolean }[] = [
  { value: "", label: "All actions", match: () => true },
  { value: "sign", label: "Sign-in and sign-out", match: (a) => a === "login" || a === "logout" },
  { value: "build", label: "Builds and design", match: (a) => a.startsWith("build.") || a.startsWith("design.") || a === "secrets.updated" || a === "login.viewed" },
  { value: "deploy", label: "Deploy and destroy", match: (a) => a.startsWith("deploy.") || a.startsWith("destroy.") },
  { value: "run", label: "Runs", match: (a) => a.startsWith("run.") },
  { value: "account", label: "AWS accounts", match: (a) => a.startsWith("account.") },
  { value: "policy", label: "Policies", match: (a) => a.startsWith("policy.") },
  { value: "library", label: "Library, tools and groups", match: (a) => a.startsWith("library.") || a.startsWith("group.") || a.startsWith("tool.") || a.startsWith("lambda.") },
  { value: "failed", label: "Failures", match: (a) => a.endsWith(".failed") },
];
const PAGE = 50;

const TOOLS: Record<string, string> = { cdk: "AWS CDK", terraform: "Terraform" };

export function actionLabel(action: string): [string, StatusIndicatorProps.Type] {
  return LABELS[action] ?? [action, "info"];
}

/** "Claims v3" — the build an event is about, if any. */
export function buildOf(e: AuditEvent): string {
  const d = e.detail;
  if (!d.build) return "—";
  const name = String(d.name || d.build);
  return d.version ? `${name} v${String(d.version)}` : name;
}

/** "account 123456789012 · eu-west-1", or "this console's account". */
export function whereOf(e: AuditEvent): string {
  const d = e.detail;
  const account = d.account === "console" ? "this console's account" : d.account ? `account ${String(d.account)}` : "";
  return [account, d.region ? String(d.region) : ""].filter(Boolean).join(" · ") || "—";
}

/** The rest: tool, error, what changed, where from. Never a secret value: the server
 *  records only names. */
export function detailsOf(e: AuditEvent): string {
  const d = e.detail;
  const parts: string[] = [];
  if (d.tool) parts.push(TOOLS[String(d.tool)] ?? String(d.tool));
  if (d.thenDelete || d.deleted) parts.push("then delete the build");
  if (d.changed && typeof d.changed === "object") {
    parts.push(Object.entries(d.changed as Record<string, string[]>)
      .map(([kind, names]) => `${kind}: ${names.join(", ")}`).join("; "));
  }
  if (e.action.startsWith("policy.") && d.name) parts.push(`policy ${String(d.name)}`);
  if (e.action.startsWith("library.") && (d.kind || d.name)) parts.push([d.kind, d.name].filter(Boolean).map(String).join(" "));
  if (e.action === "tool.tested" && d.ok === false) parts.push("failed");
  if (d.group) parts.push(`group ${String(d.group)}${typeof d.members === "number" ? `, ${d.members} members` : ""}`);
  if (d.shares && typeof d.shares === "object") {
    const s = d.shares as { everyone?: boolean; emails?: string[]; groups?: string[] };
    parts.push(s.everyone ? "shared with everyone"
      : `shared with ${[...(s.emails ?? []), ...(s.groups ?? []).map((g) => `group ${g}`)].join(", ") || "nobody"}`);
  }
  if (d.request) parts.push(`“${String(d.request)}”`);
  if (d.label) parts.push(`named ${String(d.label)}`);
  if (d.session) parts.push(`run ${String(d.session)}`);
  if (d.topic) parts.push(`“${String(d.topic)}”`);
  if (d.decision) {
    const per = d.decisions && typeof d.decisions === "object"
      ? Object.entries(d.decisions as Record<string, string>).map(([a, v]) => `${a}: ${v}`).join(", ") : "";
    parts.push(per ? `per agent (${per})` : String(d.decision));
  }
  if (Array.isArray(d.agents) && d.agents.length) parts.push(`agents: ${d.agents.join(", ")}`);
  if (d.comment) parts.push(`comment: ${String(d.comment)}`);
  if (d.message) parts.push(`“${String(d.message)}”`);
  if (d.error) parts.push(String(d.error));
  if (d.ip) parts.push(`from ${String(d.ip)}`);
  return parts.join(" · ");
}
/** The events a filter keeps: of that kind, and containing the text anywhere a person
 *  reads it (who, what, which build, where, the details). */
export function filterEvents(events: AuditEvent[], kind: string, text: string): AuditEvent[] {
  const k = KINDS.find((x) => x.value === kind) ?? KINDS[0];
  const q = text.trim().toLowerCase();
  return events.filter((e) => k.match(e.action) && (!q || [e.email, e.owner, actionLabel(e.action)[0], e.action,
    buildOf(e), whereOf(e), detailsOf(e)].join(" ").toLowerCase().includes(q)));
}

/** A UTC day, `back` days before `from`. */
export function dayBefore(back: number, from = new Date()): string {
  return new Date(from.getTime() - back * 86_400_000).toISOString().slice(0, 10);
}
/** The server reads at most this many days at once (bff/audit.py MAX_DAYS). */
export const MAX_DAYS = 31;
export const RANGES = [
  { value: "0", label: "Today" },
  { value: "6", label: "Last 7 days" },
  { value: "13", label: "Last 14 days" },
  { value: "30", label: "Last 31 days" },
  { value: "custom", label: "Custom range" },
];
/** Days from `from` to `to`, both included; 0 when either is missing or they are reversed. */
export function daysIn(from: string, to: string): number {
  const a = Date.parse(`${from}T00:00:00Z`), b = Date.parse(`${to}T00:00:00Z`);
  return Number.isNaN(a) || Number.isNaN(b) || b < a ? 0 : Math.round((b - a) / 86_400_000) + 1;
}

export function Activity({ canSeeEveryone }: { canSeeEveryone: boolean }) {
  // An auditor lands on everyone's last 7 days; anyone else on their own activity.
  const [scope, setScope] = useState<"mine" | "all">(canSeeEveryone ? "all" : "mine");
  const [range, setRange] = useState("6");
  const [from, setFrom] = useState(dayBefore(6));
  const [to, setTo] = useState(dayBefore(0));
  const days = daysIn(from, to);
  const rangeError = !days ? "“To” must be on or after “From”."
    : days > MAX_DAYS ? `Pick at most ${MAX_DAYS} days.` : "";
  const pickRange = (value: string) => {
    setRange(value);
    if (value !== "custom") { setFrom(dayBefore(Number(value))); setTo(dayBefore(0)); }
  };
  const [events, setEvents] = useState<AuditEvent[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [kind, setKind] = useState("");
  const [text, setText] = useState("");
  const [page, setPage] = useState(1);
  const shown = useMemo(() => filterEvents(events, kind, text), [events, kind, text]);
  useEffect(() => { setPage(1); }, [kind, text, events]);
  const pages = Math.max(1, Math.ceil(shown.length / PAGE));

  const load = useCallback(async () => {
    if (scope === "all" && rangeError) { setEvents([]); setLoading(false); return; }
    setLoading(true);
    setError("");
    try {
      const q = scope === "all" ? `?scope=all&from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}` : "";
      setEvents(await api.get<AuditEvent[]>(`/api/audit${q}`));
    } catch (e) {
      setError((e as Error).message);
      setEvents([]);
    } finally {
      setLoading(false);
    }
  }, [scope, from, to, rangeError]);

  useEffect(() => { void load(); }, [load]);

  const everyone = scope === "all";
  return (
    <Table
      variant="full-page"
      loading={loading}
      loadingText="Loading activity"
      items={shown.slice((page - 1) * PAGE, page * PAGE)}
      trackBy="id"
      filter={
        <SpaceBetween direction="horizontal" size="xs">
          <TextFilter filteringText={text} filteringPlaceholder="Find a user, build, run or detail"
            filteringAriaLabel="Filter activity" countText={`${shown.length} match${shown.length === 1 ? "" : "es"}`}
            onChange={({ detail }) => setText(detail.filteringText)} />
          <Select
            ariaLabel="Kind of action"
            selectedOption={{ value: kind, label: (KINDS.find((k) => k.value === kind) ?? KINDS[0]).label }}
            options={KINDS.map((k) => ({ value: k.value, label: k.label }))}
            onChange={({ detail }) => setKind(detail.selectedOption.value ?? "")}
          />
        </SpaceBetween>
      }
      pagination={<Pagination currentPageIndex={page} pagesCount={pages}
        onChange={({ detail }) => setPage(detail.currentPageIndex)} />}
      header={
        <Header
          variant="awsui-h1-sticky"
          counter={loading ? undefined : shown.length === events.length ? `(${events.length})` : `(${shown.length}/${events.length})`}
          description={everyone
            ? (rangeError || `Every user's sign-ins, builds, library items, policies, accounts, deploys and runs from ${from} to ${to} (UTC), newest first.`)
            : "Your sign-ins, and what you built, deployed, ran, approved or deleted — newest first."}
          actions={
            <SpaceBetween direction="horizontal" size="xs">
              {canSeeEveryone ? (
                <SegmentedControl
                  label="Whose activity"
                  selectedId={scope}
                  onChange={({ detail }) => setScope(detail.selectedId as "mine" | "all")}
                  options={[{ id: "mine", text: "Mine" }, { id: "all", text: "Everyone" }]}
                />
              ) : null}
              {everyone ? (
                <Select
                  ariaLabel="Date range"
                  selectedOption={RANGES.find((r) => r.value === range) ?? RANGES[1]}
                  options={RANGES}
                  onChange={({ detail }) => pickRange(detail.selectedOption.value ?? "6")}
                />
              ) : null}
              {everyone ? (
                <DatePicker
                  value={from}
                  onChange={({ detail }) => { if (detail.value) { setFrom(detail.value); setRange("custom"); } }}
                  placeholder="From YYYY/MM/DD"
                  openCalendarAriaLabel={(d) => `Choose the first day${d ? `, selected ${d}` : ""}`}
                />
              ) : null}
              {everyone ? (
                <DatePicker
                  value={to}
                  onChange={({ detail }) => { if (detail.value) { setTo(detail.value); setRange("custom"); } }}
                  placeholder="To YYYY/MM/DD"
                  openCalendarAriaLabel={(d) => `Choose the last day${d ? `, selected ${d}` : ""}`}
                />
              ) : null}
              <Button iconName="refresh" ariaLabel="Refresh activity" onClick={() => void load()} />
            </SpaceBetween>
          }
        >
          Activity
        </Header>
      }
      columnDefinitions={[
        { id: "ts", header: "When", cell: (e) => new Date(e.ts).toLocaleString() },
        ...(everyone ? [{ id: "user", header: "Who", cell: (e: AuditEvent) => e.email || e.owner }] : []),
        {
          id: "action", header: "What",
          cell: (e) => {
            const [text, type] = actionLabel(e.action);
            return <StatusIndicator type={type}>{text}</StatusIndicator>;
          },
        },
        { id: "build", header: "Build", cell: buildOf },
        { id: "where", header: "Where", cell: whereOf },
        { id: "detail", header: "Details", cell: detailsOf },
      ]}
      empty={
        <Box textAlign="center" color="inherit">
          {error ? <StatusIndicator type="error">{error}</StatusIndicator>
            : events.length ? "Nothing matches the filter." : "No activity recorded."}
        </Box>
      }
    />
  );
}
