/** Test suites of a build: saved requests with checks on what the run should produce.
 *  The shape and the limits mirror bff/suites.py, which validates and runs them; the page
 *  builds only valid checks (forms, no JSON), so it holds no second validator. */

export const MAX_SUITES = 20;
export const MAX_CASES = 50;
export const MAX_CHECKS = 20;
export const MAX_NAME = 80;
export const MAX_REQUEST = 10000;
export const MAX_TEXT = 2000;

/** How a run, and an agent in it, can end. */
export const RUN_OUTCOMES = ["done", "failed", "denied", "cancelled"] as const;
export const AGENT_OUTCOMES = ["done", "failed", "skipped", "denied", "cancelled"] as const;

/** The comparisons a check on an agent's output may use (app/common/branching.py, plus
 *  notContains). */
export const OPERATORS = ["equals", "notEquals", "in", "contains", "notContains", "exists", "gt", "gte", "lt", "lte"] as const;
export type Operator = (typeof OPERATORS)[number];
export const OPERATOR_LABEL: Record<Operator, string> = {
  equals: "equals", notEquals: "does not equal", in: "is one of", contains: "contains", notContains: "does not contain",
  exists: "is present", gt: "is more than", gte: "is at least", lt: "is less than", lte: "is at most",
};

export type CheckValue = string | number | boolean | string[];
export type Check =
  | { run: (typeof RUN_OUTCOMES)[number] }
  | { cost: Partial<Record<"gt" | "gte" | "lt" | "lte", number>> }
  | { latencyMs: Partial<Record<"gt" | "gte" | "lt" | "lte", number>> }
  | ({ agent: string; field?: string; status?: (typeof AGENT_OUTCOMES)[number] } & Partial<Record<Operator, CheckValue>>);

export interface SuiteCase {
  name: string;
  request: string;
  subject?: string;
  expect: Check[];
}
export interface Suite {
  name: string;
  cases: SuiteCase[];
}

/** One run of a suite, as the trend lists it. */
export interface TrendEntry {
  suiteRunId: string;
  startedAt: string;
  buildVersion: number | null;
  finished: boolean;
  passed?: number;
  failed?: number;
  total?: number;
  notStarted?: number;
}

export interface CaseResult {
  case: string;
  sessionId: string | null;
  status: "passed" | "failed" | "running" | "not_started";
  message: string;
  checks: { passed: number; total: number };
  failed: { expected: string; actual: string; message: string; implicit?: boolean }[];
}

export interface SuiteRunResult {
  suiteRunId: string;
  suite: string;
  buildVersion: number;
  startedAt: string;
  state: "running" | "finished";
  summary: { passed: number; failed: number; running: number; notStarted: number; total: number };
  cases: CaseResult[];
  trend: TrendEntry[];
}

export interface SuiteRunStarted {
  suiteRunId: string;
  suite: string;
  buildVersion: number;
  startedAt: string;
  started: number;
  notStarted: number;
}

export function newSuite(taken: string[]): Suite {
  let name = "New suite";
  for (let i = 2; taken.includes(name); i++) name = `New suite ${i}`;
  return { name, cases: [] };
}

export function newCase(taken: string[]): SuiteCase {
  let name = "New case";
  for (let i = 2; taken.includes(name); i++) name = `New case ${i}`;
  return { name, request: "", expect: [] };
}

/** The kind of a check, for its form. */
export type CheckKind = "run" | "agentStatus" | "agentField" | "agentText" | "cost" | "latencyMs";

export function kindOf(check: Check): CheckKind {
  if ("run" in check) return "run";
  if ("cost" in check) return "cost";
  if ("latencyMs" in check) return "latencyMs";
  if (check.status !== undefined) return "agentStatus";
  return check.field !== undefined ? "agentField" : "agentText";
}

/** A new check of a kind, valid as it is made. */
export function blankCheck(kind: CheckKind, agent = ""): Check {
  switch (kind) {
    case "run": return { run: "done" };
    case "cost": return { cost: { lt: 1 } };
    case "latencyMs": return { latencyMs: { lt: 60000 } };
    case "agentStatus": return { agent, status: "done" };
    case "agentField": return { agent, field: "field", equals: "" };
    default: return { agent, contains: "" };
  }
}

/** The operator an agent check uses, if it has exactly one. */
export function operatorOf(check: Check): Operator | undefined {
  return OPERATORS.find((op) => op in check);
}

/** A check, in words, for a list. */
export function describeCheck(check: Check): string {
  if ("run" in check) return `the run ends ${check.run}`;
  if ("cost" in check || "latencyMs" in check) {
    const [name, spec] = "cost" in check ? ["the run costs", check.cost] : ["the calls take", check.latencyMs];
    const unit = "cost" in check ? (n: number) => `$${n}` : (n: number) => `${n} ms`;
    const words: Record<string, string> = { lt: "less than", lte: "at most", gt: "more than", gte: "at least" };
    return `${name} ${Object.entries(spec).map(([op, n]) => `${words[op]} ${unit(n as number)}`).join(" and ")}`;
  }
  if (check.status !== undefined) return `agent ${check.agent || "?"} ends ${check.status}`;
  const op = operatorOf(check);
  const value = op ? check[op] : undefined;
  const shown = op === "exists" ? (value ? "" : "false") : Array.isArray(value) ? value.join(", ") : JSON.stringify(value);
  const what = check.field !== undefined ? `${check.agent || "?"} › ${check.field}` : `${check.agent || "?"} output`;
  return `${what} ${op ? OPERATOR_LABEL[op] : "?"} ${shown}`.trim();
}

export function passRate(e: TrendEntry): number | null {
  return e.finished && e.total ? (e.passed ?? 0) / e.total : null;
}

/** Why a suite cannot be run from this console, if it cannot (nothing here says whether the build is
 *  deployed: the Tests tab says that itself). */
export function runBlockReason(o: { runsHere: boolean; canStart: boolean; account?: string }): string | undefined {
  if (!o.runsHere) return "This console builds and deploys and holds no runs, so a suite cannot be run from it. You can still write suites here.";
  if (!o.canStart) return "You are not allowed to start runs, which is what a test run does.";
  if (o.account) return "This build is deployed in your own AWS account and runs from its own console: run its tests there.";
  return undefined;
}
