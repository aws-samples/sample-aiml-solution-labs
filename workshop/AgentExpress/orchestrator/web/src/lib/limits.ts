/** What the Start run box says about `orchestrator.limits`: how much of each limit the
 *  caller has used, and a warning when they are close to one or an error when they have hit
 *  it. Pure, so the wording is tested without rendering anything. The server decides what is
 *  reached (bff/limits.py usage); this only words it. */
import type { LimitsUsage, LimitUse } from "../types";

export interface UsageNotice {
  /** One short line per limit that is set. */
  lines: string[];
  /** Set when a limit is reached (error) or close (warning). */
  alert?: { type: "warning" | "error"; text: string };
  /** A limit is reached: the next run would be refused. */
  blocked: boolean;
}

/** Cents for a dollar or more, four places below that. Same rule as bff/limits.py usd(). */
export function usd(v: number): string {
  const d = Math.abs(v);
  const n = d >= 1 || d === 0 ? v.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })
    : v.toLocaleString("en-US", { minimumFractionDigits: 4, maximumFractionDigits: 4 });
  return `$${n}`;
}

const pct = (u: LimitUse) => Math.min(100, Math.floor((u.used / u.limit) * 100));

/** In the order the server checks them: the app's month, then your day, then your hour. */
const ORDER = [
  { key: "monthly" as const, line: (u: LimitUse) => `This month, everyone: ${usd(u.used)} of ${usd(u.limit)}`,
    reached: "The app's monthly spend limit is reached", close: "the app's monthly spend limit" },
  { key: "daily" as const, line: (u: LimitUse) => `Today: ${usd(u.used)} of ${usd(u.limit)}`,
    reached: "Today's spend limit is reached", close: "today's spend limit" },
  { key: "hourly" as const, line: (u: LimitUse) => `This hour: ${u.used} of ${u.limit} runs`,
    reached: "You have started this hour's runs", close: "this hour's runs" },
];

/** Null when there is nothing to say: no limit is set, or none could be read. */
export function describeUsage(usage: LimitsUsage | null | undefined): UsageNotice | null {
  if (!usage || !usage.limits) return null;
  const lines: string[] = [];
  let alert: UsageNotice["alert"];
  let blocked = false;
  for (const o of ORDER) {
    const u = usage[o.key];
    if (!u || !(u.limit > 0)) continue;
    lines.push(o.line(u));
    if (u.reached) {
      blocked = true;
      // The first reached limit is the one the server would name.
      if (alert?.type !== "error") alert = { type: "error", text: `${o.reached}. It resets at ${u.resets}.` };
    } else if (u.warn && !alert) {
      alert = { type: "warning", text: `You have used ${pct(u)}% of ${o.close}. It resets at ${u.resets}.` };
    }
  }
  return lines.length ? { lines, alert, blocked } : null;
}
