/** Timestamps for display, in the VIEWER'S timezone.
 *
 *  The backend stores US Eastern wall-clock stamps (`YYYY-MM-DD HH:MM:SS`, see
 *  app/common/clock.py): they are sort keys and the telemetry table's date partition, so
 *  changing the stored zone would re-key existing data. Display is a different question.
 *  It used to be Eastern too, hard-coded here, while other pages used the browser's zone
 *  and Activity used UTC — so one console showed three zones, and a reader outside the
 *  US East coast saw run times hours away from their own clock.
 *
 *  Now every stamp is turned into an instant (Eastern wall time -> UTC with the same
 *  statutory DST rule the Python clock uses) and shown in the browser's own timezone.
 *  Dependency-free for the same reason the Python helpers are. */

const STAMP = /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})/;

/** The nth Sunday (1-based) of a month, as a day-of-month. */
function nthSunday(year: number, month0: number, n: number): number {
  const first = new Date(Date.UTC(year, month0, 1)).getUTCDay();
  return 1 + ((7 - first) % 7) + (n - 1) * 7;
}

/** Epoch ms for a US Eastern wall-clock stamp, or NaN. EDT (UTC-4) from the 2nd Sunday of
 *  March 02:00 to the 1st Sunday of November 02:00 local, otherwise EST (UTC-5). */
export function etToEpochMs(stamp: string): number {
  const m = STAMP.exec(stamp.trim());
  if (!m) return Number.NaN;
  const [y, mo, d, h, mi, s] = m.slice(1).map(Number);
  const wall = Date.UTC(y, mo - 1, d, h, mi, s);
  const dstStart = Date.UTC(y, 2, nthSunday(y, 2, 2), 2);
  const dstEnd = Date.UTC(y, 10, nthSunday(y, 10, 1), 2);
  const offsetH = wall >= dstStart && wall < dstEnd ? 4 : 5;
  return wall + offsetH * 3600_000;
}

/** Any backend timestamp as epoch ms: an Eastern stamp (optionally "<stamp>#<seq>", the
 *  events table's sort key), epoch seconds or milliseconds, or an ISO string with a zone. */
export function toEpochMs(ts: string | number | null | undefined): number {
  if (ts == null || ts === "") return Number.NaN;
  if (typeof ts === "number") return ts < 1e12 ? ts * 1000 : ts;
  const head = ts.split("#")[0].trim();
  if (/[zZ]$|[+-]\d{2}:?\d{2}$/.test(head)) return Date.parse(head);
  if (STAMP.test(head)) return etToEpochMs(head);
  const n = Number.parseInt(head, 10);
  if (Number.isNaN(n)) return Number.NaN;
  return n < 1e12 ? n * 1000 : n;
}

/** `YYYY-MM-DD HH:MM:SS` in `timeZone` (default: the browser's own). */
export function fmtTime(ts: string | number | null | undefined, timeZone?: string): string {
  const ms = toEpochMs(ts);
  if (!ms || Number.isNaN(ms)) return "";
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false,
  }).formatToParts(new Date(ms));
  const g = (t: string) => parts.find((p) => p.type === t)?.value ?? "";
  const hh = g("hour") === "24" ? "00" : g("hour");
  return `${g("year")}-${g("month")}-${g("day")} ${hh}:${g("minute")}:${g("second")}`;
}

/** Elapsed time between two backend stamps, for a run's duration. Correct across a DST
 *  change: it used to pin both ends to -05:00, so a run spanning one was an hour off. */
export function duration(from?: string, to?: string): string {
  const a = toEpochMs(from);
  const b = toEpochMs(to);
  if (Number.isNaN(a) || Number.isNaN(b) || b < a) return "—";
  const s = Math.round((b - a) / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${s % 60}s`;
  return `${Math.floor(m / 60)}h ${m % 60}m`;
}
