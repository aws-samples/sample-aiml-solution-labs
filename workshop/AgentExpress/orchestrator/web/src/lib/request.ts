/** A run's request is whatever the user typed to start it — a line, or pages of
 *  instructions. Lists and headings show its first line, shortened; the run page shows
 *  it whole. */

/** Longest request the console accepts; the BFF enforces the same (TOPIC_MAX). */
export const REQUEST_MAX = 10000;

/** The first non-empty line, at most `max` characters, with "…" when anything was cut. */
export function requestTitle(topic: string | undefined | null, max = 90): string {
  const text = String(topic ?? "").trim();
  if (!text) return "";
  const first = text.split(/\r?\n/).find((l) => l.trim())?.trim() ?? "";
  const cut = first.length > max ? `${first.slice(0, max - 1).trimEnd()}…` : first;
  return cut !== text ? (cut.endsWith("…") ? cut : `${cut}…`) : cut;
}
