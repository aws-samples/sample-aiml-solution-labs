/** Undo and redo for a build, whoever changed it: a drag on the canvas, a form, an
 *  upload, or AgentExpress Assistant. It is a list of snapshots of the whole project, which is
 *  small (a workflow and its prompts) and makes undo exact.
 *
 *  Typing in a field changes the project on every keystroke; those are merged into one
 *  step when they come less than `mergeMs` apart, so undo reverts the edit, not a letter. */

export interface History<T> {
  past: T[];
  present: T;
  future: T[];
  /** When `present` last became a new step (ms), for merging rapid edits. */
  at: number;
}

export const LIMIT = 100;

export function start<T>(present: T, now = Date.now()): History<T> {
  return { past: [], present, future: [], at: now };
}

/** Record `next`. A change within `mergeMs` of the previous one replaces it, unless
 *  `step` says it is a separate step (an upload, a Design-with-AI change). */
export function record<T>(h: History<T>, next: T, now = Date.now(),
  { mergeMs = 1000, step = false }: { mergeMs?: number; step?: boolean } = {}): History<T> {
  if (next === h.present) return h;
  if (!step && h.past.length && now - h.at < mergeMs) {
    return { ...h, present: next, future: [], at: now };
  }
  return { past: [...h.past, h.present].slice(-LIMIT), present: next, future: [], at: now };
}

export function undo<T>(h: History<T>): History<T> {
  if (!h.past.length) return h;
  const prev = h.past[h.past.length - 1];
  return { past: h.past.slice(0, -1), present: prev, future: [h.present, ...h.future], at: 0 };
}

export function redo<T>(h: History<T>): History<T> {
  if (!h.future.length) return h;
  const [next, ...rest] = h.future;
  return { past: [...h.past, h.present], present: next, future: rest, at: 0 };
}
