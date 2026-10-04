/** Checks one Cedar policy for an AgentCore Gateway before it is saved or deployed.
 *
 *  A port of bff/cedar.py, message for message (tests/fixtures/validation_cases.json
 *  holds both to it). A structural check, not a Cedar evaluator: one permit/forbid
 *  statement; an action naming a tool of this build; the resource
 *  AgentCore::Gateway::"{{gateway}}" (the IaC fills in the ARN at deploy); and a `when`
 *  on a permit for one tool, which AgentCore otherwise rejects as overly permissive. */

export const GATEWAY = "{{gateway}}";
/** A policy in `policies` written for whichever tool it is attached to. */
export const TOOL = "{{tool}}";
export const MAX_LEN = 10000;
/** Names the AgentCore policy custom_<name>_<8 hex>: its 48-character limit leaves 32. */
export const NAME_RE = /^[A-Za-z][A-Za-z0-9]{0,31}$/;
export const SEP = "___";

type Tok = [kind: string, value: string];
const TOKEN = /(\s+)|(\/\/[^\n]*)|("(?:[^"\\]|\\.)*")|([A-Za-z_][A-Za-z0-9_]*)|(\d+)|(::|==|!=|<=|>=|&&|\|\||[()[\]{},;<>!.+\-*@])/y;
const KINDS = ["ws", "comment", "str", "ident", "num", "op"];
const CLOSE: Record<string, string> = { "(": ")", "[": "]", "{": "}" };
const CLOSERS = new Set(Object.values(CLOSE));

class Bad extends Error {}

function tokens(text: string): Tok[] {
  const out: Tok[] = [];
  let i = 0;
  while (i < text.length) {
    TOKEN.lastIndex = i;
    const m = TOKEN.exec(text);
    if (!m) {
      if (text[i] === '"') throw new Bad('a string is not closed: add the missing "');
      throw new Bad(`unexpected character \`${text[i]}\``);
    }
    const kind = KINDS[m.slice(1).findIndex((g) => g !== undefined)];
    if (kind !== "ws" && kind !== "comment") out.push([kind, m[0]]);
    i = TOKEN.lastIndex;
  }
  return out;
}

const opOf = (t: Tok) => (t[0] === "op" ? t[1] : "");

function matching(toks: Tok[], i: number): number {
  const stack: string[] = [];
  for (let j = i; j < toks.length; j++) {
    const v = opOf(toks[j]);
    if (v in CLOSE) stack.push(CLOSE[v]);
    else if (CLOSERS.has(v)) {
      if (!stack.length || stack.pop() !== v) throw new Bad(`a bracket does not match: unexpected ${v}`);
      if (!stack.length) return j;
    }
  }
  throw new Bad(`a bracket is not closed: add the missing ${stack.length ? stack[stack.length - 1] : ")"}`);
}

function parts(toks: Tok[]): Tok[][] {
  const out: Tok[][] = [];
  let cur: Tok[] = [];
  let depth = 0;
  for (const t of toks) {
    const v = opOf(t);
    if (v in CLOSE) depth++;
    else if (CLOSERS.has(v)) depth--;
    if (v === "," && depth === 0) { out.push(cur); cur = []; } else cur.push(t);
  }
  out.push(cur);
  return out;
}

function refs(toks: Tok[], kind: string): string[] {
  const out: string[] = [];
  for (let j = 0; j < toks.length - 4; j++) {
    const four = toks.slice(j, j + 4).map((t) => t[1]).join(" ");
    if (four === `AgentCore :: ${kind} ::` && toks[j + 4][0] === "str") out.push(toks[j + 4][1].slice(1, -1));
  }
  return out;
}

interface Parsed {
  effect: string; actions: string[]; gateways: string[]; conditioned: boolean; equals: boolean; readsInput: boolean;
  /** An attribute read from `context` other than `input` ("" when none). */
  badContext: string;
}

export function parse(statement: unknown): Parsed {
  if (typeof statement !== "string" || !statement.trim()) throw new Bad("is empty: write one permit(...) or forbid(...) statement");
  if (statement.length > MAX_LEN) throw new Bad(`is longer than ${MAX_LEN} characters`);
  const toks = tokens(statement);
  let i = 0;
  while (i < toks.length && toks[i][1] === "@") {
    if (i + 2 >= toks.length || toks[i + 1][0] !== "ident" || toks[i + 2][1] !== "(") throw new Bad('an annotation is @name("value")');
    i = matching(toks, i + 2) + 1;
  }
  if (i >= toks.length || !["permit", "forbid"].includes(toks[i][1])) throw new Bad("must start with permit( or forbid(");
  const effect = toks[i][1];
  if (i + 1 >= toks.length || toks[i + 1][1] !== "(") throw new Bad(`${effect} must be followed by (principal, action, resource)`);
  const end = matching(toks, i + 1);
  const head = parts(toks.slice(i + 2, end));
  if (head.length !== 3 || head.map((p) => (p.length ? p[0][1] : "")).join(",") !== "principal,action,resource") {
    throw new Bad(`the head must be ${effect}(principal…, action…, resource…), in that order`);
  }
  let j = end + 1;
  let conditioned = false;
  for (;;) {
    if (j >= toks.length) throw new Bad("must end with ;");
    const v = toks[j][1];
    if (v === ";") {
      if (j !== toks.length - 1) throw new Bad("holds more than one statement: give each its own policy");
      break;
    }
    if ((v === "when" || v === "unless") && j + 1 < toks.length && toks[j + 1][1] === "{") {
      const close = matching(toks, j + 1);
      if (close === j + 2) throw new Bad(`\`${v} { }\` is empty: give it a condition`);
      conditioned = true;
      j = close + 1;
      continue;
    }
    throw new Bad(`after the head only \`when { … }\` or \`unless { … }\` may follow, not \`${v}\``);
  }
  const action = head[1];
  let readsInput = false;
  let badContext = "";
  for (let k = end + 1; k < toks.length - 2; k++) {
    if (toks[k][1] === "context" && toks[k + 1][1] === "." && toks[k + 2][1] === "input") readsInput = true;
    else if (toks[k][1] === "context" && toks[k + 1][1] === "." && !badContext) badContext = toks[k + 2][1];
  }
  return { effect, actions: refs(action, "Action"), gateways: refs(head[2], "Gateway"), conditioned,
    equals: action.some((t) => t[1] === "=="), readsInput, badContext };
}

/** What is wrong with one statement, [] when nothing. toolKeys: the build's tool keys to
 *  check the actions against; undefined skips that (a library policy not yet attached). */
export function problems(statement: unknown, toolKeys?: string[]): string[] {
  let p: Parsed;
  try {
    p = parse(statement);
  } catch (e) {
    if (e instanceof Bad) return [e.message];
    throw e;
  }
  const out: string[] = [];
  if (p.badContext) {
    // Mirrors bff/cedar.py: context.arguments matches nothing, context.query fails the deploy.
    out.push(`AgentCore passes a tool call's arguments as context.input, not `
      + `context.${p.badContext}: write context.input.<argument>, with action == the one tool`);
  }
  if (!p.actions.length) {
    out.push('must name the tool it governs: action == AgentCore::Action::"<key>___<tool>", '
      + 'or action in AgentCore::Action::"<key>" for all of its tools');
  }
  for (const a of p.actions) {
    const key = a.split(SEP)[0];
    if (key === TOOL) {
      // A template: whichever tool it is attached to (tools.<key>.policies). The rendered
      // statement is checked against that tool when it is attached.
      if (p.equals && !a.includes(SEP)) {
        out.push(`action == needs one tool, "${TOOL}${SEP}<toolName>"; for all of its tools write `
          + `action in AgentCore::Action::"${TOOL}"`);
      } else if (p.readsInput && !a.includes(SEP)) {
        out.push(`a condition on context.input needs one tool, action == AgentCore::Action::`
          + `"${TOOL}${SEP}<toolName>": all of a target's tools together have no input to read`);
      }
      continue;
    }
    if (toolKeys !== undefined && !toolKeys.includes(key)) {
      const listed = [...toolKeys].sort().join(", ") || "none yet";
      out.push(`"${a}" is not a tool of this build (its tools: ${listed})`);
    } else if (p.equals && !a.includes(SEP)) {
      out.push(`action == needs one tool, "${a}${SEP}<toolName>"; for all of its tools write `
        + `action in AgentCore::Action::"${a}"`);
    } else if (p.readsInput && !a.includes(SEP)) {
      out.push(`a condition on context.input needs one tool, action == AgentCore::Action::`
        + `"${a}${SEP}<toolName>": all of "${a}"'s tools together have no input to read`);
    }
  }
  if (!(p.gateways.length === 1 && p.gateways[0] === GATEWAY)) {
    out.push(`the resource must be resource == AgentCore::Gateway::"${GATEWAY}": the `
      + "build's Gateway ARN is filled in when it deploys");
  }
  if (p.effect === "permit" && !p.conditioned && p.actions.some((a) => a.includes(SEP))) {
    out.push("a permit for one tool needs a `when` condition, e.g. when { context.input has "
      + "query } — AgentCore rejects an unconditioned one as overly permissive");
  }
  return out;
}

/** Written for whichever tool it is attached to: names AgentCore::Action::"{{tool}}". */
export function isTemplate(statement: unknown): boolean {
  try {
    return parse(statement).actions.some((a) => a.split(SEP)[0] === TOOL);
  } catch {
    return false;
  }
}

/** A policy template as attached to one tool. */
export const forTool = (statement: string, key: string) => statement.split(TOOL).join(key);

/** Whether a (rendered) statement names this tool. */
export function governs(statement: unknown, key: string): boolean {
  try {
    return parse(statement).actions.some((a) => a.split(SEP)[0] === key);
  } catch {
    return false;
  }
}

/** {effect, actions} for a list view; null when it does not parse. */
export function summary(statement: unknown): { effect: string; actions: string[] } | null {
  try {
    const p = parse(statement);
    return { effect: p.effect, actions: p.actions };
  } catch {
    return null;
  }
}

// --- the guided form ----------------------------------------------------------------

export type Operator = "in" | "notIn" | "lt" | "gt" | "present";
export interface Guided {
  effect: "permit" | "forbid";
  tool: string;
  /** One tool of the target, e.g. "issueRefund"; "" for all of its tools. */
  toolName: string;
  arg: string;
  op: Operator;
  /** Comma-separated, for in / notIn; one number for lt / gt. */
  values: string;
}

const quote = (s: string) => JSON.stringify(s);

/** The Cedar a guided form stands for. Arguments are checked by the caller. */
export function fromGuided(g: Guided): string {
  const action = g.toolName
    ? `action == AgentCore::Action::${quote(`${g.tool}${SEP}${g.toolName}`)}`
    : `action in AgentCore::Action::${quote(g.tool)}`;
  const head = `${g.effect}(\n  principal,\n  ${action},\n  resource == AgentCore::Gateway::"${GATEWAY}"\n)`;
  if (!g.arg) return `${head};`;
  const v = `context.input.${g.arg}`;
  const has = `context.input has ${g.arg}`;
  const list = g.values.split(",").map((s) => s.trim()).filter(Boolean);
  const cond = g.op === "present" ? has
    : g.op === "in" ? `${has} && ${JSON.stringify(list)}.contains(${v})`
      : g.op === "notIn" ? `${has} && !${JSON.stringify(list)}.contains(${v})`
        : `${has} && ${v} ${g.op === "lt" ? "<" : ">"} ${Number(g.values.trim()) || 0}`;
  return `${head} when {\n  ${cond}\n};`;
}

export const ARG_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;
