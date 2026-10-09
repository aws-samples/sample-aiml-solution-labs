/** Gateway interceptors written in the build (orchestrator.interceptors.<point>.code).
 *
 *  The code is generated from the templates checked on the Interceptors tab. Each
 *  template is a marked section of interceptor-templates/request.py or response.py
 *  (`# >>> name` ... `# <<< name`): generating keeps the checked sections, drops the
 *  rest, and writes their settings into SETTINGS. The result is ordinary code the user
 *  may edit; it lives in toolCode["interceptor-<point>"] like a code tool's files, and
 *  the deploy builds it the same way (app/tools/_code/interceptor-<point>/). */
import requestPy from "./interceptor-templates/request.py?raw";
import responsePy from "./interceptor-templates/response.py?raw";
import type { Entry, Json, ToolFiles, Workflow } from "./model";

export type Point = "request" | "response";

export interface TemplateDef {
  id: string;
  label: string;
  description: string;
  /** It reads the x-ax-* headers, so it needs passRequestHeaders. */
  headers?: boolean;
  /** What a new check of it starts with. */
  defaults: Record<string, Json>;
}

export const TEMPLATES: Record<Point, TemplateDef[]> = {
  request: [
    { id: "audit", label: "Audit log", defaults: {},
      description: "One JSON line per request in CloudWatch Logs: the method, tool, session, agent, user and decision. Argument names only, never values." },
    { id: "blockTools", label: "Block tools", defaults: { tools: [] },
      description: "Refuse calls to the tools listed, for every agent or only the agents listed. The agent is told it was refused." },
    { id: "argumentGuard", label: "Argument guard", defaults: { denyPatterns: [], maxArgumentChars: 20000 },
      description: "Refuse a tool call whose arguments are over a size or match a pattern (a regular expression), such as an SQL DROP." },
    { id: "injectContext", label: "Inject run context", headers: true, defaults: { arguments: {} },
      description: "Set tool arguments from the run (its session, agent or user), overwriting what the model sent, so an agent cannot claim to be another." },
    { id: "custom", label: "Your own check", defaults: {},
      description: "A function to fill in: return a reason to refuse the call, or None to let it through." },
  ],
  response: [
    { id: "audit", label: "Audit log", defaults: {},
      description: "One JSON line per answer in CloudWatch Logs: the method, tool, whether it failed and how much came back. Never the content." },
    { id: "redactPii", label: "Redact personal data", defaults: { types: ["email", "phone", "ssn", "card"], mask: "[REDACTED]" },
      description: "Mask email addresses, phone numbers, US social security numbers and card numbers in tool results before an agent reads them." },
    { id: "hideTools", label: "Hide tools", defaults: { tools: [] },
      description: "Leave the tools listed out of tools/list, so no agent is offered them. Block them too: a hidden tool can still be called by name." },
    { id: "capResult", label: "Cap result size", defaults: { maxChars: 20000 },
      description: "Cut each text in a tool's result to at most this many characters, and say how much was cut." },
    { id: "custom", label: "Your own change", defaults: {},
      description: "A function to fill in: change the answer and return it." },
  ],
};

const SOURCES: Record<Point, string> = { request: requestPy, response: responsePy };
const OPEN = /^\s*# >>> (\w+)\s*$/;
const CLOSE = /^\s*# <<< (\w+)\s*$/;

/** A template source with only the `keep` sections, and SETTINGS set to `settings`. */
export function render(source: string, keep: string[], settings: Record<string, unknown>): string {
  const out: string[] = [];
  const open: string[] = [];
  for (const line of source.split("\n")) {
    const o = OPEN.exec(line);
    if (o) {
      open.push(o[1]);
      if (o[1] === "settings") {
        out.push(`SETTINGS = json.loads(r"""\n${JSON.stringify(settings, null, 4)}\n""")`);
      }
      continue;
    }
    const c = CLOSE.exec(line);
    if (c) {
      open.pop();
      continue;
    }
    if (open.includes("settings")) continue;
    if (open.every((s) => keep.includes(s))) out.push(line);
  }
  return `${out.join("\n").replace(/\n{4,}/g, "\n\n\n").replace(/\n+$/, "")}\n`;
}

/** The first tool the build publishes, as "<key>___<name>", for sample events. */
function sampleTool(wf: Workflow, prefer?: unknown): string {
  if (Array.isArray(prefer) && typeof prefer[0] === "string" && prefer[0]) {
    const p = prefer[0] as string;
    return p.includes("___") ? p : `${p}___${firstName(wf.tools?.[p]) ?? "search"}`;
  }
  const [key, tool] = Object.entries(wf.tools ?? {})[0] ?? ["mytool", {} as Entry];
  return `${key}___${firstName(tool) ?? "search"}`;
}
function firstName(tool: Entry | undefined): string | undefined {
  const schema = tool && Array.isArray(tool.toolSchema) ? tool.toolSchema : [];
  const first = schema[0] as Record<string, Json> | undefined;
  return first && typeof first.name === "string" ? first.name : undefined;
}

/** Test events for the sandbox: what the Gateway sends at this point. */
export function sampleEvents(point: Point, wf: Workflow, templates: Record<string, Record<string, Json>>): Json[] {
  const agent = Object.keys(wf.agents ?? {})[0] ?? "intake";
  const headers = { "x-ax-session": "sample-session", "x-ax-agent": agent, "x-ax-user": "user@example.com" };
  const tool = sampleTool(wf, templates.blockTools?.tools ?? templates.hideTools?.tools);
  const request = (body: Json) => ({ path: "/mcp", httpMethod: "POST", headers, body });
  const listBody = { jsonrpc: "2.0", id: 1, method: "tools/list" };
  const callBody = { jsonrpc: "2.0", id: 2, method: "tools/call", params: { name: tool, arguments: { query: "example" } } };
  if (point === "request") {
    return [
      { name: "tools/list", event: { interceptorInputVersion: "1.0", mcp: { gatewayRequest: request(listBody) } } },
      { name: `a call to ${tool}`, event: { interceptorInputVersion: "1.0", mcp: { gatewayRequest: request(callBody) } } },
    ];
  }
  return [
    { name: "tools/list answer", event: { interceptorInputVersion: "1.0", mcp: {
      gatewayRequest: request(listBody),
      gatewayResponse: { statusCode: 200, body: { jsonrpc: "2.0", id: 1, result: { tools: [
        { name: tool, description: "A tool", inputSchema: { type: "object" } },
        { name: "other___tool", description: "Another", inputSchema: { type: "object" } }] } } } } } },
    { name: `${tool} answer`, event: { interceptorInputVersion: "1.0", mcp: {
      gatewayRequest: request(callBody),
      gatewayResponse: { statusCode: 200, body: { jsonrpc: "2.0", id: 2, result: { content: [
        { type: "text", text: "Contact jane.doe@example.com or 555-123-4567 about card 4111 1111 1111 1111." }] } } } } } },
  ];
}

/** A JSON value with every object's keys sorted. */
function canonical(v: Json): Json {
  if (Array.isArray(v)) return v.map(canonical);
  if (v && typeof v === "object") {
    return Object.fromEntries(Object.keys(v).sort().map((k) => [k, canonical((v as Record<string, Json>)[k])]));
  }
  return v;
}

/** The files of an interceptor generated from its checked templates. */
export function interceptorFiles(point: Point, templates: Record<string, Record<string, Json>>, wf: Workflow): ToolFiles {
  const keep = TEMPLATES[point].map((t) => t.id).filter((id) => id in templates);
  // One order whatever order the keys arrive in (a library item read back from the store
  // does not keep it): the same templates always generate the same code, so code that was
  // generated is never mistaken for code edited by hand.
  const settings = Object.fromEntries(keep.map((id) => [id, canonical(templates[id])]));
  return {
    "handler.py": render(SOURCES[point], keep, settings),
    "requirements.txt": "",
    "events.json": `${JSON.stringify(sampleEvents(point, wf, templates), null, 2)}\n`,
  };
}
