/** The tools AWS runs for an agent, offered by name in "Add a tool": Code Interpreter
 *  (`source: "codeInterpreter"`) and Browser (`source: "browser"`). Each is a type="lambda"
 *  tool whose function the framework ships (app/tools/<source>/handler.py) and deploys with a
 *  role written for it, so the only thing to choose is what agents may do with it: no ARN,
 *  no code and no tool schema to write. This file holds the presets, the fields the form
 *  hides for them, and the Browser's checks, which mirror bff/browser_tool.py word for word
 *  (both run tests/fixtures/validation_cases.json). */
import { addTool, type Entry, type Project } from "./model";
import type { Issue, Where } from "./validate";
import type { FieldOverride } from "./EntryForm";
type Add = (severity: Issue["severity"], where: Where, path: string, message: string) => void;

export const BROWSER_TOOL = "open_page";
export const MAX_DOMAINS = 50;
/** The same expression as app/tools/browser/handler.py DOMAIN_RE and bff/browser_tool.py. */
export const DOMAIN_RE = /^(?=.{1,253}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+(?:[a-z]{2,63}|xn--[a-z0-9-]{1,59})$/;

/** What each preset writes. The toolSchema is what the shipped function reads (its docs
 *  section in docs/WORKFLOW_REFERENCE.md); the Gateway publishes it as written. */
const PRESETS: Record<string, { label: string; detail: string; entry: Entry }> = {
  codeInterpreter: {
    label: "Code Interpreter",
    detail: "Runs a short program in an AWS sandbox and returns what it printed",
    entry: {
      type: "lambda", source: "codeInterpreter", call: "execute_code", arg: "code",
      toolSchema: [{
        name: "execute_code", description: "Run code in a sandbox. Returns stdout, stderr and the exit status.",
        properties: {
          language: { type: "string", required: true, description: "python, javascript or typescript." },
          code: { type: "string", required: true, description: "The program to run." },
        },
      }],
    },
  },
  browser: {
    label: "Browser",
    detail: "Opens a web page in an AWS browser and returns its text and links",
    entry: {
      type: "lambda", source: "browser", call: BROWSER_TOOL, arg: "url",
      toolSchema: [{
        name: BROWSER_TOOL, description: "Open a web page. Returns its title, visible text and links.",
        properties: { url: { type: "string", required: true, description: "The page to open, https:// or http://." } },
      }],
    },
  },
};

/** The choices "Add a tool" lists after the tool types, by the value it passes back. */
export const BUILTIN_CHOICES = Object.entries(PRESETS).map(([value, p]) => ({ value, label: p.label, description: p.detail }));
export const isBuiltinChoice = (value: string): boolean => value in PRESETS;

/** A new Code Interpreter or Browser tool named `name`. */
export function addBuiltinTool(project: Project, name: string, choice: string): { project: Project; key: string } {
  const r = addTool(project, name, "lambda");
  const preset = PRESETS[choice];
  if (!preset) return r;
  const tools = { ...r.project.workflow.tools, [r.key]: { ...structuredClone(preset.entry), description: name } as Entry };
  return { project: { ...r.project, workflow: { ...r.project.workflow, tools } }, key: r.key };
}

/** The label a tool is listed under: "Code Interpreter" or "Browser" for those, else undefined. */
export function builtinLabel(tool: Entry): string | undefined {
  return tool.type === "lambda" && typeof tool.source === "string" ? PRESETS[tool.source]?.label : undefined;
}

/** The fields the tool form hides. For Code Interpreter and Browser that is everything
 *  about the function, which the framework owns; what stays is the description, approval
 *  and policies (and, for the Browser, allowDomains). allowDomains is hidden on every other
 *  tool, since nothing else reads it. */
export function builtinOverrides(tool: Entry): Record<string, FieldOverride> {
  const hide: Record<string, FieldOverride> = tool.source === "browser" ? {} : { allowDomains: { hidden: true } };
  if (!builtinLabel(tool)) return hide;
  for (const k of ["lambdaArn", "source", "code", "toolSchema", "call", "arg", "args", "rowPath", "rowFields"]) hide[k] = { hidden: true };
  return hide;
}

export function checkBrowserTool(tool: Record<string, unknown>, path: string, where: Where, add: Add): void {
  const isBrowser = tool.source === "browser";
  const domains = tool.allowDomains;
  if ("allowDomains" in tool && !isBrowser) add("error", where, `${path}.allowDomains`, 'only the Browser tool (source "browser") reads allowDomains');
  if (!isBrowser) return;
  if (Array.isArray(domains)) {
    if (domains.length > MAX_DOMAINS) add("error", where, `${path}.allowDomains`, `at most ${MAX_DOMAINS} domains`);
    domains.forEach((d, i) => {
      if (typeof d === "string" && !DOMAIN_RE.test(d)) {
        add("error", where, `${path}.allowDomains[${i}]`, `"${d}" is not a domain name: write it like docs.aws.amazon.com, with no https://, path, port or *`);
      }
    });
  }
  const schema = Array.isArray(tool.toolSchema) ? tool.toolSchema : [];
  schema.forEach((t, i) => {
    const name = typeof t === "object" && t !== null && !Array.isArray(t) ? (t as Record<string, unknown>).name : undefined;
    if (typeof name === "string" && name && name !== BROWSER_TOOL) {
      add("error", where, `${path}.toolSchema[${i}].name`, `the Browser function publishes one tool, "${BROWSER_TOOL}"`);
    }
  });
}
