/** Track B docgen. The Document generator (`source: "docgen"`): a type="lambda" tool whose
 *  function the framework ships (app/tools/docgen/) and deploys with a role that may read
 *  only its templates and write only its outputs. This file holds its preset for "Add a
 *  tool", a sample test event, and its checks, which mirror bff/docgen_tool.py word for
 *  word (both run tests/fixtures/validation_cases.json). */
import type { Entry } from "./model";
import type { Issue, Where } from "./validate";
type Add = (severity: Issue["severity"], where: Where, path: string, message: string) => void;
export const DOCGEN_SOURCE = "docgen";
export const DOCGEN_TOOL = "generate_document";
export const DOCGEN_KEYS = ["formats", "templateS3Uri", "outputPrefix"] as const;
/** Arguments the runtime sets on every call (app/features/gateway/client.py), which the
 *  published schema must declare for the Gateway to pass them on. */
export const DOCGEN_RUN_ARGS = ["runId", "agent"] as const;
/** The build's own storage (Track E, orchestrator.storage), which the deploy resolves. */
const WHERE = String.raw`(?:s3://[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]|\$\{storage\})/`;
/** One template (a .pptx or .docx object), or a folder of them (ending in /). */
export const TEMPLATE_RE = new RegExp(String.raw`^${WHERE}(?:(?:[^*?]+/)?|[^*?]*[^/*?]\.(pptx|docx))$`);
export const OUTPUT_RE = new RegExp(String.raw`^${WHERE}(?:[^*?]+/)?$`);
/** The same as FRAMEWORK_BUCKET_RE in validate.ts: buckets no tool may reach. */
const FRAMEWORK_BUCKET_RE = /^s3:\/\/(agentcore-|[^/]*builderbuildsbucket)/;
/** What "Add a tool" -> Document generator writes. The toolSchema is what the shipped
 *  function reads (docs/WORKFLOW_REFERENCE.md "Document generator"); runId and agent are set
 *  by the runtime on every call. */
export const DOCGEN_PRESET = {
  label: "Document generator",
  detail: "Writes a PDF, PPTX or DOCX from data, into a template from S3 or a simple layout",
  entry: {
    type: "lambda", source: DOCGEN_SOURCE, call: DOCGEN_TOOL, arg: "data", formats: ["pdf", "pptx", "docx"],
    toolSchema: [{
      name: DOCGEN_TOOL,
      description: "Write a PDF, PPTX or DOCX from data. Returns its s3:// URI and a download link.",
      properties: {
        format: { type: "string", required: true, description: "pdf, pptx or docx: one of this tool's formats." },
        data: { type: "object", required: true, description: "The content as JSON. A template's {{name}} reads data.name; a list repeats the rows, paragraphs or slides using it." },
        template: { type: "string", required: false, description: "With a template folder: the template's file name in it, e.g. report.docx. Leave out for the built-in layout." },
        fileName: { type: "string", required: false, description: "The file's name. Defaults to the data's title." },
        runId: { type: "string", required: false, description: "Set by the workflow on every call; leave it out." },
        agent: { type: "string", required: false, description: "Set by the workflow on every call; leave it out." },
      },
    }],
  } as Entry,
};
/** The test run's event: sample data in the built-in layout, for a run id of its own
 *  (its file goes to runs/console-test/outputs/, expired with the run files). */
export function sampleEvent(tool: Entry): Record<string, unknown> {
  const formats = Array.isArray(tool.formats) ? tool.formats.map(String) : [];
  return {
    format: formats[0] ?? "pdf",
    data: {
      title: "Sample report",
      subtitle: "A test run with sample data",
      summary: { status: "ok", items: 2 },
      rows: [{ name: "First", value: 1 }, { name: "Second", value: 2 }],
    },
    fileName: "sample",
    runId: "console-test",
  };
}
export function checkDocgenTool(tool: Record<string, unknown>, path: string, where: Where, add: Add, filesOn: boolean): void {
  const isDocgen = tool.source === DOCGEN_SOURCE;
  for (const k of DOCGEN_KEYS) {
    if (k in tool && !isDocgen) add("error", where, `${path}.${k}`, `only the Document generator (source "docgen") reads ${k}`);
  }
  if (!isDocgen) return;
  const formats = tool.formats;
  if (!("formats" in tool)) {
    add("error", where, `${path}.formats`, "the Document generator needs `formats`: any of pdf, pptx, docx");
  } else if (Array.isArray(formats)) {
    if (!formats.length) add("error", where, `${path}.formats`, "list at least one format");
    const seen = new Set<string>();
    formats.forEach((f, i) => {
      if (typeof f !== "string") return;
      if (seen.has(f)) add("error", where, `${path}.formats[${i}]`, `"${f}" is listed twice`);
      seen.add(f);
    });
  }
  const template = tool.templateS3Uri;
  if (typeof template === "string") {
    const m = TEMPLATE_RE.exec(template);
    if (!m) {
      add("error", where, `${path}.templateS3Uri`,
        "must be one template, s3://<bucket>/<key>.pptx or .docx, or a folder of them, "
        + "s3://<bucket>/<prefix>/ (or ${storage}/...)");
    } else if (FRAMEWORK_BUCKET_RE.test(template)) {
      add("error", where, `${path}.templateS3Uri`, "is one of the framework's own buckets, which no tool may reach");
    } else if (m[1] && Array.isArray(formats) && !formats.includes(m[1])) {
      add("error", where, `${path}.templateS3Uri`, `is a .${m[1]} template, so \`formats\` must include "${m[1]}"`);
    }
  }
  const output = tool.outputPrefix;
  if (typeof output === "string") {
    if (!OUTPUT_RE.test(output)) {
      add("error", where, `${path}.outputPrefix`, "must be s3://<bucket>/ or s3://<bucket>/<prefix>/, ending in / (or ${storage}/.../)");
    } else if (FRAMEWORK_BUCKET_RE.test(output)) {
      add("error", where, `${path}.outputPrefix`, "is one of the framework's own buckets, which no tool may reach");
    }
  }
  if (!filesOn) {
    add("error", where, `${path}.source`,
      "the Document generator saves each file with its run: set orchestrator.files (even {}) "
      + "so this deployment keeps run files");
  }
  const schema = Array.isArray(tool.toolSchema) ? tool.toolSchema : [];
  schema.forEach((t, i) => {
    const entry = typeof t === "object" && t !== null && !Array.isArray(t) ? (t as Record<string, unknown>) : undefined;
    const name = entry?.name;
    if (typeof name === "string" && name && name !== DOCGEN_TOOL) {
      add("error", where, `${path}.toolSchema[${i}].name`, `the Document generator publishes one tool, "${DOCGEN_TOOL}"`);
    } else if (entry && name === DOCGEN_TOOL) {
      const p = entry.properties;
      const props = typeof p === "object" && p !== null && !Array.isArray(p) ? (p as Record<string, unknown>) : {};
      const missing = DOCGEN_RUN_ARGS.filter((k) => !(k in props));
      if (missing.length) {
        add("error", where, `${path}.toolSchema[${i}].properties`,
          `declare ${missing.join(" and ")} (optional strings): the Gateway passes the function only `
          + "the arguments its schema declares, and the workflow sets runId and agent on every call "
          + "so each file is saved with its run");
      }
    }
  });
}
