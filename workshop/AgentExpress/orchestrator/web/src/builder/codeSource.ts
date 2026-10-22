/** Where a function's code comes from: a code source. Mirrors bff/code_source.py word for word
 *  (both run tests/fixtures/validation_cases.json).
 *
 *  It sits at tools.<key>.code.codeSource (a Lambda tool written in the build),
 *  orchestrator.interceptors.<point>.code.codeSource, agents.<id>.task.codeSource (a Run code
 *  step) and tools.<key>.hosted.code (a hosted MCP server). Absent: the code is written or
 *  uploaded in the build. The deployer fetches git and S3 at deploy time; a git token is the
 *  NAME of a build secret of kind gitTokens, never the token.
 *
 *  S3 bucket: S3Location.bucket pattern,
 *  https://docs.aws.amazon.com/bedrock-agentcore-control/latest/APIReference/API_S3Location.html
 *  ECR image: ContainerConfiguration.containerUri pattern and 1..1024 length,
 *  https://docs.aws.amazon.com/bedrock-agentcore-control/latest/APIReference/API_ContainerConfiguration.html */

export type CodeSourceKind = "git" | "s3" | "image";
export type CodeSource =
  | { git: { url: string; ref: string; path?: string; token?: string } }
  | { s3: { uri: string; path?: string } }
  | { image: string };

export const CODE_SOURCE_VARIANTS: readonly CodeSourceKind[] = ["git", "s3", "image"];
const GIT_KEYS = ["url", "ref", "path", "token"];
const S3_KEYS = ["uri", "path"];

export const GIT_URL_RE = /^https:\/\/[A-Za-z0-9.-]+(:[0-9]{1,5})?(\/[A-Za-z0-9._~%+-]+)+\/?$/;
export const GIT_REF_RE = /^(?!-)(?!.*\.\.)[A-Za-z0-9._/+-]{1,255}$/;
export const REL_PATH_RE = /^(?!\/)(?!.*(^|\/)\.\.(\/|$))[A-Za-z0-9._ /+-]{1,1024}$/;
export const SECRET_NAME_RE = /^[A-Za-z0-9_.-]{1,128}$/;
export const S3_URI_RE = /^s3:\/\/([a-z0-9][a-z0-9.-]{1,61}[a-z0-9])\/(([^\s]*\/)?|[^\s]*\.zip)$/;
export const IMAGE_RE = /^(([0-9]{12})\.dkr\.ecr\.([a-z0-9-]+)\.amazonaws\.com(\.cn)?|public\.ecr\.aws)\/((?:[a-z0-9]+(?:[._-][a-z0-9]+)*\/)*[a-z0-9]+(?:[._-][a-z0-9]+)*)(?::([^:@]{1,300}))?(?:@(.+))?$/;
const IMAGE_MAX = 1024;

const isObj = (v: unknown): v is Record<string, unknown> => typeof v === "object" && v !== null && !Array.isArray(v);

/** "git" | "s3" | "image", or "" when absent or not a code source. */
export function codeSourceKind(source: unknown): CodeSourceKind | "" {
  if (!isObj(source)) return "";
  const keys = Object.keys(source);
  return keys.length === 1 && (CODE_SOURCE_VARIANTS as readonly string[]).includes(keys[0]) ? (keys[0] as CodeSourceKind) : "";
}

/** The problems of a code source at `path`; [] when it is fine. */
export function codeSourceProblems(source: unknown, path: string,
  allow: readonly CodeSourceKind[] = ["git", "s3"]): { path: string; message: string }[] {
  const out: { path: string; message: string }[] = [];
  const names = allow.join(" or ");
  if (!isObj(source) || Object.keys(source).length !== 1) return [{ path, message: `must have exactly one of: ${names}` }];
  const k = Object.keys(source)[0];
  if (!(allow as readonly string[]).includes(k)) return [{ path, message: `must have exactly one of: ${names}` }];
  const v = source[k];
  const p = `${path}.${k}`;
  if (k === "image") {
    if (typeof v !== "string" || !v || v.length > IMAGE_MAX || !IMAGE_RE.test(v)) {
      out.push({ path: p, message: "must be an Amazon ECR image URI, <account>.dkr.ecr.<region>.amazonaws.com/<repository>[:tag][@digest]" });
    }
    return out;
  }
  if (!isObj(v)) return [{ path: p, message: "must be an object" }];
  const keys = k === "git" ? GIT_KEYS : S3_KEYS;
  for (const extra of Object.keys(v)) {
    if (!keys.includes(extra)) out.push({ path: `${p}.${extra}`, message: `unknown key; ${k} takes: ${keys.join(", ")}` });
  }
  const str = (x: unknown, re: RegExp) => typeof x === "string" && re.test(x);
  if (k === "git") {
    if (!str(v.url, GIT_URL_RE)) out.push({ path: `${p}.url`, message: "must be an https:// repository URL with no user name, password, query or fragment" });
    if (!str(v.ref, GIT_REF_RE)) out.push({ path: `${p}.ref`, message: "must pin a branch, a tag or a commit (no spaces, no leading -, no ..)" });
    if ("token" in v && !str(v.token, SECRET_NAME_RE)) {
      out.push({ path: `${p}.token`, message: "must be the name of a build secret (letters, digits, _ . -), never the token itself" });
    }
  } else if (!str(v.uri, S3_URI_RE)) {
    out.push({ path: `${p}.uri`, message: "must be s3://<bucket>/<prefix>/ (ending in /) or s3://<bucket>/<key>.zip" });
  }
  if ("path" in v && !str(v.path, REL_PATH_RE)) {
    out.push({ path: `${p}.path`, message: "must be a relative path inside the source (no leading /, no ..)" });
  }
  return out;
}

/** Every git token secret the build names, sorted (hosted.code included). Mirrors
 *  bff/code_source.py secret_names. */
export function gitTokenNames(wf: { tools?: Record<string, unknown>; agents?: Record<string, unknown>; orchestrator?: unknown }): string[] {
  const sources: unknown[] = [];
  for (const t of Object.values(wf.tools ?? {})) {
    if (!isObj(t)) continue;
    if (isObj(t.code) && String(t.type ?? "").toLowerCase() === "lambda") sources.push(t.code.codeSource);
    if (isObj(t.hosted)) sources.push(t.hosted.code);
  }
  const orch = isObj(wf.orchestrator) ? wf.orchestrator : {};
  const ics = isObj(orch.interceptors) ? orch.interceptors : {};
  for (const p of ["request", "response"]) { const ic = ics[p]; if (isObj(ic) && isObj(ic.code)) sources.push(ic.code.codeSource); }
  for (const a of Object.values(wf.agents ?? {})) if (isObj(a) && isObj(a.task)) sources.push(a.task.codeSource);
  const names = new Set<string>();
  for (const src of sources) {
    if (codeSourceKind(src) === "git") {
      const g = (src as { git: unknown }).git;
      if (isObj(g) && typeof g.token === "string") names.add(g.token);
    }
  }
  return [...names].sort();
}

// --- uploads: files or a zip, unpacked here and kept in the build like written code ---------

/** The files a code function may hold, and how many and how big (scaffold.py CODE_FILE_RE,
 *  CODE_MAX_FILES, CODE_MAX_BYTES; bff/codecheck.py FILE_RE). */
export const CODE_FILE_RE = /^([a-z_][a-z0-9_]*\.py|requirements\.txt|events\.json)$/;
export const CODE_MAX_FILES = 20;
export const CODE_MAX_BYTES = 500_000;
/** A Run code step's script (app/common/task_agent.MAX_CODE_BYTES). */
export const SCRIPT_MAX_BYTES = 50_000;
export const SCRIPT_EXT: Record<string, string> = { python: ".py", javascript: ".js", typescript: ".ts" };

async function inflateRaw(data: Uint8Array): Promise<Uint8Array> {
  const source = new ReadableStream<BufferSource>({ start(c) { c.enqueue(data.slice()); c.close(); } });
  const stream = source.pipeThrough(new DecompressionStream("deflate-raw"));
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

/** The entries of a zip (stored or deflated), read from its central directory. A name
 *  that leaves the archive (absolute, `..`) or a symbolic link is refused. */
export async function readZip(buf: ArrayBuffer, maxBytes = CODE_MAX_BYTES * 4): Promise<Record<string, Uint8Array>> {
  const v = new DataView(buf);
  const u8 = new Uint8Array(buf);
  let eocd = -1;
  for (let i = buf.byteLength - 22; i >= Math.max(0, buf.byteLength - 65_557); i--) {
    if (v.getUint32(i, true) === 0x06054b50) { eocd = i; break; }
  }
  if (eocd < 0) throw new Error("not a zip file");
  const count = v.getUint16(eocd + 10, true);
  let at = v.getUint32(eocd + 16, true);
  const out: Record<string, Uint8Array> = {};
  let total = 0;
  for (let n = 0; n < count; n++) {
    if (v.getUint32(at, true) !== 0x02014b50) throw new Error("the zip's directory is damaged");
    const method = v.getUint16(at + 10, true);
    const csize = v.getUint32(at + 20, true);
    const usize = v.getUint32(at + 24, true);
    const nlen = v.getUint16(at + 28, true), xlen = v.getUint16(at + 30, true), clen = v.getUint16(at + 32, true);
    const attrs = v.getUint32(at + 38, true);
    const local = v.getUint32(at + 42, true);
    const name = new TextDecoder().decode(u8.subarray(at + 46, at + 46 + nlen)).replace(/\\/g, "/");
    at += 46 + nlen + xlen + clen;
    if (name.endsWith("/")) continue;
    if (name.startsWith("/") || name.split("/").includes("..")) throw new Error(`${name} leaves the zip`);
    if (((attrs >>> 16) & 0o170000) === 0o120000) throw new Error(`${name} is a symbolic link`);
    total += usize;
    if (total > maxBytes) throw new Error(`the zip is over ${Math.round(maxBytes / 1000)} KB unzipped`);
    const start = local + 30 + v.getUint16(local + 26, true) + v.getUint16(local + 28, true);
    const raw = u8.subarray(start, start + csize);
    if (method === 0) out[name] = raw.slice();
    else if (method === 8) out[name] = await inflateRaw(raw);
    else throw new Error(`${name} uses a compression this page cannot read`);
  }
  return out;
}

/** Uploaded files (or a zip's entries) as a code function's files: names kept to what a
 *  function may hold, one shared top folder dropped, and the limits applied. */
export function codeFilesOf(entries: Record<string, Uint8Array>): { files: Record<string, string>; skipped: string[]; error?: string } {
  const names = Object.keys(entries).filter((n) => !n.split("/").some((p) => p === "__pycache__" || p.startsWith(".")));
  const tops = new Set(names.map((n) => (n.includes("/") ? n.split("/")[0] : "")));
  const strip = tops.size === 1 && !tops.has("") ? `${[...tops][0]}/` : "";
  const files: Record<string, string> = {};
  const skipped: string[] = Object.keys(entries).filter((n) => !names.includes(n));
  for (const n of names) {
    const rel = n.slice(strip.length);
    if (rel.includes("/") || !CODE_FILE_RE.test(rel)) { skipped.push(n); continue; }
    try { files[rel] = new TextDecoder("utf-8", { fatal: true }).decode(entries[n]); } catch { skipped.push(n); }
  }
  const size = Object.values(files).reduce((s, b) => s + new TextEncoder().encode(b).length, 0);
  if (Object.keys(files).length > CODE_MAX_FILES) return { files, skipped, error: `at most ${CODE_MAX_FILES} files` };
  if (size > CODE_MAX_BYTES) return { files, skipped, error: `the files are over ${CODE_MAX_BYTES / 1000} KB` };
  if (!files["handler.py"]) return { files, skipped, error: "no handler.py among the files" };
  return { files, skipped };
}

/** An uploaded script (or a zip holding exactly one script of the step's language). */
export function scriptOf(entries: Record<string, Uint8Array>, language = "python"): { code?: string; error?: string } {
  const ext = SCRIPT_EXT[language] ?? ".py";
  const scripts = Object.keys(entries).filter((n) => n.endsWith(ext) && !n.split("/").some((p) => p.startsWith(".")));
  if (scripts.length !== 1) return { error: `upload one ${ext} script (found ${scripts.length})` };
  const data = entries[scripts[0]];
  if (data.length > SCRIPT_MAX_BYTES) return { error: `the script is over ${SCRIPT_MAX_BYTES} bytes` };
  try { return { code: new TextDecoder("utf-8", { fatal: true }).decode(data) }; } catch { return { error: "the script is not UTF-8 text" }; }
}

/** The entries of what the user picked: each file, or a zip's contents. */
export async function entriesOf(picked: File[]): Promise<Record<string, Uint8Array>> {
  const out: Record<string, Uint8Array> = {};
  for (const f of picked) {
    const buf = await f.arrayBuffer();
    if (f.name.toLowerCase().endsWith(".zip")) Object.assign(out, await readZip(buf));
    else out[f.name] = new Uint8Array(buf);
  }
  return out;
}

/** One line for the canvas and exports. Never holds a token (only its secret's name). */
export function describeCodeSource(source: unknown): string {
  const k = codeSourceKind(source);
  const s = source as Record<string, Record<string, string> | string>;
  if (k === "git") {
    const g = isObj(s.git) ? s.git : {};
    return `git ${g.url} @ ${g.ref}${g.path ? ` path ${g.path}` : ""}${g.token ? ` (token: secret ${g.token})` : ""}`;
  }
  if (k === "s3") {
    const v = isObj(s.s3) ? s.s3 : {};
    return `s3 ${v.uri}${v.path ? ` path ${v.path}` : ""}`;
  }
  if (k === "image") return `image ${s.image}`;
  return "written in the build";
}
