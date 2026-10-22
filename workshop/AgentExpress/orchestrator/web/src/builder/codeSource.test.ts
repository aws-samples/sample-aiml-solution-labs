/** Code sources: the shape (same words as bff/code_source.py; the parity cases check the
 *  validators), uploads unpacked in the page (files or a zip, stored or deflated), and git or
 *  S3 travelling as a reference only. */
import { describe, expect, it } from "vitest";
import {
  codeFilesOf, codeSourceKind, codeSourceProblems, gitTokenNames, readZip, scriptOf,
} from "./codeSource";
import { toBundle, writtenFunctions, type Project } from "./model";

const enc = (s: string) => new TextEncoder().encode(s);

/** A zip with each entry stored (method 0) or deflated (8), built the way zip tools do. */
async function zipOf(entries: Record<string, string>, deflate = false, mode = 0o100644): Promise<ArrayBuffer> {
  const locals: Uint8Array[] = [];
  const centrals: Uint8Array[] = [];
  let offset = 0;
  for (const [name, text] of Object.entries(entries)) {
    const raw = enc(text);
    const data = deflate
      ? new Uint8Array(await new Response(new ReadableStream<BufferSource>({ start(c) { c.enqueue(raw); c.close(); } })
        .pipeThrough(new CompressionStream("deflate-raw"))).arrayBuffer())
      : raw;
    const n = enc(name);
    const local = new Uint8Array(30 + n.length + data.length);
    const lv = new DataView(local.buffer);
    lv.setUint32(0, 0x04034b50, true); lv.setUint16(8, deflate ? 8 : 0, true);
    lv.setUint32(18, data.length, true); lv.setUint32(22, raw.length, true); lv.setUint16(26, n.length, true);
    local.set(n, 30); local.set(data, 30 + n.length);
    const central = new Uint8Array(46 + n.length);
    const cv = new DataView(central.buffer);
    cv.setUint32(0, 0x02014b50, true); cv.setUint16(10, deflate ? 8 : 0, true);
    cv.setUint32(20, data.length, true); cv.setUint32(24, raw.length, true); cv.setUint16(28, n.length, true);
    cv.setUint32(38, (mode << 16) >>> 0, true); cv.setUint32(42, offset, true);
    central.set(n, 46);
    locals.push(local); centrals.push(central);
    offset += local.length;
  }
  const size = centrals.reduce((s, c) => s + c.length, 0);
  const end = new Uint8Array(22);
  const ev = new DataView(end.buffer);
  ev.setUint32(0, 0x06054b50, true); ev.setUint16(8, centrals.length, true); ev.setUint16(10, centrals.length, true);
  ev.setUint32(12, size, true); ev.setUint32(16, offset, true);
  const parts = [...locals, ...centrals, end];
  const all = new Uint8Array(parts.reduce((n, b) => n + b.length, 0));
  let at = 0;
  for (const b of parts) { all.set(b, at); at += b.length; }
  return all.buffer;
}

describe("the shape", () => {
  it("takes git and S3, an image only where it is allowed, and names a bad field", () => {
    expect(codeSourceProblems({ git: { url: "https://github.com/example-org/t.git", ref: "v1", token: "repo" } }, "p")).toEqual([]);
    expect(codeSourceProblems({ s3: { uri: "s3://amzn-s3-demo-bucket/k.zip", path: "src" } }, "p")).toEqual([]);
    const image = { image: "111122223333.dkr.ecr.us-east-1.amazonaws.com/my-mcp:1.0" };
    expect(codeSourceProblems(image, "p")).toEqual([{ path: "p", message: "must have exactly one of: git or s3" }]);
    expect(codeSourceProblems(image, "p", ["git", "s3", "image"])).toEqual([]);
    expect(codeSourceProblems({ git: { url: "https://u:p@h/r", ref: "-x" } }, "p").map((i) => i.path)).toEqual(["p.git.url", "p.git.ref"]);
    expect(codeSourceKind({ s3: {} })).toBe("s3");
    expect(codeSourceKind({ git: {}, s3: {} })).toBe("");
  });
  it("lists the token secrets a build names", () => {
    const wf = { tools: { a: { type: "lambda", code: { codeSource: { git: { url: "https://h/r", ref: "x", token: "b-repo" } } } },
      m: { type: "mcp", hosted: { code: { git: { url: "https://h/r", ref: "x", token: "a-repo" } } } } },
    agents: { s: { task: { codeSource: { s3: { uri: "s3://amzn-s3-demo-bucket/s/" } } } } } };
    expect(gitTokenNames(wf)).toEqual(["a-repo", "b-repo"]);
  });
});

describe("uploads", () => {
  it("reads a stored and a deflated zip, dropping one shared top folder", async () => {
    for (const deflate of [false, true]) {
      const z = await readZip(await zipOf({ "tool/handler.py": "def lambda_handler(e, c):\n    return 1\n", "tool/requirements.txt": "", "tool/notes.md": "x" }, deflate));
      const got = codeFilesOf(z);
      expect(got.error).toBeUndefined();
      expect(Object.keys(got.files).sort()).toEqual(["handler.py", "requirements.txt"]);
      expect(got.skipped).toEqual(["tool/notes.md"]);
    }
  });
  it("refuses a zip that leaves its folder or holds a link, and code with no handler.py", async () => {
    await expect(readZip(await zipOf({ "../evil.py": "x" }))).rejects.toThrow("leaves the zip");
    await expect(readZip(await zipOf({ link: "/etc/passwd" }, false, 0o120777))).rejects.toThrow("symbolic link");
    expect(codeFilesOf({ "util.py": enc("X = 1") }).error).toBe("no handler.py among the files");
  });
  it("takes one script for a Run code step", async () => {
    expect(scriptOf({ "run.py": enc("print(1)") })).toEqual({ code: "print(1)" });
    expect(scriptOf({ "a.py": enc("1"), "b.py": enc("2") }).error).toBe("upload one .py script (found 2)");
    expect(scriptOf({ "run.js": enc("console.log(1)") }, "javascript")).toEqual({ code: "console.log(1)" });
    expect(scriptOf({ "big.py": new Uint8Array(50_001) }).error).toBe("the script is over 50000 bytes");
  });
});

describe("the bundle", () => {
  it("carries uploaded and written files, and git or S3 as a reference only", () => {
    const p = { id: "p", name: "P", createdAt: "", updatedAt: "", prompts: {},
      workflow: { agents: {}, steps: [], tools: {
        a: { type: "lambda", code: { codeSource: { git: { url: "https://h/r.git", ref: "v1" } } } },
        b: { type: "lambda", code: {} } } },
      toolCode: { a: { "handler.py": "kept in the draft" }, b: { "handler.py": "uploaded" } } } as unknown as Project;
    expect(writtenFunctions(p.workflow)).toEqual(["b"]);
    const bundle = toBundle(p);
    expect(bundle.toolCode).toEqual({ b: { "handler.py": "uploaded" } });
    expect((bundle.workflow.tools.a as Record<string, unknown>).code).toEqual({ codeSource: { git: { url: "https://h/r.git", ref: "v1" } } });
  });
});
