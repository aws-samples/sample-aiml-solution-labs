/** The code source field on a code tool and a Run code step: written, uploaded (files or a
 *  zip, kept in the build), a git repository at a pinned ref with its token as a build secret,
 *  or S3. Every input has a label a screen reader announces. */
import { act, fireEvent, render, screen } from "@testing-library/react";
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { useState } from "react";
import { describe, expect, it, vi } from "vitest";

vi.mock("../api", () => ({ api: {
  get: async () => ({ toolApiKeys: [], a2aTokens: [], identitySecrets: [], gitTokens: [] }),
  put: async () => ({}), post: async () => ({ ok: true }), del: async () => ({}),
} }));

import { CodeTool } from "./CodeTool";
import { TaskPanel } from "./TaskPanel";
import { toBundle, type Project } from "./model";
import { validate } from "./validate";

const base = (): Project => ({ id: "p1", name: "P", createdAt: "", updatedAt: "", prompts: {},
  workflow: { agents: {}, steps: [], tools: { lookup: { type: "lambda", description: "Lookups.", code: {},
    toolSchema: [{ name: "lookup", properties: { query: { type: "string", required: true } } }] } } } as never,
  toolCode: { lookup: { "handler.py": "def lambda_handler(e, c):\n    return 1\n" } } });
let latest: Project;
function Tool() {
  const [p, setP] = useState(base());
  latest = p;
  return <CodeTool project={p} id="lookup" onChange={setP} server />;
}
const segment = (name: string) => {
  const found = createWrapper(document.body).findAllSegmentedControls().flatMap((c) => c.findSegments()).find((s) => s.getElement().textContent === name);
  if (!found) throw new Error(`no segment ${name}`);
  return found.getElement();
};

describe("CodeSourceField on a code tool", () => {
  it("takes a git repository pinned to a ref, and the bundle carries it as a reference", async () => {
    render(<Tool />);
    await act(async () => { fireEvent.click(segment("Git repository")); });
    fireEvent.change(screen.getByLabelText("Repository URL"), { target: { value: "https://github.com/example-org/tools.git" } });
    fireEvent.change(screen.getByLabelText("Branch, tag or commit"), { target: { value: "v1.2.0" } });
    fireEvent.change(screen.getByLabelText("Access token name"), { target: { value: "tools-repo" } });
    const code = (latest.workflow.tools.lookup as Record<string, unknown>).code;
    expect(code).toEqual({ codeSource: { git: { url: "https://github.com/example-org/tools.git", ref: "v1.2.0", token: "tools-repo" } } });
    expect(validate(latest.workflow).filter((i) => i.path.startsWith("tools.lookup"))).toEqual([]);
    expect(toBundle(latest).toolCode).toBeUndefined();
    expect(screen.getByLabelText("Access token for tools-repo")).toBeTruthy();
    expect(screen.queryByText("Check and run in the sandbox")).toBeNull();
  });
  it("shows what is wrong with an S3 location", async () => {
    render(<Tool />);
    await act(async () => { fireEvent.click(segment("Amazon S3")); });
    fireEvent.change(screen.getByLabelText("S3 location"), { target: { value: "s3://amzn-s3-demo-bucket/tools/lookup.py" } });
    expect(screen.getByText("must be s3://<bucket>/<prefix>/ (ending in /) or s3://<bucket>/<key>.zip")).toBeTruthy();
  });
  it("keeps uploaded files in the build like written ones", async () => {
    render(<Tool />);
    await act(async () => { fireEvent.click(segment("Upload")); });
    const file = new File(["def lambda_handler(e, c):\n    return 'up'\n"], "handler.py", { type: "text/x-python" });
    Object.defineProperty(file, "arrayBuffer", { value: async () => new TextEncoder().encode("def lambda_handler(e, c):\n    return 'up'\n").buffer });
    await act(async () => { fireEvent.change(screen.getByLabelText("Code files or zip"), { target: { files: [file] } }); });
    expect(latest.toolCode?.lookup["handler.py"]).toContain("'up'");
    expect(toBundle(latest).toolCode?.lookup["handler.py"]).toContain("'up'");
    expect(screen.getByText("Uploaded handler.py.")).toBeTruthy();
  });
});

describe("CodeSourceField on a Run code step", () => {
  it("replaces the written script with a fetched one", async () => {
    const start: Project = { ...base(), workflow: { agents: { job: { name: "Job", runtime: "task", task: { kind: "code", code: "print(1)" } } },
      steps: [{ agent: "job" }], tools: {} } as never };
    let p = start;
    const Panel = () => {
      const [s, setS] = useState(start);
      p = s;
      return <TaskPanel project={s} id="job" issues={[]} onChange={setS} onSelect={() => {}} />;
    };
    render(<Panel />);
    await act(async () => { fireEvent.click(segment("Amazon S3")); });
    fireEvent.change(screen.getByLabelText("S3 location"), { target: { value: "s3://amzn-s3-demo-bucket/scripts/" } });
    fireEvent.change(screen.getByLabelText("Path of the script"), { target: { value: "run.py" } });
    expect((p.workflow.agents.job as Record<string, unknown>).task).toEqual({ kind: "code",
      codeSource: { s3: { uri: "s3://amzn-s3-demo-bucket/scripts/", path: "run.py" } } });
    expect(validate(p.workflow).filter((i) => i.path.startsWith("agents.job"))).toEqual([]);
    expect(screen.queryByLabelText("Script")).toBeNull();
  });
});
