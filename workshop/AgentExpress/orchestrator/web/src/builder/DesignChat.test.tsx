/** AgentExpress Assistant, from the page's side: it talks to bff/designer.py, reloads the build
 *  when a change lands, and every change — the designer's, a drag, an upload — can be
 *  undone the same way. The model itself is tested in tests/test_designer.py. */
import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest";

const calls: { method: string; path: string; body?: unknown }[] = [];
let replies: Record<string, unknown[]> = {};
vi.mock("../api", () => {
  const answer = (method: string, path: string, body?: unknown) => {
    calls.push({ method, path, body });
    const q = replies[`${method} ${path}`] ?? [];
    const r = q.length > 1 ? q.shift() : q[0];
    return r instanceof Error ? Promise.reject(r) : Promise.resolve(r);
  };
  return {
    api: {
      get: (p: string) => answer("GET", p),
      post: (p: string, b?: unknown) => answer("POST", p, b),
      put: (p: string, b?: unknown) => answer("PUT", p, b),
      del: (p: string) => answer("DELETE", p),
    },
  };
});

import { Builder } from "./Builder";
import { defaultsUsed, openInputs, secretsNeeded, when } from "./DesignChat";
import { record, redo, start, undo } from "./history";
import { Markdown } from "./markdown";
import { addAgent, migrateFramework, newProject, replaceWorkflow, type Project } from "./model";
import type { BuildStore, BuildSummary, DesignChange, DesignDoc } from "./storage";
import { validate } from "./validate";

beforeAll(() => {
  class RO { observe() {} unobserve() {} disconnect() {} }
  vi.stubGlobal("ResizeObserver", RO);
  vi.stubGlobal("DOMMatrixReadOnly", class { m22 = 1; constructor() {} });
});
function memoryStorage(): Storage {
  const m = new Map<string, string>();
  return {
    get length() { return m.size; },
    clear: () => m.clear(),
    getItem: (k) => m.get(k) ?? null,
    key: (i) => [...m.keys()][i] ?? null,
    removeItem: (k) => { m.delete(k); },
    setItem: (k, v) => { m.set(k, String(v)); },
  };
}
beforeEach(() => { calls.length = 0; replies = {}; vi.stubGlobal("localStorage", memoryStorage()); });

const STARTERS = [{ title: "Claims triage", message: "Triage claims" }];
const doc = (over: Partial<DesignDoc> = {}): DesignDoc => ({
  turns: [], status: "idle", revision: 0, model: "us.anthropic.claude-sonnet-5", starters: STARTERS, ...over,
});

function change(over: Partial<DesignChange> = {}): DesignChange {
  return { summary: "Added a fraud check", revision: 1,
    changed: { agents: ["fraud_check"], tools: [], steps: true, blocks: [], removed: [] }, ...over };
}

describe("history", () => {
  it("merges keystrokes into one step, keeps uploads and designer changes as their own", () => {
    let h = start("a", 0);
    h = record(h, "ab", 100);
    h = record(h, "abc", 200);          // typing: merged into the step before
    h = record(h, "abcd", 300, { step: true });
    expect(h.past).toEqual(["a", "abc"]);
    h = undo(h);
    expect(h.present).toBe("abc");
    h = undo(h);
    expect(h.present).toBe("a");
    h = redo(h);
    expect(h.present).toBe("abc");
    h = record(h, "x", 5000);
    expect(h.future).toEqual([]);         // a new edit drops what could be redone
  });
});

describe("what the designer asked for", () => {
  it("lists a question until the value it names is filled in, by chat or by hand", () => {
    const c = change({ needsInput: [{ path: "tools.crm", question: "What is your CRM endpoint?" }],
      defaultsUsed: ["maxTokens 1500"] });
    const wf = { ...newProject("x").workflow };
    wf.tools = { crm: { type: "mcp" } };
    expect(openInputs([c], validate(wf))).toEqual(c.needsInput);
    wf.tools = { crm: { type: "mcp", endpoint: "https://crm.example.com/mcp" } };
    expect(openInputs([c], validate(wf))).toEqual([]);
    expect(defaultsUsed([c, change({ defaultsUsed: ["temperature 0", "maxTokens 1500"] })]))
      .toEqual(["temperature 0", "maxTokens 1500"]);
  });

  it("renders its Markdown as text, never as markup", () => {
    const { container } = render(<Markdown text={"**Built** it:\n- one `a`\n- two\n\n<img src=x onerror=alert(1)>"} />);
    expect(container.querySelector("strong")?.textContent).toBe("Built");
    expect(container.querySelectorAll("li").length).toBe(2);
    expect(container.querySelector("img")).toBeNull();
    expect(container.textContent).toContain("<img src=x onerror=alert(1)>");
  });

  it("renders rules and tables, as the in-app assistant writes them", () => {
    const { container } = render(<Markdown text={"Costs:\n\n| Agent | Cost |\n|---|---:|\n| **Math** | $0.0033 |\n| Web | $0.0220 |\n\n---\nDone."} />);
    expect([...container.querySelectorAll("th")].map((c) => c.textContent)).toEqual(["Agent", "Cost"]);
    expect(container.querySelectorAll("tbody tr").length).toBe(2);
    expect(container.querySelector("td strong")?.textContent).toBe("Math");
    expect(container.querySelector("hr")).toBeTruthy();
    expect(container.textContent).not.toContain("---");
  });
  it("renders code inside bold, as the designer writes paths", () => {
    const { container } = render(<Markdown text={"- **`tools.crm.endpoint`** — the URL"} />);
    expect(container.querySelector("strong code")?.textContent).toBe("tools.crm.endpoint");
    expect(container.textContent).not.toContain("`");
  });
});

describe("uploading into a build", () => {
  it("replaces the workflow but keeps the build, and the prompts of agents it still has", () => {
    const p = { ...newProject("Claims"), id: "pkeep01" };
    const wf = { ...p.workflow, agents: { ...p.workflow.agents, other: { name: "Other", runtime: "main", maxTokens: 10 } } };
    const next = replaceWorkflow(p, JSON.stringify(wf));
    expect(next.id).toBe("pkeep01");
    expect(next.name).toBe("Claims");
    expect(Object.keys(next.workflow.agents)).toEqual(["first_agent", "other"]);
    expect(next.prompts.first_agent).toEqual(p.prompts.first_agent);
  });
});

describe("agent properties", () => {
  it("moves an older build's framework from its prompt onto the agent, where workflow.json carries it", () => {
    const p = newProject("Old");
    p.prompts.first_agent = { ...p.prompts.first_agent, framework: "strands" };
    const m = migrateFramework(p);
    expect(m.workflow.agents.first_agent.framework).toBe("strands");
    expect(m.prompts.first_agent.framework).toBeUndefined();
    expect(migrateFramework(m)).toBe(m);          // nothing left to move: unchanged
  });

  it("starts every new agent with the shared guardrail on for input and output", () => {
    const r = addAgent(newProject("x"), "Checker");
    expect(r.project.workflow.agents[r.id].agentcore).toEqual({ guardrails: { input: true, output: true } });
    expect(newProject("y").workflow.agents.first_agent.agentcore).toMatchObject({
      guardrails: { input: true, output: true }, evaluations: { enabled: true } });
    // Legal wherever the agent goes: the only error is that it is on no stage yet.
    expect(validate(r.project.workflow).filter((i) => i.severity === "error").map((i) => i.path))
      .toEqual([`agents.${r.id}`]);
  });
});

describe("the AgentExpress Assistant tab", () => {
  const stored: Project = { ...newProject("Claims"), id: "pclaims01" };
  const summary: BuildSummary = { id: stored.id, name: stored.name, updatedAt: stored.updatedAt,
    agentName: "ax_1a2b3c4d", versions: 0 };

  function serverStore(state: { project: Project; saves: Project[] }): BuildStore {
    return {
      server: true,
      list: async () => [summary],
      load: async () => ({ project: state.project, build: summary }),
      save: async (p) => { state.saves.push(p); state.project = p; return summary; },
      remove: async () => ({}),
    };
  }

  it("sends a starter, follows the reply, reloads the build it changed, and undoes the change", async () => {
    const state = { project: stored, saves: [] as Project[] };
    const withFraud: Project = { ...stored, workflow: { ...stored.workflow, agents: { ...stored.workflow.agents,
      fraud_check: { name: "Fraud Check", runtime: "main", maxTokens: 1500 } },
    steps: [...stored.workflow.steps, { agent: "fraud_check" }] } };
    const path = "/api/builds/pclaims01/design";
    const thinking = doc({ status: "thinking", turns: [
      { id: "u1", role: "user", text: "Triage claims", at: "" },
      { id: "a1", role: "assistant", text: "", at: "", status: "thinking" }] });
    const done = doc({ revision: 1, turns: [thinking.turns[0],
      { id: "a1", role: "assistant", text: "Added **Fraud Check**.", at: "", status: "done",
        changes: [change({ defaultsUsed: ["maxTokens 1500"] })] }] });
    replies = { [`GET ${path}`]: [doc(), done], [`POST ${path}`]: [thinking] };

    vi.useFakeTimers({ shouldAdvanceTime: true });
    await act(async () => { render(<Builder notify={vi.fn()} store={serverStore(state)} />); });
    expect(screen.getByText("What would you like to build?")).toBeTruthy();
    await act(async () => { fireEvent.click(screen.getByText("Claims triage")); });
    expect(calls.find((c) => c.method === "POST")?.body).toEqual({ message: "Triage claims" });
    expect(screen.getByText("Thinking…")).toBeTruthy();

    state.project = withFraud;                 // what the designer saved on the server
    await act(async () => { vi.advanceTimersByTime(2100); });
    vi.useRealTimers();
    await waitFor(() => expect(screen.queryAllByTestId("builder-agent-fraud_check").length).toBeGreaterThan(0));
    expect(screen.getByText("Added a fraud check")).toBeTruthy();
    expect(screen.getByText("maxTokens 1500")).toBeTruthy();   // "Defaults used"

    // Undo is the same for the designer's change as for a drag: it takes it back.
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Undo" })); });
    await waitFor(() => expect(screen.queryAllByTestId("builder-agent-fraud_check")).toEqual([]));
  });

  it("shows the reply growing as it streams, says what it is doing, and sends on Enter", async () => {
    const state = { project: stored, saves: [] as Project[] };
    const path = "/api/builds/pclaims01/design";
    const user = { id: "u1", role: "user" as const, text: "Triage claims", at: "2026-09-09T10:00:00Z" };
    const live = (text: string, phase: "thinking" | "writing" | "applying" | "checking", extra = {}) => doc({ status: "thinking",
      turns: [user, { id: "a1", role: "assistant", text, at: "", status: "thinking", phase, ...extra }] });
    replies = { [`GET ${path}`]: [live("", "thinking", { thinking: "Intake first, then a review gate" }),
      live("I'll draft", "writing"),
      live("I'll draft a triage flow.", "applying", { progress: ["Adding tool policyKb", "Adding agent claims_intake"] }),
      doc({ turns: [user, { id: "a1", role: "assistant", text: "I'll draft a triage flow.\n\n```json\n{\"a\": 1}\n```",
        at: "2026-09-09T10:00:05Z", status: "done" }] })],
    [`POST ${path}`]: [doc({ status: "thinking", turns: [user] })] };
    vi.useFakeTimers({ shouldAdvanceTime: true });
    let view!: ReturnType<typeof render>;
    await act(async () => { view = render(<Builder notify={vi.fn()} store={serverStore(state)} />); });
    expect(screen.getByText("Thinking…")).toBeTruthy();
    // Its reasoning streams under the spinner, so a long think is not a bare wait.
    expect(screen.getByTestId("design-thinking").textContent).toBe("Intake first, then a review gate");
    expect(screen.queryByText("Claude Sonnet 5")).toBeNull();          // no model name in the chat
    expect(screen.getAllByText("AgentExpress Assistant").length).toBeGreaterThan(0);
    await act(async () => { vi.advanceTimersByTime(750); });
    expect(screen.getByText("I'll draft")).toBeTruthy();
    expect(view.container.querySelector(".axd-streaming")).toBeTruthy();   // the cursor
    expect(screen.queryByText("Thinking…")).toBeNull();
    await act(async () => { vi.advanceTimersByTime(750); });
    expect(screen.getByText("Updating the build…")).toBeTruthy();
    // Each edit of the change being written, named as it streams in.
    expect(screen.getByTestId("design-progress").textContent).toBe("Adding tool policyKbAdding agent claims_intake");
    expect(screen.queryByTestId("design-thinking")).toBeNull();
    expect(view.container.querySelector(".axd-streaming")).toBeNull();
    await act(async () => { vi.advanceTimersByTime(750); });
    vi.useRealTimers();
    await waitFor(() => expect(view.container.querySelector("pre code")?.textContent).toBe('{"a": 1}'));
    expect(screen.getByRole("button", { name: "Copy reply" })).toBeTruthy();
    // Enter sends the message; the box empties for the next one.
    const box = view.container.querySelector("textarea[aria-label='Message to the designer']") as HTMLTextAreaElement;
    await act(async () => { fireEvent.change(box, { target: { value: "Add a fraud check" } }); });
    await act(async () => { fireEvent.keyDown(box, { key: "Enter", keyCode: 13 }); });
    await waitFor(() => expect(calls.filter((c) => c.method === "POST").at(-1)?.body)
      .toEqual({ message: "Add a fraud check" }));
    expect(box.value).toBe("");
  });
  it("opens the chat of a build made a moment ago, once its first save lands", async () => {
    const state = { project: stored, saves: [] as Project[] };
    replies = { "GET /api/builds/pclaims01/design": [new Error("unknown build"), doc()] };
    await act(async () => { render(<Builder notify={vi.fn()} store={serverStore(state)} />); });
    await waitFor(() => expect(screen.getByText("What would you like to build?")).toBeTruthy(), { timeout: 4000 });
    expect(screen.queryByText("unknown build")).toBeNull();
  });
  it("asks for each secret once, in a secure field, and only while its tool is in the build", () => {
    const a = change({ secretsNeeded: [{ kind: "toolApiKeys", name: "crm", label: "API key", why: "old" }] });
    const b = change({ revision: 2, secretsNeeded: [
      { kind: "toolApiKeys", name: "crm", label: "API key", why: "The CRM key" },
      { kind: "toolApiKeys", name: "gone", label: "API key", why: "removed since" },
      { kind: "a2aTokens", name: "partner", label: "Bearer token", why: "Partner token" }] });
    expect(secretsNeeded([a, b], { crm: {} }, { partner: {} })).toEqual([
      { kind: "toolApiKeys", name: "crm", label: "API key", why: "The CRM key" },
      { kind: "a2aTokens", name: "partner", label: "Bearer token", why: "Partner token" }]);
  });
  it("asks for a named identity's secret while the identity is in the build", () => {
    const c = change({ secretsNeeded: [{ kind: "identitySecrets", name: "cmsApi", label: "API key", why: "CMS key" }] });
    expect(secretsNeeded([c], {}, {}, { cmsApi: { type: "apikey" } })).toEqual(c.secretsNeeded);
    expect(secretsNeeded([c], {}, {}, {})).toEqual([]);
  });
  it("dates each message", () => {
    const now = new Date("2026-09-09T12:00:00");
    expect(when("2026-09-09T09:05:00", now)).not.toContain(",");
    expect(when("2026-09-01T09:05:00", now)).toContain(",");
    expect(when("", now)).toBe("");
  });
  it("attaches a file to the next message, shows it on the message, and names allowed S3 buckets", async () => {
    const state = { project: stored, saves: [] as Project[] };
    const path = "/api/builds/pclaims01/design";
    const fetchMock = vi.fn(async () => new Response(null, { status: 204 }));
    const realFetch = globalThis.fetch;
    globalThis.fetch = fetchMock as unknown as typeof fetch;
    const sent = doc({ turns: [{ id: "u1", role: "user", text: "Use the spec", at: "",
      attachments: [{ name: "spec.md", source: "upload" }] }], attach: { maxFiles: 5, maxBytes: 4500000, types: ["md"], s3: ["data-bucket"] } });
    replies = { [`GET ${path}`]: [doc({ attach: { maxFiles: 5, maxBytes: 4500000, types: ["md"], s3: ["data-bucket"] } }), sent],
      [`POST ${path}/attachments`]: [{ url: "https://s3.example/up", fields: { key: "k" }, key: "builds/pclaims01/attachments/ab-spec.md", name: "spec.md" }],
      [`POST ${path}`]: [sent] };
    let view!: ReturnType<typeof render>;
    await act(async () => { view = render(<Builder notify={vi.fn()} store={serverStore(state)} />); });
    await waitFor(() => expect(screen.getByText(/S3 paths can be in: data-bucket/)).toBeTruthy());
    const input = createWrapper(view.container).findPromptInput()!.findSecondaryActions()!
      .getElement().querySelector("input[type=file]") as HTMLInputElement;
    await act(async () => { fireEvent.change(input, { target: { files: [new File(["# spec"], "spec.md", { type: "text/markdown" })] } }); });
    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith("https://s3.example/up", expect.objectContaining({ method: "POST" })));
    await act(async () => { await new Promise((r) => setTimeout(r, 0)); });   // the upload settles
    const prompt = createWrapper(view.container).findPromptInput()!;
    await act(async () => { prompt.setTextareaValue("Use the spec"); });
    await act(async () => { prompt.findActionButton().click(); });
    expect(calls.find((c) => c.method === "POST" && c.path === path)!.body)
      .toEqual({ message: "Use the spec", attachments: [{ key: "builds/pclaims01/attachments/ab-spec.md", name: "spec.md" }] });
    await waitFor(() => expect(view.container.querySelector(".axd-attached-item")?.textContent).toContain("spec.md"));
    globalThis.fetch = realFetch;
  });
  it("is only offered on the console: a browser-only draft builds manually", async () => {
    await act(async () => { render(<Builder notify={vi.fn()} />); });
    expect(screen.queryByText("AgentExpress Assistant")).toBeNull();
    expect(screen.getByText("+ New agent")).toBeTruthy();
  });
});
