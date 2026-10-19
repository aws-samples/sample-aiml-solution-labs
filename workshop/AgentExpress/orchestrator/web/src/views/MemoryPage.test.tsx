/** The Memory records page: pick an agent, then a namespace, read what was remembered,
 *  search it, and delete one record after a confirmation that names it. Record text is
 *  shown as text, never as HTML. */
import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

const get = vi.fn();
const post = vi.fn();
const del = vi.fn();
vi.mock("../api", () => ({ api: { get: (p: string) => get(p), post: (p: string, b: unknown) => post(p, b), del: (p: string) => del(p) } }));
import { excerpt, MemoryPage, QUERY_MAX, type MemoryRecord } from "./MemoryPage";

const NS = "insights/research-u0123456789";
const rec = (id: string, text: string, extra: Partial<MemoryRecord> = {}): MemoryRecord =>
  ({ id, text, createdAt: "2026-01-01T00:01:00+00:00", strategyId: "insights-abc", namespace: NS, ...extra });
const HOSTILE = '<img src="x" onerror="window.__pwned=1"> **bold** [link](javascript:alert(1))';

function answer(records: MemoryRecord[], next: string | null = null) {
  get.mockImplementation(async (p: string) => {
    if (p === "/api/memory/agents") {
      return { deployed: true, agents: [{ id: "research", name: "Research", memory: "default", kinds: ["semantic", "summary"] }] };
    }
    if (p.startsWith("/api/memory/namespaces")) {
      return { truncated: false, namespaces: [{ namespace: NS, actor: "research-u0123456789", kind: "semantic", records: records.length }] };
    }
    if (p.startsWith("/api/memory/records")) {
      return p.includes("next=") ? { records: [rec("mem-2", "second page")], next: null } : { records, next };
    }
    throw new Error(`unexpected ${p}`);
  });
}

const w = () => createWrapper(document.body);
const notify = vi.fn();

async function open() {
  await act(async () => { render(<MemoryPage notify={notify} />); });
  const agent = w().findAllSelects()[0];
  await act(async () => { agent.openDropdown(); });
  await act(async () => { agent.selectOptionByValue("research"); });
  const ns = w().findAllSelects()[1];
  await act(async () => { ns.openDropdown(); });
  await act(async () => { ns.selectOptionByValue(NS); });
}

beforeEach(() => { get.mockReset(); post.mockReset(); del.mockReset(); notify.mockReset(); });

describe("reading", () => {
  it("asks for the agent's namespaces, then that namespace's records", async () => {
    answer([rec("mem-1", "acme prefers serverless")]);
    await open();
    expect(get).toHaveBeenCalledWith("/api/memory/namespaces?agent=research");
    expect(get).toHaveBeenCalledWith(`/api/memory/records?agent=research&namespace=${encodeURIComponent(NS)}`);
    await waitFor(() => expect(screen.getByText("acme prefers serverless")).toBeTruthy());
    expect(screen.getByText("insights-abc")).toBeTruthy();
    expect(screen.getByText("2026-01-01T00:01:00+00:00")).toBeTruthy();
  });

  it("shows a record's text as text, never as HTML or markdown", async () => {
    answer([rec("mem-1", HOSTILE)]);
    await open();
    await waitFor(() => expect(screen.getByText(HOSTILE)).toBeTruthy());
    expect(document.querySelector("table img")).toBeNull();
    expect(document.querySelector("table strong, table b, table a[href^='javascript']")).toBeNull();
    expect((window as unknown as { __pwned?: number }).__pwned).toBeUndefined();
  });

  it("loads the next page with the token the server gave, and adds it", async () => {
    answer([rec("mem-1", "first page")], "tok-50");
    await open();
    await waitFor(() => expect(screen.getByText("first page")).toBeTruthy());
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Load more" })); });
    expect(get).toHaveBeenCalledWith(`/api/memory/records?agent=research&namespace=${encodeURIComponent(NS)}&next=tok-50`);
    expect(screen.getByText("first page")).toBeTruthy();
    expect(screen.getByText("second page")).toBeTruthy();
    expect(screen.queryByRole("button", { name: "Load more" })).toBeNull();
  });

  it("says when the deployment has no long-term memory", async () => {
    get.mockResolvedValue({ deployed: false, agents: [] });
    await act(async () => { render(<MemoryPage notify={notify} />); });
    expect(screen.getByText("This deployment has no long-term memory.")).toBeTruthy();
    expect(w().findSelect()).toBeNull();
  });
});

describe("searching", () => {
  it("posts the agent, namespace and query, and shows the results with their score", async () => {
    answer([rec("mem-1", "everything")]);
    post.mockResolvedValue({ records: [rec("mem-9", "acme prefers serverless", { score: 0.75 })] });
    await open();
    await act(async () => { w().findInput()!.setInputValue("  serverless "); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Search" })); });
    expect(post).toHaveBeenCalledWith("/api/memory/search", { agent: "research", namespace: NS, query: "serverless" });
    expect(screen.getByText("acme prefers serverless")).toBeTruthy();
    expect(screen.getByText("0.75")).toBeTruthy();
    expect(screen.queryByText("everything")).toBeNull();
  });

  it("will not send a search longer than the server takes", async () => {
    answer([rec("mem-1", "x")]);
    await open();
    await act(async () => { w().findInput()!.setInputValue("q".repeat(QUERY_MAX + 1)); });
    const button = w().findAllButtons().find((b) => b.getElement().textContent === "Search")!;
    expect(button.isDisabled()).toBe(true);
    expect(screen.getByText(`A search is at most ${QUERY_MAX} characters.`)).toBeTruthy();
    expect(post).not.toHaveBeenCalled();
  });
});

describe("deleting", () => {
  it("asks first, naming the record, and deletes it in its namespace", async () => {
    answer([rec("mem-1", "a wrong fact about acme"), rec("mem-2", "a right fact")]);
    del.mockResolvedValue({ deleted: "mem-1" });
    await open();
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Delete memory record mem-1" })); });
    const modal = w().findModal()!;
    expect(modal.findContent().getElement().textContent).toContain("mem-1");
    expect(modal.findContent().getElement().textContent).toContain("a wrong fact about acme");
    expect(del).not.toHaveBeenCalled();
    await act(async () => {
      fireEvent.click(modal.findFooter()!.findAllButtons().find((b) => b.getElement().textContent === "Delete")!.getElement());
    });
    expect(del).toHaveBeenCalledWith(`/api/memory/records/mem-1?agent=research&namespace=${encodeURIComponent(NS)}`);
    expect(screen.queryByText("a wrong fact about acme")).toBeNull();
    expect(screen.getByText("a right fact")).toBeTruthy();
    expect(notify).toHaveBeenCalledWith("success", "Memory record mem-1 deleted.");
  });

  it("deletes nothing when the confirmation is cancelled", async () => {
    answer([rec("mem-1", "keep me")]);
    await open();
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Delete memory record mem-1" })); });
    await act(async () => {
      fireEvent.click(w().findModal()!.findFooter()!.findAllButtons().find((b) => b.getElement().textContent === "Cancel")!.getElement());
    });
    expect(del).not.toHaveBeenCalled();
    expect(screen.getByText("keep me")).toBeTruthy();
  });

  it("keeps the record and says why when the delete fails", async () => {
    answer([rec("mem-1", "still here")]);
    del.mockRejectedValue(new Error("no such memory record"));
    await open();
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Delete memory record mem-1" })); });
    await act(async () => {
      fireEvent.click(w().findModal()!.findFooter()!.findAllButtons().find((b) => b.getElement().textContent === "Delete")!.getElement());
    });
    expect(screen.getByText("still here")).toBeTruthy();
    expect(notify).toHaveBeenCalledWith("error", "Could not delete the record: no such memory record");
  });
});

describe("the excerpt in the confirmation", () => {
  it("is one line and cut at the limit", () => {
    expect(excerpt("a\n\nb   c")).toBe("a b c");
    expect(excerpt("x".repeat(200), 10)).toBe(`${"x".repeat(9)}…`);
  });
});
