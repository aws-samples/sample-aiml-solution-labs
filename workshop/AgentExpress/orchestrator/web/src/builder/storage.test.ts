/** Where builds are kept: the console's builds store, or this browser. */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { newProject } from "./model";
import { listProjects, localStore, migrateLocalDrafts, saveProject, serverStore } from "./storage";

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

type Call = { url: string; method: string; body?: unknown };
let calls: Call[] = [];

function stubFetch(reply: (c: Call) => unknown) {
  vi.stubGlobal("fetch", vi.fn(async (url: string, init: RequestInit = {}) => {
    const c = { url, method: init.method ?? "GET", body: init.body ? JSON.parse(String(init.body)) : undefined };
    calls.push(c);
    return new Response(JSON.stringify(reply(c)), { status: 200, headers: { "content-type": "application/json" } });
  }));
}

beforeEach(() => { calls = []; vi.stubGlobal("localStorage", memoryStorage()); });
afterEach(() => { vi.unstubAllGlobals(); });

describe("serverStore", () => {
  it("saves the whole project under its id, and reads the server's timestamp back", async () => {
    stubFetch(() => ({ id: "p1", name: "Claims", updated: "2026-09-09T10:00:00Z", agentName: "ax_1a2b3c4d", versions: 0 }));
    const p = { ...newProject("Claims"), id: "p1" };
    const b = await serverStore.save(p);
    expect(calls[0]).toMatchObject({ url: "/api/builds/p1", method: "PUT" });
    expect((calls[0].body as { project: { workflow: unknown } }).project.workflow).toEqual(p.workflow);
    expect(b.updatedAt).toBe("2026-09-09T10:00:00Z");
    expect(b.agentName).toBe("ax_1a2b3c4d");
  });

  it("reports a delete that has to destroy a stack first", async () => {
    stubFetch(() => ({ destroying: true, job: {} }));
    expect(await serverStore.remove("p1")).toEqual({ destroying: true });
    expect(calls[0]).toMatchObject({ url: "/api/builds/p1", method: "DELETE" });
  });
});

describe("drafts left in this browser", () => {
  it("move to the server once, and are removed here", async () => {
    saveProject({ ...newProject("Old draft"), id: "pold1" });
    stubFetch(() => ({ id: "pold1", name: "Old draft", updated: "2026-09-09T10:00:00Z" }));
    expect(await migrateLocalDrafts()).toBe(1);
    expect(calls.map((c) => c.url)).toEqual(["/api/builds/pold1"]);
    expect(listProjects()).toEqual([]);
    expect(await migrateLocalDrafts()).toBe(0);
  });

  it("stay here when the server refuses them", async () => {
    saveProject({ ...newProject("Old draft"), id: "pold1" });
    vi.stubGlobal("fetch", vi.fn(async () => new Response("{}", { status: 500 })));
    expect(await migrateLocalDrafts()).toBe(0);
    expect(listProjects().map((p) => p.name)).toEqual(["Old draft"]);
  });
});

describe("localStore", () => {
  it("is the browser fallback, with no deploy", async () => {
    const store = localStore();
    expect(store.server).toBe(false);
    await store.save({ ...newProject("Here"), id: "phere1" });
    expect((await store.list()).map((b) => b.name)).toEqual(["Here"]);
    expect((await store.load("phere1"))?.project.name).toBe("Here");
    await store.remove("phere1");
    expect(await store.list()).toEqual([]);
  });
});
