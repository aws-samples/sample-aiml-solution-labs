/** The Activity view: what it asks the server for, and how it reads an event back. */
import { render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

const get = vi.fn();
vi.mock("../api", () => ({ api: { get: (p: string) => get(p) } }));

import { Activity, KINDS, buildOf, dayBefore, daysIn, detailsOf, filterEvents, whereOf, type AuditEvent } from "./Activity";

const deploy: AuditEvent = {
  id: "1", ts: "2026-09-09T10:00:00.000001Z", owner: "u1", email: "u1@example.com",
  action: "deploy.succeeded",
  detail: { build: "b1", name: "Claims", version: 3, tool: "terraform", account: "123456789012", region: "eu-west-1" },
};

describe("reading an event", () => {
  it("names the build, the version and where it went", () => {
    expect(buildOf(deploy)).toBe("Claims v3");
    expect(whereOf(deploy)).toBe("account 123456789012 · eu-west-1");
    expect(whereOf({ ...deploy, detail: { account: "console" } })).toBe("this console's account");
    expect(buildOf({ ...deploy, action: "login", detail: {} })).toBe("—");
  });

  it("shows which secrets changed and where from, and nothing else", () => {
    const e = { ...deploy, action: "secrets.updated", detail: { changed: { tools: ["docs", "db"] }, ip: "1.2.3.4" } };
    expect(detailsOf(e)).toBe("tools: docs, db · from 1.2.3.4");
    expect(detailsOf(deploy)).toBe("Terraform");
  });
});

describe("the view", () => {
  it("asks for your own activity, and offers everyone's only with the permission", async () => {
    get.mockResolvedValue([deploy]);
    const { rerender } = render(<Activity canSeeEveryone={false} />);
    await waitFor(() => expect(screen.getByText("Deployed")).toBeTruthy());
    expect(get).toHaveBeenCalledWith("/api/audit");
    expect(screen.queryByText("Everyone")).toBeNull();
    rerender(<Activity canSeeEveryone />);
    expect(screen.getByText("Everyone")).toBeTruthy();
  });

  it("opens an auditor on everyone's last 7 days", async () => {
    get.mockReset();
    get.mockResolvedValue([deploy]);
    render(<Activity canSeeEveryone />);
    await waitFor(() => expect(get).toHaveBeenCalled());
    expect(get).toHaveBeenCalledWith(`/api/audit?scope=all&from=${dayBefore(6)}&to=${dayBefore(0)}`);
    expect(screen.getByText("Last 7 days")).toBeTruthy();
  });
});

describe("the date range", () => {
  it("counts the days in it, both ends included", () => {
    expect(dayBefore(6, new Date("2026-09-09T12:00:00Z"))).toBe("2026-09-03");
    expect(daysIn("2026-09-03", "2026-09-09")).toBe(7);
    expect(daysIn("2026-09-09", "2026-09-09")).toBe(1);
    expect(daysIn("2026-09-09", "2026-09-01")).toBe(0);
    expect(daysIn("", "2026-09-01")).toBe(0);
  });
  it("names library, group and sharing events", () => {
    const e = (action: string, detail: Record<string, unknown>): AuditEvent =>
      ({ id: action, ts: "2026-09-09T10:00:00Z", owner: "u1", email: "u1@example.com", action, detail });
    expect(detailsOf(e("library.created", { item: "x", kind: "guardrail", name: "brandSafe" }))).toBe("guardrail brandSafe");
    expect(detailsOf(e("group.saved", { group: "Group 1", members: 3 }))).toBe("group Group 1, 3 members");
    expect(detailsOf(e("build.shared", { build: "b1", shares: { everyone: true } }))).toBe("shared with everyone");
    expect(filterEvents([e("library.created", {}), e("login", {})], "library", "").length).toBe(1);
  });
});
describe("run and design events", () => {
  const ev = (action: string, detail: Record<string, unknown>, email = "u1@example.com"): AuditEvent =>
    ({ id: action + email, ts: "2026-09-09T10:00:00Z", owner: "u1", email, action, detail });
  it("says what was decided, re-run or asked", () => {
    expect(detailsOf(ev("run.decided", { session: "abc123", decision: "revise", comment: "tighten it" })))
      .toBe("run abc123 · revise · comment: tighten it");
    expect(detailsOf(ev("run.decided", { session: "abc123", decision: "per-agent", decisions: { a: "approve", b: "revise" } })))
      .toBe("run abc123 · per agent (a: approve, b: revise)");
    expect(detailsOf(ev("run.rerun", { session: "s1", agents: ["partner"] }))).toBe("run s1 · agents: partner");
    expect(detailsOf(ev("design.message", { build: "b1", name: "Claims", message: "Add a fraud check" })))
      .toBe("“Add a fraud check”");
  });
  it("filters by kind of action and by any text a person reads", () => {
    const all = [ev("login", {}), ev("run.started", { session: "s1", build: "b1", name: "Claims" }),
      ev("deploy.failed", { build: "b2", name: "Other", error: "boom" }, "u2@example.com"),
      ev("account.updated", { account: "123456789012", label: "Prod" })];
    expect(filterEvents(all, "run", "").map((e) => e.action)).toEqual(["run.started"]);
    expect(filterEvents(all, "failed", "").map((e) => e.action)).toEqual(["deploy.failed"]);
    expect(filterEvents(all, "", "u2@").map((e) => e.action)).toEqual(["deploy.failed"]);
    expect(filterEvents(all, "", "claims").map((e) => e.action)).toEqual(["run.started"]);
    expect(filterEvents(all, "account", "prod").length).toBe(1);
    expect(filterEvents(all, "sign", "").map((e) => e.action)).toEqual(["login"]);
    expect(KINDS.every((k) => k.label)).toBe(true);
  });
});
