/** A run opened through a share link: the run, read-only, with who shared it and until when,
 *  and no action on it. */
import { act, cleanup, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

const calls = vi.hoisted(() => ({ get: vi.fn(), post: vi.fn(), put: vi.fn(), del: vi.fn() }));
vi.mock("../api", () => {
  class ApiError extends Error {
    constructor(readonly status: number, m: string) { super(m); }
  }
  return { ApiError, api: calls };
});
import { ApiError } from "../api";
import { fmtTime } from "../lib/clock";
import { SharedRun, type SharedSnapshot } from "./SharedRun";

const SHARE = "S".repeat(32);
const wf = { agents: { a: { name: "Alpha" } }, steps: [{ agent: "a", hitl: true }] } as never;
const snap = (over: Partial<SharedSnapshot> = {}): SharedSnapshot => ({
  session_id: "abc123", topic: "Compare the two offers", overall: "running",
  created: "2030-01-01 10:00:00", updated_at: "2030-01-01 10:01:00",
  nodes: { a: { status: "done", output: "The answer" } }, logs: [],
  workflow: wf, sharedBy: "alice@example.com", expires: "2030-01-08T10:00:00Z", readOnly: true, ...over,
});

afterEach(() => { cleanup(); Object.values(calls).forEach((f) => f.mockReset()); });

describe("SharedRun", () => {
  it("says who shared it, that it is read-only and when it expires", async () => {
    calls.get.mockResolvedValue(snap());
    render(<SharedRun shareId={SHARE} workflow={wf} />);
    await waitFor(() => expect(screen.getByText("Shared run")).toBeTruthy());
    expect(screen.getByText(`Shared by alice@example.com, read-only, expires ${fmtTime("2030-01-08T10:00:00Z")}.`)).toBeTruthy();
    expect(screen.getAllByText("Compare the two offers").length).toBeGreaterThan(0);
    expect(calls.get).toHaveBeenCalledWith(`/api/shared/${SHARE}`);
  });

  it("offers no action on the run and reads no run route", async () => {
    calls.get.mockResolvedValue(snap({ hitl: { node: "a", question: "Approve?" } }));
    render(<SharedRun shareId={SHARE} workflow={wf} />);
    await waitFor(() => expect(screen.getByText("Shared run")).toBeTruthy());
    for (const name of [/Stop run/, /^Share$/, /Re-run/i, /Restart/i, /Approve/, /Deny/, /Revise/, /Delete/]) {
      expect(screen.queryByRole("button", { name })).toBeNull();
    }
    // Flow reads the owner's run route; the shared view has no such tab.
    expect(screen.getAllByRole("tab").map((t) => t.textContent)).toEqual(
      ["Graph view", "Table view", "Timeline", "Outputs"]);
    expect(calls.get.mock.calls.every(([p]) => p === `/api/shared/${SHARE}`)).toBe(true);
    expect(calls.post).not.toHaveBeenCalled();
    expect(calls.put).not.toHaveBeenCalled();
    expect(calls.del).not.toHaveBeenCalled();
  });

  it("does not offer to connect the owner's account for a tool", async () => {
    const url = "https://bedrock-agentcore.us-east-1.amazonaws.com/identities/oauth2/authorize?x=1";
    calls.get.mockResolvedValue(snap({
      overall: "failed", nodes: { a: { status: "failed", output: "" } },
      logs: [{ ts: "2030-01-01 10:00:01", node: "a", msg: `Connect your account for 'GitHub' first, then run this step again: ${url}` }],
    }));
    render(<SharedRun shareId={SHARE} workflow={wf} />);
    await waitFor(() => expect(screen.getByText("Shared run")).toBeTruthy());
    expect(screen.queryByText("Connect your GitHub account")).toBeNull();
    expect(screen.queryByRole("link", { name: /Connect/ })).toBeNull();
  });

  it("stops showing the run once the link is refused", async () => {
    calls.get.mockRejectedValue(new ApiError(404, "this link is unknown, expired or revoked"));
    render(<SharedRun shareId={SHARE} workflow={wf} />);
    await waitFor(() => expect(screen.getByText("This link cannot be opened")).toBeTruthy());
    expect(screen.queryByText("Shared run")).toBeNull();
  });

  it("reads a finished run once, and a running one again", async () => {
    vi.useFakeTimers();
    try {
      calls.get.mockResolvedValue(snap({ overall: "done" }));
      render(<SharedRun shareId={SHARE} workflow={wf} />);
      await act(async () => { await vi.advanceTimersByTimeAsync(0); });     // the first read lands
      await act(async () => { await vi.advanceTimersByTimeAsync(12000); });
      expect(calls.get).toHaveBeenCalledTimes(1);
      cleanup();
      calls.get.mockReset();
      calls.get.mockResolvedValue(snap({ overall: "running" }));
      render(<SharedRun shareId={SHARE} workflow={wf} />);
      await act(async () => { await vi.advanceTimersByTimeAsync(5100); });
      expect(calls.get.mock.calls.length).toBeGreaterThanOrEqual(2);
    } finally {
      vi.useRealTimers();
    }
  });
});
