/** Share on a run page: make a link with an expiry, copy it, list and revoke the run's links. */
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

const calls = vi.hoisted(() => ({ get: vi.fn(), post: vi.fn(), del: vi.fn() }));
vi.mock("../api", () => ({
  ApiError: class extends Error { constructor(readonly status: number, m: string) { super(m); } },
  api: { get: calls.get, post: calls.post, del: calls.del },
}));
import { ShareRun, shareLink, type RunShare } from "./ShareRun";

const link = (id: string, expires = "2030-01-08T10:00:00Z"): RunShare =>
  ({ shareId: id, url: `/#shared:${id}`, expires, created: "2030-01-01T10:00:00Z" });
const A = "A".repeat(26) + "aaaaaa";
const B = "B".repeat(26) + "bbbbbb";

afterEach(() => { cleanup(); calls.get.mockReset(); calls.post.mockReset(); calls.del.mockReset(); });

async function openDialog() {
  await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Share" })); });
}

describe("ShareRun", () => {
  it("lists the run's active links when opened, and reads nothing before", async () => {
    calls.get.mockResolvedValue({ shares: [link(A)], max: 10 });
    render(<ShareRun sessionId="run 1" />);
    expect(calls.get).not.toHaveBeenCalled();
    await openDialog();
    await waitFor(() => expect(screen.getByText("Active links (1 of 10)")).toBeTruthy());
    expect(calls.get).toHaveBeenCalledWith("/api/sessions/run%201/shares");
    expect(screen.getByText(/cannot\s+approve, re-run, stop or delete it/)).toBeTruthy();
  });

  it("makes a link with the expiry chosen, and offers it to copy", async () => {
    calls.get.mockResolvedValueOnce({ shares: [], max: 10 }).mockResolvedValue({ shares: [link(A)], max: 10 });
    calls.post.mockResolvedValue(link(A));
    render(<ShareRun sessionId="abc123" />);
    await openDialog();
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "1 day" })); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Create link" })); });
    expect(calls.post).toHaveBeenCalledWith("/api/sessions/abc123/shares", { expiresIn: "1d" });
    await waitFor(() => expect(screen.getByText("Link created")).toBeTruthy());
    expect(screen.getAllByText(shareLink(A)).length).toBeGreaterThan(0);
  });

  it("asks for seven days unless told otherwise", async () => {
    calls.get.mockResolvedValue({ shares: [], max: 10 });
    calls.post.mockResolvedValue(link(A));
    render(<ShareRun sessionId="abc123" />);
    await openDialog();
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Create link" })); });
    expect(calls.post).toHaveBeenCalledWith("/api/sessions/abc123/shares", { expiresIn: "7d" });
  });

  it("revokes one link by its id", async () => {
    calls.get.mockResolvedValueOnce({ shares: [link(A), link(B)], max: 10 }).mockResolvedValue({ shares: [link(A)], max: 10 });
    calls.del.mockResolvedValue({ ok: true });
    render(<ShareRun sessionId="abc123" />);
    await openDialog();
    await waitFor(() => expect(screen.getByText("Active links (2 of 10)")).toBeTruthy());
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Revoke the link ending bbbbbb" })); });
    expect(calls.del).toHaveBeenCalledWith(`/api/sessions/abc123/shares/${B}`);
    await waitFor(() => expect(screen.getByText("Active links (1 of 10)")).toBeTruthy());
  });

  it("will not make an eleventh link", async () => {
    calls.get.mockResolvedValue({ shares: Array.from({ length: 10 }, (_, i) => link(`${"C".repeat(31)}${i}`)), max: 10 });
    render(<ShareRun sessionId="abc123" />);
    await openDialog();
    await waitFor(() => expect(screen.getByText("Active links (10 of 10)")).toBeTruthy());
    expect((screen.getByRole("button", { name: "Create link" }) as HTMLButtonElement).disabled).toBe(true);
  });

  it("builds the address from this page and the link id", () => {
    expect(shareLink(A, { origin: "https://d1.cloudfront.net", pathname: "/" }))
      .toBe(`https://d1.cloudfront.net/#shared:${A}`);
  });
});
