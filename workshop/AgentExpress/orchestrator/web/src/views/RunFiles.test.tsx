/** The run page's Files tab and the app's Files page: what they ask for, how they group
 *  files, and that a file's text is shown as text. */
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

const get = vi.fn();
vi.mock("../api", () => ({ api: { get: (p: string) => get(p) } }));

import type { RunFile } from "../lib/files";
import type { Workflow } from "../types";
import { FilesPage } from "./FilesPage";
import { RunFiles } from "./RunFiles";

const wf = { agents: { writer: { name: "Writer" }, analyst: { name: "Analyst" } }, steps: [] } as unknown as Workflow;
const f = (over: Partial<RunFile>): RunFile => ({
  key: "runs/s1/outputs/x", name: "x", group: "output", agent: "", size: 10, type: "text/plain",
  preview: "text", time: "2026-10-08T10:00:00+00:00", session: "s1", ...over,
});
const FILES: RunFile[] = [
  f({ key: "runs/s1/attachments/1-brief.pdf", name: "brief.pdf", group: "input", preview: "pdf", type: "application/pdf" }),
  f({ key: "runs/s1/outputs/report.csv", name: "report.csv", agent: "writer", preview: "csv" }),
  f({ key: "runs/s1/outputs/notes.txt", name: "notes.txt", agent: "writer", preview: "text" }),
  f({ key: "runs/s1/code/analyst-1.py", name: "analyst-1.py", group: "code", agent: "analyst" }),
];

afterEach(() => { get.mockReset(); vi.unstubAllGlobals(); });

describe("the Files tab", () => {
  it("lists inputs, outputs and code, each with the agent that made it", async () => {
    get.mockResolvedValue({ stored: true, files: FILES, truncated: false });
    render(<RunFiles sessionId="s1" workflow={wf} />);
    await waitFor(() => expect(screen.getByText("report.csv")).toBeTruthy());
    expect(get).toHaveBeenCalledWith("/api/sessions/s1/files");
    expect(screen.getByText("Inputs")).toBeTruthy();
    expect(screen.getByText("Outputs")).toBeTruthy();
    expect(screen.getByText("Code")).toBeTruthy();
    expect(screen.getAllByText("Writer")).toHaveLength(2);
    expect(screen.getByText("Analyst")).toBeTruthy();
    expect(screen.getByRole("button", { name: "Preview report.csv" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "Download brief.pdf" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "Preview brief.pdf" }).textContent).toBe("Open");
  });

  it("says why there is nothing when the deployment keeps no files", async () => {
    get.mockResolvedValue({ stored: false, files: [] });
    render(<RunFiles sessionId="s1" workflow={wf} />);
    await waitFor(() => expect(screen.getByText(/Set orchestrator.files/)).toBeTruthy());
  });

  it("shows a file's text as text, never as markup", async () => {
    get.mockImplementation((p: string) => Promise.resolve(p.includes("/files/url")
      ? { url: "https://bucket.s3/notes?sig", expiresIn: 300, type: "text/plain", preview: "text", name: "notes.txt" }
      : { stored: true, files: FILES }));
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response("<img src=x onerror=alert(1)> plain", { status: 200 })));
    const { baseElement } = render(<RunFiles sessionId="s1" workflow={wf} />);
    await waitFor(() => expect(screen.getByText("notes.txt")).toBeTruthy());
    fireEvent.click(screen.getByRole("button", { name: "Preview notes.txt" }));
    await waitFor(() => expect(screen.getByText("<img src=x onerror=alert(1)> plain")).toBeTruthy());
    expect(get).toHaveBeenCalledWith("/api/sessions/s1/files/url?key=runs%2Fs1%2Foutputs%2Fnotes.txt");
    expect(baseElement.querySelector("img[src='x']")).toBeNull();
  });

  it("shows a CSV as a table of its first rows", async () => {
    get.mockImplementation((p: string) => Promise.resolve(p.includes("/files/url")
      ? { url: "https://bucket.s3/r?sig", expiresIn: 300, type: "text/csv", preview: "csv", name: "report.csv" }
      : { stored: true, files: FILES }));
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response("plan,price\nbasic,10\npro,25\n", { status: 200 })));
    render(<RunFiles sessionId="s1" workflow={wf} />);
    await waitFor(() => expect(screen.getByText("report.csv")).toBeTruthy());
    fireEvent.click(screen.getByRole("button", { name: "Preview report.csv" }));
    const dialog = await screen.findByRole("dialog");
    await waitFor(() => expect(within(dialog).getByText("pro")).toBeTruthy());
    expect(within(dialog).getByText("price")).toBeTruthy();
    expect(within(dialog).getByText("25")).toBeTruthy();
  });
});

describe("the Files page", () => {
  it("lists your runs' files, and everyone's only when you may see them", async () => {
    get.mockResolvedValue({ stored: true, files: FILES.map((x) => ({ ...x, topic: "Price the plan" })), nextPage: null });
    const open = vi.fn();
    const { rerender } = render(<FilesPage workflow={wf} canSeeEveryone={false} onOpenRun={open} />);
    await waitFor(() => expect(screen.getByText("report.csv")).toBeTruthy());
    expect(get).toHaveBeenCalledWith("/api/files?page=0");
    expect(screen.queryByText("Everyone")).toBeNull();
    fireEvent.click(screen.getAllByText("Price the plan")[0]);
    expect(open).toHaveBeenCalledWith("s1");
    rerender(<FilesPage workflow={wf} canSeeEveryone onOpenRun={open} />);
    fireEvent.click(screen.getByText("Everyone"));
    await waitFor(() => expect(get).toHaveBeenCalledWith("/api/files?page=0&scope=all"));
  });

  it("loads older runs a page at a time", async () => {
    get.mockResolvedValueOnce({ stored: true, files: [FILES[1]], nextPage: 1 })
      .mockResolvedValueOnce({ stored: true, files: [FILES[3]], nextPage: null });
    render(<FilesPage workflow={wf} canSeeEveryone={false} onOpenRun={() => {}} />);
    await waitFor(() => expect(screen.getByText("report.csv")).toBeTruthy());
    fireEvent.click(screen.getByRole("button", { name: "Show older runs" }));
    await waitFor(() => expect(screen.getByText("analyst-1.py")).toBeTruthy());
    expect(get).toHaveBeenLastCalledWith("/api/files?page=1");
    expect(screen.getByText("report.csv")).toBeTruthy();
    expect(screen.queryByRole("button", { name: "Show older runs" })).toBeNull();
  });
});
