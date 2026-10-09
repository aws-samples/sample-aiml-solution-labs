/** Starting a run: one large request box, Start, and the subject tucked under Advanced. */
import { act, fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { REQUEST_MAX, requestTitle } from "../lib/request";
import { StartRunModal } from "./StartRunModal";

const ui = { defaultTopic: "" } as never;

describe("requestTitle", () => {
  it("names a run by its first line, shortened, and marks what it left out", () => {
    expect(requestTitle("Triage claim 88213")).toBe("Triage claim 88213");
    expect(requestTitle("Triage claim 88213\n\n1. Check the policy\n2. Flag fraud")).toBe("Triage claim 88213…");
    expect(requestTitle("\n  \nSecond line first")).toBe("Second line first");
    expect(requestTitle("x".repeat(200), 50)).toHaveLength(50);
    expect(requestTitle("x".repeat(200), 50).endsWith("…")).toBe(true);
    expect(requestTitle(undefined)).toBe("");
  });
});

describe("StartRunModal", () => {
  it("takes a long, multi-line request and starts on Ctrl+Enter", async () => {
    const onStart = vi.fn().mockResolvedValue(undefined);
    render(<StartRunModal visible ui={ui} onDismiss={vi.fn()} onStart={onStart} />);
    const box = document.querySelector("textarea[aria-label='Request']") as HTMLTextAreaElement;
    expect(Number(box.getAttribute("rows"))).toBeGreaterThanOrEqual(10);
    const request = "Triage claim 88213.\n\nInstructions:\n- check the policy wording\n- flag anything unusual";
    await act(async () => { fireEvent.change(box, { target: { value: request } }); });
    expect(screen.getByText(/ \/ 10,000 characters/)).toBeTruthy();
    // The subject is optional and hidden until Advanced is opened.
    expect(screen.getByText("Advanced")).toBeTruthy();
    await act(async () => { fireEvent.keyDown(box, { key: "Enter", keyCode: 13, ctrlKey: true }); });
    expect(onStart).toHaveBeenCalledWith(request, "", []);
  });
  it("won't start an empty or over-long request", async () => {
    const onStart = vi.fn().mockResolvedValue(undefined);
    render(<StartRunModal visible ui={ui} onDismiss={vi.fn()} onStart={onStart} />);
    const start = () => screen.getByRole("button", { name: "Start run" }) as HTMLButtonElement;
    expect(start().disabled).toBe(true);
    const box = document.querySelector("textarea[aria-label='Request']") as HTMLTextAreaElement;
    await act(async () => { fireEvent.change(box, { target: { value: "x".repeat(REQUEST_MAX + 1) } }); });
    expect(start().disabled).toBe(true);
    expect(screen.getByText("At most 10,000 characters")).toBeTruthy();
  });
  it("offers no files unless an agent reads them", () => {
    render(<StartRunModal visible ui={ui} onDismiss={vi.fn()} onStart={vi.fn()} />);
    expect(screen.queryByText("Files (optional)")).toBeNull();
  });
  it("uploads picked files at once and starts the run with them", async () => {
    const onStart = vi.fn().mockResolvedValue(undefined);
    let finish: (v: { key: string; name: string }) => void = () => undefined;
    const upload = vi.fn(() => new Promise<{ key: string; name: string }>((r) => { finish = r; }));
    const attach = { maxFiles: 5, maxBytes: 4_500_000, types: ["pdf", "png"], s3: ["customer-docs/contracts"] };
    render(<StartRunModal visible ui={ui} onDismiss={vi.fn()} onStart={onStart} attach={attach} upload={upload} />);
    expect(screen.getByText(/S3 paths can be in: customer-docs\/contracts/)).toBeTruthy();
    const input = document.querySelector("input[type='file']") as HTMLInputElement;
    expect(input.getAttribute("accept")).toBe(".pdf,.png");
    const box = document.querySelector("textarea[aria-label='Request']") as HTMLTextAreaElement;
    await act(async () => { fireEvent.change(box, { target: { value: "Summarise the brief" } }); });
    const file = new File(["%PDF"], "brief.pdf", { type: "application/pdf" });
    await act(async () => { fireEvent.change(input, { target: { files: [file] } }); });
    expect(upload).toHaveBeenCalledWith(file);
    const start = () => screen.getByRole("button", { name: "Start run" }) as HTMLButtonElement;
    expect(start().disabled).toBe(true);                        // still uploading
    await act(async () => { finish({ key: "uploads/abc/1-brief.pdf", name: "brief.pdf" }); });
    expect(start().disabled).toBe(false);
    await act(async () => { start().click(); });
    expect(onStart).toHaveBeenCalledWith("Summarise the brief", "", [{ key: "uploads/abc/1-brief.pdf", name: "brief.pdf" }]);
  });
  it("won't start while a file failed to upload", async () => {
    const upload = vi.fn().mockRejectedValue(new Error("upload failed (403)"));
    const attach = { maxFiles: 5, maxBytes: 4_500_000, types: ["pdf"], s3: [] };
    render(<StartRunModal visible ui={ui} onDismiss={vi.fn()} onStart={vi.fn()} attach={attach} upload={upload} />);
    const box = document.querySelector("textarea[aria-label='Request']") as HTMLTextAreaElement;
    await act(async () => { fireEvent.change(box, { target: { value: "go" } }); });
    const input = document.querySelector("input[type='file']") as HTMLInputElement;
    await act(async () => { fireEvent.change(input, { target: { files: [new File(["x"], "a.pdf")] } }); });
    expect(screen.getByText("A file did not upload: remove it, or pick it again")).toBeTruthy();
    expect((screen.getByRole("button", { name: "Start run" }) as HTMLButtonElement).disabled).toBe(true);
  });
});
