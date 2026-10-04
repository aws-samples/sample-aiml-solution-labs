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
    expect(onStart).toHaveBeenCalledWith(request, "");
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
});
