import { act, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

vi.mock("../api", () => ({ api: {
  get: () => Promise.resolve([{ id: "abcd0001", kind: "interceptor", name: "piiRedactor", description: "Masks PII",
    definition: { point: "response", code: {}, templates: { redactPii: {} } }, files: { "handler.py": "def lambda_handler(e, c):\n    return e\n" },
    mine: true, shares: { emails: [], groups: [], everyone: false } }]),
  post: vi.fn(), put: vi.fn(), del: vi.fn() } }));
import { LibraryPage } from "./Library";

describe("the Interceptors library page", () => {
  it("lists what was published and shows its code", async () => {
    await act(async () => { render(<LibraryPage kind="interceptor" notify={() => {}} />); });
    expect(screen.getByText("Interceptors")).toBeTruthy();
    await act(async () => { screen.getAllByRole("button", { name: "View piiRedactor" })[0].click(); });
    expect(screen.getByText(/def lambda_handler/)).toBeTruthy();
    expect(screen.queryByRole("button", { name: /New interceptor/ })).toBeNull();
  });
});
