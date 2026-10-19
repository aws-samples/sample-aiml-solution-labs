/** The Live config page: it starts from the active version, shows what a draft changes, saves
 *  only the settings that are set, and activates (or rolls back) with the version it last saw. */
import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { compact, configLabel, diff, fromText, shown, toText } from "../lib/liveConfig";

const get = vi.fn();
const post = vi.fn();
const put = vi.fn();
vi.mock("../api", () => ({
  api: { get: (p: string) => get(p), post: (p: string, b: unknown) => post(p, b), put: (p: string, b: unknown) => put(p, b) },
}));
import { LiveConfigPage } from "./LiveConfigPage";

const STATE = {
  pointer: { active: 1, at: "2026-10-17 10:00:00", by: "admin@example.com" },
  agents: {
    intake: { name: "Intake", runtime: "main", deployed: { maxTokens: 1000 } },
    report: { name: "Report", runtime: "dedicated", deployed: { model: "us.m1", maxTokens: 4000 } },
  },
  promptMax: 32000, noteMax: 500,
};
const V1 = { version: 1, at: "2026-10-17 09:00:00", by: "admin@example.com", note: "cooler", agents: ["intake"],
  overlay: { intake: { temperature: 0.2 } } };

function serve(versions = [V1]) {
  get.mockImplementation(async (p: string) => {
    if (p === "/api/liveconfig") return STATE;
    if (p === "/api/liveconfig/versions") return { versions };
    if (p === "/api/liveconfig/versions/1") return V1;
    throw new Error(`unexpected ${p}`);
  });
}

beforeEach(() => { get.mockReset(); post.mockReset(); put.mockReset(); });

describe("the helpers", () => {
  it("labels a run on a live version, and nothing for one as deployed", () => {
    expect(configLabel(3)).toBe("config v3");
    for (const v of [0, undefined, null, "", "x", 1.5, -1]) expect(configLabel(v as never)).toBe("");
  });
  it("diffs field by field and reads empty as deployed", () => {
    expect(diff({ a: { temperature: 0.2 } }, { a: { temperature: 0.2, maxTokens: 9 }, b: { model: "m" } })).toEqual([
      { agent: "a", field: "maxTokens", active: undefined, draft: 9 },
      { agent: "b", field: "model", active: undefined, draft: "m" },
    ]);
    expect(shown(undefined)).toBe("(as deployed)");
    expect(fromText("temperature", " ")).toBeUndefined();
    expect(fromText("temperature", "0.5")).toBe(0.5);
    expect(fromText("maxTokens", "lots")).toBe("lots");      // kept, so the server names the problem
    expect(fromText("stopSequences", "END\n\n STOP ")).toEqual(["END", "STOP"]);
    expect(toText("stopSequences", ["END", "STOP"])).toBe("END\nSTOP");
    expect(compact({ a: { temperature: undefined }, b: { model: "m" } })).toEqual({ b: { model: "m" } });
  });
});

describe("the page", () => {
  it("starts from the active version and lists every version with v0", async () => {
    serve();
    render(<LiveConfigPage />);
    await waitFor(() => expect(screen.getByText(/Version 1 is active/)).toBeTruthy());
    expect(screen.getByText("v1 (active)")).toBeTruthy();
    expect(screen.getByText("As deployed")).toBeTruthy();
    const temp = screen.getByLabelText("Temperature for intake") as HTMLInputElement;
    expect(temp.value).toBe("0.2");
    expect(screen.getByText("Nothing changed yet.")).toBeTruthy();
  });

  it("shows a change against the active version and saves only what is set", async () => {
    serve();
    post.mockResolvedValue({ ...V1, version: 2, overlay: { intake: { temperature: 0.2, maxTokens: 2500 } } });
    render(<LiveConfigPage />);
    await waitFor(() => screen.getByLabelText("Max tokens for intake"));
    await act(async () => {
      fireEvent.change(screen.getByLabelText("Max tokens for intake"), { target: { value: "2500" } });
    });
    expect(screen.getByText("Changes against the active version")).toBeTruthy();
    expect(screen.getAllByText("Max tokens").length).toBeGreaterThanOrEqual(2);   // the label, and the diff row
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Save as a new version" })); });
    expect(post).toHaveBeenCalledWith("/api/liveconfig/versions",
      { agents: { intake: { temperature: 0.2, maxTokens: 2500 } }, note: "" });
    await waitFor(() => expect(screen.getByText(/Saved as version 2/)).toBeTruthy());
  });

  it("rolls back to the deployed config with the version it saw active", async () => {
    serve();
    put.mockResolvedValue({ before: 1, pointer: { active: 0 } });
    render(<LiveConfigPage />);
    await waitFor(() => screen.getByRole("button", { name: "Use the deployed config" }));
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Use the deployed config" })); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Activate" })); });
    expect(put).toHaveBeenCalledWith("/api/liveconfig/active", { version: 0, from: 1 });
  });

  it("shows the server's reason when a save is refused", async () => {
    serve();
    post.mockRejectedValue(new Error("agents.intake.tool: only the prompt and the model settings change live"));
    render(<LiveConfigPage />);
    await waitFor(() => screen.getByLabelText("Top P for intake"));
    await act(async () => { fireEvent.change(screen.getByLabelText("Top P for intake"), { target: { value: "0.9" } }); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Save as a new version" })); });
    await waitFor(() => expect(screen.getByText(/only the prompt and the model settings change live/)).toBeTruthy());
  });
});
