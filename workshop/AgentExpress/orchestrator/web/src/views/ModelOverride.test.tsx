/** Start run's model choice (orchestrator.models.overrides) and how the run page shows it. */
import { act, fireEvent, render, screen } from "@testing-library/react";
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { describe, expect, it, vi } from "vitest";
import { overrideLabel, overrideOf } from "../lib/modelOverride";
import { StartRunModal } from "./StartRunModal";

const ui = { defaultTopic: "" } as never;
const S = "us.anthropic.claude-sonnet-4-5-20250929-v1:0";
const O = "us.anthropic.claude-opus-4-1-20250805-v1:0";

describe("overrideOf / overrideLabel", () => {
  it("sends only what was picked, and says it in one line", () => {
    expect(overrideOf("", {})).toBeUndefined();
    expect(overrideOf("", { a: "" })).toBeUndefined();
    expect(overrideOf(S, { a: "", b: O })).toEqual({ all: S, agents: { b: O } });
    expect(overrideLabel({ all: S, agents: { b: O } }, { b: "Report" })).toBe(`all agents: ${S} · Report: ${O}`);
    expect(overrideLabel(undefined)).toBe("");
  });
});

describe("StartRunModal model override", () => {
  it("offers nothing without a list (or without the grant, which App turns into no list)", () => {
    render(<StartRunModal visible ui={ui} onDismiss={vi.fn()} onStart={vi.fn()} />);
    expect(screen.queryByText("Model for every agent (optional)")).toBeNull();
  });

  it("starts the run with the picked models, only from the list", async () => {
    const onStart = vi.fn().mockResolvedValue(undefined);
    render(<StartRunModal visible ui={ui} onDismiss={vi.fn()} onStart={onStart}
      overrideModels={[S, O]} overrideAgents={[{ id: "intake", name: "Intake" }, { id: "report", name: "Report" }]} />);
    const box = document.querySelector("textarea[aria-label='Request']") as HTMLTextAreaElement;
    await act(async () => { fireEvent.change(box, { target: { value: "Triage" } }); });
    const w = createWrapper(document.body);
    const every = w.findSelect("[aria-label='Model for every agent']") ?? w.findAllSelects()[0];
    await act(async () => { every.openDropdown(); });
    const offered = every.findDropdown().findOptions().map((o) => o.getElement().textContent);
    expect(offered).toEqual(["Each agent's own model", S, O]);
    await act(async () => { every.selectOptionByValue(O); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Start run" })); });
    expect(onStart).toHaveBeenCalledWith("Triage", "", [], { all: O });
  });

  it("starts a plain run with three arguments when nothing was picked", async () => {
    const onStart = vi.fn().mockResolvedValue(undefined);
    render(<StartRunModal visible ui={ui} onDismiss={vi.fn()} onStart={onStart} overrideModels={[S]} />);
    const box = document.querySelector("textarea[aria-label='Request']") as HTMLTextAreaElement;
    await act(async () => { fireEvent.change(box, { target: { value: "Triage" } }); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Start run" })); });
    expect(onStart).toHaveBeenCalledWith("Triage", "", []);
  });
});
