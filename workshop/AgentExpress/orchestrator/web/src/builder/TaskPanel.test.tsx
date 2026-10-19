/** Task steps in the Builder (runtime "task"): added from the palette with the keyboard,
 *  drawn as "Task · <kind>" on the canvas, edited in their own panel, checked as you type. */
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { act, fireEvent, render, screen } from "@testing-library/react";
import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { Builder } from "./Builder";
import { STARTER_SCRIPT, TASK_KINDS, addTask, newProject, taskLabel, type Project } from "./model";
import { listProjects } from "./storage";
import { TaskPanel } from "./TaskPanel";
import { validate } from "./validate";

function memoryStorage(): Storage {
  const m = new Map<string, string>();
  return {
    get length() { return m.size; }, clear: () => m.clear(), getItem: (k) => m.get(k) ?? null,
    key: (i) => [...m.keys()][i] ?? null, removeItem: (k) => { m.delete(k); }, setItem: (k, v) => { m.set(k, String(v)); },
  };
}
beforeAll(() => {
  class RO { observe() {} unobserve() {} disconnect() {} }
  vi.stubGlobal("ResizeObserver", RO);
  vi.stubGlobal("DOMMatrixReadOnly", class { m22 = 1; constructor() {} });
});
beforeEach(() => { vi.stubGlobal("localStorage", memoryStorage()); });

/** A project with the starter agent and one task step after it. */
function withTask(kind: (typeof TASK_KINDS)[number]["kind"]): { project: Project; id: string } {
  const r = addTask(newProject("x"), kind);
  r.project.workflow.steps.push({ agent: r.id });
  return r;
}
const errorsOf = (p: Project, id: string) =>
  validate(p.workflow).filter((i) => i.severity === "error" && i.where.kind === "agent" && (i.where as { id: string }).id === id);

describe("task steps", () => {
  it("makes an entry per kind that runs where it can, and says what is missing where it cannot", () => {
    const code = withTask("code");
    expect(code.project.workflow.agents[code.id]).toMatchObject({ runtime: "task", task: { kind: "code", language: "python", code: STARTER_SCRIPT } });
    expect(code.project.prompts[code.id]).toBeUndefined();
    expect(errorsOf(code.project, code.id)).toEqual([]);
    const extract = withTask("extract");
    expect(extract.project.workflow.agents[extract.id].attachments).toBe(true);
    expect(errorsOf(extract.project, extract.id)).toEqual([]);
    const tool = withTask("tool");
    expect(errorsOf(tool.project, tool.id).map((i) => i.message)).toEqual(["pick the tool this step calls"]);
    const out = withTask("output");
    expect(errorsOf(out.project, out.id).map((i) => i.message))
      .toEqual(["an output step saves a file, which needs orchestrator.files so the run keeps it"]);
    expect(taskLabel("extract")).toBe("Task · Extract");
  });

  it("adds ONE generic step from the palette with the keyboard; what it does is picked in its panel", async () => {
    vi.useFakeTimers();
    await act(async () => { render(<Builder notify={vi.fn()} />); });
    // The four "+ New step: ..." items are one now.
    expect(screen.queryByText(/^\+ New step:/)).toBeNull();
    const item = screen.getByText("+ New generic step").closest("[role=button]")!;
    await act(async () => { fireEvent.keyDown(item, { key: "Enter" }); });
    expect(screen.getAllByText("Task · step").length).toBeGreaterThan(0);
    // No kind yet: the panel says so, and there is no script to edit.
    expect(screen.getAllByText(/say what this step does: one of tool, code, extract, output/).length).toBeGreaterThan(0);
    expect(screen.queryByRole("textbox", { name: "Script" })).toBeNull();
    const field = createWrapper(document.body).findAllFormFields()
      .find((f) => f.findLabel()?.getElement().textContent === "What it does")!;
    const select = field.findControl()!.findSelect()!;
    await act(async () => { select.openDropdown(); });
    await act(async () => { select.selectOptionByValue("code"); });
    expect(screen.getAllByText("Task · Code").length).toBeGreaterThan(0);
    // Its own panel: a script, not a prompt.
    expect(screen.getByRole("textbox", { name: "Script" })).toBeTruthy();
    expect(screen.queryByText("System prompt")).toBeNull();
    await act(async () => { vi.advanceTimersByTime(900); });
    vi.useRealTimers();
    const [saved] = listProjects();
    expect(saved.workflow.agents.new_step).toMatchObject({ runtime: "task", task: { kind: "code" } });
    expect(saved.workflow.steps.map((s) => s.agent)).toEqual(["first_agent", "new_step"]);
  });
  it("keeps a generic step whose kind was never picked from deploying", () => {
    const r = addTask(newProject("x"));
    r.project.workflow.steps.push({ agent: r.id });
    expect(r.project.workflow.agents[r.id]).toMatchObject({ name: "New step", runtime: "task", task: {} });
    expect(errorsOf(r.project, r.id).map((i) => i.message))
      .toEqual(["say what this step does: one of tool, code, extract, output"]);
  });

  it("edits the task block, switches kind, and offers what an output step needs", async () => {
    const { project, id } = withTask("code");
    const onChange = vi.fn();
    const view = render(<TaskPanel project={project} id={id} issues={validate(project.workflow)} onChange={onChange} onSelect={vi.fn()} />);
    const w = createWrapper(view.container);
    await act(async () => { w.findAllTextareas()[0].setTextareaValue("print('hi')"); });
    expect(onChange.mock.lastCall![0].workflow.agents[id].task).toEqual({ kind: "code", language: "python", code: "print('hi')" });

    const select = w.findAllSelects()[1];        // Stage, then What it does
    await act(async () => { select.openDropdown(); });
    await act(async () => { select.selectOptionByValue("output"); });
    const next = onChange.mock.lastCall![0] as Project;
    expect(next.workflow.agents[id].task).toEqual({ kind: "output", format: "csv" });

    view.rerender(<TaskPanel project={next} id={id} issues={validate(next.workflow)} onChange={onChange} onSelect={vi.fn()} />);
    expect(screen.getByText(/an output step saves a file/)).toBeTruthy();
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Keep run files" })); });
    expect((onChange.mock.lastCall![0] as Project).workflow.orchestrator).toMatchObject({ files: {} });
  });

  it("names a placeholder that points at a later step", () => {
    const { project, id } = withTask("tool");
    project.workflow.tools.pricing = { type: "lambda" };
    project.workflow.agents[id].task = { kind: "tool", tool: "pricing", arguments: { q: "{{outputs.nobody.x}}" } };
    expect(errorsOf(project, id).map((i) => i.message)).toContain('{{outputs.nobody.x}}: "nobody" does not run before this step');
  });
});
