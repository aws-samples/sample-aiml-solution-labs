/** v139 Track C deliver: a deliver step (task.kind "deliver") in the Builder — picked in a
 *  generic step's panel, its targets edited there, checked as you type (taskCheck.ts). */
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { act, fireEvent, render, screen } from "@testing-library/react";
import { beforeAll, describe, expect, it, vi } from "vitest";
import { TASK_KINDS, addTask, newProject, taskLabel, type Entry, type Project } from "./model";
import { TaskPanel } from "./TaskPanel";
import { validate } from "./validate";

beforeAll(() => {
  vi.stubGlobal("ResizeObserver", class { observe() {} unobserve() {} disconnect() {} });
});

function deliverStep(files = true): { project: Project; id: string } {
  const r = addTask(newProject("x"), "deliver");
  r.project.workflow.steps.push({ agent: r.id });
  if (files) r.project.workflow.orchestrator = { ...((r.project.workflow.orchestrator ?? {}) as Entry), files: {} };
  return r;
}
const errorsOf = (p: Project, id: string) =>
  validate(p.workflow).filter((i) => i.severity === "error" && i.where.kind === "agent" && (i.where as { id: string }).id === id);
const targetsOf = (p: Project, id: string) => (p.workflow.agents[id].task as Entry).targets as Entry[];

describe("deliver steps", () => {
  it("is a generic step kind, starting with one S3 target whose bucket is still to be written", () => {
    expect(TASK_KINDS.map((k) => k.kind)).toContain("deliver");
    expect(taskLabel("deliver")).toBe("Task · Deliver");
    const { project, id } = deliverStep(false);
    expect(project.workflow.agents[id]).toMatchObject({ name: "Deliver files", runtime: "task", task: { kind: "deliver", targets: [{ type: "s3" }] } });
    expect(errorsOf(project, id).map((i) => i.message)).toEqual([
      "a deliver step sends the run's files, which needs orchestrator.files so the run keeps them",
      "write the bucket and prefix: s3://<bucket>/<prefix>/ or ${storage}/<prefix>/",
    ]);
  });

  it("edits a target, adds and removes targets, and shows each type's own fields", async () => {
    const { project, id } = deliverStep();
    const onChange = vi.fn();
    const view = render(<TaskPanel project={project} id={id} issues={validate(project.workflow)} onChange={onChange} onSelect={vi.fn()} />);
    expect(screen.getByText("Target 1 — Amazon S3")).toBeTruthy();
    expect(screen.getAllByText(/write the bucket and prefix/).length).toBeGreaterThan(0);
    const w = createWrapper(view.container);
    const field = (label: string) => w.findAllFormFields().find((f) => f.findLabel()?.getElement().textContent === label)!;
    await act(async () => { field("Bucket and prefix").findControl()!.findInput()!.setInputValue("s3://amzn-s3-demo-bucket/reports/"); });
    let next = onChange.mock.lastCall![0] as Project;
    expect(targetsOf(next, id)).toEqual([{ type: "s3", uri: "s3://amzn-s3-demo-bucket/reports/" }]);
    expect(errorsOf(next, id)).toEqual([]);

    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Add a target" })); });
    next = onChange.mock.lastCall![0] as Project;
    expect(targetsOf(next, id)).toHaveLength(2);

    // Switch the second target to email: its own fields, and what is missing.
    next.workflow.agents[id].task = { kind: "deliver", targets: [{ type: "s3", uri: "s3://amzn-s3-demo-bucket/reports/" }, { type: "email" }] };
    view.rerender(<TaskPanel project={next} id={id} issues={validate(next.workflow)} onChange={onChange} onSelect={vi.fn()} />);
    expect(screen.getByText("Target 2 — Email (Amazon SES)")).toBeTruthy();
    expect(field("SES identity ARN")).toBeTruthy();
    expect(screen.getAllByText(/write at least one recipient/).length).toBeGreaterThan(0);
    await act(async () => { field("To").findControl()!.findInput()!.setInputValue("team@example.com, lead@example.com"); });
    expect(targetsOf(onChange.mock.lastCall![0] as Project, id)[1]).toMatchObject({ type: "email", to: ["team@example.com", "lead@example.com"] });

    // A webhook target asks for an https URL; Remove takes a target off.
    next.workflow.agents[id].task = { kind: "deliver", targets: [{ type: "webhook", url: "http://hooks.example.com" }] };
    view.rerender(<TaskPanel project={next} id={id} issues={validate(next.workflow)} onChange={onChange} onSelect={vi.fn()} />);
    expect(screen.getAllByText(/must be an https:\/\/ URL/).length).toBeGreaterThan(0);
    expect(field("Signing secret name")).toBeTruthy();
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Remove" })); });
    expect(targetsOf(onChange.mock.lastCall![0] as Project, id)).toEqual([]);
  });

  it("sets a condition on a target from its field, with equals true to start", async () => {
    const { project, id } = deliverStep();
    project.workflow.agents.check = { name: "Check", runtime: "task", task: { kind: "code", code: "print(1)" } };
    project.workflow.steps.splice(project.workflow.steps.length - 1, 0, { agent: "check" });
    const onChange = vi.fn();
    const view = render(<TaskPanel project={project} id={id} issues={validate(project.workflow)} onChange={onChange} onSelect={vi.fn()} />);
    const w = createWrapper(view.container);
    const input = w.findAllInputs().find((i) => i.findNativeInput().getElement().getAttribute("aria-label") === "Condition field")!;
    await act(async () => { input.setInputValue("outputs.check.passed"); });
    const t = targetsOf(onChange.mock.lastCall![0] as Project, id)[0];
    expect(t.when).toEqual({ field: "outputs.check.passed", equals: true });
  });

  it("offers to keep run files when the build does not", async () => {
    const { project, id } = deliverStep(false);
    const onChange = vi.fn();
    render(<TaskPanel project={project} id={id} issues={validate(project.workflow)} onChange={onChange} onSelect={vi.fn()} />);
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Keep run files" })); });
    expect((onChange.mock.lastCall![0] as Project).workflow.orchestrator).toMatchObject({ files: {} });
  });
});
