/** A task step (runtime "task") on the run graph: its box says what kind of step it is,
 *  from the label the BFF projects (bff/workflow.py data_source). */
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import type { Workflow } from "../types";
import { Graph } from "./Graph";

describe("a task step on the run graph", () => {
  it("shows its kind and the task badge, and no rows", () => {
    const workflow = {
      agents: {
        intake: { name: "Intake", runtime: "main", components: [] },
        crunch: { name: "Crunch numbers", runtime: "task", task: "code", source: "Task · Code", components: [] },
      },
      steps: [{ agent: "intake" }, { agent: "crunch" }],
    } as unknown as Workflow;
    render(<Graph workflow={workflow} nodes={{}} selected={null} onSelect={() => {}} events={[]} />);
    expect(screen.getByText("Task · Code")).toBeTruthy();
    expect(screen.getByText("task")).toBeTruthy();
    expect(screen.queryByTestId("rows-crunch")).toBeNull();
  });
});
