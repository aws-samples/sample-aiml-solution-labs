/** The run graph: each agent box lists its own tools and AgentCore features as clickable
 *  rows, each saying what it did in this run (lib/runComponents.ts). */
import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { buildEvents } from "../lib/flow";
import { outcomeOf } from "../lib/runComponents";
import type { Workflow } from "../types";
import { Graph } from "./Graph";

const workflow = {
  agents: {
    writer: {
      name: "Writer", tool: ["lookup"], components: [
        { kind: "tool", name: "lookup", tool: "lookup", detail: "MCP server" },
        { kind: "guardrail", name: "strict", detail: "checks input and output" },
        { kind: "memory", name: "notes", detail: "semantic" },
        { kind: "policy", name: "readOnly", tool: "lookup", detail: "on lookup" },
        { kind: "skill", name: "triage" },
      ],
    },
    plain: { name: "Plain", components: [] },
  },
  steps: [{ agent: "writer" }, { agent: "plain" }],
} as unknown as Workflow;

const events = buildEvents([
  { agent_id: "writer", kind: "tool", label: "lookup___search", status: "ok", ts: "1", mode: "gateway" },
  { agent_id: "writer", kind: "tool", label: "lookup___get", status: "ok", ts: "2", mode: "gateway" },
  { agent_id: "writer", kind: "policy", label: "lookup___delete", status: "denied", ts: "3" },
  { agent_id: "writer", kind: "guardrail", label: "INPUT", status: "passed", ts: "4" },
  { agent_id: "writer", kind: "memory", label: "recall", status: "ok", ts: "5" },
  { agent_id: "plain", kind: "tool", label: "lookup___search", status: "ok", ts: "6" },
]);

describe("the selected agent's components", () => {
  it("are rows inside each agent's own box, without selecting it", () => {
    render(<Graph workflow={workflow} nodes={{}} selected={null} onSelect={() => {}} events={events} />);
    expect(screen.getByTestId("rows-writer").querySelectorAll("li[data-kind]")).toHaveLength(5);
    expect(screen.queryByTestId("rows-plain")).toBeNull();
  });

  it("each say what it did in this run, counting only that agent's own events", () => {
    render(<Graph workflow={workflow} nodes={{}} selected="writer" onSelect={() => {}} events={events} />);
    const box = (k: string, n: string) => screen.getByTestId(`run-comp-writer-${k}-${n}`);
    expect(box("tool", "lookup").textContent).toContain("2 calls");
    expect(box("policy", "readOnly").textContent).toContain("1 decision · 1 denied");
    expect(box("guardrail", "strict").textContent).toContain("1 check");
    expect(box("memory", "notes").textContent).toContain("recall");
    expect(box("skill", "triage").textContent).toContain("available to the agent");
    // Plain's own tool call is not counted on Writer's box.
    fireEvent.click(box("tool", "lookup").querySelector("button")!);
    expect(box("tool", "lookup").querySelectorAll(".sfn-row-events li")).toHaveLength(2);
  });

  it("say when a component was not used", () => {
    expect(outcomeOf({ kind: "tool", name: "x", tool: "x" }, "writer", events).line).toBe("not used in this run");
    expect(outcomeOf({ kind: "tool", name: "x", tool: "x" }, "writer", null).line).toBe("");
  });
});
