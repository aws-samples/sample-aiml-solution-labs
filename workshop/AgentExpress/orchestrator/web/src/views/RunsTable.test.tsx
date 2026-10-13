import { render } from "@testing-library/react";
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { describe, expect, it } from "vitest";
import { RunsTable } from "./RunsTable";

describe("the Runs table", () => {
  it("shows the workflow version each run ran on, and — for runs that predate it", () => {
    render(<RunsTable loading={false} can={() => true} onOpen={() => {}} onDelete={() => {}} onStartNew={() => {}}
      runs={[
        { session_id: "new1", topic: "after version 2", overall: "done", created: "2026-10-13T10:00:00Z", workflowVersion: 2 },
        { session_id: "old1", topic: "before versions", overall: "done", created: "2026-10-12T10:00:00Z" },
      ]} />);
    const table = createWrapper(document.body).findTable()!;
    const headers = table.findColumnHeaders().map((h) => h.getElement().textContent);
    const col = headers.findIndex((h) => h?.startsWith("Workflow")) + 1;
    expect(col).toBeGreaterThan(0);
    const cell = (row: number) => table.findBodyCell(row, col)!.getElement().textContent;
    expect([cell(1), cell(2)]).toEqual(["v2", "—"]);
  });
});
