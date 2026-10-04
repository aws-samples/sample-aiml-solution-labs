/** The Builder writes workflow.json byte-for-byte the way format_workflow.py does.
 *
 *  If it did not, the first `format_workflow.py --check` after an export would fail on
 *  a file the customer never touched — the worst possible first impression of a
 *  "no-code" tool. Asserted on the shipped workflow.json, which the Python formatter
 *  already guarantees is canonical, and on a scrambled copy of it. */

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

import { formatWorkflow } from "./format";

const SHIPPED = readFileSync(resolve(process.cwd(), "../app/workflow.json"), "utf8");

describe("formatWorkflow", () => {
  it("reproduces the shipped, already-formatted workflow.json exactly", () => {
    expect(formatWorkflow(JSON.parse(SHIPPED))).toBe(SHIPPED);
  });

  it("puts a scrambled agent, tool and step back into canonical order", () => {
    const doc = JSON.parse(SHIPPED);
    const reverse = (o: Record<string, unknown>) =>
      Object.fromEntries(Object.entries(o).reverse());
    for (const id of Object.keys(doc.agents)) doc.agents[id] = reverse(doc.agents[id]);
    for (const k of Object.keys(doc.tools)) doc.tools[k] = reverse(doc.tools[k]);
    doc.steps = doc.steps.map(reverse);
    expect(formatWorkflow(doc)).toBe(SHIPPED);
  });
});
