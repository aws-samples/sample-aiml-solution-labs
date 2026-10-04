/** Rebuilding the canvas nodes keeps each one's measured size, so React Flow keeps
 *  drawing them (a node with no measured size is invisible until it is resized). */
import { describe, expect, it } from "vitest";
import type { Node } from "@xyflow/react";

import { keepMeasured } from "./Canvas";

const node = (id: string, extra: Partial<Node> = {}): Node => ({ id, position: { x: 0, y: 0 }, data: {}, ...extra });

describe("keepMeasured", () => {
  it("carries each node's measured size onto its rebuilt copy", () => {
    const prev = [node("a", { measured: { width: 200, height: 60 } }), node("b", { measured: { width: 200, height: 60 } })];
    const next = [node("a", { data: { selected: true } }), node("b"), node("c")];
    const out = keepMeasured(prev, next);
    expect(out.map((n) => n.measured)).toEqual([{ width: 200, height: 60 }, { width: 200, height: 60 }, undefined]);
    expect(out[0].data).toEqual({ selected: true });
  });

  it("leaves a node React Flow has not measured yet alone", () => {
    expect(keepMeasured([node("a")], [node("a")])[0].measured).toBeUndefined();
  });
});
