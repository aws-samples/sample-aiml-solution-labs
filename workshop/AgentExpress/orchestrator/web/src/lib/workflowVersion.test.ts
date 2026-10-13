import { describe, expect, it } from "vitest";
import { workflowVersionLabel } from "./workflowVersion";

describe("the workflow version label", () => {
  it("names a known version, and says so when it is not known", () => {
    expect(workflowVersionLabel(2)).toBe("v2");
    expect(workflowVersionLabel("3")).toBe("v3");
    for (const v of [undefined, null, "", 0, "x", 1.5]) expect(workflowVersionLabel(v as never)).toBe("—");
  });
});
