import { describe, expect, it } from "vitest";
import { FORM_LAYOUT, splitLayout } from "./formLayout";

describe("form layout", () => {
  it("shows the main keys first, groups the rest, and never drops a key", () => {
    const got = splitLayout(FORM_LAYOUT.memory!, ["description", "strategies", "expiryDays", "scope", "brandNew"]);
    expect(got.main).toEqual(["strategies"]);
    expect(got.advanced.map((a) => a.title)).toEqual(["Retention", "Sharing", "About", "Other"]);
    expect(got.advanced.at(-1)!.keys).toEqual(["brandNew"]);
    // Only what applies is shown: a section with nothing in it is left out.
    expect(splitLayout(FORM_LAYOUT.memory!, ["strategies"]).advanced).toEqual([]);
  });
});
