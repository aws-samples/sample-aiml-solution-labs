import { describe, expect, it } from "vitest";

import { newProject, renameProject, setUi } from "./model";

const fresh = () => newProject("Trip Planner");

describe("the build's name, ui.title and ui.heading stay in step", () => {
  it("renaming the build carries the title and heading that still showed the old name", () => {
    const p = renameProject(fresh(), "Travel Brief");
    expect(p.name).toBe("Travel Brief");
    expect(p.workflow.ui).toMatchObject({ title: "Travel Brief", heading: "Travel Brief" });
  });

  it("a heading the user set to something of its own is left alone", () => {
    const custom = setUi(fresh(), { ...(fresh().workflow.ui as object), heading: "Acme Travel" } as never);
    expect(custom.name).toBe("Trip Planner");          // editing the heading renames nothing
    const p = renameProject(custom, "Travel Brief");
    expect(p.workflow.ui).toMatchObject({ title: "Travel Brief", heading: "Acme Travel" });
  });

  it("editing the title in Settings renames the build, and the heading follows", () => {
    const p = fresh();
    const q = setUi(p, { ...(p.workflow.ui as object), title: "Travel Brief" } as never);
    expect(q.name).toBe("Travel Brief");
    expect(q.workflow.ui).toMatchObject({ title: "Travel Brief", heading: "Travel Brief" });
    // Typing on keeps them together.
    const r = setUi(q, { ...(q.workflow.ui as object), title: "Travel Briefs" } as never);
    expect(r.name).toBe("Travel Briefs");
    expect(r.workflow.ui).toMatchObject({ heading: "Travel Briefs" });
  });

  it("clearing the title renames nothing", () => {
    const p = fresh();
    const q = setUi(p, { ...(p.workflow.ui as object), title: "" } as never);
    expect(q.name).toBe("Trip Planner");
    expect(q.workflow.ui).toMatchObject({ heading: "Trip Planner" });
  });

  it("another ui field changes nothing else", () => {
    const p = fresh();
    const q = setUi(p, { ...(p.workflow.ui as object), topicPlaceholder: "Where to?" } as never);
    expect(q.name).toBe("Trip Planner");
  });
});
