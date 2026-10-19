import { act, render } from "@testing-library/react";
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { useState } from "react";
import { describe, expect, it } from "vitest";

import { addTool, newProject, type Project } from "./model";
import { knownTools } from "./Policies";
import { smithyOperations } from "./smithy";
import { toolRows } from "./ToolAccess";
import { iamNote, methodsFor, ToolConnect } from "./ToolConnect";
import { validate } from "./validate";

const MODEL = {
  smithy: "2.0",
  shapes: {
    "example.places#Places": { type: "service", traits: { "aws.protocols#restJson1": {} } },
    "example.places#Search": { type: "operation", input: { target: "example.places#SearchInput" } },
    "example.places#SearchInput": { type: "structure", members: { Text: {}, MaxResults: {} } },
  },
};

describe("type=smithy in the Build view", () => {
  it("is a new tool with an empty grant, which the checks call unfinished", () => {
    const { project, key } = addTool(newProject("x"), "Places", "smithy");
    const tool = project.workflow.tools[key];
    expect(tool).toMatchObject({ type: "smithy", schemaS3Uri: "s3://", allow: { actions: [], resources: [] } });
    const errs = validate(project.workflow).filter((i) => i.severity === "error" && i.path.startsWith(`tools.${key}`)).map((i) => i.path);
    expect(errs).toEqual([`tools.${key}.schemaS3Uri`, `tools.${key}.allow.actions`, `tools.${key}.allow.resources`]);
    expect(toolRows(project, null).find((r) => r.key === key)).toMatchObject({ status: "setup", connects: "IAM role" });
  });

  it("connects only as the Gateway role, and names its operations for policies", () => {
    expect(methodsFor("smithy")).toEqual([]);
    expect(iamNote({ type: "smithy" }, "ax_1")).toBe(
      "Signed by this build's Gateway role, AgentCoreGateway-ax_1, which may make only the AWS calls listed below, on those resources.");
    expect(smithyOperations(MODEL)).toEqual([{ name: "Search", args: ["MaxResults", "Text"] }]);
    expect(knownTools({ type: "smithy", schema: MODEL })).toEqual([{ name: "Search", args: ["MaxResults", "Text"] }]);
  });

  it("writes the actions and resources it may call, one per line", async () => {
    const p = newProject("x");
    p.workflow.tools.places = { type: "smithy", description: "Places", schema: MODEL, allow: { actions: [], resources: [] } };
    let seen: Project = p;
    function Spy() {
      const [cur, setCur] = useState(p);
      seen = cur;
      return <ToolConnect project={cur} id="places" server={false} onChange={setCur} agentName="ax_1" />;
    }
    await act(async () => { render(<Spy />); });
    const area = (l: string) => createWrapper(document.body).findAllTextareas()
      .find((a) => a.findNativeTextarea().getElement().getAttribute("aria-label") === l)!;
    await act(async () => { area("AWS actions it may call").setTextareaValue("geo:SearchPlaceIndexForText\n"); });
    await act(async () => { area("On these resources").setTextareaValue("arn:aws:geo:us-east-1:111122223333:place-index/stores"); });
    expect(seen.workflow.tools.places.allow).toEqual({
      actions: ["geo:SearchPlaceIndexForText"], resources: ["arn:aws:geo:us-east-1:111122223333:place-index/stores"] });
    // The new, empty line is kept as typed, so a second action can be added.
    expect(area("AWS actions it may call").findNativeTextarea().getElement().value).toBe("geo:SearchPlaceIndexForText\n");
    expect(validate(seen.workflow).filter((i) => i.path.startsWith("tools.places"))).toEqual([]);
  });
});
