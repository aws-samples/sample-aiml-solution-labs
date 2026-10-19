import { act, render } from "@testing-library/react";
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { useState } from "react";
import { describe, expect, it } from "vitest";
import type { Json } from "./model";
import { ChunkingField, ParsingField } from "./KbIngestionFields";
import { checkKbIngestion } from "./kbIngestion";
import type { Issue } from "./validate";

function mount(Field: typeof ChunkingField, start?: Json) {
  const seen: { v: Json | undefined } = { v: start };
  function Spy() {
    const [cur, setCur] = useState<Json | undefined>(start);
    seen.v = cur;
    return <Field value={cur} onChange={setCur} />;
  }
  render(<Spy />);
  return seen;
}

const issues = (tool: Record<string, unknown>) => {
  const out: Issue[] = [];
  checkKbIngestion(tool, "tools.kb", { kind: "tool", id: "kb" }, (severity, where, path, message) =>
    out.push({ severity, where, path, message }));
  return out.filter((i) => i.severity === "error");
};

describe("a Knowledge Base's document processing in the Build view", () => {
  it("picking a split fills valid starting values, and the default removes the key", async () => {
    let seen!: { v: Json | undefined };
    await act(async () => { seen = mount(ChunkingField); });
    const select = createWrapper(document.body).findSelect()!;
    await act(async () => { select.openDropdown(); });
    await act(async () => { select.selectOptionByValue("hierarchical"); });
    expect(seen.v).toEqual({ strategy: "hierarchical", parentMaxTokens: 1500, childMaxTokens: 300, overlapTokens: 60 });
    expect(issues({ chunking: seen.v })).toEqual([]);
    const input = (l: string) => createWrapper(document.body).findAllInputs()
      .find((i) => i.findNativeInput().getElement().getAttribute("aria-label") === l)!;
    await act(async () => { input("Most tokens in a small chunk").setInputValue("200"); });
    expect(seen.v).toMatchObject({ childMaxTokens: 200 });
    await act(async () => { select.openDropdown(); });
    await act(async () => { select.selectOptionByValue(""); });
    expect(seen.v).toBeUndefined();
  });

  it("every starting value passes the checks", async () => {
    for (const s of ["fixedSize", "semantic", "none"]) {
      document.body.innerHTML = "";
      let seen!: { v: Json | undefined };
      await act(async () => { seen = mount(ChunkingField); });
      const select = createWrapper(document.body).findSelect()!;
      await act(async () => { select.openDropdown(); });
      await act(async () => { select.selectOptionByValue(s); });
      expect(issues({ chunking: seen.v })).toEqual([]);
    }
  });

  it("parsing with a model asks for the model, and writes it", async () => {
    let seen!: { v: Json | undefined };
    await act(async () => { seen = mount(ParsingField); });
    const select = createWrapper(document.body).findSelect()!;
    await act(async () => { select.openDropdown(); });
    await act(async () => { select.selectOptionByValue("foundationModel"); });
    expect(issues({ parsing: seen.v }).map((i) => i.path)).toEqual(["tools.kb.parsing.model"]);
    const model = createWrapper(document.body).findAllInputs()
      .find((i) => i.findNativeInput().getElement().getAttribute("aria-label") === "Model")!;
    await act(async () => { model.setInputValue("us.amazon.nova-pro-v1:0"); });
    expect(seen.v).toEqual({ strategy: "foundationModel", model: "us.amazon.nova-pro-v1:0" });
    expect(issues({ parsing: seen.v })).toEqual([]);
  });
});
