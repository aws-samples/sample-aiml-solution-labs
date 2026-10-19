import { act, render } from "@testing-library/react";
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { useState } from "react";
import { describe, expect, it } from "vitest";
import type { Json } from "./model";
import { vocab } from "./meta";
import { CONNECTORS, DataSourceField } from "./DataSourceField";
import { checkKbDataSource } from "./kbDataSource";
import type { Issue } from "./validate";

const OSS = { type: "opensearchServerless" };
const errors = (dataSource: unknown, vectorStore: unknown = OSS) => {
  const out: Issue[] = [];
  checkKbDataSource({ dataSource, vectorStore }, "tools.kb", { kind: "tool", id: "kb" }, (severity, where, path, message) =>
    out.push({ severity, where, path, message }));
  return out.filter((i) => i.severity === "error").map((i) => i.path);
};

function mount(vectorStore?: Json) {
  const seen: { v: Json | undefined } = { v: undefined };
  function Spy() {
    const [cur, setCur] = useState<Json | undefined>(undefined);
    seen.v = cur;
    return <DataSourceField value={cur} onChange={setCur} vectorStore={vectorStore} />;
  }
  render(<Spy />);
  return seen;
}

describe("a Knowledge Base's documents from a connector, in the Build view", () => {
  it("offers every connector the vocabulary has, and files", () => {
    expect(CONNECTORS.map((c) => c.value)).toEqual(["", ...vocab("kbDataSources")]);
  });

  it("a web crawl: start URLs one per line, numbers as numbers, the default removes the key", async () => {
    let seen!: { v: Json | undefined };
    await act(async () => { seen = mount(OSS); });
    const select = () => createWrapper(document.body).findSelect()!;
    await act(async () => { select().openDropdown(); });
    await act(async () => { select().selectOptionByValue("web"); });
    expect(errors(seen.v)).toEqual(["tools.kb.dataSource.seedUrls"]);
    const area = (l: string) => createWrapper(document.body).findAllTextareas()
      .find((a) => a.findNativeTextarea().getElement().getAttribute("aria-label") === l)!;
    const input = (l: string) => createWrapper(document.body).findAllInputs()
      .find((i) => i.findNativeInput().getElement().getAttribute("aria-label") === l)!;
    await act(async () => { area("Start URLs").setTextareaValue("https://docs.example.com/\n https://help.example.com/ "); });
    await act(async () => { input("Most pages").setInputValue("500"); });
    expect(seen.v).toEqual({ type: "web", seedUrls: ["https://docs.example.com/", "https://help.example.com/"], maxPages: 500 });
    expect(errors(seen.v)).toEqual([]);
    expect(createWrapper(document.body).findAlert()).toBeNull();
    await act(async () => { select().openDropdown(); });
    await act(async () => { select().selectOptionByValue(""); });
    expect(seen.v).toBeUndefined();
  });

  it("warns, and the checks refuse, a connector without OpenSearch Serverless", async () => {
    let seen!: { v: Json | undefined };
    await act(async () => { seen = mount(undefined); });
    const select = createWrapper(document.body).findSelect()!;
    await act(async () => { select.openDropdown(); });
    await act(async () => { select.selectOptionByValue("confluence"); });
    expect(createWrapper(document.body).findAlert()!.getElement().textContent).toContain("OpenSearch Serverless");
    expect(errors(seen.v, null)).toContain("tools.kb.dataSource.type");
  });
});
