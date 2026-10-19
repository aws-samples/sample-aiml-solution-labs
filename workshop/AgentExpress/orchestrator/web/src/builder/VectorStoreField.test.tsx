import { act, render } from "@testing-library/react";
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { useState } from "react";
import { describe, expect, it } from "vitest";
import type { Json } from "./model";
import { vocab } from "./meta";
import { STORES, VectorStoreField } from "./VectorStoreField";
import { checkKbVectorStore } from "./kbVectorStore";
import type { Issue } from "./validate";

const errors = (vectorStore: unknown) => {
  const out: Issue[] = [];
  checkKbVectorStore({ vectorStore }, "tools.kb", { kind: "tool", id: "kb" }, (severity, where, path, message) =>
    out.push({ severity, where, path, message }));
  return out.filter((i) => i.severity === "error").map((i) => i.path);
};

describe("a Knowledge Base's vector store in the Build view", () => {
  it("offers every store the vocabulary has, and the default", () => {
    expect(STORES.map((s) => s.value)).toEqual(["", ...vocab("kbVectorStores")]);
  });

  it("asks for the chosen store's settings, writes them, and the default removes the key", async () => {
    const seen: { v: Json | undefined } = { v: undefined };
    function Spy() {
      const [cur, setCur] = useState<Json | undefined>(undefined);
      seen.v = cur;
      return <VectorStoreField value={cur} onChange={setCur} />;
    }
    await act(async () => { render(<Spy />); });
    const select = createWrapper(document.body).findSelect()!;
    await act(async () => { select.openDropdown(); });
    await act(async () => { select.selectOptionByValue("opensearchServerless"); });
    expect(seen.v).toEqual({ type: "opensearchServerless" });
    expect(errors(seen.v)).toEqual(["collectionArn", "indexName", "vectorField", "textField", "metadataField"]
      .map((k) => `tools.kb.vectorStore.${k}`));
    const input = (l: string) => createWrapper(document.body).findAllInputs()
      .find((i) => i.findNativeInput().getElement().getAttribute("aria-label") === l)!;
    const fill: Record<string, string> = {
      "Collection ARN": "arn:aws:aoss:us-east-1:123456789012:collection/abc123", "Vector index name": "kb",
      "Vector field": "vec", "Text field": "text", "Metadata field": "meta",
    };
    for (const [l, v] of Object.entries(fill)) await act(async () => { input(l).setInputValue(v); });
    expect(errors(seen.v)).toEqual([]);
    await act(async () => { select.openDropdown(); });
    await act(async () => { select.selectOptionByValue(""); });
    expect(seen.v).toBeUndefined();
  });
});
