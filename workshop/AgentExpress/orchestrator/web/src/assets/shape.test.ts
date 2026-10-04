/** The shape-driven renderer, asserted against a contract the framework has never seen.
 *
 *  This is the UI half of the framework's central promise: a customer defines their own
 *  asset type, with their own field names, and it still gets first-class layout. The
 *  fixture is deliberately a claims-settlement asset, not this sample's research
 *  finding, because a test written against the shipped contract would pass even if the
 *  renderer hardcoded its field names.
 *
 *  Replaces tests/test_asset_rendering.py, which sliced functions out of the old
 *  single-file page with a regex and ran them under node. */

import { describe, expect, it } from "vitest";

import {
  claimParts, gistOf, humanizeKey, isClaimList, isVocabToken, looksLikeMarkdown, parseAsset, readAsset, safeUrl,
  sectionsOf,
} from "./shape";

/** A settlement decision. Nothing here is a name the framework knows. */
const FOREIGN = JSON.stringify({
  assetId: "asset-settlement-88213-v1",
  assetType: "settlement-decision",
  version: 2,
  status: "in-review",
  createdAt: "2026-09-20 14:32:44",
  createdByAgent: "adjust",
  sourceAssetIds: ["asset-fnol-88213-v1"],
  executiveSummary: "Claim 88213 is payable in part; storm damage is covered.",
  disposition: "partially-approved",
  settlementGbp: 4820.5,
  deductions: [{ reason: "Policy excess", amountGbp: 250 }],
  determinations: [
    {
      finding: "The escape of water is covered under section 4.",
      basis: "policy-wording",
      confidence: "high",
      tracedToAssetIds: ["asset-fnol-88213-v1"],
    },
  ],
  unresolvedQueries: ["The loss adjuster's site photographs were not supplied."],
});

describe("an asset the framework has never seen", () => {
  const asset = parseAsset(FOREIGN)!;

  it("parses", () => {
    expect(asset).not.toBeNull();
    expect(asset.assetType).toBe("settlement-decision");
  });

  it("finds the gist without knowing the field name", () => {
    expect(gistOf(asset)).toContain("payable in part");
  });

  it("drops the framework's envelope from the body", () => {
    const keys = sectionsOf(asset).map((s) => s.key);
    for (const bookkeeping of
      ["assetId", "assetType", "version", "status", "createdAt", "createdByAgent",
       "sourceAssetIds", "executiveSummary"]) {
      expect(keys).not.toContain(bookkeeping);
    }
  });

  it("keeps every field that is the customer's", () => {
    const keys = sectionsOf(asset).map((s) => s.key);
    expect(keys).toContain("disposition");
    expect(keys).toContain("settlementGbp");
    expect(keys).toContain("deductions");
    expect(keys).toContain("determinations");
  });

  it("recognises a claim list by SHAPE, under a name it has never seen", () => {
    const determinations = sectionsOf(asset).find((s) => s.key === "determinations");
    expect(determinations?.claims).toBe(true);
    // `deductions` has no text+meta pair, so it is not a claim list.
    expect(sectionsOf(asset).find((s) => s.key === "deductions")?.claims).toBe(false);
  });

  it("puts what the run could not settle last, by generic English", () => {
    const sections = sectionsOf(asset);
    const last = sections[sections.length - 1];
    expect(last.key).toBe("unresolvedQueries");
    expect(last.uncertain).toBe(true);
  });

  it("splits a claim into text, badges and provenance", () => {
    const parts = claimParts(
      (asset.determinations as Array<Record<string, never>>)[0]);
    expect(parts.text).toContain("escape of water");
    expect(parts.badges.map((b) => b.value)).toEqual(
      expect.arrayContaining(["policy-wording", "high"]));
    expect(parts.refs).toEqual(["asset-fnol-88213-v1"]);
  });
});

describe("the decisions that keep it readable", () => {
  it("labels an unknown key without a lookup table", () => {
    expect(humanizeKey("settlementGbp")).toBe("Settlement gbp");
    expect(humanizeKey("unresolved_queries")).toBe("Unresolved queries");
  });

  it("treats a short lower-case token as a badge and a sentence as prose", () => {
    expect(isVocabToken("sourced-fact")).toBe(true);
    expect(isVocabToken("high")).toBe(true);
    expect(isVocabToken("The escape of water is covered")).toBe(false);
    expect(isVocabToken("Policy excess")).toBe(false);
  });

  it("only makes an http(s) URL a link", () => {
    expect(safeUrl("https://aws.amazon.com/lambda/")).toBe("https://aws.amazon.com/lambda/");
    expect(safeUrl("http://example.com")).toBe("http://example.com");
    // A model that invents a javascript: URL must not get a live link.
    expect(safeUrl("javascript:alert(1)")).toBeNull();
    expect(safeUrl("data:text/html,<script>")).toBeNull();
    expect(safeUrl("not a url")).toBeNull();
  });

  it("treats a plain-text agent's output as prose, not as a broken asset", () => {
    expect(parseAsset("An outside reviewer's opinion, in sentences.")).toBeNull();
    expect(parseAsset("")).toBeNull();
    expect(parseAsset("[1, 2, 3]")).toBeNull();   // valid JSON, not an object
  });

  it("reads a fenced object, and closes up one cut off at the token limit", () => {
    expect(readAsset('```json\n{"a": 1}\n```')).toEqual({ asset: { a: 1 }, cutOff: false });
    expect(readAsset('Here it is:\n{"a": 1}')).toEqual({ asset: { a: 1 }, cutOff: false });
    const cut = readAsset('```json\n{\n  "summary": "Launch is blocked",\n  "risks": [\n    {"risk": "Discount", "mitigation": "Secure inventory commit');
    expect(cut.cutOff).toBe(true);
    expect(cut.asset?.summary).toBe("Launch is blocked");
    expect((cut.asset?.risks as unknown[]).length).toBe(1);
    expect(readAsset("plain prose").asset).toBeNull();
    // A tool call written out before the real asset: the last object is the answer.
    const two = readAsset('Calling it:\n{"text": "draft", "keywords": ["a"]}\nFinal:\n```json\n{"headline": "H", "body": "B"}\n```');
    expect(two).toEqual({ asset: { headline: "H", body: "B" }, cutOff: false });
  });

  it("renders a drafted post written in Markdown, and leaves plain text alone", () => {
    expect(looksLikeMarkdown("## Introduction\n\nIf you own a shop...")).toBe(true);
    expect(looksLikeMarkdown("Intro\n- one\n- two")).toBe(true);
    expect(looksLikeMarkdown("A line\nwith **bold** in it")).toBe(true);
    expect(looksLikeMarkdown("## not multi-line")).toBe(false);
    expect(looksLikeMarkdown("Two plain\nlines")).toBe(false);
  });

  it("needs both text and provenance before calling something a claim list", () => {
    expect(isClaimList([{ statement: "x", confidence: "high" }])).toBe(true);
    expect(isClaimList([{ statement: "x" }])).toBe(false);       // no provenance
    expect(isClaimList([{ confidence: "high" }])).toBe(false);    // no text
    expect(isClaimList([])).toBe(false);
    expect(isClaimList(["a string"])).toBe(false);
  });
});
