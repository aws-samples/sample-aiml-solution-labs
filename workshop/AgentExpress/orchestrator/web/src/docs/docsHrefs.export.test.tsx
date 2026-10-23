/** Not a check of its own: scripts/check_docs_links.py runs this to read every info
 *  popover's Documentation link the way a person would (open the popover, read the link),
 *  then loads each one on the live documentation site. Skipped unless DOCS_HREFS_OUT names
 *  the JSON file to write ({featureId: href}); DOCS_HREFS_BASE is the base URL to render with. */
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { writeFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

import { DOCS, DocsContext, docsBase, type DocsFeature } from "./docsLinks";
import { InfoLink } from "./InfoLink";

const out = process.env.DOCS_HREFS_OUT;

describe.skipIf(!out)("every info popover's Documentation link", () => {
  it("is written out for the live link check", async () => {
    const base = docsBase({ docsUrl: process.env.DOCS_HREFS_BASE });
    expect(base, "DOCS_HREFS_BASE must be an https URL").toBeTruthy();
    const hrefs: Record<string, string> = {};
    for (const feature of Object.keys(DOCS) as DocsFeature[]) {
      render(<DocsContext.Provider value={base!}><InfoLink feature={feature} /></DocsContext.Provider>);
      await act(async () => { fireEvent.click(screen.getByRole("button", { name: `About ${DOCS[feature].title}` })); });
      const link = screen.getByRole("link", { name: /^Documentation: / });
      expect(link.getAttribute("target")).toBe("_blank");
      hrefs[feature] = link.getAttribute("href") ?? "";
      cleanup();
    }
    writeFileSync(out!, JSON.stringify(hrefs, null, 2));
  });
});
