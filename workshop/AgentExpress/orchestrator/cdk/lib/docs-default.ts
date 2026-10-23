/**
 * The documentation a deployment links to when it sets no `-c docsUrl`: the public user
 * guide on GitHub, pinned to the release tag. One file, orchestrator/docs_default.json,
 * holds the tag (`ref`, which the release process bumps); Terraform reads the same file
 * (local.docs_default in terraform/builder.tf), so both paths deploy the same default.
 */
import * as fs from "fs";
import * as path from "path";

const DOCS_DEFAULT_PATH = path.join(__dirname, "..", "..", "docs_default.json");

interface DocsDefault {
  ref: string;
  urlTemplate: string;
}

const d = JSON.parse(fs.readFileSync(DOCS_DEFAULT_PATH, "utf8")) as DocsDefault;

/** The release tag the default user guide link is pinned to. */
export const DOCS_REF: string = d.ref;

/** The default docsUrl: urlTemplate with {ref} replaced by the release tag. */
export const DEFAULT_DOCS_URL: string = d.urlTemplate.replace("{ref}", d.ref);
