/**
 * Every workflow.json key's DEFAULT, read from app/defaults.json.
 *
 * WHY THIS READS A FILE instead of declaring the values here — the same argument as
 * lib/vocabulary.ts, and it had already gone wrong the same way. A default is a decision
 * about what an omitted key means, and it was written out once per plane: `runtime: "main"`
 * appeared FIFTEEN times across Python, HCL and TypeScript, `arg: "query"` three times,
 * `corpusKey: "doc_type"` three times. Worse, `maxResults` carried BOTH 5 and 10 inside
 * terraform/tools.tf, so that file disagreed with itself about its own default.
 *
 * A drifted default is quieter than a drifted allow-list. A missing VALUE is rejected at
 * plan or synth with a message; a default that differs by plane deploys cleanly on both and
 * behaves differently, which is indistinguishable from the feature working.
 *
 * The values are authored once in app/keys.json — beside the key's documentation and its
 * allowed values — and `build_schema.py` projects them into app/defaults.json, which every
 * plane reads: this module, app/common/defaults.py, and `local.key_defaults` in terraform/.
 * keys.json is 40 KB of prose and deliberately does not ship in the container image, which
 * is why the generated projection exists.
 */
import * as fs from "fs";
import * as path from "path";

const DEFAULTS_PATH = path.join(__dirname, "..", "..", "app", "defaults.json");
type DefaultsFile = Record<string, any>;
const RAW: DefaultsFile = JSON.parse(fs.readFileSync(DEFAULTS_PATH, "utf8"));
const PER_TYPE: DefaultsFile = RAW.perType ?? {};

/**
 * The declared default for `<block>.<key>`.
 *
 * Throws when the key declares none, rather than returning undefined. A caller asking for
 * a default has already decided the key is optional; if keys.json disagrees that is a
 * mismatch between code and spec, and `undefined` would flow on as though it were the
 * intended value — which for a `??` chain means the NEXT fallback silently wins. Same
 * reasoning as `vocabulary.values`.
 */
export function keyDefault<T = any>(block: string, key: string): T {
  const value = RAW[block]?.[key];
  if (value === undefined) {
    const declared = Object.entries(RAW)
      .filter(([b]) => !b.startsWith("$") && b !== "perType")
      .flatMap(([b, keys]) => Object.keys(keys as object).map((k) => `${b}.${k}`))
      .sort()
      .join(", ");
    throw new Error(
      `${block}.${key} declares no \`default\` in app/keys.json. Add one there and re-run ` +
        `\`python3 build_schema.py\`; do not hardcode it here. Declared: ${declared}`
    );
  }
  return value as T;
}

/**
 * The default for `<block>.<key>` under a specific variant (a tool's `type`).
 *
 * Separate from `keyDefault` because the caller MUST say which variant it means.
 * `maxResults` has no single right answer — retrieval depth for a `kb`, page size for a
 * `websearch`, 5 and 10 — so a lookup that picked one would hand a kb tool websearch's page
 * size. That is the defect this module exists to remove, so it is not reintroduced as a
 * convenience.
 */
export function keyDefaultFor<T = any>(block: string, key: string, variant: string): T {
  const value = PER_TYPE[block]?.[key]?.[variant];
  if (value === undefined) {
    const known = Object.keys(PER_TYPE[block]?.[key] ?? {}).sort().join(", ");
    throw new Error(
      `${block}.${key} declares no default for "${variant}" in app/keys.json ` +
        `(\`defaultFor\`). Declared variants: ${known || "(none)"}`
    );
  }
  return value as T;
}
