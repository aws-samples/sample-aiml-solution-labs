/**
 * Extra tags for every build this console deploys (`-c buildTags='{"k":"v"}'`), in its own
 * account or a connected one. They reach the deploy project as BUILD_EXTRA_TAGS, and
 * deployer/runner.py (extra_build_tags) merges them under the framework's agentexpress:*
 * tags, which always win. `-c tags` tags this stack only.
 *
 * Mirrors var.build_tags in terraform/builder.tf and extra_build_tags in deployer/runner.py;
 * tests/test_build_tags.py holds the three to the same rules. ASCII only, because Amazon
 * Bedrock's tag pattern is, and a tag one resource refuses fails the whole build deploy.
 */

export const BUILD_TAGS_ENV = "BUILD_EXTRA_TAGS";
export const MAX_BUILD_TAGS = 40;
export const RESERVED_TAG_PREFIXES = ["aws:", "agentexpress:"];
const TAG_CHARS = /^[A-Za-z0-9 _.:\/=+\-@]*$/;
const CHARS_RULE = "may use only letters A-Z a-z, digits, spaces and _ . : / = + - @.";
const SHAPE = `buildTags must be a JSON object of tag key -> value, e.g. '{"cost-center":"1234"}'.`;

/** The extra build tags, checked: a JSON string (from -c) or an object (from cdk.json).
 *  Unset, "" or {} is no tags. Throws, naming the rule, on anything AWS or the framework
 *  would refuse. */
export function buildTagsOf(raw: unknown): Record<string, string> {
  if (raw === undefined || raw === null || raw === "") return {};
  let parsed: unknown = raw;
  if (typeof raw === "string") {
    try {
      parsed = JSON.parse(raw);
    } catch {
      throw new Error(SHAPE);
    }
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) throw new Error(SHAPE);
  const out: Record<string, string> = {};
  const lower = new Map<string, string>();
  for (const [k, v] of Object.entries(parsed as Record<string, unknown>)) {
    if (!["string", "number", "boolean"].includes(typeof v)) {
      throw new Error(`buildTags["${k}"] must be a string, number or boolean.`);
    }
    const value = String(v);
    if (!TAG_CHARS.test(k)) throw new Error(`buildTags key "${k}" ${CHARS_RULE}`);
    if (k.length < 1 || k.length > 128) throw new Error(`buildTags key "${k}" must be 1 to 128 characters.`);
    if (RESERVED_TAG_PREFIXES.some((p) => k.toLowerCase().startsWith(p))) {
      throw new Error(`buildTags key "${k}" is reserved: keys starting with "aws:" or "agentexpress:" ` +
        "(in any case) cannot be set.");
    }
    if (!TAG_CHARS.test(value)) throw new Error(`buildTags["${k}"] ${CHARS_RULE}`);
    if (value.length > 256) throw new Error(`buildTags["${k}"] must be at most 256 characters.`);
    const seen = lower.get(k.toLowerCase());
    if (seen !== undefined) {
      throw new Error(`buildTags keys "${seen}" and "${k}" differ only in case; IAM treats them as one tag.`);
    }
    lower.set(k.toLowerCase(), k);
    out[k] = value;
  }
  const n = Object.keys(out).length;
  if (n > MAX_BUILD_TAGS) {
    throw new Error(`buildTags has ${n} tags; at most ${MAX_BUILD_TAGS} are allowed ` +
      "(a resource takes 50 and the framework sets its own).");
  }
  return out;
}

/** The deploy project's environment for them: nothing when there are none, else
 *  BUILD_EXTRA_TAGS as compact JSON with sorted keys, the bytes Terraform's jsonencode
 *  writes for var.build_tags. */
export function buildTagsEnv(tags: Record<string, string>): Record<string, string> {
  const entries = Object.entries(tags);
  if (!entries.length) return {};
  const sorted = Object.fromEntries(entries.sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)));
  return { [BUILD_TAGS_ENV]: JSON.stringify(sorted) };
}
