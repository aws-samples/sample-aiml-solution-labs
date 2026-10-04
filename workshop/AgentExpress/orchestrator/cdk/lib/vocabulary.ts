/**
 * The framework's closed value sets, read from app/vocabulary.json.
 *
 * WHY THIS READS A FILE instead of declaring the arrays here. Every one of these lists
 * used to be written out two or three times — once in Python, once in this directory,
 * and once in terraform/*.tf. `toolTypes` and the RBAC action names existed in all
 * three.
 *
 * That cost was real and it caused real bugs: adding a value meant finding every copy,
 * and a copy that got missed rejected a config the other planes accepted, so whether a
 * workflow deployed depended on which IaC path you used. A parity test existed purely
 * to compare the copies against one another — a test that a duplicate has not drifted,
 * rather than a reason for the duplicate to exist.
 *
 * JSON is the one format all three planes read natively, so the list is held once.
 * `cdk/test/parity.test.ts` now asserts that each plane READS this file.
 */
import * as fs from "fs";
import * as path from "path";

const VOCAB_PATH = path.join(__dirname, "..", "..", "app", "vocabulary.json");

type VocabFile = Record<string, { values?: string[] }>;

const RAW: VocabFile = JSON.parse(fs.readFileSync(VOCAB_PATH, "utf8"));

/**
 * The closed set called `name`. Throws when it is not declared rather than returning
 * `[]` — an empty set would make every value invalid, or every value valid, depending
 * on how the caller uses it, and neither is a good silent default.
 */
export function values(name: string): string[] {
  const block = RAW[name];
  if (!block || !Array.isArray(block.values)) {
    throw new Error(
      `"${name}" is not a vocabulary in app/vocabulary.json. Declared: ` +
        Object.keys(RAW)
          .filter((k) => !k.startsWith("$"))
          .sort()
          .join(", ")
    );
  }
  return block.values;
}

// Named bindings, so a reader sees the vocabulary at the point of use rather than a
// string lookup, and a typo is a compile error rather than a throw at synth.
export const RUNTIMES = values("runtimes");
export const TOOL_TYPES = values("toolTypes");
export const TOOL_SCHEMA_PROPERTY_TYPES = values("toolSchemaPropertyTypes");
export const TOOL_AUTH_MODES = values("toolAuthModes");
export const API_KEY_TOOL_TYPES = values("apiKeyToolTypes");
export const TOOL_LISTING_MODES = values("toolListingModes");
export const A2A_AUTH_MODES = values("a2aAuthModes");
export const A2A_SOURCES = values("a2aSources");
export const A2A_LAMBDA_SKILLS = values("a2aLambdaSkills");
export const AUTHORIZATION_ACTIONS = values("authorizationActions");
export const WEB_SEARCH_REGIONS = values("webSearchRegions");
export const GUARDRAIL_FILTER_STRENGTHS = values("guardrailFilterStrengths");
export const GUARDRAIL_PII_ACTIONS = values("guardrailPiiActions");
export const BUILTIN_LAMBDA_SOURCE = values("builtinLambdaSource")[0];
export const EMBEDDING_MODELS = values("embeddingModels");

/**
 * The embedding dimensions a model supports, FIRST being its default.
 *
 * A second shape in the same file, because this one is a map rather than a list: the
 * model and the dimension have to agree, and holding the pairing anywhere else would
 * put it in two planes again (see terraform/kb.tf, which reads the same key). Throws on
 * an unknown model for the same reason `values` does — silently defaulting the dimension
 * of a model nobody validated is how an empty corpus gets deployed successfully.
 */
/** A vocabulary's extra field (e.g. builtinEvaluators.customScale), or undefined. */
export function extra<T>(name: string, field: string): T | undefined {
  return (RAW as any)?.[name]?.[field] as T | undefined;
}

export function embeddingDimensions(model: string): number[] {
  const dims = (RAW.embeddingModels as any)?.dimensionsByModel?.[model];
  if (!Array.isArray(dims) || !dims.length) {
    throw new Error(
      `"${model}" has no dimensionsByModel entry in app/vocabulary.json. ` +
        `Declared: ${EMBEDDING_MODELS.join(", ")}`
    );
  }
  return dims;
}

/** The inference-profile geography a region's model calls must use (`us`, `eu`, ...). */
export function regionGeo(region: string): string {
  const rules = extra<[string, string][]>("inferenceProfileGeos", "byRegionPrefix") ?? [];
  const hit = rules.find(([prefix]) => (region ?? "").startsWith(prefix));
  return hit ? hit[1] : extra<string>("inferenceProfileGeos", "fallback") ?? "global";
}

/**
 * `modelId` with its cross-region profile prefix moved to `region`'s geography. The
 * framework default and the sample workflow are `us.` profiles, which answer only from a
 * US source region, so the same config deployed in eu-west-1 failed every model call.
 * `global.` and bare foundation-model ids are unchanged. Same rule as
 * app/common/vocabulary.regional_model_id and terraform/models.tf.
 */
export function regionalModelId(modelId: string, region?: string): string {
  const mid = String(modelId ?? "");
  const dot = mid.indexOf(".");
  if (!region || dot < 0) return mid;
  const head = mid.slice(0, dot);
  if (!values("inferenceProfileGeos").includes(head) || head === "global") return mid;
  const target = regionGeo(region);
  return head === target ? mid : `${target}.${mid.slice(dot + 1)}`;
}

/**
 * Why an image agent may not deploy in `region`, or "" when it may: its model is served
 * only outside this region's geography and the agent did not set
 * `image.allowCrossRegion: true`. Same rule as app/common/images.residency_error and
 * the image_residency check in terraform/images.tf.
 */
export function imageResidencyError(model: string, region: string, allow: boolean): string {
  const regions = extra<Record<string, string[]>>("imageModels", "regionsByModel")?.[model] ?? [];
  if (!regions.length || allow || regions.includes(region)) return "";
  const geo = regionGeo(region);
  if (regions.some((r) => regionGeo(r) === geo)) return "";
  return (
    `${model} is served in ${regions.join(", ")}, outside this deployment's geography ` +
    `(${region}), so its image brief would leave it. Set the agent's image.allowCrossRegion ` +
    `to true to allow that, or deploy in a region of that geography.`
  );
}
