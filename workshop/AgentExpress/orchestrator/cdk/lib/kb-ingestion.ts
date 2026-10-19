/**
 * How the Knowledge Base reads and splits its documents: a type="kb" tool's `chunking`
 * and `parsing` keys, as the data source's vectorIngestionConfiguration. Both optional:
 * with neither, the data source is exactly what it was (name "docs", no ingestion
 * configuration), so an existing stack synthesizes no change.
 *
 * Mirrors terraform/kb_ingestion.tf (locals, preconditions, the KB role statement),
 * bff/kb_ingestion.py and web/src/builder/kbIngestion.ts (the Build view's checks).
 *
 * What the docs and CfnDataSource allow, and what this uses:
 *   - ChunkingConfiguration: FIXED_SIZE (maxTokens 1-8192, overlapPercentage 1-99),
 *     HIERARCHICAL (two levelConfigurations, large then small, maxTokens 1-8192;
 *     overlapTokens >= 1), SEMANTIC (maxTokens 1-8192, bufferSize 0-1,
 *     breakpointPercentileThreshold 50-99), NONE.
 *   - ParsingConfiguration: BEDROCK_FOUNDATION_MODEL with modelArn and an optional
 *     parsingPrompt (1-10000 characters). BEDROCK_DATA_AUTOMATION is not offered: AWS
 *     documents it as a preview in US West (Oregon) only.
 *   - Both are "Update requires: Replacement", so the data source name carries a digest
 *     of them (kbDataSourceName) and a change is a clean replacement.
 * https://docs.aws.amazon.com/bedrock/latest/userguide/kb-chunking.html
 * https://docs.aws.amazon.com/bedrock/latest/userguide/kb-advanced-parsing.html
 * https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-properties-bedrock-datasource-vectoringestionconfiguration.html
 */
import * as crypto from "crypto";
import * as iam from "aws-cdk-lib/aws-iam";
import type * as bedrock from "aws-cdk-lib/aws-bedrock";
import * as vocab from "./vocabulary";

/** A Bedrock model id or a system inference profile id; no ARN, no wildcard. */
export const MODEL_ID_RE = /^[a-z0-9-]+(\.[a-z0-9-]+)+(:[0-9]+)?$/;
export const PROMPT_MAX = 10000;

export interface KbChunking { strategy: string; [setting: string]: unknown }
export interface KbParsing { strategy: string; model?: string; prompt?: string }
/** The parts of a kb tool this module reads. `vectorStore` only for Neptune's graph model. */
export interface KbIngestionSpec { chunking?: KbChunking; parsing?: KbParsing; vectorStore?: { type?: string; graphModel?: string } }

/** The GraphRAG graph construction model, when the store is Neptune Analytics. */
export function graphModel(t: KbIngestionSpec): string {
  return t.vectorStore?.type === "neptuneAnalytics" ? String(t.vectorStore.graphModel ?? "") : "";
}

const params = (strategy: string): string[] =>
  vocab.extra<Record<string, string[]>>("kbChunkingStrategies", "params")?.[strategy] ?? [];
const ranges = (): Record<string, [number, number | null]> =>
  vocab.extra<Record<string, [number, number | null]>>("kbChunkingStrategies", "ranges") ?? {};
const api = (name: string, value: string): string =>
  vocab.extra<Record<string, string>>(name, "api")?.[value] ?? "";

/** The checks a synth makes, as errors: the same rules terraform/kb.tf's data source
 *  precondition applies at plan. */
export function validateKbIngestion(name: string, t: KbIngestionSpec): void {
  const where = `workflow.json tools.${name}`;
  const c = t.chunking;
  if (c !== undefined) {
    const strategies = vocab.values("kbChunkingStrategies");
    if (!strategies.includes(c?.strategy)) {
      throw new Error(`${where}.chunking.strategy must be one of: ${strategies.join(", ")}.`);
    }
    const r = ranges();
    for (const p of params(c.strategy)) {
      const v = c[p];
      const [lo, hi] = r[p];
      if (typeof v !== "number" || !Number.isInteger(v)) {
        throw new Error(`${where}.chunking.${p} is required for ${c.strategy} chunking, as a whole number.`);
      }
      if (v < lo || (hi !== null && v > hi)) {
        throw new Error(hi === null
          ? `${where}.chunking.${p} must be a whole number, ${lo} or more.`
          : `${where}.chunking.${p} must be a whole number from ${lo} to ${hi}.`);
      }
    }
    if (c.strategy === "hierarchical" && Number(c.childMaxTokens) >= Number(c.parentMaxTokens)) {
      throw new Error(`${where}.chunking.childMaxTokens must be smaller than parentMaxTokens: the second layer holds the smaller chunks.`);
    }
  }
  const p = t.parsing;
  if (p !== undefined) {
    const strategies = vocab.values("kbParsingStrategies");
    if (!strategies.includes(p?.strategy)) {
      throw new Error(`${where}.parsing.strategy must be one of: ${strategies.join(", ")}.`);
    }
    if (typeof p.model !== "string" || !MODEL_ID_RE.test(p.model)) {
      throw new Error(`${where}.parsing.model must be a Bedrock model or inference profile id, like anthropic.claude-3-haiku-20240307-v1:0.`);
    }
    if (p.prompt !== undefined && (typeof p.prompt !== "string" || p.prompt.length < 1 || p.prompt.length > PROMPT_MAX)) {
      throw new Error(`${where}.parsing.prompt must be 1 to ${PROMPT_MAX} characters.`);
    }
  }
}

/** Everything that forces a new data source, as one string. Must match
 *  local.kb_ingestion_key in terraform/kb_ingestion.tf character for character. */
export function kbIngestionKey(t: KbIngestionSpec): string {
  const c = t.chunking;
  const p = t.parsing;
  const cs = c?.strategy ?? "";
  const values = params(cs).map((k) => (c?.[k] === undefined || c?.[k] === null ? "" : String(c[k])));
  // The graph model only when there is one, so every name without it is unchanged.
  const e = graphModel(t) ? `|e=${graphModel(t)}` : "";
  return `c=${cs}:${values.join(",")}|p=${p?.strategy ?? ""}:${p?.model ?? ""}:${p?.prompt ?? ""}${e}`;
}

/** "docs", as it always was, unless chunking or parsing is set: then a digest of them,
 *  so changing either is a replacement CloudFormation can make. */
export function kbDataSourceName(t: KbIngestionSpec): string {
  if (!t.chunking && !t.parsing && !graphModel(t)) return "docs";
  return `docs-${crypto.createHash("sha256").update(kbIngestionKey(t)).digest("hex").slice(0, 8)}`;
}

/** A system inference profile id starts with a geography (`us.`, `eu.`, `global.`, ...). */
function isProfile(model: string): boolean {
  return vocab.values("inferenceProfileGeos").includes(model.split(".")[0]);
}

/** The ARN Bedrock is given for a model id, and the resources a role needs to invoke it:
 *  a plain model id is that model here; a profile is invoked through its own ARN and routes
 *  to the model in any region of its geography, so it needs both. */
export function modelGrant(model: string, region: string, account: string): { arn: string; resources: string[] } {
  if (!isProfile(model)) {
    const arn = `arn:aws:bedrock:${region}::foundation-model/${model}`;
    return { arn, resources: [arn] };
  }
  const arn = `arn:aws:bedrock:${region}:${account}:inference-profile/${model}`;
  return { arn, resources: [arn, `arn:aws:bedrock:*::foundation-model/${model.split(".").slice(1).join(".")}`] };
}

export interface KbIngestion {
  dataSourceName: string;
  vectorIngestionConfiguration?: bedrock.CfnDataSource.VectorIngestionConfigurationProperty;
  /** For the Knowledge Base role: the parsing model, when parsing is on. */
  statements: iam.PolicyStatement[];
}

export function kbIngestion(name: string, t: KbIngestionSpec,
  region: string, account: string): KbIngestion {
  validateKbIngestion(name, t);
  const c = t.chunking;
  const p = t.parsing;
  const g = graphModel(t);
  if (!c && !p && !g) return { dataSourceName: "docs", statements: [] };
  const n = (k: string) => Number(c?.[k]);
  const chunkingConfiguration: bedrock.CfnDataSource.ChunkingConfigurationProperty | undefined = c && {
    chunkingStrategy: api("kbChunkingStrategies", c.strategy),
    ...(c.strategy === "fixedSize" ? {
      fixedSizeChunkingConfiguration: { maxTokens: n("maxTokens"), overlapPercentage: n("overlapPercentage") },
    } : {}),
    ...(c.strategy === "hierarchical" ? {
      // First the large (parent) layer, then the small (child) one: Bedrock's order.
      hierarchicalChunkingConfiguration: {
        levelConfigurations: [{ maxTokens: n("parentMaxTokens") }, { maxTokens: n("childMaxTokens") }],
        overlapTokens: n("overlapTokens"),
      },
    } : {}),
    ...(c.strategy === "semantic" ? {
      semanticChunkingConfiguration: {
        maxTokens: n("maxTokens"), bufferSize: n("bufferSize"),
        breakpointPercentileThreshold: n("breakpointPercentileThreshold"),
      },
    } : {}),
  };
  const statements: iam.PolicyStatement[] = [];
  let parsingConfiguration: bedrock.CfnDataSource.ParsingConfigurationProperty | undefined;
  if (p) {
    const { arn: modelArn, resources } = modelGrant(String(p.model), region, account);
    parsingConfiguration = {
      parsingStrategy: api("kbParsingStrategies", p.strategy),
      bedrockFoundationModelConfiguration: {
        modelArn,
        ...(p.prompt ? { parsingPrompt: { parsingPromptText: p.prompt } } : {}),
      },
    };
    statements.push(new iam.PolicyStatement({ sid: "ParsingModel", actions: ["bedrock:InvokeModel"], resources }));
  }
  // Neptune Analytics (GraphRAG): Bedrock builds the graph with this model, as the data
  // source's context enrichment ("selecting a graph construction model automatically
  // enables contextual enrichment").
  let contextEnrichmentConfiguration: bedrock.CfnDataSource.ContextEnrichmentConfigurationProperty | undefined;
  if (g) {
    const { arn, resources } = modelGrant(g, region, account);
    contextEnrichmentConfiguration = {
      type: "BEDROCK_FOUNDATION_MODEL",
      bedrockFoundationModelConfiguration: { modelArn: arn, enrichmentStrategyConfiguration: { method: "CHUNK_ENTITY_EXTRACTION" } },
    };
    statements.push(new iam.PolicyStatement({ sid: "GraphModel", actions: ["bedrock:InvokeModel"], resources }));
  }
  return {
    dataSourceName: kbDataSourceName(t),
    vectorIngestionConfiguration: {
      ...(chunkingConfiguration ? { chunkingConfiguration } : {}),
      ...(parsingConfiguration ? { parsingConfiguration } : {}),
      ...(contextEnrichmentConfiguration ? { contextEnrichmentConfiguration } : {}),
    },
    statements,
  };
}
