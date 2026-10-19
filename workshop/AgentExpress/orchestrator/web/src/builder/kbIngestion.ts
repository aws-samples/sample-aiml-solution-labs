/** A type="kb" tool's `chunking` and `parsing`: how its documents are read and split before
 *  they are embedded (the Bedrock data source's vectorIngestionConfiguration). Bedrock
 *  checks these only when the data source is created, halfway through a deploy, so the
 *  ranges it enforces are checked here first, from one home (vocabulary.json
 *  kbChunkingStrategies / kbParsingStrategies). Mirrors bff/kb_ingestion.py word for word
 *  (both run tests/fixtures/validation_cases.json), cdk/lib/kb-ingestion.ts and the
 *  preconditions in terraform/kb.tf.
 *  https://docs.aws.amazon.com/bedrock/latest/APIReference/API_agent_VectorIngestionConfiguration.html */
import type { Issue, Where } from "./validate";
import { vocab, vocabExtra } from "./meta";

/** A Bedrock model id or a system inference profile id; no ARN, no wildcard. */
export const MODEL_ID_RE = /^[a-z0-9-]+(\.[a-z0-9-]+)+(:[0-9]+)?$/;
export const PROMPT_MAX = 10000;

type Add = (severity: Issue["severity"], where: Where, path: string, message: string) => void;
const isObj = (v: unknown): v is Record<string, unknown> => typeof v === "object" && v !== null && !Array.isArray(v);
const whole = (v: unknown): v is number => typeof v === "number" && Number.isInteger(v);

/** The settings each chunking strategy requires, in order. */
export function chunkingParams(strategy: string): string[] {
  return vocabExtra<Record<string, string[]>>("kbChunkingStrategies", "params")?.[strategy] ?? [];
}

/** [min, max] for each chunking setting; max null means no maximum. */
export function chunkingRanges(): Record<string, [number, number | null]> {
  return vocabExtra<Record<string, [number, number | null]>>("kbChunkingStrategies", "ranges") ?? {};
}

export function checkKbIngestion(tool: Record<string, unknown>, path: string, where: Where, add: Add): void {
  if (isObj(tool.chunking)) checkChunking(tool.chunking, `${path}.chunking`, where, add, isObj(tool.vectorStore));
  if (isObj(tool.parsing)) checkParsing(tool.parsing, `${path}.parsing`, where, add);
}

function checkChunking(c: Record<string, unknown>, p: string, where: Where, add: Add, ownStore = false): void {
  const strategies = vocab("kbChunkingStrategies");
  const strategy = c.strategy;
  if (typeof strategy !== "string" || !strategies.includes(strategy)) {
    add("error", where, `${p}.strategy`, `must be one of: ${strategies.join(", ")}`);
    return;
  }
  const params = chunkingParams(strategy);
  const ranges = chunkingRanges();
  for (const name of params) {
    const v = c[name];
    if (v === undefined || v === null) {
      add("error", where, `${p}.${name}`, `is required for ${strategy} chunking`);
      continue;
    }
    const [lo, hi] = ranges[name] ?? [0, null];
    if (whole(v) && (v < lo || (hi !== null && v > hi))) {
      add("error", where, `${p}.${name}`,
        hi !== null ? `must be a whole number from ${lo} to ${hi}` : `must be a whole number, ${lo} or more`);
    }
  }
  for (const name of Object.keys(c)) {
    if (name !== "strategy" && name in ranges && !params.includes(name)) {
      add("error", where, `${p}.${name}`, `does not apply to ${strategy} chunking`);
    }
  }
  if (strategy === "hierarchical") {
    const parent = c.parentMaxTokens, child = c.childMaxTokens;
    if (whole(parent) && whole(child) && child >= parent) {
      add("error", where, `${p}.childMaxTokens`,
        "must be smaller than parentMaxTokens: the second layer holds the smaller chunks");
    }
    if (ownStore) return;     // the warning is about S3 Vectors' metadata limit
    add("warning", where, `${p}.strategy`,
      "AWS does not recommend hierarchical chunking with S3 Vectors, the store this framework creates: "
      + "large chunks can exceed its per-vector metadata limit and fail ingestion");
  }
}

function checkParsing(c: Record<string, unknown>, p: string, where: Where, add: Add): void {
  const strategies = vocab("kbParsingStrategies");
  if (typeof c.strategy !== "string" || !strategies.includes(c.strategy)) {
    add("error", where, `${p}.strategy`, `must be one of: ${strategies.join(", ")}`);
    return;
  }
  const model = c.model;
  if (model === undefined || model === null || model === "") {
    add("error", where, `${p}.model`, "is required: the Bedrock model that reads the documents");
  } else if (typeof model === "string" && !MODEL_ID_RE.test(model)) {
    add("error", where, `${p}.model`,
      "must be a Bedrock model or inference profile id, like anthropic.claude-3-haiku-20240307-v1:0");
  }
  const prompt = c.prompt;
  if (typeof prompt === "string" && !(prompt.length >= 1 && prompt.length <= PROMPT_MAX)) {
    add("error", where, `${p}.prompt`, `must be 1 to ${PROMPT_MAX} characters`);
  }
}
