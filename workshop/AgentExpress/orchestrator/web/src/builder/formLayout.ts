/** What each form shows up front, and how the rest is grouped under Advanced. Only the
 *  page's presentation: every key still comes from app/keys.json, a key that applies but
 *  is not listed here lands in Advanced → Other, and the saved workflow.json is unchanged. */
import type { BlockName } from "./meta";

export interface FormLayout {
  /** Shown first, in this order: what a user nearly always sets. */
  main: string[];
  /** Collapsed under Advanced, one section each. */
  advanced: { title: string; keys: string[] }[];
}

export const FORM_LAYOUT: Partial<Record<BlockName, FormLayout>> = {
  agent: {
    main: ["name", "runtime", "model", "tool", "corpus", "agentCard", "source", "skill", "auth"],
    advanced: [
      { title: "Model tuning", keys: ["maxTokens", "temperature", "topP", "stopSequences"] },
      { title: "Tool use", keys: ["toolMode", "maxToolCalls", "access"] },
      { title: "Images", keys: ["output", "image", "vision"] },
      { title: "Code", keys: ["framework"] },
      { title: "Labels", keys: ["kind", "produces"] },
    ],
  },
  guardrail: {
    main: ["contentFilters", "deniedTopics"],
    advanced: [
      { title: "Words", keys: ["deniedWords", "managedWordLists"] },
      { title: "Sensitive information", keys: ["piiEntities"] },
      { title: "Messages", keys: ["blockedInputMessage", "blockedOutputMessage"] },
    ],
  },
  memory: {
    main: ["strategies"],
    advanced: [
      { title: "Retention", keys: ["expiryDays"] },
      { title: "Sharing", keys: ["scope"] },
      { title: "About", keys: ["description"] },
    ],
  },
  evaluator: {
    main: ["instructions"],
    advanced: [
      { title: "Judge", keys: ["model"] },
      { title: "Scoring", keys: ["scale"] },
      { title: "About", keys: ["description"] },
    ],
  },
  identity: {
    main: ["type", "clientId", "tokenUrl", "discoveryUrl"],
    advanced: [
      { title: "OAuth", keys: ["scopes", "issuer"] },
      { title: "About", keys: ["description"] },
    ],
  },
  tool: {
    main: ["type", "description", "corpora", "endpoint", "lambdaArn", "source", "code", "schemaS3Uri", "schema",
      "restApiId", "stage", "knowledgeBaseId", "s3Uri", "toolSchema"],
    advanced: [
      { title: "Calling", keys: ["call", "arg", "args", "rowPath", "rowFields", "toolFilters", "toolOverrides", "listingMode", "connectorVersion"] },
      { title: "Search and filters", keys: ["maxResults", "domains", "publishedFrom", "publishedTo", "corpusKey", "corpusOperator",
        "filter", "rerank", "embeddingModel", "dimensions", "kmsKeyArn"] },
      { title: "Auth", keys: ["auth", "service", "oauth", "identity"] },
      { title: "Policy", keys: ["policy.tool", "policy.restrictTo", "policy.permit", "policies"] },
    ],
  },
};

/** The keys available in a form, split into what is shown first and the Advanced
 *  sections (with an "Other" section for anything the layout does not name). */
export function splitLayout(layout: FormLayout, available: string[]): { main: string[]; advanced: { title: string; keys: string[] }[] } {
  const have = new Set(available);
  const main = layout.main.filter((k) => have.has(k));
  const placed = new Set([...layout.main, ...layout.advanced.flatMap((s) => s.keys)]);
  const advanced = layout.advanced.map((s) => ({ title: s.title, keys: s.keys.filter((k) => have.has(k)) }))
    .filter((s) => s.keys.length);
  const other = available.filter((k) => !placed.has(k));
  if (other.length) advanced.push({ title: "Other", keys: other });
  return { main, advanced };
}
