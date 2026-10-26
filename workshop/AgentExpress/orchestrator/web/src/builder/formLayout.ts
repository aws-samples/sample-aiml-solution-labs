/** What each form shows up front, and how the rest is grouped under Advanced. Only the
 *  page's presentation: every key still comes from app/keys.json, a key that applies but
 *  is not listed here lands in Advanced → Other, and the saved workflow.json is unchanged. */
import type { DocsFeature } from "../docs/docsLinks";
import type { BlockName } from "./meta";

/** One Advanced section; `docs` is its info icon's documentation (docs/docsLinks.ts). */
export interface FormSection { title: string; keys: string[]; docs?: DocsFeature }

export interface FormLayout {
  /** Shown first, in this order: what a user nearly always sets. */
  main: string[];
  /** Collapsed under Advanced, one section each. */
  advanced: FormSection[];
  /** The documentation of the Other section, for a block whose Other can hold keys. */
  otherDocs?: DocsFeature;
}

export const FORM_LAYOUT: Partial<Record<BlockName, FormLayout>> = {
  agent: {
    main: ["name", "runtime", "model", "tool", "corpus", "agentCard", "source", "skill", "auth"],
    advanced: [
      { title: "Model tuning", keys: ["maxTokens", "temperature", "topP", "stopSequences"], docs: "agent.tuning" },
      { title: "Tool use", keys: ["toolMode", "maxToolCalls", "access"], docs: "agent.toolUse" },
      { title: "Images", keys: ["output", "image", "vision"], docs: "agent.images" },
      { title: "Code", keys: ["framework"], docs: "agent.code" },
      { title: "Labels", keys: ["kind", "produces"], docs: "agent.labels" },
    ],
    otherDocs: "agent.other",
  },
  guardrail: {
    main: ["contentFilters", "deniedTopics"],
    advanced: [
      { title: "Words", keys: ["deniedWords", "managedWordLists"], docs: "guardrail.words" },
      { title: "Sensitive information", keys: ["piiEntities"], docs: "guardrail.pii" },
      { title: "Messages", keys: ["blockedInputMessage", "blockedOutputMessage"], docs: "guardrail.messages" },
    ],
  },
  memory: {
    main: ["strategies"],
    advanced: [
      { title: "Retention", keys: ["expiryDays"], docs: "memory.retention" },
      { title: "Sharing", keys: ["scope"], docs: "memory.sharing" },
      { title: "About", keys: ["description"], docs: "memory.about" },
    ],
  },
  evaluator: {
    main: ["instructions"],
    advanced: [
      { title: "Judge", keys: ["model"], docs: "evaluator.judge" },
      { title: "Scoring", keys: ["scale"], docs: "evaluator.scoring" },
      { title: "About", keys: ["description"], docs: "evaluator.about" },
    ],
  },
  identity: {
    main: ["type", "clientId", "tokenUrl", "discoveryUrl"],
    advanced: [
      { title: "OAuth", keys: ["scopes", "issuer"], docs: "identity.oauth" },
      { title: "About", keys: ["description"], docs: "identity.about" },
    ],
  },
  tool: {
    main: ["type", "description", "corpora", "endpoint", "lambdaArn", "source", "allowDomains", "formats", "templateS3Uri", "outputPrefix", "code", "schemaS3Uri", "schema",
      "restApiId", "stage", "knowledgeBaseId", "s3Uri", "toolSchema"],
    advanced: [
      { title: "Calling", keys: ["call", "arg", "args", "rowPath", "rowFields", "toolFilters", "toolOverrides", "listingMode", "connectorVersion"], docs: "tool.calling" },
      { title: "Search and filters", keys: ["maxResults", "domains", "publishedFrom", "publishedTo", "corpusKey", "corpusOperator",
        "filter", "rerank", "embeddingModel", "dimensions", "kmsKeyArn"], docs: "tool.search" },
      { title: "Document processing", keys: ["chunking", "parsing"], docs: "tool.kbProcessing" },
      { title: "Where documents come from", keys: ["dataSource"], docs: "tool.kbDataSource" },
      { title: "Vector store", keys: ["vectorStore"], docs: "tool.kbVectorStore" },
      { title: "Auth", keys: ["auth", "service", "oauth", "identity"], docs: "tool.connects" },
      { title: "Policy", keys: ["policy.tool", "policy.restrictTo", "policy.permit", "policies"], docs: "tool.policy" },
    ],
    otherDocs: "tool.other",
  },
};

/** The keys available in a form, split into what is shown first and the Advanced
 *  sections (with an "Other" section for anything the layout does not name). */
export function splitLayout(layout: FormLayout, available: string[]): { main: string[]; advanced: FormSection[] } {
  const have = new Set(available);
  const main = layout.main.filter((k) => have.has(k));
  const placed = new Set([...layout.main, ...layout.advanced.flatMap((s) => s.keys)]);
  const advanced: FormSection[] = layout.advanced.map((s) => ({ ...s, keys: s.keys.filter((k) => have.has(k)) }))
    .filter((s) => s.keys.length);
  const other = available.filter((k) => !placed.has(k));
  if (other.length) advanced.push({ title: "Other", keys: other, ...(layout.otherDocs ? { docs: layout.otherDocs } : {}) });
  return { main, advanced };
}
