/** A Knowledge Base tool's `chunking` and `parsing`, as plain choices instead of JSON.
 *  Written as tools.<key>.chunking / tools.<key>.parsing; leaving a choice on "Bedrock
 *  default" removes the key, so a tool that never touches these deploys as before.
 *  The checks are in kbIngestion.ts. */
import FormField from "@cloudscape-design/components/form-field";
import Input from "@cloudscape-design/components/input";
import Select from "@cloudscape-design/components/select";
import SpaceBetween from "@cloudscape-design/components/space-between";
import Textarea from "@cloudscape-design/components/textarea";
import type { Json } from "./model";
import { chunkingParams } from "./kbIngestion";

type Obj = { [k: string]: Json };
const asObj = (v: Json | undefined): Obj => (v && typeof v === "object" && !Array.isArray(v) ? v : {});

const CHUNKING: { value: string; label: string }[] = [
  { value: "", label: "Bedrock default (about 300 tokens, whole sentences)" },
  { value: "fixedSize", label: "Fixed size" },
  { value: "hierarchical", label: "Two layers: search small chunks, return the larger one around them" },
  { value: "semantic", label: "Where the meaning changes (extra model cost)" },
  { value: "none", label: "Don't split: one chunk per file" },
];

/** What a new choice starts with, so the form is never empty; every value can be changed. */
const STARTING: Record<string, Obj> = {
  fixedSize: { maxTokens: 300, overlapPercentage: 20 },
  hierarchical: { parentMaxTokens: 1500, childMaxTokens: 300, overlapTokens: 60 },
  semantic: { maxTokens: 300, bufferSize: 0, breakpointPercentileThreshold: 95 },
  none: {},
};

const LABELS: Record<string, string> = {
  maxTokens: "Most tokens in a chunk",
  overlapPercentage: "Overlap between chunks (percent)",
  parentMaxTokens: "Most tokens in a large chunk",
  childMaxTokens: "Most tokens in a small chunk",
  overlapTokens: "Overlap between chunks (tokens)",
  bufferSize: "Neighbouring sentences compared (0 or 1)",
  breakpointPercentileThreshold: "How different sentences must be to split (50 to 99)",
};

export function ChunkingField({ value, onChange }: { value: Json | undefined; onChange: (v: Json | undefined) => void }) {
  const cur = asObj(value);
  const strategy = typeof cur.strategy === "string" ? cur.strategy : "";
  const selected = CHUNKING.find((o) => o.value === strategy) ?? { value: strategy, label: strategy };
  return (
    <SpaceBetween size="s">
      <Select ariaLabel="How documents are split" selectedOption={selected} options={CHUNKING}
        onChange={({ detail }) => {
          const next = detail.selectedOption.value ?? "";
          onChange(next ? { strategy: next, ...STARTING[next] } : undefined);
        }} />
      {chunkingParams(strategy).map((p) => (
        <FormField key={p} label={LABELS[p] ?? p}>
          <Input type="number" inputMode="numeric" ariaLabel={LABELS[p] ?? p}
            value={cur[p] === undefined || cur[p] === null ? "" : String(cur[p])}
            onChange={({ detail }) => {
              const next: Obj = { ...cur };
              if (detail.value.trim() === "") delete next[p];
              else next[p] = Number.isFinite(Number(detail.value)) ? Number(detail.value) : detail.value;
              onChange(next);
            }} />
        </FormField>
      ))}
    </SpaceBetween>
  );
}

const PARSING: { value: string; label: string }[] = [
  { value: "", label: "Bedrock default: text only, no charge" },
  { value: "foundationModel", label: "With a model: also reads tables, charts and images in PDFs (costs model tokens)" },
];

export function ParsingField({ value, onChange }: { value: Json | undefined; onChange: (v: Json | undefined) => void }) {
  const cur = asObj(value);
  const strategy = typeof cur.strategy === "string" ? cur.strategy : "";
  const selected = PARSING.find((o) => o.value === strategy) ?? { value: strategy, label: strategy };
  const set = (k: string, v: string) => {
    const next: Obj = { ...cur };
    if (v === "") delete next[k]; else next[k] = v;
    onChange(next);
  };
  return (
    <SpaceBetween size="s">
      <Select ariaLabel="How documents are read" selectedOption={selected} options={PARSING}
        onChange={({ detail }) => {
          const next = detail.selectedOption.value ?? "";
          onChange(next ? { strategy: next } : undefined);
        }} />
      {strategy === "foundationModel" ? (
        <SpaceBetween size="s">
          <FormField label="Model" description="A Claude, Nova or Llama 4 vision model id, or an inference profile id. Only this model is granted to the Knowledge Base.">
            <Input ariaLabel="Model" placeholder="anthropic.claude-3-haiku-20240307-v1:0"
              value={typeof cur.model === "string" ? cur.model : ""}
              onChange={({ detail }) => set("model", detail.value.trim())} />
          </FormField>
          <FormField label="Instructions for the model (optional)" description="Leave empty to use Bedrock's own instructions.">
            <Textarea ariaLabel="Instructions for the model" rows={3}
              value={typeof cur.prompt === "string" ? cur.prompt : ""}
              onChange={({ detail }) => set("prompt", detail.value)} />
          </FormField>
        </SpaceBetween>
      ) : null}
    </SpaceBetween>
  );
}
