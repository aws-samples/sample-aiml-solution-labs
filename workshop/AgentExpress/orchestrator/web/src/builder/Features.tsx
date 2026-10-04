/** Pickers for an agent's AgentCore features that a generated form cannot express well:
 *  the evaluators (13 built-ins, some not scorable here, plus this agent's own) and the
 *  custom evaluators themselves. Both write plain workflow.json values. */
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import Container from "@cloudscape-design/components/container";
import FormField from "@cloudscape-design/components/form-field";
import Input from "@cloudscape-design/components/input";
import Multiselect from "@cloudscape-design/components/multiselect";
import SpaceBetween from "@cloudscape-design/components/space-between";
import Textarea from "@cloudscape-design/components/textarea";

import { vocab, vocabExtra } from "./meta";
import type { Json } from "./model";

export interface CustomEvaluator { name: string; instructions: string; model?: string; scale?: Json }

/** The evaluator options: every built-in (the ones this framework cannot score yet
 *  disabled, with why) and each custom evaluator this agent defines. */
export function evaluatorOptions(custom: CustomEvaluator[]) {
  const unsupported = vocabExtra<string[]>("builtinEvaluators", "unsupported") ?? [];
  const levels = vocabExtra<Record<string, string>>("builtinEvaluators", "levels") ?? {};
  return [
    ...vocab("builtinEvaluators").map((v) => ({
      label: v.replace("Builtin.", ""), value: v,
      ...(unsupported.includes(v)
        ? { disabled: true, description: `Scores a ${String(levels[v] ?? "").toLowerCase().replace("_", " ")} — not offered yet` }
        : {}),
    })),
    ...custom.filter((c) => c.name).map((c) => ({ label: c.name, value: `Custom.${c.name}`, description: "Your evaluator" })),
  ];
}

export function EvaluatorPicker({ value, custom, onChange }: {
  value: string[]; custom: CustomEvaluator[]; onChange: (v: string[] | undefined) => void;
}) {
  const options = evaluatorOptions(custom);
  return (
    <Multiselect
      selectedOptions={options.filter((o) => value.includes(o.value))}
      options={options}
      placeholder="Faithfulness (the default)"
      filteringType="auto"
      onChange={({ detail }) => {
        const vals = detail.selectedOptions.map((o) => o.value!).filter(Boolean);
        onChange(vals.length ? vals : undefined);
      }}
    />
  );
}

export function CustomEvaluators({ value, onChange }: {
  value: CustomEvaluator[]; onChange: (v: CustomEvaluator[] | undefined) => void;
}) {
  const set = (i: number, patch: Partial<CustomEvaluator>) => {
    const next = value.map((c, j) => (j === i ? { ...c, ...patch } : c));
    onChange(next);
  };
  return (
    <SpaceBetween size="s">
      {value.map((c, i) => (
        <Container key={i} disableContentPaddings={false}
          header={<Box variant="h4">{c.name || "New evaluator"}</Box>}>
          <SpaceBetween size="s">
            <FormField label="Name" description="Letters and digits; used as Custom.<name>.">
              <Input value={c.name} onChange={({ detail }) => set(i, { name: detail.value.replace(/[^A-Za-z0-9]/g, "") })} />
            </FormField>
            <FormField label="What the judge scores, and how"
              description="Written for the judge model. {context} (the request and what came before) and {assistant_turn} (this agent's output) are added if you use neither.">
              <Textarea value={c.instructions} rows={4} onChange={({ detail }) => set(i, { instructions: detail.value })} />
            </FormField>
            <FormField label="Judge model" description="Optional. Default: the workflow's default model.">
              <Input value={c.model ?? ""} placeholder="(default)"
                onChange={({ detail }) => set(i, { model: detail.value || undefined })} />
            </FormField>
            <Box float="right">
              <Button variant="link" onClick={() => {
                const next = value.filter((_, j) => j !== i);
                onChange(next.length ? next : undefined);
              }}>Remove</Button>
            </Box>
          </SpaceBetween>
        </Container>
      ))}
      <Button iconName="add-plus" onClick={() => onChange([...value, { name: "", instructions: "" }])}>
        Add a custom evaluator
      </Button>
    </SpaceBetween>
  );
}
