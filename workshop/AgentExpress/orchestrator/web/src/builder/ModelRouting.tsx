/** An agent's `models` (app/common/model_routing.py): `route` rules that pick the model by
 *  input size or by what the run is about (first match wins, else the agent's model), and
 *  `fallback` models tried in order when that model is busy or not ready. */
import AttributeEditor from "@cloudscape-design/components/attribute-editor";
import Autosuggest from "@cloudscape-design/components/autosuggest";
import FormField from "@cloudscape-design/components/form-field";
import Input from "@cloudscape-design/components/input";
import Select from "@cloudscape-design/components/select";
import SpaceBetween from "@cloudscape-design/components/space-between";

import type { Json } from "./model";
import { MAX_FALLBACK, MAX_RULES } from "./modelRoutingCheck";
import type { ModelOption } from "./models";

type Rule = Record<string, Json>;
const WHEN = [
  { value: "inputTokensOver", label: "The input is over (tokens)" },
  { value: "matches", label: "The request or subject matches" },
];

const isObj = (v: unknown): v is Record<string, Json> => typeof v === "object" && v !== null && !Array.isArray(v);

export function ModelRouting({ value, set, models }: {
  value: Json | undefined;
  set: (v: Json | undefined) => void;
  models: ModelOption[];
}) {
  const cur = isObj(value) ? value : {};
  const fallback = Array.isArray(cur.fallback) ? cur.fallback.map((m) => String(m ?? "")) : [];
  const route: Rule[] = Array.isArray(cur.route) ? cur.route.map((r) => (isObj(r) ? r : {})) : [];
  const options = models.map((m) => ({ value: m.id, label: m.id, description: `${m.provider} · ${m.name}` }));
  const write = (next: { fallback?: string[]; route?: Rule[] }) => {
    const f = next.fallback ?? fallback;
    const r = next.route ?? route;
    const out: Record<string, Json> = { ...cur };
    if (f.length) out.fallback = f; else delete out.fallback;
    if (r.length) out.route = r; else delete out.route;
    set(Object.keys(out).length ? out : undefined);
  };
  const model = (v: string, onChange: (m: string) => void, label: string) => (
    <Autosuggest value={v} options={options} ariaLabel={label} enteredTextLabel={(t) => `Use "${t}"`}
      placeholder="Choose a model" empty="No models found — type a model id"
      onChange={({ detail }) => onChange(detail.value.trim())} />
  );
  const whenOf = (r: Rule) => ("matches" in r ? "matches" : "inputTokensOver");
  const editRule = (i: number, r: Rule) => write({ route: route.map((x, j) => (j === i ? r : x)) });
  return (
    <SpaceBetween size="m">
      <FormField label="Pick the model by rule"
        description="Checked in order for every model call; the first rule that matches picks the model, else this agent's model is used. Input size is estimated at 4 characters per token.">
        <AttributeEditor
          items={route}
          addButtonText="Add a rule" removeButtonText="Remove"
          disableAddButton={route.length >= MAX_RULES}
          empty="No rules: every call uses this agent's model."
          onAddButtonClick={() => write({ route: [...route, { model: "", inputTokensOver: 20000 }] })}
          onRemoveButtonClick={({ detail }) => write({ route: route.filter((_, i) => i !== detail.itemIndex) })}
          definition={[
            { label: "When", control: (r, i) => (
              <Select selectedOption={WHEN.find((w) => w.value === whenOf(r)) ?? WHEN[0]} options={WHEN}
                ariaLabel={`rule ${i + 1} condition`}
                onChange={({ detail }) => {
                  const rest = Object.fromEntries(Object.entries(r).filter(([k]) => k !== "inputTokensOver" && k !== "matches"));
                  editRule(i, detail.selectedOption.value === "matches"
                    ? { ...rest, matches: "" } : { ...rest, inputTokensOver: 20000 });
                }} />
            ) },
            { label: "Value", control: (r, i) => (whenOf(r) === "matches" ? (
              <Input value={String(r.matches ?? "")} placeholder="invoice|refund" ariaLabel={`rule ${i + 1} pattern`}
                onChange={({ detail }) => editRule(i, { ...r, matches: detail.value })} />
            ) : (
              <Input value={String(r.inputTokensOver ?? "")} inputMode="numeric" ariaLabel={`rule ${i + 1} tokens`}
                onChange={({ detail }) => editRule(i, {
                  ...r, inputTokensOver: /^\d+$/.test(detail.value) ? Number(detail.value) : detail.value })} />
            )) },
            { label: "Use model", control: (r, i) => model(String(r.model ?? ""),
              (m) => editRule(i, { ...r, model: m }), `rule ${i + 1} model`) },
          ]}
        />
      </FormField>
      <FormField label="Fallback models"
        description="Tried in order when the model is busy or not ready (throttled, unavailable, still loading). Never on any other error.">
        <AttributeEditor
          items={fallback}
          addButtonText="Add a fallback model" removeButtonText="Remove"
          disableAddButton={fallback.length >= MAX_FALLBACK}
          empty="No fallback: a busy model fails the step."
          onAddButtonClick={() => write({ fallback: [...fallback, ""] })}
          onRemoveButtonClick={({ detail }) => write({ fallback: fallback.filter((_, i) => i !== detail.itemIndex) })}
          definition={[
            { label: "Model", control: (m, i) => model(m,
              (v) => write({ fallback: fallback.map((x, j) => (j === i ? v : x)) }), `fallback ${i + 1}`) },
          ]}
        />
      </FormField>
    </SpaceBetween>
  );
}
