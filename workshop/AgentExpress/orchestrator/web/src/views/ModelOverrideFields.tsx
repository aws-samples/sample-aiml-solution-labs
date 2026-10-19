/** Start run's model choice (orchestrator.models.overrides; bff/model_override.py): one model
 *  for every agent, or one per agent, picked only from the workflow's list. Shown only to a
 *  person granted `override`; the server checks the grant and the list again. */
import ExpandableSection from "@cloudscape-design/components/expandable-section";
import FormField from "@cloudscape-design/components/form-field";
import Select from "@cloudscape-design/components/select";
import SpaceBetween from "@cloudscape-design/components/space-between";

const OWN = { value: "", label: "Its own model" };

export function ModelOverrideFields({ models, agents, all, perAgent, onAll, onAgent }: {
  models: string[];
  /** The agents whose model may be picked (main and dedicated placements). */
  agents: { id: string; name: string }[];
  all: string;
  perAgent: Record<string, string>;
  onAll: (m: string) => void;
  onAgent: (id: string, m: string) => void;
}) {
  const listed = models.map((m) => ({ value: m, label: m }));
  const options = [OWN, ...listed];
  const everyOptions = [{ value: "", label: "Each agent's own model" }, ...listed];
  const pick = (opts: typeof options, m: string) => opts.find((o) => o.value === m) ?? opts[0];
  return (
    <SpaceBetween size="s">
      <FormField label="Model for every agent (optional)"
        description="Run every agent on this model instead of its own, for this run only. No fallback model is tried.">
        <Select selectedOption={pick(everyOptions, all)} options={everyOptions}
          ariaLabel="Model for every agent" onChange={({ detail }) => onAll(detail.selectedOption.value ?? "")} />
      </FormField>
      {agents.length ? (
        <ExpandableSection headerText="Model per agent" variant="footer">
          <SpaceBetween size="s">
            {agents.map((a) => (
              <FormField key={a.id} label={a.name}>
                <Select selectedOption={pick(options, perAgent[a.id] ?? "")} options={options}
                  ariaLabel={`Model for ${a.name}`} onChange={({ detail }) => onAgent(a.id, detail.selectedOption.value ?? "")} />
              </FormField>
            ))}
          </SpaceBetween>
        </ExpandableSection>
      ) : null}
    </SpaceBetween>
  );
}
