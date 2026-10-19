/** The canvas's "Show" menu: which kinds of component the agent cards list. Every kind is on
 *  by default, so everything attached is visible; a reader turns off what they do not need.
 *  Plain HTML (a disclosure of checkboxes), so it works by keyboard and screen reader.
 *  Nothing here changes the workflow. */
import { COMPONENT_TYPES, type ComponentKind } from "./components";

export function ComponentMenu({ counts, shown, onChange, rail }: {
  counts: Record<ComponentKind, number>;
  shown: ComponentKind[];
  onChange: (next: ComponentKind[]) => void;
  /** The AgentCore rail's switch (Rail.tsx), off by default; absent on a read-only canvas. */
  rail?: { on: boolean; onChange: (on: boolean) => void };
}) {
  const toggle = (id: ComponentKind) =>
    onChange(shown.includes(id) ? shown.filter((x) => x !== id) : [...shown, id]);
  const hidden = COMPONENT_TYPES.length - shown.length;
  return (
    <details className="axb-layer-menu">
      <summary aria-label={`Show on agents: ${shown.length} of ${COMPONENT_TYPES.length} kinds shown`}>
        Show{hidden ? ` (${hidden} hidden)` : ""}
      </summary>
      <fieldset>
        <legend>Show on agents</legend>
        {COMPONENT_TYPES.map((t) => (
          <label key={t.id} title={t.hint}>
            <input type="checkbox" checked={shown.includes(t.id)} onChange={() => toggle(t.id)} />
            <span className={`axb-comp-tag axb-comp-${t.id}`} aria-hidden="true">{t.tag}</span>
            {` ${t.label}`}
            <span className="axb-layer-n">{counts[t.id] === 0 ? " (none)" : ` (${counts[t.id]})`}</span>
          </label>
        ))}
        {rail ? (
          <label title="Gateway, Memory, Policy engine, Guardrail, Identity, Observability and Triggers, with counts">
            <input type="checkbox" checked={rail.on} onChange={() => rail.onChange(!rail.on)} />
            {" AgentCore rail"}
          </label>
        ) : null}
      </fieldset>
    </details>
  );
}
