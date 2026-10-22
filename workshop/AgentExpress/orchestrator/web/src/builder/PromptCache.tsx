/** The agent panel's "Prompt caching" section: `agents.<id>.promptCache` (Bedrock Converse
 *  cache points, app/common/prompt_cache.py). What each model supports comes from the AWS
 *  table copied into app/vocabulary.json `promptCacheModels`
 *  (https://docs.aws.amazon.com/bedrock/latest/userguide/prompt-caching.html); an option the
 *  agent's model does not support is disabled with the reason. */
import Box from "@cloudscape-design/components/box";
import ExpandableSection from "@cloudscape-design/components/expandable-section";
import FormField from "@cloudscape-design/components/form-field";
import SegmentedControl from "@cloudscape-design/components/segmented-control";
import SpaceBetween from "@cloudscape-design/components/space-between";
import Toggle from "@cloudscape-design/components/toggle";
import { vocabExtra } from "./meta";
import { agentModel, baseId, cacheModel, copiedOn, notOnConverse } from "./promptCacheCheck";

type Json = unknown;
type Flag = "system" | "tools" | "messages";

/** Why the model takes no cache point at all, or "" when it does. */
export function unsupportedReason(model: string): string {
  if (cacheModel(model)) return "";
  if (model.startsWith("litellm/")) return "LiteLLM models are called without cache points.";
  if (notOnConverse(model)) {
    return "Explicit caching for this model needs the Responses API or Chat Completions, and AgentExpress calls models through Converse.";
  }
  const implicit = vocabExtra<{ prefix?: string; reason?: string }>("promptCacheModels", "implicitOnly");
  if (implicit?.prefix && baseId(model).startsWith(implicit.prefix)) return String(implicit.reason ?? "");
  return `This model has no documented explicit prompt caching through Converse (AWS table copied ${copiedOn()}).`;
}

export function PromptCacheSettings({ agent, orchestrator, onAgent, disabledReason }: {
  agent: Record<string, Json>;
  orchestrator: unknown;
  onAgent: (next: Record<string, Json>) => void;
  /** Set when caching does not apply to this kind of agent at all (e.g. a harness). */
  disabledReason?: string;
}) {
  const pc = (agent.promptCache && typeof agent.promptCache === "object" ? agent.promptCache : {}) as Record<string, Json>;
  const model = agentModel(agent, orchestrator);
  const row = disabledReason ? null : cacheModel(model);
  const off = disabledReason || unsupportedReason(model);
  const toolCalls = agent.toolMode === "model";
  const set = (key: string, v: Json | undefined) => {
    const next = { ...pc };
    if (v === undefined || v === false) delete next[key]; else next[key] = v;
    // Turning the last option off turns caching off entirely (no stray ttl left behind).
    if (key !== "ttl" && !["system", "tools", "messages"].some((k) => next[k] === true)) delete next.ttl;
    const out = { ...agent };
    if (Object.keys(next).length) out.promptCache = next; else delete out.promptCache;
    onAgent(out);
  };
  const flag = (key: Flag, label: string, why: string) => (
    <Toggle checked={pc[key] === true} disabled={Boolean(why) && pc[key] !== true}
      onChange={({ detail }) => set(key, detail.checked || undefined)}
      description={why || undefined}>
      {label}
    </Toggle>
  );
  // OpenAI GPT-5.6+ (vocabulary openaiExplicit): the system prompt of single calls only,
  // through InvokeModel, 30 minutes (the only documented TTL).
  const openai = row?.api === "InvokeModel";
  const openaiWhy = openai
    ? String(vocabExtra<{ reason?: string }>("promptCacheModels", "openaiExplicit")?.reason ?? "")
    : "";
  const noTools = openaiWhy || (row && !row.fields.includes("tools") ? `${row.name} caches the system prompt and messages only.` : "");
  const no1h = row && !row.ttls.includes("1h") ? `${row.name} caches for 5 minutes only.` : "";
  const loopOnly = toolCalls ? "" : "Applies to agents that call tools themselves (toolMode \"model\").";
  return (
    <ExpandableSection headerText="Prompt caching" variant="container"
      headerDescription="Bedrock cache points for this agent's model calls, so a repeated prefix is billed at the cache-read rate. Off unless set.">
      <SpaceBetween size="m">
        {off ? (
          <Box variant="small" color="text-status-inactive" data-testid="prompt-cache-reason">{off}</Box>
        ) : row ? (
          <Box variant="small" color="text-body-secondary" data-testid="prompt-cache-minimum">
            {openai
              ? `${row.name}: at least ${row.minTokensText} tokens per cache breakpoint. The system prompt is cached through InvokeModel; cache writes are billed at 1.25x the input rate.`
              : `${row.name}: at least ${row.minTokensText} tokens per cache checkpoint, counted over tools, system and messages together. Below it the call still runs, uncached.`}
          </Box>
        ) : null}
        {flag("system", "System prompt", off)}
        {flag("tools", "Tool definitions", off || noTools || loopOnly)}
        {flag("messages", "Conversation so far", off || openaiWhy || loopOnly)}
        {openai ? (
          <FormField label="Cache TTL" description={`${row?.name} caches for at least 30 minutes.`}>
            <SegmentedControl selectedId="30m" options={[{ id: "30m", text: "30 minutes" }]}
              onChange={() => set("ttl", undefined)} label="Cache TTL" />
          </FormField>
        ) : (
        <FormField label="Cache TTL" description={off ? undefined : no1h || "How long a cached prefix lives after its last use."}>
          <SegmentedControl
            selectedId={pc.ttl === "1h" ? "1h" : "5m"}
            options={[
              { id: "5m", text: "5 minutes", disabled: Boolean(off) },
              { id: "1h", text: "1 hour", disabled: Boolean(off || no1h) && pc.ttl !== "1h" },
            ]}
            onChange={({ detail }) => set("ttl", detail.selectedId === "1h" ? "1h" : undefined)}
            label="Cache TTL"
          />
        </FormField>
        )}
      </SpaceBetween>
    </ExpandableSection>
  );
}
