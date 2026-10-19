/** One emoji per kind of thing, so the left panes read at a glance. Decoration only:
 *  every place that shows one marks it aria-hidden, so screen readers read the words. */

/** A tool by its `type`, or by its framework `source` for a Lambda the framework ships. */
const TOOL_TYPE: Record<string, string> = {
  kb: "📚", websearch: "🔎", mcp: "🔌", openapi: "🧩", lambda: "⚡", apigateway: "🌐", smithy: "☁️",
};
const TOOL_SOURCE: Record<string, string> = { codeInterpreter: "🧮", browser: "🧭", pricing: "💲" };

export function toolEmoji(tool: { type?: unknown; source?: unknown } | undefined): string {
  const source = typeof tool?.source === "string" ? TOOL_SOURCE[tool.source] : undefined;
  return source ?? TOOL_TYPE[String(tool?.type ?? "")] ?? "🛠️";
}

/** AgentCore features and the other things a build attaches (attach.ts kinds and the
 *  library's kinds share these). */
export const FEATURE_EMOJI: Record<string, string> = {
  tool: "🛠️", guardrail: "🛡️", memory: "🧠", policy: "📜", skill: "🎓", evaluator: "📊",
  evaluation: "📊", identity: "🔑", interceptor: "🚦", trigger: "⏰", human: "🙋",
  agent: "🤖", task: "⚙️", accounts: "☁️", teams: "👥",
};
