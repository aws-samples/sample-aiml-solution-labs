/** The agent panel's Prompt caching section: options follow the agent's model (the AWS
 *  table in app/vocabulary.json promptCacheModels). */
import { act, render } from "@testing-library/react";
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { describe, expect, it, vi } from "vitest";
import { PromptCacheSettings } from "./PromptCache";

const HAIKU = "us.anthropic.claude-haiku-4-5-20251001-v1:0";
const NOVA = "us.amazon.nova-lite-v1:0";

function toggles() {
  return createWrapper(document.body).findAllToggles();
}
function getByTestIdIn(root: HTMLElement, id: string) {
  return root.querySelector(`[data-testid="${id}"]`)?.textContent ?? "";
}

describe("PromptCacheSettings", () => {
  it("shows the model's documented minimum and writes promptCache", async () => {
    const onAgent = vi.fn();
    const { getByTestId } = render(
      <PromptCacheSettings agent={{ name: "A", model: HAIKU, toolMode: "model" }} orchestrator={{}} onAgent={onAgent} />);
    expect(getByTestId("prompt-cache-minimum").textContent).toContain("4,096 tokens per cache checkpoint");
    const [system, tools, messages] = toggles();
    expect(tools.findNativeInput().getElement().disabled).toBe(false);
    expect(messages.findNativeInput().getElement().disabled).toBe(false);
    await act(async () => { system.findNativeInput().click(); });
    expect(onAgent).toHaveBeenLastCalledWith({ name: "A", model: HAIKU, toolMode: "model", promptCache: { system: true } });
  });

  it("disables tool definitions and 1 hour on Nova, with the reason", () => {
    render(<PromptCacheSettings agent={{ name: "A", model: NOVA, toolMode: "model" }} orchestrator={{}} onAgent={vi.fn()} />);
    const [system, tools] = toggles();
    expect(system.findNativeInput().getElement().disabled).toBe(false);
    expect(tools.findNativeInput().getElement().disabled).toBe(true);
    expect(document.body.textContent).toContain("Amazon Nova Lite caches the system prompt and messages only.");
    expect(document.body.textContent).toContain("1K tokens per cache checkpoint");
    expect(document.body.textContent).toContain("Amazon Nova Lite caches for 5 minutes only.");
  });

  it("disables everything on a model with no explicit caching, and for OpenAI says why", () => {
    const { getByTestId, unmount } = render(
      <PromptCacheSettings agent={{ name: "A", model: "openai.gpt-oss-120b-1:0" }} orchestrator={{}} onAgent={vi.fn()} />);
    expect(getByTestId("prompt-cache-reason").textContent).toContain("OpenAI models cache implicitly");
    expect(toggles().every((t) => t.findNativeInput().getElement().disabled)).toBe(true);
    unmount();
    // GPT-6 Sol is listed in the account but not in the AWS explicit-caching table.
    render(<PromptCacheSettings agent={{ name: "A", model: "us.openai.gpt-6-sol" }} orchestrator={{}} onAgent={vi.fn()} />);
    expect(getByTestIdIn(document.body, "prompt-cache-reason")).toContain("OpenAI models cache implicitly");
    expect(toggles().every((t) => t.findNativeInput().getElement().disabled)).toBe(true);
  });
  it("on OpenAI GPT-5.6 Sol enables the system prompt only, 30 minutes, 1,024 minimum", async () => {
    const onAgent = vi.fn();
    const agent = { name: "A", model: "us.openai.gpt-5.6-sol", toolMode: "model" };
    const { getByTestId } = render(<PromptCacheSettings agent={agent} orchestrator={{}} onAgent={onAgent} />);
    expect(getByTestId("prompt-cache-minimum").textContent).toContain("GPT-5.6 Sol: at least 1,024 tokens per cache breakpoint");
    const [system, tools, messages] = toggles();
    expect(system.findNativeInput().getElement().disabled).toBe(false);
    expect(tools.findNativeInput().getElement().disabled).toBe(true);
    expect(messages.findNativeInput().getElement().disabled).toBe(true);
    expect(document.body.textContent).toContain("Tool-calling turns go through Converse");
    expect(document.body.textContent).toContain("30 minutes");
    expect(document.body.textContent).not.toContain("1 hour");
    await act(async () => { system.findNativeInput().click(); });
    expect(onAgent).toHaveBeenLastCalledWith({ ...agent, promptCache: { system: true } });
  });

  it("uses the build's default model when the agent names none, and turns off cleanly", async () => {
    const onAgent = vi.fn();
    render(<PromptCacheSettings agent={{ name: "A", promptCache: { system: true, ttl: "1h" } }}
      orchestrator={{ defaultModel: HAIKU }} onAgent={onAgent} />);
    await act(async () => { toggles()[0].findNativeInput().click(); });
    expect(onAgent).toHaveBeenLastCalledWith({ name: "A" });
  });

  it("is disabled for a harness agent with the reason", () => {
    render(<PromptCacheSettings agent={{ name: "A", runtime: "harness", model: HAIKU }} orchestrator={{}}
      onAgent={vi.fn()} disabledReason="No caching setting on a harness." />);
    expect(document.body.textContent).toContain("No caching setting on a harness.");
    expect(toggles().every((t) => t.findNativeInput().getElement().disabled)).toBe(true);
  });
});
