/** Which models an agent is offered, from its own settings, and the Problems when its
 *  model cannot do what they ask. */
import { describe, expect, it } from "vitest";

import { capsLabel, fits, modelIssues, needsOf, type ModelOption } from "./models";

const claude: ModelOption = { id: "us.claude", name: "Claude", provider: "Anthropic", vision: true, tools: true };
const oss: ModelOption = { id: "gpt-oss", name: "gpt-oss", provider: "OpenAI", vision: false, tools: true };
const gemma: ModelOption = { id: "gemma", name: "Gemma", provider: "Google", vision: true, tools: false };
const fable: ModelOption = { id: "fable", name: "Fable", provider: "Anthropic", vision: true, tools: true, note: "needs the opt-in" };

describe("the model picker", () => {
  it("reads what an agent needs from its settings, not its prompt", () => {
    expect(needsOf({})).toEqual({ vision: false, tools: false });
    expect(needsOf({ vision: { from: ["artist"] } })).toEqual({ vision: true, tools: false });
    expect(needsOf({ vision: { from: [] }, toolMode: "model" })).toEqual({ vision: false, tools: true });
  });

  it("offers every model to a text agent, and only capable ones otherwise", () => {
    const all = [claude, oss, gemma, fable];
    expect(all.filter((m) => fits(m, needsOf({}))).length).toBe(4);
    expect(all.filter((m) => fits(m, needsOf({ vision: { from: ["a"] } }))).map((m) => m.id)).toEqual(["us.claude", "gemma", "fable"]);
    expect(all.filter((m) => fits(m, needsOf({ toolMode: "model" }))).map((m) => m.id)).toEqual(["us.claude", "gpt-oss", "fable"]);
    // A model with no tags (an older API, a typed id) is not hidden.
    expect(fits({ id: "x", name: "x", provider: "" }, { vision: true, tools: true })).toBe(true);
    expect(capsLabel(gemma)).toBe("reads images · no tool calls");
    expect(capsLabel(fable)).toBe("reads images · calls tools · needs data-retention opt-in");
  });

  it("lists an agent whose model cannot do what it asks as a problem", () => {
    const wf = { orchestrator: { defaultModel: "gpt-oss" }, agents: {
      checker: { vision: { from: ["artist"] } },
      pricer: { model: "gemma", toolMode: "model" },
      fine: { model: "us.claude", vision: { from: ["artist"] }, toolMode: "model" },
      typed: { model: "my.import", toolMode: "model" },
      story: { model: "fable" },
    } };
    expect(modelIssues(wf, [claude, oss, gemma, fable]).map((i) => [i.where.kind === "agent" ? i.where.id : "", i.severity]))
      .toEqual([["checker", "error"], ["pricer", "error"], ["story", "warning"]]);
  });
});
