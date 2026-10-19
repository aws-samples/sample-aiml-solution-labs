/** The model picker with a LiteLLM proxy: its models are offered "via LiteLLM" beside
 *  Bedrock's, and an agent on a litellm/ model the deployment cannot call is a Problem. */
import { renderHook, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

let litellm: "on" | "off" | "broken" = "on";
vi.mock("../api", () => {
  class ApiError extends Error {
    constructor(readonly status: number, message: string) { super(message); }
  }
  return {
    ApiError,
    api: {
      get: async (url: string) => {
        if (url === "/api/models") {
          return { models: [{ id: "us.claude", name: "Claude", provider: "Anthropic", vision: true, tools: true }] };
        }
        if (url === "/api/models/litellm") {
          if (litellm === "off") throw new ApiError(404, "LiteLLM is not set up on this deployment");
          if (litellm === "broken") throw new ApiError(502, "bad gateway");
          return { configured: true, models: [{ id: "litellm/gpt-4o", name: "gpt-4o", provider: "LiteLLM", via: "litellm" }] };
        }
        throw new Error(url);
      },
    },
  };
});

import { capsLabel, fits, modelIssues, resetModelsForTests, useModels, type ModelOption } from "./models";

const gpt: ModelOption = { id: "litellm/gpt-4o", name: "gpt-4o", provider: "LiteLLM", via: "litellm" };
const wf = (model: string) => ({ agents: { a: { model }, b: {}, t: { runtime: "task", model } } });

beforeEach(() => resetModelsForTests());

describe("the LiteLLM models in the picker", () => {
  it("are listed after Bedrock's, tagged via LiteLLM, and offered to any agent", async () => {
    litellm = "on";
    const { result } = renderHook(() => useModels());
    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.models.map((m) => m.id)).toEqual(["us.claude", "litellm/gpt-4o"]);
    expect(result.current.litellm).toBe(true);
    expect(capsLabel(result.current.models[1])).toBe("via LiteLLM");
    expect(fits(gpt, { vision: true, tools: true })).toBe(true);
  });

  it("knows LiteLLM is off from a 404, and does not guess from any other failure", async () => {
    litellm = "off";
    const off = renderHook(() => useModels());
    await waitFor(() => expect(off.result.current.loading).toBe(false));
    expect(off.result.current.litellm).toBe(false);
    expect(off.result.current.models.map((m) => m.id)).toEqual(["us.claude"]);
    resetModelsForTests();
    litellm = "broken";
    const broken = renderHook(() => useModels());
    await waitFor(() => expect(broken.result.current.loading).toBe(false));
    expect(broken.result.current.litellm).toBeUndefined();
  });
});

describe("Problems for litellm/ models (mirrors bff/litellm_models.py problems)", () => {
  it("warns when LiteLLM is not set up on this deployment", () => {
    expect(modelIssues(wf("litellm/gpt-4o"), [], false)).toEqual([{
      severity: "warning", where: { kind: "agent", id: "a" }, path: "agents.a.model",
      message: "litellm/gpt-4o is a LiteLLM model, and LiteLLM is not set up on this deployment: deploy with a LiteLLM proxy (DEPLOYMENT.md), or pick a Bedrock model",
    }]);
  });
  it("warns on a name the proxy does not serve, and says nothing it cannot know", () => {
    expect(modelIssues(wf("litellm/gpt-4o"), [gpt], true)).toEqual([]);
    expect(modelIssues(wf("litellm/gpt-9"), [gpt], true).map((i) => i.message))
      .toEqual(["the LiteLLM proxy does not list gpt-9: pick one it serves"]);
    expect(modelIssues(wf("litellm/gpt-9"), [], undefined)).toEqual([]);
    expect(modelIssues(wf("litellm/gpt-9"), [], true)).toEqual([]);
  });
});
