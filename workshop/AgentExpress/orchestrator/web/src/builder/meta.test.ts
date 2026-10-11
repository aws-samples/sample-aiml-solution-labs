import { describe, expect, it } from "vitest";
import { imageModelOf, vocab } from "./meta";

describe("an image agent's image model", () => {
  it("is the framework default when the agent sets none", () => {
    const got = imageModelOf({ output: "image", image: { aspectRatio: "16:9" } });
    expect(got).toEqual({ id: vocab("imageModels")[0], isDefault: true });
    expect(got?.id).toBeTruthy();
  });
  it("is the agent's own image.model when it sets one", () => {
    const own = vocab("imageModels").at(-1)!;
    expect(imageModelOf({ output: "image", image: { model: own } })).toEqual({ id: own, isDefault: false });
  });
  it("is null for an agent that writes text", () => {
    expect(imageModelOf({ name: "Writer" })).toBeNull();
    expect(imageModelOf(undefined)).toBeNull();
  });
});
