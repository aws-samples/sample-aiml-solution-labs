import { describe, expect, it } from "vitest";

import { demoVideoUrl } from "./demoVideo";

describe("demoVideoUrl", () => {
  const url = "https://videos.example.com/demo";
  it("is the configured https link on a control-plane console", () => {
    expect(demoVideoUrl({ consoleMode: "builder", demoVideoUrl: url })).toBe(url);
  });
  it("keeps a link's #fragment, whole and unchanged", () => {
    const hashed = "https://amazon.awsapps.com/workdocs-amazon/index.html#/document/599976615066782999eeb979bf63d7fa2b7eed78bf6e20390b414d6d4cd0b9db";
    expect(demoVideoUrl({ consoleMode: "builder", demoVideoUrl: hashed })).toBe(hashed);
  });
  it("is nothing on a build's own app, when unset, or for anything but https", () => {
    expect(demoVideoUrl({ consoleMode: "app", demoVideoUrl: url })).toBeUndefined();
    expect(demoVideoUrl({ demoVideoUrl: url })).toBeUndefined();
    expect(demoVideoUrl({ consoleMode: "builder" })).toBeUndefined();
    expect(demoVideoUrl({ consoleMode: "builder", demoVideoUrl: "" })).toBeUndefined();
    for (const bad of ["http://videos.example.com/demo", "javascript:alert(1)", "//videos.example.com", "not a url"]) {
      expect(demoVideoUrl({ consoleMode: "builder", demoVideoUrl: bad })).toBeUndefined();
    }
  });
});
