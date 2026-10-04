/** @vitest-environment node */

/** The build config pins NODE_ENV, and that is worth a test because the failure is
 *  invisible.
 *
 *  Runs under `node`, not jsdom: importing the config pulls in vite, which pulls in
 *  esbuild, and esbuild refuses to load where `new TextEncoder().encode("")` is not a
 *  real Uint8Array — which is exactly what jsdom provides.
 *
 *  A build launched from a shell where NODE_ENV is not "production" bundles React's
 *  development build and compiles JSX through `jsxDEV`. The page renders identically —
 *  only the bundle size and the runtime speed change — so no rendering test, no
 *  screenshot and no deployment check would notice. It was found by comparing the bundle
 *  size printed by `npx jest` (which sets NODE_ENV=test) against a local build: 1,376 kB
 *  against 1,113 kB.
 *
 *  Both IaC paths also export NODE_ENV=production, but they are not the only way this
 *  gets built. The config is. */

import { afterEach, describe, expect, it } from "vitest";

import config from "../vite.config";

type Factory = (env: { command: "build" | "serve"; mode: string }) => {
  define: Record<string, string>;
};

const factory = config as unknown as Factory;
const original = process.env.NODE_ENV;

afterEach(() => {
  process.env.NODE_ENV = original;
});

describe("the production build", () => {
  it("forces NODE_ENV=production even when the shell says otherwise", () => {
    process.env.NODE_ENV = "test";
    factory({ command: "build", mode: "production" });
    // Vite reads this to set `isProduction`, which @vitejs/plugin-react reads to pick
    // the production JSX runtime over jsxDEV.
    expect(process.env.NODE_ENV).toBe("production");
  });

  it("bundles the production copy of React", () => {
    const { define } = factory({ command: "build", mode: "production" });
    expect(define["process.env.NODE_ENV"]).toBe('"production"');
  });

  it("leaves the dev server alone", () => {
    process.env.NODE_ENV = "development";
    const { define } = factory({ command: "serve", mode: "development" });
    expect(process.env.NODE_ENV).toBe("development");
    expect(define["process.env.NODE_ENV"]).toBe('"development"');
  });
});
