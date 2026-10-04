import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig(({ command, mode }) => {
  // `vite build` IS a production build, whatever shell launched it.
  //
  // Vite reads the AMBIENT NODE_ENV — not the mode — to decide `isProduction`, and
  // @vitejs/plugin-react reads that to choose between the production JSX runtime and
  // `jsxDEV`, which embeds a source location in every element. `process.env.NODE_ENV`
  // separately decides which copy of React gets bundled. Both are downstream of one
  // environment variable that the IaC has no control over, because it shells out to
  // `npm run build` from whatever shell invoked the deploy — and `test` is the value
  // that actually shows up, since that is what jest sets.
  //
  // The result was a 1,376 kB bundle instead of 1,113 kB, carrying React's development
  // build and its warning machinery. Nothing would have caught it: the page renders
  // identically, only the size and the speed change. Pinning it HERE rather than
  // exporting the variable at each call site means the artifact is correct whoever
  // builds it, which is the property that matters.
  if (command === "build") process.env.NODE_ENV = "production";

  return {
    plugins: [react()],
    define: {
      "process.env.NODE_ENV": JSON.stringify(
        mode === "production" ? "production" : "development"),
    },
    build: {
      // Both IaC paths upload this directory. Keeping it inside web/ means the
      // Terraform `aws_s3_object` set and CDK's `Source.asset` point at one place.
      outDir: "dist",
      emptyOutDir: true,
      // Hashed asset names so a CloudFront cache cannot serve a stale bundle against
      // a fresh index.html. index.html itself is uploaded with no-cache by the IaC.
      assetsInlineLimit: 0,
      sourcemap: false,
    },
    server: {
      port: 5173,
      // `npm run dev` against a deployed API: the BFF is on a different origin, so the
      // dev server proxies /api to it rather than fighting CORS.
      proxy: process.env.VITE_API_BASE
        ? { "/api": { target: process.env.VITE_API_BASE, changeOrigin: true } }
        : undefined,
    },
    test: {
      environment: "jsdom",
      globals: true,
      include: ["src/**/*.test.ts", "src/**/*.test.tsx"],
    },
  };
}) as never;
