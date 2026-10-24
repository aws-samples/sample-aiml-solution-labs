/**
 * Config-plane tests for the CDK path — the TypeScript counterpart to
 * orchestrator/tests/ (pytest). See test/README.md.
 *
 * Run from orchestrator/cdk with: npm test
 * No AWS credentials and no Docker: the stack tests synthesize with a stubbed
 * container image asset, and everything else is a pure function.
 */
module.exports = {
  testEnvironment: "node",
  roots: ["<rootDir>/test"],
  testMatch: ["**/*.test.ts"],
  transform: {
    "^.+\\.ts$": ["ts-jest", { tsconfig: "<rootDir>/tsconfig.test.json" }],
  },
  // CDK synth in the stack tests is the slow part; everything else is instant.
  testTimeout: 60000,
  // One file at a time. Each stack synth bundles the UI with `npm ci && npm run build` in
  // ../web (the UiDeploy asset's local bundling), and npm ci deletes node_modules first:
  // two workers doing it at once broke each other's builds ("Failed to bundle asset") and
  // left ../web/node_modules empty.
  maxWorkers: 1,
};
