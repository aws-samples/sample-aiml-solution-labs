/** New tool kinds in the Build view: an API Gateway REST API, an inline or uploaded
 *  OpenAPI document, OAuth client credentials, and a KB over your own S3 or KB. */
import { describe, expect, it } from "vitest";

import { parseOpenApi } from "./Inspector";
import { addTool, newProject } from "./model";
import { validate } from "./validate";

const DOC = { openapi: "3.0.1", info: { title: "Orders", version: "1" },
  paths: { "/orders/{id}": { get: { operationId: "getOrder", responses: { 200: { description: "ok" } } } } } };

describe("tool kinds", () => {
  it("starts an API Gateway tool with a filter and SigV4, and asks only for the API id", () => {
    const r = addTool(newProject("x"), "Orders API", "apigateway");
    const tool = r.project.workflow.tools[r.key];
    expect(tool).toMatchObject({ type: "apigateway", stage: "prod", auth: "sigv4",
      toolFilters: [{ path: "/*", methods: ["GET"] }] });
    const errs = validate(r.project.workflow).filter((i) => i.severity === "error" && i.path.startsWith(`tools.${r.key}`));
    expect(errs.map((e) => e.path)).toEqual([`tools.${r.key}.restApiId`]);
  });
  it("reads an uploaded OpenAPI document, and says why it cannot use another file", () => {
    expect(parseOpenApi(JSON.stringify(DOC))).toEqual(DOC);
    expect(() => parseOpenApi("openapi: 3.0.1")).toThrow(/not JSON/);
    expect(() => parseOpenApi(JSON.stringify({ swagger: "2.0", paths: {} }))).toThrow(/not an OpenAPI 3/);
  });
  it("accepts an inline schema with OAuth client credentials as a whole tool", () => {
    const p = newProject("x");
    p.workflow.tools.ordersApi = { type: "openapi", description: "Orders", schema: DOC, auth: "oauth2",
      oauth: { clientId: "abc", scopes: ["orders.read"], tokenUrl: "https://auth.example.com/oauth2/token" } };
    expect(validate(p.workflow).filter((i) => i.path.startsWith("tools.ordersApi") && i.severity === "error")).toEqual([]);
  });
});

describe("model-chosen tool calls", () => {
  it("lets a Builder agent's model choose among its tools once it has one, and forgets it with none", async () => {
    const { bindTool, setTools } = await import("./model");
    const p = newProject("x");
    const withTool = addTool(p, "Orders API", "apigateway");
    const agent = Object.keys(withTool.project.workflow.agents)[0];
    const bound = bindTool(withTool.project, agent, withTool.key);
    expect(bound.workflow.agents[agent].toolMode).toBe("model");
    const kept = { ...bound, workflow: { ...bound.workflow, agents: { ...bound.workflow.agents,
      [agent]: { ...bound.workflow.agents[agent], toolMode: "direct" } } } };
    expect(bindTool(kept, agent, addTool(kept, "Docs", "mcp").key).workflow.agents[agent].toolMode).toBe("direct");
    expect(setTools(bound, agent, []).workflow.agents[agent].toolMode).toBeUndefined();
  });
});

describe("evaluators and memory", () => {
  it("offers the 13 built-in evaluators, marks the three it cannot score yet, and adds the agent's own", async () => {
    const { evaluatorOptions } = await import("./Features");
    const opts = evaluatorOptions([{ name: "tone", instructions: "x" }, { name: "", instructions: "" }]);
    expect(opts.filter((o) => o.value.startsWith("Builtin.")).length).toBe(13);
    expect(opts.filter((o) => "disabled" in o && o.disabled).map((o) => o.label).sort())
      .toEqual(["GoalSuccessRate", "ToolParameterAccuracy", "ToolSelectionAccuracy"]);
    expect(opts.at(-1)).toMatchObject({ label: "tone", value: "Custom.tone" });
  });
  it("offers the four memory strategies and the scopes", async () => {
    const { vocab } = await import("./meta");
    expect(vocab("memoryStrategies")).toEqual(["semantic", "summary", "userPreference", "episodic"]);
    expect(vocab("memoryScopes")).toEqual(["user", "subject", "agent", "run"]);
  });
});
