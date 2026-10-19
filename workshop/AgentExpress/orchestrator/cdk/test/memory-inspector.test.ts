/** The memory inspector (bff/memories.py), synthesized: the BFF may read and delete
 *  records in the LONG-TERM memories only (the semantic one and each named one), never the
 *  checkpointer's, and is told their ids. Terraform's side is held to the same actions and
 *  resources by tests/test_memory_inspector.py. Synthesized with the Lab 5 context
 *  (idp cognito, createCognito, enableGateway). */
import * as fs from "fs";
import * as path from "path";
import * as cdk from "aws-cdk-lib";
import { Template } from "aws-cdk-lib/assertions";
import { OrchestratorStack } from "../lib/orchestrator-stack";

const ORCH = path.resolve(__dirname, "..", "..");
const SAMPLE = JSON.parse(fs.readFileSync(path.join(ORCH, "app", "workflow.json"), "utf8"));

function synth(wf: any, extra: Record<string, any> = {}): Template {
  const app = new cdk.App();
  const stack = new OrchestratorStack(app, "MemoryInspectorStack", {
    env: { account: "123456789012", region: "us-east-1" },
    agentName: "ax_12345678", modelId: "us.anthropic.claude-haiku-4-5-20251001-v1:0",
    memoryEventExpiryDays: 30, idp: "cognito", createCognito: true, cognitoUserPoolId: "",
    cognitoClientId: "", cognitoDomainPrefix: "", auth0Domain: "", auth0ClientId: "",
    enableGateway: true, gatewayClientId: "", gatewayClientSecret: "dummy", gatewayAudience: "",
    toolApiKeys: {}, a2aTokens: {}, transactionSearchIndexingPercentage: 100, workflow: wf, ...extra,
  } as any);
  return Template.fromStack(stack);
}

/** The logical id of the one resource of `type` whose Name ends with `suffix`. */
function memoryId(t: Template, suffix: string): string {
  const found = Object.entries<any>(t.findResources("AWS::BedrockAgentCore::Memory"))
    .filter(([, r]) => String(r.Properties.Name).endsWith(suffix));
  expect(found).toHaveLength(1);
  return found[0][0];
}

const statements = (t: Template, sid: string): any[] =>
  Object.values(t.findResources("AWS::IAM::Policy"))
    .flatMap((p: any) => p.Properties.PolicyDocument.Statement)
    .filter((s: any) => s.Sid === sid);

describe("the memory inspector's grant", () => {
  let t: Template;
  beforeAll(() => {
    const wf = structuredClone(SAMPLE);
    wf.memories = { shared: { strategies: ["semantic", "userPreference"] } };
    t = synth(wf);
  });

  it("reads and deletes on the semantic and named memories, as two statements", () => {
    const [read] = statements(t, "MemoryInspectorRead");
    const [del] = statements(t, "MemoryInspectorDelete");
    expect(read.Action).toEqual(["bedrock-agentcore:ListActors", "bedrock-agentcore:ListMemoryRecords",
      "bedrock-agentcore:RetrieveMemoryRecords", "bedrock-agentcore:GetMemoryRecord"]);
    expect(del.Action).toBe("bedrock-agentcore:DeleteMemoryRecord");
    const want = [memoryId(t, "_semantic"), memoryId(t, "_m_shared")]
      .map((id) => ({ "Fn::GetAtt": [id, "MemoryArn"] }));
    expect(read.Resource).toEqual(want);
    expect(del.Resource).toEqual(want);
  });

  it("never reaches the checkpointer's memory, and has no wildcard", () => {
    const checkpoint = memoryId(t, "_memory");
    for (const s of [...statements(t, "MemoryInspectorRead"), ...statements(t, "MemoryInspectorDelete")]) {
      expect(JSON.stringify(s.Resource)).not.toContain(`"${checkpoint}"`);
      expect(JSON.stringify(s.Resource)).not.toContain("*");
    }
  });

  it("is on the BFF's role only", () => {
    const bffRole = Object.entries<any>(t.findResources("AWS::Lambda::Function"))
      .find(([, f]) => String(f.Properties.FunctionName ?? "").startsWith("AgentCoreBFF-"))![1].Properties.Role;
    const holders = Object.values<any>(t.findResources("AWS::IAM::Policy"))
      .filter((p) => p.Properties.PolicyDocument.Statement.some((s: any) => s.Sid?.startsWith("MemoryInspector")));
    expect(holders).toHaveLength(1);
    expect(holders[0].Properties.Roles).toEqual([{ Ref: bffRole["Fn::GetAtt"][0] }]);
  });

  it("tells the BFF the long-term memory ids, and not the checkpointer's", () => {
    const bff = Object.values<any>(t.findResources("AWS::Lambda::Function"))
      .find((f) => String(f.Properties.FunctionName ?? "").startsWith("AgentCoreBFF-"));
    const vars = bff.Properties.Environment.Variables;
    expect(vars.SEMANTIC_MEMORY_ID).toEqual({ "Fn::GetAtt": [memoryId(t, "_semantic"), "MemoryId"] });
    expect(JSON.stringify(vars.MEMORIES)).toContain(memoryId(t, "_m_shared"));
    expect(vars.MEMORY_ID).toBeUndefined();
    expect(JSON.stringify(vars)).not.toContain(`"${memoryId(t, "_memory")}"`);
  });

  it("puts each memory route behind the JWT authorizer", () => {
    const routes = Object.values<any>(t.findResources("AWS::ApiGatewayV2::Route"))
      .filter((r) => String(r.Properties.RouteKey).includes("/api/memory"));
    expect(routes.map((r) => r.Properties.RouteKey).sort()).toEqual([
      "DELETE /api/memory/records/{id}", "GET /api/memory/agents", "GET /api/memory/namespaces",
      "GET /api/memory/records", "POST /api/memory/search"]);
    for (const r of routes) expect(r.Properties.AuthorizationType).toBe("JWT");
  });
});

describe("a control-plane console", () => {
  it("has no memory to inspect, so no grant and no ids", () => {
    const t = synth(structuredClone(SAMPLE), { consoleMode: "builder" });
    expect(statements(t, "MemoryInspectorRead")).toHaveLength(0);
    const bff = Object.values<any>(t.findResources("AWS::Lambda::Function"))
      .find((f) => String(f.Properties.FunctionName ?? "").startsWith("AgentCoreBFF-"));
    expect(bff.Properties.Environment.Variables.SEMANTIC_MEMORY_ID).toBeUndefined();
  });
});
