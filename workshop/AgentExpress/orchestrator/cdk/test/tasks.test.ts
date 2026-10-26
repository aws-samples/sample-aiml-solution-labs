/**
 * Task steps (runtime "task", lib/tasks.ts): the runtime role gets the Code Interpreter
 * sandbox only when a code step exists and Textract only when an extract step does; the
 * shipped workflow gets neither; an unknown kind fails the synth. Same as terraform/tasks.tf.
 */
import * as cdk from "aws-cdk-lib";
import { Template } from "aws-cdk-lib/assertions";
import * as fs from "fs";
import * as path from "path";
import { OrchestratorStack } from "../lib/orchestrator-stack";
import { taskKindsOf, taskStatements, validateTasks } from "../lib/tasks";
const ORCH_ROOT = path.join(__dirname, "..", "..");
const shipped = require(`${ORCH_ROOT}/app/workflow.json`);
function synth(workflow: any): Template {
  const app = new cdk.App();
  const stack = new OrchestratorStack(app, "TestStack", {
    env: { account: "123456789012", region: "us-east-1" },
    agentName: "multiagent_orchestrator",
    modelId: "us.anthropic.claude-haiku-4-5-20251001-v1:0",
    memoryEventExpiryDays: 30,
    idp: "cognito", createCognito: true, cognitoUserPoolId: "", cognitoClientId: "",
    cognitoDomainPrefix: "", auth0Domain: "", auth0ClientId: "",
    enableGateway: true, gatewayClientId: "", gatewayClientSecret: "dummy", gatewayAudience: "",
    toolApiKeys: {}, a2aTokens: {}, transactionSearchIndexingPercentage: 100,
    workflow,
  } as any);
  return Template.fromStack(stack);
}
const withTasks = (tasks: Record<string, any>) => ({
  ...shipped,
  agents: { ...shipped.agents, ...tasks },
  steps: [...shipped.steps, ...Object.keys(tasks).map((id) => ({ agent: id }))],
});
const runtimeStatements = (t: Template) => Object.entries<any>(t.findResources("AWS::IAM::Policy"))
  .filter(([k]) => k.startsWith("RuntimeRole"))
  .flatMap(([, p]) => p.Properties.PolicyDocument.Statement as any[]);
const CODE = { crunch: { name: "Crunch", runtime: "task", task: { kind: "code", code: "print(1)" } } };
const EXTRACT = { read_docs: { name: "Read", runtime: "task", attachments: true, task: { kind: "extract" } } };

describe("task steps", () => {
  it("knows which kinds a workflow uses, and refuses one it does not run", () => {
    expect(taskKindsOf(shipped)).toEqual([]);
    expect(taskKindsOf(withTasks({ ...CODE, ...EXTRACT }))).toEqual(["code", "extract"]);
    expect(() => validateTasks(withTasks({ x: { name: "X", runtime: "task", task: { kind: "lambda" } } })))
      .toThrow(/task step\(s\) x: task\.kind must be one of tool, code, extract, output, deliver/);
    expect(taskStatements(withTasks({ t: { name: "T", runtime: "task", task: { kind: "tool", tool: "kb" } } }), "us-east-1")).toEqual([]);
  });
  it("gives the shipped workflow nothing new", () => {
    const all = JSON.stringify(runtimeStatements(synth(shipped)));
    expect(all).not.toContain("TaskCodeInterpreter");
    expect(all).not.toContain("TaskTextract");
  });
  it("grants the AWS-managed sandbox for a code step and Textract for an extract step, nothing else", () => {
    const st = runtimeStatements(synth(withTasks({ ...CODE, ...EXTRACT })));
    const code = st.find((s) => s.Sid === "TaskCodeInterpreter");
    expect(code.Action).toEqual(["bedrock-agentcore:StartCodeInterpreterSession",
      "bedrock-agentcore:InvokeCodeInterpreter", "bedrock-agentcore:StopCodeInterpreterSession"]);
    expect(code.Resource).toBe("arn:aws:bedrock-agentcore:us-east-1:aws:code-interpreter/aws.codeinterpreter.v1");
    const textract = st.find((s) => s.Sid === "TaskTextract");
    expect(textract.Action).toEqual(["textract:AnalyzeDocument", "textract:StartDocumentAnalysis",
      "textract:GetDocumentAnalysis"]);
    expect(textract.Resource).toBe("*");
    // The extract step reads the run's files from S3 itself (attachments): GetObject on runs/*.
    expect(JSON.stringify(st)).toContain("s3:GetObject");
    const onlyCode = JSON.stringify(runtimeStatements(synth(withTasks(CODE))));
    expect(onlyCode).toContain("TaskCodeInterpreter");
    expect(onlyCode).not.toContain("TaskTextract");
  });
  it("matches the Terraform path", () => {
    const tf = fs.readFileSync(path.join(ORCH_ROOT, "terraform", "tasks.tf"), "utf8");
    for (const s of ['Sid    = "TaskCodeInterpreter"', 'Sid    = "TaskTextract"',
      '"bedrock-agentcore:StartCodeInterpreterSession", "bedrock-agentcore:InvokeCodeInterpreter", "bedrock-agentcore:StopCodeInterpreterSession"',
      '"arn:aws:bedrock-agentcore:${var.region}:aws:code-interpreter/aws.codeinterpreter.v1"',
      '"textract:AnalyzeDocument", "textract:StartDocumentAnalysis", "textract:GetDocumentAnalysis"',
      'Resource = "*"', 'contains(local.task_kinds, "code")', 'contains(local.task_kinds, "extract")',
      "local.vocab.taskKinds.values", "task.kind must be one of"]) {
      expect(tf).toContain(s);
    }
  });
});
