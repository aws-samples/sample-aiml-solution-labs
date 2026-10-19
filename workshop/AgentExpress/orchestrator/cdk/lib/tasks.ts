/**
 * Task steps (agents with runtime "task", app/common/task_agent.py): a step with no model
 * that calls a tool, runs a script in the Code Interpreter sandbox, reads the run's
 * documents with Textract, or writes a file. It runs in the orchestrator's own runtime, so
 * only that role gets anything, and only what a step of the workflow uses:
 *   code    -> start, use and stop sessions on the AWS-managed interpreter, nothing else;
 *   extract -> Textract's document analysis. Textract has no resource type for these
 *              actions (service authorization reference, Amazon Textract), so "*".
 * A tool step uses the Gateway like any agent, and an output step writes under runs/*,
 * which the runtime may already do. A workflow with no task step gets nothing.
 * Mirrors terraform/tasks.tf.
 */
import * as iam from "aws-cdk-lib/aws-iam";
import { values } from "./vocabulary";

/** The task kinds the workflow's steps use. */
export function taskKindsOf(workflow: any): string[] {
  return [...new Set(Object.values<any>(workflow?.agents ?? {})
    .filter((a) => a?.runtime === "task").map((a) => String(a?.task?.kind ?? "")))];
}

/** Throws at synth for a task step whose kind the framework does not run. */
export function validateTasks(workflow: any): void {
  const kinds = values("taskKinds");
  const bad = Object.entries<any>(workflow?.agents ?? {})
    .filter(([, a]) => a?.runtime === "task" && !kinds.includes(String(a?.task?.kind ?? ""))).map(([id]) => id);
  if (bad.length) {
    throw new Error(`app/workflow.json task step(s) ${bad.join(", ")}: task.kind must be one of ${kinds.join(", ")}.`);
  }
}

/** What the runtime role needs for the workflow's task steps (none without them). */
export function taskStatements(workflow: any, region: string): iam.PolicyStatement[] {
  validateTasks(workflow);
  const kinds = taskKindsOf(workflow);
  const out: iam.PolicyStatement[] = [];
  if (kinds.includes("code")) {
    out.push(new iam.PolicyStatement({
      sid: "TaskCodeInterpreter",
      actions: ["bedrock-agentcore:StartCodeInterpreterSession", "bedrock-agentcore:InvokeCodeInterpreter",
        "bedrock-agentcore:StopCodeInterpreterSession"],
      // The AWS-managed interpreter only (account "aws" in its ARN), as for the codeInterpreter tool.
      resources: [`arn:aws:bedrock-agentcore:${region}:aws:code-interpreter/aws.codeinterpreter.v1`],
    }));
  }
  if (kinds.includes("extract")) {
    out.push(new iam.PolicyStatement({
      sid: "TaskTextract",
      actions: ["textract:AnalyzeDocument", "textract:StartDocumentAnalysis", "textract:GetDocumentAnalysis"],
      // No resource type: Textract authorizes these on "*" only.
      resources: ["*"],
    }));
  }
  return out;
}
