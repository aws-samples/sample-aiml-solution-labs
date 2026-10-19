/**
 * type="smithy": an AWS service described by a Smithy model, as a Gateway target. The
 * Gateway turns each operation into a tool (`<target>___<operation>`) and calls the
 * service signed by the build's Gateway role, so the tool's `allow` (AWS actions on
 * resources) is granted to that role, and nothing else is.
 *
 * Mirrors terraform/tools_smithy.tf (target, preconditions, statements), bff/smithy_tool.py
 * and web/src/builder/smithy.ts (the Build view's checks).
 *
 * What the docs and CfnGatewayTarget allow, and what this uses:
 *   - McpTargetConfiguration.smithyModel: ApiSchemaConfiguration { inlinePayload | s3.uri }.
 *   - Outbound auth: GATEWAY_IAM_ROLE with no iamCredentialProvider ("For Lambda, API
 *     Gateway, and Smithy targets, do not include the iamCredentialProvider field").
 *     The outbound-auth table also lists OAuth client credentials; not offered here, since
 *     a Smithy model is for an AWS service, which signs with SigV4.
 *   - Custom Smithy models are for AWS services only; RestJson protocol; up to 10 MB.
 * https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/gateway-building-smithy-targets.html
 * https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/gateway-outbound-auth.html
 */
import * as iam from "aws-cdk-lib/aws-iam";
import type * as agentcore from "aws-cdk-lib/aws-bedrockagentcore";
import * as vocab from "./vocabulary";

/** One named IAM action: a service prefix and an action name, no wildcard. */
export const ACTION_RE = /^[a-z0-9-]+:[A-Za-z0-9]+$/;
/** A full ARN: region and account written out or empty, never "*", and a resource part
 *  that is not a bare wildcard. Same pattern as bff/smithy_tool.py ARN_RE. */
export const ARN_RE = /^arn:aws[a-z-]*:[a-z0-9-]+:([a-z]{2}(-[a-z]+)+-[0-9]+)?:([0-9]{12})?:[^*\s]\S*$/;
export const MAX_ENTRIES = 20;
const S3_URI_RE = /^s3:\/\/([a-z0-9.-]{3,63})\/(.+)$/;

export interface SmithyAllow { actions: string[]; resources: string[] }

/** The checks a synth makes, as errors: the same rules terraform/tools_smithy.tf applies at plan. */
export function validateSmithy(name: string, t: Record<string, any>): void {
  const where = `workflow.json tools.${name}`;
  if ([t.schemaS3Uri, t.schema].filter(Boolean).length !== 1) {
    throw new Error(`${where} needs EXACTLY ONE of "schemaS3Uri" (an s3:// Smithy JSON model you already host) or "schema" (the model itself, inline).`);
  }
  if (t.schemaS3Uri && !S3_URI_RE.test(String(t.schemaS3Uri))) {
    throw new Error(`${where}.schemaS3Uri must be a full s3:// URI including the object key, e.g. "s3://amzn-s3-demo-bucket/models/orders.json".`);
  }
  if (t.schema) {
    const shapes = t.schema?.shapes;
    const kinds = shapes && typeof shapes === "object" ? Object.values<any>(shapes) : [];
    if (typeof t.schema !== "object" || typeof t.schema.smithy !== "string"
        || !kinds.some((s) => s?.type === "service") || !kinds.some((s) => s?.type === "operation")) {
      throw new Error(`${where}.schema must be a Smithy JSON model ({"smithy": "2.0", "shapes": {...}}) with a service shape and at least one operation.`);
    }
  }
  const allow = t.allow;
  const actions: unknown[] = Array.isArray(allow?.actions) ? allow.actions : [];
  const resources: unknown[] = Array.isArray(allow?.resources) ? allow.resources : [];
  if (!actions.length || actions.length > MAX_ENTRIES || actions.some((a) => typeof a !== "string" || !ACTION_RE.test(a))) {
    throw new Error(`${where}.allow.actions must list 1 to ${MAX_ENTRIES} named IAM actions, like "dynamodb:GetItem" (no *): what the Gateway role may call for this tool.`);
  }
  const denied = vocab.values("smithyDeniedServices");
  const bad = (actions as string[]).filter((a) => denied.includes(a.split(":")[0]));
  if (bad.length) {
    throw new Error(`${where}.allow.actions may not name ${bad.join(", ")}: a Smithy tool's calls run as the Gateway role every tool shares, so ${denied.join(", ")} are refused.`);
  }
  if (!resources.length || resources.length > MAX_ENTRIES || resources.some((r) => typeof r !== "string" || !ARN_RE.test(r))) {
    throw new Error(`${where}.allow.resources must list 1 to ${MAX_ENTRIES} full ARNs, region and account written out (or empty where the service has none), and no bare *.`);
  }
}

/** The target's `mcp` configuration: the model inline, or the object in S3. */
export function smithyConfig(spec: { schema?: Record<string, unknown>; schemaS3Uri?: string }):
    agentcore.CfnGatewayTarget.McpTargetConfigurationProperty {
  if (spec.schema) return { smithyModel: { inlinePayload: JSON.stringify(spec.schema) } };
  return { smithyModel: { s3: { uri: String(spec.schemaS3Uri) } } };
}

/** What the Gateway role needs for the smithy tools: GetObject on each hosted model (the
 *  Gateway reads it as the role when the target is created), and each tool's `allow`.
 *  Empty when there is no smithy tool, so nothing changes for a build without one. */
export function smithyStatements(entries: [string, { type: string; schemaS3Uri?: string; schema?: unknown; allow?: SmithyAllow }][]):
    iam.PolicyStatement[] {
  const smithy = entries.filter(([, s]) => s.type === "smithy");
  const out: iam.PolicyStatement[] = [];
  const models = smithy.filter(([, s]) => !s.schema && s.schemaS3Uri)
    .map(([, s]) => S3_URI_RE.exec(String(s.schemaS3Uri)))
    .filter((m): m is RegExpExecArray => Boolean(m))
    .map((m) => `arn:aws:s3:::${m[1]}/${m[2]}`);
  if (models.length) out.push(new iam.PolicyStatement({ sid: "ReadSmithyModels", actions: ["s3:GetObject"], resources: models }));
  for (const [name, s] of smithy) {
    if (!s.allow?.actions?.length || !s.allow.resources?.length) continue;
    out.push(new iam.PolicyStatement({ sid: `SmithyCalls${name}`, actions: s.allow.actions, resources: s.allow.resources }));
  }
  return out;
}
