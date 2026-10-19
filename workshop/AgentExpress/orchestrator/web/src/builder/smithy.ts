/** A type="smithy" tool: an AWS service described by a Smithy model, called through the
 *  Gateway and signed by the build's Gateway role. `allow` is what that role may call
 *  for it. Its checks mirror bff/smithy_tool.py word for word (both run
 *  tests/fixtures/validation_cases.json), cdk/lib/smithy-tool.ts and
 *  terraform/tools_smithy.tf.
 *  https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/gateway-building-smithy-targets.html */
import type { Issue, Where } from "./validate";

const S3_URI_RE = /^s3:\/\/[a-z0-9.-]{3,63}\/.+/;
/** One named IAM action: a service prefix and an action name, no wildcard. */
export const ACTION_RE = /^[a-z0-9-]+:[A-Za-z0-9]+$/;
/** A full ARN: region and account written out or empty, never "*", and a resource part
 *  that is not a bare wildcard. */
export const ARN_RE = /^arn:aws[a-z-]*:[a-z0-9-]+:([a-z]{2}(-[a-z]+)+-[0-9]+)?:([0-9]{12})?:[^*\s]\S*$/;
export const MAX_ENTRIES = 20;

type Add = (severity: Issue["severity"], where: Where, path: string, message: string) => void;
const isObj = (v: unknown): v is Record<string, unknown> => typeof v === "object" && v !== null && !Array.isArray(v);

export function checkSmithy(tool: Record<string, unknown>, path: string, where: Where, add: Add, denied: string[]): void {
  if ("schema" in tool) {
    const doc = tool.schema;
    const shapes = isObj(doc) ? doc.shapes : undefined;
    if (!isObj(doc) || typeof doc.smithy !== "string" || !isObj(shapes)) {
      add("error", where, `${path}.schema`, 'must be a Smithy JSON model: {"smithy": "2.0", "shapes": {...}}');
    } else {
      const kinds = Object.values(shapes).filter(isObj);
      const services = kinds.filter((s) => s.type === "service");
      if (!services.length) add("error", where, `${path}.schema.shapes`, "has no service shape: the model must describe one AWS service");
      if (!kinds.some((s) => s.type === "operation")) {
        add("error", where, `${path}.schema.shapes`, "has no operation, so the target would expose no tool");
      }
      if (services.length && !services.some((s) => isObj(s.traits) && "aws.protocols#restJson1" in s.traits)) {
        add("warning", where, `${path}.schema.shapes`,
          "names no service using aws.protocols#restJson1: the Gateway supports only the RestJson protocol");
      }
    }
  }
  if (typeof tool.schemaS3Uri === "string" && !S3_URI_RE.test(tool.schemaS3Uri)) {
    add("error", where, `${path}.schemaS3Uri`, "must be s3://<bucket>/<key> — the Gateway loads a Smithy model from S3 or inline");
  }
  if (!("allow" in tool)) return;
  const allow = tool.allow;
  if (!isObj(allow)) {
    add("error", where, `${path}.allow`, 'must be {"actions": [...], "resources": [...]}');
    return;
  }
  const actions = allow.actions;
  if (!Array.isArray(actions) || !actions.length || actions.length > MAX_ENTRIES) {
    add("error", where, `${path}.allow.actions`, "must list 1 to 20 IAM actions, each one named action like dynamodb:GetItem (no *)");
  } else {
    actions.forEach((a, i) => {
      if (typeof a !== "string" || !ACTION_RE.test(a)) {
        add("error", where, `${path}.allow.actions[${i}]`, "must be one named IAM action, like dynamodb:GetItem (no *)");
      } else if (denied.includes(a.split(":")[0])) {
        add("error", where, `${path}.allow.actions[${i}]`,
          `${a.split(":")[0]} is not allowed: a Smithy tool's calls run as the Gateway role every tool shares`);
      }
    });
  }
  const resources = allow.resources;
  if (!Array.isArray(resources) || !resources.length || resources.length > MAX_ENTRIES) {
    add("error", where, `${path}.allow.resources`, "must list 1 to 20 resource ARNs");
  } else {
    resources.forEach((r, i) => {
      if (typeof r !== "string" || !ARN_RE.test(r)) {
        add("error", where, `${path}.allow.resources[${i}]`,
          "must be a full ARN with its region and account written out (or empty where the service has none), not a bare *");
      }
    });
  }
}

/** The operations a Smithy model publishes, each with its input's member names: the
 *  tool names (`<target>___<operation>`) and arguments a policy can name. */
export function smithyOperations(doc: unknown): { name: string; args: string[] }[] {
  const shapes = isObj(doc) && isObj(doc.shapes) ? doc.shapes : {};
  const out: { name: string; args: string[] }[] = [];
  for (const [id, s] of Object.entries(shapes)) {
    if (!isObj(s) || s.type !== "operation") continue;
    const input = isObj(s.input) && typeof s.input.target === "string" ? shapes[s.input.target] : undefined;
    out.push({ name: id.split("#").pop() ?? id, args: Object.keys(isObj(input) && isObj(input.members) ? input.members : {}).sort() });
  }
  return out;
}
