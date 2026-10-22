/** Checks for a hosted MCP server, `tools.<key> = { type: "mcp", hosted: { code?, network? } }`.
 *  The generic key checks (keys.json) say `hosted` applies to type "mcp", holds only `code` and
 *  `network`, and that exactly one of `endpoint` / `hosted` is set; networkCheck.ts checks
 *  `hosted.network`. These are the rest:
 *  - the key is part of the Runtime name ax_<build>_mcp_<key>; AgentRuntimeName is
 *    [a-zA-Z][a-zA-Z0-9_]{0,47}, so with the 11-character build name the key is at most 32
 *    https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-bedrockagentcore-runtime.html
 *  - the Gateway reaches it with SigV4 as its role, signing for "bedrock-agentcore"
 *    https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/gateway-building-adding-targets-authorization.html
 *  - `code` is a code source, checked by codeSource.ts; only here may it be an `image`.
 *    The containerUri pattern also admits public.ecr.aws, .amazonaws.com.cn and tag+digest
 *    https://docs.aws.amazon.com/bedrock-agentcore-control/latest/APIReference/API_ContainerConfiguration.html
 *    but the CDK and Terraform pull grants only parse a private, commercial-partition ECR URI
 *    with a tag or a sha256 digest, so an image must also match ECR_IMAGE_RE.
 *  Mirrors bff/hosted_mcp_check.py word for word (both run tests/fixtures/hosted_mcp_cases.json). */
import { codeSourceProblems } from "./codeSource";
import type { Issue, Where } from "./validate";
type Add = (severity: Issue["severity"], where: Where, path: string, message: string) => void;
const isObj = (v: unknown): v is Record<string, unknown> => typeof v === "object" && v !== null && !Array.isArray(v);

/** 48 (AgentRuntimeName) - "ax_12345678_mcp_".length. */
export const HOSTED_MCP_MAX_KEY = 32;
/** What cdk/lib/hosted-mcp.ts ecrRepositoryArn and terraform/tools_hosted_mcp.tf parse. */
export const ECR_IMAGE_RE = /^\d{12}\.dkr\.ecr\.[a-z0-9-]+\.amazonaws\.com\/[a-z0-9._/-]+(:[\w.-]+|@sha256:[0-9a-f]{64})?$/;
const VARIANTS = ["git", "s3", "image"] as const;
function codeProblems(code: unknown, p: string): { path: string; message: string }[] {
  const out = codeSourceProblems(code, p, VARIANTS);
  const image = isObj(code) && Object.keys(code).length === 1 ? code.image : undefined;
  if (out.length === 0 && typeof image === "string" && !ECR_IMAGE_RE.test(image)) {
    out.push({ path: `${p}.image`, message: "a hosted MCP server's image must be a private ECR image URI, "
      + "<account>.dkr.ecr.<region>.amazonaws.com/<repo>[:tag | @sha256:<digest>]" });
  }
  return out;
}
export function checkHostedMcp(key: string, tool: Record<string, unknown>, path: string, where: Where, add: Add): void {
  const hosted = tool.hosted;
  if (!isObj(hosted)) return;
  if (key.length > HOSTED_MCP_MAX_KEY) {
    add("error", where, path, "a hosted MCP server's key is part of its Runtime name "
      + `ax_<build>_mcp_<key>, which AgentCore caps at 48 characters: use at most ${HOSTED_MCP_MAX_KEY}`);
  }
  if (tool.auth !== undefined && tool.auth !== null && tool.auth !== "sigv4") {
    add("error", where, `${path}.auth`,
      'a hosted MCP server is reached with SigV4 as the Gateway role: leave auth unset or set "sigv4"');
  }
  if (tool.service !== undefined && tool.service !== null && tool.service !== "bedrock-agentcore") {
    add("error", where, `${path}.service`, 'a hosted MCP server is signed for "bedrock-agentcore": leave service unset');
  }
  if (hosted.code !== undefined && hosted.code !== null) {
    for (const pr of codeProblems(hosted.code, `${path}.hosted.code`)) add("error", where, pr.path, pr.message);
  }
}
