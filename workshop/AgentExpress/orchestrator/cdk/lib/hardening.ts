/**
 * Organisation guardrails, for accounts whose rules require them. Mirrors
 * terraform/hardening.tf; test/hardening.test.ts and tests/test_hardening.py keep the two in
 * step. Every one is OFF by default, and unset none of them changes the template.
 *
 *   * permissionsBoundaryArn: the boundary of EVERY IAM role in the stack, the CDK framework's
 *     own (custom-resource providers, the UI deployment, auto-delete) included, through
 *     iam.PermissionsBoundary, an Aspect over every AWS::IAM::Role. The Builder passes it to
 *     each build it deploys, and its deploy role may create roles only with it.
 *   * iamRolePath: the path of every role, by an Aspect of the same shape. Changing it
 *     replaces the roles.
 *   * customDomain + acmCertificateArn: a CloudFront alias with TLS 1.2 (2021 policy) and SNI.
 *   * enableWaf: a CLOUDFRONT web ACL on the distribution (us-east-1 stacks only).
 */
import * as cdk from "aws-cdk-lib";
import { Construct, IConstruct } from "constructs";
import { aws_cloudfront as cf, aws_iam as iam, aws_wafv2 as wafv2 } from "aws-cdk-lib";
import { CODE_BOUNDARY_DENY } from "./tool-plane";

/** The oldest TLS a viewer may use on the custom domain. CloudFront's own certificate is
 *  fixed by CloudFront and cannot be raised. Mirrors local.min_viewer_tls. */
export const MIN_VIEWER_TLS = cf.SecurityPolicyProtocol.TLS_V1_2_2021;

/** AWS managed rule groups on the web ACL, in priority order (after the rate rule). Mirrors
 *  local.waf_managed_rule_groups. */
export const WAF_MANAGED_RULE_GROUPS = [
  "AWSManagedRulesAmazonIpReputationList", "AWSManagedRulesCommonRuleSet", "AWSManagedRulesKnownBadInputsRuleSet",
];

/** Rules of a managed group that COUNT rather than block. The Common rule set blocks request
 *  bodies over 8 KB, and saving a build or a workflow sends more than that. */
export const WAF_COUNTED_RULES: Record<string, string[]> = {
  AWSManagedRulesCommonRuleSet: ["SizeRestrictions_BODY"],
};

export const DEFAULT_ROLE_PATH = "/";
export const DEFAULT_WAF_RATE_LIMIT = 2000;

export interface HardeningProps {
  permissionsBoundaryArn?: string;
  iamRolePath?: string;
  customDomain?: string;
  acmCertificateArn?: string;
  enableWaf?: boolean;
  wafRateLimit?: number;
}

export interface Hardening {
  permissionsBoundaryArn: string;
  iamRolePath: string;
  customDomain: string;
  acmCertificateArn: string;
  enableWaf: boolean;
  wafRateLimit: number;
}

// The same shapes as the `validation` blocks in terraform/variables.tf.
const BOUNDARY_ARN = /^arn:aws[a-z-]*:iam::(aws|[0-9]{12}):policy\/[A-Za-z0-9+=,.@_/-]{1,512}$/;
// Letters, digits and + = , . @ _ - between slashes: it is spliced into role ARN patterns in
// the deploy policy, so a * or a quote must not get in.
const ROLE_PATH = /^\/([A-Za-z0-9+=,.@_-]+\/)*$/;
const HOST_NAME = /^([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}$/;
const CERT_ARN = /^arn:aws[a-z-]*:acm:us-east-1:[0-9]{12}:certificate\/[0-9a-f-]{36}$/;

/** The settings, checked. Throws, at synth, on anything Terraform's validation refuses. */
export function hardeningOf(p: HardeningProps, region: string): Hardening {
  const h: Hardening = {
    permissionsBoundaryArn: p.permissionsBoundaryArn ?? "",
    iamRolePath: p.iamRolePath || DEFAULT_ROLE_PATH,
    customDomain: p.customDomain ?? "",
    acmCertificateArn: p.acmCertificateArn ?? "",
    enableWaf: p.enableWaf === true,
    wafRateLimit: p.wafRateLimit ?? DEFAULT_WAF_RATE_LIMIT,
  };
  if (h.permissionsBoundaryArn && !BOUNDARY_ARN.test(h.permissionsBoundaryArn)) {
    throw new Error("permissionsBoundaryArn must be a managed policy ARN, e.g. arn:aws:iam::123456789012:policy/OrgBoundary.");
  }
  if (h.iamRolePath.length > 512 || !ROLE_PATH.test(h.iamRolePath)) {
    throw new Error('iamRolePath must start and end with "/" (e.g. "/agentexpress/"), using letters, digits and + = , . @ _ - only.');
  }
  if (h.customDomain && (h.customDomain.length > 253 || !HOST_NAME.test(h.customDomain))) {
    throw new Error("customDomain must be a lower-case host name such as agents.example.com: no scheme, path, port or wildcard.");
  }
  if (!h.customDomain !== !h.acmCertificateArn) {
    throw new Error("customDomain and acmCertificateArn go together: set both, or neither.");
  }
  if (h.acmCertificateArn && !CERT_ARN.test(h.acmCertificateArn)) {
    throw new Error("acmCertificateArn must be an ACM certificate ARN in us-east-1 (arn:aws:acm:us-east-1:<account>:certificate/<id>): CloudFront uses no other region.");
  }
  // A CLOUDFRONT web ACL exists only in us-east-1, and creating one from a stack elsewhere
  // would take a second stack in us-east-1 and a cross-region reference. Not offered.
  if (h.enableWaf && region !== "us-east-1") {
    throw new Error(`enableWaf needs the stack in us-east-1 (it is in ${cdk.Token.isUnresolved(region) ? "an unresolved region" : region}): a web ACL for CloudFront can only be created there.`);
  }
  if (!Number.isInteger(h.wafRateLimit) || h.wafRateLimit < 100 || h.wafRateLimit > 2000000) {
    throw new Error("wafRateLimit must be a whole number from 100 to 2000000.");
  }
  return h;
}

/** Sets Path on every AWS::IAM::Role in scope, CDK's own included. An Aspect, because the
 *  framework's roles are made deep inside constructs this stack does not configure. */
class RolePath implements cdk.IAspect {
  constructor(private readonly path: string) {}
  visit(node: IConstruct): void {
    if (cdk.CfnResource.isCfnResource(node) && node.cfnResourceType === iam.CfnRole.CFN_RESOURCE_TYPE_NAME) {
      node.addPropertyOverride("Path", this.path);
    }
  }
}

/** The boundary and the path on every role in `scope`. Nothing at all when both are unset. */
export function applyIamGuardrails(scope: Construct, h: Hardening): void {
  if (h.permissionsBoundaryArn) {
    // Over every role, a code tool's included: a role has ONE boundary, and an account rule
    // usually refuses a role without the organisation's. The code tool's own Deny moves into
    // its policy (denyFrameworkDataToCodeTools).
    iam.PermissionsBoundary.of(scope).apply(
      iam.ManagedPolicy.fromManagedPolicyArn(scope, "OrgPermissionsBoundary", h.permissionsBoundaryArn));
  }
  if (h.iamRolePath !== DEFAULT_ROLE_PATH) {
    cdk.Aspects.of(scope).add(new RolePath(h.iamRolePath), { priority: cdk.AspectPriority.MUTATING });
  }
}

/** Under the organisation's boundary, a code tool's role no longer has its own, so its Deny on
 *  the framework's tables, secrets and buckets goes into the role's policy, where it still
 *  beats any Allow. Mirrors aws_iam_role_policy.tool_code. Call once every role exists. */
export function denyFrameworkDataToCodeTools(scope: Construct, h: Hardening): void {
  if (!h.permissionsBoundaryArn) return;
  for (const c of scope.node.findAll()) {
    if (c instanceof iam.Role && c.node.id.startsWith("ToolCodeRole-")) {
      for (const d of CODE_BOUNDARY_DENY) {
        c.addToPrincipalPolicy(new iam.PolicyStatement({
          sid: d.sid, effect: iam.Effect.DENY, actions: d.actions, resources: d.resources,
        }));
      }
    }
  }
}

/**
 * The deploy role may create and change roles only with the boundary on them, and may not take
 * it off or edit the boundary policy. Deny statements, so they hold whatever else the role is
 * granted. Resource "*" because the condition is the scope: any role that lacks the boundary.
 * Mirrors local.boundary_enforcement.
 */
export function boundaryEnforcementStatements(boundaryArn: string): iam.PolicyStatement[] {
  if (!boundaryArn) return [];
  return [
    new iam.PolicyStatement({
      sid: "RolesOnlyWithTheBoundary", effect: iam.Effect.DENY,
      actions: ["iam:CreateRole", "iam:PutRolePolicy", "iam:AttachRolePolicy", "iam:PutRolePermissionsBoundary"],
      resources: ["*"],
      conditions: { ArnNotEquals: { "iam:PermissionsBoundary": boundaryArn } },
    }),
    new iam.PolicyStatement({
      sid: "KeepTheBoundary", effect: iam.Effect.DENY,
      actions: ["iam:DeleteRolePermissionsBoundary"], resources: ["*"],
    }),
    new iam.PolicyStatement({
      sid: "LeaveTheBoundaryPolicyAlone", effect: iam.Effect.DENY,
      actions: ["iam:CreatePolicyVersion", "iam:DeletePolicyVersion", "iam:SetDefaultPolicyVersion", "iam:DeletePolicy"],
      resources: [boundaryArn],
    }),
  ];
}

/** What the deploy runner passes on to every build it deploys (deployer/runner.py). Mirrors
 *  local.build_guardrail_env. */
export function buildGuardrailEnv(h: Pick<Hardening, "permissionsBoundaryArn" | "iamRolePath">): Record<string, string> {
  return {
    ...(h.permissionsBoundaryArn ? { PERMISSIONS_BOUNDARY_ARN: h.permissionsBoundaryArn } : {}),
    ...(h.iamRolePath && h.iamRolePath !== DEFAULT_ROLE_PATH ? { IAM_ROLE_PATH: h.iamRolePath } : {}),
  };
}

/**
 * The console's web ACL: a per-IP rate limit, then the managed groups. Scope CLOUDFRONT, so
 * the stack must be in us-east-1 (hardeningOf). The HTTP API behind /api/* cannot take a web
 * ACL (API Gateway associates WAF with REST APIs only), so /api/* is covered here, on its way
 * through CloudFront; calls straight to the execute-api URL are not, and still need a JWT.
 */
export function uiWebAcl(scope: Construct, agentName: string, rateLimit: number): wafv2.CfnWebACL {
  const name = `agentcore-${agentName.replace(/_/g, "-")}-ui`;
  const visibility = (metricName: string) => ({
    cloudWatchMetricsEnabled: true, metricName, sampledRequestsEnabled: true,
  });
  return new wafv2.CfnWebACL(scope, "UiWebAcl", {
    name,
    description: "AgentExpress console: AWS managed rule groups and a per-IP rate limit",
    scope: "CLOUDFRONT",
    defaultAction: { allow: {} },
    visibilityConfig: visibility(name),
    rules: [
      {
        name: "RateLimitPerIp", priority: 0, action: { block: {} },
        statement: { rateBasedStatement: { limit: rateLimit, aggregateKeyType: "IP" } },
        visibilityConfig: visibility("RateLimitPerIp"),
      },
      ...WAF_MANAGED_RULE_GROUPS.map((group, i) => ({
        name: group, priority: i + 1, overrideAction: { none: {} },
        statement: { managedRuleGroupStatement: {
          vendorName: "AWS", name: group,
          ...(WAF_COUNTED_RULES[group] ? { ruleActionOverrides: WAF_COUNTED_RULES[group].map((r) => (
            { name: r, actionToUse: { count: {} } })) } : {}),
        } },
        visibilityConfig: visibility(group),
      })),
    ],
  });
}
