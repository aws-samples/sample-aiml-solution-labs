/**
 * The Builder's control plane: where builds are stored, and the project that deploys
 * and destroys them. Mirrors terraform/builder.tf; cdk/test/parity.test.ts keeps the two
 * in step.
 *
 *   * a DynamoDB table of builds (layout in bff/buildstore.py) with an owner index
 *   * a versioned S3 bucket: drafts, the immutable version each deploy used, and the
 *     Terraform state of builds deployed with Terraform
 *   * the framework source, uploaded as an asset, which every deploy starts from
 *   * a CodeBuild project running deployer/runner.py — `cdk deploy|destroy` or
 *     `terraform apply|destroy` for ONE build, as its own stack
 *
 * THE DEPLOY PROJECT CAN CREATE IAM ROLES. It deploys whole stacks, so on the CDK path
 * it assumes the CDK bootstrap roles (whose execution role is administrator by default)
 * and on the Terraform path it carries terraform/deploy-role-policy.json. That is why
 * starting it is behind its own permissions, `deploy` and `destroy`, in workflow.json.
 */

import * as path from "path";
import * as fs from "fs";
import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import {
  aws_codebuild as codebuild,
  aws_dynamodb as dynamodb,
  aws_iam as iam,
  aws_kms as kms,
  aws_lambda as lambda,
  aws_logs as logs,
  aws_s3 as s3,
  aws_s3_assets as s3assets,
  custom_resources as cr,
} from "aws-cdk-lib";
import { boundaryEnforcementStatements, buildGuardrailEnv } from "./hardening";
import { buildTagsEnv } from "./build-tags";

/** Every build's resources are named `ax_xxxxxxxx` (bff/buildstore.py AGENT_PREFIX). */
export const BUILD_AGENT_PREFIX = "ax_";
/** The tag every stack puts on its resources (bin/orchestrator.ts), as a condition key. */
export const BUILD_POOL_TAG = "aws:ResourceTag/agentexpress:app";

/** The role a connected account creates (bff/buildstore.py CONNECT_ROLE_PREFIX). */
export const CONNECT_ROLE_PREFIX = "AgentExpressDeploy-";

/** The Terraform release the runner installs, when the image has none. */
export const TERRAFORM_VERSION = "1.15.8";

/** What never goes into the source a deploy starts from: build output, installed
 *  packages, local state, and a developer's own settings — terraform.tfvars and
 *  backend.hcl are per-deployer, and may carry secrets. */
export const SOURCE_EXCLUDES = [
  "node_modules", "cdk.out", "dist", ".vite", ".terraform", ".terraform.lock.hcl",
  ".build", "__pycache__", ".pytest_cache", ".ruff_cache", ".venv", ".DS_Store",
  "terraform.tfvars", "backend.hcl", "tfplan", "*.tfplan", "*.tfstate", "*.tfstate.*", ".env", ".env.*",
  "zz_builder_backend_override.tf", "builder.auto.tfvars.json",
];

/** Statements per managed policy. Terraform splits the same file the same way
 *  (chunklist(..., 7) in terraform/builder.tf), so both paths attach the same policies. */
export const DEPLOY_POLICY_CHUNK = 7;

/**
 * terraform/deploy-role-policy.json, as policy documents small enough to attach.
 *
 * One file is the Terraform path's documented deploy permissions; minified it is over
 * the 6,144-character limit of one managed policy, so it is split by statement count.
 * tests/test_builder_plane.py checks every chunk stays under the limit.
 */
export function deployPolicyChunks(orchRoot: string, rolePath = "/"): any[][] {
  // Its role ARNs name roles at "/": with a role path they live under that path instead.
  // Mirrors local.deploy_policy in terraform/builder.tf.
  const doc = JSON.parse(fs.readFileSync(path.join(orchRoot, "terraform", "deploy-role-policy.json"), "utf8")
    .split(":role/").join(`:role${rolePath}`));
  const chunks: any[][] = [];
  for (let i = 0; i < doc.Statement.length; i += DEPLOY_POLICY_CHUNK) {
    chunks.push(doc.Statement.slice(i, i + DEPLOY_POLICY_CHUNK));
  }
  return chunks;
}

/** The buildspec, from the one file both IaC paths read. */
export function buildSpecObject(orchRoot: string): any {
  return JSON.parse(fs.readFileSync(path.join(orchRoot, "deployer", "buildspec.json"), "utf8"));
}

export interface BuilderPlaneProps {
  agentName: string;
  orchRoot: string;
  logRetention: logs.RetentionDays;
  /** The stack's data key (DataKey): the builds table and the deploy project use it. */
  dataKey: kms.IKey;
  /** Leave the builds table deletion-protected when the stack is destroyed, so a destroy
   *  stops at the table and you clear the protection yourself. Default false: destroy
   *  clears it just before deleting the table. `-c keepBuildsTableProtected=true`.
   *  Mirrors var.keep_builds_table_protected in terraform/builder.tf. */
  keepTableProtected?: boolean;
  /** Organisation guardrails (lib/hardening.ts): passed on to every build, and the boundary
   *  required of every role the deploy role creates. Unset by default. */
  permissionsBoundaryArn?: string;
  iamRolePath?: string;
  /** This console's LiteLLM proxy and key (lib/litellm.ts), passed on to every build. */
  litellmEnv?: Record<string, string>;
  /** This console's documentation links, passed on to every build: DOCS_URL (-c docsUrl),
   *  or DOCS_LINKS=false when they are off (deployer/runner.py docs_settings). */
  docsEnv?: Record<string, string>;
  /** Buckets an S3 code source may name in this console's own account (deployer/runner.py). */
  codeSourceBuckets?: string[];
  /** Extra tags for every build this console deploys (-c buildTags), passed to the runner
   *  as BUILD_EXTRA_TAGS. Mirrors var.build_tags. */
  buildTags?: Record<string, string>;
}

export class BuilderPlane extends Construct {
  readonly table: dynamodb.Table;
  readonly bucket: s3.Bucket;
  readonly project: codebuild.Project;
  /** Where a build's tool API keys and A2A tokens live: <prefix><build id>. */
  readonly secretsPrefix: string;
  private readonly agentName: string;

  constructor(scope: Construct, id: string, props: BuilderPlaneProps) {
    super(scope, id);
    const stack = cdk.Stack.of(this);
    const { agentName } = props;
    this.agentName = agentName;
    this.secretsPrefix = `agentexpress/${agentName}/builds/`;

    this.table = new dynamodb.Table(this, "BuildsTable", {
      tableName: `${agentName}_builds`,
      encryption: dynamodb.TableEncryption.CUSTOMER_MANAGED,
      encryptionKey: props.dataKey,
      partitionKey: { name: "pk", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "sk", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      pointInTimeRecoverySpecification: { pointInTimeRecoveryEnabled: true },
      // Every build, its versions, shares and the console's audit log live here, so no
      // account clean-up tool or stray DeleteTable may take it. A destroy of this stack
      // clears it at the last moment (BuildsTableUnprotect, below) unless
      // keepTableProtected is set. Mirrors aws_dynamodb_table.builds.
      deletionProtection: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
    this.table.addGlobalSecondaryIndex({
      indexName: "by_owner",
      partitionKey: { name: "owner", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "updated", type: dynamodb.AttributeType.STRING },
      projectionType: dynamodb.ProjectionType.ALL,
    });

    // Clears the table's deletion protection when the stack is destroyed, so `cdk destroy`
    // finishes instead of stopping at the table. A delete-only custom resource: it does
    // nothing on create or update, and runs `UpdateTable` when CloudFormation deletes it.
    // It depends on the table, so CloudFormation deletes it FIRST.
    //
    // It is always in the template and carries the flag as DATA. Were it omitted when
    // keepTableProtected is set, turning the flag on would delete the resource, and
    // deleting it runs the call: the opposite of what the flag asks for. With the flag on,
    // the call sets protection to true, which leaves the table as it is.
    const unprotect = new cr.AwsCustomResource(this, "BuildsTableUnprotect", {
      logGroup: new logs.LogGroup(this, "BuildsTableUnprotectLogGroup", {
        retention: props.logRetention,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      }),
      onDelete: {
        service: "DynamoDB",
        action: "updateTable",
        parameters: {
          TableName: this.table.tableName,
          DeletionProtectionEnabled: props.keepTableProtected === true,
        },
        physicalResourceId: cr.PhysicalResourceId.of(`${agentName}-builds-unprotect`),
      },
      policy: cr.AwsCustomResourcePolicy.fromStatements([
        new iam.PolicyStatement({ actions: ["dynamodb:UpdateTable"], resources: [this.table.tableArn] }),
      ]),
      installLatestAwsSdk: false,
    });
    unprotect.node.addDependency(this.table);

    // Name generated: `agentcore-<agentName>-builds-<account>-<region>` would not fit
    // in 63 characters for the default agentName.
    this.bucket = new s3.Bucket(this, "BuildsBucket", {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      enforceSSL: true,
      // Versioned: Terraform state lives here, and a draft overwritten by autosave can
      // be recovered.
      versioned: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    const source = new s3assets.Asset(this, "Source", {
      path: props.orchRoot,
      exclude: SOURCE_EXCLUDES,
    });

    const projectName = `${agentName.replace(/_/g, "-")}-deploy`;
    this.project = new codebuild.Project(this, "Deploy", {
      projectName,
      description: "Deploys and destroys AgentExpress Builder builds, one stack each",
      buildSpec: codebuild.BuildSpec.fromObject(buildSpecObject(props.orchRoot)),
      encryptionKey: props.dataKey,
      environment: {
        // ARM, because the AgentCore runtime image is linux/arm64 and building it
        // natively is far faster than under emulation. Privileged for Docker.
        buildImage: codebuild.LinuxArmBuildImage.AMAZON_LINUX_2023_STANDARD_3_0,
        computeType: codebuild.ComputeType.LARGE,
        privileged: true,
      },
      environmentVariables: {
        SOURCE_URI: { value: source.s3ObjectUrl },
        BUILDS_TABLE: { value: this.table.tableName },
        BUILDS_BUCKET: { value: this.bucket.bucketName },
        TERRAFORM_VERSION: { value: TERRAFORM_VERSION },
        SECRETS_PREFIX: { value: this.secretsPrefix },
        CONSOLE_NAME: { value: agentName },
        // S3 code sources a build in this account may read (runner.py code_source_s3).
        CODE_SOURCE_BUCKETS: { value: (props.codeSourceBuckets ?? []).join(",") },
        // The organisation's boundary and role path, for every build's roles.
        ...Object.fromEntries(Object.entries(buildGuardrailEnv({
          permissionsBoundaryArn: props.permissionsBoundaryArn ?? "", iamRolePath: props.iamRolePath ?? "/",
        })).map(([k, v]) => [k, { value: v }])),
        // This console's LiteLLM proxy and key, for every build's litellm/ agents.
        ...Object.fromEntries(Object.entries(props.litellmEnv ?? {}).map(([k, v]) => [k, { value: v }])),
        // This console's documentation links, for every build's app (runner.py docs_settings).
        ...Object.fromEntries(Object.entries(props.docsEnv ?? {}).map(([k, v]) => [k, { value: v }])),
        // Extra tags for every build this console deploys (runner.py extra_build_tags).
        ...Object.fromEntries(Object.entries(buildTagsEnv(props.buildTags ?? {})).map(([k, v]) => [k, { value: v }])),
      },
      timeout: cdk.Duration.hours(2),
      logging: {
        cloudWatch: {
          logGroup: new logs.LogGroup(this, "DeployLogGroup", {
            logGroupName: `/aws/codebuild/${projectName}`,
            retention: props.logRetention,
            removalPolicy: cdk.RemovalPolicy.DESTROY,
          }),
        },
      },
    });
    const role = this.project.role!;
    source.grantRead(role);
    this.table.grantReadWriteData(role);
    this.bucket.grantReadWrite(role);
    role.addToPrincipalPolicy(new iam.PolicyStatement({
      actions: ["s3:ListBucketVersions", "s3:DeleteObjectVersion"],
      resources: [this.bucket.bucketArn, `${this.bucket.bucketArn}/*`],
    }));
    // A CDK build's destroy that a late CloudFront log delivery failed: the runner empties
    // that build's UI logs bucket and destroys again (deployer/runner.py
    // empty_late_log_buckets). Only buckets named after a build's stack.
    role.addToPrincipalPolicy(new iam.PolicyStatement({
      sid: "LateLogBuckets",
      actions: ["s3:ListBucketVersions", "s3:DeleteObject", "s3:DeleteObjectVersion"],
      resources: [
        `arn:aws:s3:::ax-*-stack-uilogsbucket*`,
        `arn:aws:s3:::ax-*-stack-uilogsbucket*/*`,
      ],
    }));
    // After a destroy, the runner deletes the log groups neither tool owns (AgentCore's
    // runtime logs, CDK provider Lambdas), matched by the build's unique ax_ id.
    role.addToPrincipalPolicy(new iam.PolicyStatement({
      actions: ["logs:DeleteLogGroup"],
      resources: [
        `arn:aws:logs:${stack.region}:${stack.account}:log-group:/aws/bedrock-agentcore/runtimes/${BUILD_AGENT_PREFIX}*`,
        // A harness's runtime logs: AgentCore names them harness_<agent>_<step>-<id>-DEFAULT.
        `arn:aws:logs:${stack.region}:${stack.account}:log-group:/aws/bedrock-agentcore/runtimes/harness_${BUILD_AGENT_PREFIX}*`,
        `arn:aws:logs:${stack.region}:${stack.account}:log-group:/aws/lambda/ax-*`,
        `arn:aws:logs:${stack.region}:${stack.account}:log-group:ax-*`,
      ],
    }));
    // A build's tool API keys and A2A tokens, read at deploy and removed with the build.
    role.addToPrincipalPolicy(new iam.PolicyStatement({
      // ...and it keeps the owner's first-sign-in password for the build's console there.
      actions: ["secretsmanager:GetSecretValue", "secretsmanager:DeleteSecret",
        "secretsmanager:CreateSecret", "secretsmanager:PutSecretValue"],
      resources: [`arn:aws:secretsmanager:${stack.region}:${stack.account}:secret:${this.secretsPrefix}*`],
    }));
    // Deploying into a CONNECTED account: its deploy role, and only that kind of role.
    role.addToPrincipalPolicy(new iam.PolicyStatement({
      actions: ["sts:AssumeRole"],
      resources: [`arn:aws:iam::*:role/${CONNECT_ROLE_PREFIX}*`],
    }));
    // Inviting the owner into a build's own console (for builds in this account).
    role.addToPrincipalPolicy(new iam.PolicyStatement({
      actions: ["cognito-idp:AdminCreateUser", "cognito-idp:AdminAddUserToGroup", "cognito-idp:ListGroups"],
      resources: [`arn:aws:cognito-idp:${stack.region}:${stack.account}:userpool/*`],
      // Only a build's pool: every stack tags its resources agentexpress:app=<agentName>,
      // and a build's agentName is ax_<id>. Not this console's pool, nor anyone else's.
      conditions: { StringLike: { [BUILD_POOL_TAG]: `${BUILD_AGENT_PREFIX}*` } },
    }));
    // CDK path: the bootstrap roles do the deploying.
    role.addToPrincipalPolicy(new iam.PolicyStatement({
      actions: ["sts:AssumeRole"],
      resources: [`arn:aws:iam::${stack.account}:role/cdk-hnb659fds-*`],
    }));
    role.addToPrincipalPolicy(new iam.PolicyStatement({
      actions: ["ssm:GetParameter"],
      resources: [`arn:aws:ssm:${stack.region}:${stack.account}:parameter/cdk-bootstrap/*`],
    }));
    // The deploy policy may manage AgentExpressDeploy-* policies (a human deploying a
    // console needs that), and this role carries some, so it must not be able to grant
    // itself more: no edit of its own role or policies. Mirrors terraform/builder.tf.
    role.addToPrincipalPolicy(new iam.PolicyStatement({
      sid: "DenySelfEscalation",
      effect: iam.Effect.DENY,
      actions: ["iam:PutRolePolicy", "iam:DeleteRolePolicy", "iam:AttachRolePolicy",
        "iam:DetachRolePolicy", "iam:UpdateAssumeRolePolicy", "iam:PutRolePermissionsBoundary",
        "iam:DeleteRolePermissionsBoundary", "iam:DeleteRole", "iam:CreatePolicyVersion",
        "iam:DeletePolicyVersion", "iam:SetDefaultPolicyVersion", "iam:DeletePolicy"],
      resources: [role.roleArn, `arn:aws:iam::${stack.account}:policy/AgentExpressDeploy-${agentName}-*`],
    }));
    // Terraform path: the documented deploy permissions — plus read-only everywhere,
    // because a refresh reads far more of each resource than the write permissions name
    // (every Get* on a bucket, tags on a log group...), and a read gap fails the apply.
    // SAMPLE: ReadOnlyAccess reads every service in the account. For production, replace
    // it with a read policy covering only the services your workflows deploy.
    role.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName("ReadOnlyAccess"));
    // With a boundary set: roles only with it on them (lib/hardening.ts).
    boundaryEnforcementStatements(props.permissionsBoundaryArn ?? "").forEach((st) => role.addToPrincipalPolicy(st));
    deployPolicyChunks(props.orchRoot, props.iamRolePath ?? "/").forEach((statements, i) => {
      role.addManagedPolicy(new iam.ManagedPolicy(this, `TerraformDeploy${i}`, {
        managedPolicyName: `AgentExpressDeploy-${agentName}-${i}`,
        document: iam.PolicyDocument.fromJson({ Version: "2012-10-17", Statement: statements }),
      }));
    });
  }

  /** Knowledge-base documents are uploaded from the browser straight to this bucket
   *  (a presigned POST), so it must accept a POST from the console's origin. */
  allowUploadsFrom(...origins: string[]): void {
    this.bucket.addCorsRule({
      allowedMethods: [s3.HttpMethods.POST],
      allowedOrigins: origins,
      allowedHeaders: ["*"],
      maxAge: 3000,
    });
  }

  /** What the console's BFF needs to serve /api/builds and to run deployed builds. */
  grantConsole(bff: lambda.Function): void {
    const stack = cdk.Stack.of(this);
    bff.addEnvironment("BUILDS_TABLE", this.table.tableName);
    bff.addEnvironment("BUILDS_BUCKET", this.bucket.bucketName);
    bff.addEnvironment("DEPLOY_PROJECT", this.project.projectName);
    bff.addEnvironment("SECRETS_PREFIX", this.secretsPrefix);
    // The two roles a connected account's deploy role trusts (bff/accounts.py template).
    bff.addEnvironment("DEPLOY_ROLE_ARN", this.project.role!.roleArn);
    bff.addEnvironment("BFF_ROLE_ARN", bff.role!.roleArn);
    // Someone a build is shared with gets a login to its app on first ask
    // (bff/builds.py _invite_collaborator), like the owner's from the deploy.
    bff.addToRolePolicy(new iam.PolicyStatement({
      actions: ["cognito-idp:AdminCreateUser", "cognito-idp:AdminAddUserToGroup", "cognito-idp:ListGroups"],
      resources: [`arn:aws:cognito-idp:${stack.region}:${stack.account}:userpool/*`],
      // Only a build's pool: every stack tags its resources agentexpress:app=<agentName>,
      // and a build's agentName is ax_<id>. Not this console's pool, nor anyone else's.
      conditions: { StringLike: { [BUILD_POOL_TAG]: `${BUILD_AGENT_PREFIX}*` } },
    }));
    bff.addToRolePolicy(new iam.PolicyStatement({
      actions: ["secretsmanager:CreateSecret", "secretsmanager:PutSecretValue",
        "secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret", "secretsmanager:DeleteSecret"],
      resources: [`arn:aws:secretsmanager:${stack.region}:${stack.account}:secret:${this.secretsPrefix}*`],
    }));
    // Verifying a connection: assume the connected account's deploy role once.
    bff.addToRolePolicy(new iam.PolicyStatement({
      actions: ["sts:AssumeRole"],
      resources: [`arn:aws:iam::*:role/${CONNECT_ROLE_PREFIX}*`],
    }));
    // Scan: an admin's list of every user's builds (handler GET /api/builds?scope=all,
    // behind the `admin` permission). Mirrors aws_iam_role_policy.bff_builder.
    bff.addToRolePolicy(new iam.PolicyStatement({
      actions: ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem",
        "dynamodb:DeleteItem", "dynamodb:Query", "dynamodb:Scan"],
      resources: [this.table.tableArn, `${this.table.tableArn}/index/*`],
    }));
    bff.addToRolePolicy(new iam.PolicyStatement({
      actions: ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:DeleteObjectVersion"],
      resources: [`${this.bucket.bucketArn}/*`],
    }));
    bff.addToRolePolicy(new iam.PolicyStatement({
      actions: ["s3:ListBucket", "s3:ListBucketVersions"],
      resources: [this.bucket.bucketArn],
    }));
    bff.addToRolePolicy(new iam.PolicyStatement({
      actions: ["codebuild:StartBuild", "codebuild:BatchGetBuilds"],
      resources: [this.project.projectArn],
    }));
    // AWS Agent Registry (bff/registry.py): list the registries, find approved records to
    // take into a build, and (an admin) publish a build or a skill for approval and
    // deprecate it on destroy. Records only: the console never creates or deletes a
    // registry, nor approves its own records. Mirrors terraform/builder.tf.
    bff.addToRolePolicy(new iam.PolicyStatement({
      sid: "AgentRegistryList",
      actions: ["agent-registry:ListRegistries"],
      resources: ["*"],
    }));
    bff.addToRolePolicy(new iam.PolicyStatement({
      sid: "AgentRegistryRecords",
      actions: ["agent-registry:SearchDiscoverableRegistryRecords", "agent-registry:ListDiscoverableRegistryRecords",
        "agent-registry:GetDiscoverableRegistryRecord", "agent-registry:GetRegistry",
        "agent-registry:CreateRegistryRecord", "agent-registry:UpdateRegistryRecord",
        "agent-registry:GetRegistryRecord", "agent-registry:SubmitRegistryRecordForApproval",
        "agent-registry:UpdateRegistryRecordStatus",
        // A published record is tagged agentexpress:published (CreateRegistryRecord tags).
        "agent-registry:TagResource"],
      resources: [`arn:aws:agent-registry:${stack.region}:${stack.account}:registry/*`],
    }));
    bff.addToRolePolicy(new iam.PolicyStatement({
      actions: ["logs:GetLogEvents"],
      resources: [`arn:aws:logs:${stack.region}:${stack.account}:log-group:/aws/codebuild/${this.project.projectName}:*`],
    }));
    // Running a deployed build: its tables and its runtime, all named ax_xxxxxxxx.
    const t = `arn:aws:dynamodb:${stack.region}:${stack.account}:table/${BUILD_AGENT_PREFIX}*`;
    bff.addToRolePolicy(new iam.PolicyStatement({
      actions: ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem",
        "dynamodb:DeleteItem", "dynamodb:Query", "dynamodb:Scan"],
      resources: [t, `${t}/index/*`],
    }));
    // ...and the images their image agents render (bff/handler.py _image_link), in either
    // naming scheme (lib/bucket-names.ts): a build deployed here is in this Region.
    const assets = `arn:aws:s3:::agentcore-${BUILD_AGENT_PREFIX.replace(/_/g, "-")}*-assets-${stack.account}`;
    bff.addToRolePolicy(new iam.PolicyStatement({
      actions: ["s3:GetObject"],
      resources: [`${assets}/runs/*`, `${assets}-${stack.region}/runs/*`],
    }));
    const r = `arn:aws:bedrock-agentcore:${stack.region}:${stack.account}:runtime/${BUILD_AGENT_PREFIX}*`;
    bff.addToRolePolicy(new iam.PolicyStatement({
      actions: ["bedrock-agentcore:InvokeAgentRuntime"],
      resources: [r, `${r}/*`],
    }));
    // A code tool, checked in the AWS-managed Code Interpreter sandbox (bff/codecheck.py),
    // and — once a build in this account deploys it — called by Test tool. Mirrors
    // CodeToolChecks / TestCodeTools in terraform/builder.tf.
    bff.addToRolePolicy(new iam.PolicyStatement({
      sid: "CodeToolChecks",
      actions: ["bedrock-agentcore:StartCodeInterpreterSession", "bedrock-agentcore:InvokeCodeInterpreter",
        "bedrock-agentcore:StopCodeInterpreterSession"],
      resources: [`arn:aws:bedrock-agentcore:${stack.region}:aws:code-interpreter/aws.codeinterpreter.v1`],
    }));
    // Plain English -> Cedar by AgentCore Policy, against a build's own engine and Gateway
    // in this account (bff/policies.py generate). Mirrors WritePoliciesInPlainEnglish in
    // terraform/builder.tf.
    bff.addToRolePolicy(new iam.PolicyStatement({
      sid: "WritePoliciesInPlainEnglish",
      actions: ["bedrock-agentcore:StartPolicyGeneration", "bedrock-agentcore:GetPolicyGeneration",
        "bedrock-agentcore:ListPolicyGenerationAssets", "bedrock-agentcore:GetGateway",
        // AgentCore reads the Gateway's tools, as the caller, to write the policy.
        "bedrock-agentcore:ListGatewayTargets", "bedrock-agentcore:GetGatewayTarget", "bedrock-agentcore:GetPolicyEngine",
        "bedrock-agentcore:InvokeGateway"],
      resources: [`arn:aws:bedrock-agentcore:*:${stack.account}:policy-engine/${BUILD_AGENT_PREFIX}*`,
        `arn:aws:bedrock-agentcore:*:${stack.account}:gateway/${BUILD_AGENT_PREFIX.replace(/_/g, "-")}*`],
    }));
    bff.addToRolePolicy(new iam.PolicyStatement({
      sid: "FindBuildPolicyEngines",
      actions: ["bedrock-agentcore:ListPolicyEngines", "bedrock-agentcore:ListGateways"],
      resources: ["*"],
    }));
    bff.addToRolePolicy(new iam.PolicyStatement({
      sid: "TestCodeTools",
      actions: ["lambda:InvokeFunction"],
      resources: [`arn:aws:lambda:${stack.region}:${stack.account}:function:ToolLambda-${BUILD_AGENT_PREFIX}*`],
    }));
  }
}
