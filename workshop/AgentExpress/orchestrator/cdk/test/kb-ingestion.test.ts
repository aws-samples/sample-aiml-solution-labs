/**
 * A type="kb" tool's `chunking` and `parsing` (lib/kb-ingestion.ts), synthesized through
 * ToolPlane: what reaches the data source, what the Knowledge Base role is granted, and
 * that a tool without them synthesizes exactly what it did before.
 */
import * as cdk from "aws-cdk-lib";
import { Template } from "aws-cdk-lib/assertions";
import * as path from "path";

import { ToolPlane, ToolSpec } from "../lib/tool-plane";
import { validateTools } from "../lib/orchestrator-stack";
import { kbDataSourceName, kbIngestionKey } from "../lib/kb-ingestion";

const ORCH_ROOT = path.join(__dirname, "..", "..");
const ACCOUNT = "123456789012";

function plane(over: Record<string, unknown> = {}) {
  const app = new cdk.App();
  const stack = new cdk.Stack(app, "T", { env: { account: ACCOUNT, region: "us-east-1" } });
  new ToolPlane(stack, "ToolPlane", {
    agentName: "test_orch",
    tools: { kb: { type: "kb", description: "Corpus", corpora: ["reference"], ...over } as ToolSpec },
    toolApiKeys: {},
    gatewayDiscoveryUrl: "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_abc/.well-known/openid-configuration",
    gatewayClientId: "client-abc",
    gatewayAudience: "gateway/invoke",
    isCognito: true,
    policyEnabled: true,
    policyMode: "ENFORCE",
    customPolicies: [],
    orchRoot: ORCH_ROOT,
  });
  return Template.fromStack(stack);
}

const dataSource = (t: Template) => Object.values<any>(t.findResources("AWS::Bedrock::DataSource"))[0].Properties;
const kbRoleStatements = (t: Template): any[] => Object.entries<any>(t.findResources("AWS::IAM::Policy"))
  .filter(([id]) => id.includes("KbRole")).flatMap(([, p]) => p.Properties.PolicyDocument.Statement);

const FIXED = { strategy: "fixedSize", maxTokens: 300, overlapPercentage: 20 };
const HIER = { strategy: "hierarchical", parentMaxTokens: 1500, childMaxTokens: 300, overlapTokens: 60 };

describe("kb chunking and parsing", () => {
  it("changes nothing when neither is set: name docs, no ingestion configuration", () => {
    const ds = dataSource(plane());
    expect(ds.Name).toBe("docs");
    expect(ds.VectorIngestionConfiguration).toBeUndefined();
    expect(kbRoleStatements(plane()).find((s) => s.Sid === "ParsingModel")).toBeUndefined();
  });

  it("sends fixed-size chunking in Bedrock's shape", () => {
    const ds = dataSource(plane({ chunking: FIXED }));
    expect(ds.VectorIngestionConfiguration).toEqual({
      ChunkingConfiguration: {
        ChunkingStrategy: "FIXED_SIZE",
        FixedSizeChunkingConfiguration: { MaxTokens: 300, OverlapPercentage: 20 },
      },
    });
  });

  it("sends hierarchical levels large then small, and semantic and none", () => {
    expect(dataSource(plane({ chunking: HIER })).VectorIngestionConfiguration.ChunkingConfiguration).toEqual({
      ChunkingStrategy: "HIERARCHICAL",
      HierarchicalChunkingConfiguration: { LevelConfigurations: [{ MaxTokens: 1500 }, { MaxTokens: 300 }], OverlapTokens: 60 },
    });
    const semantic = { strategy: "semantic", maxTokens: 300, bufferSize: 0, breakpointPercentileThreshold: 95 };
    expect(dataSource(plane({ chunking: semantic })).VectorIngestionConfiguration.ChunkingConfiguration).toEqual({
      ChunkingStrategy: "SEMANTIC",
      SemanticChunkingConfiguration: { MaxTokens: 300, BufferSize: 0, BreakpointPercentileThreshold: 95 },
    });
    expect(dataSource(plane({ chunking: { strategy: "none" } })).VectorIngestionConfiguration.ChunkingConfiguration)
      .toEqual({ ChunkingStrategy: "NONE" });
  });

  it("parses with a foundation model and grants the KB role that model only", () => {
    const t = plane({ parsing: { strategy: "foundationModel", model: "anthropic.claude-3-haiku-20240307-v1:0" } });
    const arn = "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-haiku-20240307-v1:0";
    expect(dataSource(t).VectorIngestionConfiguration).toEqual({
      ParsingConfiguration: { ParsingStrategy: "BEDROCK_FOUNDATION_MODEL", BedrockFoundationModelConfiguration: { ModelArn: arn } },
    });
    const grant = kbRoleStatements(t).find((s) => s.Sid === "ParsingModel");
    expect(grant.Action).toBe("bedrock:InvokeModel");
    expect(grant.Resource).toBe(arn);
  });

  it("an inference profile is invoked by its ARN and granted with its model in the profile's regions", () => {
    const t = plane({ parsing: { strategy: "foundationModel", model: "us.amazon.nova-pro-v1:0", prompt: "Read tables." } });
    const cfg = dataSource(t).VectorIngestionConfiguration.ParsingConfiguration.BedrockFoundationModelConfiguration;
    expect(cfg.ModelArn).toBe(`arn:aws:bedrock:us-east-1:${ACCOUNT}:inference-profile/us.amazon.nova-pro-v1:0`);
    expect(cfg.ParsingPrompt).toEqual({ ParsingPromptText: "Read tables." });
    const grant = kbRoleStatements(t).find((s) => s.Sid === "ParsingModel");
    expect(grant.Resource).toEqual([cfg.ModelArn, "arn:aws:bedrock:*::foundation-model/amazon.nova-pro-v1:0"]);
    expect(JSON.stringify(grant.Resource)).not.toContain("foundation-model/*");
  });

  it("names the data source from a digest, so a change is a replacement (same digest as Terraform)", () => {
    // These literals are what terraform/kb_ingestion.tf computes for the same settings
    // (checked with `terraform console`): the two paths must name it identically.
    expect(kbIngestionKey({ chunking: FIXED })).toBe("c=fixedSize:300,20|p=::");
    expect(kbDataSourceName({ chunking: FIXED })).toBe("docs-4947a8b0");
    expect(kbDataSourceName({
      chunking: HIER, parsing: { strategy: "foundationModel", model: "us.amazon.nova-pro-v1:0", prompt: "Read tables." },
    })).toBe("docs-3af9a5e6");
    expect(kbDataSourceName({ parsing: { strategy: "foundationModel", model: "anthropic.claude-3-haiku-20240307-v1:0" } }))
      .toBe("docs-5dfc2566");
    expect(dataSource(plane({ chunking: FIXED })).Name).toBe("docs-4947a8b0");
  });

  it("refuses what Bedrock would refuse halfway through a deploy", () => {
    const tools = (over: Record<string, unknown>) => ({ kb: { type: "kb", corpora: ["r"], ...over } });
    const v = (over: Record<string, unknown>) => () => validateTools(tools(over), {}, "us-east-1");
    expect(v({ chunking: { strategy: "FIXED_SIZE" } })).toThrow(/chunking.strategy must be one of: fixedSize/);
    expect(v({ chunking: { strategy: "fixedSize", maxTokens: 300 } })).toThrow(/overlapPercentage is required/);
    expect(v({ chunking: { ...FIXED, maxTokens: 9000 } })).toThrow(/from 1 to 8192/);
    expect(v({ chunking: { ...HIER, overlapTokens: 0 } })).toThrow(/1 or more/);
    expect(v({ chunking: { ...HIER, childMaxTokens: 1500 } })).toThrow(/smaller than parentMaxTokens/);
    expect(v({ parsing: { strategy: "foundationModel" } })).toThrow(/parsing.model must be/);
    expect(v({ parsing: { strategy: "foundationModel", model: "arn:aws:bedrock:*" } })).toThrow(/parsing.model must be/);
    expect(v({ parsing: { strategy: "dataAutomation" } })).toThrow(/parsing.strategy must be one of: foundationModel/);
    expect(v({ knowledgeBaseId: "ABCDEFGH12", chunking: FIXED })).toThrow(/chunking do not apply/);
    expect(v({ chunking: FIXED })).not.toThrow();
  });
});
