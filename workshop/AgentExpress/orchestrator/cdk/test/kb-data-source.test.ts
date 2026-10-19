/**
 * A type="kb" tool's `dataSource` (lib/kb-data-source.ts), synthesized through ToolPlane:
 * the data source is the connector in Bedrock's shape, no documents bucket is made, and the
 * Knowledge Base role may read the connector's secret and nothing else.
 */
import * as cdk from "aws-cdk-lib";
import { Template } from "aws-cdk-lib/assertions";
import * as path from "path";

import { ToolPlane, ToolSpec } from "../lib/tool-plane";
import { validateTools } from "../lib/orchestrator-stack";

const ORCH_ROOT = path.join(__dirname, "..", "..");
const ACCOUNT = "123456789012";
const SECRET = `arn:aws:secretsmanager:us-east-1:${ACCOUNT}:secret:c-AbCdEf`;
const OSS = { type: "opensearchServerless", collectionArn: `arn:aws:aoss:us-east-1:${ACCOUNT}:collection/abc123`,
  indexName: "kb-index", vectorField: "vec", textField: "text", metadataField: "meta" };

function plane(dataSource: { type: string; [setting: string]: unknown }) {
  const app = new cdk.App();
  const stack = new cdk.Stack(app, "T", { env: { account: ACCOUNT, region: "us-east-1" } });
  new ToolPlane(stack, "ToolPlane", {
    agentName: "test_orch",
    tools: { kb: { type: "kb", description: "Corpus", corpora: ["reference"], vectorStore: OSS, dataSource } as ToolSpec },
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

const dsConfig = (t: Template) => Object.values<any>(t.findResources("AWS::Bedrock::DataSource"))[0].Properties.DataSourceConfiguration;
const roleStatements = (t: Template): any[] => Object.entries<any>(t.findResources("AWS::IAM::Policy"))
  .filter(([id]) => id.includes("KbRole")).flatMap(([, p]) => p.Properties.PolicyDocument.Statement);
const FILTERS = [{ objectType: "Page", include: [".*Policy.*"] }];

describe("kb connectors", () => {
  it("a web crawl: seed URLs, limits and filters in Bedrock's shape; no bucket, no grant", () => {
    const t = plane({ type: "web", seedUrls: ["https://docs.example.com/"], scope: "hostOnly", rateLimit: 60, maxPages: 500,
      exclusionFilters: [".*\\.pdf$"], userAgent: "acme-docs-crawler-01" });
    expect(dsConfig(t)).toEqual({
      Type: "WEB",
      WebConfiguration: {
        SourceConfiguration: { UrlConfiguration: { SeedUrls: [{ Url: "https://docs.example.com/" }] } },
        CrawlerConfiguration: { CrawlerLimits: { RateLimit: 60, MaxPages: 500 }, ExclusionFilters: [".*\\.pdf$"],
          Scope: "HOST_ONLY", UserAgent: "acme-docs-crawler-01" },
      },
    });
    expect(Object.keys(t.findResources("AWS::S3::Bucket")).filter((k) => k.includes("KbDocs"))).toEqual([]);
    const sids = roleStatements(t).map((s) => s.Sid);
    expect(sids).not.toContain("ConnectorSecret");
    expect(sids).not.toContain("DocsRead");
    expect(sids).not.toContain("DocsList");
  });

  it("Confluence with OAuth: may read and write back its secret; content filters passed", () => {
    const t = plane({ type: "confluence", hostUrl: "https://acme.atlassian.net", authType: "oauth2", credentialsSecretArn: SECRET, filters: FILTERS });
    expect(dsConfig(t).ConfluenceConfiguration).toEqual({
      SourceConfiguration: { HostUrl: "https://acme.atlassian.net", HostType: "SAAS", AuthType: "OAUTH2_CLIENT_CREDENTIALS", CredentialsSecretArn: SECRET },
      CrawlerConfiguration: { FilterConfiguration: { Type: "PATTERN",
        PatternObjectFilter: { Filters: [{ ObjectType: "Page", InclusionFilters: [".*Policy.*"] }] } } },
    });
    expect(roleStatements(t).find((s) => s.Sid === "ConnectorSecret")).toMatchObject({
      Action: ["secretsmanager:GetSecretValue", "secretsmanager:PutSecretValue"], Resource: SECRET });
    const basic = plane({ type: "confluence", hostUrl: "https://acme.atlassian.net", authType: "basic", credentialsSecretArn: SECRET });
    expect(roleStatements(basic).find((s) => s.Sid === "ConnectorSecret").Action).toBe("secretsmanager:GetSecretValue");
    expect(dsConfig(basic).ConfluenceConfiguration.CrawlerConfiguration).toBeUndefined();
  });

  it("SharePoint and Salesforce: OAuth client credentials, read-only on the secret", () => {
    const sp = plane({ type: "sharepoint", domain: "acme", siteUrls: ["https://acme.sharepoint.com/sites/hr"],
      tenantId: "0b1c2d3e-4f50-6172-8394-a5b6c7d8e9f0", credentialsSecretArn: SECRET });
    expect(dsConfig(sp).SharePointConfiguration.SourceConfiguration).toEqual({ Domain: "acme",
      SiteUrls: ["https://acme.sharepoint.com/sites/hr"], HostType: "ONLINE", AuthType: "OAUTH2_CLIENT_CREDENTIALS",
      CredentialsSecretArn: SECRET, TenantId: "0b1c2d3e-4f50-6172-8394-a5b6c7d8e9f0" });
    const sf = plane({ type: "salesforce", hostUrl: "https://acme.my.salesforce.com", credentialsSecretArn: SECRET });
    expect(dsConfig(sf).SalesforceConfiguration.SourceConfiguration).toEqual({
      HostUrl: "https://acme.my.salesforce.com", AuthType: "OAUTH2_CLIENT_CREDENTIALS", CredentialsSecretArn: SECRET });
    for (const t of [sp, sf]) {
      expect(roleStatements(t).find((s) => s.Sid === "ConnectorSecret")).toMatchObject({ Action: "secretsmanager:GetSecretValue", Resource: SECRET });
    }
  });

  it("refuses what Bedrock would refuse, and any store but OpenSearch Serverless", () => {
    const v = (dataSource: { type: string; [setting: string]: unknown }, over: Record<string, unknown> = {}) => () =>
      validateTools({ kb: { type: "kb", corpora: ["r"], vectorStore: OSS, dataSource, ...over } }, {}, "us-east-1");
    const web = { type: "web", seedUrls: ["https://docs.example.com/"] };
    expect(v(web)).not.toThrow();
    expect(v(web, { vectorStore: undefined })).toThrow(/needs vectorStore.type "opensearchServerless"/);
    expect(v(web, { s3Uri: "s3://b1b" })).toThrow(/takes the place of s3Uri/);
    expect(v({ type: "gdrive" })).toThrow(/type must be one of: web/);
    expect(v({ type: "web" })).toThrow(/seedUrls is required/);
    expect(v({ ...web, rateLimit: 999 })).toThrow(/rateLimit must be a whole number from 1 to 300/);
    expect(v({ ...web, scope: "all" })).toThrow(/scope must be one of: hostOnly, subdomains/);
    expect(v({ ...web, seedUrls: ["ftp://x"] })).toThrow(/does not match/);
    expect(v({ ...web, hostUrl: "https://x" })).toThrow(/hostUrl does not apply to the web connector/);
    expect(v({ type: "salesforce", hostUrl: "https://a.b", credentialsSecretArn: "arn:aws:secretsmanager:us-east-1::secret:x" }))
      .toThrow(/credentialsSecretArn .* does not match/);
    expect(v({ type: "confluence", hostUrl: "https://acme.atlassian.net", authType: "basic", credentialsSecretArn: SECRET,
      filters: [{ objectType: "" }] })).toThrow(/objectType/);
  });
});
