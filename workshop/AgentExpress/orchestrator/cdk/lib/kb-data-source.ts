/**
 * A type="kb" tool's `dataSource`: documents from a web crawl or a Confluence, SharePoint or
 * Salesforce site, as the Knowledge Base's data source instead of S3, and the Knowledge
 * Base role's read on the connector's secret.
 *
 * Mirrors terraform/kb_data_source.tf (locals, the KB role statement, the precondition),
 * bff/kb_data_source.py and web/src/builder/kbDataSource.ts (the Build view's checks).
 *
 * What the docs and CfnDataSource allow, and what this uses:
 *   - DataSourceConfiguration types WEB, CONFLUENCE, SHAREPOINT, SALESFORCE; each only on an
 *     OpenSearch Serverless vector store ("Currently, only Amazon OpenSearch Serverless
 *     vector store is available to use with this data source").
 *   - Confluence hostType SAAS (the only value); SharePoint hostType ONLINE and authType
 *     OAUTH2_CLIENT_CREDENTIALS (App-Only was retired by Microsoft on April 2, 2026);
 *     Salesforce authType OAUTH2_CLIENT_CREDENTIALS (the only value).
 *   - Content filters: CrawlFilterConfiguration type PATTERN with patternObjectFilter.
 *   - KB role: secretsmanager:GetSecretValue on the secret; Confluence with OAuth also
 *     PutSecretValue, since Bedrock rotates the refresh token into it.
 * https://docs.aws.amazon.com/bedrock/latest/userguide/data-source-connectors.html
 * https://docs.aws.amazon.com/bedrock/latest/userguide/kb-permissions.html
 */
import * as iam from "aws-cdk-lib/aws-iam";
import type * as bedrock from "aws-cdk-lib/aws-bedrock";
import * as vocab from "./vocabulary";

export interface KbConnectorFilter { objectType: string; include?: string[]; exclude?: string[] }
export interface KbConnector { type: string; [setting: string]: unknown }
interface Spec { type: string; pattern?: string; min?: number; max?: number; values?: Record<string, string> }

const extra = <T>(field: string): T => vocab.extra<T>("kbDataSources", field) as T;
export const CONNECTOR_STORE = "opensearchServerless";

/** The checks a synth makes, as errors: the same rules terraform/kb.tf applies at plan. */
export function validateKbConnector(name: string, t: { dataSource?: KbConnector; vectorStore?: { type?: string }; s3Uri?: string }): void {
  const ds = t.dataSource;
  if (ds === undefined) return;
  const where = `workflow.json tools.${name}.dataSource`;
  const kinds = vocab.values("kbDataSources");
  if (!kinds.includes(ds?.type)) throw new Error(`${where}.type must be one of: ${kinds.join(", ")}.`);
  if (t.vectorStore?.type !== CONNECTOR_STORE) {
    throw new Error(`${where} needs vectorStore.type "${CONNECTOR_STORE}": AWS supports the ${ds.type} connector only on OpenSearch Serverless.`);
  }
  if (t.s3Uri) throw new Error(`${where} takes the place of s3Uri: the documents come from one or the other.`);
  const required = extra<Record<string, string[]>>("required")[ds.type];
  const takes = [...required, ...extra<Record<string, string[]>>("optional")[ds.type]];
  for (const k of required) {
    const v = ds[k];
    if (v === undefined || v === null || v === "" || (Array.isArray(v) && !v.length)) throw new Error(`${where}.${k} is required for the ${ds.type} connector.`);
  }
  const specs = extra<Record<string, Spec>>("settings");
  for (const [k, v] of Object.entries(ds)) {
    if (k === "type") continue;
    if (!takes.includes(k)) throw new Error(`${where}.${k} does not apply to the ${ds.type} connector.`);
    const s = specs[k];
    const bad = (why: string) => { throw new Error(`${where}.${k} ${why}.`); };
    if (s.type === "string" && (typeof v !== "string" || !new RegExp(s.pattern!).test(v))) bad(`"${v}" does not match ${s.pattern}`);
    if (s.type === "enum" && !(typeof v === "string" && v in s.values!)) bad(`must be one of: ${Object.keys(s.values!).join(", ")}`);
    if (s.type === "int" && !(typeof v === "number" && Number.isInteger(v) && v >= s.min! && v <= s.max!)) bad(`must be a whole number from ${s.min} to ${s.max}`);
    if (s.type === "list") {
      if (!Array.isArray(v) || v.length < s.min! || v.length > s.max! || !v.every((x) => typeof x === "string")) bad(`must list ${s.min} to ${s.max}`);
      for (const x of v as string[]) if (s.pattern && !new RegExp(s.pattern).test(x)) bad(`"${x}" does not match ${s.pattern}`);
    }
    if (s.type === "filters") {
      if (!Array.isArray(v) || v.length < s.min! || v.length > s.max!) bad(`must list ${s.min} to ${s.max} filters`);
      for (const f of v as any[]) {
        if (typeof f?.objectType !== "string" || f.objectType.length < 1 || f.objectType.length > 50) bad("needs an objectType on every filter");
        for (const x of ["include", "exclude"]) {
          if (x in f && !(Array.isArray(f[x]) && f[x].length >= 1 && f[x].length <= 25)) bad(`${x} must list 1 to 25 patterns`);
        }
        if (Object.keys(f).some((x) => !["objectType", "include", "exclude"].includes(x))) bad("filters take objectType, include and exclude only");
      }
    }
  }
}

const strs = (v: unknown): string[] => (Array.isArray(v) ? v.map(String) : []);
const enumApi = (setting: string, v: unknown): string => extra<Record<string, Spec>>("settings")[setting].values![String(v)];

function crawlFilters(ds: KbConnector) {
  const fs = (ds.filters ?? []) as KbConnectorFilter[];
  if (!fs.length) return {};
  return {
    crawlerConfiguration: {
      filterConfiguration: {
        type: "PATTERN",
        patternObjectFilter: {
          filters: fs.map((f) => ({
            objectType: f.objectType,
            ...(f.include ? { inclusionFilters: f.include } : {}),
            ...(f.exclude ? { exclusionFilters: f.exclude } : {}),
          })),
        },
      },
    },
  };
}

/** The data source's configuration for the connector, and the KB role's grant on its secret. */
export function kbConnector(ds: KbConnector): {
  dataSourceConfiguration: bedrock.CfnDataSource.DataSourceConfigurationProperty; statements: iam.PolicyStatement[];
} {
  const type = extra<Record<string, string>>("api")[ds.type];
  const secret = String(ds.credentialsSecretArn ?? "");
  const refreshes = ds.type === "confluence" && ds.authType === "oauth2";
  const statements = secret ? [new iam.PolicyStatement({
    sid: "ConnectorSecret",
    actions: refreshes ? ["secretsmanager:GetSecretValue", "secretsmanager:PutSecretValue"] : ["secretsmanager:GetSecretValue"],
    resources: [secret],
  })] : [];
  switch (ds.type) {
    case "web": {
      const limits = { ...(ds.rateLimit !== undefined ? { rateLimit: Number(ds.rateLimit) } : {}),
        ...(ds.maxPages !== undefined ? { maxPages: Number(ds.maxPages) } : {}) };
      const crawler = {
        ...(Object.keys(limits).length ? { crawlerLimits: limits } : {}),
        ...(ds.inclusionFilters ? { inclusionFilters: strs(ds.inclusionFilters) } : {}),
        ...(ds.exclusionFilters ? { exclusionFilters: strs(ds.exclusionFilters) } : {}),
        ...(ds.scope ? { scope: enumApi("scope", ds.scope) } : {}),
        ...(ds.userAgent ? { userAgent: String(ds.userAgent) } : {}),
      };
      return {
        dataSourceConfiguration: {
          type,
          webConfiguration: {
            sourceConfiguration: { urlConfiguration: { seedUrls: strs(ds.seedUrls).map((url) => ({ url })) } },
            ...(Object.keys(crawler).length ? { crawlerConfiguration: crawler } : {}),
          },
        },
        statements,
      };
    }
    case "confluence":
      return {
        dataSourceConfiguration: {
          type,
          confluenceConfiguration: {
            sourceConfiguration: { hostUrl: String(ds.hostUrl), hostType: "SAAS", authType: enumApi("authType", ds.authType), credentialsSecretArn: secret },
            ...crawlFilters(ds),
          },
        },
        statements,
      };
    case "sharepoint":
      return {
        dataSourceConfiguration: {
          type,
          sharePointConfiguration: {
            sourceConfiguration: {
              domain: String(ds.domain), siteUrls: strs(ds.siteUrls), hostType: "ONLINE", authType: "OAUTH2_CLIENT_CREDENTIALS",
              credentialsSecretArn: secret, ...(ds.tenantId ? { tenantId: String(ds.tenantId) } : {}),
            },
            ...crawlFilters(ds),
          },
        },
        statements,
      };
    case "salesforce":
      return {
        dataSourceConfiguration: {
          type,
          salesforceConfiguration: {
            sourceConfiguration: { hostUrl: String(ds.hostUrl), authType: "OAUTH2_CLIENT_CREDENTIALS", credentialsSecretArn: secret },
            ...crawlFilters(ds),
          },
        },
        statements,
      };
    default:
      throw new Error(`dataSource type ${ds.type} is not a known connector.`);
  }
}
