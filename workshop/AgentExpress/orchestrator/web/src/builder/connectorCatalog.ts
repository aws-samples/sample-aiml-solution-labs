/** Connectors: ready-made tool setups for major SaaS systems.
 *
 *  Each is data. Picking one and filling in its fields (the site or tenant, and the OAuth
 *  client id) writes an ordinary `tools.<key>` entry with keys the framework already has:
 *  type "openapi" with an inline `schema` (a few of the vendor's documented REST
 *  operations), and `auth` "user" (each person signs in, 3LO) or "oauth2" (the app's own
 *  client, 2LO) with `oauth`. The client secret is set afterwards on the tool, in the
 *  build's secrets, as for any OAuth tool. Nothing here adds a key, a route or runtime code.
 *
 *  Why OpenAPI rather than the vendors' remote MCP servers or the Gateway's own templates:
 *  - The Gateway's integration-provider templates can be added only in the AWS console,
 *    not through the API that Terraform and CDK use (docs: gateway-target-integrations).
 *    The operations below follow those templates where one exists.
 *  - An MCP tool that acts as the person must list its tools in `toolSchema`, which changes
 *    with each server release, and the framework sends an API key as X-API-Key, which
 *    neither GitHub's nor Atlassian's MCP server accepts.
 *  - The framework creates every OAuth provider as AgentCore Identity's CustomOauth2
 *    (terraform/tools.tf, cdk/lib/tool-plane.ts). `vendor` records the built-in
 *    credentialProviderVendor value for the same provider (botocore
 *    CreateOauth2CredentialProvider), for reference; the deploy does not use it.
 *
 *  Values the user types are checked by pattern before they are put into a URL, so a
 *  connector only ever points at its vendor's own domain (static server URLs, as the
 *  Gateway's OpenAPI guidance recommends). */
import { toolKeyFromName, uniqueKey, type Entry, type Json, type Project } from "./model";

export type ConnectorMethod = "user" | "app";

export interface ConnectorField {
  id: string;
  label: string;
  description: string;
  placeholder: string;
  pattern: RegExp;
  /** Shown when the value does not match `pattern`. */
  error: string;
  /** Applied before the check, e.g. lower-casing a host name. */
  normalize?: (v: string) => string;
}

/** How a method signs in. Strings may hold {fieldId} placeholders. */
export interface ConnectorAuth {
  /** One line under the choice. */
  about: string;
  tokenUrl?: string;
  authorizationUrl?: string;
  discoveryUrl?: string;
  scopes: string[];
}

interface Param { name: string; description: string; type?: "string" | "integer" | "boolean"; required?: boolean }
type Schema = { [k: string]: Json };
interface Operation {
  id: string;
  method: "get" | "post" | "put" | "patch";
  path: string;
  summary: string;
  query?: Param[];
  /** A JSON request body. */
  body?: Schema;
}

export interface Connector {
  id: string;
  name: string;
  /** What it lets an agent do, in a sentence. No logos. */
  description: string;
  /** The AgentCore Identity built-in vendor for this provider (reference only, see above). */
  vendor: string;
  fields: ConnectorField[];
  methods: Partial<Record<ConnectorMethod, ConnectorAuth>>;
  /** The REST base URL, with {fieldId} placeholders. */
  server: string;
  operations: Operation[];
  /** What to set up at the vendor before it works. */
  setup: string;
  /** What it does not do, said plainly. */
  limits: string;
  docs: { label: string; url: string }[];
}

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const lower = (v: string) => v.trim().toLowerCase();

const str = (description: string): Schema => ({ type: "string", description });
const obj = (description: string, properties?: Record<string, Schema>, required?: string[]): Schema => ({
  type: "object", description,
  ...(properties ? { properties } : {}),
  ...(required?.length ? { required } : {}),
});

const ATLASSIAN_CLOUD_ID: ConnectorField = {
  id: "cloudId", label: "Cloud ID",
  description: "Your Atlassian site's cloud ID. Open https://<your-site>.atlassian.net/_edge/tenant_info to see it.",
  placeholder: "11223344-a1b2-3b33-c444-def123456789",
  pattern: UUID, error: "A cloud ID looks like 11223344-a1b2-3b33-c444-def123456789", normalize: lower,
};
const ATLASSIAN_APP: ConnectorAuth = {
  about: "An Atlassian service account's OAuth 2.0 credential (client credentials).",
  tokenUrl: "https://auth.atlassian.com/oauth/token",
  // A service account's credential carries the scopes chosen when it was created.
  scopes: [],
};
const ATLASSIAN_LIMITS = "Acts as the service account, with its permissions. Each person signing in with their own "
  + "Atlassian account is not offered: Atlassian's sign-in needs an audience parameter that this framework's "
  + "OAuth provider does not send.";

export const CONNECTORS: Connector[] = [
  {
    id: "salesforce", name: "Salesforce",
    description: "Query, read, create and update Salesforce records (accounts, contacts, cases, opportunities, any object).",
    vendor: "SalesforceOauth2",
    fields: [{
      id: "domain", label: "My Domain address",
      description: "Your org's My Domain host name, as in https://<this>/.",
      placeholder: "acme.my.salesforce.com",
      pattern: /^[a-z0-9][a-z0-9-]*(\.[a-z0-9-]+)*\.my\.salesforce\.com$/,
      error: "Ends with .my.salesforce.com, e.g. acme.my.salesforce.com", normalize: lower,
    }],
    methods: {
      user: {
        about: "Each person connects their own Salesforce login (authorization code).",
        authorizationUrl: "https://{domain}/services/oauth2/authorize",
        tokenUrl: "https://{domain}/services/oauth2/token",
        scopes: ["api", "refresh_token"],
      },
      app: {
        about: "An External Client App with the Client Credentials Flow, acting as its Run As user.",
        tokenUrl: "https://{domain}/services/oauth2/token",
        scopes: [],
      },
    },
    server: "https://{domain}/services/data/v60.0",
    operations: [
      { id: "query", method: "get", path: "/query", summary: "Run a SOQL query, e.g. SELECT Id, Name FROM Account WHERE Name LIKE 'Acme%' LIMIT 10",
        query: [{ name: "q", description: "The SOQL query", required: true }] },
      { id: "getRecord", method: "get", path: "/sobjects/{sObject}/{id}", summary: "Get one record by its object type and id",
        query: [{ name: "fields", description: "Optional comma-separated field names to return" }] },
      { id: "createRecord", method: "post", path: "/sobjects/{sObject}", summary: "Create a record of an object type",
        body: obj("Field names and values, e.g. {\"Name\": \"Acme\"}") },
      { id: "updateRecord", method: "patch", path: "/sobjects/{sObject}/{id}", summary: "Update fields of one record",
        body: obj("Field names and their new values") },
    ],
    setup: "Create an External Client App (or Connected App) with OAuth enabled and the api scope. For each person "
      + "signing in, add the callback URL the tool shows after the first deploy. For the app's own login, enable the "
      + "Client Credentials Flow and set its Run As user.",
    limits: "Four operations over the REST API (v60.0): SOQL query and single-record read, create and update. No "
      + "delete, bulk, metadata or Apex calls.",
    docs: [
      { label: "REST API Developer Guide", url: "https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/intro_rest.htm" },
      { label: "Client Credentials Flow (Bedrock guide)", url: "https://docs.aws.amazon.com/bedrock/latest/userguide/kb-managed-salesforce-oauth2-setup.html" },
    ],
  },
  {
    id: "slack", name: "Slack",
    description: "List channels, read a channel's recent messages and post a message, as your Slack app.",
    vendor: "SlackOauth2",
    fields: [],
    methods: {
      user: {
        about: "Install your Slack app in the workspace (OAuth v2). Calls use the app's bot token.",
        authorizationUrl: "https://slack.com/oauth/v2/authorize",
        tokenUrl: "https://slack.com/api/oauth.v2.access",
        scopes: ["channels:read", "channels:history", "chat:write"],
      },
    },
    server: "https://slack.com/api",
    operations: [
      { id: "listChannels", method: "get", path: "/conversations.list", summary: "List public channels in the workspace",
        query: [{ name: "limit", description: "How many to return (up to 1000)", type: "integer" }] },
      { id: "channelHistory", method: "get", path: "/conversations.history", summary: "Read a channel's recent messages",
        query: [{ name: "channel", description: "The channel id, e.g. C0123456789", required: true },
          { name: "limit", description: "How many messages (up to 999)", type: "integer" }] },
      { id: "postMessage", method: "post", path: "/chat.postMessage", summary: "Post a message to a channel",
        body: obj("The message", { channel: str("The channel id"), text: str("The message text") }, ["channel", "text"]) },
    ],
    setup: "Create a Slack app with the bot scopes channels:read, channels:history and chat:write, and add the callback "
      + "URL the tool shows after the first deploy as a redirect URL.",
    limits: "Public channels the app was added to. Posts as the app, not as the person. No search, files or private "
      + "channels. Slack's token response names the token type \"bot\"; whether the Gateway accepts it from a custom "
      + "OAuth provider is not verified.",
    docs: [
      { label: "Installing with OAuth", url: "https://docs.slack.dev/authentication/installing-with-oauth" },
      { label: "Web API methods", url: "https://docs.slack.dev/reference/methods" },
    ],
  },
  {
    id: "jira", name: "Jira",
    description: "Search issues with JQL, read an issue, create one and add a comment, in Jira Cloud.",
    vendor: "AtlassianOauth2",
    fields: [ATLASSIAN_CLOUD_ID],
    methods: { app: ATLASSIAN_APP },
    server: "https://api.atlassian.com/ex/jira/{cloudId}",
    operations: [
      { id: "searchIssues", method: "get", path: "/rest/api/3/search/jql", summary: "Search issues with JQL, e.g. project = OPS AND status = \"In Progress\"",
        query: [{ name: "jql", description: "The JQL query", required: true },
          { name: "maxResults", description: "How many issues to return", type: "integer" },
          { name: "fields", description: "Optional comma-separated fields, e.g. summary,status,assignee" }] },
      { id: "getIssue", method: "get", path: "/rest/api/2/issue/{issueIdOrKey}", summary: "Get one issue by key or id, e.g. OPS-12" },
      { id: "createIssue", method: "post", path: "/rest/api/2/issue", summary: "Create an issue",
        body: obj("The new issue", { fields: obj("At least project {\"key\"}, summary and issuetype {\"name\"}; optionally description (plain text)") }, ["fields"]) },
      { id: "addComment", method: "post", path: "/rest/api/2/issue/{issueIdOrKey}/comment", summary: "Add a comment to an issue",
        body: obj("The comment", { body: str("The comment text") }, ["body"]) },
    ],
    setup: "In Atlassian Administration, create a service account, give it access to Jira, and create an OAuth 2.0 "
      + "credential for it with the scopes read:jira-work and write:jira-work. Its client ID goes here; its secret on the tool.",
    limits: ATLASSIAN_LIMITS + " No transitions, attachments or admin calls.",
    docs: [
      { label: "OAuth 2.0 credentials for service accounts", url: "https://support.atlassian.com/user-management/docs/create-oauth-2-0-credential-for-service-accounts/" },
      { label: "Jira Cloud REST API", url: "https://developer.atlassian.com/cloud/jira/platform/rest/v3/intro/" },
    ],
  },
  {
    id: "confluence", name: "Confluence",
    description: "Search pages with CQL, read a page, list spaces and create a page, in Confluence Cloud.",
    vendor: "AtlassianOauth2",
    fields: [ATLASSIAN_CLOUD_ID],
    methods: { app: ATLASSIAN_APP },
    server: "https://api.atlassian.com/ex/confluence/{cloudId}",
    operations: [
      { id: "searchContent", method: "get", path: "/wiki/rest/api/search", summary: "Search with CQL, e.g. type = page AND text ~ \"onboarding\"",
        query: [{ name: "cql", description: "The CQL query", required: true },
          { name: "limit", description: "How many results", type: "integer" }] },
      { id: "getPage", method: "get", path: "/wiki/api/v2/pages/{id}", summary: "Get one page by id",
        query: [{ name: "body-format", description: "storage to include the page body" }] },
      { id: "listSpaces", method: "get", path: "/wiki/api/v2/spaces", summary: "List spaces",
        query: [{ name: "limit", description: "How many spaces", type: "integer" }] },
      { id: "createPage", method: "post", path: "/wiki/api/v2/pages", summary: "Create a page in a space",
        body: obj("The new page", {
          spaceId: str("The space's id"), title: str("The page title"),
          body: obj("The page body", { representation: str("storage"), value: str("The body in Confluence storage format (XHTML)") }),
        }, ["spaceId", "title"]) },
    ],
    setup: "In Atlassian Administration, create a service account, give it access to Confluence, and create an OAuth "
      + "2.0 credential for it with the Confluence scopes it needs (search, read pages and spaces, write pages).",
    limits: ATLASSIAN_LIMITS + " No page updates, attachments or comments.",
    docs: [
      { label: "OAuth 2.0 credentials for service accounts", url: "https://support.atlassian.com/user-management/docs/create-oauth-2-0-credential-for-service-accounts/" },
      { label: "Confluence Cloud REST API v2", url: "https://developer.atlassian.com/cloud/confluence/rest/v2/intro/" },
    ],
  },
  {
    id: "servicenow", name: "ServiceNow",
    description: "List, read, create and update records in any ServiceNow table (incidents, changes, requests).",
    vendor: "CustomOauth2",
    fields: [{
      id: "instance", label: "Instance name",
      description: "The part before .service-now.com in your instance's address.",
      placeholder: "acme", pattern: /^[a-z0-9][a-z0-9-]{0,62}$/,
      error: "Letters, digits and -, e.g. acme for acme.service-now.com", normalize: lower,
    }],
    methods: {
      user: {
        about: "Each person connects their own ServiceNow login (authorization code).",
        authorizationUrl: "https://{instance}.service-now.com/oauth_auth.do",
        tokenUrl: "https://{instance}.service-now.com/oauth_token.do",
        scopes: [],
      },
      app: {
        about: "Client credentials, as the OAuth client's user. Needs the Washington DC release or later with that grant enabled.",
        tokenUrl: "https://{instance}.service-now.com/oauth_token.do",
        scopes: [],
      },
    },
    server: "https://{instance}.service-now.com",
    operations: [
      { id: "listRecords", method: "get", path: "/api/now/table/{tableName}", summary: "List records of a table, e.g. incident",
        query: [{ name: "sysparm_query", description: "An encoded query, e.g. active=true^priority=1" },
          { name: "sysparm_limit", description: "How many records", type: "integer" },
          { name: "sysparm_fields", description: "Optional comma-separated fields to return" }] },
      { id: "getRecord", method: "get", path: "/api/now/table/{tableName}/{sys_id}", summary: "Get one record by sys_id" },
      { id: "createRecord", method: "post", path: "/api/now/table/{tableName}", summary: "Create a record",
        body: obj("Field names and values, e.g. {\"short_description\": \"VPN down\"}") },
      { id: "updateRecord", method: "patch", path: "/api/now/table/{tableName}/{sys_id}", summary: "Update fields of one record",
        body: obj("Field names and their new values") },
    ],
    setup: "Create an OAuth API endpoint for external clients (Application Registry). For each person signing in, add "
      + "the callback URL the tool shows after the first deploy as its redirect URL.",
    limits: "The Table API only: no attachments, catalog ordering or scripted REST APIs. The OAuth client's or the "
      + "person's roles decide which tables it can read and write.",
    docs: [
      { label: "Table API", url: "https://docs.servicenow.com/bundle/rome-now-intelligence/page/integrate/inbound-rest/reference/table-GET.html" },
      { label: "Instance as OAuth authorization server", url: "https://docs.servicenow.com/bundle/quebec-platform-administration/page/administer/security/task/t_InstanceAsAuthorizationServer.html" },
    ],
  },
  {
    id: "github", name: "GitHub",
    description: "Search issues and pull requests, read an issue, open one and comment, as the person using the app.",
    vendor: "GithubOauth2",
    fields: [],
    methods: {
      user: {
        about: "Each person connects their own GitHub account through your OAuth app.",
        authorizationUrl: "https://github.com/login/oauth/authorize",
        tokenUrl: "https://github.com/login/oauth/access_token",
        scopes: ["repo"],
      },
    },
    server: "https://api.github.com",
    operations: [
      { id: "searchIssues", method: "get", path: "/search/issues", summary: "Search issues and pull requests, e.g. repo:octo/app is:open label:bug",
        query: [{ name: "q", description: "GitHub search syntax", required: true },
          { name: "per_page", description: "How many results (up to 100)", type: "integer" }] },
      { id: "getIssue", method: "get", path: "/repos/{owner}/{repo}/issues/{issue_number}", summary: "Get one issue or pull request" },
      { id: "createIssue", method: "post", path: "/repos/{owner}/{repo}/issues", summary: "Open an issue",
        body: obj("The new issue", { title: str("The title"), body: str("The description, in Markdown") }, ["title"]) },
      { id: "addIssueComment", method: "post", path: "/repos/{owner}/{repo}/issues/{issue_number}/comments", summary: "Comment on an issue or pull request",
        body: obj("The comment", { body: str("The comment, in Markdown") }, ["body"]) },
    ],
    setup: "Register an OAuth app on GitHub and set its authorization callback URL to the one the tool shows after "
      + "the first deploy.",
    limits: "github.com only (not GitHub Enterprise Server). No code, commits or reviews. GitHub returns its token as "
      + "form data unless asked for JSON; whether AgentCore Identity's custom provider asks is not verified.",
    docs: [
      { label: "Authorizing OAuth apps", url: "https://docs.github.com/en/apps/oauth-apps/building-oauth-apps/authorizing-oauth-apps" },
      { label: "REST API: issues", url: "https://docs.github.com/en/rest/issues/issues" },
    ],
  },
  {
    id: "microsoft365", name: "Microsoft 365",
    description: "Search mail, files and sites, read the calendar, send mail and post in a Teams channel, through Microsoft Graph.",
    vendor: "MicrosoftOauth2",
    fields: [{
      id: "tenant", label: "Tenant",
      description: "Your Microsoft Entra tenant ID or primary domain.",
      placeholder: "contoso.onmicrosoft.com",
      pattern: /^([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|[a-z0-9-]+(\.[a-z0-9-]+)+)$/,
      error: "A tenant ID (a GUID) or a domain, e.g. contoso.onmicrosoft.com", normalize: lower,
    }],
    methods: {
      user: {
        about: "Each person signs in with their work account (delegated permissions).",
        discoveryUrl: "https://login.microsoftonline.com/{tenant}/v2.0/.well-known/openid-configuration",
        scopes: ["offline_access", "User.Read", "Mail.Read", "Mail.Send", "Calendars.Read", "Files.Read.All", "Sites.Read.All", "ChannelMessage.Send"],
      },
    },
    server: "https://graph.microsoft.com/v1.0",
    operations: [
      { id: "search", method: "post", path: "/search/query", summary: "Search Microsoft 365: mail (message), files (driveItem), SharePoint (site, listItem) or events",
        body: obj("The search", {
          requests: { type: "array", description: "One search request", items: obj("A search request", {
            entityTypes: { type: "array", items: { type: "string" }, description: "e.g. [\"driveItem\"] or [\"message\"]" },
            query: obj("The query", { queryString: str("The words to search for") }, ["queryString"]),
            size: { type: "integer", description: "How many results" },
          }, ["entityTypes", "query"]) },
        }, ["requests"]) },
      { id: "listEvents", method: "get", path: "/me/calendarView", summary: "List the person's calendar events between two times",
        query: [{ name: "startDateTime", description: "ISO 8601, e.g. 2025-01-06T00:00:00Z", required: true },
          { name: "endDateTime", description: "ISO 8601, e.g. 2025-01-13T00:00:00Z", required: true }] },
      { id: "sendMail", method: "post", path: "/me/sendMail", summary: "Send an email as the person",
        body: obj("The email", {
          message: obj("The message", {
            subject: str("The subject"),
            body: obj("The body", { contentType: str("Text or HTML"), content: str("The body text") }),
            toRecipients: { type: "array", description: "Who it goes to", items: obj("A recipient", {
              emailAddress: obj("The address", { address: str("An email address") }, ["address"]) }) },
          }, ["subject", "body", "toRecipients"]),
        }, ["message"]) },
      { id: "postChannelMessage", method: "post", path: "/teams/{teamId}/channels/{channelId}/messages", summary: "Post a message in a Teams channel",
        body: obj("The message", { body: obj("The message body", { content: str("The message text") }, ["content"]) }, ["body"]) },
    ],
    setup: "Register an app in Microsoft Entra ID with these delegated Microsoft Graph permissions and a client secret, "
      + "and add the callback URL the tool shows after the first deploy as a Web redirect URI.",
    limits: "Work or school accounts only. Acts as the person, so it sees only what they can. Sending mail and "
      + "posting in Teams act immediately: consider `approval` on the tool.",
    docs: [
      { label: "Microsoft Search API", url: "https://learn.microsoft.com/en-us/graph/api/search-query?view=graph-rest-1.0" },
      { label: "List calendarView", url: "https://learn.microsoft.com/en-us/graph/api/user-list-calendarview?view=graph-rest-1.0" },
      { label: "Send mail", url: "https://learn.microsoft.com/en-us/graph/api/user-sendmail?view=graph-rest-1.0" },
    ],
  },
  {
    id: "googleWorkspace", name: "Google Workspace",
    description: "Search the person's Google Drive files and read their Google Calendar events.",
    vendor: "GoogleOauth2",
    fields: [],
    methods: {
      user: {
        about: "Each person connects their own Google account.",
        discoveryUrl: "https://accounts.google.com/.well-known/openid-configuration",
        scopes: ["https://www.googleapis.com/auth/drive.readonly", "https://www.googleapis.com/auth/calendar.readonly"],
      },
    },
    server: "https://www.googleapis.com",
    operations: [
      { id: "searchFiles", method: "get", path: "/drive/v3/files", summary: "Search Drive files, e.g. q = name contains 'budget' and trashed = false",
        query: [{ name: "q", description: "A Drive search query" },
          { name: "pageSize", description: "How many files", type: "integer" }] },
      { id: "getFile", method: "get", path: "/drive/v3/files/{fileId}", summary: "Get one file's details",
        query: [{ name: "fields", description: "Optional, e.g. id,name,mimeType,webViewLink,modifiedTime" }] },
      { id: "listEvents", method: "get", path: "/calendar/v3/calendars/{calendarId}/events", summary: "List events on a calendar (primary for the person's own)",
        query: [{ name: "timeMin", description: "RFC 3339 start, e.g. 2025-01-06T00:00:00Z" },
          { name: "timeMax", description: "RFC 3339 end" },
          { name: "q", description: "Optional words to match" },
          { name: "singleEvents", description: "true to expand recurring events", type: "boolean" }] },
    ],
    setup: "In Google Cloud, enable the Drive and Calendar APIs, configure the OAuth consent screen, and create a Web "
      + "OAuth client with the callback URL the tool shows after the first deploy as a redirect URI.",
    limits: "Read only: file details, not their contents, and events. drive.readonly is a restricted scope, so an app "
      + "used outside your Workspace organization needs Google's verification. Google returns a refresh token only "
      + "when asked for offline access, which the custom provider does not request, so people may need to connect again.",
    docs: [
      { label: "Drive files.list", url: "https://developers.google.com/workspace/drive/api/reference/rest/v3/files/list" },
      { label: "Calendar events.list", url: "https://developers.google.com/workspace/calendar/api/v3/reference/events/list" },
    ],
  },
  {
    id: "zendesk", name: "Zendesk",
    description: "Search tickets, users and organizations, read a ticket, create one and update it, in Zendesk Support.",
    vendor: "CustomOauth2",
    fields: [{
      id: "subdomain", label: "Subdomain",
      description: "The part before .zendesk.com in your Zendesk address.",
      placeholder: "acme", pattern: /^[a-z0-9][a-z0-9-]{0,62}$/,
      error: "Letters, digits and -, e.g. acme for acme.zendesk.com", normalize: lower,
    }],
    methods: {
      app: {
        about: "A confidential OAuth client with the client credentials grant, as the client's user.",
        tokenUrl: "https://{subdomain}.zendesk.com/oauth/tokens",
        scopes: ["read", "write"],
      },
    },
    server: "https://{subdomain}.zendesk.com",
    operations: [
      { id: "search", method: "get", path: "/api/v2/search", summary: "Search, e.g. type:ticket status:open printer",
        query: [{ name: "query", description: "Zendesk search syntax", required: true }] },
      { id: "showTicket", method: "get", path: "/api/v2/tickets/{ticket_id}", summary: "Get one ticket" },
      { id: "createTicket", method: "post", path: "/api/v2/tickets", summary: "Create a ticket",
        body: obj("The new ticket", { ticket: obj("subject, comment {\"body\"}, and optionally priority, requester") }, ["ticket"]) },
      { id: "updateTicket", method: "put", path: "/api/v2/tickets/{ticket_id}", summary: "Update a ticket or add a comment",
        body: obj("The change", { ticket: obj("Fields to change, or comment {\"body\", \"public\"}") }, ["ticket"]) },
    ],
    setup: "In Admin Center (Apps and integrations > APIs > OAuth clients), create a confidential OAuth client. Its "
      + "identifier is the client ID; its secret goes on the tool.",
    limits: "Acts as the OAuth client's user. Each person signing in is not offered here. No attachments, macros or "
      + "Help Center content.",
    docs: [
      { label: "OAuth tokens for grant types", url: "https://developer.zendesk.com/api-reference/ticketing/oauth/grant_type_tokens/" },
      { label: "Tickets API", url: "https://developer.zendesk.com/api-reference/ticketing/tickets/tickets/" },
    ],
  },
  {
    id: "hubspot", name: "HubSpot",
    description: "Search and read CRM contacts, companies and deals, and create a contact.",
    vendor: "HubspotOauth2",
    fields: [],
    methods: {
      user: {
        about: "Each person connects their HubSpot account through your app.",
        authorizationUrl: "https://app.hubspot.com/oauth/authorize",
        tokenUrl: "https://api.hubapi.com/oauth/v3/token",
        scopes: ["oauth", "crm.objects.contacts.read", "crm.objects.contacts.write", "crm.objects.companies.read", "crm.objects.deals.read"],
      },
    },
    server: "https://api.hubapi.com",
    operations: [
      { id: "searchObjects", method: "post", path: "/crm/v3/objects/{objectType}/search", summary: "Search contacts, companies or deals (objectType)",
        body: obj("The search", {
          query: str("Words to search for"),
          limit: { type: "integer", description: "How many results (up to 200)" },
          properties: { type: "array", items: { type: "string" }, description: "Properties to return, e.g. [\"email\", \"firstname\"]" },
        }) },
      { id: "getObject", method: "get", path: "/crm/v3/objects/{objectType}/{objectId}", summary: "Get one contact, company or deal",
        query: [{ name: "properties", description: "Optional comma-separated properties" }] },
      { id: "createContact", method: "post", path: "/crm/v3/objects/contacts", summary: "Create a contact",
        body: obj("The contact", { properties: obj("e.g. {\"email\": \"a@example.com\", \"firstname\": \"Ana\"}") }, ["properties"]) },
    ],
    setup: "Create a HubSpot app with these scopes and add the callback URL the tool shows after the first deploy as "
      + "a redirect URL.",
    limits: "Contacts, companies and deals only; creates contacts but changes nothing else.",
    docs: [
      { label: "Working with OAuth", url: "https://developers.hubspot.com/docs/apps/developer-platform/build-apps/authentication/oauth/working-with-oauth" },
      { label: "CRM search", url: "https://developers.hubspot.com/docs/api-reference/latest/crm/search-the-crm" },
    ],
  },
];

/** The methods a connector offers, the one to start with first. */
export function methodsOf(c: Connector): ConnectorMethod[] {
  return (["user", "app"] as ConnectorMethod[]).filter((m) => c.methods[m]);
}

/** Why a field's value cannot be used, or "" when it can. */
export function fieldError(f: ConnectorField, raw: string): string {
  const v = (f.normalize ?? ((x: string) => x.trim()))(raw);
  if (!v) return "Required";
  return f.pattern.test(v) ? "" : f.error;
}

const fill = (s: string, values: Record<string, string>) =>
  s.replace(/\{(\w+)\}/g, (m, id: string) => (id in values ? values[id] : m));

/** The OpenAPI 3 document: one operation per entry, its path parameters taken from the path. */
export function openApiOf(c: Connector, values: Record<string, string>): Schema {
  const paths: Record<string, Schema> = {};
  for (const op of c.operations) {
    const inPath = [...op.path.matchAll(/\{([^}]+)\}/g)].map((m) => m[1]);
    const parameters: Json[] = [
      ...inPath.map((name) => ({ name, in: "path", required: true, schema: { type: "string" } })),
      ...(op.query ?? []).map((p) => ({
        name: p.name, in: "query", description: p.description, required: Boolean(p.required),
        schema: { type: p.type ?? "string" },
      })),
    ];
    const item = (paths[op.path] ?? {}) as Schema;
    item[op.method] = {
      operationId: op.id, summary: op.summary,
      ...(parameters.length ? { parameters } : {}),
      ...(op.body ? { requestBody: { required: true, content: { "application/json": { schema: op.body } } } } : {}),
      responses: { "200": { description: "OK" } },
    };
    paths[op.path] = item;
  }
  return {
    openapi: "3.0.1",
    info: { title: c.name, version: "1" },
    servers: [{ url: fill(c.server, values) }],
    paths,
  };
}

/** The values as they go into URLs: normalized, and only if every one is valid. */
export function cleanValues(c: Connector, raw: Record<string, string>): Record<string, string> | null {
  const out: Record<string, string> = {};
  for (const f of c.fields) {
    if (fieldError(f, raw[f.id] ?? "")) return null;
    out[f.id] = (f.normalize ?? ((x: string) => x.trim()))(raw[f.id] ?? "");
  }
  return out;
}

/** The tools.<key> entry a connector writes. Throws on a field that is not valid. */
export function connectorTool(c: Connector, raw: Record<string, string>, method: ConnectorMethod, clientId: string): Entry {
  const values = cleanValues(c, raw);
  const auth = c.methods[method];
  if (!values || !auth) throw new Error(`${c.name}: not every field is valid`);
  const site = c.fields.map((f) => values[f.id]).join(", ");
  const oauth: Record<string, Json> = { clientId: clientId.trim(), scopes: [...auth.scopes] };
  if (auth.discoveryUrl) oauth.discoveryUrl = fill(auth.discoveryUrl, values);
  if (auth.tokenUrl) oauth.tokenUrl = fill(auth.tokenUrl, values);
  if (method === "user" && auth.authorizationUrl) oauth.authorizationUrl = fill(auth.authorizationUrl, values);
  return {
    type: "openapi",
    // The Gateway target's description (cut to 190 characters on deploy).
    description: `${c.name}${site ? ` (${site})` : ""}: ${c.description}`.slice(0, 190),
    schema: openApiOf(c, values),
    auth: method === "user" ? "user" : "oauth2",
    oauth,
  };
}

/** Add a connector's tool to the build, under a key of its own. */
export function addConnector(project: Project, c: Connector, raw: Record<string, string>, method: ConnectorMethod,
  clientId: string): { project: Project; key: string } {
  const entry = connectorTool(c, raw, method, clientId);
  const key = uniqueKey(toolKeyFromName(c.name), Object.keys(project.workflow.tools), "");
  const next = structuredClone(project);
  next.workflow.tools[key] = entry;
  return { project: next, key };
}

/** Whether a connector matches the words typed in the search box. */
export function matches(c: Connector, text: string): boolean {
  const words = text.toLowerCase().split(/\s+/).filter(Boolean);
  const hay = `${c.name} ${c.description}`.toLowerCase();
  return words.every((w) => hay.includes(w));
}
