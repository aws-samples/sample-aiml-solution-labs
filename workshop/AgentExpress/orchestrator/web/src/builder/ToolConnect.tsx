/** How a tool connects: one question per tool, and only the answers its kind allows.
 *
 *  It writes the same keys as before (tools.<key>.auth / oauth / identity), so nothing
 *  downstream changes: "Public" is auth "none", "IAM role" is "sigv4", "API key" is
 *  "apikey", "App login" (2LO) is "oauth2", "User login" (3LO) is "user", "On behalf of
 *  user" (OBO) is "obo"; `identity`, a saved login, for an API key or app login. The
 *  secret is the build's (toolApiKeys / identitySecrets), set here, never shown again. */
import Box from "@cloudscape-design/components/box";
import FormField from "@cloudscape-design/components/form-field";
import Input from "@cloudscape-design/components/input";
import Select from "@cloudscape-design/components/select";
import SpaceBetween from "@cloudscape-design/components/space-between";
import Tiles from "@cloudscape-design/components/tiles";

import { SecretField } from "./BuildResources";
import type { Entry, Json, Project } from "./model";
import { updateEntry } from "./model";

export type Method = "none" | "aws" | "apikey" | "app" | "user" | "obo";
type ToolEntry = Entry & { type?: string; auth?: string; identity?: string; oauth?: Record<string, Json> };

export const METHOD: Record<Method, { label: string; about: string; sees: "app" | "person" }> = {
  none: { label: "Public", about: "No sign-in", sees: "app" },
  aws: { label: "IAM role", about: "SigV4, no keys", sees: "app" },
  apikey: { label: "API key", about: "Shared key", sees: "app" },
  app: { label: "App login", about: "2LO, app's client", sees: "app" },
  user: { label: "User login", about: "3LO, each user connects", sees: "person" },
  obo: { label: "On behalf of user", about: "OBO, user's sign-in", sees: "person" },
};
/** The ones that use an OAuth client (tools.<key>.oauth). */
const OAUTH: Method[] = ["app", "user", "obo"];

/** What each kind of tool can use (AgentCore Gateway's outbound auth, by target type). */
export function methodsFor(type: string): Method[] {
  switch (type) {
    case "mcp": case "openapi": return ["none", "aws", "apikey", "app", "user", "obo"];
    case "apigateway": return ["none", "aws", "apikey"];
    default: return [];       // lambda, kb: always AWS; websearch: managed by AWS
  }
}
/** What a tool whose kind allows no choice connects with. */
export function fixedOf(type: string): string {
  void type; // lambda, kb and websearch: the Gateway's own role, always
  return "IAM role";
}

/** The role an "IAM role" tool is called as, and what it may reach: the build's Gateway
 *  role (AgentCoreGateway-<agentName>, cdk/lib/tool-plane.ts and terraform/gateway.tf),
 *  granted that one tool only. Shown on the tool and as the tables' tooltip. */
export function gatewayRole(agentName?: string): string {
  return agentName ? `AgentCoreGateway-${agentName}` : "";
}
export function iamNote(tool: { type?: unknown; service?: unknown }, agentName?: string): string {
  const role = gatewayRole(agentName);
  const who = role ? `this build's Gateway role, ${role}` : "this build's Gateway role";
  switch (String(tool.type ?? "")) {
    case "apigateway": return `Signed by ${who}, which may call only this API and stage.`;
    case "lambda": return `Called by ${who}, which may invoke only this function. The function's own role controls what it reaches.`;
    case "kb": return `Read by ${who}, which may query only this Knowledge Base.`;
    case "websearch": return `Called by ${who}, through AWS's managed web search.`;
    default: return `Signed by ${who}${tool.service ? `, for ${String(tool.service)}` : ""}.`;
  }
}

type Identities = Record<string, Entry>;
function identitiesOf(project: Project): Identities {
  return ((project.workflow as Record<string, unknown>).identities ?? {}) as Identities;
}

/** How a tool connects now: its method and, when it uses one, its saved login. */
export function connectionOf(tool: ToolEntry, project: Project): { method: Method | "fixed"; login?: string } {
  const type = String(tool.type ?? "");
  if (!methodsFor(type).length) return { method: "fixed" };
  if (tool.identity) {
    const t = String(identitiesOf(project)[tool.identity]?.type ?? "");
    return { method: t === "apikey" ? "apikey" : "app", login: tool.identity };
  }
  const auth = String(tool.auth ?? "");
  return { method: auth === "sigv4" ? "aws" : auth === "apikey" ? "apikey" : auth === "oauth2" ? "app"
    : auth === "user" ? "user" : auth === "obo" ? "obo" : "none" };
}

/** The tool with this method: the old keys for it set, the rest removed. */
export function withMethod(tool: ToolEntry, method: Method, login?: string): ToolEntry {
  const { auth: _a, oauth, identity: _i, service: _s, ...rest } = tool as ToolEntry & { service?: string };
  if (login) return { ...rest, identity: login };
  if (method === "none") return { ...rest, auth: "none" };
  if (method === "aws") return { ...rest, auth: "sigv4" };
  if (method === "apikey") return { ...rest, auth: "apikey" };
  const keep = { ...(oauth ?? { clientId: "", scopes: [] }) };
  if (method !== "user") delete keep.authorizationUrl;
  if (method !== "obo") delete keep.audience;
  return { ...rest, auth: method === "app" ? "oauth2" : method, oauth: keep };
}

/** "https://idp/.well-known/openid-configuration" or a token URL: one field, either. */
function providerUrl(oauth: Record<string, Json> | undefined): string {
  return String(oauth?.discoveryUrl ?? oauth?.tokenUrl ?? "");
}
function setProviderUrl(oauth: Record<string, Json>, url: string): Record<string, Json> {
  const { discoveryUrl: _d, tokenUrl: _t, ...rest } = oauth;
  const v = url.trim();
  if (!v) return rest;
  return /\/\.well-known\/openid-configuration$/.test(v) ? { ...rest, discoveryUrl: v } : { ...rest, tokenUrl: v };
}

export function ToolConnect({ project, id, onChange, server, callbackUrl, agentName }: {
  project: Project; id: string; onChange: (p: Project) => void; server: boolean;
  /** The build's fixed AWS name: names the Gateway role an "IAM role" tool is called as. */
  agentName?: string;
  /** For "User login" (3LO): the address to register at the tool's provider. */
  callbackUrl?: string;
}) {
  const tool = (project.workflow.tools[id] ?? {}) as ToolEntry;
  const type = String(tool.type ?? "");
  const methods = methodsFor(type);
  const cur = connectionOf(tool, project);
  const set = (next: ToolEntry) => onChange(updateEntry(project, "tools", id, next));

  if (cur.method === "fixed") {
    return <RoleNote tool={tool} agentName={agentName} lead={`${fixedOf(type)} (SigV4). Nothing to set up.`} />;
  }
  const method = cur.method;
  const saved = Object.entries(identitiesOf(project))
    .filter(([, e]) => String(e.type) === (method === "apikey" ? "apikey" : "oauth2"))
    .map(([name, e]) => ({ value: name, label: name, description: String(e.description ?? "") }));
  const NEW = { value: "", label: "New, for this tool only" };
  const oauth = (tool.oauth ?? {}) as Record<string, Json>;
  const setOauth = (o: Record<string, Json>) => set({ ...tool, oauth: o });
  const signIn = (((project.workflow as Record<string, unknown>).authorization ?? {}) as Record<string, Record<string, unknown>>).signIn?.provider;
  /** Why a choice cannot be used for this build, if it cannot. */
  const unavailable = (m: Method) => (m === "obo" && signIn === "entra"
    ? "Not with Entra ID sign-in yet" : undefined);

  return (
    <SpaceBetween size="m">
      <Tiles value={method} columns={3} ariaLabel="How it connects"
        onChange={({ detail }) => set(withMethod(tool, detail.value as Method))}
        items={methods.map((m) => ({ value: m, label: METHOD[m].label, description: unavailable(m) ?? METHOD[m].about,
          disabled: Boolean(unavailable(m)) && m !== method }))} />

      {(method === "apikey" || method === "app") && saved.length ? (
        <FormField label="Login">
          <Select selectedOption={saved.find((o) => o.value === cur.login) ?? NEW} options={[NEW, ...saved]} ariaLabel="Login"
            onChange={({ detail }) => set(withMethod(tool, method, String(detail.selectedOption.value) || undefined))} />
        </FormField>
      ) : null}

      {method === "apikey" && !cur.login && server ? (
        <SecretField buildId={project.id} kind="toolApiKeys" name={id} label="API key"
          description="Sent by the Gateway; the agent never sees it." />
      ) : null}

      {OAUTH.includes(method) && !cur.login ? (
        <SpaceBetween size="s">
          <FormField label="Provider address" description="Its token URL, or its .well-known/openid-configuration address."
            errorText={providerUrl(oauth) && !/^https:\/\//.test(providerUrl(oauth)) ? "Starts with https://" : undefined}>
            <Input value={providerUrl(oauth)} placeholder="https://login.example.com/oauth2/token" ariaLabel="Provider address"
              onChange={({ detail }) => setOauth(setProviderUrl(oauth, detail.value))} />
          </FormField>
          {method === "user" && oauth.tokenUrl ? (
            <FormField label="Sign-in address" description="Where people sign in to it: its authorize URL.">
              <Input value={String(oauth.authorizationUrl ?? "")} placeholder="https://login.example.com/oauth2/authorize"
                ariaLabel="Sign-in address" onChange={({ detail }) => setOauth({ ...oauth, authorizationUrl: detail.value.trim() })} />
            </FormField>
          ) : null}
          <FormField label="Client ID">
            <Input value={String(oauth.clientId ?? "")} ariaLabel="Client ID"
              onChange={({ detail }) => setOauth({ ...oauth, clientId: detail.value.trim() })} />
          </FormField>
          <FormField label="Scopes" description="Optional, separated by spaces.">
            <Input value={Array.isArray(oauth.scopes) ? oauth.scopes.join(" ") : ""} placeholder="orders.read" ariaLabel="Scopes"
              onChange={({ detail }) => setOauth({ ...oauth, scopes: detail.value.split(/[\s,]+/).filter(Boolean) })} />
          </FormField>
          {method === "obo" ? (
            <FormField label="Tool's API" description="Optional: the audience the exchanged token is for.">
              <Input value={String(oauth.audience ?? "")} placeholder="api://orders" ariaLabel="Tool's API"
                onChange={({ detail }) => setOauth({ ...oauth, audience: detail.value.trim() || undefined } as Record<string, Json>)} />
            </FormField>
          ) : null}
          {server ? (
            <SecretField buildId={project.id} kind="toolApiKeys" name={id} label="Client secret"
              description="Kept by AgentCore Identity; the agent never sees it." />
          ) : null}
          {method === "user" ? (
            <Box variant="small" color="text-body-secondary">
              {callbackUrl
                ? <>Register this as the redirect (callback) URL at the tool&apos;s provider: <Box variant="code" fontSize="body-s">{callbackUrl}</Box></>
                : "After the first deploy, the redirect (callback) URL to register at the tool's provider appears here."}
              {type === "mcp" ? " List the tools it publishes under Tool schema above." : ""}
            </Box>
          ) : null}
          {method === "obo" ? (
            <Box variant="small" color="text-body-secondary">The tool&apos;s provider must accept this app&apos;s sign-in token for an exchange.</Box>
          ) : null}
        </SpaceBetween>
      ) : null}

      {method === "aws" ? <RoleNote tool={tool} agentName={agentName} lead="Nothing to enter." /> : null}
    </SpaceBetween>
  );
}

/** "IAM role": which role, and what it may reach, with the name copyable. */
function RoleNote({ tool, agentName, lead }: { tool: ToolEntry; agentName?: string; lead: string }) {
  const role = gatewayRole(agentName);
  const note = iamNote(tool, agentName);
  const [before, after] = role ? note.split(role) : [note, ""];
  return (
    <SpaceBetween size="xxs">
      <Box>{lead}</Box>
      <Box variant="small" color="text-body-secondary">
        {before}{role ? <Box variant="code" fontSize="body-s">{role}</Box> : null}{after}
      </Box>
    </SpaceBetween>
  );
}
