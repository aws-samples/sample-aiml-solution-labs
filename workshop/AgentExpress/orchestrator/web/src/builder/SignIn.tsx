/** Identity > Sign-in: where people sign in to the app this build deploys
 *  (authorization.signIn). Built in (a Cognito user list the deployment makes), or the
 *  customer's own Okta, Microsoft Entra ID or Auth0. Kept to what each one needs: two
 *  values copied from the provider, and one address to paste back into it. */
import Box from "@cloudscape-design/components/box";
import ColumnLayout from "@cloudscape-design/components/column-layout";
import Container from "@cloudscape-design/components/container";
import CopyToClipboard from "@cloudscape-design/components/copy-to-clipboard";
import FormField from "@cloudscape-design/components/form-field";
import Header from "@cloudscape-design/components/header";
import Input from "@cloudscape-design/components/input";
import KeyValuePairs from "@cloudscape-design/components/key-value-pairs";
import Link from "@cloudscape-design/components/link";
import SpaceBetween from "@cloudscape-design/components/space-between";
import Tiles from "@cloudscape-design/components/tiles";

import type { Json, Project } from "./model";
import { GROUPS_CLAIM_FOR } from "./validate";

export type Provider = "cognito" | "okta" | "entra" | "auth0";
type Field = "domain" | "clientId" | "tenantId";
type SignIn = { provider?: Provider } & Partial<Record<Field | "authorizationServer", string>>;

export const PROVIDER_NAME: Record<Provider, string> = {
  cognito: "Built in", okta: "Okta", entra: "Microsoft Entra ID", auth0: "Auth0",
};
const ABOUT: Record<Provider, string> = {
  cognito: "We create the user list for you",
  okta: "Your Okta org",
  entra: "Your Microsoft work accounts",
  auth0: "Your Auth0 tenant",
};

const GUID = /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;
const HOST = /^[A-Za-z0-9.-]+\.[A-Za-z]{2,}$/;
const CLIENT = /^[A-Za-z0-9._~-]{1,128}$/;

/** Each provider's two values, named as its own console names them. */
const FIELDS: Record<Exclude<Provider, "cognito">, { key: Field; label: string; placeholder: string; ok: RegExp; hint: string }[]> = {
  okta: [
    { key: "domain", label: "Okta domain", placeholder: "dev-123456.okta.com", ok: HOST, hint: "Like dev-123456.okta.com" },
    { key: "clientId", label: "Client ID", placeholder: "0oa1b2c3d4e5f6g7h8i9", ok: CLIENT, hint: "Letters and digits, no spaces" },
  ],
  entra: [
    { key: "tenantId", label: "Directory (tenant) ID", placeholder: "00000000-0000-0000-0000-000000000000", ok: GUID, hint: "A GUID, like 1a2b3c4d-0000-0000-0000-000000000000" },
    { key: "clientId", label: "Application (client) ID", placeholder: "00000000-0000-0000-0000-000000000000", ok: CLIENT, hint: "Letters and digits, no spaces" },
  ],
  auth0: [
    { key: "domain", label: "Domain", placeholder: "your-tenant.us.auth0.com", ok: HOST, hint: "Like your-tenant.us.auth0.com" },
    { key: "clientId", label: "Client ID", placeholder: "aBcD1234eFgH5678", ok: CLIENT, hint: "Letters and digits, no spaces" },
  ],
};

/** Step 1: what to create there, and where its own guide is. */
const CREATE: Record<Exclude<Provider, "cognito">, { text: string; href: string }> = {
  okta: { text: "In Okta, create an app integration: OIDC, Single-Page Application, with the Refresh Token grant. Assign the people who may use it.",
    href: "https://developer.okta.com/docs/guides/sign-into-spa-redirect/react/main/#create-an-okta-integration-for-your-app" },
  entra: { text: "In Microsoft Entra, register an app for this organization only, and add the Single-page application platform.",
    href: "https://learn.microsoft.com/en-us/entra/identity-platform/scenario-spa-app-registration" },
  auth0: { text: "In Auth0, create an application of type Single Page Web Applications.",
    href: "https://auth0.com/docs/get-started/auth0-overview/create-applications/single-page-web-apps" },
};
/** Step 3: where the app's address goes there. */
const PASTE: Record<Exclude<Provider, "cognito">, string> = {
  okta: "Sign-in redirect URI, Sign-out redirect URI and Trusted Origins",
  entra: "Redirect URI (under Single-page application)",
  auth0: "Allowed Callback URLs, Allowed Logout URLs and Allowed Web Origins",
};

export function signInOf(project: Project): SignIn {
  const auth = (project.workflow.authorization ?? {}) as Record<string, unknown>;
  const s = auth.signIn;
  return s && typeof s === "object" && !Array.isArray(s) ? s as SignIn : {};
}
export function providerOf(project: Project): Provider {
  return (signInOf(project).provider as Provider) || "cognito";
}

/** What people paste: a whole URL or an issuer. Keep the host only. */
export function hostOnly(v: string): string {
  return v.trim().replace(/^[a-z]+:\/\//i, "").split(/[/?#]/)[0];
}

function withSignIn(project: Project, next: SignIn | undefined, groupsClaim?: string): Project {
  const wf = project.workflow;
  const auth = { ...((wf.authorization ?? {}) as Record<string, Json>) };
  if (next) auth.signIn = next as unknown as Json; else delete auth.signIn;
  if (groupsClaim) auth.groupsClaim = groupsClaim;
  return { ...project, workflow: { ...wf, authorization: auth } as Project["workflow"] };
}

/** Change provider: keep only the values it takes, and point roles at its claim. */
export function chooseProvider(project: Project, provider: Provider): Project {
  if (provider === providerOf(project)) return project;
  const claim = provider === "cognito" ? "cognito:groups" : GROUPS_CLAIM_FOR[provider];
  if (provider === "cognito") return withSignIn(project, undefined, claim);
  const cur = signInOf(project);
  const keep = Object.fromEntries(FIELDS[provider].map((f) => [f.key, cur[f.key]]).filter(([, v]) => v));
  return withSignIn(project, { provider, ...keep }, claim);
}

/** The groups the deploy creates in the app's Cognito user pool: every group named in
 *  authorization.actions (cdk authzGroups, terraform local.authz_groups). */
export function poolGroups(project: Project): string[] {
  const auth = (project.workflow.authorization ?? {}) as Record<string, unknown>;
  const actions = auth.actions && typeof auth.actions === "object" ? auth.actions as Record<string, unknown> : {};
  return [...new Set(Object.values(actions).flatMap((g) => (Array.isArray(g) ? g.map(String) : [])))].sort();
}

/** Built in: what Amazon Cognito is here, what the deploy creates, and who gets in. */
function CognitoDetails({ project, userPoolId, region }: { project: Project; userPoolId?: string; region?: string }) {
  const groups = poolGroups(project);
  const consoleUrl = userPoolId && region
    ? `https://${region}.console.aws.amazon.com/cognito/v2/idp/user-pools/${encodeURIComponent(userPoolId)}/users?region=${region}`
    : undefined;
  return (
    <Container header={<Header variant="h3" description="Nothing to set up: the deploy creates it, and there are no values to copy.">
      Built-in sign-in: Amazon Cognito</Header>}>
      <KeyValuePairs columns={2} items={[
        { label: "What it is",
          value: "Amazon Cognito, the AWS sign-in service. The deploy creates a user pool for this app in the AWS account and region it deploys to, so no outside provider is involved." },
        { label: "How people sign in",
          value: "With their email address and a password of at least 12 characters, with upper and lower case letters, a digit and a symbol. An authenticator app can be added as a second factor." },
        { label: "Who gets in",
          value: "You, when it deploys: the Deployment panel shows your temporary password, and Cognito emails it too. People you share the build with get their own login there. Nobody can sign themselves up, and the first sign-in sets a new password." },
        { label: "Roles",
          value: groups.length
            ? `The pool has one group per role in Settings → authorization: ${groups.join(", ")}. Everyone invited joins all of them; remove someone from a group in the Cognito console to take that role away.`
            : "No roles: everyone who signs in may do everything the app allows." },
        { label: "User pool",
          value: userPoolId ? (
            <SpaceBetween size="xxs">
              <CopyToClipboard variant="inline" textToCopy={userPoolId} copyButtonAriaLabel="Copy the user pool ID"
                copySuccessText="User pool ID copied" copyErrorText="Could not copy" />
              {consoleUrl ? <Link href={consoleUrl} external fontSize="body-s">Open it in the Amazon Cognito console</Link> : null}
            </SpaceBetween>
          ) : "Created on the first deploy." },
        { label: "When you destroy the build",
          value: "The user pool goes with it, and every login in it." },
      ]} />
    </Container>
  );
}

export function SignInSettings({ project, setProject, appUrl, userPoolId, region }: {
  project: Project; setProject: (p: Project) => void;
  /** The deployed app's address, once there is one. */
  appUrl?: string;
  /** The deployed app's Cognito user pool, and the region it is in (Built in only). */
  userPoolId?: string;
  region?: string;
}) {
  const provider = providerOf(project);
  const s = signInOf(project);
  const auth = (project.workflow.authorization ?? {}) as Record<string, unknown>;
  const usesRoles = Boolean(auth.actions && typeof auth.actions === "object" && Object.keys(auth.actions).length);
  const set = (key: Field, value: string) => {
    const { [key]: _old, ...rest } = s;
    setProject(withSignIn(project, value ? { ...rest, provider, [key]: value } : { ...rest, provider }));
  };

  return (
    <SpaceBetween size="l">
      <Tiles value={provider} columns={4} ariaLabel="Where people sign in"
        onChange={({ detail }) => setProject(chooseProvider(project, detail.value as Provider))}
        items={(Object.keys(PROVIDER_NAME) as Provider[]).map((p) => ({ value: p, label: PROVIDER_NAME[p], description: ABOUT[p] }))} />

      {provider === "cognito" ? (
        <CognitoDetails project={project} userPoolId={userPoolId} region={region} />
      ) : (
        <Container header={<Header variant="h3">Connect {PROVIDER_NAME[provider]}</Header>}>
          <ol className="axb-steps">
            <li>
              <SpaceBetween size="xxs">
                <span>{CREATE[provider].text}</span>
                <Link href={CREATE[provider].href} external variant="primary" fontSize="body-s">How to do this in {PROVIDER_NAME[provider]}</Link>
              </SpaceBetween>
            </li>
            <li>
              <SpaceBetween size="s">
                <span>Copy these from it:</span>
                <ColumnLayout columns={2}>
                  {FIELDS[provider].map((f) => {
                    const v = s[f.key] ?? "";
                    return (
                      <FormField key={f.key} label={f.label} errorText={v && !f.ok.test(v) ? f.hint : undefined}>
                        <Input value={v} placeholder={f.placeholder} ariaLabel={f.label}
                          onChange={({ detail }) => set(f.key, f.key === "domain" ? hostOnly(detail.value) : detail.value.trim())} />
                      </FormField>
                    );
                  })}
                </ColumnLayout>
              </SpaceBetween>
            </li>
            <li>
              {appUrl ? (
                <SpaceBetween size="xxs">
                  <span>Add this address in {PROVIDER_NAME[provider]} as the {PASTE[provider]}:</span>
                  <CopyToClipboard variant="inline" textToCopy={appUrl} copyButtonAriaLabel="Copy the app's address"
                    copySuccessText="Address copied" copyErrorText="Could not copy" />
                </SpaceBetween>
              ) : (
                <span>Deploy. The app&apos;s address appears here, to add in {PROVIDER_NAME[provider]} as the {PASTE[provider]}.</span>
              )}
            </li>
          </ol>
          {usesRoles && provider === "auth0" ? (
            <Box padding={{ top: "s" }}>
              <FormField label="Roles claim" description="The claim your post-login Action adds with each person's roles."
                errorText={!auth.groupsClaim || auth.groupsClaim === "cognito:groups" ? "Needed, or every restricted action is denied" : undefined}>
                <Input value={auth.groupsClaim === "cognito:groups" ? "" : String(auth.groupsClaim ?? "")}
                  placeholder="https://your-app/roles" ariaLabel="Roles claim"
                  onChange={({ detail }) => setProject(withSignIn(project, s, detail.value.trim() || "cognito:groups"))} />
              </FormField>
            </Box>
          ) : null}
          {usesRoles && provider !== "auth0" ? (
            <Box padding={{ top: "s" }} variant="small" color="text-body-secondary">
              {provider === "okta"
                ? "Roles come from Okta groups with the same names: add a groups claim to the ID token."
                : "Roles come from the app's roles in Entra with the same names: assign them to people."}
            </Box>
          ) : null}
        </Container>
      )}
    </SpaceBetween>
  );
}
