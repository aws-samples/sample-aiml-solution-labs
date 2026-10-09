/** Login, one strategy per provider.
 *
 *  `auth-config.js` is rendered at DEPLOY time by the IaC from workflow.json + the
 *  `idp` variable, and sets window.AUTH_CONFIG. So the same built bundle serves
 *  Cognito, Auth0, Okta, Entra ID and no-login — which is the point: adding a provider
 *  is one entry here plus one branch in terraform/identity.tf, and no rebuild of the UI.
 *
 *  Ported from the pre-Cloudscape page unchanged in behaviour. The one thing worth
 *  keeping in mind: initAuth() REDIRECTS for the code flow, so anything after it in
 *  the boot sequence may not run. The caller must stop when it returns false. */

import { setRefresher, setToken } from "./api";

export interface AuthConfig {
  enabled: boolean;
  provider?: "cognito" | "auth0" | "okta" | "entra" | "none";
  /** Cognito: the Hosted UI domain is COMPOSED from these two, not supplied. */
  domainPrefix?: string;
  region?: string;
  clientId?: string;
  /** Auth0. The IaC also writes `domain` here for the Auth0 tenant. */
  domain?: string;
  auth0Domain?: string;
  auth0ClientId?: string;
  /** Okta and Entra: the provider's endpoints, written by the IaC (one OIDC strategy). */
  authorizeUrl?: string;
  tokenUrl?: string;
  logoutUrl?: string;
}

declare global {
  interface Window {
    AUTH_CONFIG?: AuthConfig;
    /** Set by the legacy observability island so it can reuse the same token. */
    __idToken?: string | null;
  }
}

export interface Claims {
  email?: string;
  name?: string;
  sub?: string;
  "cognito:username"?: string;
  "cognito:groups"?: string[] | string;
  [k: string]: unknown;
}

export function parseJwt(token: string): Claims {
  try {
    const part = token.split(".")[1].replace(/-/g, "+").replace(/_/g, "/");
    return JSON.parse(decodeURIComponent(escape(atob(part)))) as Claims;
  } catch {
    return {};
  }
}

function loadScript(src: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const s = document.createElement("script");
    s.src = src;
    s.onload = () => resolve();
    s.onerror = () => reject(new Error("failed to load " + src));
    document.head.appendChild(s);
  });
}

interface Strategy {
  /** Returns a token, or null when the caller should redirect to login. */
  init(cfg: AuthConfig): Promise<string | null>;
  login(cfg: AuthConfig): void;
  logout(cfg: AuthConfig): void;
  /** A fresh ID token without user interaction, or null when that is impossible. */
  refresh?(cfg: AuthConfig): Promise<string | null>;
}

const NONE: Strategy = {
  // idp = "none": there is no token and nothing to redirect to.
  init: async () => "",
  login: () => {},
  logout: () => {},
};

/** Cognito Hosted UI, AUTHORIZATION-CODE flow done by hand.
 *
 *  Two things here are load-bearing and were both got wrong on the first attempt:
 *
 *  1. THE DOMAIN IS COMPOSED, not supplied. `auth-config.js` gives `domainPrefix` and
 *     `region` for Cognito and leaves `domain` empty (that field is Auth0's), so
 *     reading `cfg.domain` produces `https:///oauth2/authorize`.
 *  2. THE FLOW IS `code`, NOT `token`. Both IaC paths configure the app client for the
 *     authorization-code grant only, so an implicit-flow request is rejected by
 *     Cognito. A public client can exchange a code without PKCE, which is why no SDK
 *     is needed.
 *
 *  Tokens live in localStorage and are re-validated for expiry on load, so a reload
 *  does not bounce through the IdP. */
const COGNITO: Strategy = {
  async init(cfg) {
    const base = cognitoBase(cfg);
    const params = new URLSearchParams(location.search);
    let token: string | null;
    if (params.has("code")) {
      token = await exchangeCode(base, cfg, params.get("code")!);
      history.replaceState({}, document.title, location.pathname);
    } else {
      token = localStorage.getItem("cognito_id_token");
    }
    // Drop an expired token rather than sending a dead one and getting a 401, and
    // renew it with the refresh token: without that, a page opened after an hour sent
    // the user round the Hosted UI to click "Sign in as ..." again.
    if (token && expired(token)) {
      localStorage.removeItem("cognito_id_token");
      localStorage.removeItem("cognito_access_token");
      token = await refreshCognito(cfg);
    }
    return token || null;
  },
  refresh: refreshCognito,
  login(cfg) {
    const params = new URLSearchParams({
      client_id: cfg.clientId ?? "",
      response_type: "code",
      scope: "openid email profile",
      // No trailing slash: this must match the callback URL the IaC registered on the
      // app client exactly, or Cognito refuses with redirect_mismatch.
      redirect_uri: location.origin,
    });
    location.assign(`${cognitoBase(cfg)}/login?${params.toString()}`);
  },
  logout(cfg) {
    localStorage.removeItem("cognito_id_token");
    localStorage.removeItem("cognito_access_token");
    localStorage.removeItem("cognito_refresh_token");
    const params = new URLSearchParams({
      client_id: cfg.clientId ?? "",
      logout_uri: location.origin,
    });
    location.assign(`${cognitoBase(cfg)}/logout?${params.toString()}`);
  },
};

function cognitoBase(cfg: AuthConfig): string {
  return `https://${cfg.domainPrefix}.auth.${cfg.region}.amazoncognito.com`;
}

async function exchangeCode(
  base: string, cfg: AuthConfig, code: string,
): Promise<string | null> {
  const res = await fetch(`${base}/oauth2/token`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "authorization_code",
      client_id: cfg.clientId ?? "",
      code,
      redirect_uri: location.origin,
    }),
  });
  if (!res.ok) return null;   // a stale or replayed code: fall through to login()
  const tokens = (await res.json()) as {
    id_token?: string; access_token?: string; refresh_token?: string;
  };
  if (tokens.id_token) localStorage.setItem("cognito_id_token", tokens.id_token);
  if (tokens.access_token) localStorage.setItem("cognito_access_token", tokens.access_token);
  // THE REFRESH TOKEN IS THE POINT. Cognito returns one for the code grant, it lasts
  // 30 days by default, and without keeping it the app is dead an hour after login.
  if (tokens.refresh_token) localStorage.setItem("cognito_refresh_token", tokens.refresh_token);
  return tokens.id_token ?? null;
}

/** Trade the stored refresh token for a new ID token. Returns null when there is none
 *  or Cognito refuses it, which means the session is over. */
async function refreshCognito(cfg: AuthConfig): Promise<string | null> {
  const refresh = localStorage.getItem("cognito_refresh_token");
  if (!refresh) return null;
  const res = await fetch(`${cognitoBase(cfg)}/oauth2/token`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "refresh_token",
      client_id: cfg.clientId ?? "",
      refresh_token: refresh,
    }),
  });
  if (!res.ok) {
    // A revoked or expired refresh token: clear it so we do not retry forever.
    localStorage.removeItem("cognito_refresh_token");
    return null;
  }
  const tokens = (await res.json()) as { id_token?: string; access_token?: string };
  if (tokens.id_token) localStorage.setItem("cognito_id_token", tokens.id_token);
  if (tokens.access_token) localStorage.setItem("cognito_access_token", tokens.access_token);
  return tokens.id_token ?? null;
}

/** Auth0 via auth0-spa-js, loaded on demand so a Cognito deployment never fetches it. */
interface Auth0Client {
  handleRedirectCallback(): Promise<unknown>;
  getIdTokenClaims(): Promise<{ __raw?: string } | undefined>;
  getTokenSilently(o?: unknown): Promise<string>;
  loginWithRedirect(): Promise<void>;
  logout(o: unknown): void;
}
let auth0: Auth0Client | null = null;

const AUTH0: Strategy = {
  async init(cfg) {
    await loadScript("https://cdn.auth0.com/js/auth0-spa-js/2.1/auth0-spa-js.production.js");
    const factory = (window as unknown as {
      auth0: { createAuth0Client(o: unknown): Promise<Auth0Client> };
    }).auth0;
    auth0 = await factory.createAuth0Client({
      // The CDK template writes the tenant into `domain`; Terraform writes
      // `auth0Domain`. Accept either rather than depending on which plane deployed.
      domain: cfg.auth0Domain || cfg.domain,
      clientId: cfg.auth0ClientId || cfg.clientId,
      authorizationParams: { redirect_uri: location.origin + "/" },
      cacheLocation: "localstorage",
    });
    if (location.search.includes("code=")) {
      await auth0.handleRedirectCallback();
      history.replaceState({}, "", location.pathname);
    }
    const claims = await auth0.getIdTokenClaims();
    return claims?.__raw ?? null;
  },
  async refresh() {
    if (!auth0) return null;
    try {
      // `cacheMode: "off"` is load-bearing. getIdTokenClaims() alone reads the CACHE,
      // so at the moment this is called it hands back the very token that just returned
      // 401 — the replay fails, the session is declared lost, and the user is bounced to
      // the login page for no reason. getTokenSilently with the cache off performs the
      // actual renewal (refresh token, or a hidden iframe); only then are the claims new.
      await auth0.getTokenSilently({ cacheMode: "off" });
      const claims = await auth0.getIdTokenClaims();
      return claims?.__raw ?? null;
    } catch {
      // login_required / consent_required: the session really is over.
      return null;
    }
  },
  login() {
    void auth0?.loginWithRedirect();
  },
  logout() {
    auth0?.logout({ logoutParams: { returnTo: location.origin + "/" } });
  },
};

/** Sign-in refused by the provider, or a reply that cannot be trusted. Shown, never
 *  answered with another redirect: the provider would refuse again, and the page
 *  would bounce between the two for ever. */
export class SignInError extends Error {}

const OIDC_KEYS = { id: "oidc_id_token", refresh: "oidc_refresh_token", pending: "oidc_pending" };
/** offline_access: a refresh token, so the session outlives the ID token's hour.
 *  Okta issues one only when the app integration allows the Refresh Token grant. */
const OIDC_SCOPE = "openid profile email offline_access";

function base64url(bytes: Uint8Array): string {
  let s = "";
  for (const b of bytes) s += String.fromCharCode(b);
  return btoa(s).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}
function randomString(): string {
  return base64url(crypto.getRandomValues(new Uint8Array(32)));
}
export async function pkceChallenge(verifier: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(verifier));
  return base64url(new Uint8Array(digest));
}

interface Pending { state: string; nonce: string; verifier: string; back: string }

function storeTokens(t: { id_token?: string; refresh_token?: string }): void {
  if (t.id_token) localStorage.setItem(OIDC_KEYS.id, t.id_token);
  // Okta rotates it and Entra hands out a new one on every use: keep the newest.
  if (t.refresh_token) localStorage.setItem(OIDC_KEYS.refresh, t.refresh_token);
}

function clearOidc(): void {
  localStorage.removeItem(OIDC_KEYS.id);
  localStorage.removeItem(OIDC_KEYS.refresh);
}

async function postToken(cfg: AuthConfig, form: Record<string, string>): Promise<Response> {
  return fetch(cfg.tokenUrl ?? "", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({ client_id: cfg.clientId ?? "", ...form }),
  });
}

/** The provider's answer to login(): check it is the reply to OUR request (state),
 *  trade the code with the PKCE verifier, and check the ID token was minted for this
 *  sign-in (nonce). Returns null to sign in afresh; throws SignInError to stop. */
async function oidcCallback(cfg: AuthConfig, params: URLSearchParams): Promise<string | null> {
  const raw = sessionStorage.getItem(OIDC_KEYS.pending);
  sessionStorage.removeItem(OIDC_KEYS.pending);   // one use, whatever happens next
  const pending = raw ? (JSON.parse(raw) as Pending) : null;
  // Where the user was going (a #build:<id> link), back on the address bar.
  history.replaceState({}, document.title, location.pathname + (pending?.back ?? ""));
  if (params.has("error")) {
    throw new SignInError(params.get("error_description") || params.get("error") || "Sign-in was refused.");
  }
  // No request of ours (a reload of the callback, an old tab) or not this one: not
  // trusted, and not an error either. Sign in again.
  if (!pending || params.get("state") !== pending.state) return null;
  const res = await postToken(cfg, {
    grant_type: "authorization_code", code: params.get("code") ?? "",
    redirect_uri: location.origin, code_verifier: pending.verifier,
  });
  const tokens = (await res.json().catch(() => ({}))) as {
    id_token?: string; refresh_token?: string; error?: string; error_description?: string;
  };
  if (!res.ok || !tokens.id_token) {
    throw new SignInError(tokens.error_description || tokens.error || `The token endpoint answered ${res.status}.`);
  }
  if (parseJwt(tokens.id_token).nonce !== pending.nonce) {
    throw new SignInError("The sign-in reply was not for this sign-in (nonce mismatch).");
  }
  storeTokens(tokens);
  return tokens.id_token;
}

async function refreshOidc(cfg: AuthConfig): Promise<string | null> {
  const refresh = localStorage.getItem(OIDC_KEYS.refresh);
  if (!refresh) return null;
  const res = await postToken(cfg, { grant_type: "refresh_token", refresh_token: refresh, scope: OIDC_SCOPE })
    .catch(() => null);
  if (!res?.ok) {
    // Revoked, expired, or rotated away: drop it so it is not tried again.
    localStorage.removeItem(OIDC_KEYS.refresh);
    return null;
  }
  const tokens = (await res.json()) as { id_token?: string; refresh_token?: string };
  if (!tokens.id_token) return null;
  storeTokens(tokens);
  return tokens.id_token;
}

/** Okta and Microsoft Entra ID: standard OIDC, authorization code with PKCE, done by
 *  hand like Cognito's — the IaC writes each provider's endpoints into auth-config.js,
 *  so one strategy serves both, and no SDK is loaded. Both register the UI as a
 *  single-page app: a public client, so PKCE stands in for a secret, `state` ties the
 *  reply to this browser's request, and `nonce` the ID token to this sign-in. */
const OIDC: Strategy = {
  async init(cfg) {
    const params = new URLSearchParams(location.search);
    let token: string | null;
    if (params.has("code") || params.has("error")) {
      token = await oidcCallback(cfg, params);
    } else {
      token = localStorage.getItem(OIDC_KEYS.id);
    }
    // An hour-old token on load: renew it quietly rather than send the user round
    // the provider; only with no refresh token left is it a new sign-in.
    if (token && expired(token)) {
      localStorage.removeItem(OIDC_KEYS.id);
      token = await refreshOidc(cfg);
    }
    return token || null;
  },
  refresh: refreshOidc,
  login(cfg) {
    void (async () => {
      const pending: Pending = { state: randomString(), nonce: randomString(), verifier: randomString(), back: location.hash };
      sessionStorage.setItem(OIDC_KEYS.pending, JSON.stringify(pending));
      const params = new URLSearchParams({
        client_id: cfg.clientId ?? "",
        response_type: "code",
        scope: OIDC_SCOPE,
        // No trailing slash: it must match the redirect URI registered on the SPA client.
        redirect_uri: location.origin,
        state: pending.state,
        nonce: pending.nonce,
        code_challenge: await pkceChallenge(pending.verifier),
        code_challenge_method: "S256",
      });
      location.assign(`${cfg.authorizeUrl}?${params.toString()}`);
    })();
  },
  logout(cfg) {
    const hint = localStorage.getItem(OIDC_KEYS.id);
    clearOidc();
    // Okta needs the ID token to end its session; Entra accepts it. Both send the
    // browser back only to a sign-out URL registered on the client.
    const params = new URLSearchParams({ post_logout_redirect_uri: location.origin, client_id: cfg.clientId ?? "" });
    if (hint) params.set("id_token_hint", hint);
    location.assign(`${cfg.logoutUrl}?${params.toString()}`);
  },
};

/** A sign-in the provider refused, said on the page instead of redirecting again. */
function showSignInError(message: string): void {
  const box = document.createElement("main");
  box.setAttribute("role", "alert");
  box.style.cssText = "font-family: sans-serif; max-width: 40rem; margin: 4rem auto; line-height: 1.5";
  const h = document.createElement("h1");
  h.textContent = "Could not sign you in";
  const p = document.createElement("p");
  p.textContent = message;   // the provider's words: text, never markup
  const a = document.createElement("a");
  a.href = location.origin + "/";
  a.textContent = "Try again";
  box.append(h, p, a);
  document.body.replaceChildren(box);
}

const STRATEGIES: Record<string, Strategy> = { none: NONE, cognito: COGNITO, auth0: AUTH0, okta: OIDC, entra: OIDC };

function expired(token: string): boolean {
  const { exp } = parseJwt(token) as { exp?: number };
  return typeof exp === "number" && exp * 1000 < Date.now() + 30_000;
}

export function authConfig(): AuthConfig {
  return window.AUTH_CONFIG ?? { enabled: false, provider: "none" };
}

function strategy(): Strategy {
  const cfg = authConfig();
  if (!cfg.enabled) return NONE;
  return STRATEGIES[cfg.provider ?? "cognito"] ?? COGNITO;
}

export interface AuthState {
  ready: boolean;
  user: string;
}

/** Resolve a token, or redirect to the provider's login page.
 *
 *  Returns false when a redirect is under way, and the caller MUST stop booting.
 *  The pre-Cloudscape page did not, so `loadWorkflow`/`loadMe` fired without a token
 *  and every first visit logged two 401s on the way to the login screen. */
export async function initAuth(): Promise<AuthState | null> {
  const cfg = authConfig();
  if (!cfg.enabled) {
    setToken(null);
    return { ready: true, user: "" };
  }
  const s = strategy();
  let token: string | null = null;
  try {
    token = await s.init(cfg);
  } catch (e) {
    if (e instanceof SignInError) {
      showSignInError(e.message);
      return null;
    }
    console.error("auth init failed", e);
  }
  if (!token) {
    s.login(cfg);
    return null;
  }
  setToken(token);
  window.__idToken = token;
  // From here on, api.ts recovers from a 401 by asking for a new token rather than
  // surfacing "Unauthorized" and leaving the page stuck.
  setRefresher(
    async () => {
      const fresh = s.refresh ? await s.refresh(cfg) : null;
      if (fresh) window.__idToken = fresh;
      return fresh;
    },
    () => { s.login(cfg); },
  );
  const c = parseJwt(token);
  return {
    ready: true,
    // Entra ID sends no email unless configured: its sign-in name is preferred_username.
    user: c.email || (c.preferred_username as string | undefined) || c.name || c["cognito:username"] || c.sub || "",
  };
}

export function logout(): void {
  strategy().logout(authConfig());
}

export function authEnabled(): boolean {
  return authConfig().enabled;
}
