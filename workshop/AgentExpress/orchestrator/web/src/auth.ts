/** Login, one strategy per provider.
 *
 *  `auth-config.js` is rendered at DEPLOY time by the IaC from workflow.json + the
 *  `idp` variable, and sets window.AUTH_CONFIG. So the same built bundle serves
 *  Cognito, Auth0 and no-login — which is the point: adding a provider is one entry
 *  here plus one branch in terraform/identity.tf, and no rebuild of the UI.
 *
 *  Ported from the pre-Cloudscape page unchanged in behaviour. The one thing worth
 *  keeping in mind: initAuth() REDIRECTS for the code flow, so anything after it in
 *  the boot sequence may not run. The caller must stop when it returns false. */

import { setRefresher, setToken } from "./api";

export interface AuthConfig {
  enabled: boolean;
  provider?: "cognito" | "auth0" | "none";
  /** Cognito: the Hosted UI domain is COMPOSED from these two, not supplied. */
  domainPrefix?: string;
  region?: string;
  clientId?: string;
  /** Auth0. The IaC also writes `domain` here for the Auth0 tenant. */
  domain?: string;
  auth0Domain?: string;
  auth0ClientId?: string;
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
    // Drop an expired token rather than sending a dead one and getting a 401.
    if (token && expired(token)) {
      localStorage.removeItem("cognito_id_token");
      localStorage.removeItem("cognito_access_token");
      token = null;
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

const STRATEGIES: Record<string, Strategy> = { none: NONE, cognito: COGNITO, auth0: AUTH0 };

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
    user: c.email || c.name || c["cognito:username"] || c.sub || "",
  };
}

export function logout(): void {
  strategy().logout(authConfig());
}

export function authEnabled(): boolean {
  return authConfig().enabled;
}
