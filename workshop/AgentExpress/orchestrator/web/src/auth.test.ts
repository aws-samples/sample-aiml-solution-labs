/** Okta and Entra ID sign-in: authorization code with PKCE, state and nonce, done by
 *  hand. What is pinned: the request login() makes, that a reply is trusted only when
 *  it answers that request, that a refused sign-in is shown rather than retried for
 *  ever, renewal with the refresh token, and sign-out. */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { initAuth, logout, pkceChallenge } from "./auth";

const ORIGIN = "https://d123.cloudfront.net";
const TENANT = "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47";
const CFG = { enabled: true, provider: "entra", clientId: "spa-app",
  authorizeUrl: `${TENANT}/oauth2/v2.0/authorize`, tokenUrl: `${TENANT}/oauth2/v2.0/token`,
  logoutUrl: `${TENANT}/oauth2/v2.0/logout` };

/** Node's own localStorage shadows jsdom's (see Builder.test.tsx). */
function memoryStorage(): Storage {
  const m = new Map<string, string>();
  return {
    get length() { return m.size; }, clear: () => m.clear(), getItem: (k) => m.get(k) ?? null,
    key: (i) => [...m.keys()][i] ?? null, removeItem: (k) => { m.delete(k); }, setItem: (k, v) => { m.set(k, String(v)); },
  };
}
const b64 = (o: object) => btoa(JSON.stringify(o)).replace(/=+$/, "");
const jwt = (claims: object) => `${b64({ alg: "RS256" })}.${b64(claims)}.sig`;
const later = () => Math.floor(Date.now() / 1000) + 3600;

let assign: ReturnType<typeof vi.fn>;
let fetchMock: ReturnType<typeof vi.fn>;
function at(search = "", hash = "") {
  assign = vi.fn();
  vi.stubGlobal("location", { origin: ORIGIN, pathname: "/", search, hash, assign } as unknown as Location);
}
const sent = (i = 0) => Object.fromEntries(new URLSearchParams(fetchMock.mock.calls[i][1].body as URLSearchParams));
const reply = (status: number, body: object) => Promise.resolve(new Response(JSON.stringify(body), { status }));
const pending = () => JSON.parse(sessionStorage.getItem("oidc_pending") ?? "null");

beforeEach(() => {
  vi.stubGlobal("localStorage", memoryStorage());
  vi.stubGlobal("sessionStorage", memoryStorage());
  vi.spyOn(history, "replaceState").mockImplementation(() => {});
  fetchMock = vi.fn();
  vi.stubGlobal("fetch", fetchMock);
  window.AUTH_CONFIG = { ...CFG } as never;
  document.body.replaceChildren();
});
afterEach(() => { vi.unstubAllGlobals(); vi.restoreAllMocks(); });

const settle = () => new Promise((r) => setTimeout(r, 0));
/** login() redirects once the PKCE challenge is hashed, which is asynchronous. */
const redirected = () => vi.waitFor(() => expect(assign).toHaveBeenCalled());

describe("Okta / Entra ID sign-in", () => {
  it("sends a first visit to the provider with PKCE, state and nonce, keeping where it was going", async () => {
    at("", "#build:abc");
    expect(await initAuth()).toBeNull();
    await redirected();
    const url = new URL(assign.mock.calls[0][0]);
    expect(`${url.origin}${url.pathname}`).toBe(CFG.authorizeUrl);
    const q = Object.fromEntries(url.searchParams);
    const p = pending();
    expect(q).toMatchObject({ client_id: "spa-app", response_type: "code", redirect_uri: ORIGIN,
      scope: "openid profile email offline_access", code_challenge_method: "S256", state: p.state, nonce: p.nonce });
    expect(q.code_challenge).toBe(await pkceChallenge(p.verifier));
    expect(p.state).not.toBe(p.nonce);
    expect(p.back).toBe("#build:abc");
  });

  it("trades the code with the verifier, checks the nonce, and returns where the user was going", async () => {
    sessionStorage.setItem("oidc_pending", JSON.stringify({ state: "s1", nonce: "n1", verifier: "v1", back: "#build:abc" }));
    const id = jwt({ nonce: "n1", email: "ana@example.com", exp: later() });
    fetchMock.mockReturnValue(reply(200, { id_token: id, refresh_token: "r1" }));
    at("?code=c1&state=s1");
    expect(await initAuth()).toEqual({ ready: true, user: "ana@example.com" });
    expect(fetchMock.mock.calls[0][0]).toBe(CFG.tokenUrl);
    expect(sent()).toEqual({ client_id: "spa-app", grant_type: "authorization_code", code: "c1",
      redirect_uri: ORIGIN, code_verifier: "v1" });
    expect(localStorage.getItem("oidc_id_token")).toBe(id);
    expect(localStorage.getItem("oidc_refresh_token")).toBe("r1");
    expect(history.replaceState).toHaveBeenCalledWith({}, expect.anything(), "/#build:abc");
    expect(pending()).toBeNull();   // one use
  });

  it("ignores a reply to someone else's request, and signs in afresh", async () => {
    sessionStorage.setItem("oidc_pending", JSON.stringify({ state: "mine", nonce: "n", verifier: "v", back: "" }));
    at("?code=stolen&state=theirs");
    expect(await initAuth()).toBeNull();
    await redirected();
    expect(fetchMock).not.toHaveBeenCalled();
    expect(assign).toHaveBeenCalledWith(expect.stringContaining(CFG.authorizeUrl));
  });

  it("refuses an ID token minted for another sign-in, and says so instead of looping", async () => {
    sessionStorage.setItem("oidc_pending", JSON.stringify({ state: "s1", nonce: "n1", verifier: "v1", back: "" }));
    fetchMock.mockReturnValue(reply(200, { id_token: jwt({ nonce: "other", exp: later() }) }));
    at("?code=c1&state=s1");
    expect(await initAuth()).toBeNull();
    await settle();
    expect(assign).not.toHaveBeenCalled();
    expect(localStorage.getItem("oidc_id_token")).toBeNull();
    expect(document.body.textContent).toContain("nonce mismatch");
  });

  it("shows a sign-in the provider refused, as text, with a way to try again", async () => {
    at("?error=access_denied&error_description=" + encodeURIComponent("AADSTS50105: <b>not assigned</b>"));
    expect(await initAuth()).toBeNull();
    await settle();
    expect(assign).not.toHaveBeenCalled();
    const alert = document.querySelector("[role=alert]")!;
    expect(alert.textContent).toContain("AADSTS50105: <b>not assigned</b>");
    expect(alert.querySelector("b")).toBeNull();
    expect(alert.querySelector("a")!.getAttribute("href")).toBe(`${ORIGIN}/`);
  });

  it("renews an expired token with the refresh token on load, keeping the rotated one", async () => {
    localStorage.setItem("oidc_id_token", jwt({ email: "ana@example.com", exp: 1 }));
    localStorage.setItem("oidc_refresh_token", "r1");
    const fresh = jwt({ email: "ana@example.com", exp: later() });
    fetchMock.mockReturnValue(reply(200, { id_token: fresh, refresh_token: "r2" }));
    at();
    expect(await initAuth()).toEqual({ ready: true, user: "ana@example.com" });
    expect(sent()).toMatchObject({ grant_type: "refresh_token", refresh_token: "r1", client_id: "spa-app" });
    expect(localStorage.getItem("oidc_refresh_token")).toBe("r2");
    expect(localStorage.getItem("oidc_id_token")).toBe(fresh);
  });

  it("signs in again when the refresh token is refused, and forgets it", async () => {
    localStorage.setItem("oidc_id_token", jwt({ exp: 1 }));
    localStorage.setItem("oidc_refresh_token", "revoked");
    fetchMock.mockReturnValue(reply(400, { error: "invalid_grant" }));
    at();
    expect(await initAuth()).toBeNull();
    await redirected();
    expect(localStorage.getItem("oidc_refresh_token")).toBeNull();
    expect(assign).toHaveBeenCalledWith(expect.stringContaining(CFG.authorizeUrl));
  });

  it("signs out at the provider, with the ID token as hint, and forgets the tokens", () => {
    const id = jwt({ exp: later() });
    localStorage.setItem("oidc_id_token", id);
    localStorage.setItem("oidc_refresh_token", "r1");
    at();
    logout();
    const url = new URL(assign.mock.calls[0][0]);
    expect(`${url.origin}${url.pathname}`).toBe(CFG.logoutUrl);
    expect(Object.fromEntries(url.searchParams)).toEqual({ post_logout_redirect_uri: ORIGIN, client_id: "spa-app", id_token_hint: id });
    expect(localStorage.getItem("oidc_id_token")).toBeNull();
    expect(localStorage.getItem("oidc_refresh_token")).toBeNull();
  });

  it("Cognito, too, renews an expired token on load instead of going round the Hosted UI", async () => {
    window.AUTH_CONFIG = { enabled: true, provider: "cognito", clientId: "c1", domainPrefix: "app", region: "us-east-1" } as never;
    localStorage.setItem("cognito_id_token", jwt({ email: "ana@example.com", exp: 1 }));
    localStorage.setItem("cognito_refresh_token", "r1");
    const fresh = jwt({ email: "ana@example.com", exp: later() });
    fetchMock.mockReturnValue(reply(200, { id_token: fresh }));
    at();
    expect(await initAuth()).toEqual({ ready: true, user: "ana@example.com" });
    expect(fetchMock.mock.calls[0][0]).toBe("https://app.auth.us-east-1.amazoncognito.com/oauth2/token");
    expect(sent()).toMatchObject({ grant_type: "refresh_token", refresh_token: "r1" });
    expect(localStorage.getItem("cognito_id_token")).toBe(fresh);
    expect(assign).not.toHaveBeenCalled();
  });

  it("serves Okta with the same flow, from its own endpoints", async () => {
    const okta = "https://acme.okta.com/oauth2/default";
    window.AUTH_CONFIG = { enabled: true, provider: "okta", clientId: "0oaSpa", authorizeUrl: `${okta}/v1/authorize`,
      tokenUrl: `${okta}/v1/token`, logoutUrl: `${okta}/v1/logout` } as never;
    at();
    expect(await initAuth()).toBeNull();
    await redirected();
    expect(assign.mock.calls[0][0]).toMatch(/^https:\/\/acme\.okta\.com\/oauth2\/default\/v1\/authorize\?client_id=0oaSpa&/);
  });
});
