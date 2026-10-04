/** The one place a request leaves the browser.
 *
 *  Every call carries the IdP's ID token as a Bearer, because the API Gateway JWT
 *  authorizer on /api/* is what stops an unauthenticated request reaching the BFF.
 *
 *  AND IT RECOVERS FROM AN EXPIRED TOKEN, which is the whole reason this file is more
 *  than a fetch wrapper. A Cognito ID token lasts an hour. Without renewal, every call
 *  after that hour returns 401 and the app sits there showing "Unauthorized" —
 *  observed live: the runs list, Stop run, and submitting a review gate all failed at
 *  once, and the page gave no way out but a manual reload. So a 401 is not an error
 *  here, it is a signal: refresh once, replay the request, and only surface a failure
 *  if the refresh itself fails (at which point the session really is over and the
 *  caller is sent back to the IdP).
 */

let idToken: string | null = null;

/** Set by auth.ts. Returns a fresh token, or null when the session cannot be renewed. */
let refresher: (() => Promise<string | null>) | null = null;
/** Called when renewal fails, so the app can send the user back to the IdP. */
let onSessionLost: (() => void) | null = null;

export function setToken(token: string | null): void {
  idToken = token;
}

export function getToken(): string | null {
  return idToken;
}

export function setRefresher(
  refresh: () => Promise<string | null>, lost: () => void,
): void {
  refresher = refresh;
  onSessionLost = lost;
}

export class ApiError extends Error {
  constructor(readonly status: number, message: string) {
    super(message);
    this.name = "ApiError";
  }
}

/** One in-flight refresh at a time. Without this, a page that polls three endpoints
 *  would fire three refreshes the moment the token expires, and two of them would race
 *  to write a different token. */
let refreshing: Promise<string | null> | null = null;

async function renew(): Promise<string | null> {
  if (!refresher) return null;
  refreshing ??= refresher().finally(() => { refreshing = null; });
  return refreshing;
}

async function send(path: string, init: RequestInit): Promise<Response> {
  const headers = new Headers(init.headers);
  if (idToken) headers.set("Authorization", "Bearer " + idToken);
  if (init.body && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }
  return fetch(path, { ...init, headers });
}

async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
  let res = await send(path, init);

  if (res.status === 401) {
    const fresh = await renew();
    if (fresh) {
      setToken(fresh);
      res = await send(path, init);    // replay once with the new token
    }
    if (res.status === 401) {
      // The session is genuinely over. Tell the app so it can re-authenticate rather
      // than leaving a wall of red flashes behind.
      onSessionLost?.();
      throw new ApiError(401, "Your session expired. Signing you in again…");
    }
  }

  if (!res.ok) {
    // The BFF returns {"error": "..."} on a refusal, and that message names the group
    // a 403 wanted — surfacing it beats "Request failed".
    let detail = res.statusText;
    try {
      const body = (await res.json()) as { error?: string; message?: string };
      detail = body.error || body.message || detail;
    } catch {
      /* a non-JSON body is still a failure; keep the status text */
    }
    throw new ApiError(res.status, detail);
  }
  if (res.status === 204) return undefined as T;
  return (await res.json()) as T;
}

export const api = {
  get: <T>(path: string) => request<T>(path),
  post: <T>(path: string, body?: unknown) =>
    request<T>(path, { method: "POST", body: body === undefined ? undefined : JSON.stringify(body) }),
  put: <T>(path: string, body: unknown) =>
    request<T>(path, { method: "PUT", body: JSON.stringify(body) }),
  del: <T>(path: string) => request<T>(path, { method: "DELETE" }),
};
