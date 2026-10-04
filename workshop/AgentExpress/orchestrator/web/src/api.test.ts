/** The 401 recovery path, asserted rather than assumed.
 *
 *  This is the defect the reviewer saw four separate ways — "Could not list runs:
 *  Unauthorized", Stop run doing nothing, Submit decisions doing nothing, and a bare 401
 *  on /api/sessions in the network tab. All four were one cause: a Cognito ID token
 *  lasts an hour and nothing renewed it. So the behaviour under test is not "a 401 is
 *  reported nicely", it is "a 401 is RECOVERED FROM", and the three things that can go
 *  wrong with a recovery are covered here: it must replay the original request, it must
 *  not stampede when several polls expire at the same instant, and it must give up
 *  exactly once when the session is genuinely over. */

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { ApiError, api, setRefresher, setToken } from "./api";

type Reply = { status: number; body?: unknown };

/** A fetch stub that returns a scripted sequence and records what it was sent. */
function scriptFetch(replies: Reply[]) {
  const calls: { path: string; auth: string | null; method: string }[] = [];
  const impl = vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
    const headers = new Headers(init?.headers);
    calls.push({
      path: String(input),
      auth: headers.get("Authorization"),
      method: init?.method ?? "GET",
    });
    const next = replies.shift() ?? { status: 500 };
    return Promise.resolve(new Response(
      next.body === undefined ? "{}" : JSON.stringify(next.body),
      { status: next.status, headers: { "Content-Type": "application/json" } },
    ));
  });
  vi.stubGlobal("fetch", impl);
  return calls;
}

beforeEach(() => {
  setToken("stale-token");
});

afterEach(() => {
  vi.unstubAllGlobals();
  // Leave no refresher behind for the next test.
  setRefresher(() => Promise.resolve(null), () => undefined);
});

describe("a 401 on an expired token", () => {
  it("refreshes once and replays the request with the new token", async () => {
    const calls = scriptFetch([
      { status: 401 },
      { status: 200, body: [{ session_id: "abc" }] },
    ]);
    const refresh = vi.fn(() => Promise.resolve("fresh-token"));
    const lost = vi.fn();
    setRefresher(refresh, lost);

    const runs = await api.get<{ session_id: string }[]>("/api/sessions");

    expect(runs).toEqual([{ session_id: "abc" }]);
    expect(refresh).toHaveBeenCalledTimes(1);
    // The caller must never see the failure, so `lost` must not fire.
    expect(lost).not.toHaveBeenCalled();
    // And the replay must carry the NEW token, not the stale one it just failed with.
    expect(calls.map((c) => c.auth)).toEqual([
      "Bearer stale-token",
      "Bearer fresh-token",
    ]);
  });

  it("replays a POST as a POST", async () => {
    // The page polls with GETs but the actions the reviewer saw fail — Stop run,
    // Submit decisions — are POSTs. A recovery that replayed them as GETs would look
    // like success and do nothing, which is exactly the symptom reported.
    const calls = scriptFetch([{ status: 401 }, { status: 200, body: {} }]);
    setRefresher(() => Promise.resolve("fresh-token"), () => undefined);

    await api.post("/api/sessions/x/cancel");

    expect(calls.map((c) => c.method)).toEqual(["POST", "POST"]);
  });

  it("refreshes once for several requests that expire together", async () => {
    // Three pollers hit 401 in the same tick. Without a single in-flight refresh they
    // would each start one and the last to finish would win, so two requests would be
    // replayed with a token that had already been replaced.
    scriptFetch([
      { status: 401 }, { status: 401 }, { status: 401 },
      { status: 200, body: { a: 1 } },
      { status: 200, body: { b: 2 } },
      { status: 200, body: { c: 3 } },
    ]);
    let refreshes = 0;
    setRefresher(() => { refreshes += 1; return Promise.resolve("fresh-token"); },
                 () => undefined);

    await Promise.all([
      api.get("/api/sessions"),
      api.get("/api/workflow"),
      api.get("/api/me"),
    ]);

    expect(refreshes).toBe(1);
  });

  it("reports the session as lost when the refresh cannot help", async () => {
    scriptFetch([{ status: 401 }, { status: 401 }]);
    const lost = vi.fn();
    setRefresher(() => Promise.resolve("still-no-good"), lost);

    await expect(api.get("/api/sessions")).rejects.toBeInstanceOf(ApiError);
    expect(lost).toHaveBeenCalledTimes(1);
  });

  it("does not retry when there is no refresher at all", async () => {
    // Auth disabled, or an IdP with no refresh flow: one attempt, one clear failure.
    const calls = scriptFetch([{ status: 401 }]);
    setRefresher(() => Promise.resolve(null), () => undefined);

    await expect(api.get("/api/sessions")).rejects.toMatchObject({ status: 401 });
    expect(calls).toHaveLength(1);
  });
});

describe("other failures", () => {
  it("surfaces the BFF's own error message, which names the group a 403 wanted", async () => {
    scriptFetch([{ status: 403, body: { error: "decision requires group approvers" } }]);

    await expect(api.post("/api/sessions/x/decision", {})).rejects.toMatchObject({
      status: 403,
      message: "decision requires group approvers",
    });
  });
});
