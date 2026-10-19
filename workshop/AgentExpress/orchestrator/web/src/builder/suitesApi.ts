/** The suites routes (bff/suiteruns.py). */
import { api } from "../api";
import type { Suite, SuiteRunResult, SuiteRunStarted, TrendEntry } from "./suiteSpec";

const enc = encodeURIComponent;

export const suitesApi = {
  /** The trend of each suite (the suites themselves are in the project, which autosaves). */
  trends: (build: string) =>
    api.get<{ suites: Suite[]; trends: Record<string, TrendEntry[]> }>(`/api/builds/${enc(build)}/suites`)
      .then((r) => r.trends ?? {}),
  /** Run one suite: a run per case, in the deployed build. */
  start: (build: string, suite: string) =>
    api.post<SuiteRunStarted>(`/api/builds/${enc(build)}/suite-runs`, { suite }),
  /** Where a suite run stands, case by case. */
  result: (build: string, run: string) =>
    api.get<SuiteRunResult>(`/api/builds/${enc(build)}/suite-runs/${enc(run)}`),
};
