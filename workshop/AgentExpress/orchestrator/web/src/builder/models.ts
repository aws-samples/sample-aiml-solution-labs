/** The Bedrock text models this deployment's account can invoke, for the model picker.
 *
 *  Read from `GET /api/models`, which lists them from the account itself (see
 *  bff/handler.py `_models`) — any list written into the UI would be stale within weeks
 *  and would offer models the account cannot use. Fetched once per page load. When it
 *  cannot be read (the Builder running with no API, or the call refused), the picker
 *  still takes a typed model id, so a missing list never blocks anyone. */

import { useEffect, useState } from "react";

import { api, ApiError } from "../api";
import type { Issue } from "./validate";

export interface ModelOption {
  id: string;
  name: string;
  provider: string;
  /** Accepts images (Bedrock's inputModalities). Absent: not known, taken as yes. */
  vision?: boolean;
  /** Can choose its own tool calls (toolMode "model"). Absent: taken as yes. */
  tools?: boolean;
  /** Something the account must do first (Fable's data-retention opt-in). */
  note?: string;
  /** Not Bedrock: served by this deployment's LiteLLM proxy (`GET /api/models/litellm`), as
   *  `litellm/<name>`. Its capabilities are the proxy's to know, so none are claimed. */
  via?: "litellm";
}

/** What an agent's settings ask of its model: reading images (`vision.from`) and
 *  choosing its own tool calls (toolMode "model"). Nothing else is read — not the prompt. */
export function needsOf(agent: Record<string, unknown> | undefined): { vision: boolean; tools: boolean } {
  const v = agent?.vision as { from?: unknown[] } | undefined;
  return { vision: Boolean(v && Array.isArray(v.from) && v.from.length), tools: agent?.toolMode === "model" };
}

/** Whether a model can do what an agent needs. */
export function fits(m: ModelOption, needs: { vision: boolean; tools: boolean }): boolean {
  return (!needs.vision || m.vision !== false) && (!needs.tools || m.tools !== false);
}

/** "reads images · calls tools · needs data-retention opt-in" for the picker. */
export function capsLabel(m: ModelOption): string {
  if (m.via === "litellm") return "via LiteLLM";
  return [m.vision ? "reads images" : "text only", m.tools === false ? "no tool calls" : "calls tools",
    ...(m.note ? ["needs data-retention opt-in"] : [])].join(" · ");
}

/** Agents whose model cannot do what their settings ask, as Problems. Mirrors
 *  bff/handler.py model_problems, which the Assistant sees. Only models the account
 *  lists are judged: a typed id nothing lists (a custom import) is taken on trust. */
export function modelIssues(wf: { agents: Record<string, Record<string, unknown>>; orchestrator?: unknown },
  models: ModelOption[], litellm?: boolean): Issue[] {
  const listed = new Map(models.map((m) => [m.id, m]));
  const def = String((wf.orchestrator as Record<string, unknown> | undefined)?.defaultModel ?? "");
  const out: Issue[] = [];
  for (const [id, a] of Object.entries(wf.agents ?? {})) {
    if (!a || a.runtime === "a2a" || a.runtime === "task") continue;
    const mid = String(a.model ?? def);
    const where = { kind: "agent", id } as Issue["where"];
    const path = `agents.${id}.model`;
    if (mid.startsWith(LITELLM_PREFIX)) {
      // Mirrors bff/litellm_models.py problems. `litellm` undefined: not known, so not judged.
      const served = models.filter((x) => x.via === "litellm");
      if (litellm === false) {
        out.push({ severity: "warning", where, path, message: `${mid} is a LiteLLM model, and LiteLLM is not set up on this deployment: deploy with a LiteLLM proxy (DEPLOYMENT.md), or pick a Bedrock model` });
      } else if (litellm && served.length && !served.some((x) => x.id === mid)) {
        out.push({ severity: "warning", where, path, message: `the LiteLLM proxy does not list ${mid.slice(LITELLM_PREFIX.length)}: pick one it serves` });
      }
      continue;
    }
    const m = listed.get(mid);
    if (!m) continue;
    const needs = needsOf(a);
    if (needs.vision && m.vision === false) {
      out.push({ severity: "error", where, path, message: `${mid} cannot read images, and this agent reads them (vision): pick a model that accepts images` });
    }
    if (needs.tools && m.tools === false) {
      out.push({ severity: "error", where, path, message: `${mid} cannot call tools itself (toolMode "model"): pick another model, or let the framework call the tools (toolMode "direct")` });
    }
    if (m.note) out.push({ severity: "warning", where, path, message: `${mid} ${m.note}` });
  }
  return out;
}

export interface ModelList {
  models: ModelOption[];
  error?: string;
  loading: boolean;
  /** Whether this deployment has a LiteLLM proxy; undefined when that could not be read. */
  litellm?: boolean;
}

/** An agent's model id for a model the LiteLLM proxy serves. */
export const LITELLM_PREFIX = "litellm/";

type Loaded = { models: ModelOption[]; error?: string; litellm?: boolean };
let pending: Promise<Loaded> | null = null;

/** The LiteLLM proxy's models: a 404 means it is not set up here; any other failure, unknown. */
function loadLitellm(): Promise<{ models: ModelOption[]; configured?: boolean; error?: string }> {
  return api.get<{ models?: ModelOption[]; configured?: boolean; error?: string }>("/api/models/litellm")
    .then((r) => ({ models: (r.models ?? []).map((m) => ({ ...m, via: "litellm" as const })), configured: r.configured !== false, error: r.error }))
    .catch((e: unknown) => ({ models: [], configured: e instanceof ApiError && e.status === 404 ? false : undefined }));
}

function load() {
  pending ??= Promise.all([
    api.get<{ models?: ModelOption[]; error?: string }>("/api/models")
      .then((r) => ({ models: r.models ?? [], error: r.error }))
      .catch((e: Error) => ({ models: [] as ModelOption[], error: `Could not list models (${e.message}); type a model id.` })),
    loadLitellm(),
  ]).then(([bedrock, proxy]) => ({
    models: [...bedrock.models, ...proxy.models],
    error: [bedrock.error, proxy.error].filter(Boolean).join(" ") || undefined,
    litellm: proxy.configured,
  }));
  return pending;
}

export function useModels(): ModelList {
  const [state, setState] = useState<ModelList>({ models: [], loading: true });
  useEffect(() => {
    let live = true;
    void load().then((r) => { if (live) setState({ ...r, loading: false }); });
    return () => { live = false; };
  }, []);
  return state;
}

/** Test seam: forget the cached list. */
export function resetModelsForTests(): void {
  pending = null;
}
