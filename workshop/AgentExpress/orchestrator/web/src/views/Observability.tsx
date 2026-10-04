/** The Observability tab.
 *
 *  MOUNTED AS AN ISLAND, deliberately. `legacy/observability.js` is ~1,500 lines of
 *  self-contained rendering — charts, the Prompts & I/O inspector, evaluation scores,
 *  Insights, CSV/JSON export — and porting it to components is a separate piece of work
 *  from restructuring the app. Rewriting it in the same change would have meant holding
 *  two large rewrites in flight at once, so it keeps working exactly as it did while
 *  the shell around it becomes Cloudscape.
 *
 *  The contract between the two is narrow and stated here: this component gives the
 *  module a container to render into, the auth token to call the API with, and the
 *  projected workflow definition, and the module renders into it. Nothing else is
 *  shared.
 *
 *  THE WORKFLOW IS PART OF THAT CONTRACT BECAUSE LEAVING IT OUT BROKE SOMETHING
 *  SILENTLY. The module needs `evalAgents` to decide whether an agent gets an
 *  Evaluation block and an Evaluate button, and it needs `agents[id].name` to label a
 *  row. The pre-React index.html published both as a global named `workflow`, which
 *  this shell does not, so the module read `undefined` and quietly concluded NO agent
 *  had evaluations enabled — the scores were recorded and billed, and then not
 *  rendered. Passing it explicitly means a missing dependency is a type error rather
 *  than an empty panel. */

import Box from "@cloudscape-design/components/box";
import Container from "@cloudscape-design/components/container";
import Header from "@cloudscape-design/components/header";
import Spinner from "@cloudscape-design/components/spinner";
import { useEffect, useRef, useState } from "react";

import { getToken } from "../api";
import type { Workflow } from "../types";

declare global {
  interface Window {
    /** Set by legacy/observability.js when it loads. */
    ObservabilityIsland?: {
      mount(el: HTMLElement, token: string | null, workflow: Workflow,
        opts?: { build?: string }): void;
    };
    /** Set by legacy/observability.js: open one run's detail (the Run detail tab). */
    obsOpenSession?: (sessionId: string) => void;
  }
}

let loading: Promise<void> | null = null;

/** Load the module once per session, not once per mount. */
function loadIsland(): Promise<void> {
  if (window.ObservabilityIsland) return Promise.resolve();
  loading ??= new Promise<void>((resolve, reject) => {
    const s = document.createElement("script");
    s.src = "/observability.js";
    s.onload = () => resolve();
    s.onerror = () => reject(new Error("could not load observability.js"));
    document.head.appendChild(s);
  });
  return loading;
}

/** `session`, when given, opens that run's detail — the side navigation lists every run
 *  under Observability and lands here with one picked. `build`, when given, scopes the
 *  run list and the cost rollups to that deployed Builder build. */
export function Observability({ workflow, session, build = "" }: {
  workflow: Workflow; session?: string | null; build?: string;
}) {
  const host = useRef<HTMLDivElement>(null);
  const [error, setError] = useState<string | null>(null);
  const [ready, setReady] = useState(false);

  useEffect(() => {
    let cancelled = false;
    void loadIsland()
      .then(() => {
        if (cancelled || !host.current) return;
        const island = window.ObservabilityIsland;
        if (!island) throw new Error("observability.js loaded but registered nothing");
        island.mount(host.current, getToken(), workflow, { build });
        setReady(true);
      })
      .catch((e: Error) => { if (!cancelled) setError(e.message); });
    return () => { cancelled = true; };
  }, [workflow, build]);

  useEffect(() => {
    if (ready && session) window.obsOpenSession?.(session);
  }, [ready, session]);

  if (error) {
    return (
      <Container header={<Header variant="h1">Observability</Header>}>
        <Box color="text-status-error">{error}</Box>
      </Container>
    );
  }

  return (
    <>
      {ready ? null : <Box padding="l"><Spinner /> Loading observability…</Box>}
      <div ref={host} />
    </>
  );
}
