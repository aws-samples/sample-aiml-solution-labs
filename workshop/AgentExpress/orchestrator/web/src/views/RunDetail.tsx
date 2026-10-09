/** A run, as a console resource-detail page: breadcrumbs, a page header with the
 *  resource's actions, a KeyValuePairs summary, then Tabs.
 *
 *  The tab set mirrors a Step Functions execution: the graph, the same information as
 *  a table, the event timeline, and the final output. There is no "Re-run" tab — a
 *  re-run is an action on a STEP, so it lives in that step's panel, and the
 *  whole-workflow restart lives on the graph's own toolbar.
 *
 *  Every table here is sorted and filtered through `useCollection`, Cloudscape's own
 *  collection hook, rather than through hand-rolled state. Sorting a column is not a
 *  nice-to-have on these two: Steps is the only place to see which step is slowest or
 *  which produced nothing, and Events is append-only, so without sort and filter a long
 *  run buries the one guardrail line a reader came for. */

import { useCollection } from "@cloudscape-design/collection-hooks";
import Alert from "@cloudscape-design/components/alert";
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import Container from "@cloudscape-design/components/container";
import ExpandableSection from "@cloudscape-design/components/expandable-section";
import Header from "@cloudscape-design/components/header";
import KeyValuePairs from "@cloudscape-design/components/key-value-pairs";
import Pagination from "@cloudscape-design/components/pagination";
import SpaceBetween from "@cloudscape-design/components/space-between";
import Table from "@cloudscape-design/components/table";
import Tabs from "@cloudscape-design/components/tabs";
import TextFilter from "@cloudscape-design/components/text-filter";
import { useMemo } from "react";

import { AssetView } from "../assets/AssetView";
import { duration, fmtTime } from "../lib/clock";
import { requestTitle } from "../lib/request";
import { isSettled, statusIndicator } from "../lib/status";
import type { Action, NodeStatus, SessionSnapshot, Workflow } from "../types";
import { Graph } from "./Graph";
import { ConnectAccount } from "./ConnectAccount";
import { toolsLabel } from "../lib/tools";

/** One row of the Steps table. FLATTENED on purpose: a sortable column needs a scalar
 *  `sortingField`, so anything the table sorts by is computed once here rather than
 *  inside a cell renderer. */
interface StepRow {
  id: string;
  stage: number;
  name: string;
  status: NodeStatus;
  placement: string;
  source: string;
  produces: string;
  bytes: number;
}

interface EventRow {
  key: number;
  ts: string;
  step: string;
  msg: string;
}

const EVENT_PAGE = 20;

/** Why a finished run stopped where it did, read from its timeline: a branch rule that
 *  sent it to END, a guardrail block, or an agent that failed. Null for a run that ran
 *  to its last step (or has not finished). Before this, the reason was only on the
 *  Timeline tab, so a run that ended early looked like one that produced nothing. */
export function endReason(snap: SessionSnapshot, workflow?: Workflow): { type: "info" | "error"; text: string } | null {
  const overall = String(snap.overall ?? "");
  const logs = (snap.logs ?? []).slice().reverse();
  if (overall === "done") {
    // nodes.py make_branch_node: "Branch after <agent>: <rule> -> END · skipping a, b"
    for (const l of logs) {
      const m = /^Branch after (.+?): (.+) -> END(?: · skipping (.+))?$/.exec(l.msg);
      if (!m) continue;
      const agents = (workflow ?? snap.workflow)?.agents ?? {};
      const name = (id: string) => agents[id]?.name ?? id;
      const skipped = m[3] ? m[3].split(", ").map((id) => name(id.trim())).join(", ") : "";
      return { type: "info", text: `Ended early: after ${name(m[1])}, the branch rule "${m[2]}" sent the run to END`
        + (skipped ? `, so ${skipped} did not run.` : ".") };
    }
    return null;
  }
  if (overall === "failed") {
    const why = logs.find((l) => / blocked by guardrail: | failed: /.test(l.msg));
    return { type: "error", text: why ? why.msg : "The run failed. The Timeline tab has each step's events." };
  }
  return null;
}

export function RunDetail({
  snap, workflow, selected, onSelect, can, onCancel, activeTab, onTabChange, onRerun,
}: {
  snap: SessionSnapshot;
  workflow: Workflow;
  selected: string | null;
  onSelect: (id: string | null) => void;
  can: (a: Action) => boolean;
  onCancel: () => void;
  activeTab: string;
  onTabChange: (id: string) => void;
  onRerun: (agents: string[], comment: string) => Promise<void>;
}) {
  const nodes = snap.nodes ?? {};
  const overall = String(snap.overall ?? "");
  const running = ["running", "waiting_human", "cancelling"].includes(overall);
  const settled = isSettled(overall);
  const ids = useMemo(
    () => (workflow.steps ?? []).flatMap((s) => s.parallel ?? s.sequence ?? [s.agent!]),
    [workflow]);
  const doneCount = ids.filter((i) => nodes[i]?.status === "done").length;
  const ended = useMemo(() => endReason(snap, workflow), [snap, workflow]);

  const stageOf = (id: string): number => {
    const i = (workflow.steps ?? []).findIndex(
      (s) => (s.parallel ?? s.sequence ?? [s.agent]).includes(id));
    return i + 1;
  };

  const ran = (id: string) =>
    Boolean(nodes[id]?.output) || nodes[id]?.status === "done";

  /** What the run is waiting for. `hitl.node` is a GATE id on a parallel or sequence
   *  stage and an AGENT id on a single-agent stage — the two namespaces are separate, so
   *  looking only in `agents` printed a raw id like "research" for every group gate. */
  const awaiting = useMemo(() => {
    const node = snap.hitl?.node;
    if (!node) return "—";
    const gate = (workflow.steps ?? []).find((s) => s.gateId === node);
    return gate?.gateName ?? workflow.agents[node]?.name ?? node;
  }, [snap.hitl?.node, workflow]);

  const stepRows = useMemo<StepRow[]>(() => ids.map((id) => {
    const meta = workflow.agents[id] ?? {};
    const st = nodes[id];
    return {
      id,
      stage: stageOf(id),
      name: meta.name ?? id,
      status: st?.status ?? "pending",
      placement: meta.runtime ?? "main",
      source: toolsLabel(meta.tool) || (meta.runtime === "a2a" ? "remote agent" : (meta.access ?? "—")),
      produces: meta.produces ?? "—",
      bytes: st?.output?.length ?? 0,
    };
  }), [ids, nodes, workflow]);

  const eventRows = useMemo<EventRow[]>(
    () => (snap.logs ?? []).slice().reverse().map((l, i) => ({
      key: i,
      ts: l.ts,
      step: l.node ? (workflow.agents[l.node]?.name ?? l.node) : "—",
      msg: l.msg,
    })),
    [snap.logs, workflow.agents]);

  const steps = useCollection(stepRows, {
    filtering: {
      empty: <Box textAlign="center" padding={{ vertical: "l" }}>No steps.</Box>,
      noMatch: <Box textAlign="center" padding={{ vertical: "l" }}>No step matches.</Box>,
    },
    sorting: { defaultState: { sortingColumn: { sortingField: "stage" } } },
  });

  const events = useCollection(eventRows, {
    filtering: {
      empty: <Box textAlign="center" padding={{ vertical: "l" }}>No events yet.</Box>,
      noMatch: <Box textAlign="center" padding={{ vertical: "l" }}>No event matches.</Box>,
    },
    sorting: {},
    pagination: { pageSize: EVENT_PAGE },
  });

  return (
    <SpaceBetween size="l">
      <Header
        variant="h1"
        description={`Run ${snap.session_id}`}
        actions={
          <SpaceBetween direction="horizontal" size="xs">
            {running ? (
              <Button iconName="close" disabled={!can("cancel")} onClick={onCancel}>
                Stop run
              </Button>
            ) : null}
          </SpaceBetween>
        }
      >
        {requestTitle(snap.topic) || "(no topic)"}
      </Header>
      {snap.topic && requestTitle(snap.topic) !== snap.topic.trim() ? (
        <ExpandableSection variant="container" headerText="Request" defaultExpanded={false}>
          <Box variant="p"><span style={{ whiteSpace: "pre-wrap", overflowWrap: "anywhere" }}>{snap.topic}</span></Box>
        </ExpandableSection>
      ) : null}

      <ConnectAccount snap={snap} canRerun={settled && can("rerun")} onRerun={onRerun} />
      {ended ? (
        <Alert type={ended.type} header={ended.type === "error" ? "Why the run stopped" : "Why the run ended here"}
          action={<Button onClick={() => onTabChange("timeline")}>Open the Timeline</Button>}>
          {ended.text}
        </Alert>
      ) : null}
      <Container header={<Header variant="h2">Details</Header>}>
        <KeyValuePairs
          columns={4}
          items={[
            { label: "Status", value: statusIndicator(overall) },
            { label: "Progress", value: `${doneCount} of ${ids.length} steps complete` },
            { label: "Started", value: fmtTime(snap.created) || "—" },
            { label: "Duration", value: duration(snap.created, snap.updated_at) },
            { label: "Run ID", value: <Box variant="code" fontSize="body-s">{snap.session_id}</Box> },
            { label: "Started by", value: snap.user || "—" },
            { label: "Subject", value: snap.subject_id || "—" },
            ...(snap.attachments?.length ? [{
              label: "Files",
              value: (
                <SpaceBetween size="xxxs">
                  {snap.attachments.map((f) => (
                    <Box key={f.key} fontSize="body-s">
                      📎 <span title={f.uri ?? f.name}>{f.name}</span>
                      {" "}<Box variant="span" color="text-body-secondary" fontSize="body-s">
                        ({(f.size / 1e6 >= 0.1 ? `${(f.size / 1e6).toFixed(1)} MB` : `${Math.max(1, Math.round(f.size / 1e3))} KB`)}{f.uri ? ", from S3" : ""})
                      </Box>
                    </Box>
                  ))}
                </SpaceBetween>
              ),
            }] : []),
            { label: "Awaiting", value: awaiting },
          ]}
        />
      </Container>

      <Tabs
        activeTabId={activeTab}
        onChange={({ detail }) => onTabChange(detail.activeTabId)}
        tabs={[
          {
            id: "graph",
            label: "Graph view",
            content: (
              <Container
                header={
                  <Header
                    variant="h2"
                    description={settled
                      ? "Select a step to see its configuration, output and re-run controls. Restart the whole workflow from the toolbar."
                      : "Select a step to see its configuration and output in the panel."}
                  >
                    Workflow
                  </Header>
                }
                disableContentPaddings
              >
                <Graph
                  workflow={workflow} nodes={nodes} selected={selected} onSelect={onSelect}
                  rerunnable={(id) => settled && can("rerun") && ran(id)}
                  onRerunFrom={settled && can("rerun") ? (id) => onSelect(id) : undefined}
                  onRestart={
                    settled && can("rerun") && ids.length > 0 && ran(ids[0])
                      ? () => void onRerun([ids[0]], "")
                      : undefined
                  }
                />
              </Container>
            ),
          },
          {
            id: "table",
            label: "Table view",
            content: (
              <Table
                {...steps.collectionProps}
                items={steps.items}
                variant="container"
                trackBy="id"
                onRowClick={({ detail }) => onSelect(detail.item.id)}
                selectedItems={stepRows.filter((r) => r.id === selected)}
                header={
                  <Header variant="h2" counter={`(${steps.filteredItemsCount ?? ids.length})`}>
                    Steps
                  </Header>
                }
                filter={
                  <TextFilter
                    {...steps.filterProps}
                    filteringPlaceholder="Find steps"
                    filteringAriaLabel="Filter steps"
                    countText={steps.filterProps.filteringText
                      ? `${steps.filteredItemsCount} matches`
                      : ""}
                  />
                }
                columnDefinitions={[
                  {
                    id: "stage", header: "Stage", width: 100,
                    sortingField: "stage",
                    cell: (r) => r.stage,
                  },
                  {
                    id: "name", header: "Step", isRowHeader: true, minWidth: 220,
                    sortingField: "name",
                    cell: (r) => r.name,
                  },
                  {
                    id: "status", header: "Status", width: 170,
                    sortingField: "status",
                    cell: (r) => statusIndicator(r.status),
                  },
                  {
                    id: "placement", header: "Placement", width: 140,
                    sortingField: "placement",
                    cell: (r) => r.placement,
                  },
                  {
                    id: "source", header: "Data source", width: 180,
                    sortingField: "source",
                    cell: (r) => r.source,
                  },
                  {
                    id: "produces", header: "Produces", width: 170,
                    sortingField: "produces",
                    cell: (r) => r.produces,
                  },
                  {
                    id: "bytes", header: "Output", width: 120,
                    // Sorted on the NUMBER, displayed as KB. Sorting the formatted
                    // string would have put 9.9 KB above 10.1 KB.
                    sortingField: "bytes",
                    cell: (r) => (r.bytes ? `${(r.bytes / 1024).toFixed(1)} KB` : "—"),
                  },
                ]}
              />
            ),
          },
          {
            id: "timeline",
            label: "Timeline",
            content: (
              <Table
                {...events.collectionProps}
                items={events.items}
                variant="container"
                trackBy="key"
                header={
                  <Header
                    variant="h2"
                    counter={`(${events.filteredItemsCount ?? eventRows.length})`}
                    description="Newest first. Branch decisions, guardrail blocks and ungrounded-figure warnings appear here."
                  >
                    Events
                  </Header>
                }
                filter={
                  <TextFilter
                    {...events.filterProps}
                    filteringPlaceholder="Find events"
                    filteringAriaLabel="Filter events"
                    countText={events.filterProps.filteringText
                      ? `${events.filteredItemsCount} matches`
                      : ""}
                  />
                }
                pagination={<Pagination {...events.paginationProps} />}
                columnDefinitions={[
                  {
                    id: "ts", header: "Time", width: 190,
                    sortingField: "ts",
                    cell: (l) => <Box fontSize="body-s" variant="code">{fmtTime(l.ts)}</Box>,
                  },
                  {
                    id: "step", header: "Step", width: 190,
                    sortingField: "step",
                    cell: (l) => l.step,
                  },
                  {
                    id: "msg", header: "Message", minWidth: 320,
                    sortingField: "msg",
                    cell: (l) => l.msg,
                  },
                ]}
              />
            ),
          },
          {
            id: "outputs",
            label: "Outputs",
            content: (
              <Container
                header={
                  <Header variant="h2" counter={`(${ids.filter((i) => nodes[i]?.output).length})`}>
                    Step outputs
                  </Header>
                }
              >
                <SpaceBetween size="s">
                  {ids.filter((i) => nodes[i]?.output).map((id) => (
                    <ExpandableSection
                      key={id}
                      variant="container"
                      headerText={workflow.agents[id]?.name ?? id}
                    >
                      <AssetView text={nodes[id]?.output ?? ""} />
                    </ExpandableSection>
                  ))}
                  {ids.every((i) => !nodes[i]?.output) ? (
                    <Box color="text-status-inactive">{ended && settled ? ended.text : "No outputs yet."}</Box>
                  ) : null}
                </SpaceBetween>
              </Container>
            ),
          },
        ]}
      />
    </SpaceBetween>
  );
}
