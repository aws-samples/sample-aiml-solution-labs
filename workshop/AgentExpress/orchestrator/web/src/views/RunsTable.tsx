/** The Runs collection, as a console Table.
 *
 *  This is the change that makes the app read as a service screen: a collection lives
 *  in a Table in the content area with a header count, a filter and row actions — it
 *  does not live in the side navigation, which a console reserves for navigating.
 *
 *  Filtering, sorting and pagination all come from `useCollection`, Cloudscape's own
 *  collection hook, rather than from three pieces of hand-rolled state. Sorting matters
 *  most on Started: the BFF returns runs in whatever order it stored them, so without a
 *  sort the newest run is not reliably at the top.
 */

import { useCollection } from "@cloudscape-design/collection-hooks";
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import Header from "@cloudscape-design/components/header";
import Link from "@cloudscape-design/components/link";
import Pagination from "@cloudscape-design/components/pagination";
import SpaceBetween from "@cloudscape-design/components/space-between";
import Table from "@cloudscape-design/components/table";
import TextFilter from "@cloudscape-design/components/text-filter";

import { fmtTime } from "../lib/clock";
import { requestTitle } from "../lib/request";
import { statusIndicator } from "../lib/status";
import type { SessionSummary } from "../types";

const PAGE_SIZE = 12;

export function RunsTable({
  runs, loading, can, onOpen, onDelete, onStartNew,
}: {
  runs: SessionSummary[];
  loading: boolean;
  can: (a: "delete" | "start") => boolean;
  onOpen: (id: string) => void;
  onDelete: (id: string) => void;
  onStartNew: () => void;
}) {
  const empty = (
    <Box textAlign="center" color="inherit" padding={{ vertical: "l" }}>
      <SpaceBetween size="s">
        <b>No runs</b>
        <Box variant="p" color="inherit">Start a run to see it here.</Box>
        <Button disabled={!can("start")} onClick={onStartNew}>Start run</Button>
      </SpaceBetween>
    </Box>
  );

  const { items, collectionProps, filterProps, paginationProps, filteredItemsCount } =
    useCollection(runs, {
      filtering: {
        empty,
        noMatch: (
          <Box textAlign="center" color="inherit" padding={{ vertical: "l" }}>
            <b>No matches</b>
          </Box>
        ),
        // The default matcher stringifies the whole row, which would let a search hit a
        // field the table does not show. Restrict it to the three the reader can see.
        filteringFunction: (r, text) => {
          const q = text.trim().toLowerCase();
          if (!q) return true;
          return r.topic.toLowerCase().includes(q)
            || r.session_id.toLowerCase().includes(q)
            || String(r.overall).toLowerCase().includes(q);
        },
      },
      sorting: {
        defaultState: {
          sortingColumn: { sortingField: "created" },
          isDescending: true,
        },
      },
      pagination: { pageSize: PAGE_SIZE },
    });

  return (
    <Table
      {...collectionProps}
      variant="container"
      loading={loading}
      loadingText="Loading runs"
      items={items}
      trackBy="session_id"
      onRowClick={({ detail }) => onOpen(detail.item.session_id)}
      header={
        <Header
          counter={`(${filteredItemsCount ?? runs.length})`}
          description="Each run is one execution of the workflow defined in workflow.json."
          actions={
            <SpaceBetween direction="horizontal" size="xs">
              <Button iconName="add-plus" variant="primary" disabled={!can("start")}
                      onClick={onStartNew}>
                Start run
              </Button>
            </SpaceBetween>
          }
        >
          Runs
        </Header>
      }
      filter={
        <TextFilter
          {...filterProps}
          filteringPlaceholder="Find runs"
          filteringAriaLabel="Filter runs"
          countText={filterProps.filteringText ? `${filteredItemsCount} matches` : ""}
        />
      }
      pagination={<Pagination {...paginationProps} />}
      empty={empty}
      columnDefinitions={[
        {
          id: "topic",
          header: "Request",
          sortingField: "topic",
          cell: (r) => (
            <Link
              href="#"
              onFollow={(e) => { e.preventDefault(); onOpen(r.session_id); }}
            >
              <span title={r.topic}>{requestTitle(r.topic, 120) || "(no topic)"}</span>
            </Link>
          ),
          isRowHeader: true,
          minWidth: 320,
        },
        {
          id: "status",
          header: "Status",
          sortingField: "overall",
          cell: (r) => statusIndicator(String(r.overall)),
          width: 170,
        },
        {
          id: "id",
          header: "Run ID",
          sortingField: "session_id",
          cell: (r) => <Box fontSize="body-s" variant="code">{r.session_id}</Box>,
          width: 180,
        },
        {
          id: "created",
          header: "Started",
          // Sorted on the raw ISO timestamp, not on the formatted Eastern-Time string,
          // which sorts alphabetically and would interleave months.
          sortingField: "created",
          cell: (r) => fmtTime(r.created) || "—",
          width: 190,
        },
        {
          id: "by",
          header: "Started by",
          // A trigger, not a person (bff/triggers.py): its name and kind.
          cell: (r) => (r.trigger ? `${r.trigger.name} (${r.trigger.type})` : "A person"),
          width: 170,
        },
        {
          id: "actions",
          header: "",
          cell: (r) =>
            can("delete")
              ? (
                <Button
                  variant="inline-icon" iconName="remove" ariaLabel={`Delete ${r.session_id}`}
                  onClick={() => onDelete(r.session_id)}
                />
              )
              : null,
          width: 70,
        },
      ]}
    />
  );
}
