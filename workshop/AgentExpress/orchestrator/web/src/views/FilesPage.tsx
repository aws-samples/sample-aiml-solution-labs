/** The Files page: the files of your recent runs (an admin may show everyone's), newest
 *  run first, ten runs at a time (GET /api/files, bff/runartifacts.py). Find a file by its
 *  name, agent or run; open its run; preview or download it. */

import { useCollection } from "@cloudscape-design/collection-hooks";
import Alert from "@cloudscape-design/components/alert";
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import Header from "@cloudscape-design/components/header";
import Link from "@cloudscape-design/components/link";
import SegmentedControl from "@cloudscape-design/components/segmented-control";
import SpaceBetween from "@cloudscape-design/components/space-between";
import Table from "@cloudscape-design/components/table";
import TextFilter from "@cloudscape-design/components/text-filter";
import { useEffect, useState } from "react";

import { api } from "../api";
import { fmtTime } from "../lib/clock";
import { GROUP_LABEL, fmtSize, type RunFile } from "../lib/files";
import { requestTitle } from "../lib/request";
import type { Workflow } from "../types";
import { FileActions, FilePreview, agentName } from "./RunFiles";

interface FilesPageBody {
  stored: boolean;
  files: RunFile[];
  nextPage: number | null;
}

export function FilesPage({ workflow, canSeeEveryone, onOpenRun }: {
  workflow: Workflow; canSeeEveryone: boolean; onOpenRun: (sessionId: string) => void;
}) {
  const [everyone, setEveryone] = useState(false);
  const [files, setFiles] = useState<RunFile[]>([]);
  const [next, setNext] = useState<number | null>(null);
  const [stored, setStored] = useState(true);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [previewing, setPreviewing] = useState<RunFile | null>(null);

  const load = async (page: number, scopeAll: boolean, append: boolean) => {
    setLoading(true);
    setError(null);
    try {
      const b = await api.get<FilesPageBody>(`/api/files?page=${page}${scopeAll ? "&scope=all" : ""}`);
      setStored(b.stored);
      setFiles((prev) => (append ? [...prev, ...b.files] : b.files));
      setNext(b.nextPage);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setLoading(false);
    }
  };
  useEffect(() => { void load(0, everyone, false); }, [everyone]);

  const rows = files.map((f) => ({ ...f, kind: GROUP_LABEL[f.group], agentLabel: agentName(workflow, f.agent), run: requestTitle(f.topic ?? "", 80) || f.session }));
  const { items, filterProps, filteredItemsCount, collectionProps } = useCollection(rows, {
    filtering: {
      fields: ["name", "kind", "agentLabel", "run", "session"],
      empty: <Box color="text-status-inactive">{stored ? "Your runs have no files yet." : "This deployment keeps no run files. Set orchestrator.files in workflow.json to keep what agents make."}</Box>,
      noMatch: <Box color="text-status-inactive">No file matches.</Box>,
    },
    sorting: { defaultState: { sortingColumn: { sortingField: "time" }, isDescending: true } },
  });

  return (
    <SpaceBetween size="l">
      {error ? <Alert type="error" header="Could not list files">{error}</Alert> : null}
      <Table
        {...collectionProps}
        variant="full-page"
        items={items}
        trackBy="key"
        loading={loading && files.length === 0}
        loadingText="Loading files"
        header={
          <Header
            variant="h1"
            counter={`(${filteredItemsCount ?? files.length})`}
            description="What your runs were started with, and what their agents made. Newest runs first."
            actions={canSeeEveryone ? (
              <SegmentedControl
                label="Whose runs"
                selectedId={everyone ? "all" : "mine"}
                options={[{ id: "mine", text: "Mine" }, { id: "all", text: "Everyone" }]}
                onChange={({ detail }) => setEveryone(detail.selectedId === "all")}
              />
            ) : undefined}
          >
            Files
          </Header>
        }
        filter={
          <TextFilter
            {...filterProps}
            filteringPlaceholder="Find by name, agent or run"
            filteringAriaLabel="Filter files"
            countText={filterProps.filteringText ? `${filteredItemsCount} matches` : ""}
          />
        }
        footer={next !== null ? (
          <Box textAlign="center">
            <Button onClick={() => void load(next, everyone, true)} loading={loading}>Show older runs</Button>
          </Box>
        ) : undefined}
        columnDefinitions={[
          { id: "name", header: "Name", isRowHeader: true, minWidth: 200, sortingField: "name", cell: (f) => f.name },
          { id: "kind", header: "Kind", width: 100, sortingField: "kind", cell: (f) => f.kind },
          {
            id: "run", header: "Run", minWidth: 200, sortingField: "run",
            cell: (f) => (
              <Link href={`#run:${f.session}`} onFollow={(e) => { e.preventDefault(); onOpenRun(f.session); }}>
                {f.run}
              </Link>
            ),
          },
          { id: "agent", header: "Agent", sortingField: "agentLabel", cell: (f) => f.agentLabel },
          { id: "size", header: "Size", width: 100, sortingField: "size", cell: (f) => fmtSize(f.size) },
          { id: "time", header: "Time", width: 190, sortingField: "time", cell: (f) => fmtTime(f.time) || "—" },
          { id: "actions", header: "Actions", width: 200, cell: (f) => <FileActions file={f} onPreview={setPreviewing} /> },
        ]}
      />
      <FilePreview file={previewing} onClose={() => setPreviewing(null)} />
    </SpaceBetween>
  );
}
