/** Problems, each with where it is in workflow.json and the deploy's own reason. */

import Box from "@cloudscape-design/components/box";
import Link from "@cloudscape-design/components/link";
import SpaceBetween from "@cloudscape-design/components/space-between";
import StatusIndicator from "@cloudscape-design/components/status-indicator";

import type { Issue, Where } from "./validate";

export function IssueList({ issues, onPick }: { issues: Issue[]; onPick?: (w: Where) => void }) {
  const sorted = [...issues].sort((a, b) => (a.severity === b.severity ? 0 : a.severity === "error" ? -1 : 1));
  return (
    <SpaceBetween size="xs">
      {sorted.map((i, n) => (
        <div key={`${i.path}-${n}`} className="axb-issue">
          <StatusIndicator type={i.severity === "error" ? "error" : "warning"}>
            {onPick && i.where.kind !== "workflow" && i.where.kind !== "block"
              ? <Link onFollow={(e) => { e.preventDefault(); onPick(i.where); }}>{i.path}</Link>
              : <Box variant="code" fontSize="body-s">{i.path}</Box>}
          </StatusIndicator>
          <Box variant="small" color="text-body-secondary">{i.message}</Box>
        </div>
      ))}
    </SpaceBetween>
  );
}
