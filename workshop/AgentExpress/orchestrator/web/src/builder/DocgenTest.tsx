/** Track B docgen. The Document generator's test run: call the deployed function once
 *  with sample data (editable), as the Gateway does, and show the file it wrote with its
 *  link. The same route as a code tool's Test tool (POST /api/builds/{id}/test-tool). */
import Alert from "@cloudscape-design/components/alert";
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import ExpandableSection from "@cloudscape-design/components/expandable-section";
import FormField from "@cloudscape-design/components/form-field";
import Link from "@cloudscape-design/components/link";
import SpaceBetween from "@cloudscape-design/components/space-between";
import StatusIndicator from "@cloudscape-design/components/status-indicator";
import Textarea from "@cloudscape-design/components/textarea";
import { useState } from "react";
import { DOCGEN_TOOL, sampleEvent } from "./docgenTool";
import type { Entry } from "./model";
import { codeTool, type ToolTest } from "./storage";
import { useDocsBase } from "../docs/docsLinks";
import { infoSlot } from "../docs/InfoLink";

/** The file the function says it wrote, when its answer is that shape. */
function written(output: string): { url: string; s3Uri: string; fileName: string } | null {
  try {
    const v = JSON.parse(output);
    return v && typeof v.url === "string" && v.url.startsWith("https://") ? v : null;
  } catch { return null; }
}

export function DocgenTest({ buildId, id, tool }: { buildId: string; id: string; tool: Entry }) {
  const docs = useDocsBase();
  const [event, setEvent] = useState(() => JSON.stringify(sampleEvent(tool), null, 2));
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<ToolTest | null>(null);
  const [error, setError] = useState("");
  let parsed: Record<string, unknown> | null = null;
  try {
    const v = JSON.parse(event);
    parsed = v && typeof v === "object" && !Array.isArray(v) ? v : null;
  } catch { parsed = null; }
  const file = result?.ok ? written(result.output) : null;
  return (
    <ExpandableSection headerText="Test run with sample data" variant="container" headerInfo={infoSlot(docs, "tool.docgen")}>
      <SpaceBetween size="s">
        <Box variant="small" color="text-body-secondary">
          Calls the deployed function once, as the Gateway does. The file goes to the run files of a run named
          console-test, and to the output prefix when the tool has one.
        </Box>
        <FormField label="Event" stretch errorText={parsed ? undefined : "A JSON object"}>
          <div className="axp-cedar"><Textarea value={event} rows={10} spellcheck={false} ariaLabel="Sample event"
            onChange={({ detail }) => setEvent(detail.value)} /></div>
        </FormField>
        <Button loading={busy} disabled={!parsed} onClick={async () => {
          setBusy(true); setError(""); setResult(null);
          try { setResult(await codeTool.test(buildId, id, DOCGEN_TOOL, parsed!)); } catch (e) { setError((e as Error).message); }
          finally { setBusy(false); }
        }}>Run with sample data</Button>
        {error ? <Alert type="error">{error}</Alert> : null}
        {result ? (
          <SpaceBetween size="xxs">
            <StatusIndicator type={result.ok && !/"error"/.test(result.output) ? "success" : "error"}>
              {result.ok ? "Returned" : `Failed: ${result.error}`} · {result.ms} ms
            </StatusIndicator>
            {file ? <Link external href={file.url}>{`Download ${file.fileName}`}</Link> : null}
            <pre className="axb-code">{result.output}</pre>
          </SpaceBetween>
        ) : null}
      </SpaceBetween>
    </ExpandableSection>
  );
}
