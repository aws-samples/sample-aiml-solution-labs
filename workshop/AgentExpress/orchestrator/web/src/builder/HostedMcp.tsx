/** An MCP tool's server: one you run (by its URL, `endpoint`) or one AgentExpress hosts on its own
 *  AgentCore Runtime (`hosted`): its code written here, from git or S3, or a container image you
 *  built, and the Runtime's network. The Gateway reaches a hosted server with SigV4 as its role,
 *  so it has no auth to set. Checked by hostedMcpCheck.ts and networkCheck.ts; every control is a
 *  Cloudscape one, so it is reachable by keyboard.
 *  https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/runtime-mcp.html */
import Box from "@cloudscape-design/components/box";
import FormField from "@cloudscape-design/components/form-field";
import Link from "@cloudscape-design/components/link";
import SegmentedControl from "@cloudscape-design/components/segmented-control";
import SpaceBetween from "@cloudscape-design/components/space-between";
import Tabs from "@cloudscape-design/components/tabs";
import Textarea from "@cloudscape-design/components/textarea";
import { useState } from "react";
import type { Entry, Json, Project, ToolFiles } from "./model";
import type { CodeSource, CodeSourceKind } from "./codeSource";
import { CodeSourceField } from "./CodeSourceField";
import { NetworkFields } from "./NetworkSettings";
import type { Issue } from "./validate";

type Obj = Record<string, any>;
const isObj = (v: unknown): v is Obj => typeof v === "object" && v !== null && !Array.isArray(v);
export const MCP_DOC = "https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/runtime-mcp-protocol-contract.html";
/** The starter's requirements.txt: the MCP Python SDK and pydantic as the framework itself pins
 *  them (requirements.lock), the same set as bff/designer.py HOSTED_MCP_REQUIREMENTS (checked by
 *  tests/test_hosted_mcp.py). With no SDK the server cannot import FastMCP; an older SDK (1.12)
 *  or an unpinned pydantic can fail at import. */
export const HOSTED_MCP_REQUIREMENTS = "mcp==1.30.0\npydantic==2.13.5\n";

/** A starting server.py: the MCP Python SDK's FastMCP, streamable HTTP on 0.0.0.0:8000/mcp,
 *  stateless (the AgentCore Runtime MCP contract). */
export function starterServer(key: string): ToolFiles {
  return {
    "server.py": [
      "from mcp.server.fastmcp import FastMCP",
      "",
      "# AgentCore Runtime calls the server on 0.0.0.0:8000/mcp, streamable HTTP, stateless.",
      'mcp = FastMCP(host="0.0.0.0", stateless_http=True)',
      "",
      "",
      "@mcp.tool()",
      `def ${key.replace(/[^A-Za-z0-9_]/g, "_")}_echo(text: str) -> str:`,
      '    """Return the text it was given."""',
      "    return text",
      "",
      "",
      'if __name__ == "__main__":',
      '    mcp.run(transport="streamable-http")',
      "",
    ].join("\n"),
    "requirements.txt": HOSTED_MCP_REQUIREMENTS,
  };
}

const FILES = ["server.py", "requirements.txt", "Dockerfile"];
/** A hosted server's code: written or uploaded, git, S3, or an image used as it is
 *  (code-source shape, codeSource.ts; `image` only here). */
const HOSTED_SOURCES: readonly CodeSourceKind[] = ["git", "s3", "image"];
/** The uploaded files a hosted server may hold (scaffold.py HOSTED_FILE_RE). */
const HOSTED_FILE_RE = /^([a-z_][a-z0-9_]*\.py|requirements\.txt|Dockerfile)$/;
const hostedFiles = (f: ToolFiles): ToolFiles => Object.fromEntries(Object.entries(f).filter(([n]) => HOSTED_FILE_RE.test(n)));
const HINTS: Record<string, string> = {
  "server.py": "Run as `python server.py`. Serve MCP over streamable HTTP on 0.0.0.0, port 8000, path /mcp.",
  "requirements.txt": "One name==version per line, installed into the image (for example the mcp package).",
  "Dockerfile": "Optional. Empty: an ARM64 Python 3.13 image that installs requirements.txt and runs server.py.",
};

function ServerFiles({ files, onChange }: { files: ToolFiles; onChange: (f: ToolFiles) => void }) {
  const [active, setActive] = useState("server.py");
  return (
    <Tabs activeTabId={active} onChange={({ detail }) => setActive(detail.activeTabId)}
      tabs={FILES.map((n) => ({
        id: n, label: n,
        content: (
          <FormField stretch description={HINTS[n]}>
            <div className="axp-cedar">
              <Textarea value={files[n] ?? ""} rows={n === "server.py" ? 16 : 6} spellcheck={false} ariaLabel={n}
                onChange={({ detail }) => {
                  const next = { ...files, [n]: detail.value };
                  if (n === "Dockerfile" && !detail.value.trim()) delete next.Dockerfile;
                  onChange(next);
                }} />
            </div>
          </FormField>
        ),
      }))} />
  );
}

export function HostedMcp({ project, id, onChange, issues = [], server = false }: {
  project: Project; id: string; onChange: (p: Project) => void; issues?: Issue[];
  /** With a server, a git token is set as a build secret here (CodeSourceField). */
  server?: boolean;
}) {
  const tool = project.workflow.tools[id] as Obj;
  const hosted = isObj(tool.hosted) ? tool.hosted : undefined;
  const code = hosted && isObj(hosted.code) ? hosted.code : undefined;
  const files = project.toolCode?.[id] ?? {};
  const path = `tools.${id}`;
  const setTool = (t: Obj, toolCode?: ToolFiles) => onChange({
    ...project,
    workflow: { ...project.workflow, tools: { ...project.workflow.tools, [id]: t as Entry } },
    ...(toolCode ? { toolCode: { ...(project.toolCode ?? {}), [id]: toolCode } } : {}),
  });
  const setHosted = (h: Obj) => setTool({ ...tool, hosted: h as Json });
  const { code: _c, ...hostedRest } = hosted ?? {};
  return (
    <SpaceBetween size="m">
      <FormField label="The server" description={hosted
        ? "Hosted by AgentExpress on its own AgentCore Runtime and registered on the build's Gateway. The Gateway signs its calls with SigV4."
        : "An MCP server you run, by its streamable HTTP URL."}>
        <SegmentedControl label="The server" selectedId={hosted ? "hosted" : "url"}
          options={[{ id: "url", text: "Yours, by URL" }, { id: "hosted", text: "Host it here" }]}
          onChange={({ detail }) => {
            const { endpoint: _e, hosted: _h, auth: _a, service: _s, ...rest } = tool;
            if (detail.selectedId === "hosted") {
              setTool({ ...rest, hosted: {} }, Object.keys(files).length ? undefined : starterServer(id));
            } else {
              setTool({ ...rest, endpoint: "https://" });
            }
          }} />
      </FormField>
      {hosted ? (
        <>
          <FormField label="Its code" errorText={issues.filter((i) => i.path === `${path}.hosted.code`).map((i) => i.message).join(" ") || undefined}
            description="Written or uploaded here, fetched from git or S3 at deploy, or a container image you built (ARM64, serving 0.0.0.0:8000/mcp). The folder needs a server.py or a Dockerfile at its root.">
            <CodeSourceField source={code as CodeSource | undefined} allow={HOSTED_SOURCES} path={`${path}.hosted.code`}
              buildId={server ? project.id : undefined}
              onSource={(s) => s ? setHosted({ ...hostedRest, code: s as unknown as Json })
                : setTool({ ...tool, hosted: hostedRest }, Object.keys(files).length ? undefined : starterServer(id))}
              onFiles={(f) => onChange({ ...project, toolCode: { ...(project.toolCode ?? {}), [id]: hostedFiles(f) } })} />
          </FormField>
          {!code ? <ServerFiles files={files} onChange={(f) => onChange({ ...project, toolCode: { ...(project.toolCode ?? {}), [id]: f } })} /> : null}
          <FormField label="Network" description="Where the server's Runtime runs.">
            <NetworkFields value={hosted.network} path={`${path}.hosted.network`} issues={issues}
              onChange={(net) => {
                const { network: _n, ...rest } = hosted;
                setHosted(net ? { ...rest, network: net } : rest);
              }} />
          </FormField>
          <Box variant="small" color="text-body-secondary">
            The server gets no AWS permissions in this version.{" "}
            <Link href={MCP_DOC} external externalIconAriaLabel="Opens in a new tab">AgentCore MCP server contract</Link>
          </Box>
        </>
      ) : null}
    </SpaceBetween>
  );
}
