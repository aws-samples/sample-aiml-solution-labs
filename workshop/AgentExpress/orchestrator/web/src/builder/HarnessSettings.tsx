/** The settings of an AgentCore Harness agent (workflow.json `harness`, app/common/harness_agent.py):
 *  who serves its model, the tools it may use besides its Gateway tools (picked under Tools),
 *  skills, memory, its environment, a named endpoint, and limits; or "Use an existing harness"
 *  by ARN, which turns everything the harness itself holds off. Checked as you type by
 *  harnessCheck.ts; every control is a Cloudscape one, so it is reachable by keyboard. */
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import ExpandableSection from "@cloudscape-design/components/expandable-section";
import FormField from "@cloudscape-design/components/form-field";
import Autosuggest from "@cloudscape-design/components/autosuggest";
import Input from "@cloudscape-design/components/input";
import Select from "@cloudscape-design/components/select";
import SpaceBetween from "@cloudscape-design/components/space-between";
import Toggle from "@cloudscape-design/components/toggle";
import type { Entry, Json, Workflow } from "./model";

type Obj = Record<string, Json>;
const isObj = (v: unknown): v is Obj => typeof v === "object" && v !== null && !Array.isArray(v);
const list = (v: Json | undefined): Obj[] => (Array.isArray(v) ? v.filter(isObj) : []);
const empty = (v: Json | undefined) => v === undefined || v === "" || v === false || (Array.isArray(v) && !v.length)
  || (isObj(v) && !Object.keys(v).length);

/** What a harness you bring keeps (harness_agent.BYO_KEYS), and the agent keys it drops. */
const BYO_KEYS = ["arn", "endpoint", "maxIterations", "timeoutSeconds"];
const BYO_NOT = ["instructions", "model", "temperature", "topP", "maxTokens", "tool"];

const PROVIDERS = [
  { value: "bedrock", label: "Amazon Bedrock", description: "No API key: the harness's own role" },
  { value: "openai", label: "OpenAI", description: "Needs an API key" },
  { value: "gemini", label: "Google Gemini", description: "Needs an API key" },
  { value: "litellm", label: "LiteLLM", description: "Any LiteLLM provider, e.g. gemini/gemini-2.5-pro" },
];
const SKILL_KINDS = [
  { value: "aws", label: "AWS skills catalog" },
  { value: "git", label: "Git repository" },
  { value: "s3", label: "Amazon S3" },
];
const MOUNT_KINDS = [
  { value: "session", label: "Session storage" },
  { value: "efs", label: "Amazon EFS" },
  { value: "s3Files", label: "Amazon S3 Files" },
];

export function HarnessSettings({ agent, workflow, onAgent }: {
  agent: Entry; workflow: Workflow; onAgent: (next: Entry) => void;
}) {
  const h: Obj = isObj(agent.harness) ? agent.harness : {};
  /** Set (or clear) one key of the block; an empty block goes. */
  const put = (key: string, v: Json | undefined) => {
    const next: Obj = { ...h };
    if (empty(v)) delete next[key]; else next[key] = v as Json;
    const { harness: _drop, ...rest } = agent;
    onAgent(Object.keys(next).length ? { ...rest, harness: next } : rest);
  };
  const brought = typeof h.arn === "string";
  const provider = String(h.provider ?? "bedrock");
  const keys = Object.entries((workflow as unknown as Obj).identities ?? {})
    .filter(([, d]) => isObj(d) && String(d.type ?? "").toLowerCase() === "apikey").map(([k]) => k);
  const memories = Object.keys(isObj((workflow as unknown as Obj).memories) ? (workflow as unknown as Obj).memories as Obj : {});
  const num = (key: string, label: string, hint: string) => (
    <FormField label={label} description={hint}>
      <Input type="number" ariaLabel={label} value={h[key] === undefined ? "" : String(h[key])}
        onChange={({ detail }) => put(key, detail.value === "" ? undefined : Number(detail.value))} />
    </FormField>
  );
  const rows = (key: string) => list(h[key]);
  const setRow = (key: string, i: number, row: Obj) => put(key, rows(key).map((r, j) => (j === i ? row : r)));
  const dropRow = (key: string, i: number) => put(key, rows(key).filter((_, j) => j !== i));
  /** true, false, or a custom resource's ARN. */
  const builtIn = (key: string, label: string, what: string) => {
    const v = h[key];
    return (
      <SpaceBetween size="xxs">
        <Toggle checked={v === true || typeof v === "string"} onChange={({ detail }) => put(key, detail.checked || undefined)}>{label}</Toggle>
        {v === true || typeof v === "string" ? (
          <FormField label={`Custom ${what} ARN (optional)`} description={`Leave empty for the managed ${what}.`}>
            <Input ariaLabel={`Custom ${what} ARN`} value={typeof v === "string" ? v : ""} placeholder="arn:aws:bedrock-agentcore:..."
              onChange={({ detail }) => put(key, detail.value.trim() || true)} />
          </FormField>
        ) : null}
      </SpaceBetween>
    );
  };

  return (
    <SpaceBetween size="m">
      <Toggle checked={brought} onChange={({ detail }) => {
        if (detail.checked) {
          // A harness you bring has its own prompt, model, tools and settings.
          const kept = Object.fromEntries(Object.entries(h).filter(([k]) => BYO_KEYS.includes(k)));
          const rest = Object.fromEntries(Object.entries(agent).filter(([k]) => !BYO_NOT.includes(k) && k !== "harness"));
          onAgent({ ...rest, harness: { ...kept, arn: "" } });
        } else put("arn", undefined);
      }}>Use an existing harness (ARN)</Toggle>

      {brought ? (
        <SpaceBetween size="s">
          <FormField label="Harness ARN" description="A harness in this account and region. Nothing is created for this agent; the harness keeps its own prompt, model and tools.">
            <Input ariaLabel="Harness ARN" value={String(h.arn ?? "")} placeholder="arn:aws:bedrock-agentcore:<region>:<account>:harness/<name>-<id>"
              onChange={({ detail }) => onAgent({ ...agent, harness: { ...h, arn: detail.value.trim() } })} />
          </FormField>
          <FormField label="Endpoint (optional)" description="One of that harness's named endpoints. Empty calls its latest version.">
            <Input ariaLabel="Endpoint" value={String(h.endpoint ?? "")} onChange={({ detail }) => put("endpoint", detail.value.trim())} />
          </FormField>
          {num("maxIterations", "Most model turns", "1-100, for each step. Empty: the harness's own.")}
          {num("timeoutSeconds", "Longest step (seconds)", "1-900. Empty: the harness's own.")}
        </SpaceBetween>
      ) : (
        <SpaceBetween size="s">
          <ExpandableSection headerText="Model" variant="footer" defaultExpanded>
            <SpaceBetween size="s">
              <FormField label="Provider" description="Who serves the model named under Model above.">
                <Select ariaLabel="Provider" options={PROVIDERS}
                  selectedOption={PROVIDERS.find((p) => p.value === provider) ?? PROVIDERS[0]}
                  onChange={({ detail }) => {
                    const v = detail.selectedOption.value!;
                    const next: Obj = { ...h, provider: v };
                    if (v === "bedrock") { delete next.provider; delete next.apiKey; }
                    if (v !== "litellm") delete next.apiBase;
                    const { harness: _drop, ...rest } = agent;
                    onAgent(Object.keys(next).length ? { ...rest, harness: next } : rest);
                  }} />
              </FormField>
              {provider !== "bedrock" ? (
                <FormField label="API key" description="An API key identity of this build (its key in the build's secrets), or an AgentCore API key credential provider ARN. Never the key itself.">
                  <Autosuggest ariaLabel="API key" value={String(h.apiKey ?? "")} options={keys.map((k) => ({ value: k }))}
                    enteredTextLabel={(v) => `Use "${v}"`} empty="No API key identities yet: add one under Identities"
                    onChange={({ detail }) => put("apiKey", detail.value.trim())} />
                </FormField>
              ) : null}
              {provider === "litellm" ? (
                <FormField label="API base URL (optional)" description="An OpenAI-compatible endpoint (a proxy or gateway), https on a public host. The API key is sent to it.">
                  <Input ariaLabel="API base URL" value={String(h.apiBase ?? "")} placeholder="https://"
                    onChange={({ detail }) => put("apiBase", detail.value.trim())} />
                </FormField>
              ) : null}
            </SpaceBetween>
          </ExpandableSection>

          <ExpandableSection headerText="Tools" variant="footer" defaultExpanded
            headerDescription="This build's tools are picked under Tools; the harness calls them through the Gateway. It gets nothing else unless turned on here.">
            <SpaceBetween size="s">
              <Toggle checked={h.shell === true} onChange={({ detail }) => put("shell", detail.checked || undefined)}>Shell and files</Toggle>
              {builtIn("browser", "Web browser", "browser")}
              {builtIn("codeInterpreter", "Code interpreter", "code interpreter")}
              <Box variant="awsui-key-label">MCP servers</Box>
              {rows("mcp").map((m, i) => (
                <SpaceBetween key={i} direction="horizontal" size="xs">
                  <Input ariaLabel={`MCP server ${i + 1} name`} value={String(m.name ?? "")} placeholder="name"
                    onChange={({ detail }) => setRow("mcp", i, { ...m, name: detail.value.trim() })} />
                  <Input ariaLabel={`MCP server ${i + 1} URL`} value={String(m.url ?? "")} placeholder="https://"
                    onChange={({ detail }) => setRow("mcp", i, { ...m, url: detail.value.trim() })} />
                  <Button variant="icon" iconName="remove" ariaLabel={`Remove MCP server ${i + 1}`} onClick={() => dropRow("mcp", i)} />
                </SpaceBetween>
              ))}
              <Button iconName="add-plus" onClick={() => put("mcp", [...rows("mcp"), { name: "", url: "" }])}>Add MCP server</Button>
            </SpaceBetween>
          </ExpandableSection>

          <ExpandableSection headerText="Skills" variant="footer"
            headerDescription="Know-how the harness fetches when a session starts: from the AWS catalog, a Git repository or S3.">
            <SpaceBetween size="s">
              {rows("skills").map((s, i) => {
                const kind = "git" in s ? "git" : "s3" in s ? "s3" : "aws";
                return (
                  <SpaceBetween key={i} direction="horizontal" size="xs">
                    <Select ariaLabel={`Skill ${i + 1} source`} options={SKILL_KINDS}
                      selectedOption={SKILL_KINDS.find((k) => k.value === kind)!}
                      onChange={({ detail }) => setRow("skills", i, { [detail.selectedOption.value!]: detail.selectedOption.value === "aws" ? [] : "" })} />
                    <Input ariaLabel={`Skill ${i + 1}`} value={kind === "aws" ? (Array.isArray(s.aws) ? s.aws.join(", ") : "") : String(s[kind] ?? "")}
                      placeholder={kind === "aws" ? "core-skills/* (empty for all)" : kind === "git" ? "https://github.com/..." : "s3://bucket/prefix/"}
                      onChange={({ detail }) => setRow("skills", i, kind === "aws"
                        ? { aws: detail.value.split(",").map((x) => x.trim()).filter(Boolean) }
                        : { ...s, [kind]: detail.value.trim() })} />
                    {kind === "git" ? (
                      <Input ariaLabel={`Skill ${i + 1} folder`} value={String(s.path ?? "")} placeholder="folder (optional)"
                        onChange={({ detail }) => { const { path: _p, ...r } = s; setRow("skills", i, detail.value.trim() ? { ...r, path: detail.value.trim() } : r); }} />
                    ) : null}
                    <Button variant="icon" iconName="remove" ariaLabel={`Remove skill ${i + 1}`} onClick={() => dropRow("skills", i)} />
                  </SpaceBetween>
                );
              })}
              <Button iconName="add-plus" onClick={() => put("skills", [...rows("skills"), { aws: [] }])}>Add skill</Button>
            </SpaceBetween>
          </ExpandableSection>

          <ExpandableSection headerText="Memory" variant="footer">
            <FormField label="Memory" description="One of this build's memories: the harness keeps its conversation there and recalls what it learned, per user. Not together with the agent's own memory feature.">
              <Select ariaLabel="Memory" options={[{ value: "", label: "No memory" }, ...memories.map((m) => ({ value: m, label: m }))]}
                selectedOption={{ value: String(h.memory ?? ""), label: String(h.memory || "No memory") }}
                empty="No memories yet: add one under Memories"
                onChange={({ detail }) => put("memory", detail.selectedOption.value || undefined)} />
            </FormField>
          </ExpandableSection>

          <ExpandableSection headerText="Environment" variant="footer">
            <SpaceBetween size="s">
              <FormField label="Container image (optional)" description="Your own image for the session, built for linux/arm64. The harness may pull that repository only.">
                <Input ariaLabel="Container image" value={String(h.container ?? "")} placeholder="<account>.dkr.ecr.<region>.amazonaws.com/<repo>:<tag>"
                  onChange={({ detail }) => put("container", detail.value.trim())} />
              </FormField>
              <Box variant="awsui-key-label">Storage</Box>
              {rows("mounts").map((m, i) => {
                const kind = "efs" in m ? "efs" : "s3Files" in m ? "s3Files" : "session";
                return (
                  <SpaceBetween key={i} direction="horizontal" size="xs">
                    <Select ariaLabel={`Storage ${i + 1} kind`} options={MOUNT_KINDS} selectedOption={MOUNT_KINDS.find((k) => k.value === kind)!}
                      onChange={({ detail }) => setRow("mounts", i, detail.selectedOption.value === "session"
                        ? { session: "/mnt/data" } : { [detail.selectedOption.value!]: "", path: "/mnt/data" })} />
                    <Input ariaLabel={`Storage ${i + 1} path`} value={String(kind === "session" ? m.session ?? "" : m.path ?? "")} placeholder="/mnt/data"
                      onChange={({ detail }) => setRow("mounts", i, kind === "session" ? { session: detail.value.trim() } : { ...m, path: detail.value.trim() })} />
                    {kind !== "session" ? (
                      <Input ariaLabel={`Storage ${i + 1} access point ARN`} value={String(m[kind] ?? "")} placeholder="access point ARN"
                        onChange={({ detail }) => setRow("mounts", i, { ...m, [kind]: detail.value.trim() })} />
                    ) : null}
                    <Button variant="icon" iconName="remove" ariaLabel={`Remove storage ${i + 1}`} onClick={() => dropRow("mounts", i)} />
                  </SpaceBetween>
                );
              })}
              <Button iconName="add-plus" onClick={() => put("mounts", [...rows("mounts"), { session: "/mnt/data" }])}>Add storage</Button>
              <FormField label="VPC subnets (optional)" description="Run the session in your VPC; EFS and S3 Files need it. Comma-separated subnet ids.">
                <Input ariaLabel="VPC subnets" value={isObj(h.network) && Array.isArray(h.network.subnetIds) ? h.network.subnetIds.join(", ") : ""}
                  onChange={({ detail }) => {
                    const ids = detail.value.split(",").map((x) => x.trim()).filter(Boolean);
                    const net: Obj = { ...(isObj(h.network) ? h.network : {}), subnetIds: ids };
                    put("network", ids.length || (Array.isArray(net.securityGroupIds) && net.securityGroupIds.length) ? net : undefined);
                  }} />
              </FormField>
              <FormField label="Security groups" description="Comma-separated security group ids, with the VPC subnets.">
                <Input ariaLabel="Security groups" value={isObj(h.network) && Array.isArray(h.network.securityGroupIds) ? h.network.securityGroupIds.join(", ") : ""}
                  onChange={({ detail }) => {
                    const ids = detail.value.split(",").map((x) => x.trim()).filter(Boolean);
                    const net: Obj = { ...(isObj(h.network) ? h.network : {}), securityGroupIds: ids };
                    put("network", ids.length || (Array.isArray(net.subnetIds) && net.subnetIds.length) ? net : undefined);
                  }} />
              </FormField>
            </SpaceBetween>
          </ExpandableSection>

          <ExpandableSection headerText="Version and endpoint" variant="footer">
            <FormField label="Endpoint name (optional)" description="A named endpoint the run calls, moved to each new version a deploy makes. Letters, digits and underscores.">
              <Input ariaLabel="Endpoint name" value={String(h.endpoint ?? "")} onChange={({ detail }) => put("endpoint", detail.value.trim())} />
            </FormField>
          </ExpandableSection>

          <ExpandableSection headerText="Limits" variant="footer">
            <SpaceBetween size="s">
              {num("maxIterations", "Most model turns", "1-100, for each step. Empty: the harness's own.")}
              {num("timeoutSeconds", "Longest step (seconds)", "1-900. Empty: the harness's own.")}
            </SpaceBetween>
          </ExpandableSection>
        </SpaceBetween>
      )}
    </SpaceBetween>
  );
}
