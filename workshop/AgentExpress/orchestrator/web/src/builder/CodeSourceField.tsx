/** Where a function's code comes from, shared by CodeTool.tsx, Interceptors.tsx and
 *  TaskPanel.tsx (and the hosted MCP server's panel, with `image`): written here, uploaded
 *  (files or a zip, unpacked in the page and kept in the build like written code), a git
 *  repository pinned to a ref, or S3. A git token is a build secret (gitTokens), set in its
 *  own field and never written into the workflow. Shape and checks: codeSource.ts. */
import Alert from "@cloudscape-design/components/alert";
import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import FormField from "@cloudscape-design/components/form-field";
import Input from "@cloudscape-design/components/input";
import SegmentedControl from "@cloudscape-design/components/segmented-control";
import SpaceBetween from "@cloudscape-design/components/space-between";
import { useRef, useState } from "react";

import { SecretField } from "./BuildResources";
import {
  codeFilesOf, codeSourceKind, codeSourceProblems, entriesOf, scriptOf,
  type CodeSource, type CodeSourceKind,
} from "./codeSource";

type Mode = "written" | "upload" | CodeSourceKind;

export function CodeSourceField({ source, onSource, allow = ["git", "s3"], mode = "files", language,
  onFiles, onScript, buildId, path = "codeSource" }: {
  source: CodeSource | undefined;
  onSource: (s: CodeSource | undefined) => void;
  allow?: readonly CodeSourceKind[];
  /** "files": a function's files (onFiles); "script": one Run code script (onScript). */
  mode?: "files" | "script";
  language?: string;
  onFiles?: (files: Record<string, string>) => void;
  onScript?: (code: string) => void;
  buildId?: string;
  /** Where it sits, for the messages (e.g. tools.hr.code.codeSource). */
  path?: string;
}) {
  const kind = codeSourceKind(source);
  const [picked, setPicked] = useState<Mode | null>(null);
  const shown: Mode = kind || (picked === "upload" ? "upload" : picked && picked !== "written" ? picked : "written");
  const [note, setNote] = useState<{ type: "success" | "error"; text: string } | null>(null);
  const input = useRef<HTMLInputElement>(null);
  const problems = source ? codeSourceProblems(source, path, allow) : [];
  const errorOf = (field: string) => problems.find((p) => p.path === `${path}.${shown}${field}`)?.message;
  const options = [
    { id: "written", text: "Written here" },
    { id: "upload", text: "Upload" },
    ...(allow.includes("git") ? [{ id: "git", text: "Git repository" }] : []),
    ...(allow.includes("s3") ? [{ id: "s3", text: "Amazon S3" }] : []),
    ...(allow.includes("image") ? [{ id: "image", text: "Container image" }] : []),
  ];
  const choose = (id: Mode) => {
    setPicked(id);
    setNote(null);
    if (id === "git") onSource({ git: { url: "", ref: "" } });
    else if (id === "s3") onSource({ s3: { uri: "" } });
    else if (id === "image") onSource({ image: "" });
    else onSource(undefined);
  };
  const upload = async (list: FileList | null) => {
    if (!list?.length) return;
    try {
      const entries = await entriesOf([...list]);
      if (mode === "script") {
        const r = scriptOf(entries, language);
        if (r.error || r.code === undefined) { setNote({ type: "error", text: r.error ?? "no script" }); return; }
        onScript?.(r.code);
        setNote({ type: "success", text: "Script uploaded. It is kept in the build like a written one." });
      } else {
        const r = codeFilesOf(entries);
        if (r.error) { setNote({ type: "error", text: r.error }); return; }
        onFiles?.(r.files);
        setNote({ type: "success", text: `Uploaded ${Object.keys(r.files).sort().join(", ")}.`
          + (r.skipped.length ? ` Skipped ${r.skipped.join(", ")} (a function holds *.py, requirements.txt and events.json).` : "") });
      }
      setPicked("written");
    } catch (e) {
      setNote({ type: "error", text: (e as Error).message });
    }
  };
  const git = kind === "git" ? (source as { git: Record<string, string> }).git : null;
  const s3 = kind === "s3" ? (source as { s3: Record<string, string> }).s3 : null;
  const setPart = (part: Record<string, string>) => {
    const k = kind as "git" | "s3";
    const cur = { ...(k === "git" ? git : s3), ...part };
    for (const f of Object.keys(cur)) if (cur[f] === "" && f !== "url" && f !== "ref" && f !== "uri") delete cur[f];
    onSource({ [k]: cur } as CodeSource);
  };
  return (
    <SpaceBetween size="s">
      <FormField label={mode === "script" ? "Where the script comes from" : "Where the code comes from"}
        description="Written or uploaded code is kept in the build and exported with it. Git and S3 are fetched when the build deploys.">
        <SegmentedControl selectedId={shown} label="Code source" options={options}
          onChange={({ detail }) => choose(detail.selectedId as Mode)} />
      </FormField>
      {shown === "upload" ? (
        <SpaceBetween size="xs">
          <Box variant="small" color="text-body-secondary">
            {mode === "script" ? `One script file, or a zip holding exactly one, up to 50,000 bytes.`
              : "handler.py and its other *.py files, requirements.txt and events.json, or a zip of them (20 files, 500 KB in all)."}
          </Box>
          <Button iconName="upload" onClick={() => input.current?.click()}>
            {mode === "script" ? "Upload a script or a zip" : "Upload files or a zip"}</Button>
          <input ref={input} type="file" multiple={mode !== "script"} hidden
            aria-label={mode === "script" ? "Script file or zip" : "Code files or zip"}
            accept={mode === "script" ? ".py,.js,.ts,.zip" : ".py,.txt,.json,.zip"}
            onChange={(e) => { void upload(e.target.files); e.target.value = ""; }} />
        </SpaceBetween>
      ) : null}
      {note ? <Alert type={note.type}>{note.text}</Alert> : null}
      {git ? (
        <SpaceBetween size="s">
          <FormField label="Repository URL" description="https:// only, with no user name or password in it." errorText={errorOf(".url")}>
            <Input value={git.url ?? ""} placeholder="https://github.com/example-org/tools.git" ariaLabel="Repository URL"
              onChange={({ detail }) => setPart({ url: detail.value.trim() })} />
          </FormField>
          <FormField label="Branch, tag or commit" description="Pin a tag or a commit for a deploy that does not change by itself." errorText={errorOf(".ref")}>
            <Input value={git.ref ?? ""} placeholder="v1.2.0" ariaLabel="Branch, tag or commit"
              onChange={({ detail }) => setPart({ ref: detail.value.trim() })} />
          </FormField>
          <FormField label={mode === "script" ? "Path of the script" : "Folder"} errorText={errorOf(".path")}
            description={mode === "script" ? "The script file, relative to the repository root." : "Optional. The folder with handler.py at its root; default the repository root."}>
            <Input value={git.path ?? ""} placeholder={mode === "script" ? "scripts/run.py" : "tools/lookup"} ariaLabel={mode === "script" ? "Path of the script" : "Folder"}
              onChange={({ detail }) => setPart({ path: detail.value.trim() })} />
          </FormField>
          <FormField label="Access token name" errorText={errorOf(".token")}
            description="Optional, for a private repository: a name for the token. The token itself is saved as a build secret, never in the workflow.">
            <Input value={git.token ?? ""} placeholder="tools-repo" ariaLabel="Access token name"
              onChange={({ detail }) => setPart({ token: detail.value.trim() })} />
          </FormField>
          {git.token && buildId ? (
            <SecretField buildId={buildId} kind="gitTokens" name={git.token} label={`Access token for ${git.token}`}
              description="A token with read access to the repository, or user:token. Used only by the deploy." />
          ) : null}
        </SpaceBetween>
      ) : null}
      {s3 ? (
        <SpaceBetween size="s">
          <FormField label="S3 location" errorText={errorOf(".uri")}
            description="s3://bucket/prefix/ (every object under it) or s3://bucket/key.zip. Read with the deploy role, only at that location.">
            <Input value={s3.uri ?? ""} placeholder="s3://amzn-s3-demo-bucket/tools/lookup/" ariaLabel="S3 location"
              onChange={({ detail }) => setPart({ uri: detail.value.trim() })} />
          </FormField>
          <FormField label={mode === "script" ? "Path of the script" : "Folder"} errorText={errorOf(".path")}
            description={mode === "script" ? "The script, relative to the prefix or inside the zip." : "Optional. A folder inside the prefix or zip."}>
            <Input value={s3.path ?? ""} placeholder={mode === "script" ? "run.py" : "src"} ariaLabel={mode === "script" ? "Path of the script" : "Folder"}
              onChange={({ detail }) => setPart({ path: detail.value.trim() })} />
          </FormField>
        </SpaceBetween>
      ) : null}
      {kind === "image" ? (
        <FormField label="Container image URI" errorText={errorOf("")}
          description="An Amazon ECR image, used as it is: <account>.dkr.ecr.<region>.amazonaws.com/<repository>:<tag>.">
          <Input value={(source as { image: string }).image} ariaLabel="Container image URI"
            placeholder="111122223333.dkr.ecr.us-east-1.amazonaws.com/my-mcp:1.0"
            onChange={({ detail }) => onSource({ image: detail.value.trim() })} />
        </FormField>
      ) : null}
    </SpaceBetween>
  );
}
