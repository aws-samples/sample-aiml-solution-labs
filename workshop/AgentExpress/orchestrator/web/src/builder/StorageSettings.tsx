/** Settings > Storage: the build's own S3 bucket (workflow.json `orchestrator.storage`).
 *
 *  On writes {"create": true}, with "retain": true when the bucket is to outlive the build;
 *  off removes `orchestrator.storage`, so the workflow deploys as it did without it. Tools
 *  reach the bucket through the ${storage} token (storageCheck.ts says where). */
import Box from "@cloudscape-design/components/box";
import Checkbox from "@cloudscape-design/components/checkbox";
import Container from "@cloudscape-design/components/container";
import FormField from "@cloudscape-design/components/form-field";
import Header from "@cloudscape-design/components/header";
import SpaceBetween from "@cloudscape-design/components/space-between";
import Toggle from "@cloudscape-design/components/toggle";
import type { Project } from "./model";
import type { Issue } from "./validate";
import { STORAGE_TOKEN } from "./storageCheck";
import { useDocsBase } from "../docs/docsLinks";
import { infoSlot } from "../docs/InfoLink";

export interface StorageSetup { create?: boolean; retain?: boolean }

export function storageSetup(project: Project): StorageSetup | null {
  const orch = (project.workflow.orchestrator ?? {}) as Record<string, unknown>;
  const st = orch.storage;
  return st && typeof st === "object" && !Array.isArray(st) ? st as StorageSetup : null;
}

export function withStorage(project: Project, setup: StorageSetup | null): Project {
  const orch = { ...((project.workflow.orchestrator ?? {}) as Record<string, unknown>) };
  if (setup) orch.storage = setup;
  else delete orch.storage;
  return { ...project, workflow: { ...project.workflow, orchestrator: orch } as Project["workflow"] };
}

export function StorageSettings({ project, setProject, issues = [] }: {
  project: Project; setProject: (p: Project) => void; issues?: Issue[];
}) {
  const docs = useDocsBase();
  const setup = storageSetup(project);
  const on = setup?.create === true;
  const err = (k: string) => issues.filter((i) => i.path === `orchestrator.storage.${k}`).map((i) => i.message).join(" ") || undefined;
  return (
    <Container header={<Header variant="h2"
      description="A private S3 bucket the deploy creates for this build, for the tools that read or write its files."
      info={infoSlot(docs, "settings.storage")}>
      Storage</Header>}>
      <SpaceBetween size="m">
        <FormField errorText={err("create")}>
          <Toggle checked={on}
            onChange={({ detail }) => setProject(withStorage(project, detail.checked
              ? { create: true, ...(setup?.retain ? { retain: true } : {}) } : null))}>
            Create storage for this build</Toggle>
        </FormField>
        {on ? (
          <SpaceBetween size="m">
            <FormField errorText={err("retain")}>
              <Checkbox checked={setup?.retain === true}
                onChange={({ detail }) => setProject(withStorage(project, detail.checked ? { create: true, retain: true } : { create: true }))}
                description="Off: destroying the build empties the bucket and deletes it. On: the bucket and its files stay, encrypted with S3-managed keys, and a new deploy of this build needs it deleted or imported first.">
                Keep it when the build is destroyed</Checkbox>
            </FormField>
            <Box variant="small" color="text-body-secondary" data-testid="storage-token">
              Tools reach it as <Box variant="code">{STORAGE_TOKEN}</Box>: for example <Box variant="code">{`${STORAGE_TOKEN}/templates/`}</Box> as
              a code tool&apos;s S3 grant, or <Box variant="code">{`${STORAGE_TOKEN}/kb/`}</Box> as a knowledge base&apos;s documents.
              Its name is the build&apos;s name, then -storage- and the account. Private, HTTPS only, versioned, encrypted.
            </Box>
          </SpaceBetween>
        ) : null}
      </SpaceBetween>
    </Container>
  );
}
