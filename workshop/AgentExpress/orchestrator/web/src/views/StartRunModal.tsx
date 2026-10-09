/** Starting a run, as a console Modal with a Form.
 *
 *  A run needs one thing: the request — what the workflow should work on, in as much
 *  detail as the user likes (a line, or pages of instructions). So that is the form: a
 *  large text box and Start. The workflow picker appears only when there is more than
 *  one to choose from, and the memory subject sits under Advanced. Every label,
 *  placeholder and hint comes from workflow.json `ui`, so a customer's wording appears
 *  here without a code change.
 *
 *  When some agent reads a run's files (`attachments` in workflow.json), the form also
 *  takes files — uploaded the moment they are picked — and says which S3 locations the
 *  request may name as s3:// paths, like the AgentExpress Assistant's chat. */

import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import ExpandableSection from "@cloudscape-design/components/expandable-section";
import FileInput from "@cloudscape-design/components/file-input";
import FileTokenGroup from "@cloudscape-design/components/file-token-group";
import Form from "@cloudscape-design/components/form";
import FormField from "@cloudscape-design/components/form-field";
import Input from "@cloudscape-design/components/input";
import Modal from "@cloudscape-design/components/modal";
import Select from "@cloudscape-design/components/select";
import SpaceBetween from "@cloudscape-design/components/space-between";
import Textarea from "@cloudscape-design/components/textarea";
import { useEffect, useState } from "react";

import { uploadRunFile } from "../api";
import { REQUEST_MAX } from "../lib/request";
import type { RunAttach, UiConfig } from "../types";

/** A file picked for the run, uploaded as soon as it is picked. */
interface Picked { file: File; key?: string; name?: string; loading: boolean; error?: string }

/** What a run can be started against: this deployment's own workflow (value "") or a
 *  deployed Builder build (its id). */
export interface RunTarget {
  value: string;
  label: string;
  description?: string;
}

export function StartRunModal({
  visible, ui, onDismiss, onStart, targets = [], target = "", onTarget, attach = null,
  upload = uploadRunFile,
}: {
  visible: boolean;
  ui: UiConfig;
  onDismiss: () => void;
  onStart: (topic: string, subject: string, files: { key: string; name: string }[]) => Promise<void>;
  /** What the run may bring (the workflow view's `attachments`); null: no files. */
  attach?: RunAttach | null;
  /** How a picked file is uploaded (a test stands in for S3). */
  upload?: (file: File) => Promise<{ key: string; name: string }>;
  /** More than one means there are deployed builds to choose between. */
  targets?: RunTarget[];
  target?: string;
  onTarget?: (value: string) => void;
}) {
  const [topic, setTopic] = useState("");
  const [subject, setSubject] = useState("");
  const [busy, setBusy] = useState(false);
  const [files, setFiles] = useState<Picked[]>([]);
  const uploading = files.some((f) => f.loading);
  const failed = files.some((f) => f.error);
  const pick = (picked: File[]) => {
    const room = Math.max(0, (attach?.maxFiles ?? 5) - files.length);
    const next = picked.slice(0, room);
    if (!next.length) return;
    setFiles((cur) => [...cur, ...next.map((file) => ({ file, loading: true }))]);
    for (const file of next) {
      upload(file).then(
        (got) => setFiles((cur) => cur.map((f) => (f.file === file ? { ...f, ...got, loading: false } : f))),
        (e) => setFiles((cur) => cur.map((f) => (f.file === file ? { ...f, loading: false, error: (e as Error).message } : f))));
    }
  };

  // Seed the default request when the modal opens, not on every render, so a reader's
  // edit is not overwritten.
  useEffect(() => {
    if (visible) setTopic(ui.defaultTopic ?? "");
    if (visible) setFiles([]);
  }, [visible, ui.defaultTopic, target]);
  const picked = targets.find((t) => t.value === target) ?? targets[0];
  const tooLong = topic.length > REQUEST_MAX;
  const ready = Boolean(topic.trim()) && !tooLong && !busy && !uploading && !failed;

  const start = async () => {
    if (!ready) return;
    setBusy(true);
    const uploaded = files.filter((f) => f.key).map((f) => ({ key: f.key!, name: f.name ?? f.file.name }));
    try { await onStart(topic.trim(), subject.trim(), uploaded); } finally { setBusy(false); }
  };

  return (
    <Modal
      visible={visible}
      onDismiss={onDismiss}
      size="large"
      header="Start run"
      footer={
        <Box float="right">
          <SpaceBetween direction="horizontal" size="xs">
            <Button variant="link" onClick={onDismiss} disabled={busy}>Cancel</Button>
            <Button variant="primary" loading={busy} disabled={!ready} onClick={() => void start()}>
              Start run
            </Button>
          </SpaceBetween>
        </Box>
      }
    >
      <Form variant="embedded">
        <SpaceBetween size="l">
          {targets.length > 1 && picked ? (
            <FormField label="Workflow" description="Which deployed workflow runs this request.">
              <Select
                selectedOption={picked}
                options={targets}
                onChange={({ detail }) => onTarget?.(detail.selectedOption.value ?? "")}
              />
            </FormField>
          ) : null}
          <FormField
            label="Request"
            description="What the workflow should work on — as much detail and as many instructions as you like. Every agent in the run reads it."
            constraintText={`${topic.length.toLocaleString()} / ${REQUEST_MAX.toLocaleString()} characters · Ctrl+Enter or ⌘+Enter starts the run`}
            errorText={tooLong ? `At most ${REQUEST_MAX.toLocaleString()} characters` : undefined}
            stretch
          >
            <Textarea
              value={topic}
              rows={12}
              placeholder={ui.topicPlaceholder ?? "What should the workflow work on?"}
              ariaLabel="Request"
              onChange={({ detail }) => setTopic(detail.value)}
              onKeyDown={({ detail }) => {
                if (detail.key === "Enter" && (detail.ctrlKey || detail.metaKey)) void start();
              }}
              autoFocus
            />
          </FormField>
          {attach ? (
            <FormField
              label="Files (optional)"
              description={
                <>
                  Attach files for the agents to read, or refer to the S3 path where your files reside
                  (s3://bucket/file, or s3://bucket/folder/ for the files in it) in the request.
                  {" "}Up to {attach.maxFiles} files, {(attach.maxBytes / 1e6).toFixed(1)} MB each.
                  {attach.s3.length ? <> S3 paths can be in: {attach.s3.join(", ")}.</> : null}
                </>
              }
              errorText={failed ? "A file did not upload: remove it, or pick it again" : undefined}
              stretch
            >
              <SpaceBetween size="xs">
                <FileInput multiple value={[]} ariaLabel="Attach files"
                  accept={attach.types.map((t) => `.${t}`).join(",")}
                  onChange={({ detail }) => pick(detail.value)}>Attach files</FileInput>
                {files.length ? (
                  <FileTokenGroup alignment="horizontal" showFileSize
                    items={files.map((f) => ({ file: f.file, loading: f.loading, errorText: f.error ?? null }))}
                    onDismiss={({ detail }) => setFiles((cur) => cur.filter((_, i) => i !== detail.fileIndex))}
                    i18nStrings={{ removeFileAriaLabel: (_i, n) => `Remove ${n}`, errorIconAriaLabel: "Error",
                      limitShowFewer: "Show fewer", limitShowMore: "Show more",
                      formatFileSize: (b) => `${(b / 1e6).toFixed(2)} MB` }} />
                ) : null}
              </SpaceBetween>
            </FormField>
          ) : null}
          <ExpandableSection headerText="Advanced">
            <FormField
              label="Subject (optional)"
              description={
                ui.subjectHint
                ?? "Scopes long-term memory, so agents recall insights from earlier runs on the same subject."
              }
              stretch
            >
              <Input
                value={subject}
                placeholder={ui.subjectPlaceholder ?? "A customer, product or project"}
                onChange={({ detail }) => setSubject(detail.value)}
              />
            </FormField>
          </ExpandableSection>
        </SpaceBetween>
      </Form>
    </Modal>
  );
}
