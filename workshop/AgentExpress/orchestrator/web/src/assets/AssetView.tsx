/** An agent's asset, rendered with Cloudscape components and no knowledge of any
 *  particular contract. All the decisions live in shape.ts; this file only maps them
 *  onto components. */

import Badge from "@cloudscape-design/components/badge";
import Box from "@cloudscape-design/components/box";
import ColumnLayout from "@cloudscape-design/components/column-layout";
import ExpandableSection from "@cloudscape-design/components/expandable-section";
import Link from "@cloudscape-design/components/link";
import SpaceBetween from "@cloudscape-design/components/space-between";
import StatusIndicator from "@cloudscape-design/components/status-indicator";
import Spinner from "@cloudscape-design/components/spinner";
import TextContent from "@cloudscape-design/components/text-content";
import { useEffect, useState } from "react";
import { api } from "../api";

import {
  type Json, type Section, claimParts, gistOf, humanizeKey, isClaimList, isObject,
  isVocabToken, KNOWN_CLASSIFICATIONS, looksLikeMarkdown, readAsset, safeUrl, sectionsOf,
} from "./shape";
import { Markdown } from "../builder/markdown";

/** Cloudscape's badge colours. An unrecognised classification gets the neutral one
 *  rather than being coerced to a known value. */
function badgeColor(value: string): "blue" | "green" | "grey" | "red" | "severity-medium" {
  if (!KNOWN_CLASSIFICATIONS.has(value)) {
    if (/^high$/i.test(value)) return "green";
    if (/^(medium|med)$/i.test(value)) return "severity-medium";
    if (/^low$/i.test(value)) return "red";
    return "grey";
  }
  switch (value) {
    case "sourced-fact": return "green";
    case "calculation": return "blue";
    case "assumption": return "severity-medium";
    default: return "grey";
  }
}

function Scalar({ value }: { value: Json }) {
  if (typeof value === "boolean") {
    return <StatusIndicator type={value ? "success" : "stopped"}>{String(value)}</StatusIndicator>;
  }
  if (typeof value === "number") return <Box variant="span">{String(value)}</Box>;
  const url = safeUrl(value);
  if (url) {
    return (
      <Link href={url} external target="_blank">
        {url}
      </Link>
    );
  }
  if (isVocabToken(value)) return <Badge color={badgeColor(String(value))}>{String(value)}</Badge>;
  // A drafted post or report body: headings, lists, bold. Rendered as React elements,
  // never as HTML, so nothing in the model's text becomes markup.
  if (typeof value === "string" && looksLikeMarkdown(value)) return <Markdown text={value} />;
  if (typeof value === "string" && value.includes("\n")) {
    return <Box variant="span"><span style={{ whiteSpace: "pre-wrap" }}>{value}</span></Box>;
  }
  return <Box variant="span">{String(value)}</Box>;
}

/** An image an image agent rendered (app/common/images.py): the asset holds its key, and
 *  the BFF answers with a short-lived link — only for the run's owner. */
export function GeneratedImage({ img }: { img: Record<string, Json> }) {
  const [url, setUrl] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const key = String(img.imageKey);
  useEffect(() => {
    let live = true;
    api.get<{ url: string }>(`/api/images?key=${encodeURIComponent(key)}`)
      .then((r) => { if (live) setUrl(r.url); })
      .catch((e: Error) => { if (live) setError(e.message); });
    return () => { live = false; };
  }, [key]);
  const alt = String(img.caption ?? `Image rendered by ${String(img.model ?? "the image model")}`);
  if (error) return <StatusIndicator type="error">Could not load the image: {error}</StatusIndicator>;
  if (!url) return <Spinner />;
  return (
    <SpaceBetween size="xxs">
      <a href={url} target="_blank" rel="noopener noreferrer">
        <img src={url} alt={alt} style={{ maxWidth: "100%", maxHeight: 420, borderRadius: 8, display: "block" }} />
      </a>
      <Box variant="small" color="text-body-secondary">
        {String(img.model ?? "")}{img.seed !== undefined && img.seed !== null ? ` · seed ${String(img.seed)}` : ""}
      </Box>
    </SpaceBetween>
  );
}

function ValueView({ value, depth = 0 }: { value: Json; depth?: number }) {
  if (value == null) return <Box variant="span" color="text-status-inactive">—</Box>;

  if (Array.isArray(value)) {
    if (value.length === 0) return <Box variant="span" color="text-status-inactive">—</Box>;
    if (value.every((v) => typeof v === "string" && isVocabToken(v))) {
      return (
        <SpaceBetween direction="horizontal" size="xxs">
          {value.map((v, i) => <Badge key={i} color={badgeColor(String(v))}>{String(v)}</Badge>)}
        </SpaceBetween>
      );
    }
    return (
      <TextContent>
        <ul>
          {value.map((v, i) => (
            <li key={i}>
              {isObject(v) && typeof v.imageKey === "string" ? <GeneratedImage img={v} />
                : isObject(v) ? <ObjectView obj={v} depth={depth + 1} /> : <Scalar value={v} />}
            </li>
          ))}
        </ul>
      </TextContent>
    );
  }

  if (isObject(value) && typeof value.imageKey === "string") return <GeneratedImage img={value} />;
  if (isObject(value)) return <ObjectView obj={value} depth={depth} />;
  return <Scalar value={value} />;
}

function ObjectView({ obj, depth }: { obj: Record<string, Json>; depth: number }) {
  const entries = Object.entries(obj).filter(([, v]) => v != null && v !== "");
  if (entries.length === 0) return <Box variant="span" color="text-status-inactive">—</Box>;
  // Beyond two levels a nested object becomes unreadable inline, so it collapses.
  if (depth >= 2) {
    return (
      <ExpandableSection headerText={`${entries.length} field${entries.length === 1 ? "" : "s"}`}>
        <Rows entries={entries} depth={depth} />
      </ExpandableSection>
    );
  }
  return <Rows entries={entries} depth={depth} />;
}

function Rows({ entries, depth }: { entries: Array<[string, Json]>; depth: number }) {
  return (
    <SpaceBetween size="xs">
      {entries.map(([k, v]) => (
        <div key={k}>
          <Box variant="awsui-key-label">{humanizeKey(k)}</Box>
          <ValueView value={v} depth={depth + 1} />
        </div>
      ))}
    </SpaceBetween>
  );
}

function ClaimList({ items }: { items: Array<Record<string, Json>> }) {
  return (
    <SpaceBetween size="m">
      {items.map((item, i) => {
        const { text, badges, refs, rest } = claimParts(item);
        return (
          <div key={i}>
            <SpaceBetween size="xxs">
              <SpaceBetween direction="horizontal" size="xxs">
                {badges.map((b) => (
                  <Badge key={b.key} color={badgeColor(b.value)}>{b.value}</Badge>
                ))}
              </SpaceBetween>
              {text ? <Box variant="p">{text}</Box> : null}
              {refs.length > 0 ? (
                <Box variant="small" color="text-status-inactive">
                  {refs.join(" · ")}
                </Box>
              ) : null}
              {rest.length > 0 ? (
                <ColumnLayout columns={2} variant="text-grid">
                  {rest.map((r) => (
                    <div key={r.key}>
                      <Box variant="awsui-key-label">{r.label}</Box>
                      <ValueView value={r.value} depth={1} />
                    </div>
                  ))}
                </ColumnLayout>
              ) : null}
            </SpaceBetween>
          </div>
        );
      })}
    </SpaceBetween>
  );
}

function SectionView({ section }: { section: Section }) {
  const body = section.claims
    ? <ClaimList items={section.value as Array<Record<string, Json>>} />
    : <ValueView value={section.value} />;
  const count = Array.isArray(section.value) ? section.value.length : undefined;
  return (
    <ExpandableSection
      variant="footer"
      defaultExpanded={!section.uncertain}
      headerText={section.label}
      headerCounter={count === undefined ? undefined : `(${count})`}
    >
      {body}
    </ExpandableSection>
  );
}

/** The public component. `text` is whatever the agent returned. */
export function AssetView({ text }: { text: string }) {
  const { asset, cutOff } = readAsset(text);
  if (!asset) {
    // Not JSON: a plain-text agent is legitimate. Show it as written.
    return text
      ? <Box variant="p"><span style={{ whiteSpace: "pre-wrap" }}>{text}</span></Box>
      : <Box color="text-status-inactive">No output yet.</Box>;
  }
  const gist = gistOf(asset);
  const sections = sectionsOf(asset);
  const type = typeof asset.assetType === "string" ? asset.assetType : null;
  const version = asset.version;

  return (
    <SpaceBetween size="m">
      {cutOff ? (
        <StatusIndicator type="warning">
          This output was cut off: the agent ran out of its maxTokens. What it wrote is shown; raise the agent&apos;s maxTokens for the rest.
        </StatusIndicator>
      ) : null}
      {type || version != null ? (
        <Box variant="small" color="text-status-inactive">
          {[type ? humanizeKey(type) : null,
            version != null ? `v${String(version)}` : null,
            typeof asset.status === "string" ? asset.status : null,
          ].filter(Boolean).join(" · ")}
        </Box>
      ) : null}
      {gist ? (
        <Box variant="p" padding={{ left: "s" }}>
          <div style={{ borderLeft: "3px solid #0972d3", paddingLeft: 12 }}>{gist}</div>
        </Box>
      ) : null}
      {sections.map((s) => <SectionView key={s.key} section={s} />)}
      {sections.length === 0 && !gist ? (
        <Box color="text-status-inactive">This asset has no readable body.</Box>
      ) : null}
    </SpaceBetween>
  );
}

export { isClaimList };
