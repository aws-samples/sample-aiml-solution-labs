import Box from "@cloudscape-design/components/box";
import Button from "@cloudscape-design/components/button";
import Link from "@cloudscape-design/components/link";
import Popover from "@cloudscape-design/components/popover";
import SpaceBetween from "@cloudscape-design/components/space-between";
import type { ReactNode } from "react";

import { DOCS, docsHref, useDocsBase, type DocsFeature } from "./docsLinks";

/** A feature's info icon: a popover with the feature's name, what it is, and a
 *  Documentation link to its section (docsLinks.ts). Renders nothing when the deployment
 *  sets no documentation URL.
 *
 *  Click (or Enter / Space on the focused icon) opens it: Cloudscape's Popover has no
 *  hover trigger, and a link inside a hover-only tooltip is out of reach of a keyboard
 *  or a screen reader. */
export function InfoLink({ feature }: { feature: DocsFeature }) {
  const base = useDocsBase();
  if (!base) return null;
  const { title, description } = DOCS[feature];
  return (
    <span className="ax-docs-info" data-docs-feature={feature} style={{ marginInlineStart: 4 }}>
      <Popover
        header={title} size="small" triggerType="custom" dismissButton dismissAriaLabel="Close"
        // In a portal, so a neighbouring panel (or a clipping container) never covers it.
        renderWithPortal
        content={(
          <SpaceBetween size="xs">
            <Box variant="p">{description}</Box>
            <Link href={docsHref(base, feature)} external target="_blank"
              ariaLabel={`Documentation: ${title} (opens in a new tab)`}>Documentation</Link>
          </SpaceBetween>
        )}
      >
        <Button variant="inline-icon" iconName="status-info" ariaLabel={`About ${title}`} />
      </Popover>
    </span>
  );
}

/** For a component's `info` / `headerInfo` slot: the info icon, or undefined without a
 *  documentation URL, so the slot is not filled at all (a filled slot changes the layout
 *  even when what fills it renders nothing). `base` is useDocsBase(). */
export function infoSlot(base: string, feature: DocsFeature): ReactNode {
  return base ? <InfoLink feature={feature} /> : undefined;
}

/** Several features' info icons in one line, each after its name: for a page whose
 *  sections are not React (the Observability island). Nothing without a documentation URL. */
export function InfoStrip({ features }: { features: DocsFeature[] }) {
  const base = useDocsBase();
  if (!base) return null;
  return (
    <Box padding={{ bottom: "s" }} data-testid="docs-strip">
      <SpaceBetween direction="horizontal" size="m" alignItems="center">
        {features.map((f) => (
          <span key={f}>
            <Box variant="span" fontSize="body-s" color="text-body-secondary">{DOCS[f].title}</Box>{" "}
            <InfoLink feature={f} />
          </span>
        ))}
      </SpaceBetween>
    </Box>
  );
}
