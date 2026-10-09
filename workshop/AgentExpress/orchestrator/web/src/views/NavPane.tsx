/** The left pane: Build, Runs and Observability, each listing what it holds.
 *
 *  WHY NOT Cloudscape's SideNavigation. It was, until each group needed a control on
 *  its heading — "+" to start a build or a run — and each entry a delete button.
 *  SideNavigation's expandable group has no slot for either (only a plain link has an
 *  `info` slot, and a heading is not a plain link). So this is the same shape built
 *  from Cloudscape parts and design tokens: headings you can expand, entries that are
 *  links, and icon buttons beside them with their own accessible names. */

import Badge from "@cloudscape-design/components/badge";
import Button from "@cloudscape-design/components/button";
import Icon from "@cloudscape-design/components/icon";
import TextFilter from "@cloudscape-design/components/text-filter";
import StatusIndicator, { type StatusIndicatorProps } from "@cloudscape-design/components/status-indicator";
import { useState, type MouseEvent, type ReactNode } from "react";

import "./nav.css";

export interface NavEntry {
  href: string;
  text: string;
  /** Full text, when `text` is truncated. */
  title?: string;
  status?: { type: StatusIndicatorProps.Type; label: string };
  onDelete?: () => void;
  deleteLabel?: string;
}

export interface NavGroup {
  id: string;
  text: string;
  href: string;
  badge?: string;
  onAdd?: () => void;
  addLabel?: string;
  entries: NavEntry[];
  empty?: string;
  /** Rendered between the heading and the entries — the Runs group's workflow picker. */
  extra?: ReactNode;
}

export interface NavLink {
  href: string;
  text: string;
  external?: boolean;
  /** Under a NavHeading: indented beneath it. */
  indent?: boolean;
}
/** A line between two sections of links. */
export const NAV_DIVIDER = { divider: true } as const;
/** A small heading over the links after it (the library's groups: Agent, Gateway...). */
export interface NavHeading { heading: string }
export type NavItem = NavLink | typeof NAV_DIVIDER | NavHeading;
/** A group with more entries than this gets a filter box above its list. */
export const FILTER_FROM = 6;

function Group({ group, active, onFollow }: {
  group: NavGroup; active: string; onFollow: (href: string) => void;
}) {
  const [open, setOpen] = useState(true);
  const [query, setQuery] = useState("");
  const q = query.trim().toLowerCase();
  const shown = q ? group.entries.filter((e) => `${e.text} ${e.title ?? ""}`.toLowerCase().includes(q)) : group.entries;
  const follow = (href: string) => (e: MouseEvent) => { e.preventDefault(); onFollow(href); };
  const listId = `nav-${group.id}`;
  return (
    <li className="axn-group">
      <div className={`axn-head${active === group.href ? " axn-active" : ""}`}>
        <button
          type="button" className="axn-caret" aria-expanded={open} aria-controls={listId}
          aria-label={`${open ? "Collapse" : "Expand"} ${group.text}`} onClick={() => setOpen((o) => !o)}
        >
          <Icon name={open ? "caret-down-filled" : "caret-right-filled"} />
        </button>
        <a href={group.href} className="axn-head-link" onClick={follow(group.href)}>{group.text}</a>
        {group.badge ? <Badge color="blue">{group.badge}</Badge> : null}
        {group.onAdd ? (
          <span className="axn-actions">
            <Button variant="icon" iconName="add-plus" ariaLabel={group.addLabel ?? `New ${group.text}`}
              onClick={group.onAdd} />
          </span>
        ) : null}
      </div>
      {open && group.extra ? <div className="axn-extra">{group.extra}</div> : null}
      {open && group.entries.length > FILTER_FROM ? (
        <div className="axn-filter">
          <TextFilter filteringText={query} filteringPlaceholder={`Find ${group.text.toLowerCase()}`}
            filteringAriaLabel={`Find in ${group.text}`} countText={q ? `${shown.length} of ${group.entries.length}` : undefined}
            onChange={({ detail }) => setQuery(detail.filteringText)} />
        </div>
      ) : null}
      {open ? (
        <ul id={listId} className="axn-entries">
          {group.entries.length === 0 && group.empty
            ? <li className="axn-empty">{group.empty}</li> : null}
          {q && !shown.length ? <li className="axn-empty">Nothing matches “{query.trim()}”.</li> : null}
          {shown.map((e) => (
            <li key={e.href} className={`axn-entry${active === e.href ? " axn-active" : ""}`}>
              <a href={e.href} className="axn-entry-link" title={e.title ?? e.text} onClick={follow(e.href)}
                aria-current={active === e.href ? "page" : undefined}>
                {e.text}
              </a>
              <span className="axn-actions">
                {e.status ? <StatusIndicator type={e.status.type} iconAriaLabel={e.status.label} /> : null}
                {e.onDelete ? (
                  <Button variant="icon" iconName="remove" ariaLabel={e.deleteLabel ?? `Delete ${e.text}`}
                    onClick={e.onDelete} />
                ) : null}
              </span>
            </li>
          ))}
        </ul>
      ) : null}
    </li>
  );
}

export function NavPane({ heading, homeHref, groups, links, active, onFollow }: {
  heading: ReactNode;
  homeHref: string;
  groups: NavGroup[];
  links: NavItem[];
  active: string;
  onFollow: (href: string) => void;
}) {
  return (
    <nav className="axn" aria-label="Navigation">
      <div className="axn-header">
        <a href={homeHref} onClick={(e) => { e.preventDefault(); onFollow(homeHref); }}>{heading}</a>
      </div>
      <ul className="axn-groups">
        {groups.map((g) => <Group key={g.id} group={g} active={active} onFollow={onFollow} />)}
      </ul>
      <hr className="axn-divider" />
      <ul className="axn-links">
        {links.map((item, i) => ("divider" in item ? (
          <li key={`divider-${i}`} className="axn-divider-li" role="separator"><hr className="axn-divider" /></li>
        ) : "heading" in item ? (
          <li key={`heading-${item.heading}`} className="axn-heading">{item.heading}</li>
        ) : (
          <li key={item.href} className={item.indent ? "axn-indent" : undefined}>
            <a href={item.href}
              {...(item.external ? { target: "_blank", rel: "noopener noreferrer" }
                : { onClick: (e: MouseEvent) => { e.preventDefault(); onFollow(item.href); } })}>
              {item.text}
              {item.external ? <span className="axn-ext"><Icon name="external" ariaLabel="Opens in a new tab" /></span> : null}
            </a>
          </li>
        )))}
      </ul>
    </nav>
  );
}
