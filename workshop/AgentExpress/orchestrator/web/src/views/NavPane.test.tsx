/** The left pane's controls: + on a heading, delete on an entry, and collapse.
 *
 *  Each is an icon button, so what a screen-reader user (and this test) finds it by is
 *  its accessible name — "New build", "Start a new run", "Delete run …". If those names
 *  went missing the buttons would still render and nobody could say which was which. */

import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { NavPane, type NavGroup } from "./NavPane";

function groups(over: Partial<Record<string, () => void>> = {}): NavGroup[] {
  return [
    { id: "build", text: "Build", href: "#build", onAdd: over.addBuild, addLabel: "New build",
      entries: [{ href: "#build:p1", text: "Claims", onDelete: over.delBuild, deleteLabel: "Delete build Claims" }] },
    { id: "runs", text: "Runs", href: "#runs", onAdd: over.addRun, addLabel: "Start a new run",
      entries: [{ href: "#run:abc", text: "Triage a claim", status: { type: "success", label: "Succeeded" },
        onDelete: over.delRun, deleteLabel: "Delete run Triage a claim" }] },
    { id: "obs", text: "Observability", href: "#obs",
      entries: [{ href: "#obs:abc", text: "Triage a claim" }] },
  ];
}

const pane = (g: NavGroup[], onFollow = vi.fn(), active = "#run:abc") =>
  render(<NavPane heading="AgentExpress" homeHref="#runs" groups={g} links={[]} active={active} onFollow={onFollow} />);

describe("NavPane", () => {
  it("puts + on the Build and Runs headings, and nowhere else", () => {
    const addBuild = vi.fn();
    const addRun = vi.fn();
    pane(groups({ addBuild, addRun }));
    fireEvent.click(screen.getByRole("button", { name: "New build" }));
    fireEvent.click(screen.getByRole("button", { name: "Start a new run" }));
    expect(addBuild).toHaveBeenCalledOnce();
    expect(addRun).toHaveBeenCalledOnce();
    expect(screen.queryByRole("button", { name: /New Observability/ })).toBeNull();
  });

  it("deletes the entry the button sits beside, without following its link", () => {
    const delRun = vi.fn();
    const delBuild = vi.fn();
    const onFollow = vi.fn();
    pane(groups({ delRun, delBuild }), onFollow);
    fireEvent.click(screen.getByRole("button", { name: "Delete run Triage a claim" }));
    fireEvent.click(screen.getByRole("button", { name: "Delete build Claims" }));
    expect(delRun).toHaveBeenCalledOnce();
    expect(delBuild).toHaveBeenCalledOnce();
    expect(onFollow).not.toHaveBeenCalled();
    // Observability lists the same run with no delete of its own: one action, one button.
    expect(screen.getAllByRole("button", { name: /^Delete run/ })).toHaveLength(1);
  });

  it("follows an entry, marks the active one, and collapses a group", () => {
    const onFollow = vi.fn();
    pane(groups(), onFollow, "#run:abc");
    const [runLink] = screen.getAllByRole("link", { name: "Triage a claim" });
    expect(runLink.getAttribute("aria-current")).toBe("page");
    fireEvent.click(runLink);
    expect(onFollow).toHaveBeenCalledWith("#run:abc");
    fireEvent.click(screen.getByRole("button", { name: "Collapse Build" }));
    expect(screen.queryByRole("link", { name: "Claims" })).toBeNull();
  });
});

describe("a long group", () => {
  it("gets a filter box that narrows its entries", async () => {
    const { fireEvent, render, screen } = await import("@testing-library/react");
    const { NavPane } = await import("./NavPane");
    const entries = Array.from({ length: 9 }, (_, i) => ({ href: `#build:b${i}`, text: i === 4 ? "Claims triage" : `Build ${i}` }));
    render(<NavPane heading="H" homeHref="#" active="" onFollow={() => {}} links={[]}
      groups={[{ id: "build", text: "Build", href: "#build", entries }]} />);
    const box = screen.getByRole("searchbox", { name: "Find in Build" });
    fireEvent.change(box, { target: { value: "claims" } });
    expect(screen.getByText("Claims triage")).toBeTruthy();
    expect(screen.queryByText("Build 1")).toBeNull();
    fireEvent.change(box, { target: { value: "zzz" } });
    expect(screen.getByText(/Nothing matches/)).toBeTruthy();
  });
});
