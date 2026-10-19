/** Teams in the console (bff/teams.py): the Teams page, the workspace switcher, and Move to
 *  team. The server enforces every rule; these check what each person is offered and what
 *  is sent. */
import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

const calls: { method: string; path: string; body?: unknown }[] = [];
let teams: unknown[] = [];
vi.mock("../api", () => {
  const answer = (method: string, path: string, body?: unknown) => {
    calls.push({ method, path, body });
    if (method === "GET" && path.startsWith("/api/teams")) return Promise.resolve(teams);
    if (method === "GET" && path.startsWith("/api/builds")) return Promise.resolve([]);
    if (method === "PUT" && path.endsWith("/team")) return Promise.resolve({ id: "b1", name: "B", updated: "", ...(body as object) });
    return Promise.resolve({ ok: true, ...(body as object) });
  };
  return {
    api: {
      get: (p: string) => answer("GET", p),
      post: (p: string, b?: unknown) => answer("POST", p, b),
      put: (p: string, b?: unknown) => answer("PUT", p, b),
      del: (p: string) => answer("DELETE", p),
    },
  };
});

import { MoveToTeam, TeamsPage, WorkspaceSelect, membersLabel, type Team } from "./Teams";
import { currentWorkspace, serverStore, setWorkspace } from "./storage";

const CLAIMS: Team = { id: "aaaa0001", name: "Claims", role: "owner",
  members: [{ email: "o@example.com", role: "owner" }, { email: "e@example.com", role: "editor" }], groups: [] };
const OPS: Team = { id: "aaaa0002", name: "Ops", role: "viewer", members: [], groups: [{ name: "ops", role: "viewer" }] };
const notify = vi.fn();

beforeEach(() => { calls.length = 0; teams = []; notify.mockReset(); setWorkspace(""); });

describe("membersLabel", () => {
  it("counts people and groups", () => {
    expect(membersLabel(CLAIMS)).toBe("2 members");
    expect(membersLabel(OPS)).toBe("0 members · 1 group");
  });
});

describe("the Teams page", () => {
  it("lets an admin create a team, and reads every team", async () => {
    render(<TeamsPage admin notify={notify} />);
    await waitFor(() => expect(calls.some((c) => c.path === "/api/teams?scope=all")).toBe(true));
    fireEvent.click(screen.getByRole("button", { name: "New team" }));
    fireEvent.change(screen.getByLabelText("Team name"), { target: { value: "Claims" } });
    fireEvent.click(screen.getByRole("button", { name: "Add person" }));
    fireEvent.change(screen.getByLabelText("Person 1"), { target: { value: "o@example.com" } });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Save" })); });
    const post = calls.find((c) => c.method === "POST");
    // An admin's first person is an owner by default.
    expect(post).toEqual({ method: "POST", path: "/api/teams",
      body: { name: "Claims", members: [{ email: "o@example.com", role: "owner" }], groups: [] } });
  });

  it("shows no New team to someone who is not an admin, and keeps an owner's owners locked", async () => {
    teams = [CLAIMS];
    render(<TeamsPage admin={false} notify={notify} />);
    await screen.findByText("Claims");
    expect(screen.queryByRole("button", { name: "New team" })).toBeNull();
    fireEvent.click(screen.getByText("Claims"));
    // The owner row cannot be removed by a team owner; the editor row can.
    expect(screen.getByRole("button", { name: "Remove o@example.com" })).toHaveProperty("disabled", true);
    expect(screen.getByRole("button", { name: "Remove e@example.com" })).toHaveProperty("disabled", false);
    fireEvent.click(screen.getByRole("button", { name: "Remove e@example.com" }));
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Save" })); });
    const put = calls.find((c) => c.method === "PUT");
    // No name: only an admin renames.
    expect(put).toEqual({ method: "PUT", path: "/api/teams/aaaa0001",
      body: { members: [{ email: "o@example.com", role: "owner" }], groups: [] } });
  });

  it("shows a viewer who is in the team, without anything to change", async () => {
    teams = [OPS];
    render(<TeamsPage admin={false} notify={notify} />);
    fireEvent.click(await screen.findByText("Ops"));
    expect(screen.queryByRole("button", { name: "Save" })).toBeNull();
    expect(screen.getByText("group ops")).toBeTruthy();
  });
});

describe("the workspace switcher", () => {
  it("is not there for someone in no team", async () => {
    const { container } = render(<WorkspaceSelect />);
    await waitFor(() => expect(calls.length).toBe(1));
    expect(container.innerHTML).toBe("");
  });

  it("switches the builds list, and a new build is created in the team", async () => {
    teams = [CLAIMS];
    const { container } = render(<WorkspaceSelect />);
    await waitFor(() => expect(createWrapper(container).findSelect()).toBeTruthy());
    const select = createWrapper(container).findSelect()!;
    select.openDropdown();
    select.selectOptionByValue("aaaa0001");
    expect(currentWorkspace()).toBe("aaaa0001");
    await serverStore.list();
    expect(calls.at(-1)?.path).toBe("/api/builds?team=aaaa0001");
    await serverStore.save({ id: "p1", name: "P", createdAt: "", updatedAt: "", prompts: {}, workflow: {} as never });
    expect((calls.at(-1)?.body as { team?: string }).team).toBe("aaaa0001");
    setWorkspace("");
    await serverStore.list();
    expect(calls.at(-1)?.path).toBe("/api/builds");
  });
});

describe("Move to team", () => {
  it("offers only the teams the caller edits in, and sends the choice", async () => {
    teams = [CLAIMS, OPS];
    const onMoved = vi.fn();
    render(<MoveToTeam build={{ id: "b1", name: "B", updatedAt: "" }} notify={notify} onDismiss={() => {}} onMoved={onMoved} />);
    const w = createWrapper(document.body);
    await waitFor(() => expect(calls.some((c) => c.path === "/api/teams")).toBe(true));
    const select = w.findSelect('[aria-label="Move to"]') ?? w.findSelect()!;
    select.openDropdown();
    const labels = select.findDropdown().findOptions().map((o) => o.getElement().textContent);
    expect(labels).toEqual(["Claims"]);               // a viewer in Ops cannot move a build there
    select.selectOptionByValue("aaaa0001");
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Move" })); });
    expect(calls.at(-1)).toEqual({ method: "PUT", path: "/api/builds/b1/team", body: { team: "aaaa0001" } });
    expect(onMoved).toHaveBeenCalled();
  });

  it("offers a team build's way back to its creator's builds", async () => {
    teams = [CLAIMS];
    render(<MoveToTeam build={{ id: "b1", name: "B", updatedAt: "", team: "aaaa0001" }} notify={notify}
      onDismiss={() => {}} onMoved={() => {}} />);
    await waitFor(() => expect(calls.some((c) => c.path === "/api/teams")).toBe(true));
    const select = createWrapper(document.body).findSelect()!;
    select.openDropdown();
    expect(select.findDropdown().findOptions().map((o) => o.getElement().textContent))
      .toEqual(["Its creator's own buildsOnly the person who created it can do this"]);
  });
});
