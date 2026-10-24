/** Connectors: each one writes a tool both validators accept, points only at its vendor's
 *  own domain, and is reachable from the Project menu and from Add a tool. The template
 *  gallery it replaced is gone. */
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { act, cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { Builder } from "./Builder";
import { Connectors } from "./Connectors";
import {
  addConnector, CONNECTORS, connectorTool, fieldError, matches, methodsOf, openApiOf, type Connector,
} from "./connectorCatalog";
import { newProject, type Project } from "./model";
import { listProjects } from "./storage";
import { validate } from "./validate";

function memoryStorage(): Storage {
  const m = new Map<string, string>();
  return {
    get length() { return m.size; },
    clear: () => m.clear(),
    getItem: (k) => m.get(k) ?? null,
    key: (i) => [...m.keys()][i] ?? null,
    removeItem: (k) => { m.delete(k); },
    setItem: (k, v) => { m.set(k, String(v)); },
  };
}
beforeAll(() => {
  class RO { observe() {} unobserve() {} disconnect() {} }
  vi.stubGlobal("ResizeObserver", RO);
  vi.stubGlobal("DOMMatrixReadOnly", class { m22 = 1; constructor() {} });
});
beforeEach(() => { vi.stubGlobal("localStorage", memoryStorage()); });
afterEach(cleanup);
/** Wait out the autosave (800 ms) in real time: the gallery loads lazily, so fake timers would stall it. */
/** The Cloudscape input whose native input carries this aria-label. */
const field = (label: string) => createWrapper(document.body).findAllInputs()
  .find((i) => i.findNativeInput().getElement().getAttribute("aria-label") === label)!;
const autosaved = () => act(async () => { await new Promise((r) => setTimeout(r, 950)); });
/** A whole Builder render plus the autosave's real-time wait: about 2 s alone, more than
 *  vitest's default 5 s under the whole suite's parallel load. */
const SLOW = 30_000;

/** A valid value for each field, by connector. */
const SAMPLE: Record<string, Record<string, string>> = {
  salesforce: { domain: "acme.my.salesforce.com" },
  jira: { cloudId: "11223344-a1b2-3b33-c444-def123456789" },
  confluence: { cloudId: "11223344-a1b2-3b33-c444-def123456789" },
  servicenow: { instance: "acme" },
  microsoft365: { tenant: "contoso.onmicrosoft.com" },
  zendesk: { subdomain: "acme" },
};
/** The vendor host every URL a connector writes must be on. */
const VENDOR_HOST: Record<string, RegExp> = {
  salesforce: /^acme\.my\.salesforce\.com$/,
  slack: /^slack\.com$/,
  jira: /^(api|auth)\.atlassian\.com$/,
  confluence: /^(api|auth)\.atlassian\.com$/,
  servicenow: /^acme\.service-now\.com$/,
  github: /^(api\.)?github\.com$/,
  microsoft365: /^(graph\.microsoft\.com|login\.microsoftonline\.com)$/,
  googleWorkspace: /^(www\.googleapis\.com|accounts\.google\.com)$/,
  zendesk: /^acme\.zendesk\.com$/,
  hubspot: /^(api\.hubapi\.com|app\.hubspot\.com)$/,
};
const VENDORS = ["GoogleOauth2", "GithubOauth2", "SlackOauth2", "SalesforceOauth2", "MicrosoftOauth2", "CustomOauth2",
  "AtlassianOauth2", "HubspotOauth2"]; // botocore CreateOauth2CredentialProvider credentialProviderVendor (subset)

const tool = (c: Connector, m = methodsOf(c)[0]) => connectorTool(c, SAMPLE[c.id] ?? {}, m, "client-123");
const urlsOf = (t: ReturnType<typeof tool>) => {
  const oauth = t.oauth as Record<string, unknown>;
  const schema = t.schema as { servers: { url: string }[] };
  return [schema.servers[0].url, ...["tokenUrl", "authorizationUrl", "discoveryUrl"].map((k) => oauth[k]).filter(Boolean) as string[]];
};

describe("the connectors as data", () => {
  it("offers 8 to 12, each with a name, a description, a setup, its limits and docs", () => {
    expect(CONNECTORS.length).toBeGreaterThanOrEqual(8);
    expect(CONNECTORS.length).toBeLessThanOrEqual(12);
    expect(new Set(CONNECTORS.map((c) => c.id)).size).toBe(CONNECTORS.length);
    expect(new Set(CONNECTORS.map((c) => c.name)).size).toBe(CONNECTORS.length);
    for (const c of CONNECTORS) {
      expect(c.description.length).toBeGreaterThan(20);
      expect(c.setup.length).toBeGreaterThan(20);
      expect(c.limits.length).toBeGreaterThan(20);
      expect(c.docs.length).toBeGreaterThan(0);
      for (const d of c.docs) expect(d.url).toMatch(/^https:\/\//);
      expect(VENDORS).toContain(c.vendor);
      expect(methodsOf(c).length).toBeGreaterThan(0);
      expect(VENDOR_HOST[c.id]).toBeTruthy();
    }
  });

  for (const c of CONNECTORS) {
    for (const m of methodsOf(c)) {
      it(`${c.name} (${m}) writes a tool the validator accepts, on ${c.name}'s own domain`, () => {
        const r = addConnector(newProject("x"), c, SAMPLE[c.id] ?? {}, m, "client-123");
        const t = r.project.workflow.tools[r.key];
        expect(t.type).toBe("openapi");
        expect(t.auth).toBe(m === "user" ? "user" : "oauth2");
        expect(validate(r.project.workflow).filter((i) => i.severity === "error")).toEqual([]);
        for (const u of urlsOf(t)) {
          expect(u).toMatch(/^https:\/\//);
          expect(u).not.toMatch(/[{}]/);
          expect(new URL(u).hostname).toMatch(VENDOR_HOST[c.id]);
        }
        expect(String(t.description).length).toBeLessThanOrEqual(190);
      });
    }
    it(`${c.name}'s operations are distinct tool names that fit the model's 64-character limit`, () => {
      const ids = c.operations.map((o) => o.id);
      expect(new Set(ids).size).toBe(ids.length);
      for (const id of ids) {
        expect(id).toMatch(/^[A-Za-z][A-Za-z0-9_]*$/);
        expect(`${c.id}___${id}`.length).toBeLessThanOrEqual(64);
      }
      // Every {param} in a path is declared as a required path parameter.
      const doc = openApiOf(c, SAMPLE[c.id] ?? {}) as { paths: Record<string, Record<string, { parameters?: { name: string; in: string; required: boolean }[] }>> };
      for (const [p, item] of Object.entries(doc.paths)) {
        const want = [...p.matchAll(/\{([^}]+)\}/g)].map((x) => x[1]);
        for (const op of Object.values(item)) {
          expect((op.parameters ?? []).filter((x) => x.in === "path").map((x) => x.name)).toEqual(want);
          expect((op.parameters ?? []).filter((x) => x.in === "path").every((x) => x.required)).toBe(true);
        }
      }
    });
  }

  it("refuses a value that would point a connector anywhere but its vendor", () => {
    const sf = CONNECTORS.find((c) => c.id === "salesforce")!;
    for (const bad of ["evil.example.com", "acme.my.salesforce.com.evil.com", "acme.my.salesforce.com/x", "https://acme.my.salesforce.com", ""]) {
      expect(fieldError(sf.fields[0], bad)).not.toBe("");
      expect(() => connectorTool(sf, { domain: bad }, "user", "c")).toThrow();
    }
    expect(fieldError(sf.fields[0], " Acme.My.Salesforce.com ")).toBe("");
    const sn = CONNECTORS.find((c) => c.id === "servicenow")!;
    for (const bad of ["acme.evil", "acme/x", "a b", "evil.com#"]) expect(fieldError(sn.fields[0], bad)).not.toBe("");
  });

  it("finds connectors by name or what they do", () => {
    expect(CONNECTORS.filter((c) => matches(c, "jira")).map((c) => c.id)).toEqual(["jira"]);
    expect(CONNECTORS.filter((c) => matches(c, "CALENDAR")).map((c) => c.id).sort()).toEqual(["googleWorkspace", "microsoft365"]);
    expect(CONNECTORS.filter((c) => matches(c, "")).length).toBe(CONNECTORS.length);
  });

  it("gives a second tool from the same connector a key of its own", () => {
    const gh = CONNECTORS.find((c) => c.id === "github")!;
    const a = addConnector(newProject("x"), gh, {}, "user", "c1");
    const b = addConnector(a.project, gh, {}, "user", "c2");
    expect([a.key, b.key]).toEqual(["github", "github2"]);
  });

  it("is what the two connector cases in tests/fixtures/validation_cases.json hold", () => {
    const fixture = JSON.parse(readFileSync(resolve(process.cwd(), "../tests/fixtures/validation_cases.json"), "utf8")) as
      { cases: { name: string; ops: [string, string[], unknown][] }[] };
    const sf = fixture.cases.find((c) => c.name === "a Salesforce connector, each person signing in")!;
    const jira = fixture.cases.find((c) => c.name === "a Jira connector, a service account's app login")!;
    expect(sf.ops).toEqual([["set", ["tools", "salesforce"],
      connectorTool(CONNECTORS.find((c) => c.id === "salesforce")!, SAMPLE.salesforce, "user", "client-123")]]);
    expect(jira.ops).toEqual([["set", ["tools", "jira"],
      connectorTool(CONNECTORS.find((c) => c.id === "jira")!, SAMPLE.jira, "app", "client-123")]]);
  });
});

describe("Connectors", () => {
  const open = () => {
    const onAdded = vi.fn(), onDismiss = vi.fn();
    const project = newProject("x");
    render(<Connectors visible project={project} onDismiss={onDismiss} onAdded={onAdded} />);
    return { onAdded, onDismiss, project };
  };

  it("shows every connector as a card in a labelled list, and the search narrows it", () => {
    open();
    const list = () => screen.getByRole("list", { name: "Connectors" });
    expect(within(list()).getAllByRole("listitem")).toHaveLength(CONNECTORS.length);
    for (const c of CONNECTORS) expect(within(list()).getByRole("heading", { name: c.name })).toBeTruthy();
    act(() => { createWrapper(document.body).findTextFilter()!.findInput().setInputValue("zendesk"); });
    const cards = within(list()).getAllByRole("listitem");
    expect(cards).toHaveLength(1);
    expect(within(cards[0]).getByRole("heading", { name: "Zendesk" })).toBeTruthy();
    act(() => { createWrapper(document.body).findTextFilter()!.findInput().setInputValue("no such service"); });
    expect(screen.queryByRole("list", { name: "Connectors" })).toBeNull();
    expect(screen.getByText(/No connector matches/)).toBeTruthy();
  });

  it("is operable with the keyboard: each card's action is a real button, and Cancel closes it", () => {
    const { onDismiss } = open();
    const buttons = CONNECTORS.map((c) => screen.getByRole("button", { name: `Set up ${c.name}` }));
    for (const b of buttons) { expect(b.tagName).toBe("BUTTON"); expect(b.hasAttribute("disabled")).toBe(false); }
    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    expect(onDismiss).toHaveBeenCalled();
  });

  it("asks for the site and client, refuses a bad site, and adds exactly the connector's tool", () => {
    const { onAdded, project } = open();
    fireEvent.click(screen.getByRole("button", { name: "Set up Salesforce" }));
    expect(screen.getByText("Connect Salesforce")).toBeTruthy();
    const w = createWrapper(document.body);
    fireEvent.click(screen.getByRole("button", { name: "Add tool" }));
    expect(onAdded).not.toHaveBeenCalled();
    act(() => { field("My Domain address").setInputValue("evil.example.com"); });
    act(() => { field("Client ID").setInputValue("client-123"); });
    fireEvent.click(screen.getByRole("button", { name: "Add tool" }));
    expect(onAdded).not.toHaveBeenCalled();
    expect(screen.getByText(/Ends with \.my\.salesforce\.com/)).toBeTruthy();
    act(() => { field("My Domain address").setInputValue("acme.my.salesforce.com"); });
    act(() => { w.findTiles()!.findItemByValue("app")!.findNativeInput().click(); });
    fireEvent.click(screen.getByRole("button", { name: "Add tool" }));
    expect(onAdded).toHaveBeenCalledTimes(1);
    const [r, c] = onAdded.mock.calls[0] as [{ project: Project; key: string }, Connector];
    expect(c.id).toBe("salesforce");
    expect(r.key).toBe("salesforce");
    expect(r.project.workflow.tools.salesforce).toEqual(
      connectorTool(c, SAMPLE.salesforce, "app", "client-123"));
    expect(project.workflow.tools).toEqual({});       // the build it was given is not changed in place
  });

  it("goes back to the gallery", () => {
    open();
    fireEvent.click(screen.getByRole("button", { name: "Set up GitHub" }));
    fireEvent.click(screen.getByRole("button", { name: "Back" }));
    expect(screen.getByRole("list", { name: "Connectors" })).toBeTruthy();
  });
});

describe("Connectors in the Builder", () => {
  it("adds a connector from the Tools palette into the build's workflow.json, as a normal tool", async () => {
    const notify = vi.fn();
    await act(async () => { render(<Builder notify={notify} />); });
    await act(async () => { fireEvent.click(screen.getAllByRole("button", { name: "Add a connector" })[0]); });
    // findBy outside act: a wait inside an act scope holds that scope open, and if the test
    // times out the next test's render lands in it and never commits.
    const jira = await screen.findByRole("button", { name: "Set up Jira" });
    await act(async () => { fireEvent.click(jira); });
    act(() => { field("Cloud ID").setInputValue(SAMPLE.jira.cloudId); });
    act(() => { field("Client ID").setInputValue("client-123"); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Add tool" })); });
    await autosaved();
    const saved = listProjects()[0];
    expect(saved.workflow.tools.jira).toEqual(connectorTool(CONNECTORS.find((c) => c.id === "jira")!, SAMPLE.jira, "app", "client-123"));
    expect(notify).toHaveBeenCalledWith("success", expect.stringContaining("Jira"));
    expect(screen.getByText("Valid")).toBeTruthy();
  }, SLOW);

  it("is not in the Project menu: it belongs with the tools", async () => {
    await act(async () => { render(<Builder notify={vi.fn()} />); });
    const menu = createWrapper(document.body).findButtonDropdown()!;
    await act(async () => { menu.openDropdown(); });
    expect(menu.findItemById("connector")).toBeNull();
  });

  it("is offered from Add a tool", async () => {
    await act(async () => { render(<Builder notify={vi.fn()} />); });
    await act(async () => { fireEvent.click(screen.getAllByRole("button", { name: "Add a tool" })[0]); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Use a connector" })); });
    expect(await screen.findByRole("list", { name: "Connectors" })).toBeTruthy();
  });
});

describe("the template gallery", () => {
  it("is gone: no menu item, no offer, no files", async () => {
    await act(async () => { render(<Builder notify={vi.fn()} />); });
    const menu = createWrapper(document.body).findButtonDropdown()!;
    await act(async () => { menu.openDropdown(); });
    expect(menu.findItemById("template")).toBeNull();
    expect(menu.findItemById("new")).not.toBeNull();     // New build still makes a blank build
    expect(screen.queryByTestId("template-offer")).toBeNull();
    expect(screen.queryByText(/from a template/i)).toBeNull();
    for (const f of ["Gallery.tsx", "Gallery.css", "templates"]) expect(existsSync(resolve(process.cwd(), "src/builder", f))).toBe(false);
  });
});
