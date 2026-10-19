/** AgentCore Harness agents in the Builder (runtime "harness"): picked as the agent's runtime,
 *  which keeps only what a harness reads and starts its instructions from the prompt; edited
 *  as Instructions rather than a prompts.py; offered the build's tools (its Gateway tools);
 *  its own settings (HarnessSettings: model provider, tools, skills, memory, environment,
 *  endpoint, or an existing harness by ARN); checked as you type. */
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { addChoices } from "./componentOps";
import { HarnessSettings } from "./HarnessSettings";
import { Inspector } from "./Inspector";
import { newProject, type Entry, type Project, type Workflow } from "./model";
import { validate } from "./validate";

beforeAll(() => {
  vi.stubGlobal("ResizeObserver", class { observe() {} unobserve() {} disconnect() {} });
  vi.stubGlobal("DOMMatrixReadOnly", class { m22 = 1; constructor() {} });
});
afterEach(cleanup);

/** The starter project, its one agent, and a tool it could be given. */
function starter(): { project: Project; id: string } {
  const project = newProject("x");
  const id = Object.keys(project.workflow.agents)[0];
  project.workflow.tools = { docs: { type: "mcp", endpoint: "https://docs.example/mcp" } };
  project.prompts[id] = { systemPrompt: "You write the summary.", schema: "" };
  return { project, id };
}

const show = async (project: Project, id: string, onChange = vi.fn()) => {
  await act(async () => {
    render(<Inspector project={project} selection={{ kind: "agent", id }} issues={[]} onChange={onChange} onSelect={() => {}} />);
  });
  return onChange;
};

describe("harness agents", () => {
  it("switches an agent to a harness: keeps what a harness reads, its tools too, and starts its instructions from the prompt", async () => {
    const { project, id } = starter();
    project.workflow.agents[id].tool = "docs";
    const onChange = await show(project, id);
    const trigger = screen.getByRole("button", { name: /Runtime/ });
    const runtime = createWrapper(document.body).findAllSelects().find((s) => s.findTrigger().getElement() === trigger)!;
    await act(async () => { runtime.openDropdown(); });
    await act(async () => { runtime.selectOptionByValue("harness"); });
    const next: Project = onChange.mock.calls.at(-1)![0];
    const agent = next.workflow.agents[id];
    expect(agent).toMatchObject({ runtime: "harness", instructions: "You write the summary.", maxTokens: 4000 });
    // Its Gateway tools: the harness calls them itself.
    expect(agent.tool).toBe("docs");
    expect(agent.access).toBeUndefined();
    expect(validate(next.workflow).filter((i) => i.severity === "error").map((i) => `${i.path}: ${i.message}`)).toEqual([]);
  });

  it("edits Instructions, not a prompts.py, and offers the build's tools", async () => {
    const { project, id } = starter();
    project.workflow.agents[id] = { name: "Writer", runtime: "harness", instructions: "Write.", agentcore: {} };
    const onChange = await show(project, id);
    expect(screen.queryByText("Output shape")).toBeNull();
    const box = screen.getByRole("textbox", { name: "System prompt" }) as HTMLTextAreaElement;
    expect(box.value).toBe("Write.");
    await act(async () => { fireEvent.change(box, { target: { value: "Write it well." } }); });
    expect(onChange.mock.calls.at(-1)![0].workflow.agents[id].instructions).toBe("Write it well.");
    expect(addChoices(project, id).filter((c) => c.group === "Tools").map((c) => c.label)).toEqual(["docs"]);
    // Its own settings panel is there.
    expect(screen.getByText("Use an existing harness (ARN)")).toBeTruthy();
  });
});

describe("harness settings", () => {
  const settings = async (agent: Entry, workflow: Partial<Workflow> = {}) => {
    const onAgent = vi.fn();
    await act(async () => {
      render(<HarnessSettings agent={agent} workflow={{ ...newProject("x").workflow, ...workflow } as Workflow} onAgent={onAgent} />);
    });
    return onAgent;
  };
  const W: Entry = { name: "Writer", runtime: "harness", instructions: "Write.", model: "m", tool: "docs" };

  it("turns on the browser and code interpreter, and adds an MCP server", async () => {
    let onAgent = await settings(W);
    await act(async () => { fireEvent.click(screen.getByRole("checkbox", { name: "Web browser" })); });
    expect(onAgent.mock.calls.at(-1)![0].harness).toEqual({ browser: true });
    cleanup();
    onAgent = await settings({ ...W, harness: { browser: true } });
    // A custom browser by ARN, and back to the managed one when the box is cleared.
    await act(async () => { fireEvent.change(screen.getByLabelText("Custom browser ARN"), { target: { value: "arn:aws:bedrock-agentcore:x" } }); });
    expect(onAgent.mock.calls.at(-1)![0].harness).toEqual({ browser: "arn:aws:bedrock-agentcore:x" });
    await act(async () => { fireEvent.click(screen.getByRole("checkbox", { name: "Code interpreter" })); });
    expect(onAgent.mock.calls.at(-1)![0].harness).toEqual({ browser: true, codeInterpreter: true });
    cleanup();
    onAgent = await settings({ ...W, harness: { mcp: [{ name: "", url: "" }] } });
    await act(async () => { fireEvent.change(screen.getByLabelText("MCP server 1 URL"), { target: { value: "https://wiki.example.com/mcp" } }); });
    expect(onAgent.mock.calls.at(-1)![0].harness).toEqual({ mcp: [{ name: "", url: "https://wiki.example.com/mcp" }] });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Remove MCP server 1" })); });
    expect(onAgent.mock.calls.at(-1)![0].harness).toBeUndefined();
  });

  it("asks for an API key only for another provider", async () => {
    expect(screen.queryByLabelText("API key")).toBeNull();
    const onAgent = await settings(W);
    expect(screen.queryByLabelText("API key")).toBeNull();
    const trigger = screen.getByRole("button", { name: /Provider/ });
    const select = createWrapper(document.body).findAllSelects().find((s) => s.findTrigger().getElement() === trigger)!;
    await act(async () => { select.openDropdown(); });
    await act(async () => { select.selectOptionByValue("openai"); });
    expect(onAgent.mock.calls.at(-1)![0].harness).toEqual({ provider: "openai" });
    cleanup();
    await settings({ ...W, harness: { provider: "litellm" } });
    expect(screen.getByLabelText("API key")).toBeTruthy();
    expect(screen.getByLabelText("API base URL")).toBeTruthy();
  });

  it("uses an existing harness: drops what that harness holds, keeps the endpoint and limits", async () => {
    const onAgent = await settings({ ...W, temperature: 0.2, harness: { shell: true, endpoint: "live", maxIterations: 5 } });
    await act(async () => { fireEvent.click(screen.getByRole("checkbox", { name: "Use an existing harness (ARN)" })); });
    const next = onAgent.mock.calls.at(-1)![0];
    expect(next).toEqual({ name: "Writer", runtime: "harness", harness: { endpoint: "live", maxIterations: 5, arn: "" } });
    // Until it is a real ARN, the check says so.
    const wf = { ...newProject("x").workflow };
    wf.agents = { ...wf.agents, writer: next };
    expect(validate(wf).filter((i) => i.path.startsWith("agents.writer.")).map((i) => `${i.path}: ${i.message}`))
      .toEqual(["agents.writer.harness.arn: must be a harness ARN: arn:aws:bedrock-agentcore:<region>:<account>:harness/<name>-<id>"]);
    cleanup();
    const again = await settings(next);
    await act(async () => {
      fireEvent.change(screen.getByLabelText("Harness ARN"), { target: { value: "arn:aws:bedrock-agentcore:us-east-1:123456789012:harness/h-AbCdEf1234" } });
    });
    expect(again.mock.calls.at(-1)![0].harness.arn).toBe("arn:aws:bedrock-agentcore:us-east-1:123456789012:harness/h-AbCdEf1234");
    expect(screen.queryByText("Tools")).toBeNull();
  });

  it("picks a memory of the build and a session storage mount", async () => {
    const onAgent = await settings({ ...W, harness: { mounts: [{ session: "/mnt/data" }] } }, { memories: { notes: { strategies: ["semantic"] } } } as any);
    await act(async () => { fireEvent.change(screen.getByLabelText("Storage 1 path"), { target: { value: "/mnt/work" } }); });
    expect(onAgent.mock.calls.at(-1)![0].harness).toEqual({ mounts: [{ session: "/mnt/work" }] });
    const select = createWrapper(document.body).findAllSelects()
      .find((s) => s.findTrigger().getElement().textContent?.includes("No memory"))!;
    await act(async () => { select.openDropdown(); });
    await act(async () => { select.selectOptionByValue("notes"); });
    expect(onAgent.mock.calls.at(-1)![0].harness).toEqual({ mounts: [{ session: "/mnt/data" }], memory: "notes" });
  });
});
