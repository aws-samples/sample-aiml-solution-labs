/** An MCP tool's server panel: by URL, or hosted by AgentExpress with its code written here,
 *  fetched, or an image, and its Runtime's network. */
import { fireEvent, render, screen } from "@testing-library/react";
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { useState } from "react";
import { describe, expect, it } from "vitest";
import { HostedMcp, starterServer } from "./HostedMcp";
import { hostedMcpCode, toBundle, type Project } from "./model";
import { validate } from "./validate";

const IMAGE = "123456789012.dkr.ecr.us-east-1.amazonaws.com/team/orders-mcp:1.2";
const base = (): Project => ({ id: "p1", name: "P", createdAt: "", updatedAt: "", prompts: {},
  workflow: { agents: {}, steps: [], tools: { orders: { type: "mcp", description: "Orders", endpoint: "https://x.example.com/mcp", auth: "apikey" } } } as never });
let latest: Project;
function Harness({ start = base() }: { start?: Project }) {
  const [p, setP] = useState(start);
  latest = p;
  return <HostedMcp project={p} id="orders" onChange={setP} issues={validate(p.workflow)} />;
}
/** The panel's segmented controls, in order: the server, its code (CodeSourceField), the network mode. */
const ORDER = ["The server", "Code source", "Network mode"];
const segment = (label: string) => createWrapper(document.body).findAllSegmentedControls()[ORDER.indexOf(label)]!;

describe("a hosted MCP server", () => {
  it("switching to hosted drops the URL and auth, and starts a server written here", () => {
    render(<Harness />);
    segment("The server").findSegments()[1].click();
    const tool = latest.workflow.tools.orders as Record<string, unknown>;
    expect(tool).toEqual({ type: "mcp", description: "Orders", hosted: {} });
    expect(latest.toolCode?.orders["server.py"]).toContain('FastMCP(host="0.0.0.0", stateless_http=True)');
    // The framework's tested pins (requirements.lock; tests/test_hosted_mcp.py checks the set).
    expect(latest.toolCode?.orders["requirements.txt"]).toBe("mcp==1.30.0\npydantic==2.13.5\n");
    expect(hostedMcpCode(latest.workflow)).toEqual(["orders"]);
    expect(toBundle(latest).toolCode?.orders).toEqual(starterServer("orders"));
    expect(validate(latest.workflow).filter((i) => i.path.startsWith("tools.orders") && i.severity === "error")).toEqual([]);
  });

  it("an image replaces the written files and is checked as private ECR", () => {
    const p = base();
    (p.workflow.tools as Record<string, unknown>).orders = { type: "mcp", hosted: {} };
    render(<Harness start={p} />);
    // CodeSourceField with the image option: written, upload, git, S3, image.
    segment("Code source").findSegments()[4].click();
    fireEvent.change(screen.getByLabelText("Container image URI"), { target: { value: "docker.io/x:1" } });
    expect((latest.workflow.tools.orders as any).hosted.code).toEqual({ image: "docker.io/x:1" });
    expect(validate(latest.workflow).some((i) => i.path === "tools.orders.hosted.code.image")).toBe(true);
    fireEvent.change(screen.getByLabelText("Container image URI"), { target: { value: IMAGE } });
    expect(hostedMcpCode(latest.workflow)).toEqual([]);
    expect(toBundle(latest).toolCode).toBeUndefined();
  });

  it("git code is set with the shared code-source fields and checked by the shared validator", () => {
    const p = base();
    (p.workflow.tools as Record<string, unknown>).orders = { type: "mcp", hosted: {} };
    p.toolCode = { orders: starterServer("orders") };
    render(<Harness start={p} />);
    expect(screen.getAllByLabelText("server.py").length).toBeGreaterThan(0);
    segment("Code source").findSegments()[2].click();
    fireEvent.change(screen.getByLabelText("Repository URL"), { target: { value: "http://git.example.com/o/r.git" } });
    fireEvent.change(screen.getByLabelText("Branch, tag or commit"), { target: { value: "v1" } });
    expect((latest.workflow.tools.orders as any).hosted.code).toEqual({ git: { url: "http://git.example.com/o/r.git", ref: "v1" } });
    expect(screen.queryAllByLabelText("server.py")).toEqual([]);
    expect(validate(latest.workflow).some((i) => i.path === "tools.orders.hosted.code.git.url")).toBe(true);
    expect(toBundle(latest).toolCode).toBeUndefined();
    // Back to written here: the code key goes and the files are kept.
    segment("Code source").findSegments()[0].click();
    expect((latest.workflow.tools.orders as any).hosted).toEqual({});
    expect(latest.toolCode?.orders).toEqual(starterServer("orders"));
  });
  it("a VPC network is written under hosted.network, and Public removes it", () => {
    const p = base();
    (p.workflow.tools as Record<string, unknown>).orders = { type: "mcp", hosted: { code: { image: IMAGE } } };
    render(<Harness start={p} />);
    segment("Network mode").findSegments()[1].click();
    expect((latest.workflow.tools.orders as any).hosted.network).toEqual({ mode: "VPC", subnetIds: [], securityGroupIds: [] });
    expect(validate(latest.workflow).some((i) => i.path === "tools.orders.hosted.network.subnetIds")).toBe(true);
    segment("Network mode").findSegments()[0].click();
    expect((latest.workflow.tools.orders as any).hosted).toEqual({ code: { image: IMAGE } });
  });
});
