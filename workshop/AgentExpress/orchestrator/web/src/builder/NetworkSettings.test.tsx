/** The Network sections: a dedicated agent's own Runtime, the shared orchestrator Runtime, and
 *  what the other runtimes say instead (orchestrator.network, agents.<id>.network). */
import { act, render } from "@testing-library/react";
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { describe, expect, it, vi } from "vitest";
import { AgentNetworkSettings, NetworkFields } from "./NetworkSettings";

const wrapper = () => createWrapper(document.body);
const mode = () => wrapper().findSegmentedControl()!;
const input = (label: string) => wrapper().findAllInputs()
  .find((i) => i.findNativeInput().getElement().getAttribute("aria-label") === label)!;

describe("NetworkFields", () => {
  it("is Public with no ids when unset, and VPC writes mode and empty id lists", async () => {
    const onChange = vi.fn();
    render(<NetworkFields value={undefined} path="orchestrator.network" onChange={onChange} />);
    expect(mode().findSelectedSegment()!.getElement().textContent).toBe("Public");
    expect(wrapper().findInput()).toBeNull();
    await act(async () => { mode().findSegments()[1].click(); });
    expect(onChange).toHaveBeenLastCalledWith({ mode: "VPC", subnetIds: [], securityGroupIds: [] });
  });

  it("edits the ids, shows their issues, and Public removes the setting", async () => {
    const onChange = vi.fn();
    const net = { mode: "VPC", subnetIds: ["subnet-0123456789abcdef0"], securityGroupIds: [] };
    render(<NetworkFields value={net} path="agents.r.network" onChange={onChange}
      issues={[{ severity: "error", where: { kind: "agent", id: "r" }, path: "agents.r.network.securityGroupIds",
        message: 'mode "VPC" needs 1-16 ids, each sg-...' }]} />);
    expect(input("Subnet ids").getInputValue()).toBe("subnet-0123456789abcdef0");
    expect(document.body.textContent).toContain('mode "VPC" needs 1-16 ids, each sg-...');
    await act(async () => { input("Security group ids").setInputValue("sg-0123456789abcdef0, sg-0fedcba9876543210"); });
    expect(onChange).toHaveBeenLastCalledWith({ ...net, securityGroupIds: ["sg-0123456789abcdef0", "sg-0fedcba9876543210"] });
    await act(async () => { mode().findSegments()[0].click(); });
    expect(onChange).toHaveBeenLastCalledWith(undefined);
  });
});

describe("AgentNetworkSettings", () => {
  it("lets a dedicated agent set its own network, and drops the key on Public", async () => {
    const onAgent = vi.fn();
    const agent = { name: "R", runtime: "dedicated", network: { mode: "VPC", subnetIds: [], securityGroupIds: [] } };
    render(<AgentNetworkSettings agent={agent} runtime="dedicated" path="agents.r.network" onAgent={onAgent} />);
    await act(async () => { mode().findSegments()[0].click(); });
    expect(onAgent).toHaveBeenLastCalledWith({ name: "R", runtime: "dedicated" });
  });

  it("tells a main agent it runs in the shared orchestrator Runtime", () => {
    const { getByTestId } = render(<AgentNetworkSettings agent={{ name: "I" }} runtime="main" path="agents.i.network" onAgent={vi.fn()} />);
    expect(getByTestId("network-note").textContent).toBe(
      "Runs inside the shared orchestrator Runtime; switch this agent to Dedicated to give it its own network.");
    expect(wrapper().findSegmentedControl()).toBeNull();
  });

  it("is disabled with the reason for an a2a agent", () => {
    const { getByTestId } = render(<AgentNetworkSettings agent={{ name: "P", runtime: "a2a" }} runtime="a2a" path="agents.p.network" onAgent={vi.fn()} />);
    expect(getByTestId("network-reason").textContent).toContain("Not available for a remote (a2a) agent");
    expect(mode().findSegments().every((s) => s.getElement().hasAttribute("disabled") || s.getElement().getAttribute("aria-disabled") === "true")).toBe(true);
  });
});
