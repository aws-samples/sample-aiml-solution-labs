/** A step that needs the person to connect their own account: the failed step's log
 *  line (app/features/gateway/client.py ToolNeedsConsent, as nodes.py writes it)
 *  becomes a Connect button and a "Run the step again" button for that step. */
import { act, fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { SessionSnapshot } from "../types";
import { ConnectAccount, consentOf } from "./ConnectAccount";

const URL_ = "https://bedrock-agentcore.us-east-1.amazonaws.com/identities/oauth2/authorize?request_uri=urn%3Aabc&x=1";
const snap = (msg: string, overall = "failed"): SessionSnapshot => ({
  session_id: "s1", topic: "t", overall: overall as never,
  nodes: { intake: { status: "done" }, orders: { status: "failed" } },
  logs: [{ ts: "1", node: "intake", msg: "Intake done" },
    { ts: "2", node: "orders", msg }],
});
const LINE = `Orders failed: ToolNeedsConsent: Connect your account for 'crm' first, then run this step again: ${URL_}`;

describe("Connect your account", () => {
  it("reads the tool, where to connect, and the step from the run", () => {
    expect(consentOf(snap(LINE))).toEqual({ tool: "crm", url: URL_, agent: "orders" });
    expect(consentOf(snap("Orders failed: ToolUnavailable: timeout"))).toBeNull();
    // Only AgentCore Identity's own address becomes a button.
    expect(consentOf(snap(LINE.replace("bedrock-agentcore.us-east-1.amazonaws.com", "evil.example.com")))).toBeNull();
  });

  it("offers Connect, and the re-run of that step", async () => {
    const onRerun = vi.fn(async () => {});
    render(<ConnectAccount snap={snap(LINE)} canRerun onRerun={onRerun} />);
    expect(screen.getByText("Connect your crm account")).toBeTruthy();
    expect(screen.getByText("Connect").closest("a")?.getAttribute("href")).toBe(URL_);
    await act(async () => { fireEvent.click(screen.getByText("Run the step again")); });
    expect(onRerun).toHaveBeenCalledWith(["orders"], "");
  });

  it("shows nothing while the run is going or once it went through, and no re-run without the right", () => {
    const { container } = render(<ConnectAccount snap={snap(LINE, "running")} canRerun onRerun={vi.fn()} />);
    expect(container.textContent).toBe("");
    const done = render(<ConnectAccount snap={snap(LINE, "done")} canRerun onRerun={vi.fn()} />);
    expect(done.container.textContent).toBe("");
    render(<ConnectAccount snap={snap(LINE)} canRerun={false} onRerun={vi.fn()} />);
    expect(screen.queryByText("Run the step again")).toBeNull();
  });
});
