/** Publish to registry: a build goes as one request, skills one each; what comes back is
 *  shown with its status, a rejection with the curator's reason. */
import { act, render, screen } from "@testing-library/react";
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const posts: unknown[] = [];
const REJECTED = { registryId: "R1", records: {
  workflow: { recordId: "a", name: "ax-1-workflow", version: "2", status: "REJECTED", statusReason: "Needs an owner" } } };
const state: { published: unknown; post?: unknown } = { published: REJECTED };
vi.mock("../api", () => ({ api: {
  get: async (url: string) => (url === "/api/registry"
    ? [{ id: "R1", name: "Org", description: "", status: "READY" }]
    : { updates: [], published: state.published }),
  post: async (_url: string, body: { what: string; skill?: string }) => {
    posts.push(body);
    if (state.post) return state.post;
    return body.what === "build"
      ? { registryId: "R1", records: { workflow: { recordId: "a", name: "ax-1-workflow", version: "3", status: "PENDING_APPROVAL" },
        gateway: { recordId: "b", name: "ax-1-tools", version: "3", status: "PENDING_APPROVAL" } } }
      : { registryId: "R1", skills: { [String(body.skill)]: { recordId: "s", name: "refund-policy", version: "1.0.0", status: "APPROVED" } } };
  },
  put: async () => ({}), del: async () => ({}),
} }));
import { RegistryPublish } from "./RegistryPublish";

const w = () => createWrapper(document.body);
const submit = async () => {
  await act(async () => { w().findAllButtons().find((b) => b.getElement().textContent === "Submit for approval")!.click(); });
};

describe("publishing", () => {
  beforeEach(() => { state.published = REJECTED; state.post = undefined; });

  it("a build: shows where it stands, then submits the deployed version", async () => {
    posts.length = 0;
    const notify = vi.fn();
    await act(async () => { render(<RegistryPublish visible buildId="b1" what="build" version={3} onDismiss={() => {}} notify={notify} />); });
    expect(screen.getByText("Rejected: Needs an owner")).toBeTruthy();
    await submit();
    expect(posts).toEqual([{ registry: "R1", what: "build" }]);
    expect(screen.getAllByText("Waiting for approval")).toHaveLength(2);
    expect(screen.getByText("Its tools (MCP server)")).toBeTruthy();
    expect(notify).toHaveBeenCalledWith("success", "Version 3 submitted for approval in the registry.");
  });

  it("skills: one request each, starting with the one picked", async () => {
    posts.length = 0;
    await act(async () => { render(<RegistryPublish visible buildId="b1" what="skill" skills={["refundPolicy", "tone"]}
      preselect="refundPolicy" onDismiss={() => {}} notify={() => {}} />); });
    await submit();
    expect(posts).toEqual([{ registry: "R1", what: "skill", skill: "refundPolicy" }]);
    expect(screen.getByText("Approved")).toBeTruthy();
  });

  describe("while the registry is still updating a record", () => {
    afterEach(() => { vi.useRealTimers(); });
    it("looks again until it settles", async () => {
      vi.useFakeTimers({ shouldAdvanceTime: true });
      state.post = { registryId: "R1", records: {
        workflow: { recordId: "a", name: "ax-1-workflow", version: "2.0.0", status: "UPDATING" } } };
      await act(async () => { render(<RegistryPublish visible buildId="b1" what="build" version={3} onDismiss={() => {}} notify={() => {}} />); });
      await submit();
      expect(screen.getByText("Updating")).toBeTruthy();
      state.published = { registryId: "R1", records: {
        workflow: { recordId: "a", name: "ax-1-workflow", version: "3.0.0", status: "PENDING_APPROVAL" } } };
      await act(async () => { await vi.advanceTimersByTimeAsync(3100); });
      expect(screen.getByText("Waiting for approval")).toBeTruthy();
      expect(screen.getByText("3.0.0")).toBeTruthy();
    });
  });
});
