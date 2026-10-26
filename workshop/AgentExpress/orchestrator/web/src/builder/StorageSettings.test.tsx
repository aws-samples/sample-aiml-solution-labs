/** Settings > Storage (orchestrator.storage): off writes nothing, on writes {create: true},
 *  keep writes retain, off again removes the key, and the token is shown as tools use it. */
import { act, render } from "@testing-library/react";
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { describe, expect, it, vi } from "vitest";
import { newProject, type Project } from "./model";
import { StorageSettings, storageSetup, withStorage } from "./StorageSettings";
const wrapper = () => createWrapper(document.body);
const orch = (p: Project) => p.workflow.orchestrator as Record<string, unknown>;

describe("StorageSettings", () => {
  it("is off for a build without it, and on writes create: true only", async () => {
    const project = newProject("Storage test");
    expect(storageSetup(project)).toBeNull();
    const setProject = vi.fn();
    render(<StorageSettings project={project} setProject={setProject} />);
    expect(wrapper().findToggle()!.findNativeInput().getElement().checked).toBe(false);
    expect(wrapper().findCheckbox()).toBeNull();
    await act(async () => { wrapper().findToggle()!.findNativeInput().click(); });
    expect(orch(setProject.mock.calls[0][0]).storage).toEqual({ create: true });
  });

  it("keeps the bucket on destroy when asked, shows the token, and off removes the key", async () => {
    const project = withStorage(newProject("Storage test"), { create: true });
    const setProject = vi.fn();
    const { getByTestId } = render(<StorageSettings project={project} setProject={setProject}
      issues={[{ severity: "warning", where: { kind: "block", name: "orchestrator" }, path: "orchestrator.storage.retain", message: "a retain note" }]} />);
    expect(getByTestId("storage-token").textContent).toContain("${storage}/templates/");
    expect(document.body.textContent).toContain("a retain note");
    await act(async () => { wrapper().findCheckbox()!.findNativeInput().click(); });
    expect(orch(setProject.mock.calls[0][0]).storage).toEqual({ create: true, retain: true });
    await act(async () => { wrapper().findToggle()!.findNativeInput().click(); });
    expect("storage" in orch(setProject.mock.calls[1][0])).toBe(false);
  });
});
