/** An agent's Models section: route rules and fallback models, written as workflow.json `models`. */
import { act, render } from "@testing-library/react";
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { describe, expect, it, vi } from "vitest";
import { ModelRouting } from "./ModelRouting";

const S = "us.anthropic.claude-sonnet-4-5-20250929-v1:0";

describe("ModelRouting", () => {
  it("adds a fallback model and a rule, and clears the key when both are removed", async () => {
    const set = vi.fn();
    const { rerender } = render(<ModelRouting value={undefined} set={set} models={[]} />);
    const w = createWrapper(document.body);
    const [rules, fallback] = w.findAllAttributeEditors();
    await act(async () => { fallback.findAddButton().click(); });
    expect(set).toHaveBeenLastCalledWith({ fallback: [""] });
    await act(async () => { rules.findAddButton().click(); });
    expect(set).toHaveBeenLastCalledWith({ route: [{ model: "", inputTokensOver: 20000 }] });

    rerender(<ModelRouting value={{ fallback: [S], route: [{ model: S, inputTokensOver: 20000 }] }} set={set} models={[]} />);
    const tokens = w.findInput("[aria-label='rule 1 tokens']") ?? w.findAllAttributeEditors()[0].findRow(1)!.findField(2)!.findControl()!.findInput()!;
    await act(async () => { tokens.setInputValue("5000"); });
    expect(set).toHaveBeenLastCalledWith({ fallback: [S], route: [{ model: S, inputTokensOver: 5000 }] });

    rerender(<ModelRouting value={{ fallback: [S] }} set={set} models={[]} />);
    await act(async () => { w.findAllAttributeEditors()[1].findRow(1)!.findRemoveButton()!.click(); });
    expect(set).toHaveBeenLastCalledWith(undefined);
  });

  it("switches a rule to a pattern on the request or subject", async () => {
    const set = vi.fn();
    render(<ModelRouting value={{ route: [{ model: S, inputTokensOver: 20000 }] }} set={set} models={[]} />);
    const when = createWrapper(document.body).findAllAttributeEditors()[0].findRow(1)!.findField(1)!.findControl()!.findSelect()!;
    await act(async () => { when.openDropdown(); });
    await act(async () => { when.selectOptionByValue("matches"); });
    expect(set).toHaveBeenLastCalledWith({ route: [{ model: S, matches: "" }] });
  });
});
