/** The in-app assistant shows its replies formatted (it answers in Markdown), and what
 *  the user typed exactly as typed. */
import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { describe, expect, it, vi } from "vitest";

vi.mock("../api", () => ({
  api: {
    post: async () => ({ reply: "Here's the **cost breakdown**:\n\n- **Total cost:** $0.0274\n- **Math Agent:** $0.0033\n\n---" }),
    get: async () => ({}),
  },
}));

import { Assistant } from "./Assistant";

describe("the in-app assistant", () => {
  it("formats its Markdown reply", async () => {
    Element.prototype.scrollIntoView = vi.fn();
    const { container } = render(<Assistant ui={{ assistantTitle: "Run Assistant" }} chatbot={{ enabled: true }}
      sessionId={null} onActed={() => {}} />);
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Run Assistant" })); });
    await act(async () => { createWrapper(container).findInput()!.setInputValue("**cost** please"); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "Send" })); });
    await waitFor(() => expect(container.querySelectorAll(".asst-turn-md li").length).toBe(2));
    expect(container.querySelector(".asst-turn-md strong")?.textContent).toBe("cost breakdown");
    expect(container.querySelector(".asst-turn-md hr")).toBeTruthy();
    expect(container.querySelector(".asst-turn-md")?.textContent).not.toContain("**");
    // The user's own message is left as typed.
    expect(container.querySelector(".asst-turn-user .asst-turn-text")?.textContent).toBe("**cost** please");
  });
});
