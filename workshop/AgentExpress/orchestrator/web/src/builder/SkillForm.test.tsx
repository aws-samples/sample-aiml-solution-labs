/** A skill in the Builder: written by hand or imported from a SKILL.md, with text
 *  reference files; and the SKILL.md it is published to an Agent Registry as. */
import { act, fireEvent, render } from "@testing-library/react";
import createWrapper from "@cloudscape-design/components/test-utils/dom";
import { useState } from "react";
import { describe, expect, it } from "vitest";

import type { Entry } from "./model";
import { parseSkillMd, SkillForm, skillKey, toSkillMd } from "./SkillForm";
import { validate } from "./validate";

const MD = "---\nname: refund-policy\ndescription: \"When a customer asks for money back.\"\n---\n\n# Refunds\n\n1. Check the date.\n";

describe("SKILL.md", () => {
  it("reads the frontmatter and the body, and a file with none is all body", () => {
    expect(parseSkillMd(MD)).toEqual({ name: "refund-policy", description: "When a customer asks for money back.",
      instructions: "# Refunds\n\n1. Check the date." });
    expect(parseSkillMd("Just steps.")).toEqual({ name: "", description: "", instructions: "Just steps." });
  });
  it("names it for a build, and writes it back in the Agent Skills shape", () => {
    expect(skillKey("refund-policy")).toBe("refundPolicy");
    expect(skillKey("2fa setup")).toBe("skill2faSetup");
    const md = toSkillMd("refundPolicy", { description: "When a customer\nasks.", instructions: "1. Check." });
    expect(md).toBe('---\nname: refund-policy\ndescription: "When a customer asks."\n---\n\n1. Check.\n');
    // Quoted, so ": " in a description stays valid YAML, and it reads back as written.
    const desc = 'Use when writing the headline (title): how to draft it, and "why".';
    expect(parseSkillMd(toSkillMd("headlineRules", { description: desc, instructions: "1." })).description).toBe(desc);
    expect(parseSkillMd(md).instructions).toBe("1. Check.");
  });
});

let latest: Entry = {};
let named = "";
function Harness() {
  const [v, setV] = useState<Entry>({ description: "", instructions: "" });
  latest = v;
  return <SkillForm value={v} onChange={setV} onName={(n) => { named = n; }} />;
}
const w = () => createWrapper(document.body);
const input = (label: string) => w().findAllInputs().find((i) => i.findNativeInput().getElement().getAttribute("aria-label") === label)!;
const area = (label: string) => w().findAllTextareas().find((t) => t.findNativeTextarea().getElement().getAttribute("aria-label") === label)!;
const upload = async (label: string, files: File[]) => {
  const el = document.querySelector(`input[type=file][aria-label="${label}"]`) as HTMLInputElement;
  await act(async () => { fireEvent.change(el, { target: { files } }); });
  await act(async () => { await new Promise((r) => setTimeout(r, 0)); });
};

describe("the skill form", () => {
  it("imports a SKILL.md, takes text reference files only, and validates clean", async () => {
    render(<Harness />);
    await upload("SKILL.md file", [new File([MD], "SKILL.md", { type: "text/markdown" })]);
    expect(latest.description).toBe("When a customer asks for money back.");
    expect(latest.instructions).toBe("# Refunds\n\n1. Check the date.");
    expect(named).toBe("refundPolicy");
    await upload("Reference files", [new File(["Receipt needed."], "policy.md"), new File(["rm -rf"], "run.sh")]);
    expect(latest.files).toEqual({ "policy.md": "Receipt needed." });
    expect(document.body.textContent).toContain("Not added (text files only");
    const wf = { agents: { a: { name: "A", skills: ["refundPolicy"] } }, steps: [{ agent: "a" }], tools: {},
      skills: { refundPolicy: latest } };
    expect(validate(wf as never).filter((i) => i.path.startsWith("skills"))).toEqual([]);
  });

  it("edits by hand: the description, the steps, a file's name and contents, and removing it", async () => {
    render(<Harness />);
    await act(async () => { input("When to use it").setInputValue("When writing to a customer."); });
    await act(async () => { area("Instructions").setTextareaValue("Be brief."); });
    await act(async () => { w().findAllButtons().find((b) => b.getElement().textContent === "Add a file")!.click(); });
    expect(latest.files).toEqual({ "notes1.md": "" });
    await act(async () => { area("Contents of notes1.md").setTextareaValue("Sign off with your name."); });
    await act(async () => { input("File name notes1.md").setInputValue("signoff.md"); });
    expect(latest).toEqual({ description: "When writing to a customer.", instructions: "Be brief.",
      files: { "signoff.md": "Sign off with your name." } });
    await act(async () => { w().findAllButtons().find((b) => b.getElement().getAttribute("aria-label") === "Remove signoff.md")!.click(); });
    expect(latest.files).toBeUndefined();
  });
});
