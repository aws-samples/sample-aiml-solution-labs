/** An agent asking a person mid-step (app/common/asks.py) is answered the way a review gate
 *  is: a tool approved by EventBridge needs the decision rule (and the runtime's PutEvents),
 *  and an askPerson / approvalTimeout that times out needs the five-minute sweep. Neither is
 *  created for a workflow that asks for neither. Mirrors terraform/gates.tf. */
const { gateSteps } = require("../lib/orchestrator-stack");

describe("gates and asks (gateSteps)", () => {
  it("adds nothing for a workflow that does not ask", () => {
    expect(gateSteps({ steps: [{ agent: "a" }], agents: { a: { askPerson: true } },
      tools: { pay: { type: "lambda", approval: "person" } } })).toEqual({ events: false, timeouts: false });
  });

  it("wires the decision rule for a tool approved by event", () => {
    expect(gateSteps({ steps: [{ agent: "a" }], tools: { pay: { approval: "event" } } }))
      .toEqual({ events: true, timeouts: false });
  });

  it("wires the sweep for a question or a tool approval that times out", () => {
    expect(gateSteps({ steps: [], agents: { a: { askPerson: { timeout: "24h" } } } }))
      .toEqual({ events: false, timeouts: true });
    expect(gateSteps({ steps: [], tools: { pay: { approval: "person", approvalTimeout: { after: "1h", action: "deny" } } } }))
      .toEqual({ events: false, timeouts: true });
    expect(gateSteps({ steps: [], agents: { a: { askPerson: { maxQuestions: 2 } } } }))
      .toEqual({ events: false, timeouts: false });
  });

  it("is mirrored in Terraform", () => {
    const fs = require("fs");
    const tf = fs.readFileSync(require("path").join(__dirname, "..", "..", "terraform", "gates.tf"), "utf8");
    expect(tf).toContain('try(t.approval, "") == "event"');
    expect(tf).toContain("can(a.askPerson.timeout)");
    expect(tf).toContain("can(keys(t.approvalTimeout))");
  });
});
