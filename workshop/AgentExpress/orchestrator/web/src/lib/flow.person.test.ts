/** The Flow tab's events for an agent asking a person (telemetry rows of kind "person",
 *  app/features/observability/meter.py record_person). */
import { describe, expect, it } from "vitest";
import { buildEvents, railOf } from "./flow";

const nameOf = (id: string) => (id === "triage" ? "Triage" : id);

describe("a person asked mid-step", () => {
  it("reads as a question asked and answered, on the HITL lane", () => {
    const ev = buildEvents([
      { agent_id: "triage", kind: "person", label: "question", status: "asked", ts: "1" },
      { agent_id: "triage", kind: "person", label: "question", status: "answered", ts: "2" },
    ], nameOf);
    expect(ev.map((e) => e.sentence)).toEqual([
      "Triage paused to ask HITL a question.",
      "HITL answered Triage's question, and it carried on.",
    ]);
    expect(ev.every((e) => e.component === "HITL" && e.tone === "ok")).toBe(true);
  });

  it("marks an unanswered question and a denied call as a stop", () => {
    const ev = buildEvents([
      { agent_id: "triage", kind: "person", label: "question", status: "unanswered", ts: "1" },
      { agent_id: "triage", kind: "person", label: "payments___pay", status: "waiting", ts: "2" },
      { agent_id: "triage", kind: "person", label: "payments___pay", status: "denied", ts: "3" },
    ], nameOf);
    expect(ev[1].sentence).toBe("Triage asked HITL to approve a call to `payments___pay`.");
    expect(ev[2].sentence).toBe("Triage's call to `payments___pay` was not approved, so it was not made.");
    expect(ev.map((e) => e.tone)).toEqual(["stop", "ok", "stop"]);
    const person = railOf(ev).find((r) => r.component === "HITL");
    expect(person).toMatchObject({ count: 3, stopped: 2 });
  });
});
