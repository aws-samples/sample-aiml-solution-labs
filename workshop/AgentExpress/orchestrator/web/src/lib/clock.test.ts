import { describe, expect, it } from "vitest";

import { duration, etToEpochMs, fmtTime } from "./clock";

describe("clock", () => {
  it("reads a stored Eastern stamp as the right instant, either side of DST", () => {
    expect(new Date(etToEpochMs("2026-01-15 09:00:00")).toISOString()).toBe("2026-01-15T14:00:00.000Z");
    expect(new Date(etToEpochMs("2026-07-15 09:00:00")).toISOString()).toBe("2026-07-15T13:00:00.000Z");
    // 2026: DST from Sun 8 March to Sun 1 November.
    expect(new Date(etToEpochMs("2026-03-08 03:00:00")).toISOString()).toBe("2026-03-08T07:00:00.000Z");
    expect(new Date(etToEpochMs("2026-11-01 03:00:00")).toISOString()).toBe("2026-11-01T08:00:00.000Z");
  });

  it("shows a stamp in the zone asked for", () => {
    expect(fmtTime("2026-07-15 09:00:00", "UTC")).toBe("2026-07-15 13:00:00");
    expect(fmtTime("2026-07-15 09:00:00#0003", "Europe/Berlin")).toBe("2026-07-15 15:00:00");
    expect(fmtTime("2026-07-15T13:00:00Z", "America/New_York")).toBe("2026-07-15 09:00:00");
    expect(fmtTime(1784120400, "UTC")).toBe("2026-07-15 13:00:00");
    expect(fmtTime("")).toBe("");
  });

  it("measures a run across a DST change correctly", () => {
    // 01:30 EST -> 03:30 EDT on 8 March 2026 is one real hour, not two.
    expect(duration("2026-03-08 01:30:00", "2026-03-08 03:30:00")).toBe("1h 0m");
    expect(duration("2026-07-15 09:00:00", "2026-07-15 09:00:42")).toBe("42s");
    expect(duration("2026-07-15 09:00:00", "garbage")).toBe("—");
  });
});
