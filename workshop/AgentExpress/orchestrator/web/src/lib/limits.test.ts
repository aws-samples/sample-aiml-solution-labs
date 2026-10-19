import { describe, expect, it } from "vitest";
import { describeUsage, usd } from "./limits";
import type { LimitUse } from "../types";

const use = (used: number, limit: number, over: Partial<LimitUse> = {}): LimitUse => ({
  used, limit, resets: "midnight Eastern time", warn: used / limit >= 0.8, reached: used >= limit, ...over });

describe("describeUsage", () => {
  it("says nothing when no limit is set, or none could be read", () => {
    expect(describeUsage(null)).toBeNull();
    expect(describeUsage(undefined)).toBeNull();
    expect(describeUsage({ limits: false })).toBeNull();
    expect(describeUsage({ limits: true })).toBeNull();                    // set, but every counter unreadable
  });

  it("shows each limit that is set, in the order the server checks them", () => {
    const n = describeUsage({ limits: true, hourly: use(3, 20), daily: use(1.2, 5), monthly: use(50, 200) })!;
    expect(n.lines).toEqual(["This month, everyone: $50.00 of $200.00", "Today: $1.20 of $5.00", "This hour: 3 of 20 runs"]);
    expect(n.alert).toBeUndefined();
    expect(n.blocked).toBe(false);
  });

  it("warns, with the share used and when it resets, once a limit is close", () => {
    const n = describeUsage({ limits: true, daily: use(4.5, 5) })!;
    expect(n.alert).toEqual({ type: "warning", text: "You have used 90% of today's spend limit. It resets at midnight Eastern time." });
    expect(n.blocked).toBe(false);
  });

  it("blocks, with the reason, when a limit is reached", () => {
    const n = describeUsage({ limits: true, hourly: use(20, 20, { resets: "15:00 Eastern time" }) })!;
    expect(n.alert).toEqual({ type: "error", text: "You have started this hour's runs. It resets at 15:00 Eastern time." });
    expect(n.blocked).toBe(true);
  });

  it("names the limit the server would: reached beats close, and the app's month comes first", () => {
    const n = describeUsage({ limits: true, monthly: use(200, 200, { resets: "the 1st, Eastern time" }),
      daily: use(5, 5), hourly: use(19, 20) })!;
    expect(n.alert!.type).toBe("error");
    expect(n.alert!.text).toContain("monthly");
    // A close limit never hides a reached one that comes after it.
    const m = describeUsage({ limits: true, monthly: use(170, 200), daily: use(5, 5) })!;
    expect(m.alert!.type).toBe("error");
    expect(m.alert!.text).toContain("Today's");
  });

  it("never reports more than 100% used", () => {
    const n = describeUsage({ limits: true, daily: use(9, 5, { reached: false, warn: true }) })!;
    expect(n.alert!.text).toContain("100%");
  });

  it("ignores a limit of zero", () => {
    expect(describeUsage({ limits: true, daily: { limit: 0, used: 0, resets: "x", warn: false, reached: false } })).toBeNull();
  });
});

describe("usd", () => {
  it("uses cents from a dollar up and four places below, as the server does", () => {
    expect(usd(5)).toBe("$5.00");
    expect(usd(1234.5)).toBe("$1,234.50");
    expect(usd(0.0042)).toBe("$0.0042");
    expect(usd(0)).toBe("$0.00");
  });
});
