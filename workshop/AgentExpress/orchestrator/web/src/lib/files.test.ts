import { afterEach, describe, expect, it, vi } from "vitest";

import { fmtSize, parseCsv, prettyJson, readText } from "./files";

describe("reading a CSV", () => {
  it("handles quotes, doubled quotes, commas and newlines inside a field", () => {
    expect(parseCsv('name,note\r\n"Smith, J","said ""hi""\nthen left"\nLee,ok\n'))
      .toEqual([["name", "note"], ["Smith, J", 'said "hi"\nthen left'], ["Lee", "ok"]]);
  });
  it("stops at the row limit, and drops a half line when the file was cut", () => {
    expect(parseCsv("a\n1\n2\n3\n", 2)).toEqual([["a"], ["1"]]);
    expect(parseCsv("a,b\n1,2\n3,", 10, true)).toEqual([["a", "b"], ["1", "2"]]);
    expect(parseCsv("a,b\n1,2", 10, false)).toEqual([["a", "b"], ["1", "2"]]);
    expect(parseCsv("\uFEFFx\n")).toEqual([["x"]]);
  });
});

describe("showing a file", () => {
  it("says sizes plainly", () => {
    expect(fmtSize(512)).toBe("512 B");
    expect(fmtSize(25_400)).toBe("25 KB");
    expect(fmtSize(3_240_000)).toBe("3.2 MB");
  });
  it("pretty-prints JSON only when all of it was read", () => {
    expect(prettyJson('{"a":1}', false)).toBe('{\n  "a": 1\n}');
    expect(prettyJson('{"a":', false)).toBe('{"a":');
    expect(prettyJson('{"a":1}', true)).toBe('{"a":1}');
  });
});

describe("reading text through the link", () => {
  afterEach(() => vi.unstubAllGlobals());
  it("asks for the first bytes only, and says when there was more", async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response("hello", {
      status: 206, headers: { "Content-Range": "bytes 0-4/900000" },
    }));
    vi.stubGlobal("fetch", fetchMock);
    const got = await readText("https://bucket.s3/x?sig", 5);
    expect(fetchMock).toHaveBeenCalledWith("https://bucket.s3/x?sig", { headers: { Range: "bytes=0-4" } });
    expect(got).toEqual({ text: "hello", cut: true });
  });
  it("reads a small file whole", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response("hi", { status: 200 })));
    expect(await readText("u")).toEqual({ text: "hi", cut: false });
  });
  it("fails with the status when the link does not read", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response("", { status: 403 })));
    await expect(readText("u")).rejects.toThrow("403");
  });
});
