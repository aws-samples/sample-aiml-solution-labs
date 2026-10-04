/** An image agent's asset shows its image, fetched through the BFF's owner-checked link. */
import { render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

const get = vi.fn();
vi.mock("../api", () => ({ api: { get: (p: string) => get(p) } }));

import { AssetView } from "./AssetView";

describe("an image agent's asset", () => {
  it("shows the image it rendered, with its caption as the alt text", async () => {
    get.mockResolvedValue({ url: "https://bucket.s3.amazonaws.com/runs/s1/hero/1.png?sig" });
    const asset = { summary: "Dawn over the coast", brief: { prompt: "a lighthouse", caption: "Dawn over the coast" },
      images: [{ imageKey: "runs/s1/hero/1.png", model: "stability.sd3-5-large-v1:0", seed: 42 }] };
    render(<AssetView text={JSON.stringify(asset)} />);
    await waitFor(() => expect(screen.getByRole("img")).toBeTruthy());
    expect(get).toHaveBeenCalledWith("/api/images?key=runs%2Fs1%2Fhero%2F1.png");
    expect((screen.getByRole("img") as HTMLImageElement).src).toContain("runs/s1/hero/1.png");
    expect(screen.getByText(/seed 42/)).toBeTruthy();
  });
});
