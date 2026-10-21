import type { Me } from "../types";

/** The product demo video a control-plane console links to (GET /api/me demoVideoUrl, from
 *  -c demoVideoUrl / var.demo_video_url), or undefined: on a build's own app, when none is
 *  set, or when the value is not an https URL. The IaC already refuses anything else; this
 *  keeps a bad value from ever becoming a link. */
export function demoVideoUrl(me: Pick<Me, "consoleMode" | "demoVideoUrl">): string | undefined {
  if (me.consoleMode !== "builder" || !me.demoVideoUrl) return undefined;
  try {
    return new URL(me.demoVideoUrl).protocol === "https:" ? me.demoVideoUrl : undefined;
  } catch {
    return undefined;
  }
}
