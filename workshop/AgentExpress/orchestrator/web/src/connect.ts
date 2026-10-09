/** Back from connecting an account for a tool that uses each person's own account
 *  (auth "user"): AgentCore Identity sends the browser here with ?session_id=<the
 *  session it opened>, and the app binds it to whoever is signed in (POST /api/connect),
 *  which is what lets the tool act as them. */
import { api } from "./api";

const SESSION = /^urn:ietf:params:oauth:request_uri:[A-Za-z0-9._~-]+$/;

/** null when this page load is not such a return; else what happened. */
export async function completeConnection(): Promise<{ ok: boolean; message: string } | null> {
  const params = new URLSearchParams(location.search);
  const session = params.get("session_id") ?? "";
  if (!SESSION.test(session)) return null;
  params.delete("session_id");
  const rest = params.toString();
  history.replaceState({}, document.title, location.pathname + (rest ? `?${rest}` : "") + location.hash);
  try {
    await api.post("/api/connect", { sessionUri: session });
    return { ok: true, message: "Account connected. Go back to the run and run the step again." };
  } catch (e) {
    return { ok: false, message: (e as Error).message || "That connection could not be completed." };
  }
}
