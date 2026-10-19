"""
Gateway Lambda target: open a web page in the AWS-managed AgentCore Browser, as a tool.

The third BUILT-IN `type: "lambda"` function (`source: "browser"`), next to
`app/tools/pricing/` and `app/tools/codeInterpreter/`. It publishes ONE tool:

  open_page(url) - open `url` and return the page's title, its visible text and its links.

HOW THE BROWSER IS DRIVEN, AND WHY THIS WAY
The AgentCore Browser is driven over its automation stream: a WebSocket that speaks the
Chrome DevTools Protocol (CDP), signed with SigV4 like any AgentCore call. The
`InvokeBrowser` API only clicks, types and takes screenshots at the operating-system
level; it cannot navigate or read a page. The usual clients (Playwright, browser-use,
`websockets`) are not in the Lambda runtime and this function ships as a plain zip of
this folder, so it speaks the few CDP commands it needs over a small RFC 6455 client
written on the standard library (`_WebSocket` below).
  https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/browser-resource-session-management.html
  https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/browser-invoke.html

WHAT IT OPENS
A URL a MODEL chose, often one it read on another page. So, in this file and not in a
prompt:

  * only http:// and https://, no user:password@, at most MAX_URL_CHARS;
  * the host must not be, or resolve to, a non-public address (the netguard copy here),
    checked before a session starts AND for the page the browser ended on, since a
    redirect can go anywhere;
  * with ALLOW_DOMAINS set (the tool's `allowDomains`), the host must be one of those
    domains or under one, again for both the asked-for and the final page;
  * every call starts a session and ALWAYS stops it; the page load, the whole call and
    the text and links returned are all capped.

The browser runs in AWS's network, not this deployment's VPC, so a private address
there is not one of yours; the address check is there so a model cannot use the tool to
probe whatever is local to the browser. It is not a filter on everything the page loads:
scripts, images and frames are fetched by the browser itself.

Invocation contract (identical to app/tools/pricing/handler.py): the Gateway passes the
tool arguments as `event`, and the tool name as
`context.client_context.custom["bedrockAgentCoreToolName"]`, formatted
"<targetName>___<toolName>".
"""

import base64
import hashlib
import json
import os
import re
import socket
import ssl
import struct
import time
from urllib.parse import urlsplit

import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.config import Config

import netguard  # this folder's copy of app/common/netguard.py (the zip holds only this folder)

REGION = os.environ.get("AWS_REGION", "us-east-1")

# The AWS-managed browser, which every account has and nothing has to provision. A
# constant on purpose: the execution role may start and stop sessions on exactly this
# browser (terraform/browser_tool.tf, cdk/lib/browser-tool.ts).
BROWSER = "aws.browser.v1"
# The AgentCore data-plane host the automation stream is served from. Fixed by the
# Region, never taken from a response or an argument, so the signed connection can only
# ever go to AWS.
STREAM_HOST = f"bedrock-agentcore.{REGION}.amazonaws.com"

# --- Hard limits ------------------------------------------------------------
MAX_URL_CHARS = 2048
PAGE_LOAD_SECONDS = 20       # waiting for the page's load event
CALL_SECONDS = 40            # the whole call after the session starts; the Lambda's timeout is 60
MAX_TEXT_CHARS = 20_000      # of the page's visible text
MAX_LINKS = 50
MAX_LINK_TEXT = 200
MAX_MESSAGE_BYTES = 4 * 1024 * 1024   # one CDP message; the page text is cut in the browser first
MAX_HANDSHAKE_BYTES = 16 * 1024
CONNECT_SECONDS = 10
# How long the service keeps a session whose stop call failed. The finally below stops
# it in every normal case; this only bounds the abnormal one.
SESSION_TTL_SECONDS = 120

#: A domain in ALLOW_DOMAINS: lower-case labels and a TLD, no scheme, port, path or
#: wildcard. The same expression as bff/browser_tool.py and web/src/builder/browserTool.ts.
DOMAIN_RE = re.compile(r"^(?=.{1,253}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+(?:[a-z]{2,63}|xn--[a-z0-9-]{1,59})$")
SESSION_ID_RE = re.compile(r"^[0-9a-zA-Z]{1,40}$")   # the service's BrowserSessionId pattern
_WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"   # RFC 6455 section 1.3

_client = boto3.client(
    "bedrock-agentcore",
    region_name=REGION,
    # No retries: a retried start would leave a second session running until it expires.
    config=Config(retries={"total_max_attempts": 1}, connect_timeout=5, read_timeout=15),
)

# What the page is read with: title, visible text and links, cut in the browser so a huge
# page never crosses the wire whole. `+ 1` on the text so the caller can tell it was cut.
_READ_PAGE = """(() => {
  const text = (document.body && document.body.innerText) || "";
  const links = [];
  for (const a of document.querySelectorAll("a[href]")) {
    if (links.length >= %(links)d) break;
    const href = String(a.href || "");
    if (!/^https?:/i.test(href)) continue;
    links.push({text: String(a.innerText || "").trim().slice(0, %(link_text)d), href: href.slice(0, %(url)d)});
  }
  return {url: String(location.href), title: String(document.title || ""), text: text.slice(0, %(text)d), links};
})()""" % {"links": MAX_LINKS, "link_text": MAX_LINK_TEXT, "url": MAX_URL_CHARS, "text": MAX_TEXT_CHARS + 1}


class Refused(Exception):
    """A URL this tool will not open. Its message is the answer the model gets."""


class _Timeout(Exception):
    pass


# --- which pages may be opened ------------------------------------------------

def allowed_domains() -> list:
    """ALLOW_DOMAINS as a list; [] means any public host. Raises Refused on a malformed
    entry: a list that cannot be read must not silently become "anything"."""
    raw = os.environ.get("ALLOW_DOMAINS", "")
    domains = [d.strip() for d in raw.split(",") if d.strip()]
    bad = [d for d in domains if not DOMAIN_RE.match(d)]
    if bad:
        raise Refused(f"this tool's allowDomains setting is not a list of domain names ({', '.join(bad)}), "
                      "so it opens nothing until that is fixed.")
    return domains


def _host_of(url: str) -> tuple:
    """(scheme, ASCII host, port) of a URL this tool may open. Raises Refused."""
    if not isinstance(url, str) or not url.strip():
        raise Refused("`url` is required: the page to open, e.g. https://docs.aws.amazon.com/.")
    url = url.strip()
    if len(url) > MAX_URL_CHARS:
        raise Refused(f"`url` is {len(url)} characters; the limit is {MAX_URL_CHARS}.")
    try:
        parts = urlsplit(url)
        port = parts.port
    except ValueError as e:
        raise Refused(f"`url` is not a URL: {e}") from e
    scheme = parts.scheme.lower()
    if scheme not in ("http", "https"):
        raise Refused(f"only http:// and https:// pages are opened, not {scheme or 'none'}://")
    if parts.username is not None or parts.password is not None or "@" in parts.netloc:
        raise Refused("a URL with user:password@ in it is not opened")
    host = (parts.hostname or "").rstrip(".")
    if not host:
        raise Refused("the URL has no host")
    try:
        host = host.encode("idna").decode("ascii").lower()
    except UnicodeError as e:
        raise Refused(f"the host {host!r} is not a valid name") from e
    return scheme, host, port or (443 if scheme == "https" else 80)


def check_url(url: str, domains: list) -> str:
    """The URL, stripped, if it may be opened. Raises Refused saying why not."""
    _scheme, host, port = _host_of(url)
    if domains and not any(host == d or host.endswith("." + d) for d in domains):
        raise Refused(f"{host} is not one of the domains this tool may open: {', '.join(domains)}")
    try:
        netguard.resolve(host, port)
    except netguard.BlockedURL as e:
        raise Refused(str(e.reason)) from e
    except OSError as e:  # URLError for a name that does not resolve
        raise Refused(f"{host} could not be resolved: {getattr(e, 'reason', e)}") from e
    return url.strip()


# --- a minimal WebSocket client (RFC 6455), enough for CDP ----------------------

class _WebSocket:
    """Text messages over an already-connected socket: masked frames out, unmasked in,
    fragments joined, pings answered, a close ends it. Nothing else is needed for CDP.

    A frame is taken off the buffer only once all of it has arrived, and a message's
    fragments are kept on the object, so a read that times out (waiting for a page's load
    event) leaves the stream where it was and the next read carries on from there."""

    def __init__(self, sock, leftover: bytes = b""):
        self._sock = sock
        self._buf = leftover
        self._parts: list = []
        self._size = 0

    def _fill(self, n: int, deadline: float) -> None:
        """Wait until at least `n` bytes are buffered."""
        while len(self._buf) < n:
            left = deadline - time.monotonic()
            if left <= 0:
                raise _Timeout()
            self._sock.settimeout(left)
            try:
                chunk = self._sock.recv(65536)
            except socket.timeout as e:
                raise _Timeout() from e
            if not chunk:
                raise ConnectionError("the browser's automation stream closed")
            self._buf += chunk

    def _frame(self, deadline: float) -> tuple:
        """(fin, opcode, payload) of the next whole frame."""
        self._fill(2, deadline)
        b0, b1 = self._buf[0], self._buf[1]
        n, at = b1 & 0x7F, 2
        if n == 126:
            self._fill(4, deadline)
            n, at = struct.unpack("!H", self._buf[2:4])[0], 4
        elif n == 127:
            self._fill(10, deadline)
            n, at = struct.unpack("!Q", self._buf[2:10])[0], 10
        if self._size + n > MAX_MESSAGE_BYTES:
            raise ConnectionError(f"a message from the browser is over {MAX_MESSAGE_BYTES} bytes")
        masked = 4 if b1 & 0x80 else 0
        self._fill(at + masked + n, deadline)
        mask = self._buf[at:at + masked]
        data = self._buf[at + masked:at + masked + n]
        self._buf = self._buf[at + masked + n:]
        if mask:
            data = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
        return b0 & 0x80, b0 & 0x0F, data

    def _send_frame(self, opcode: int, payload: bytes) -> None:
        n = len(payload)
        head = bytes([0x80 | opcode])
        if n < 126:
            head += bytes([0x80 | n])
        elif n < 65536:
            head += bytes([0x80 | 126]) + struct.pack("!H", n)
        else:
            head += bytes([0x80 | 127]) + struct.pack("!Q", n)
        mask = os.urandom(4)   # a client MUST mask every frame (RFC 6455 section 5.3)
        self._sock.sendall(head + mask + bytes(b ^ mask[i % 4] for i, b in enumerate(payload)))

    def send(self, text: str) -> None:
        self._send_frame(0x1, text.encode("utf-8"))

    def recv(self, deadline: float) -> str:
        while True:
            fin, opcode, data = self._frame(deadline)
            if opcode == 0x9:       # ping: answer it, keep reading
                self._send_frame(0xA, data)
                continue
            if opcode == 0xA:       # pong
                continue
            if opcode == 0x8:       # close
                raise ConnectionError("the browser's automation stream closed")
            self._parts.append(data)
            self._size += len(data)
            if fin:
                text = b"".join(self._parts).decode("utf-8", errors="replace")
                self._parts, self._size = [], 0
                return text

    def close(self) -> None:
        try:
            self._send_frame(0x8, struct.pack("!H", 1000))
        except OSError:
            pass
        try:
            self._sock.close()
        except OSError:
            pass


def _signed_headers(host: str, path: str) -> dict:
    """SigV4 headers for the stream's upgrade request, made with this function's role, the
    way the AgentCore SDK's BrowserClient.generate_ws_headers makes them."""
    creds = boto3.Session().get_credentials()
    if creds is None:
        raise RuntimeError("no AWS credentials to sign the browser connection with")
    req = AWSRequest(method="GET", url=f"https://{host}{path}", headers={"host": host})
    SigV4Auth(creds.get_frozen_credentials(), "bedrock-agentcore", REGION).add_auth(req)
    out = {"Host": host, "X-Amz-Date": req.headers["X-Amz-Date"], "Authorization": req.headers["Authorization"]}
    if req.headers.get("X-Amz-Security-Token"):
        out["X-Amz-Security-Token"] = req.headers["X-Amz-Security-Token"]
    return out


def handshake(sock, host: str, path: str, headers: dict, deadline: float) -> _WebSocket:
    """Upgrade `sock` to a WebSocket on `path`. Raises ConnectionError unless the server
    answers 101 with the Sec-WebSocket-Accept this key calls for."""
    key = base64.b64encode(os.urandom(16)).decode()
    lines = [f"GET {path} HTTP/1.1", *(f"{k}: {v}" for k, v in headers.items()),
             "Upgrade: websocket", "Connection: Upgrade", f"Sec-WebSocket-Key: {key}",
             "Sec-WebSocket-Version: 13", "", ""]
    sock.sendall("\r\n".join(lines).encode())
    buf = b""
    while b"\r\n\r\n" not in buf:
        left = deadline - time.monotonic()
        if left <= 0:
            raise _Timeout()
        if len(buf) > MAX_HANDSHAKE_BYTES:
            raise ConnectionError("the browser's automation stream sent an oversized handshake")
        sock.settimeout(left)
        try:
            chunk = sock.recv(4096)
        except socket.timeout as e:
            raise _Timeout() from e
        if not chunk:
            raise ConnectionError("the browser's automation stream closed during the handshake")
        buf += chunk
    head, leftover = buf.split(b"\r\n\r\n", 1)
    status, *fields = head.decode("latin-1").split("\r\n")
    if len(status.split()) < 2 or status.split()[1] != "101":
        raise ConnectionError(f"the browser's automation stream refused the connection: {status[:200]}")
    got = {k.strip().lower(): v.strip() for k, _, v in (f.partition(":") for f in fields)}
    # RFC 6455 section 4.2.2 fixes SHA-1 for Sec-WebSocket-Accept: a handshake check, not a security hash.
    want = base64.b64encode(hashlib.sha1((key + _WS_GUID).encode(), usedforsecurity=False).digest()).decode()  # nosec B324
    if got.get("sec-websocket-accept") != want:
        raise ConnectionError("the browser's automation stream answered with the wrong Sec-WebSocket-Accept")
    return _WebSocket(sock, leftover)


class CDP:
    """Chrome DevTools Protocol calls over one WebSocket: one command at a time, events
    remembered by (method, session) so a wait can find one that already arrived."""

    def __init__(self, ws):
        self._ws = ws
        self._id = 0
        self._seen: set = set()

    def call(self, method: str, params: dict, deadline: float, session: str = "") -> dict:
        self._id += 1
        msg = {"id": self._id, "method": method, "params": params}
        if session:
            msg["sessionId"] = session
        self._ws.send(json.dumps(msg))
        while True:
            reply = json.loads(self._ws.recv(deadline))
            if reply.get("id") == self._id:
                if "error" in reply:
                    raise RuntimeError(f"{method}: {(reply['error'] or {}).get('message', reply['error'])}")
                return reply.get("result") or {}
            if "method" in reply:
                self._seen.add((reply["method"], reply.get("sessionId", "")))

    def wait_for(self, method: str, deadline: float, session: str = "") -> bool:
        """True once `method` has arrived for `session`; False if `deadline` passes first."""
        while (method, session) not in self._seen:
            try:
                reply = json.loads(self._ws.recv(deadline))
            except _Timeout:
                return False
            if "method" in reply:
                self._seen.add((reply["method"], reply.get("sessionId", "")))
        return True

    def close(self) -> None:
        self._ws.close()


def _open_stream(session_id: str, deadline: float) -> CDP:
    """A CDP connection to session `session_id`'s automation stream."""
    if not SESSION_ID_RE.match(session_id or ""):
        raise RuntimeError("the browser returned a session id this function will not put in a URL")
    path = f"/browser-streams/{BROWSER}/sessions/{session_id}/automation"
    raw = socket.create_connection((STREAM_HOST, 443), timeout=CONNECT_SECONDS)
    try:
        sock = ssl.create_default_context().wrap_socket(raw, server_hostname=STREAM_HOST)
        return CDP(handshake(sock, STREAM_HOST, path, _signed_headers(STREAM_HOST, path), deadline))
    except BaseException:
        raw.close()
        raise


# --- the tool -------------------------------------------------------------------

def _read_page(cdp: CDP, url: str, deadline: float) -> dict:
    target = cdp.call("Target.createTarget", {"url": "about:blank"}, deadline)["targetId"]
    page = cdp.call("Target.attachToTarget", {"targetId": target, "flatten": True}, deadline)["sessionId"]
    cdp.call("Page.enable", {}, deadline, page)
    nav = cdp.call("Page.navigate", {"url": url}, deadline, page)
    if nav.get("errorText"):
        return {"error": f"the page did not open: {nav['errorText']}"}
    if nav.get("isDownload"):
        return {"error": "that URL is a file download, not a page; this tool only reads pages."}
    loaded = cdp.wait_for("Page.loadEventFired", min(deadline, time.monotonic() + PAGE_LOAD_SECONDS), page)
    got = cdp.call("Runtime.evaluate", {"expression": _READ_PAGE, "returnByValue": True}, deadline, page)
    if got.get("exceptionDetails"):
        return {"error": "the page could not be read: "
                         + str((got["exceptionDetails"] or {}).get("text") or "a script error")[:300]}
    value = (got.get("result") or {}).get("value") or {}
    return {"loaded": loaded, **(value if isinstance(value, dict) else {})}


def open_page(args: dict) -> dict:
    """Open a page in a fresh browser session and return its title, text and links."""
    try:
        domains = allowed_domains()
        url = check_url(args.get("url"), domains)
    except Refused as e:
        # Refused BEFORE a session starts: a refused call costs nothing.
        return {"error": str(e)}

    started = time.monotonic()
    sid = _client.start_browser_session(
        browserIdentifier=BROWSER, name="ax-open-page",
        sessionTimeoutSeconds=SESSION_TTL_SECONDS)["sessionId"]
    cdp = None
    try:
        deadline = time.monotonic() + CALL_SECONDS
        cdp = _open_stream(sid, deadline)
        page = _read_page(cdp, url, deadline)
    except _Timeout:
        page = {"error": f"the page took longer than {CALL_SECONDS} seconds and was abandoned."}
    finally:
        if cdp is not None:
            cdp.close()
        # ALWAYS, including when anything above raised: a session left open is billed
        # until it expires.
        try:
            _client.stop_browser_session(browserIdentifier=BROWSER, sessionId=sid)
        except Exception as e:  # noqa: BLE001 - it expires by itself (SESSION_TTL_SECONDS)
            print(f"[browser] stop session failed: {type(e).__name__}: {e}")

    seconds = round(time.monotonic() - started, 2)
    if "error" in page:
        return {"url": url, "error": page["error"], "seconds": seconds}
    final = str(page.get("url") or url)
    try:
        # Where the browser ENDED UP, which a redirect decides: held to the same rules.
        check_url(final, domains)
    except Refused as e:
        return {"url": url, "error": f"the page redirected to {final[:MAX_URL_CHARS]}, which this tool may not "
                                     f"open ({e}); nothing from it is returned.", "seconds": seconds}
    text = str(page.get("text") or "")
    links = [{"text": str(link.get("text") or "")[:MAX_LINK_TEXT], "href": str(link.get("href") or "")[:MAX_URL_CHARS]}
             for link in (page.get("links") or [])[:MAX_LINKS] if isinstance(link, dict)]
    result = {"url": final, "title": str(page.get("title") or "")[:500],
              "text": text[:MAX_TEXT_CHARS], "links": links, "seconds": seconds}
    notes = []
    if len(text) > MAX_TEXT_CHARS:
        result["truncated"] = True
        notes.append(f"Text cut at {MAX_TEXT_CHARS} characters; open a more specific page for the rest.")
    if not page.get("loaded"):
        notes.append(f"The page had not finished loading after {PAGE_LOAD_SECONDS} seconds; this is what it showed then.")
    if notes:
        result["note"] = " ".join(notes)
    return result


# Tool name -> implementation. `call` in workflow.json says which one the bound agent invokes.
_TOOLS = {"open_page": open_page}


def _tool_name(context) -> str:
    """The tool the Gateway is asking for, minus the "<target>___" prefix."""
    try:
        full = context.client_context.custom.get("bedrockAgentCoreToolName", "")
    except Exception:  # noqa: BLE001 - a direct test invoke has no client_context
        return ""
    return full.rsplit("___", 1)[-1] if full else ""


def lambda_handler(event, context):
    tool = _tool_name(context) or ("open_page" if len(_TOOLS) == 1 else "")
    fn = _TOOLS.get(tool)
    if fn is None:
        return {"error": f"unknown tool {tool!r}", "availableTools": sorted(_TOOLS)}
    try:
        return fn(event or {})
    except Exception as e:  # noqa: BLE001 - surface the reason, never a fake answer
        print(f"[browser] {tool} failed: {type(e).__name__}: {e}")
        return {"error": f"{tool} failed: {type(e).__name__}: {e}"}
