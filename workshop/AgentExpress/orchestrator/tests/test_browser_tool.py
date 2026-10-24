"""The built-in Browser tool (orchestrator/app/tools/browser/handler.py).

`source: "browser"` opens a web page in the AWS-managed AgentCore Browser and returns its
title, text and links. Nothing here reaches AWS: the session calls go to a fake client whose
inputs are validated against botocore's own model of the service, the page is driven
through a fake CDP connection, and the small WebSocket client is run against a fake server
on a socketpair (no listening socket). So these establish the function's own logic, its
wire format and the shape of its IAM, not that a deployed browser behaves this way.

What has to hold:

  * a normal call opens the page in a new tab and returns its title, text and links;
  * the session is stopped on EVERY path after it started;
  * a URL that is not plain http(s), carries user:password@, is too long, or whose host is
    or resolves to a non-public address is refused before any session starts;
  * allowDomains admits a domain and the names under it, nothing else, and a malformed
    list refuses everything;
  * the page the browser ENDED on (after redirects) is held to the same rules;
  * text and links are capped, and a cut says so;
  * the WebSocket frames are RFC 6455 (masked out, fragments joined, pings answered), the
    handshake is checked, and a read that times out mid-frame loses nothing;
  * the role may start and stop sessions on one browser and connect to an automation
    stream, identically on both IaC paths.
"""
from __future__ import annotations

import ast
import base64
import hashlib
import importlib.util
import ipaddress
import json
import re
import socket
import struct
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import botocore.session
import pytest
from botocore.credentials import Credentials
from botocore.validate import validate_parameters

ORCH = Path(__file__).resolve().parent.parent
TOOL_DIR = ORCH / "app" / "tools" / "browser"
HANDLER_PATH = TOOL_DIR / "handler.py"
MODEL = botocore.session.get_session().get_service_model("bedrock-agentcore")
GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def _valid(op: str, params: dict) -> None:
    """Raise as botocore would for a request the real API model rejects."""
    validate_parameters(params, MODEL.operation_model(op).input_shape)


class FakeClient:
    """StartBrowserSession / StopBrowserSession, inputs checked against botocore's model."""

    def __init__(self, start_error=None, stop_error=None):
        self.start_error = start_error
        self.stop_error = stop_error
        self.started: list = []
        self.stopped: list = []
        self.open: set = set()

    def start_browser_session(self, **kw):
        _valid("StartBrowserSession", kw)
        if self.start_error:
            raise self.start_error
        sid = f"sess{len(self.started) + 1}"
        self.started.append(kw)
        self.open.add(sid)
        return {"browserIdentifier": kw["browserIdentifier"], "sessionId": sid,
                "createdAt": "2026-01-01T00:00:00Z", "streams": {}}

    def stop_browser_session(self, **kw):
        _valid("StopBrowserSession", kw)
        self.stopped.append(kw)
        if self.stop_error:
            raise self.stop_error
        self.open.discard(kw["sessionId"])
        return {"browserIdentifier": kw["browserIdentifier"], "sessionId": kw["sessionId"]}


PAGE = {"url": "https://docs.aws.amazon.com/x", "title": "Docs", "text": "Hello world",
        "links": [{"text": "Next", "href": "https://docs.aws.amazon.com/y"}]}


class FakeCDP:
    """The CDP calls open_page makes, answered from a script, recorded in order."""

    def __init__(self, page=None, loaded=True, navigate=None, evaluate=None, raise_on=None, raise_with=None):
        self.page = PAGE if page is None else page
        self.loaded = loaded
        self.navigate = navigate or {"frameId": "F1", "loaderId": "L1"}
        self.evaluate = evaluate
        self.raise_on = raise_on
        self.raise_with = raise_with or RuntimeError("CDP broke")
        self.calls: list = []
        self.closed = False

    def call(self, method, params, deadline, session=""):
        self.calls.append((method, params, session))
        if method == self.raise_on:
            raise self.raise_with
        if method == "Target.createTarget":
            return {"targetId": "T1"}
        if method == "Target.attachToTarget":
            return {"sessionId": "S1"}
        if method == "Page.navigate":
            return self.navigate
        if method == "Runtime.evaluate":
            return self.evaluate if self.evaluate is not None else {"result": {"type": "object", "value": self.page}}
        return {}

    def wait_for(self, method, deadline, session=""):
        self.calls.append(("wait", method, session))
        return self.loaded

    def close(self):
        self.closed = True


PUBLIC = "93.184.216.34"


@pytest.fixture()
def br(monkeypatch):
    """The handler module, loaded under its own name, with THIS folder's netguard copy and a
    DNS where every name is public unless a test says otherwise."""
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.delenv("ALLOW_DOMAINS", raising=False)
    before = sys.modules.pop("netguard", None)
    sys.path.insert(0, str(TOOL_DIR))
    try:
        spec = importlib.util.spec_from_file_location("ax_browser_handler", HANDLER_PATH)
        module = importlib.util.module_from_spec(spec)
        sys.modules["ax_browser_handler"] = module
        spec.loader.exec_module(module)
    finally:
        sys.path.remove(str(TOOL_DIR))
        sys.modules.pop("netguard", None)
        if before is not None:
            sys.modules["netguard"] = before
    dns = {}
    module.dns = dns

    def getaddrinfo(host, port, *a, **k):
        try:
            ip = str(ipaddress.ip_address(host))   # a literal resolves to itself
        except ValueError:
            ip = dns.get(host, PUBLIC)
        if ip is None:
            raise socket.gaierror(8, "nodename nor servname provided")
        fam = socket.AF_INET6 if ":" in ip else socket.AF_INET
        return [(fam, socket.SOCK_STREAM, 6, "", (ip, port))]
    monkeypatch.setattr(module.netguard.socket, "getaddrinfo", getaddrinfo)
    yield module
    sys.modules.pop("ax_browser_handler", None)


def _wire(br, client=None, cdp=None):
    br._client = client or FakeClient()
    br.cdp_fake = cdp or FakeCDP()
    br.opened = []
    br._open_stream = lambda sid, deadline: br.opened.append(sid) or br.cdp_fake
    return br._client, br.cdp_fake


# --- a normal call ---------------------------------------------------------------------

def test_a_page_is_opened_in_a_new_tab_and_read(br):
    _client, cdp = _wire(br)
    out = br.open_page({"url": "https://docs.aws.amazon.com/x"})
    assert out["title"] == "Docs" and out["text"] == "Hello world"
    assert out["url"] == "https://docs.aws.amazon.com/x"
    assert out["links"] == [{"text": "Next", "href": "https://docs.aws.amazon.com/y"}]
    assert "error" not in out and "truncated" not in out and "note" not in out
    assert [c[0] for c in cdp.calls] == ["Target.createTarget", "Target.attachToTarget", "Page.enable",
                                         "Page.navigate", "wait", "Runtime.evaluate"]
    assert cdp.calls[0][1] == {"url": "about:blank"}
    assert cdp.calls[1][1] == {"targetId": "T1", "flatten": True}
    # Everything after attaching goes to the page's own session.
    assert all(c[2] == "S1" for c in cdp.calls[2:])
    assert cdp.calls[3][1] == {"url": "https://docs.aws.amazon.com/x"}
    assert cdp.calls[5][1]["returnByValue"] is True
    assert cdp.closed


def test_the_session_is_on_the_system_browser_and_is_stopped(br):
    client, _ = _wire(br)
    br.open_page({"url": "https://example.com/"})
    assert client.started[0]["browserIdentifier"] == "aws.browser.v1"
    assert client.started[0]["sessionTimeoutSeconds"] == br.SESSION_TTL_SECONDS
    assert br.opened == ["sess1"]
    assert client.stopped == [{"browserIdentifier": "aws.browser.v1", "sessionId": "sess1"}]
    assert client.open == set()


def test_each_call_gets_its_own_session(br):
    client, _ = _wire(br)
    br.open_page({"url": "https://example.com/a"})
    br.open_page({"url": "https://example.com/b"})
    assert br.opened == ["sess1", "sess2"]
    assert len(client.stopped) == 2


def test_a_page_still_loading_is_read_as_it_is_and_says_so(br):
    _wire(br, cdp=FakeCDP(loaded=False))
    out = br.open_page({"url": "https://example.com/"})
    assert out["text"] == "Hello world"
    assert "had not finished loading" in out["note"]


def test_a_page_that_does_not_open_is_an_error_result(br):
    client, cdp = _wire(br, cdp=FakeCDP(navigate={"frameId": "F", "errorText": "net::ERR_NAME_NOT_RESOLVED"}))
    out = br.open_page({"url": "https://example.com/"})
    assert "net::ERR_NAME_NOT_RESOLVED" in out["error"]
    assert "Runtime.evaluate" not in [c[0] for c in cdp.calls]
    assert client.open == set()


def test_a_download_is_not_read(br):
    _wire(br, cdp=FakeCDP(navigate={"frameId": "F", "isDownload": True}))
    assert "file download" in br.open_page({"url": "https://example.com/file.zip"})["error"]


def test_a_script_error_reading_the_page_is_an_error_result(br):
    _wire(br, cdp=FakeCDP(evaluate={"result": {"type": "object"}, "exceptionDetails": {"text": "Uncaught"}}))
    assert "could not be read: Uncaught" in br.open_page({"url": "https://example.com/"})["error"]


# --- the session is always stopped ------------------------------------------------------

@pytest.mark.parametrize("method", ["Target.createTarget", "Page.navigate", "Runtime.evaluate"])
def test_the_session_is_stopped_when_a_cdp_call_fails(br, method):
    client, cdp = _wire(br, cdp=FakeCDP(raise_on=method))
    out = br.lambda_handler({"url": "https://example.com/"}, None)
    assert "CDP broke" in out["error"]
    assert len(client.stopped) == 1 and client.open == set()
    assert cdp.closed


def test_the_session_is_stopped_when_the_stream_cannot_be_opened(br):
    client, _ = _wire(br)

    def fail(sid, deadline):
        raise ConnectionError("refused")
    br._open_stream = fail
    out = br.lambda_handler({"url": "https://example.com/"}, None)
    assert "refused" in out["error"]
    assert len(client.stopped) == 1 and client.open == set()


def test_a_call_that_runs_out_of_time_is_stopped_and_says_so(br):
    client, _ = _wire(br, cdp=FakeCDP(raise_on="Runtime.evaluate", raise_with=br._Timeout()))
    out = br.open_page({"url": "https://example.com/"})
    assert f"longer than {br.CALL_SECONDS} seconds" in out["error"]
    assert len(client.stopped) == 1


def test_a_session_that_will_not_stop_does_not_hide_the_page(br):
    _wire(br, client=FakeClient(stop_error=RuntimeError("stop refused")))
    out = br.open_page({"url": "https://example.com/"})
    assert out["text"] == "Hello world"
    assert "error" not in out


def test_a_session_that_cannot_start_is_an_error_and_nothing_is_stopped(br):
    client, _ = _wire(br, client=FakeClient(start_error=RuntimeError("throttled")))
    out = br.lambda_handler({"url": "https://example.com/"}, None)
    assert "throttled" in out["error"]
    assert client.stopped == [] and br.opened == []


# --- what is refused before a session starts -------------------------------------------

@pytest.mark.parametrize("url, fragment", [
    (None, "`url` is required"),
    ("", "`url` is required"),
    ("   ", "`url` is required"),
    (7, "`url` is required"),
    ("ftp://example.com/x", "only http:// and https://"),
    ("file:///etc/passwd", "only http:// and https://"),
    ("javascript:alert(1)", "only http:// and https://"),
    ("chrome://settings", "only http:// and https://"),
    ("example.com/no-scheme", "only http:// and https://"),
    ("https://user:pw@example.com/", "user:password@"),
    ("https://@example.com/", "user:password@"),
    ("https:///path-only", "has no host"),
    ("https://example.com:99999/", "not a URL"),
    ("http://127.0.0.1/", "is loopback"),
    ("http://169.254.169.254/latest/meta-data/", "is link-local"),
    ("http://10.1.2.3/", "is private"),
    ("http://[::1]/", "is loopback"),
    ("http://[::ffff:10.0.0.1]/", "private"),
    ("http://100.64.0.1/", "CGNAT"),
    ("http://0.0.0.0/", "non-public address"),
])
def test_a_bad_url_is_refused_without_starting_a_session(br, url, fragment):
    client, _ = _wire(br)
    out = br.lambda_handler({"url": url}, None)
    assert fragment in out["error"], out
    assert client.started == [] and br.opened == []


def test_a_url_over_the_limit_is_refused(br):
    client, _ = _wire(br)
    out = br.open_page({"url": "https://example.com/" + "a" * br.MAX_URL_CHARS})
    assert "the limit is" in out["error"]
    assert client.started == []


def test_a_name_that_resolves_to_a_private_address_is_refused(br):
    client, _ = _wire(br)
    br.dns["intranet.example.com"] = "10.0.0.5"
    out = br.open_page({"url": "https://intranet.example.com/"})
    assert "non-public address" in out["error"]
    assert client.started == []


def test_a_name_that_does_not_resolve_is_refused(br):
    client, _ = _wire(br)
    br.dns["nowhere.example"] = None
    assert "could not be resolved" in br.open_page({"url": "https://nowhere.example/"})["error"]
    assert client.started == []


def test_the_host_is_checked_on_its_port(br, monkeypatch):
    seen = []
    monkeypatch.setattr(br.netguard, "resolve", lambda host, port, allow_private=False: seen.append((host, port)) or [])
    _wire(br)
    br.open_page({"url": "http://Example.COM./a"})
    br.open_page({"url": "https://example.com:8443/a"})
    assert seen[0] == ("example.com", 80) and ("example.com", 8443) in seen


# --- allowDomains -------------------------------------------------------------------------

@pytest.mark.parametrize("url", ["https://docs.aws.amazon.com/", "https://a.b.docs.aws.amazon.com/x",
                                 "http://DOCS.aws.amazon.com/", "https://example.org/"])
def test_an_allowed_domain_and_the_names_under_it_are_opened(br, monkeypatch, url):
    monkeypatch.setenv("ALLOW_DOMAINS", "docs.aws.amazon.com,example.org")
    client, _cdp = _wire(br, cdp=FakeCDP(page={**PAGE, "url": url}))
    out = br.open_page({"url": url})
    assert "error" not in out, out
    assert client.started


@pytest.mark.parametrize("url", ["https://aws.amazon.com/", "https://evildocs.aws.amazon.com/",
                                 "https://docs.aws.amazon.com.evil.test/", "https://example.org.evil.test/"])
def test_anything_else_is_refused_when_domains_are_listed(br, monkeypatch, url):
    monkeypatch.setenv("ALLOW_DOMAINS", "docs.aws.amazon.com,example.org")
    client, _ = _wire(br)
    out = br.open_page({"url": url})
    assert "is not one of the domains this tool may open" in out["error"]
    assert client.started == []


@pytest.mark.parametrize("raw", ["https://docs.aws.amazon.com", "*.example.com", "example.com/path",
                                 "example.com:443", "Example.com", "localhost"])
def test_a_malformed_allow_list_refuses_everything(br, monkeypatch, raw):
    monkeypatch.setenv("ALLOW_DOMAINS", f"example.org,{raw}")
    client, _ = _wire(br)
    out = br.open_page({"url": "https://example.org/"})
    assert "allowDomains setting is not a list of domain names" in out["error"]
    assert client.started == []


def test_an_empty_allow_list_is_any_public_host(br, monkeypatch):
    monkeypatch.setenv("ALLOW_DOMAINS", " , ")
    _wire(br)
    assert "error" not in br.open_page({"url": "https://example.com/"})


# --- where the browser ended up --------------------------------------------------------------

def test_a_redirect_to_a_domain_not_allowed_returns_nothing_from_it(br, monkeypatch):
    monkeypatch.setenv("ALLOW_DOMAINS", "example.org")
    client, _ = _wire(br, cdp=FakeCDP(page={**PAGE, "url": "https://elsewhere.test/secret", "text": "SECRET"}))
    out = br.open_page({"url": "https://example.org/go"})
    assert "redirected to https://elsewhere.test/secret" in out["error"]
    assert "SECRET" not in json.dumps(out)
    assert len(client.stopped) == 1


def test_a_redirect_to_a_private_address_returns_nothing_from_it(br):
    _wire(br, cdp=FakeCDP(page={**PAGE, "url": "http://169.254.169.254/latest/meta-data/", "text": "ROLE"}))
    out = br.open_page({"url": "https://example.com/go"})
    assert "which this tool may not open" in out["error"] and "link-local" in out["error"]
    assert "ROLE" not in json.dumps(out)


# --- limits on what comes back ----------------------------------------------------------------

def test_text_over_the_limit_is_cut_and_says_so(br):
    _wire(br, cdp=FakeCDP(page={**PAGE, "text": "x" * (br.MAX_TEXT_CHARS + 1)}))
    out = br.open_page({"url": "https://example.com/"})
    assert len(out["text"]) == br.MAX_TEXT_CHARS
    assert out["truncated"] is True
    assert f"cut at {br.MAX_TEXT_CHARS}" in out["note"]


def test_text_exactly_at_the_limit_is_not_cut(br):
    _wire(br, cdp=FakeCDP(page={**PAGE, "text": "y" * br.MAX_TEXT_CHARS}))
    out = br.open_page({"url": "https://example.com/"})
    assert "truncated" not in out and "note" not in out


def test_links_are_capped_and_odd_entries_dropped(br):
    links = [{"text": "t" * 500, "href": f"https://example.com/{i}"} for i in range(br.MAX_LINKS + 10)]
    _wire(br, cdp=FakeCDP(page={**PAGE, "links": ["junk", *links]}))
    out = br.open_page({"url": "https://example.com/"})
    assert len(out["links"]) == br.MAX_LINKS - 1
    assert all(len(link["text"]) == br.MAX_LINK_TEXT for link in out["links"])


def test_the_page_is_cut_in_the_browser_before_it_is_sent(br):
    js = br._READ_PAGE
    assert f"text.slice(0, {br.MAX_TEXT_CHARS + 1})" in js
    assert f"links.length >= {br.MAX_LINKS}" in js


def test_the_client_never_retries(br):
    assert br._client.meta.config.retries["total_max_attempts"] == 1


# --- the Gateway entry point ------------------------------------------------------------------

def _context(tool):
    return SimpleNamespace(client_context=SimpleNamespace(custom={"bedrockAgentCoreToolName": f"web___{tool}"}))


def test_the_gateway_names_the_tool_in_the_client_context(br):
    _wire(br)
    assert br.lambda_handler({"url": "https://example.com/"}, _context("open_page"))["title"] == "Docs"


def test_an_unknown_tool_is_named_not_guessed_at(br):
    client, _ = _wire(br)
    out = br.lambda_handler({"url": "https://example.com/"}, _context("click_everything"))
    assert out == {"error": "unknown tool 'click_everything'", "availableTools": ["open_page"]}
    assert client.started == []


# --- the automation stream: signing, handshake and frames ------------------------------------------

def _server_frame(opcode, payload, fin=True, mask=b""):
    n = len(payload)
    head = bytes([(0x80 if fin else 0) | opcode])
    flag = 0x80 if mask else 0
    if n < 126:
        head += bytes([flag | n])
    elif n < 65536:
        head += bytes([flag | 126]) + struct.pack("!H", n)
    else:
        head += bytes([flag | 127]) + struct.pack("!Q", n)
    if mask:
        payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    return head + mask + payload


def _read_client_frame(sock, keys=None):
    """One frame the client sent: (opcode, payload, was_masked). Its masking key goes in `keys`."""
    def need(n):
        buf = b""
        while len(buf) < n:
            buf += sock.recv(n - len(buf))
        return buf
    b0, b1 = need(2)
    n = b1 & 0x7F
    if n == 126:
        n = struct.unpack("!H", need(2))[0]
    elif n == 127:
        n = struct.unpack("!Q", need(8))[0]
    mask = need(4) if b1 & 0x80 else b""
    if keys is not None:
        keys.append(mask)
    data = need(n)
    if mask:
        data = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
    return b0 & 0x0F, data, bool(mask)


def _read_request(sock):
    buf = b""
    while b"\r\n\r\n" not in buf:
        buf += sock.recv(4096)
    head = buf.split(b"\r\n\r\n", 1)[0].decode()
    line, *fields = head.split("\r\n")
    return line, {k.strip().lower(): v.strip() for k, _, v in (f.partition(":") for f in fields)}


def _accept(key):
    # RFC 6455 section 4.2.2 fixes SHA-1 for Sec-WebSocket-Accept: a handshake check, not a security hash.
    return base64.b64encode(hashlib.sha1((key + GUID).encode(), usedforsecurity=False).digest()).decode()  # nosec B324


def _serve(sock, script):
    t = threading.Thread(target=script, args=(sock,), daemon=True)
    t.start()
    return t


def test_the_handshake_upgrades_with_the_signed_headers_and_checks_the_answer(br):
    client, server = socket.socketpair()
    seen = {}

    def script(s):
        seen["line"], seen["headers"] = _read_request(s)
        s.sendall(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                   f"Sec-WebSocket-Accept: {_accept(seen['headers']['sec-websocket-key'])}\r\n\r\n").encode()
                  + _server_frame(0x1, b'{"early": 1}'))
    t = _serve(server, script)
    ws = br.handshake(client, "h.test", "/browser-streams/aws.browser.v1/sessions/s1/automation",
                      {"Host": "h.test", "Authorization": "AWS4-HMAC-SHA256 x", "X-Amz-Date": "20260101T000000Z"},
                      time.monotonic() + 5)
    t.join(5)
    assert seen["line"] == "GET /browser-streams/aws.browser.v1/sessions/s1/automation HTTP/1.1"
    h = seen["headers"]
    assert h["authorization"] == "AWS4-HMAC-SHA256 x" and h["x-amz-date"] == "20260101T000000Z"
    assert h["upgrade"] == "websocket" and h["connection"] == "Upgrade" and h["sec-websocket-version"] == "13"
    assert len(base64.b64decode(h["sec-websocket-key"])) == 16
    # Bytes that arrived with the handshake are the first frame, not lost.
    assert ws.recv(time.monotonic() + 5) == '{"early": 1}'
    client.close()
    server.close()


@pytest.mark.parametrize("answer, fragment", [
    ("HTTP/1.1 403 Forbidden\r\n\r\n", "refused the connection: HTTP/1.1 403"),
    ("HTTP/1.1 101 Switching Protocols\r\nSec-WebSocket-Accept: wrong\r\n\r\n", "wrong Sec-WebSocket-Accept"),
])
def test_a_handshake_the_server_did_not_accept_properly_is_refused(br, answer, fragment):
    client, server = socket.socketpair()

    def script(s):
        _read_request(s)
        s.sendall(answer.encode())
    t = _serve(server, script)
    with pytest.raises(ConnectionError, match=fragment):
        br.handshake(client, "h.test", "/p", {}, time.monotonic() + 5)
    t.join(5)
    client.close()
    server.close()


def test_frames_out_are_masked_and_frames_in_are_joined_with_pings_answered(br):
    client, server = socket.socketpair()
    ws = br._WebSocket(client)
    got = {}
    keys: list = []

    def script(s):
        got["sent"] = _read_client_frame(s, keys)
        s.sendall(_server_frame(0x1, b'{"a":', fin=False) + _server_frame(0x9, b"hi")
                  + _server_frame(0x0, b" 1}") + _server_frame(0x1, b"m" * 300)
                  + _server_frame(0x1, b"z" * 70_000) + _server_frame(0x1, b"masked", mask=b"\x01\x02\x03\x04"))
        got["pong"] = _read_client_frame(s, keys)
    t = _serve(server, script)
    ws.send('{"id": 1}')
    assert ws.recv(time.monotonic() + 5) == '{"a": 1}'
    assert ws.recv(time.monotonic() + 5) == "m" * 300            # 16-bit length
    assert ws.recv(time.monotonic() + 5) == "z" * 70_000         # 64-bit length
    assert ws.recv(time.monotonic() + 5) == "masked"
    t.join(5)
    assert got["sent"] == (0x1, b'{"id": 1}', True)
    assert got["pong"] == (0xA, b"hi", True)
    # A fresh, unpredictable key per frame (RFC 6455 section 5.3), not a constant.
    assert len(keys) == 2 and keys[0] != keys[1] and b"\x00\x00\x00\x00" not in keys
    client.close()
    server.close()


def test_a_read_that_times_out_mid_frame_loses_nothing(br):
    client, server = socket.socketpair()
    ws = br._WebSocket(client)
    frame = _server_frame(0x1, json.dumps({"method": "Page.loadEventFired", "sessionId": "S1"}).encode())
    server.sendall(frame[:5])
    cdp = br.CDP(ws)
    assert cdp.wait_for("Page.loadEventFired", time.monotonic() + 0.2, "S1") is False
    server.sendall(frame[5:])
    assert cdp.wait_for("Page.loadEventFired", time.monotonic() + 5, "S1") is True
    client.close()
    server.close()


def test_a_message_over_the_cap_is_refused(br, monkeypatch):
    monkeypatch.setattr(br, "MAX_MESSAGE_BYTES", 100)
    client, server = socket.socketpair()
    server.sendall(_server_frame(0x1, b"x" * 101))
    with pytest.raises(ConnectionError, match="over 100 bytes"):
        br._WebSocket(client).recv(time.monotonic() + 5)
    client.close()
    server.close()


def test_a_close_from_the_browser_ends_the_read(br):
    client, server = socket.socketpair()
    server.sendall(_server_frame(0x8, struct.pack("!H", 1000)))
    with pytest.raises(ConnectionError, match="closed"):
        br._WebSocket(client).recv(time.monotonic() + 5)
    client.close()
    server.close()


def test_cdp_matches_replies_by_id_and_remembers_events_on_the_way(br):
    client, server = socket.socketpair()
    cdp = br.CDP(br._WebSocket(client))

    def script(s):
        _, data, _ = _read_client_frame(s)
        msg = json.loads(data)
        assert msg == {"id": 1, "method": "Page.navigate", "params": {"url": "https://example.com/"}, "sessionId": "S1"}
        s.sendall(_server_frame(0x1, json.dumps({"method": "Page.loadEventFired", "sessionId": "S1", "params": {}}).encode())
                  + _server_frame(0x1, json.dumps({"id": 1, "result": {"frameId": "F"}}).encode()))
        _, data, _ = _read_client_frame(s)
        s.sendall(_server_frame(0x1, json.dumps({"id": 2, "error": {"code": -32000, "message": "No target"}}).encode()))
    t = _serve(server, script)
    assert cdp.call("Page.navigate", {"url": "https://example.com/"}, time.monotonic() + 5, "S1") == {"frameId": "F"}
    # The load event arrived before the reply, and is not lost.
    assert cdp.wait_for("Page.loadEventFired", time.monotonic() + 0.1, "S1") is True
    with pytest.raises(RuntimeError, match=r"Target\.attachToTarget: No target"):
        cdp.call("Target.attachToTarget", {"targetId": "x"}, time.monotonic() + 5)
    t.join(5)
    client.close()
    server.close()


def test_the_stream_goes_to_the_agentcore_host_over_tls_for_the_session_started(br, monkeypatch):
    client, server = socket.socketpair()
    seen = {}

    def connect(address, timeout=None):
        seen["address"] = address
        return client

    class Context:
        def wrap_socket(self, sock, server_hostname):
            seen["sni"] = server_hostname
            return sock
    monkeypatch.setattr(br.socket, "create_connection", connect)
    monkeypatch.setattr(br.ssl, "create_default_context", lambda: Context())
    monkeypatch.setattr(br, "_signed_headers", lambda host, path: {"Host": host, "Authorization": "sig"})

    def script(s):
        seen["line"], h = _read_request(s)
        s.sendall(f"HTTP/1.1 101 OK\r\nSec-WebSocket-Accept: {_accept(h['sec-websocket-key'])}\r\n\r\n".encode())
    t = _serve(server, script)
    cdp = br._open_stream("sess1", time.monotonic() + 5)
    t.join(5)
    assert seen["address"] == ("bedrock-agentcore.us-east-1.amazonaws.com", 443)
    assert seen["sni"] == "bedrock-agentcore.us-east-1.amazonaws.com"
    assert seen["line"] == "GET /browser-streams/aws.browser.v1/sessions/sess1/automation HTTP/1.1"
    assert isinstance(cdp, br.CDP)
    client.close()
    server.close()


@pytest.mark.parametrize("sid", ["", "a/../b", "x" * 41, "sess 1", None])
def test_a_session_id_that_is_not_the_services_shape_is_not_put_in_a_url(br, monkeypatch, sid):
    monkeypatch.setattr(br.socket, "create_connection", lambda *a, **k: pytest.fail("connected"))
    with pytest.raises(RuntimeError, match="will not put in a URL"):
        br._open_stream(sid, time.monotonic() + 5)


def test_the_upgrade_is_signed_with_sigv4_for_bedrock_agentcore(br, monkeypatch):
    creds = Credentials("AKIDEXAMPLE", "secret", "token123")
    monkeypatch.setattr(br.boto3, "Session", lambda: SimpleNamespace(get_credentials=lambda: creds))
    h = br._signed_headers("bedrock-agentcore.us-east-1.amazonaws.com", "/browser-streams/aws.browser.v1/sessions/s/automation")
    assert re.match(r"AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/\d{8}/us-east-1/bedrock-agentcore/aws4_request, "
                    r"SignedHeaders=host;x-amz-date;x-amz-security-token, Signature=[0-9a-f]{64}$", h["Authorization"])
    assert h["Host"] == "bedrock-agentcore.us-east-1.amazonaws.com"
    assert re.match(r"^\d{8}T\d{6}Z$", h["X-Amz-Date"])
    assert h["X-Amz-Security-Token"] == "token123"
    # No traceparent of our own: ADOT rewrites one after signing (COMMON section 9).
    assert not any(k.lower() == "traceparent" for k in h)


# --- one domain rule everywhere ------------------------------------------------------------------

def test_the_domain_rule_is_the_same_in_the_function_and_both_validators(br):
    sys.path.insert(0, str(ORCH / "bff"))
    try:
        import browser_tool
    finally:
        sys.path.remove(str(ORCH / "bff"))
    ts = (ORCH / "web" / "src" / "builder" / "browserTool.ts").read_text()
    assert br.DOMAIN_RE.pattern == browser_tool.DOMAIN_RE.pattern
    assert f"export const DOMAIN_RE = /{br.DOMAIN_RE.pattern}/;" in ts
    assert browser_tool.TOOL in br._TOOLS and f'BROWSER_TOOL = "{browser_tool.TOOL}"' in ts


# --- IAM: the same few actions on both IaC paths -------------------------------------------------

TF = (ORCH / "terraform" / "browser_tool.tf").read_text()
TOOLS_TF = (ORCH / "terraform" / "tools.tf").read_text()
TS = (ORCH / "cdk" / "lib" / "browser-tool.ts").read_text()
PLANE_TS = (ORCH / "cdk" / "lib" / "tool-plane.ts").read_text()
SESSION_ACTIONS = {"bedrock-agentcore:StartBrowserSession", "bedrock-agentcore:StopBrowserSession"}
CONNECT = "bedrock-agentcore:ConnectBrowserAutomationStream"


def _quoted(text):
    return re.findall(r'"([^"\n]+)"', text)


def _statements_tf():
    """Each `{ Sid = ... }` statement in browser_tool.tf, as (actions, resource text)."""
    out = {}
    for block in TF.split("Sid      = ")[1:]:
        sid = block.split('"')[1]
        out[sid] = ({a for a in _quoted(block.split("Resource")[0]) if ":" in a and not a.startswith("http")},
                    block.split("Resource =", 1)[1].split("\n", 1)[0].strip())
    return out


def _statements_ts():
    out = {}
    for block in TS.split("new iam.PolicyStatement({")[1:]:
        sid = block.split('sid: "')[1].split('"')[0]
        out[sid] = ({a for a in _quoted(block.split("resources:")[0]) if a.startswith("bedrock-agentcore:")},
                    block.split("resources:", 1)[1].split("\n", 1)[0].strip())
    return out


def test_terraform_grants_session_actions_on_the_system_browser_and_connect_only_on_star():
    st = _statements_tf()
    assert st["UseSystemBrowser"] == (SESSION_ACTIONS, '["arn:aws:bedrock-agentcore:${var.region}:aws:browser/aws.browser.v1"]')
    assert st["DriveTheSessionItStarted"] == ({CONNECT}, '"*"')
    assert st["Logs"][0] == {"logs:CreateLogStream", "logs:PutLogEvents"}
    assert "aws_cloudwatch_log_group.tool[each.key].arn" in st["Logs"][1]
    assert "bedrock-agentcore:*" not in TF and "LiveView" not in TF and "InvokeBrowser" not in TF


def test_cdk_grants_the_same():
    st = _statements_ts()
    assert st["UseSystemBrowser"] == (SESSION_ACTIONS, "[`arn:aws:bedrock-agentcore:${region}:aws:browser/aws.browser.v1`],")
    assert st["DriveTheSessionItStarted"] == ({CONNECT}, '["*"],')
    assert "bedrock-agentcore:*" not in TS and "LiveView" not in TS and "InvokeBrowser" not in TS
    assert "logGroup.grantWrite(role)" in TS


def test_terraform_attaches_the_policy_to_browser_functions_only():
    assert "for_each = local.builtin_lambda_browser_tools" in TF
    assert re.search(r'builtin_lambda_browser_tools\s*=\s*\{ for n, t in local\.builtin_lambda_tools : '
                     r'n => t if t\.source == "browser" \}', TOOLS_TF)
    assert "role     = aws_iam_role.tool_lambda[each.key].id" in TF


def test_both_paths_give_the_function_60_seconds_and_allow_domains_only_when_set():
    assert 'timeout     = contains(["codeInterpreter", "browser"], each.value.source) ? 60 : 30' in TOOLS_TF
    assert "timeout: cdk.Duration.seconds(60)" in TS
    assert 'each.value.source == "browser" && length(each.value.allow_domains) > 0' in TOOLS_TF
    assert 'ALLOW_DOMAINS = join(",", each.value.allow_domains)' in TOOLS_TF
    assert 'allow_domains = try(t.allowDomains, [])' in TOOLS_TF
    assert '...(domains.length ? { environment: { ALLOW_DOMAINS: domains.join(",") } } : {})' in TS
    handler = HANDLER_PATH.read_text()
    assert "CALL_SECONDS = 40" in handler and 'os.environ.get("ALLOW_DOMAINS"' in handler


def test_cdk_deploys_it_through_its_own_function_from_the_tool_plane():
    assert "if (spec.source === BROWSER_SOURCE) {" in PLANE_TS
    assert 'export const BROWSER_SOURCE = "browser";' in TS
    assert 'lambda.Code.fromAsset(path.join(orchRoot, "app", "tools", BROWSER_SOURCE))' in TS


def test_the_source_is_in_the_vocabulary_and_the_shipped_sample_does_not_use_it():
    vocab = json.loads((ORCH / "app" / "vocabulary.json").read_text())
    assert "browser" in vocab["builtinLambdaSource"]["values"]
    workflow = json.loads((ORCH / "app" / "workflow.json").read_text())
    assert "browser" not in {t.get("source") for t in workflow["tools"].values()}
    assert HANDLER_PATH.is_file() and (TOOL_DIR / "netguard.py").is_file()


def test_the_function_imports_nothing_the_lambda_runtime_lacks():
    """It ships as a plain zip of its folder: the standard library, boto3/botocore (in the
    runtime) and its own netguard copy, nothing else."""
    mods = set()
    for node in ast.walk(ast.parse(HANDLER_PATH.read_text())):
        if isinstance(node, ast.Import):
            mods |= {a.name.split(".")[0] for a in node.names}
        elif isinstance(node, ast.ImportFrom):
            mods.add((node.module or "").split(".")[0])
    assert mods - set(sys.stdlib_module_names) == {"boto3", "botocore", "netguard"}
