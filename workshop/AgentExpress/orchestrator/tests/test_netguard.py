"""The SSRF guard (app/common/netguard.py, copied as bff/netguard.py), and the inventory of
every place server code fetches a URL.

The guard is only worth having if nothing goes around it, so the inventory test reads every
server-side Python file and fails on any fetch that is not in INVENTORY with how it is
guarded: a new fetch has to be routed through the guard, or exempted here with a reason a
reviewer can check.

The redirect tests run a real TLS server on 127.0.0.1 with a certificate made for the test,
and resolve the test's host names by monkeypatching getaddrinfo: what is under test is that
the guard checks the address it CONNECTS to, on the first request and on every redirect.
"""
from __future__ import annotations

import ast
import importlib.util
import socket
import ssl
import sys
import threading
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest
from conftest import ORCH_ROOT


def _load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# Loaded by path, under names of their own, so no test that pops app.* from sys.modules can
# hand these tests a different module object from the one they patch.
ng = _load(ORCH_ROOT / "app" / "common" / "netguard.py", "_netguard_under_test")
bff_ng = _load(ORCH_ROOT / "bff" / "netguard.py", "_bff_netguard_under_test")


# --- which addresses are refused ---------------------------------------------------------------

REFUSED = [  # nosec B104 - "0.0.0.0" below is an address the guard must refuse, not a bind
    "127.0.0.1", "127.1.2.3",                   # loopback
    "169.254.169.254", "169.254.170.2",         # link-local: instance and container metadata
    "10.0.0.5", "172.16.0.1", "172.31.255.255", "192.168.1.1",   # RFC 1918
    "100.64.0.1", "100.127.255.254",            # CGNAT
    "0.0.0.0", "224.0.0.1", "240.0.0.1", "255.255.255.255",  # noqa: S104 - addresses the guard refuses, not a bind  # nosec B104
    "::1", "::", "fe80::1", "fc00::1", "fd12:3456::1",           # IPv6 loopback, link-local, ULA
    "::ffff:127.0.0.1", "::ffff:169.254.169.254", "::ffff:10.0.0.1",   # IPv4-mapped
    "64:ff9b::a9fe:a9fe", "64:ff9b::a00:1",     # NAT64 of 169.254.169.254, 10.0.0.1
    "2002:a9fe:a9fe::1", "2002:7f00:1::1",      # 6to4 of the metadata address, loopback
    "fd00:ec2::254",                            # the EC2 IPv6 metadata address
]
ALLOWED = ["8.8.8.8", "1.1.1.1", "52.94.236.248", "2606:4700:4700::1111", "::ffff:8.8.8.8",
           "64:ff9b::808:808"]


@pytest.mark.parametrize("address", REFUSED)
def test_a_non_public_address_is_refused(address):
    assert ng.blocked_address(address), f"{address} would be fetched"


@pytest.mark.parametrize("address", ALLOWED)
def test_a_public_address_is_allowed(address):
    assert ng.blocked_address(address) == ""


def test_an_unparseable_address_is_refused():
    assert ng.blocked_address("not-an-ip")


@pytest.mark.parametrize("url,why", [
    ("http://example.com/", "only https"),
    ("file:///etc/passwd", "only https"),
    ("ftp://example.com/x", "only https"),
    ("https://user:pw@example.com/", "user:password"),
    ("https://user@example.com/", "user:password"),
    ("https:///nohost", "no host"),
    ("", "only https"),
])
def test_a_url_that_is_not_plain_https_is_refused(url, why):
    with pytest.raises(ng.BlockedURL, match=why):
        ng.check_url(url)


def test_a_refusal_is_a_urlerror_so_callers_handling_an_unreachable_endpoint_cover_it():
    assert issubclass(ng.BlockedURL, urllib.error.URLError)


def _resolving(monkeypatch, mod, table):
    """getaddrinfo, answering from `table` (host -> addresses)."""
    def fake(host, port, *a, **k):
        if host not in table:
            raise socket.gaierror(f"unknown host {host}")
        return [(socket.AF_INET6 if ":" in ip else socket.AF_INET, socket.SOCK_STREAM, 6, "",
                 (ip, port) if ":" not in ip else (ip, port, 0, 0)) for ip in table[host]]
    monkeypatch.setattr(mod.socket, "getaddrinfo", fake)


def test_a_host_is_refused_if_any_address_it_resolves_to_is_not_public(monkeypatch):
    _resolving(monkeypatch, ng, {"mixed.test": ["8.8.8.8", "169.254.169.254"]})
    with pytest.raises(ng.BlockedURL, match=r"169\.254\.169\.254"):
        ng.resolve("mixed.test", 443)


def test_allow_private_lifts_the_address_check_only(monkeypatch):
    _resolving(monkeypatch, ng, {"internal.test": ["10.0.0.5"]})
    assert ng.resolve("internal.test", 443, allow_private=True)
    with pytest.raises(ng.BlockedURL, match="only https"):
        ng.urlopen("http://internal.test/", allow_private=True)


def test_a_refused_host_is_never_connected_to(monkeypatch):
    """Refused before a socket exists, not after a request went out."""
    _resolving(monkeypatch, ng, {"meta.test": ["169.254.169.254"]})
    def no_socket(*a, **k):
        raise AssertionError("a socket was opened for a refused address")
    monkeypatch.setattr(ng.socket, "socket", no_socket)
    with pytest.raises(urllib.error.URLError) as e:
        ng.urlopen("https://meta.test/latest/meta-data/", timeout=2)
    assert isinstance(e.value.reason, ng.BlockedURL) or isinstance(e.value, ng.BlockedURL)


def test_only_https_is_spoken_at_all():
    """No http, file, ftp or data handler, and no proxy handler, in the opener."""
    names = {type(h).__name__ for h in ng.opener().handlers}
    assert not names & {"HTTPHandler", "FileHandler", "FTPHandler", "DataHandler", "ProxyHandler",
                        "UnknownHandler"}


# --- a real TLS server, so the connection and every redirect go through the guard ---------------

@pytest.fixture
def tls_server(tmp_path):
    """An HTTPS server on 127.0.0.1 whose certificate names public.test and meta.test, and a
    client context that trusts it. GET /redirect?to=<url> answers 302 to <url>."""
    x509 = pytest.importorskip("cryptography.x509")
    import datetime

    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "public.test")])
    now = datetime.datetime.now(datetime.UTC)
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
            .serial_number(x509.random_serial_number()).not_valid_before(now - datetime.timedelta(days=1))
            .not_valid_after(now + datetime.timedelta(days=1))
            .add_extension(x509.SubjectAlternativeName([x509.DNSName("public.test"), x509.DNSName("meta.test")]), False)
            .add_extension(x509.BasicConstraints(ca=True, path_length=None), True)
            .sign(key, hashes.SHA256()))
    cert_pem, key_pem = tmp_path / "cert.pem", tmp_path / "key.pem"
    cert_pem.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_pem.write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                          serialization.NoEncryption()))
    hits: list[str] = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            hits.append(self.path)
            if self.path.startswith("/redirect?to="):
                self.send_response(302)
                self.send_header("Location", self.path.split("=", 1)[1])
                self.end_headers()
                return
            body = b'{"ok": true}'
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(cert_pem, key_pem)
    server.socket = ctx.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    client = ssl.create_default_context(cafile=str(cert_pem))
    try:
        yield server.server_address[1], client, hits
    finally:
        server.shutdown()
        server.server_close()


def _guard(monkeypatch, mod, client_ctx, *, public=("127.0.0.1",)):
    """The guard, trusting the test certificate, with only `public` addresses let through:
    127.0.0.1 stands in for a public server, 127.0.0.2 for the metadata service."""
    real = mod.blocked_address
    monkeypatch.setattr(mod, "blocked_address", lambda a: "" if a in public else (real(a) or "refused"))
    real_init = mod._HTTPSHandler.__init__

    def init(self, allow_private):
        real_init(self, allow_private)
        self._context = client_ctx
    monkeypatch.setattr(mod._HTTPSHandler, "__init__", init)
    _resolving(monkeypatch, mod, {"public.test": ["127.0.0.1"], "meta.test": ["127.0.0.2"]})


@pytest.mark.parametrize("mod", [ng, bff_ng], ids=["app", "bff"])
def test_a_public_host_is_fetched_over_verified_tls(monkeypatch, tls_server, mod):
    port, client, hits = tls_server
    _guard(monkeypatch, mod, client)
    with mod.urlopen(f"https://public.test:{port}/card", timeout=5) as r:
        assert r.read() == b'{"ok": true}'
    assert hits == ["/card"]


@pytest.mark.parametrize("mod", [ng, bff_ng], ids=["app", "bff"])
def test_a_redirect_to_a_refused_address_is_not_followed(monkeypatch, tls_server, mod):
    """The first hop is public; the 302 points at the metadata stand-in. Refused at the
    connection, by the guard, not by a connection error (nothing listens on 127.0.0.2:port,
    so anything but BlockedURL here means the guard was not asked)."""
    port, client, hits = tls_server
    _guard(monkeypatch, mod, client)
    with pytest.raises(urllib.error.URLError) as e:
        mod.urlopen(f"https://public.test:{port}/redirect?to=https://meta.test:{port}/latest", timeout=5)
    reason = e.value if isinstance(e.value, mod.BlockedURL) else e.value.reason
    assert isinstance(reason, mod.BlockedURL), repr(e.value)
    assert "meta.test" in str(reason)
    assert hits == [f"/redirect?to=https://meta.test:{port}/latest"]


@pytest.mark.parametrize("target", ["http://public.test:{port}/x", "https://u:p@public.test:{port}/x"])
def test_a_redirect_to_anything_but_plain_https_is_not_followed(monkeypatch, tls_server, target):
    port, client, hits = tls_server
    _guard(monkeypatch, ng, client)
    with pytest.raises(urllib.error.URLError) as e:
        ng.urlopen(f"https://public.test:{port}/redirect?to={target.format(port=port)}", timeout=5)
    reason = e.value if isinstance(e.value, ng.BlockedURL) else e.value.reason
    assert isinstance(reason, ng.BlockedURL), repr(e.value)
    assert len(hits) == 1


def test_a_redirect_to_a_local_file_is_not_followed(monkeypatch, tls_server):
    """urllib itself refuses a redirect to file:// before the guard is asked; either way
    nothing past the first hop is fetched."""
    port, client, hits = tls_server
    _guard(monkeypatch, ng, client)
    with pytest.raises(urllib.error.URLError, match="not allowed"):
        ng.urlopen(f"https://public.test:{port}/redirect?to=file:///etc/passwd", timeout=5)
    assert len(hits) == 1


def test_the_connection_goes_to_the_address_that_was_checked(monkeypatch, tls_server):
    """DNS rebinding: a name that answers public when checked and private when connected.
    The guard resolves once and connects to what it checked, so a second answer is never
    used: count the lookups."""
    port, client, _hits = tls_server
    _guard(monkeypatch, ng, client)
    lookups = []
    real = ng.socket.getaddrinfo

    def counting(host, *a, **k):
        lookups.append(host)
        return real(host, *a, **k)
    monkeypatch.setattr(ng.socket, "getaddrinfo", counting)
    with ng.urlopen(f"https://public.test:{port}/x", timeout=5) as r:
        r.read()
    assert lookups == ["public.test"]


def test_the_certificate_is_checked_against_the_host_name(monkeypatch, tls_server):
    """Connecting to an address must not mean trusting whatever certificate is there."""
    port, client, _hits = tls_server
    _guard(monkeypatch, ng, client)
    _resolving(monkeypatch, ng, {"other.test": ["127.0.0.1"]})
    with pytest.raises(urllib.error.URLError) as e:
        ng.urlopen(f"https://other.test:{port}/x", timeout=5)
    assert isinstance(e.value.reason, ssl.SSLCertVerificationError)


# --- the BFF's copy is the original --------------------------------------------------------------

def _tree(path: Path) -> str:
    tree = ast.parse(path.read_text())
    body = tree.body
    if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
        tree.body = body[1:]
    return ast.dump(tree)


def test_the_bff_copy_is_the_original():
    """The BFF cannot import app.*, so bff/netguard.py is a copy. Held to the original as a
    syntax tree (module docstring aside): a fix made to one must be made to both."""
    assert _tree(ORCH_ROOT / "bff" / "netguard.py") == _tree(ORCH_ROOT / "app" / "common" / "netguard.py")


def test_the_browser_tools_copy_is_the_original():
    """The Browser tool's Lambda is zipped from app/tools/browser/ alone, so it carries a copy too."""
    assert _tree(ORCH_ROOT / "app" / "tools" / "browser" / "netguard.py") == _tree(ORCH_ROOT / "app" / "common" / "netguard.py")


# --- the inventory: every server-side fetch, and what guards it ---------------------------------

#: (file, function) -> how its fetch is guarded. A fetch found anywhere else fails the test.
INVENTORY = {
    ("app/common/a2a_agent.py", "_open"):
        "netguard.urlopen: an A2A agent's card and RPC endpoint (workflow.json agentCard, or the "
        "IaC's A2A_ENDPOINTS). _require_https refuses early; the guard holds on connect and redirect.",
    ("app/features/gateway/client.py", "_gateway_token"):
        "netguard.urlopen: the IdP token URL (deployment config); carries the Gateway client secret.",
    ("app/common/litellm_proxy.py", "_post"):
        "netguard.urlopen: the LiteLLM proxy's /v1/chat/completions (deployment config "
        "LITELLM_BASE_URL, https-checked at plan/synth and at call); carries the virtual key.",
    ("app/features/observability/runexport.py", "_post"):
        "netguard.urlopen: the observability tool a build sends its runs to (workflow.json "
        "orchestrator.export: a preset host, or the https endpoint given for otlp, grafana and "
        "self-hosted Langfuse); carries the tool's API key from Secrets Manager.",
    ("bff/litellm_models.py", "_fetch"):
        "netguard.urlopen: the LiteLLM proxy's /v1/models (deployment config LITELLM_BASE_URL); "
        "carries the virtual key.",
    ("app/common/netguard.py", "opener"): "the guard itself",
    ("bff/netguard.py", "opener"): "the guard itself (the BFF's copy)",
    ("app/tools/browser/netguard.py", "opener"): "the guard itself (the Browser tool's copy)",
    ("app/tools/browser/handler.py", "_open_stream"):
        "EXEMPT: a TLS socket to bedrock-agentcore.<AWS_REGION>.amazonaws.com:443, the AgentCore "
        "Browser automation stream for the session the function just started; the host is a "
        "constant of the Region. The URL the model asks for is opened by the browser, not "
        "fetched here, and is checked with the netguard copy (check_url) before and after.",
    ("app/features/gateway/client.py", "_gateway_tools"):
        "EXEMPT: MultiServerMCPClient (httpx) to GATEWAY_URL / GATEWAY_USER_URL, the AgentCore "
        "Gateway's own endpoints, written by the IaC from the Gateway it created; no caller or "
        "workflow.json value reaches them.",
    ("app/features/observability/export.py", "_otlp_exporter"):
        "EXEMPT: the OpenTelemetry SDK's own requests session to OTLP_ENDPOINT, which only the "
        "operator sets (Terraform otlp_endpoint / CDK otlpEndpoint, off by default). It is "
        "https-only with no user info, checked at plan/synth and again at start "
        "(export.ENDPOINT_PATTERN); no caller or workflow.json value reaches it. The SDK takes a "
        "requests.Session, so netguard (urllib) cannot sit in front of it.",
    ("deployer/runner.py", "ensure_terraform"):
        "EXEMPT: a constant https://releases.hashicorp.com URL, SHA256-checked against a pinned value.",
    ("deployer/container_creds.py", "main"):
        "EXEMPT: the container credentials endpoint CodeBuild names (169.254.170.2 or its _FULL_URI); "
        "link-local by design, and set by the platform, not by a caller or a config file.",
}
GUARDED_BY_NETGUARD = {k for k, v in INVENTORY.items() if v.startswith("netguard.urlopen")}

#: Server-side code: what runs in the runtime, the BFF, the Lambdas and the deploy runner.
SERVER_CODE = ["app", "bff", "deployer", "a2a_lambda", "kb_lambda", "signup_lambda", "scaffold.py"]
FETCH_CALLS = {"urlopen", "build_opener", "OpenerDirector", "HTTPConnection", "HTTPSConnection",
               "create_connection", "MultiServerMCPClient"}
FETCH_MODULES = {"requests", "httpx", "aiohttp", "urllib3", "pycurl", "websocket", "websockets"}


def _server_files():
    for entry in SERVER_CODE:
        root = ORCH_ROOT / entry
        files = [root] if root.is_file() else sorted(root.rglob("*.py"))
        for f in files:
            if "__pycache__" in f.parts or "botocore_data" in f.parts:
                continue
            yield f


def _fetch_sites():
    """(file, enclosing function, call) for every fetch-like call, and every import of an
    HTTP client library, in server code."""
    sites = []
    for f in _server_files():
        rel = f.relative_to(ORCH_ROOT).as_posix()
        tree = ast.parse(f.read_text())
        parents = {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}

        def owner(node, parents=parents):
            while node in parents:
                node = parents[node]
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    return node.name
            return "<module>"
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                fn = node.func
                name = fn.attr if isinstance(fn, ast.Attribute) else fn.id if isinstance(fn, ast.Name) else ""
                if name in FETCH_CALLS:
                    sites.append((rel, owner(node), ast.unparse(fn)))
            elif isinstance(node, (ast.Import, ast.ImportFrom)):
                mods = [a.name for a in node.names] if isinstance(node, ast.Import) else [node.module or ""]
                sites.extend((rel, owner(node), f"import {m}") for m in mods if m.split(".")[0] in FETCH_MODULES)
    return sites


def test_every_server_side_fetch_is_in_the_inventory():
    """A fetch nobody has looked at fails here: route it through netguard.urlopen (app/ or
    bff/), or add it to INVENTORY with why it needs no guard."""
    found = {(f, fn) for f, fn, _call in _fetch_sites()}
    unknown = sorted(s for s in _fetch_sites() if (s[0], s[1]) not in INVENTORY)
    assert not unknown, f"fetches not in the inventory: {unknown}"
    gone = sorted(set(INVENTORY) - found)
    assert not gone, f"inventory entries with no fetch any more (remove them): {gone}"


def test_every_guarded_site_calls_the_guard_and_nothing_else():
    by_site: dict = {}
    for f, fn, call in _fetch_sites():
        by_site.setdefault((f, fn), set()).add(call)
    for site in GUARDED_BY_NETGUARD:
        assert by_site[site] == {"netguard.urlopen"}, f"{site} fetches with {by_site[site]}"


def test_the_a2a_client_sends_every_request_through_the_guard(monkeypatch):
    """_open is the only way out of the A2A client, and it passes the private-endpoint
    switch along: read from the module the agent code holds (conftest pops app.*)."""
    sys.modules.pop("app.common.a2a_agent", None)
    import app.common.a2a_agent as a2a
    calls = []
    monkeypatch.setattr(a2a.netguard, "urlopen", lambda req, timeout, allow_private: calls.append(
        (req, timeout, allow_private)) or "resp")
    assert a2a._open("REQ", 7.0) == "resp"
    monkeypatch.setattr(a2a, "_ALLOW_PRIVATE", True)
    a2a._open("REQ", 7.0)
    assert calls == [("REQ", 7.0, False), ("REQ", 7.0, True)]
