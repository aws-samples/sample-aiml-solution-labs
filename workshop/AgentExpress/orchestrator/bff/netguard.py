"""A copy of app/common/netguard.py, the SSRF guard: the BFF ships as its own zip and cannot
import app.*. tests/test_netguard.py holds the two to the same syntax tree, so change both.

Every fetch the BFF makes of a URL it did not write itself goes through `urlopen` here: https
only, no user:password@, every resolved address public (loopback, link-local and the
metadata address, private, CGNAT and their IPv6 forms refused), the connection made to the
address that was checked, and every redirect checked again. See the original for why.
"""
from __future__ import annotations

import http.client
import ipaddress
import socket
import urllib.error
import urllib.request
from urllib.parse import urlsplit

#: NAT64's well-known prefix: the low 32 bits ARE an IPv4 address, private or not.
_NAT64 = ipaddress.ip_network("64:ff9b::/96")
#: CGNAT. Python's is_private leaves it out (it is not private, it is shared), so name it.
_CGNAT = ipaddress.ip_network("100.64.0.0/10")


class BlockedURL(urllib.error.URLError):
    """A URL the guard will not fetch. A URLError, so a caller's existing handling of an
    unreachable endpoint covers it too."""


def blocked_address(address: str) -> str:
    """Why `address` (an IP literal) must not be fetched from, or "" if it may."""
    try:
        ip = ipaddress.ip_address(address.split("%", 1)[0])
    except ValueError:
        return "not an IP address"
    if isinstance(ip, ipaddress.IPv6Address):
        embedded = ip.ipv4_mapped or ip.sixtofour or (ip.teredo[1] if ip.teredo else None)
        if embedded is None and ip in _NAT64:
            embedded = ipaddress.IPv4Address(int(ip) & 0xFFFFFFFF)
        if embedded is not None:
            inner = blocked_address(str(embedded))
            return f"{inner} (inside {ip})" if inner else ""
    for name in ("is_loopback", "is_link_local", "is_private", "is_unspecified",
                 "is_multicast", "is_reserved"):
        if getattr(ip, name):
            return f"{ip} is {name[3:].replace('_', '-')}"
    if isinstance(ip, ipaddress.IPv4Address) and ip in _CGNAT:
        return f"{ip} is shared address space (CGNAT)"
    if not ip.is_global:
        return f"{ip} is not a public address"
    return ""


def check_url(url: str) -> str:
    """The URL if it may be fetched: https, a host, no user:password@. Raises BlockedURL."""
    parts = urlsplit(str(url or ""))
    if parts.scheme.lower() != "https":
        raise BlockedURL(f"only https:// URLs are fetched, not {parts.scheme or 'none'}://")
    if parts.username is not None or parts.password is not None or "@" in parts.netloc:
        raise BlockedURL("a URL with user:password@ in it is not fetched")
    if not parts.hostname:
        raise BlockedURL("the URL has no host")
    return url


def resolve(host: str, port: int, allow_private: bool = False) -> list:
    """Every address `host` resolves to, each checked. Raises BlockedURL if any is refused:
    a name with one public and one private answer is not safe to reach."""
    try:
        infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except socket.gaierror as e:
        raise urllib.error.URLError(e) from e
    if not allow_private:
        for info in infos:
            why = blocked_address(info[4][0])
            if why:
                raise BlockedURL(f"{host} resolves to a non-public address: {why}")
    return infos


def _connector(allow_private: bool):
    """socket.create_connection, to the addresses resolve() checked and to nothing else."""
    def create_connection(address, timeout=socket._GLOBAL_DEFAULT_TIMEOUT, source_address=None):
        host, port = address
        last: OSError | None = None
        for family, kind, proto, _name, sockaddr in resolve(host, port, allow_private):
            sock = socket.socket(family, kind, proto)
            try:
                if timeout is not socket._GLOBAL_DEFAULT_TIMEOUT:
                    sock.settimeout(timeout)
                if source_address:
                    sock.bind(source_address)
                sock.connect(sockaddr)
                return sock
            except OSError as e:
                sock.close()
                last = e
        raise last or OSError(f"could not connect to {host}")
    return create_connection


class _HTTPSHandler(urllib.request.HTTPSHandler):
    def __init__(self, allow_private: bool):
        super().__init__()
        self._allow_private = allow_private

    def https_open(self, req):
        allow_private = self._allow_private

        class Connection(http.client.HTTPSConnection):
            def __init__(self, *a, **k):
                super().__init__(*a, **k)
                # HTTPSConnection connects through this, then wraps the socket in TLS with
                # server_hostname=self.host: the certificate is checked against the NAME.
                self._create_connection = _connector(allow_private)

        return self.do_open(Connection, req, context=self._context)


class _RedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        check_url(newurl)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def opener(allow_private: bool = False) -> urllib.request.OpenerDirector:
    """An opener that speaks https only, through the guard. No proxy, file, ftp or data
    handler: anything but https is refused for want of one."""
    o = urllib.request.OpenerDirector()
    for h in (_HTTPSHandler(allow_private), _RedirectHandler(),
              urllib.request.HTTPDefaultErrorHandler(), urllib.request.HTTPErrorProcessor()):
        o.add_handler(h)
    return o


def urlopen(request, timeout: float = 10, allow_private: bool = False):
    """urllib.request.urlopen, through the guard. `request` is a URL or a Request."""
    url = request.full_url if isinstance(request, urllib.request.Request) else str(request)
    check_url(url)
    return opener(allow_private).open(request, timeout=timeout)
