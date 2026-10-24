#!/usr/bin/env python3
"""Check every in-product documentation link against the documentation it points to.

The UI's info popovers link each feature to a section of the deployment's documentation
(web/src/docs/docsLinks.ts). This reads each popover's Documentation link the way a person
would, by rendering every popover and reading its link (web/src/docs/docsHrefs.export.test.tsx,
under vitest), then checks that each link's #anchor names a section of its page.

Two targets, matching the two shapes docsLinks.ts writes:

--target wiki (the default): an XWiki copy of the guide, --base-url ending in .../bin/view/<Space>/.
  Opens each page in a real browser (Playwright, Chromium) and checks the element the #H... id
  names is on it. Usually needs a signed-in session: pass it as a Netscape-format cookie file
  (--cookies). A page that lands on another host than the documentation's is taken as a
  sign-in page: the check stops there with exit code 2 and the --signin-hint text.

    python3 scripts/check_docs_links.py --target wiki \\
        --base-url https://wiki.example.com/bin/view/AgentExpress/ \\
        --cookies ~/cookies.txt --signin-hint "sign in again to refresh ~/cookies.txt"

--target github: the public user guide's Markdown (<page>.md#<slug>). Checks each slug against
  the headings of the LOCAL generated Markdown (--guide-dir, default <repo>/docs/user-guide),
  computed the way GitHub does (lower case, punctuation but - and _ removed, spaces as -, a
  repeat -1, -2). With --live it also fetches each page from github.com (public, no sign-in)
  and looks for GitHub's rendered id, user-content-<slug>, in the raw HTML (quoted or escaped:
  a blob page may carry the rendered Markdown inside a JSON payload). --base-url defaults to
  the deployment default in orchestrator/docs_default.json.

    python3 scripts/check_docs_links.py --target github
    python3 scripts/check_docs_links.py --target github --live

Needs: Node (for vitest, in web/ with `npm ci` done); for --target wiki, `pip install
playwright` then `playwright install chromium`.

Exit codes: 0 every anchor found; 1 an anchor or a page is missing; 2 it could not run
(bad arguments, no Playwright, no guide directory, the UI step failed, or a sign-in page).
"""
from __future__ import annotations

import argparse
import contextlib
import json
import os
import re
import subprocess
import sys
import tempfile
import urllib.request
from collections import defaultdict
from pathlib import Path
from urllib.parse import unquote, urldefrag, urlparse

ORCH = Path(__file__).resolve().parent.parent
WEB = ORCH / "web"
EXPORT_TEST = "src/docs/docsHrefs.export.test.tsx"
GUIDE = ORCH.parent / "docs" / "user-guide"


def default_github_base() -> str:
    """The default docsUrl (orchestrator/docs_default.json): the guide at the release tag."""
    d = json.loads((ORCH / "docs_default.json").read_text())
    return d["urlTemplate"].replace("{ref}", d["ref"])


def parse_cookies(path: Path) -> list[dict]:
    """A Netscape cookie file (curl's and most SSO tools' format) as Playwright cookies.
    `#HttpOnly_` lines are cookies; other `#` lines are comments."""
    cookies = []
    for line in path.read_text().splitlines():
        if line.startswith("#HttpOnly_"):
            line = line[len("#HttpOnly_"):]
        elif line.startswith("#") or not line.strip():
            continue
        p = line.split("\t")
        if len(p) < 7:
            continue
        cookies.append({"name": p[5], "value": p[6], "domain": p[0], "path": p[2],
                        "secure": p[3] == "TRUE",
                        "expires": int(p[4]) if p[4].isdigit() and int(p[4]) > 0 else -1})
    return cookies


def hrefs_from_ui(base: str) -> dict[str, str]:
    """{featureId: href}, read from the rendered popovers (vitest, jsdom)."""
    with tempfile.TemporaryDirectory() as d:
        out = Path(d) / "hrefs.json"
        env = {**os.environ, "DOCS_HREFS_OUT": str(out), "DOCS_HREFS_BASE": base}
        # S607: npx from PATH, as a developer runs it by hand; the argv is fixed and there is no shell.
        r = subprocess.run(["npx", "vitest", "run", EXPORT_TEST], cwd=WEB, env=env,  # noqa: S603,S607 - fixed argv, no shell; npx from PATH  # nosec B603
                           capture_output=True, text=True, check=False)
        if r.returncode != 0 or not out.exists():
            sys.stderr.write(r.stdout[-4000:] + r.stderr[-4000:])
            raise SystemExit("check_docs_links: could not read the popovers' links (vitest failed); "
                             "run `npm ci` in orchestrator/web first")
        return json.loads(out.read_text())


def github_slug(text: str) -> str:
    """GitHub's heading id (github-slugger): lower case, every character but letters, digits,
    marks, "-", "_" and spaces removed, each space a "-". Mirrors githubSlug in docsLinks.ts."""
    return re.sub(r"[^\w\- ]", "", text.lower()).replace(" ", "-")


def markdown_headings(md: str) -> list[str]:
    """A Markdown page's ATX headings outside fenced code, as GitHub renders their text."""
    out, fence = [], ""
    for line in md.splitlines():
        f = re.match(r"^\s{0,3}(`{3,}|~{3,})", line)
        if f:
            if not fence:
                fence = f.group(1)[0]
            elif f.group(1)[0] == fence:
                fence = ""
            continue
        if fence:
            continue
        m = re.match(r"^\s{0,3}#{1,6}\s+(.*?)(?:\s+#+)?\s*$", line)
        if not m:
            continue
        t = m.group(1)
        t = re.sub(r"!\[([^\]]*)\]\([^)]*\)", r"\1", t)
        t = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", t)
        t = re.sub(r"<[^>]+>", "", t).replace("`", "")
        t = re.sub(r"\*\*|__|\*", "", t)
        t = re.sub(r"\\([\\`*_{}\[\]()#+\-.!])", r"\1", t)
        out.append(t.strip())
    return out


def github_ids(headings: list[str]) -> set[str]:
    """Every heading's id on the page: a repeat of the same slug gets -1, -2."""
    seen: dict[str, int] = {}
    ids = set()
    for h in headings:
        s = github_slug(h)
        n = seen.get(s, 0)
        seen[s] = n + 1
        ids.add(f"{s}-{n}" if n else s)
    return ids


def by_page(hrefs: dict[str, str], base: str) -> tuple[dict[str, list[tuple[str, str]]], list[str]]:
    """{page URL: [(feature, anchor)]}, and the links that are not under base with an #anchor."""
    pages: dict[str, list[tuple[str, str]]] = defaultdict(list)
    bad = []
    for feature, href in sorted(hrefs.items()):
        page, anchor = urldefrag(href)
        if not href.startswith(base) or not anchor:
            bad.append(f"{feature}: {href} is not under {base} with an #anchor")
            continue
        pages[page].append((feature, anchor))
    return pages, bad


def fetch(url: str, timeout: int) -> str:
    # https only: main() refuses any other base URL, and every page is under it.
    req = urllib.request.Request(url, headers={"User-Agent": "agentexpress-check-docs-links"})  # noqa: S310 - https:// base only  # nosec B310 - https:// base only
    with urllib.request.urlopen(req, timeout=timeout) as r:  # noqa: S310 - https:// base only  # nosec B310 - https:// base only
        return r.read().decode("utf-8", "replace")


def check_github(a, base: str) -> int:
    guide = Path(a.guide_dir).expanduser().resolve()
    if not guide.is_dir():
        print(f"check_docs_links: no user guide at {guide} (--guide-dir)", file=sys.stderr)
        return 2
    hrefs = hrefs_from_ui(base)
    pages, bad = by_page(hrefs, base)
    print(f"{len(hrefs)} links on {len(pages)} pages; local Markdown in {guide}"
          + ("; live on GitHub" if a.live else ""))
    missing = []
    for url, want in sorted(pages.items()):
        rel = unquote(url[len(base):])
        f = guide / rel
        ids = github_ids(markdown_headings(f.read_text())) if f.is_file() else None
        html, load_err = "", ""
        if a.live:
            try:
                html = fetch(url, a.timeout)
            except Exception as e:  # noqa: BLE001 - reported, then the next page
                load_err = f"{type(e).__name__}: {str(e).splitlines()[0] if str(e) else ''}"
        for feature, anchor in want:
            ok_local = ids is not None and anchor in ids
            # GitHub's rendered id, quoted in the HTML or escaped in its JSON payload.
            ok_live = None if not a.live else (not load_err and bool(
                re.search(rf"user-content-{re.escape(anchor)}(?![\w-])", html)))
            ok = ok_local and ok_live is not False
            live = "" if ok_live is None else (" live:ok" if ok_live else " live:MISS")
            print(f"  {'ok  ' if ok else 'MISS'} {feature:28} {rel}#{anchor}{live}")
            if ids is None:
                missing.append(f"{feature}: no {rel} in {guide}")
            elif not ok_local:
                missing.append(f"{feature}: no heading #{anchor} in {rel}")
            if load_err:
                missing.append(f"{feature}: {url} did not load ({load_err})")
            elif ok_live is False:
                missing.append(f"{feature}: no user-content-{anchor} on {url}")
    for line in bad + missing:
        print(f"FAIL {line}", file=sys.stderr)
    failed = len(bad) + len({m.split(":", 1)[0] for m in missing})
    print(f"{len(hrefs) - failed} of {len(hrefs)} links found their section")
    return 1 if bad or missing else 0


def check_wiki(a, base: str) -> int:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("check_docs_links: needs Playwright: pip install playwright && playwright install chromium",
              file=sys.stderr)
        return 2

    hrefs = hrefs_from_ui(base)
    pages, bad = by_page(hrefs, base)
    print(f"{len(hrefs)} links on {len(pages)} pages")

    site = urlparse(base).hostname
    missing = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        ctx = browser.new_context()
        if a.cookies:
            ctx.add_cookies(parse_cookies(a.cookies.expanduser()))
        tab = ctx.new_page()
        for url, want in sorted(pages.items()):
            try:
                tab.goto(url, wait_until="domcontentloaded", timeout=a.timeout * 1000)
                # A page that keeps polling never goes network-idle, and is still loaded.
                with contextlib.suppress(Exception):
                    tab.wait_for_load_state("networkidle", timeout=a.timeout * 1000)
            except Exception as e:  # noqa: BLE001 - reported, then the next page
                why = f"{type(e).__name__}: {str(e).splitlines()[0]}"
                missing += [f"{feature}: {url} did not load ({why})" for feature, _ in want]
                continue
            landed = urlparse(tab.url).hostname
            if landed != site:
                browser.close()
                print(f"check_docs_links: {url} landed on {landed}, a sign-in page, not the documentation: "
                      f"{a.signin_hint}", file=sys.stderr)
                return 2
            ids = set(tab.evaluate("() => [...document.querySelectorAll('[id]')].map((e) => e.id)"))
            for feature, anchor in want:
                ok = anchor in ids
                print(f"  {'ok  ' if ok else 'MISS'} {feature:28} {url}#{anchor}")
                if not ok:
                    missing.append(f"{feature}: no #{anchor} on {url}")
        browser.close()
    for line in bad + missing:
        print(f"FAIL {line}", file=sys.stderr)
    print(f"{len(hrefs) - len(bad) - len(missing)} of {len(hrefs)} links found their section")
    return 1 if bad or missing else 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0],
                                 epilog="\n\n".join(__doc__.split("\n\n")[1:]),
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--target", choices=("wiki", "github"), default="wiki",
                    help="wiki: an XWiki site, checked in a browser (default); github: the user guide's "
                         "Markdown, checked locally and, with --live, on github.com")
    ap.add_argument("--base-url", default=os.environ.get("DOCS_URL", ""),
                    help="the documentation base URL the deployment sets as docsUrl (or $DOCS_URL). Required "
                         "for --target wiki; for --target github, the default in orchestrator/docs_default.json")
    ap.add_argument("--cookies", type=Path, default=None,
                    help="--target wiki: a Netscape-format cookie file holding a signed-in session for the site")
    ap.add_argument("--signin-hint", default="sign in again, refresh the cookie file, and run this again",
                    help="--target wiki: what to tell the reader when a page lands on a sign-in page")
    ap.add_argument("--guide-dir", default=str(GUIDE),
                    help="--target github: the generated Markdown to check against (default <repo>/docs/user-guide)")
    ap.add_argument("--live", action="store_true",
                    help="--target github: also fetch each page from github.com (public, no sign-in) and look "
                         "for its user-content-<slug> id")
    ap.add_argument("--timeout", type=int, default=120, help="seconds per page (default 120)")
    a = ap.parse_args()
    base = a.base_url.strip() or (default_github_base() if a.target == "github" else "")
    if not base.startswith("https://"):
        print("check_docs_links: --base-url (or $DOCS_URL) must be an https:// URL", file=sys.stderr)
        return 2
    if not base.endswith("/"):
        base += "/"
    # The link's shape follows the URL (docsTarget in docsLinks.ts): /bin/view/ is a wiki.
    wiki_shaped = "/bin/view/" in urlparse(base).path
    if wiki_shaped != (a.target == "wiki"):
        print(f"check_docs_links: --target {a.target} needs a base URL "
              f"{'with' if a.target == 'wiki' else 'without'} /bin/view/ in its path, not {base}", file=sys.stderr)
        return 2
    return check_github(a, base) if a.target == "github" else check_wiki(a, base)


if __name__ == "__main__":
    sys.exit(main())
