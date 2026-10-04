"""`web/legacy/observability.js` must parse.

WHY THIS TEST EXISTS
--------------------
The whole stylesheet for the Observability tab lives in a JS TEMPLATE LITERAL, so a
backtick anywhere inside it — including inside a CSS comment — closes the string and turns
the rest of the file into garbage. Written while editing that very stylesheet:

    /* EVERY code surface, including `.obs-seg pre` ... */

The result was `SyntaxError: Unexpected identifier 'pre'`, the module never registered
`window.ObservabilityIsland`, and the Observability tab silently failed to mount. Nothing
in the Python suite, the type-checker or the bundler looks at this file — it is copied to
S3 verbatim, not imported by the bundle — so the first sign of it was a blank tab.

One `node --check` closes the gap for every syntax error, not just that one.
"""

import shutil
import subprocess
from pathlib import Path

import pytest

ISLAND = Path(__file__).resolve().parent.parent / "web" / "legacy" / "observability.js"


def test_the_file_is_still_there():
    """If it moves, the IaC upload and Observability.tsx both break; fail loudly here."""
    assert ISLAND.is_file(), f"{ISLAND} is missing"


@pytest.mark.skipif(shutil.which("node") is None,
                    reason="node is not installed; the UI has its own suite for this")
def test_it_parses_as_javascript():
    # The absolute path from shutil.which, and a fixed argument list: no shell, and
    # nothing here comes from outside the repo.
    node = shutil.which("node")
    assert node is not None
    r = subprocess.run([node, "--check", str(ISLAND)],  # noqa: S603
                       capture_output=True, text=True, check=False)
    assert r.returncode == 0, (
        f"observability.js does not parse, so the Observability tab will not mount:\n"
        f"{r.stderr.strip()}"
    )


def test_the_stylesheet_template_literal_contains_no_backticks():
    """The specific trap, named. `node --check` catches the consequence; this catches the
    cause, and says why in the failure message."""
    text = ISLAND.read_text()
    start = text.index("const CSS = `")
    end = text.index("`;", start)
    css = text[start + len("const CSS = `"):end]
    assert "`" not in css, (
        "a backtick inside the CSS template literal terminates it early. Use plain "
        "quotes in CSS comments, or none at all."
    )
