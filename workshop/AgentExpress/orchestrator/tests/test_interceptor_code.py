"""The server generates an interceptor's files exactly as the page does (bff/interceptor_code.py,
a port of web/src/builder/interceptorCode.ts). The Interceptors tab takes handler.py for edited
by hand when it is not what its templates generate, so code the Assistant generated must be
byte-identical to the page's. Both fixtures are written by the page's own generator:
  interceptor_cases.json        handler.py for many template sets (interceptorCode.test.ts)
  interceptor_files_cases.json  every file, settings in any order (interceptorCode.parity.test.ts)
"""
from __future__ import annotations

import json
from pathlib import Path

import interceptor_code  # bff/ is on sys.path (conftest)
import pytest

FIXTURES = Path(__file__).resolve().parent / "fixtures"
HANDLERS = json.loads((FIXTURES / "interceptor_cases.json").read_text())["cases"]
FILES = json.loads((FIXTURES / "interceptor_files_cases.json").read_text())["cases"]


@pytest.mark.parametrize("case", HANDLERS, ids=[c["name"] for c in HANDLERS])
def test_handler_matches_the_page(case):
    got = interceptor_code.files(case["point"], case["templates"], {"agents": {}, "tools": {}})
    assert got["handler.py"] == case["handler"]


@pytest.mark.parametrize("case", FILES, ids=[c["name"] for c in FILES])
def test_every_file_matches_the_page(case):
    assert interceptor_code.files(case["point"], case["templates"], case["workflow"]) == case["expect"]


def test_the_templates_are_read_from_the_package_first(tmp_path, monkeypatch):
    """In the Lambda, the staged copy beside the module; in the repo, the page's own."""
    (tmp_path / "request.py").write_text("# staged\n")
    monkeypatch.setattr(interceptor_code, "_DIRS", (str(tmp_path), *interceptor_code._DIRS))
    assert interceptor_code.source("request") == "# staged\n"


def test_which_templates_need_the_request_headers():
    assert interceptor_code.needs_headers("request", {"injectContext": {}})
    assert interceptor_code.needs_headers("request", {"blockTools": {"tools": ["x"], "agents": ["a"]}})
    assert not interceptor_code.needs_headers("request", {"blockTools": {"tools": ["x"]}, "audit": {}})
    assert not interceptor_code.needs_headers("response", {"hideTools": {"tools": ["x"]}})
