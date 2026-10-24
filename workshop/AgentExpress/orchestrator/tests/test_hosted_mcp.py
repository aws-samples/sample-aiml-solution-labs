"""A hosted MCP server (tools.<key> = {"type": "mcp", "hosted": {code?, network?}}):
  * the server validator gives the page's issues for tests/fixtures/hosted_mcp_cases.json
    (vitest: web/src/builder/hostedMcp.parity.test.ts);
  * the deployer fills app/tools/_code/<key>/ for the image (git / S3 code fetched through
    deployer/code_fetch.py, mocked here; a default Dockerfile when the code has none);
  * scaffold.py writes the files written in the build, the bundle carries them, and the
    Assistant can write them with set_tool_code.
"""
from __future__ import annotations

import copy
import importlib
import json
import sys
import types

import pytest
from conftest import ORCH_ROOT

sys.path.insert(0, str(ORCH_ROOT / "bff"))
import buildstore
import validate_build

BASE = json.loads((ORCH_ROOT / "tests" / "fixtures" / "validation_cases.json").read_text())["base"]
CASES = json.loads((ORCH_ROOT / "tests" / "fixtures" / "hosted_mcp_cases.json").read_text())["cases"]
IMAGE = "123456789012.dkr.ecr.us-east-1.amazonaws.com/team/orders-mcp:1.2"


def _apply(ops: list) -> dict:
    wf = copy.deepcopy(BASE)
    for _, path, value in ops:
        at = wf
        for k in path[:-1]:
            at = at[k]
        at[path[-1]] = copy.deepcopy(value)
    return wf


@pytest.mark.parametrize("case", CASES, ids=[c["name"] for c in CASES])
def test_the_server_validator_agrees_with_the_page_on_hosted_mcp(case):
    got = [i for i in validate_build.validate(_apply(case["ops"])) if i["path"].startswith("tools.")]
    assert got == case["expect"]


# --- the deployer ------------------------------------------------------------------------

@pytest.fixture()
def runner(monkeypatch, tmp_path):
    sys.modules.pop("runner", None)
    sys.path.insert(0, str(ORCH_ROOT / "deployer"))
    try:
        mod = importlib.import_module("runner")
    finally:
        sys.path.pop(0)
    monkeypatch.setattr(mod, "ROOT", tmp_path)
    yield mod
    sys.modules.pop("runner", None)


def _wf(hosted: dict) -> dict:
    return {"tools": {"orders": {"type": "mcp", "description": "Orders", "hosted": hosted},
                      "web": {"type": "mcp", "endpoint": "https://x.example.com/mcp"}}}


@pytest.fixture()
def fetched(monkeypatch):
    """A stand-in deployer/code_fetch.py: records each materialise and writes a server.py."""
    calls = []

    class CodeSourceError(Exception):
        pass

    def materialise(source, dest_dir, *, tokens=None, s3=None, **_):
        calls.append((source, dest_dir, tokens, s3))
        dest_dir.mkdir(parents=True, exist_ok=True)
        if source.get("git", {}).get("ref") == "missing":
            raise CodeSourceError("ref not found")
        (dest_dir / "server.py").write_text("print('mcp')\n")
        return dest_dir
    monkeypatch.setitem(sys.modules, "code_fetch", types.SimpleNamespace(
        materialise=materialise, CodeSourceError=CodeSourceError))
    return calls


def test_code_written_in_the_build_gets_the_default_dockerfile(runner, fetched):
    folder = runner.ROOT / "app" / "tools" / "_code" / "orders"
    folder.mkdir(parents=True)
    (folder / "server.py").write_text("print('mcp')\n")
    (folder / "requirements.txt").write_text("mcp==1.0.0\n")
    assert runner.prepare_hosted_mcp(_wf({}), "b1", {}) == ["orders"]
    assert (folder / "Dockerfile").read_text() == runner.HOSTED_MCP_DOCKERFILE
    assert "linux/arm64" in runner.HOSTED_MCP_DOCKERFILE and "EXPOSE 8000" in runner.HOSTED_MCP_DOCKERFILE
    assert fetched == []


def test_its_own_dockerfile_is_kept(runner, fetched):
    folder = runner.ROOT / "app" / "tools" / "_code" / "orders"
    folder.mkdir(parents=True)
    (folder / "Dockerfile").write_text("FROM x\n")
    runner.prepare_hosted_mcp(_wf({}), "b1", {})
    assert (folder / "Dockerfile").read_text() == "FROM x\n"


def test_git_code_is_fetched_with_the_build_tokens(runner, fetched, monkeypatch):
    src = {"git": {"url": "https://git.example.com/o/r.git", "ref": "main", "token": "gh"}}
    monkeypatch.setattr(runner, "read_git_tokens", lambda env, bid: {"gh": "secret"}, raising=False)
    assert runner.prepare_hosted_mcp(_wf({"code": src}), "b1", {}) == ["orders"]
    [(source, dest, tokens, s3)] = fetched
    assert source == src and dest == runner.ROOT / "app" / "tools" / "_code" / "orders"
    assert tokens == {"gh": "secret"} and s3 is None
    assert (dest / "Dockerfile").is_file()


def test_a_later_private_git_server_gets_the_tokens_whatever_comes_first(runner, fetched,
                                                                          monkeypatch):
    reads = []
    monkeypatch.setattr(runner, "read_git_tokens",
                        lambda env, bid: reads.append(bid) or {"gh": "secret"}, raising=False)
    wf = {"tools": {
        "first": {"type": "mcp", "hosted": {"code": {"s3": {"uri": "s3://bucket-name/mcp/"}}}},
        "plain": {"type": "mcp", "hosted": {"code": {"git": {
            "url": "https://git.example.com/o/a.git", "ref": "main"}}}},
        "private": {"type": "mcp", "hosted": {"code": {"git": {
            "url": "https://git.example.com/o/b.git", "ref": "main", "token": "gh"}}}}}}
    assert runner.prepare_hosted_mcp(wf, "b1", {}, s3_for=lambda s, t: object()) == [
        "first", "plain", "private"]
    assert reads == ["b1"]
    assert [tokens for _, _, tokens, _ in fetched] == [{"gh": "secret"}] * 3


def test_no_token_named_or_a_destroy_reads_no_tokens(runner, fetched, monkeypatch):
    def boom(env, bid):
        raise AssertionError("tokens read")
    monkeypatch.setattr(runner, "read_git_tokens", boom, raising=False)
    src = {"git": {"url": "https://git.example.com/o/r.git", "ref": "main"}}
    runner.prepare_hosted_mcp(_wf({"code": src}), "b1", {})
    assert fetched[0][2] == {}
    src = {"git": {"url": "https://git.example.com/o/r.git", "ref": "main", "token": "gh"}}
    runner.prepare_hosted_mcp(_wf({"code": src}), "b1", {}, action="destroy")


def test_s3_code_is_read_with_the_scoped_client(runner, fetched):
    src = {"s3": {"uri": "s3://bucket-name/mcp/"}}
    client = object()
    runner.prepare_hosted_mcp(_wf({"code": src}), "b1", {}, s3_for=lambda s, t: client)
    assert fetched[0][3] is client


def test_s3_code_in_the_consoles_own_account_reads_only_a_listed_bucket(runner, fetched, monkeypatch):
    """No connected account: the read goes through runner.code_source_s3, so a bucket that is
    not in CODE_SOURCE_BUCKETS is refused before anything is fetched, as for a Lambda tool;
    bff/builds.py deploy says the same before a job starts."""
    import boto3
    import code_source

    class Sts:
        def get_caller_identity(self):
            return {"Account": "111122223333"}

    class S3:
        def get_bucket_tagging(self, **_):
            raise type("E", (Exception,), {"response": {"Error": {"Code": "NoSuchTagSet"}}})()
    s3 = S3()
    monkeypatch.setattr(boto3, "client", lambda name, **_: s3 if name == "s3" else Sts())
    wf = _wf({"code": {"s3": {"uri": "s3://bucket-name/mcp/"}}})
    with pytest.raises(runner.StepFailed, match=r"tools\.orders\.hosted\.code: .*not in this "
                       r"console's codeSourceBuckets"):
        runner.prepare_hosted_mcp(wf, "b1", {})
    assert fetched == []
    [refusal] = code_source.console_account_refusals(wf, code_source.allowed_buckets({}))
    assert refusal.startswith("tools.orders.hosted.code reads bucket bucket-name")
    # Listed: read with this container's client.
    runner.prepare_hosted_mcp(wf, "b1", {"CODE_SOURCE_BUCKETS": "bucket-name"})
    assert fetched[0][3] is s3


def test_hosted_code_is_checked_by_the_shared_code_source_validator():
    import code_source
    import hosted_mcp_check
    for src in ({"git": {"url": "http://x/r.git", "ref": "main"}}, {"s3": {"uri": "s3://B/x"}},
                {"image": "docker.io/x:1"}, {"git": {}, "s3": {}}):
        got = []
        hosted_mcp_check.check("orders", {"type": "mcp", "hosted": {"code": src}}, "tools.orders", {},
                               lambda sev, w, p, m, got=got: got.append((p, m)))
        assert got == code_source.problems(src, "tools.orders.hosted.code", allow=("git", "s3", "image"))
        assert got


def test_an_image_needs_nothing_and_a_destroy_fetches_nothing(runner, fetched):
    assert runner.prepare_hosted_mcp(_wf({"code": {"image": IMAGE}}), "b1", {}) == []
    src = {"git": {"url": "https://git.example.com/o/r.git", "ref": "main"}}
    assert runner.prepare_hosted_mcp(_wf({"code": src}), "b1", {}, action="destroy") == ["orders"]
    assert fetched == []
    assert (runner.ROOT / "app" / "tools" / "_code" / "orders" / "Dockerfile").is_file()


def test_a_fetch_failure_or_no_server_fails_the_deploy_by_name(runner, fetched):
    src = {"git": {"url": "https://git.example.com/o/r.git", "ref": "missing"}}
    with pytest.raises(runner.StepFailed, match=r"tools\.orders\.hosted\.code: ref not found"):
        runner.prepare_hosted_mcp(_wf({"code": src}), "b1", {})
    with pytest.raises(runner.StepFailed, match=r"neither a Dockerfile nor a server\.py"):
        runner.prepare_hosted_mcp({"tools": {"other": {"type": "mcp", "hosted": {}}}}, "b1", {})


def test_unpinned_requirements_are_refused_before_the_image_is_built(runner, fetched):
    folder = runner.ROOT / "app" / "tools" / "_code" / "orders"
    folder.mkdir(parents=True)
    (folder / "server.py").write_text("x\n")
    (folder / "requirements.txt").write_text("git+https://example.com/x.git\n")
    with pytest.raises(runner.StepFailed, match=r"requirements\.txt"):
        runner.prepare_hosted_mcp(_wf({}), "b1", {})


# --- scaffold, bundle and the Assistant ----------------------------------------------------

def test_scaffold_checks_the_files_of_a_hosted_server():
    sys.path.insert(0, str(ORCH_ROOT))
    try:
        scaffold = importlib.import_module("scaffold")
    finally:
        sys.path.pop(0)
    assert scaffold.hosted_mcp_code(_wf({})) == ["orders"]
    assert scaffold.hosted_mcp_code(_wf({"code": {"image": IMAGE}})) == []
    assert scaffold.hosted_code_problems("orders", {"server.py": "x", "requirements.txt": "", "Dockerfile": "FROM x"}) == []
    assert scaffold.hosted_code_problems("orders", {"requirements.txt": ""})[0].endswith("neither server.py nor a Dockerfile")
    assert "is not a file" in scaffold.hosted_code_problems("orders", {"server.py": "x", "run.sh": "x"})[0]


def test_the_bundle_carries_a_hosted_servers_files_and_only_while_written_in_the_build():
    files = {"server.py": "x"}
    project = {"workflow": _wf({}), "toolCode": {"orders": files, "gone": {"handler.py": "x"}}}
    assert buildstore.bundle_of(project, 1)["toolCode"] == {"orders": files}
    project["workflow"] = _wf({"code": {"image": IMAGE}})
    assert "toolCode" not in buildstore.bundle_of(project, 1)


def test_the_assistant_can_write_a_hosted_server_and_is_told_how():
    designer = importlib.import_module("designer")
    project = {"workflow": {**copy.deepcopy(BASE), "tools": _wf({})["tools"]}, "prompts": {}}
    new, _ = designer.apply_ops(project, [
        {"op": "set_tool_code", "key": "orders", "files": {"server.py": "print(1)", "requirements.txt": "mcp==1.0.0"}}])
    assert new["toolCode"]["orders"] == {"server.py": "print(1)", "requirements.txt": "mcp==1.0.0"}
    with pytest.raises(designer.OpError):
        designer.apply_ops(project, [{"op": "set_tool_code", "key": "web", "files": {"server.py": "x"}}])
    text = designer._spec_text()
    assert "### Hosted MCP server (tools.<key>.hosted)" in text and "0.0.0.0, port 8000, path /mcp" in text


def test_a_hosted_server_starts_from_the_frameworks_own_mcp_and_pydantic_pins():
    """One pin set for a hosted server written in the build: the Assistant is told it and the
    canvas starter writes it. It is the framework's own (requirements.lock, which the runtime
    image installs), so it is a set known to import FastMCP; mcp==1.12.4 with an unpinned
    pydantic failed at import on a live deploy."""
    designer = importlib.import_module("designer")
    pins = designer.HOSTED_MCP_REQUIREMENTS
    lock = {ln.split("==")[0].lower().replace("_", "-"): ln.strip()
            for ln in (ORCH_ROOT / "requirements.lock").read_text().splitlines() if "==" in ln}
    assert [p.split("==")[0] for p in pins] == ["mcp", "pydantic"]
    assert all(lock[p.split("==")[0]] == p for p in pins)
    assert ", ".join(pins) in designer._spec_text()
    tsx = (ORCH_ROOT / "web" / "src" / "builder" / "HostedMcp.tsx").read_text()
    m = __import__("re").search(r'export const HOSTED_MCP_REQUIREMENTS = "([^"]*)";', tsx)
    assert m and m.group(1).encode().decode("unicode_escape") == "".join(p + "\n" for p in pins)
