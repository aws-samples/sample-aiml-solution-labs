"""The Assistant and code sources (F2): it knows the codeSource keys, takes a file the user
attached as a tool's file (set_tool_code fromAttachments) rather than retyping it, and asks
for a private repository's token in the secure field (gitTokens), never in the chat."""
# ruff: noqa: F811 - `env` is a fixture, imported so every test can take it
from __future__ import annotations

import copy

import pytest
from test_builds import env  # noqa: F401 - env is the builds fixture, taken by name
from test_designer import BASE

pytest.importorskip("moto")

CODE_TOOL = {"type": "lambda", "description": "Looks things up.", "code": {},
             "toolSchema": [{"name": "lookup", "description": "Look up.",
                             "properties": {"query": {"type": "string", "required": True, "description": "What."}}}]}


def _with_tool():
    p = copy.deepcopy(BASE)
    p["workflow"].setdefault("tools", {})["lookup"] = copy.deepcopy(CODE_TOOL)
    return p


def test_an_attached_file_becomes_a_tools_file(env):
    d = env.handler.designer
    attached = {"my_tool.py": "def lambda_handler(event, context):\n    return {'ok': True}\n"}
    new, changed = d.apply_ops(_with_tool(), [{"op": "set_tool_code", "key": "lookup",
                                               "fromAttachments": {"handler.py": "my_tool.py"},
                                               "files": {"requirements.txt": ""}}],
                               attached=lambda name: attached[name])
    assert new["toolCode"]["lookup"] == {"handler.py": attached["my_tool.py"], "requirements.txt": ""}
    assert "lookup" in changed["tools"]


@pytest.mark.parametrize("op,attached,why", [
    ({"fromAttachments": {"handler.py": "x.py"}}, None, "brought no attached files"),
    ({}, None, "needs `files` or `fromAttachments`"),
])
def test_a_tool_file_from_nothing_is_refused(env, op, attached, why):
    d = env.handler.designer
    with pytest.raises(d.OpError, match=why):
        d.apply_ops(_with_tool(), [{"op": "set_tool_code", "key": "lookup", **op}], attached=attached)


def test_an_attachment_not_in_the_message_is_named(env):
    d = env.handler.designer
    read = d._attached_reader([{"name": "a.py", "source": "upload", "key": "k"}])
    with pytest.raises(d.OpError, match=r"'b\.py' is not attached"):
        read("b.py")
    assert d._attached_reader([]) is None


def test_the_assistant_knows_the_key_and_asks_for_a_repo_token_in_the_secure_field(env):
    d = env.handler.designer
    spec = d._spec_text()
    assert "codeSource" in spec and "gitTokens" in spec
    wf = _with_tool()["workflow"]
    wf["tools"]["lookup"]["code"] = {"codeSource": {"git": {"url": "https://github.com/example-org/t.git",
                                                            "ref": "v1", "token": "tools-repo"}}}
    got = d._secrets_needed([{"gitToken": "tools-repo", "why": "Private repo"}, {"gitToken": "made-up"}], wf)
    assert got == [{"kind": "gitTokens", "name": "tools-repo", "why": "Private repo", "label": "Repository access token"}]
