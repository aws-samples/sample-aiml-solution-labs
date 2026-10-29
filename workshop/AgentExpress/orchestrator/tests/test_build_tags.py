"""Extra tags for every build a console deploys: `-c buildTags='{"k":"v"}'` (CDK) and
`build_tags` (Terraform) put them on the console's deploy project as BUILD_EXTRA_TAGS, and the
deploy runner (deployer/runner.py extra_build_tags, build_tags) merges them into the tags of
every build, under the framework's agentexpress:* tags, which always win.

This file covers the runner's parsing, skip-and-print and precedence, that the merged tags
reach both tools, runs `terraform test` of terraform/tests/build_tags.tftest.hcl, and holds
the three copies of the rules (runner, cdk/lib/build-tags.ts, var.build_tags) equal. The CDK
half is cdk/test/build-tags.test.ts; main() for a console and a connected account is in
tests/test_builds.py.
"""
from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys

import pytest
from conftest import ORCH_ROOT

TF = ORCH_ROOT / "terraform"
HCL = (TF / "builder.tf").read_text()
TS = (ORCH_ROOT / "cdk" / "lib" / "build-tags.ts").read_text()


@pytest.fixture
def runner():
    sys.path.insert(0, str(ORCH_ROOT / "deployer"))
    import runner as r
    return r


def extra(runner, value) -> dict:
    return runner.extra_build_tags({"BUILD_EXTRA_TAGS": json.dumps(value) if not isinstance(value, str) else value})


# --- parsing ----------------------------------------------------------------------------------

@pytest.mark.parametrize("env", [{}, {"BUILD_EXTRA_TAGS": ""}, {"BUILD_EXTRA_TAGS": "{}"}])
def test_unset_or_empty_is_no_tags_and_prints_nothing(runner, env, capsys):
    assert runner.extra_build_tags(env) == {}
    assert capsys.readouterr().out == ""


def test_valid_tags_are_kept_as_given(runner, capsys):
    tags = {"cost-center": "1234", "team": "data platform", "path": "a/b=c+d@e:f_g.h-i", "empty": ""}
    assert extra(runner, tags) == tags
    out = capsys.readouterr().out
    assert "adding 4 extra tag(s) from the console to this build: cost-center, empty, path, team" in out
    assert "skipped" not in out


SECRET = "do-not-print-me"


@pytest.mark.parametrize("key,value,reason", [
    ("aws:x", SECRET, "the key starts with a reserved prefix (aws: or agentexpress:)"),
    ("AWS:x", SECRET, "the key starts with a reserved prefix (aws: or agentexpress:)"),
    ("agentexpress:build", SECRET, "the key starts with a reserved prefix (aws: or agentexpress:)"),
    ("AgentExpress:Foo", SECRET, "the key starts with a reserved prefix (aws: or agentexpress:)"),
    ("", SECRET, "the key must be 1 to 128 characters"),
    ("k" * 129, SECRET, "the key must be 1 to 128 characters"),
    ("long", SECRET + "v" * 257, "the value must be at most 256 characters"),
    ("a#b", SECRET, "the key may use only letters A-Z a-z, digits, spaces and _ . : / = + - @"),
    ("lt", SECRET + "<", "the value may use only letters A-Z a-z, digits, spaces and _ . : / = + - @"),
    ("équipe", SECRET, "the key may use only letters A-Z a-z, digits, spaces and _ . : / = + - @"),
    ("obj", {"x": SECRET}, "the value is not a string"),
])
def test_an_invalid_tag_is_skipped_with_its_reason_and_its_value_never_printed(runner, capsys, key, value, reason):
    assert extra(runner, {key: value, "team": "data"}) == {"team": "data"}
    out = capsys.readouterr().out
    assert f"runner: extra build tag {key[:130]!r} skipped: {reason}" in out
    assert SECRET not in out
    assert "adding 1 extra tag(s) from the console to this build: team" in out


def test_of_two_keys_differing_only_in_case_the_first_is_kept(runner, capsys):
    assert extra(runner, {"team": "b", "Team": "a"}) == {"Team": "a"}
    assert "runner: extra build tag 'team' skipped: the key differs only in case from Team" in capsys.readouterr().out


def test_past_forty_the_rest_are_skipped(runner, capsys):
    tags = {f"k{i:02d}": "v" for i in range(41)}
    kept = extra(runner, tags)
    assert sorted(kept) == [f"k{i:02d}" for i in range(40)]
    assert "runner: extra build tag 'k40' skipped: over the limit of 40 extra tags" in capsys.readouterr().out


@pytest.mark.parametrize("raw", ["{oops", "[]", '"x"'])
def test_not_a_json_object_is_no_tags(runner, raw, capsys):
    assert extra(runner, raw) == {}
    assert ("runner: BUILD_EXTRA_TAGS is not a JSON object of tag key -> value; no extra tags added"
            in capsys.readouterr().out)


# --- precedence and both tools ----------------------------------------------------------------

FRAMEWORK = {"agentexpress:build": "pclaims01", "agentexpress:version": "3",
             "agentexpress:owner": "me@example.com", "agentexpress:console": "console"}


def test_the_framework_tags_always_win(runner):
    tags = runner.build_tags("pclaims01", 3, "sub-1", "me@example.com", "console",
                             {"agentexpress:build": "evil", "agentexpress:owner": "x", "team": "a"})
    assert tags == {**FRAMEWORK, "team": "a"}


def test_no_extras_is_the_framework_tags_alone(runner):
    assert runner.build_tags("pclaims01", 3, "sub-1", "me@example.com", "console") == FRAMEWORK
    assert runner.build_tags("pclaims01", 3, "sub-1", "me@example.com", "console", {}) == FRAMEWORK


def test_the_merged_tags_reach_a_cdk_and_a_terraform_build(runner):
    tags = runner.build_tags("pclaims01", 3, "sub-1", "me@example.com", "console", {"team": "a"})
    flags = runner.cdk_context("ax_12345678", False, tags)
    ctx = [f for f in flags if f.startswith("tags=")]
    assert len(ctx) == 1 and json.loads(ctx[0][len("tags="):]) == tags
    assert runner.tf_vars("ax_12345678", False, "us-east-1", tags)["tags"] == tags


# --- terraform test ---------------------------------------------------------------------------

TERRAFORM = shutil.which("terraform")


@pytest.mark.skipif(not TERRAFORM or not (TF / ".terraform").exists(),
                    reason="terraform, initialised, is needed for the offline plan")
def test_terraform_plans_build_tags_offline():
    """terraform/tests/build_tags.tftest.hcl: unset passes nothing, set passes sorted JSON,
    and each rule refuses a bad value. Mocked providers: no AWS call."""
    r = subprocess.run(  # noqa: S603 - fixed argv, no shell  # nosec B603  # nosemgrep
        [TERRAFORM, "test", "-no-color", "-filter=tests/build_tags.tftest.hcl"],
        cwd=TF, capture_output=True, text=True, timeout=600, check=False)
    assert r.returncode == 0, r.stdout[-4000:] + r.stderr[-2000:]
    assert re.search(r"Success! (\d+) passed, 0 failed", r.stdout), r.stdout[-2000:]


# --- the three copies agree -------------------------------------------------------------------

def _hcl_block() -> str:
    return re.search(r'variable "build_tags" \{.*?\n\}', HCL, re.DOTALL).group(0)


def _hcl_regex() -> str:
    return re.search(r'regex\("((?:[^"\\]|\\.)*)"', _hcl_block()).group(1).replace("\\\\", "\\")


def _ts_regex() -> str:
    return re.search(r"const TAG_CHARS = /(.*)/;", TS).group(1).replace("\\/", "/")


def test_the_three_accept_the_same_characters(runner):
    assert runner.TAG_CHARS.pattern == _hcl_regex() == _ts_regex()


def test_the_three_have_the_same_limit_and_reserved_prefixes(runner):
    assert runner.MAX_BUILD_TAGS == 40
    assert "export const MAX_BUILD_TAGS = 40;" in TS
    assert "length(var.build_tags) <= 40" in _hcl_block()
    assert runner.RESERVED_TAG_PREFIXES == ("aws:", "agentexpress:")
    assert 'export const RESERVED_TAG_PREFIXES = ["aws:", "agentexpress:"];' in TS
    for prefix in runner.RESERVED_TAG_PREFIXES:
        assert f'startswith(lower(k), "{prefix}")' in _hcl_block()


def test_the_three_use_the_same_variable_name(runner):
    assert runner.BUILD_TAGS_ENV == "BUILD_EXTRA_TAGS"
    assert 'export const BUILD_TAGS_ENV = "BUILD_EXTRA_TAGS";' in TS
    assert "BUILD_EXTRA_TAGS = jsonencode(var.build_tags)" in HCL
