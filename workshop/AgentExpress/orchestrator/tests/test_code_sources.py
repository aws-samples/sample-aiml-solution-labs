"""Code sources (F2): where a Lambda tool's, an interceptor's or a Run code step's code
comes from, when it is not written or uploaded in the build.
  * bff/code_source.py checks the shape (the parity cases in validation_cases.json check
    the same words in validate.ts);
  * a git or S3 function travels as a reference: the bundle holds no files for it;
  * deployer/code_fetch.py fetches git at a pinned ref (a local repository stands in for a
    host) and S3 (a fake client), inside the limits, refusing zip slip and symbolic links,
    and never says a token;
  * the runner puts a fetched function where CDK and Terraform package written code, and a
    fetched script into workflow.json as task.code;
  * the Assistant takes an attached file as a tool's file, and knows the key.
"""
from __future__ import annotations

import io
import json
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest
from conftest import ORCH_ROOT

sys.path.insert(0, str(ORCH_ROOT / "bff"))
sys.path.insert(0, str(ORCH_ROOT / "deployer"))
import buildstore
import code_fetch
import code_source
import runner

GIT = {"git": {"url": "https://github.com/example-org/tools.git", "ref": "v1.2.0", "path": "tools/lookup",
               "token": "tools-repo"}}
S3_ZIP = {"s3": {"uri": "s3://amzn-s3-demo-bucket/tools/lookup.zip"}}
IMAGE = {"image": "111122223333.dkr.ecr.us-east-1.amazonaws.com/my-mcp:1.0"}


# --- the shape -------------------------------------------------------------------------

def test_the_three_variants_and_where_each_is_allowed():
    assert code_source.problems(GIT, "p") == [] and code_source.problems(S3_ZIP, "p") == []
    assert code_source.problems({"s3": {"uri": "s3://amzn-s3-demo-bucket/tools/"}}, "p") == []
    # An image only where it is allowed (a hosted MCP server's hosted.code).
    assert code_source.problems(IMAGE, "p") == [("p", "must have exactly one of: git or s3")]
    assert code_source.problems(IMAGE, "p", allow=("git", "s3", "image")) == []
    assert code_source.problems({"image": "docker.io/library/nginx"}, "p", allow=("image",))[0][0] == "p.image"
    assert code_source.kind(GIT) == "git" and code_source.kind({}) == "" and code_source.kind(None) == ""


@pytest.mark.parametrize("bad,field", [
    ({"git": {"url": "http://example.com/r.git", "ref": "main"}}, "url"),
    ({"git": {"url": "https://user:pw@example.com/r.git", "ref": "main"}}, "url"),
    ({"git": {"url": "https://example.com/r.git?x=1", "ref": "main"}}, "url"),
    ({"git": {"url": "https://example.com/r.git", "ref": "--upload-pack=x"}}, "ref"),
    ({"git": {"url": "https://example.com/r.git", "ref": "a..b"}}, "ref"),
    ({"git": {"url": "https://example.com/r.git", "ref": "main", "token": "ghp_x y"}}, "token"),
    ({"git": {"url": "https://example.com/r.git", "ref": "main", "path": "../etc"}}, "path"),
    ({"git": {"url": "https://example.com/r.git", "ref": "main", "path": "/abs"}}, "path"),
    ({"s3": {"uri": "s3://B/x/"}}, "uri"),
    ({"s3": {"uri": "s3://amzn-s3-demo-bucket/run.py"}}, "uri"),
])
def test_a_bad_source_is_named_by_its_field(bad, field):
    (path, _msg), *_ = code_source.problems(bad, "c")
    assert path.endswith(f".{field}")


def test_where_sources_sit_and_which_tokens_a_build_names():
    wf = {"tools": {"a": {"type": "lambda", "code": {"codeSource": GIT}}, "b": {"type": "lambda", "code": {}},
                    "m": {"type": "mcp", "hosted": {"code": {"git": {"url": "https://h/r", "ref": "x", "token": "mcp-repo"}}}}},
          "orchestrator": {"interceptors": {"request": {"code": {"codeSource": S3_ZIP}}}},
          "agents": {"s": {"runtime": "task", "task": {"kind": "code", "codeSource": S3_ZIP}}}}
    assert code_source.located(wf) == [("tool", "a", GIT), ("interceptor", "interceptor-request", S3_ZIP),
                                       ("task", "s", S3_ZIP)]
    assert code_source.fetched_functions(wf) == {"a", "interceptor-request"}
    assert code_source.secret_names(wf) == ["mcp-repo", "tools-repo"]
    assert "tools-repo" in code_source.describe(GIT) and "secret" in code_source.describe(GIT)


def test_a_fetched_function_travels_as_a_reference_only():
    wf = {"tools": {"a": {"type": "lambda", "code": {"codeSource": GIT}}, "b": {"type": "lambda", "code": {}}},
          "agents": {}, "steps": []}
    project = {"id": "p", "workflow": wf, "toolCode": {"a": {"handler.py": "old"}, "b": {"handler.py": "x"}}}
    assert buildstore.code_functions(wf) == ["a", "b"] and buildstore.written_functions(wf) == ["b"]
    assert buildstore.bundle_of(project, 1)["toolCode"] == {"b": {"handler.py": "x"}}
    import scaffold
    assert scaffold.fetched_functions(wf) == {"a"}


# --- git -------------------------------------------------------------------------------

def _git(repo: Path, *args: str) -> str:
    return subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True, text=True,  # noqa: S603,S607 - git from PATH, argv list, no shell  # nosec B603 - test helper, argv list, no shell; git from PATH
                          env={"GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1", "PATH": "/usr/bin:/bin:/opt/homebrew/bin",
                               "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@example.com",
                               "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@example.com"}).stdout.strip()


@pytest.fixture()
def repo(tmp_path):
    r = tmp_path / "origin"
    (r / "tools" / "lookup").mkdir(parents=True)
    (r / "tools" / "lookup" / "handler.py").write_text("def lambda_handler(e, c):\n    return 'v1'\n")
    (r / "scripts").mkdir()
    (r / "scripts" / "run.py").write_text("print('{\"ok\": 1}')\n")
    _git(r, "init", "-q", "-b", "main")
    _git(r, "add", ".")
    _git(r, "commit", "-q", "-m", "v1")
    _git(r, "tag", "v1")
    (r / "tools" / "lookup" / "handler.py").write_text("def lambda_handler(e, c):\n    return 'v2'\n")
    _git(r, "commit", "-q", "-am", "v2")
    return r


def test_git_is_fetched_at_the_pinned_ref_and_path(repo, tmp_path):
    src = {"git": {"url": repo.as_uri(), "ref": "v1", "path": "tools/lookup"}}
    out = code_fetch.materialise(src, tmp_path / "dest", allow_local=True)
    assert (out / "handler.py").read_text().endswith("'v1'\n") and not (out / ".git").exists()
    main = code_fetch.materialise({"git": {**src["git"], "ref": "main"}}, tmp_path / "dest", allow_local=True)
    assert (main / "handler.py").read_text().endswith("'v2'\n")
    assert code_fetch.read_script({"git": {"url": repo.as_uri(), "ref": "main", "path": "scripts/run.py"}},
                                  allow_local=True) == "print('{\"ok\": 1}')\n"


def test_a_deploy_never_fetches_a_local_or_plain_http_repository(repo, tmp_path):
    with pytest.raises(code_fetch.CodeSourceError, match="https://"):
        code_fetch.materialise({"git": {"url": repo.as_uri(), "ref": "main"}}, tmp_path / "d")


def test_a_missing_token_and_a_failed_fetch_never_show_the_token(tmp_path):
    src = {"git": {"url": "https://127.0.0.1:9/org/repo.git", "ref": "main", "token": "repo"}}
    with pytest.raises(code_fetch.CodeSourceError, match="'repo' is not set"):
        code_fetch.materialise(src, tmp_path / "d", tokens={})
    with pytest.raises(code_fetch.CodeSourceError) as e:
        code_fetch.materialise(src, tmp_path / "d", tokens={"repo": "ghp_SECRET123"})
    assert "ghp_SECRET123" not in str(e.value) and "git fetch failed" in str(e.value)
    assert code_fetch._scrub("https://x:ghp_SECRET123@h/r ghp_SECRET123", ["ghp_SECRET123"]) == "https://***@h/r ***"
    # The token goes to git through the environment, never the command line.
    env = code_fetch._git_env("ghp_SECRET123", False)
    assert env["GIT_CONFIG_KEY_4"] == "http.extraHeader" and env["GIT_CONFIG_VALUE_4"].startswith("Authorization: Basic ")


# --- s3 --------------------------------------------------------------------------------

class FakeS3:
    def __init__(self, objects: dict[str, bytes]):
        self.objects, self.read = objects, []

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return self

    def paginate(self, Bucket, Prefix):
        yield {"Contents": [{"Key": k, "Size": len(v)} for k, v in self.objects.items() if k.startswith(Prefix)]}

    def head_object(self, Bucket, Key):
        return {"ContentLength": len(self.objects[Key])}

    def download_file(self, bucket, key, path):
        self.read.append(key)
        Path(path).write_bytes(self.objects[key])


def _zip(entries: dict[str, bytes], link: str = "") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for n, b in entries.items():
            z.writestr(n, b)
        if link:
            info = zipfile.ZipInfo(link)
            info.external_attr = 0o120777 << 16
            z.writestr(info, "/etc/passwd")
    return buf.getvalue()


def test_s3_prefix_and_zip_are_read_inside_the_path(tmp_path):
    s3 = FakeS3({"tools/lookup/handler.py": b"def lambda_handler(e, c): return 1\n",
                 "tools/lookup/lib/util.py": b"X = 1\n", "tools/other/handler.py": b"no"})
    out = code_fetch.materialise({"s3": {"uri": "s3://amzn-s3-demo-bucket/tools/lookup/"}}, tmp_path / "d", s3=s3)
    assert sorted(str(p.relative_to(out)) for p in out.rglob("*.py")) == ["handler.py", "lib/util.py"]
    assert "tools/other/handler.py" not in s3.read
    z = FakeS3({"k.zip": _zip({"src/handler.py": b"def lambda_handler(e, c): return 2\n", "README.md": b"x"})})
    out = code_fetch.materialise({"s3": {"uri": "s3://amzn-s3-demo-bucket/k.zip", "path": "src"}}, tmp_path / "d", s3=z)
    assert [p.name for p in out.iterdir()] == ["handler.py"]
    assert code_fetch.read_script({"s3": {"uri": "s3://amzn-s3-demo-bucket/scripts/", "path": "run.py"}},
                                  s3=FakeS3({"scripts/run.py": b"print(1)"})) == "print(1)"


@pytest.mark.parametrize("archive,why", [
    (_zip({"../escape.py": b"x"}), "relative path"),
    (_zip({"ok.py": b"x"}, link="evil"), "symbolic link"),
    (_zip({"big.bin": b"0" * 2048}), "over"),
], ids=["zip-slip", "symlink", "too-big"])
def test_a_zip_that_escapes_links_or_is_too_big_is_refused(tmp_path, archive, why):
    with pytest.raises(code_fetch.CodeSourceError, match=why):
        code_fetch.materialise({"s3": {"uri": "s3://amzn-s3-demo-bucket/k.zip"}}, tmp_path / "d", s3=FakeS3({"k.zip": archive}),
                               max_bytes=1024)


def test_a_script_over_the_limit_is_refused(tmp_path):
    with pytest.raises(code_fetch.CodeSourceError, match="50000 bytes"):
        code_fetch.read_script({"s3": {"uri": "s3://amzn-s3-demo-bucket/s/", "path": "run.py"}}, s3=FakeS3({"s/run.py": b"#" * 50_001}))


def test_the_s3_read_is_scoped_to_the_configured_path():
    p = code_fetch.scoped_s3_policy("s3://amzn-s3-demo-bucket/tools/lookup/")
    assert p["Statement"] == [
        {"Effect": "Allow", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::amzn-s3-demo-bucket/tools/lookup/*"},
        {"Effect": "Allow", "Action": "s3:ListBucket", "Resource": "arn:aws:s3:::amzn-s3-demo-bucket",
         "Condition": {"StringLike": {"s3:prefix": "tools/lookup/*"}}}]
    z = code_fetch.scoped_s3_policy("s3://amzn-s3-demo-bucket/k.zip")
    assert z["Statement"] == [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::amzn-s3-demo-bucket/k.zip"}]
    # A folder under a path covers that folder only, not a sibling such as lookup-old/.
    f = code_fetch.scoped_s3_policy("s3://amzn-s3-demo-bucket/tools/", "lookup")
    assert f["Statement"][0]["Resource"] == "arn:aws:s3:::amzn-s3-demo-bucket/tools/lookup/*"
    assert f["Statement"][1]["Condition"] == {"StringLike": {"s3:prefix": "tools/lookup/*"}}
    # A Run code script: its one key.
    s = code_fetch.scoped_s3_policy("s3://amzn-s3-demo-bucket/scripts/", "run.py", script=True)
    assert s["Statement"] == [{"Effect": "Allow", "Action": "s3:GetObject",
                               "Resource": "arn:aws:s3:::amzn-s3-demo-bucket/scripts/run.py"}]


# --- a build in the console's own account ------------------------------------------------

class TaggedS3:
    """get_bucket_tagging as S3 answers it: a tag set, NoSuchTagSet, or 403 for a bucket of
    another account (ExpectedBucketOwner)."""
    def __init__(self, tags: dict[str, object]):
        self.tags, self.asked = tags, []

    def get_bucket_tagging(self, Bucket, ExpectedBucketOwner):
        from botocore.exceptions import ClientError
        self.asked.append((Bucket, ExpectedBucketOwner))
        t = self.tags.get(Bucket)
        if isinstance(t, str):
            status = 403 if t == "AccessDenied" else 404
            raise ClientError({"Error": {"Code": t}, "ResponseMetadata": {"HTTPStatusCode": status}}, "GetBucketTagging")
        return {"TagSet": [{"Key": k, "Value": v} for k, v in (t or {}).items()]}


def test_console_owned_buckets_are_refused():
    env = {"BUILDS_BUCKET": "console-builds", "SOURCE_URI": "s3://console-source/deployer.zip"}
    s3 = TaggedS3({"tagged": {"agentexpress:app": "x"}, "plain": "NoSuchTagSet", "theirs": "AccessDenied",
                   "team": {"team": "a"}, "odd": "InternalError"})
    acct = "111122223333"
    assert "of this console" in runner.console_bucket_refusal("console-builds", env, s3, acct)
    assert "of this console" in runner.console_bucket_refusal("console-source", env, s3, acct)
    assert "CDK staging" in runner.console_bucket_refusal(f"cdk-hnb659fds-assets-{acct}-us-east-1", env, s3, acct)
    assert "deployed by it" in runner.console_bucket_refusal("tagged", env, s3, acct)
    assert "cannot be checked" in runner.console_bucket_refusal("odd", env, s3, acct)
    for ok in ("plain", "theirs", "team"):
        assert runner.console_bucket_refusal(ok, env, s3, acct) is None
    assert ("tagged", acct) in s3.asked


def test_without_a_connected_account_only_listed_buckets_are_read(monkeypatch):
    import boto3

    class Sts:
        def get_caller_identity(self):
            return {"Account": "111122223333"}
    s3 = TaggedS3({"code-bucket": {"team": "a"}, "tagged": {"agentexpress:build": "b"}})
    monkeypatch.setattr(boto3, "client", lambda name, **_: s3 if name == "s3" else Sts())
    src = {"s3": {"uri": "s3://code-bucket/tools/"}}
    with pytest.raises(code_fetch.CodeSourceError, match="not in this console's codeSourceBuckets"):
        runner.code_source_s3(src, None, {})
    assert runner.code_source_s3(src, None, {"CODE_SOURCE_BUCKETS": "code-bucket, other"}) is s3
    # Listed, but a bucket of a build: still refused.
    with pytest.raises(code_fetch.CodeSourceError, match="deployed by it"):
        runner.code_source_s3({"s3": {"uri": "s3://tagged/t/"}}, None, {"CODE_SOURCE_BUCKETS": "tagged"})


def test_the_deploy_check_names_each_unlisted_bucket():
    wf = {"tools": {"lookup": {"type": "lambda", "code": {"codeSource": {"s3": {"uri": "s3://code-bucket/t/"}}}},
                    "other": {"type": "lambda", "code": {"codeSource": GIT}}},
          "agents": {"job": {"runtime": "task", "task": {"kind": "code", "codeSource": {"s3": {"uri": "s3://scripts/s/", "path": "a.py"}}}}}}
    out = code_source.console_account_refusals(wf, code_source.allowed_buckets({"CODE_SOURCE_BUCKETS": "code-bucket"}))
    assert len(out) == 1 and out[0].startswith("agents.job.task.codeSource reads bucket scripts")
    assert len(code_source.console_account_refusals(wf, set())) == 2
    assert code_source.console_account_refusals(wf, {"code-bucket", "scripts"}) == []
    assert code_fetch.image_uri(IMAGE) == IMAGE["image"] and code_fetch.image_uri(GIT) is None


# --- the runner ------------------------------------------------------------------------

def test_the_runner_puts_fetched_code_where_written_code_goes(tmp_path, monkeypatch):
    root = tmp_path / "orch"
    (root / "app").mkdir(parents=True)
    wf = {"tools": {"lookup": {"type": "lambda", "code": {"codeSource": {"s3": {"uri": "s3://amzn-s3-demo-bucket/t/"}}}}},
          "agents": {"job": {"runtime": "task", "task": {"kind": "code", "codeSource": {"s3": {"uri": "s3://amzn-s3-demo-bucket/s/", "path": "run.py"}}}}},
          "steps": [{"agent": "job"}]}
    (root / "app" / "workflow.json").write_text(json.dumps(wf))
    monkeypatch.setattr(runner, "ROOT", root)
    s3 = FakeS3({"t/handler.py": b"def lambda_handler(e, c): return 1\n", "s/run.py": b"print('hi')\n"})
    assert runner.materialise_code_sources(wf, "b1", {}, tokens={}, s3_for=lambda _s, script=False: s3) == ["lookup", "job"]
    assert (root / "app" / "tools" / "_code" / "lookup" / "handler.py").is_file()
    task = json.loads((root / "app" / "workflow.json").read_text())["agents"]["job"]["task"]
    assert task == {"kind": "code", "code": "print('hi')\n"}
    # A destroy fetches nothing: stand-ins, so both tools still find what they remove.
    shutil.rmtree(root / "app" / "tools")
    (root / "app" / "workflow.json").write_text(json.dumps(wf))
    runner.materialise_code_sources(wf, "b1", {}, action="destroy", tokens={},
                                    s3_for=lambda _s, script=False: pytest.fail("a destroy reads no S3"))
    assert (root / "app" / "tools" / "_code" / "lookup" / "handler.py").is_file()


def test_a_fetch_that_fails_fails_the_deploy_with_its_place(tmp_path, monkeypatch):
    root = tmp_path / "orch"
    (root / "app").mkdir(parents=True)
    wf = {"tools": {"lookup": {"type": "lambda", "code": {"codeSource": {"s3": {"uri": "s3://amzn-s3-demo-bucket/t/"}}}}}, "agents": {}}
    (root / "app" / "workflow.json").write_text(json.dumps(wf))
    monkeypatch.setattr(runner, "ROOT", root)
    with pytest.raises(runner.StepFailed, match=r"tools\.lookup\.codeSource .*no handler\.py"):
        runner.materialise_code_sources(wf, "b1", {}, tokens={}, s3_for=lambda _s, script=False: FakeS3({"t/util.py": b"x"}))


# --- the runtime -----------------------------------------------------------------------

def test_the_runtime_refuses_a_script_that_was_not_fetched():
    sys.path.insert(0, str(ORCH_ROOT))
    from app.common import task_agent
    spec = {"runtime": "task", "task": {"kind": "code", "codeSource": {"s3": {"uri": "s3://amzn-s3-demo-bucket/s/", "path": "r.py"}}}}
    assert "not fetched at deploy" in task_agent.problem("job", spec, {})
    spec["task"]["code"] = "print(1)"
    assert task_agent.problem("job", spec, {}) == ""
