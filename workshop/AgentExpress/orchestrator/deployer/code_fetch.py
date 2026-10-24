"""Fetch a code source at deploy time (F2): a git repository pinned to a ref, or S3.

The shape and its checks are bff/code_source.py's (the same rules the Builder applies).
This module does the fetching, inside the deploy's CodeBuild container:

  * git: `git init` + `git fetch --depth 1 origin <ref>` + `git checkout --detach FETCH_HEAD`
    in a temporary directory, so a branch, a tag or a commit is fetched the same way. A
    private repository's token is a build secret (kind gitTokens); it reaches git through
    GIT_CONFIG_* environment variables (an http.extraHeader), never the command line, and
    every message is scrubbed of it.
  * s3: every object under s3://bucket/prefix/ (ListObjectsV2 + GetObject), or one .zip
    object extracted safely (no member outside the directory, no symbolic link).

Limits: MAX_TREE_BYTES is Lambda's 250 MB for the unzipped contents of a deployment
package (https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html), which
is also AgentCore direct code deployment's 250 MB
(https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/runtime-get-started-code-deploy.html).
A Run code script is at most 50,000 bytes (app/common/task_agent.MAX_CODE_BYTES).

The S3 read is narrowed to the configured path with an STS session policy
(scoped_s3_policy): the session's permissions are the intersection of the role's and the
session policy's, and the policy is at most 2,048 characters
(https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html). ListObjectsV2
needs s3:ListBucket on the bucket ARN, narrowed with the s3:prefix condition key;
GetObject needs s3:GetObject on the object ARN
(https://docs.aws.amazon.com/AmazonS3/latest/userguide/amazon-s3-policy-keys.html).
"""
from __future__ import annotations

import base64
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path, PurePosixPath

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "bff"))
import code_source  # the shape and its checks, shared with the Builder

MAX_TREE_BYTES = 250 * 1024 * 1024
MAX_FILES = 10_000
MAX_SCRIPT_BYTES = 50_000
#: STS AssumeRole: inline plus managed session policies, at most 2,048 characters.
MAX_SESSION_POLICY = 2048
GIT_TIMEOUT_SECONDS = 300
SKIP = {".git", "__pycache__"}


class CodeSourceError(Exception):
    """A code source could not be fetched. The message never holds a token."""


# --- helpers -------------------------------------------------------------------------------

def _scrub(text: str, secrets: list[str]) -> str:
    for s in secrets:
        if s:
            text = text.replace(s, "***")
    # Any URL with credentials in it, whatever put them there.
    return re.sub(r"(https?://)[^/@\s]+@", r"\1***@", text)


def _rel(path: str) -> PurePosixPath:
    p = PurePosixPath(path or ".")
    if p.is_absolute() or ".." in p.parts:
        raise CodeSourceError(f"path {path!r} must be a relative path inside the source")
    return p


def _inside(base: Path, rel: PurePosixPath) -> Path:
    target = (base / rel).resolve()
    if target != base.resolve() and base.resolve() not in target.parents:
        raise CodeSourceError(f"path {str(rel)!r} leaves the source")
    return target


def _measure(root: Path, max_bytes: int) -> None:
    """Refuse a tree over the limits, or one holding a symbolic link."""
    total, count = 0, 0
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in SKIP]
        for name in dirnames + filenames:
            if os.path.islink(os.path.join(dirpath, name)):
                raise CodeSourceError(f"{os.path.relpath(os.path.join(dirpath, name), root)} is a "
                                      "symbolic link, which a code source may not hold")
        for name in filenames:
            count += 1
            total += os.path.getsize(os.path.join(dirpath, name))
            if count > MAX_FILES:
                raise CodeSourceError(f"the source holds more than {MAX_FILES} files")
            if total > max_bytes:
                raise CodeSourceError(f"the source is over {max_bytes // (1024 * 1024)} MB")


def _copy_tree(src: Path, dest: Path, max_bytes: int) -> Path:
    if not src.is_dir():
        raise CodeSourceError("path must name a folder of the source, with handler.py at its root"
                              if src.exists() else "path is not in the source")
    _measure(src, max_bytes)
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(src, dest, ignore=shutil.ignore_patterns(*SKIP))
    return dest


def _read_file(path: Path) -> str:
    if path.is_symlink() or not path.is_file():
        raise CodeSourceError("path must name the script file in the source")
    if path.stat().st_size > MAX_SCRIPT_BYTES:
        raise CodeSourceError(f"the script is over {MAX_SCRIPT_BYTES} bytes")
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError as e:
        raise CodeSourceError("the script is not UTF-8 text") from e


# --- git -----------------------------------------------------------------------------------

def _auth_header(token: str) -> str:
    """HTTP Basic: `user:token` as given, else `x-access-token:<token>`."""
    pair = token if ":" in token else f"x-access-token:{token}"
    return "Authorization: Basic " + base64.b64encode(pair.encode()).decode()


def _git_env(token: str, allow_local: bool) -> dict:
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    # No prompt, no system or user git config (credential helpers, hooks paths, rewrites).
    env.update({"GIT_TERMINAL_PROMPT": "0", "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull})
    config = [("protocol.allow", "never"), ("protocol.https.allow", "always"),
              # Symbolic links are checked out as plain files, never followed.
              ("core.symlinks", "false"), ("advice.detachedHead", "false")]
    if allow_local:
        config.append(("protocol.file.allow", "always"))
    if token:
        config.append(("http.extraHeader", _auth_header(token)))
    env["GIT_CONFIG_COUNT"] = str(len(config))
    for i, (k, v) in enumerate(config):
        env[f"GIT_CONFIG_KEY_{i}"], env[f"GIT_CONFIG_VALUE_{i}"] = k, v
    return env


def _git(args: list[str], cwd: Path, env: dict, secrets: list[str]) -> None:
    try:
        proc = subprocess.run(["git", *args], cwd=cwd, env=env, capture_output=True, text=True,  # noqa: S603, S607 - git from PATH, argv list, no shell  # nosec B603 B607
                              timeout=GIT_TIMEOUT_SECONDS, check=False)
    except subprocess.TimeoutExpired as e:
        raise CodeSourceError(f"git {args[0]} took longer than {GIT_TIMEOUT_SECONDS} s") from e
    if proc.returncode != 0:
        raise CodeSourceError(f"git {args[0]} failed: " + _scrub((proc.stderr or proc.stdout)[-800:].strip(), secrets))


def _git_checkout(g: dict, tokens: dict, work: Path, allow_local: bool) -> Path:
    name = g.get("token")
    token = ""  # nosec B105 - empty default; the token comes from the build's gitTokens secret
    if name:
        token = str((tokens or {}).get(name) or "")
        if not token:
            raise CodeSourceError(f"the git token secret {name!r} is not set on the build")
    basic = (token if ":" in token else f"x-access-token:{token}").encode()
    secrets = [token, base64.b64encode(basic).decode()] if token else []
    env = _git_env(token, allow_local)
    repo = work / "repo"
    repo.mkdir()
    _git(["init", "--quiet"], repo, env, secrets)
    _git(["remote", "add", "origin", g["url"]], repo, env, secrets)
    _git(["fetch", "--depth", "1", "--quiet", "origin", g["ref"]], repo, env, secrets)
    _git(["checkout", "--quiet", "--detach", "FETCH_HEAD"], repo, env, secrets)
    return repo


# --- s3 ------------------------------------------------------------------------------------

def _split(uri: str) -> tuple[str, str]:
    bucket, _, key = uri[len("s3://"):].partition("/")
    return bucket, key


def scoped_s3_policy(uri: str, path: str = "", partition: str = "aws", script: bool = False) -> dict:
    """The STS session policy that lets a session read only this source: a .zip's one
    object; a Run code script's one key (`script`, prefix + path); else the folder
    prefix + path + "/" (so a sibling such as `<path>-old/` is not covered)."""
    bucket, key = _split(uri)
    if key.endswith(".zip") or (script and path):
        obj = key if key.endswith(".zip") else key + str(_rel(path))
        stmts = [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": f"arn:{partition}:s3:::{bucket}/{obj}"}]
    else:
        prefix = key + (str(_rel(path)).rstrip("/") + "/" if path else "")
        stmts = [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": f"arn:{partition}:s3:::{bucket}/{prefix}*"},
                 {"Effect": "Allow", "Action": "s3:ListBucket", "Resource": f"arn:{partition}:s3:::{bucket}",
                  "Condition": {"StringLike": {"s3:prefix": f"{prefix}*"}}}]
    policy = {"Version": "2012-10-17", "Statement": stmts}
    import json
    if len(json.dumps(policy, separators=(",", ":"))) > MAX_SESSION_POLICY:
        raise CodeSourceError(f"the S3 location is too long for a {MAX_SESSION_POLICY}-character session policy")
    return policy


def _s3_prefix(s3, bucket: str, prefix: str, dest: Path, max_bytes: int) -> int:
    total, count = 0, 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get("Contents", []):
            rel = o["Key"][len(prefix):]
            if not rel or rel.endswith("/"):
                continue
            count += 1
            total += int(o.get("Size") or 0)
            if count > MAX_FILES:
                raise CodeSourceError(f"the source holds more than {MAX_FILES} files")
            if total > max_bytes:
                raise CodeSourceError(f"the source is over {max_bytes // (1024 * 1024)} MB")
            target = _inside(dest, _rel(rel))
            target.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(bucket, o["Key"], str(target))
    return count


def _safe_extract(archive: Path, dest: Path, max_bytes: int) -> None:
    """Extract a zip, refusing a member outside `dest` (zip slip), a symbolic link, or a
    total over the limits (checked on the declared sizes first, then on what is written)."""
    with zipfile.ZipFile(archive) as zf:
        infos = zf.infolist()
        if len(infos) > MAX_FILES:
            raise CodeSourceError(f"the zip holds more than {MAX_FILES} files")
        if sum(i.file_size for i in infos) > max_bytes:
            raise CodeSourceError(f"the zip is over {max_bytes // (1024 * 1024)} MB unzipped")
        written = 0
        for info in infos:
            name = info.filename.replace("\\", "/")
            if stat.S_ISLNK(info.external_attr >> 16):
                raise CodeSourceError(f"{name} is a symbolic link, which a code source may not hold")
            target = _inside(dest, _rel(name))
            if info.is_dir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(info) as src, open(target, "wb") as out:
                while chunk := src.read(1 << 20):
                    written += len(chunk)
                    if written > max_bytes:
                        raise CodeSourceError(f"the zip is over {max_bytes // (1024 * 1024)} MB unzipped")
                    out.write(chunk)


def _s3_tree(src: dict, s3, work: Path, max_bytes: int) -> Path:
    """The S3 source unpacked under `work`; returns the root it names (before `path`)."""
    if s3 is None:
        raise CodeSourceError("no S3 client to read the source with")
    bucket, key = _split(src["uri"])
    root = work / "s3"
    root.mkdir()
    try:
        if key.endswith(".zip"):
            head = s3.head_object(Bucket=bucket, Key=key)
            if int(head.get("ContentLength") or 0) > max_bytes:
                raise CodeSourceError(f"the zip is over {max_bytes // (1024 * 1024)} MB")
            archive = work / "source.zip"
            s3.download_file(bucket, key, str(archive))
            try:
                _safe_extract(archive, root, max_bytes)
            except zipfile.BadZipFile as e:
                raise CodeSourceError("the S3 object is not a zip file") from e
        else:
            prefix = key + (str(_rel(src["path"])).rstrip("/") + "/" if src.get("path") else "")
            if not _s3_prefix(s3, bucket, prefix, root / _rel(src.get("path") or ""), max_bytes):
                raise CodeSourceError(f"nothing under {src['uri']}{src.get('path') or ''}")
    except CodeSourceError:
        raise
    except Exception as e:  # botocore's, with no secret in it
        raise CodeSourceError(f"cannot read {src['uri']}: {type(e).__name__}: {e}") from e
    return root


def _check(source, allow_local: bool) -> str:
    probs = code_source.problems(source, "codeSource")
    if allow_local and code_source.kind(source) == "git" and str(source["git"].get("url", "")).startswith("file://"):
        probs = [p for p in probs if not p[0].endswith(".url")]
    if probs:
        raise CodeSourceError("; ".join(f"{p}: {m}" for p, m in probs))
    return code_source.kind(source)


# --- the interface (code-source-interface.md) ---------------------------------------------

def image_uri(source) -> str | None:
    """The image a hosted MCP server uses as-is, else None."""
    return source["image"] if code_source.kind(source) == "image" else None


def materialise(source: dict | None, dest_dir: Path, *, files: dict[str, str] | None = None,
                tokens: dict[str, str] | None = None, s3=None, max_bytes: int = MAX_TREE_BYTES,
                allow_local: bool = False) -> Path:
    """Put a function's code into `dest_dir` (emptied first) and return `dest_dir`.
    None: the build's own `files` (name -> text). git / s3: fetched, `path` taken from it.
    `allow_local` lets a test use a file:// repository; a deploy never sets it."""
    dest_dir = Path(dest_dir)
    if source is None:
        if dest_dir.exists():
            shutil.rmtree(dest_dir)
        dest_dir.mkdir(parents=True)
        for name, body in (files or {}).items():
            target = _inside(dest_dir, _rel(name))
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(body)
        return dest_dir
    kind = _check(source, allow_local)
    if kind == "image":
        raise CodeSourceError("an image is used as it is: there is nothing to fetch (image_uri)")
    work = Path(tempfile.mkdtemp(prefix="ax-code-"))
    try:
        if kind == "git":
            root = _git_checkout(source["git"], tokens or {}, work, allow_local)
            sub = source["git"].get("path") or ""
        else:
            root = _s3_tree(source["s3"], s3, work, max_bytes)
            sub = source["s3"].get("path") or ""
        return _copy_tree(_inside(root, _rel(sub)), dest_dir, max_bytes)
    finally:
        shutil.rmtree(work, ignore_errors=True)


def read_script(source: dict, *, tokens: dict[str, str] | None = None, s3=None,
                allow_local: bool = False) -> str:
    """A Run code step's script: the one file `path` names (at most 50,000 bytes, UTF-8)."""
    kind = _check(source, allow_local)
    if kind == "image":
        raise CodeSourceError("a Run code step takes git or s3, not an image")
    spec = source[kind]
    if not spec.get("path"):
        raise CodeSourceError("path must name the script file in the source")
    work = Path(tempfile.mkdtemp(prefix="ax-code-"))
    try:
        if kind == "git":
            root = _git_checkout(spec, tokens or {}, work, allow_local)
        elif spec["uri"].endswith(".zip"):
            root = _s3_tree(spec, s3, work, MAX_TREE_BYTES)
        else:
            # One object: s3://bucket/prefix/ + path.
            if s3 is None:
                raise CodeSourceError("no S3 client to read the source with")
            bucket, key = _split(spec["uri"])
            key += str(_rel(spec["path"]))
            target = work / "script"
            try:
                head = s3.head_object(Bucket=bucket, Key=key)
                if int(head.get("ContentLength") or 0) > MAX_SCRIPT_BYTES:
                    raise CodeSourceError(f"the script is over {MAX_SCRIPT_BYTES} bytes")
                s3.download_file(bucket, key, str(target))
            except CodeSourceError:
                raise
            except Exception as e:  # botocore's, with no secret in it
                raise CodeSourceError(f"cannot read s3://{bucket}/{key}: {type(e).__name__}: {e}") from e
            return _read_file(target)
        return _read_file(_inside(root, _rel(spec["path"])))
    finally:
        shutil.rmtree(work, ignore_errors=True)
