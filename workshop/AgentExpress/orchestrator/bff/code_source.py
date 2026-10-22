"""Where a function's code comes from: a code source (F2). Mirrors web/src/builder/codeSource.ts.

A code source sits at tools.<key>.code.codeSource (a Lambda tool written in the build),
orchestrator.interceptors.<point>.code.codeSource, agents.<id>.task.codeSource (a Run code
step) and tools.<key>.hosted.code (a hosted MCP server). Exactly one key:

  {"git": {"url": "https://...", "ref": "<branch|tag|commit>", "path"?: "dir", "token"?: "<secret name>"}}
  {"s3": {"uri": "s3://bucket/prefix/" | "s3://bucket/key.zip", "path"?: "dir"}}
  {"image": "<ECR image URI>"}                      (a hosted MCP server only)

Absent: the code is written or uploaded in the build (the project's toolCode, or task.code).
The deployer fetches git and S3 sources at deploy time (deployer/code_fetch.py). A git
token is the NAME of a build secret of kind gitTokens (bff/builds.py), never the token.

Sources for the values checked here:
  * S3 bucket name: S3Location.bucket pattern,
    https://docs.aws.amazon.com/bedrock-agentcore-control/latest/APIReference/API_S3Location.html
  * ECR image URI: ContainerConfiguration.containerUri pattern and 1..1024 length,
    https://docs.aws.amazon.com/bedrock-agentcore-control/latest/APIReference/API_ContainerConfiguration.html
"""
from __future__ import annotations

import re

VARIANTS = ("git", "s3", "image")
GIT_KEYS = ("url", "ref", "path", "token")
S3_KEYS = ("uri", "path")

#: https only, a host, a path; no user:password@, no query, no fragment.
GIT_URL_RE = re.compile(r"^https://[A-Za-z0-9.-]+(:[0-9]{1,5})?(/[A-Za-z0-9._~%+-]+)+/?$")
#: A branch, a tag or a commit: no spaces, no leading "-", no "..", 1-255 characters.
GIT_REF_RE = re.compile(r"^(?!-)(?!.*\.\.)[A-Za-z0-9._/+-]{1,255}$")
#: A relative POSIX path inside the repository, prefix or zip.
REL_PATH_RE = re.compile(r"^(?!/)(?!.*(^|/)\.\.(/|$))[A-Za-z0-9._ /+-]{1,1024}$")
#: A build secret name (bff/builds.py set_secrets takes names of 1-128 characters).
SECRET_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")
#: S3Location.bucket (API_S3Location.html), then a prefix ending in "/" or a key ending in ".zip".
S3_URI_RE = re.compile(r"^s3://([a-z0-9][a-z0-9.-]{1,61}[a-z0-9])/(([^\s]*/)?|[^\s]*\.zip)$")
#: ContainerConfiguration.containerUri (API_ContainerConfiguration.html), length 1..1024.
IMAGE_RE = re.compile(r"^(([0-9]{12})\.dkr\.ecr\.([a-z0-9-]+)\.amazonaws\.com(\.cn)?|public\.ecr\.aws)/"
                      r"((?:[a-z0-9]+(?:[._-][a-z0-9]+)*/)*[a-z0-9]+(?:[._-][a-z0-9]+)*)"
                      r"(?::([^:@]{1,300}))?(?:@(.+))?$")
IMAGE_MAX = 1024


def kind(source) -> str:
    """"git" | "s3" | "image", or "" when absent or not a code source."""
    if isinstance(source, dict) and len(source) == 1:
        (k,) = source
        return k if k in VARIANTS else ""
    return ""


def problems(source, path: str, allow: tuple[str, ...] = ("git", "s3")) -> list[tuple[str, str]]:
    """[(json path, message)] for a code source at `path`; [] when it is fine."""
    out: list[tuple[str, str]] = []
    names = " or ".join(allow)
    if not isinstance(source, dict) or len(source) != 1:
        return [(path, f"must have exactly one of: {names}")]
    (k,) = source
    if k not in allow:
        return [(path, f"must have exactly one of: {names}")]
    v = source[k]
    p = f"{path}.{k}"
    if k == "image":
        if not isinstance(v, str) or not v or len(v) > IMAGE_MAX or not IMAGE_RE.fullmatch(v):
            out.append((p, ("must be an Amazon ECR image URI, "
                            "<account>.dkr.ecr.<region>.amazonaws.com/<repository>[:tag][@digest]")))
        return out
    if not isinstance(v, dict):
        return [(p, "must be an object")]
    keys = GIT_KEYS if k == "git" else S3_KEYS
    out += [(f"{p}.{extra}", f"unknown key; {k} takes: {', '.join(keys)}") for extra in v if extra not in keys]
    if k == "git":
        url = v.get("url")
        if not isinstance(url, str) or not GIT_URL_RE.fullmatch(url):
            out.append((f"{p}.url",
                        "must be an https:// repository URL with no user name, password, query or fragment"))
        ref = v.get("ref")
        if not isinstance(ref, str) or not GIT_REF_RE.fullmatch(ref):
            out.append((f"{p}.ref", "must pin a branch, a tag or a commit (no spaces, no leading -, no ..)"))
        if "token" in v and not (isinstance(v["token"], str) and SECRET_NAME_RE.fullmatch(v["token"])):
            out.append((f"{p}.token",
                        "must be the name of a build secret (letters, digits, _ . -), never the token itself"))
    else:
        uri = v.get("uri")
        if not isinstance(uri, str) or not S3_URI_RE.fullmatch(uri):
            out.append((f"{p}.uri", "must be s3://<bucket>/<prefix>/ (ending in /) or s3://<bucket>/<key>.zip"))
    if "path" in v and not (isinstance(v["path"], str) and REL_PATH_RE.fullmatch(v["path"])):
        out.append((f"{p}.path", "must be a relative path inside the source (no leading /, no ..)"))
    return out


def located(workflow: dict) -> list[tuple[str, str, dict]]:
    """Every code source in the build Track B handles: (where, function key, source).
    `where` is "tool" (app/tools/_code/<key>/), "interceptor" (app/tools/_code/interceptor-<point>/)
    or "task" (an agent id, task.code). The hosted MCP server's hosted.code is Track A's."""
    out: list[tuple[str, str, dict]] = []
    for key, t in (workflow.get("tools") or {}).items():
        code = t.get("code") if isinstance(t, dict) else None
        if isinstance(code, dict) and "codeSource" in code and str(t.get("type") or "").lower() == "lambda":
            out.append(("tool", key, code["codeSource"]))
    orch = workflow.get("orchestrator") if isinstance(workflow.get("orchestrator"), dict) else {}
    ics = orch.get("interceptors") if isinstance(orch.get("interceptors"), dict) else {}
    for point in ("request", "response"):
        code = (ics.get(point) or {}).get("code") if isinstance(ics.get(point), dict) else None
        if isinstance(code, dict) and "codeSource" in code:
            out.append(("interceptor", f"interceptor-{point}", code["codeSource"]))
    for aid, spec in (workflow.get("agents") or {}).items():
        task = spec.get("task") if isinstance(spec, dict) else None
        if isinstance(task, dict) and "codeSource" in task:
            out.append(("task", aid, task["codeSource"]))
    return out


def fetched_functions(workflow: dict) -> set[str]:
    """The code function keys (buildstore.code_functions) whose files come from git or S3
    at deploy time, so the build holds no toolCode for them."""
    return {key for where, key, src in located(workflow)
            if where in ("tool", "interceptor") and kind(src) in ("git", "s3")}


def secret_names(workflow: dict) -> list[str]:
    """Every git token secret the build names, sorted (hosted.code included)."""
    sources = [src for _, _, src in located(workflow)]
    for t in (workflow.get("tools") or {}).values():
        hosted = t.get("hosted") if isinstance(t, dict) else None
        if isinstance(hosted, dict) and isinstance(hosted.get("code"), dict):
            sources.append(hosted["code"])
    return sorted({s["git"]["token"] for s in sources if kind(s) == "git"
                   and isinstance(s["git"], dict) and isinstance(s["git"].get("token"), str)})


def allowed_buckets(env: dict) -> set[str]:
    """CODE_SOURCE_BUCKETS (-c codeSourceBuckets / var.code_source_buckets): the buckets an
    S3 code source may name when a build deploys into this console's own account."""
    return {b.strip() for b in str(env.get("CODE_SOURCE_BUCKETS") or "").split(",") if b.strip()}


def console_account_refusals(workflow: dict, allowed: set[str]) -> list[str]:
    """For a deploy into this console's own account (no connected account): every S3 code
    source whose bucket is not in `allowed`. There the read uses the deploy project's own
    role, which can read the whole account, so only buckets the operator listed are read.
    A connected account reads with its own deploy role and is not limited by this."""
    sources = [((f"agents.{key}.task" if where == "task" else (
        f"orchestrator.interceptors.{key.split('-', 1)[1]}.code" if where == "interceptor" else f"tools.{key}.code"))
        + ".codeSource", src) for where, key, src in located(workflow)]
    for key, t in (workflow.get("tools") or {}).items():
        hosted = t.get("hosted") if isinstance(t, dict) else None
        if isinstance(hosted, dict) and isinstance(hosted.get("code"), dict):
            sources.append((f"tools.{key}.hosted.code", hosted["code"]))
    out = []
    for where, src in sources:
        if kind(src) != "s3" or not isinstance(src["s3"], dict):
            continue
        bucket = str(src["s3"].get("uri") or "")[len("s3://"):].split("/", 1)[0]
        if bucket not in allowed:
            out.append(f"{where} reads bucket {bucket}, which this console does not allow "
                       f"for builds in its own account (codeSourceBuckets"
                       f"{': ' + ', '.join(sorted(allowed)) if allowed else ' is empty'}); "
                       f"connect an AWS account and deploy there, or ask the operator to list the bucket")
    return out


def describe(source) -> str:
    """One line for logs and exports. Never holds a token (only its secret's name)."""
    k = kind(source)
    if k == "git":
        g = source["git"] if isinstance(source["git"], dict) else {}
        return (f"git {g.get('url')} @ {g.get('ref')}" + (f" path {g['path']}" if g.get("path") else "")
                + (f" (token: secret {g['token']})" if g.get("token") else ""))
    if k == "s3":
        s = source["s3"] if isinstance(source["s3"], dict) else {}
        return f"s3 {s.get('uri')}" + (f" path {s['path']}" if s.get("path") else "")
    if k == "image":
        return f"image {source['image']}"
    return "written in the build"
