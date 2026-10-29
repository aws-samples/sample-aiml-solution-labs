"""Deploy or destroy ONE Builder build, from inside CodeBuild.

The console's BFF starts this (POST /api/builds/{id}/deploy or /destroy) as a CodeBuild
build over a FRESH copy of the framework source, so every deploy starts from the same
tree. What it does, in order:

  1. fetches the immutable bundle for the requested version (builds/<id>/versions/<n>)
  2. `scaffold.py apply --exact` — the tree now mirrors that version and nothing else
  3. `cdk deploy|destroy` or `terraform apply|destroy`, whichever the user chose, with
     the build's own `agentName`, so each build is its own stack and deploying one never
     touches another
  4. records the outcome on the build (bff/buildstore.py owns the layout)

Destroy runs steps 1-3 too: both tools need the same configuration to know what to
remove. It uses the tool the build was DEPLOYED with — the BFF passes that, never the
user — because a `cdk destroy` of a Terraform-built stack would remove nothing.

Every setting comes from the environment (the CodeBuild project's plus the per-build
overrides the BFF sends); see REQUIRED. Nothing here is secret.
"""

from __future__ import annotations

import collections
import hashlib
import io
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "bff"))
import bucket_names  # noqa: E402  - the buckets' names, mirrored by both IaC paths
import buildstore  # noqa: E402  - the shared layout, next to the BFF that reads it

REQUIRED = ("ACTION", "TOOL", "BUILD_ID", "VERSION", "AGENT_NAME",
            "BUILDS_TABLE", "BUILDS_BUCKET")
ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
#: Scratch space for the bundle, the terraform binary and the CDK outputs file. A
#: CodeBuild container runs one build and is thrown away, so nothing here is shared.
WORK = Path(tempfile.gettempdir()) / "agentexpress-runner"


class StepFailed(Exception):
    """A command failed; carries the tail of its output for the Build view."""


#: Context keys whose value is never echoed, and any email address elsewhere in an argument
#: (the agentexpress:owner value inside -c tags=...): the build owner's email is PII.
SENSITIVE_KEYS = ("runOwnerEmail",)
EMAIL = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")


def redact(cmd: list[str]) -> list[str]:
    """A copy of argv safe to print: sensitive context values and email addresses are
    `<redacted>`. The command that runs is the original argv, unchanged."""
    out = []
    for arg in cmd:
        key = arg.split("=", 1)[0]
        out.append(f"{key}=<redacted>" if "=" in arg and key in SENSITIVE_KEYS
                   else EMAIL.sub("<redacted>", arg))
    return out


#: The build owner's exact email, once main() has read it. Terraform's own plan/apply/destroy
#: output prints it in the agentexpress:owner tag (tags / tags_all), so every output line
#: written to the log has it replaced. Exact strings, not EMAIL: npm prints name@version.
PRIVATE: set[str] = set()


def redact_output(text: str) -> str:
    """`text` safe to write to the log: each PRIVATE value is `<redacted>`."""
    for value in PRIVATE:
        text = text.replace(value, "<redacted>")
    return text


def run(cmd: list[str], cwd: Path, env: dict | None = None, keep: int = 60) -> list[str]:
    """Run a command, streaming its output to the CodeBuild log, and keep the tail. The log
    gets redact_output(line); the tail stays raw, for the failure matchers."""
    print(f"$ {' '.join(redact(cmd))}", flush=True)
    tail: collections.deque[str] = collections.deque(maxlen=keep)
    proc = subprocess.Popen(cmd, cwd=cwd, env={**os.environ, **(env or {})},  # noqa: S603 - argv list, no shell  # nosec B603  # nosemgrep
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    assert proc.stdout is not None
    for line in proc.stdout:
        sys.stdout.write(redact_output(line))
        tail.append(ANSI.sub("", line.rstrip()))
    sys.stdout.flush()
    if proc.wait() != 0:
        raise StepFailed(f"`{' '.join(redact(cmd[:3]))}` failed (exit {proc.returncode}):\n"
                         + "\n".join(tail))
    return list(tail)


def sign_in(workflow: dict) -> dict:
    """authorization.signIn, or Cognito when the build says nothing: who signs its users in."""
    s = (workflow.get("authorization") or {}).get("signIn") if isinstance(workflow, dict) else None
    s = s if isinstance(s, dict) else {}
    return {**s, "provider": str(s.get("provider") or "cognito")}


#: authorization.signIn field -> CDK context key, per provider.
SIGN_IN_CDK = {"okta": {"domain": "oktaDomain", "clientId": "oktaClientId", "authorizationServer": "oktaAuthServer"},
               "auth0": {"domain": "auth0Domain", "clientId": "auth0ClientId"},
               "entra": {"tenantId": "entraTenantId", "clientId": "entraClientId"}}
#: ... and -> terraform variable field (variables.tf okta / auth0 / entra).
SIGN_IN_TF = {"okta": {"domain": "domain", "clientId": "client_id", "authorizationServer": "authorization_server"},
              "auth0": {"domain": "domain", "clientId": "client_id"},
              "entra": {"tenantId": "tenant_id", "clientId": "client_id"}}


#: The shapes terraform/variables.tf accepts for the console's organisation guardrails.
BOUNDARY_ARN = re.compile(r"^arn:aws[a-z-]*:iam::(aws|[0-9]{12}):policy/[A-Za-z0-9+=,.@_/-]{1,512}$")
ROLE_PATH = re.compile(r"^/([A-Za-z0-9+=,.@_-]+/)*$")


def guardrails(env: dict, target: Target | None = None) -> dict:
    """The console's organisation guardrails, for a build (terraform/hardening.tf): the
    permissions boundary and role path its roles get, or {} when the console sets neither.

    A boundary is a managed policy in the roles' own account, so a build deployed into a
    connected account names THAT account's policy of the same name: an organisation that
    requires a boundary has it in every account. An AWS managed policy is the same everywhere."""
    out: dict = {}
    arn = str(env.get("PERMISSIONS_BOUNDARY_ARN") or "")
    if arn:
        if not BOUNDARY_ARN.match(arn):
            raise StepFailed(f"PERMISSIONS_BOUNDARY_ARN is not a managed policy ARN: {arn!r}")
        if target and ":iam::aws:" not in arn:
            arn = re.sub(r"^(arn:[^:]+:iam::)[0-9]{12}:", rf"\g<1>{target.account}:", arn)
        out["permissionsBoundaryArn"] = arn
    path = str(env.get("IAM_ROLE_PATH") or "/")
    if path != "/":
        if len(path) > 512 or not ROLE_PATH.match(path):
            raise StepFailed(f"IAM_ROLE_PATH is not an IAM path: {path!r}")
        out["iamRolePath"] = path
    return out


def litellm_settings(env: dict) -> dict:
    """The console's LiteLLM proxy and key secret (terraform/litellm.tf), for a build's
    litellm/<model> agents, or {} when the console sets neither. CDK and Terraform check them
    again at synth and plan."""
    base, arn = str(env.get("LITELLM_BASE_URL") or ""), str(env.get("LITELLM_KEY_SECRET_ARN") or "")
    return {"litellmBaseUrl": base, "litellmKeySecretArn": arn} if base and arn else {}


#: Mirrors var.docs_url's validation (terraform/builder.tf) and docsUrl's (cdk/lib/orchestrator-stack.ts).
DOCS_URL = re.compile(r"^https://[^\s/?#]+[^\s]*$")


def docs_settings(env: dict) -> dict:
    """The console's documentation base URL (-c docsUrl / var.docs_url), for a build's own
    app: its pages link to the same documentation. With the console's links off
    (DOCS_LINKS=false: -c docsLinks=false / var.docs_links = false, or an empty docsUrl), the
    build's are off too. {} when the console passes neither: the build's own default."""
    if str(env.get("DOCS_LINKS") or "").strip().lower() == "false":
        return {"docsLinks": "false"}
    url = str(env.get("DOCS_URL") or "").strip()
    if not url:
        return {}
    if not DOCS_URL.match(url):
        raise StepFailed(f"DOCS_URL is not an https:// URL: {url!r}")
    return {"docsUrl": url}


def cdk_context(agent: str, gateway: bool, tags: dict | None = None, signin: dict | None = None,
                guard: dict | None = None, bucket_naming: str = "") -> list[str]:
    """The same flags a developer deploys with, pinned for a build stack.
    Who signs its users in (authorization.signIn): a Cognito pool of its own by default,
    or the build's Okta, Auth0 or Entra ID app — the agents' Gateway client is then made
    in a pool for machines only. The tool plane only when the workflow has tools, and
    `builder=false`: a build is a workflow, not another console, so it gets no Build view,
    deploy project or builds store."""
    s = signin or {"provider": "cognito"}
    provider = s["provider"]
    if provider == "cognito":
        idp = ["-c", "idp=cognito", "-c", "createCognito=true"]
    else:
        idp = ["-c", f"idp={provider}"]
        for field, key in SIGN_IN_CDK[provider].items():
            if s.get(field):
                idp += ["-c", f"{key}={s[field]}"]
    flags = ["-c", f"agentName={agent}", *idp,
             "-c", f"enableGateway={'true' if gateway else 'false'}", "-c", "builder=false"]
    # Whose runs a trigger with runAs "owner" starts (bff/triggers.py): the build's owner.
    if "@" in str((tags or {}).get("agentexpress:owner") or ""):
        flags += ["-c", f"runOwnerEmail={tags['agentexpress:owner']}"]
    if tags:
        flags += ["-c", f"tags={json.dumps(tags, separators=(',', ':'))}"]
    # The console's permissions boundary and role path (guardrails()), on this build's roles.
    flags += [x for k, v in (guard or {}).items() for x in ("-c", f"{k}={v}")]
    # How its buckets are named (bucket_naming(); lib/bucket-names.ts). Unset = "legacy".
    if bucket_naming:
        flags += ["-c", f"bucketNaming={bucket_naming}"]
    return flags


def tf_vars(agent: str, gateway: bool, region: str, tags: dict | None = None,
            signin: dict | None = None, guard: dict | None = None, bucket_naming: str = "") -> dict:
    """terraform/variables.tf values, mirroring cdk_context()."""
    s = signin or {"provider": "cognito"}
    provider = s["provider"]
    who: dict = ({"idp": "cognito", "cognito": {"create": True}} if provider == "cognito" else
                 {"idp": provider, provider: {v: s[k] for k, v in SIGN_IN_TF[provider].items() if s.get(k)}})
    out = {"region": region, "agent_name": agent, **who, "enable_gateway": gateway,
           "container_engine": "docker", "enable_builder": False}
    if tags:
        out["tags"] = tags
    if "@" in str((tags or {}).get("agentexpress:owner") or ""):
        out["run_owner_email"] = tags["agentexpress:owner"]
    guard = guard or {}
    if guard.get("permissionsBoundaryArn"):
        out["iam_permissions_boundary_arn"] = guard["permissionsBoundaryArn"]
    if guard.get("iamRolePath"):
        out["iam_role_path"] = guard["iamRolePath"]
    if guard.get("litellmBaseUrl"):
        out["litellm_base_url"] = guard["litellmBaseUrl"]
        out["litellm_key_secret_arn"] = guard["litellmKeySecretArn"]
    if guard.get("docsUrl"):
        out["docs_url"] = guard["docsUrl"]
    if guard.get("docsLinks") == "false":
        out["docs_links"] = False
    if bucket_naming:
        out["bucket_naming"] = bucket_naming
    return out


def bucket_naming(meta: dict, *, stack_exists: bool = False, state_exists: bool = False) -> tuple[str, str]:
    """How the build's buckets are named (bff/bucket_names.py), and why: the scheme the build
    already has, else "legacy" for anything that may already hold a bucket, else "regional".

    A bucket whose name changes is REPLACED by CloudFormation and Terraform, with what it
    holds, so a build that may have deployed keeps the legacy names: one with a deploy on
    record from before schemes were recorded, a CDK stack that still exists (after
    recover_failed_create cleared a failed first create), or Terraform state under the build.
    Everything else has nothing deployed and gets the Region in its names, so a build moved
    to another Region never meets its old names while S3 still holds them."""
    for scheme, where in ((meta.get("bucketNaming"), "recorded on the build"),
                          ((meta.get("deployed") or {}).get("bucketNaming"), "recorded with its deploy")):
        if scheme in bucket_names.SCHEMES:
            return str(scheme), where
    if meta.get("deployed"):
        return "legacy", "the build was deployed before bucket names were recorded"
    if stack_exists:
        return "legacy", "the build's stack exists"
    if state_exists:
        return "legacy", "the build has Terraform state"
    return "regional", "nothing of the build is deployed"


#: The console's extra tags for every build it deploys (-c buildTags / var.build_tags), a JSON
#: object on the deploy project. The same rules as cdk/lib/build-tags.ts and var.build_tags in
#: terraform/builder.tf: ASCII only, because Amazon Bedrock's tag pattern is, and a tag one
#: resource refuses fails the whole deploy.
BUILD_TAGS_ENV = "BUILD_EXTRA_TAGS"
MAX_BUILD_TAGS = 40
RESERVED_TAG_PREFIXES = ("aws:", "agentexpress:")
TAG_CHARS = re.compile(r"^[A-Za-z0-9 _.:/=+\-@]*$")
_TAG_CHARS_RULE = "may use only letters A-Z a-z, digits, spaces and _ . : / = + - @"


def tag_problem(key: str, value) -> str:
    """Why an extra build tag cannot be used, or "" when it can."""
    if not isinstance(value, str):
        return "the value is not a string"
    if not TAG_CHARS.match(key):
        return f"the key {_TAG_CHARS_RULE}"
    if not 1 <= len(key) <= 128:
        return "the key must be 1 to 128 characters"
    if key.lower().startswith(RESERVED_TAG_PREFIXES):
        return "the key starts with a reserved prefix (aws: or agentexpress:)"
    if not TAG_CHARS.match(value):
        return f"the value {_TAG_CHARS_RULE}"
    if len(value) > 256:
        return "the value must be at most 256 characters"
    return ""


def extra_build_tags(env: dict) -> dict:
    """The console's extra tags for this build, from BUILD_EXTRA_TAGS. CDK and Terraform
    refuse an invalid one at synth and plan; the project's environment can still be edited
    by hand, so here an invalid entry is skipped and printed (its key, never its value),
    and a deploy never fails for a tag."""
    raw = str(env.get(BUILD_TAGS_ENV) or "").strip()
    if not raw:
        return {}
    try:
        given = json.loads(raw)
    except ValueError:
        given = None
    if not isinstance(given, dict):
        print(f"runner: {BUILD_TAGS_ENV} is not a JSON object of tag key -> value; no extra tags added")
        return {}
    out: dict = {}
    seen: dict = {}
    for key in sorted(given):           # JSON object keys are strings
        value = given[key]
        why = tag_problem(key, value)
        if not why and key.lower() in seen:
            why = f"the key differs only in case from {seen[key.lower()]}"
        if not why and len(out) >= MAX_BUILD_TAGS:
            why = f"over the limit of {MAX_BUILD_TAGS} extra tags"
        if why:
            print(f"runner: extra build tag {key[:130]!r} skipped: {why}")
            continue
        seen[key.lower()] = key
        out[key] = value
    if out:
        print(f"runner: adding {len(out)} extra tag(s) from the console to this build: "
              f"{', '.join(sorted(out))}")
    return out


def build_tags(bid: str, version: int, owner: str, email: str, console: str,
               extra: dict | None = None) -> dict:
    """What every resource of a build is tagged with, so a bill or an inventory can say
    which build, version, user and console it came from, plus the console's `extra` tags
    (extra_build_tags). The framework's agentexpress:* keys always win over an extra one."""
    tags = {"agentexpress:build": bid, "agentexpress:version": str(version),
            "agentexpress:owner": email or owner}
    if console:
        tags["agentexpress:console"] = console
    # A tag value allows letters, digits, spaces and _ . : / = + - @ only.
    framework = {k: re.sub(r"[^\w .:/=+\-@]", "_", v)[:256] for k, v in tags.items()}
    return {**(extra or {}), **framework}


# --- where the build goes ---------------------------------------------------------------

class Target:
    """A connected customer account (bff/accounts.py), reached through its deploy role.

    Deploys there use a PROFILE, not exported keys: the SDKs refresh an assumed role
    before it expires, and a deploy can outlive the one-hour cap on a chained session.
    Terraform's state stays in this console's account, under the `console` profile."""

    def __init__(self, account: str, region: str, role_arn: str, external_id: str, bid: str):
        self.account, self.region = account, region
        self.role_arn, self.external_id, self.bid = role_arn, external_id, bid

    def env(self, console_region: str, terraform: bool = False) -> dict:
        """`console` is this CodeBuild container's own role. The AWS SDK for Go —
        Terraform's — accepts `credential_source` only together with a `role_arn`, so on
        the Terraform path it reads the container's credentials through
        `credential_process` instead (container_creds.py), which every SDK supports."""
        cfg = WORK / "aws-config"
        source = (f"credential_process = {sys.executable} {ROOT / 'deployer' / 'container_creds.py'}\n"
                  if terraform else "credential_source = EcsContainer\n")
        cfg.write_text(
            "[profile console]\n"
            f"{source}"
            f"region = {console_region}\n\n"
            "[profile target]\n"
            f"role_arn = {self.role_arn}\n"
            f"external_id = {self.external_id}\n"
            "source_profile = console\n"
            f"role_session_name = agentexpress-{self.bid}\n"
            f"region = {self.region}\n")
        return {"AWS_CONFIG_FILE": str(cfg), "AWS_PROFILE": "target", "AWS_SDK_LOAD_CONFIG": "1",
                "AWS_REGION": self.region, "AWS_DEFAULT_REGION": self.region,
                "CDK_DEFAULT_REGION": self.region, "CDK_DEFAULT_ACCOUNT": self.account}

    def session(self):
        """A short boto3 session in the target account, assumed fresh each time."""
        import boto3
        c = boto3.client("sts").assume_role(
            RoleArn=self.role_arn, ExternalId=self.external_id, DurationSeconds=900,
            RoleSessionName=f"agentexpress-{self.bid}")["Credentials"]
        return boto3.Session(aws_access_key_id=c["AccessKeyId"],
                             aws_secret_access_key=c["SecretAccessKey"],
                             aws_session_token=c["SessionToken"], region_name=self.region)


def agentcore_available(session, region: str) -> None:
    """Fail fast, with the reason, in a region AgentCore does not serve — rather than
    twenty minutes into a deploy, on the first AgentCore resource."""
    try:
        session.client("bedrock-agentcore-control", region_name=region).list_agent_runtimes(
            maxResults=1)
    except Exception as e:
        text = f"{type(e).__name__}: {e}"
        if any(t in text for t in ("EndpointConnectionError", "Could not connect",
                                   "UnknownEndpoint", "not supported in")):
            raise StepFailed(f"Amazon Bedrock AgentCore is not available in {region}. Deploy "
                             f"this build to a region that has it (us-east-1, us-west-2, ...)."
                             ) from e
        print(f"runner: could not check AgentCore in {region} ({text}); deploying anyway")


def resolve_target(table, meta: dict, owner: str, bid: str) -> Target | None:
    """The job's account from the store (set by the BFF when it started the job), and
    the connection from the store — never from the request."""
    account = str((meta.get("job") or {}).get("account") or "")
    if not account:
        return None
    conn = table.get_item(Key=buildstore.account_key(owner, account)).get("Item") or {}
    if conn.get("status") != "connected":
        raise StepFailed(f"account {account} is not connected any more — reconnect it in the "
                         f"console, or remove this build's resources there by hand")
    # The region the deploy asked for (bff/builds.py puts it on the job), else the
    # connection's: one connected account serves every region.
    region = str((meta.get("job") or {}).get("region") or conn["region"])
    return Target(account, region, str(conn["roleArn"]), str(conn["externalId"]), bid)


def local_session():
    import boto3
    return boto3.Session()


def read_secrets(env: dict, bid: str) -> dict:
    """A build's tool API keys and A2A tokens (bff/builds.py set_secrets)."""
    import boto3
    sm = boto3.client("secretsmanager")
    try:
        data = json.loads(sm.get_secret_value(
            SecretId=f"{env.get('SECRETS_PREFIX') or 'agentexpress/builds/'}{bid}")["SecretString"])
    except sm.exceptions.ResourceNotFoundException:
        return {}
    keys, tokens = data.get("toolApiKeys") or {}, data.get("a2aTokens") or {}
    idents = data.get("identitySecrets") or {}
    # orchestrator.export's headers, by name (terraform/export.tf, cdk/lib/export.ts).
    exports = data.get("exportHeaders") or {}
    # A deliver step's webhook signing secrets, by name (v139 Track C deliver;
    # terraform/deliver.tf, cdk/lib/deliver.ts).
    delivers = data.get("deliverSecrets") or {}
    out = {}
    if keys:
        out["TOOL_API_KEYS"] = out["TF_VAR_tool_api_keys"] = json.dumps(keys)
    if tokens:
        out["A2A_TOKENS"] = out["TF_VAR_a2a_tokens"] = json.dumps(tokens)
    if idents:
        out["IDENTITY_SECRETS"] = out["TF_VAR_identity_secrets"] = json.dumps(idents)
    if exports:
        out["EXPORT_HEADERS"] = out["TF_VAR_export_headers"] = json.dumps(exports)
    if delivers:
        out["DELIVER_SECRETS"] = out["TF_VAR_deliver_secrets"] = json.dumps(delivers)
    print(f"runner: using {len(keys)} tool API key(s), {len(tokens)} A2A token(s), "
          f"{len(idents)} identity secret(s), {len(exports)} export header set(s) and "
          f"{len(delivers)} delivery secret(s)")
    return out


#: A code tool runs on Lambda's Python 3.12, x86_64: its requirements are installed for
#: that, as wheels only, so nothing compiles on this machine for the wrong platform.
CODE_PIP = ["--platform", "manylinux2014_x86_64", "--implementation", "cp",
            "--python-version", "3.12", "--only-binary=:all:", "--no-compile", "--quiet",
            # Ignore any pip config or PIP_* environment in this container: the index is
            # PyPI and nothing else.
            "--isolated"]


def check_requirements(text: str, key: str) -> None:
    """Refuse a requirements.txt line pip would read as an option, a URL or a path.

    bff/codecheck.py rejects these before a deploy is accepted; this is the same rule
    where the install actually happens, because a line such as `--index-url
    https://attacker/simple` would make this container fetch packages from anywhere."""
    for n, line in enumerate(text.splitlines(), 1):
        req = line.split("#", 1)[0].strip()
        if req and (req.startswith(("-", ".", "/")) or "://" in req or "@" in req):
            raise StepFailed(f"tools.{key} requirements.txt line {n}: only `name==version` "
                             f"lines are installed (no options, URLs, paths or other indexes)")


#: The image a hosted MCP server is built into when its code brings no Dockerfile: the
#: AgentCore MCP contract is a container on ARM64 serving streamable HTTP on 0.0.0.0:8000 at
#: /mcp (https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/runtime-mcp-protocol-contract.html).
#: The bind is inside the container, which publishes no port: the Runtime is reached only
#: through AgentCore with SigV4 (the build's Gateway role). Same base image as ../Dockerfile.
HOSTED_MCP_DOCKERFILE = """\
# Written by the deployer for a hosted MCP server whose code has no Dockerfile.
FROM --platform=linux/arm64 public.ecr.aws/docker/library/python:3.13-slim-bookworm@sha256:5024f48ba9441d4b13a95d3945abc6365538e3a31109833367a1923523c6efed
WORKDIR /app
COPY . /app
RUN if [ -f requirements.txt ]; then pip install --no-cache-dir -r requirements.txt; fi \\
    && useradd --create-home --uid 1000 mcp
USER mcp
EXPOSE 8000
CMD ["python", "server.py"]
"""  # noqa: E501 - the FROM line pins the base image by digest, one Dockerfile line


def hosted_mcp_tools(workflow: dict) -> dict:
    """tools.<key>.hosted for each hosted MCP server (type "mcp" with `hosted`)."""
    return {k: t["hosted"] for k, t in (workflow.get("tools") or {}).items()
            if isinstance(t, dict) and str(t.get("type") or "").lower() == "mcp"
            and isinstance(t.get("hosted"), dict)}


def prepare_hosted_mcp(workflow: dict, bid: str, env: dict, target: Target | None = None,
                       action: str = "deploy", tokens: dict | None = None, s3_for=None) -> list[str]:
    """Fill app/tools/_code/<key>/ for each hosted MCP server CDK and Terraform build an image
    from, after `scaffold.py apply`: git or S3 code is fetched there (deployer/code_fetch.py);
    code written in the build is already there. A server with no Dockerfile gets
    HOSTED_MCP_DOCKERFILE, which runs its server.py. An image the build brings (code.image)
    needs nothing. A destroy fetches nothing: a stand-in folder is enough for both tools."""
    done = []
    servers = hosted_mcp_tools(workflow)
    # The build's git tokens are read once, before any server, when a hosted git source
    # names one (as materialise_code_sources does), so every server sees the same tokens
    # whatever the key order.
    if tokens is None:
        tokens = read_git_tokens(env, bid) if action == "deploy" and any(
            isinstance(h.get("code"), dict) and isinstance(h["code"].get("git"), dict)
            and h["code"]["git"].get("token") for h in servers.values()) else {}
    for key, hosted in servers.items():
        src = hosted.get("code")
        kind = next((k for k in ("git", "s3", "image") if isinstance(src, dict) and k in src), "")
        if kind == "image":
            continue
        folder = ROOT / "app" / "tools" / "_code" / key
        if action != "deploy":
            folder.mkdir(parents=True, exist_ok=True)
        elif kind:
            import code_fetch  # deployer/code_fetch.py
            try:
                # S3 is read through code_source_s3: a connected account's deploy role scoped
                # to this path, or, in this console's own account, only a bucket listed in
                # CODE_SOURCE_BUCKETS and never one of the console's (console_bucket_refusal).
                s3 = (s3_for(src, target) if s3_for else code_source_s3(src, target, env)) if kind == "s3" else None
                code_fetch.materialise(src, folder, tokens=tokens, s3=s3)
            except code_fetch.CodeSourceError as e:
                raise StepFailed(f"tools.{key}.hosted.code: {e}") from e
        if not (folder / "Dockerfile").is_file():
            if action == "deploy" and not (folder / "server.py").is_file():
                raise StepFailed(f"tools.{key}.hosted: its code has neither a Dockerfile nor a "
                                 f"server.py at the root")
            if action == "deploy" and (folder / "requirements.txt").is_file():
                check_requirements((folder / "requirements.txt").read_text(), key)
            folder.mkdir(parents=True, exist_ok=True)
            (folder / "Dockerfile").write_text(HOSTED_MCP_DOCKERFILE)
        print(f"runner: tools.{key}: hosted MCP server "
              + ("stand-in for destroy" if action != "deploy" else f"code ready ({kind or 'written in the build'})"))
        done.append(key)
    return done


def install_code_requirements(workflow: dict) -> list[str]:
    """pip install each code function's requirements.txt into its folder, which CDK and
    Terraform then zip as they are (app/tools/_code/<key>/; interceptors included)."""
    done = []
    for key in buildstore.code_functions(workflow):
        folder = ROOT / "app" / "tools" / "_code" / key
        req = folder / "requirements.txt"
        if not req.exists() or not [ln for ln in req.read_text().splitlines()
                                    if ln.strip() and not ln.strip().startswith("#")]:
            continue
        check_requirements(req.read_text(), key)
        run([sys.executable, "-m", "pip", "install", "-r", str(req), "-t", str(folder), *CODE_PIP],
            ROOT)
        done.append(key)
    if done:
        print(f"runner: installed the requirements of {', '.join(done)}")
    return done


# --- begin Track B docgen
def install_builtin_requirements(workflow: dict) -> list[str]:
    """pip install the pinned requirements.txt of each built-in function a tool names
    (type "lambda" with `source`; today only "docgen" has one) into its folder,
    app/tools/<source>/, which CDK and Terraform then zip as they are. Once per source,
    with the same options as a code tool's (CODE_PIP), so its wheels are Lambda's."""
    done = []
    for t in (workflow.get("tools") or {}).values():
        source = t.get("source") if isinstance(t, dict) and str(t.get("type") or "").lower() == "lambda" else None
        if not isinstance(source, str) or not re.fullmatch(r"[A-Za-z][A-Za-z0-9]*", source) or source in done:
            continue
        folder = ROOT / "app" / "tools" / source
        req = folder / "requirements.txt"
        if not req.is_file():
            continue
        check_requirements(req.read_text(), source)
        run([sys.executable, "-m", "pip", "install", "-r", str(req), "-t", str(folder), *CODE_PIP], ROOT)
        done.append(source)
    if done:
        print(f"runner: installed the libraries of the built-in {', '.join(done)} function(s)")
    return done
# --- end Track B docgen


def read_git_tokens(env: dict, bid: str) -> dict:
    """The build's gitTokens (bff/builds.py set_secrets), for code sources only: never put
    in the environment of CDK or Terraform."""
    import boto3
    sm = boto3.client("secretsmanager")
    try:
        data = json.loads(sm.get_secret_value(
            SecretId=f"{env.get('SECRETS_PREFIX') or 'agentexpress/builds/'}{bid}")["SecretString"])
    except sm.exceptions.ResourceNotFoundException:
        return {}
    return dict(data.get("gitTokens") or {})


def console_bucket_refusal(bucket: str, env: dict, s3, account: str) -> str | None:
    """Why an S3 code source may not be read from `bucket` in this console's own account,
    else None. Without a connected account the read uses this container's role, which
    reads the whole account, so a build may not name a bucket that holds other builds' data:
      * BUILDS_BUCKET (every build's versions, documents and Terraform state) and the
        bucket of this job's SOURCE_URI;
      * the CDK bootstrap staging bucket, `cdk-<qualifier>-assets-<account-ID>-<Region>`
        (https://docs.aws.amazon.com/cdk/v2/guide/bootstrapping-env.html), which holds
        every build's packaged code;
      * any bucket of this account tagged `agentexpress:*` (cdk/bin/orchestrator.ts and
        terraform/versions.tf tag every resource of the console and of each build).
    GetBucketTagging with ExpectedBucketOwner fails with 403 when the bucket belongs to
    another account, and NoSuchTagSet means no tags
    (https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketTagging.html)."""
    import re
    source_bucket = str(env.get("SOURCE_URI") or "")
    source_bucket = source_bucket[len("s3://"):].split("/", 1)[0] if source_bucket.startswith("s3://") else ""
    if bucket in {str(env.get("BUILDS_BUCKET") or ""), source_bucket} - {""}:
        return "it is a bucket of this console"
    if re.fullmatch(rf"cdk-[a-z0-9]+-assets-{re.escape(account)}-[a-z0-9-]+", bucket):
        return "it is the CDK staging bucket of this console's account"
    try:
        tags = s3.get_bucket_tagging(Bucket=bucket, ExpectedBucketOwner=account).get("TagSet") or []
    except Exception as e:  # noqa: BLE001 - botocore's ClientError, read by code
        code = str(getattr(e, "response", {}).get("Error", {}).get("Code", ""))
        status = getattr(e, "response", {}).get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code == "NoSuchTagSet":
            return None
        if code in ("AccessDenied", "403") or status == 403:
            return None  # not this account's bucket (ExpectedBucketOwner)
        if code == "NoSuchBucket":
            return None  # the read then fails with its own message
        return f"its tags cannot be checked ({code or type(e).__name__})"
    if any(str(t.get("Key", "")).startswith("agentexpress:") for t in tags):
        return "it is a bucket of this console or of a build deployed by it"
    return None


def code_source_s3(source: dict, target: Target | None, env: dict | None = None, script: bool = False):
    """An S3 client that may read only this code source's path: in a connected account its
    deploy role assumed again with a session policy (code_fetch.scoped_s3_policy); in this
    console's own account, this container's role, after console_bucket_refusal."""
    import boto3
    import code_fetch
    s3 = source["s3"]
    if not target:
        client = boto3.client("s3")
        account = str(boto3.client("sts").get_caller_identity()["Account"])
        bucket = s3["uri"][len("s3://"):].split("/", 1)[0]
        import code_source
        # Only the buckets the operator listed (CODE_SOURCE_BUCKETS; bff/builds.py deploy
        # says the same before a job starts), and never one of this console's own.
        allowed = code_source.allowed_buckets(env or {})
        reason = (f"it is not in this console's codeSourceBuckets "
                  f"({', '.join(sorted(allowed)) if allowed else 'empty'})" if bucket not in allowed
                  else console_bucket_refusal(bucket, env or {}, client, account))
        if reason:
            raise code_fetch.CodeSourceError(
                f"bucket {bucket} cannot be read without a connected account: {reason}")
        return client
    c = boto3.client("sts").assume_role(
        RoleArn=target.role_arn, ExternalId=target.external_id, DurationSeconds=900,
        RoleSessionName=f"agentexpress-{target.bid}-code",
        Policy=json.dumps(code_fetch.scoped_s3_policy(s3["uri"], s3.get("path") or "", script=script)))["Credentials"]
    return boto3.client("s3", region_name=target.region, aws_access_key_id=c["AccessKeyId"],
                        aws_secret_access_key=c["SecretAccessKey"], aws_session_token=c["SessionToken"])


def materialise_code_sources(workflow: dict, bid: str, env: dict, target: Target | None = None,
                             action: str = "deploy", tokens: dict | None = None, s3_for=None) -> list[str]:
    """Fetch every git or S3 code source (bff/code_source.py located) after `scaffold.py
    apply`: a Lambda tool's or interceptor's into app/tools/_code/<key>/, which CDK and
    Terraform then package as written code; a Run code step's script into app/workflow.json
    as its task.code (codeSource removed there). A destroy fetches nothing: it gets stand-ins,
    because both tools only need the folders and keys to exist to remove what they made."""
    import code_fetch
    import code_source
    found = [(w, k, s) for w, k, s in code_source.located(workflow) if code_source.kind(s) in ("git", "s3")]
    if not found:
        return []
    if tokens is None:
        tokens = read_git_tokens(env, bid) if code_source.secret_names(workflow) and action == "deploy" else {}
    s3_for = s3_for or (lambda src, script=False: code_source_s3(src, target, env, script))
    wf_path = ROOT / "app" / "workflow.json"
    deployed = json.loads(wf_path.read_text())
    done = []
    for where, key, src in found:
        label = f"agents.{key}.task" if where == "task" else (
            f"orchestrator.interceptors.{key.split('-', 1)[1]}" if where == "interceptor" else f"tools.{key}")
        try:
            s3 = (s3_for(src, script=where == "task")
                  if code_source.kind(src) == "s3" and action == "deploy" else None)
            if where == "task":
                task = deployed["agents"][key]["task"]
                task.pop("codeSource", None)
                task["code"] = ("# Stand-in for a destroy: the script is not fetched.\n" if action != "deploy"
                                else code_fetch.read_script(src, tokens=tokens, s3=s3))
            elif action != "deploy":
                folder = ROOT / "app" / "tools" / "_code" / key
                folder.mkdir(parents=True, exist_ok=True)
                (folder / "handler.py").write_text(
                    "# Stand-in for a destroy: the code is not fetched.\n"
                    "def lambda_handler(event, context):\n    return {}\n")
            else:
                code_fetch.materialise(src, ROOT / "app" / "tools" / "_code" / key, tokens=tokens, s3=s3)
                if not (ROOT / "app" / "tools" / "_code" / key / "handler.py").is_file():
                    raise code_fetch.CodeSourceError("has no handler.py at the root of its path")
        except code_fetch.CodeSourceError as e:
            raise StepFailed(f"{label}.codeSource ({code_source.describe(src)}): {e}") from e
        print(f"runner: {label}: {'stand-in for destroy' if action != 'deploy' else 'fetched'} "
              f"{code_source.describe(src)}")
        done.append(key)
    wf_path.write_text(json.dumps(deployed, indent=2, ensure_ascii=False) + "\n")
    return done


def sync_documents(s3, bucket: str, bid: str) -> int:
    """Copy the documents uploaded for this build into kb_docs/<corpus>/.

    A corpus the user uploaded to holds THEIR documents and nothing else: the source
    checkout ships sample documents under kb_docs/, and without clearing the folder
    first they were indexed into the build's knowledge base alongside the user's. A
    corpus with no uploads keeps what the source has, so a build made from the sample
    still finds its documents."""
    prefix, n = buildstore.kb_prefix(bid), 0
    cleared: set[str] = set()
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get("Contents", []):
            corpus, _, name = o["Key"][len(prefix):].partition("/")
            if not name or "/" in name or not buildstore.CORPUS_RE.match(corpus):
                continue
            if corpus not in cleared:
                shutil.rmtree(ROOT / "kb_docs" / corpus, ignore_errors=True)
                cleared.add(corpus)
            dest = ROOT / "kb_docs" / corpus / name
            dest.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(bucket, o["Key"], str(dest))
            n += 1
    if n:
        print(f"runner: copied {n} uploaded document(s) into kb_docs/")
    return n


def stand_in_corpora(workflow: dict) -> list[str]:
    """For a DESTROY: make an empty kb_docs/<corpus>/ for each declared corpus with no
    documents. The synth checks that every corpus has a folder, which is right for a
    deploy (no documents means a knowledge base that finds nothing) and wrong for a
    destroy: removing a build whose documents were never uploaded, or since deleted,
    failed on that check and left the build impossible to delete (observed live)."""
    made = []
    for t in (workflow.get("tools") or {}).values():
        if not isinstance(t, dict) or t.get("type") != "kb" or t.get("s3Uri") or t.get("knowledgeBaseId"):
            continue
        for corpus in t.get("corpora") or []:
            d = ROOT / "kb_docs" / str(corpus)
            if buildstore.CORPUS_RE.match(str(corpus)) and not d.is_dir():
                d.mkdir(parents=True, exist_ok=True)
                (d / "README.md").write_text("Stand-in for a destroy: this corpus had no documents.\n")
                made.append(str(corpus))
    if made:
        print(f"runner: destroying a build with no documents in {', '.join(made)}")
    return made


def temporary_password() -> str:
    """A first-sign-in password that meets both IaC paths' pool policy (12+ characters,
    upper case, lower case, a digit, a symbol). Cognito makes the owner replace it on
    first use."""
    import secrets
    import string
    symbols = "!#%*+-=?@^_"
    alphabet = string.ascii_letters + string.digits + symbols
    while True:
        pw = "".join(secrets.choice(alphabet) for _ in range(16))
        if any(c.isupper() for c in pw) and any(c.islower() for c in pw) \
                and any(c.isdigit() for c in pw) and any(c in symbols for c in pw):
            return pw


def login_secret_id(env: dict, bid: str) -> str:
    """Where the owner's first-sign-in password for the build's console is kept: next to
    the build's tool secrets, under the prefix only this console's roles can read."""
    return f"{env.get('SECRETS_PREFIX') or 'agentexpress/builds/'}{bid}-login"


def store_login(env: dict, bid: str, login: dict) -> None:
    import boto3
    sm = boto3.client("secretsmanager")
    blob = json.dumps(login)
    try:
        sm.put_secret_value(SecretId=login_secret_id(env, bid), SecretString=blob)
    except sm.exceptions.ResourceNotFoundException:
        sm.create_secret(Name=login_secret_id(env, bid), SecretString=blob,
                         Description=f"AgentExpress build {bid}: the owner's temporary "
                                     f"password for the build's own console")


def invite_owner(session, pool_id: str, email: str, password: str = "") -> str:  # nosec B107 - "" = Cognito mints one
    """Give the build's owner a login to the build's own console.

    With `password`, that is the temporary password (the console shows it to the owner,
    and Cognito emails it too); the first sign-in sets a real one. The owner joins every
    group the build's pool has, because it is their application. Best effort: a deploy
    that worked is not failed for this.

    Returns "created" (a new login, with `password`), "exists" (the owner had one — a
    redeploy keeps it) or "" (no login: no pool, no email, or the invite failed)."""
    if not (pool_id and email):
        return ""
    try:
        idp = session.client("cognito-idp")
        state = "created"
        try:
            idp.admin_create_user(
                UserPoolId=pool_id, Username=email, DesiredDeliveryMediums=["EMAIL"],
                UserAttributes=[{"Name": "email", "Value": email},
                                {"Name": "email_verified", "Value": "true"}],
                **({"TemporaryPassword": password} if password else {}))
            print("runner: invited the owner into the build's console")
        except idp.exceptions.UsernameExistsException:
            state = "exists"
        for g in idp.list_groups(UserPoolId=pool_id).get("Groups", []):
            idp.admin_add_user_to_group(UserPoolId=pool_id, Username=email, GroupName=g["GroupName"])
        return state
    except Exception as e:  # noqa: BLE001 - a failed invite is logged; the deploy goes on
        print(f"runner: could not invite the owner: {type(e).__name__}: {e}")
        return ""


#: SHA-256 of each pinned Terraform release, from HashiCorp's signed
#: terraform_<version>_SHA256SUMS. Held HERE rather than fetched next to the download: a
#: checksum read from the same host as the binary proves only that the two agree, not
#: that either is genuine. Another version deploys only with TERRAFORM_SHA256 set to its
#: checksum.
TERRAFORM_SHA256 = {
    "terraform_1.15.8_linux_arm64.zip": "8891e9dcedc9e3b8950bc6af9d4d8af1f4cfade3062f53b9dc403a89f6ce8c9c",
    "terraform_1.15.8_linux_amd64.zip": "d25ce7b6902013ad905db3d2eab0be4cd905887fe88b81a6171b8d5503c31f3d",
}


def ensure_terraform(version: str) -> str:
    """The terraform binary: the one on PATH, else the pinned release, checksum-verified."""
    found = shutil.which("terraform")
    if found:
        return found
    arch = {"aarch64": "arm64", "arm64": "arm64", "x86_64": "amd64"}[platform.machine()]
    name = f"terraform_{version}_linux_{arch}.zip"
    expected = os.environ.get("TERRAFORM_SHA256") or TERRAFORM_SHA256.get(name)
    if not expected:
        raise StepFailed(f"no pinned checksum for {name}: add it to TERRAFORM_SHA256 in "
                         f"deployer/runner.py, or set TERRAFORM_SHA256")
    base = f"https://releases.hashicorp.com/terraform/{version}"
    with urllib.request.urlopen(f"{base}/{name}", timeout=120) as r:  # noqa: S310 - fixed https host  # nosec B310
        blob = r.read()
    if hashlib.sha256(blob).hexdigest() != expected:
        raise StepFailed(f"terraform {version} download failed its SHA256 check")
    dest = WORK / "terraform-bin"
    dest.mkdir(parents=True, exist_ok=True)
    zipfile.ZipFile(io.BytesIO(blob)).extract("terraform", dest)
    (dest / "terraform").chmod(0o755)
    return str(dest / "terraform")


def cdk_outputs(path: Path, stack: str) -> dict:
    out = json.loads(path.read_text()).get(stack, {})
    try:  # tool -> the callback URL to register at its provider (auth "user"); shown on the tool
        callbacks = json.loads(out.get("toolCallbackUrls") or "{}")
    except ValueError:
        callbacks = {}
    return {"runtimeArn": out.get("agentRuntimeArn", ""), "uiUrl": out.get("uiUrl", ""),
            "apiUrl": out.get("apiEndpoint", ""), "userPoolId": out.get("cognitoUserPoolId", ""),
            # The Gateway's MCP endpoint: what publishing the build to a registry names.
            **({"gatewayUrl": out["gatewayUrl"]} if out.get("gatewayUrl") else {}),
            **({"callbackUrls": callbacks} if callbacks else {})}


def tf_outputs(raw: str) -> dict:
    out = {k: (v or {}).get("value") for k, v in json.loads(raw or "{}").items()}
    return {"runtimeArn": out.get("agent_runtime_arn") or "", "uiUrl": out.get("ui_url") or "",
            "apiUrl": out.get("api_endpoint") or "",
            "userPoolId": out.get("cognito_user_pool_id") or "",
            **({"gatewayUrl": out["gateway_url"]} if out.get("gateway_url") else {}),
            **({"callbackUrls": out["tool_callback_urls"]} if out.get("tool_callback_urls") else {})}


#: A stack whose FIRST create failed: nothing in it ever worked, and CloudFormation can
#: only delete it. ROLLBACK_COMPLETE and ROLLBACK_FAILED follow only a failed or cancelled
#: create; DELETE_FAILED can follow anything, so the stack's history decides
#: (never_deployed). https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/view-stack-events.html
FAILED_CREATE = ("ROLLBACK_COMPLETE", "ROLLBACK_FAILED", "DELETE_FAILED")
#: A stack-level status that means the stack deployed, or was changed after its create.
DEPLOYED_STATUS = re.compile(r"^(CREATE_COMPLETE|UPDATE_|IMPORT_)")
#: The first status of a stack: CREATE_IN_PROGRESS, or REVIEW_IN_PROGRESS when it was created
#: from a change set, which is how `cdk deploy` creates one.
FIRST_STATUS = ("REVIEW_IN_PROGRESS", "CREATE_IN_PROGRESS")
#: Resources CloudFormation has already let go of.
GONE = ("DELETE_COMPLETE", "DELETE_SKIPPED")


def cdk_cloudformation(session, region: str):
    """CloudFormation as the account's CDK deploy role, and the account. The role CloudFormation
    already trusts with the build's stack: it can delete it, and it carries
    AWSCloudFormationReadOnlyAccess (the CDK bootstrap template), which the connected
    account's deploy role does not."""
    import boto3
    account = session.client("sts").get_caller_identity()["Account"]
    c = session.client("sts").assume_role(
        RoleArn=f"arn:aws:iam::{account}:role/cdk-hnb659fds-deploy-role-{account}-{region}",
        RoleSessionName="agentexpress-recover")["Credentials"]
    return boto3.client("cloudformation", region_name=region, aws_access_key_id=c["AccessKeyId"],
                        aws_secret_access_key=c["SecretAccessKey"],
                        aws_session_token=c["SessionToken"]), account


def never_deployed(cfn, stack: str, stack_id: str, *, build_account: str, region: str,
                   meta: dict) -> str:
    """"" when the stack provably never deployed, else why that is not proven. Fails closed:
    a history that cannot be read, or that does not reach back to the stack's create, is not
    proof. `build_account` is the build's target account, "" for this console's own.
    The events are read by StackId, every page
    (https://docs.aws.amazon.com/AWSCloudFormation/latest/APIReference/API_DescribeStackEvents.html),
    newest first."""
    deployed = meta.get("deployed") or {}
    if deployed and str(deployed.get("account") or "") == build_account \
            and str(deployed.get("region") or "") == region:
        return "the build has a deploy on record in this account and Region"
    try:
        events = [e for page in cfn.get_paginator("describe_stack_events").paginate(StackName=stack_id)
                  for e in page.get("StackEvents") or []]
    except Exception as e:  # noqa: BLE001 - unreadable is not proof
        return f"its history could not be read ({type(e).__name__}: {e})"
    statuses = [str(e.get("ResourceStatus") or "") for e in events
                if e.get("LogicalResourceId") == stack and e.get("ResourceType") == "AWS::CloudFormation::Stack"]
    done = [s for s in statuses if DEPLOYED_STATUS.match(s)]
    if done:
        return f"its history shows {done[0]}"
    if not statuses or statuses[-1] not in FIRST_STATUS or "CREATE_IN_PROGRESS" not in statuses:
        return "its history does not reach back to its create"
    if "ROLLBACK_IN_PROGRESS" not in statuses:
        return "its history shows no rollback of its create"
    return ""


def empty_bucket(s3, name: str, account: str) -> int | None:
    """Delete every version and delete marker in `name`, only if `account` owns it
    (ExpectedBucketOwner: https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucket-owner-condition.html).
    How many were deleted; None when the bucket does not exist."""
    from botocore.exceptions import ClientError
    n = 0
    try:
        for page in s3.get_paginator("list_object_versions").paginate(Bucket=name, ExpectedBucketOwner=account):
            objects = [{"Key": v["Key"], "VersionId": v["VersionId"]}
                       for v in (page.get("Versions") or []) + (page.get("DeleteMarkers") or [])]
            for i in range(0, len(objects), 1000):      # DeleteObjects takes 1000 keys a call
                out = s3.delete_objects(Bucket=name, ExpectedBucketOwner=account,
                                        Delete={"Objects": objects[i:i + 1000], "Quiet": True})
                if out.get("Errors"):
                    e = out["Errors"][0]
                    raise StepFailed(f"could not empty {name}: {e.get('Code')} on {e.get('Key')}: "
                                     f"{e.get('Message')}")
            n += len(objects)
    except ClientError as e:
        if (getattr(e, "response", {}).get("Error") or {}).get("Code") == "NoSuchBucket":
            return None
        raise
    return n


def recover_failed_create(session, stack: str, region: str, *, agent: str, build_account: str = "",
                          meta: dict | None = None, workflow: dict | None = None, attempts: int = 4,
                          pause: float = 60.0) -> bool:
    """Before a CDK deploy: clear the build's stack when its first create failed, so the
    deploy can create it again (`cdk deploy` refuses a stack in ROLLBACK_FAILED). Whether
    the stack exists afterwards.

    Seen live: a failed first create rolled back, but could not delete the UI bucket the
    deploy had already filled (BucketNotEmpty), and ended ROLLBACK_FAILED; every later
    deploy failed. So, only when never_deployed() proves the stack never deployed (no user
    data can be in it): empty this stack's own framework buckets (an AWS::S3::Bucket of the
    stack, named as this agent's buckets are in either scheme, or the UI logs bucket
    CloudFormation named after the stack), delete the stack by its StackId, wait, and again
    if it ends DELETE_FAILED. Any other bucket is left alone, and so is storage kept with
    orchestrator.storage.retain. A stack that may have deployed is refused with why."""
    from botocore.exceptions import ClientError
    meta, workflow = meta or {}, workflow or {}
    cfn, account = cdk_cloudformation(session, region)
    try:
        found = cfn.describe_stacks(StackName=stack)["Stacks"][0]
    except ClientError as e:
        if "does not exist" in str(e):
            return False
        raise
    status, stack_id = found["StackStatus"], found["StackId"]
    if status not in FAILED_CREATE:
        return True
    print(f"runner: {stack} is {status}: checking that it never deployed before clearing it", flush=True)
    why = never_deployed(cfn, stack, stack_id, build_account=build_account, region=region, meta=meta)
    if why:
        raise StepFailed(f"{stack} is {status} and is not cleared automatically: {why}, so its "
                         f"buckets may hold data. Destroy the build from the console, or empty and "
                         f"delete the stack by hand, then deploy again.")
    print(f"runner: {stack} never deployed (its first create failed): emptying its own buckets, "
          f"then deleting it", flush=True)
    s3 = session.client("s3", region_name=region)
    names = bucket_names.all_names(agent, account, region)
    kept = ({bucket_names.name("storage", agent, account, region, s) for s in bucket_names.SCHEMES}
            if storage_retained(workflow) else set())
    logs_bucket = re.compile(rf"{re.escape(stack.lower())}-uilogsbucket[a-z0-9-]*")
    for attempt in range(1, attempts + 1):
        resources = stack_resources(session, stack_id, region, cfn=cfn)
        if resources is None:
            raise StepFailed(f"could not read {stack}'s resources; delete the stack by hand and "
                             f"deploy again")
        for r in resources:
            if r.get("ResourceType") != "AWS::S3::Bucket" or r.get("ResourceStatus") in GONE:
                continue
            logical, name = str(r.get("LogicalResourceId") or ""), str(r.get("PhysicalResourceId") or "")
            if not name:
                continue
            if name in kept:
                print(f"runner: not emptying {logical} ({name}): orchestrator.storage.retain keeps it",
                      flush=True)
                continue
            if name not in names and not logs_bucket.fullmatch(name):
                print(f"runner: not emptying {logical} ({name}): not one of this build's buckets",
                      flush=True)
                continue
            n = empty_bucket(s3, name, account)
            print(f"runner: emptied {name} ({n} object version(s))" if n is not None
                  else f"runner: {name} does not exist; nothing to empty", flush=True)
        print(f"runner: deleting {stack} (attempt {attempt} of {attempts})", flush=True)
        cfn.delete_stack(StackName=stack_id)
        try:
            cfn.get_waiter("stack_delete_complete").wait(
                StackName=stack_id, WaiterConfig={"Delay": 15, "MaxAttempts": 80})
            print(f"runner: {stack} deleted; deploying it again", flush=True)
            return False
        except Exception as e:  # noqa: BLE001 - DELETE_FAILED: a resource was still settling
            print(f"runner: {stack} did not delete ({type(e).__name__}: {e}); "
                  f"trying again in {pause:.0f}s", flush=True)
            time.sleep(pause)
    raise StepFailed(f"{stack} could not be deleted after {attempts} attempts; delete it by hand "
                     "and deploy again")


#: The one bucket a CDK build's destroy can find refilled: CloudFront's access logs
#: (UiLogsBucket in lib/orchestrator-stack.ts). Its physical name contains "uilogsbucket".
LATE_LOG_BUCKET = "UiLogsBucket"
#: Destroys run again after the first fails, shared by both retryable failures: a logs
#: bucket refilled late (CloudFront can deliver again while a destroy is running, so each
#: attempt empties the bucket afresh), an AgentCore Runtime whose delete timed out, and
#: one CloudFormation failed with "Access denied" after AgentCore had already deleted it.
DESTROY_RETRIES = 3
#: The type of every AgentCore Runtime a CDK build's stack holds: the orchestrator and each
#: dedicated agent (lib/orchestrator-stack.ts).
RUNTIME_TYPE = "AWS::BedrockAgentCore::Runtime"
#: Seconds to wait before destroying again after a Runtime's delete timed out: the delete
#: goes on in AgentCore after CloudFormation's handler gives up, so a later attempt finds it
#: further along or done.
RUNTIME_DELETE_PAUSE = 60.0


def stack_resources(session, stack: str, region: str, cfn=None) -> list[dict] | None:
    """Every resource of the stack, read with ListStackResources, paged: DescribeStackResources
    returns only the first 100, and a build's stack has more (seen live: 100 read, the failed
    resource not among them). None when it cannot be read. `cfn`: a CloudFormation client to
    read with instead of the session's (recover_failed_create's CDK deploy role)."""
    cfn = cfn or session.client("cloudformation", region_name=region)
    try:
        return [r for page in cfn.get_paginator("list_stack_resources").paginate(StackName=stack)
                for r in page.get("StackResourceSummaries") or []]
    except Exception as e:  # noqa: BLE001 - nothing to look at: report the destroy's own failure
        print(f"runner: could not read {stack}'s resources: {type(e).__name__}: {e}", flush=True)
        return None


def timed_out_runtimes(stack: str, resources: list[dict]) -> list[str]:
    """After a failed `cdk destroy`: the stack's AgentCore Runtimes whose delete failed on a
    timeout, by logical id, each printed.

    Seen live: on a large build's stack, CloudFormation's handler gave up on deleting a
    Runtime after about 5m20s ("Request timed out while deleting
    AWS::BedrockAgentCore::Runtime", HandlerErrorCode NotStabilized), twice in a row on
    two different Runtimes, while AgentCore went on deleting it; the third destroy
    succeeded. Any other Runtime failure is not one a later attempt can get past."""
    out = []
    for r in resources:
        reason = str(r.get("ResourceStatusReason") or "")
        if r.get("ResourceStatus") != "DELETE_FAILED" or r.get("ResourceType") != RUNTIME_TYPE \
                or not ("notstabilized" in reason.lower() or "timed out" in reason.lower()):
            continue
        logical = str(r.get("LogicalResourceId") or "")
        print(f"runner: {stack}'s AgentCore Runtime {logical} "
              f"({r.get('PhysicalResourceId') or 'no physical id'}) timed out while deleting "
              f"({reason}); its delete goes on in AgentCore", flush=True)
        out.append(logical)
    return out


def deleted_runtimes(session, stack: str, region: str, resources: list[dict]) -> list[str]:
    """After a failed `cdk destroy`: the stack's AgentCore Runtimes that CloudFormation
    reports DELETE_FAILED with "Access denied for operation" while AgentCore says they no
    longer exist, by logical id, each printed.

    Seen live (runtime-vpc cdk test): after a NotStabilized retry, the handler failed a
    Runtime it had already deleted with "Access denied for operation
    'AWS::BedrockAgentCore::Runtime'" (HandlerErrorCode AccessDenied) while GetAgentRuntime
    returned ResourceNotFound for it; the next destroy succeeded. A Runtime counts only when
    GetAgentRuntime raises ResourceNotFoundException (404, "a resource referenced by the
    operation does not exist":
    https://docs.aws.amazon.com/bedrock-agentcore-control/latest/APIReference/API_GetAgentRuntime.html).
    Any other answer (it exists, or the read is denied or fails) leaves it to be reported."""
    out = []
    control = None
    for r in resources:
        reason = str(r.get("ResourceStatusReason") or "")
        if r.get("ResourceStatus") != "DELETE_FAILED" or r.get("ResourceType") != RUNTIME_TYPE \
                or "access denied" not in reason.lower():
            continue
        logical = str(r.get("LogicalResourceId") or "")
        physical = str(r.get("PhysicalResourceId") or "")
        # The id is the ARN's last segment (agentRuntimeArn pattern ...:runtime/<id>, same page).
        runtime_id = physical.rsplit("runtime/", 1)[-1]
        if not runtime_id:
            continue
        try:
            control = control or session.client("bedrock-agentcore-control", region_name=region)
            control.get_agent_runtime(agentRuntimeId=runtime_id)
            gone = False
        except Exception as e:  # noqa: BLE001 - only ResourceNotFound means gone
            code = str(getattr(e, "response", {}).get("Error", {}).get("Code", ""))
            gone = code == "ResourceNotFoundException"
            if not gone:
                print(f"runner: could not confirm {stack}'s AgentCore Runtime {logical} is gone: "
                      f"{type(e).__name__}: {e}", flush=True)
        if gone:
            print(f"runner: {stack}'s AgentCore Runtime {logical} ({physical}) failed to delete "
                  f"({reason}) but AgentCore no longer has it (GetAgentRuntime: "
                  f"ResourceNotFoundException); destroying again", flush=True)
            out.append(logical)
    return out


#: A Terraform build's AgentCore Runtimes (main.tf, subagent_runtimes.tf), named in an error.
TF_RUNTIME = re.compile(r"with (awscc_bedrockagentcore_runtime\.[^\s,]+)")
#: How a Runtime whose error names no address is listed (its type is in Cloud Control's message).
TF_UNNAMED_RUNTIME = "an awscc_bedrockagentcore_runtime (no address in the error)"


def timed_out_tf_runtimes(output: str) -> list[str]:
    """After a failed `terraform destroy`: the AgentCore Runtimes whose delete timed out, by
    address, from the tail of its output. awscc deletes through Cloud Control, which runs the
    same handler as CloudFormation and gives up the same way (NotStabilized; DEPLOYMENT.md,
    "When a destroy fails"). Each error is read on its own: a Runtime counts only when its
    own error says it timed out.

    Seen live (hosted MCP tf test): the error can come with no `with <address>,` line, only
    Cloud Control's "Request timed out while deleting AWS::BedrockAgentCore::Runtime.
    ErrorCode: NotStabilized" (wrapped over lines). Such an error counts too, as
    TF_UNNAMED_RUNTIME, when its own text names the Runtime type."""
    out = []
    for block in re.split(r"^[^\w\n]*Error: ", output, flags=re.MULTILINE)[1:]:
        m = TF_RUNTIME.search(block)
        low = block.lower()
        if not ("notstabilized" in low or "timed out" in low):
            continue
        if m:
            name = m.group(1)
        elif not re.search(r"with \S", block) and RUNTIME_TYPE.lower() in " ".join(low.split()):
            name = TF_UNNAMED_RUNTIME
        else:
            continue
        if name not in out:
            out.append(name)
    return out


def empty_late_log_buckets(session, stack: str, region: str, resources: list[dict]) -> list[str]:
    """After a failed `cdk destroy`: empty the stack's CloudFront logs bucket when that is
    what failed, and return its name ([] when it was not, so the failure is reported as is).

    Seen live: CloudFront delivers an access-log file minutes after a request, so one lands
    after the stack's auto-delete emptied the bucket and before CloudFormation deleted it,
    and the destroy ends DELETE_FAILED, "The bucket you tried to delete is not empty".
    Running the destroy again cannot help (the stack empties a bucket only once), and the
    person destroying from the console has no way to empty it. Only that bucket is touched:
    it must be the stack's UiLogsBucket resource, an S3 bucket, DELETE_FAILED, and the
    name CloudFormation reports for it must contain "uilogsbucket". The reason text is not
    required: it is worded differently across accounts and handlers. The deploy role's
    IAM (LateLogBuckets) limits the deletes to ax-*-stack-uilogsbucket* in any case.
    Every DELETE_FAILED resource left alone is printed with why. `resources` is
    stack_resources()."""
    s3 = session.client("s3", region_name=region)
    emptied = []
    for r in resources:
        if r.get("ResourceStatus") != "DELETE_FAILED":
            continue
        logical = str(r.get("LogicalResourceId") or "")
        name = str(r.get("PhysicalResourceId") or "")
        if r.get("ResourceType") != "AWS::S3::Bucket":
            skip = f"it is a {r.get('ResourceType')}, not an S3 bucket"
        elif not logical.startswith(LATE_LOG_BUCKET):
            skip = f"it is not the {LATE_LOG_BUCKET} resource"
        elif LATE_LOG_BUCKET.lower() not in name.lower():
            skip = f"its name {name!r} does not contain {LATE_LOG_BUCKET.lower()!r}"
        else:
            skip = ""
        if skip:
            print(f"runner: {stack}'s {logical} ({name or 'no physical id'}) is DELETE_FAILED "
                  f"({r.get('ResourceStatusReason') or 'no reason given'}); not emptied: {skip}",
                  flush=True)
            continue
        n = 0
        for page in s3.get_paginator("list_object_versions").paginate(Bucket=name):
            objects = [{"Key": v["Key"], "VersionId": v["VersionId"]}
                       for v in (page.get("Versions") or []) + (page.get("DeleteMarkers") or [])]
            if objects:
                s3.delete_objects(Bucket=name, Delete={"Objects": objects, "Quiet": True})
                n += len(objects)
        print(f"runner: {stack}'s CloudFront logs bucket {name} was refilled after it was emptied "
              f"(a late access-log delivery): deleted {n} object(s), destroying again", flush=True)
        emptied.append(name)
    return emptied


def deploy_cdk(action: str, agent: str, gateway: bool, env: dict | None = None,
               tags: dict | None = None, target: Target | None = None, signin: dict | None = None,
               guard: dict | None = None, naming=None, meta: dict | None = None,
               workflow: dict | None = None) -> dict:
    """`naming(stack_exists) -> scheme`: how the stack's buckets are named, asked once the
    stack's state is known (main's bucket naming); None = "legacy"."""
    env = env or {}
    cdk = ROOT / "cdk"
    run(["npm", "ci", "--no-fund", "--no-audit"], cdk, env)
    stack = buildstore.stack_name(agent)
    flags = cdk_context(agent, gateway, tags, signin, guard)
    if target and action == "deploy":
        # A connected account may never have run CDK. Bootstrapping is idempotent, but
        # it is a stack update every time, so only when the account has none.
        try:
            target.session().client("ssm").get_parameter(Name="/cdk-bootstrap/hnb659fds/version")
        except Exception:  # noqa: BLE001 - absent, or unreadable: bootstrap either way
            run(["npx", "cdk", "bootstrap", f"aws://{target.account}/{target.region}",
                 *flags], cdk, env)
    if action == "destroy":
        destroy = ["npx", "cdk", "destroy", stack, "--force", *flags,
                   *(["-c", f"bucketNaming={naming(True)}"] if naming else [])]
        region = target.region if target else env.get("AWS_REGION") or "us-east-1"
        for attempt in range(DESTROY_RETRIES + 1):
            if attempt:
                print(f"runner: destroying {stack} again (attempt {attempt} of "
                      f"{DESTROY_RETRIES})", flush=True)
            try:
                run(destroy, cdk, env)
                return {}
            except StepFailed:
                if attempt == DESTROY_RETRIES:
                    raise                   # the last attempt's failure is reported as is
                # A fresh session each time: a target's lasts 15 minutes, a destroy longer.
                session = target.session() if target else local_session()
                resources = stack_resources(session, stack, region)
                if resources is None:
                    raise
                emptied = empty_late_log_buckets(session, stack, region, resources)
                runtimes = timed_out_runtimes(stack, resources)
                gone = deleted_runtimes(session, stack, region, resources)
                if not emptied and not runtimes and not gone:
                    failed = sum(r.get("ResourceStatus") == "DELETE_FAILED" for r in resources)
                    print(f"runner: no {LATE_LOG_BUCKET} bucket to empty and no timed-out "
                          f"or already-deleted AgentCore Runtime in {stack}: {len(resources)} resource(s) read, "
                          f"{failed} DELETE_FAILED; reporting the destroy's own failure", flush=True)
                    raise
                if runtimes:
                    print(f"runner: waiting {RUNTIME_DELETE_PAUSE:.0f}s for the delete of "
                          f"{', '.join(runtimes)} to go on, then destroying {stack} again", flush=True)
                    time.sleep(RUNTIME_DELETE_PAUSE)
    region = target.region if target else env.get("AWS_REGION") or "us-east-1"
    exists = recover_failed_create(target.session() if target else local_session(), stack, region,
                                   agent=agent, build_account=target.account if target else "",
                                   meta=meta, workflow=workflow)
    scheme = naming(exists) if naming else "legacy"
    out = WORK / "cdk-outputs.json"
    run(["npx", "cdk", "deploy", stack, "--require-approval", "never",
         "--outputs-file", str(out), *flags, "-c", f"bucketNaming={scheme}"], cdk, env)
    return {"stack": stack, **cdk_outputs(out, stack)}


# --- begin Track E storage ---
#: The Terraform addresses of a build's storage (terraform/storage.tf): the bucket and its
#: settings, policy and ABAC status.
STORAGE_ADDRESS = re.compile(r"^aws_s3_bucket(_[a-z_]+)?\.storage(_tls)?\[0\]$")


def storage_retained(workflow: dict) -> bool:
    """orchestrator.storage {create: true, retain: true}: a destroy keeps the bucket."""
    orch = workflow.get("orchestrator") if isinstance(workflow.get("orchestrator"), dict) else {}
    st = orch.get("storage") if isinstance(orch.get("storage"), dict) else {}
    return st.get("create") is True and st.get("retain") is True


def keep_retained_storage(tf: str, tf_dir: Path, sub_env: dict) -> list[str]:
    """Before a `terraform destroy`: with orchestrator.storage.retain, take the storage out of
    the state, so the destroy leaves the bucket and its contents. (The CDK path keeps it with
    RemovalPolicy.RETAIN, cdk/lib/storage.ts.) The addresses removed, [] otherwise."""
    wf_path = ROOT / "app" / "workflow.json"
    if not wf_path.exists() or not storage_retained(json.loads(wf_path.read_text())):
        return []
    listed = subprocess.run([tf, "state", "list"], cwd=tf_dir,  # noqa: S603 - terraform argv list, no shell  # nosec B603  # nosemgrep
                            capture_output=True, text=True, check=False,
                            env={**os.environ, **sub_env}).stdout.split()
    keep = [a for a in listed if STORAGE_ADDRESS.match(a)]
    if keep:
        run([tf, "state", "rm", *keep], tf_dir, sub_env)
        print(f"runner: orchestrator.storage.retain: keeping the build's storage ({len(keep)} resource(s) "
              "left out of the destroy)", flush=True)
    return keep
# --- end Track E storage ---


def deploy_terraform(action: str, agent: str, gateway: bool, env: dict,
                     sub_env: dict | None = None, tags: dict | None = None,
                     target: Target | None = None, signin: dict | None = None,
                     guard: dict | None = None, bucket_naming: str = "") -> dict:
    sub_env = sub_env or {}
    tf_dir = ROOT / "terraform"
    tf = ensure_terraform(env.get("TERRAFORM_VERSION") or "1.15.8")
    console_region = env.get("AWS_REGION", "us-east-1")
    region = target.region if target else console_region
    key = env["STATE_PREFIX"] + "terraform.tfstate"
    # An override file, not -backend-config: override files are MERGED into the
    # backend block by documented rules, so the committed `key` is replaced for certain.
    # The state stays in THIS console's bucket even when the stack goes elsewhere — the
    # `console` profile — so a customer account never holds the stack's full wiring.
    (tf_dir / "zz_builder_backend_override.tf").write_text(
        'terraform {\n  backend "s3" {\n'
        f'    bucket = "{env["BUILDS_BUCKET"]}"\n    key    = "{key}"\n'
        f'    region = "{console_region}"\n'
        + ('    profile = "console"\n' if target else "")
        + '  }\n}\n')
    (tf_dir / "builder.auto.tfvars.json").write_text(
        json.dumps(tf_vars(agent, gateway, region, tags, signin, guard, bucket_naming), indent=2))
    run([tf, "init", "-input=false", "-no-color"], tf_dir, sub_env)
    # The UI build FIRST, on its own — for a destroy too. The files it emits are ui.tf's
    # `for_each`, and whenever the build step is not in state (a stack's first apply, or
    # a destroy that already removed it and then failed) they are "known only after
    # apply", which a plan refuses. Building first makes them known.
    build_ui = [tf, "apply", "-auto-approve", "-input=false", "-no-color",
                "-target=null_resource.ui_build"]
    # This checkout is fresh, so web/dist/ never exists here — but the state can say
    # the UI was built (a redeploy, or a retry). Then the build step is skipped, ui.tf
    # finds no files, and the apply DELETES the build's whole UI from its bucket. So a
    # build step that is in state is rebuilt whenever its output is missing.
    if not (ROOT / "web" / "dist" / "index.html").exists():
        listed = subprocess.run([tf, "state", "list"], cwd=tf_dir,  # noqa: S603 - terraform argv list, no shell  # nosec B603  # nosemgrep
                                capture_output=True, text=True, check=False,
                                env={**os.environ, **sub_env}).stdout
        if "null_resource.ui_build" in listed.split():
            build_ui.append("-replace=null_resource.ui_build")
    run(build_ui, tf_dir, sub_env)
    if action == "destroy":
        keep_retained_storage(tf, tf_dir, sub_env)  # Track E storage
        destroy = [tf, "destroy", "-auto-approve", "-input=false", "-no-color"]
        for attempt in range(DESTROY_RETRIES + 1):
            if attempt:
                print(f"runner: destroying {agent} again (attempt {attempt} of {DESTROY_RETRIES})",
                      flush=True)
                # The failed destroy can have removed the UI build step from state; rebuilding
                # it makes ui.tf's files known again. web/dist/ exists now, so no -replace.
                run([c for c in build_ui if not c.startswith("-replace=")], tf_dir, sub_env)
            try:
                run(destroy, tf_dir, sub_env)
                return {}
            except StepFailed as e:
                if attempt == DESTROY_RETRIES:
                    raise                   # the last attempt's failure is reported as is
                runtimes = timed_out_tf_runtimes(str(e))
                if not runtimes:
                    raise
                print(f"runner: AgentCore Runtime {', '.join(runtimes)} timed out while deleting; "
                      f"its delete goes on in AgentCore: waiting {RUNTIME_DELETE_PAUSE:.0f}s, then "
                      f"destroying again", flush=True)
                time.sleep(RUNTIME_DELETE_PAUSE)
        return {}
    run([tf, "apply", "-auto-approve", "-input=false", "-no-color"], tf_dir, sub_env)
    raw = subprocess.run([tf, "output", "-json"], cwd=tf_dir,  # noqa: S603 - terraform argv list, no shell  # nosec B603  # nosemgrep
                         capture_output=True, text=True, check=True,
                         env={**os.environ, **sub_env}).stdout
    return {"stack": key, **tf_outputs(raw)}


def leftover_log_groups(logs, agent: str) -> list[str]:
    """Log groups a destroyed build leaves behind, found by its unique id.

    Neither tool owns all of them: AgentCore writes /aws/bedrock-agentcore/runtimes/<rt>,
    and on the CDK path the provider framework's own Lambdas log to groups the stack
    never declared. Matching on the build's 8-hex id (ax_xxxxxxxx / ax-xxxxxxxx) is what
    makes deleting them safe: nothing else in the account carries it."""
    hexid = agent[len(buildstore.AGENT_PREFIX):]
    names = (agent, agent.replace("_", "-"))
    out, kwargs = [], {"logGroupNamePattern": hexid}
    while True:
        page = logs.describe_log_groups(**kwargs)
        out += [g["logGroupName"] for g in page.get("logGroups", [])
                if any(n in g["logGroupName"] for n in names)]
        if not page.get("nextToken"):
            return out
        kwargs["nextToken"] = page["nextToken"]


def delete_leftover_logs(agent: str, logs=None, session=None) -> None:
    try:
        if logs is None:
            logs = (session or local_session()).client("logs")
        names = leftover_log_groups(logs, agent)
    except Exception as e:  # noqa: BLE001 - tidying up must never fail a destroy that worked
        print(f"runner: could not list leftover log groups: {type(e).__name__}: {e}")
        return
    # One at a time: a group this role may not delete must not keep the rest behind.
    for name in names:
        try:
            logs.delete_log_group(logGroupName=name)
            print(f"runner: deleted leftover log group {name}")
        except Exception as e:  # noqa: BLE001 - tidying up must never fail a destroy that worked
            print(f"runner: could not delete leftover log group {name}: {type(e).__name__}: {e}")


def delete_login(env: dict, bid: str) -> None:
    try:
        import boto3
        boto3.client("secretsmanager").delete_secret(SecretId=login_secret_id(env, bid),
                                                     ForceDeleteWithoutRecovery=True)
    except Exception as e:  # noqa: BLE001 - absent is the common case
        print(f"runner: no login secret deleted ({type(e).__name__})")


def delete_build_secrets(env: dict, bid: str) -> None:
    try:
        import boto3
        boto3.client("secretsmanager").delete_secret(
            SecretId=f"{env.get('SECRETS_PREFIX') or 'agentexpress/builds/'}{bid}",
            ForceDeleteWithoutRecovery=True)
    except Exception as e:  # noqa: BLE001 - absent is the common case
        print(f"runner: no secrets deleted ({type(e).__name__})")


def main(env: dict | None = None, table=None, s3=None) -> int:
    env = dict(os.environ if env is None else env)
    missing = [k for k in REQUIRED if not env.get(k)]
    if missing:
        print(f"runner: missing environment {missing}", file=sys.stderr)
        return 2
    action, tool = env["ACTION"], env["TOOL"]
    bid, agent, version = env["BUILD_ID"], env["AGENT_NAME"], int(env["VERSION"])
    if action not in ("deploy", "destroy") or tool not in buildstore.TOOLS \
            or not buildstore.AGENT_RE.match(agent):
        print(f"runner: bad request action={action!r} tool={tool!r} agent={agent!r}",
              file=sys.stderr)
        return 2
    if table is None or s3 is None:
        import boto3
        table = table or boto3.resource("dynamodb").Table(env["BUILDS_TABLE"])
        s3 = s3 or boto3.client("s3")
    bucket = env["BUILDS_BUCKET"]
    meta: dict = {}
    where = {"account": "console", "region": env.get("AWS_REGION", "us-east-1")}

    def audit(result: str, **detail) -> None:
        """The outcome, in the audit log, next to the request the BFF logged."""
        if meta.get("owner"):
            buildstore.record_audit(
                table, str(meta["owner"]),
                str((meta.get("job") or {}).get("by") or meta.get("ownerEmail") or ""),
                f"{action}.{result}", build=bid, name=str(meta.get("name") or ""),
                agentName=agent, version=version, tool=tool, **where, **detail)

    try:
        # Whose build this is, and where it goes, from the store — never from the
        # request — because they decide the state path and the account deployed into.
        meta = table.get_item(Key=buildstore.meta_key(bid)).get("Item") or {}
        owner = str(meta.get("owner") or "")
        PRIVATE.clear()
        if meta.get("ownerEmail"):
            PRIVATE.add(str(meta["ownerEmail"]))
        if not owner:
            raise StepFailed(f"build {bid} has no owner in the builds store")
        state = buildstore.state_prefix(owner, bid)
        env["STATE_PREFIX"] = state
        target = resolve_target(table, meta, owner, bid)
        console_region = env.get("AWS_REGION", "us-east-1")
        if target:
            where = {"account": target.account, "region": target.region}
        sub_env: dict = {}
        if target:
            WORK.mkdir(parents=True, exist_ok=True)
            sub_env = target.env(console_region, terraform=tool == "terraform")
            try:
                session = target.session()
                session.client("sts").get_caller_identity()
            except Exception as e:
                raise StepFailed(f"cannot reach account {target.account} through its deploy role "
                                 f"({type(e).__name__}): was the connection stack deleted? "
                                 f"Reconnect the account in the console.") from e
            if action == "deploy":
                agentcore_available(session, target.region)
            print(f"runner: deploying into connected account {target.account} ({target.region})")
        remote = target.session if target else local_session

        buildstore.set_job(table, bid, status="RUNNING", phase="generating")
        bundle = json.loads(s3.get_object(
            Bucket=bucket, Key=buildstore.version_key(bid, version))["Body"].read())
        WORK.mkdir(parents=True, exist_ok=True)
        path = WORK / "bundle.json"
        path.write_text(json.dumps(bundle))
        built_on = str((bundle.get("framework") or {}).get("version") or "")
        if built_on and built_on != buildstore.FRAMEWORK_VERSION:
            print(f"runner: version {version} was frozen on framework {built_on}; deploying "
                  f"it with framework {buildstore.FRAMEWORK_VERSION}")
        run([sys.executable, "scaffold.py", "apply", str(path), "--exact"], ROOT)
        # Code from git or S3 (codeSource), before its requirements are installed.
        materialise_code_sources(bundle.get("workflow") or {}, bid, env, target, action)
        # Hosted MCP servers' code (tools.<key>.hosted), for the image CDK / Terraform build.
        prepare_hosted_mcp(bundle.get("workflow") or {}, bid, env, target, action)
        install_code_requirements(bundle.get("workflow") or {})
        install_builtin_requirements(bundle.get("workflow") or {})  # Track B docgen
        sync_documents(s3, bucket, bid)
        if action == "destroy":
            stand_in_corpora(bundle.get("workflow") or {})
        gateway = buildstore.needs_gateway(bundle.get("workflow") or {})
        sub_env.update(read_secrets(env, bid))
        # The console's extra tags (-c buildTags / var.build_tags), in any account.
        extra = extra_build_tags(env)
        tags = build_tags(bid, version, owner, str(meta.get("ownerEmail") or ""),
                          env.get("CONSOLE_NAME", ""), extra)

        buildstore.set_job(table, bid, phase="destroying" if action == "destroy" else "deploying")
        signin = sign_in(bundle.get("workflow") or {})
        # The console's boundary and role path, if it sets them, on this build's roles too.
        guard = guardrails(env, target)
        # And its LiteLLM proxy, for the build's litellm/<model> agents (cdk_context passes it on).
        guard.update(litellm_settings(env))
        # And its documentation links, for the build's own app.
        guard.update(docs_settings(env))
        workflow = bundle.get("workflow") or {}
        chosen: dict = {}

        def naming(stack_exists: bool = False, state_exists: bool = False) -> str:
            """How the build's buckets are named (bucket_naming), recorded on the build before
            a deploy creates any of them. A destroy uses what is recorded and writes nothing."""
            scheme, why = bucket_naming(meta, stack_exists=stack_exists, state_exists=state_exists)
            if action == "deploy" and meta.get("bucketNaming") != scheme:
                from botocore.exceptions import ClientError
                try:
                    buildstore.set_bucket_naming(table, bid, scheme)
                except ClientError as e:
                    if (e.response.get("Error") or {}).get("Code") != "ConditionalCheckFailedException":
                        raise
                    raise StepFailed("the build's bucket naming was recorded by another job while "
                                     "this one ran; deploy again") from e
            chosen["scheme"] = scheme
            print(f"runner: bucket names: {scheme} ({why})", flush=True)
            return scheme

        if tool == "cdk":
            outputs = deploy_cdk(action, agent, gateway, sub_env, tags, target, signin, guard,
                                 naming, meta, workflow)
        else:
            # Terraform state under the build: something of it may be deployed.
            listed = (s3.list_objects_v2(Bucket=bucket, Prefix=state, MaxKeys=1)
                      if action == "deploy" else {"KeyCount": 1})
            tf_naming = naming(state_exists=bool(listed.get("KeyCount") or listed.get("Contents")))
            outputs = deploy_terraform(action, agent, gateway, env, sub_env, tags, target, signin, guard,
                                       tf_naming)

        if action == "destroy":
            delete_leftover_logs(agent, session=remote())
            # Nothing of a destroyed build stays in this account but its definition.
            removed = buildstore.delete_prefix(s3, bucket, state)
            if removed:
                print(f"runner: deleted {removed} Terraform state object(s) under {state}")
            buildstore.forget_runs(table, bid)
            delete_login(env, bid)          # its console is gone, so is its login
            if env.get("DELETE_AFTER") == "1":
                delete_build_secrets(env, bid)
                buildstore.purge(table, s3, bucket, bid, owner)
                print(f"runner: build {bid} destroyed and deleted")
                audit("succeeded", deleted=True)
            else:
                # Storage kept past the destroy keeps its name, so the build keeps its scheme.
                keep = bucket_naming(meta, stack_exists=True)[0] if storage_retained(workflow) else ""
                buildstore.record_destroyed(table, bid, keep_naming=keep)
                print(f"runner: build {bid} destroyed")
                audit("succeeded")
            return 0
        email = str(meta.get("ownerEmail") or "")
        password = temporary_password()
        invited = invite_owner(remote(), outputs.get("userPoolId", ""), email, password)
        if invited == "created":
            # Shown to the owner on the build page (GET /api/builds/{id}/login), marked
            # temporary: Cognito makes them replace it at the first sign-in.
            try:
                store_login(env, bid, {"user": email, "password": password,
                                       "temporary": True, "at": buildstore.now()})
            except Exception as e:  # noqa: BLE001 - the email still carries it
                print(f"runner: could not store the owner's login: {type(e).__name__}: {e}")
        deployed = {"version": version, "tool": tool, "agentName": agent,
                    "account": target.account if target else "",
                    "region": target.region if target else console_region,
                    "statusTable": f"{agent}_status", "eventsTable": f"{agent}_events",
                    "telemetryTable": f"{agent}_telemetry", "at": buildstore.now(),
                    "appUser": email if invited else "",
                    # What actually deployed it: the framework in this source zip.
                    "frameworkVersion": buildstore.FRAMEWORK_VERSION,
                    **({"bucketNaming": chosen["scheme"]} if chosen.get("scheme") else {}), **outputs}
        buildstore.record_deployed(table, bid, deployed)
        print(f"runner: build {bid} v{version} deployed with {tool}: {json.dumps(outputs)}")
        audit("succeeded", uiUrl=str(outputs.get("uiUrl") or ""))
        return 0
    except Exception as e:  # noqa: BLE001 - every failure must reach the Build view
        msg = str(e) if isinstance(e, StepFailed) else f"{type(e).__name__}: {e}"
        print(f"runner: FAILED — {redact_output(msg)}", file=sys.stderr)
        try:
            buildstore.record_failed(table, bid, msg)
        except Exception as rec:  # noqa: BLE001 - recording the failure must not hide it
            print(f"runner: could not record the failure: {rec}", file=sys.stderr)
        audit("failed", error=msg[-300:])
        return 1


if __name__ == "__main__":
    sys.exit(main())
