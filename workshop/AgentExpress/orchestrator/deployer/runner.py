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
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "bff"))
import buildstore  # noqa: E402  - the shared layout, next to the BFF that reads it

REQUIRED = ("ACTION", "TOOL", "BUILD_ID", "VERSION", "AGENT_NAME",
            "BUILDS_TABLE", "BUILDS_BUCKET")
ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
#: Scratch space for the bundle, the terraform binary and the CDK outputs file. A
#: CodeBuild container runs one build and is thrown away, so nothing here is shared.
WORK = Path(tempfile.gettempdir()) / "agentexpress-runner"


class StepFailed(Exception):
    """A command failed; carries the tail of its output for the Build view."""


def run(cmd: list[str], cwd: Path, env: dict | None = None, keep: int = 60) -> list[str]:
    """Run a command, streaming its output to the CodeBuild log, and keep the tail."""
    print(f"$ {' '.join(cmd)}", flush=True)
    tail: collections.deque[str] = collections.deque(maxlen=keep)
    proc = subprocess.Popen(cmd, cwd=cwd, env={**os.environ, **(env or {})},  # noqa: S603
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    assert proc.stdout is not None
    for line in proc.stdout:
        sys.stdout.write(line)
        tail.append(ANSI.sub("", line.rstrip()))
    sys.stdout.flush()
    if proc.wait() != 0:
        raise StepFailed(f"`{' '.join(cmd[:3])}` failed (exit {proc.returncode}):\n"
                         + "\n".join(tail))
    return list(tail)


def cdk_context(agent: str, gateway: bool, tags: dict | None = None) -> list[str]:
    """The same flags a developer deploys with, pinned for a build stack.

    Its own Cognito pool (the Gateway's machine-to-machine client needs one), the tool
    plane only when the workflow has tools, and `builder=false`: a build is a workflow,
    not another console, so it gets no Build view, deploy project or builds store."""
    flags = ["-c", f"agentName={agent}", "-c", "idp=cognito", "-c", "createCognito=true",
             "-c", f"enableGateway={'true' if gateway else 'false'}", "-c", "builder=false"]
    if tags:
        flags += ["-c", f"tags={json.dumps(tags, separators=(',', ':'))}"]
    return flags


def tf_vars(agent: str, gateway: bool, region: str, tags: dict | None = None) -> dict:
    """terraform/variables.tf values, mirroring cdk_context()."""
    out = {"region": region, "agent_name": agent, "idp": "cognito",
           "cognito": {"create": True}, "enable_gateway": gateway,
           "container_engine": "docker", "enable_builder": False}
    if tags:
        out["tags"] = tags
    return out


def build_tags(bid: str, version: int, owner: str, email: str, console: str) -> dict:
    """What every resource of a build is tagged with, so a bill or an inventory can say
    which build, version, user and console it came from."""
    tags = {"agentexpress:build": bid, "agentexpress:version": str(version),
            "agentexpress:owner": email or owner}
    if console:
        tags["agentexpress:console"] = console
    # A tag value allows letters, digits, spaces and _ . : / = + - @ only.
    return {k: re.sub(r"[^\w .:/=+\-@]", "_", v)[:256] for k, v in tags.items()}


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
    out = {}
    if keys:
        out["TOOL_API_KEYS"] = out["TF_VAR_tool_api_keys"] = json.dumps(keys)
    if tokens:
        out["A2A_TOKENS"] = out["TF_VAR_a2a_tokens"] = json.dumps(tokens)
    if idents:
        out["IDENTITY_SECRETS"] = out["TF_VAR_identity_secrets"] = json.dumps(idents)
    print(f"runner: using {len(keys)} tool API key(s), {len(tokens)} A2A token(s) and "
          f"{len(idents)} identity secret(s)")
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


def install_code_requirements(workflow: dict) -> list[str]:
    """pip install each code tool's requirements.txt into its folder, which CDK and
    Terraform then zip as they are (app/tools/_code/<key>/)."""
    done = []
    for key in buildstore.code_tools(workflow):
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


def invite_owner(session, pool_id: str, email: str, password: str = "") -> str:
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
    except Exception as e:  # noqa: BLE001
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
    return {"runtimeArn": out.get("agentRuntimeArn", ""), "uiUrl": out.get("uiUrl", ""),
            "apiUrl": out.get("apiEndpoint", ""), "userPoolId": out.get("cognitoUserPoolId", "")}


def tf_outputs(raw: str) -> dict:
    out = {k: (v or {}).get("value") for k, v in json.loads(raw or "{}").items()}
    return {"runtimeArn": out.get("agent_runtime_arn") or "", "uiUrl": out.get("ui_url") or "",
            "apiUrl": out.get("api_endpoint") or "",
            "userPoolId": out.get("cognito_user_pool_id") or ""}


#: A stack whose FIRST create failed: nothing in it ever worked, and CloudFormation can
#: only delete it. ROLLBACK_FAILED is what a create leaves when rolling back hits a
#: resource still coming up (AgentCore Memory: "in transitional state CREATING").
FAILED_CREATE = ("ROLLBACK_COMPLETE", "ROLLBACK_FAILED", "DELETE_FAILED")


def clear_failed_create(session, stack: str, region: str, attempts: int = 4,
                        pause: float = 60.0) -> None:
    """Delete the build's stack when its first create failed, so the retry can create it
    again: `cdk deploy` refuses a stack in ROLLBACK_FAILED. Only those states — a stack
    that ever deployed (UPDATE_ROLLBACK_FAILED...) is never touched. Done as the CDK
    deploy role, which is what CloudFormation already trusts with this stack."""
    import time

    import boto3
    from botocore.exceptions import ClientError
    account = session.client("sts").get_caller_identity()["Account"]
    c = session.client("sts").assume_role(
        RoleArn=f"arn:aws:iam::{account}:role/cdk-hnb659fds-deploy-role-{account}-{region}",
        RoleSessionName="agentexpress-recover")["Credentials"]
    cfn = boto3.client("cloudformation", region_name=region, aws_access_key_id=c["AccessKeyId"],
                       aws_secret_access_key=c["SecretAccessKey"], aws_session_token=c["SessionToken"])
    for i in range(attempts):
        try:
            status = cfn.describe_stacks(StackName=stack)["Stacks"][0]["StackStatus"]
        except ClientError as e:
            if "does not exist" in str(e):
                return
            raise
        if status not in FAILED_CREATE:
            return
        print(f"[recover] {stack} is {status} from a failed first create: deleting it before "
              f"deploying again (attempt {i + 1})", flush=True)
        cfn.delete_stack(StackName=stack)
        try:
            cfn.get_waiter("stack_delete_complete").wait(
                StackName=stack, WaiterConfig={"Delay": 15, "MaxAttempts": 80})
            return
        except Exception:  # noqa: BLE001 - DELETE_FAILED: a resource was still settling
            time.sleep(pause)
    raise RuntimeError(f"{stack} is still {status} after {attempts} deletes; delete it by hand "
                       "and deploy again")


def deploy_cdk(action: str, agent: str, gateway: bool, env: dict | None = None,
               tags: dict | None = None, target: Target | None = None) -> dict:
    env = env or {}
    cdk = ROOT / "cdk"
    run(["npm", "ci", "--no-fund", "--no-audit"], cdk, env)
    stack = buildstore.stack_name(agent)
    flags = cdk_context(agent, gateway, tags)
    if target and action == "deploy":
        # A connected account may never have run CDK. Bootstrapping is idempotent, but
        # it is a stack update every time, so only when the account has none.
        try:
            target.session().client("ssm").get_parameter(Name="/cdk-bootstrap/hnb659fds/version")
        except Exception:  # noqa: BLE001 - absent, or unreadable: bootstrap either way
            run(["npx", "cdk", "bootstrap", f"aws://{target.account}/{target.region}",
                 *flags], cdk, env)
    if action == "destroy":
        run(["npx", "cdk", "destroy", stack, "--force", *flags], cdk, env)
        return {}
    clear_failed_create(target.session() if target else local_session(), stack,
                        target.region if target else env.get("AWS_REGION") or "us-east-1")
    out = WORK / "cdk-outputs.json"
    run(["npx", "cdk", "deploy", stack, "--require-approval", "never",
         "--outputs-file", str(out), *flags], cdk, env)
    return {"stack": stack, **cdk_outputs(out, stack)}


def deploy_terraform(action: str, agent: str, gateway: bool, env: dict,
                     sub_env: dict | None = None, tags: dict | None = None,
                     target: Target | None = None) -> dict:
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
        json.dumps(tf_vars(agent, gateway, region, tags), indent=2))
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
        listed = subprocess.run([tf, "state", "list"], cwd=tf_dir, capture_output=True,  # noqa: S603
                                text=True, check=False, env={**os.environ, **sub_env}).stdout
        if "null_resource.ui_build" in listed.split():
            build_ui.append("-replace=null_resource.ui_build")
    run(build_ui, tf_dir, sub_env)
    if action == "destroy":
        run([tf, "destroy", "-auto-approve", "-input=false", "-no-color"], tf_dir, sub_env)
        return {}
    run([tf, "apply", "-auto-approve", "-input=false", "-no-color"], tf_dir, sub_env)
    raw = subprocess.run([tf, "output", "-json"], cwd=tf_dir, capture_output=True,  # noqa: S603
                         text=True, check=True, env={**os.environ, **sub_env}).stdout
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
        for name in leftover_log_groups(logs, agent):
            logs.delete_log_group(logGroupName=name)
            print(f"runner: deleted leftover log group {name}")
    except Exception as e:  # noqa: BLE001 - tidying up must never fail a destroy that worked
        print(f"runner: could not delete leftover log groups: {type(e).__name__}: {e}")


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
        install_code_requirements(bundle.get("workflow") or {})
        sync_documents(s3, bucket, bid)
        if action == "destroy":
            stand_in_corpora(bundle.get("workflow") or {})
        gateway = buildstore.needs_gateway(bundle.get("workflow") or {})
        sub_env.update(read_secrets(env, bid))
        tags = build_tags(bid, version, owner, str(meta.get("ownerEmail") or ""),
                          env.get("CONSOLE_NAME", ""))

        buildstore.set_job(table, bid, phase="destroying" if action == "destroy" else "deploying")
        outputs = (deploy_cdk(action, agent, gateway, sub_env, tags, target) if tool == "cdk"
                   else deploy_terraform(action, agent, gateway, env, sub_env, tags, target))

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
                buildstore.record_destroyed(table, bid)
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
                    "frameworkVersion": buildstore.FRAMEWORK_VERSION, **outputs}
        buildstore.record_deployed(table, bid, deployed)
        print(f"runner: build {bid} v{version} deployed with {tool}: {json.dumps(outputs)}")
        audit("succeeded", uiUrl=str(outputs.get("uiUrl") or ""))
        return 0
    except Exception as e:  # noqa: BLE001 - every failure must reach the Build view
        msg = str(e) if isinstance(e, StepFailed) else f"{type(e).__name__}: {e}"
        print(f"runner: FAILED — {msg}", file=sys.stderr)
        try:
            buildstore.record_failed(table, bid, msg)
        except Exception as rec:  # noqa: BLE001
            print(f"runner: could not record the failure: {rec}", file=sys.stderr)
        audit("failed", error=msg[-300:])
        return 1


if __name__ == "__main__":
    sys.exit(main())
