"""A runtime "harness" agent's own checks, for validate_build.py.

A harness agent has no code: a managed AgentCore Harness is built from its `instructions`,
model, `tool` and `harness` block (app/common/harness_agent.py), or `harness.arn` names one
made elsewhere. The generic key checks (app/keys.json) already say which keys apply and
their types; these are the rest: what each setting may hold, in the shapes CreateHarness
accepts (botocore bedrock-agentcore-control), and which settings exclude each other.

Mirrors web/src/builder/harnessCheck.ts word for word (both run
tests/fixtures/validation_cases.json) and, for what the container refuses,
app/common/harness_agent.problem. Whether the deployment has a Gateway, its machine client
is Cognito's, and a harness ARN is in this account and region are checked by the IaC at
plan/synth (terraform/harness.tf, cdk/lib/harness.ts), which knows them.
"""

from __future__ import annotations

import re

from netguard import blocked_address

#: The Document generator's source (docgen_tool.SOURCE).
DOCGEN_SOURCE = "docgen"

#: Bounds of the `harness` block (app/common/harness_agent.LIMITS).
LIMITS = {"maxIterations": (1, 100), "timeoutSeconds": (1, 900)}
PROVIDERS = ("bedrock", "openai", "gemini", "litellm")
#: What a harness you bring (`harness.arn`) may still set (harness_agent.BYO_KEYS / BYO_NOT).
BYO_KEYS = ("arn", "endpoint", "maxIterations", "timeoutSeconds")
BYO_NOT = ("instructions", "model", "temperature", "topP", "maxTokens", "tool")
#: Tool auth modes that act as the person: the harness calls the Gateway as a machine.
PERSON_AUTH = ("user", "obo")
#: The tool names the framework gives the Gateway, browser and code interpreter.
RESERVED_TOOLS = ("gateway", "browser", "code_interpreter")
MAX_MCP, MAX_SKILLS, MAX_MOUNTS = 10, 20, 5

# The patterns are botocore's (bedrock-agentcore-control CreateHarness), anchored.
HARNESS_ARN_RE = re.compile(r"^arn:aws[a-z-]*:bedrock-agentcore:[a-z0-9-]+:[0-9]{12}:harness/[a-zA-Z][a-zA-Z0-9_]{0,39}-[a-zA-Z0-9]{10}$")  # noqa: E501 - botocore's pattern verbatim, so it can be compared with the model
API_KEY_ARN_RE = re.compile(r"^arn:aws:bedrock-agentcore:[a-z0-9-]+:[0-9]{12}:token-vault/[a-zA-Z0-9-.]+/apikeycredentialprovider/[a-zA-Z0-9-.]+$")  # noqa: E501 - botocore's pattern verbatim, so it can be compared with the model
BROWSER_ARN_RE = re.compile(r"^arn:aws(-[^:]+)?:bedrock-agentcore:[a-z0-9-]+:(aws|[0-9]{12}):browser(-custom)?/(aws\.browser\.v1|[a-zA-Z][a-zA-Z0-9_]{0,47}-[a-zA-Z0-9]{10})$")  # noqa: E501 - botocore's pattern verbatim, so it can be compared with the model
CODE_ARN_RE = re.compile(r"^arn:aws(-[^:]+)?:bedrock-agentcore:[a-z0-9-]+:(aws|[0-9]{12}):code-interpreter(-custom)?/(aws\.codeinterpreter\.v1|[a-zA-Z][a-zA-Z0-9_]{0,47}-[a-zA-Z0-9]{10})$")  # noqa: E501 - botocore's pattern verbatim, so it can be compared with the model
CONTAINER_RE = re.compile(r"^(([0-9]{12})\.dkr\.ecr\.([a-z0-9-]+)\.amazonaws\.com|public\.ecr\.aws)/((?:[a-z0-9]+(?:[._-][a-z0-9]+)*/)*[a-z0-9]+(?:[._-][a-z0-9]+)*)(?::([^:@]{1,300}))?(?:@(.+))?$")  # noqa: E501 - botocore's pattern verbatim, so it can be compared with the model
EFS_ARN_RE = re.compile(r"^arn:aws[-a-z]*:elasticfilesystem:[0-9a-z-]+:[0-9]{12}:access-point/fsap-[0-9a-f]{8,40}$")
S3FILES_ARN_RE = re.compile(r"^arn:aws[-a-z]*:s3files:[0-9a-z-]+:[0-9]{12}:file-system/fs-[0-9a-f]{17,40}/access-point/fsap-[0-9a-f]{17,40}$")  # noqa: E501 - botocore's pattern verbatim, so it can be compared with the model
MOUNT_RE = re.compile(r"^/mnt/[a-zA-Z0-9._-]+/?$")
ENDPOINT_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]{0,47}$")
TOOL_NAME_RE = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")
SUBNET_RE = re.compile(r"^subnet-[0-9a-zA-Z]{8,17}$")
SG_RE = re.compile(r"^sg-[0-9a-zA-Z]{8,17}$")
S3_URI_RE = re.compile(r"^s3://[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]/\S*$")


def _obj(v) -> bool:
    return isinstance(v, dict)


def public_https(url) -> bool:
    """An https:// URL, no user:password@, on a host that is not this machine or a private
    network: an IP literal must be public (netguard's rule), and a name may not be
    localhost or a .local / .internal one. Names are not resolved here."""
    m = re.match(r"^https://([^/?#]+)", str(url or ""))
    if not m or "@" in m.group(1):
        return False
    host = m.group(1)
    host = host[1:host.index("]")] if host.startswith("[") and "]" in host else host.rsplit(":", 1)[0]
    host = host.lower().rstrip(".")
    if not host or host == "localhost" or host.endswith((".localhost", ".local", ".internal")) or ":" in host:
        # An IPv6 literal is refused outright: the page's twin has no IPv6 classifier.
        return False
    if re.fullmatch(r"[0-9.]+", host):
        return not blocked_address(host)
    return "." in host


def _api_key(value, identities: dict, add, where, p: str) -> None:
    """An `identities` name of type apikey, or an API key credential provider ARN."""
    v = str(value)
    if v.startswith("arn:"):
        if not API_KEY_ARN_RE.match(v):
            add("error", where, p, "must be an API key credential provider ARN "
                "(arn:aws:bedrock-agentcore:<region>:<account>:token-vault/default/apikeycredentialprovider/<name>)")
    else:
        ident = identities.get(v)
        if not (_obj(ident) and str(ident.get("type") or "").lower() == "apikey"):
            add("error", where, p, f'"{v}" is not an apikey identity. Declare it under `identities` '
                "(type apikey, its key in the build's secrets), or give a credential provider ARN")


def check(agents: dict, add, wf: dict | None = None) -> None:
    """Add the issues of every harness agent."""
    wf = wf or {}
    tools = wf.get("tools") if _obj(wf.get("tools")) else {}
    identities = wf.get("identities") if _obj(wf.get("identities")) else {}
    memories = wf.get("memories") if _obj(wf.get("memories")) else {}
    for aid, a in agents.items():
        if not _obj(a) or str(a.get("runtime") or "").lower() != "harness":
            continue
        where = {"kind": "agent", "id": aid}
        path = f"agents.{aid}"
        block = a.get("harness") if _obj(a.get("harness")) else {}
        hp = f"{path}.harness"
        brought = "arn" in block
        text = a.get("instructions")
        if isinstance(text, str) and not text.strip():
            add("error", where, f"{path}.instructions", "write the system prompt the harness is created with")
        elif "instructions" not in a and not brought:
            add("error", where, f"{path}.instructions", '`instructions` is required for runtime "harness"')
        for key, (low, high) in LIMITS.items():
            v = block.get(key)
            if isinstance(v, int) and not isinstance(v, bool) and not low <= v <= high:
                add("error", where, f"{hp}.{key}", f"must be a whole number from {low} to {high}")
        if block.get("shell") is True:
            add("warning", where, f"{hp}.shell",
                "the agent can run shell commands and reach the internet from its own session, "
                "with the request and every earlier step's output in hand")
        core = a.get("agentcore")
        if _obj(core) and "identity" in core:
            add("error", where, f"{path}.agentcore.identity",
                "a harness agent calls no API with this build's identities — remove it")
        if "hooks" in block:
            add("error", where, f"{hp}.hooks", "lifecycle hooks cannot be deployed yet: neither CloudFormation "
                "(AWS::BedrockAgentCore::Harness) nor the Terraform provider can set them on a harness")

        if brought:
            if not HARNESS_ARN_RE.match(str(block["arn"])):
                add("error", where, f"{hp}.arn",
                    "must be a harness ARN: arn:aws:bedrock-agentcore:<region>:<account>:harness/<name>-<id>")
            for k in BYO_NOT:
                if k in a:
                    add("error", where, f"{path}.{k}", "is set on the harness you brought (harness.arn) — remove it")
            for k in block:
                if k not in BYO_KEYS and k != "hooks":
                    add("error", where, f"{hp}.{k}", "is set on the harness you brought (harness.arn) — remove it")
        _endpoint(block, add, where, hp)
        if brought:
            continue

        # --- the model
        provider = block.get("provider", "bedrock")
        if "provider" in block and provider not in PROVIDERS:
            add("error", where, f"{hp}.provider", f"must be one of: {', '.join(PROVIDERS)}")
        model = a.get("model")
        if isinstance(model, str) and model.startswith("litellm/"):
            add("error", where, f"{path}.model", "a harness calls its model itself, not through this deployment's "
                'LiteLLM proxy: set harness.provider "litellm" and the provider\'s model id '
                '(e.g. gemini/gemini-2.5-pro)')
        if provider in ("openai", "gemini", "litellm") and not (isinstance(model, str) and model.strip()):
            add("error", where, f"{path}.model",
                f'harness.provider "{provider}" needs `model`: the provider\'s model id')
        if "apiKey" in block:
            if provider == "bedrock":
                add("error", where, f"{hp}.apiKey", "a Bedrock model needs no API key — remove it")
            elif isinstance(block["apiKey"], str):
                _api_key(block["apiKey"], identities, add, where, f"{hp}.apiKey")
        elif provider in ("openai", "gemini"):
            add("error", where, f"{hp}.apiKey", f'harness.provider "{provider}" needs `apiKey`: an apikey '
                "identity or an API key credential provider ARN")
        if "apiBase" in block:
            if provider != "litellm":
                add("error", where, f"{hp}.apiBase", 'only for harness.provider "litellm"')
            elif not public_https(block["apiBase"]):
                add("error", where, f"{hp}.apiBase", "must be an https:// URL on a public host")

        # --- tools
        bound = a.get("tool") if isinstance(a.get("tool"), list) else [a.get("tool")] if a.get("tool") else []
        for key in bound:
            t = tools.get(key) if isinstance(key, str) else None
            if _obj(t) and str(t.get("auth") or "").lower() in PERSON_AUTH:
                add("error", where, f"{path}.tool", f'"{key}" acts as the person who runs it, and a harness '
                    "calls the Gateway as this build's machine client — pick a tool that does not")
            if _obj(t) and t.get("source") == DOCGEN_SOURCE:
                add("error", where, f"{path}.tool", f'"{key}" is the Document generator, which saves each file '
                    "with the run the workflow sets on every call, and a harness calls the Gateway itself, "
                    "so no run is set — bind it to an agent that is not a harness")
            if isinstance(key, str) and len(f"@gateway/{key}___*") > 64:
                add("error", where, f"{path}.tool", f'"{key}" is too long for a harness: at most 51 characters')
        for name, pattern in (("browser", BROWSER_ARN_RE), ("codeInterpreter", CODE_ARN_RE)):
            v = block.get(name)
            if isinstance(v, str) and not pattern.match(v):
                add("error", where, f"{hp}.{name}", "must be true, false or a custom "
                    f"{'browser' if name == 'browser' else 'code interpreter'} ARN")
        mcp = block.get("mcp") if isinstance(block.get("mcp"), list) else []
        if len(mcp) > MAX_MCP:
            add("error", where, f"{hp}.mcp", f"at most {MAX_MCP} servers")
        seen: set = set()
        for i, m in enumerate(mcp):
            mp = f"{hp}.mcp[{i}]"
            if not _obj(m):
                add("error", where, mp, "must be {\"name\": ..., \"url\": \"https://...\"}")
                continue
            for k in m:
                if k not in ("name", "url"):
                    add("error", where, f"{mp}.{k}", "unknown key; allowed: name, url")
            name = m.get("name")
            if not isinstance(name, str) or not TOOL_NAME_RE.match(name):
                add("error", where, f"{mp}.name", "1-64 letters, digits, _ and -")
            elif name in RESERVED_TOOLS or name in seen:
                add("error", where, f"{mp}.name", f'"{name}" is taken — pick another name')
            seen.add(name)
            if not public_https(m.get("url")):
                add("error", where, f"{mp}.url", "must be an https:// URL on a public host")
        if block.get("shell") is True and (bound or "apiKey" in block):
            add("warning", where, f"{hp}.shell", "shell commands run in the session that holds this agent's "
                "Gateway token and model API key — turn shell off unless the agent needs it")

        # --- skills
        skills = block.get("skills") if isinstance(block.get("skills"), list) else []
        if len(skills) > MAX_SKILLS:
            add("error", where, f"{hp}.skills", f"at most {MAX_SKILLS} skills")
        for i, s in enumerate(skills):
            _skill(s, identities, add, where, f"{hp}.skills[{i}]")

        # --- memory
        mem = block.get("memory")
        if isinstance(mem, str):
            if not _obj(memories.get(mem)):
                add("error", where, f"{hp}.memory", f'"{mem}" is not one of the build\'s memories: '
                    f"{', '.join(memories) or '(none yet)'}")
            if _obj(core) and core.get("memory"):
                add("error", where, f"{hp}.memory", "harness.memory and agentcore.memory would both store this "
                    "agent's turns — keep one")

        # --- environment
        if isinstance(block.get("container"), str) and not CONTAINER_RE.match(block["container"]):
            add("error", where, f"{hp}.container", "must be an image URI: <account>.dkr.ecr.<region>.amazonaws.com/"
                "<repo>:<tag> or public.ecr.aws/<repo>:<tag>")
        net = block.get("network")
        if _obj(net):
            for k, pattern, what in (("subnetIds", SUBNET_RE, "subnet-"), ("securityGroupIds", SG_RE, "sg-")):
                v = net.get(k)
                if (not isinstance(v, list) or not 1 <= len(v) <= 16
                        or not all(isinstance(x, str) and pattern.match(x) for x in v)):
                    add("error", where, f"{hp}.network.{k}", f"1-16 ids, each {what}...")
            for k in net:
                if k not in ("subnetIds", "securityGroupIds"):
                    add("error", where, f"{hp}.network.{k}", "unknown key; allowed: subnetIds, securityGroupIds")
        mounts = block.get("mounts") if isinstance(block.get("mounts"), list) else []
        if len(mounts) > MAX_MOUNTS:
            add("error", where, f"{hp}.mounts", f"at most {MAX_MOUNTS} mounts")
        paths: set = set()
        for i, m in enumerate(mounts):
            mp = f"{hp}.mounts[{i}]"
            kinds = [k for k in ("session", "efs", "s3Files") if k in m] if _obj(m) else []
            if len(kinds) != 1:
                add("error", where, mp, 'set exactly one of "session", "efs", "s3Files"')
                continue
            kind = kinds[0]
            mount = m["session"] if kind == "session" else m.get("path")
            if not isinstance(mount, str) or not MOUNT_RE.match(mount):
                add("error", where, f"{mp}.{'session' if kind == 'session' else 'path'}",
                    "must be a path under /mnt, like /mnt/data")
            elif mount.rstrip("/") in paths:
                add("error", where, mp, f"{mount} is mounted twice")
            if isinstance(mount, str):
                paths.add(mount.rstrip("/"))
            if kind != "session":
                pattern = EFS_ARN_RE if kind == "efs" else S3FILES_ARN_RE
                if not isinstance(m.get(kind), str) or not pattern.match(m[kind]):
                    add("error", where, f"{mp}.{kind}",
                        f"must be an {'EFS' if kind == 'efs' else 'S3 Files'} access point ARN")
                if not _obj(net):
                    add("error", where, f"{mp}.{kind}", "EFS and S3 Files mounts need harness.network (a VPC)")


def _endpoint(block: dict, add, where, hp: str) -> None:
    v = block.get("endpoint")
    if isinstance(v, str) and (not ENDPOINT_RE.match(v) or v == "DEFAULT"):
        add("error", where, f"{hp}.endpoint",
            "1-48 letters, digits and underscores, starting with a letter, not DEFAULT")


def _skill(s, identities: dict, add, where, sp: str) -> None:
    kinds = [k for k in ("aws", "git", "s3") if k in s] if _obj(s) else []
    if len(kinds) != 1:
        add("error", where, sp, 'set exactly one of "aws", "git", "s3"')
        return
    kind = kinds[0]
    allowed = {"aws": ("aws",), "git": ("git", "path", "credential"), "s3": ("s3",)}[kind]
    for k in s:
        if k not in allowed:
            add("error", where, f"{sp}.{k}", f"unknown key; allowed: {', '.join(allowed)}")
    v = s[kind]
    if kind == "aws":
        if not isinstance(v, list) or not all(isinstance(p, str) and p and not p.startswith("/") and ".." not in p
                                              and not re.search(r"[?\[\]]", p) for p in v):
            add("error", where, f"{sp}.aws",
                'a list of catalog paths like "core-skills/*" ([] for all), relative, no ".."')
    elif kind == "git":
        if not isinstance(v, str) or not public_https(v) or "#" in v or "@" in v:
            add("error", where, f"{sp}.git", "must be the repository's https:// URL on a public host")
        if "credential" in s and isinstance(s["credential"], str):
            _api_key(s["credential"], identities, add, where, f"{sp}.credential")
    elif not isinstance(v, str) or not S3_URI_RE.match(v):
        add("error", where, f"{sp}.s3", "must be s3://<bucket>/<prefix>/")
