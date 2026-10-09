"""The Build view's validation, on the server — so a deploy cannot skip it.

A line-for-line port of web/src/builder/validate.ts, driven by the same files: the key
spec (app/keys.json) and the value sets (app/vocabulary.json). The page runs it as you
type; the BFF runs THIS before it freezes a version for deploy, so a build that the page
marks with an error — or one sent straight to the API — is refused with the same message
instead of reaching CodeBuild.

The two are held to each other by tests/fixtures/validation_cases.json: the same
workflows, the same expected (severity, path) pairs, asserted by pytest here and by
vitest there. Change a rule in one and the other's test fails until they agree.
"""

from __future__ import annotations

import json
import math
import re
from pathlib import Path

import cedar

_HERE = Path(__file__).resolve().parent


def _load(name: str) -> dict:
    path = _HERE / name
    if not path.exists():  # a source checkout, where nothing has staged a copy
        path = _HERE.parent / "app" / name
    return json.loads(path.read_text())


KEYS = {k: v for k, v in _load("keys.json").items() if not k.startswith("$")}
VOCAB = _load("vocabulary.json")

TOP_LEVEL = ["$schema", "$comment", "orchestrator", "ui", "guardrail", "authorization",
             "tools", "agents", "steps"]
LAMBDA_ARN_RE = re.compile(
    r"^arn:aws[a-z-]*:lambda:[a-z0-9-]+:[0-9]{12}:function:[a-zA-Z0-9-_]+(:[a-zA-Z0-9-_$]+)?$")
S3_URI_RE = re.compile(r"^s3://[a-z0-9.-]{3,63}/.+")
#: The one tool name the framework gives a target of these types (cdk/lib/tool-plane.ts):
#: the web search connector's, and the knowledge-base function's.
_FIXED_TOOL_NAMES = {"websearch": "WebSearch", "kb": "retrieve"}
#: A kb tool's own documents: s3://bucket or s3://bucket/prefix (the prefix optional).
S3_LOCATION_RE = re.compile(r"^s3://[a-z0-9][a-z0-9.-]{1,61}[a-z0-9](/.*)?$")
KB_ID_RE = re.compile(r"^[0-9A-Z]{10}$")
KMS_KEY_ARN_RE = re.compile(r"^arn:aws[a-z-]*:kms:[a-z0-9-]+:[0-9]{12}:key/[A-Za-z0-9-]+$")
REST_API_ID_RE = re.compile(r"^[a-z0-9]{10}$")
STAGE_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
TOOL_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{0,63}$")
HTTPS_RE = re.compile(r"^https://\S+\.\S+")
OPENAPI_OPS = ("get", "put", "post", "delete", "options", "head", "patch", "trace")
AGENT_ID_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]*$")
TOOL_KEY_RE = re.compile(r"^[A-Za-z][A-Za-z0-9]*$")
BRANCH_OPS = ["equals", "notEquals", "in", "contains", "exists", "gt", "gte", "lt", "lte"]


# --- the JS semantics the TypeScript relies on ------------------------------------------

def _is_obj(v) -> bool:
    return isinstance(v, dict)


def _truthy(v) -> bool:
    return v is not None and v != "" and v is not False and not (isinstance(v, list) and not v)


def _jst(v) -> bool:
    """JavaScript truthiness: unlike Python, an empty list or object is truthy."""
    if v is None or v is False or v == "":
        return False
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return v != 0 and not math.isnan(v)
    return True


def _num(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


def _type_ok(v, t) -> bool:
    if isinstance(t, list):
        return any(_type_ok(v, x) for x in t)
    if t is None:
        return True
    if t == "string":
        return isinstance(v, str)
    if t == "number":
        return _num(v)
    if t == "integer":
        return _num(v) and float(v).is_integer()
    if t == "boolean":
        return isinstance(v, bool)
    if t == "array":
        return isinstance(v, list)
    if t == "object":
        return _is_obj(v)
    return True


def _js(v) -> str:
    """String(v), as the messages in validate.ts print a value."""
    if v is True:
        return "true"
    if v is False:
        return "false"
    if v is None:
        return "null"
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    if isinstance(v, list):
        return ",".join("" if x is None else _js(x) for x in v)
    if isinstance(v, dict):
        return "[object Object]"
    return str(v)


def vocab_extra(name: str, field: str):
    """A vocabulary's extra field (e.g. builtinEvaluators.unsupported), or None."""
    return (VOCAB.get(name) or {}).get(field)


def vocab(name: str) -> list[str]:
    return list((VOCAB.get(name) or {}).get("values") or [])


def block(name: str) -> dict:
    return KEYS[name]


def variant_of(name: str, entry: dict) -> str:
    spec = block(name)
    key = spec.get("$variantKey")
    if not key:
        return ""
    v = entry.get(key)
    return v.lower() if isinstance(v, str) and v else str(spec.get("$variantDefault") or "")


def _expand(scope, aliases: dict) -> list[str]:
    if not scope or scope == "*":
        return []
    out = []
    for s in scope:
        out += aliases.get(s, [s])
    return out


def applies(name: str, key: str, variant: str) -> bool:
    spec = block(name)
    k = spec["keys"].get(key)
    if not k:
        return False
    if not spec.get("$variantKey") or k.get("appliesTo") in (None, "*"):
        return True
    return variant in _expand(k["appliesTo"], spec.get("$variantAliases") or {})


def required_for(name: str, key: str, variant: str) -> bool:
    spec = block(name)
    k = spec["keys"].get(key)
    if not k:
        return False
    if k.get("required"):
        return True
    return variant in _expand(k.get("requiredFor"), spec.get("$variantAliases") or {})


def allowed_values(name: str, key: str, variant: str) -> list[str] | None:
    k = block(name)["keys"].get(key)
    if not k:
        return None
    per = (k.get("vocabularyFor") or {}).get(variant)
    if per:
        return vocab(per)
    if k.get("vocabulary"):
        return vocab(k["vocabulary"])
    if k.get("enum"):
        return [_js(x) for x in k["enum"]]
    return None


def tools_of(value) -> list[str]:
    if not value:
        return []
    if isinstance(value, str):
        return [value]
    return [t for t in value if isinstance(t, str) and t] if isinstance(value, list) else []


def step_agents(step: dict) -> list[str]:
    if _jst(step.get("parallel")):
        return list(step["parallel"])
    if _jst(step.get("sequence")):
        return list(step["sequence"])
    return [step["agent"]] if step.get("agent") else []


def upstream_ids(steps: list, agent_id: str) -> list[str]:
    """The agents that run before `agent_id`: every earlier step's, and the ones before
    it in its own `sequence` (a `parallel` group's peers run at the same time). Mirrors
    app/common/config.upstream_of."""
    seen: list[str] = []
    for step in steps:
        ids = step_agents(step) if _is_obj(step) else []
        if agent_id in ids:
            if _jst(step.get("sequence")):
                seen.extend(ids[: ids.index(agent_id)])
            return seen
        seen.extend(ids)
    return seen


def _check_vision(a: dict, aid: str, agents: dict, steps: list, where: dict, path: str, add) -> None:
    """`vision`: the agents named must run earlier, and draw images."""
    v = a.get("vision") if _is_obj(a.get("vision")) else None
    if v is None:
        return
    sources = v.get("from")
    if not isinstance(sources, list) or not sources:
        add("error", where, f"{path}.vision.from", "name at least one earlier agent whose images this agent reads")
        sources = []
    before = upstream_ids(steps, aid)
    for src in sources:
        if not isinstance(src, str) or src not in agents:
            add("error", where, f"{path}.vision.from", f'"{_js(src)}" is not an agent in this workflow')
        elif src == aid:
            add("error", where, f"{path}.vision.from", "an agent cannot read its own images")
        elif src not in before:
            add("error", where, f"{path}.vision.from",
                f'"{src}" does not run before {aid}, so it has no images yet when {aid} runs')
        elif not _is_obj(agents[src]) or agents[src].get("output") != "image":
            add("warning", where, f"{path}.vision.from",
                f'"{src}" is not an image agent (output "image"): only images its output lists are read')
    n = v.get("maxImages")
    if "maxImages" in v and not (_num(n) and float(n).is_integer() and 1 <= n <= 20):
        add("error", where, f"{path}.vision.maxImages", "must be a whole number from 1 to 20")


def step_name(step: dict, index: int) -> str:
    return step.get("agent") or step.get("gateId") or f"group{index}"


# --- shape ------------------------------------------------------------------------------

def _check_entry(name: str, entry, path: str, where: dict, out: list) -> None:
    def err(p, message, severity="error"):
        out.append({"severity": severity, "where": where, "path": p, "message": message})

    if not _is_obj(entry):
        err(path, "must be an object")
        return
    spec = block(name)
    variant = variant_of(name, entry)
    vkey = spec.get("$variantKey")
    variant_label = f'{vkey} "{variant}"' if vkey else ""

    if vkey and isinstance(entry.get(vkey), str):
        allowed = allowed_values(name, vkey, variant)
        if allowed and entry[vkey].lower() not in allowed:
            err(f"{path}.{vkey}", f'"{entry[vkey]}" is not one of: {", ".join(allowed)}')
            return

    keys = spec["keys"]
    dotted = [k for k in keys if "." in k]
    parents = {k.split(".")[0] for k in dotted}

    def check_value(key, value, p):
        k = keys[key]
        if not applies(name, key, variant):
            err(p, f"`{key.split('.')[-1]}` does not apply to {variant_label or 'this entry'} — "
                   "a key that looks like a setting and controls nothing is rejected, not ignored")
            return
        if not _type_ok(value, k.get("type")):
            t = " or ".join(k["type"]) if isinstance(k.get("type"), list) else (k.get("type") or "")
            article = "n" if re.match(r"^[aeiou]", t) else ""
            err(p, f"must be {'a whole number' if t == 'integer' else f'a{article} {t}'}")
            return
        allowed = allowed_values(name, key, variant)
        pattern = k.get("pattern")
        if isinstance(value, list):
            for i, item in enumerate(value):
                items = k.get("items")
                if items and items != "object" and not _type_ok(item, items):
                    err(f"{p}[{i}]", f"must be a {items}")
                if allowed and isinstance(item, str) and item not in allowed:
                    err(f"{p}[{i}]", f'"{item}" is not one of: {", ".join(allowed)}')
                if pattern and isinstance(item, str) and not re.search(pattern, item):
                    err(f"{p}[{i}]", f'"{item}" does not match {pattern}')
                if k.get("itemRequired") and _is_obj(item):
                    for f in k["itemRequired"]:
                        if not _truthy(item.get(f)):
                            err(f"{p}[{i}]", f"each entry needs `{f}`")
        elif allowed and isinstance(value, str) and value not in allowed:
            err(p, f'"{value}" is not one of: {", ".join(allowed)}')
        if pattern and isinstance(value, str) and not re.search(pattern, value):
            err(p, f'"{value}" does not match {pattern}')
        if _is_obj(value):
            if k.get("valueVocabulary"):
                ok = vocab(k["valueVocabulary"])
                for mk, mv in value.items():
                    if _js(mv) not in ok:
                        err(f"{p}.{mk}", f'"{_js(mv)}" is not one of: {", ".join(ok)}')
            if k.get("keyVocabulary"):
                ok = vocab(k["keyVocabulary"])
                for mk in value:
                    if mk not in ok:
                        err(f"{p}.{mk}", f'"{mk}" is not one of: {", ".join(ok)}')
            if k.get("properties"):
                for sk, sv in value.items():
                    sub = k["properties"].get(sk)
                    if not sub:
                        err(f"{p}.{sk}", f"unknown key; allowed: {', '.join(k['properties'])}")
                    elif not _type_ok(sv, sub.get("type")):
                        st = sub.get("type") or "string"
                        article = "n" if st in ("array", "object", "integer") else ""
                        err(f"{p}.{sk}", f"must be a{article} {st}")

    for key, value in entry.items():
        p = f"{path}.{key}"
        if key in keys:
            check_value(key, value, p)
        elif key in parents:
            if not _is_obj(value):
                err(p, "must be an object")
                continue
            for sub, sv in value.items():
                full = f"{key}.{sub}"
                if full not in keys:
                    legal = [d.split(".")[1] for d in dotted if d.startswith(f"{key}.")]
                    err(f"{p}.{sub}", f"unknown key; {key} takes: {', '.join(legal)}")
                else:
                    check_value(full, sv, f"{p}.{sub}")
        elif (spec.get("$removed") or {}).get(key):
            err(p, spec["$removed"][key])
        else:
            err(p, f"unknown key — nothing in the framework reads `{key}`")

    for key in keys:
        if "." in key:
            continue
        if required_for(name, key, variant) and key not in entry:
            err(f"{path}.{key}", f"`{key}` is required{f' for {variant_label}' if variant_label else ''}")

    for rule in spec.get("$exactlyOne") or []:
        if rule["appliesTo"] != "*" and variant not in rule["appliesTo"]:
            continue
        found = [k for k in rule["keys"] if _truthy(entry.get(k))]
        if len(found) != 1:
            names = " / ".join(f"`{k}`" for k in rule["keys"])
            found_label = f" (found {' and '.join(found)})" if found else ""
            err(path, f"set exactly one of {names}{found_label}. {rule['why']}")

    if name == "agent" and "agentcore" in entry:
        _check_entry("agentcore", entry["agentcore"], f"{path}.agentcore", where, out)


# --- meaning ----------------------------------------------------------------------------

def _check_branch(steps: list, i: int, out: list) -> None:
    step = steps[i]
    where = {"kind": "step", "index": i}
    path = f"steps[{i}].branch"

    def err(message, p=path):
        out.append({"severity": "error", "where": where, "path": p, "message": message})

    if "branch" not in step:
        return
    b = step["branch"]
    if _jst(step.get("parallel")):
        err("a parallel stage cannot branch — no single agent's output decides. Branch on a stage after it.")
    if i == len(steps) - 1:
        err("the last stage cannot branch — there is nowhere left to route.")
    if not _is_obj(b):
        err("must be an object with `when` and/or `default`")
        return
    for k in b:
        if k not in ("when", "default"):
            err(f"unknown key `{k}`; a branch takes `when` and `default`")
    has_default = isinstance(b.get("default"), str) and b["default"].strip() != ""
    if "when" not in b and not has_default:
        err("needs `when` rules, a `default`, or both")
    if "default" in b and not has_default:
        err("`default` must name a later stage or END", f"{path}.default")

    names = [step_name(s, n) for n, s in enumerate(steps)]

    def target(t, p):
        if not isinstance(t, str) or not t.strip() or t == "END":
            return
        if t not in names:
            later = ", ".join(names[i + 1:]) or "(none)"
            err(f'"{t}" is not a stage. Targets are END or a later stage\'s name: {later}', p)
        elif names.index(t) <= i:
            err(f'"{t}" is not AFTER this stage — a branch may only skip forward, never loop back', p)

    if has_default:
        target(b["default"], f"{path}.default")

    if "when" in b:
        when = b["when"]
        if not isinstance(when, list) or not when:
            err("`when` must be a non-empty list of rules", f"{path}.when")
            return
        for r, rule in enumerate(when):
            p = f"{path}.when[{r}]"
            if not _is_obj(rule):
                err("each rule must be an object", p)
                continue
            for k in rule:
                if k not in ["field", "goto", *BRANCH_OPS]:
                    err(f"unknown key `{k}`; operators are {', '.join(BRANCH_OPS)}", p)
            if not isinstance(rule.get("goto"), str) or not rule["goto"].strip():
                err("each rule needs a `goto`", p)
            ops = [o for o in BRANCH_OPS if o in rule]
            if not ops:
                err(f"each rule needs an operator: {', '.join(BRANCH_OPS)}", p)
            for o in ops:
                v = rule[o]
                if o == "in" and not isinstance(v, list):
                    err("`in` takes a list", f"{p}.in")
                if o == "exists" and not isinstance(v, bool):
                    err("`exists` takes true or false", f"{p}.exists")
                if o in ("gt", "gte", "lt", "lte"):
                    ok = _num(v)
                    if isinstance(v, str) and v.strip():
                        try:
                            ok = math.isfinite(float(v))
                        except ValueError:
                            ok = False
                    if not ok:
                        err(f"`{o}` takes a number", f"{p}.{o}")
                if o in ("equals", "notEquals", "contains") and (isinstance(v, list) or _is_obj(v)):
                    err(f"`{o}` takes a single value, not a list or object", f"{p}.{o}")
            target(rule.get("goto"), f"{p}.goto")


CUSTOM_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9]{0,31}$")
POLICY_KEYS = ("name", "description", "statement", "libraryId")
CODE_GRANTS = ("secret", "table", "tableAccess", "s3Prefix", "s3Access", "vpc")
SECRET_NAME_RE = re.compile(r"^[A-Za-z0-9/_+=.@-]{1,512}$")
TABLE_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]{3,255}$")
S3_PREFIX_RE = re.compile(r"^s3://[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]/([^*?]+/)?$")
SUBNET_RE = re.compile(r"^subnet-[0-9a-f]{8,17}$")
SG_RE = re.compile(r"^sg-[0-9a-f]{8,17}$")
ENV_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")
#: ToolLambda-<agentName>-<key> within Lambda's 64: ax_xxxxxxxx leaves 41 for the key.
CODE_KEY_MAX = 41
#: What the framework itself keeps: a code tool may never be granted it, in any account
#: (the IaC's permissions boundary denies it too). Builds, users' activity, other builds'
#: run tables, the vaulted keys, and the framework's buckets.
FRAMEWORK_TABLE_RE = re.compile(r"^ax_|_(builds|audit|status|events|telemetry|insights)$")
FRAMEWORK_SECRET_RE = re.compile(r"^(agentexpress/|bedrock-agentcore)")
FRAMEWORK_BUCKET_RE = re.compile(r"^s3://(agentcore-|[^/]*builderbuildsbucket)")


def _int_in(v, lo: int, hi: int) -> bool:
    return isinstance(v, int) and not isinstance(v, bool) and lo <= v <= hi


def _check_code(key: str, code, path: str, where: dict, add) -> None:
    """tools.<key>.code: a function written in the build. Mirrors validate.ts checkCode."""
    p = f"{path}.code"
    if len(key) > CODE_KEY_MAX:
        add("error", where, path, f"a code tool's key is at most {CODE_KEY_MAX} characters: it names "
            "the function ToolLambda-<agentName>-<key>")
    if not _is_obj(code):
        return
    grants = code.get("grants")
    if grants is not None and not _is_obj(grants):
        add("error", where, f"{p}.grants", "must be an object of grants: " + ", ".join(CODE_GRANTS))
    grants = grants if _is_obj(grants) else {}
    for k in grants:
        if k not in CODE_GRANTS:
            add("error", where, f"{p}.grants.{k}", f"unknown grant; a code tool may have: {', '.join(CODE_GRANTS)}")
    if "secret" in grants and not (isinstance(grants["secret"], str) and SECRET_NAME_RE.match(grants["secret"])):
        add("error", where, f"{p}.grants.secret", "must be the name of a Secrets Manager secret in the build's account")
    elif "secret" in grants and FRAMEWORK_SECRET_RE.match(grants["secret"]):
        add("error", where, f"{p}.grants.secret", "is one of the framework's own secrets, which no tool may read")
    if "table" in grants and not (isinstance(grants["table"], str) and TABLE_NAME_RE.match(grants["table"])):
        add("error", where, f"{p}.grants.table", "must be the name of a DynamoDB table in the build's account")
    elif "table" in grants and FRAMEWORK_TABLE_RE.search(grants["table"]):
        add("error", where, f"{p}.grants.table",
            "is named like one of the framework's own tables, which no tool may reach")
    if "s3Prefix" in grants and not (isinstance(grants["s3Prefix"], str) and S3_PREFIX_RE.match(grants["s3Prefix"])):
        add("error", where, f"{p}.grants.s3Prefix", "must be s3://<bucket>/ or s3://<bucket>/<prefix>/, ending in /")
    elif "s3Prefix" in grants and FRAMEWORK_BUCKET_RE.match(grants["s3Prefix"]):
        add("error", where, f"{p}.grants.s3Prefix", "is one of the framework's own buckets, which no tool may reach")
    access = vocab("codeGrantAccess")
    for k, needs in (("tableAccess", "table"), ("s3Access", "s3Prefix")):
        if k not in grants:
            continue
        if grants[k] not in access:
            add("error", where, f"{p}.grants.{k}", f"must be one of: {', '.join(access)}")
        elif needs not in grants:
            add("error", where, f"{p}.grants.{k}", f"only applies with {needs}")
    if "vpc" in grants:
        vpc = grants["vpc"] if _is_obj(grants["vpc"]) else {}
        subnets, groups = vpc.get("subnetIds"), vpc.get("securityGroupIds")
        if (not isinstance(subnets, list) or not subnets
                or not all(isinstance(s, str) and SUBNET_RE.match(s) for s in subnets)):
            add("error", where, f"{p}.grants.vpc.subnetIds", "must list one or more subnet ids, subnet-...")
        if (not isinstance(groups, list) or not groups
                or not all(isinstance(s, str) and SG_RE.match(s) for s in groups)):
            add("error", where, f"{p}.grants.vpc.securityGroupIds", "must list one or more security group ids, sg-...")
    if "timeoutSeconds" in code and not _int_in(code["timeoutSeconds"], 1, 300):
        add("error", where, f"{p}.timeoutSeconds", "must be a whole number of seconds, 1 to 300")
    if "memoryMB" in code and not _int_in(code["memoryMB"], 128, 10240):
        add("error", where, f"{p}.memoryMB", "must be a whole number of MB, 128 to 10240")
    env = code.get("environment")
    for name, value in (env.items() if _is_obj(env) else []):
        if not ENV_NAME_RE.match(name) or name.upper().startswith("AWS_") or name in (
                "SECRET_NAME", "TABLE_NAME", "S3_PREFIX"):
            add("error", where, f"{p}.environment.{name}",
                "is not a name you can set: letters, digits and _, not AWS_..., SECRET_NAME, TABLE_NAME or S3_PREFIX")
        elif not isinstance(value, str) or len(value) > 1000:
            add("error", where, f"{p}.environment.{name}", "must be a string of at most 1000 characters")


#: An S3 location runs may name: a bucket, or bucket/prefix (no scheme, no trailing slash).
ATTACH_S3_RE = re.compile(r"^[a-z0-9][a-z0-9.\-]{1,61}[a-z0-9](/[^\s/][^\s]*[^\s/]|/[^\s/])?$")


AFTER_RE = re.compile(r"^([1-9][0-9]*)(m|h|d)$")
GATE_KEYS = ("mode", "when", "approval", "timeout")


def _check_hitl(step: dict, i: int, add) -> None:
    """steps[].hitl as an object: how its gate decides (app/common/gates.py). true and
    false need nothing. Mirrors validate.ts checkHitl."""
    h = step.get("hitl")
    if not _is_obj(h):
        return
    where = {"kind": "step", "index": i}
    p = f"steps[{i}].hitl"
    for k in h:
        if k not in GATE_KEYS:
            add("error", where, f"{p}.{k}", f"unknown key; a gate takes {', '.join(GATE_KEYS)}")
    mode = h.get("mode", "always")
    if "mode" in h and mode not in vocab("gateModes"):
        add("error", where, f"{p}.mode", f"must be one of: {', '.join(vocab('gateModes'))}")
    when = h.get("when")
    if mode == "threshold" and not (isinstance(when, list) and when):
        add("error", where, f"{p}.when", "a threshold gate needs `when`: the rules that call for a person")
    elif "when" in h and not (isinstance(when, list) and when):
        add("error", where, f"{p}.when", "must be a non-empty list of rules")
    for r, rule in enumerate(when if isinstance(when, list) else []):
        rp = f"{p}.when[{r}]"
        if not _is_obj(rule):
            add("error", where, rp, "each rule must be an object")
            continue
        for k in rule:
            if k not in ["field", *BRANCH_OPS]:
                add("error", where, rp,
                    f"unknown key `{k}`; a rule is a `field` and operators: {', '.join(BRANCH_OPS)}")
        ops = [o for o in BRANCH_OPS if o in rule]
        if not ops:
            add("error", where, rp, f"each rule needs an operator: {', '.join(BRANCH_OPS)}")
        for o in ops:
            v = rule[o]
            if o == "in" and not isinstance(v, list):
                add("error", where, f"{rp}.in", "`in` takes a list")
            if o == "exists" and not isinstance(v, bool):
                add("error", where, f"{rp}.exists", "`exists` takes true or false")
            if o in ("gt", "gte", "lt", "lte") and not _num(v):
                add("error", where, f"{rp}.{o}", f"`{o}` takes a number")
            if o in ("equals", "notEquals", "contains") and (isinstance(v, list) or _is_obj(v)):
                add("error", where, f"{rp}.{o}", f"`{o}` takes a single value, not a list or object")
    if isinstance(when, list) and when and mode != "threshold":
        add("warning", where, f"{p}.when", "only a threshold gate reads `when`")
    if "approval" in h and h["approval"] not in vocab("gateApprovals"):
        add("error", where, f"{p}.approval", f"must be one of: {', '.join(vocab('gateApprovals'))}")
    if "timeout" in h:
        t = h["timeout"]
        if not _is_obj(t):
            add("error", where, f"{p}.timeout", 'must be {"after": "24h", "action": "approve" | "deny"}')
        else:
            for k in t:
                if k not in ("after", "action"):
                    add("error", where, f"{p}.timeout.{k}", "unknown key; a timeout takes after, action")
            m = AFTER_RE.match(t["after"]) if isinstance(t.get("after"), str) else None
            if not m:
                add("error", where, f"{p}.timeout.after", "must be <n>m, <n>h or <n>d, e.g. 30m, 24h, 2d")
            elif int(m.group(1)) * {"m": 60, "h": 3600, "d": 86400}[m.group(2)] > 30 * 86400:
                add("error", where, f"{p}.timeout.after", "is at most 30d")
            if t.get("action") not in vocab("gateTimeoutActions"):
                add("error", where, f"{p}.timeout.action", f"must be one of: {', '.join(vocab('gateTimeoutActions'))}")
    if mode == "auto":
        add("warning", where, f"{p}.mode", "this gate approves itself: no one reviews this stage")


def _check_attachments(orch, agents: dict, add) -> None:
    """orchestrator.attachments.s3: where a run's request may point, and that some agent
    reads what it brings. Mirrors validate.ts checkAttachments."""
    att = orch.get("attachments") if _is_obj(orch) and _is_obj(orch.get("attachments")) else {}
    where = {"kind": "block", "name": "orchestrator"}
    s3 = att.get("s3")
    for i, loc in enumerate(s3 if isinstance(s3, list) else []):
        p = f"orchestrator.attachments.s3[{i}]"
        if not (isinstance(loc, str) and ATTACH_S3_RE.match(loc)):
            add("error", where, p, 'must be a bucket, or bucket/folder, without s3:// — e.g. "my-docs/contracts"')
        elif FRAMEWORK_BUCKET_RE.match("s3://" + loc):
            add("error", where, p, "is one of the framework's own buckets, which a run may not read")
    readers = [aid for aid, a in agents.items() if _is_obj(a) and a.get("attachments") is True]
    if isinstance(s3, list) and s3 and not readers:
        add("warning", where, "orchestrator.attachments.s3",
            "no agent reads a run's files: set `attachments` on the agents that should")


INTERCEPTOR_KEYS = ("code", "lambdaArn", "passRequestHeaders", "templates")
#: Each template's settings, and what they hold. Mirrors INTERCEPTOR_SETTINGS in validate.ts.
INTERCEPTOR_SETTINGS = {
    "audit": (), "custom": (), "blockTools": ("tools", "agents"),
    "argumentGuard": ("denyPatterns", "maxArgumentChars"), "injectContext": ("arguments",),
    "redactPii": ("types", "mask"), "hideTools": ("tools",), "capResult": ("maxChars",),
}
ARG_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")


def _strings(v) -> bool:
    return isinstance(v, list) and all(isinstance(s, str) and s for s in v)


def _check_tool_names(names, tools: dict, p: str, where: dict, add) -> None:
    """Gateway tool names a template refers to: "<toolKey>" or "<toolKey>___<toolName>"."""
    for i, n in enumerate(names):
        if n.split("___")[0] not in tools:
            add("warning", where, f"{p}[{i}]", f'"{n}" is not a tool in this build: write "<toolKey>" or '
                '"<toolKey>___<toolName>"')


def _check_template(point: str, name: str, s: dict, ic: dict, tools: dict, agents: dict,
                    p: str, where: dict, add) -> None:
    for k in s:
        if k not in INTERCEPTOR_SETTINGS[name]:
            allowed = ", ".join(INTERCEPTOR_SETTINGS[name]) or "nothing"
            add("error", where, f"{p}.{k}", f"unknown setting; {name} takes: {allowed}")
    headers = ic.get("passRequestHeaders") is True
    if name in ("blockTools", "hideTools"):
        if not (_strings(s.get("tools")) and s["tools"]):
            add("error", where, f"{p}.tools", 'name the tools: "<toolKey>" or "<toolKey>___<toolName>"')
        else:
            _check_tool_names(s["tools"], tools, f"{p}.tools", where, add)
    if name == "blockTools" and "agents" in s:
        if not _strings(s["agents"]):
            add("error", where, f"{p}.agents", "must list agent ids")
        else:
            for i, a in enumerate(s["agents"]):
                if a not in agents:
                    add("warning", where, f"{p}.agents[{i}]", f'"{a}" is not an agent in this workflow')
            if s["agents"] and not headers:
                add("warning", where, f"{p}.agents", "needs passRequestHeaders: true — the calling agent "
                    "arrives as the x-ax-agent header, and without headers every call is refused")
    if name == "argumentGuard":
        if "denyPatterns" in s and not _strings(s["denyPatterns"]):
            add("error", where, f"{p}.denyPatterns", "must list regular expressions")
        if "maxArgumentChars" in s and not _int_in(s["maxArgumentChars"], 1, 1_000_000):
            add("error", where, f"{p}.maxArgumentChars", "must be a whole number, 1 to 1000000")
    if name == "injectContext":
        args = s.get("arguments")
        sources = vocab("interceptorContextSources")
        if not (_is_obj(args) and args):
            add("error", where, f"{p}.arguments", "map each argument to set to where its value comes from: "
                + ", ".join(sources))
        else:
            for arg, src in args.items():
                if not ARG_NAME_RE.match(arg):
                    add("error", where, f"{p}.arguments.{arg}", "is not an argument name")
                elif src not in sources:
                    add("error", where, f"{p}.arguments.{arg}", f"must be one of: {', '.join(sources)}")
            if not headers:
                add("warning", where, f"{p}.arguments", "needs passRequestHeaders: true — the run's session, "
                    "agent and user arrive as x-ax-* headers")
    if name == "redactPii":
        types = vocab("interceptorPiiTypes")
        if "types" in s and not (_strings(s["types"]) and s["types"] and all(t in types for t in s["types"])):
            add("error", where, f"{p}.types", f"must list one or more of: {', '.join(types)}")
        if "mask" in s and not (isinstance(s["mask"], str) and len(s["mask"]) <= 40):
            add("error", where, f"{p}.mask", "must be text of at most 40 characters")
    if name == "capResult" and "maxChars" in s and not _int_in(s["maxChars"], 100, 1_000_000):
        add("error", where, f"{p}.maxChars", "must be a whole number, 100 to 1000000")


def _check_interceptors(orch, tools: dict, agents: dict, add) -> None:
    """orchestrator.interceptors: a Gateway Lambda before each request and after each
    response, written here (`code`) or yours (`lambdaArn`). Mirrors validate.ts
    checkInterceptors."""
    ics = orch.get("interceptors") if _is_obj(orch) and _is_obj(orch.get("interceptors")) else {}
    where = {"kind": "block", "name": "orchestrator"}
    if any(_is_obj(ics.get(pt)) for pt in ("request", "response")) and not tools:
        add("warning", where, "orchestrator.interceptors",
            "the build has no tools, so it deploys no Gateway for these to run on")
    for point in ("request", "response"):
        ic = ics.get(point)
        if not _is_obj(ic):
            continue
        p = f"orchestrator.interceptors.{point}"
        for k in ic:
            if k not in INTERCEPTOR_KEYS:
                add("error", where, f"{p}.{k}", f"unknown key; an interceptor takes: {', '.join(INTERCEPTOR_KEYS)}")
        has_code, has_arn = "code" in ic, "lambdaArn" in ic
        if has_code == has_arn:
            add("error", where, p, 'needs exactly one of "code" (a function written in this build) or '
                '"lambdaArn" (a function you own)')
        if has_code:
            if not _is_obj(ic["code"]):
                add("error", where, f"{p}.code", "must be an object: {} or its grants, timeoutSeconds, "
                    "memoryMB and environment")
            else:
                _check_code(f"interceptor-{point}", ic["code"], p, where, add)
        if has_arn and not (isinstance(ic["lambdaArn"], str) and LAMBDA_ARN_RE.match(ic["lambdaArn"])):
            add("error", where, f"{p}.lambdaArn", "must be a Lambda function ARN: "
                "arn:aws:lambda:<region>:<account>:function:<name>")
        if "passRequestHeaders" in ic and not isinstance(ic["passRequestHeaders"], bool):
            add("error", where, f"{p}.passRequestHeaders", "must be true or false")
        if "templates" not in ic:
            continue
        templates = ic["templates"]
        if not _is_obj(templates):
            add("error", where, f"{p}.templates", "must be an object: template name -> its settings")
            continue
        if has_arn and not has_code and templates:
            add("warning", where, f"{p}.templates", "only shape code written here: a lambdaArn function "
                "runs as you deployed it")
        known = vocab("interceptorRequestTemplates" if point == "request" else "interceptorResponseTemplates")
        for name, s in templates.items():
            tp = f"{p}.templates.{name}"
            if name not in known:
                add("error", where, tp, f"not a {point} template; one of: {', '.join(known)}")
            elif not _is_obj(s):
                add("error", where, tp, "its settings must be an object ({} for none)")
            else:
                _check_template(point, name, s, ic, tools, agents, tp, where, add)


TRIGGER_COMMON = ("type", "description", "prompt", "enabled", "runAs", "approvers", "gates", "maxRunsPerHour",
                  "attachPayload", "idempotencyKey")
#: What each trigger type adds. Mirrors TRIGGER_TYPE_KEYS in validate.ts.
TRIGGER_TYPE_KEYS = {"webhook": ("signature",), "schedule": ("expression", "timezone"),
                     "eventbridge": ("pattern", "bus"), "s3": ("bucket", "prefix"), "sqs": ("queueArn",)}
TRIGGER_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9]{0,31}$")
SCHEDULE_RE = re.compile(r"^(cron\(\S+( \S+){5}\)|rate\([1-9][0-9]* (minute|minutes|hour|hours|day|days)\))$")
TIMEZONE_RE = re.compile(r"^[A-Za-z]+(/[A-Za-z0-9_+\-]+){0,2}$")
BUS_RE = re.compile(r"^(arn:aws[a-z-]*:events:[a-z0-9-]+:[0-9]{12}:event-bus/)?[A-Za-z0-9._\-/]{1,256}$")
BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9.\-]{1,61}[a-z0-9]$")
SQS_ARN_RE = re.compile(r"^arn:aws[a-z-]*:sqs:[a-z0-9-]+:[0-9]{12}:[A-Za-z0-9_.\-]{1,80}$")


def _s3_attachable(orch, bucket: str, prefix: str) -> bool:
    """Whether orchestrator.attachments.s3 lets a run read objects under bucket/prefix."""
    att = orch.get("attachments") if _is_obj(orch) and _is_obj(orch.get("attachments")) else {}
    for e in att.get("s3") if isinstance(att.get("s3"), list) else []:
        e = str(e).strip("/")
        if e == bucket:
            return True
        if e.startswith(f"{bucket}/") and prefix and f"{prefix.rstrip('/')}/".startswith(f"{e[len(bucket) + 1:]}/"):
            return True
    return False


#: Tool auth modes that get a token from an OAuth provider (tools.<key>.oauth): the app's
#: own client (oauth2), each person's own account (user, 3LO), the person's sign-in (obo).
OAUTH_MODES = ("oauth2", "user", "obo")
#: ...and the two of them that act as the person: such a tool is reached through the
#: person Gateway, with the caller's own sign-in (bff/handler.py, gateway/client.py).
PERSON_MODES = ("user", "obo")

#: What each sign-in provider needs (authorization.signIn), and what it does not take.
SIGN_IN_FIELDS = {"cognito": [], "okta": ["domain", "clientId"], "auth0": ["domain", "clientId"],
                  "entra": ["tenantId", "clientId"]}
SIGN_IN_OPTIONAL = {"okta": ["authorizationServer"]}
SIGN_IN_LABEL = {"domain": "the org's domain", "clientId": "the app's client ID", "tenantId": "the tenant ID"}
GROUPS_CLAIM_FOR = {"okta": "groups", "entra": "roles"}


def _check_sign_in(auth, add) -> None:
    """authorization.signIn: the fields the chosen provider needs, and none it would ignore.
    Mirrors validate.ts checkSignIn."""
    s = auth.get("signIn") if _is_obj(auth) and _is_obj(auth.get("signIn")) else None
    if s is None:
        return
    where = {"kind": "block", "name": "authorization"}
    keys = block("authorization")["keys"]
    provider = s["provider"] if s.get("provider") else keys["signIn.provider"]["default"]
    if provider not in SIGN_IN_FIELDS:
        return                      # the vocabulary check has said so
    need = SIGN_IN_FIELDS[provider]
    allowed = {"provider", *need, *SIGN_IN_OPTIONAL.get(provider, [])}
    for k in s:
        if k not in allowed and k in {*SIGN_IN_LABEL, "authorizationServer"}:
            add("error", where, f"authorization.signIn.{k}", f"`{k}` does not apply to {provider} sign-in")
    for k in need:
        if not (isinstance(s.get(k), str) and s[k].strip()):
            add("error", where, f"authorization.signIn.{k}", f"{provider} sign-in needs {SIGN_IN_LABEL[k]}")
    cognito_claim = keys["groupsClaim"]["default"]
    claim = auth["groupsClaim"] if auth.get("groupsClaim") else cognito_claim
    if provider != "cognito" and claim == cognito_claim and _is_obj(auth.get("actions")) and auth["actions"]:
        hint = (f'set groupsClaim to "{GROUPS_CLAIM_FOR[provider]}"' if provider in GROUPS_CLAIM_FOR
                else "set groupsClaim to the namespaced claim your post-login Action adds")
        add("warning", where, "authorization.groupsClaim",
            f"{provider} sends no cognito:groups claim, so every restricted action would be denied: {hint}")


def _check_triggers(orch, agents: dict, add) -> None:
    """orchestrator.triggers: what starts a run without anyone typing it (bff/triggers.py).
    Mirrors validate.ts checkTriggers."""
    trs = orch.get("triggers") if _is_obj(orch) and _is_obj(orch.get("triggers")) else {}
    where = {"kind": "block", "name": "orchestrator"}
    readers = [a for a in agents.values() if _is_obj(a) and a.get("attachments") is True]
    for name, t in trs.items():
        p = f"orchestrator.triggers.{name}"
        if not TRIGGER_NAME_RE.match(name):
            add("error", where, p, "a trigger's name is letters and digits, at most 32, starting with a letter: "
                "it names the rule, schedule or queue")
        if not _is_obj(t):
            add("error", where, p, "must be an object: {type, prompt, ...}")
            continue
        ty = t.get("type")
        types = vocab("triggerTypes")
        if ty not in types:
            add("error", where, f"{p}.type", f"must be one of: {', '.join(types)}")
        else:
            for k in t:
                if k in TRIGGER_COMMON or k in TRIGGER_TYPE_KEYS[ty]:
                    continue
                if any(k in v for v in TRIGGER_TYPE_KEYS.values()):
                    add("error", where, f"{p}.{k}", f"does not apply to a {ty} trigger")
                else:
                    add("error", where, f"{p}.{k}", "unknown key; a trigger takes: "
                        + ", ".join([*TRIGGER_COMMON, *TRIGGER_TYPE_KEYS[ty]]))
        if not (isinstance(t.get("prompt"), str) and t["prompt"].strip()):
            add("error", where, f"{p}.prompt",
                "is required: the run's request, with {{placeholders}} from the delivery")
        for k in ("enabled", "attachPayload"):
            if k in t and not isinstance(t[k], bool):
                add("error", where, f"{p}.{k}", "must be true or false")
        for k, vname in (("runAs", "triggerRunAs"), ("gates", "triggerGates")):
            if k in t and t[k] not in vocab(vname):
                add("error", where, f"{p}.{k}", f"must be one of: {', '.join(vocab(vname))}")
        if "approvers" in t and not _strings(t["approvers"]):
            add("error", where, f"{p}.approvers", "must list group names")
        has_approvers = isinstance(t.get("approvers"), list) and len(t["approvers"]) > 0
        if t.get("runAs") == "service" and not has_approvers:
            add("warning", where, f"{p}.runAs",
                "no group can read or decide its runs in the app: list them in approvers")
        if has_approvers and t.get("runAs") != "service":
            add("warning", where, f"{p}.approvers", "only a service trigger's runs have approvers: the owner's runs "
                "are the owner's")
        if t.get("gates") == "auto":
            add("warning", where, f"{p}.gates", "every review gate of its runs approves itself")
        if "maxRunsPerHour" in t and not _int_in(t["maxRunsPerHour"], 1, 1000):
            add("error", where, f"{p}.maxRunsPerHour", "must be a whole number, 1 to 1000")
        if "idempotencyKey" in t and not isinstance(t["idempotencyKey"], str):
            add("error", where, f"{p}.idempotencyKey", "must be text: a template, e.g. {{headers.x-request-id}}")
        if t.get("attachPayload") is True and not readers:
            add("warning", where, f"{p}.attachPayload",
                "no agent reads a run's files: set `attachments` on the agents that should")
        if ty == "webhook" and "signature" in t and t["signature"] not in vocab("triggerSignatures"):
            add("error", where, f"{p}.signature", f"must be one of: {', '.join(vocab('triggerSignatures'))}")
        if ty == "schedule":
            if not (isinstance(t.get("expression"), str) and SCHEDULE_RE.match(t["expression"])):
                add("error", where, f"{p}.expression", "needs cron(minutes hours day-of-month month day-of-week year) "
                    "or rate(<n> minutes|hours|days)")
            if "timezone" in t and not (isinstance(t["timezone"], str) and TIMEZONE_RE.match(t["timezone"])):
                add("error", where, f"{p}.timezone", "must be an IANA time zone, e.g. Europe/Paris")
        if ty == "eventbridge":
            if not (_is_obj(t.get("pattern")) and t["pattern"]):
                add("error", where, f"{p}.pattern", 'needs an event pattern, e.g. {"source": ["aws.cloudwatch"]}')
            if "bus" in t and not (isinstance(t["bus"], str) and BUS_RE.match(t["bus"])):
                add("error", where, f"{p}.bus", "must be an event bus name or ARN")
        if ty == "s3":
            bucket = t.get("bucket")
            prefix = t.get("prefix") if isinstance(t.get("prefix"), str) else ""
            if not (isinstance(bucket, str) and BUCKET_RE.match(bucket)):
                add("error", where, f"{p}.bucket", "must be a bucket name")
            elif not _s3_attachable(orch, bucket, prefix):
                add("warning", where, f"{p}.bucket",
                    f'its objects are not attached to the run: add "{bucket}" to orchestrator.attachments.s3')
            if "prefix" in t and not (isinstance(t["prefix"], str) and not t["prefix"].startswith("/")):
                add("error", where, f"{p}.prefix", "must be a key prefix, without a leading /")
        if ty == "sqs" and "queueArn" in t and not (isinstance(t["queueArn"], str) and SQS_ARN_RE.match(t["queueArn"])):
            add("error", where, f"{p}.queueArn",
                "must be an SQS queue ARN, or left out for a queue the framework creates")


def _fixed_name_problems(statement, tools: dict) -> list[str]:
    """A custom policy naming a tool whose one name the framework fixes (a web search's
    "WebSearch", a knowledge base's "retrieve") by any other name: AgentCore refuses it
    at deploy ("unrecognized action"). Observed live: a policy on a
    library web search tool, written as orchestrator.policy.custom, named "___search".
    Mirrors validate.ts fixedNameProblems."""
    if not isinstance(statement, str):
        return []
    try:
        acts = cedar.parse(statement)["actions"]
    except Exception:  # noqa: BLE001 - cedar.problems names a parse error
        return []
    out = []
    for a in acts:
        key, sep, name = a.partition(cedar.SEP)
        tool = tools.get(key) if sep and _is_obj(tools) else None
        t = str(tool.get("type") or "").lower() if _is_obj(tool) else ""
        fixed = _FIXED_TOOL_NAMES.get(t)
        if fixed and name != fixed:
            out.append(f'names {a}, but a {t} tool\'s one tool is "{fixed}": write '
                       f'AgentCore::Action::"{key}{cedar.SEP}{fixed}"')
    return out


def _check_policy(orch, tools: dict, add) -> None:
    """orchestrator.policy: the mode, and each custom Cedar policy (bff/cedar.py).
    Mirrors validate.ts checkPolicy."""
    pol = orch.get("policy") if _is_obj(orch) and _is_obj(orch.get("policy")) else {}
    where = {"kind": "block", "name": "orchestrator"}
    modes = vocab("policyModes")
    if isinstance(pol.get("mode"), str) and pol["mode"].upper() not in modes:
        add("error", where, "orchestrator.policy.mode", f"must be one of: {', '.join(modes)}")
    custom = pol.get("custom") if isinstance(pol.get("custom"), list) else []
    seen: list = []
    for i, c in enumerate(custom):
        p = f"orchestrator.policy.custom[{i}]"
        if not _is_obj(c):
            add("error", where, p, "must be {name, description, statement}")
            continue
        for k in c:
            if k not in POLICY_KEYS:
                add("error", where, f"{p}.{k}", f"unknown key; a policy takes: {', '.join(POLICY_KEYS)}")
        name = c.get("name")
        if not isinstance(name, str) or not cedar.NAME_RE.match(name):
            add("error", where, f"{p}.name", "needs a name: a letter, then letters and digits (32 at most)")
        elif name in seen:
            add("error", where, f"{p}.name", f'"{name}" is used twice')
        else:
            seen.append(name)
        if "description" in c and not isinstance(c["description"], str):
            add("error", where, f"{p}.description", "must be a string")
        for message in cedar.problems(c.get("statement"), list(tools)):
            add("error", where, f"{p}.statement", message)
        for message in _fixed_name_problems(c.get("statement"), tools):
            add("error", where, f"{p}.statement", message)
    if custom and pol.get("enabled") is False:
        add("warning", where, "orchestrator.policy.custom",
            "is not deployed: the policy engine is off (orchestrator.policy.enabled)")


#: The optional named maps, and the block each entry is checked against.
NAMED = {"guardrails": "guardrail", "memories": "memory", "evaluators": "evaluator",
         "identities": "identity", "policies": "policy", "skills": "skill"}
#: A skill's reference file: a plain name with a text extension, and how much they may hold.
SKILL_FILE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}\.(md|txt|json|csv|yaml|yml)$")
SKILL_MAX_FILES = 20
SKILL_MAX_CHARS = 200_000
#: What a guardrail enforces: one of these, or it is refused by Bedrock.
GUARDRAIL_POLICIES = ("contentFilters", "deniedWords", "managedWordLists", "deniedTopics", "piiEntities")
#: A build keeps a shared (library) item as {"library": "<id>"} until it is resolved.
SHARED_WHAT = {"tools": "tool", "guardrails": "guardrail", "memories": "memory",
               "evaluators": "evaluator", "identities": "identity", "policies": "policy", "skills": "skill"}


def _named(wf: dict, name: str) -> dict:
    v = wf.get(name)
    return v if _is_obj(v) else {}


#: Bedrock Guardrails' limits on a denied topic (standard topic tier). Past them the
#: deploy fails in CreateGuardrail/UpdateGuardrail ("One or more of your guardrail topic
#: definitions exceeds the maximum allowed length"), seen live.
TOPIC_NAME_RE = re.compile(r"^[0-9A-Za-z_ !?.-]{1,100}$")
TOPIC_DEFINITION_MAX, TOPIC_EXAMPLES_MAX, TOPIC_EXAMPLE_MAX = 200, 5, 100


def _check_topics(entry, path: str, where: dict, add) -> None:
    """A guardrail's deniedTopics within Bedrock's limits. Mirrors validate.ts checkTopics."""
    topics = entry.get("deniedTopics") if _is_obj(entry) else None
    if not isinstance(topics, list):
        return
    for i, t in enumerate(topics):
        if not _is_obj(t):
            continue
        p = f"{path}.deniedTopics[{i}]"
        if isinstance(t.get("name"), str) and not TOPIC_NAME_RE.match(t["name"]):
            add("error", where, f"{p}.name", "1 to 100 letters, digits, spaces and - _ ! ? .")
        d = t.get("definition")
        if isinstance(d, str) and len(d) > TOPIC_DEFINITION_MAX:
            add("error", where, f"{p}.definition", f"is {len(d)} characters; Bedrock takes at most "
                f"{TOPIC_DEFINITION_MAX}: say what the topic is in one sentence")
        ex = t.get("examples")
        if isinstance(ex, list):
            if len(ex) > TOPIC_EXAMPLES_MAX:
                add("error", where, f"{p}.examples", f"at most {TOPIC_EXAMPLES_MAX} examples")
            for j, e in enumerate(ex):
                if not isinstance(e, str) or not 1 <= len(e) <= TOPIC_EXAMPLE_MAX:
                    add("error", where, f"{p}.examples[{j}]", f"an example is 1 to {TOPIC_EXAMPLE_MAX} characters")


def _check_named(wf: dict, add, out: list) -> None:
    """The named maps (guardrails, memories, evaluators, identities, policies), what
    agents and tools name from them, and shared items that could not be loaded. Mirrors
    validate.ts checkNamed."""
    tools = wf.get("tools") if _is_obj(wf.get("tools")) else {}
    agents = wf.get("agents") if _is_obj(wf.get("agents")) else {}
    if _is_obj(wf.get("guardrail")):
        _check_topics(wf["guardrail"], "guardrail", {"kind": "block", "name": "guardrail"}, add)
    for m in SHARED_WHAT:
        for key, entry in _named(wf, m).items():
            if _is_obj(entry) and "library" in entry:
                where = {"kind": "tool", "id": key} if m == "tools" else {"kind": "block", "name": m}
                add("error", where, f"{m}.{key}",
                    f"a shared {SHARED_WHAT[m]} that could not be loaded: it was deleted, or is no "
                    f"longer shared with you. Remove it, or pick another")
    for m, blk in NAMED.items():
        if m in wf and not _is_obj(wf[m]):
            add("error", {"kind": "block", "name": m}, m, "must be an object of named entries")
            continue
        for key, entry in _named(wf, m).items():
            where, path = {"kind": "block", "name": m}, f"{m}.{key}"
            if _is_obj(entry) and "library" in entry:
                continue
            if not cedar.NAME_RE.match(key):
                add("error", where, path, "a name is a letter, then letters and digits (32 at most)")
            _check_entry(blk, entry, path, where, out)
            if not _is_obj(entry):
                continue
            if m == "guardrails" and not any(_truthy(entry.get(k)) for k in GUARDRAIL_POLICIES):
                add("error", where, path, "a guardrail needs something to enforce: content filters, denied "
                    "words or topics, managed word lists or PII entities")
            if m == "guardrails":
                _check_topics(entry, path, where, add)
            if m == "memories":
                if isinstance(entry.get("strategies"), list) and not entry["strategies"]:
                    add("error", where, f"{path}.strategies", "name at least one strategy")
                days = entry.get("expiryDays")
                if _num(days) and not 3 <= days <= 365:
                    add("error", where, f"{path}.expiryDays", "must be from 3 to 365")
            if m == "evaluators":
                if isinstance(entry.get("instructions"), str) and not entry["instructions"].strip():
                    add("error", where, f"{path}.instructions", "is required: what the judge scores, and how")
                scale = entry.get("scale")
                if scale is not None and (not isinstance(scale, list) or not 2 <= len(scale) <= 20 or any(
                        not _is_obj(s) or not _num(s.get("value")) or not isinstance(s.get("label"), str)
                        or not isinstance(s.get("definition"), str) for s in scale)):
                    add("error", where, f"{path}.scale", "must be 2 to 20 {value, label, definition} points")
            if m == "skills":
                _check_skill(entry, path, where, add, agents, key)
            if m == "identities" and entry.get("type") == "oauth2":
                urls = [u for u in (entry.get("discoveryUrl"), entry.get("tokenUrl")) if u not in (None, "")]
                if len(urls) != 1:
                    add("error", where, path, "an oauth2 identity needs exactly one of discoveryUrl or tokenUrl")
                for u in urls:
                    if not isinstance(u, str) or not HTTPS_RE.match(u):
                        add("error", where, path, "discoveryUrl / tokenUrl must be an https:// URL")
            if m == "policies":
                for message in cedar.problems(entry.get("statement"), list(tools)):
                    add("error", where, f"{path}.statement", message)
                attached = [k for k, t in tools.items()
                            if _is_obj(t) and isinstance(t.get("policies"), list) and key in t["policies"]]
                if cedar.is_template(entry.get("statement")) and not attached:
                    add("warning", where, path, "is written for {{tool}} and attached to no tool, so it "
                        "deploys nowhere — attach it to a tool")
    # --- what tools name
    identities, policies = _named(wf, "identities"), _named(wf, "policies")
    pol = (wf.get("orchestrator") or {}).get("policy") if _is_obj(wf.get("orchestrator")) else None
    engine_off = _is_obj(pol) and pol.get("enabled") is False
    for key, tool in tools.items():
        if not _is_obj(tool) or "library" in tool:
            continue
        where, path = {"kind": "tool", "id": key}, f"tools.{key}"
        ident = tool.get("identity")
        if isinstance(ident, str) and ident:
            got = identities.get(ident)
            t = _js(tool.get("type") or "").lower()
            if not _is_obj(got):
                add("error", where, f"{path}.identity",
                    f'"{ident}" is not an identity. Define it under Identity, or pick one of: '
                    f'{", ".join(identities) or "(none yet)"}')
            elif got.get("type") == "oauth2" and t not in vocab("oauthToolTypes"):
                add("error", where, f"{path}.identity",
                    f"an oauth2 identity works with {' or '.join(vocab('oauthToolTypes'))} tools")
            elif got.get("type") == "apikey" and t not in vocab("apiKeyToolTypes"):
                add("error", where, f"{path}.identity",
                    f"an apikey identity works with {', '.join(vocab('apiKeyToolTypes'))} tools")
            for k in ("auth", "oauth"):
                if tool.get(k) not in (None, ""):
                    add("error", where, f"{path}.{k}", "the identity says how to authenticate — remove this")
        names = tool.get("policies") if isinstance(tool.get("policies"), list) else []
        for i, n in enumerate(names):
            p = f"{path}.policies[{i}]"
            if not isinstance(n, str):
                continue          # _check_entry names the type
            got = policies.get(n)
            if not _is_obj(got):
                add("error", where, p, f'"{n}" is not a policy. Define it under Policies, or pick one of: '
                    f'{", ".join(policies) or "(none yet)"}')
                continue
            if names.index(n) != i:
                add("error", where, p, f'"{n}" is listed twice')
                continue
            if "library" in got or not isinstance(got.get("statement"), str):
                continue
            # A tool whose one name the framework fixes: a policy naming any other is
            # refused by AgentCore at deploy (CREATE_FAILED, and a stuck rollback), so
            # it is caught here. Observed live: "webSearch___search" for a web search.
            t = str(tool.get("type") or "").lower()
            fixed = _FIXED_TOOL_NAMES.get(t)
            if fixed:
                stmt = cedar.for_tool(got["statement"], key)
                try:
                    acts = cedar.parse(stmt)["actions"]
                except Exception:  # noqa: BLE001 - cedar.problems names a parse error below
                    acts = []
                for a in acts:
                    if a.startswith(f"{key}{cedar.SEP}") and a != f"{key}{cedar.SEP}{fixed}":
                        add("error", where, p, f'"{n}" names {a}, but a {t} tool\'s one tool is '
                            f'"{fixed}": write AgentCore::Action::"{key}{cedar.SEP}{fixed}"')
            if not cedar.is_template(got["statement"]):
                if not cedar.governs(got["statement"], key):
                    add("error", where, p, f'"{n}" names other tools, not {key}: write it for '
                        f'AgentCore::Action::"{{{{tool}}}}" to attach it to any tool')
                continue
            for message in cedar.problems(cedar.for_tool(got["statement"], key), list(tools)):
                add("error", where, p, f'"{n}" for {key}: {message}')
        if names and engine_off:
            add("warning", where, f"{path}.policies",
                "is not deployed: the policy engine is off (orchestrator.policy.enabled)")
    # --- what agents name
    skills = _named(wf, "skills")
    for aid, a in agents.items():
        names = a.get("skills") if _is_obj(a) and isinstance(a.get("skills"), list) else []
        for i, n in enumerate(names):
            if not isinstance(n, str):
                continue          # _check_entry names the type
            p = f"agents.{aid}.skills[{i}]"
            if n not in skills:
                add("error", {"kind": "agent", "id": aid}, p, f'"{n}" is not a skill. Define it under Skills, '
                    f'or pick one of: {", ".join(skills) or "(none yet)"}')
            elif names.index(n) != i:
                add("error", {"kind": "agent", "id": aid}, p, f'"{n}" is listed twice')
    guardrails, memories = _named(wf, "guardrails"), _named(wf, "memories")
    for aid, a in agents.items():
        if not _is_obj(a) or not _is_obj(a.get("agentcore")):
            continue
        core, where, path = a["agentcore"], {"kind": "agent", "id": aid}, f"agents.{aid}.agentcore"
        g = core.get("guardrails") if _is_obj(core.get("guardrails")) else {}
        use = g.get("use")
        if isinstance(use, str) and use:
            if use not in guardrails:
                add("error", where, f"{path}.guardrails.use", f'"{use}" is not a guardrail. Define it under '
                    f'Guardrails, or pick one of: {", ".join(guardrails) or "(none yet)"}')
            if _jst(g.get("guardrailId")):
                add("error", where, f"{path}.guardrails.guardrailId", "use a guardrail by name or by id, not both")
            if not _jst(g.get("input")) and not _jst(g.get("output")):
                add("warning", where, f"{path}.guardrails",
                    "names a guardrail but checks neither input nor output — turn one on")
        mem = core.get("memory") if _is_obj(core.get("memory")) else {}
        use = mem.get("use")
        if isinstance(use, str) and use:
            if use not in memories:
                add("error", where, f"{path}.memory.use", f'"{use}" is not a memory. Define it under Memory, '
                    f'or pick one of: {", ".join(memories) or "(none yet)"}')
            for k in ("longTerm", "scope", "custom"):
                if k in mem:
                    add("error", where, f"{path}.memory.{k}", "the memory it uses sets this — remove it")


def _check_skill(entry: dict, path: str, where: dict, add, agents: dict, key: str) -> None:
    """A skill's instructions and reference files. Mirrors validate.ts checkSkill."""
    if isinstance(entry.get("instructions"), str) and not entry["instructions"].strip():
        add("error", where, f"{path}.instructions", "is required: the steps the agent follows")
    files = entry.get("files")
    if files is not None and _is_obj(files):
        if len(files) > SKILL_MAX_FILES:
            add("error", where, f"{path}.files", f"at most {SKILL_MAX_FILES} reference files")
        total = len(entry.get("instructions") or "") if isinstance(entry.get("instructions"), str) else 0
        for name, text in files.items():
            if not SKILL_FILE_RE.match(name):
                add("error", where, f"{path}.files.{name}", "a reference file is a plain name ending in .md, .txt, "
                    ".json, .csv, .yaml or .yml")
            if not isinstance(text, str):
                add("error", where, f"{path}.files.{name}", "must be the file's text")
            else:
                total += len(text)
        if total > SKILL_MAX_CHARS:
            add("error", where, f"{path}.files", f"a skill holds at most {SKILL_MAX_CHARS // 1000} KB of text, "
                "instructions and files together")
    if not any(_is_obj(a) and isinstance(a.get("skills"), list) and key in a["skills"] for a in agents.values()):
        add("warning", where, path, "no agent uses it: add it to an agent's Skills")


def _check_features(core: dict, path: str, where: dict, add, shared: list | None = None) -> None:
    """Evaluators, custom evaluators and a custom memory strategy. Mirrors validate.ts
    and registry.features_problem. `shared`: the build's own `evaluators` names."""
    ev = core.get("evaluations") if _is_obj(core.get("evaluations")) else {}
    custom = ev.get("custom") if isinstance(ev.get("custom"), list) else []
    names = [c.get("name") for c in custom if _is_obj(c)] + list(shared or [])
    unsupported = vocab_extra("builtinEvaluators", "unsupported") or []
    levels = vocab_extra("builtinEvaluators", "levels") or {}
    for i, e in enumerate(ev.get("evaluators") if isinstance(ev.get("evaluators"), list) else []):
        if e in unsupported:
            add("error", where, f"{path}.evaluations.evaluators[{i}]",
                f"{e} scores a {str(levels.get(e, '')).lower().replace('_', ' ')}, and this framework "
                f"scores one agent's run — not offered yet")
        elif isinstance(e, str) and e.startswith("Custom.") and e[7:] not in names:
            add("error", where, f"{path}.evaluations.evaluators[{i}]",
                f"{e} is not defined in evaluations.custom or in the build's evaluators")
    seen: list = []
    for i, c in enumerate(custom):
        p = f"{path}.evaluations.custom[{i}]"
        if not _is_obj(c) or not isinstance(c.get("name"), str) or not CUSTOM_NAME_RE.match(c["name"]):
            add("error", where, f"{p}.name", "needs a name: a letter, then letters and digits (32 at most)")
            continue
        if c["name"] in seen:
            add("error", where, f"{p}.name", f'"{c["name"]}" is used twice')
        seen.append(c["name"])
        if not isinstance(c.get("instructions"), str) or not c["instructions"].strip():
            add("error", where, f"{p}.instructions", "is required: what the judge scores, and how")
        scale = c.get("scale")
        if scale is not None and (not isinstance(scale, list) or not 2 <= len(scale) <= 20 or any(
                not _is_obj(s) or not isinstance(s.get("value"), (int, float)) or isinstance(s.get("value"), bool)
                or not isinstance(s.get("label"), str) or not isinstance(s.get("definition"), str) for s in scale)):
            add("error", where, f"{p}.scale", "must be 2 to 20 {value, label, definition} points")
    mem = core.get("memory") if _is_obj(core.get("memory")) else {}
    if "custom" in mem:
        m = mem["custom"]
        bases = vocab("customMemoryBases")
        if not _is_obj(m) or m.get("base") not in bases:
            add("error", where, f"{path}.memory.custom.base", f"must be one of: {', '.join(bases)}")
        elif not isinstance(m.get("instructions"), str) or not m["instructions"].strip():
            add("error", where, f"{path}.memory.custom.instructions",
                "is required: what to extract, and what to leave out")


def validate(wf: dict) -> list[dict]:
    out: list[dict] = []
    top = {"kind": "workflow"}

    def add(severity, where, path, message):
        out.append({"severity": severity, "where": where, "path": path, "message": message})

    for k in wf:
        if k not in TOP_LEVEL and k not in NAMED:
            add("error", top, k, f"unknown top-level block `{k}`")
    for k in TOP_LEVEL:
        if k not in wf:
            add("warning", top, k, f"`{k}` is missing — the test suite expects every top-level block to be present")
    for name in ("orchestrator", "ui", "guardrail", "authorization"):
        if name in wf:
            _check_entry(name, wf[name], name, {"kind": "block", "name": name}, out)

    agents = wf.get("agents") if _is_obj(wf.get("agents")) else {}
    tools = wf.get("tools") if _is_obj(wf.get("tools")) else {}
    steps = wf.get("steps") if isinstance(wf.get("steps"), list) else []
    _check_policy(wf.get("orchestrator"), tools, add)
    _check_attachments(wf.get("orchestrator"), agents, add)
    _check_interceptors(wf.get("orchestrator"), tools, agents, add)
    _check_triggers(wf.get("orchestrator"), agents, add)
    _check_sign_in(wf.get("authorization"), add)
    _check_named(wf, add, out)

    # --- tools
    kinds: dict[str, list[str]] = {}
    auth_block = wf.get("authorization") if _is_obj(wf.get("authorization")) else {}
    sign_in = (auth_block.get("signIn") or {}).get("provider") if _is_obj(auth_block.get("signIn")) else None
    for key, tool in tools.items():
        where = {"kind": "tool", "id": key}
        path = f"tools.{key}"
        if _is_obj(tool) and "library" in tool:
            continue          # a shared tool that was not loaded (_check_named says so)
        if not TOOL_KEY_RE.match(key):
            add("error", where, path,
                "a tool key must be letters and digits, starting with a letter — it names both the "
                "Gateway target (no underscores) and the Cedar policy permit_<key> (no hyphens). "
                "Use camelCase, e.g. \"claimsDb\".")
        _check_entry("tool", tool, path, where, out)
        if not _is_obj(tool):
            continue
        t = _js(tool.get("type") if tool.get("type") is not None else "").lower()
        kinds.setdefault(t, []).append(key)
        if t == "kb":
            if not isinstance(tool.get("corpora"), list) or not tool["corpora"]:
                add("error", where, f"{path}.corpora",
                    "a Knowledge Base needs at least one corpus — a folder under kb_docs/")
            dims = (VOCAB.get("embeddingModels") or {}).get("dimensionsByModel") or {}
            first = (vocab("embeddingModels") or [""])[0]
            model = _js(tool["embeddingModel"] if tool.get("embeddingModel") is not None else first)
            if "dimensions" in tool and dims.get(model):
                try:
                    d = float(tool["dimensions"])
                except (TypeError, ValueError):
                    d = float("nan")
                if d not in dims[model]:
                    add("error", where, f"{path}.dimensions", f"{model} supports {', '.join(map(str, dims[model]))}")
        endpoint = tool.get("endpoint")
        if t == "mcp" and isinstance(endpoint, str) and not re.search(r"^https://\S+\.\S+", endpoint):
            add("error", where, f"{path}.endpoint", "must be the server's https:// URL")
        if t == "kb":
            kb_id, s3_uri, kms = tool.get("knowledgeBaseId"), tool.get("s3Uri"), tool.get("kmsKeyArn")
            if isinstance(kb_id, str) and not KB_ID_RE.match(kb_id):
                add("error", where, f"{path}.knowledgeBaseId",
                    "must be a Knowledge Base id: 10 capital letters and digits")
            if isinstance(s3_uri, str) and not S3_LOCATION_RE.match(s3_uri):
                add("error", where, f"{path}.s3Uri", "must be s3://<bucket> or s3://<bucket>/<prefix>/")
            if isinstance(kms, str) and not KMS_KEY_ARN_RE.match(kms):
                add("error", where, f"{path}.kmsKeyArn",
                    "must be a KMS key ARN, arn:aws:kms:<region>:<account>:key/<id>")
            if kb_id:
                for k in ("s3Uri", "kmsKeyArn", "embeddingModel", "dimensions"):
                    if tool.get(k) is not None:
                        add("error", where, f"{path}.{k}",
                            "does not apply with knowledgeBaseId: that Knowledge Base already has its "
                            "documents and embeddings")
            elif kms and not s3_uri:
                add("error", where, f"{path}.kmsKeyArn", "only applies with s3Uri: it decrypts that bucket")
        if t == "apigateway":
            if isinstance(tool.get("restApiId"), str) and not REST_API_ID_RE.match(tool["restApiId"]):
                add("error", where, f"{path}.restApiId", "must be a REST API id: 10 lowercase letters and digits")
            if isinstance(tool.get("stage"), str) and not STAGE_RE.match(tool["stage"]):
                add("error", where, f"{path}.stage", "must be a stage name: letters, digits, - and _")
            methods = vocab("httpMethods")
            filters = tool.get("toolFilters")
            if isinstance(filters, list):
                if not filters:
                    add("error", where, f"{path}.toolFilters",
                        "needs at least one filter, or the target exposes no tool")
                for i, f in enumerate(filters):
                    fp = f"{path}.toolFilters[{i}]"
                    if not _is_obj(f) or not isinstance(f.get("path"), str) or not f["path"].startswith("/"):
                        add("error", where, fp, 'must be {"path": "/...", "methods": [...]}, the path starting with /')
                        continue
                    ms = f.get("methods")
                    if not isinstance(ms, list) or not ms or any(m not in methods for m in ms):
                        add("error", where, f"{fp}.methods", f"must list one or more of: {', '.join(methods)}")
            overrides = tool.get("toolOverrides")
            if isinstance(overrides, list):
                for i, o in enumerate(overrides):
                    op = f"{path}.toolOverrides[{i}]"
                    if (not _is_obj(o) or not isinstance(o.get("path"), str) or not o["path"].startswith("/")
                            or "*" in o["path"]):
                        add("error", where, op, "needs an explicit path (no *), starting with /")
                        continue
                    if o.get("method") not in methods:
                        add("error", where, f"{op}.method", f"must be one of: {', '.join(methods)}")
                    if not isinstance(o.get("name"), str) or not TOOL_NAME_RE.match(o["name"]):
                        add("error", where, f"{op}.name",
                            "must be a tool name: a letter, then letters, digits, - or _ (64 at most)")
        if t == "openapi" and "schema" in tool:
            doc = tool.get("schema")
            if not _is_obj(doc) or not str(doc.get("openapi") or "").startswith("3") or not _is_obj(doc.get("paths")):
                add("error", where, f"{path}.schema",
                    'must be an OpenAPI 3 document: {"openapi": "3.0.x", "info": {...}, "paths": {...}}')
            else:
                if not doc["paths"]:
                    add("error", where, f"{path}.schema.paths", "has no operations, so the target would expose no tool")
                for p, item in doc["paths"].items():
                    for m, oper in (item.items() if _is_obj(item) else []):
                        if m in OPENAPI_OPS and (not _is_obj(oper) or not oper.get("operationId")):
                            add("error", where, f"{path}.schema.paths.{p}.{m}",
                                "needs an operationId: it becomes the tool's name")
                if _is_obj(doc.get("components")) and doc["components"].get("securitySchemes"):
                    add("error", where, f"{path}.schema.components.securitySchemes",
                        "is not supported by the Gateway: set `auth` on the tool instead")
        mode = tool.get("auth")
        if mode in OAUTH_MODES:
            if t not in vocab("oauthToolTypes"):
                add("error", where, f"{path}.auth",
                    f"{mode} is only for {', '.join(vocab('oauthToolTypes'))} tools")
            oa = tool.get("oauth")
            if not _is_obj(oa):
                add("error", where, f"{path}.oauth",
                    f'auth "{mode}" needs {{"clientId", "scopes", and "discoveryUrl" or "tokenUrl"}}')
            else:
                if not isinstance(oa.get("clientId"), str) or not oa["clientId"].strip():
                    add("error", where, f"{path}.oauth.clientId", "is required: the OAuth client's id")
                if not isinstance(oa.get("scopes"), list) or any(not isinstance(s, str) for s in oa["scopes"]):
                    add("error", where, f"{path}.oauth.scopes", "must be a list of scope names (it may be empty)")
                urls = [k for k in ("discoveryUrl", "tokenUrl") if oa.get(k)]
                if len(urls) != 1:
                    add("error", where, f"{path}.oauth",
                        "needs exactly one of discoveryUrl (the issuer's .well-known/openid-configuration) or tokenUrl")
                for k in ("discoveryUrl", "tokenUrl", "issuer", "authorizationUrl"):
                    if oa.get(k) is not None and (not isinstance(oa[k], str) or not HTTPS_RE.match(oa[k])):
                        add("error", where, f"{path}.oauth.{k}", "must be an https:// URL")
                if mode == "user" and oa.get("tokenUrl") and not oa.get("authorizationUrl"):
                    add("error", where, f"{path}.oauth.authorizationUrl",
                        "with tokenUrl, each person's sign-in needs the provider's authorizationUrl too")
                if mode != "user" and "authorizationUrl" in oa:
                    add("warning", where, f"{path}.oauth.authorizationUrl", 'is only used with auth "user"')
                if mode != "obo" and "audience" in oa:
                    add("warning", where, f"{path}.oauth.audience", 'is only used with auth "obo"')
            listed = isinstance(tool.get("toolSchema"), list) and tool["toolSchema"]
            if mode in PERSON_MODES and t == "mcp" and not listed:
                add("error", where, f"{path}.toolSchema",
                    "an MCP tool used as the person must list its tools here: the Gateway cannot ask the "
                    "server for them before anyone has signed in")
            if mode == "obo" and sign_in == "entra":
                add("error", where, f"{path}.auth",
                    "Entra ID's on-behalf-of exchange needs an access token for this app, which sign-in does not "
                    "request yet: use \"user\" (each person connects their account) for now")
        elif "oauth" in tool:
            add("warning", where, f"{path}.oauth", 'is only used with auth "oauth2", "user" or "obo"')
        if t == "openapi" and isinstance(tool.get("schemaS3Uri"), str) and not S3_URI_RE.search(tool["schemaS3Uri"]):
            add("error", where, f"{path}.schemaS3Uri",
                "must be s3://<bucket>/<key> — the Gateway loads an OpenAPI schema only from S3")
        if t == "lambda":
            if "code" in tool:
                _check_code(key, tool["code"], path, where, add)
            arn = tool.get("lambdaArn")
            if isinstance(arn, str) and arn and not LAMBDA_ARN_RE.match(arn):
                add("error", where, f"{path}.lambdaArn",
                    "must be a Lambda function ARN, arn:aws:lambda:<region>:<account>:function:<name>")
            schema = tool.get("toolSchema") if isinstance(tool.get("toolSchema"), list) else []
            if not schema:
                add("error", where, f"{path}.toolSchema",
                    "a Lambda tool needs at least one tool in `toolSchema`, so the Gateway can publish it")
            names, props_of = [], {}
            for i, ts in enumerate(schema):
                p = f"{path}.toolSchema[{i}]"
                if not _is_obj(ts):
                    add("error", where, p, "each entry must be an object")
                    continue
                if not isinstance(ts.get("name"), str) or not ts["name"]:
                    add("error", where, p, "each entry needs a `name`")
                else:
                    names.append(ts["name"])
                props = ts.get("properties") if _is_obj(ts.get("properties")) else {}
                if not props:
                    add("error", where, f"{p}.properties", "each entry needs at least one property")
                props_of[_js(ts.get("name"))] = list(props)
                for pn, pv in props.items():
                    pt = _js((pv.get("type") if _is_obj(pv) else None) or "string").lower()
                    if pt not in vocab("toolSchemaPropertyTypes"):
                        add("error", where, f"{p}.properties.{pn}.type",
                            f'"{pt}" is not one of: {", ".join(vocab("toolSchemaPropertyTypes"))}')
            if len(names) > 1 and not _jst(tool.get("call")):
                add("error", where, f"{path}.call",
                    "with more than one tool in `toolSchema`, say which one agents call")
            if _jst(tool.get("call")) and names and _js(tool["call"]) not in names:
                add("error", where, f"{path}.call",
                    f'"{_js(tool["call"])}" is not a name in toolSchema ({", ".join(names)})')
            called = _js(tool["call"]) if tool.get("call") is not None else (names[0] if names else "")
            arg = _js(tool["arg"]) if tool.get("arg") is not None else "query"
            if called in props_of and arg not in props_of[called]:
                add("error", where, f"{path}.arg",
                    f'"{arg}" is not a property of {called} ({", ".join(props_of[called])}). '
                    "Set `arg` to the property the agent's query goes into.")
    for t in ("kb", "websearch"):
        if len(kinds.get(t, [])) > 1:
            for key in kinds[t]:
                add("error", {"kind": "tool", "id": key}, f"tools.{key}",
                    f"at most one {t} tool per workflow (found {', '.join(kinds[t])})")

    # --- agents
    local = evaluated = 0
    for aid, a in agents.items():
        where = {"kind": "agent", "id": aid}
        path = f"agents.{aid}"
        if not AGENT_ID_RE.match(aid):
            add("error", where, path, "an agent id must be letters, digits and underscores, starting with a letter — "
                "it becomes part of an AgentCore Runtime name, so no hyphens")
        _check_entry("agent", a, path, where, out)
        if not _is_obj(a):
            continue
        runtime = variant_of("agent", a)
        if runtime != "a2a":
            local += 1
            if _num(a.get("maxTokens")) and a["maxTokens"] <= 0:
                add("error", where, f"{path}.maxTokens", "must be greater than 0")
            bound = tools_of(a.get("tool"))
            if "tool" in a and not bound:
                add("error", where, f"{path}.tool", "`tool` must name at least one tool — remove it to have none")
            if bound and _jst(a.get("access")):
                add("error", where, f"{path}.access",
                    "`access` describes what an agent reads when it has NO tool — remove it now that `tool` is set")
            for i, key in enumerate(bound):
                if not _jst(tools.get(key)):
                    add("error", where, f"{path}.tool",
                        f'"{key}" is not a tool. Declare it under Tools, or pick one of: '
                        f'{", ".join(tools) or "(none yet)"}')
                if bound.index(key) != i:
                    add("error", where, f"{path}.tool", f'"{key}" is listed twice')
            mtc = a.get("maxToolCalls")
            if isinstance(mtc, (int, float)) and not isinstance(mtc, bool) and not 1 <= mtc <= 20:
                add("error", where, f"{path}.maxToolCalls", "must be from 1 to 20")
            if a.get("toolMode") == "model" and not bound:
                add("warning", where, f"{path}.toolMode",
                    '"model" lets the model choose among the agent\'s tools, and it has none — bind one with `tool`')
            _check_vision(a, aid, agents, steps, where, path, add)
            if "image" in a and a.get("output") != "image":
                add("error", where, f"{path}.image",
                    "`image` configures how an image agent renders — set `output` to \"image\", "
                    "or remove it")
            if a.get("output") == "image":
                img = a.get("image") if _is_obj(a.get("image")) else {}
                model = _js(img["model"] if img.get("model") is not None
                            else (vocab("imageModels") or [""])[0])
                regions = ", ".join(((VOCAB.get("imageModels") or {}).get("regionsByModel")
                                     or {}).get(model) or []) or "its own region"
                for k, vset in (("model", "imageModels"), ("aspectRatio", "imageAspectRatios"),
                                ("outputFormat", "imageFormats")):
                    if k in img and _js(img[k]) not in vocab(vset):
                        add("error", where, f"{path}.image.{k}",
                            f'"{_js(img[k])}" is not one of: {", ".join(vocab(vset))}')
                seed = img.get("seed")
                if _num(seed) and (seed < 0 or seed > 4294967294):
                    add("error", where, f"{path}.image.seed", "must be between 0 and 4294967294")
                add("warning", where, f"{path}.output",
                    f"images are rendered by {model}, which Bedrock serves in {regions}: a "
                    "deployment elsewhere sends this agent's image brief there, and one outside "
                    "that geography is refused unless image.allowCrossRegion is true. Bedrock "
                    "guardrails and evaluations do not check images")
            if "corpus" in a:
                kb = next((tools[k] for k in bound
                           if _is_obj(tools.get(k)) and _js(tools[k].get("type")) == "kb"), None)
                if not kb:
                    add("error", where, f"{path}.corpus", "`corpus` needs a Knowledge Base among this agent's tools")
                elif not isinstance(kb.get("corpora"), list) or a["corpus"] not in kb["corpora"]:
                    have = ", ".join(kb["corpora"]) if isinstance(kb.get("corpora"), list) else "none"
                    add("error", where, f"{path}.corpus",
                        f'"{_js(a["corpus"])}" is not one of this KB\'s corpora ({have})')
        else:
            if a.get("source") == "a2a_lambda" and (a.get("auth") if a.get("auth") is not None else "none") != "sigv4":
                add("error", where, f"{path}.auth",
                    "the framework-deployed stand-in (source \"a2a_lambda\") is called with SigV4 — "
                    "set auth to \"sigv4\"")
            if _jst(a.get("skill")) and not _jst(a.get("source")):
                add("error", where, f"{path}.skill", "`skill` picks a stand-in skill, so it needs `source`")
            card = a.get("agentCard")
            if isinstance(card, str) and card and not card.startswith("https://"):
                add("error", where, f"{path}.agentCard", "must be an https:// URL")
            core = a.get("agentcore")
            identity = core.get("identity") if _is_obj(core) else None
            outbound = identity.get("outbound") if _is_obj(identity) else None
            if a.get("auth") == "oauth2" and not _truthy(outbound):
                add("error", where, f"{path}.auth", "oauth2 needs a credential provider in agentcore.identity.outbound")
        core = a.get("agentcore")
        if _is_obj(core):
            if not core:
                add("warning", where, f"{path}.agentcore",
                    "an empty agentcore block reads like a setting and does nothing — remove it")
            for feature, cfg in core.items():
                if _is_obj(cfg) and len(cfg) == 1 and cfg.get("enabled") is False:
                    add("warning", where, f"{path}.agentcore.{feature}",
                        f'`{{"enabled": false}}` is the same as leaving {feature} out — remove it')
            if _is_obj(core.get("evaluations")) and core["evaluations"].get("enabled") is True:
                evaluated += 1
            _check_features(core, f"{path}.agentcore", where, add, list(_named(wf, "evaluators")))
    if agents and local == 0:
        add("error", top, "agents",
            "at least one agent must run here (runtime main or dedicated) — a workflow of only "
            "remote agents has nothing to deploy")
    if agents and evaluated == 0:
        add("warning", top, "agents",
            "no agent has evaluations enabled; the test suite requires at least one (agentcore.evaluations.enabled)")

    # --- steps
    if not steps:
        add("error", top, "steps", "a workflow needs at least one stage — drag an agent onto the canvas")
    seen: dict[str, int] = {}
    for i, step in enumerate(steps):
        where = {"kind": "step", "index": i}
        path = f"steps[{i}]"
        _check_entry("step", step, path, where, out)
        if not _is_obj(step):
            continue
        for lst in ("parallel", "sequence"):
            if isinstance(step.get(lst), list) and not step[lst]:
                add("error", where, f"{path}.{lst}", "must name at least one agent")
        for aid in step_agents(step):
            if aid not in agents:
                add("error", where, path, f'"{aid}" is not a defined agent')
            if aid in seen:
                add("error", where, path,
                    f'"{aid}" already runs in stage {seen[aid] + 1} — an agent can appear in one stage only')
            else:
                seen[aid] = i
        if _jst(step.get("gateId")) and step["gateId"] in agents:
            add("error", where, f"{path}.gateId",
                f'"{step["gateId"]}" is also an agent id — pick a different gate id')
        _check_branch(steps, i, out)
        _check_hitl(step, i, add)
        prev = steps[i - 1] if i > 0 else None
        if _is_obj(prev) and _jst(prev.get("parallel")) and not _jst(prev.get("hitl")) and _jst(step.get("hitl")):
            add("warning", where, path,
                "the stage before this is an ungated parallel group, so a reviewer here cannot send "
                "work back past it (re-run cannot rewind across it). Gate that group, or accept the limit.")
    names = [step_name(s, n) if _is_obj(s) else f"group{n}" for n, s in enumerate(steps)]
    if any(_is_obj(s) and _jst(s.get("branch")) for s in steps):
        for i, n in enumerate(names):
            if names.index(n) != i:
                add("error", {"kind": "step", "index": i}, f"steps[{i}]",
                    f'two stages are both named "{n}", so a branch target is ambiguous — give one a gateId')
    for aid in agents:
        if aid not in seen:
            add("error", {"kind": "agent", "id": aid}, f"agents.{aid}",
                "not in any stage — drag it onto the canvas, or delete it. The deploy rejects an "
                "agent that never runs.")
    return out


def errors(wf: dict) -> list[dict]:
    return [i for i in validate(wf) if i["severity"] == "error"]
