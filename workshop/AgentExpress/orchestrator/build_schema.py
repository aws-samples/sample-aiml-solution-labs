#!/usr/bin/env python3
"""Generate app/workflow.schema.json from app/keys.json + app/vocabulary.json.

WHY A SCHEMA AT ALL, WHEN THREE PLANES ALREADY VALIDATE
Terraform, CDK and the container all reject a bad `workflow.json`, with good
messages. But all three reject it AFTER you have finished editing and run
something. For a customer meeting this framework for the first time, the questions
are earlier and simpler than that:

    which keys may this agent have?
    which of them are required?
    what values are allowed here?
    what does this one actually do?

A JSON Schema answers all four IN THE EDITOR, as you type, because `$schema` in
workflow.json points at it and every mainstream editor picks that up. Autocomplete
lists the legal keys for the `runtime` you chose, a wrong value is underlined with
the allowed set, and hovering a key shows what reads it. That is the difference
between a config file you can adopt and one you have to be taught.

WHY IT IS GENERATED AND NOT WRITTEN
A hand-written schema would be a FOURTH place encoding the same rules, and this
repo has spent a lot of effort deleting the second and third (see
app/vocabulary.json, which exists because the closed value sets were written out in
Python, TypeScript and HCL and drifted). A schema that disagreed with the
validators would be worse than none: an editor saying a config is fine and a deploy
rejecting it teaches a customer not to trust the editor.

So it is derived, `--check` keeps it in sync in CI, and a test asserts it accepts
the shipped workflow and rejects what the other planes reject.

    python3 build_schema.py            # regenerate
    python3 build_schema.py --check     # exit 1 if it is out of date (for CI)
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

APP = Path(__file__).resolve().parent / "app"
KEYS = APP / "keys.json"
VOCAB = APP / "vocabulary.json"
OUT = APP / "workflow.schema.json"
#: The second generated artifact: every key's DEFAULT, projected out of keys.json so the
#: running code can read it.
#:
#: WHY A SECOND FILE RATHER THAN READING keys.json. keys.json is 40 KB of prose and is
#: deliberately excluded from the container image (.dockerignore) — editing a doc string
#: must not invalidate the image layer and trigger a rebuild and redeploy. But the defaults
#: in it are needed AT RUNTIME by app/, bff/ and kb_lambda/. So the values are projected
#: into a small file that ships: it changes when a default changes and never when a comment
#: does, which keeps both properties. Same "one JSON, three planes" shape as
#: app/vocabulary.json, which is the pattern this repo already proved.
DEFAULTS_OUT = APP / "defaults.json"
#: The third: the key spec, the closed value sets and the defaults, copied into the web
#: app for the Build view (web/src/builder/).
#:
#: WHY A COPY RATHER THAN AN IMPORT FROM ../app. The CDK path builds the UI inside a
#: container that mounts ONLY web/, so an import reaching outside it fails the synth; and
#: Terraform's UI rebuild hash covers web/src/** but not app/, so an edit to keys.json
#: would never reach a deployed builder. A generated file inside web/src/ is visible to
#: both builds and triggers both rebuilds — and `--check` fails CI when it drifts, the
#: same guarantee the schema and defaults already have.
#:
#: WHAT IT DELIBERATELY DOES NOT CARRY: workflow.json itself. The UI bundle is served to
#: anyone who can reach CloudFront, unauthenticated, and a customer's workflow names
#: their tool endpoints, ARNs and policies — exactly what bff/workflow.py refuses to send.
#: keys.json and vocabulary.json are framework metadata with no deployment in them.
WEB_META_OUT = APP.parent / "web" / "src" / "generated" / "builder-meta.json"

#: JSON Schema draft. 2020-12 is what editors ship support for, and it is the draft
#: whose `if`/`then` and `dependentSchemas` express the per-variant key sets below.
DRAFT = "https://json-schema.org/draft/2020-12/schema"


def _load() -> tuple[dict, dict]:
    return json.loads(KEYS.read_text()), json.loads(VOCAB.read_text())


def _property(spec: dict, vocab: dict) -> dict:
    """One key's schema: its type, its allowed values, and the text an editor shows.

    `description` carries BOTH halves — what the key does and what reads it — because
    the hover tooltip is the one place a customer reliably looks, and "which code
    consumes this" is exactly the question that otherwise sends them into the source.
    """
    out: dict = {"description": spec["doc"] + f"\n\nRead by: {spec['reads']}"}
    kind = spec.get("type")
    if kind:
        out["type"] = kind
    # A list of types is a key that takes EITHER form — `tool` is one key or a list of
    # them. JSON Schema says that with a type array; the array branch still needs its
    # item type.
    kinds = kind if isinstance(kind, list) else [kind]
    if "array" in kinds:
        out["items"] = {"type": spec.get("items", "string")}
        if spec.get("itemRequired"):
            out["items"]["required"] = spec["itemRequired"]

    def note(vocabulary: str, what: str) -> None:
        out["description"] += (
            f"\n\nAllowed {what} ({vocabulary} in app/vocabulary.json): "
            + ", ".join(repr(v) for v in vocab[vocabulary]["values"]))

    # THREE DIFFERENT THINGS A CLOSED SET CAN CONSTRAIN, and conflating them is how the
    # first version of this generator produced a schema that rejected the shipped file:
    # `guardrail.contentFilters` is a MAP whose values are strengths, and putting the
    # strength enum on the key itself asserted that the map WAS a strength.
    if spec.get("vocabulary"):
        target = out["items"] if kind == "array" else out
        target["enum"] = vocab[spec["vocabulary"]]["values"]
        note(spec["vocabulary"], "values")
    elif spec.get("enum"):
        out["enum"] = spec["enum"]
    if spec.get("valueVocabulary"):
        out["additionalProperties"] = {"enum": vocab[spec["valueVocabulary"]]["values"]}
        note(spec["valueVocabulary"], "values")
    if spec.get("keyVocabulary"):
        out["propertyNames"] = {"enum": vocab[spec["keyVocabulary"]]["values"]}
        note(spec["keyVocabulary"], "keys")

    if spec.get("pattern"):
        target = out["items"] if kind == "array" else out
        target["pattern"] = spec["pattern"]

    # `default` is a real JSON Schema keyword, so an editor shows it on hover and offers it
    # in completion. That is most of the point of declaring defaults in one place: a
    # customer sees what omitting the key will do without opening any code. `defaultFor`
    # cannot go here — it depends on the variant, so it lands in the per-variant branch.
    if "default" in spec:
        out["default"] = spec["default"]
        out["description"] += f"\n\nDefault: {json.dumps(spec['default'])}"

    # An inline object with declared sub-keys, e.g. `domains: {include, exclude}`. Closed
    # like everything else, so a typo inside it is caught rather than ignored — which is
    # the whole reason collapsing four flat keys into one nested key is safe.
    if spec.get("properties"):
        out["additionalProperties"] = False
        out["properties"] = {
            name: ({"type": "array", "items": {"type": sub.get("items", "string")},
                    "description": sub["doc"]}
                   if sub.get("type") == "array"
                   else {"type": sub.get("type", "string"), "description": sub["doc"]})
            for name, sub in spec["properties"].items()
        }
    return out


def _nest(flat: dict[str, dict]) -> dict:
    """Turn dotted keys (`memory.longTerm`, `policy.tool`) into nested object schemas."""
    root: dict = {}
    for dotted, schema in flat.items():
        head, _, tail = dotted.partition(".")
        if not tail:
            root[head] = schema
            continue
        parent = root.setdefault(head, {"type": "object", "additionalProperties": False,
                                        "properties": {}})
        parent["properties"][tail] = schema
    return root


def _entry_schema(block: dict, vocab: dict) -> dict:
    """The schema for ONE agent / tool / step entry.

    The shape is: properties = every key the spec knows, so an editor can autocomplete
    all of them; then `allOf` of `if`/`then` clauses that NARROW by variant — the keys
    that do not apply to the chosen `runtime` or `type` are forbidden there rather than
    merely undocumented. That ordering matters for usability: a customer who has not
    yet typed `runtime` still gets completions, and one who has gets only the right
    ones.
    """
    keys = block["keys"]
    variant_key = block.get("$variantKey")
    props = _nest({k: _property(v, vocab) for k, v in keys.items()})

    schema: dict = {"type": "object", "additionalProperties": False, "properties": props}

    universal_required = [k for k, v in keys.items() if v.get("required")]
    if universal_required:
        schema["required"] = universal_required

    rules: list[dict] = []

    # Exactly-one groups (agentCard vs source; lambdaArn vs source; agent vs parallel
    # vs sequence). `oneOf` over single-key `required` is how JSON Schema says XOR.
    for rule in block.get("$exactlyOne") or []:
        clause = {
            "oneOf": [{"required": [k]} for k in rule["keys"]],
            "$comment": rule["why"],
        }
        applies = rule.get("appliesTo")
        if applies == "*":
            rules.append(clause)
        else:
            rules.append({"if": {"properties": {variant_key: {"enum": applies}},
                                 "required": [variant_key]},
                          "then": clause})

    if variant_key:
        variants = _variants(block, vocab)
        aliases = block.get("$variantAliases") or {}
        for variant in variants:
            allowed, required = [], []
            # A key whose allowed VALUES depend on the variant (`vocabularyFor`), so the
            # enum lands in this branch only. `source` is the case: for type="lambda" it
            # is closed to what the framework has an execution role for, and for
            # type="openapi" it is any folder name, because uploading a schema file needs
            # no permissions. One enum on the key itself could only state the stricter
            # rule, which would reject a customer's own schema folder in an editor while
            # the deploy accepted it.
            narrowed: dict[str, dict] = {}
            for key, spec in keys.items():
                scope = spec.get("appliesTo")
                if scope == "*" or variant in _expand(scope, aliases):
                    allowed.append(key)
                if variant in _expand(spec.get("requiredFor") or [], aliases):
                    required.append(key)
                vocabulary = (spec.get("vocabularyFor") or {}).get(variant)
                if vocabulary:
                    narrowed[key] = {
                        **props[key],
                        "enum": vocab[vocabulary]["values"],
                        "description": props[key]["description"] + (
                            f"\n\nFor type=\"{variant}\" the allowed values "
                            f"({vocabulary} in app/vocabulary.json) are: "
                            + ", ".join(repr(v) for v in vocab[vocabulary]["values"])),
                    }
                # A per-variant DEFAULT, for a key that legitimately means different things
                # per variant — `maxResults` is retrieval depth for a kb and page size for
                # websearch. Landing it in the branch is what lets an editor show 5 on one
                # and 10 on the other instead of one misleading number on both.
                variant_default = (spec.get("defaultFor") or {}).get(variant)
                if variant_default is not None:
                    base = narrowed.get(key, props[key])
                    narrowed[key] = {
                        **base,
                        "default": variant_default,
                        "description": base["description"]
                        + f"\n\nDefault for type=\"{variant}\": {json.dumps(variant_default)}",
                    }
            head = sorted({k.partition(".")[0] for k in allowed})
            clause: dict = {"properties": {k: narrowed.get(k, props[k]) for k in head},
                            "additionalProperties": False}
            if required:
                clause["required"] = sorted({k.partition(".")[0] for k in required})
            rules.append({
                "if": {"properties": {variant_key: {"const": variant}},
                       **({} if variant == block.get("$variantDefault")
                          else {"required": [variant_key]})},
                "then": clause,
            })

    if rules:
        schema["allOf"] = rules
    return schema


def _expand(scope, aliases: dict) -> list[str]:
    """`appliesTo` with any alias (`local` -> main, dedicated) resolved."""
    if scope == "*" or scope is None:
        return []
    out: list[str] = []
    for name in scope:
        out += aliases.get(name, [name])
    return out


def _variants(block: dict, vocab: dict) -> list[str]:
    """The variant values this block branches on, from the vocabulary that closes them."""
    key = block["$variantKey"]
    spec = block["keys"][key]
    if spec.get("vocabulary"):
        return list(vocab[spec["vocabulary"]]["values"])
    return list(spec.get("enum") or [])


def build() -> dict:
    keys, vocab = _load()
    agent = _entry_schema(keys["agent"], vocab)
    # The feature block is its own spec, spliced in where the agent declares it, so
    # `agentcore` gets completions and per-key hover text too instead of being an
    # opaque object.
    agent["properties"]["agentcore"] = {
        **_entry_schema(keys["agentcore"], vocab),
        "description": keys["agent"]["keys"]["agentcore"]["doc"],
    }
    # `additionalProperties: false` inside each variant clause lists the keys allowed
    # there, and `agentcore` is in every one of them — so the spliced schema has to
    # replace the placeholder in those lists as well.
    for rule in agent.get("allOf") or []:
        then = rule.get("then") or {}
        if "agentcore" in (then.get("properties") or {}):
            then["properties"]["agentcore"] = agent["properties"]["agentcore"]

    return {
        "$schema": DRAFT,
        # This project's own repository. It used to name the AWS samples repo, which
        # does not host this schema — a `$id` is the schema's identity, so pointing it at
        # somewhere the file is not makes `$ref` resolution and any registry lookup wrong.
        "$id": ("https://github.com/praven80/AgentExpress"
                "/blob/main/orchestrator/app/workflow.schema.json"),
        "title": "Multi-Agent Orchestrator workflow",
        "description": (
            "GENERATED — do not edit. Run `python3 build_schema.py` after changing "
            "app/keys.json or app/vocabulary.json. This exists so your editor can tell "
            "you which keys are legal, which are required and what values they take, "
            "before you deploy rather than after."),
        "type": "object",
        "additionalProperties": False,
        "required": ["agents", "steps"],
        "properties": {
            "$schema": {"type": "string",
                        "description": "Points your editor at this file."},
            "$comment": {"type": "string",
                         "description": "Orientation for whoever opens the file. Not read "
                                        "by anything."},
            "agents": {
                "type": "object",
                "description": "One entry per agent, keyed by agent id.\n\n"
                               + keys["agent"]["$comment"],
                "propertyNames": {"pattern": "^[a-zA-Z][a-zA-Z0-9_]*$"},
                "additionalProperties": agent,
            },
            "tools": {
                "type": "object",
                "description": "One entry per data source.\n\n" + keys["tool"]["$comment"],
                # ALPHANUMERIC, and narrower than either AWS rule alone: the key builds
                # the Gateway target `<key>` (no underscores allowed) AND the Cedar policy
                # `permit_<key>` (no hyphens allowed). Both were found by CloudFormation
                # refusing a change set after a clean synth, so the editor now says it
                # first.
                "propertyNames": {
                    "pattern": "^[A-Za-z][A-Za-z0-9]*$",
                    "description": (
                        "A tool's key must be letters and digits, starting with a letter — "
                        "it builds both the Gateway target name (no underscores) and the "
                        "Cedar policy name permit_<key> (no hyphens), so neither separator "
                        "survives both. Use camelCase, e.g. \"policyDocs\". An agent id is "
                        "different and may contain underscores."),
                },
                "additionalProperties": _entry_schema(keys["tool"], vocab),
            },
            "steps": {
                "type": "array",
                "minItems": 1,
                "description": keys["step"]["$comment"],
                "items": _entry_schema(keys["step"], vocab),
            },
            **{name: {**_entry_schema(keys[name], vocab),
                      "description": keys[name]["$comment"]}
               for name in ("orchestrator", "ui", "guardrail", "authorization")},
            # The optional NAMED maps (guardrails, memories, evaluators, identities,
            # policies): each entry is one block's entry, keyed by a name that agents and
            # tools refer to.
            **{spec["$path"].split(".")[0]: {
                "type": "object",
                "description": spec["$comment"],
                "propertyNames": {"pattern": spec["$namePattern"]},
                "additionalProperties": _entry_schema(keys[spec.get("$entry") or name], vocab),
            } for name, spec in keys.items() if not name.startswith("$") and spec.get("$optional")},
        },
    }


def build_defaults() -> dict:
    """Every declared default, as `{block: {key: value}}` plus a per-variant map.

    Flat and boring on purpose: this is the file three planes parse at runtime, so it
    carries values and nothing else — no docs, no types, no ordering significance. The
    prose stays in keys.json, which does not ship.

    `perType` is separate rather than folded in, because a caller has to ASK for the
    variant. `maxResults` has no single correct default, and a lookup that silently
    returned one of the two would be worse than no default at all: a kb tool would get
    websearch's page size and retrieve ten chunks where the framework promises five.
    """
    keys, _vocab = _load()
    out: dict = {"$comment": (
        "GENERATED from app/keys.json by build_schema.py — do not edit. Every key's "
        "default value, projected here because keys.json is 40 KB of prose that "
        "deliberately does not ship in the container image while these values are needed "
        "at runtime. Read by app/common/defaults.py, cdk/lib/defaults.ts and "
        "local.key_defaults in terraform/. `perType` holds the defaults that depend on a "
        "tool's `type`, which a caller must ask for by variant.")}
    for block, spec in keys.items():
        if block.startswith("$"):
            continue
        plain = {k: v["default"] for k, v in (spec.get("keys") or {}).items()
                 if "default" in v}
        per_variant = {k: v["defaultFor"] for k, v in (spec.get("keys") or {}).items()
                       if v.get("defaultFor")}
        if plain:
            out[block] = plain
        if per_variant:
            out.setdefault("perType", {})[block] = per_variant
    return out


def build_web_meta() -> dict:
    """What the Build view needs to render forms and validate, in one file.

    `keys` drives which fields a form shows for a given runtime/type, their order, help
    text and allowed values; `vocabulary` closes those values; `defaults` is what an
    empty field means. The same three things the other planes read, so a builder cannot
    offer a key the deploy would reject.
    """
    keys, vocab = _load()
    return {
        "$comment": ("GENERATED from app/keys.json, app/vocabulary.json and the defaults "
                     "by build_schema.py — do not edit. Read by web/src/builder/."),
        "keys": keys,
        "vocabulary": vocab,
        "defaults": build_defaults(),
        # Stamped into every bundle the page exports, so `scaffold.py apply` can say
        # when a bundle was built on a different framework than the one applying it.
        "frameworkVersion": (APP.parent / "VERSION").read_text().strip(),
    }


def main() -> int:
    out = json.dumps(build(), indent=2, ensure_ascii=False) + "\n"
    defaults = json.dumps(build_defaults(), indent=2, ensure_ascii=False) + "\n"
    web_meta = json.dumps(build_web_meta(), indent=1, ensure_ascii=False) + "\n"
    artifacts = ((OUT, out), (DEFAULTS_OUT, defaults), (WEB_META_OUT, web_meta))
    if "--check" in sys.argv:
        stale = [path.name for path, want in artifacts
                 if (path.read_text() if path.exists() else "") != want]
        if not stale:
            print(f"build_schema: {OUT.name}, {DEFAULTS_OUT.name} and "
                  f"{WEB_META_OUT.name} are up to date")
            return 0
        print(f"build_schema: {', '.join(stale)} out of date (run: python3 "
              f"{Path(__file__).name})", file=sys.stderr)
        return 1
    for path, want in artifacts:
        # The web copy's folder does not exist in a fresh checkout's history before this
        # generator added it; create rather than fail.
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(want)
    print(f"build_schema: wrote {OUT.name} ({out.count(chr(10))} lines), "
          f"{DEFAULTS_OUT.name} and {WEB_META_OUT.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
