"""The config surface as a CUSTOMER meets it: one spec, a canonical shape, and an
editor that can answer their questions before they deploy.

Three planes already reject a bad `workflow.json` with good messages. All three do it
AFTER you have finished editing and run something, and the questions a customer has
are earlier than that — which keys may this agent have, which are required, what values
are allowed, what does this one do. `app/keys.json` answers them, `build_schema.py`
turns that into `app/workflow.schema.json`, and `$schema` in workflow.json is what makes
an editor use it.

WHAT THIS FILE IS ACTUALLY GUARDING
A generated schema is a FOURTH encoding of rules that already exist in Python, CDK and
Terraform. This repo has spent real effort deleting the second and third copies of
things (`app/vocabulary.json` exists because the closed value sets were written out in
three languages and drifted), so adding a fourth needs a reason to trust it. A schema
that DISAGREED with the validators would be worse than no schema at all: an editor
saying a config is fine and a deploy rejecting it teaches a customer to ignore the
editor.

So the tests below are mostly about agreement, in both directions:
  * the schema accepts the shipped workflow, and accepts a foreign one;
  * the schema rejects each mistake the other planes reject;
  * the schema is in sync with the spec it is generated from;
  * the spec is in sync with the vocabulary it references;
  * the canonical key order the formatter writes is the order the spec declares.
"""

from __future__ import annotations

import copy
import json
import subprocess
import sys

import pytest
from conftest import ORCH_ROOT, some_agent, some_tool

jsonschema = pytest.importorskip("jsonschema", reason="dev-only; see requirements-dev.txt")

SPEC = json.loads((ORCH_ROOT / "app" / "keys.json").read_text())
VOCAB = json.loads((ORCH_ROOT / "app" / "vocabulary.json").read_text())
SCHEMA = json.loads((ORCH_ROOT / "app" / "workflow.schema.json").read_text())
SHIPPED = json.loads((ORCH_ROOT / "app" / "workflow.json").read_text())

ENTRY_BLOCKS = ("agent", "agentcore", "tool", "step")


def validator():
    return jsonschema.Draft202012Validator(SCHEMA)


def errors(doc: dict) -> list[str]:
    return [e.message for e in validator().iter_errors(doc)]


def wf_with(fn) -> dict:
    """The shipped workflow with one edit applied, for the rejection tests."""
    doc = copy.deepcopy(SHIPPED)
    fn(doc)
    return doc


# ---------------------------------------------------------------------------
# The schema is a valid schema, and it accepts what it must
# ---------------------------------------------------------------------------

def test_the_generated_schema_is_itself_a_valid_json_schema():
    """Otherwise every editor silently ignores it and the whole mechanism is decorative
    — which looks exactly like it working."""
    jsonschema.Draft202012Validator.check_schema(SCHEMA)


def test_the_shipped_workflow_validates():
    assert errors(SHIPPED) == []


def test_workflow_json_points_at_the_schema():
    """`$schema` is the entire delivery mechanism. Without it a customer's editor has no
    idea the schema exists, and the autocomplete this is all for never appears."""
    assert SHIPPED["$schema"] == "./workflow.schema.json"
    assert (ORCH_ROOT / "app" / "workflow.schema.json").exists()


def test_the_schema_is_in_sync_with_the_spec_it_is_generated_from():
    """`build_schema.py --check` in the gate, asserted here too so a spec edit without a
    regenerate fails the suite rather than only CI."""
    result = subprocess.run(  # noqa: S603 - fixed argv, no shell
        [sys.executable, str(ORCH_ROOT / "build_schema.py"), "--check"],
        capture_output=True, text=True, cwd=ORCH_ROOT, check=False)
    assert result.returncode == 0, result.stderr or result.stdout


def test_a_foreign_workflow_validates_too():
    """The schema must describe the FRAMEWORK, not this sample. A 3-agent pipeline in
    another domain, with its own tool and its own vocabulary, has to pass — otherwise
    the schema is a description of the demo and every customer's first edit is underlined
    in red."""
    foreign = {
        "$schema": "./workflow.schema.json",
        "orchestrator": {"defaultModel": "us.anthropic.claude-haiku-4-5-20251001-v1:0"},
        "ui": {"title": "Claims Adjudicator", "defaultTopic": "Assess claim CL-4471"},
        "tools": {
            "claimsDb": {
                "type": "lambda",
                "description": "Claim lookup against the policy warehouse.",
                "lambdaArn": "arn:aws:lambda:us-east-1:123456789012:function:ClaimsLookup",
                "call": "lookup_claim",
                "arg": "claim_id",
                "toolSchema": [{"name": "lookup_claim", "properties": {
                    "claim_id": {"type": "string", "required": True,
                                 "description": "The claim reference."}}}],
            },
        },
        "agents": {
            "triage": {"name": "Claim Triage", "runtime": "main", "produces": "claim-triage",
                       "maxTokens": 2000, "tool": "claimsDb"},
            "fraud_check": {"name": "Partner Fraud Check", "runtime": "a2a",
                            "produces": "fraud-assessment",
                            "agentCard": "https://agents.partner.example/fraud",
                            "auth": "bearer"},
            "decision": {"name": "Decision", "runtime": "dedicated", "produces": "claim-decision",
                         "maxTokens": 4000,
                         "access": ["Upstream assets"],
                         "agentcore": {"guardrails": {"output": True}}},
        },
        "steps": [
            {"agent": "triage", "hitl": True,
             "branch": {"when": [{"field": "amount", "lt": 100, "goto": "decision"}]}},
            {"agent": "fraud_check"},
            {"agent": "decision"},
        ],
    }
    assert errors(foreign) == []


# ---------------------------------------------------------------------------
# It rejects what the other planes reject
# ---------------------------------------------------------------------------
# Each case here is a mistake one of the three validators already catches. The schema
# catching it too is what makes the editor's silence trustworthy: nothing that passes
# here should fail at plan/synth/start, and nothing that fails there should pass here.

# EVERY TARGET BELOW IS RESOLVED FROM THE WORKFLOW UNDER TEST, not named.
#
# These used to say `w["agents"]["analysis"]` and `w["tools"]["kb"]`. Measured against a
# foreign five-agent insurance workflow, 23 of them failed with `KeyError: 'analysis'` —
# in a file whose own docstring says it is about the framework. A framework test that
# needs a remote agent to mutate needs *a* remote agent, and taking it from the config is
# both portable and a stronger check, because it exercises whatever the customer actually
# wrote. `some_agent`/`some_tool` skip cleanly when a workflow has nothing of that kind.

def a_remote(w):
    return w["agents"][some_agent(runtime="a2a")]


def a_local(w):
    return w["agents"][some_agent(runtime="main")]


def a_kb_tool(w):
    return w["tools"][some_tool("kb")]


def an_mcp_tool(w):
    return w["tools"][some_tool("mcp")]


def a_lambda_tool(w):
    return w["tools"][some_tool("lambda")]


def with_agentcore(w, block: str):
    """A local agent that has `block` configured, so the mutation lands somewhere real."""
    for spec in (w.get("agents") or {}).values():
        if isinstance((spec.get("agentcore") or {}).get(block), dict):
            return spec["agentcore"][block]
    pytest.skip(f"no agent in this workflow configures agentcore.{block}")


@pytest.mark.parametrize("label,edit", [
    # --- placement keys that would read as settings and control nothing -------
    ("maxTokens on a remote agent", lambda w: a_remote(w).update(maxTokens=6000)),
    ("tool on a remote agent", lambda w: a_remote(w).update(tool=some_tool())),
    ("model on a remote agent", lambda w: a_remote(w).update(model="anthropic.claude-3")),
    ("agentCard on a local agent", lambda w: a_local(w).update(agentCard="https://x.example")),
    ("skill on a local agent", lambda w: a_local(w).update(skill="compliance")),
    # --- exactly-one rules ----------------------------------------------------
    ("a remote agent with BOTH agentCard and source",
     lambda w: a_remote(w).update(agentCard="https://x.example", source="a2a_lambda")),
    ("a remote agent with NEITHER",
     lambda w: [a_remote(w).pop("source", None), a_remote(w).pop("agentCard", None)]),
    ("a lambda tool with BOTH lambdaArn and source",
     lambda w: a_lambda_tool(w).update(
         source="pricing",
         lambdaArn="arn:aws:lambda:us-east-1:123456789012:function:f")),
    ("a step with both agent and parallel",
     lambda w: w["steps"][0].update(agent=next(iter(w["agents"])),
                                    parallel=[next(iter(w["agents"]))])),
    # --- required ------------------------------------------------------------
    ("a local agent with no maxTokens", lambda w: a_local(w).pop("maxTokens")),
    ("an mcp tool with no endpoint", lambda w: an_mcp_tool(w).pop("endpoint")),
    ("a kb tool with no corpora", lambda w: a_kb_tool(w).pop("corpora")),
    # setdefault, not indexing: these run against the CUSTOMER'S workflow.json, and a lean
    # one (from the Builder, or hand-written) need not declare deniedTopics, piiEntities
    # or authorization actions at all. Indexing made the safety net fail with a KeyError
    # on day one, for a file the deploy accepts.
    ("a denied topic with no definition",
     lambda w: w.setdefault("guardrail", {}).setdefault("deniedTopics", []).append({"name": "X"})),
    # --- closed value sets ---------------------------------------------------
    ("a misspelled runtime", lambda w: a_local(w).update(runtime="Main")),
    ("an unknown a2a skill", lambda w: a_remote(w).update(skill="not_a_skill")),
    ("an unknown a2a auth mode", lambda w: a_remote(w).update(auth="basic")),
    ("an unknown tool type", lambda w: w["tools"][some_tool()].update(type="graphql")),
    ("a lower-case listingMode", lambda w: an_mcp_tool(w).update(listingMode="dynamic")),
    ("an unprovisioned memory strategy",
     lambda w: with_agentcore(w, "memory").update(longTerm=["userPreferences"])),
    ("an evaluator with no Builtin/Custom prefix",
     lambda w: with_agentcore(w, "evaluations").update(evaluators=["Faithfulness"])),
    ("an unknown authorization action",
     lambda w: w.setdefault("authorization", {}).setdefault("actions", {}).update(approve=["approvers"])),
    ("an invalid guardrail strength",
     lambda w: w.setdefault("guardrail", {}).setdefault("contentFilters", {}).update(HATE="EXTREME")),
    ("an invalid PII action",
     lambda w: w.setdefault("guardrail", {}).setdefault("piiEntities", {}).update(EMAIL="REDACT")),
    # --- typos, which are the everyday case ----------------------------------
    ("a typo'd agent key", lambda w: a_local(w).update(maxTokenz=3000)),
    ("a typo'd agentcore block",
     lambda w: a_local(w).setdefault("agentcore", {}).update(guardrail={"input": True})),
    ("a typo'd step key", lambda w: w["steps"][0].update(hilt=True)),
    ("a typo'd tool key",
     lambda w: w["tools"][some_tool()].update(endpont="https://x.example")),
    ("an unknown top-level block", lambda w: w.update(agentz={})),
    ("an agent id with a hyphen",
     lambda w: w["agents"].update({"not-a-valid-id": dict(a_local(w))})),
])
def test_the_schema_rejects(label, edit):
    assert errors(wf_with(edit)), f"the schema ACCEPTED {label}"


def test_the_agent_id_rule_matches_the_runtime_name_constraint():
    """A hyphen is rejected because the id becomes part of an AgentCore Runtime name.
    Asserted separately from the list above because the reason is not obvious from the
    pattern."""
    assert SCHEMA["properties"]["agents"]["propertyNames"]["pattern"] == \
        "^[a-zA-Z][a-zA-Z0-9_]*$"


# ---------------------------------------------------------------------------
# The spec is internally coherent
# ---------------------------------------------------------------------------

def test_every_key_says_what_it_does_and_what_reads_it():
    """`doc` becomes the editor's hover text and `reads` is the answer to "is this key
    even wired up?". A key with neither is the failure this whole mechanism exists to
    prevent, one level up."""
    for block in SPEC:
        if block.startswith("$"):
            continue
        for key, spec in SPEC[block]["keys"].items():
            # A real sentence, not a length contest — "Browser tab title." is a complete
            # answer, and padding it would make the hover text worse.
            assert len(spec.get("doc", "")) > 10, f"{block}.{key} has no usable doc"
            assert spec.get("reads"), f"{block}.{key} names no reader"
            assert spec.get("type"), f"{block}.{key} declares no type"


def test_every_vocabulary_a_key_references_actually_exists():
    """A typo here would silently drop the value check from the schema — the key would
    still autocomplete, just without its allowed values, which is the hardest kind of
    gap to notice."""
    for block in SPEC:
        if block.startswith("$"):
            continue
        for key, spec in SPEC[block]["keys"].items():
            for field in ("vocabulary", "keyVocabulary", "valueVocabulary"):
                name = spec.get(field)
                if name:
                    assert name in VOCAB, f"{block}.{key} {field} -> unknown {name!r}"


def test_every_key_of_an_ordered_block_is_in_a_declared_group():
    """The groups ARE the shape a customer learns. A key outside them would be emitted
    after everything else, in a position that means nothing."""
    for block in ENTRY_BLOCKS:
        groups = SPEC[block].get("$groups") or {}
        if not SPEC[block].get("$ordered"):
            continue
        for key, spec in SPEC[block]["keys"].items():
            assert spec.get("group") in groups, (
                f"{block}.{key} is in group {spec.get('group')!r}, not one of "
                f"{sorted(groups)}")


def test_appliesto_only_names_real_variants():
    """`appliesTo: ["a2a"]` is what forbids a key elsewhere, so a typo'd variant name
    would quietly forbid it EVERYWHERE — the key would become unusable rather than
    restricted."""
    for block in ENTRY_BLOCKS:
        spec_block = SPEC[block]
        variant_key = spec_block.get("$variantKey")
        if not variant_key:
            continue
        declared = spec_block["keys"][variant_key]
        known = set(VOCAB[declared["vocabulary"]]["values"]) if declared.get("vocabulary") \
            else set(declared.get("enum") or [])
        known |= set(spec_block.get("$variantAliases") or {})
        for key, spec in spec_block["keys"].items():
            for field in ("appliesTo", "requiredFor"):
                scope = spec.get(field)
                if scope and scope != "*":
                    assert set(scope) <= known, (
                        f"{block}.{key} {field}={scope} names a variant that is not one "
                        f"of {sorted(known)}")


def test_the_spec_and_the_registry_agree_on_what_a_remote_agent_may_not_have():
    """The one place the spec could disagree with a plane that actually gates a deploy.

    `registry.validate_runtimes` keeps its own list of keys it rejects on an `a2a`
    agent, because it runs in the container and should not need a 30 KB editor-facing
    spec shipped in the image to start up. So the copy stays, and this makes it
    self-proving instead: a key the spec restricts to local placements and the registry
    forgets would be accepted at container start and underlined in the customer's
    editor — the editor and the deploy disagreeing, which is the failure that makes the
    schema worth less than nothing.

    Asserted by RUNNING the validator over a remote agent carrying each key, rather than
    by grepping for a literal, so it holds however the registry is written.
    """
    from conftest import workflow

    local_only = sorted(k for k, v in SPEC["agent"]["keys"].items()
                        if v.get("appliesTo") not in (None, "*")
                        and "a2a" not in v["appliesTo"])
    assert local_only, "the spec restricts nothing to local placements; this is vacuous"

    sample = {"model": "m", "temperature": 0.5, "topP": 0.9, "stopSequences": ["END"],
              "maxTokens": 100, "framework": "strands", "output": "image",
              "image": {"aspectRatio": "16:9"}, "vision": {"from": ["intake"]}, "attachments": True,
              "tool": "kb",
              "corpus": "reference", "access": ["Session input"], "toolMode": "model",
              "maxToolCalls": 3, "skills": ["triage"]}
    assert set(sample) == set(local_only), (
        f"this test does not know how to set {sorted(set(local_only) - set(sample))}; add a "
        f"value so the new key is actually exercised")

    for key in local_only:
        defn = {
            "orchestrator": {"defaultModel": "m"},
            "tools": {"kb": {"type": "kb", "corpora": ["reference"]}},
            "agents": {
                "intake": {"name": "I", "runtime": "dedicated", "maxTokens": 100},
                "partner": {"name": "P", "runtime": "a2a", "produces": "x",
                            "agentCard": "https://agents.partner.example", key: sample[key]},
            },
            "steps": [{"agent": "intake"}, {"agent": "partner"}],
        }
        with workflow(defn) as imp:
            with pytest.raises(ValueError) as caught:
                imp("app.orchestrator.registry").load_agents()
            assert key in str(caught.value), (
                f"the registry rejected a remote agent carrying {key!r}, but its message "
                f"does not name the key: {caught.value}")


def test_the_spec_is_framework_owned_not_customer_owned():
    """Same rule as the vocabulary: a customer's file must not be able to widen a set the
    framework enforces, so the spec lives beside it rather than inside it."""
    assert "keys" not in SHIPPED
    assert "NOT a file a customer edits" in SPEC["$comment"]


# ---------------------------------------------------------------------------
# The canonical order is real
# ---------------------------------------------------------------------------

def test_every_agent_tool_and_step_carries_its_keys_in_the_canonical_order():
    """The user-facing promise: every entry reads the same way, so the shape is learned
    once instead of re-read per entry. `format_workflow.py` writes it and `--check` holds
    it; this asserts the result on the shipped file, so the two cannot both be wrong.
    """
    sys.path.insert(0, str(ORCH_ROOT))
    try:
        from format_workflow import top_level_order
    finally:
        sys.path.pop(0)

    def assert_ordered(where: str, entry: dict, order: list[str]):
        expected = [k for k in order if k in entry]
        actual = [k for k in entry if k in expected]
        assert actual == expected, f"{where}: keys are {actual}, canonical is {expected}"

    for aid, agent in SHIPPED["agents"].items():
        assert_ordered(f"agents.{aid}", agent, top_level_order("agent"))
    for name, tool in SHIPPED["tools"].items():
        assert_ordered(f"tools.{name}", tool, top_level_order("tool"))
    for i, step in enumerate(SHIPPED["steps"]):
        assert_ordered(f"steps[{i}]", step, top_level_order("step"))


def test_every_agent_starts_with_the_same_four_keys():
    """The concrete version of "standardised": whatever an agent's placement, the first
    four keys are who it is and what it produces. That is what lets a reader scan the
    block down a column instead of parsing each entry."""
    for aid, agent in SHIPPED["agents"].items():
        head = [k for k in list(agent)[:4]]
        assert head[:2] == ["name", "runtime"] or head[:3] == ["name", "kind", "runtime"], (
            f"agents.{aid} starts {head}")
        assert "produces" in head, f"agents.{aid} does not declare produces up front: {head}"


def test_every_tool_starts_with_type_then_description():
    for name, tool in SHIPPED["tools"].items():
        assert list(tool)[:2] == ["type", "description"], f"tools.{name} starts {list(tool)[:2]}"


def test_the_formatter_refuses_to_write_if_reordering_changed_anything():
    """The guard that makes canonical ordering safe to run unattended. The pre-existing
    round-trip check compares two dicts, and `==` on dicts ignores key order — so it
    would have passed a document whose keys had been shuffled, dropped and re-added,
    which is exactly what a reordering bug does."""
    sys.path.insert(0, str(ORCH_ROOT))
    try:
        import format_workflow as fw
    finally:
        sys.path.pop(0)

    doc = copy.deepcopy(SHIPPED)
    seqs = fw.key_sequences(doc)
    assert seqs, "key_sequences found nothing to compare, so the guard is vacuous"
    # Dropping a key must be visible to the guard's own comparison.
    victim = next(iter(doc["agents"]))
    damaged = copy.deepcopy(doc)
    damaged["agents"][victim].pop("name")
    assert fw.key_sequences(damaged) != seqs
    # And so must a pure reorder, which `==` on the documents would not catch.
    shuffled = copy.deepcopy(doc)
    entry = shuffled["agents"][victim]
    shuffled["agents"][victim] = dict(reversed(list(entry.items())))
    assert shuffled == doc, "the premise: these compare equal as dicts"
    assert fw.key_sequences(shuffled) != seqs, "but their key order differs"


def test_an_unrecognised_key_is_kept_by_the_formatter_not_silently_dropped():
    """A customer's typo must survive to the validator that can NAME it. Formatting that
    deleted it would mean the mistake vanished on save and the config quietly did
    something else."""
    sys.path.insert(0, str(ORCH_ROOT))
    try:
        import format_workflow as fw
    finally:
        sys.path.pop(0)

    entry = {"agentcore": {}, "maxTokenz": 1, "name": "X", "runtime": "main"}
    out = fw.reorder(entry, fw.canonical_order("agent"))
    assert list(out) == ["name", "runtime", "agentcore", "maxTokenz"]
    assert out["maxTokenz"] == 1


# ---------------------------------------------------------------------------
# A tool key is a Gateway target name
# ---------------------------------------------------------------------------
# FOUND BY DEPLOYING A FOREIGN WORKFLOW, and it is the worst place to find anything: a
# tool named `policyDocs` passed this schema, `registry.validate_tool_types`, and
# `cdk synth` (148 resources, no error), and was then refused by CloudFormation when the
# change set was created — "does not match pattern ^([0-9a-zA-Z][-]?){1,100}$".
#
# Every tool in the shipped sample is a single word (kb, websearch, docs, pricing), so
# nothing had ever exercised an underscore. `policyDocs` is the obvious name for a
# Knowledge Base of policy wordings, which is exactly why this was worth fixing rather
# than documenting.

ILLEGAL_TOOL_KEYS = ("policy_docs", "policy-docs")


@pytest.mark.parametrize("bad", ILLEGAL_TOOL_KEYS)
def test_the_schema_rejects_a_tool_key_aws_will_not_accept(bad):
    doc = copy.deepcopy(SHIPPED)
    name = some_tool()
    doc["tools"][bad] = doc["tools"].pop(name)
    for spec in doc["agents"].values():
        if spec.get("tool") == name:
            spec["tool"] = bad
    messages = errors(doc)
    assert messages, f"the schema accepted tools.{bad}, which CloudFormation refuses"
    assert any("does not match" in m for m in messages), messages


def test_every_shipped_tool_key_is_deployable():
    """The other direction: whatever this workflow declares must survive BOTH AWS rules."""
    import re

    from app.orchestrator.registry import TARGET_NAME_RE

    gateway_target = re.compile(r"^([0-9a-zA-Z][-]?){1,100}$")
    cedar_policy = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
    for name in (SHIPPED.get("tools") or {}):
        assert TARGET_NAME_RE.match(name), f"tools.{name} fails the framework's own rule"
        # And the two AWS constraints it is the intersection of, asserted independently so
        # this test would notice if the framework's rule drifted away from either.
        assert gateway_target.match(name), f"tools.{name} is not a legal Gateway target name"
        assert cedar_policy.match(f"permit_{name}"), (
            f"permit_{name} is not a legal Cedar policy name")


@pytest.mark.parametrize("bad", ILLEGAL_TOOL_KEYS)
def test_the_registry_refuses_it_too_and_explains_the_contradiction(bad):
    """All three planes, because a customer meets whichever they run first — and the
    message has to explain WHY both separators are out, or the rule looks arbitrary and
    the obvious workaround (swap _ for -) is the other failure."""
    from conftest import workflow

    defn = {
        "orchestrator": {},
        "tools": {bad: {"type": "kb", "corpora": ["x"]}},
        "agents": {"a": {"name": "A", "runtime": "dedicated", "maxTokens": 100, "tool": bad}},
        "steps": [{"agent": "a"}],
    }
    with workflow(defn) as imp:
        registry = imp("app.orchestrator.registry")
        with pytest.raises(ValueError) as caught:
            registry.validate_tool_types()
    message = str(caught.value)
    assert bad in message
    assert "policyDocs" in message               # the camelCase rename to make
    assert "Gateway target" in message and "Cedar policy" in message
    assert "underscores" in message and "hyphens" in message
    # And that an agent id is NOT subject to the same rule, because that asymmetry is the
    # first thing a reader will doubt.
    assert "agent id" in message.lower()


def test_all_three_planes_carry_the_same_rule():
    """A rule one plane enforces and another does not is the drift app/vocabulary.json
    exists to prevent — here it would mean `cdk synth` accepting what `terraform plan`
    rejects, with CloudFormation as the tie-breaker."""
    pattern = "[A-Za-z][A-Za-z0-9]*"
    for path in (ORCH_ROOT / "app" / "orchestrator" / "registry.py",
                 ORCH_ROOT / "cdk" / "lib" / "orchestrator-stack.ts",
                 ORCH_ROOT / "terraform" / "tools.tf",
                 ORCH_ROOT / "app" / "workflow.schema.json"):
        assert pattern in path.read_text(), f"{path.name} does not carry the tool-key rule"
