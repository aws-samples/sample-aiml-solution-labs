"""The KB retrieve Lambda (orchestrator/kb_lambda/handler.py) — every retrieval
parameter now comes from the `kb` tool's entry in workflow.json, and ONE of them
deliberately does not come from the agent.

WHY THIS FILE EXISTS. The handler used to hardcode the request shape: depth from one env
var, and a filter that was always `{"equals": {"key": "doc_type", ...}}`. Two ordinary
requirements were therefore impossible without editing the framework — a corpus tagged
with anything other than `doc_type`, and any narrowing beyond a single equality.

THE PART THAT IS A SECURITY BOUNDARY, NOT A FEATURE. There are two filters, and the
difference is the whole model:

    the AGENT's    one scalar, its `corpus`, arriving as the `filter` ARGUMENT. The
                   generated Cedar permit is a scalar value match
                   (`["reference"].contains(context.input.filter)`), so the Gateway
                   refuses an ungranted corpus BEFORE this function runs.
    the TARGET's   `KB_STATIC_FILTER`, arbitrary Bedrock filter syntax, set in
                   workflow.json and unreachable by the caller.

That is why `corpusOperator` is restricted to single-value operators and why the rich
filter is target-level. A list-valued operator would make the agent's argument a list,
which the permit cannot express — so it would degrade to "has a filter at all" while
still looking enforced. These tests pin that asymmetry, because it is the kind of thing a
later "let the agent pass a full filter" convenience would quietly undo.
"""
from __future__ import annotations

import importlib
import json
import sys

import pytest
from conftest import ORCH_ROOT

KB_LAMBDA = ORCH_ROOT / "kb_lambda"


@pytest.fixture
def load(monkeypatch):
    """Import the handler with a given env, and capture what it asks Bedrock for.

    The module reads its config at import time (module-level constants), which is right
    for a Lambda — the values cannot change within a container's life — so each case
    needs a fresh import rather than a setter.
    """
    def _load(env: dict, retrieved: list | None = None):
        for key in ("KB_ID", "KB_NUM_RESULTS", "KB_SEARCH_TYPE", "KB_CORPUS_KEY",
                    "KB_CORPUS_OPERATOR", "KB_STATIC_FILTER", "KB_RERANK"):
            monkeypatch.delenv(key, raising=False)
        monkeypatch.setenv("KB_ID", "kb-123")
        for k, v in env.items():
            monkeypatch.setenv(k, v)
        monkeypatch.syspath_prepend(str(KB_LAMBDA))
        sys.modules.pop("handler", None)

        calls: list[dict] = []

        class _Stub:
            def retrieve(self, **kwargs):
                calls.append(kwargs)
                return {"retrievalResults": retrieved or []}

        import boto3
        monkeypatch.setattr(boto3, "client", lambda *a, **k: _Stub())
        mod = importlib.import_module("handler")
        return mod, calls

    return _load


def _vector_cfg(calls):
    return calls[0]["retrievalConfiguration"]["vectorSearchConfiguration"]


# ---------------------------------------------------------------------------
# The defaults, which are what an absent config key must produce
# ---------------------------------------------------------------------------

def test_with_no_config_the_behaviour_is_what_it_always_was(load):
    """Every new key is optional, so a workflow.json that declares none of them must
    retrieve exactly as before this change. Otherwise "opening this up" is a silent
    behaviour change for every existing deployment."""
    mod, calls = load({})
    mod.lambda_handler({"query": "q", "filter": "reference"}, None)
    assert _vector_cfg(calls) == {
        "numberOfResults": 5,
        "filter": {"equals": {"key": "doc_type", "value": "reference"}},
    }


def test_no_corpus_and_no_static_filter_means_no_filter_at_all(load):
    """An agent with no `corpus` retrieves across the whole store, which is what an
    unscoped agent asked for. The key must be ABSENT rather than an empty dict, which
    Bedrock rejects."""
    mod, calls = load({})
    mod.lambda_handler({"query": "q"}, None)
    assert "filter" not in _vector_cfg(calls)


def test_a_missing_query_is_refused_before_any_retrieval(load):
    mod, calls = load({})
    out = mod.lambda_handler({"filter": "reference"}, None)
    assert "error" in out and not calls


# ---------------------------------------------------------------------------
# The tuning knobs: config, not code
# ---------------------------------------------------------------------------

def test_the_corpus_key_and_operator_come_from_config(load):
    """The point of the change: a corpus need not be a folder name tagged `doc_type`. A
    customer whose documents already carry a customer id or product line sets two keys."""
    mod, calls = load({"KB_CORPUS_KEY": "product_line", "KB_CORPUS_OPERATOR": "startsWith"})
    mod.lambda_handler({"query": "q", "filter": "retail"}, None)
    assert _vector_cfg(calls)["filter"] == {
        "startsWith": {"key": "product_line", "value": "retail"}}


def test_there_is_no_search_type_knob_because_s3_vectors_rejects_hybrid(load):
    """A knob was added here, deployed, and removed the same day.

    S3 Vectors — the only vector store this framework provisions — refuses hybrid search:
    "HYBRID search type is not supported for search operation on index <id>". That left
    SEMANTIC, the default, as the only legal value. A key whose one working value is the
    default expresses nothing, and its other value failed at RETRIEVAL rather than at
    deploy — a green stack and an agent whose tool errors later. Found by configuring it
    and calling the real API, which is the only way it could have been found.
    """
    mod, calls = load({"KB_SEARCH_TYPE": "HYBRID"})
    mod.lambda_handler({"query": "q"}, None)
    # Even if the env var is present, nothing reads it.
    assert "overrideSearchType" not in _vector_cfg(calls)
    keys = json.loads((ORCH_ROOT / "app" / "keys.json").read_text())
    assert "searchType" not in keys["tool"]["keys"]
    assert "not supported" in (KB_LAMBDA / "handler.py").read_text()


def test_reranking_is_requested_in_bedrocks_own_shape(load):
    mod, calls = load({"KB_NUM_RESULTS": "10",
                       "KB_RERANK": json.dumps({"model": "amazon.rerank-v1:0", "count": 3})})
    mod.lambda_handler({"query": "q"}, None)
    rr = _vector_cfg(calls)["rerankingConfiguration"]
    assert rr["type"] == "BEDROCK_RERANKING_MODEL"
    cfg = rr["bedrockRerankingConfiguration"]
    assert cfg["numberOfRerankedResults"] == 3
    assert cfg["modelConfiguration"]["modelArn"].endswith("foundation-model/amazon.rerank-v1:0")


def test_a_full_model_arn_is_used_as_given(load):
    """A cross-region or inference-profile ARN must not be re-prefixed into nonsense."""
    arn = "arn:aws:bedrock:eu-west-1::foundation-model/cohere.rerank-v3-5:0"
    mod, calls = load({"KB_RERANK": json.dumps({"model": arn})})
    mod.lambda_handler({"query": "q"}, None)
    assert (_vector_cfg(calls)["rerankingConfiguration"]["bedrockRerankingConfiguration"]
            ["modelConfiguration"]["modelArn"]) == arn


def test_keeping_more_than_was_retrieved_is_clamped(load):
    """Reranking reorders what retrieval already found. Asking to keep 20 of 5 is a config
    mistake with a silent outcome — you pay for the rerank call and get the same list
    back — so it is clamped to the retrieval depth rather than forwarded."""
    mod, calls = load({"KB_NUM_RESULTS": "5",
                       "KB_RERANK": json.dumps({"model": "amazon.rerank-v1:0", "count": 20})})
    mod.lambda_handler({"query": "q"}, None)
    cfg = _vector_cfg(calls)["rerankingConfiguration"]["bedrockRerankingConfiguration"]
    assert cfg["numberOfRerankedResults"] == 5


def test_reranking_without_a_model_is_ignored_rather_than_half_configured(load):
    mod, calls = load({"KB_RERANK": json.dumps({"count": 3})})
    mod.lambda_handler({"query": "q"}, None)
    assert "rerankingConfiguration" not in _vector_cfg(calls)


# ---------------------------------------------------------------------------
# The boundary: which filter comes from where
# ---------------------------------------------------------------------------

def test_the_target_filter_applies_even_when_the_agent_sends_nothing(load):
    """It is the tool's filter, not the agent's. An agent that sends no `corpus` must
    still be inside it — otherwise "set on the target" would mean "applied when the
    caller cooperates", which is not a boundary."""
    static = {"equals": {"key": "tier", "value": "public"}}
    mod, calls = load({"KB_STATIC_FILTER": json.dumps(static)})
    mod.lambda_handler({"query": "q"}, None)
    assert _vector_cfg(calls)["filter"] == static


def test_the_two_filters_are_ANDed_so_the_target_can_only_narrow(load):
    """Both present must intersect. If the agent's replaced the target's, an agent could
    widen its own scope by supplying a corpus — the exact inversion this guards."""
    static = {"notEquals": {"key": "tier", "value": "restricted"}}
    mod, calls = load({"KB_STATIC_FILTER": json.dumps(static)})
    mod.lambda_handler({"query": "q", "filter": "reference"}, None)
    assert _vector_cfg(calls)["filter"] == {
        "andAll": [
            {"equals": {"key": "doc_type", "value": "reference"}},
            static,
        ]
    }


def test_a_single_filter_is_not_wrapped_in_a_one_member_andAll(load):
    """Bedrock rejects a one-member andAll, so wrapping unconditionally would turn the
    COMMON case — one filter, either kind — into a failed retrieval."""
    mod, calls = load({})
    mod.lambda_handler({"query": "q", "filter": "reference"}, None)
    assert "andAll" not in json.dumps(_vector_cfg(calls)["filter"])


def test_an_arbitrary_multi_condition_target_filter_survives_intact(load):
    """This is the capability the change unlocks: narrowing the framework could not
    express before. It is passed through untouched because it is Bedrock's syntax, not
    a shape this framework re-invents."""
    static = {"andAll": [
        {"equals": {"key": "tier", "value": "public"}},
        {"orAll": [{"equals": {"key": "region", "value": "emea"}},
                   {"equals": {"key": "region", "value": "amer"}}]},
    ]}
    mod, calls = load({"KB_STATIC_FILTER": json.dumps(static)})
    mod.lambda_handler({"query": "q"}, None)
    assert _vector_cfg(calls)["filter"] == static


def test_the_agent_cannot_send_a_filter_document_only_a_corpus_value(load):
    """THE BOUNDARY. The agent's argument is interpolated as a VALUE, never merged as a
    filter, so a caller that sends a whole filter document gets it treated as the corpus
    name it claims to be — which matches nothing — rather than having it honoured.

    If this ever fails, the Cedar permit has stopped being enforceable: it matches a
    scalar, so a structured argument would slip past it while still looking checked."""
    mod, calls = load({})
    injected = {"orAll": [{"equals": {"key": "doc_type", "value": "secret"}}]}
    mod.lambda_handler({"query": "q", "filter": json.dumps(injected)}, None)
    sent = _vector_cfg(calls)["filter"]
    assert set(sent) == {"equals"}
    assert sent["equals"]["key"] == "doc_type"
    assert sent["equals"]["value"] == json.dumps(injected)
    assert "orAll" not in sent


def test_a_malformed_target_filter_refuses_to_retrieve(load):
    """Not a silent `{}` fallback. Retrieving successfully with the filter dropped returns
    real chunks from outside the configured scope, and nothing downstream can tell. The
    error names the variable."""
    mod, _calls = load({"KB_STATIC_FILTER": "{not json"})
    with pytest.raises(RuntimeError, match="KB_STATIC_FILTER"):
        mod.lambda_handler({"query": "q"}, None)


# ---------------------------------------------------------------------------
# The config surface agrees with the handler
# ---------------------------------------------------------------------------

def test_only_single_value_operators_are_offered_in_the_vocabulary():
    """A list-valued operator would make the agent's argument a list, which the Cedar
    permit cannot express — so the permit would silently weaken to "has a filter"."""
    vocab = json.loads((ORCH_ROOT / "app" / "vocabulary.json").read_text())
    offered = vocab["kbCorpusOperators"]["values"]
    for listy in ("in", "notIn", "listContains"):
        assert listy not in offered, listy
    assert "Cedar" in vocab["kbCorpusOperators"]["$comment"]


def test_the_shipped_kb_tool_declares_nothing_it_does_not_need():
    """The sample keeps the defaults, which is what makes the "absent key changes nothing"
    test above meaningful rather than theoretical."""
    wf = json.loads((ORCH_ROOT / "app" / "workflow.json").read_text())
    kb = next((t for t in wf["tools"].values() if t["type"] == "kb"), None)
    if kb is None:
        pytest.skip("this workflow declares no kb tool")
    for tuning in ("corpusKey", "corpusOperator", "filter", "rerank",
                   "embeddingModel", "dimensions"):
        assert tuning not in kb, (
            f"the sample sets {tuning!r}; it is meant to demonstrate the DEFAULTS")
