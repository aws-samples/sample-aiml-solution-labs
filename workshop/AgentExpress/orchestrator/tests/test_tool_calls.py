"""How a `tools` entry becomes an actual tool call.

Three pure functions in app/features/gateway/client.py carry the whole "any MCP
server / any REST API from config alone" claim:

  _tool_arguments — the argument shape, including the parameter NAME, from config
  _select_tool    — which published tool to invoke, given the Gateway's naming
  _extract_chunks — flattening a result into evidence, keeping its citations

They are the config plane for the tool layer, so they are worth pinning exactly.
`TOOLS` is a module-level binding read from config, so each test patches it
directly rather than re-importing the world.
"""

import json

import pytest


@pytest.fixture()
def client():
    from app.features.gateway import client as c
    return c


def with_tools(client, tools, monkeypatch):
    monkeypatch.setattr(client, "TOOLS", tools)


class FakeTool:
    """Stand-in for a langchain MCP tool: `_select_tool` only reads `.name`."""

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return f"FakeTool({self.name!r})"


# --- _tool_arguments -------------------------------------------------------

def test_the_query_parameter_name_is_config_not_code(client, monkeypatch):
    """The reason ANY MCP server can be wired up from JSON: servers disagree about
    what to call the query parameter, so `arg` names it."""
    with_tools(client, {"t": {"type": "mcp", "arg": "search_phrase"}}, monkeypatch)
    assert client._tool_arguments("t", "vpc peering") == {"search_phrase": "vpc peering"}


def test_query_is_the_default_parameter_name(client, monkeypatch):
    with_tools(client, {"t": {"type": "mcp"}}, monkeypatch)
    assert client._tool_arguments("t", "hello") == {"query": "hello"}


def test_fixed_args_are_merged_on_every_call(client, monkeypatch):
    """A server whose tool needs a required argument the agent knows nothing about
    (a repo name, a page size) is reachable without touching the app."""
    with_tools(client, {"wiki": {"type": "mcp", "arg": "question",
                                 "args": {"repoName": "aws/aws-cdk", "limit": 5}}},
               monkeypatch)
    assert client._tool_arguments("wiki", "how do I bootstrap?") == {
        "question": "how do I bootstrap?", "repoName": "aws/aws-cdk", "limit": 5}


def test_an_unknown_tool_key_still_produces_a_usable_default(client, monkeypatch):
    with_tools(client, {}, monkeypatch)
    assert client._tool_arguments("missing", "q") == {"query": "q"}


def test_websearch_uses_the_connectors_documented_schema(client, monkeypatch):
    with_tools(client, {"ws": {"type": "websearch", "maxResults": 7}}, monkeypatch)
    assert client._tool_arguments("ws", "serverless") == {
        "query": "serverless", "maxResults": 7}


def test_websearch_query_is_clamped_to_200_characters(client, monkeypatch):
    """The connector's documented cap. Clamping here means an over-long brief
    produces a shorter query instead of a rejected call."""
    with_tools(client, {"ws": {"type": "websearch"}}, monkeypatch)
    args = client._tool_arguments("ws", "x" * 500)
    assert len(args["query"]) == 200


def test_websearch_omits_maxresults_and_filters_when_not_configured(client, monkeypatch):
    """An empty filter object is not the same as no filter object — send the
    minimal documented payload rather than empty scaffolding."""
    with_tools(client, {"ws": {"type": "websearch"}}, monkeypatch)
    assert client._tool_arguments("ws", "q") == {"query": "q"}


def test_websearch_date_bounds_nest_under_publisheddatefilter(client, monkeypatch):
    with_tools(client, {"ws": {"type": "websearch",
                              "publishedFrom": "2025-01-01T00:00:00Z",
                              "publishedTo": "2026-01-01T00:00:00Z"}}, monkeypatch)
    assert client._tool_arguments("ws", "q")["filters"]["publishedDateFilter"] == {
        "from": "2025-01-01T00:00:00Z", "to": "2026-01-01T00:00:00Z"}


def test_websearch_accepts_one_date_bound(client, monkeypatch):
    with_tools(client, {"ws": {"type": "websearch",
                              "publishedFrom": "2026-01-01T00:00:00Z"}}, monkeypatch)
    assert client._tool_arguments("ws", "q")["filters"]["publishedDateFilter"] == {
        "from": "2026-01-01T00:00:00Z"}


def test_the_domain_filter_never_reaches_the_request(client, monkeypatch):
    """`domains` is applied on the Gateway target, and must stay INVISIBLE here — that
    is what makes it a boundary rather than a preference.

    There used to be a second, request-level pair (`includeDomains`/`excludeDomains`)
    that this process DID send, and a test asserting it nested correctly under
    `filters.domainFilter`. Both are gone: four keys expressed one intent, the pair with
    the obvious name was the weaker one, and since both lists came from the same config
    file setting both only ever produced their intersection. What is left to assert is
    that no domain list leaves this process at all — including one a caller put in
    `domains` by mistake, expecting it to be sent.
    """
    with_tools(client, {"ws": {"type": "websearch",
                               "domains": {"include": ["aws.amazon.com"],
                                           "exclude": ["spam.example"]}}}, monkeypatch)
    args = client._tool_arguments("ws", "q")
    assert args == {"query": "q"}
    assert "aws.amazon.com" not in json.dumps(args)
    assert "domainFilter" not in json.dumps(args)


def test_the_app_is_not_even_given_the_domain_lists(monkeypatch):
    """One level lower than the test above: `domains` is stripped from TOOLS_JSON by both
    IaC projections, so the runtime cannot send a domain filter even if a future edit to
    `_tool_arguments` tried to. Asserted on the projection the app itself performs when
    it reads workflow.json directly (local dev), which is the same allow-list."""
    from conftest import workflow

    defn = {
        "orchestrator": {},
        "tools": {"ws": {"type": "websearch", "maxResults": 5,
                         "domains": {"include": ["aws.amazon.com"]}}},
        "agents": {"a": {"name": "A", "runtime": "dedicated", "maxTokens": 100,
                         "tool": "ws"}},
        "steps": [{"agent": "a"}],
    }
    with workflow(defn) as imp:
        tools = imp("app.common.config").TOOLS
    assert tools["ws"] == {"type": "websearch", "maxResults": 5}
    assert "domains" not in json.dumps(tools)


# --- _select_tool ----------------------------------------------------------

def test_select_tool_matches_the_gateways_target_prefix(client, monkeypatch):
    with_tools(client, {"kb": {"type": "kb"}}, monkeypatch)
    tools = [FakeTool("kb___retrieve"), FakeTool("websearch___WebSearch")]
    assert client._select_tool(tools, "kb").name == "kb___retrieve"


def test_select_tool_handles_a_doubly_prefixed_name(client, monkeypatch):
    """The Gateway composes '<target>___<tool>', and AWS's managed servers ALSO
    prefix their tools with 'aws___'. So the published name is
    'docs___aws___search_documentation' and `call` is the name minus the target
    prefix. Nothing splits on '___' — this is the test that says so."""
    with_tools(client, {"docs": {"type": "mcp", "call": "aws___search_documentation"}},
               monkeypatch)
    tools = [FakeTool("docs___aws___read_documentation"),
             FakeTool("docs___aws___search_documentation"),
             FakeTool("docs___aws___list_regions")]
    assert client._select_tool(tools, "docs").name == "docs___aws___search_documentation"


def test_select_tool_refuses_to_guess_between_several_candidates(client, monkeypatch):
    """A target publishing five tools with no `call` set returns None, which the
    caller turns into a ToolUnavailable naming the candidates. Picking arbitrarily
    would call the wrong tool with the wrong arguments and look like a bad answer."""
    with_tools(client, {"docs": {"type": "mcp"}}, monkeypatch)
    tools = [FakeTool("docs___aws___read_documentation"),
             FakeTool("docs___aws___search_documentation")]
    assert client._select_tool(tools, "docs") is None


def test_select_tool_takes_a_single_candidate_without_a_call_field(client, monkeypatch):
    with_tools(client, {"kb": {"type": "kb"}}, monkeypatch)
    assert client._select_tool([FakeTool("kb___retrieve")], "kb").name == "kb___retrieve"


def test_select_tool_falls_back_to_a_substring_match(client, monkeypatch):
    """For a target that does not prefix its tools at all."""
    with_tools(client, {"kb": {"type": "kb"}}, monkeypatch)
    assert client._select_tool([FakeTool("KbRetrieve")], "kb").name == "KbRetrieve"


def test_select_tool_returns_none_when_nothing_matches(client, monkeypatch):
    with_tools(client, {"kb": {"type": "kb"}}, monkeypatch)
    assert client._select_tool([FakeTool("websearch___WebSearch")], "kb") is None


def test_select_tool_is_case_insensitive(client, monkeypatch):
    with_tools(client, {"docs": {"type": "mcp", "call": "AWS___Search_Documentation"}},
               monkeypatch)
    tools = [FakeTool("docs___aws___search_documentation")]
    assert client._select_tool(tools, "docs") is tools[0]


# --- _extract_chunks -------------------------------------------------------

# --- the MCP envelope ------------------------------------------------------
# The bug these exist for: a tool result does NOT arrive as the payload the tool
# returned. MCP wraps it as a list of content blocks with the payload as a JSON
# STRING one level down. The old code only matched a top-level dict with a
# "results" key, so every real tool fell through to a `json.dumps(...)[:2000]`
# fallback — evidence cut to 2000 characters mid-token, citations left buried in
# the JSON text, and the whole citation-retention path dead in practice.
#
# The envelopes below are the shapes the four live tools actually returned, taken
# from the telemetry of a real run.

def mcp(payload) -> list:
    """Wrap a payload the way MCP delivers it to the caller."""
    return [{"type": "text", "text": json.dumps(payload)}]


def test_the_mcp_envelope_is_unwrapped_before_extraction(client):
    out = client._extract_chunks(mcp({"results": [
        {"text": "body", "title": "T", "url": "https://e.test/a"}]}))
    assert out.startswith("[1]")
    assert "https://e.test/a" in out
    # The failure signature of the old code: raw escaped JSON reaching the model.
    assert '\\"' not in out
    assert not out.lstrip().startswith("[{")


def test_web_search_envelope_keeps_every_citation(client):
    out = client._extract_chunks(mcp({"id": "abf2b2fd", "results": [
        {"text": "first", "title": "One", "url": "https://e.test/1",
         "publishedDate": "2026-07-05"},
        {"text": "second", "title": "Two", "url": "https://e.test/2"}]}))
    for expected in ("https://e.test/1", "https://e.test/2", "One", "Two", "2026-07-05"):
        assert expected in out


def test_aws_knowledge_mcp_envelope_is_understood(client):
    """That server nests its list at content.result and calls the prose `context`,
    not `text` — so both the list location and the field name have to be looked up
    rather than assumed."""
    out = client._extract_chunks(mcp({"content": {"result": [
        {"rank_order": 1, "title": "Agentic AI patterns",
         "context": "## Objectives\n\nThis guide provides a design framework.",
         "url": "https://docs.aws.amazon.com/prescriptive-guidance/x.html"}]}}))
    assert out.startswith("[1]")
    assert "https://docs.aws.amazon.com/prescriptive-guidance/x.html" in out
    assert "This guide provides a design framework." in out


def test_kb_envelope_keeps_the_s3_source_as_the_citation(client):
    out = client._extract_chunks(mcp({"query": "q", "count": 1, "results": [
        {"text": "chunk body", "score": 0.62,
         "source": "s3://bucket/reference/notes.md"}]}))
    assert "s3://bucket/reference/notes.md" in out
    assert "chunk body" in out


def test_evidence_is_no_longer_capped_at_2000_characters(client):
    """The exact regression. Four live tools each returned precisely 2000 chars."""
    out = client._extract_chunks(mcp({"results": [
        {"text": "x" * 3000, "title": "T", "url": "https://e.test/a"}]}))
    assert len(out) > 2000


def test_an_unrecognised_shape_keeps_the_characters_the_source_wrote(client):
    """The evidence string IS the grounding baseline for that agent's output
    (app/common/rules.py checks figures against it). Serialising it with
    ensure_ascii defaults meant a source that wrote "2–4 weeks" reached the model
    as "2\\u20134 weeks", so an agent quoting the page verbatim was told its figure
    was unsupported. Any escaping here is invisible until it silently invalidates
    a rule."""
    out = client._extract_chunks(mcp({"unexpected": {
        "note": "rollout takes 2\u20134 weeks \u2014 per the vendor\u2019s guide"}}))
    assert "2\u20134 weeks" in out
    assert "\\u2013" not in out


def test_the_evidence_budget_is_enforced_and_labelled(client):
    """A cap is fine; a SILENT cap is not. The agents could see their evidence
    ended mid-word but could not tell whether the source was incomplete or the
    framework had trimmed it."""
    out = client._extract_chunks(mcp({"results": [
        {"text": "y" * 9000, "title": f"doc {i}", "url": f"https://e.test/{i}"}
        for i in range(20)]}))
    assert len(out) <= client.MAX_EVIDENCE_CHARS + 400
    assert "omitted" in out                            # says results were dropped
    assert "truncated by the framework" in out         # says text was clipped


def test_several_text_blocks_in_one_envelope_are_all_read(client):
    out = client._extract_chunks([
        {"type": "text", "text": json.dumps({"results": [{"text": "from block one"}]})},
        {"type": "text", "text": json.dumps({"results": [{"text": "from block two"}]})},
    ])
    assert "from block one" in out


def test_a_bare_list_of_results_is_accepted(client):
    out = client._extract_chunks(mcp([{"text": "a", "url": "https://e.test/a"}]))
    assert out.startswith("[1]")
    assert "https://e.test/a" in out


def test_records_with_no_prose_field_are_the_evidence_not_nothing(client):
    """A keyword API answers [{"word", "score"}]: no passage to cite, but a full answer.
    Observed live (Datamuse): every record was dropped as textless and the agent was
    told "(no matching context)"."""
    out = client._extract_chunks(mcp([{"word": "mom and pop store", "score": 38328},
                                      {"word": "proprietor", "score": 27011}]))
    assert "mom and pop store" in out and "proprietor" in out
    assert "no matching context" not in out


def test_an_unrecognised_shape_is_kept_readable_not_dropped(client):
    out = client._extract_chunks(mcp({"unexpected": "shape"}))
    assert "unexpected" in out and "shape" in out


def test_plain_prose_in_the_envelope_survives(client):
    assert "just prose" in client._extract_chunks(
        [{"type": "text", "text": "just prose, not json"}])


def test_extract_chunks_retains_title_url_and_date(client):
    """The regression this exists for: this function used to keep only `text`. With
    the URLs stripped, any URL in the model's output was one it invented — and Web
    Search's terms require the citations to be retained and displayed."""
    result = {"results": [
        {"text": "Lambda scales automatically.",
         "title": "AWS Lambda FAQs", "url": "https://aws.amazon.com/lambda/faqs/",
         "publishedDate": "2026-02-01"},
    ]}
    out = client._extract_chunks(result)
    assert "https://aws.amazon.com/lambda/faqs/" in out
    assert "AWS Lambda FAQs" in out
    assert "2026-02-01" in out
    assert "Lambda scales automatically." in out


def test_extract_chunks_numbers_results_so_a_finding_can_cite_one(client):
    result = {"results": [{"text": "first"}, {"text": "second"}]}
    out = client._extract_chunks(result)
    assert out.startswith("[1]")
    assert "[2]" in out
    assert "\n---\n" in out


def test_extract_chunks_omits_fields_a_source_does_not_have(client):
    """KB chunks carry no title/url/date. Their absence must not produce empty
    labels that look like a missing citation."""
    out = client._extract_chunks({"results": [{"text": "kb chunk"}]})
    assert out == "[1]\nkb chunk"


def test_extract_chunks_parses_a_json_string_result(client):
    payload = json.dumps({"results": [{"text": "t", "url": "https://x.test/a"}]})
    assert "https://x.test/a" in client._extract_chunks(payload)


def test_extract_chunks_skips_results_with_no_text(client):
    """A result with no prose is dropped; a result that IS prose is kept.

    A bare string in a results list is a legitimate shape — some tools return
    `["first", "second"]` — so it is emitted rather than discarded. Only the
    textless entry disappears."""
    out = client._extract_chunks({"results": [
        {"url": "https://x.test/empty"}, {"text": "real"}, "a bare string result"]})
    assert "[1]" not in out          # the textless entry contributed nothing
    assert "[2]\nreal" in out
    assert "a bare string result" in out


def test_extract_chunks_reports_an_empty_result_set_explicitly(client):
    """An empty answer is real information — the research runner turns this into a
    named data limitation rather than letting the model fill the gap."""
    assert client._extract_chunks({"results": []}) == "(no matching context)"


def test_extract_chunks_passes_through_unparseable_text(client):
    assert client._extract_chunks("plain text, not json") == "plain text, not json"


def test_extract_chunks_serialises_an_unrecognised_shape(client):
    out = client._extract_chunks({"unexpected": "shape"})
    assert json.loads(out) == {"unexpected": "shape"}


# --- type=lambda -----------------------------------------------------------
# A customer's own function, fronting whatever the Gateway cannot reach directly.
# From the app's side it is just another Gateway tool: the argument shape and the
# tool selection come from the same three config fields.

def test_a_lambda_tool_uses_the_generic_argument_shape(client, monkeypatch):
    with_tools(client, {"claims": {"type": "lambda", "arg": "question",
                                   "args": {"limit": 100}}}, monkeypatch)
    assert client._tool_arguments("claims", "how many open claims?") == {
        "question": "how many open claims?", "limit": 100}


def test_a_lambda_tool_defaults_to_the_query_parameter(client, monkeypatch):
    with_tools(client, {"claims": {"type": "lambda"}}, monkeypatch)
    assert client._tool_arguments("claims", "q") == {"query": "q"}


def test_a_lambda_tools_deploy_only_fields_never_reach_the_call(client, monkeypatch):
    """lambdaArn and toolSchema configure the TARGET; sending them as arguments
    would be a schema violation the function would reject."""
    with_tools(client, {"claims": {
        "type": "lambda", "arg": "question",
        "lambdaArn": "arn:aws:lambda:us-east-1:123456789012:function:query-claims",
        "toolSchema": [{"name": "query_claims", "properties": {"question": {}}}],
    }}, monkeypatch)
    args = client._tool_arguments("claims", "q")
    assert args == {"question": "q"}


def test_select_tool_finds_a_lambda_tool_by_its_declared_name(client, monkeypatch):
    """The Gateway publishes a Lambda's tools as '<target>___<toolName>', same as
    any other target, so `call` resolves them the same way."""
    with_tools(client, {"claims": {"type": "lambda", "call": "query_claims"}}, monkeypatch)
    tools = [FakeTool("claims___query_claims"), FakeTool("claims___list_tables")]
    assert client._select_tool(tools, "claims").name == "claims___query_claims"


def test_select_tool_refuses_to_guess_between_a_lambdas_several_tools(client, monkeypatch):
    """Which is why both IaC paths REQUIRE `call` once a lambda tool declares more
    than one toolSchema entry — the config guard exists to stop this at plan time."""
    with_tools(client, {"claims": {"type": "lambda"}}, monkeypatch)
    tools = [FakeTool("claims___query_claims"), FakeTool("claims___list_tables")]
    assert client._select_tool(tools, "claims") is None


def test_every_tool_type_has_an_evidence_label(client, monkeypatch):
    """A type with no label falls back to the generic "TOOL" heading, so the model is
    never told what kind of source the evidence came from. Cheap to forget when
    adding a type, so pin it."""
    from app.subagents._shared.research import _EVIDENCE_LABELS

    assert set(_EVIDENCE_LABELS) == {"kb", "websearch", "mcp", "openapi", "lambda", "apigateway"}
    assert all(v and v.isupper() for v in _EVIDENCE_LABELS.values())


# --- kb tool lookup --------------------------------------------------------

def test_kb_tool_key_is_found_by_type_not_by_name(client, monkeypatch):
    """So the KB tool can be called anything in workflow.json."""
    with_tools(client, {"websearch": {"type": "websearch"},
                       "company_docs": {"type": "kb"}}, monkeypatch)
    assert client._kb_tool_key() == "company_docs"


def test_retrieve_with_no_kb_declared_raises_instead_of_guessing_a_name(client, monkeypatch):
    """It used to fall back to the literal "kb", which is THIS SAMPLE's key.

    A customer who names theirs `policies`, and then calls ctx.retrieve() from an
    agent that is not bound to a Knowledge Base, got a Gateway call for a tool called
    "kb" that does not exist — and an empty retrieval reported as a successful one.
    The only hardcoded sample tool label in framework code, and this is it.
    """
    from app.common.errors import ToolUnavailable

    with_tools(client, {"websearch": {"type": "websearch"}}, monkeypatch)
    with pytest.raises(ToolUnavailable, match='type="kb"'):
        client._kb_tool_key()
