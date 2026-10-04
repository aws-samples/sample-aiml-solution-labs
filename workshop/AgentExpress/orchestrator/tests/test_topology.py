"""Topology derivation from `workflow.json`.

Everything here used to be implicit. `FIRST_AGENT_ID` was the hardcoded string
"intake", so renaming the first agent silently dropped the request brief from
every downstream prompt — no error, just worse answers. These tests pin the
derivation so that cannot come back.
"""

from conftest import all_ids, expected_first, expected_last, expected_upstream, wf, workflow

# --- step_agents: one function, three step shapes --------------------------

def test_step_agents_handles_every_step_shape():
    with workflow(wf([{"agent": "a"}])) as imp:
        cfg = imp("app.common.config")
        assert cfg.step_agents({"agent": "a"}) == ["a"]
        assert cfg.step_agents({"parallel": ["x", "y"]}) == ["x", "y"]
        assert cfg.step_agents({"sequence": ["p", "q"]}) == ["p", "q"]
        # A malformed step yields nothing rather than raising: the workflow
        # validators in both IaC paths are what reject it, at plan time.
        assert cfg.step_agents({}) == []


# --- FIRST / LAST agent ----------------------------------------------------

def test_first_and_last_agent_for_single_agent_steps():
    with workflow(wf([{"agent": "start"}, {"agent": "finish"}])) as imp:
        cfg = imp("app.common.config")
        assert cfg.FIRST_AGENT_ID == "start"
        assert cfg.LAST_AGENT_ID == "finish"


def test_first_agent_can_be_any_name():
    """The regression this exists for: it was hardcoded to "intake"."""
    with workflow(wf([{"agent": "triage"}, {"agent": "decide"}])) as imp:
        assert imp("app.common.config").FIRST_AGENT_ID == "triage"


def test_first_and_last_agent_when_the_edge_steps_are_groups():
    with workflow(wf([{"parallel": ["p1", "p2", "p3"]},
                      {"sequence": ["s1", "s2"]}])) as imp:
        cfg = imp("app.common.config")
        # A parallel first step: the brief comes from the group's first member.
        assert cfg.FIRST_AGENT_ID == "p1"
        # A sequence last step: the result is the chain's final agent.
        assert cfg.LAST_AGENT_ID == "s2"


def test_first_equals_last_for_a_one_agent_workflow():
    with workflow(wf([{"agent": "only"}])) as imp:
        cfg = imp("app.common.config")
        assert cfg.FIRST_AGENT_ID == cfg.LAST_AGENT_ID == "only"


def test_shipped_workflow_topology(shipped):
    """Derived from the shipped steps, not restated: renaming an agent in
    workflow.json must not turn this red. See conftest for why."""
    with workflow(shipped) as imp:
        cfg = imp("app.common.config")
        assert expected_first(shipped) == cfg.FIRST_AGENT_ID
        assert expected_last(shipped) == cfg.LAST_AGENT_ID
        assert list(shipped["agents"].keys()) == cfg.NODE_IDS


# --- upstream_of: the rule that makes "add an agent" a config-only change ---

def test_upstream_is_reverse_order_so_the_newest_asset_comes_first():
    steps = [{"agent": "a"}, {"agent": "b"}, {"agent": "c"}]
    with workflow(wf(steps)) as imp:
        assert imp("app.common.config").upstream_of("c") == ["b", "a"]


def test_parallel_peers_are_not_upstream_of_each_other():
    """A parallel group's members run concurrently, so none may depend on another.
    Treating them as upstream would make the group's output order-dependent."""
    steps = [{"agent": "intake"}, {"parallel": ["r1", "r2", "r3"]}]
    with workflow(wf(steps)) as imp:
        cfg = imp("app.common.config")
        for peer in ("r1", "r2", "r3"):
            assert cfg.upstream_of(peer) == ["intake"]


def test_sequence_members_before_this_one_are_upstream():
    steps = [{"agent": "intake"}, {"sequence": ["first", "second", "third"]}]
    with workflow(wf(steps)) as imp:
        cfg = imp("app.common.config")
        assert cfg.upstream_of("first") == ["intake"]
        assert cfg.upstream_of("second") == ["first", "intake"]
        assert cfg.upstream_of("third") == ["second", "first", "intake"]


def test_a_new_parallel_agent_is_picked_up_downstream_with_no_code_change():
    """The whole point of deriving upstream from the topology: adding a research
    agent to the group must make the synthesis agent read it automatically."""
    base = [{"agent": "intake"}, {"parallel": ["r1", "r2"]}, {"agent": "analysis"}]
    with workflow(wf(base)) as imp:
        assert imp("app.common.config").upstream_of("analysis") == ["r2", "r1", "intake"]

    grown = [{"agent": "intake"}, {"parallel": ["r1", "r2", "r3"]}, {"agent": "analysis"}]
    with workflow(wf(grown)) as imp:
        assert imp("app.common.config").upstream_of("analysis") == ["r3", "r2", "r1", "intake"]


def test_first_agent_has_no_upstream():
    with workflow(wf([{"agent": "a"}, {"agent": "b"}])) as imp:
        assert imp("app.common.config").upstream_of("a") == []


def test_unknown_agent_raises_instead_of_returning_everything():
    """The synthesis agents call `upstream_of("<their own id>")` at import. If the
    agent was renamed in workflow.json but not in the module, this used to fall off
    the end of the loop and return EVERY agent — including the caller itself, so the
    agent would synthesize from its own previous output. It must fail loudly."""
    import pytest

    with workflow(wf([{"agent": "a"}, {"agent": "b"}])) as imp:
        cfg = imp("app.common.config")
        with pytest.raises(ValueError, match="no `steps` entry runs that agent"):
            cfg.upstream_of("renamed_report")


def test_shipped_workflow_upstreams(shipped):
    """Every agent, not a spot-check: for each one, upstream_of must equal the
    agents from earlier steps plus earlier members of its own sequence, newest
    first, with parallel peers excluded."""
    with workflow(shipped) as imp:
        cfg = imp("app.common.config")
        for agent_id in all_ids(shipped):
            assert cfg.upstream_of(agent_id) == expected_upstream(shipped, agent_id), agent_id
        # The first agent has nothing upstream; the last can see everything before it —
        # except, when the last stage is a parallel group, its own peers, which are never
        # upstream of each other. (This assumed a single last step, which the sample has
        # and a customer's workflow need not.)
        assert cfg.upstream_of(expected_first(shipped)) == []
        peers = len(shipped["steps"][-1].get("parallel") or []) or 1
        assert len(cfg.upstream_of(expected_last(shipped))) == len(all_ids(shipped)) - peers
