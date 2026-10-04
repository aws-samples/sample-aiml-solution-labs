"""An asset's `version` must agree with the run count the framework reports.

Two independent places number the same thing, and they have to match:

  * `nodes.make_agent_node` appends each run to the `history` channel as
    `version = len(prior) + 1`, which is what the timeline shows ("complete (v1)")
    and what the UI's version picker lists;
  * `assets.prior_version` stamps `"version"` INSIDE the asset the agent returns.

They disagreed. `prior_version` was `int(prev.get("version", 1)) + 1` with no check
for "there is no previous asset", so on a first run `prev` was `{}`, the default 1
was incremented, and every asset came out `"version": 2` under a timeline that said
v1. Measured on three live runs: 24 assets, all stamped 2, every agent's `history`
exactly one entry long, no revise anywhere. A reviewer comparing the two numbers
cannot tell which one to believe, and "is this the output I already rejected?" is
precisely the question the version is there to answer.
"""

from conftest import wf, workflow


class Ctx:
    """The two attributes `prior_version` touches."""

    def __init__(self, agent_id="a", prior=None):
        self.agent_id = agent_id
        self._prior = prior

    def input(self, agent_id):
        return self._prior if agent_id == self.agent_id else None


def version_for(prior):
    with workflow(wf([{"agent": "a"}])) as imp:
        return imp("app.common.assets").prior_version(Ctx(prior=prior))


def test_the_first_run_is_version_1():
    """The regression. No previous asset means this is run one."""
    assert version_for(None) == 1
    assert version_for("") == 1


def test_a_re_run_counts_from_the_previous_asset():
    assert version_for('{"version": 1}') == 2
    assert version_for('{"version": 3}') == 4


def test_output_that_is_not_an_asset_at_all_is_treated_as_no_previous_run():
    """`parse` returns {} for anything unparseable, and {} must not be mistaken for
    a version-1 asset — that is exactly how the off-by-one happened."""
    assert version_for("not json") == 1
    assert version_for("[1, 2, 3]") == 1   # valid JSON, but not an object


def test_a_previous_asset_with_an_unusable_version_still_counts_as_a_re_run():
    """It happened, so this run is at least the second. Restarting at 1 would claim
    the earlier output never existed."""
    assert version_for('{"summary": "x"}') == 2      # an asset, but no version field
    assert version_for('{"version": "two"}') == 2    # unreadable
    assert version_for('{"version": null}') == 2


def test_a_literally_empty_object_counts_as_no_previous_run():
    """`parse` returns {} both for "nothing stored" and for a stored "{}", so the two
    cannot be told apart. Documented rather than pretended otherwise: an agent whose
    asset is an empty object has produced nothing to version from."""
    assert version_for("{}") == 1


def test_the_asset_version_tracks_the_history_version_across_a_revise_cycle():
    """The invariant that matters: for run N the asset says N, which is what
    `make_agent_node` independently records as `len(history) + 1`."""
    prior_output = None
    for run in (1, 2, 3):
        version = version_for(prior_output)
        assert version == run, f"run {run} stamped version {version}"
        # What the agent stores becomes the next run's previous asset.
        prior_output = f'{{"version": {version}}}'
