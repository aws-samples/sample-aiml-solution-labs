"""Unit tests for the dual-anchor DriftScorer — NO AWS creds (stub embeddings).

We inject a deterministic embed_fn so the dual-anchor MATH is tested in isolation from
Bedrock. A separate live check (run manually with creds) validates real embeddings.

    python3 iris_goalfence/tests/test_drift.py
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from iris_goalfence.drift import DriftScorer  # noqa: E402


# A toy 3-dim "embedding space": axis 0 = on-goal-ness, axis 1 = off-goal-ness, axis 2 noise.
# Stub maps keywords to coordinates so we can reason about cosine deterministically.
def _stub_embed(text: str) -> list[float]:
    t = text.lower()
    on = 1.0 if any(k in t for k in ("my order", "shipment", "my own", "eligible")) else 0.0
    off = 1.0 if any(k in t for k in ("all", "every", "export", "bulk", "external")) else 0.0
    # ensure non-zero vector
    return [on + 0.1, off + 0.1, 0.05]


EXAMPLES = ("where is my order", "my shipment status", "refund my own order if eligible")
ANTI = ("export all records", "every customer in bulk", "share data external")


def _scorer():
    return DriftScorer(EXAMPLES, ANTI, embed_fn=_stub_embed)


def test_requires_both_example_sets():
    try:
        DriftScorer((), ANTI, embed_fn=_stub_embed)
    except ValueError:
        return
    raise AssertionError("expected ValueError when on-goal examples missing")


def test_on_goal_scores_low():
    s = _scorer()
    d = s.score([("user", "where is my order A1"), ("assistant", "your order shipped")])
    assert d < 0.5, f"on-goal should score <0.5, got {d}"


def test_off_goal_scores_high():
    s = _scorer()
    d = s.score([("user", "export all customer records"), ("assistant", "compiling all")])
    assert d > 0.5, f"off-goal should score >0.5, got {d}"


def test_empty_trajectory_neutral():
    assert _scorer().score([]) == 0.5


def test_only_user_assistant_turns_embedded():
    # system / tool turns must be dropped from the embedded text.
    s = _scorer()
    txt = s.trajectory_text([("system", "SECRET PROMPT export all"),
                             ("user", "where is my order"),
                             ("tool", "bulk dump"),
                             ("assistant", "shipped")])
    assert "SECRET" not in txt and "bulk dump" not in txt
    assert "my order" in txt and "shipped" in txt


def test_centroids_cached_one_embed_per_text():
    calls = {"n": 0}

    def counting(text):
        calls["n"] += 1
        return _stub_embed(text)

    s = DriftScorer(EXAMPLES, ANTI, embed_fn=counting)
    base = len(EXAMPLES) + len(ANTI)
    s.score([("user", "where is my order")])          # embeds centroids (once) + 1 traj
    assert calls["n"] == base + 1
    s.score([("user", "my shipment status")])          # centroids cached -> +1 traj only
    assert calls["n"] == base + 2


if __name__ == "__main__":
    fns = [f for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    passed = failed = 0
    for fn in fns:
        try:
            fn(); passed += 1; print(f"  ok   {fn.__name__}")
        except AssertionError as e:
            failed += 1; print(f"  FAIL {fn.__name__}: {e}")
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(1 if failed else 0)
