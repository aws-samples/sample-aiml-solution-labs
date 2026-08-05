"""The Charter — the fence's policy, expressed as DATA.

A charter is a frozen bundle of the agent's chartered goal plus the hard bounds on how
much / whose data it may touch and which actions need extra oversight. It is NOT a prose
instruction the model can be argued out of — the engine enforces these bounds at the
action boundary regardless of what the model decided to do.

Every field maps to exactly one rule (see rules.py). A rule whose config is empty/None
is simply inert, so a minimal charter (just a goal + caps) is valid and each dimension
can be switched on independently for the demo.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Charter:
    # --- identity of the goal (used by the drift rule + operator-visible reasoning) ----
    goal: str = ""

    # --- SCALE bounds -----------------------------------------------------------------
    # max_actions: cumulative tool calls allowed in one trajectory (the bulk-sweep guard).
    # max_records: cumulative rows tools may return before the trajectory is a mass harvest.
    max_actions: int = 5
    max_records: int = 100
    # Per-call argument markers that imply an ALL/BULK read (the exfil shape). A single
    # call whose args contain any of these is out of charter for a per-record agent.
    bulk_arg_markers: tuple[str, ...] = (
        "all", "every", "entire", "bulk", "export", "dump", "*",
        "select *", "limit 1000", "no limit",
    )

    # --- SCOPE ------------------------------------------------------------------------
    # Argument keys that name the TARGET customer of an action. If any is present and its
    # value != the verified caller, the scope rule denies (cross-actor access). Empty =>
    # scope rule inert (e.g. for tools that never carry a target id).
    scope_arg_keys: tuple[str, ...] = ("customer_id", "target_customer", "account_id")
    # Tools exempt from the scope check (they act only on the caller by construction — the
    # gateway injects the id — so a stray customer_id arg shouldn't trip the fence).
    scope_exempt_tools: frozenset[str] = frozenset()

    # --- VALUE / irreversibility (ESCALATE to a human) --------------------------------
    # tool -> max auto-approved value for its `amount`-shaped arg. Over the cap => ESCALATE
    # (human approval), NOT deny — a large but legitimate refund should be approvable.
    value_caps: dict[str, float] = field(default_factory=dict)
    # Which arg key holds the value, per tool (defaults to "amount").
    value_arg_key: str = "amount"

    # --- SEQUENCE / composition -------------------------------------------------------
    # Ordered tool-name subsequences that are dangerous even when each call is individually
    # allowed (e.g. read-sensitive -> mutate). Matched as a contiguous tail of history.
    forbidden_sequences: tuple[tuple[str, ...], ...] = ()

    # --- DRIFT (semantic) -------------------------------------------------------------
    # The adapter computes drift_score once per invoke (embeddings) and the engine only
    # COMPARES it to drift_threshold. We use DUAL ANCHORING, not cosine-to-one-goal-string:
    # a single goal sentence sits in the same embedding neighborhood as any support-about-
    # orders text, so on-goal and off-goal trajectories overlap and legit requests
    # false-flag (measured live: separation gap -0.04). Instead we anchor against a small
    # set of ON-GOAL exemplars AND a set of ANTI-GOAL exemplars, and score by which centroid
    # the trajectory is closer to. Both centroids embed ONCE and cache; runtime cost is
    # still one trajectory embedding per invoke.
    #
    #   drift_score = 0.5 + (cosine(traj, anti_centroid) - cosine(traj, goal_centroid)) / 2
    #
    # so >0.5 leans off-goal, <0.5 leans on-goal; drift_threshold picks the cut.
    drift_examples: tuple[str, ...] = ()        # canonical ON-GOAL utterances
    drift_anti_examples: tuple[str, ...] = ()   # canonical OFF-GOAL utterances
    drift_threshold: float = 0.5
    drift_action: str = "DENY"   # "DENY" | "ESCALATE"

    # --- global posture ---------------------------------------------------------------
    # fail_closed: when the caller identity can't be resolved (or a rule errors), HALT
    # rather than allow. This is the safe default and the fail-closed dimension.
    fail_closed: bool = True
