"""The rule set — one small, pure evaluator per fence dimension.

Each rule is a callable `(Charter, TrajectoryState, Action) -> Verdict | None`:
  - return a blocking Verdict (DENY/ESCALATE/HALT) when the rule fires,
  - return None when the rule has no opinion (inert / not its phase / within bounds).

The engine runs them in order and the FIRST blocking verdict wins (see engine.py). Rules
never mutate state — the engine owns accumulation. Keeping them tiny and side-effect-free
is what makes the whole set table-testable with literals.

Ordering matters for the DEMO STORY, most-severe posture first:
    fail_closed -> scope -> scale -> value -> sequence -> drift
so a missing identity HALTs before anything else is considered, and a cross-actor access
is denied before we bother counting records.
"""
from __future__ import annotations

import json

from .charter import Charter
from .entities import Action, Phase, TrajectoryState, Verdict

# Argument keys that denote a COUNT/LIMIT (how many records to touch). Only these are
# checked against the record cap — scanning every number in the args would false-positive
# on IDs ("C-1001"), amounts ("9999"), order numbers, etc. Bulk SHAPE is caught separately
# by the marker scan below; this is specifically "asked for N records where N is too big".
_LIMIT_KEYS = ("limit", "count", "max", "max_records", "top", "n", "size",
               "quantity", "qty", "rows", "records", "page_size", "per_page")


def _args_blob(action: Action) -> str:
    """Lowercased JSON of the args — used for substring/number scans. Total-safe."""
    try:
        return json.dumps(action.args, default=str).lower()
    except Exception:
        return str(action.args).lower()


# ---------------------------------------------------------------------------------------
# 1. FAIL-CLOSED — no verified caller => we cannot reason about scope/attribution => HALT.
def rule_fail_closed(charter: Charter, state: TrajectoryState, action: Action) -> Verdict | None:
    if action.phase not in (Phase.BEFORE_TOOL, Phase.BEFORE_INVOKE):
        return None
    if not charter.fail_closed:
        return None
    caller = (action.verified_caller or "").strip()
    if not caller:
        return Verdict.halt(
            "fail_closed",
            "no verified caller identity — the fence fails closed and stops the agent "
            "rather than act without knowing whom it is acting for.",
        )
    return None


# ---------------------------------------------------------------------------------------
# 2. SCOPE — the action targets a customer other than the verified caller (cross-actor).
def rule_scope(charter: Charter, state: TrajectoryState, action: Action) -> Verdict | None:
    if action.phase != Phase.BEFORE_TOOL:
        return None
    if action.tool_name in charter.scope_exempt_tools:
        return None
    caller = (action.verified_caller or "").strip()
    if not caller:
        return None  # fail_closed already owns the missing-caller case
    for key in charter.scope_arg_keys:
        target = action.args.get(key)
        if target is None:
            continue
        target = str(target).strip()
        if target and target != caller:
            return Verdict.deny(
                "scope",
                f"cross-actor access: action targets '{target}' but the verified caller "
                f"is '{caller}'. The charter binds the agent to the caller's OWN records.",
                target=target, caller=caller, arg=key,
            )
    return None


# ---------------------------------------------------------------------------------------
# 3. SCALE — the trajectory (or one call) exceeds the charter's volume bounds.
#    Cumulative sweep => HALT (kill the harvest cold). Single over-broad call => DENY.
def rule_scale(charter: Charter, state: TrajectoryState, action: Action) -> Verdict | None:
    if action.phase != Phase.BEFORE_TOOL:
        return None

    # (a) Cumulative bulk sweep. state.action_count is calls ALREADY seen; this one would
    #     be the next. Once the trajectory would exceed the cap it's a mass harvest the
    #     agent was never chartered to do -> hard stop.
    prospective = state.action_count + 1
    if prospective > charter.max_actions:
        return Verdict.halt(
            "scale",
            f"bulk sweep: this trajectory would make {prospective} tool calls, over the "
            f"charter cap of {charter.max_actions}. Chartered scope is one customer, small "
            f"result sets — a sweep across many records is out of charter.",
            action_count=prospective, cap=charter.max_actions,
        )

    blob = _args_blob(action)

    # (b) Bulk / all-records shape in this single call's arguments.
    for marker in charter.bulk_arg_markers:
        if marker in blob:
            return Verdict.deny(
                "scale",
                f"bulk-shaped request: argument contains '{marker}', implying an "
                f"all-records read. The charter caps a trajectory to the caller's own, "
                f"small result sets.",
                marker=marker,
            )

    # (c) Explicit numeric limit over the per-call record cap. Only inspect keys that
    #     actually mean "how many records" — not IDs or amounts.
    for key, val in action.args.items():
        if key.lower() not in _LIMIT_KEYS:
            continue
        try:
            n = int(val)
        except (TypeError, ValueError):
            continue
        if n > charter.max_records:
            return Verdict.deny(
                "scale",
                f"requested {n} records exceeds the charter per-call cap of "
                f"{charter.max_records}.",
                requested=n, cap=charter.max_records,
            )
    return None


# ---------------------------------------------------------------------------------------
# 4. VALUE — high-impact / irreversible action over its auto-approve cap => ESCALATE.
#    Note: ESCALATE, not DENY — a large but legitimate refund should be approvable by a
#    human, which is the whole point of the human-in-the-loop tier.
def rule_value(charter: Charter, state: TrajectoryState, action: Action) -> Verdict | None:
    if action.phase != Phase.BEFORE_TOOL:
        return None
    cap = charter.value_caps.get(action.tool_name)
    if cap is None:
        return None
    raw = action.args.get(charter.value_arg_key)
    if raw is None:
        return None
    try:
        amount = float(raw)
    except (TypeError, ValueError):
        return None
    if amount > cap:
        return Verdict.escalate(
            "value",
            f"high-value action: {action.tool_name} for {amount:g} exceeds the "
            f"auto-approve cap of {cap:g} — human approval required.",
            amount=amount, cap=cap, tool=action.tool_name,
        )
    return None


# ---------------------------------------------------------------------------------------
# 5. SEQUENCE — a dangerous ordered pattern of tools, even if each call is allowed.
#    Matched as a contiguous TAIL of (history + this call): the moment the pattern
#    completes, the final action is denied.
def rule_sequence(charter: Charter, state: TrajectoryState, action: Action) -> Verdict | None:
    if action.phase != Phase.BEFORE_TOOL:
        return None
    if not charter.forbidden_sequences:
        return None
    seq = state.tool_history + [action.tool_name]
    for pattern in charter.forbidden_sequences:
        n = len(pattern)
        if n and len(seq) >= n and tuple(seq[-n:]) == tuple(pattern):
            return Verdict.deny(
                "sequence",
                f"forbidden action sequence: {' -> '.join(pattern)}. This combination is "
                f"out of charter even though each step is individually permitted.",
                pattern=list(pattern),
            )
    return None


# ---------------------------------------------------------------------------------------
# 6. DRIFT — the trajectory's effective goal has diverged from the charter goal.
#    The adapter computes action.drift_score (embedding cosine distance, once per invoke);
#    the engine only compares to the threshold. Outcome is charter-configurable.
def rule_drift(charter: Charter, state: TrajectoryState, action: Action) -> Verdict | None:
    if action.phase != Phase.BEFORE_TOOL:
        return None
    score = action.drift_score
    if score is None:
        score = state.drift_score
    if score is None or score <= charter.drift_threshold:
        return None
    reason = (
        f"goal drift: the trajectory has diverged from the chartered goal "
        f"(drift {score:.2f} > threshold {charter.drift_threshold:.2f}). The effective "
        f"objective no longer matches '{charter.goal[:60]}'."
    )
    detail = {"drift": round(score, 4), "threshold": charter.drift_threshold}
    if charter.drift_action.upper() == "ESCALATE":
        return Verdict.escalate("drift", reason, **detail)
    return Verdict.deny("drift", reason, **detail)


# The pipeline, in evaluation order (first blocking verdict wins).
DEFAULT_RULES = (
    rule_fail_closed,
    rule_scope,
    rule_scale,
    rule_value,
    rule_sequence,
    rule_drift,
)
