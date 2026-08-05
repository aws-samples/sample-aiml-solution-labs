"""
Threat engine (Layer 7 · Observe & Contain) — PURE, dependency-free.

Detection vs. response, made explicit. Every lower layer BLOCKS bad actions all day
(a Cedar deny, a goal-fence HALT, an IAM AccessDenied) — that is the control working,
not an emergency. This module is the judgment on top: it ingests those blocks as raw
ATTEMPTS, groups them by the actor tuple, and PROMOTES a tuple to an INCIDENT only when
a "continued threat" threshold trips — i.e. one identity keeps trying after being told no.

Kept pure (no boto3 / FastAPI / network) so it is unit-testable with literals, exactly
like iris_goalfence. The server owns time (passes `now` in) so this stays deterministic.

Core model — an incident is a TUPLE, not "an agent":
    (agent_runtime, session, user_id, peer)   ← who/what actually went rogue
The whole point of the page is to resolve that tuple so containment is SURGICAL:
kill the smallest noun (usually the session) that stops the threat, leaving the rest alive.
"""
from dataclasses import dataclass, field, asdict
from typing import Optional


# --- Severity / decision vocabulary (mirrors the goal fence + Cedar/IAM outcomes) -----
_SEV_ORDER = {"low": 0, "medium": 1, "high": 2, "critical": 3}


@dataclass(frozen=True)
class ThreatPolicy:
    """When does a pile of blocked attempts become an INCIDENT worth acting on?
    Policy is DATA (like the goal-fence Charter) so it can be shown/tuned in the UI and
    reasoned about, not buried in code. First rule that trips wins → promote."""
    # rate rule: N blocked attempts from the SAME tuple within window_secs.
    # 60s (not 30s) because each real rogue invoke is a full model round-trip (~15-20s), so
    # a 30s window aged out the 1st block before the 3rd landed — the meter dropped instead
    # of reaching the threshold.
    rate_count: int = 3
    window_secs: float = 60.0
    # persistence rule: the agent was blocked and tried AGAIN toward the same goal
    # (retry-after-block). This is the compelling "it won't stop" signal.
    retry_after_block: bool = True
    retry_min: int = 2                      # ≥2 blocked attempts where a later one repeats an already-blocked goal
    # severity short-circuit: a single attempt this severe promotes immediately
    critical_short_circuit: str = "critical"


@dataclass
class Attempt:
    """One blocked (or notable) action, as emitted by a control the moment it fires.
    Everything needed to attribute + later contain must be captured HERE — after the
    fact, CloudTrail redacts tool args, so the control emits the structured record."""
    id: int
    ts: float
    # the actor tuple ---------------------------------------------------------
    agent_runtime: str                 # e.g. "iris_rogue" (the definition)
    session_id: str                    # the surgical unit — what L1 kill targets
    user_id: str                       # OBO customer_id / sub — on whose behalf
    peer: Optional[str] = None         # a second actor (A2A confused-deputy), if any
    # what happened -----------------------------------------------------------
    control: str = ""                  # which control fired: goal-fence / cedar / iam / memory-gate
    rule: str = ""                     # sub-rule: scale / scope / value / sequence / drift / ...
    decision: str = "DENY"             # ALLOW / DENY / ESCALATE / HALT
    severity: str = "medium"           # low / medium / high / critical
    goal: str = ""                     # the objective the action served (for retry detection)
    tool: str = ""                     # tool/resource touched
    detail: str = ""                   # human line for the feed
    blocked: bool = True               # did a control stop it (vs. an allowed step)

    def tuple_key(self) -> str:
        # attribution key: same agent+session+user (peer folded in — a confused-deputy
        # incident is still owned by the caller session that triggered it).
        return f"{self.agent_runtime}|{self.session_id}|{self.user_id}"


@dataclass
class Incident:
    """A promoted tuple: the console has judged this actor rogue and it needs a decision."""
    id: int
    opened_ts: float
    tuple_key: str
    agent_runtime: str
    session_id: str
    user_id: str
    peer: Optional[str]
    trigger: str                       # which policy rule promoted it (rate / retry / critical)
    severity: str
    attempt_ids: list = field(default_factory=list)
    controls: list = field(default_factory=list)   # distinct controls that fired
    rules: list = field(default_factory=list)       # distinct sub-rules
    goals: list = field(default_factory=list)
    status: str = "open"               # open / contained
    contained_by: Optional[str] = None  # which noun was blocked (session/user/agent-id/agent-def)
    contained_ts: Optional[float] = None
    summary: str = ""

    def to_dict(self):
        return asdict(self)


def _max_sev(a: str, b: str) -> str:
    return a if _SEV_ORDER.get(a, 0) >= _SEV_ORDER.get(b, 0) else b


class ThreatEngine:
    """Stateful over a demo session: feed attempts in, get incidents out.

    Usage (server owns the clock):
        eng = ThreatEngine(ThreatPolicy())
        result = eng.observe(attempt, now=ts)   # -> {"attempt":..., "incident": Incident|None, "promoted": bool}
    """

    def __init__(self, policy: Optional[ThreatPolicy] = None):
        self.policy = policy or ThreatPolicy()
        self._attempt_seq = 0
        self._incident_seq = 0
        self.attempts: list = []                    # all attempts, in order
        self.incidents: dict = {}                   # tuple_key -> Incident (open or contained)
        self._by_tuple: dict = {}                   # tuple_key -> [attempt, ...]

    # -- ingest ----------------------------------------------------------------
    def new_attempt(self, *, now: float, **kw) -> Attempt:
        self._attempt_seq += 1
        return Attempt(id=self._attempt_seq, ts=now, **kw)

    def observe(self, attempt: Attempt, now: float) -> dict:
        """Record an attempt; promote its tuple to an incident if the policy trips.
        Returns the attempt, the incident (new or already-open for this tuple), and
        whether THIS call is the one that promoted it (for the UI 'INCIDENT DECLARED' beat)."""
        self.attempts.append(attempt)
        key = attempt.tuple_key()
        bucket = self._by_tuple.setdefault(key, [])
        bucket.append(attempt)

        existing = self.incidents.get(key)
        if existing and existing.status == "open":
            # already an open incident for this tuple — fold the attempt in, no re-promote.
            self._fold(existing, attempt)
            return {"attempt": attempt, "incident": existing, "promoted": False}

        trigger = self._trips(bucket, attempt, now)
        if not trigger:
            return {"attempt": attempt, "incident": None, "promoted": False}

        # a contained incident for this tuple that trips AGAIN → reopen as a new incident
        # (the operator killed the session but the actor came back on a new one, etc.)
        inc = self._open(key, bucket, trigger, now)
        self.incidents[key] = inc
        return {"attempt": attempt, "incident": inc, "promoted": True}

    # -- threshold rules (first match wins) ------------------------------------
    def _trips(self, bucket, attempt, now) -> Optional[str]:
        p = self.policy
        blocked = [a for a in bucket if a.blocked]

        # 1) severity short-circuit — one critical block is an incident on its own.
        if _SEV_ORDER.get(attempt.severity, 0) >= _SEV_ORDER.get(p.critical_short_circuit, 3) and attempt.blocked:
            return "critical-severity"

        # 2) retry-after-block — the actor repeated an already-blocked goal (won't stop).
        if p.retry_after_block and attempt.blocked and attempt.goal:
            prior_goals = [a.goal for a in blocked[:-1]]
            if attempt.goal in prior_goals and len(blocked) >= p.retry_min:
                return "retry-after-block"

        # 3) rate — N blocked attempts from this tuple inside the window.
        recent = [a for a in blocked if now - a.ts <= p.window_secs]
        if len(recent) >= p.rate_count:
            return "rate-threshold"

        return None

    # -- incident construction / folding ---------------------------------------
    def _open(self, key, bucket, trigger, now) -> Incident:
        self._incident_seq += 1
        first = bucket[-1]
        inc = Incident(
            id=self._incident_seq, opened_ts=now, tuple_key=key,
            agent_runtime=first.agent_runtime, session_id=first.session_id,
            user_id=first.user_id, peer=next((a.peer for a in bucket if a.peer), None),
            trigger=trigger, severity="low",
        )
        for a in bucket:
            self._fold(inc, a)
        inc.summary = self._summarize(inc, trigger)
        return inc

    def _fold(self, inc: Incident, a: Attempt):
        if a.id not in inc.attempt_ids:
            inc.attempt_ids.append(a.id)
        if a.control and a.control not in inc.controls:
            inc.controls.append(a.control)
        if a.rule and a.rule not in inc.rules:
            inc.rules.append(a.rule)
        if a.goal and a.goal not in inc.goals:
            inc.goals.append(a.goal)
        if a.peer and not inc.peer:
            inc.peer = a.peer
        inc.severity = _max_sev(inc.severity, a.severity)

    def _summarize(self, inc: Incident, trigger: str) -> str:
        n = len(inc.attempt_ids)
        who = f"{inc.user_id} · session {inc.session_id[:12]}"
        rules = "/".join(inc.rules) or "policy"
        reason = {
            "retry-after-block": "kept trying the same out-of-bounds goal after being blocked",
            "rate-threshold": f"{n} blocked attempts in a short window",
            "critical-severity": "a single critical out-of-bounds action",
        }.get(trigger, "sustained out-of-bounds activity")
        return f"{who}: {reason} ({rules})."

    # -- containment -----------------------------------------------------------
    def contain(self, tuple_key: str, noun: str, now: float) -> Optional[Incident]:
        """Mark an incident contained by whichever NOUN the operator blocked
        (session / user-id / agent-id / agent-def). The actual kill is the server's job;
        this just records the decision + closes the incident."""
        inc = self.incidents.get(tuple_key)
        if not inc:
            return None
        inc.status = "contained"
        inc.contained_by = noun
        inc.contained_ts = now
        return inc

    # -- views for the UI ------------------------------------------------------
    def snapshot(self) -> dict:
        return {
            "policy": asdict(self.policy),
            "attempts": [asdict(a) for a in self.attempts],
            "incidents": [i.to_dict() for i in self.incidents.values()],
            "counts": {
                "attempts": len(self.attempts),
                "incidents": len(self.incidents),
                "open": sum(1 for i in self.incidents.values() if i.status == "open"),
            },
        }

    def progress_to_trigger(self, tuple_key: str, now: float) -> dict:
        """For the live threat METER: how close is this tuple to promotion right now?
        Returns the rate-rule fraction (the visible mechanic) + whether an incident exists."""
        p = self.policy
        bucket = self._by_tuple.get(tuple_key, [])
        blocked = [a for a in bucket if a.blocked]
        recent = [a for a in blocked if now - a.ts <= p.window_secs]
        inc = self.incidents.get(tuple_key)
        return {
            "blocked_recent": len(recent),
            "rate_count": p.rate_count,
            "fraction": min(1.0, len(recent) / p.rate_count) if p.rate_count else 0.0,
            "incident_open": bool(inc and inc.status == "open"),
        }
