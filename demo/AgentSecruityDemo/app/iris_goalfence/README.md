# iris_goalfence

A standalone, framework-agnostic **goal fence** for agentic systems. It watches an
agent's *trajectory* — not one action in isolation — and decides whether the next action
still serves the chartered goal, at the chartered scope and scale.

The engine is deliberately **pure**: no Strands, no boto3, no network, no I/O. Its whole
universe is three plain values:

```
GoalFenceEngine(charter).evaluate(state, action) -> Verdict
```

so it can be unit-tested on a laptop with dicts and literals — no agent, no AWS
credentials, no model. A thin Strands adapter (added separately, later) is the only piece
that imports a framework; it turns hook events into `Action`s, calls this engine, and
applies the `Verdict` to the event.

## The six dimensions

| Rule | Fires when | Verdict |
|------|-----------|---------|
| `fail_closed` | no verified caller identity | **HALT** |
| `scope` | action targets a customer ≠ the verified caller | **DENY** |
| `scale` | trajectory exceeds `max_actions` (sweep) / bulk-shaped arg / over-limit count | **HALT** (sweep) / **DENY** |
| `value` | high-impact tool over its auto-approve cap (e.g. refund > $500) | **ESCALATE** (human) |
| `sequence` | a forbidden ordered tool pattern completes (e.g. read → mutate) | **DENY** |
| `drift` | trajectory's semantic drift from the goal exceeds threshold | **DENY** or **ESCALATE** |

First blocking rule wins, in the order above (a missing identity halts before anything
else; a cross-actor access is denied before we bother counting).

## The four verdicts → four enforcement behaviors

| `Decision` | The agent experiences | Adapter mechanism (later) |
|-----------|----------------------|---------------------------|
| `ALLOW` | tool runs normally | no-op |
| `DENY` | this tool blocked; model keeps reasoning, may explain | `event.cancel_tool` |
| `ESCALATE` | run pauses for a human; resumes on approval | `event.interrupt(...)` |
| `HALT` | agent stopped cold; cannot respond further | raise `FenceHalt` → server cuts the stream |

## Contract

**Input** — `Charter` (frozen policy, as data), `TrajectoryState` (accumulates across the
run), `Action` (one normalized tool call; `verified_caller` comes from a trusted source,
never from `args`; `drift_score` is computed by the adapter and passed in — the engine
never embeds anything).

**Output** — a `Verdict` with `decision`, `rule`, human-readable `reason`, and structured
`detail` for a UI.

The engine also owns accumulation via `observe(state, action)` (call once per admitted
action) so the rules stay pure.

## Run it standalone

```bash
# from demo-code/app
python3 iris_goalfence/tests/test_engine.py     # 24 tests, no deps
python3 iris_goalfence/demo.py                  # scripted trajectory, colorized verdicts
# or with pytest:
python3 -m pytest iris_goalfence/tests -q
```

## Files

```
iris_goalfence/
  charter.py     Charter — the policy, as data (every field maps to one rule)
  entities.py    Action, TrajectoryState, Verdict, Decision, Phase
  rules.py       the six pure rule evaluators + DEFAULT_RULES pipeline
  engine.py      GoalFenceEngine.evaluate() + observe() — the pure core
  demo.py        standalone scripted demo (no agent)
  tests/         table-driven tests, no AWS / no Strands
```

> Note: the drift rule only *compares* a `drift_score` to the charter threshold. Computing
> that score (an embedding cosine distance, once per invoke) belongs to the adapter, which
> keeps drift unit-testable with a plain float and keeps the engine free of network calls.
