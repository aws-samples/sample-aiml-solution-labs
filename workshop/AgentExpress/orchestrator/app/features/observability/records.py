"""Telemetry record schema — one row per captured event.

Stored in the telemetry DynamoDB table (see store.py). PK = session_id,
SK = "ts#seq#rand" so events sort chronologically within a session. The by_date
GSI powers the aggregation views (by date / model / user).

`kind` says what the row is:
  llm        - a Bedrock model call (tokens, rates, cost, prompt I/O)
  tool       - a Gateway MCP / KB retrieve call
  agentcore  - AgentCore Runtime compute for one active burst
  memory     - a long-term memory recall or store
  guardrail  - a Bedrock Guardrails check on agent input/output
  policy     - a Cedar policy decision on a Gateway tool call
  eval       - one AgentCore Evaluations (LLM-as-judge) result
"""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass, field
from decimal import Decimal

from app.common import clock


@dataclass
class CallRecord:
    session_id: str
    agent_id: str
    user: str
    kind: str                      # see the module docstring
    # model (for llm), provider label (tool), op (memory), source (guardrail),
    # tool name (policy) or evaluator id (eval)
    label: str
    mode: str                      # "bedrock" | "gateway" | "agentcore" | "error"
    input_tokens: int = 0          # exact model tokens (from Bedrock usage)
    output_tokens: int = 0         # exact model tokens
    system_tokens: int = 0         # system prompt slice of input (exact via CountTokens, else estimated)
    system_tokens_exact: bool = True   # False when CountTokens was unavailable and we estimated
    # False when neither `orchestrator.modelRates` nor pricing.py's built-in table knew
    # this model and the fallback rate was used, so `cost_usd`/`in_rate`/`out_rate` on
    # this row are a GUESS. Same idea as system_tokens_exact above, applied to prices:
    # the fallback used to be invisible, so a customer on a model pricing.py has never
    # heard of read fabricated figures as measured ones.
    rates_known: bool = True
    # What priced this row (llm only): "config" (orchestrator.modelRates), "pricelist"
    # (the AWS Price List API for this region), "table" (pricing.py's built-in snapshot)
    # or "fallback" (the guess; rates_known=False). Empty on rows that carry no rate.
    rates_source: str = ""
    embed_tokens_est: int = 0      # estimate: KB query embedding tokens (tool rows)
    latency_ms: int = 0
    cost_usd: Decimal = Decimal(0)
    # Per-1M-token rates that priced this row (llm only), for UI display.
    in_rate: Decimal = Decimal(0)
    out_rate: Decimal = Decimal(0)
    # Request parameters + outcome, surfaced in the "Prompts & I/O" inspector.
    temperature: Decimal = Decimal(0)
    max_tokens: int = 0
    finish_reason: str = ""          # e.g. end_turn | max_tokens (llm rows)
    status: str = "ok"               # ok | error | passed | blocked | allowed | denied
    # Captured call content for the observability "Prompts & I/O" inspector.
    # For llm rows: the system prompt, the user/human input, and the model's
    # response text. For tool rows: user_input holds the tool query and
    # output_text the result. All are truncated in meter.py, and store.py
    # additionally guarantees the row fits DynamoDB's 400KB item limit.
    system_prompt: str = ""
    user_input: str = ""
    output_text: str = ""
    # For kind="memory" rows: the long-term namespace the recall/store hit
    # (e.g. "insights/analysis-acme"), surfaced in the UI inspector.
    namespace: str = ""
    # The named model call this row belongs to (the run_llm `name`, e.g.
    # "analysis" vs "analysis-scoring"). An agent can issue several distinct
    # prompts; this lets evaluation scope to ONE prompt at a time. Set on
    # kind="llm" rows and carried onto their kind="eval" rows.
    prompt: str = ""
    # Agent-run version this event belongs to (1 = initial, 2+ = re-run with
    # reviewer feedback). 0 = unknown/legacy. Lets the UI label "Version N".
    version: int = 0
    # For kind="eval" rows: the evaluator's numeric score (0.0-1.0). `label`
    # holds the evaluator name and `output_text` the judge's explanation.
    value: Decimal = Decimal(0)
    # For kind="eval" rows: the judge's OWN qualitative band, verbatim from
    # AgentCore (e.g. "Very Helpful"). Kept because it is authoritative — without
    # it the UI has to re-derive a band from `value`, inventing thresholds the
    # evaluator never agreed to.
    eval_label: str = ""
    # Eastern-Time 'YYYY-MM-DD HH:MM:SS'; sorts lexicographically = chronologically.
    ts: str = field(default_factory=clock.now_str)
    date: str = field(default_factory=clock.today_str)  # ET date, GSI hash key
    seq: int = 0

    def key(self) -> str:
        # unique, chronological sort key within the session (ts is fixed-width ET)
        return f"{self.ts}#{self.seq:04d}#{uuid.uuid4().hex[:6]}"

    def to_item(self) -> dict:
        item = asdict(self)
        item["sk"] = self.key()
        return item
