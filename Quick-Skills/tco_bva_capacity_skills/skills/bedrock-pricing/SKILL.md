---
name: bedrock-pricing
description: >
  Use when calculating Amazon Bedrock foundation model inference costs, comparing
  model pricing across tiers/regions, or estimating monthly spend for agentic workloads.
  Do NOT use for AgentCore infrastructure (load agentcore-pricing),
  RPM/TPM capacity (load bedrock-capacity), or business value ROI (load agent-business-value).
---

# Bedrock Model Pricing

## Critical Rules

- **ALWAYS ask for region first.** No default region — never assume one.
- **ALWAYS ask for model family.** If user hasn't specified (e.g., "Sonnet", "Opus", "Haiku"), ask.
- **ALWAYS use `list_models()` before `get_model_prices()`.** Present versions to user, let them pick. Never guess "latest."
- **NEVER use training data for prices.** All prices must come from the local pricing cache files at runtime via `query_model_pricing()`.
- **NEVER implement cost formulas manually.** Always use `estimate_cost()` or `calculate_agent_session_compounded_cost()`.
- **NEVER implement token formulas manually.** Always use `calculate_compounded_tokens_for_agent()`, `calculate_rag_subagent_tokens()`, or `calculate_research_subagent_tokens()` for token calculations — if the user is only asking about token usage, not pricing.
- **NEVER generate your own calculations.** Present function output verbatim — it handles all token math internally.
- **If user asks for detailed explanation**, read the report file at `result["file_path"]`. Present the information as-is, then explain as needed. Do NOT recompute or manually derive calculations.
- **ALWAYS run capacity fit check** after calculating pricing using `get_tier_limits_for_model()` and `check_capacity_fit()`. This is not optional — every pricing result must include a capacity verdict for each model in the workload.
- **All values in code examples are illustrative only.** Always use user-specified values when provided. Prices must always come from the pricing cache, never from examples in this document.
- **If user describes an agentic workload** ("agent", "multi-agent", "sub-agent"), load `agentcore-pricing` and present combined costs.

## Quick Reference

```python
# §Load Script
import sys, os
sys.argv = ['bedrock_pricing.py']
script = ("tco_bva_capacity_skills/skills/bedrock-pricing/scripts/bedrock_pricing.py"
          if os.environ.get("USE_IN_KIRO") or os.environ.get("USE_IN_CLAUDE_CODE")
          else os.path.expanduser("~/.quickwork/skills/bedrock-pricing/scripts/bedrock_pricing.py"))
exec(open(script).read())
home = os.path.expanduser("~/bedrock_cache")

# 1. Check freshness
cache_status = check_pricing_data_status()  # handle "missing" → tell user to --refresh

# 2. Resolve model
models = list_models(home, "us-west-2", "Sonnet")  # → present to user, let them pick

# 3. Create session directory (all reports go here)
session_dir = create_report_session(model_name="Claude Sonnet 4.6", volume=10000)

# 4a. FAST PATH (single agent ONLY — raises error if subagents passed):
result = estimate_cost(home, "us-west-2", "Claude Sonnet 4.6", 10000, output_dir=session_dir)

# 4b. FULL PATH (multi-agent or custom params — REQUIRED for sub-agents):
prices = get_model_prices(home, "us-west-2", "Claude Sonnet 4.6")
result = calculate_agent_session_compounded_cost(
    main_agent_config={**prices, "agent_sessions_per_month": 10000, ...},
    subagents=[...],
    output_dir=session_dir,
)

# 5. Capacity (ALWAYS — not optional)
tier_limits = get_tier_limits_for_model(home, "Claude Sonnet 4.6", "us-west-2")
cap = check_capacity_fit(
    capacity_profile=result["capacity_profile"]["main_agent"],
    questions_per_month=50000, tier_limits=tier_limits, output_dir=session_dir,
)
```

## Workflow

### 1. Check Pricing Data Freshness (once per session)

```python
cache_status = check_pricing_data_status()
```

- `"ok"` — proceed.
- `"stale"` — warn user, suggest refresh, proceed with available data.
- `"missing"` — **stop.** Tell user to run the refresh command from `cache_status["refresh_command"]`.

### 2. Resolve Model

```python
models = list_models(home, "us-west-2", "Sonnet")
# → ["Claude 3 Sonnet", "Claude 3.5 Sonnet", ..., "Claude Sonnet 4.6"]
```

- Present the list to user, ask which version.
- Empty list → no models for that family/region.
- User provides exact model name upfront → skip to Step 3.

### 3. Present Assumptions

Before calculating, confirm parameters with the user:
- Sessions per month
- Questions per session (default: 5)
- Tools passed / invoked per question
- System prompt tokens
- Input/output tokens per question
- For multi-agent: which sub-agents, their models, invocation pattern

Only proceed after user confirms or adjusts values.

### 4. Calculate Cost

**Always create a session directory first** — all reports (pricing, capacity, agentcore, BVA) go here:

```python
session_dir = create_report_session(model_name="Claude Sonnet 4.6", volume=10000)
```

**Fast path** — **single agent ONLY.** For multi-agent, you MUST use the full path below:

```python
result = estimate_cost(home, "us-west-2", "Claude Sonnet 4.6", 10000, output_dir=session_dir)
# Pass any override as keyword: questions_per_agent_session=3, system_prompt_tokens=4000
```

**Full path** — multi-agent or custom sub-agents:

```python

main_prices = get_model_prices(home, "us-west-2", "Claude Sonnet 4.6")
rag_prices = get_model_prices(home, "us-west-2", "Nova 2.0 Lite")

result = calculate_agent_session_compounded_cost(
    main_agent_config={
        **main_prices,
        "agent_sessions_per_month": 10000,
        "questions_per_agent_session": 5,
        "tools_passed_to_agent": 10,
        "tools_invoked": 5,
    },
    subagents=[
        {
            "type": "rag",
            "token_params": {"rag_n_chunks": 10, "rag_chunk_size": 300, "output_tokens": 300},
            "model_prices": rag_prices,
            "questions_invoked": 3,
        },
        {
            "type": "research",
            "token_params": {"n_research_iterations": 4, "output_tokens": 1000},
            "model_prices": research_prices,
            "questions_invoked": 0,  # 0 = pre-session (output cached in system prompt)
        },
    ],
    output_dir=session_dir,
)
```

**Key rules for sub-agents:**
- `questions_invoked=0` → pre-session (output added to system_prompt, cached)
- `questions_invoked=N` → invoked as tool in the first N questions
- `tools_invoked` must be >= number of sub-agents with `questions_invoked > 0`
- Sub-agent models that support caching (non-None `cache_read_price`) get caching automatically

### 5. Detect Prompt Caching Support

A model supports caching if `cache_read_price` and `cache_write_price` are non-None in `get_model_prices()` output. Some models (Nova) have `cache_write_price=0.0` — caching is free for writes.

### 6. Present Results

The function writes a full report and returns a compact summary:

```python
{
    "file_path": "~/bedrock_reports/...",
    "monthly_total": 3247.48,
    "annual_total": 38969.70,
    "session_total": 0.324748,
    "savings_pct": 60.9,
    "recommended_ttl": "5min",
    "capacity_profile": {...},  # pass to check_capacity_fit()
}
```

- Present as markdown table. Include `file_path` for the user.
- If `_file_write_failed: True` → full result is inline; render `token_table` verbatim.

### 7. Capacity Fit Check (MANDATORY — run for every pricing result)

Run `check_capacity_fit()` for **each model** in the workload. Do not skip this step.

```python
tier_limits = get_tier_limits_for_model(home, "Claude Sonnet 4.6", "us-west-2")
cap_result = check_capacity_fit(
    capacity_profile=result["capacity_profile"]["main_agent"],
    questions_per_month=50000,
    tier_limits=tier_limits,
    output_dir=session_dir,
)
# Returns compact: fits, utilization %, recommendations, report_file
```

- If `tier_limits` is None → warn user quota data is missing, suggest `--refresh`
- If workload doesn't fit → present recommendations from the result
- For multi-agent: check each distinct model separately

### 8. Completeness Check (MANDATORY)

| # | Check | Condition | Action if not done |
|---|-------|-----------|-------------------|
| 1 | **Capacity fit for EVERY model** | Always (every pricing result) | Run `check_capacity_fit()` per distinct model — this is NOT optional |
| 2 | AgentCore costs included | Agentic workload | Load `agentcore-pricing` skill, present combined total |
| 3 | Reports grouped in one folder | Always | Call `create_report_session()` once at the start, pass `output_dir=session_dir` to every function that writes a report |
| 4 | Multiple scenarios stay separate | User asks for comparisons | Create a separate session directory per scenario |

## Token-Only Calculations

If user asks about token usage (not pricing), no cache files needed:
- `calculate_compounded_tokens_for_agent()` — main agent session tokens
- `calculate_rag_subagent_tokens()` — RAG sub-agent tokens
- `calculate_research_subagent_tokens()` — research sub-agent tokens

## Related Skills

| Skill | When to load |
|-------|-------------|
| `agentcore-pricing` | User describes an agentic workload needing infrastructure costs |
| `bedrock-capacity` | User asks about RPM/TPM limits or capacity planning |
| `agent-business-value` | User wants ROI, productivity gains, or FTE equivalents |
