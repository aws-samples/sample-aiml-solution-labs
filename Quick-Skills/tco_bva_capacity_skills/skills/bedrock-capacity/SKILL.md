---
name: bedrock-capacity
description: >
  Use when checking RPM/TPM capacity limits, quota utilization, throttling risk,
  or capacity fit for Amazon Bedrock workloads.
  Also activate as a follow-up after bedrock-pricing estimates.
  Do NOT use for model pricing (load bedrock-pricing),
  AgentCore costs (load agentcore-pricing), or business value ROI (load agent-business-value).
---

# Bedrock Capacity Planning

## Critical Rules

- **ALWAYS ask for region first.** No default region — never assume one.
- **ALWAYS ask for model family.** If user hasn't specified (e.g., "Sonnet", "Opus", "Haiku"), ask.
- **ALWAYS use `list_models()` before proceeding.** Present versions to user, let them pick. Never guess.
- **ALWAYS get real limits from `get_tier_limits_for_model()`.** Never assume or hardcode quota values.
- **NEVER compute tokens manually.** Always use `calculate_compounded_tokens_for_agent()` and `build_capacity_profile_from_tokens()` to derive the capacity profile.
- **NEVER recommend quota increase before walking through the 3-Step Optimization Framework.**
- **All values in code examples are illustrative only.** Always use user-specified values when provided. Prices and quotas must come from the cache, never from examples in this document.
- **If user asks for detailed explanation**, read the report file at `result["report_file"]`. Present the information as-is, then explain as needed. Do NOT recompute or manually derive calculations.

## Capacity Facts (always apply)

- **Claude 3.7 and ALL later Claude models: `output_burndown_rate=5`.** All others use 1. This is the #1 capacity miscalculation error.
- **Prompt cache reads don't count toward TPM** — the biggest TPM saver.
- **`max_tokens` is reserved upfront** — if set to 4096 but output is ~100, you're wasting 3,996 TPM/request. The gap between `max_tokens` and actual output wastes quota on every request.

## Key Concepts

| Term | Definition |
|------|-----------|
| **RPM** | Requests/min. Each LLM call = 1 request. N tools/question = N+1 requests/question |
| **TPM** | Tokens/min. Deducted at request start using `max_tokens` (not actual output) |
| **Burndown** | Output multiplier. Claude 3.7+ = 5x (1 output token = 5 TPM). All others = 1x |
| **Effective TPM** | Peak TPM + max_tokens overhead. The REAL quota consumption |
| **TPD** | Tokens/day. Some models have hard daily caps regardless of RPM/TPM headroom |

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

# 1. Resolve model
models = list_models(home, "us-west-2", "Sonnet")  # → present to user, let them pick

# 2. Create session directory
session_dir = create_report_session(model_name="Claude Sonnet 4.6", volume=1000000)

# 3. Get token profile
#    PATH A — after pricing already ran in conversation:
capacity_profile = cost_result["capacity_profile"]["main_agent"]
#    PATH B — standalone (user asked capacity directly):
token_result = calculate_compounded_tokens_for_agent(
    questions_per_agent_session=5, input_tokens=100, output_tokens=150,
    system_prompt_tokens=2000, tools_passed_to_agent=10, tool_spec_tokens=100,
    tools_invoked=5, tool_call_tokens=100, tool_result_tokens=100,
    detail_level="full",  # REQUIRED for build_capacity_profile_from_tokens
)
capacity_profile = build_capacity_profile_from_tokens(
    token_result, sessions_per_month=1000000, model_name="Claude Sonnet 4.6"
)

# 4. Get limits
tier_limits = get_tier_limits_for_model(home, "Claude Sonnet 4.6", "us-west-2")

# 5. Check fit
result = check_capacity_fit(
    capacity_profile=capacity_profile,
    questions_per_month=5000000,
    output_burndown_rate=5,  # 5 for Claude 3.7+, 1 for others
    tier_limits=tier_limits,
    output_dir=session_dir,
)
```

## Workflow

### 1. Resolve Model

```python
models = list_models(home, "us-west-2", "Sonnet")
# → ["Claude 3 Sonnet", ..., "Claude Sonnet 4.6"]
```

- Present the list to user, ask which version.
- User provides exact model name upfront → skip to Step 2.

### 2. Present Assumptions

Before computing, confirm parameters with the user:
- Sessions per month
- Questions per session
- Tools passed / invoked per question
- System prompt size (tokens)
- Input/output tokens per question

Only proceed after user confirms or adjusts values.

### 3. Obtain Token Profile

**Path A — pricing already ran in this conversation** (preferred, avoids recomputation):

```python
capacity_profile = cost_result["capacity_profile"]["main_agent"]
```

**Path B — standalone capacity check** (user asked about capacity directly):

```python
token_result = calculate_compounded_tokens_for_agent(
    questions_per_agent_session=5, input_tokens=100, output_tokens=150,
    system_prompt_tokens=2000, tools_passed_to_agent=10, tool_spec_tokens=100,
    tools_invoked=5, tool_call_tokens=100, tool_result_tokens=100,
    detail_level="full",
)
capacity_profile = build_capacity_profile_from_tokens(
    token_result, sessions_per_month=1000000, model_name="Claude Sonnet 4.6"
)
```

For multi-agent: use `aggregate_capacity_by_model(cost_result["capacity_profile"])` to group load by model.

### 4. Get Real Limits

```python
tier_limits = get_tier_limits_for_model(home, "Claude Sonnet 4.6", "us-west-2")
# → {"rpm_high": 10000, "tpm_high": 6000000, "tpd_high": 8640000000}
```

If returns `None` → tell user to run `--refresh`. Do NOT proceed without limits.

### 5. Check Capacity Fit

```python
session_dir = create_report_session(model_name="Claude Sonnet 4.6", volume=1000000)

result = check_capacity_fit(
    capacity_profile=capacity_profile,
    questions_per_month=5000000,
    output_burndown_rate=5,     # 5 for Claude 3.7+, 1 for others
    max_tokens_setting=4096,
    peak_to_avg_ratio=3.0,
    active_hours_per_day=12,
    active_days_per_month=22,
    tier_limits=tier_limits,
    output_dir=session_dir,
)
```

Returns compact summary:
```python
{
    "fits": True,
    "rpm_utilization_pct": 5.7,
    "tpm_utilization_pct": 96.6,
    "tpd_utilization_pct": 9.8,
    "recommendations": ["..."],
    "report_file": "~/bedrock_reports/.../capacity-claude-sonnet-4-6.md",
}
```

Full calculations (RPM/TPM/TPD derivation, optimization checklist) are in the report file.

### 6. Present Results

Per-model comparison table:

| Metric | Your Workload (Peak) | Quota Limit | Fits? | Utilization |
|--------|---------------------|-------------|-------|-------------|
| RPM | 568 | 10,000 | ✅ | 6% |
| TPM | 5,793,182 | 6,000,000 | ✅ | 97% |
| TPD | 846,590,909 | 8,640,000,000 | ✅ | 10% |

- If fits: highlight metrics close to limit (>80%)
- If doesn't fit: walk through the 3-Step Framework below

### 7. Completeness Check (MANDATORY)

| # | Check | Condition | Action |
|---|-------|-----------|--------|
| 1 | Every model checked | Multi-agent | `check_capacity_fit()` per distinct model |
| 2 | Real limits used | Any check | From `get_tier_limits_for_model()`, never hardcoded |
| 3 | Correct burndown | Claude 3.7+ | `output_burndown_rate=5` |
| 4 | Reports in session directory | Any check | Use `create_report_session()` + `output_dir` |

## When Workload Doesn't Fit: 3-Step Framework

Walk through in order. Do NOT skip to quota increase.

### Step 1: Optimize

| Check | Action |
|-------|--------|
| `max_tokens` set higher than actual output? | Reduce to ~3× actual output — with 5× burndown this is the biggest lever |
| Prompt caching enabled? | Cache reads DON'T count toward TPM |
| System prompt large? | Shorten — sent every turn, compounds |
| Too many tools? | Use AC Gateway dynamic tool selection |
| Conversation history growing? | Limit to 3-5 past turns |
| Sub-agent responses large? | Reduce output tokens — compounds in main context |
| Output length itself? | With 5× burndown, every token saved frees 5 TPM |

### Step 2: Traffic Profile

Ask user to validate:
- Peak RPM (P99 over 1-min window)?
- Active hours/days? Off-peak = lower effective RPM
- Peak-to-average ratio (default 3×)?

### Step 3: Ramp Plan

- Volume needed now or in 3-6 months?
- Current monthly volume (baseline)?
- Target at 1 / 3 / 6 months?
- Increases granted after demonstrating usage.

## Related Skills

| Skill | When to load |
|-------|-------------|
| `bedrock-pricing` | Need model cost estimates alongside capacity |
| `agentcore-pricing` | Need infrastructure costs |
| `agent-business-value` | Build business case after capacity is confirmed |
