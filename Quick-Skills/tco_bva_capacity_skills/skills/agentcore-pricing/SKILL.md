---
name: agentcore-pricing
description: >
  Use when estimating Amazon Bedrock AgentCore infrastructure costs including Runtime,
  Gateway, Memory, BrowserTool, CodeInterpreter, or Evaluations.
  Do NOT use for model-only pricing (load bedrock-pricing),
  RPM/TPM capacity (load bedrock-capacity), or business value ROI (load agent-business-value).
---

# AgentCore Pricing

## Critical Rules

- **NEVER use training data for prices.** All prices must come from the local pricing cache via `query_agentcore_pricing()`.
- **NEVER implement billing formulas manually.** Always use `calculate_agentcore_cost()` for infrastructure and `calculate_evaluation_cost()` for evaluations.
- **Default components: Runtime + Gateway + Memory + Evaluations.** Do NOT auto-add BrowserTool or CodeInterpreter unless user asks.
- **ALWAYS use `list_agentcore_components()`** to discover available components for the region before querying prices.
- **All values in code examples are illustrative only.** Always use user-specified values when provided. Prices must always come from the pricing cache, never from examples in this document.
- **If user asks for detailed explanation**, read the report file at `result["file_path"]`. Present the information as-is, then explain as needed. Do NOT recompute or manually derive calculations.
- **STM reads are free** — only writes are billed.
- **vCPU is free during I/O wait** — each component has its own I/O wait profile: Runtime 70% (waiting for LLM), BrowserTool 70% (waiting for pages + LLM), CodeInterpreter 20% (mostly active CPU execution).

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
cache_status = check_pricing_data_status()

# 2. Discover components
components = list_agentcore_components(home, "us-west-2")
# → ["BrowserTool", "CodeInterpreter", "Evaluations", "Gateway", "Memory", "Runtime"]

# 3. Get prices (defaults: Runtime + Gateway + Memory + Evaluations)
ac_prices = query_agentcore_pricing(home, "us-west-2", components=["Runtime", "Gateway", "Memory", "Evaluations"])

# 4. Extract and calculate infrastructure
result = calculate_agentcore_cost(
    runtime_vcpu_price_hr=...,  # from ac_prices (see mapping below)
    ...,
    questions_per_month=30000,
    output_dir=session_dir,
)

# 5. Calculate evaluations (ALWAYS)
eval_result = calculate_evaluation_cost(
    questions_per_month=30000,
    sessions_per_month=6000,
    builtin_input_price=...,   # from ac_prices: Evaluations + BuiltIn-Input
    builtin_output_price=...,  # from ac_prices: Evaluations + BuiltIn-Output
    output_dir=session_dir,
)
```

## Workflow

### 1. Check Freshness

```python
cache_status = check_pricing_data_status()
```

If `bedrock_pricing_agentcore.json` is in `cache_status["missing"]` → stop, tell user to run `cache_status["refresh_command"]`.

### 2. Discover Available Components

```python
components = list_agentcore_components(home, "us-west-2")
```

Present to user if they haven't specified which to include. Defaults: **Runtime, Gateway, Memory, Evaluations**.

### 3. Get Prices and Map to Parameters

```python
ac_prices = query_agentcore_pricing(home, "us-west-2", components=["Runtime", "Gateway", "Memory", "Evaluations"])
```

The `components` filter is case-insensitive. Returns a list of dicts with `sub_component` and `dimensions`.

**Critical: Map query results to `calculate_agentcore_cost()` parameters:**

| sub_component contains | Parameter |
|----------------------|-----------|
| `Runtime` + `vCPU` | `runtime_vcpu_price_hr` |
| `Runtime` + `Memory` | `runtime_mem_price_hr` |
| `Gateway` + `API-Invocations` | `gateway_invocation_price` |
| `Gateway` + `Search-API` | `gateway_search_price` |
| `Gateway` + `Tool-Indexing` | `gateway_indexing_price` |
| `Memory` + `Short-Term` | `stm_event_price` |
| `Memory` + `Long-Term-Memory-Storage` + `Built-in` | `ltm_storage_price` |
| `Memory` + `Long-Term-Memory-Retrieval` | `ltm_retrieval_price` |
| `BrowserTool` + `vCPU` | `browser_vcpu_price_hr` |
| `BrowserTool` + `Memory` | `browser_mem_price_hr` |
| `CodeInterpreter` + `vCPU` | `ci_vcpu_price_hr` |
| `CodeInterpreter` + `Memory` | `ci_mem_price_hr` |
| `Evaluations` + `BuiltIn-Input` | `builtin_input_price` (for `calculate_evaluation_cost()`) |
| `Evaluations` + `BuiltIn-Output` | `builtin_output_price` (for `calculate_evaluation_cost()`) |
| `Evaluations` + `CustomEvaluators` | `custom_evaluator_price` (for `calculate_evaluation_cost()`) |

Extract price: `float(entry["dimensions"][0]["price_usd"])`

### 4. Calculate AgentCore Cost

```python
result = calculate_agentcore_cost(
    # Prices (from Step 3 mapping)
    runtime_vcpu_price_hr=0.0895,
    runtime_mem_price_hr=0.00945,
    gateway_invocation_price=5e-6,
    gateway_search_price=2.5e-5,
    gateway_indexing_price=0.0002,
    stm_event_price=0.00025,
    ltm_storage_price=0.00075,
    ltm_retrieval_price=0.0005,
    # Optional (None = not included)
    browser_vcpu_price_hr=None,
    browser_mem_price_hr=None,
    ci_vcpu_price_hr=None,
    ci_mem_price_hr=None,
    # Workload
    questions_per_month=30000,
    questions_per_session=5,
    tools_invoked=5,
    tools_indexed=50,
    # Runtime
    num_vcpus=2,
    peak_memory_gb=4,
    io_wait_pct=0.70,
    # Report
    output_dir=session_dir,
)
```

### 5. Calculate Evaluations

```python
eval_result = calculate_evaluation_cost(
    questions_per_month=30000,
    sessions_per_month=6000,
    sampling_rate=0.10,
    num_builtin_evaluators=3,
    builtin_input_price=2.40,   # from cache: Evaluations + BuiltIn-Input
    builtin_output_price=12.00, # from cache: Evaluations + BuiltIn-Output
    questions_per_session=5,    # MUST match actual workload — default is 10, which overcounts
    output_dir=session_dir,
)
```

**Important:** `questions_per_session` defaults to 10 inside `calculate_evaluation_cost()`. Always pass the actual value from the workload to avoid overcounting evaluated questions.

### 6. Present Results

The function writes a detailed report and returns a compact summary:

```python
{
    "file_path": "~/bedrock_reports/.../agentcore.md",
    "total_monthly": 75.55,
    "total_annual": 906.65,
    "runtime_monthly": 21.55,
    "gateway_monthly": 1.50,
    "memory_monthly": 52.50,
    "evaluations_monthly": 8.40,
    "top_cost_component": "memory (69%)",
}
```

Present per-component with the "AgentCore —" prefix. Example combined table:

| Component | Monthly |
|-----------|---------|
| Bedrock model inference | $65,262.50 |
| AgentCore — Runtime | $43.10 |
| AgentCore — Gateway | $2.75 |
| AgentCore — Memory | $72.50 |
| AgentCore — Evaluations | $11,088.00 |
| **Total** | **$76,468.85** |

This shows per-component cost concentration at a glance.

### 7. Completeness Check (MANDATORY)

| # | Check | Condition | Action if not done |
|---|-------|-----------|-------------------|
| 1 | Evaluations included | Always (default component) | Run `calculate_evaluation_cost()` |
| 2 | Combined total presented | AgentCore + model pricing both ran | Present sum as grand total (infrastructure + evaluations + model) |
| 3 | Reports in session directory | Multiple calculations | Use `create_report_session()` + `output_dir` |
| 4 | Only BrowserTool/CodeInterpreter if requested | User explicitly asked | Don't auto-add these two — all others are defaults |
| 5 | Prices from cache | Any calculation | All prices came from `query_agentcore_pricing()` |

## Multi-Agent Architecture

When parent + sub-agents run on shared infrastructure:

| Agent | Guidance |
|-------|----------|
| Parent (router) | Include its tool invocations in `tools_invoked` |
| Sub-agents | Their tool calls are WITHIN the same runtime session |
| Shared Runtime | Scale `num_vcpus` and `peak_memory_gb` for total agent count |

## Configuration

Run `python3 bedrock_pricing.py --init-config` for all available settings.
See `agentcore_defaults` section for overridable values.

## Related Skills

| Skill | When to load |
|-------|-------------|
| `bedrock-pricing` | Need model inference prices for combined estimates |
| `bedrock-capacity` | User asks about RPM/TPM limits |
| `agent-business-value` | User wants ROI after cost is established |
