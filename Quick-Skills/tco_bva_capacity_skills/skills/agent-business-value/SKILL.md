---
name: agent-business-value
description: >
  Use when calculating business value, ROI, or cost justification for AI agents.
  Three dimensions: (1) Time Savings, (2) Customer Churn Reduction, (3) Sales Increase.
  Works standalone or alongside bedrock-pricing/agentcore-pricing estimates.
  Do NOT use for model pricing (load bedrock-pricing),
  AgentCore costs (load agentcore-pricing), or capacity planning (load bedrock-capacity).
---

# Agent Business Value

## Critical Rules

- **ALWAYS use `calculate_business_value()`.** Never implement business value formulas manually.
- **ALWAYS present assumptions to user and confirm** before running the calculation.
- **Agent cost is deducted once** from grand total, not per dimension.
- **Never fabricate customer data.** If lookup fails, say so and ask the user.
- **All values in code examples are illustrative only.** Always use user-specified values when provided.
- **If user asks for detailed explanation**, read the report file at `result["file_path"]`. Present the information as-is, then explain as needed. Do NOT recompute or manually derive calculations.

## Business Value Facts (always apply)

- **Dim 1a (Productivity Increase)** frames time savings as revenue uplift — how much more revenue can employees generate with freed time.
- **Dim 1b (Cost Savings)** frames the same time savings as labor cost reduction — how many FTE-equivalents are freed.
- **Dims 1a and 1b are mutually exclusive.** Same time savings, different framing. NEVER add them together.
- **Dims 2 and 3 are optional add-ons.** Additive with Dim 1 (different value streams). Only for customer-facing agents.
- **Three tiers** (Conservative/Moderate/Optimistic) are always computed for Dim 1 — present all three to give stakeholders a range.

## Quick Reference

```python
# §Load Script
import sys, os
sys.argv = ['bedrock_pricing.py']
script = ("tco_bva_capacity_skills/skills/bedrock-pricing/scripts/bedrock_pricing.py"
          if os.environ.get("USE_IN_KIRO") or os.environ.get("USE_IN_CLAUDE_CODE")
          else os.path.expanduser("~/.quickwork/skills/bedrock-pricing/scripts/bedrock_pricing.py"))
exec(open(script).read())

# 1. Create session directory
session_dir = create_report_session(label="roi-analysis", volume=10000)

# 2. Get agent cost (from prior pricing run or user-provided)
agent_cost_monthly = bedrock_cost["monthly_total"] + agentcore_cost["total_monthly"]

# 3. Calculate business value
result = calculate_business_value(
    sessions_per_month=10000,
    agent_cost_monthly=agent_cost_monthly,
    output_dir=session_dir,
)
# Returns: grand_total_annual, roi_pct, payback_days, net_value_annual
```

## Workflow

### 1. Present Dimension Menu

Ask user which dimensions apply:

| # | Dimension | Default | Notes |
|---|-----------|---------|-------|
| **1a** | Productivity Increase (revenue uplift) | Selected | Mutually exclusive with 1b |
| **1b** | Cost Savings (labor cost reduction) | Available | Mutually exclusive with 1a |
| **2** | Customer Churn Reduction | Unselected | Customer-facing agents only |
| **3** | Sales Increase from Better CX | Unselected | Customer-facing agents only |

- If user asks for Dims 2 or 3 without a customer name, ask for it.
- Do NOT auto-include Dims 2/3.

### 2. Customer Data Lookup (if customer name provided)

Search for: annual revenue, employees, customer base, churn rate, industry.

```
Search: "{customer_name} annual revenue employees headcount 2024 2025"
revenue_per_hour = annual_revenue / employees / 2000
```

- Be transparent — share what you found, note gaps, let user decide.
- For Dims 2/3: also search for total customers, churn rate, annual sales revenue.

### 3. Present Assumptions

Show all parameters and values. Ask user to confirm before calculating:
- Sessions per month, agent cost
- Time without AI vs. with AI
- Agent effectiveness % and efficiency factor %
- Human cost/hr and revenue/hr
- Dim 2: total customers, churn rates, revenue per customer
- Dim 3: annual sales revenue, sales increase %

Only proceed after user confirms or adjusts values.

### 4. Calculate

```python
session_dir = create_report_session(label="roi-analysis", volume=1000000)

result = calculate_business_value(
    sessions_per_month=1_000_000,
    agent_cost_monthly=44497,
    # Dim 1
    time_without_ai_min=20,
    time_with_ai_min=10,
    human_cost_per_hour=75,
    revenue_per_hour=300,
    # Dim 2 (set total_customers=0 to skip)
    total_customers=100_000,
    churn_without_ai_pct=2.0,
    churn_with_ai_pct=1.0,
    revenue_per_customer_year=1000,
    # Dim 3 (set annual_sales_revenue=0 to skip)
    annual_sales_revenue=100_000_000,
    sales_increase_pct=10.0,
    # Report
    output_dir=session_dir,
)
```

### 5. Present Results

The function writes a detailed report and returns:

```python
{
    "file_path": "~/bedrock_reports/.../business-value.md",
    "grand_total_annual": 5400000.00,
    "net_value_annual": 4866000.00,
    "roi_pct": 912,
    "payback_days": 36,
    "dim1_moderate_annual": 4680000.00,
    "dim2_annual": 500000.00,
    "dim3_annual": 220000.00,
    "agent_cost_annual": 534000.00,
}
```

Present:
1. **Key metrics** — ROI, payback, net value
2. **Grand total** — combined annual value, net of agent cost
3. **File reference** — point user to report for detailed breakdown

- Show annual projections (monthly looks small to stakeholders)
- Always show all 3 tiers for Dim 1 (Conservative/Moderate/Optimistic)
- If `_file_write_failed: True` → full result is inline, format all dimensions

### 6. Completeness Check (MANDATORY)

| # | Check | Condition | Action |
|---|-------|-----------|--------|
| 1 | Assumptions confirmed | Always | Present all parameters to user before calculating |
| 2 | Agent cost sourced correctly | Always | From pricing result or user-provided — never guessed |
| 3 | Reports in session directory | Multiple calculations | Use `create_report_session()` + `output_dir` |
| 4 | All 3 tiers shown for Dim 1 | Always | Conservative/Moderate/Optimistic range |

### 7. Offer Follow-ups

- Add/change dimensions
- Adjust assumptions
- Different customer
- Switch between 1a and 1b

## Related Skills

| Skill | When to load |
|-------|-------------|
| `bedrock-pricing` | Need model cost to feed `agent_cost_monthly` |
| `agentcore-pricing` | Need infrastructure cost to feed `agent_cost_monthly` |
| `bedrock-capacity` | Verify workload fits before building business case |
