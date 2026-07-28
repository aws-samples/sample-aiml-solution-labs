---
name: bedrock-tier-advisor
display_name: Bedrock Tier & Variant Advisor
description: "Recommends the right Bedrock service tier (Flex/Standard/Priority/Reserved/Batch) and inference variant (Global/Regional) based on workload characteristics. Activate when the user asks which tier to use, wants to optimize cost vs performance, or hasn't specified a tier for a pricing estimate. Reads guidance from bedrock-tier-guidance.md which can be refreshed from AWS documentation."
icon: "🎯"
trigger: bedrock tier advice
depends-on: [bedrock-pricing]
inputs:
  - name: workload_type
    description: "Type of workload: production, dev-test, batch, mission-critical. Helps narrow tier recommendation."
    type: string
    required: false
  - name: model
    description: "Model name (e.g. 'Claude Sonnet 4.6'). Used to discover available tiers from cache."
    type: string
    required: false
  - name: region
    description: "AWS region code (e.g. us-east-1). Used to discover available variants."
    type: string
    required: false
tools: [run_python, file_read]
---

## Overview

Advises which Bedrock service tier and cross-region inference variant to use for a given workload. Combines static guidance (from `bedrock-tier-guidance.md`) with dynamic discovery (from the pricing cache) to produce a recommendation.

Part of the skill family: `bedrock-pricing`, `agentcore-pricing`, `bedrock-capacity`, `agent-business-value`, `bedrock-tier-advisor` (this).

## Guidance File

The tier and variant guidance is stored in `bedrock-tier-guidance.md` in the same directory as this skill file.

## Workflow

### Step 1: Load Guidance
Read `bedrock-tier-guidance.md` for the decision framework.

### Step 2: Discover Available Tiers for the Model
Use `extract_bedrock_model_prices(results, all_tiers=True)` from the `bedrock-pricing` script to see which tiers and variants actually exist for the user's model in their region.

```python
import sys, os
sys.argv = ['bedrock_pricing.py']
_p = os.path.join(os.getcwd(), "skills/bedrock-pricing/scripts/bedrock_pricing.py")
if not os.path.exists(_p):
    _p = os.path.expanduser("~/.quickwork/skills/bedrock-pricing/scripts/bedrock_pricing.py")
exec(open(_p).read())

home = os.path.expanduser("~/bedrock_cache")
results = query_model_pricing(home, region_filter="us-east-1", model_filter="Claude Sonnet 4.6")
all_prices = extract_bedrock_model_prices(results, all_tiers=True)
# Returns: {"Standard Global": {...}, "Standard Regional": {...}, "Batch Global": {...}, ...}
```

### Step 3: Apply Decision Framework
Match the user's workload characteristics against the guidance:

| User Says | Recommend |
|-----------|-----------|
| "production agent" / "customer-facing" | Standard Global (with prompt caching if available) |
| "dev/test" / "experimenting" | Flex if available, else Standard |
| "mission-critical" / "can't tolerate latency" | Priority if available, else Standard |
| "bulk processing" / "offline" / "not real-time" | Batch |
| "data must stay in region" / "compliance" | Regional variant |
| No preference stated | Standard Global (cheapest on-demand with prompt caching) |

### Step 4: Present Recommendation
Show the recommended tier + variant with reasoning, and list alternatives with trade-offs.

Example output:
```
Recommended: Standard Global ($3.00/$15.00 per M tokens)
- Prompt caching available (saves ~60% on input costs)
- Global variant is ~10% cheaper than Regional
- Good for production workloads with moderate traffic

Alternatives:
- Batch Global ($1.50/$7.50) — 50% cheaper, but async (24h window)
- Standard Regional ($3.30/$16.50) — use if data must stay in us-east-1
```

## Keeping Guidance Current

The `bedrock-tier-guidance.md` file should be refreshed when AWS updates their tier structure or pricing guidance. This typically happens 1-2 times per year.

**After running `--refresh` on the pricing cache**, update the guidance by opening Quick Desktop and asking:

> "Read the current guidance file at `~/.quickwork/skills/bedrock-tier-advisor/bedrock-tier-guidance.md`. Query AWS documentation using the `aws-documentation-mcp-server` to extract the latest tier and variant guidance from these pages:
> - https://docs.aws.amazon.com/bedrock/latest/userguide/service-tiers-inference.html
> - https://docs.aws.amazon.com/bedrock/latest/userguide/capacity-limits-cost-optimization.html
> - https://docs.aws.amazon.com/bedrock/latest/userguide/cross-region-inference.html
>
> Update `bedrock-tier-guidance.md` with any changes. Preserve the file structure and source citations."

## Configuration

Model preferences and tier defaults are managed by the YAML configuration system in `bedrock_pricing.py`.
Override any default via `~/.bedrock_skills/config.yaml` (user-level) or `./.bedrock_skills.yaml` (project-level).

Run `python3 bedrock_pricing.py --init-config` to generate a commented template showing all
available settings with their current defaults.

**Precedence:** function parameter > environment variable > project config > user config > hardcoded default

**Config values are defaults only.** If the user specifies a value in their prompt, always use
the user's value. Config defaults apply only to parameters the user has not mentioned.

Relevant config sections:
- `model_preferences` — default model selections by agent role (router, general, rag, research) and version
- `defaults.tier_preference` — preferred service tier when user doesn't specify

## Lessons Learned

### Do
- Always discover available tiers from cache before recommending — not all models support all tiers
- Default to Standard Global when the user hasn't expressed a preference
- Prefer tiers with prompt caching support — it's the biggest cost lever
- Show the price difference between recommended and alternative tiers
- Cite the guidance source URLs so users can verify

### Don't
- Don't recommend Flex for production workloads — no SLA, high throttling risk
- Don't recommend Priority unless the user has a clear latency/reliability need — it's 3.5× the cost
- Don't assume all models have Global variants — Llama, Qwen, and GPT-OSS are Regional only
- Don't hardcode tier names in recommendations — always discover from cache
