# Bedrock Agent Pricing & Business Value Skills

A 4-skill toolkit for estimating costs, planning capacity, and calculating business value for Amazon Bedrock agent workloads. Works with **Quick Desktop**, **Kiro**, and **Claude Code**. All pricing data comes from live AWS Pricing API — never from model training data.

---

## Skill Family Overview

| Skill | What it does |
|-------|-------------|
| **bedrock-pricing** | Model inference costs (per-token pricing, prompt caching, multi-turn sessions) |
| **agentcore-pricing** | AgentCore infrastructure costs (Runtime, Gateway, Memory, BrowserTool, CodeInterpreter, Evaluations) |
| **bedrock-capacity** | RPM/TPM capacity planning — checks if your workload fits within quota limits |
| **agent-business-value** | ROI calculation — time savings, cost reduction, churn reduction, sales uplift |

All four skills share a single Python script (`bedrock_pricing.py`) and local JSON cache files.

---

## Prerequisites

1. **Python 3.10+** — for running the cache refresh from the command line
2. **AWS CLI credentials** — required *only* for the cache refresh step (the script calls the AWS Pricing API and Service Quotas API). You need:
   - `aws configure` or AWS SSO configured (`aws sso login`)
   - IAM permissions: `pricing:GetProducts`, `service-quotas:ListServiceQuotas`
3. **Environment variable** (Kiro / Claude Code only) — set one of the following in your shell profile (`~/.zshrc` or `~/.bashrc`):

   ```bash
   # If using Kiro:
   export USE_IN_KIRO=1

   # If using Claude Code:
   export USE_IN_CLAUDE_CODE=1
   ```

   If neither is set, the skills assume **Quick Desktop** and use the hardcoded path `~/.quickwork/skills/...`.

   > **Note:** This repo must be the open workspace root in Kiro or Claude Code. The skills resolve the script path relative to the workspace directory.

---

## Installation

### Quick Desktop

Copy the entire skill folders into your Quick Desktop skills directory:

```
~/.quickwork/skills/
├── bedrock-pricing/
│   ├── SKILL.md
│   ├── pricing_spec_v1.1.md
│   └── scripts/
│       └── bedrock_pricing.py
├── agentcore-pricing/
│   └── SKILL.md
├── bedrock-capacity/
│   └── SKILL.md
└── agent-business-value/
    └── SKILL.md
```

**To import:** Copy the folders into `~/.quickwork/skills/`. Quick Desktop scans this directory automatically — the skills will appear in your next new conversation. No restart required.

### Kiro

1. Open this repo as your workspace in Kiro
2. Set the environment variable in your shell profile:
   ```bash
   export USE_IN_KIRO=1
   ```
3. Restart your terminal (or `source ~/.zshrc`) so Kiro picks up the variable

The skills will resolve the script path relative to the workspace root.

### Claude Code

1. Open this repo as your workspace in Claude Code
2. Set the environment variable in your shell profile:
   ```bash
   export USE_IN_CLAUDE_CODE=1
   ```
3. Restart your terminal (or `source ~/.zshrc`) so Claude Code picks up the variable

The skills will resolve the script path relative to the workspace root.

### Create the Pricing Cache Files (all platforms)

The skills read pricing data from local JSON cache files in `~/bedrock_cache/`. You **must** generate these before first use.

**Run from your terminal:**

```bash
# Kiro / Claude Code (from workspace root):
python3 tco_bva_capacity_skills/skills/bedrock-pricing/scripts/bedrock_pricing.py --refresh

# Quick Desktop:
python3 ~/.quickwork/skills/bedrock-pricing/scripts/bedrock_pricing.py --refresh
```

This command fetches:
- **Pricing data** (per-token costs for all models, all tiers) — covers **all 35 Bedrock regions** automatically. The AWS Pricing API returns global data in a single call.
- **RPM/TPM/TPD quotas** — fetched per-region for **10 major regions** by default: `us-east-1`, `us-west-2`, `eu-west-1`, `eu-central-1`, `ap-northeast-1`, `ap-southeast-1`, `ap-southeast-2`, `ap-south-1`, `ca-central-1`, `sa-east-1`. Use `--all-regions` for all 33, or `--quota-regions` to specify your own.

This creates 5 files in `~/bedrock_cache/`:

| File | Contents | Size |
|------|----------|------|
| `bedrock_pricing.json` | 1P Amazon models + newer 3P models | ~12 MB |
| `bedrock_pricing_3p.json` | 3P Marketplace models (Anthropic, Meta, Mistral, etc.) | ~5 MB |
| `bedrock_pricing_service.json` | Very new models (latest additions) | ~300 KB |
| `bedrock_pricing_agentcore.json` | AgentCore component pricing (Runtime, Gateway, Memory, etc.) | ~230 KB |
| `bedrock_quotas.json` | RPM/TPM default quotas per model/region | Varies |

**Optional flags** (append to either command above):

```bash
--skip-quotas                    # Pricing only, skip quotas (~2 min)
--quota-regions "us-west-2,us-east-1"  # Override default regions
--all-regions                    # ALL 33 Bedrock regions (slow — ~15 min)
```

> **💡 Tip:** Re-run `--refresh` periodically (e.g., monthly) to pick up new models and price changes. The skills always read from these local files — they never call the Pricing API at runtime.

### Verify Installation

Open a new conversation and type:

> *"Get Bedrock pricing for Claude Sonnet 4 in Oregon"*

If the skill activates and returns pricing, you're all set.

---

## Usage Examples

### Use Case 1: Compare Model Costs for an Agent

**Prompt:**
> *"I'm building an agent that handles 500K sessions per month. Each session has 3 questions. Compare Claude Sonnet 4.6 vs Nova Pro in us-east-1."*

**What it does:**
- Looks up per-token prices for both models from the cache
- Calculates monthly cost with incremental prefix caching enabled
- Shows a side-by-side comparison: cached vs. no-cache baseline, per-session cost, annual projection
- Highlights savings percentage from prompt caching

**Expected output:** A comparison table showing monthly cost, per-session cost, caching savings %, and annual totals for both models.

---

### Use Case 2: Full Agent Cost + Capacity Check

**Prompt:**
> *"Estimate the full cost for an agent on AgentCore using Claude Sonnet 4.6 in Oregon. 1M sessions/month, 5 questions per session, 3 tool calls per question. Include Runtime, Gateway, and Memory. Then check if it fits within default RPM/TPM quotas."*

**What it does:**
1. Calculates model inference cost (with prompt caching)
2. Calculates AgentCore infrastructure: Runtime (vCPU + memory), Gateway (invocations + search), Memory (STM + LTM)
3. Produces a combined monthly total (model + infra)
4. Runs capacity planning: computes required peak RPM/TPM and compares against default Standard tier quotas
5. If it doesn't fit, provides an optimization checklist (reduce `max_tokens`, enable caching, trim RAG chunks, etc.)

**Expected output:** Combined cost breakdown table + capacity fit analysis with recommendations.

---

### Use Case 3: Business Value & ROI Justification

**Prompt:**
> *"For the agent above, calculate the business value. This is an internal support agent replacing 20-minute manual tasks. With AI it takes 10 minutes. Human cost is $75/hr fully loaded. Show cost savings and ROI."*

**What it does:**
1. Uses the agent cost from the prior estimate (or asks you to specify)
2. Calculates time savings across 3 tiers (conservative/moderate/optimistic) with research-backed effectiveness and efficiency factors
3. Computes net value (savings − agent cost), ROI %, and FTE equivalents
4. Can generate a visual chart comparing value vs. cost

**Expected output:** Tiered savings table, net ROI, FTE equivalents, and optionally a Highcharts visualization.

---

## Architecture

```
┌──────────────────────────────────────────────────┐
│         Quick / Kiro / Claude Code                │
│                                                   │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────┐ │
│  │  bedrock-    │  │  agentcore-  │  │ bedrock- │ │
│  │  pricing     │  │  pricing     │  │ capacity │ │
│  │  (SKILL.md)  │  │  (SKILL.md)  │  │(SKILL.md)│ │
│  └──────┬───────┘  └──────┬───────┘  └────┬─────┘ │
│         │                 │               │       │
│  ┌──────┴─────────────────┴───────────────┴─────┐ │
│  │         bedrock_pricing.py (shared)           │ │
│  │  Path resolved via env var:                   │ │
│  │  • USE_IN_KIRO / USE_IN_CLAUDE_CODE → repo   │ │
│  │  • Neither set → ~/.quickwork/skills/...      │ │
│  │                                               │ │
│  │  Functions:                                   │ │
│  │  • query_model_pricing()                      │ │
│  │  • query_agentcore_pricing()                  │ │
│  │  • extract_bedrock_model_prices()             │ │
│  │  • calculate_agent_session_compounded_cost()   │ │
│  │  • calculate_agentcore_cost()                 │ │
│  │  • calculate_business_value()                 │ │
│  │  • calculate_evaluation_cost()                │ │
│  │  • check_capacity_fit()                       │ │
│  │  • query_quotas()                             │ │
│  └──────────────────┬───────────────────────────┘ │
│                     │                             │
│  ┌──────────────────┴───────────────────────────┐ │
│  │          Local JSON Cache (~/*)               │ │
│  │  bedrock_pricing.json                         │ │
│  │  bedrock_pricing_3p.json                      │ │
│  │  bedrock_pricing_service.json                 │ │
│  │  bedrock_pricing_agentcore.json               │ │
│  │  bedrock_quotas.json                          │ │
│  └──────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────┘
                      ▲
                      │ --refresh (one-time setup)
                      │
            ┌─────────┴──────────┐
            │  AWS Pricing API   │
            │  AWS Service Quotas│
            └────────────────────┘
```

---

## Report Output

When you run a pricing, capacity, or business value calculation, the skills write detailed reports to markdown files on disk. This keeps the in-conversation response compact while preserving full detail for review, sharing, or auditing.

### Where reports are saved

Reports are written to `~/bedrock_reports/` by default. When multiple calculations are run for the same question (e.g., model pricing + AgentCore + business value), they are grouped in a **session directory**:

```
~/bedrock_reports/
└── claude-sonnet-4.6_10k-sessions_20260527-133154-0e72/
    ├── bedrock-pricing.md      # Full token breakdown, caching strategy, capacity summary
    ├── agentcore.md            # Runtime/Gateway/Memory cost breakdown
    └── business-value.md       # ROI, payback, tiered value analysis
```

### What's in the reports

Each report contains:
- **YAML front-matter** — metadata (timestamp, totals, inputs hash) for programmatic parsing
- **Summary tables** — key metrics at a glance
- **Assumptions** — all parameters used in the calculation
- **Detailed breakdown** — step-by-step formulas and intermediate values

### Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `reports.output_dir` | `~/bedrock_reports` | Where reports are written |
| `reports.retention_days` | 30 | Auto-cleanup threshold |
| `reports.auto_cleanup` | false | Delete old reports automatically |
| `reports.include_metadata` | true | Include YAML front-matter |

Configure via `~/.bedrock_skills/config.yaml` (user-level) or `./.bedrock_skills.yaml` (project-level). Run `python3 bedrock_pricing.py --init-config` to generate a template.

### Manual cleanup

```bash
python3 bedrock_pricing.py --cleanup-reports
```

Deletes reports and session directories older than the configured retention period.

---

## Troubleshooting

| Problem | Cause | Fix |
|---------|-------|-----|
| "Cache file not found" | Haven't run `--refresh` yet | Run the refresh command (see Installation) |
| Skill doesn't activate | Skill folder not in the right path | Verify env var is set (Kiro/Claude Code) or folder is at `~/.quickwork/skills/` (Quick) |
| "No results for model X" | Model name doesn't match cache | Try a broader search (e.g., "Sonnet" instead of "Sonnet 4.6") or refresh cache for latest models |
| AWS credentials error on `--refresh` | No AWS CLI config | Run `aws configure` or `aws sso login` first |
| Quotas file empty | Region not included in refresh | Re-run with `--quota-regions "us-west-2,us-east-1"` |
| Stale pricing data | Cache not refreshed recently | Re-run `--refresh` — recommend monthly |
| Skill works but numbers seem off | Using old cache with new models | Re-run `--refresh` to pick up latest prices |
| "bedrock_pricing.py not found" | Env var not set or wrong workspace | Set `USE_IN_KIRO=1` or `USE_IN_CLAUDE_CODE=1` and ensure repo is workspace root |

---

## Key Defaults & Assumptions

These defaults are used when you don't specify values. You can override any of them in your prompt.

| Parameter | Default | Notes |
|-----------|---------|-------|
| Service tier | Standard (On-Demand) | Also supports Priority, Flex |
| Inference variant | Global (cross-region) | Also supports Regional |
| Prompt caching | Enabled | Auto-enabled for models that support it |
| Questions per session | 5 | Multi-turn conversation |
| Input tokens per question | 100 | User's question text |
| Output tokens per question | 100 | Model's response |
| System prompt tokens | 1,000 | Sent every LLM call |
| Tool description tokens | 500 | Tool schemas in prompt |
| RAG chunks per question | 10 | Retrieved context |
| Tokens per RAG chunk | 300 | Chunk size |
| Tools invoked per question | 3 | Tool-use turns |
| Output burndown rate | 5× for Claude 3.7+ | Each output token = 5 TPM quota |
| Peak-to-average ratio | 3× | For capacity planning |
| Active hours/day | 12 | Business hours + buffer |

---

## Limitations

- **Pricing only, not billing** — estimates are based on published list prices. Actual bills may differ due to committed-use discounts, EDPs, or negotiated rates.
- **No Reserved Instances pricing** — the skills cover On-Demand (Standard/Priority/Flex) and Batch tiers only.
- **Quota defaults** — the capacity skill uses default service quotas. Your account may have custom limits from prior increase requests.
- **Business value is illustrative** — the ROI calculations use research-backed assumptions (BCG, Harvard, Gartner) but are estimates, not guarantees. Always validate with your specific use case data.
- **Costs depend on your assumptions** — all estimates are driven by the input parameters you provide (sessions, tokens, tools, time saved, etc.) and built-in defaults. Different use cases and workload profiles will produce very different numbers. Always ask Quick to *"list assumptions"* and *"show the step-by-step explanation"* so you understand exactly how the costs and business value were calculated before sharing results.

---

## Refreshing the Cache

Re-run periodically to pick up new models and price changes:

```bash
# Kiro / Claude Code (from workspace root):
python3 tco_bva_capacity_skills/skills/bedrock-pricing/scripts/bedrock_pricing.py --refresh

# Quick Desktop:
python3 ~/.quickwork/skills/bedrock-pricing/scripts/bedrock_pricing.py --refresh

# Add --skip-quotas for pricing only (~2 min)
# Add --all-regions for all 33 Bedrock regions (~15 min)
```

> **⚠️ The skill automatically warns you when cache files are older than 7 days.** If you see a staleness warning during a pricing query, re-run the `--refresh` command above to update.

You can also share the generated JSON files directly with teammates who don't have AWS CLI access — just have them copy the 5 JSON files to their `~/` directory.

---

## Related Skills: QA Testing & Validation

- **bedrock-cost-test-generator** — Generates test case JSON specs for any agent pricing scenario. Give it a use case name, model, region, and volume, and it produces a structured test fixture with expected values. These test specs are consumed by the QA judge below. Prompt: *"Generate test cases for a Haiku 4.5 agent in us-east-1 with 200K sessions"*

- **bedrock-cost-qa** — An independent QA judge that verifies pricing and business value estimates using pure math. It re-implements every formula from the pricing spec, independently recalculates all values, and produces a pass/fail report with per-field verification. Runs a mandatory self-test first to prove its own correctness. Prompt: *"QA the pricing estimate"* or *"Verify this cost calculation"*
