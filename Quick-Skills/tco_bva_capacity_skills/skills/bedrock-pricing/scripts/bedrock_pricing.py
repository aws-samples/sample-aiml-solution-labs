#!/usr/bin/env python3
"""
bedrock_pricing.py — Bedrock cost estimation, capacity planning, and business value analysis.

Shared engine for 4 skills: bedrock-pricing, agentcore-pricing, bedrock-capacity, agent-business-value.
Works with Quick Desktop, Kiro, and Claude Code.

CLI usage:
  python3 bedrock_pricing.py --refresh                    # Fetch pricing + quotas from AWS
  python3 bedrock_pricing.py --region us-west-2 --model "Claude Sonnet 4"  # Query prices
  python3 bedrock_pricing.py --init-config                # Generate config template
  python3 bedrock_pricing.py --cleanup-reports            # Remove old reports

═══════════════════════════════════════════════════════════════════════════════
API QUICK REFERENCE — 11 Common Patterns
═══════════════════════════════════════════════════════════════════════════════

All patterns assume: cache_dir = "~/bedrock_cache", region is always required.

FASTEST PATH (most common):
    cost = estimate_cost(cache_dir, "us-west-2", "Claude Sonnet 4", 10000)

Pattern 1 — Single model cost (with control over params):
    prices = get_model_prices(cache_dir, "us-west-2", "Claude Sonnet 4")
    cost = calculate_agent_session_compounded_cost(
        main_agent_config={**prices, "agent_sessions_per_month": 10000})

Pattern 2 — Compare models:
    for model in ["Claude Sonnet 4", "Claude Haiku 4.5", "Nova 2.0 Lite"]:
        cost = estimate_cost(cache_dir, "us-west-2", model, 10000)

Pattern 3 — Compare tiers (Standard vs Batch vs Priority):
    results = query_model_pricing(cache_dir, "us-west-2", model_filter="Claude Sonnet 4")
    all_prices = extract_bedrock_model_prices(results, all_tiers=True)
    # → {"Standard Global": {...}, "Batch Global": {...}, "Priority Global": {...}}

Pattern 4 — Compare regions:
    for region in ["us-west-2", "eu-west-1", "ap-northeast-1"]:
        cost = estimate_cost(cache_dir, region, "Claude Sonnet 4", 10000)

Pattern 5 — Token-only (no prices needed):
    tokens = calculate_compounded_tokens_for_agent(questions_per_agent_session=5, tools_invoked=3)
    rag_tokens = calculate_rag_subagent_tokens(rag_n_chunks=10, output_tokens=300)
    research_tokens = calculate_research_subagent_tokens(n_research_iterations=4)

Pattern 6 — Cost → Capacity fit:
    cost = estimate_cost(cache_dir, "us-west-2", "Claude Sonnet 4", 10000)
    fit = check_capacity_fit(cost["capacity_profile"]["main_agent"], questions_per_month=1000000)

Pattern 7 — Cost → Business value:
    cost = estimate_cost(cache_dir, "us-west-2", "Claude Sonnet 4", 10000)
    bva = calculate_business_value(10000, agent_cost_monthly=cost["monthly_total"])

Pattern 8 — AgentCore infrastructure costs:
    ac_prices = query_agentcore_pricing(cache_dir, "us-west-2")
    ac_cost = calculate_agentcore_cost(runtime_vcpu_price_hr=..., ...)

Pattern 9 — What-if / sensitivity (change one param):
    cost_a = estimate_cost(cache_dir, "us-west-2", "Claude Sonnet 4", 10000, system_prompt_tokens=2000)
    cost_b = estimate_cost(cache_dir, "us-west-2", "Claude Sonnet 4", 10000, system_prompt_tokens=5000)

Pattern 10 — Blended cost (model routing):
    cost_complex = estimate_cost(cache_dir, "us-west-2", "Claude Sonnet 4", 10000)
    cost_simple = estimate_cost(cache_dir, "us-west-2", "Claude Haiku 4.5", 10000)
    blended = cost_complex["session_total"] * 0.15 + cost_simple["session_total"] * 0.85

Pattern 11 — Full stack (Bedrock + AgentCore + Capacity + Business Value):
    cost = estimate_cost(cache_dir, "us-west-2", "Claude Sonnet 4", 10000)
    ac_cost = calculate_agentcore_cost(...)
    fit = check_capacity_fit(cost["capacity_profile"]["main_agent"], questions_per_month=10000*5)
    bva = calculate_business_value(10000, agent_cost_monthly=cost["monthly_total"] + ac_cost["total_monthly"])

Sub-agent prices (for multi-agent):
    prices = get_model_prices(cache_dir, "us-west-2", "Nova 2.0 Lite")
    # → {"input_price": 0.33, "output_price": 2.75, "cache_read_price": 0.0825,
    #    "cache_write_price": 0.0, "min_cache_tokens": 1024, "model_name": "Nova 2.0 Lite"}
    # Pass directly as sub-agent model_prices (keys already match)

═══════════════════════════════════════════════════════════════════════════════
"""

import json
import sys
import argparse
import os
import time
from collections import defaultdict


def _fmt(n):
    """Format a number with commas for display in explanations."""
    return f"{n:,.0f}" if isinstance(n, (int, float)) else str(n)

SERVICE_CODES = {
    "AmazonBedrockFoundationModels": "3P Marketplace models",
    "AmazonBedrock": "1P Amazon models + newer 3P models",
    "AmazonBedrockService": "Very new models",
    "AmazonBedrockAgentCore": "AgentCore components",
}

MODEL_SERVICE_CODES = [
    "AmazonBedrockFoundationModels",
    "AmazonBedrock",
    "AmazonBedrockService",
]

AGENTCORE_SERVICE_CODE = "AmazonBedrockAgentCore"

CACHE_FILES = {
    "AmazonBedrock": "bedrock_pricing.json",
    "AmazonBedrockFoundationModels": "bedrock_pricing_3p.json",
    "AmazonBedrockService": "bedrock_pricing_service.json",
    "AmazonBedrockAgentCore": "bedrock_pricing_agentcore.json",
}

QUOTAS_CACHE_FILE = "bedrock_quotas.json"

PROVIDER_RULES = [
    (["Claude", "Anthropic"], "Anthropic"),
    (["Llama", "Meta"], "Meta (Llama)"),
    (["Nova", "Titan"], "Amazon"),
    (["Mistral", "Mixtral", "Pixtral", "Ministral", "Magistral", "Devstral", "Voxtral"], "Mistral AI"),
    (["Cohere", "Command", "Embed"], "Cohere"),
    (["Stable", "Stability", "SDXL"], "Stability AI"),
    (["DeepSeek"], "DeepSeek"),
    (["Qwen"], "Alibaba (Qwen)"),
    (["Gemma"], "Google (Gemma)"),
    (["GPT", "gpt"], "OpenAI (GPT-OSS)"),
    (["Nemotron", "NVIDIA", "Nvidia"], "NVIDIA"),
    (["Writer", "Palmyra"], "Writer"),
    (["Kimi"], "Moonshot (Kimi)"),
    (["GLM"], "Zhipu (GLM)"),
    (["MiniMax", "Minimax"], "MiniMax"),
    (["AI21", "Jamba", "Jurassic"], "AI21 Labs"),
    (["Ray"], "Ray"),
]

# TODO: MODEL_FAMILY_RULES requires manual maintenance when AWS adds new model families.
# Unrecognized models fall into "Other" silently. Consider auto-discovering families from
# model name patterns at refresh time instead of maintaining a hardcoded list.
#
# Matching uses exact token match (split on spaces/hyphens), so rule ordering does NOT
# matter for version numbers ("4" won't match "3.4" since they're different tokens).
# Keywords must match tokens exactly as they appear in model names (e.g., "qwen3" not "qwen").
MODEL_FAMILY_RULES = [
    (["opus"], "Opus"),
    (["sonnet"], "Sonnet"),
    (["haiku"], "Haiku"),
    (["claude", "instant"], "Claude Instant"),
    (["claude"], "Claude"),
    (["nova", "sonic"], "Nova Sonic"),
    (["nova", "canvas"], "Nova Canvas"),
    (["nova", "reel"], "Nova Reel"),
    (["nova", "omni"], "Nova Omni"),
    (["nova", "pro"], "Nova Pro"),
    (["nova", "lite"], "Nova Lite"),
    (["nova", "micro"], "Nova Micro"),
    (["nova", "premier"], "Nova Premier"),
    (["nova"], "Nova"),
    (["titan", "text"], "Titan Text"),
    (["titan", "embed"], "Titan Embed"),
    (["titan", "image"], "Titan Image"),
    (["titan", "multimodal"], "Titan Multimodal"),
    (["titan"], "Titan"),
    (["llama", "4"], "Llama 4"),
    (["llama", "3.3"], "Llama 3.3"),
    (["llama", "3.2"], "Llama 3.2"),
    (["llama", "3.1"], "Llama 3.1"),
    (["llama", "3"], "Llama 3"),
    (["llama", "2"], "Llama 2"),
    (["llama"], "Llama"),
    (["mistral", "large"], "Mistral Large"),
    (["mistral", "small"], "Mistral Small"),
    (["mixtral"], "Mixtral"),
    (["ministral"], "Ministral"),
    (["mistral"], "Mistral"),
    (["command", "r+"], "Command R+"),
    (["command", "r"], "Command R"),
    (["command"], "Command"),
    (["embed"], "Embed"),
    (["jamba"], "Jamba"),
    (["deepseek"], "DeepSeek"),
    (["qwen"], "Qwen"),
    (["palmyra"], "Palmyra"),
    (["stable", "diffusion"], "Stable Diffusion"),
    (["stable", "image"], "Stable Image"),
    (["sdxl"], "SDXL"),
    (["pegasus"], "Pegasus"),
    (["marengo"], "Marengo"),
    (["glm"], "GLM"),
    (["gemma"], "Gemma"),
    (["nemotron"], "Nemotron"),
    (["phi"], "Phi"),
    (["kimi"], "Kimi"),
    (["minimax"], "MiniMax"),
]

MODEL_INDEX_FILE = "bedrock_model_index.json"


def _classify_model_family(model_name):
    """Classify a model name into its family using MODEL_FAMILY_RULES.

    Uses exact token matching (split on spaces/hyphens) to avoid substring
    false positives (e.g., "4" matching inside "3.4" or "405B").

    Alphabetic-only keywords also match as prefixes, so "qwen" matches
    tokens like "qwen3", "qwen4" without needing a rule per version.
    Keywords containing digits/dots use exact match only.
    """
    tokens = _re.split(r"[\s\-]+", model_name.lower())
    for keywords, family in MODEL_FAMILY_RULES:
        if all(_keyword_matches(kw, tokens) for kw in keywords):
            return family
    return "Other"


def _keyword_matches(keyword, tokens):
    """Check if a keyword matches any token — exact, or prefix if keyword is alpha-only."""
    if keyword in tokens:
        return True
    if keyword.isalpha():
        return any(t.startswith(keyword) for t in tokens)
    return False


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION SYSTEM
# ═══════════════════════════════════════════════════════════════════════════════
# Provides YAML-based config file support with layered precedence:
#   function parameter > environment variable > project config > user config > hardcoded default
#
# Config files:
#   User-level:    ~/.bedrock_skills/config.yaml
#   Project-level: ./.bedrock_skills.yaml
#
# Run: python3 bedrock_pricing.py --init-config  to generate a commented template.
# ═══════════════════════════════════════════════════════════════════════════════

# Try to import PyYAML — required for config file parsing
try:
    import yaml
    _YAML_AVAILABLE = True
except ImportError:
    _YAML_AVAILABLE = False

# Single source of truth for all config sections, keys, types, defaults, and validation rules.
# Used by: _load_config(), _validate_config(), _resolve_setting(), generate_config_template()
CONFIG_SCHEMA = {
    "reports": {
        "_description": "Configure report output preferences (used by file-based report output feature)",
        "output_dir": {"type": str, "default": "~/bedrock_reports", "description": "Directory for report files"},
        "format": {"type": str, "default": "markdown", "choices": ["markdown", "json", "csv"], "description": "Report output format"},
        "retention_days": {"type": int, "default": 30, "min": 1, "max": 3650, "description": "Days to keep old reports before auto-cleanup"},
        "naming_template": {"type": str, "default": "{model}_{volume}_{timestamp}.md", "max_len": 128, "description": "Report filename template. Placeholders: {model}, {volume}, {timestamp}, {region}, {format}"},
        "include_metadata": {"type": bool, "default": True, "description": "Include YAML front-matter metadata in reports"},
        "auto_cleanup": {"type": bool, "default": False, "description": "Automatically delete reports older than retention_days"},
    },
    "defaults": {
        "_description": "General defaults for region, tier, and history mode",
        "region": {"type": str, "default": None, "description": "Default AWS region for pricing queries (e.g., us-west-2)"},
        "tier_preference": {"type": str, "default": None, "description": "Preferred service tier (e.g., Standard, Priority, Flex)"},
        "history_mode": {"type": str, "default": "full", "choices": ["full", "condensed"], "description": "How conversation history is carried across questions"},
    },
    "agent_defaults": {
        "_description": "Default token parameters for main agent cost calculations",
        "questions_per_session": {"type": int, "default": 5, "min": 1, "description": "Questions per agent session"},
        "input_tokens": {"type": int, "default": 100, "min": 1, "description": "User question tokens per question"},
        "output_tokens": {"type": int, "default": 150, "min": 1, "description": "Agent final answer tokens per question"},
        "system_prompt_tokens": {"type": int, "default": 2000, "min": 1, "description": "System prompt tokens (sent every LLM call)"},
        "tools_passed": {"type": int, "default": 10, "min": 0, "description": "Number of tools in agent schema"},
        "tool_spec_tokens": {"type": int, "default": 100, "min": 1, "description": "Tokens per tool specification"},
        "tools_invoked": {"type": int, "default": 5, "min": 0, "description": "Tool calls per question"},
        "tool_call_tokens": {"type": int, "default": 100, "min": 1, "description": "Model output tokens per tool call"},
        "tool_result_tokens": {"type": int, "default": 100, "min": 1, "description": "Tokens per tool result returned to agent"},
    },
    "rag_defaults": {
        "_description": "Default parameters for RAG sub-agent token calculations",
        "system_prompt_tokens": {"type": int, "default": 500, "min": 1, "description": "RAG sub-agent system prompt tokens"},
        "n_tools": {"type": int, "default": 2, "min": 0, "description": "Tools available to RAG sub-agent"},
        "tool_spec_tokens": {"type": int, "default": 100, "min": 1, "description": "Tokens per tool spec in RAG sub-agent"},
        "input_query_tokens": {"type": int, "default": 100, "min": 1, "description": "Query tokens from main agent to RAG"},
        "tool_call_tokens": {"type": int, "default": 50, "min": 1, "description": "Model output per tool call in RAG sub-agent"},
        "rag_n_retrieval_calls": {"type": int, "default": 2, "min": 1, "description": "Number of KB retrieval calls"},
        "rag_n_chunks": {"type": int, "default": 10, "min": 1, "description": "Chunks returned per retrieval call"},
        "rag_chunk_size": {"type": int, "default": 300, "min": 1, "description": "Tokens per RAG chunk"},
        "n_other_tool_calls": {"type": int, "default": 1, "min": 0, "description": "Other tool calls (reranker, etc.)"},
        "other_tool_result_tokens": {"type": int, "default": 200, "min": 1, "description": "Tokens per other tool result"},
        "output_tokens": {"type": int, "default": 300, "min": 1, "description": "RAG response tokens returned to main agent"},
    },
    "research_defaults": {
        "_description": "Default parameters for research sub-agent token calculations",
        "system_prompt_tokens": {"type": int, "default": 500, "min": 1, "description": "Research sub-agent system prompt tokens"},
        "n_tools": {"type": int, "default": 2, "min": 0, "description": "Tools available to research sub-agent (search, fetch)"},
        "tool_spec_tokens": {"type": int, "default": 50, "min": 1, "description": "Tokens per tool spec in research sub-agent"},
        "input_query_tokens": {"type": int, "default": 100, "min": 1, "description": "Query tokens from main agent to research"},
        "tool_call_tokens": {"type": int, "default": 50, "min": 1, "description": "Model output per tool call in research sub-agent"},
        "n_research_iterations": {"type": int, "default": 4, "min": 1, "description": "Search-then-optional-fetch cycles"},
        "fetch_probability": {"type": float, "default": 0.5, "min": 0.0, "max": 1.0, "description": "Probability each search leads to a fetch (0.0-1.0)"},
        "search_result_tokens": {"type": int, "default": 100, "min": 1, "description": "Tokens from web_search result"},
        "fetch_result_tokens": {"type": int, "default": 2000, "min": 1, "description": "Tokens from web_fetch result"},
        "output_tokens": {"type": int, "default": 1000, "min": 1, "description": "Research response tokens returned to main agent"},
    },
    "agentcore_defaults": {
        "_description": "Default AgentCore infrastructure parameters for runtime cost calculations",
        "num_vcpus": {"type": int, "default": 2, "min": 1, "description": "vCPUs allocated to agent runtime"},
        "peak_memory_gb": {"type": float, "default": 4.0, "min": 0.5, "description": "Peak memory (GB) allocated to agent runtime"},
        "io_wait_pct": {"type": float, "default": 0.70, "min": 0.0, "max": 1.0, "description": "Fraction of time spent in I/O wait (vCPU is free during wait)"},
        "idle_time_between_questions_s": {"type": int, "default": 30, "min": 0, "description": "User think time between questions (seconds)"},
        "stm_events_per_question": {"type": int, "default": 2, "min": 0, "description": "Short-term memory write events per question"},
        "ltm_records_per_session": {"type": int, "default": 3, "min": 0, "description": "Long-term memory records extracted per session"},
        "ltm_retrievals_per_question": {"type": int, "default": 1, "min": 0, "description": "Long-term memory retrievals per question"},
        "tools_indexed": {"type": int, "default": 50, "min": 0, "description": "Number of tools indexed in Gateway (flat monthly fee)"},
        "browser_io_wait_pct": {"type": float, "default": 0.70, "min": 0.0, "max": 1.0, "description": "BrowserTool I/O wait fraction (estimated — page loads + LLM responses). Validate against actual billing."},
        "ci_io_wait_pct": {"type": float, "default": 0.20, "min": 0.0, "max": 1.0, "description": "CodeInterpreter I/O wait fraction (estimated — mostly active CPU). Validate against actual billing."},
        "eval_sampling_rate": {"type": float, "default": 0.10, "min": 0.0, "max": 1.0, "description": "Fraction of sessions evaluated (0.0-1.0)"},
        "eval_builtin_evaluators": {"type": int, "default": 3, "min": 0, "description": "Number of built-in evaluators (e.g., Helpfulness, Correctness, Safety)"},
    },
    "business_value_defaults": {
        "_description": "Default assumptions for business value / ROI calculations",
        "time_without_ai_min": {"type": float, "default": 20.0, "min": 0.1, "description": "Minutes per task without AI assistance"},
        "time_with_ai_min": {"type": float, "default": 10.0, "min": 0.1, "description": "Minutes per task with AI assistance"},
        "human_cost_per_hour": {"type": float, "default": 75.0, "min": 0.0, "description": "Fully-loaded human labor cost ($/hr)"},
        "revenue_per_hour": {"type": float, "default": 300.0, "min": 0.0, "description": "Revenue per employee hour (for productivity dimension)"},
        "agent_effectiveness_pct": {"type": float, "default": 0.65, "min": 0.0, "max": 1.0, "description": "Fraction of sessions where AI meaningfully helps (moderate tier)"},
        "efficiency_factor_pct": {"type": float, "default": 0.60, "min": 0.0, "max": 1.0, "description": "Fraction of saved time converted to productive output"},
        "churn_without_ai_pct": {"type": float, "default": 2.0, "min": 0.0, "description": "Monthly churn rate without AI (%)"},
        "churn_with_ai_pct": {"type": float, "default": 1.0, "min": 0.0, "description": "Monthly churn rate with AI (%)"},
        "sales_increase_pct": {"type": float, "default": 10.0, "min": 0.0, "description": "Sales increase from better CX (%)"},
    },
    "capacity": {
        "_description": "Capacity planning assumptions for RPM/TPM/TPD fit checks",
        "peak_to_avg_ratio": {"type": float, "default": 3.0, "min": 1.0, "max": 100.0, "description": "Peak-to-average traffic ratio during active hours"},
        "active_hours_per_day": {"type": int, "default": 12, "min": 1, "max": 24, "description": "Hours per day with active traffic"},
        "active_days_per_month": {"type": int, "default": 22, "min": 1, "max": 31, "description": "Business days per month with active traffic"},
        "max_tokens_setting": {"type": int, "default": 4096, "min": 1, "max": 65536, "description": "max_tokens parameter reserved per request (affects TPM)"},
    },
    "pricing_cache": {
        "_description": "Settings for the local pricing data cache (JSON files from AWS Pricing API)",
        "dir": {"type": str, "default": "~/bedrock_cache", "description": "Directory for pricing cache files"},
        "max_age_days": {"type": int, "default": 7, "min": 1, "max": 365, "description": "Days before cache files are considered stale"},
        "auto_refresh": {"type": bool, "default": False, "description": "Automatically refresh stale cache before queries"},
    },
    "behavior": {
        "_description": "Behavioral preferences for script execution",
        "skip_confirmation": {"type": bool, "default": False, "description": "Skip interactive confirmation prompts before calculations"},
        "auto_capacity_check": {"type": bool, "default": False, "description": "Automatically run capacity fit check after cost calculations"},
    },
    "model_preferences": {
        "_description": "Default model selections by agent role (search hints for query_model_pricing)",
        "router": {"type": str, "default": "Claude Opus", "description": "Model for orchestrator/router agents"},
        "general": {"type": str, "default": "Claude Sonnet", "description": "Model for main inference agents"},
        "rag": {"type": str, "default": "Claude Haiku", "description": "Model for RAG sub-agents (cost-efficient)"},
        "research": {"type": str, "default": "Nova Lite", "description": "Model for research sub-agents"},
        "version": {"type": str, "default": "latest", "description": "Model version: 'latest' or pin to specific (e.g., '4.6')"},
    },
}

# Module-level config state (loaded lazily on first access)
_LOADED_CONFIG = None


def _ensure_config_loaded():
    """Load config on first access (lazy initialization)."""
    global _LOADED_CONFIG
    if _LOADED_CONFIG is None:
        _LOADED_CONFIG = _load_config()


def _deep_merge(base, override):
    """Recursively merge override into base. Override wins for leaf values.

    Args:
        base (dict): Base dictionary (e.g., user config).
        override (dict): Override dictionary (e.g., project config). Wins at leaf level.

    Returns:
        dict: New merged dictionary. Neither input is mutated.
    """
    result = dict(base)
    for key, val in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(val, dict):
            result[key] = _deep_merge(result[key], val)
        else:
            result[key] = val
    return result


def _read_yaml_file(path):
    """Read and parse a YAML file using PyYAML.

    Args:
        path (str): Absolute or relative path to the YAML file.

    Returns:
        dict | None: Parsed YAML as dict, or None on error.
        Emits warnings to stderr on parse errors or missing PyYAML.
    """
    if not _YAML_AVAILABLE:
        print("⚠️  PyYAML is required for config file support. Install with: pip install pyyaml",
              file=sys.stderr)
        return None

    if not os.path.exists(path):
        return None

    try:
        with open(path, "r") as f:
            content = f.read()
    except PermissionError:
        print(f"⚠️  Config: Permission denied reading '{path}'. Skipping.", file=sys.stderr)
        return None
    except OSError as e:
        print(f"⚠️  Config: Cannot read '{path}': {e}. Skipping.", file=sys.stderr)
        return None

    # Handle empty files
    if not content.strip():
        return {}

    try:
        data = yaml.safe_load(content)
        # yaml.safe_load returns None for empty/comment-only YAML
        return data if isinstance(data, dict) else {}
    except yaml.YAMLError as e:
        # Extract line number if available
        if hasattr(e, 'problem_mark') and e.problem_mark:
            line = e.problem_mark.line + 1
            print(f"⚠️  Config: Invalid YAML in '{path}' at line {line}: {e.problem}. Skipping file.",
                  file=sys.stderr)
        else:
            print(f"⚠️  Config: Invalid YAML in '{path}': {e}. Skipping file.", file=sys.stderr)
        return None


def _validate_config(config, source_path):
    """Validate a config dict against CONFIG_SCHEMA.

    Checks for: unknown sections, type mismatches, out-of-range values, invalid choices.
    Reports ALL errors (doesn't stop at first).

    Args:
        config (dict): Parsed config dictionary.
        source_path (str): File path (for error messages).

    Returns:
        tuple[dict, list[str]]: (validated_config with invalid values removed, list of warning messages)
    """
    warnings = []
    validated = {}

    if not isinstance(config, dict):
        warnings.append(f"Config '{source_path}': Expected a YAML mapping (dict), got {type(config).__name__}. Skipping.")
        return {}, warnings

    for section_name, section_data in config.items():
        # Check if section is recognized
        if section_name not in CONFIG_SCHEMA:
            warnings.append(f"Config '{source_path}': Unrecognized section '{section_name}'. Ignoring.")
            continue

        if not isinstance(section_data, dict):
            warnings.append(f"Config '{source_path}': Section '{section_name}' should be a mapping, got {type(section_data).__name__}. Ignoring.")
            continue

        schema_section = CONFIG_SCHEMA[section_name]
        validated_section = {}

        for key, value in section_data.items():
            # Skip internal keys
            if key.startswith("_"):
                continue

            # Check if key is recognized in this section
            if key not in schema_section:
                warnings.append(f"Config '{source_path}': Unknown key '{section_name}.{key}'. Ignoring.")
                continue

            field_spec = schema_section[key]
            expected_type = field_spec["type"]

            # Type check
            if expected_type == float and isinstance(value, int):
                value = float(value)  # Allow int where float expected
            elif expected_type == int and isinstance(value, float) and value == int(value):
                value = int(value)  # Allow 5.0 where int expected

            if not isinstance(value, expected_type):
                warnings.append(
                    f"Config '{source_path}': '{section_name}.{key}' expected {expected_type.__name__}, "
                    f"got {type(value).__name__} ({repr(value)}). Using default."
                )
                continue

            # Range check (min)
            if "min" in field_spec and isinstance(value, (int, float)):
                if value < field_spec["min"]:
                    warnings.append(
                        f"Config '{source_path}': '{section_name}.{key}' = {value} is below minimum "
                        f"{field_spec['min']}. Using default."
                    )
                    continue

            # Range check (max)
            if "max" in field_spec and isinstance(value, (int, float)):
                if value > field_spec["max"]:
                    warnings.append(
                        f"Config '{source_path}': '{section_name}.{key}' = {value} is above maximum "
                        f"{field_spec['max']}. Using default."
                    )
                    continue

            # Choices check
            if "choices" in field_spec and isinstance(value, str):
                if value not in field_spec["choices"]:
                    warnings.append(
                        f"Config '{source_path}': '{section_name}.{key}' = '{value}' is not valid. "
                        f"Options: {field_spec['choices']}. Using default."
                    )
                    continue

            # Max length check (strings)
            if "max_len" in field_spec and isinstance(value, str):
                if len(value) > field_spec["max_len"]:
                    warnings.append(
                        f"Config '{source_path}': '{section_name}.{key}' exceeds max length "
                        f"{field_spec['max_len']} (got {len(value)}). Using default."
                    )
                    continue

            # Value passed all checks
            validated_section[key] = value

        if validated_section:
            validated[section_name] = validated_section

    return validated, warnings


def _load_config(user_path=None, project_path=None):
    """Load and merge YAML configuration files.

    Discovers user-level and project-level config files, validates both,
    and produces a merged config where project values override user values.

    Args:
        user_path (str|None): Override user config path. Default: ~/.bedrock_skills/config.yaml
        project_path (str|None): Override project config path. Default: ./.bedrock_skills.yaml

    Returns:
        dict: Merged and validated configuration. Empty dict if no config files found.
    """
    global _LOADED_CONFIG

    if user_path is None:
        user_path = os.path.expanduser("~/.bedrock_skills/config.yaml")
    if project_path is None:
        project_path = os.path.join(os.getcwd(), ".bedrock_skills.yaml")

    user_dict = {}
    project_dict = {}

    # Read user config
    raw_user = _read_yaml_file(user_path)
    if raw_user is not None and raw_user:
        validated, warnings = _validate_config(raw_user, user_path)
        for w in warnings:
            print(w, file=sys.stderr)
        user_dict = validated

    # Read project config
    raw_project = _read_yaml_file(project_path)
    if raw_project is not None and raw_project:
        validated, warnings = _validate_config(raw_project, project_path)
        for w in warnings:
            print(w, file=sys.stderr)
        project_dict = validated

    # Deep merge: project wins over user
    merged = _deep_merge(user_dict, project_dict)

    _LOADED_CONFIG = merged
    return merged


def _get_config(section=None, key=None):
    """Access the loaded configuration.

    Args:
        section (str|None): Config section name. None returns full config.
        key (str|None): Key within section. None returns full section dict.

    Returns:
        dict | Any | None: Full config, section dict, specific value, or None if not found.
    """
    _ensure_config_loaded()

    if section is None:
        return _LOADED_CONFIG or {}

    section_data = (_LOADED_CONFIG or {}).get(section)
    if section_data is None:
        return None if key else {}

    if key is None:
        return section_data

    return section_data.get(key)


def _resolve_setting(section, key, explicit_value=None, env_var=None):
    """Resolve a setting through the full precedence chain.

    Precedence: explicit_value > environment variable > config file > schema default

    Args:
        section (str): Config section name (e.g., "agent_defaults").
        key (str): Setting key within section (e.g., "input_tokens").
        explicit_value: Function parameter value. None means "not provided".
        env_var (str|None): Environment variable name to check. If None, auto-generates
            from section+key as BEDROCK_{SECTION}_{KEY} (uppercase).

    Returns:
        The resolved value with correct type.
    """
    # 1. Explicit value wins (anything that is not None)
    if explicit_value is not None:
        return explicit_value

    # Get field spec for type info and default
    field_spec = CONFIG_SCHEMA.get(section, {}).get(key)
    if field_spec is None:
        return None  # Unknown section/key

    expected_type = field_spec["type"]
    default_value = field_spec["default"]

    # 2. Check environment variable
    if env_var is None:
        env_var = f"BEDROCK_{section.upper()}_{key.upper()}"

    env_value = os.environ.get(env_var, "")
    if env_value:
        try:
            if expected_type == bool:
                converted = env_value.lower() in ("true", "1", "yes")
            elif expected_type == int:
                converted = int(env_value)
            elif expected_type == float:
                converted = float(env_value)
            else:
                converted = env_value
            return converted
        except (ValueError, TypeError):
            print(f"⚠️  Config: Environment variable '{env_var}' = '{env_value}' cannot be converted "
                  f"to {expected_type.__name__}. Ignoring.", file=sys.stderr)

    # 3. Check config file
    _ensure_config_loaded()
    config_value = (_LOADED_CONFIG or {}).get(section, {}).get(key)
    if config_value is not None:
        return config_value

    # 4. Schema default
    return default_value


def generate_config_template(output_path=None, force=False):
    """Generate a commented YAML config template from CONFIG_SCHEMA.

    Writes a fully documented template with all settings commented out,
    showing types, descriptions, valid options, and default values.

    Args:
        output_path (str|None): Where to write. Default: ~/.bedrock_skills/config.yaml
        force (bool): Overwrite existing file without prompting.

    Returns:
        str: The generated YAML content.
    """
    if output_path is None:
        output_path = os.path.expanduser("~/.bedrock_skills/config.yaml")

    # Build template content
    lines = []
    lines.append("# Bedrock Skills Configuration")
    lines.append("# Generated by: python3 bedrock_pricing.py --init-config")
    lines.append("#")
    lines.append("# Precedence: function parameter > env var > project config > user config > hardcoded default")
    lines.append("# Config values are defaults only. If the user specifies a value in their prompt,")
    lines.append("# always use the user's value. Config defaults apply only to parameters not mentioned.")
    lines.append("#")
    lines.append(f"# User-level:    ~/.bedrock_skills/config.yaml")
    lines.append(f"# Project-level: ./.bedrock_skills.yaml")
    lines.append("")

    section_names = []
    for section_name, section_spec in CONFIG_SCHEMA.items():
        section_names.append(section_name)
        description = section_spec.get("_description", section_name)
        lines.append(f"# {'─' * 70}")
        lines.append(f"# {description}")
        lines.append(f"# {'─' * 70}")
        lines.append(f"{section_name}:")

        for key, field_spec in section_spec.items():
            if key.startswith("_"):
                continue

            field_type = field_spec["type"].__name__
            field_desc = field_spec.get("description", key)
            field_default = field_spec["default"]

            # Build type annotation with constraints
            type_info = f"{field_type}"
            if "min" in field_spec and "max" in field_spec:
                type_info += f", {field_spec['min']}-{field_spec['max']}"
            elif "min" in field_spec:
                type_info += f", min {field_spec['min']}"
            if "choices" in field_spec:
                type_info += f", options: {field_spec['choices']}"

            lines.append(f"  # {key} ({type_info}): {field_desc}")
            if field_default is None:
                lines.append(f"  # {key}: null")
            elif isinstance(field_default, bool):
                lines.append(f"  # {key}: {'true' if field_default else 'false'}")
            else:
                lines.append(f"  # {key}: {field_default}")
            lines.append("")

        lines.append("")

    content = "\n".join(lines)

    # Handle file writing
    target_dir = os.path.dirname(output_path)
    if target_dir and not os.path.exists(target_dir):
        try:
            os.makedirs(target_dir, exist_ok=True)
        except OSError as e:
            print(f"❌ Cannot create directory '{target_dir}': {e}", file=sys.stderr)
            return content

    # Check if file exists
    if os.path.exists(output_path) and not force:
        if sys.stdin.isatty():
            response = input(f"Config file already exists at '{output_path}'. Overwrite? [y/N] ")
            if response.lower() not in ("y", "yes"):
                print("Aborted. No changes made.", file=sys.stderr)
                return content
        else:
            print(f"❌ Config file already exists at '{output_path}'. Use --force to overwrite.",
                  file=sys.stderr)
            return content

    try:
        with open(output_path, "w") as f:
            f.write(content)
        print(f"✅ Config template written to: {os.path.abspath(output_path)}")
        print(f"   Sections: {', '.join(section_names)}")
    except OSError as e:
        print(f"❌ Cannot write config file '{output_path}': {e}", file=sys.stderr)

    return content


# ═══════════════════════════════════════════════════════════════════════════════
# REPORT FILE OUTPUT
# ═══════════════════════════════════════════════════════════════════════════════
# Writes detailed markdown reports to disk and returns compact summaries.
# Default output: ~/bedrock_reports/{model}_{volume}_{timestamp}-{hex}.md
# ═══════════════════════════════════════════════════════════════════════════════

import re as _re
import hashlib as _hashlib


def _sanitize_filename(name):
    """Convert string to filesystem-safe slug.

    'Claude Sonnet 4.6' → 'claude-sonnet-4.6'
    'Nova Lite (v2)' → 'nova-lite-v2'
    '' → 'unknown-model'
    """
    if not name:
        return "unknown-model"
    slug = name.lower().strip()
    slug = _re.sub(r'[^a-z0-9.\-]', '-', slug)
    slug = _re.sub(r'-+', '-', slug)
    return slug.strip('-') or "unknown-model"


def _format_volume(sessions_per_month):
    """Format session volume for filename.

    10000 → '10k-sessions', 1500000 → '1m-sessions', 500 → '500-sessions'
    """
    if not sessions_per_month or sessions_per_month <= 0:
        return "0-sessions"
    if sessions_per_month >= 1_000_000:
        return f"{sessions_per_month // 1_000_000}m-sessions"
    elif sessions_per_month >= 1_000:
        return f"{sessions_per_month // 1_000}k-sessions"
    else:
        return f"{sessions_per_month}-sessions"


def _generate_report_path(model_name, sessions_per_month, output_dir=None, output_path=None):
    """Resolve full file path for a report.

    Args:
        model_name: Model name (e.g., "Claude Sonnet 4.6")
        sessions_per_month: Volume for filename
        output_dir: Override output directory (None = use config/default)
        output_path: Explicit full path (overrides everything)

    Returns:
        str: Absolute path to the report file
    """
    if output_path:
        return os.path.abspath(os.path.expanduser(output_path))

    if output_dir is None:
        output_dir = _resolve_setting("reports", "output_dir")
    output_dir = os.path.expanduser(output_dir)

    template = _resolve_setting("reports", "naming_template")
    random_hex = os.urandom(2).hex()  # 4-char hex to prevent same-second collisions
    timestamp = time.strftime("%Y%m%d-%H%M%S") + f"-{random_hex}"
    model_slug = _sanitize_filename(model_name or "")
    volume_slug = _format_volume(sessions_per_month)

    filename = template.format(
        model=model_slug, volume=volume_slug, timestamp=timestamp,
        region="", format="md",
    )
    return os.path.join(output_dir, filename)


def _build_front_matter(result, main_agent_config):
    """Build YAML front-matter metadata block for the report file.

    Returns empty string if reports.include_metadata config is False.
    """
    if not _resolve_setting("reports", "include_metadata"):
        return ""

    inputs_str = json.dumps(main_agent_config, sort_keys=True, default=str)
    inputs_hash = _hashlib.sha256(inputs_str.encode()).hexdigest()[:16]

    # Calculate savings_pct for front-matter
    no_cache = result.get("session_total_no_cache", 0)
    with_cache = result.get("session_total", 0)
    savings_pct = ((no_cache - with_cache) / no_cache * 100) if no_cache > 0 else 0

    lines = ["---"]
    lines.append(f'generated_at: "{time.strftime("%Y-%m-%dT%H:%M:%S")}"')
    lines.append(f'model: "{main_agent_config.get("model_name", "unknown")}"')
    lines.append(f'region: "{main_agent_config.get("region", "unknown")}"')
    lines.append(f'sessions_per_month: {main_agent_config.get("agent_sessions_per_month", 0)}')
    lines.append(f'session_total: {round(result.get("session_total", 0), 6)}')
    lines.append(f'monthly_total: {round(result.get("monthly_total", 0), 2)}')
    lines.append(f'annual_total: {round(result.get("annual_total", 0), 2)}')
    lines.append(f'savings_pct: {round(savings_pct, 1)}')
    lines.append(f'inputs_hash: "{inputs_hash}"')
    lines.append("---")
    lines.append("")
    return "\n".join(lines)


def _try_write(file_path, content):
    """Attempt atomic write to file_path. Returns absolute path on success, None on failure."""
    target_dir = os.path.dirname(file_path)
    try:
        os.makedirs(target_dir, exist_ok=True)
    except OSError as e:
        print(f"⚠️  Report: Cannot create directory '{target_dir}': {e}", file=sys.stderr)
        return None

    # Atomic write: write to temp file, then rename
    tmp_path = file_path + ".tmp"
    try:
        with open(tmp_path, "w") as f:
            f.write(content)
        os.rename(tmp_path, file_path)
    except OSError as e:
        print(f"⚠️  Report: Cannot write '{file_path}': {e}", file=sys.stderr)
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        return None

    return os.path.abspath(file_path)


def _write_report_to_file(result, main_agent_config, subagents=None, output_path=None):
    """Write full report to markdown file with cascade fallback.

    Cascade: tries output_path/configured path → default dir → returns None.

    Returns:
        str: Absolute path to written file, or None if all attempts fail.
    """
    model_name = main_agent_config.get("model_name", "unknown")
    sessions = main_agent_config.get("agent_sessions_per_month", 0)

    # Build content first (before touching filesystem)
    front_matter = _build_front_matter(result, main_agent_config)
    try:
        body = _format_full_output(result)
    except Exception:
        # Fallback: write token_table + basic summary as JSON
        body = result.get("token_table", "") + "\n\n" + json.dumps(
            result.get("explanation", {}), indent=2, default=str)
    content = front_matter + body

    # Try writing — cascade: configured/explicit path → default dir
    file_path = _generate_report_path(model_name, sessions, output_path=output_path)
    written_path = _try_write(file_path, content)

    if written_path is None and output_path:
        # Explicit path failed — try default directory as fallback
        default_path = _generate_report_path(model_name, sessions)
        written_path = _try_write(default_path, content)

    # Auto-cleanup if configured and write succeeded
    if written_path and _resolve_setting("reports", "auto_cleanup"):
        _cleanup_old_reports()

    return written_path


def _identify_top_cost_driver(result):
    """Identify the largest cost component for the summary."""
    main = result.get("main_agent", {})
    # "with_cache" = prompt-cached cost, "no_cache" = without prompt caching
    main_cost = (main.get("with_cache") or main.get("no_cache") or {}).get("session_total", 0)
    subagents = result.get("subagents", [])
    sub_total = sum(sa.get("session_cost", 0) for sa in subagents)

    if sub_total > main_cost and subagents:
        top_sa = max(subagents, key=lambda x: x.get("session_cost", 0))
        return f"sub-agent ({top_sa.get('type', 'unknown')})"
    return "main agent (token compounding)"


def _build_compact_summary(result, file_path):
    """Build the compact summary dict returned to the agent.

    Contains key metrics, file_path, and capacity_profile for downstream
    use by check_capacity_fit().
    """
    # Calculate savings_pct
    no_cache = result.get("session_total_no_cache", 0)
    with_cache = result.get("session_total", 0)
    savings_pct = ((no_cache - with_cache) / no_cache * 100) if no_cache > 0 else 0

    # Safe access to main agent session cost (prompt-cache-aware)
    main = result.get("main_agent", {})
    cached = main.get("with_cache") or main.get("no_cache") or {}
    main_session_cost = cached.get("session_total", 0)  # with prompt cache if available

    # Build per-sub-agent summary with caching status
    subagents_summary = []
    for sa in result.get("subagents", []):
        cost_detail = sa.get("cost_detail", {})
        sa_summary = {
            "type": sa.get("type"),
            "session_cost": round(sa.get("session_cost", 0), 6),
            "caching_applied": cost_detail.get("caching_applied", False),
        }
        if cost_detail.get("caching_applied"):
            sa_summary["cache_savings_pct"] = round(cost_detail.get("cache_savings_pct", 0), 1)
        else:
            caching_str = cost_detail.get("explanation", {}).get("pricing", {}).get("caching", "")
            if "Not applied" in caching_str:
                reason = caching_str.removeprefix("Not applied (")
                if reason.endswith(")"):
                    reason = reason[:-1]
                sa_summary["cache_not_applied_reason"] = reason
        subagents_summary.append(sa_summary)

    return {
        "file_path": file_path,
        "sessions_per_month": result.get("sessions_per_month", 0),
        "monthly_total": round(result.get("monthly_total", 0), 2),
        "annual_total": round(result.get("annual_total", 0), 2),
        "session_total": round(with_cache, 6),
        "session_total_no_cache": round(no_cache, 6),
        "savings_pct": round(savings_pct, 1),
        "main_agent_session_cost": round(main_session_cost, 6),
        "subagent_session_cost": round(
            sum(sa.get("session_cost", 0) for sa in result.get("subagents", [])), 6),
        "subagents_summary": subagents_summary,
        "recommended_ttl": main.get("recommended_ttl"),
        "top_cost_driver": _identify_top_cost_driver(result),
        "capacity_profile": result.get("capacity_profile"),
    }


def _cleanup_old_reports(output_dir=None, max_age_days=None):
    """Delete report files and session directories older than retention threshold.

    For flat files: matches naming template pattern, checks file mtime.
    For session directories: checks directory mtime only, deletes entire dir.

    Returns:
        dict: {"deleted_count": int, "freed_bytes": int}
    """
    if output_dir is None:
        output_dir = os.path.expanduser(_resolve_setting("reports", "output_dir"))
    if max_age_days is None:
        max_age_days = _resolve_setting("reports", "retention_days")

    if not os.path.isdir(output_dir):
        return {"deleted_count": 0, "freed_bytes": 0}

    # Derive filename pattern from template (for flat bedrock pricing files)
    template = _resolve_setting("reports", "naming_template")
    pattern = _re.escape(template)
    pattern = pattern.replace(_re.escape("{model}"), r"[a-z0-9.\-]+")
    pattern = pattern.replace(_re.escape("{volume}"), r"[a-z0-9\-]+")
    pattern = pattern.replace(_re.escape("{timestamp}"), r"\d{8}-\d{6}-[a-f0-9]{4}")
    pattern = pattern.replace(_re.escape("{region}"), r"[a-z0-9\-]*")
    pattern = pattern.replace(_re.escape("{format}"), r"[a-z]+")
    report_re = _re.compile(f"^{pattern}$")

    # Pattern for typed flat files (agentcore, eval, bva)
    typed_re = _re.compile(r"^(agentcore|eval|bva)_[a-z0-9\-]+_\d{8}-\d{6}-[a-f0-9]{4}\.md$")

    cutoff = time.time() - (max_age_days * 86400)
    deleted = 0
    freed = 0

    for f in os.listdir(output_dir):
        path = os.path.join(output_dir, f)

        # Handle session directories
        if os.path.isdir(path):
            if os.path.getmtime(path) < cutoff:
                # Calculate total size before deletion
                dir_size = sum(
                    os.path.getsize(os.path.join(dp, fn))
                    for dp, _, fns in os.walk(path) for fn in fns
                )
                try:
                    _shutil.rmtree(path)
                    deleted += 1
                    freed += dir_size
                except OSError:
                    pass
            continue

        # Handle flat files (bedrock naming template or typed prefix)
        if not (report_re.match(f) or typed_re.match(f)):
            continue
        if not os.path.isfile(path):
            continue
        if os.path.getmtime(path) < cutoff:
            size = os.path.getsize(path)
            try:
                os.unlink(path)
                deleted += 1
                freed += size
            except OSError:
                pass

    return {"deleted_count": deleted, "freed_bytes": freed}


# ── Report filename constants ──
_BEDROCK_REPORT_FILENAME = "bedrock-pricing.md"
_AGENTCORE_REPORT_FILENAME = "agentcore.md"
_EVAL_REPORT_FILENAME = "evaluations.md"
_BVA_REPORT_FILENAME = "business-value.md"

import shutil as _shutil


def create_report_session(model_name=None, volume=None, label=None):
    """Create a timestamped session directory for grouping related report files.

    Args (optional):
        model_name (str|None): Model name for the directory slug. Default None.
        volume (int|None): Volume number for the directory slug. Default None.
        label (str|None): Custom label — overrides model_name. Default None.

    Example:
        create_report_session(model_name="Claude Sonnet 4.6", volume=10000)

    Returns: absolute path (str) to the created session directory.

    --- Detailed Documentation ---

    The naming convention is enforced by this function — callers pass raw inputs
    and the function handles sanitization, formatting, and timestamp generation.

    Args:
        model_name (str|None): Model name for the slug (e.g., "Claude Sonnet 4.6").
        volume (int|None): Session or question volume for the slug.
        label (str|None): Custom label — overrides model_name in the slug.

    Returns:
        str: Absolute path to the created session directory.
    """
    if label:
        slug = _sanitize_filename(label)
    elif model_name:
        slug = _sanitize_filename(model_name)
    else:
        slug = "report"

    volume_slug = _format_volume(volume)
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    hex_suffix = os.urandom(2).hex()

    dir_name = f"{slug}_{volume_slug}_{timestamp}-{hex_suffix}"
    base_dir = os.path.expanduser(_resolve_setting("reports", "output_dir"))
    session_dir = os.path.join(base_dir, dir_name)
    os.makedirs(session_dir, exist_ok=True)
    return os.path.abspath(session_dir)


def _generate_typed_report_path(report_type, volume, output_dir=None):
    """Generate flat file path for non-bedrock reports (fallback when no session dir).

    Pattern: {report_type}_{volume_slug}_{timestamp}-{hex}.md
    Does NOT use reports.naming_template config.
    """
    if output_dir is None:
        output_dir = os.path.expanduser(_resolve_setting("reports", "output_dir"))
    volume_slug = _format_volume(volume)
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    hex_suffix = os.urandom(2).hex()
    filename = f"{report_type}_{volume_slug}_{timestamp}-{hex_suffix}.md"
    return os.path.join(output_dir, filename)


def _build_typed_front_matter(report_type, result, inputs_dict):
    """Build YAML front-matter for non-bedrock reports.

    Args:
        report_type: "agentcore", "evaluations", or "business-value"
        result: the full result dict from the function
        inputs_dict: dict of key input parameters (for inputs_hash)

    Returns empty string if reports.include_metadata config is False.
    """
    if not _resolve_setting("reports", "include_metadata"):
        return ""

    inputs_str = json.dumps(inputs_dict, sort_keys=True, default=str)
    inputs_hash = _hashlib.sha256(inputs_str.encode()).hexdigest()[:16]

    total_monthly = result.get("total_monthly", 0)
    total_annual = result.get("total_annual", total_monthly * 12)

    lines = ["---"]
    lines.append(f'generated_at: "{time.strftime("%Y-%m-%dT%H:%M:%S")}"')
    lines.append(f'report_type: "{report_type}"')
    lines.append(f"total_monthly: {round(total_monthly, 2)}")
    lines.append(f"total_annual: {round(total_annual, 2)}")
    lines.append(f'inputs_hash: "{inputs_hash}"')
    lines.append("---")
    lines.append("")
    return "\n".join(lines)


def _identify_top_ac_component(result):
    """Identify the largest AgentCore cost component. Returns string like 'runtime (65%)'."""
    components = {
        "runtime": result["runtime"]["total"],
        "gateway": result["gateway"]["total"],
        "memory": result["memory"]["total"],
    }
    if result["browser"]["included"]:
        components["browser"] = result["browser"]["total"]
    if result["code_interpreter"]["included"]:
        components["code_interpreter"] = result["code_interpreter"]["total"]

    total = result["total_monthly"]
    if total <= 0:
        return "none (zero cost)"
    top_name = max(components, key=components.get)
    top_pct = components[top_name] / total * 100
    return f"{top_name} ({top_pct:.0f}%)"


def _format_agentcore_report(result):
    """Format AgentCore cost result as a markdown report string."""
    lines = ["# AgentCore Infrastructure Cost Report", ""]

    # Summary table
    lines.append("## Summary")
    lines.append("")
    lines.append("| Component | Monthly | Annual | % of Total |")
    lines.append("|-----------|---------|--------|------------|")
    total = result["total_monthly"]
    def _pct(v):
        return f"{v / total * 100:.0f}%" if total > 0 else "0%"

    rt = result["runtime"]["total"]
    gw = result["gateway"]["total"]
    mem = result["memory"]["total"]
    lines.append(f"| Runtime (vCPU + Memory) | ${rt:,.2f} | ${rt * 12:,.2f} | {_pct(rt)} |")
    lines.append(f"| Gateway | ${gw:,.2f} | ${gw * 12:,.2f} | {_pct(gw)} |")
    lines.append(f"| Memory (STM + LTM) | ${mem:,.2f} | ${mem * 12:,.2f} | {_pct(mem)} |")
    if result["browser"]["included"]:
        br = result["browser"]["total"]
        lines.append(f"| BrowserTool | ${br:,.2f} | ${br * 12:,.2f} | {_pct(br)} |")
    if result["code_interpreter"]["included"]:
        ci = result["code_interpreter"]["total"]
        lines.append(f"| CodeInterpreter | ${ci:,.2f} | ${ci * 12:,.2f} | {_pct(ci)} |")
    lines.append(f"| **Total** | **${total:,.2f}** | **${total * 12:,.2f}** | **100%** |")
    lines.append("")

    # Assumptions
    lines.append("## Assumptions")
    lines.append("")
    lines.append("| Parameter | Value |")
    lines.append("|-----------|-------|")
    for k, v in result.get("assumptions", {}).items():
        lines.append(f"| {k} | {v} |")
    lines.append("")

    # Detailed Breakdown
    lines.append("## Detailed Breakdown")
    lines.append("")
    explanation = result.get("explanation", {})
    for section_name, section_data in explanation.items():
        lines.append(f"### {section_name.replace('_', ' ').title()}")
        lines.append("")
        if isinstance(section_data, dict):
            for k, v in section_data.items():
                lines.append(f"- **{k}:** {v}")
        else:
            lines.append(str(section_data))
        lines.append("")

    return "\n".join(lines)


def _format_evaluation_report(result):
    """Format evaluation cost result as a markdown report string."""
    lines = ["# AgentCore Evaluations Cost Report", ""]

    # Summary
    lines.append("## Summary")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    lines.append(f"| Sampling rate | {result.get('sampling_rate', 0.10) * 100:.0f}% |")
    lines.append(f"| Evaluated sessions/month | {result['evaluated_sessions']:,.0f} |")
    lines.append(f"| Evaluated questions/month | {result['evaluated_questions']:,.0f} |")
    lines.append(f"| Trace tokens/question | {result['trace_tokens_per_q']:,} |")
    lines.append(f"| Total monthly | ${result['total_monthly']:,.2f} |")
    lines.append(f"| Total annual | ${result['total_annual']:,.2f} |")
    lines.append("")

    # Cost Breakdown
    lines.append("## Cost Breakdown")
    lines.append("")
    lines.append("| Component | Monthly | Annual |")
    lines.append("|-----------|---------|--------|")
    bi = result["builtin"]["total"]
    cl = result["custom_llm"]["total"]
    cc = result["custom_code"]["total"]
    lines.append(f"| Built-in evaluators | ${bi:,.2f} | ${bi * 12:,.2f} |")
    lines.append(f"| Custom LLM evaluators | ${cl:,.2f} | ${cl * 12:,.2f} |")
    lines.append(f"| Custom code evaluators | ${cc:,.2f} | ${cc * 12:,.2f} |")
    lines.append(f"| **Total** | **${result['total_monthly']:,.2f}** | **${result['total_annual']:,.2f}** |")
    lines.append("")

    # Warnings
    if result.get("warnings"):
        lines.append("## Warnings")
        lines.append("")
        for w in result["warnings"]:
            lines.append(f"- ⚠️ {w}")
        lines.append("")

    # Detailed Breakdown
    lines.append("## Detailed Breakdown")
    lines.append("")
    explanation = result.get("explanation", {})
    for section_name, section_data in explanation.items():
        lines.append(f"### {section_name.replace('_', ' ').title()}")
        lines.append("")
        if isinstance(section_data, dict):
            for k, v in section_data.items():
                lines.append(f"- **{k}:** {v}")
        else:
            lines.append(str(section_data))
        lines.append("")

    return "\n".join(lines)


def _format_business_value_report(result):
    """Format business value result as a markdown report string."""
    lines = ["# Agent Business Value Report", ""]

    # Summary
    summary = result.get("summary", {})
    lines.append("## Summary")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    lines.append(f"| Grand Total (annual) | ${summary.get('grand_total', 0):,.2f} |")
    lines.append(f"| Agent Cost (annual) | ${summary.get('agent_cost_annual', 0):,.2f} |")
    lines.append(f"| Net Value (annual) | ${summary.get('net_value', 0):,.2f} |")
    roi = summary.get("roi_pct", 0)
    lines.append(f"| ROI | {'∞' if roi == float('inf') else f'{roi:,.0f}%'} |")
    payback = summary.get("payback_days", 0)
    lines.append(f"| Payback Period | {'N/A' if payback == float('inf') else f'{payback:.0f} days'} |")
    lines.append("")

    # Dimension 1: Time Savings
    lines.append("## Dimension 1: Time Savings")
    lines.append("")
    lines.append("| Tier | Effectiveness | Efficiency | Annual Productivity Uplift | Annual Cost Savings |")
    lines.append("|------|--------------|------------|---------------------------|---------------------|")
    for tier_name in ["Conservative", "Moderate", "Optimistic"]:
        prod = result.get("dim1_productivity", {}).get(tier_name, {})
        cost = result.get("dim1_cost_savings", {}).get(tier_name, {})
        eff = prod.get("effectiveness", 0)
        effi = prod.get("efficiency", 0)
        lines.append(f"| {tier_name} | {eff:.0%} | {effi:.0%} | ${prod.get('annual', 0):,.2f} | ${cost.get('annual', 0):,.2f} |")
    lines.append("")

    # Dimension 2
    dim2 = result.get("dim2", {})
    if dim2:
        lines.append("## Dimension 2: Churn Reduction")
        lines.append("")
        lines.append("| Metric | Value |")
        lines.append("|--------|-------|")
        lines.append(f"| Churn reduction | {dim2.get('churn_reduction_pp', 0):.1f} pp |")
        lines.append(f"| Customers retained | {dim2.get('customers_retained', 0):,.0f} |")
        lines.append(f"| Annual value | ${dim2.get('annual', 0):,.2f} |")
        lines.append("")

    # Dimension 3
    dim3 = result.get("dim3", {})
    if dim3:
        lines.append("## Dimension 3: Sales Increase")
        lines.append("")
        lines.append("| Metric | Value |")
        lines.append("|--------|-------|")
        lines.append(f"| Sales increase | {dim3.get('sales_increase_pct', 0):.1f}% |")
        lines.append(f"| Annual value | ${dim3.get('annual', 0):,.2f} |")
        lines.append("")

    # Assumptions
    lines.append("## Assumptions")
    lines.append("")
    lines.append("| Parameter | Value |")
    lines.append("|-----------|-------|")
    for k, v in result.get("assumptions", {}).items():
        lines.append(f"| {k} | {v} |")
    lines.append("")

    # Detailed Breakdown
    lines.append("## Detailed Breakdown")
    lines.append("")
    explanation = result.get("explanation", {})
    for section_name, section_data in explanation.items():
        lines.append(f"### {section_name.replace('_', ' ').title()}")
        lines.append("")
        if isinstance(section_data, dict):
            for k, v in section_data.items():
                lines.append(f"- **{k}:** {v}")
        else:
            lines.append(str(section_data))
        lines.append("")

    return "\n".join(lines)


def _build_agentcore_summary(result, file_path):
    """Build compact summary dict for AgentCore cost."""
    return {
        "file_path": file_path,
        "total_monthly": round(result["total_monthly"], 2),
        "total_annual": round(result["total_annual"], 2),
        "runtime_monthly": round(result["runtime"]["total"], 2),
        "gateway_monthly": round(result["gateway"]["total"], 2),
        "memory_monthly": round(result["memory"]["total"], 2),
        "browser_monthly": round(result["browser"]["total"], 2),
        "code_interpreter_monthly": round(result["code_interpreter"]["total"], 2),
        "sessions_per_month": result["assumptions"]["sessions_per_month"],
        "questions_per_month": result["assumptions"]["questions_per_month"],
        "top_cost_component": _identify_top_ac_component(result),
    }


def _build_evaluation_summary(result, file_path):
    """Build compact summary dict for evaluation cost."""
    return {
        "file_path": file_path,
        "total_monthly": round(result["total_monthly"], 2),
        "total_annual": round(result["total_annual"], 2),
        "evaluated_sessions": result["evaluated_sessions"],
        "evaluated_questions": result["evaluated_questions"],
        "sampling_rate": result["sampling_rate"],
        "builtin_total": round(result["builtin"]["total"], 2),
        "custom_total": round(result["custom_llm"]["total"] + result["custom_code"]["total"], 2),
    }


def _build_bva_summary(result, file_path):
    """Build compact summary dict for business value."""
    summary = result["summary"]
    roi = summary["roi_pct"]
    payback = summary["payback_days"]
    return {
        "file_path": file_path,
        "grand_total_annual": round(summary["grand_total"], 2),
        "net_value_annual": round(summary["net_value"], 2),
        "roi_pct": "∞" if roi == float("inf") else round(roi, 1),
        "payback_days": "N/A" if payback == float("inf") else round(payback, 0),
        "dim1_moderate_annual": round(summary["dim1_moderate_productivity_annual"], 2),
        "dim2_annual": round(summary["dim2_annual"], 2),
        "dim3_annual": round(summary["dim3_annual"], 2),
        "agent_cost_annual": round(summary["agent_cost_annual"], 2),
        "sessions_per_month": result["assumptions"]["sessions_per_month"],
    }


def _classify_provider(name: str) -> str:
    for keywords, provider in PROVIDER_RULES:
        for kw in keywords:
            if kw.lower() in name.lower():
                return provider
    return "Other"


def _fuzzy_match(query: str, text: str) -> bool:
    q = query.lower().strip()
    t = text.lower()
    if q in t:
        return True
    words = q.split()
    if len(words) > 1 and all(w in t for w in words):
        return True
    if len(words) == 1 and len(q) >= 4:
        for i in range(len(q)):
            if (q[:i] + q[i+1:]) in t:
                return True
    return False


def _parse_price_dimensions(terms: dict) -> list:
    dimensions = []
    for term_type in ["OnDemand", "Reserved"]:
        term_data = terms.get(term_type, {})
        for sku_val in term_data.values():
            for dim_val in sku_val.get("priceDimensions", {}).values():
                price_usd = dim_val.get("pricePerUnit", {}).get("USD", "0")
                try:
                    if price_usd and float(price_usd) > 0:
                        dimensions.append({
                            "term_type": term_type,
                            "description": dim_val.get("description", ""),
                            "unit": dim_val.get("unit", ""),
                            "price_usd": price_usd,
                        })
                except (ValueError, TypeError):
                    pass
    return dimensions


def _get_model_name(attrs: dict) -> str:
    name = attrs.get("model", "")
    if not name:
        name = attrs.get("servicename", attrs.get("group", "Unknown"))
        name = name.replace(" (Amazon Bedrock Edition)", "").strip()
    if not name:
        name = attrs.get("titanModel", attrs.get("titanModelUnit", "Unknown"))
    return name


def _detect_tier(attrs: dict) -> str:
    usage_type = attrs.get("usagetype", "").lower()
    inference_type = attrs.get("inferenceType", "").lower()
    feature = attrs.get("feature", "").lower()
    if "priority" in usage_type or "priority" in inference_type:
        return "Priority"
    elif "flex" in usage_type or "flex" in inference_type:
        return "Flex"
    elif "provisioned" in usage_type or "provisioned" in feature:
        return "Provisioned"
    elif "batch" in usage_type or "batch" in feature:
        return "Batch"
    elif "cache" in usage_type or "cache" in inference_type:
        return "Prompt Caching"
    elif "custom-model" in usage_type or "customization" in feature:
        return "Custom Model"
    return "Standard"


def _detect_variant(attrs: dict) -> str:
    usage_type = attrs.get("usagetype", "").lower()
    if "cross-region-global" in usage_type:
        return "Cross-Region (Global)"
    elif "cross-region" in usage_type:
        return "Cross-Region"
    return "Standard"


def check_pricing_data_status(cache_dir=None):
    """Check whether local pricing cache files are present and fresh.

    Args (optional):
        cache_dir (str): Directory to check. Default ~/bedrock_cache.

    Example:
        check_pricing_data_status()

    Returns: dict with keys status ("ok"|"stale"|"missing"), found, missing, stale, refresh_command.

    --- Detailed Documentation ---

    Lightweight pricing data freshness check — uses only stat calls, no file reads.

    Call once at the start of a session to verify cache files are present and fresh.

    Args:
        cache_dir: Directory to check. Defaults to ~/bedrock_cache.

    Returns:
        dict with keys:
            - status: "ok", "stale", or "missing"
            - found: list of {"file": str, "path": str, "age_days": float}
            - missing: list of filenames not found anywhere
            - stale: list of {"file": str, "path": str, "age_days": int}
            - refresh_command: str — command to run to fix issues
    """
    if cache_dir is None:
        cache_dir = os.path.expanduser(_resolve_setting("pricing_cache", "dir"))

    all_files = {
        "pricing": list(CACHE_FILES.values()),
        "quotas": [QUOTAS_CACHE_FILE],
    }
    flat_files = [f for files in all_files.values() for f in files if f]

    found = []
    missing = []
    stale = []

    max_age_days = _resolve_setting("pricing_cache", "max_age_days")

    for filename in flat_files:
        filepath = os.path.join(cache_dir, filename)
        if os.path.exists(filepath):
            age_days = (time.time() - os.path.getmtime(filepath)) / 86400
            found.append({"file": filename, "path": filepath, "age_days": round(age_days, 1)})
            if age_days > max_age_days:
                stale.append({"file": filename, "path": filepath, "age_days": int(age_days)})
        else:
            missing.append(filename)

    # Determine overall status
    if missing and all(f in missing for f in list(CACHE_FILES.values()) if f):
        status = "missing"
    elif stale:
        status = "stale"
    elif missing:
        status = "partial"
    else:
        status = "ok"

    # Build refresh command based on environment
    if os.environ.get("USE_IN_KIRO") or os.environ.get("USE_IN_CLAUDE_CODE"):
        refresh_cmd = "python3 tco_bva_capacity_skills/skills/bedrock-pricing/scripts/bedrock_pricing.py --refresh"
    else:
        refresh_cmd = "python3 ~/.quickwork/skills/bedrock-pricing/scripts/bedrock_pricing.py --refresh"

    return {
        "status": status,
        "found": found,
        "missing": missing,
        "stale": stale,
        "refresh_command": refresh_cmd,
    }


def _load_cache_file(cache_dir: str, service_code: str) -> list:
    """Load a pricing cache JSON file. Warns if file is older than 7 days."""
    filename = CACHE_FILES.get(service_code, "")
    if not filename:
        return []
    filepath = os.path.join(cache_dir, filename)
    if os.path.exists(filepath):
        try:
            with open(filepath, "r") as f:
                # Check cache age — warn if older than 7 days
                file_age_days = (time.time() - os.path.getmtime(filepath)) / 86400
                if file_age_days > 7:
                    print(f"⚠️  Cache file '{filename}' is {int(file_age_days)} days old. "
                          f"Run --refresh to update.", file=sys.stderr)
                data = json.load(f)
            items = data.get("PriceList", [])
            parsed = []
            for item in items:
                if isinstance(item, str):
                    parsed.append(json.loads(item))
                else:
                    parsed.append(item)
            return parsed
        except Exception as e:
            print(f"Warning: Failed to load {filepath}: {e}", file=sys.stderr)
    return []


def query_model_pricing(cache_dir, region_filter, provider_filter=None, model_filter=None):
    """Query cached pricing data for Bedrock models. Returns raw product entries.

    Warning: output can be very large. Use extract_bedrock_model_prices()
    on the result to collapse to a single {input, output, cache_read, cache_write} dict.

    Args (required):
        cache_dir (str): Path to cache directory (e.g., "~/bedrock_cache").
        region_filter (str): AWS region code (e.g., "us-west-2").
    Args (optional filters):
        provider_filter (str): Provider name (fuzzy match).
        model_filter (str): Model name (fuzzy match, e.g., "Claude Sonnet 4").

    Example:
        results = query_model_pricing("~/bedrock_cache", region_filter="us-west-2",
                                      model_filter="Claude Sonnet 4")
        prices = extract_bedrock_model_prices(results, tier="Standard", variant="Global")
        # prices = {"input": 3.0, "output": 15.0, "cache_read": 0.3, ...}

    Returns: list of raw product dicts (pass to extract_bedrock_model_prices).
    """
    results = []
    for sc in MODEL_SERVICE_CODES:
        products = _load_cache_file(cache_dir, sc)
        for prod in products:
            attrs = prod.get("product", {}).get("attributes", {})
            region_code = attrs.get("regionCode", "")
            if region_code != region_filter:
                continue
            model_name = _get_model_name(attrs)
            provider = attrs.get("provider", "")
            provider = _classify_provider(provider if provider else model_name)
            if provider_filter and not _fuzzy_match(provider_filter, provider):
                continue
            if model_filter and not _fuzzy_match(model_filter, model_name):
                continue
            dimensions = _parse_price_dimensions(prod.get("terms", {}))
            if not dimensions:
                continue
            tier = _detect_tier(attrs)
            variant = _detect_variant(attrs)
            results.append({
                "provider": provider,
                "model": model_name,
                "region": region_code,
                "tier": tier,
                "variant": variant,
                "dimensions": dimensions,
            })
    return results


def extract_bedrock_model_prices(results, tier="Standard", variant="Global", all_tiers=False, include_multimodal=False):
    """Collapse query_model_pricing() results into a flat price dict ($/M tokens).

    Args (required):
        results (list): Output from query_model_pricing().
    Args (optional):
        tier (str): "Standard", "Batch", "Priority", "Flex". Default "Standard".
        variant (str): "Global" or "Regional". Default "Global".
        all_tiers (bool): Return all tier/variant combos. Default False.

    Example:
        results = query_model_pricing(cache_dir, region_filter="us-west-2", model_filter="Claude Sonnet 4")
        prices = extract_bedrock_model_prices(results)
        # {"input": 3.0, "output": 15.0, "cache_read": 0.3, "cache_write": 3.75}

    Returns: dict keyed by "Tier Variant" → {input, output, cache_read, cache_write}
        (single dict if all_tiers=False, nested dict if all_tiers=True).

    --- Detailed Documentation ---

    Handles differences in pricing format across model families:
    - Most models: prices stored per 1M tokens with "Million Input Tokens" descriptions.
    - Amazon Nova models: prices stored per 1K tokens — auto-detected and converted to per-1M.
    - Models with "Response Tokens" instead of "Output Tokens" (e.g., Haiku 4.5).

    Args:
        results: List of dicts from query_model_pricing().
        tier: Target tier: "Standard", "Priority", "Flex", "Batch". Default: "Standard".
             Prompt Caching prices are always included alongside the selected tier.
        variant: Target variant: "Global" (cross-region) or "Regional" (in-region).
                 Matched against the description field, not the variant field. Default: "Global".
        all_tiers: If True, return nested dict keyed by "{tier} {variant}" with all
                   available tier/variant combos. Default: False (returns flat dict for
                   the specified tier+variant).
        include_multimodal: If True, include image_input, audio_input, video_input keys.
                           Default: False (text tokens only).

    Returns:
        If all_tiers=False: Dict with keys: input, output, cache_read, cache_write
            (and optionally image_input, audio_input, video_input). Values are float
            ($/M tokens) or None if not found.
        If all_tiers=True: Dict of "{Tier} {Variant}" → price dict. Only tiers with
            at least one non-None price are included.
    """

    def _classify_variant(desc):
        """Determine if a description is Global or Regional."""
        d = desc.lower()
        if "cross-region-global" in d or ("global" in d and "regional" not in d):
            return "Global"
        elif "regional" in d or "cris" in d:
            return "Regional"
        # For per-1K Nova format: "USW2-" prefix = Regional, "cross-region" = Global
        if "cross-region" in d:
            return "Global"
        # Default: if no variant indicator, treat as Regional (in-region)
        return "Regional"

    def _classify_dim(desc):
        """Classify a price dimension into a price key."""
        d = desc.lower()
        # Skip reserved throughput and grounding
        if "per hour" in d or "reserved" in d or "grounding" in d:
            return None
        # Cache read
        if ("cache" in d and "read" in d) or "cacheread" in d.replace(" ", ""):
            return "cache_read"
        # Cache write — skip 1-hour TTL (prefer standard 5-min)
        if ("cache" in d and "write" in d) or "cachewrite" in d.replace(" ", ""):
            if "1h" in d or "1 hour" in d or "1-hr" in d:
                return None  # Skip 1-hour TTL
            return "cache_write"
        # Text output (some models use "response" instead of "output")
        # Must come BEFORE multimodal checks — "output-image-token" should be "output", not "image_input"
        if "output" in d or "response" in d:
            return "output"
        # Multimodal inputs (checked AFTER output to avoid misclassifying multimodal outputs)
        if "image" in d and ("input" in d or "image-token" in d):
            return "image_input"
        if "audio" in d and ("input" in d or "audio-token" in d):
            return "audio_input"
        if "video" in d and ("input" in d or "video-token" in d):
            return "video_input"
        # Text input (must come after cache/multimodal checks)
        if "input" in d:
            return "input"
        return None

    def _extract_for_tier_variant(results, target_tier, target_variant, include_mm=False):
        """Extract prices for a specific tier+variant combo."""
        keys = ["input", "output", "cache_read", "cache_write"]
        if include_mm:
            keys += ["image_input", "audio_input", "video_input"]
        prices = {k: None for k in keys}

        # For Standard/Priority/Flex/Batch, also pull in Prompt Caching tier
        tier_match = [target_tier.lower()]
        if target_tier.lower() in ("standard", "priority", "flex"):
            tier_match.append("prompt caching")

        filtered = [r for r in results if r.get("tier", "").lower() in tier_match]

        for r in filtered:
            for dim in r.get("dimensions", []):
                desc = dim.get("description", "")
                price = float(dim.get("price_usd", 0))
                if price == 0:
                    continue

                # Detect per-1K vs per-1M
                desc_lower = desc.lower()
                is_per_1k = ("per 1k" in desc_lower or "/1k" in desc_lower) and "million" not in desc_lower
                multiplier = 1000.0 if is_per_1k else 1.0

                # Check variant match
                dim_variant = _classify_variant(desc)
                if dim_variant.lower() != target_variant.lower():
                    continue

                # Classify the dimension
                key = _classify_dim(desc)
                if key is None:
                    continue
                if key not in prices:
                    continue  # e.g., multimodal key when include_mm=False

                # First match wins — iteration order determines which price is kept
                if prices[key] is None:
                    prices[key] = price * multiplier

        return prices

    if all_tiers:
        # Return nested dict with all available tier × variant combos
        all_results = {}
        tier_names = set(r.get("tier", "") for r in results)

        # Map detected tiers to canonical names
        canonical_tiers = set()
        for t in tier_names:
            if t.lower() in ("standard", "prompt caching"):
                canonical_tiers.add("Standard")
            elif t.lower() == "priority":
                canonical_tiers.add("Priority")
            elif t.lower() == "flex":
                canonical_tiers.add("Flex")
            elif t.lower() == "batch":
                canonical_tiers.add("Batch")
            elif t.lower() == "provisioned":
                canonical_tiers.add("Provisioned")
            elif t.lower() == "custom model":
                canonical_tiers.add("Custom Model")

        for t in sorted(canonical_tiers):
            for v in ["Global", "Regional"]:
                prices = _extract_for_tier_variant(results, t, v, include_multimodal)
                # Only include if at least one price was found
                if any(val is not None for val in prices.values()):
                    label = f"{t} {v}"
                    all_results[label] = prices

        return all_results

    else:
        # Single tier+variant extraction (backward compatible)
        return _extract_for_tier_variant(results, tier, variant, include_multimodal)


# ═══════════════════════════════════════════════════════════════════════════════
# CONVENIENCE LAYER
# ═══════════════════════════════════════════════════════════════════════════════

# Minimum cache token thresholds per model family.
# Used by get_model_prices() to populate min_cache_tokens automatically.
# Models not listed here default to None (no caching applied).
_MODEL_CACHE_THRESHOLDS = {
    "Nova": 1024,
    "Sonnet": 2048,
    "Opus": 2048,
    "Haiku": 4096,
}


def _get_min_cache_tokens(model_name):
    """Look up the minimum cache token threshold for a model name."""
    if not model_name:
        return None
    for pattern, threshold in _MODEL_CACHE_THRESHOLDS.items():
        if pattern in model_name:
            return threshold
    return None


# TODO: list_models() returns alphabetically sorted names. Newest-first would be
# more natural UX but requires version parsing (non-trivial across naming conventions).
def list_models(cache_dir, region, family):
    """List available model versions for a family in a region.

    Requires model family (e.g., "Sonnet", "Opus", "Haiku", "Nova Pro").
    Returns a list of exact model name strings suitable for get_model_prices().

    Args:
        cache_dir (str): Path to cache directory (e.g., "~/bedrock_cache").
        region (str): AWS region code (e.g., "us-west-2").
        family (str): Model family name (e.g., "Sonnet", "Opus", "Haiku").

    Example:
        models = list_models("~/bedrock_cache", "us-west-2", "Sonnet")
        # → ["Claude 3 Sonnet", "Claude 3.5 Sonnet", "Claude 3.5 Sonnet v2",
        #    "Claude 3.7 Sonnet", "Claude Sonnet 4", "Claude Sonnet 4.5", "Claude Sonnet 4.6"]

    Returns: list of model name strings, or empty list if family/region not found.

    Raises:
        FileNotFoundError: If model index does not exist (run --refresh).
    """
    cache_dir = os.path.expanduser(cache_dir)
    index_path = os.path.join(cache_dir, MODEL_INDEX_FILE)

    # Check if index needs (re)generation: missing or stale vs cache files
    needs_regen = False
    if not os.path.exists(index_path):
        needs_regen = True
    else:
        # Regenerate if any cache file is newer than the index
        index_mtime = os.path.getmtime(index_path)
        for f in CACHE_FILES.values():
            if f:
                cache_path = os.path.join(cache_dir, f)
                if os.path.exists(cache_path) and os.path.getmtime(cache_path) > index_mtime:
                    needs_regen = True
                    break

    if needs_regen:
        cache_files_exist = any(
            os.path.exists(os.path.join(cache_dir, f))
            for f in CACHE_FILES.values() if f
        )
        if cache_files_exist:
            _generate_model_index(cache_dir)
        else:
            raise FileNotFoundError(
                f"No pricing cache found at {cache_dir}. "
                f"Run --refresh to fetch pricing data."
            )

    with open(index_path, "r") as f:
        index = json.load(f)

    region_data = index.get(region, {})
    if not region_data:
        return []

    # Exact family match first
    if family in region_data:
        return region_data[family]

    # Case-insensitive fallback
    family_lower = family.lower()
    for fam_key, models in region_data.items():
        if fam_key.lower() == family_lower:
            return models

    return []


def get_model_prices(cache_dir, region, model_name, tier="Standard", variant="Global"):
    """One-call price lookup: model name → ready-to-use price dict.

    Returns keys matching main_agent_config and sub-agent model_prices contracts
    directly — no manual key renaming needed.

    Args (required):
        cache_dir (str): Path to cache directory (e.g., "~/bedrock_cache").
        region (str): AWS region code (e.g., "us-west-2").
        model_name (str): Exact or fuzzy model name (e.g., "Claude Sonnet 4").
    Args (optional):
        tier (str): "Standard", "Batch", "Priority", "Flex". Default "Standard".
        variant (str): "Global" or "Regional". Default "Global".

    Example:
        prices = get_model_prices("~/bedrock_cache", "us-west-2", "Claude Sonnet 4")
        # → {"input_price": 3.0, "output_price": 15.0, "cache_read_price": 0.3,
        #    "cache_write_price": 3.75, "min_cache_tokens": 2048}

    Returns: dict with input_price, output_price, cache_read_price (or None),
        cache_write_price (or None), min_cache_tokens (or None).

    Raises:
        ValueError: If no model found, or multiple models match (ambiguous).
    """
    cache_dir = os.path.expanduser(cache_dir)
    results = query_model_pricing(cache_dir, region, model_filter=model_name)

    if not results:
        raise ValueError(
            f"No pricing found for '{model_name}' in {region}. "
            f"Check spelling or run --refresh to update the cache."
        )

    models_found = sorted(set(r["model"] for r in results))
    if len(models_found) > 1:
        # Prefer exact match if one exists (e.g., "Claude Sonnet 4" shouldn't match "Claude Sonnet 4.5")
        exact = [m for m in models_found if m.lower() == model_name.lower()]
        if len(exact) == 1:
            models_found = exact
            results = [r for r in results if r["model"] == exact[0]]
        else:
            raise ValueError(
                f"Ambiguous: '{model_name}' matched {len(models_found)} models: {models_found}. "
                f"Use a more specific name (e.g., '{models_found[0]}')."
            )

    prices = extract_bedrock_model_prices(results, tier=tier, variant=variant)

    # Fall back to alternate variant if preferred variant not available
    if prices.get("input") is None:
        alt_variant = "Regional" if variant == "Global" else "Global"
        prices = extract_bedrock_model_prices(results, tier=tier, variant=alt_variant)

    if prices.get("input") is None:
        raise ValueError(
            f"No {tier} prices for '{models_found[0]}' in {region}. "
            f"Try extract_bedrock_model_prices(results, all_tiers=True) to see available tiers."
        )

    resolved_name = models_found[0]
    min_cache = _get_min_cache_tokens(resolved_name)

    cache_read = prices.get("cache_read")
    cache_write = prices.get("cache_write")

    # If cache_read exists but cache_write is None, the model has free writes (e.g., Nova)
    if cache_read is not None and cache_write is None:
        cache_write = 0.0

    return {
        "input_price": prices["input"],
        "output_price": prices["output"],
        "cache_read_price": cache_read,
        "cache_write_price": cache_write,
        "min_cache_tokens": min_cache,
        "model_name": resolved_name,
    }


def estimate_cost(cache_dir, region, model_name, sessions_per_month, **overrides):
    """End-to-end: model name → monthly cost estimate in one call.

    Combines get_model_prices() + calculate_agent_session_compounded_cost().

    Args (required):
        cache_dir (str): Path to cache directory (e.g., "~/bedrock_cache").
        region (str): AWS region code (e.g., "us-west-2").
        model_name (str): Model name (e.g., "Claude Sonnet 4").
        sessions_per_month (int): Monthly session volume.

    Keyword overrides:
        output_dir (str): Session directory for the report file.
        output_path (str): Explicit report file path (overrides output_dir).
        Any other keyword: passed as a main_agent_config key
            (e.g., questions_per_agent_session=3, system_prompt_tokens=4000).

    Example:
        cost = estimate_cost("~/bedrock_cache", "us-west-2", "Claude Sonnet 4", 10000)

        # With session directory:
        cost = estimate_cost("~/bedrock_cache", "us-west-2", "Claude Sonnet 4", 10000,
                             output_dir=session_dir)

        # With config overrides:
        cost = estimate_cost("~/bedrock_cache", "us-west-2", "Claude Sonnet 4", 10000,
                             questions_per_agent_session=3, system_prompt_tokens=4000,
                             output_dir=session_dir)

    Returns: Same as calculate_agent_session_compounded_cost() — dict with
        session_total, monthly_total, annual_total, savings_pct, file_path, etc.
    """
    # Separate report output params from main_agent_config overrides
    output_dir = overrides.pop("output_dir", None)
    output_path = overrides.pop("output_path", None)

    # Guard: estimate_cost is single-agent only
    if "subagents" in overrides:
        raise ValueError(
            "estimate_cost() does not support sub-agents. "
            "Use calculate_agent_session_compounded_cost() for multi-agent workloads."
        )

    prices = get_model_prices(cache_dir, region, model_name)

    main_agent_config = {
        "input_price": prices["input_price"],
        "output_price": prices["output_price"],
        "cache_read_price": prices["cache_read_price"],
        "cache_write_price": prices["cache_write_price"],
        "agent_sessions_per_month": sessions_per_month,
        "model_name": prices["model_name"],
    }
    main_agent_config.update(overrides)

    return calculate_agent_session_compounded_cost(
        main_agent_config=main_agent_config,
        output_dir=output_dir,
        output_path=output_path,
    )


def _extract_agentcore_component(usagetype):
    """Extract the top-level component name from an AgentCore usagetype string.

    Pattern: '<REGION_PREFIX>-<Component>:Consumption-based:<SubType>'
    Example: 'USW2-Runtime:Consumption-based:vCPU' → 'Runtime'
    """
    if "-" not in usagetype:
        return None
    after_region = usagetype.split("-", 1)[1]
    if ":" not in after_region:
        return None
    return after_region.split(":")[0]


def list_agentcore_components(cache_dir, region):
    """List available AgentCore components in a region.

    Returns the top-level component names (e.g., Runtime, Gateway, Memory)
    that have pricing entries in the cache for the given region.

    Args:
        cache_dir (str): Path to cache directory (e.g., "~/bedrock_cache").
        region (str): AWS region code (e.g., "us-west-2").

    Example:
        components = list_agentcore_components("~/bedrock_cache", "us-west-2")
        # → ["BrowserTool", "CodeInterpreter", "Evaluations", "Gateway", "Memory", "Runtime"]

    Returns: sorted list of component name strings, or empty list if region not found.

    Raises:
        FileNotFoundError: If AgentCore cache file does not exist.
    """
    cache_dir = os.path.expanduser(cache_dir)
    filepath = os.path.join(cache_dir, CACHE_FILES.get(AGENTCORE_SERVICE_CODE, ""))
    if not os.path.exists(filepath):
        raise FileNotFoundError(
            f"AgentCore pricing cache not found at {filepath}. "
            f"Run --refresh to fetch pricing data."
        )

    products = _load_cache_file(cache_dir, AGENTCORE_SERVICE_CODE)
    components = set()
    for prod in products:
        attrs = prod.get("product", {}).get("attributes", {})
        if attrs.get("regionCode", "") != region:
            continue
        component = _extract_agentcore_component(attrs.get("usagetype", ""))
        if component:
            components.add(component)

    return sorted(components)


def query_agentcore_pricing(cache_dir, region_filter, components=None):
    """Query AgentCore pricing from the local cache and return structured price entries.

    Args (required):
        cache_dir (str): Path to cache directory containing AgentCore pricing JSON.
        region_filter (str): AWS region code (e.g., "us-east-1").
    Args (optional):
        components (list): Filter to specific top-level components
            (e.g., ["Runtime", "Gateway", "Memory"]). If None, returns all.

    Example:
        # All components:
        query_agentcore_pricing("~/bedrock_cache", "us-east-1")

        # Only defaults:
        query_agentcore_pricing("~/bedrock_cache", "us-east-1",
                                components=["Runtime", "Gateway", "Memory"])

    Returns: list of dicts with keys component, sub_component, region, dimensions.
        Returns empty list if no entries match (invalid region or component names).
    """
    cache_dir = os.path.expanduser(cache_dir)

    # Validate components parameter
    if components is not None:
        if not isinstance(components, (list, tuple)):
            raise TypeError(
                f"components must be a list or None, got {type(components).__name__}"
            )
        if len(components) == 0:
            return []
        components_lower = {c.lower() for c in components}
    else:
        components_lower = None

    results = []
    products = _load_cache_file(cache_dir, AGENTCORE_SERVICE_CODE)
    for prod in products:
        attrs = prod.get("product", {}).get("attributes", {})
        region_code = attrs.get("regionCode", "")
        if region_code != region_filter:
            continue

        usagetype = attrs.get("usagetype", "")
        top_component = _extract_agentcore_component(usagetype)

        # Filter by components if specified (case-insensitive)
        if components_lower is not None:
            if not top_component or top_component.lower() not in components_lower:
                continue

        service_name = attrs.get("servicename", attrs.get("group", "AgentCore"))
        component = attrs.get("group", attrs.get("usagetype", "Unknown"))
        dimensions = _parse_price_dimensions(prod.get("terms", {}))
        if not dimensions:
            continue
        results.append({
            "component": service_name,
            "sub_component": component,
            "region": region_code,
            "dimensions": dimensions,
        })
    return results


# ─── DEPRECATED: calculate_agent_cost_with_incremental_caching ───────────────
# Replaced by calculate_agent_session_compounded_cost() which handles sub-agents,
# multi-model architectures, and report generation. Kept for reference only.
#
# def calculate_agent_cost_with_incremental_caching(
#     input_price,
#     output_price,
#     cache_read_price,
#     cache_write_price,
#     sessions_per_month,
#     questions_per_session=5,
#     input_tokens=100,
#     output_tokens=100,
#     system_prompt_tokens=1000,
#     tools_passed_to_agent=10,
#     tool_spec_tokens=100,
#     rag_chunks=10,
#     rag_tokens_per_chunk=300,
#     tools_invoked=5,
#     tool_call_tokens=100,
#     tool_result_tokens=100,
# ):
#     """Calculate agent monthly cost with prompt caching vs. without, showing savings.
# 
#     Args (required):
#         input_price (float): Input token price, $/M tokens.
#         output_price (float): Output token price, $/M tokens.
#         cache_read_price (float|None): Cache read price, $/M tokens.
#         cache_write_price (float|None): Cache write price, $/M tokens.
#         sessions_per_month (int): Total sessions per month.
#     Args (optional):
#         questions_per_session (float): Questions per session. Default 5.
#         tools_invoked (int): Tool calls per question. Default 5.
#         rag_chunks (int): RAG chunks per question. Default 10.
# 
#     Example:
#         calculate_agent_cost_with_incremental_caching(3.0, 15.0, 0.3, 3.75, 10000)
# 
#     Returns: dict with with_cache, no_cache, savings_pct, and explanation.
# 
#     --- Detailed Documentation ---
# 
#     Uses incremental caching: each LLM turn's prompt extends the prior turn's.
#     The unchanged prefix is a cache read; only the new delta is new.
# 
#     Args:
#         input_price (float): Input token price, $/M tokens.
#         output_price (float): Output token price, $/M tokens.
#         cache_read_price (float | None): Cache read price, $/M. None → 0.0.
#         cache_write_price (float | None): Cache write price, $/M. None → 0.0.
#         sessions_per_month (int): Total sessions per month.
#         questions_per_session (float): Questions/session (default 5).
#         input_tokens (int): User question tokens (default 100).
#         output_tokens (int): Final turn output tokens (default 100).
#         system_prompt_tokens (int): System prompt tokens (default 1000).
#         tools_passed_to_agent (int): Number of tools in the agent's schema (default 10).
#         tool_spec_tokens (int): Tokens per tool specification (default 100).
#         rag_chunks (int): RAG chunks per question (default 10).
#         rag_tokens_per_chunk (int): Tokens per RAG chunk (default 300).
#         tools_invoked (int): Tool calls per question (default 5).
#         tool_call_tokens (int): Tokens per tool call JSON (default 100).
#         tool_result_tokens (int): Tokens per tool result (default 100).
# 
#     Returns:
#         dict: assumptions, per_question, per_session, monthly_tokens,
#         with_cache (dict with total_monthly, total_annual),
#         no_cache (dict with total_monthly, total_annual),
#         savings_monthly, savings_annual, savings_pct (float),
#         explanation (dict): token_profile, turn_by_turn_q1,
#             cross_question_caching, cache_math, no_cache_baseline,
#             monthly_rollup, prices_used.
#     """
#     # Input validation
#     if sessions_per_month <= 0:
#         raise ValueError(f"sessions_per_month must be > 0, got {sessions_per_month}")
#     if questions_per_session <= 0:
#         raise ValueError(f"questions_per_session must be > 0, got {questions_per_session}")
#     if tools_invoked > tools_passed_to_agent:
#         raise ValueError(f"tools_invoked ({tools_invoked}) cannot exceed tools_passed_to_agent ({tools_passed_to_agent})")
# 
#     # Guard against None cache prices (models without caching support)
#     # When caching is not supported, price all tokens at input_price so
#     # with_cache total equals no_cache total (0% savings).
#     cache_read_price = cache_read_price if cache_read_price is not None else input_price
#     cache_write_price = cache_write_price if cache_write_price is not None else input_price
#     tool_desc_tokens = tools_passed_to_agent * tool_spec_tokens
#     N = tools_invoked
#     turns_per_question = N + 1
#     rag_tokens = rag_chunks * rag_tokens_per_chunk
#     delta = tool_call_tokens + tool_result_tokens
#     questions_per_month = sessions_per_month * questions_per_session
# 
#     if questions_per_month == 0 or sessions_per_month == 0:
#         return {
#             "assumptions": {"sessions_per_month": sessions_per_month, "questions_per_session": questions_per_session},
#             "per_question": {}, "per_session": {}, "monthly_tokens": {},
#             "with_cache": {"total_monthly": 0, "total_annual": 0},
#             "no_cache": {"total_monthly": 0, "total_annual": 0},
#             "savings_monthly": 0, "savings_annual": 0, "savings_pct": 0,
#             "explanation": {"note": "Zero sessions or questions - no cost."},
#         }
# 
#     # Base prompt on turn 0 of any question
#     cacheable_base = system_prompt_tokens + tool_desc_tokens
#     base_prompt = cacheable_base + input_tokens + rag_tokens
# 
#     # --- First question in session ---
#     # Turn 0: all new → cache write (will be re-read on turn 1)
#     # Turns 1..N-1: prefix = cache read, delta = cache write (will be re-read)
#     # Turn N (last): prefix = cache read, delta = regular input (not re-read)
#     q1_cache_write = base_prompt + (N - 1) * delta
#     q1_cache_read = 0
#     for k in range(1, turns_per_question):
#         q1_cache_read += base_prompt + (k - 1) * delta
#     q1_regular = delta  # last turn only
# 
#     # --- Subsequent questions (2nd..last) in session ---
#     # Turn 0: system+tools = cache read (from prior Q), user+RAG = cache write (new)
#     # Turns 1..N-1: full prefix = cache read, delta = cache write
#     # Turn N (last): full prefix = cache read, delta = regular input
#     q2_cache_write = (input_tokens + rag_tokens) + (N - 1) * delta
#     q2_cache_read = cacheable_base  # turn 0: system+tools cached
#     for k in range(1, turns_per_question):
#         q2_cache_read += base_prompt + (k - 1) * delta
#     q2_regular = delta
# 
#     # --- Per session ---
#     n_subsequent = questions_per_session - 1
#     session_cw = q1_cache_write + n_subsequent * q2_cache_write
#     session_cr = q1_cache_read + n_subsequent * q2_cache_read
#     session_reg = q1_regular + n_subsequent * q2_regular
#     output_per_question = output_tokens + tools_invoked * tool_call_tokens
#     session_out = questions_per_session * output_per_question
# 
#     # --- Monthly costs ---
#     monthly_cw = sessions_per_month * (session_cw / 1e6) * cache_write_price
#     monthly_cr = sessions_per_month * (session_cr / 1e6) * cache_read_price
#     monthly_reg = sessions_per_month * (session_reg / 1e6) * input_price
#     monthly_out = sessions_per_month * (session_out / 1e6) * output_price
#     total_cached = monthly_cw + monthly_cr + monthly_reg + monthly_out
# 
#     # --- No-cache baseline ---
#     # Every turn sends full prompt at regular input price
#     total_input_per_question = 0
#     for t in range(turns_per_question):
#         total_input_per_question += base_prompt + t * delta
#     total_no_cache_input = questions_per_month * (total_input_per_question / 1e6) * input_price
#     total_no_cache_output = questions_per_month * (output_per_question / 1e6) * output_price
#     total_no_cache = total_no_cache_input + total_no_cache_output
# 
#     savings = total_no_cache - total_cached
#     savings_pct = (savings / total_no_cache) * 100 if total_no_cache > 0 else 0
# 
#     # ── Build step-by-step explanation ──
#     # Section 1: Token profile
#     token_profile = {
#         "base_context": f"{_fmt(base_prompt)} = {_fmt(system_prompt_tokens)} (system) + {_fmt(tool_desc_tokens)} (tools) + {_fmt(input_tokens)} (user) + {_fmt(rag_tokens)} (RAG)",
#         "cacheable_prefix": f"{_fmt(cacheable_base)} = {_fmt(system_prompt_tokens)} (system) + {_fmt(tool_desc_tokens)} (tools)",
#         "delta_per_turn": f"{_fmt(delta)} = {_fmt(tool_call_tokens)} (tool call) + {_fmt(tool_result_tokens)} (tool result)",
#         "turns_per_question": f"{turns_per_question} = {N} tool invocations + 1",
#         "output_per_question": f"{_fmt(output_per_question)} = {_fmt(output_tokens)} (response) + {N} × {_fmt(tool_call_tokens)} (tool calls)",
#     }
# 
#     # Section 2: Turn-by-turn breakdown for Q1
#     turn_details = []
#     for t in range(turns_per_question):
#         tokens_in = base_prompt + t * delta
#         if t == 0:
#             cache_action = f"WRITE {_fmt(tokens_in)} (entire prompt — first turn of session)"
#         elif t < turns_per_question - 1:
#             prefix = base_prompt + (t - 1) * delta
#             cache_action = f"READ {_fmt(prefix)} (cached prefix) + WRITE {_fmt(delta)} (new tool delta)"
#         else:
#             prefix = base_prompt + (t - 1) * delta
#             cache_action = f"READ {_fmt(prefix)} (cached prefix) + REG {_fmt(delta)} (last turn — won't be re-read)"
#         turn_details.append(f"Turn {t}: {_fmt(tokens_in)} input tokens → {cache_action}")
#     total_input_q1 = sum(base_prompt + t * delta for t in range(turns_per_question))
#     turn_details.append(f"Total Q1 input: {_fmt(total_input_q1)} tokens across {turns_per_question} turns")
# 
#     # Section 3: Cross-question caching
#     cross_question = {
#         "q2_turn0": f"READ {_fmt(cacheable_base)} (system+tools still cached from Q1) + WRITE {_fmt(input_tokens + rag_tokens)} (new user question + RAG)",
#         "savings": f"Cross-Q caching saves re-writing {_fmt(cacheable_base)} tokens at ${cache_write_price}/M on each subsequent question",
#     }
# 
#     # Section 4: Cache math (monthly)
#     cache_math = {
#         "cache_write": f"{_fmt(sessions_per_month * session_cw)} tokens × ${cache_write_price}/M = ${monthly_cw:,.2f}",
#         "cache_read": f"{_fmt(sessions_per_month * session_cr)} tokens × ${cache_read_price}/M = ${monthly_cr:,.2f}",
#         "regular_input": f"{_fmt(sessions_per_month * session_reg)} tokens × ${input_price}/M = ${monthly_reg:,.2f}",
#         "output": f"{_fmt(sessions_per_month * session_out)} tokens × ${output_price}/M = ${monthly_out:,.2f}",
#         "total_cached": f"${monthly_cw:,.2f} + ${monthly_cr:,.2f} + ${monthly_reg:,.2f} + ${monthly_out:,.2f} = ${total_cached:,.2f}",
#     }
# 
#     # Section 5: No-cache baseline
#     no_cache_math = {
#         "total_input_per_q": f"{_fmt(total_input_per_question)} tokens/question × {_fmt(questions_per_month)} questions × ${input_price}/M = ${total_no_cache_input:,.2f}",
#         "total_output": f"{_fmt(output_per_question)} tokens/question × {_fmt(questions_per_month)} questions × ${output_price}/M = ${total_no_cache_output:,.2f}",
#         "total_no_cache": f"${total_no_cache_input:,.2f} + ${total_no_cache_output:,.2f} = ${total_no_cache:,.2f}",
#     }
# 
#     # Section 6: Monthly rollup
#     monthly_rollup = {
#         "questions_per_month": _fmt(questions_per_month),
#         "with_caching": f"${total_cached:,.2f}/mo (${total_cached / sessions_per_month:.4f}/session, ${total_cached / questions_per_month:.4f}/question)",
#         "without_caching": f"${total_no_cache:,.2f}/mo (${total_no_cache / sessions_per_month:.4f}/session)",
#         "savings": f"${savings:,.2f}/mo ({savings_pct:.1f}%)",
#     }
# 
#     # Section 7: Prices used
#     prices_used = {
#         "input": f"${input_price}/M tokens",
#         "output": f"${output_price}/M tokens",
#         "cache_read": f"${cache_read_price}/M tokens ({cache_read_price/input_price*100:.0f}% of input)" if input_price > 0 else f"${cache_read_price}/M tokens",
#         "cache_write": f"${cache_write_price}/M tokens ({cache_write_price/input_price*100:.0f}% of input)" if input_price > 0 else f"${cache_write_price}/M tokens",
#     }
# 
#     explanation = {
#         "token_profile": token_profile,
#         "turn_by_turn_q1": turn_details,
#         "cross_question_caching": cross_question,
#         "cache_math": cache_math,
#         "no_cache_baseline": no_cache_math,
#         "monthly_rollup": monthly_rollup,
#         "prices_used": prices_used,
#     }
# 
#     return {
#         "assumptions": {
#             "sessions_per_month": sessions_per_month,
#             "questions_per_session": questions_per_session,
#             "questions_per_month": questions_per_month,
#             "tools_invoked": N,
#             "turns_per_question": turns_per_question,
#             "system_prompt_tokens": system_prompt_tokens,
#             "tools_passed_to_agent": tools_passed_to_agent,
#             "tool_spec_tokens": tool_spec_tokens,
#             "tool_desc_tokens": tool_desc_tokens,
#             "input_tokens": input_tokens,
#             "output_tokens": output_tokens,
#             "rag_tokens": rag_tokens,
#             "delta_per_tool_turn": delta,
#             "base_prompt": base_prompt,
#             "cacheable_base": cacheable_base,
#         },
#         "per_question": {
#             "q1_cache_write": q1_cache_write,
#             "q1_cache_read": q1_cache_read,
#             "q1_regular": q1_regular,
#             "q2_cache_write": q2_cache_write,
#             "q2_cache_read": q2_cache_read,
#             "q2_regular": q2_regular,
#         },
#         "per_session": {
#             "cache_write": session_cw,
#             "cache_read": session_cr,
#             "regular_input": session_reg,
#             "output": session_out,
#         },
#         "monthly_tokens": {
#             "cache_write": sessions_per_month * session_cw,
#             "cache_read": sessions_per_month * session_cr,
#             "regular_input": sessions_per_month * session_reg,
#             "output": sessions_per_month * session_out,
#         },
#         "with_cache": {
#             "cache_write_cost": monthly_cw,
#             "cache_read_cost": monthly_cr,
#             "regular_input_cost": monthly_reg,
#             "output_cost": monthly_out,
#             "total_monthly": total_cached,
#             "total_annual": total_cached * 12,
#         },
#         "no_cache": {
#             "input_cost": total_no_cache_input,
#             "output_cost": total_no_cache_output,
#             "total_monthly": total_no_cache,
#             "total_annual": total_no_cache * 12,
#         },
#         "savings_monthly": savings,
#         "savings_annual": savings * 12,
#         "savings_pct": savings_pct,
#         "explanation": explanation,
#     }
# 
# ─── END DEPRECATED ──────────────────────────────────────────────────────────


def calculate_compounded_tokens_for_agent(
    questions_per_agent_session=5,
    input_tokens=100,
    output_tokens=150,
    system_prompt_tokens=2000,
    tools_passed_to_agent=10,
    tool_spec_tokens=100,
    tools_invoked=5,
    tool_call_tokens=100,
    tool_result_tokens=100,
    history_mode="full",  # "full" (b) or "condensed" (a)
    detail_level="summary",  # "summary" or "full"
):
    """Calculate compounded input/output tokens across a multi-question agent session.

    Args (optional):
        questions_per_agent_session (int): Questions in the session. Default 5.
        tools_invoked (int): Tool calls per question. Default 5.
        history_mode (str): "full" or "condensed". Default "full".

    Example:
        calculate_compounded_tokens_for_agent(questions_per_agent_session=5, tools_invoked=5)

    Returns: dict with session_total_input, session_total_output, session_total_tokens.

    --- Detailed Documentation ---

    Models two levels of token compounding:
    1. Within a question (span-level): each tool call cycle re-sends the full context
       accumulated so far within that question.
    2. Across questions (turn-level): each new question carries the full conversation
       history from all prior questions in the session.

    The model is stateless — every model call receives system_prompt + tool_specs
    (as separate parameters) plus the full messages list (conversation history).

    Args:
        questions_per_agent_session (int): Number of questions in the session (default 5).
        input_tokens (int): User question tokens per question (default 100).
        output_tokens (int): Final answer tokens per question (default 150).
        system_prompt_tokens (int): System prompt tokens, sent every call (default 2000).
        tools_passed_to_agent (int): Number of tools in schema (default 10).
        tool_spec_tokens (int): Tokens per tool specification (default 100).
        tools_invoked (int): Tool calls per question (default 5).
        tool_call_tokens (int): Model output tokens per tool call JSON (default 100).
        tool_result_tokens (int|list): Tool response tokens per tool result (default 100).
            If int: uniform size for all tool results.
            If list: per-tool result sizes. Length must equal tools_invoked.
            This allows modeling sub-agents (which return larger results) alongside
            regular tools. Sub-agent results should be placed first in the list
            (e.g., [300, 1000, 100, 100, 100] for RAG sub-agent + research sub-agent + 3 regular tools).
        history_mode (str): "full" = carry all tool calls/results in history (default).
            "condensed" = carry only user_input + final_answer per prior question.

    Returns:
        dict: session (list of per-question dicts with per-cycle details),
            session_total_input, session_total_output,
            assumptions, explanation.
    """
    # NOTE: This function does NOT model the SlidingWindowConversationManager
    # (default window_size=20 messages in Strands). It assumes NullConversationManager
    # (no trimming) for worst-case estimation. We may need to revisit this later
    # to add an option for sliding window trimming.

    # Input validation
    if questions_per_agent_session <= 0:
        raise ValueError(f"questions_per_agent_session must be > 0, got {questions_per_agent_session}")
    if tools_invoked > tools_passed_to_agent:
        raise ValueError(f"tools_invoked ({tools_invoked}) cannot exceed tools_passed_to_agent ({tools_passed_to_agent})")

    # Normalize tool_result_tokens to a list
    if isinstance(tool_result_tokens, (int, float)):
        tool_result_tokens_list = [int(tool_result_tokens)] * tools_invoked
    elif isinstance(tool_result_tokens, list):
        if len(tool_result_tokens) != tools_invoked:
            raise ValueError(
                f"tool_result_tokens list length ({len(tool_result_tokens)}) must equal "
                f"tools_invoked ({tools_invoked})"
            )
        tool_result_tokens_list = tool_result_tokens
    else:
        raise ValueError(f"tool_result_tokens must be int or list, got {type(tool_result_tokens)}")

    # Derived constants
    tool_desc_tokens = tools_passed_to_agent * tool_spec_tokens
    fixed_per_call = system_prompt_tokens + tool_desc_tokens  # sent every model call
    cycles_per_question = tools_invoked + 1  # N tool calls + 1 final answer

    # Per-tool delta (tool_call output + that tool's result)
    # delta_per_tool[i] = tokens added to context after tool i completes
    delta_per_tool = [tool_call_tokens + tool_result_tokens_list[i] for i in range(tools_invoked)]
    total_delta = sum(delta_per_tool)  # total tokens added by all tool exchanges in a question

    # For explanation, compute average delta
    avg_delta = total_delta / tools_invoked if tools_invoked > 0 else 0

    # History added per question depends on mode
    if history_mode == "full":
        # Full trace: user_input + all tool exchanges + final_answer
        history_per_question = input_tokens + total_delta + output_tokens
    elif history_mode == "condensed":
        # Condensed: only user_input + final_answer
        history_per_question = input_tokens + output_tokens
    else:
        raise ValueError(f"history_mode must be 'full' or 'condensed', got '{history_mode}'")

    # Build session cycle-by-cycle
    session = []
    accumulated_history = 0  # tokens from all prior questions

    session_total_input = 0
    session_total_output = 0

    for q in range(1, questions_per_agent_session + 1):
        # Base input for cycle 1 of this question
        question_base = fixed_per_call + accumulated_history + input_tokens

        cycles = []
        accumulated_within_question = 0  # tool exchanges within this question

        for c in range(1, cycles_per_question + 1):
            # Input = base + tool exchanges accumulated within this question so far
            cycle_input = question_base + accumulated_within_question

            # Output: tool_call for intermediate cycles, final answer for last cycle
            if c < cycles_per_question:
                cycle_output = tool_call_tokens
                cycle_type = "tool_use"
            else:
                cycle_output = output_tokens
                cycle_type = "end_turn"

            cycle_data = {
                "cycle": c,
                "input_tokens": cycle_input,
                "output_tokens": cycle_output,
                "type": cycle_type,
            }

            # Add breakdown only for the very first cycle of Q1
            if q == 1 and c == 1:
                cycle_data["breakdown"] = (
                    f"system_prompt({_fmt(system_prompt_tokens)}) + "
                    f"tool_specs({_fmt(tool_desc_tokens)}) + "
                    f"user_input({_fmt(input_tokens)}) = "
                    f"{_fmt(cycle_input)}"
                )

            cycles.append(cycle_data)

            # Accumulate within-question context (only for tool cycles, not final answer)
            if c < cycles_per_question:
                accumulated_within_question += delta_per_tool[c - 1]

        question_total_input = sum(cyc["input_tokens"] for cyc in cycles)
        question_total_output = sum(cyc["output_tokens"] for cyc in cycles)

        session.append({
            "question": q,
            "cycles": cycles,
            "question_total_input": question_total_input,
            "question_total_output": question_total_output,
        })

        session_total_input += question_total_input
        session_total_output += question_total_output

        # Accumulate history for next question
        accumulated_history += history_per_question

    # ── Build explanation ──
    # Determine if uniform or per-tool result sizes
    is_uniform = len(set(tool_result_tokens_list)) <= 1 if tool_result_tokens_list else True
    if is_uniform:
        delta_explanation = f"tool_call({_fmt(tool_call_tokens)}) + tool_result({_fmt(tool_result_tokens_list[0] if tool_result_tokens_list else 0)}) = {_fmt(delta_per_tool[0] if delta_per_tool else 0)}"
    else:
        delta_parts = [f"tool_{i+1}: {_fmt(tool_call_tokens)}+{_fmt(tool_result_tokens_list[i])}={_fmt(delta_per_tool[i])}" for i in range(tools_invoked)]
        delta_explanation = f"Per-tool deltas: [{', '.join(delta_parts)}], total={_fmt(total_delta)}"

    if is_uniform:
        history_detail = (
            f"user({_fmt(input_tokens)}) + {tools_invoked} x delta({_fmt(delta_per_tool[0] if delta_per_tool else 0)}) + answer({_fmt(output_tokens)})"
            if history_mode == "full"
            else f"user({_fmt(input_tokens)}) + answer({_fmt(output_tokens)})"
        )
    else:
        history_detail = (
            f"user({_fmt(input_tokens)}) + total_delta({_fmt(total_delta)}) + answer({_fmt(output_tokens)})"
            if history_mode == "full"
            else f"user({_fmt(input_tokens)}) + answer({_fmt(output_tokens)})"
        )

    explanation = {
        "derived_constants": {
            "tool_desc_tokens": f"{tools_passed_to_agent} tools x {tool_spec_tokens} tokens = {_fmt(tool_desc_tokens)}",
            "fixed_per_call": f"system_prompt({_fmt(system_prompt_tokens)}) + tool_specs({_fmt(tool_desc_tokens)}) = {_fmt(fixed_per_call)}",
            "delta_per_tool_exchange": delta_explanation,
            "cycles_per_question": f"{tools_invoked} tool calls + 1 final = {cycles_per_question}",
            "history_per_question": f"{_fmt(history_per_question)} tokens ({history_mode} mode: {history_detail})",
        },
        "per_question_summary": [],
        "session_summary": {
            "total_input_tokens": _fmt(session_total_input),
            "total_output_tokens": _fmt(session_total_output),
            "total_tokens": _fmt(session_total_input + session_total_output),
            "total_model_calls": _fmt(questions_per_agent_session * cycles_per_question),
        },
    }

    # Per-question explanation
    running_history = 0
    for q in range(1, questions_per_agent_session + 1):
        q_base = fixed_per_call + running_history + input_tokens
        q_data = session[q - 1]
        history_after = running_history + history_per_question
        # Build history breakdown showing how accumulated history was calculated
        if q == 1:
            history_breakdown = f"0 (first question, no prior history)"
            history_after_breakdown = (
                f"Q1 contributes: user({_fmt(input_tokens)}) + "
                f"tool_exchanges({_fmt(total_delta)}) + "
                f"answer({_fmt(output_tokens)}) = {_fmt(history_per_question)}"
                if history_mode == "full"
                else f"Q1 contributes: user({_fmt(input_tokens)}) + answer({_fmt(output_tokens)}) = {_fmt(history_per_question)}"
            )
        else:
            history_breakdown = f"{_fmt(running_history)} = {q - 1} prior questions × {_fmt(history_per_question)} each"
            history_after_breakdown = f"{_fmt(running_history)} (prior) + {_fmt(history_per_question)} (Q{q}) = {_fmt(history_after)}"

        q_explanation = {
            "question": q,
            "accumulated_history": history_breakdown,
            "base_for_cycle_1": f"fixed({_fmt(fixed_per_call)}) + history({_fmt(running_history)}) + user_input({_fmt(input_tokens)}) = {_fmt(q_base)}",
            "compounding": f"Cycle 1: {_fmt(q_base)}, Cycle {cycles_per_question}: {_fmt(q_base + total_delta - (delta_per_tool[-1] if delta_per_tool else 0) + (delta_per_tool[-1] if delta_per_tool else 0))}",
            "question_total_input": _fmt(q_data["question_total_input"]),
            "question_total_output": _fmt(q_data["question_total_output"]),
            "history_after_this_question": _fmt(history_after),
            "history_after_breakdown": history_after_breakdown,
        }
        explanation["per_question_summary"].append(q_explanation)
        running_history += history_per_question

    result = {
        "assumptions": {
            "questions_per_agent_session": questions_per_agent_session,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "system_prompt_tokens": system_prompt_tokens,
            "tools_passed_to_agent": tools_passed_to_agent,
            "tool_spec_tokens": tool_spec_tokens,
            "tools_invoked": tools_invoked,
            "tool_call_tokens": tool_call_tokens,
            "tool_result_tokens": tool_result_tokens_list,
            "history_mode": history_mode,
            "tool_desc_tokens": tool_desc_tokens,
            "fixed_per_call": fixed_per_call,
            "delta_per_tool": delta_per_tool,
            "total_delta": total_delta,
            "cycles_per_question": cycles_per_question,
            "history_per_question": history_per_question,
        },
        "session": session,
        "session_total_input": session_total_input,
        "session_total_output": session_total_output,
        "session_total_tokens": session_total_input + session_total_output,
        "explanation": explanation,
    }

    if detail_level == "summary":
        return {
            "assumptions": result["assumptions"],
            "session_total_input": result["session_total_input"],
            "session_total_output": result["session_total_output"],
            "session_total_tokens": result["session_total_tokens"],
        }
    return result


def calculate_rag_subagent_tokens(
    system_prompt_tokens=None,
    n_tools=None,
    tool_spec_tokens=None,
    input_query_tokens=None,
    tool_call_tokens=None,
    rag_n_retrieval_calls=None,
    rag_n_chunks=None,
    rag_chunk_size=None,
    n_other_tool_calls=None,
    other_tool_result_tokens=None,
    output_tokens=None,
    detail_level="summary",  # "summary" or "full"
):
    """Calculate total token usage for a single RAG sub-agent invocation.

    Args (optional):
        rag_n_retrieval_calls (int): Number of KB retrieval calls. Default 2.
        rag_n_chunks (int): Chunks returned per retrieval. Default 10.
        rag_chunk_size (int): Tokens per chunk. Default 300.

    Example:
        calculate_rag_subagent_tokens(rag_n_retrieval_calls=2, rag_n_chunks=10)

    Returns: dict with total_input, total_output, total_tokens, output_tokens_to_main_agent.

    --- Detailed Documentation ---

    Models a RAG sub-agent that:
    1. Receives a query from the main agent (input_query_tokens)
    2. Calls KB retrieval tool one or more times (each returning rag_n_chunks × rag_chunk_size tokens)
    3. Optionally calls other tools (reranker, etc.)
    4. Produces a synthesized response (output_tokens) sent back to the main agent

    The sub-agent is stateless — every model call sends system_prompt + tool_specs + full
    accumulated history. Token compounding applies within this invocation.

    Tool call order: all retrieval calls first, then all other tool calls, then final answer.
    # NOTE: Interleaved tool call order (retrieve → rerank → retrieve → rerank) could be
    # added later if needed. For now, sequential grouping is assumed.

    # NOTE: Prompt caching is modeled via _calculate_subagent_cost() when cache prices
    # are provided. The cacheable prefix (system_prompt + tool_specs + query) is
    # identical across all LLM cycles within a single invocation. Cycle 1 pays
    # cache_write, cycles 2+ pay cache_read. Caching is only applied when the prefix
    # meets the model's minimum threshold (e.g., 1024 for Nova, 2048 for Sonnet).

    Args:
        system_prompt_tokens (int): Sub-agent system prompt (default 500).
        n_tools (int): Number of tools available to sub-agent (default 2).
        tool_spec_tokens (int): Tokens per tool specification (default 100).
        input_query_tokens (int): Query from main agent — the input prompt to this
            sub-agent (default 100).
        tool_call_tokens (int): Model output per tool invocation JSON (default 50).
        rag_n_retrieval_calls (int): Number of KB retrieval calls (default 2).
        rag_n_chunks (int): Number of chunks returned per retrieval call (default 10).
        rag_chunk_size (int): Tokens per RAG chunk (default 300).
        n_other_tool_calls (int): Other tool invocations — reranker, etc. (default 1).
        other_tool_result_tokens (int): Result size for non-retrieval tools (default 200).
        output_tokens (int): Final synthesized response tokens (default 300).
            This is what the main agent receives as tool_result.

    Returns:
        dict: cycles (list of per-model-call details), total_input, total_output,
            output_tokens_to_main_agent, assumptions, explanation.
    """
    # Resolve defaults from config (explicit values passed by caller always win)
    system_prompt_tokens = _resolve_setting("rag_defaults", "system_prompt_tokens", system_prompt_tokens)
    n_tools = _resolve_setting("rag_defaults", "n_tools", n_tools)
    tool_spec_tokens = _resolve_setting("rag_defaults", "tool_spec_tokens", tool_spec_tokens)
    input_query_tokens = _resolve_setting("rag_defaults", "input_query_tokens", input_query_tokens)
    tool_call_tokens = _resolve_setting("rag_defaults", "tool_call_tokens", tool_call_tokens)
    rag_n_retrieval_calls = _resolve_setting("rag_defaults", "rag_n_retrieval_calls", rag_n_retrieval_calls)
    rag_n_chunks = _resolve_setting("rag_defaults", "rag_n_chunks", rag_n_chunks)
    rag_chunk_size = _resolve_setting("rag_defaults", "rag_chunk_size", rag_chunk_size)
    n_other_tool_calls = _resolve_setting("rag_defaults", "n_other_tool_calls", n_other_tool_calls)
    other_tool_result_tokens = _resolve_setting("rag_defaults", "other_tool_result_tokens", other_tool_result_tokens)
    output_tokens = _resolve_setting("rag_defaults", "output_tokens", output_tokens)

    # Input validation
    if rag_n_retrieval_calls < 0:
        raise ValueError(f"rag_n_retrieval_calls must be >= 0, got {rag_n_retrieval_calls}")
    if n_other_tool_calls < 0:
        raise ValueError(f"n_other_tool_calls must be >= 0, got {n_other_tool_calls}")
    if rag_n_retrieval_calls == 0 and n_other_tool_calls == 0 and output_tokens == 0:
        raise ValueError("At least one of rag_n_retrieval_calls, n_other_tool_calls, or output_tokens must be > 0")

    # Derived constants
    tool_desc_tokens = n_tools * tool_spec_tokens
    fixed_per_call = system_prompt_tokens + tool_desc_tokens  # sent every model call
    retrieval_result_tokens = rag_n_chunks * rag_chunk_size  # tokens returned per KB retrieval
    total_tool_calls = rag_n_retrieval_calls + n_other_tool_calls
    total_cycles = total_tool_calls + 1  # tool calls + final answer

    # Build cycle-by-cycle breakdown
    cycles = []
    accumulated_context = 0  # tool exchanges accumulated so far

    for c in range(1, total_cycles + 1):
        # Input = fixed + query + all prior tool exchanges
        cycle_input = fixed_per_call + input_query_tokens + accumulated_context

        if c <= rag_n_retrieval_calls:
            # This is a retrieval call
            cycle_output = tool_call_tokens
            cycle_type = "tool_use (retrieval)"
            result_tokens = retrieval_result_tokens
        elif c <= total_tool_calls:
            # This is an "other" tool call (reranker, etc.)
            cycle_output = tool_call_tokens
            cycle_type = "tool_use (other)"
            result_tokens = other_tool_result_tokens
        else:
            # Final answer
            cycle_output = output_tokens
            cycle_type = "end_turn"
            result_tokens = 0

        cycle_data = {
            "cycle": c,
            "input_tokens": cycle_input,
            "output_tokens": cycle_output,
            "type": cycle_type,
        }

        # Add breakdown for first cycle
        if c == 1:
            cycle_data["breakdown"] = (
                f"system_prompt({_fmt(system_prompt_tokens)}) + "
                f"tool_specs({_fmt(tool_desc_tokens)}) + "
                f"input_query({_fmt(input_query_tokens)}) = "
                f"{_fmt(cycle_input)}"
            )

        cycles.append(cycle_data)

        # Accumulate context for next cycle (tool_call output + tool_result)
        if c <= total_tool_calls:
            accumulated_context += tool_call_tokens + result_tokens

    total_input = sum(cyc["input_tokens"] for cyc in cycles)
    total_output = sum(cyc["output_tokens"] for cyc in cycles)

    # Build explanation
    explanation = {
        "derived_constants": {
            "tool_desc_tokens": f"{n_tools} tools × {tool_spec_tokens} tokens = {_fmt(tool_desc_tokens)}",
            "fixed_per_call": f"system_prompt({_fmt(system_prompt_tokens)}) + tool_specs({_fmt(tool_desc_tokens)}) = {_fmt(fixed_per_call)}",
            "retrieval_result_tokens": f"{rag_n_chunks} chunks × {rag_chunk_size} tokens = {_fmt(retrieval_result_tokens)} per retrieval call",
            "total_tool_calls": f"{rag_n_retrieval_calls} retrieval + {n_other_tool_calls} other = {total_tool_calls}",
            "total_model_calls": f"{total_tool_calls} tool calls + 1 final = {total_cycles}",
        },
        "compounding_detail": (
            f"Each retrieval adds {_fmt(tool_call_tokens)} (call) + {_fmt(retrieval_result_tokens)} (result) = "
            f"{_fmt(tool_call_tokens + retrieval_result_tokens)} tokens to context. "
            f"Each other tool adds {_fmt(tool_call_tokens)} + {_fmt(other_tool_result_tokens)} = "
            f"{_fmt(tool_call_tokens + other_tool_result_tokens)} tokens."
        ),
        "summary": {
            "total_input_tokens": _fmt(total_input),
            "total_output_tokens": _fmt(total_output),
            "total_tokens": _fmt(total_input + total_output),
            "output_to_main_agent": f"{_fmt(output_tokens)} tokens (this becomes tool_result in the main agent)",
        },
    }

    # Cacheable within this invocation's cycles (not across invocations — the query changes)
    intra_invocation_cacheable_prefix = fixed_per_call + input_query_tokens

    result = {
        "assumptions": {
            "system_prompt_tokens": system_prompt_tokens,
            "n_tools": n_tools,
            "tool_spec_tokens": tool_spec_tokens,
            "input_query_tokens": input_query_tokens,
            "tool_call_tokens": tool_call_tokens,
            "rag_n_retrieval_calls": rag_n_retrieval_calls,
            "rag_n_chunks": rag_n_chunks,
            "rag_chunk_size": rag_chunk_size,
            "retrieval_result_tokens": retrieval_result_tokens,
            "n_other_tool_calls": n_other_tool_calls,
            "other_tool_result_tokens": other_tool_result_tokens,
            "output_tokens": output_tokens,
            "tool_desc_tokens": tool_desc_tokens,
            "fixed_per_call": fixed_per_call,
            "total_tool_calls": total_tool_calls,
            "total_cycles": total_cycles,
        },
        "cycles": cycles,
        "total_input": total_input,
        "total_output": total_output,
        "total_tokens": total_input + total_output,
        "output_tokens_to_main_agent": output_tokens,
        "intra_invocation_cacheable_prefix": intra_invocation_cacheable_prefix,
        "explanation": explanation,
    }

    if detail_level == "summary":
        return {
            "assumptions": result["assumptions"],
            "total_input": result["total_input"],
            "total_output": result["total_output"],
            "total_tokens": result["total_tokens"],
            "output_tokens_to_main_agent": result["output_tokens_to_main_agent"],
            "intra_invocation_cacheable_prefix": result["intra_invocation_cacheable_prefix"],
        }
    return result


def calculate_research_subagent_tokens(
    system_prompt_tokens=None,
    n_tools=None,
    tool_spec_tokens=None,
    input_query_tokens=None,
    tool_call_tokens=None,
    n_research_iterations=None,
    fetch_probability=None,
    search_result_tokens=None,
    fetch_result_tokens=None,
    output_tokens=None,
    detail_level="summary",  # "summary" or "full"
):
    """Calculate total token usage for a single internet research sub-agent invocation.

    Args (optional):
        n_research_iterations (int): Search-fetch iteration pairs. Default 4.
        fetch_probability (float): Probability a search leads to a fetch. Default 0.5.
        output_tokens (int): Final synthesized response tokens. Default 1000.

    Example:
        calculate_research_subagent_tokens(n_research_iterations=4, fetch_probability=0.5)

    Returns: dict with total_input, total_output, total_tokens, output_tokens_to_main_agent.

    --- Detailed Documentation ---

    Models a research sub-agent that iteratively searches and fetches web content:
    1. Each iteration: model calls web_search → gets snippets
    2. With probability fetch_probability: model also calls web_fetch → gets full page
    3. After all iterations: model synthesizes a final response

    The pattern is interleaved (search → optional fetch → search → optional fetch → ...)
    based on how real research agents behave (see internet-research-agent-patterns.md).

    The model is stateless — every call sends system_prompt + tool_specs + full
    accumulated history. Token compounding applies within this invocation.

    # NOTE: Prompt caching is modeled via _calculate_subagent_cost() when cache prices
    # are provided. The cacheable prefix (system_prompt + tool_specs + query) is
    # identical across all LLM cycles within a single invocation. Cycle 1 pays
    # cache_write, cycles 2+ pay cache_read. Caching is only applied when the prefix
    # meets the model's minimum threshold (e.g., 1024 for Nova, 2048 for Sonnet).

    Args:
        system_prompt_tokens (int): Sub-agent system prompt (default 500).
        n_tools (int): Number of tools available — search, fetch (default 2).
        tool_spec_tokens (int): Tokens per tool specification (default 50).
        input_query_tokens (int): Query from main agent — the input prompt to this
            sub-agent (default 100).
        tool_call_tokens (int): Model output per tool invocation JSON (default 50).
        n_research_iterations (int): Number of search→(optional fetch) pairs (default 4).
        fetch_probability (float): Probability that a search leads to a fetch, 0.0-1.0 (default 0.5).
        search_result_tokens (int): Tokens returned by web_search (default 100).
        fetch_result_tokens (int): Tokens returned by web_fetch (default 2000).
        output_tokens (int): Final synthesized response tokens (default 1000).
            This is what the main agent receives as tool_result.

    Returns:
        dict: cycles (list of per-model-call details), total_input, total_output,
            output_tokens_to_main_agent, assumptions, explanation.
    """
    # Resolve defaults from config (explicit values passed by caller always win)
    system_prompt_tokens = _resolve_setting("research_defaults", "system_prompt_tokens", system_prompt_tokens)
    n_tools = _resolve_setting("research_defaults", "n_tools", n_tools)
    tool_spec_tokens = _resolve_setting("research_defaults", "tool_spec_tokens", tool_spec_tokens)
    input_query_tokens = _resolve_setting("research_defaults", "input_query_tokens", input_query_tokens)
    tool_call_tokens = _resolve_setting("research_defaults", "tool_call_tokens", tool_call_tokens)
    n_research_iterations = _resolve_setting("research_defaults", "n_research_iterations", n_research_iterations)
    fetch_probability = _resolve_setting("research_defaults", "fetch_probability", fetch_probability)
    search_result_tokens = _resolve_setting("research_defaults", "search_result_tokens", search_result_tokens)
    fetch_result_tokens = _resolve_setting("research_defaults", "fetch_result_tokens", fetch_result_tokens)
    output_tokens = _resolve_setting("research_defaults", "output_tokens", output_tokens)

    # Input validation
    if n_research_iterations < 0:
        raise ValueError(f"n_research_iterations must be >= 0, got {n_research_iterations}")
    if not (0.0 <= fetch_probability <= 1.0):
        raise ValueError(f"fetch_probability must be between 0.0 and 1.0, got {fetch_probability}")
    if n_research_iterations == 0 and output_tokens == 0:
        raise ValueError("At least one of n_research_iterations or output_tokens must be > 0")

    # Derived constants
    tool_desc_tokens = n_tools * tool_spec_tokens
    fixed_per_call = system_prompt_tokens + tool_desc_tokens  # sent every model call
    n_fetches = round(n_research_iterations * fetch_probability)

    # Total model calls = n_research_iterations (searches) + n_fetches (fetches) + 1 (final answer)
    total_tool_calls = n_research_iterations + n_fetches
    total_cycles = total_tool_calls + 1

    # Determine which iterations include a fetch.
    # Distribute fetches across the first n_fetches iterations (most realistic:
    # early iterations are more likely to need full content).
    iterations_with_fetch = set(range(1, n_fetches + 1))

    # Build cycle-by-cycle breakdown
    cycles = []
    accumulated_context = 0  # tool exchanges accumulated so far
    current_iteration = 0

    for iteration in range(1, n_research_iterations + 1):
        current_iteration = iteration
        has_fetch = iteration in iterations_with_fetch

        # --- Search call ---
        cycle_input = fixed_per_call + input_query_tokens + accumulated_context
        cycle_num = len(cycles) + 1

        cycle_data = {
            "cycle": cycle_num,
            "input_tokens": cycle_input,
            "output_tokens": tool_call_tokens,
            "type": "tool_use (search)",
            "iteration": iteration,
        }
        if cycle_num == 1:
            cycle_data["breakdown"] = (
                f"system_prompt({_fmt(system_prompt_tokens)}) + "
                f"tool_specs({_fmt(tool_desc_tokens)}) + "
                f"input_query({_fmt(input_query_tokens)}) = "
                f"{_fmt(cycle_input)}"
            )
        cycles.append(cycle_data)

        # Accumulate: search tool_call output + search result
        accumulated_context += tool_call_tokens + search_result_tokens

        # --- Fetch call (if this iteration has one) ---
        if has_fetch:
            cycle_input = fixed_per_call + input_query_tokens + accumulated_context
            cycle_num = len(cycles) + 1

            cycles.append({
                "cycle": cycle_num,
                "input_tokens": cycle_input,
                "output_tokens": tool_call_tokens,
                "type": "tool_use (fetch)",
                "iteration": iteration,
            })

            # Accumulate: fetch tool_call output + fetch result
            accumulated_context += tool_call_tokens + fetch_result_tokens

    # --- Final answer ---
    cycle_input = fixed_per_call + input_query_tokens + accumulated_context
    cycle_num = len(cycles) + 1

    cycles.append({
        "cycle": cycle_num,
        "input_tokens": cycle_input,
        "output_tokens": output_tokens,
        "type": "end_turn",
        "iteration": None,
    })

    total_input = sum(cyc["input_tokens"] for cyc in cycles)
    total_output = sum(cyc["output_tokens"] for cyc in cycles)

    # Build explanation
    explanation = {
        "derived_constants": {
            "tool_desc_tokens": f"{n_tools} tools × {tool_spec_tokens} tokens = {_fmt(tool_desc_tokens)}",
            "fixed_per_call": f"system_prompt({_fmt(system_prompt_tokens)}) + tool_specs({_fmt(tool_desc_tokens)}) = {_fmt(fixed_per_call)}",
            "n_fetches": f"round({n_research_iterations} iterations × {fetch_probability} probability) = {n_fetches} fetches",
            "total_tool_calls": f"{n_research_iterations} searches + {n_fetches} fetches = {total_tool_calls}",
            "total_model_calls": f"{total_tool_calls} tool calls + 1 final = {total_cycles}",
        },
        "compounding_detail": (
            f"Each search adds {_fmt(tool_call_tokens)} (call) + {_fmt(search_result_tokens)} (result) = "
            f"{_fmt(tool_call_tokens + search_result_tokens)} tokens to context. "
            f"Each fetch adds {_fmt(tool_call_tokens)} (call) + {_fmt(fetch_result_tokens)} (result) = "
            f"{_fmt(tool_call_tokens + fetch_result_tokens)} tokens to context."
        ),
        "pattern": (
            f"Interleaved: {n_research_iterations} iterations, "
            f"{n_fetches} include fetch (iterations {sorted(iterations_with_fetch) if iterations_with_fetch else 'none'}). "
            f"Iterations without fetch: search snippet was sufficient."
        ),
        "summary": {
            "total_input_tokens": _fmt(total_input),
            "total_output_tokens": _fmt(total_output),
            "total_tokens": _fmt(total_input + total_output),
            "output_to_main_agent": f"{_fmt(output_tokens)} tokens (this becomes tool_result in the main agent)",
        },
    }

    # Cacheable within this invocation's cycles (not across invocations — the query changes)
    intra_invocation_cacheable_prefix = fixed_per_call + input_query_tokens

    result = {
        "assumptions": {
            "system_prompt_tokens": system_prompt_tokens,
            "n_tools": n_tools,
            "tool_spec_tokens": tool_spec_tokens,
            "input_query_tokens": input_query_tokens,
            "tool_call_tokens": tool_call_tokens,
            "n_research_iterations": n_research_iterations,
            "fetch_probability": fetch_probability,
            "n_fetches": n_fetches,
            "search_result_tokens": search_result_tokens,
            "fetch_result_tokens": fetch_result_tokens,
            "output_tokens": output_tokens,
            "tool_desc_tokens": tool_desc_tokens,
            "fixed_per_call": fixed_per_call,
            "total_tool_calls": total_tool_calls,
            "total_cycles": total_cycles,
        },
        "cycles": cycles,
        "total_input": total_input,
        "total_output": total_output,
        "total_tokens": total_input + total_output,
        "output_tokens_to_main_agent": output_tokens,
        "intra_invocation_cacheable_prefix": intra_invocation_cacheable_prefix,
        "explanation": explanation,
    }

    if detail_level == "summary":
        return {
            "assumptions": result["assumptions"],
            "total_input": result["total_input"],
            "total_output": result["total_output"],
            "total_tokens": result["total_tokens"],
            "output_tokens_to_main_agent": result["output_tokens_to_main_agent"],
            "intra_invocation_cacheable_prefix": result["intra_invocation_cacheable_prefix"],
        }
    return result


def _calculate_subagent_cost(
    token_result,
    input_price,
    output_price,
    cache_read_price=None,
    cache_write_price=None,
    min_cache_tokens=None,
):
    """Calculate cost for a sub-agent invocation (RAG or research) with optional caching.

    Models intra-invocation caching: within a single invocation, the fixed prefix
    (system_prompt + tool_specs + query) is identical across all LLM cycles. When
    caching is supported and the prefix meets the model's minimum threshold:
      - Cycle 1: prefix charged at cache_write_price (0.0 for free-write models), remainder at input_price
      - Cycles 2+: prefix charged at cache_read_price, remainder at input_price

    When cache_read_price is None or prefix is below min_cache_tokens, falls back to
    simple input_price × total_input calculation (no caching).

    Limitation: cross-invocation caching is not modeled. When a sub-agent is called
    multiple times per session (questions_invoked > 1), the fixed_per_call portion
    (system_prompt + tool_specs) is stable across invocations and could be cache-read
    on invocations 2+ if the TTL holds. This is not yet captured here.

    Works with the output of either calculate_rag_subagent_tokens() or
    calculate_research_subagent_tokens().

    Args:
        token_result (dict): Output from calculate_rag_subagent_tokens() or
            calculate_research_subagent_tokens(). Must contain 'total_input',
            'total_output', and 'intra_invocation_cacheable_prefix' keys.
        input_price (float): Input token price $/M for the sub-agent's model (REQUIRED).
        output_price (float): Output token price $/M for the sub-agent's model (REQUIRED).
        cache_read_price (float|None): Cache read price $/M. None = caching not supported.
        cache_write_price (float|None): Cache write price $/M. Use 0.0 for models with
            free cache writes (e.g., Nova 2.0 Lite). None = caching not supported.
        min_cache_tokens (int|None): Minimum prefix size to qualify for caching (REQUIRED
            when cache_read_price is provided). Model-specific — e.g., 1024 for Nova,
            2048 for Sonnet 4.6, 4096 for Haiku 4.5.

    Returns:
        dict: input_cost, output_cost, total_cost, cache_savings, caching_applied,
            per_cycle_costs, output_tokens_to_main_agent, explanation.

    Raises:
        ValueError: If cache_read_price is provided but min_cache_tokens is None.
    """
    total_input = token_result["total_input"]
    total_output = token_result["total_output"]
    cacheable_prefix = token_result.get("intra_invocation_cacheable_prefix", 0)
    cycles = token_result.get("cycles", [])

    # Validate: if caching prices are provided, min_cache_tokens is required
    if cache_read_price is not None and min_cache_tokens is None:
        raise ValueError(
            "min_cache_tokens is required when cache_read_price is provided. "
            "Use the model's minimum cache threshold (e.g., 1024 for Nova, 2048 for Sonnet, 4096 for Haiku)."
        )

    # Caching requires cache_read_price; cache_write_price=None means not supported,
    # cache_write_price=0.0 means free writes (e.g., Nova 2.0 Lite)
    caching_applied = (
        cache_read_price is not None
        and cache_write_price is not None
        and cacheable_prefix >= (min_cache_tokens or 0)
        and len(cycles) > 1
    )

    per_cycle_costs = []
    total_input_cost = 0.0
    total_input_cost_no_cache = 0.0

    for cyc in cycles:
        cyc_input = cyc["input_tokens"]
        cyc_output = cyc["output_tokens"]
        cyc_output_cost = (cyc_output / 1_000_000) * output_price

        if caching_applied:
            non_cached_tokens = cyc_input - cacheable_prefix
            if cyc["cycle"] == 1:
                prefix_cost = (cacheable_prefix / 1_000_000) * cache_write_price
                remainder_cost = (non_cached_tokens / 1_000_000) * input_price
                cache_type = "write"
            else:
                prefix_cost = (cacheable_prefix / 1_000_000) * cache_read_price
                remainder_cost = (non_cached_tokens / 1_000_000) * input_price
                cache_type = "read"
            cyc_input_cost = prefix_cost + remainder_cost
        else:
            cyc_input_cost = (cyc_input / 1_000_000) * input_price
            cache_type = None

        cyc_no_cache_input_cost = (cyc_input / 1_000_000) * input_price
        total_input_cost += cyc_input_cost
        total_input_cost_no_cache += cyc_no_cache_input_cost

        per_cycle_costs.append({
            "cycle": cyc["cycle"],
            "input_tokens": cyc_input,
            "output_tokens": cyc_output,
            "input_cost": cyc_input_cost,
            "output_cost": cyc_output_cost,
            "cycle_cost": cyc_input_cost + cyc_output_cost,
            "type": cyc["type"],
            "cache_type": cache_type,
        })

    output_cost = (total_output / 1_000_000) * output_price
    total_cost = total_input_cost + output_cost
    total_cost_no_cache = total_input_cost_no_cache + output_cost
    cache_savings = total_cost_no_cache - total_cost if caching_applied else 0.0
    cache_savings_pct = (cache_savings / total_cost_no_cache * 100) if caching_applied and total_cost_no_cache > 0 else 0.0

    if caching_applied:
        caching_detail = (
            f"Applied (prefix={_fmt(cacheable_prefix)} tokens >= threshold={_fmt(min_cache_tokens)}). "
            f"Cycle 1: write @ ${cache_write_price}/M, cycles 2-{len(cycles)}: read @ ${cache_read_price}/M. "
            f"Savings: ${cache_savings:.6f} ({cache_savings_pct:.1f}%)"
        )
    else:
        if cache_read_price is None:
            reason = "model does not support caching"
        elif cache_write_price is None:
            reason = "model does not support caching"
        elif cacheable_prefix < min_cache_tokens:
            reason = f"prefix ({_fmt(cacheable_prefix)} tokens) below threshold ({_fmt(min_cache_tokens)})"
        else:
            reason = "single-cycle invocation (no reuse opportunity)"
        caching_detail = f"Not applied ({reason})"

    explanation = {
        "pricing": {
            "input_price": f"${input_price}/M tokens",
            "output_price": f"${output_price}/M tokens",
            "cache_read_price": f"${cache_read_price}/M tokens" if cache_read_price is not None else "N/A",
            "cache_write_price": f"${cache_write_price}/M tokens" if cache_write_price is not None else ("free" if cache_write_price == 0.0 else "N/A"),
            "caching": caching_detail,
        },
        "cost_breakdown": {
            "input": f"{_fmt(total_input)} tokens → ${total_input_cost:.6f} (with caching)" if caching_applied else f"{_fmt(total_input)} tokens × ${input_price}/M = ${total_input_cost:.6f}",
            "output": f"{_fmt(total_output)} tokens × ${output_price}/M = ${output_cost:.6f}",
            "total": f"${total_input_cost:.6f} + ${output_cost:.6f} = ${total_cost:.6f}",
        },
        "output_to_main_agent": f"{_fmt(token_result['output_tokens_to_main_agent'])} tokens (becomes tool_result in main agent)",
    }

    return {
        "input_cost": total_input_cost,
        "output_cost": output_cost,
        "total_cost": total_cost,
        "total_cost_no_cache": total_cost_no_cache,
        "cache_savings": cache_savings,
        "cache_savings_pct": cache_savings_pct,
        "caching_applied": caching_applied,
        "intra_invocation_cacheable_prefix": cacheable_prefix,
        "per_cycle_costs": per_cycle_costs,
        "output_tokens_to_main_agent": token_result["output_tokens_to_main_agent"],
        "explanation": explanation,
    }


def _calculate_main_agent_compounded_cost(
    # Pricing parameters (REQUIRED — no defaults)
    input_price,
    output_price,
    cache_read_price,       # None = model doesn't support caching
    cache_write_price,      # None = model doesn't support caching
    agent_sessions_per_month,
    # Token parameters (passed to calculate_compounded_tokens_for_agent)
    questions_per_agent_session=None,
    input_tokens=None,
    output_tokens=None,
    system_prompt_tokens=None,
    tools_passed_to_agent=None,
    tool_spec_tokens=None,
    tools_invoked=None,
    tool_call_tokens=None,
    tool_result_tokens=None,
    history_mode=None,
    # Volume & TTL parameters
    days_per_month=30,
    usage_hours_per_day=12,
    # Caching strategy
    cache_history_checkpoints=3,
    # Output detail level
    detail_level="summary",  # "summary" or "full"
):
    """Calculate compounded token costs for the MAIN AGENT ONLY, with and without prompt caching.

    This function models the cost of a single (main/orchestrator) agent in isolation.
    It does NOT account for sub-agent costs (RAG, research, etc.) or the impact of
    sub-agent outputs on the main agent's context size. For a complete multi-agent
    system cost that includes sub-agents, use calculate_agent_session_compounded_cost.

    Takes the same token parameters as calculate_compounded_tokens_for_agent, plus
    pricing and volume parameters. Computes costs with prompt caching (if supported)
    and without, showing savings.

    Caching strategy:
    - Checkpoint 1 (always): system_prompt + tool_specs (the fixed prefix)
    - Checkpoints 2-4: conversation history after Q1, Q2, Q3
      (max 3 history checkpoints because checkpoint 1 is reserved for system+tools,
       and the Bedrock API allows max 4 checkpoints total per request)

    TTL selection:
    - Automatically determines 5-min vs 1-hour TTL based on session volume.
    - If avg gap between sessions > 5 min but <= 60 min → 1-hour TTL recommended.
    - Otherwise → 5-min TTL (cheaper write cost, cache stays warm within session).

    Args:
        input_price (float): Input token price $/M (REQUIRED).
        output_price (float): Output token price $/M (REQUIRED).
        cache_read_price (float|None): Cache read $/M (REQUIRED). None = no caching support.
        cache_write_price (float|None): Cache write $/M (REQUIRED). None = no caching support.
        agent_sessions_per_month (int): Monthly session volume (REQUIRED).
        questions_per_agent_session (int): Questions per session (default 5).
        input_tokens (int): User question tokens (default 100).
        output_tokens (int): Final answer tokens (default 150).
        system_prompt_tokens (int): System prompt tokens (default 2000).
        tools_passed_to_agent (int): Tools in schema (default 10).
        tool_spec_tokens (int): Tokens per tool spec (default 100).
        tools_invoked (int): Tool calls per question (default 5).
        tool_call_tokens (int): Model output per tool call (default 100).
        tool_result_tokens (int): Tool response tokens (default 100).
        history_mode (str): "full" or "condensed" (default "full").
        days_per_month (int): Days per month (default 30).
        usage_hours_per_day (int): Active hours per day (default 12).
        cache_history_checkpoints (int): History checkpoints to use, 0-3 (default 3).

    Returns:
        dict: token_result, no_cache_cost, with_cache_cost (if supported),
            savings, ttl_recommendation, checkpoint_analysis, monthly costs,
            explanation.
    """
    # Resolve defaults from config (explicit values passed by caller always win)
    questions_per_agent_session = _resolve_setting("agent_defaults", "questions_per_session", questions_per_agent_session)
    input_tokens = _resolve_setting("agent_defaults", "input_tokens", input_tokens)
    output_tokens = _resolve_setting("agent_defaults", "output_tokens", output_tokens)
    system_prompt_tokens = _resolve_setting("agent_defaults", "system_prompt_tokens", system_prompt_tokens)
    tools_passed_to_agent = _resolve_setting("agent_defaults", "tools_passed", tools_passed_to_agent)
    tool_spec_tokens = _resolve_setting("agent_defaults", "tool_spec_tokens", tool_spec_tokens)
    tools_invoked = _resolve_setting("agent_defaults", "tools_invoked", tools_invoked)
    tool_call_tokens = _resolve_setting("agent_defaults", "tool_call_tokens", tool_call_tokens)
    tool_result_tokens = _resolve_setting("agent_defaults", "tool_result_tokens", tool_result_tokens)
    history_mode = _resolve_setting("defaults", "history_mode", history_mode)

    # Validate cache_history_checkpoints
    # Max 3 because: Bedrock allows 4 checkpoints total per request.
    # Checkpoint 1 is always reserved for system_prompt + tool_specs.
    # That leaves at most 3 for conversation history.
    if cache_history_checkpoints < 0 or cache_history_checkpoints > 3:
        raise ValueError(
            f"cache_history_checkpoints must be 0-3 (max 4 checkpoints per request, "
            f"1 reserved for system+tools), got {cache_history_checkpoints}"
        )

    if days_per_month <= 0:
        raise ValueError(f"days_per_month must be > 0, got {days_per_month}")
    if usage_hours_per_day <= 0:
        raise ValueError(f"usage_hours_per_day must be > 0, got {usage_hours_per_day}")

    # NOTE: We do not currently check whether fixed_per_call meets the model's
    # minimum token threshold for caching (e.g., 4096 for Sonnet 4.5).
    # If below threshold, caching silently fails — no error, just no benefit.
    # TODO: Revisit this later — add min_cache_tokens parameter and warn.

    # Determine if caching is supported
    caching_supported = cache_read_price is not None and cache_write_price is not None

    # Get token breakdown from the token function
    token_result = calculate_compounded_tokens_for_agent(
        questions_per_agent_session=questions_per_agent_session,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        system_prompt_tokens=system_prompt_tokens,
        tools_passed_to_agent=tools_passed_to_agent,
        tool_spec_tokens=tool_spec_tokens,
        tools_invoked=tools_invoked,
        tool_call_tokens=tool_call_tokens,
        tool_result_tokens=tool_result_tokens,
        history_mode=history_mode,
        detail_level="full",  # always need full detail internally
    )

    # Extract key values from token result
    fixed_per_call = token_result["assumptions"]["fixed_per_call"]
    history_per_question = token_result["assumptions"]["history_per_question"]
    cycles_per_question = token_result["assumptions"]["cycles_per_question"]

    # ── Determine TTL ──
    sessions_per_hour = agent_sessions_per_month / (days_per_month * usage_hours_per_day)
    avg_gap_minutes = 60.0 / sessions_per_hour if sessions_per_hour > 0 else float("inf")

    if avg_gap_minutes > 5 and avg_gap_minutes <= 60:
        recommended_ttl = "1hour"
        cache_write_multiplier = 2.0  # 1-hour TTL write cost = 2× input price
        ttl_reason = (
            f"Avg gap between sessions = {avg_gap_minutes:.1f} min (> 5 min). "
            f"5-min cache would expire between sessions. 1-hour TTL keeps cache warm "
            f"across sessions at {sessions_per_hour:.1f} sessions/hour."
        )
    elif avg_gap_minutes > 60:
        recommended_ttl = "5min"
        cache_write_multiplier = 1.25  # 5-min TTL write cost = 1.25× input price
        ttl_reason = (
            f"Avg gap between sessions = {avg_gap_minutes:.1f} min (> 60 min). "
            f"Even 1-hour cache would expire between sessions. Using 5-min TTL "
            f"(cheaper write cost) — caching only benefits within-session."
        )
    else:
        recommended_ttl = "5min"
        cache_write_multiplier = 1.25
        ttl_reason = (
            f"Avg gap between sessions = {avg_gap_minutes:.1f} min (≤ 5 min). "
            f"5-min cache stays warm between sessions. No need for expensive 1-hour TTL."
        )

    # ── Calculate NO-CACHE cost (baseline) ──
    no_cache_session_input_cost = (token_result["session_total_input"] / 1_000_000) * input_price
    no_cache_session_output_cost = (token_result["session_total_output"] / 1_000_000) * output_price
    no_cache_session_total = no_cache_session_input_cost + no_cache_session_output_cost
    no_cache_monthly = no_cache_session_total * agent_sessions_per_month

    # If caching not supported, return early with just no-cache costs
    if not caching_supported:
        no_cache_result = {
            "token_result": token_result,
            "caching_supported": False,
            "no_cache": {
                "session_input_cost": no_cache_session_input_cost,
                "session_output_cost": no_cache_session_output_cost,
                "session_total": no_cache_session_total,
                "monthly_total": no_cache_monthly,
                "annual_total": no_cache_monthly * 12,
            },
            "with_cache": None,
            "savings": None,
            "explanation": {
                "note": "Model does not support prompt caching (cache_read_price and/or cache_write_price is None).",
                "no_cache_breakdown": f"Input: {_fmt(token_result['session_total_input'])} tokens × ${input_price}/M = ${no_cache_session_input_cost:.4f}, "
                    f"Output: {_fmt(token_result['session_total_output'])} tokens × ${output_price}/M = ${no_cache_session_output_cost:.4f}",
            },
        }
        if detail_level == "summary":
            return {
                "caching_supported": False,
                "no_cache": no_cache_result["no_cache"],
                "with_cache": None,
                "savings": None,
                "token_result": {
                    "assumptions": token_result["assumptions"],
                    "session_total_input": token_result["session_total_input"],
                    "session_total_output": token_result["session_total_output"],
                },
            }
        return no_cache_result

    # ── Calculate WITH-CACHE cost ──
    # Determine which questions have history checkpoints
    # Checkpoint after Q_n means: all cycles in Q_{n+1}, Q_{n+2}, ... can read Q_n's history from cache
    history_checkpoint_after_questions = list(range(1, min(cache_history_checkpoints + 1, questions_per_agent_session)))

    # Build per-cycle cost breakdown
    cached_session_cost = 0.0
    per_question_costs = []
    accumulated_history = 0
    # Track which history is cached (cumulative tokens cached at each checkpoint)
    cached_history_tokens = 0  # how much of the accumulated history is in cache

    # Checkpoint break-even analysis
    checkpoint_analysis = []

    for q_idx, q_data in enumerate(token_result["session"]):
        q_num = q_idx + 1
        question_cost = 0.0
        cycle_costs = []

        for cyc in q_data["cycles"]:
            cycle_input = cyc["input_tokens"]
            cycle_output = cyc["output_tokens"]

            # Split input into: cached_prefix + cached_history + dynamic
            # cached_prefix = fixed_per_call (system + tools) — always checkpoint 1
            # cached_history = portion of accumulated history that has a checkpoint
            # dynamic = everything else (current question's user input + tool exchanges + uncached history)

            cached_prefix = fixed_per_call
            cached_history = cached_history_tokens
            dynamic = cycle_input - cached_prefix - cached_history

            # Determine billing for this cycle
            is_first_cycle_of_session = (q_num == 1 and cyc["cycle"] == 1)
            # History checkpoint write happens on first cycle of the question AFTER the checkpoint was placed
            is_first_cycle_of_question = (cyc["cycle"] == 1)
            # Did we just get a new history checkpoint from the previous question?
            new_history_checkpoint_this_q = (q_num - 1) in history_checkpoint_after_questions and is_first_cycle_of_question
            new_history_tokens = history_per_question if new_history_checkpoint_this_q else 0

            if is_first_cycle_of_session:
                # Very first call: write system+tools to cache
                # But only if there will be subsequent reads to benefit from it
                total_remaining_cycles = (questions_per_agent_session * cycles_per_question) - 1
                if total_remaining_cycles > 0:
                    prefix_cost = (cached_prefix / 1_000_000) * cache_write_price
                    cache_action = "prefix_write"
                else:
                    # Single call in entire session — no benefit from caching
                    prefix_cost = (cached_prefix / 1_000_000) * input_price
                    cache_action = "no_cache (single call)"
                history_cost = 0.0  # no history yet
                dynamic_cost = (dynamic / 1_000_000) * input_price
            elif new_history_checkpoint_this_q and new_history_tokens > 0:
                # First cycle of a question where we write a new history checkpoint
                prefix_cost = (cached_prefix / 1_000_000) * cache_read_price
                # Previously cached history is read, new history portion is written
                old_cached = cached_history - new_history_tokens
                if old_cached > 0:
                    history_cost = (old_cached / 1_000_000) * cache_read_price + \
                                   (new_history_tokens / 1_000_000) * cache_write_price
                else:
                    history_cost = (cached_history / 1_000_000) * cache_write_price
                dynamic_cost = (dynamic / 1_000_000) * input_price
                cache_action = "prefix_read+history_write"
            else:
                # Normal cycle: read prefix + read cached history + pay regular for dynamic
                prefix_cost = (cached_prefix / 1_000_000) * cache_read_price
                history_cost = (cached_history / 1_000_000) * cache_read_price
                dynamic_cost = (dynamic / 1_000_000) * input_price
                cache_action = "prefix_read" + ("+history_read" if cached_history > 0 else "")

            output_cost = (cycle_output / 1_000_000) * output_price
            cycle_total = prefix_cost + history_cost + dynamic_cost + output_cost

            cycle_costs.append({
                "cycle": cyc["cycle"],
                "input_tokens": cycle_input,
                "output_tokens": cycle_output,
                "cached_prefix_tokens": cached_prefix,
                "cached_history_tokens": cached_history,
                "dynamic_tokens": dynamic,
                "prefix_cost": prefix_cost,
                "history_cost": history_cost,
                "dynamic_cost": dynamic_cost,
                "output_cost": output_cost,
                "cycle_total": cycle_total,
                "cache_action": cache_action,
            })

            question_cost += cycle_total

        per_question_costs.append({
            "question": q_num,
            "cycles": cycle_costs,
            "question_total": question_cost,
        })
        cached_session_cost += question_cost

        # After this question, update cached history if a checkpoint was placed
        accumulated_history += history_per_question
        if q_num in history_checkpoint_after_questions:
            cached_history_tokens = accumulated_history

    # ── Checkpoint break-even analysis ──
    for cp_q in history_checkpoint_after_questions:
        # Tokens written at this checkpoint
        write_tokens = history_per_question
        write_cost = (write_tokens / 1_000_000) * cache_write_price

        # Benefit: all subsequent cycles read these tokens at cache_read_price instead of input_price
        remaining_questions = questions_per_agent_session - cp_q
        total_reads = remaining_questions * cycles_per_question
        savings_per_read = (write_tokens / 1_000_000) * (input_price - cache_read_price)
        total_savings = total_reads * savings_per_read

        net_benefit = total_savings - write_cost
        is_worth_it = net_benefit > 0

        checkpoint_analysis.append({
            "checkpoint_after_question": cp_q,
            "tokens_cached": write_tokens,
            "write_cost": write_cost,
            "subsequent_reads": total_reads,
            "savings_per_read": savings_per_read,
            "total_savings": total_savings,
            "net_benefit": net_benefit,
            "worth_it": is_worth_it,
            "reasoning": (
                f"Write {_fmt(write_tokens)} tokens at ${cache_write_price}/M = ${write_cost:.6f}. "
                f"Then {total_reads} reads save ${savings_per_read:.6f} each = ${total_savings:.6f}. "
                f"Net: ${net_benefit:.6f} ({'✅ worth it' if is_worth_it else '❌ not worth it'})"
            ),
        })

    # ── Session and monthly totals ──
    cached_session_output_cost = (token_result["session_total_output"] / 1_000_000) * output_price
    cached_monthly = cached_session_cost * agent_sessions_per_month

    savings_session = no_cache_session_total - cached_session_cost
    savings_pct = (savings_session / no_cache_session_total * 100) if no_cache_session_total > 0 else 0
    savings_monthly = no_cache_monthly - cached_monthly

    # ── Build explanation ──
    explanation = {
        "ttl_recommendation": {
            "recommended": recommended_ttl,
            "write_multiplier": f"{cache_write_multiplier}× input price",
            "sessions_per_hour": f"{sessions_per_hour:.2f}",
            "avg_gap_minutes": f"{avg_gap_minutes:.1f}",
            "reason": ttl_reason,
        },
        "caching_strategy": {
            "checkpoint_1": f"system_prompt({_fmt(system_prompt_tokens)}) + tool_specs({_fmt(tools_passed_to_agent * tool_spec_tokens)}) = {_fmt(fixed_per_call)} tokens",
            "history_checkpoints": f"{len(history_checkpoint_after_questions)} checkpoints after questions: {history_checkpoint_after_questions}",
            "max_checkpoints_note": "Max 3 history checkpoints (Bedrock allows 4 total, 1 reserved for system+tools)",
        },
        "cost_comparison": {
            "no_cache_per_session": f"${no_cache_session_total:.4f}",
            "with_cache_per_session": f"${cached_session_cost:.4f}",
            "savings_per_session": f"${savings_session:.4f} ({savings_pct:.1f}%)",
            "no_cache_monthly": f"${no_cache_monthly:,.2f}",
            "with_cache_monthly": f"${cached_monthly:,.2f}",
            "savings_monthly": f"${savings_monthly:,.2f}",
        },
        "checkpoint_analysis": checkpoint_analysis,
    }

    # Warnings for checkpoints that aren't worth it
    warnings = []
    for cp in checkpoint_analysis:
        if not cp["worth_it"]:
            warnings.append(
                f"History checkpoint after Q{cp['checkpoint_after_question']} is not cost-effective "
                f"(net loss: ${abs(cp['net_benefit']):.6f}). Consider reducing cache_history_checkpoints."
            )

    result = {
        "token_result": token_result,
        "caching_supported": True,
        "recommended_ttl": recommended_ttl,
        "no_cache": {
            "session_input_cost": no_cache_session_input_cost,
            "session_output_cost": no_cache_session_output_cost,
            "session_total": no_cache_session_total,
            "monthly_total": no_cache_monthly,
            "annual_total": no_cache_monthly * 12,
        },
        "with_cache": {
            "per_question": per_question_costs,
            "session_total": cached_session_cost,
            "monthly_total": cached_monthly,
            "annual_total": cached_monthly * 12,
        },
        "savings": {
            "session": savings_session,
            "session_pct": savings_pct,
            "monthly": savings_monthly,
            "annual": savings_monthly * 12,
        },
        "warnings": warnings,
        "explanation": explanation,
    }

    if detail_level == "summary":
        return {
            "caching_supported": True,
            "recommended_ttl": recommended_ttl,
            "no_cache": result["no_cache"],
            "with_cache": {
                "session_total": cached_session_cost,
                "monthly_total": cached_monthly,
                "annual_total": cached_monthly * 12,
            },
            "savings": result["savings"],
            "token_result": {
                "assumptions": token_result["assumptions"],
                "session_total_input": token_result["session_total_input"],
                "session_total_output": token_result["session_total_output"],
            },
        }
    return result


def calculate_agent_session_compounded_cost(
    # Main agent configuration (REQUIRED)
    main_agent_config,
    # Sub-agent configurations (optional)
    subagents=None,
    # Report output path (optional — overrides config and output_dir)
    output_path=None,
    # Report output directory (optional — writes bedrock-pricing.md within it)
    output_dir=None,
):
    """Total cost for an agent session (main + sub-agents). Accepts prices directly — no cache lookup.

    Required keys in main_agent_config:
        input_price (float): $/M input tokens.
        output_price (float): $/M output tokens.
        cache_read_price (float|None): $/M cache read. None = no caching.
        cache_write_price (float|None): $/M cache write.
        agent_sessions_per_month (int): Monthly session volume.

    Example:
        calculate_agent_session_compounded_cost(
            main_agent_config={"input_price": 3.0, "output_price": 15.0,
                "cache_read_price": 0.3, "cache_write_price": 3.75,
                "agent_sessions_per_month": 10000}
        )

    Returns dict: session_total, monthly_total, annual_total, savings_pct,
        subagents_summary, file_path (detailed report written to disk).

    --- Detailed Documentation ---

    Use this tool when a user asks about cost, price, spend, or budget for any
    Bedrock agent workload — single agent or multi-agent. This is the primary
    cost estimation function. It handles:
    - Token compounding (context grows with each tool call and question)
    - Prompt caching (automatic TTL selection, checkpoint break-even)
    - Sub-agent costs (RAG, research) on potentially different/cheaper models
    - How sub-agent outputs flow back into the main agent's context

    If no sub-agents are provided, it calculates single-agent cost with caching.

    Args:
        main_agent_config (dict): Main agent configuration.
            REQUIRED keys:
                input_price (float): Input token price $/M tokens.
                output_price (float): Output token price $/M tokens.
                cache_read_price (float|None): Cache read $/M. None = no caching.
                cache_write_price (float|None): Cache write $/M. None = no caching.
                agent_sessions_per_month (int): Monthly session volume.
            OPTIONAL keys (with defaults):
                questions_per_agent_session (int): Questions per session. Default: 5.
                input_tokens (int): User question tokens. Default: 100.
                output_tokens (int): Agent final answer tokens. Default: 150.
                system_prompt_tokens (int): System prompt size. Default: 2000.
                tools_passed_to_agent (int): Tools in schema. Default: 10.
                tool_spec_tokens (int): Tokens per tool spec. Default: 100.
                tools_invoked (int): Tool calls per question. Default: 5.
                    MUST be >= number of per-question sub-agents.
                tool_call_tokens (int): Model output per tool call. Default: 100.
                tool_result_tokens (int): Tokens per tool result. Default: 100.
                history_mode (str): "full" or "condensed". Default: "full".
                days_per_month (int): For TTL calculation. Default: 30.
                usage_hours_per_day (int): Active hours/day. Default: 12.
                cache_history_checkpoints (int): 0-3. Default: 3.

        subagents (list|None): Optional list of sub-agent configs. Each dict:
            "type" (str): "rag" or "research".
            "token_params" (dict): Parameters for the sub-agent (see below).
            "model_prices" (dict): Sub-agent model pricing. Keys:
                "input_price" (float): REQUIRED. Input token price $/M.
                "output_price" (float): REQUIRED. Output token price $/M.
                "cache_read_price" (float|None): Optional. Cache read $/M. Omit if model has no caching.
                "cache_write_price" (float|None): Optional. Cache write $/M. Use 0.0 for free-write models.
                "min_cache_tokens" (int): REQUIRED when cache_read_price is provided. Model's minimum
                    cache threshold (e.g., 1024 for Nova, 2048 for Sonnet, 4096 for Haiku).
            "questions_invoked" (int):
                0 = pre-session (output added to main agent system_prompt, cached).
                1-N = invoked as tool in first N questions.

            token_params for type="rag":
                system_prompt_tokens (int): Default 500.
                n_tools (int): Default 2.
                tool_spec_tokens (int): Default 100.
                input_query_tokens (int): Default 100.
                tool_call_tokens (int): Default 50.
                rag_n_retrieval_calls (int): KB retrieval calls. Default 2.
                rag_n_chunks (int): Chunks per retrieval. Default 10.
                rag_chunk_size (int): Tokens per chunk. Default 300.
                n_other_tool_calls (int): Other tools (reranker). Default 1.
                other_tool_result_tokens (int): Default 200.
                output_tokens (int): Response to main agent. Default 300.

            token_params for type="research":
                system_prompt_tokens (int): Default 500.
                n_tools (int): Default 2.
                tool_spec_tokens (int): Default 50.
                input_query_tokens (int): Default 100.
                tool_call_tokens (int): Default 50.
                n_research_iterations (int): Search cycles. Default 4.
                fetch_probability (float): 0.0-1.0. Default 0.5.
                search_result_tokens (int): Default 100.
                fetch_result_tokens (int): Default 2000.
                output_tokens (int): Response to main agent. Default 1000.

        detail_level (str): "summary" (default) or "full".
            Use "summary" for presenting results. Use "full" for detailed reports.

    Returns:
        When detail_level="summary":
            session_total (float): Cost per session (with caching).
            session_total_no_cache (float): Cost per session without caching.
            monthly_total (float): Monthly cost.
            annual_total (float): Annual cost.
            sessions_per_month (int): Volume used.
            savings_pct (float): Caching savings percentage.
            main_agent_session_cost (float): Main agent portion.
            subagent_session_cost (float): Sub-agents portion.
            subagents_summary (list): Per sub-agent cost breakdown.
            recommended_ttl (str): "5min" or "1hour".

        When detail_level="full":
            All of the above plus per-cycle token/cost detail, caching
            strategy, checkpoint analysis, and full explanation dicts.

    Example:
        # Single agent (no sub-agents):
        calculate_agent_session_compounded_cost(
            main_agent_config={
                "input_price": 3.0,
                "output_price": 15.0,
                "cache_read_price": 0.3,
                "cache_write_price": 3.75,
                "agent_sessions_per_month": 10000,
            }
        )

        # Multi-agent (main + RAG with caching + research without caching):
        calculate_agent_session_compounded_cost(
            main_agent_config={
                "input_price": 3.0,
                "output_price": 15.0,
                "cache_read_price": 0.3,
                "cache_write_price": 3.75,
                "agent_sessions_per_month": 10000,
                "questions_per_agent_session": 5,
                "system_prompt_tokens": 2000,
                "tools_invoked": 5,
            },
            subagents=[
                {
                    "type": "rag",
                    "token_params": {"rag_n_chunks": 10, "output_tokens": 300},
                    "model_prices": {
                        "input_price": 1.0,
                        "output_price": 5.0,
                        "cache_read_price": 0.10,
                        "cache_write_price": 1.25,
                        "min_cache_tokens": 4096,
                    },
                    "questions_invoked": 3,
                },
                {
                    "type": "research",
                    "token_params": {"n_research_iterations": 4, "output_tokens": 1000},
                    "model_prices": {"input_price": 1.0, "output_price": 5.0},
                    "questions_invoked": 0,
                },
            ],
        )

    Constraints:
        - tools_invoked must be >= number of sub-agents with questions_invoked > 0.
        - cache_history_checkpoints max 3 (Bedrock allows 4 total, 1 for system+tools).
        - cache_read_price=None disables caching analysis (returns no-cache cost only).
        - Sub-agent token_params use defaults for any omitted key — only override
          what the user specifies.
    """
    if subagents is None:
        subagents = []

    questions_per_session = main_agent_config.get(
        "questions_per_agent_session",
        _resolve_setting("agent_defaults", "questions_per_session")
    )
    tools_invoked = main_agent_config.get(
        "tools_invoked",
        _resolve_setting("agent_defaults", "tools_invoked")
    )

    # Validate sub-agent configs
    for i, sa in enumerate(subagents):
        if "type" not in sa:
            raise ValueError(f"subagents[{i}] missing 'type' key")
        if "token_params" not in sa:
            raise ValueError(f"subagents[{i}] missing 'token_params' key")
        if "model_prices" not in sa:
            raise ValueError(f"subagents[{i}] missing 'model_prices' key")
        if "questions_invoked" not in sa:
            raise ValueError(f"subagents[{i}] missing 'questions_invoked' key")
        qi = sa["questions_invoked"]
        if qi < 0:
            raise ValueError(f"subagents[{i}] questions_invoked must be >= 0, got {qi}")
        if qi > questions_per_session:
            raise ValueError(
                f"subagents[{i}] questions_invoked ({qi}) cannot exceed "
                f"questions_per_agent_session ({questions_per_session})"
            )

    # ── Step 1: Calculate sub-agent tokens ──
    subagent_results = []
    for sa in subagents:
        sa_type = sa["type"]
        # Strip detail_level from token_params if present (we always use full internally)
        token_params = {k: v for k, v in sa["token_params"].items() if k != "detail_level"}

        if sa_type == "rag":
            token_result = calculate_rag_subagent_tokens(**token_params, detail_level="full")
        elif sa_type == "research":
            token_result = calculate_research_subagent_tokens(**token_params, detail_level="full")
        else:
            # Future extensibility: caller can pass a pre-computed token_result
            # or we raise for unknown types
            if "token_result" in sa:
                token_result = sa["token_result"]
            else:
                raise ValueError(
                    f"Unknown sub-agent type '{sa_type}'. Supported: 'rag', 'research'. "
                    f"For custom types, include a 'token_result' key with the pre-computed result."
                )

        subagent_results.append({
            "config": sa,
            "token_result": token_result,
        })

    # ── Step 2: Adjust main agent config based on sub-agent invocation patterns ──
    adjusted_config = dict(main_agent_config)

    # Sub-agents with questions_invoked=0: add response to system_prompt (cacheable prefix)
    pre_session_tokens = 0
    for sa_res in subagent_results:
        if sa_res["config"]["questions_invoked"] == 0:
            pre_session_tokens += sa_res["token_result"]["output_tokens_to_main_agent"]

    if pre_session_tokens > 0:
        current_sys = adjusted_config.get("system_prompt_tokens", 2000)
        adjusted_config["system_prompt_tokens"] = current_sys + pre_session_tokens

    # Sub-agents with questions_invoked > 0: their responses are tool_results
    # Build the tool_result_tokens list with sub-agent responses first, then regular tools
    per_question_subagents = [
        sa_res for sa_res in subagent_results
        if sa_res["config"]["questions_invoked"] > 0
    ]

    if per_question_subagents:
        # Number of regular tools (non-sub-agent)
        n_subagent_tools = len(per_question_subagents)
        regular_tool_result = adjusted_config.get("tool_result_tokens", 100)
        current_tools_invoked = adjusted_config.get("tools_invoked", 5)

        # The sub-agents ARE tools — they count toward tools_invoked
        # Total tools = sub-agent tools + remaining regular tools
        n_regular_tools = current_tools_invoked - n_subagent_tools

        if n_regular_tools < 0:
            raise ValueError(
                f"tools_invoked ({current_tools_invoked}) must be >= number of per-question "
                f"sub-agents ({n_subagent_tools}). Sub-agents count as tools."
            )

        # Build per-tool result sizes: sub-agents first, then regular tools
        tool_result_list = []
        for sa_res in per_question_subagents:
            tool_result_list.append(sa_res["token_result"]["output_tokens_to_main_agent"])
        # Fill remaining with regular tool result size
        if isinstance(regular_tool_result, list):
            # If already a list, take the non-sub-agent portion
            tool_result_list.extend(regular_tool_result[:n_regular_tools])
        else:
            tool_result_list.extend([regular_tool_result] * n_regular_tools)

        adjusted_config["tool_result_tokens"] = tool_result_list

    # ── Step 3: Calculate main agent cost ──
    # Remove keys that aren't parameters of _calculate_main_agent_compounded_cost
    _excluded_keys = {"detail_level", "model_name", "min_cache_tokens"}
    cost_params = {k: v for k, v in adjusted_config.items() if k not in _excluded_keys}
    cost_params["detail_level"] = "full"  # always need full detail internally
    main_cost_result = _calculate_main_agent_compounded_cost(**cost_params)

    # ── Step 4: Calculate sub-agent costs ──
    subagent_cost_results = []
    for sa_res in subagent_results:
        sa_config = sa_res["config"]
        model_prices = sa_config["model_prices"]
        token_result = sa_res["token_result"]
        qi = sa_config["questions_invoked"]

        # Cost per invocation (with caching if prices provided)
        cost_per_invocation = _calculate_subagent_cost(
            token_result,
            input_price=model_prices["input_price"],
            output_price=model_prices["output_price"],
            cache_read_price=model_prices.get("cache_read_price"),
            cache_write_price=model_prices.get("cache_write_price"),
            min_cache_tokens=model_prices.get("min_cache_tokens"),
        )

        # Number of invocations per session
        if qi == 0:
            invocations_per_session = 1
        else:
            invocations_per_session = qi

        session_cost = cost_per_invocation["total_cost"] * invocations_per_session
        sessions_per_month = main_agent_config.get("agent_sessions_per_month", 0)
        monthly_cost = session_cost * sessions_per_month

        subagent_cost_results.append({
            "type": sa_config["type"],
            "questions_invoked": qi,
            "invocations_per_session": invocations_per_session,
            "cost_per_invocation": cost_per_invocation["total_cost"],
            "session_cost": session_cost,
            "monthly_cost": monthly_cost,
            "annual_cost": monthly_cost * 12,
            "model_prices": model_prices,
            "token_result": token_result,
            "cost_detail": cost_per_invocation,
        })

    # ── Step 5: Compute totals ──
    main_session = main_cost_result["no_cache"]["session_total"]
    main_session_cached = main_cost_result["with_cache"]["session_total"] if main_cost_result["caching_supported"] else main_session
    subagent_session_total = sum(sa["session_cost"] for sa in subagent_cost_results)

    sessions_per_month = main_agent_config.get("agent_sessions_per_month", 0)

    # Grand totals (using cached main agent cost if available)
    session_total = main_session_cached + subagent_session_total
    monthly_total = session_total * sessions_per_month
    annual_total = monthly_total * 12

    # No-cache comparison
    session_total_no_cache = main_session + subagent_session_total
    monthly_total_no_cache = session_total_no_cache * sessions_per_month

    # ── Build explanation ──
    explanation = {
        "architecture": {
            "main_agent_model": f"Prices: input=${main_agent_config.get('input_price', '?')}/M, output=${main_agent_config.get('output_price', '?')}/M",
            "sub_agents": [
                {
                    "type": sa["type"],
                    "model_prices": sa["model_prices"],
                    "questions_invoked": sa["questions_invoked"],
                    "invocations_per_session": sa["invocations_per_session"],
                    "output_to_main_agent": f"{_fmt(sa['token_result']['output_tokens_to_main_agent'])} tokens",
                    "role": "cacheable prefix (pre-session)" if sa["questions_invoked"] == 0 else f"tool in first {sa['questions_invoked']} questions",
                }
                for sa in subagent_cost_results
            ],
        },
        "cost_breakdown": {
            "main_agent_per_session": f"${main_session_cached:.6f}" + (" (with cache)" if main_cost_result["caching_supported"] else ""),
            "subagents_per_session": f"${subagent_session_total:.6f}",
            "total_per_session": f"${session_total:.6f}",
            "total_monthly": f"${monthly_total:,.2f}",
            "total_annual": f"${annual_total:,.2f}",
        },
        "savings_from_caching": {
            "session_no_cache": f"${session_total_no_cache:.6f}",
            "session_with_cache": f"${session_total:.6f}",
            "savings_per_session": f"${session_total_no_cache - session_total:.6f}",
        } if main_cost_result["caching_supported"] else None,
    }

    result = {
        "main_agent": main_cost_result,
        "subagents": subagent_cost_results,
        "session_total": session_total,
        "session_total_no_cache": session_total_no_cache,
        "monthly_total": monthly_total,
        "annual_total": annual_total,
        "sessions_per_month": sessions_per_month,
        "explanation": explanation,
    }

    # ── Step 6: Build capacity_profile for use by check_capacity_fit() ──
    # This provides the token summary needed for capacity planning without re-computing.
    main_token_result = main_cost_result.get("token_result", {})
    main_session_data = main_token_result.get("session", [])
    questions_per_session = main_agent_config.get("questions_per_agent_session", 5)
    main_tools_invoked = main_agent_config.get("tools_invoked", 5)
    main_cycles_per_question = main_tools_invoked + 1

    # Compute per-call averages from the full session token data
    if main_session_data:
        total_input = sum(q.get("question_total_input", 0) for q in main_session_data)
        total_output = sum(q.get("question_total_output", 0) for q in main_session_data)
        total_calls = len(main_session_data) * main_cycles_per_question
        avg_input_per_call = total_input / total_calls if total_calls > 0 else 0
        avg_output_per_call = total_output / total_calls if total_calls > 0 else 0
        tokens_per_question = (total_input + total_output) / len(main_session_data) if main_session_data else 0
    else:
        # Fallback: use the cost result's token assumptions
        avg_input_per_call = 0
        avg_output_per_call = 0
        tokens_per_question = 0

    capacity_profile = {
        "sessions_per_month": sessions_per_month,
        "main_agent": {
            "model_name": main_agent_config.get("model_name"),
            "llm_calls_per_question": main_cycles_per_question,
            "avg_input_tokens_per_call": avg_input_per_call,
            "avg_output_tokens_per_call": avg_output_per_call,
            "tokens_per_question": tokens_per_question,
            "questions_per_session": questions_per_session,
        },
        "sub_agents": [],
    }

    for sa_res in subagent_cost_results:
        sa_config = sa_res.get("config", sa_res)
        sa_token_result = sa_res.get("token_result", {})
        qi = sa_res.get("questions_invoked", sa_config.get("questions_invoked", 0))

        # Skip pre-session sub-agents (questions_invoked=0) — they don't generate runtime calls
        if qi == 0:
            continue

        # Determine LLM calls per invocation from the token result
        sa_cycles = sa_token_result.get("cycles", [])
        llm_calls_per_invocation = len(sa_cycles) if sa_cycles else 1

        sa_total_input = sa_token_result.get("total_input", sa_token_result.get("total_input_tokens", 0))
        sa_total_output = sa_token_result.get("total_output", sa_token_result.get("total_output_tokens", 0))
        sa_avg_input = sa_total_input / llm_calls_per_invocation if llm_calls_per_invocation > 0 else 0
        sa_avg_output = sa_total_output / llm_calls_per_invocation if llm_calls_per_invocation > 0 else 0

        capacity_profile["sub_agents"].append({
            "type": sa_res["type"],
            "model_name": sa_config.get("model_prices", {}).get("model_name") or sa_config.get("model_name"),
            "llm_calls_per_invocation": llm_calls_per_invocation,
            "invocations_per_session": sa_res["invocations_per_session"],
            "avg_input_tokens_per_call": sa_avg_input,
            "avg_output_tokens_per_call": sa_avg_output,
            "tokens_per_invocation": sa_total_input + sa_total_output,
        })

    result["capacity_profile"] = capacity_profile

    # ── Step 7: Generate token breakdown table ──
    result["token_table"] = _format_token_table(result)
    result["capacity_profile_table"] = _format_capacity_profile_table(capacity_profile)

    # ── Step 8: Write report to file and return compact summary ──
    # Precedence: output_path > output_dir > generated path
    _effective_output_path = output_path
    if _effective_output_path is None and output_dir is not None:
        _effective_output_path = os.path.join(output_dir, _BEDROCK_REPORT_FILENAME)
    file_path = _write_report_to_file(result, main_agent_config, subagents, _effective_output_path)

    if file_path is None:
        # File write failed — fall back to full inline result with warning
        print(
            "⚠️  Report: Failed to write report file. Returning full result inline.\n"
            "    This increases token usage and latency. Specify a writable folder via\n"
            "    reports.output_dir in ~/.bedrock_skills/config.yaml or pass output_path='/writable/path/report.md'.",
            file=sys.stderr
        )
        result["_file_write_failed"] = True
        return result

    return _build_compact_summary(result, file_path)


def _format_price(p):
    try:
        v = float(p)
        if v == 0: return "$0.00"
        elif v < 0.0001: return f"${v:.8f}"
        elif v < 0.01: return f"${v:.6f}"
        elif v < 1: return f"${v:.4f}"
        else: return f"${v:.2f}"
    except: return p


def _format_token_table(result):
    """Generate a markdown token breakdown table from calculate_agent_session_compounded_cost full output.

    Produces separate tables for:
    1. Pre-session sub-agents (questions_invoked=0)
    2. Per-question sub-agents (one table per type, showing a single invocation)
    3. Main agent session (per-cycle breakdown with running totals)
    4. Session summary

    Args:
        result (dict): Full output from calculate_agent_session_compounded_cost(detail_level="full").

    Returns:
        str: Markdown-formatted token breakdown tables.
    """
    lines = []
    main_cost_result = result["main_agent"]
    token_result = main_cost_result["token_result"]
    assumptions = token_result["assumptions"]
    session_data = token_result["session"]
    subagent_cost_results = result.get("subagents", [])

    system_prompt_tokens = assumptions["system_prompt_tokens"]
    tool_desc_tokens = assumptions["tool_desc_tokens"]
    fixed_per_call = assumptions["fixed_per_call"]
    history_per_question = assumptions["history_per_question"]
    cycles_per_question = assumptions["cycles_per_question"]
    tools_invoked = assumptions["tools_invoked"]
    delta_per_tool = assumptions["delta_per_tool"]
    input_tokens = assumptions["input_tokens"]
    output_tokens = assumptions["output_tokens"]
    tool_call_tokens = assumptions["tool_call_tokens"]
    tool_result_tokens_list = assumptions["tool_result_tokens"]

    # Identify sub-agent tool positions (sub-agents are first in tool_result_tokens list)
    per_question_subagents = [sa for sa in subagent_cost_results if sa["questions_invoked"] > 0]
    pre_session_subagents = [sa for sa in subagent_cost_results if sa["questions_invoked"] == 0]

    # ── Pre-session sub-agents ──
    for sa in pre_session_subagents:
        sa_token = sa["token_result"]
        sa_cycles = sa_token.get("cycles", [])
        sa_assumptions = sa_token.get("assumptions", {})
        model_name = sa.get("model_prices", {}).get("model_name") or sa["type"].upper()

        lines.append(f"## Pre-Session: {sa['type'].title()} Sub-Agent (1 invocation on {model_name})")
        lines.append("")
        lines.append("| Cycle | Type | Input Components | Input Tokens | Output Tokens | Cumulative In | Cumulative Out |")
        lines.append("|-------|------|------------------|--------------|---------------|---------------|----------------|")

        sa_fixed = sa_assumptions.get("fixed_per_call", 0)
        sa_sys = sa_assumptions.get("system_prompt_tokens", 0)
        sa_tool_desc = sa_assumptions.get("tool_desc_tokens", 0)
        sa_input_query = sa_assumptions.get("input_query_tokens", 0)
        accumulated = 0
        sa_cum_in = 0
        sa_cum_out = 0

        for cyc in sa_cycles:
            c_num = cyc["cycle"]
            c_input = cyc["input_tokens"]
            c_output = cyc["output_tokens"]
            c_type = cyc.get("type", "")

            if c_num == 1:
                components = f"system({_fmt(sa_sys)}) + tools({_fmt(sa_tool_desc)}) + query({_fmt(sa_input_query)})"
            else:
                components = f"prev({_fmt(c_input - accumulated)}) + result({_fmt(accumulated - (sa_fixed + sa_input_query) if accumulated > 0 else 0)})"
                # Simpler: just show the delta
                prev_input = sa_cycles[c_num - 2]["input_tokens"]
                prev_output = sa_cycles[c_num - 2]["output_tokens"]
                # The accumulated context grows by prev_output + tool_result
                components = f"prev({_fmt(prev_input)}+{_fmt(prev_output)}) + result"

            sa_cum_in += c_input
            sa_cum_out += c_output

            lines.append(f"| {c_num} | {c_type} | {components} | {_fmt(c_input)} | {_fmt(c_output)} | {_fmt(sa_cum_in)} | {_fmt(sa_cum_out)} |")

        sa_total_in = sa_token.get("total_input", 0)
        sa_total_out = sa_token.get("total_output", 0)
        sa_output_to_main = sa_token.get("output_tokens_to_main_agent", 0)
        lines.append(f"| | **Totals** | | **{_fmt(sa_total_in)}** | **{_fmt(sa_total_out)}** | | |")
        lines.append("")
        lines.append(f"→ Output ({_fmt(sa_output_to_main)} tokens) added to main agent system prompt (cached)")
        lines.append("")
        lines.append("---")
        lines.append("")

    # ── Per-question sub-agents ──
    for sa in per_question_subagents:
        sa_token = sa["token_result"]
        sa_cycles = sa_token.get("cycles", [])
        sa_assumptions = sa_token.get("assumptions", {})
        model_name = sa.get("model_prices", {}).get("model_name") or sa["type"].upper()
        qi = sa["questions_invoked"]

        lines.append(f"## {sa['type'].title()} Sub-Agent (per invocation on {model_name}, invoked in Q1–Q{qi})")
        lines.append("")
        lines.append("| Cycle | Type | Input Components | Input Tokens | Output Tokens | Cumulative In | Cumulative Out |")
        lines.append("|-------|------|------------------|--------------|---------------|---------------|----------------|")

        sa_sys = sa_assumptions.get("system_prompt_tokens", 0)
        sa_tool_desc = sa_assumptions.get("tool_desc_tokens", 0)
        sa_input_query = sa_assumptions.get("input_query_tokens", 0)
        sa_cum_in = 0
        sa_cum_out = 0

        for cyc in sa_cycles:
            c_num = cyc["cycle"]
            c_input = cyc["input_tokens"]
            c_output = cyc["output_tokens"]
            c_type = cyc.get("type", "")

            if c_num == 1:
                components = f"system({_fmt(sa_sys)}) + tools({_fmt(sa_tool_desc)}) + query({_fmt(sa_input_query)})"
            else:
                components = f"prev + accumulated_context"

            sa_cum_in += c_input
            sa_cum_out += c_output

            lines.append(f"| {c_num} | {c_type} | {components} | {_fmt(c_input)} | {_fmt(c_output)} | {_fmt(sa_cum_in)} | {_fmt(sa_cum_out)} |")

        sa_total_in = sa_token.get("total_input", 0)
        sa_total_out = sa_token.get("total_output", 0)
        sa_output_to_main = sa_token.get("output_tokens_to_main_agent", 0)
        lines.append(f"| | **Totals** | | **{_fmt(sa_total_in)}** | **{_fmt(sa_total_out)}** | | |")
        lines.append("")
        lines.append(f"→ Output ({_fmt(sa_output_to_main)} tokens) returned to main agent as tool_result")
        lines.append("")
        lines.append("---")
        lines.append("")

    # ── Main agent session ──
    model_name = result.get("capacity_profile", {}).get("main_agent", {}).get("model_name") or "Main Agent"
    lines.append(f"## Main Agent Session ({model_name})")
    lines.append("")
    lines.append("| Question | Cycle | Type | Input Components | Input Tokens | Output Tokens | Cumulative In | Cumulative Out |")
    lines.append("|----------|-------|------|------------------|--------------|---------------|---------------|----------------|")

    running_input = 0
    running_output = 0
    accumulated_history = 0

    # Determine pre-session addition to system prompt
    pre_session_tokens_added = sum(
        sa["token_result"].get("output_tokens_to_main_agent", 0)
        for sa in pre_session_subagents
    )
    # The system_prompt_tokens in assumptions already includes pre-session tokens
    # (they were added in Step 2 of the cost function). Show the original + addition.
    original_system_prompt = system_prompt_tokens - pre_session_tokens_added

    for q_data in session_data:
        q_num = q_data["question"]

        for cyc in q_data["cycles"]:
            c_num = cyc["cycle"]
            c_input = cyc["input_tokens"]
            c_output = cyc["output_tokens"]
            c_type = cyc.get("type", "tool_use")

            # Build input components description
            if c_num == 1:
                parts = []
                if pre_session_tokens_added > 0:
                    parts.append(f"system({_fmt(original_system_prompt)})+pre-session({_fmt(pre_session_tokens_added)})")
                else:
                    parts.append(f"system({_fmt(system_prompt_tokens)})")
                parts.append(f"tools({_fmt(tool_desc_tokens)})")
                if accumulated_history > 0:
                    parts.append(f"history({_fmt(accumulated_history)})")
                parts.append(f"user({_fmt(input_tokens)})")
                components = " + ".join(parts)
            else:
                # Subsequent cycles: previous context + tool exchange
                tool_idx = c_num - 2  # which tool result we're adding (0-indexed)
                if tool_idx < len(tool_result_tokens_list):
                    tr_size = tool_result_tokens_list[tool_idx]
                    # Check if this is a sub-agent tool
                    if tool_idx < len(per_question_subagents) and q_num <= per_question_subagents[tool_idx]["questions_invoked"]:
                        sa_type = per_question_subagents[tool_idx]["type"]
                        components = f"prev + call({_fmt(tool_call_tokens)}) + {sa_type}_result({_fmt(tr_size)})"
                    else:
                        components = f"prev + call({_fmt(tool_call_tokens)}) + tool_result({_fmt(tr_size)})"
                else:
                    components = f"prev + call({_fmt(tool_call_tokens)}) + tool_result"

            # Determine output type label
            if c_type == "end_turn":
                type_label = "final_answer"
            elif c_num - 1 < len(per_question_subagents) and q_num <= per_question_subagents[c_num - 2]["questions_invoked"] if c_num > 1 and (c_num - 2) < len(per_question_subagents) else False:
                type_label = f"→{per_question_subagents[c_num - 2]['type']}"
            else:
                type_label = "tool_call"

            running_input += c_input
            running_output += c_output

            lines.append(
                f"| {q_num} | {c_num} | {type_label} | {components} | "
                f"{_fmt(c_input)} | {_fmt(c_output)} | {_fmt(running_input)} | {_fmt(running_output)} |"
            )

        # Question subtotal row
        q_total_in = q_data["question_total_input"]
        q_total_out = q_data["question_total_output"]
        lines.append(f"| | | | **Q{q_num} Subtotal** | **{_fmt(q_total_in)}** | **{_fmt(q_total_out)}** | | |")

        # History explanation row (italic, not for the last question)
        questions_per_session = len(session_data)
        if q_num < questions_per_session:
            # Build the history formula
            total_delta = assumptions["total_delta"]
            history_components = f"user({_fmt(input_tokens)}) + tool_exchanges({_fmt(total_delta)}) + answer({_fmt(output_tokens)})"
            new_cumulative = accumulated_history + history_per_question
            lines.append(
                f"| | | | *→ History added to next Q: {history_components} = {_fmt(history_per_question)}. "
                f"Cumulative history: {_fmt(new_cumulative)}* | | | | |"
            )

        # Update accumulated history for next question
        accumulated_history += history_per_question

    # Session total row
    session_total_in = token_result["session_total_input"]
    session_total_out = token_result["session_total_output"]
    lines.append(f"| | | | **Session Total** | **{_fmt(session_total_in)}** | **{_fmt(session_total_out)}** | | |")
    lines.append("")

    # ── Session summary ──
    lines.append("---")
    lines.append("")
    lines.append("## Session Summary")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    lines.append(f"| Total Input Tokens (Main Agent) | {_fmt(session_total_in)} |")
    lines.append(f"| Total Output Tokens (Main Agent) | {_fmt(session_total_out)} |")
    lines.append(f"| Total Tokens (Main Agent) | {_fmt(session_total_in + session_total_out)} |")

    # Sub-agent totals
    all_sa_tokens = 0
    all_sa_calls = 0
    for sa in subagent_cost_results:
        sa_token = sa["token_result"]
        sa_total = sa_token.get("total_input", 0) + sa_token.get("total_output", 0)
        sa_invocations = sa["invocations_per_session"]
        sa_calls = len(sa_token.get("cycles", [])) or 1
        sa_type = sa["type"]
        model_name = sa.get("model_prices", {}).get("model_name") or sa_type.upper()

        total_for_session = sa_total * sa_invocations
        total_calls_for_session = sa_calls * sa_invocations
        all_sa_tokens += total_for_session
        all_sa_calls += total_calls_for_session

        lines.append(f"| Total Tokens ({sa_type.title()} × {sa_invocations} invocations) | {_fmt(total_for_session)} |")
        lines.append(f"| LLM Calls ({sa_type.title()} × {sa_invocations}) | {total_calls_for_session} |")

    grand_total = session_total_in + session_total_out + all_sa_tokens
    main_calls = len(session_data) * cycles_per_question
    lines.append(f"| **Total Tokens (All Agents)** | **{_fmt(grand_total)}** |")
    lines.append(f"| LLM Calls (Main Agent) | {main_calls} |")
    lines.append(f"| **Total LLM Calls** | **{main_calls + all_sa_calls}** |")

    return "\n".join(lines)


def _format_capacity_profile_table(capacity_profile):
    """Generate a markdown derivation table showing how capacity profile values were calculated.

    Args:
        capacity_profile (dict): The capacity_profile from calculate_agent_session_compounded_cost.

    Returns:
        str: Markdown-formatted table with Field | Formula | Value columns.
    """
    lines = []
    main = capacity_profile.get("main_agent", {})
    sessions = capacity_profile.get("sessions_per_month", 0)
    questions_per_session = main.get("questions_per_session", 0)
    llm_calls_per_q = main.get("llm_calls_per_question", 0)
    avg_input = main.get("avg_input_tokens_per_call", 0)
    avg_output = main.get("avg_output_tokens_per_call", 0)
    tokens_per_q = main.get("tokens_per_question", 0)

    total_calls = questions_per_session * llm_calls_per_q
    total_input = avg_input * total_calls
    total_output = avg_output * total_calls
    questions_per_month = sessions * questions_per_session

    model_name = main.get("model_name") or "Main Agent"
    lines.append(f"### Capacity Profile Derivation — {model_name}")
    lines.append("")
    lines.append("| Field | Formula | Value |")
    lines.append("|-------|---------|-------|")
    lines.append(f"| LLM calls per question | tools_invoked + 1 | {llm_calls_per_q} |")
    lines.append(f"| Total input tokens/session | sum(Q1..Q{questions_per_session} input) | {_fmt(total_input)} |")
    lines.append(f"| Total output tokens/session | sum(Q1..Q{questions_per_session} output) | {_fmt(total_output)} |")
    lines.append(f"| Total LLM calls/session | questions × cycles_per_question | {questions_per_session} × {llm_calls_per_q} = {total_calls} |")
    lines.append(f"| Avg input tokens per call | total_input / total_calls | {_fmt(total_input)} ÷ {total_calls} = {_fmt(avg_input)} |")
    lines.append(f"| Avg output tokens per call | total_output / total_calls | {_fmt(total_output)} ÷ {total_calls} = {_fmt(avg_output)} |")
    lines.append(f"| Tokens per question | (total_input + total_output) / questions | ({_fmt(total_input)} + {_fmt(total_output)}) ÷ {questions_per_session} = {_fmt(tokens_per_q)} |")
    lines.append(f"| Questions per month | sessions × questions_per_session | {_fmt(sessions)} × {questions_per_session} = {_fmt(questions_per_month)} |")

    # Sub-agents
    sub_agents = capacity_profile.get("sub_agents", [])
    for sa in sub_agents:
        sa_model = sa.get("model_name") or sa.get("type", "Sub-Agent").title()
        sa_calls = sa.get("llm_calls_per_invocation", 0)
        sa_invocations = sa.get("invocations_per_session", 0)
        sa_avg_in = sa.get("avg_input_tokens_per_call", 0)
        sa_avg_out = sa.get("avg_output_tokens_per_call", 0)
        sa_tokens = sa.get("tokens_per_invocation", 0)
        sa_total_in = sa_avg_in * sa_calls
        sa_total_out = sa_avg_out * sa_calls

        lines.append("")
        lines.append(f"### Capacity Profile Derivation — {sa_model} ({sa.get('type', 'sub-agent')})")
        lines.append("")
        lines.append("| Field | Formula | Value |")
        lines.append("|-------|---------|-------|")
        lines.append(f"| LLM calls per invocation | from token_result cycles | {sa_calls} |")
        lines.append(f"| Invocations per session | questions_invoked | {sa_invocations} |")
        lines.append(f"| Total input tokens/invocation | sum(cycle inputs) | {_fmt(sa_total_in)} |")
        lines.append(f"| Total output tokens/invocation | sum(cycle outputs) | {_fmt(sa_total_out)} |")
        lines.append(f"| Avg input tokens per call | total_input / calls | {_fmt(sa_total_in)} ÷ {sa_calls} = {_fmt(sa_avg_in)} |")
        lines.append(f"| Avg output tokens per call | total_output / calls | {_fmt(sa_total_out)} ÷ {sa_calls} = {_fmt(sa_avg_out)} |")
        lines.append(f"| Tokens per invocation | total_input + total_output | {_fmt(sa_total_in)} + {_fmt(sa_total_out)} = {_fmt(sa_tokens)} |")
        sa_monthly_invocations = sessions * sa_invocations
        lines.append(f"| Invocations per month | sessions × invocations_per_session | {_fmt(sessions)} × {sa_invocations} = {_fmt(sa_monthly_invocations)} |")

    return "\n".join(lines)


def _format_full_output(result, cache_status=None, models_found=None, all_tiers=None,
                        prices=None, tier_limits=None, cap_result=None):
    """Generate the complete formatted markdown report in the standard section order.

    Section order:
    1. Cost Summary (top)
    2. Capacity Summary — Capacity Fit Result only (top)
    3. Pricing Data Freshness
    4. Model Resolution
    5. Pricing (tiers table)
    6. Inputs & Assumptions
    7. Token Breakdown (Main Agent, Sub-Agents, Session Summary as sub-sections)
    8. Prompt Caching Strategy (Checkpoint Config, Break-Even, Cost Comparison)
    9. Capacity Detailed Calculations (Profile Derivation, Assumptions, RPM, TPM, TPD)

    Args:
        result (dict): Full output from calculate_agent_session_compounded_cost(detail_level="full").
        cache_status (dict|None): Output from check_pricing_data_status().
        models_found (list|None): List of model names found during resolution.
        all_tiers (dict|None): Output from extract_bedrock_model_prices(all_tiers=True).
        prices (dict|None): Selected prices dict.
        tier_limits (dict|None): Output from get_tier_limits_for_model().
        cap_result (dict|None): Output from check_capacity_fit().

    Returns:
        str: Complete markdown report.
    """
    r = result
    main = r["main_agent"]
    tr = main["token_result"]
    assumptions = tr["assumptions"]
    exp = main["explanation"]
    sessions_per_month = r["sessions_per_month"]
    savings_pct = (r["session_total_no_cache"] - r["session_total"]) / r["session_total_no_cache"] * 100 if r["session_total_no_cache"] > 0 else 0

    lines = []

    # ═══════════════════════════════════════════════════════════════
    # SECTION 1: Cost Summary
    # ═══════════════════════════════════════════════════════════════
    lines.append("## Cost Summary")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    lines.append(f"| **Cost per session (with caching)** | ${r['session_total']:.6f} |")
    lines.append(f"| **Cost per session (no caching)** | ${r['session_total_no_cache']:.6f} |")
    lines.append(f"| **Monthly total** | ${r['monthly_total']:,.2f} |")
    lines.append(f"| **Annual total** | ${r['annual_total']:,.2f} |")
    lines.append(f"| Sessions per month | {sessions_per_month:,} |")
    lines.append(f"| Savings from caching | {savings_pct:.1f}% |")
    lines.append(f"| Main agent session cost | ${main['with_cache']['session_total']:.6f} |")

    subagent_cost_results = r.get("subagents", [])
    if subagent_cost_results:
        subagent_total = sum(sa["session_cost"] for sa in subagent_cost_results)
        lines.append(f"| Sub-agent session cost | ${subagent_total:.6f} |")
        for sa in subagent_cost_results:
            cost_detail = sa.get("cost_detail", {})
            if cost_detail.get("caching_applied"):
                cache_note = f" (cached, saves {cost_detail['cache_savings_pct']:.0f}%)"
            else:
                cache_reason = cost_detail.get("explanation", {}).get("pricing", {}).get("caching", "")
                if "Not applied" in cache_reason:
                    reason_short = cache_reason.removeprefix("Not applied (")
                    if reason_short.endswith(")"):
                        reason_short = reason_short[:-1]
                    cache_note = f" (no cache: {reason_short})"
                else:
                    cache_note = ""
            lines.append(f"|   — {sa['type'].title()} ({sa['invocations_per_session']}x) | ${sa['session_cost']:.6f}{cache_note} |")

    lines.append(f"| Recommended TTL | {main.get('recommended_ttl', 'N/A')} |")
    lines.append("")

    # ═══════════════════════════════════════════════════════════════
    # SECTION 2: Capacity Summary (Fit Result only)
    # ═══════════════════════════════════════════════════════════════
    if cap_result and tier_limits:
        lines.append("---")
        lines.append("")
        lines.append("## Capacity Summary")
        lines.append("")
        lines.append("| Metric | Your Workload (Peak) | Quota Limit | Fits? | Utilization |")
        lines.append("|--------|:--------------------:|:-----------:|:-----:|:-----------:|")

        rpm_fit = "✅" if cap_result.get("rpm_fits") else "❌"
        tpm_fit = "✅" if cap_result.get("tpm_fits") else "❌"
        tpd_fit = "✅" if cap_result.get("tpd_fits") else "❌"

        lines.append(f"| RPM | {cap_result['peak_rpm']:.0f} | {tier_limits['rpm_high']:,.0f} | {rpm_fit} | {cap_result['rpm_utilization_pct']:.1f}% |")
        lines.append(f"| TPM (effective) | {cap_result['effective_peak_tpm']:,.0f} | {tier_limits['tpm_high']:,.0f} | {tpm_fit} | {cap_result['tpm_utilization_pct']:.1f}% |")
        lines.append(f"| TPD | {cap_result['estimated_tpd']:,.0f} | {tier_limits['tpd_high']:,.0f} | {tpd_fit} | {cap_result['tpd_utilization_pct']:.1f}% |")
        lines.append("")

        overall = "✅ Workload fits within all limits" if cap_result.get("fits") else "❌ Workload EXCEEDS limits"
        lines.append(f"**{overall}**")
        lines.append("")

    # ═══════════════════════════════════════════════════════════════
    # SECTION 3: Pricing Data Freshness
    # ═══════════════════════════════════════════════════════════════
    if cache_status:
        lines.append("---")
        lines.append("")
        lines.append("## Pricing Data Freshness")
        lines.append("")
        lines.append(f"**Status:** {cache_status['status']}")
        lines.append("")
        if cache_status.get("found"):
            lines.append("| File | Age (days) |")
            lines.append("|------|-----------|")
            for f in cache_status["found"]:
                lines.append(f"| {f['file']} | {f['age_days']} |")
            lines.append("")

    # ═══════════════════════════════════════════════════════════════
    # SECTION 4: Model Resolution
    # ═══════════════════════════════════════════════════════════════
    if models_found:
        lines.append("---")
        lines.append("")
        lines.append("## Model Resolution")
        lines.append("")
        model_name = r.get("capacity_profile", {}).get("main_agent", {}).get("model_name") or "Unknown"
        lines.append(f"Models found: {', '.join(models_found)}")
        lines.append("")
        lines.append(f"**Selected:** {model_name} (latest)")
        lines.append("")

    # ═══════════════════════════════════════════════════════════════
    # SECTION 5: Pricing
    # ═══════════════════════════════════════════════════════════════
    if all_tiers and prices:
        lines.append("---")
        lines.append("")
        lines.append("## Pricing")
        lines.append("")
        lines.append("| Tier | Input ($/M) | Output ($/M) | Cache Read ($/M) | Cache Write ($/M) |")
        lines.append("|------|-------------|--------------|-------------------|-------------------|")
        for tier_name, tier_prices in sorted(all_tiers.items()):
            cr = f"${tier_prices['cache_read']}" if tier_prices.get('cache_read') else "N/A"
            cw = f"${tier_prices['cache_write']}" if tier_prices.get('cache_write') else "N/A"
            lines.append(f"| {tier_name} | ${tier_prices['input']} | ${tier_prices['output']} | {cr} | {cw} |")
        lines.append("")
        lines.append(f"**Selected:** Input: ${prices['input']}/M, Output: ${prices['output']}/M, Cache Read: ${prices.get('cache_read', 'N/A')}/M, Cache Write: ${prices.get('cache_write', 'N/A')}/M")
        lines.append("")

    # ═══════════════════════════════════════════════════════════════
    # SECTION 6: Inputs & Assumptions
    # ═══════════════════════════════════════════════════════════════
    lines.append("---")
    lines.append("")
    lines.append("## Inputs & Assumptions")
    lines.append("")
    lines.append("### Main Agent")
    lines.append("")
    lines.append("| Parameter | Value |")
    lines.append("|-----------|-------|")
    lines.append(f"| Questions per session | {assumptions['questions_per_agent_session']} |")
    lines.append(f"| Input tokens per question | {assumptions['input_tokens']} |")
    lines.append(f"| Output tokens per question | {assumptions['output_tokens']} |")
    lines.append(f"| System prompt tokens | {_fmt(assumptions['system_prompt_tokens'])} |")
    lines.append(f"| Tools passed to agent | {assumptions['tools_passed_to_agent']} |")
    lines.append(f"| Tool spec tokens | {assumptions['tool_spec_tokens']} |")
    lines.append(f"| Tools invoked per question | {assumptions['tools_invoked']} |")
    lines.append(f"| Tool call tokens | {assumptions['tool_call_tokens']} |")
    lines.append(f"| Tool result tokens | {assumptions['tool_result_tokens']} |")
    lines.append(f"| History mode | {assumptions['history_mode']} |")
    lines.append("")

    # Sub-agent assumptions
    for sa in subagent_cost_results:
        sa_assumptions = sa.get("token_result", {}).get("assumptions", {})
        sa_type = sa["type"]
        lines.append(f"### {sa_type.title()} Sub-Agent")
        lines.append("")
        lines.append("| Parameter | Value |")
        lines.append("|-----------|-------|")
        lines.append(f"| Questions invoked | {sa['questions_invoked']} |")
        lines.append(f"| Invocations per session | {sa['invocations_per_session']} |")
        for k, v in sa_assumptions.items():
            if k not in ("tool_desc_tokens", "fixed_per_call", "total_tool_calls", "total_cycles", "retrieval_result_tokens"):
                lines.append(f"| {k} | {v} |")
        lines.append("")

    # ═══════════════════════════════════════════════════════════════
    # SECTION 7: Token Breakdown (with Session Summary as sub-section)
    # ═══════════════════════════════════════════════════════════════
    lines.append("---")
    lines.append("")
    lines.append("## Token Breakdown")
    lines.append("")
    lines.append(r.get("token_table", ""))
    lines.append("")

    # ═══════════════════════════════════════════════════════════════
    # SECTION 8: Prompt Caching Strategy
    # ═══════════════════════════════════════════════════════════════
    lines.append("---")
    lines.append("")
    lines.append("## Prompt Caching Strategy")
    lines.append("")

    # Checkpoint Configuration
    lines.append("### Checkpoint Configuration")
    lines.append("")
    lines.append(f"- **Checkpoint 1:** {exp['caching_strategy']['checkpoint_1']}")
    lines.append(f"- **History checkpoints:** {exp['caching_strategy']['history_checkpoints']}")
    lines.append(f"- **Note:** {exp['caching_strategy']['max_checkpoints_note']}")
    lines.append("")

    # Checkpoint Break-Even Analysis
    lines.append("### Checkpoint Break-Even Analysis")
    lines.append("")
    lines.append("| Checkpoint After | Tokens Cached | Write Cost | Subsequent Reads | Total Savings | Net Benefit | Worth It? |")
    lines.append("|:----------------:|:-------------:|:----------:|:----------------:|:-------------:|:-----------:|:---------:|")
    for cp in exp.get("checkpoint_analysis", []):
        worth = "✅ Yes" if cp["worth_it"] else "❌ No"
        lines.append(f"| Q{cp['checkpoint_after_question']} | {cp['tokens_cached']:,} | ${cp['write_cost']:.6f} | {cp['subsequent_reads']} | ${cp['total_savings']:.6f} | ${cp['net_benefit']:.6f} | {worth} |")
    lines.append("")

    # Cost Comparison
    lines.append("### Cost Comparison")
    lines.append("")
    lines.append("| Metric | With Cache | Without Cache |")
    lines.append("|--------|-----------|---------------|")
    lines.append(f"| Per session | ${main['with_cache']['session_total']:.6f} | ${main['no_cache']['session_total']:.6f} |")
    lines.append(f"| Monthly | ${main['with_cache']['monthly_total']:,.2f} | ${main['no_cache']['monthly_total']:,.2f} |")
    lines.append(f"| Annual | ${main['with_cache']['annual_total']:,.2f} | ${main['no_cache']['annual_total']:,.2f} |")
    savings_monthly = main["no_cache"]["monthly_total"] - main["with_cache"]["monthly_total"]
    lines.append(f"| **Savings** | **${savings_monthly:,.2f}/month ({savings_pct:.1f}%)** | — |")
    lines.append("")

    # TTL Recommendation
    lines.append("### TTL Recommendation")
    lines.append("")
    lines.append("| Parameter | Value |")
    lines.append("|-----------|-------|")
    lines.append(f"| Recommended TTL | {exp['ttl_recommendation']['recommended']} |")
    lines.append(f"| Sessions per hour | {exp['ttl_recommendation']['sessions_per_hour']} |")
    lines.append(f"| Avg gap between sessions | {exp['ttl_recommendation']['avg_gap_minutes']} min |")
    lines.append(f"| Reason | {exp['ttl_recommendation']['reason']} |")
    lines.append("")

    # ═══════════════════════════════════════════════════════════════
    # SECTION 9: Capacity Detailed Calculations
    # ═══════════════════════════════════════════════════════════════
    if cap_result and tier_limits:
        lines.append("---")
        lines.append("")
        lines.append("## Capacity Detailed Calculations")
        lines.append("")

        # Capacity Profile Derivation Table
        cap_profile = r.get("capacity_profile", {})
        lines.append(_format_capacity_profile_table(cap_profile))
        lines.append("")

        # Tier Limits
        lines.append("### Tier Limits")
        lines.append("")
        lines.append("| Metric | Limit |")
        lines.append("|--------|-------|")
        lines.append(f"| RPM | {tier_limits['rpm_high']:,.0f} |")
        lines.append(f"| TPM | {tier_limits['tpm_high']:,.0f} |")
        lines.append(f"| TPD | {tier_limits['tpd_high']:,.0f} |")
        lines.append("")

        # Assumptions
        lines.append("### Assumptions")
        lines.append("")
        lines.append("| Parameter | Value |")
        lines.append("|-----------|-------|")
        for k, v in cap_result.get("assumptions", {}).items():
            label = k.replace("_", " ").title()
            lines.append(f"| {label} | {v} |")
        lines.append("")

        # RPM
        lines.append("### RPM Calculation")
        lines.append("")
        lines.append("| Step | Value |")
        lines.append("|------|-------|")
        for k, v in cap_result.get("explanation", {}).get("rpm_calculation", {}).items():
            lines.append(f"| {k.replace('_', ' ').title()} | {v} |")
        lines.append("")

        # TPM
        lines.append("### TPM Calculation")
        lines.append("")
        lines.append("| Step | Value |")
        lines.append("|------|-------|")
        for k, v in cap_result.get("explanation", {}).get("tpm_calculation", {}).items():
            lines.append(f"| {k.replace('_', ' ').title()} | {v} |")
        lines.append("")

        # TPD
        lines.append("### TPD Calculation")
        lines.append("")
        lines.append("| Step | Value |")
        lines.append("|------|-------|")
        for k, v in cap_result.get("explanation", {}).get("tpd_calculation", {}).items():
            lines.append(f"| {k.replace('_', ' ').title()} | {v} |")
        lines.append("")

    return "\n".join(lines)


def _generate_model_markdown(results):
    if not results:
        return "No pricing data found matching the specified filters.\n"
    tree = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list))))
    for r in results:
        key = r["tier"]
        if r.get("variant", "Standard") != "Standard":
            key = f"{r['tier']} ({r['variant']})"
        tree[r["region"]][r["provider"]][r["model"]][key].extend(r["dimensions"])
    lines = ["## Bedrock Model Pricing\n"]
    region_count = len(tree)
    model_set = set()
    for rd in tree.values():
        for pd in rd.values():
            for mn in pd:
                model_set.add(mn)
    lines.append(f"**{len(model_set)} models** across **{region_count} regions**\n")
    for region in sorted(tree.keys()):
        lines.append(f"\n### Region: `{region}`\n")
        for provider in sorted(tree[region].keys()):
            lines.append(f"\n#### {provider}\n")
            for model in sorted(tree[region][provider].keys()):
                lines.append(f"\n**{model}**\n")
                lines.append("| Tier | Description | Unit | Price |")
                lines.append("|------|-------------|------|-------|")
                for tier in sorted(tree[region][provider][model].keys()):
                    for d in tree[region][provider][model][tier]:
                        desc = d["description"][:80] if d["description"] else "-"
                        lines.append(f"| {tier} | {desc} | {d['unit']} | {_format_price(d['price_usd'])} |")
    return "\n".join(lines)


def _generate_agentcore_markdown(results):
    if not results:
        return ""
    tree = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for r in results:
        tree[r["region"]][r["component"]][r["sub_component"]].extend(r["dimensions"])
    lines = ["\n## AgentCore Pricing\n"]
    for region in sorted(tree.keys()):
        lines.append(f"\n### Region: `{region}`\n")
        for component in sorted(tree[region].keys()):
            lines.append(f"\n#### {component}\n")
            lines.append("| Resource | Description | Unit | Price |")
            lines.append("|----------|-------------|------|-------|")
            for sub in sorted(tree[region][component].keys()):
                for d in tree[region][component][sub]:
                    desc = d["description"][:80] if d["description"] else "-"
                    lines.append(f"| {sub} | {desc} | {d['unit']} | {_format_price(d['price_usd'])} |")
    return "\n".join(lines)


def _generate_model_index(cache_dir):
    """Generate the model index file from existing pricing cache.

    Reads all pricing JSON files and builds a lookup:
        {region: {family: [model_name, ...]}}

    Called automatically after refresh_cache() completes.
    """
    models_by_region = defaultdict(set)

    service_codes_to_scan = ["AmazonBedrock", "AmazonBedrockService", "AmazonBedrockFoundationModels"]
    for sc in service_codes_to_scan:
        filename = CACHE_FILES.get(sc, "")
        if not filename:
            continue
        filepath = os.path.join(cache_dir, filename)
        if not os.path.exists(filepath):
            continue
        try:
            with open(filepath, "r") as f:
                data = json.load(f)
        except Exception:
            continue

        is_3p = (sc == "AmazonBedrockFoundationModels")
        for raw in data.get("PriceList", []):
            item = json.loads(raw) if isinstance(raw, str) else raw
            attrs = item.get("product", {}).get("attributes", {})
            region = attrs.get("regionCode", "")
            if not region:
                continue
            if is_3p:
                model = attrs.get("servicename", "").replace(" (Amazon Bedrock Edition)", "")
            else:
                model = attrs.get("model", "")
            if model:
                models_by_region[region].add(model)

    # Build {region: {family: [sorted model names]}}
    index = {}
    for region in sorted(models_by_region.keys()):
        families = defaultdict(list)
        for model in sorted(models_by_region[region]):
            family = _classify_model_family(model)
            families[family].append(model)
        index[region] = dict(sorted(families.items()))

    index_path = os.path.join(cache_dir, MODEL_INDEX_FILE)
    with open(index_path, "w") as f:
        json.dump(index, f, indent=2)
    model_count = sum(len(m) for fams in index.values() for m in fams.values())
    print(f"  Generated model index: {len(index)} regions, {model_count} entries → {index_path}", file=sys.stderr)


def refresh_cache(output_dir):
    try:
        import boto3
    except ImportError:
        print("ERROR: boto3 is required for refresh mode. Install with: pip install boto3", file=sys.stderr)
        sys.exit(1)
    os.makedirs(output_dir, exist_ok=True)
    client = boto3.client("pricing", region_name="us-east-1")
    for sc, filename in CACHE_FILES.items():
        filepath = os.path.join(output_dir, filename)
        print(f"Fetching {sc}...", file=sys.stderr)
        all_items = []
        paginator = client.get_paginator("get_products")
        for page in paginator.paginate(ServiceCode=sc):
            all_items.extend(page["PriceList"])
        data = {"PriceList": all_items, "FormatVersion": "aws_v1"}
        with open(filepath, "w") as f:
            json.dump(data, f)
        print(f"  Saved {len(all_items)} entries to {filepath}", file=sys.stderr)
    _generate_model_index(output_dir)
    print("\nCache refresh complete!", file=sys.stderr)


def refresh_quotas(output_dir, regions=None):
    """Fetch default RPM/TPM quotas for Bedrock models using Service Quotas API.

    Uses list-service-quotas (not get-aws-default-service-quota) to get all quotas,
    then filters to RPM/TPM entries. Results are universal defaults regardless of
    which AWS account calls the API.

    Args:
        output_dir: Directory to save bedrock_quotas.json
        regions: List of region codes to query. Default: major Bedrock regions.
    """
    try:
        import boto3
    except ImportError:
        print("ERROR: boto3 is required for quota refresh. Install with: pip install boto3", file=sys.stderr)
        return

    os.makedirs(output_dir, exist_ok=True)

    if regions is None:
        regions = [
            "us-east-1",
            "us-west-2",
            "eu-west-1",
            "eu-central-1",
            "ap-northeast-1",
            "ap-southeast-1",
            "ap-southeast-2",
            "ap-south-1",
            "ca-central-1",
            "sa-east-1",
        ]  # Major Bedrock regions; override with --quota-regions for others

    # Keywords to filter relevant quota names
    RPM_KEYWORDS = ["requests per minute", "invocations per minute"]
    TPM_KEYWORDS = ["tokens per minute"]
    TPD_KEYWORDS = ["tokens per day"]

    all_quotas = []
    errors = []

    for region in regions:
        print(f"Fetching quotas for {region}...", file=sys.stderr)
        try:
            client = boto3.client("service-quotas", region_name=region)
            region_quotas = []
            region_count = 0
            max_attempts = 2

            for attempt in range(1, max_attempts + 1):
                region_quotas = []
                region_count = 0

                try:
                    paginator = client.get_paginator("list_service_quotas")
                    for page in paginator.paginate(ServiceCode="bedrock"):
                        for quota in page.get("Quotas", []):
                            name = quota.get("QuotaName", "")
                            name_lower = name.lower()

                            # Determine quota type
                            quota_type = None
                            if any(kw in name_lower for kw in RPM_KEYWORDS):
                                quota_type = "RPM"
                            elif any(kw in name_lower for kw in TPM_KEYWORDS):
                                quota_type = "TPM"
                            elif any(kw in name_lower for kw in TPD_KEYWORDS):
                                quota_type = "TPD"

                            if quota_type is None:
                                continue

                            # Determine inference type from name
                            inference_type = "On-demand"
                            if "global cross-region" in name_lower:
                                inference_type = "Global"
                            elif "cross-region" in name_lower:
                                inference_type = "Cross-region"
                            elif "latency-optimized" in name_lower:
                                inference_type = "Latency-optimized"
                            elif "custom model" in name_lower or "model customization" in name_lower:
                                inference_type = "Custom-model"

                            entry = {
                                "region": region,
                                "quota_name": name,
                                "quota_code": quota.get("QuotaCode", ""),
                                "value": quota.get("Value", 0),
                                "unit": quota.get("Unit", ""),
                                "adjustable": quota.get("Adjustable", False),
                                "quota_type": quota_type,
                                "inference_type": inference_type,
                                "service_code": quota.get("ServiceCode", "bedrock"),
                            }
                            region_quotas.append(entry)
                            region_count += 1
                except client.exceptions.InvalidPaginationTokenException:
                    if attempt < max_attempts:
                        print(f"  Pagination token error, retrying {region} in 2s...", file=sys.stderr)
                        time.sleep(2)
                        continue
                    else:
                        # Keep partial data from this final attempt
                        err_msg = f"Pagination error in {region} after {max_attempts} attempts (partial data kept: {region_count} quotas)"
                        print(f"  WARNING: {err_msg}", file=sys.stderr)
                        errors.append(err_msg)

                # Success or final attempt — exit retry loop
                break

            all_quotas.extend(region_quotas)
            print(f"  Found {region_count} RPM/TPM/TPD quotas in {region}", file=sys.stderr)

        except Exception as e:
            err_msg = f"Error fetching quotas for {region}: {str(e)}"
            print(f"  WARNING: {err_msg}", file=sys.stderr)
            errors.append(err_msg)
            continue

    if not all_quotas:
        print("WARNING: No quotas found. Check AWS credentials and region availability.", file=sys.stderr)
        return

    # Save to cache
    filepath = os.path.join(output_dir, QUOTAS_CACHE_FILE)
    import time
    cache_data = {
        "quotas": all_quotas,
        "regions_queried": regions,
        "total_entries": len(all_quotas),
        "refresh_timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "errors": errors,
    }
    with open(filepath, "w") as f:
        json.dump(cache_data, f, indent=2, default=str)
    print(f"  Saved {len(all_quotas)} quota entries to {filepath}", file=sys.stderr)
    if errors:
        print(f"  ({len(errors)} region(s) had errors — see file for details)", file=sys.stderr)


def query_quotas(cache_dir, region_filter=None, model_filter=None, quota_type_filter=None,
                 inference_type_filter=None):
    """Query cached RPM/TPM/TPD quota data with optional filters.

    Args (required):
        cache_dir (str): Directory containing bedrock_quotas.json.
    Args (optional):
        region_filter (str): AWS region code (e.g. "us-west-2"). Default None.
        model_filter (str): Fuzzy model name match (e.g. "Claude Sonnet 4.6"). Default None.
        quota_type_filter (str): "RPM", "TPM", or "TPD". Default None.

    Example:
        query_quotas("~/bedrock_cache", model_filter="Claude Sonnet 4.6", quota_type_filter="RPM")

    Returns: list of quota dicts, or empty list if cache file not found.

    --- Detailed Documentation ---

    Works in sandbox (no boto3 needed).

    Args:
        cache_dir: Directory containing bedrock_quotas.json
        region_filter: Filter by region code (e.g. 'us-west-2')
        model_filter: Fuzzy match against quota_name (e.g. 'Claude Sonnet 4.6', 'Nova')
        quota_type_filter: Filter by type: 'RPM', 'TPM', or 'TPD'
        inference_type_filter: Filter by inference type: 'On-demand', 'Cross-region', 'Global'

    Returns:
        List of quota dicts, or empty list if cache file not found.
    """
    filepath = os.path.join(cache_dir, QUOTAS_CACHE_FILE)
    if not os.path.exists(filepath):
        return []

    try:
        with open(filepath, "r") as f:
            data = json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        print(f"Warning: Failed to load {filepath}: {e}", file=sys.stderr)
        return []

    results = data.get("quotas", [])

    if region_filter:
        results = [q for q in results if q.get("region") == region_filter]
    if model_filter:
        results = [q for q in results if _fuzzy_match(model_filter, q.get("quota_name", ""))]
    if quota_type_filter:
        results = [q for q in results if q.get("quota_type") == quota_type_filter]
    if inference_type_filter:
        results = [q for q in results if q.get("inference_type") == inference_type_filter]

    return results


def main():
    parser = argparse.ArgumentParser(description="Fetch Bedrock & AgentCore pricing")
    parser.add_argument("--refresh", action="store_true", help="Refresh cache from AWS Pricing API")
    parser.add_argument("--init-config", action="store_true", help="Generate commented config template at ~/.bedrock_skills/config.yaml")
    parser.add_argument("--force", action="store_true", help="Overwrite existing config file without prompting (use with --init-config)")
    parser.add_argument("--cleanup-reports", action="store_true", help="Delete report files older than retention_days (default 30)")
    parser.add_argument("--cache-dir", type=str, default=os.path.expanduser("~/bedrock_cache"), help="Dir to read cache files")
    parser.add_argument("--quota-regions", type=str,
                        default="us-east-1,us-west-2,eu-west-1,eu-central-1,ap-northeast-1,ap-southeast-1,ap-southeast-2,ap-south-1,ca-central-1,sa-east-1",
                        help="Comma-separated regions for quota refresh (default: 10 major Bedrock regions)")
    parser.add_argument("--all-regions", action="store_true", default=False, help="Fetch quotas for ALL 35 Bedrock regions (slow)")
    parser.add_argument("--region", type=str, default=None, help="AWS region code")
    parser.add_argument("--provider", type=str, default=None, help="Provider/model family")
    parser.add_argument("--model", type=str, default=None, help="Model name")
    parser.add_argument("--include-agentcore", action="store_true", default=False, help="Include AgentCore")
    parser.add_argument("--json", action="store_true", default=False, help="Output raw JSON")
    parser.add_argument("--skip-quotas", action="store_true", default=False, help="Skip quota refresh (pricing only)")
    args = parser.parse_args()
    cache_dir = os.path.expanduser("~/bedrock_cache")
    if args.init_config:
        generate_config_template(force=args.force)
        return
    if args.cleanup_reports:
        cleanup_result = _cleanup_old_reports()
        print(f"Deleted {cleanup_result['deleted_count']} report(s), freed {cleanup_result['freed_bytes'] / 1024:.1f} KB")
        return
    if args.refresh:
        refresh_cache(cache_dir)
        if not args.skip_quotas:
            if args.all_regions:
                # Fetch quotas for all known Bedrock regions
                quota_regions = [
                    "af-south-1", "ap-east-2", "ap-northeast-1", "ap-northeast-2", "ap-northeast-3",
                    "ap-south-1", "ap-south-2", "ap-southeast-1", "ap-southeast-2", "ap-southeast-3",
                    "ap-southeast-4", "ap-southeast-5", "ap-southeast-6", "ap-southeast-7",
                    "ca-central-1", "ca-west-1", "eu-central-1", "eu-central-2", "eu-north-1",
                    "eu-south-1", "eu-south-2", "eu-west-1", "eu-west-2", "eu-west-3",
                    "il-central-1", "me-central-1", "me-south-1", "mx-central-1",
                    "sa-east-1", "us-east-1", "us-east-2", "us-west-1", "us-west-2",
                ]
                print(f"Fetching quotas for ALL {len(quota_regions)} regions...", file=sys.stderr)
            else:
                quota_regions = [r.strip() for r in args.quota_regions.split(",") if r.strip()]
            refresh_quotas(cache_dir, regions=quota_regions)
        return
    if not args.region:
        if args.model or args.provider or args.include_agentcore:
            parser.error("--region is required for queries (e.g., --region us-west-2)")
        return
    model_results = query_model_pricing(args.cache_dir, args.region, args.provider, args.model)
    agentcore_results = []
    if args.include_agentcore:
        agentcore_results = query_agentcore_pricing(args.cache_dir, args.region)
    if args.json:
        output = {"model_pricing": model_results}
        if agentcore_results:
            output["agentcore_pricing"] = agentcore_results
        print(json.dumps(output, indent=2))
    else:
        md = _generate_model_markdown(model_results)
        if args.include_agentcore:
            md += _generate_agentcore_markdown(agentcore_results)
        print(md)
    print(f"\nTotal model price entries: {len(model_results)}", file=sys.stderr)
    if agentcore_results:
        print(f"Total AgentCore price entries: {len(agentcore_results)}", file=sys.stderr)


def calculate_evaluation_cost(
    questions_per_month,
    sessions_per_month,
    # Sampling
    sampling_rate=None,                    # 10% of sessions evaluated (from config)
    # Built-in evaluators (fixed-price LLM judge, model chosen by AWS)
    num_builtin_evaluators=None,           # e.g. Helpfulness, Correctness, Safety (from config)
    builtin_input_price=2.40,              # per 1M tokens (from AC cache)
    builtin_output_price=12.00,            # per 1M tokens (from AC cache)
    # Custom LLM-as-a-Judge evaluators (you pick the model)
    num_custom_llm_evaluators=0,
    custom_eval_invocation_price=0.0015,   # per evaluation (from AC cache)
    custom_judge_model_input_price=0.0,    # per 1M tokens (the judge model's own Bedrock price)
    custom_judge_model_output_price=0.0,   # per 1M tokens
    # Custom code-based evaluators (Lambda)
    num_code_evaluators=0,
    code_eval_invocation_price=0.0015,     # per evaluation (from AC cache)
    # Trace size — what the judge sees per question
    agent_input_tokens_per_q=100,
    agent_output_tokens_per_q=100,
    system_prompt_tokens=1000,
    tools_passed_to_agent=10,
    tool_spec_tokens=100,
    tools_invoked=5,
    tool_call_tokens=100,
    tool_result_tokens=100,
    rag_chunks=10,
    rag_tokens_per_chunk=300,
    # Judge output
    judge_output_tokens_per_eval=300,      # score + reasoning per evaluator
    questions_per_session=10,
    # Report output directory (optional)
    output_dir=None,
):
    """Calculate AgentCore Evaluations cost (built-in + custom LLM-as-a-Judge).

    Args (required):
        questions_per_month (int): Total questions per month.
        sessions_per_month (int): Total sessions per month.
    Args (optional):
        sampling_rate (float): Fraction of sessions evaluated. Default 0.10.
        num_builtin_evaluators (int): Built-in evaluators per question. Default 3.
        num_custom_llm_evaluators (int): Custom LLM evaluators. Default 0.

    Example:
        calculate_evaluation_cost(1_000_000, 200_000)

    Returns: dict with total_monthly, total_annual, builtin/custom breakdowns, and explanation.

    --- Detailed Documentation ---

    Args:
        questions_per_month (int): Total questions/month.
        sessions_per_month (int): Total sessions/month.
        sampling_rate (float): Fraction of sessions evaluated (default 0.10).
        num_builtin_evaluators (int): Built-in evaluators/question (default 3).
        builtin_input_price (float): Judge input $/M tokens (default 2.40).
        builtin_output_price (float): Judge output $/M tokens (default 12.00).
        num_custom_llm_evaluators (int): Custom LLM evaluators (default 0).
        num_code_evaluators (int): Code evaluators (default 0).
        agent_input/output_tokens_per_q, system_prompt_tokens,
            tools_passed_to_agent, tool_spec_tokens,
            tools_invoked, tool_call/result_tokens, rag_chunks/tokens_per_chunk:
            Agent trace profile for judge input sizing.
        judge_output_tokens_per_eval (int): Judge output/eval (default 300).

    Returns:
        dict: evaluated_sessions, evaluated_questions, trace_tokens_per_q,
        builtin/custom_llm/custom_code (dicts with cost breakdowns),
        total_monthly, total_annual, warnings (list[str]),
        explanation (dict): sampling, trace_size, builtin_evaluators,
            custom_llm_evaluators, custom_code_evaluators, grand_total.
    """
    # Input validation
    if tools_invoked > tools_passed_to_agent:
        raise ValueError(f"tools_invoked ({tools_invoked}) cannot exceed tools_passed_to_agent ({tools_passed_to_agent})")

    # Resolve defaults from config (explicit values passed by caller always win)
    sampling_rate = _resolve_setting("agentcore_defaults", "eval_sampling_rate", sampling_rate)
    num_builtin_evaluators = _resolve_setting("agentcore_defaults", "eval_builtin_evaluators", num_builtin_evaluators)

    # Derive evaluated volume
    evaluated_sessions = sessions_per_month * sampling_rate
    evaluated_questions = evaluated_sessions * questions_per_session

    # Trace size per question (what the judge sees)
    rag_tokens = rag_chunks * rag_tokens_per_chunk
    tool_desc_tokens = tools_passed_to_agent * tool_spec_tokens
    tool_trace_tokens = tools_invoked * (tool_call_tokens + tool_result_tokens)
    trace_tokens_per_q = (
        system_prompt_tokens
        + tool_desc_tokens
        + agent_input_tokens_per_q
        + rag_tokens
        + tool_trace_tokens
        + agent_output_tokens_per_q
    )

    # ── Built-in evaluators ──
    builtin_judge_calls = evaluated_questions * num_builtin_evaluators
    builtin_input_cost = builtin_judge_calls * (trace_tokens_per_q / 1_000_000) * builtin_input_price
    builtin_output_cost = builtin_judge_calls * (judge_output_tokens_per_eval / 1_000_000) * builtin_output_price
    builtin_total = builtin_input_cost + builtin_output_cost

    # ── Custom LLM-as-a-Judge evaluators ──
    custom_llm_judge_calls = evaluated_questions * num_custom_llm_evaluators
    custom_llm_invocation_cost = custom_llm_judge_calls * custom_eval_invocation_price
    custom_llm_input_cost = custom_llm_judge_calls * (trace_tokens_per_q / 1_000_000) * custom_judge_model_input_price
    custom_llm_output_cost = custom_llm_judge_calls * (judge_output_tokens_per_eval / 1_000_000) * custom_judge_model_output_price
    custom_llm_total = custom_llm_invocation_cost + custom_llm_input_cost + custom_llm_output_cost

    # ── Custom code-based evaluators ──
    code_eval_calls = evaluated_questions * num_code_evaluators
    code_eval_total = code_eval_calls * code_eval_invocation_price

    total_monthly = builtin_total + custom_llm_total + code_eval_total

    # Warnings
    warnings = []
    if custom_judge_model_input_price > 0 and custom_judge_model_input_price == builtin_input_price:
        warnings.append(
            "Custom judge model price matches the agent's inference model price. "
            "If using the same model for both agent inference and evaluation judging, "
            "consider a cheaper model (e.g. Nova Lite) as the judge to reduce costs."
        )

    # ── Build step-by-step explanation ──
    explanation = {
        "sampling": {
            "total_sessions": _fmt(sessions_per_month),
            "sampling_rate": f"{sampling_rate*100:.0f}%",
            "evaluated_sessions": _fmt(evaluated_sessions),
            "questions_per_session": _fmt(questions_per_session),
            "evaluated_questions": _fmt(evaluated_questions),
        },
        "trace_size": {
            "components": f"{_fmt(system_prompt_tokens)} (sys) + {_fmt(tool_desc_tokens)} (tools) + {_fmt(agent_input_tokens_per_q)} (input) + {_fmt(rag_tokens)} (RAG) + {_fmt(tool_trace_tokens)} (tool traces) + {_fmt(agent_output_tokens_per_q)} (output)",
            "total_per_question": f"{_fmt(trace_tokens_per_q)} tokens",
        },
        "builtin_evaluators": {
            "count": _fmt(num_builtin_evaluators),
            "judge_calls": f"{_fmt(evaluated_questions)} questions × {num_builtin_evaluators} evaluators = {_fmt(builtin_judge_calls)}",
            "input_cost": f"{_fmt(builtin_judge_calls)} calls × {_fmt(trace_tokens_per_q)} tokens × ${builtin_input_price}/M = ${builtin_input_cost:,.2f}",
            "output_cost": f"{_fmt(builtin_judge_calls)} calls × {_fmt(judge_output_tokens_per_eval)} tokens × ${builtin_output_price}/M = ${builtin_output_cost:,.2f}",
            "total": f"${builtin_total:,.2f}",
        },
        "custom_llm_evaluators": {
            "count": _fmt(num_custom_llm_evaluators),
            "total": f"${custom_llm_total:,.2f}",
        },
        "custom_code_evaluators": {
            "count": _fmt(num_code_evaluators),
            "total": f"${code_eval_total:,.2f}",
        },
        "grand_total": f"${builtin_total:,.2f} + ${custom_llm_total:,.2f} + ${code_eval_total:,.2f} = ${total_monthly:,.2f}/mo",
    }

    result = {
        "evaluated_sessions": evaluated_sessions,
        "evaluated_questions": evaluated_questions,
        "trace_tokens_per_q": trace_tokens_per_q,
        "sampling_rate": sampling_rate,
        "builtin": {
            "judge_calls": builtin_judge_calls,
            "input_cost": builtin_input_cost,
            "output_cost": builtin_output_cost,
            "total": builtin_total,
        },
        "custom_llm": {
            "judge_calls": custom_llm_judge_calls,
            "invocation_cost": custom_llm_invocation_cost,
            "model_input_cost": custom_llm_input_cost,
            "model_output_cost": custom_llm_output_cost,
            "total": custom_llm_total,
        },
        "custom_code": {
            "eval_calls": code_eval_calls,
            "total": code_eval_total,
        },
        "total_monthly": total_monthly,
        "total_annual": total_monthly * 12,
        "warnings": warnings,
        "explanation": explanation,
    }

    # ── Write report to file and return compact summary ──
    if output_dir is not None:
        file_path = os.path.join(output_dir, _EVAL_REPORT_FILENAME)
    else:
        file_path = _generate_typed_report_path("eval", questions_per_month)

    inputs_dict = {"questions_per_month": questions_per_month, "sessions_per_month": sessions_per_month,
                   "sampling_rate": sampling_rate, "num_builtin_evaluators": num_builtin_evaluators}
    content = _build_typed_front_matter("evaluations", result, inputs_dict) + _format_evaluation_report(result)
    written_path = _try_write(file_path, content)

    # Cascade: if session dir failed, try flat file fallback
    if written_path is None and output_dir is not None:
        print(
            f"⚠️  Report: Could not write to session directory '{output_dir}', trying default location.",
            file=sys.stderr
        )
        fallback_path = _generate_typed_report_path("eval", questions_per_month)
        written_path = _try_write(fallback_path, content)

    if written_path is None:
        print(
            "⚠️  Report: Failed to write evaluations report file. Returning full result inline.\n"
            "    This increases token usage and latency. Specify a writable folder via\n"
            "    reports.output_dir in ~/.bedrock_skills/config.yaml or pass output_dir='/writable/path/'.",
            file=sys.stderr
        )
        result["_file_write_failed"] = True
        return result

    # Auto-cleanup if configured
    if _resolve_setting("reports", "auto_cleanup"):
        _cleanup_old_reports()

    return _build_evaluation_summary(result, written_path)


def build_capacity_profile_from_tokens(token_result, sessions_per_month, model_name=None, sub_agents=None):
    """Build a capacity_profile dict from token calculation output for capacity checks.

    Args (required):
        token_result (dict): Output from calculate_compounded_tokens_for_agent(detail_level="full").
        sessions_per_month (int): Monthly session volume.
    Args (optional):
        model_name (str|None): Model name for quota lookup. Default None.
        sub_agents (list|None): Sub-agent profiles list. Default None.

    Example:
        build_capacity_profile_from_tokens(token_result, 10000, model_name="Claude Sonnet 4.6")

    Returns: dict with sessions_per_month, main_agent, sub_agents keys for check_capacity_fit().

    --- Detailed Documentation ---

    Use this when computing capacity directly (without going through the cost function).
    The cost function already outputs capacity_profile — use that instead if available.

    Args:
        token_result (dict): Output from calculate_compounded_tokens_for_agent(detail_level="full").
        sessions_per_month (int): Monthly session volume. Embedded in the profile
            so that aggregate_capacity_by_model() can derive questions_per_month
            automatically.
        model_name (str|None): Model name for quota lookup (e.g., "Claude Sonnet 4.6").
        sub_agents (list|None): Optional list of sub-agent profiles. Each dict:
            "type" (str): "rag" or "research".
            "model_name" (str): Sub-agent model name.
            "token_result" (dict): Output from calculate_rag_subagent_tokens() or
                calculate_research_subagent_tokens() with detail_level="full".
            "invocations_per_session" (int): How many times invoked per session.

    Returns:
        dict: capacity_profile with "sessions_per_month", "main_agent", and
        "sub_agents" keys, ready for check_capacity_fit() or
        aggregate_capacity_by_model().
    """
    session_data = token_result.get("session")
    if session_data is None:
        raise ValueError(
            "token_result must contain 'session' key (per-question cycle data). "
            "This requires calling calculate_compounded_tokens_for_agent(detail_level='full'). "
            "The default detail_level='summary' omits the session data needed here."
        )
    assumptions = token_result.get("assumptions")
    if assumptions is None or "cycles_per_question" not in assumptions:
        raise ValueError(
            "token_result must contain 'assumptions' with 'cycles_per_question'. "
            "Ensure calculate_compounded_tokens_for_agent(detail_level='full') was used."
        )
    questions_per_session = len(session_data)
    cycles_per_question = assumptions["cycles_per_question"]
    total_calls = questions_per_session * cycles_per_question
    total_input = sum(q["question_total_input"] for q in session_data)
    total_output = sum(q["question_total_output"] for q in session_data)

    profile = {
        "sessions_per_month": sessions_per_month,
        "main_agent": {
            "model_name": model_name,
            "llm_calls_per_question": cycles_per_question,
            "avg_input_tokens_per_call": total_input / total_calls if total_calls > 0 else 0,
            "avg_output_tokens_per_call": total_output / total_calls if total_calls > 0 else 0,
            "tokens_per_question": (total_input + total_output) / questions_per_session if questions_per_session > 0 else 0,
            "questions_per_session": questions_per_session,
        },
        "sub_agents": [],
    }

    if sub_agents:
        for sa in sub_agents:
            sa_token_result = sa["token_result"]
            sa_cycles = sa_token_result.get("cycles", [])
            llm_calls = len(sa_cycles) if sa_cycles else 1
            sa_total_input = sa_token_result.get("total_input", sa_token_result.get("total_input_tokens", 0))
            sa_total_output = sa_token_result.get("total_output", sa_token_result.get("total_output_tokens", 0))

            profile["sub_agents"].append({
                "type": sa.get("type", "unknown"),
                "model_name": sa.get("model_name"),
                "llm_calls_per_invocation": llm_calls,
                "invocations_per_session": sa.get("invocations_per_session", 1),
                "avg_input_tokens_per_call": sa_total_input / llm_calls if llm_calls > 0 else 0,
                "avg_output_tokens_per_call": sa_total_output / llm_calls if llm_calls > 0 else 0,
                "tokens_per_invocation": sa_total_input + sa_total_output,
            })

    return profile


def get_tier_limits_for_model(cache_dir, model_name, region):
    """Look up the highest RPM/TPM/TPD quota limits for a model in a given region.

    Args (required):
        cache_dir (str): Path to cache directory (e.g., ~/bedrock_cache).
        model_name (str): Model name to search for (e.g., "Claude Sonnet 4.6").
        region (str): AWS region (e.g., "us-west-2").

    Example:
        get_tier_limits_for_model("~/bedrock_cache", "Claude Sonnet 4.6", "us-west-2")

    Returns: dict {"rpm_high": N, "tpm_high": N, "tpd_high": N} or None if not found.

    --- Detailed Documentation ---

    Queries all quota types and returns the highest limit for each (across inference types).
    Returns None if no quotas are found for the model/region.

    Args:
        cache_dir (str): Path to cache directory (e.g., ~/bedrock_cache).
        model_name (str): Model name to search for (e.g., "Claude Sonnet 4.6").
        region (str): AWS region (e.g., "us-west-2").

    Returns:
        dict|None: {"rpm_high": N, "tpm_high": N, "tpd_high": N} or None if not found.
        Keys with no data are omitted (e.g., no "tpd_high" if model has no TPD quota).
    """
    rpm_quotas = query_quotas(cache_dir, model_filter=model_name, region_filter=region, quota_type_filter="RPM")
    tpm_quotas = query_quotas(cache_dir, model_filter=model_name, region_filter=region, quota_type_filter="TPM")
    tpd_quotas = query_quotas(cache_dir, model_filter=model_name, region_filter=region, quota_type_filter="TPD")

    rpm_high = max((q["value"] for q in rpm_quotas), default=None)
    tpm_high = max((q["value"] for q in tpm_quotas), default=None)
    tpd_high = max((q["value"] for q in tpd_quotas), default=None)

    # Must have BOTH RPM and TPM to be useful for check_capacity_fit()
    if rpm_high is None or tpm_high is None:
        return None

    result = {"rpm_high": rpm_high, "tpm_high": tpm_high}
    if tpd_high is not None:
        result["tpd_high"] = tpd_high

    return result


# ── No hardcoded tier limits. Always use real quotas from bedrock_quotas.json ──
# Pass actual RPM/TPM limits via tier_limits parameter (from query_quotas()).
# If not provided, the function will report that limits could not be found.


def aggregate_capacity_by_model(capacity_profile):
    """Aggregate multi-agent capacity into per-model load profiles for quota checks.

    Args (required):
        capacity_profile (dict): From calculate_agent_session_compounded_cost() or
            build_capacity_profile_from_tokens().

    Example:
        aggregate_capacity_by_model(cost_result["capacity_profile"])

    Returns: dict keyed by model_name, each with capacity_profile, questions_per_month, components.

    --- Detailed Documentation ---

    When main agent and sub-agents use the same model, their RPM/TPM/TPD load
    must be summed before checking against that model's shared quota.

    Args:
        capacity_profile (dict): From calculate_agent_session_compounded_cost() or
            build_capacity_profile_from_tokens(). Must contain "sessions_per_month",
            "main_agent", and "sub_agents" keys.

    Returns:
        dict: Keyed by model_name. Each value is a dict with:
            "capacity_profile" (dict): Aggregated profile for check_capacity_fit().
            "questions_per_month" (int): Total questions hitting this model per month.
            "sessions_per_month" (int): Sessions per month.
            "components" (list): Which agents contribute to this model's load.
    """
    main = capacity_profile["main_agent"]
    sub_agents = capacity_profile.get("sub_agents", [])
    main_model = main.get("model_name") or "main_agent_model"

    # Derive sessions and questions from the profile
    sessions_per_month = capacity_profile.get("sessions_per_month")
    if sessions_per_month is None:
        raise ValueError(
            "capacity_profile must contain 'sessions_per_month'. "
            "Use the capacity_profile from calculate_agent_session_compounded_cost() "
            "or build_capacity_profile_from_tokens() which includes it automatically."
        )

    main_questions_per_session = main.get("questions_per_session", 5)
    questions_per_month = sessions_per_month * main_questions_per_session

    # Collect all load contributions per model
    model_loads = defaultdict(lambda: {
        "total_calls_per_session": 0,
        "total_input_tokens_per_session": 0,
        "total_output_tokens_per_session": 0,
        "total_tokens_per_session": 0,
        "questions_per_month": 0,
        "components": [],
    })

    # Main agent contribution
    main_calls_per_session = main["llm_calls_per_question"] * main_questions_per_session
    main_input_per_session = main["avg_input_tokens_per_call"] * main_calls_per_session
    main_output_per_session = main["avg_output_tokens_per_call"] * main_calls_per_session
    main_tokens_per_session = main["tokens_per_question"] * main_questions_per_session

    model_loads[main_model]["total_calls_per_session"] += main_calls_per_session
    model_loads[main_model]["total_input_tokens_per_session"] += main_input_per_session
    model_loads[main_model]["total_output_tokens_per_session"] += main_output_per_session
    model_loads[main_model]["total_tokens_per_session"] += main_tokens_per_session
    model_loads[main_model]["questions_per_month"] = questions_per_month
    model_loads[main_model]["components"].append({"role": "main_agent", "model": main_model})

    # Sub-agent contributions
    for sa in sub_agents:
        sa_model = sa.get("model_name") or f"sub_agent_{sa.get('type', 'unknown')}"
        invocations_per_session = sa.get("invocations_per_session", 1)
        calls_per_invocation = sa.get("llm_calls_per_invocation", 1)

        sa_calls_per_session = calls_per_invocation * invocations_per_session
        sa_input_per_session = sa["avg_input_tokens_per_call"] * sa_calls_per_session
        sa_output_per_session = sa["avg_output_tokens_per_call"] * sa_calls_per_session
        sa_tokens_per_session = sa.get("tokens_per_invocation", 0) * invocations_per_session

        model_loads[sa_model]["total_calls_per_session"] += sa_calls_per_session
        model_loads[sa_model]["total_input_tokens_per_session"] += sa_input_per_session
        model_loads[sa_model]["total_output_tokens_per_session"] += sa_output_per_session
        model_loads[sa_model]["total_tokens_per_session"] += sa_tokens_per_session
        # Sub-agent questions = invocations per session * sessions
        sa_questions = invocations_per_session * sessions_per_month
        model_loads[sa_model]["questions_per_month"] += sa_questions
        model_loads[sa_model]["components"].append({"role": f"sub_agent ({sa.get('type', '?')})", "model": sa_model})

    # Build per-model capacity profiles for check_capacity_fit()
    result = {}
    for model_name, load in model_loads.items():
        total_calls = load["total_calls_per_session"]
        total_input = load["total_input_tokens_per_session"]
        total_output = load["total_output_tokens_per_session"]
        total_tokens = load["total_tokens_per_session"]

        # Derive per-call averages
        avg_input_per_call = total_input / total_calls if total_calls > 0 else 0
        avg_output_per_call = total_output / total_calls if total_calls > 0 else 0

        # For questions_per_month: if this model serves both main + sub-agent,
        # we need the total "questions" (invocations) hitting this model
        # For RPM: calls_per_session * sessions_per_month / active_minutes
        # We express this as: effective_questions * calls_per_question
        # where effective_questions = total_calls_per_session * sessions_per_month / calls_per_question
        # Simplification: use calls_per_question = total_calls / questions_equivalent
        questions_per_session_equiv = main_questions_per_session  # use main agent's session structure
        calls_per_question_equiv = total_calls / questions_per_session_equiv if questions_per_session_equiv > 0 else total_calls

        # tokens_per_question for TPD
        tokens_per_question_equiv = total_tokens / questions_per_session_equiv if questions_per_session_equiv > 0 else total_tokens

        result[model_name] = {
            "capacity_profile": {
                "llm_calls_per_question": calls_per_question_equiv,
                "avg_input_tokens_per_call": avg_input_per_call,
                "avg_output_tokens_per_call": avg_output_per_call,
                "tokens_per_question": tokens_per_question_equiv,
                "questions_per_session": questions_per_session_equiv,
            },
            "questions_per_month": questions_per_month,  # main agent's question volume drives the session rate
            "sessions_per_month": sessions_per_month,
            "components": load["components"],
        }

    return result


def _write_capacity_report(result, filepath):
    """Write the full capacity fit detail to a markdown report file."""
    lines = []
    lines.append("# Capacity Fit Report")
    lines.append("")

    # Summary table
    fits_icon = "✅" if result.get("fits") else "❌" if result.get("fits") is False else "⚠️"
    lines.append(f"**Overall: {fits_icon} {'Fits' if result.get('fits') else 'Does NOT fit' if result.get('fits') is False else 'Unknown (no limits)'}**")
    lines.append("")
    lines.append("## Capacity Summary")
    lines.append("")
    lines.append("| Metric | Your Workload (Peak) | Quota Limit | Fits? | Utilization |")
    lines.append("|--------|:--------------------:|:-----------:|:-----:|:-----------:|")

    tier_limits = result.get("tier_limits", {})
    rpm_fit = "✅" if result.get("rpm_fits") else "❌" if result.get("rpm_fits") is False else "—"
    tpm_fit = "✅" if result.get("tpm_fits") else "❌" if result.get("tpm_fits") is False else "—"
    tpd_fit = "✅" if result.get("tpd_fits") else "❌" if result.get("tpd_fits") is False else "—"

    rpm_limit = tier_limits.get("rpm_high")
    tpm_limit = tier_limits.get("tpm_high")
    tpd_limit = tier_limits.get("tpd_high")

    rpm_util = result.get("rpm_utilization_pct")
    tpm_util = result.get("tpm_utilization_pct")
    tpd_util = result.get("tpd_utilization_pct")

    lines.append(f"| RPM | {result['peak_rpm']:,.0f} | {rpm_limit:,.0f} | {rpm_fit} | {rpm_util:.1f}% |" if rpm_limit else f"| RPM | {result['peak_rpm']:,.0f} | — | — | — |")
    lines.append(f"| TPM (effective) | {result['effective_peak_tpm']:,.0f} | {tpm_limit:,.0f} | {tpm_fit} | {tpm_util:.1f}% |" if tpm_limit else f"| TPM (effective) | {result['effective_peak_tpm']:,.0f} | — | — | — |")
    if tpd_limit and tpd_limit > 0:
        lines.append(f"| TPD | {result['estimated_tpd']:,.0f} | {tpd_limit:,.0f} | {tpd_fit} | {tpd_util:.1f}% |")
    lines.append("")

    # Recommendations
    if result.get("recommendations"):
        lines.append("## Recommendations")
        lines.append("")
        for rec in result["recommendations"]:
            lines.append(f"- {rec}")
        lines.append("")

    # Optimization checklist
    if result.get("optimization_checklist"):
        lines.append("## Optimization Checklist")
        lines.append("")
        lines.append("| Area | Current | Action |")
        lines.append("|------|---------|--------|")
        for item in result["optimization_checklist"]:
            lines.append(f"| {item['area']} | {item['current']} | {item['action']} |")
        lines.append("")

    # Assumptions
    if result.get("assumptions"):
        lines.append("## Assumptions")
        lines.append("")
        lines.append("| Parameter | Value |")
        lines.append("|-----------|-------|")
        for k, v in result["assumptions"].items():
            lines.append(f"| {k} | {v} |")
        lines.append("")

    # Explanation
    explanation = result.get("explanation", {})
    if explanation:
        lines.append("## Detailed Calculations")
        lines.append("")
        for section_name, section_data in explanation.items():
            lines.append(f"### {section_name}")
            lines.append("")
            lines.append("| Step | Calculation |")
            lines.append("|------|-------------|")
            for step, calc in section_data.items():
                lines.append(f"| {step} | {calc} |")
            lines.append("")

    content = "\n".join(lines)

    try:
        dir_path = os.path.dirname(filepath)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)
        with open(filepath, "w") as f:
            f.write(content)
        return True
    except Exception as e:
        print(f"⚠️  Capacity report: Could not write to {filepath}: {e}", file=sys.stderr)
        return False


def check_capacity_fit(
    capacity_profile,
    # Traffic profile
    questions_per_month,
    peak_to_avg_ratio=None,            # peak RPM = avg RPM × this factor
    active_hours_per_day=None,         # hours with traffic (rest = 0)
    active_days_per_month=None,        # business days
    # Model characteristics
    output_burndown_rate=1,            # 5 for Claude 3.7+, 1 for all others
    max_tokens_setting=None,           # what max_tokens is set to in the API call
    # Quota limits — must provide actual limits from query_quotas()
    tier_limits=None,                  # Required: {"rpm_high": N, "tpm_high": N, "tpd_high": N (optional)} from quota cache
    # Output control
    report_file=None,                  # Explicit file path for the report (overrides output_dir)
    output_dir=None,                   # Session directory — writes capacity.md inside it
):
    """Check if a workload fits within Bedrock RPM/TPM/TPD quota limits.

    Args (required):
        capacity_profile (dict): Token profile from cost or token calculation output.
        questions_per_month (int): Total questions per month hitting this model.
    Args (optional):
        tier_limits (dict|None): {"rpm_high": N, "tpm_high": N}. Default None.
        peak_to_avg_ratio (float): Peak traffic multiplier. Default 3.0.
        output_burndown_rate (int): Output TPM multiplier (5 for Claude 3.7+). Default 1.

    Example:
        check_capacity_fit(capacity_profile, 1_000_000, tier_limits={"rpm_high": 1000, "tpm_high": 400000})

    Returns: dict with fits (bool), utilization_pct, recommendations, and explanation.

    --- Detailed Documentation ---

    Uses a capacity_profile (from calculate_agent_session_compounded_cost() or
    calculate_compounded_tokens_for_agent()) as the source of token math.
    Does NOT compute tokens internally — all token values come from the profile.

    Args:
        capacity_profile (dict): Token profile for one model. Must contain:
            "llm_calls_per_question" (int): LLM API calls per question.
            "avg_input_tokens_per_call" (float): Average input tokens per API call.
            "avg_output_tokens_per_call" (float): Average output tokens per API call.
            "tokens_per_question" (float): Total tokens (in+out) per question.
            "questions_per_session" (int): Questions per session.
        questions_per_month (int): Total questions per month hitting this model.
        peak_to_avg_ratio (float): Peak multiplier (default 3.0).
        active_hours_per_day (int): Traffic hours (default 12).
        active_days_per_month (int): Business days (default 22).
        output_burndown_rate (int): Output TPM multiplier. 5 for Claude 3.7+, 1 for others.
        max_tokens_setting (int): API max_tokens parameter (default 4096).
        tier_limits (dict | None): Required. Must contain {"rpm_high": N, "tpm_high": N}.
            Optionally include "tpd_high": N for daily token limit.
            If None, the function cannot perform a fit check.

    Returns:
        dict: avg/peak_rpm, avg/peak/effective_peak_tpm, estimated_tpd,
        fits (bool), rpm/tpm/tpd_fits (bool), utilization_pct (float),
        recommendations (list[str]), optimization_checklist (list[dict]),
        explanation (dict).
    """
    # Resolve defaults from config (explicit values passed by caller always win)
    peak_to_avg_ratio = _resolve_setting("capacity", "peak_to_avg_ratio", peak_to_avg_ratio)
    active_hours_per_day = _resolve_setting("capacity", "active_hours_per_day", active_hours_per_day)
    active_days_per_month = _resolve_setting("capacity", "active_days_per_month", active_days_per_month)
    max_tokens_setting = _resolve_setting("capacity", "max_tokens_setting", max_tokens_setting)

    # Input validation
    if questions_per_month <= 0:
        raise ValueError(f"questions_per_month must be > 0, got {questions_per_month}")
    if active_hours_per_day <= 0:
        raise ValueError(f"active_hours_per_day must be > 0, got {active_hours_per_day}")
    if active_days_per_month <= 0:
        raise ValueError(f"active_days_per_month must be > 0, got {active_days_per_month}")

    # Validate tier_limits has required keys if provided
    if tier_limits is not None:
        missing = [k for k in ("rpm_high", "tpm_high") if k not in tier_limits]
        if missing:
            raise ValueError(
                f"tier_limits must contain both 'rpm_high' and 'tpm_high'. "
                f"Missing: {missing}. Use get_tier_limits_for_model() to obtain valid limits."
            )

    # Validate capacity_profile has required keys directly (no auto-extraction)
    if "main_agent" in capacity_profile and "llm_calls_per_question" not in capacity_profile:
        raise ValueError(
            "capacity_profile contains 'main_agent' wrapper. "
            "Pass capacity_profile['main_agent'] for the main agent, "
            "or the specific sub-agent profile dict for sub-agent checks."
        )

    # Extract token values from capacity_profile
    llm_calls_per_question = capacity_profile["llm_calls_per_question"]
    avg_input_per_call = capacity_profile["avg_input_tokens_per_call"]
    avg_output_per_call = capacity_profile["avg_output_tokens_per_call"]
    tokens_per_question = capacity_profile["tokens_per_question"]

    # ── Compute average RPM ──
    active_minutes_per_month = active_hours_per_day * 60 * active_days_per_month
    avg_questions_per_min = questions_per_month / active_minutes_per_month
    avg_rpm = avg_questions_per_min * llm_calls_per_question
    peak_rpm = avg_rpm * peak_to_avg_ratio

    # ── Compute TPM ──
    avg_tpm_input = avg_rpm * avg_input_per_call
    avg_tpm_output = avg_rpm * avg_output_per_call * output_burndown_rate
    avg_tpm = avg_tpm_input + avg_tpm_output

    # TPM at peak
    peak_tpm = avg_tpm * peak_to_avg_ratio

    # max_tokens overhead: at request start, max_tokens is reserved
    # Clamp to 0 minimum (in case max_tokens < actual output)
    max_tokens_overhead_per_req = max(0, max_tokens_setting - avg_output_per_call)
    effective_peak_tpm = peak_tpm + (peak_rpm * max_tokens_overhead_per_req)

    # ── Compute TPD (tokens per day) ──
    days_per_month = active_days_per_month
    questions_per_day = questions_per_month / days_per_month
    estimated_tpd = tokens_per_question * questions_per_day

    # ── Compare against tier ──
    if tier_limits is None:
        # No limits provided — cannot perform fit check
        rpm_fits = None
        tpm_fits = None
        effective_tpm_fits = None
        tpd_fits = None
        fits = None
        rpm_util = None
        tpm_util = None
        tpd_util = None
        limits = {"rpm_high": None, "tpm_high": None, "tpd_high": None}
        recommendations = [
            "Could not find RPM/TPM limits for this model in the quota cache. "
            "Run query_quotas() with the specific model and region to get actual limits, "
            "then pass them via tier_limits={'rpm_high': N, 'tpm_high': N, 'tpd_high': N}."
        ]
    else:
        limits = tier_limits
        rpm_fits = peak_rpm <= limits["rpm_high"]
        tpm_fits = peak_tpm <= limits["tpm_high"]
        effective_tpm_fits = effective_peak_tpm <= limits["tpm_high"]

        # TPD check — optional, only if tpd_high is provided and > 0
        tpd_limit = limits.get("tpd_high")
        if tpd_limit is not None and tpd_limit > 0:
            tpd_fits = estimated_tpd <= tpd_limit
            tpd_util = (estimated_tpd / tpd_limit) * 100
        else:
            tpd_fits = None  # No TPD quota for this model
            tpd_util = None

        fits = rpm_fits and effective_tpm_fits and (tpd_fits is not False)

        rpm_util = (peak_rpm / limits["rpm_high"]) * 100 if limits["rpm_high"] > 0 else float('inf')
        tpm_util = (effective_peak_tpm / limits["tpm_high"]) * 100 if limits["tpm_high"] > 0 else float('inf')

        # ── Recommendations ──
        recommendations = []
        if not rpm_fits:
            recommendations.append(f"Peak RPM ({peak_rpm:,.0f}) exceeds limit ({limits['rpm_high']:,}). Consider a quota increase.")
        if not effective_tpm_fits and tpm_fits:
            recommendations.append(f"Peak TPM fits ({peak_tpm:,.0f}) but effective TPM with max_tokens overhead ({effective_peak_tpm:,.0f}) exceeds limit. Reduce max_tokens from {max_tokens_setting:,} to ~{int(avg_output_per_call * 2)}.")
        if not tpm_fits:
            recommendations.append(f"Peak TPM ({peak_tpm:,.0f}) exceeds limit ({limits['tpm_high']:,}). Consider a quota increase.")
        if tpd_fits is False:
            recommendations.append(f"Estimated daily tokens ({estimated_tpd:,.0f}) exceeds TPD limit ({tpd_limit:,}). Reduce token volume or distribute across regions.")
        if output_burndown_rate > 1:
            recommendations.append(f"Output burndown rate is {output_burndown_rate}× — each output token consumes {output_burndown_rate} TPM quota. Reducing output length has {output_burndown_rate}× impact.")
        if fits:
            recommendations.append(f"Workload fits within limits. RPM utilization: {rpm_util:.0f}%, TPM utilization: {tpm_util:.0f}%.")

    # ── Optimization checklist ──
    optimization_checklist = [
        {"area": "Prompt caching", "current": "Check if enabled", "action": "Prompt cache reads do NOT count toward TPM — enable prompt caching to free quota"},
        {"area": "max_tokens", "current": f"{max_tokens_setting:,} (actual output ~{int(avg_output_per_call)})", "action": f"Reduce to ~{int(avg_output_per_call * 3)} to free {int(max_tokens_overhead_per_req):,} TPM/request"},
        {"area": "System prompt", "current": "Check size", "action": "Shorten instructions, remove redundancy — sent every turn, compounding"},
        {"area": "Tool count", "current": "Check tool descriptions", "action": "Use AC Gateway dynamic tool selection to reduce tool descriptions per request"},
        {"area": "Conversation history", "current": "Included in context", "action": "Limit past Q&A turns packed as context"},
        {"area": "Sub-agent responses", "current": "Check response sizes", "action": "Reduce sub-agent output tokens — they compound in main agent context"},
        {"area": "Output length", "current": f"~{int(avg_output_per_call)} tokens" + (f" (×{output_burndown_rate} burndown)" if output_burndown_rate > 1 else ""), "action": "Constrain output with max_tokens and prompt instructions"},
    ]

    # ── Build step-by-step explanation ──
    if tier_limits is None:
        tier_comparison = {
            "rpm_limit": "NOT AVAILABLE — no quota data found",
            "tpm_limit": "NOT AVAILABLE — no quota data found",
            "tpd_limit": "NOT AVAILABLE — no quota data found",
            "rpm_utilization": "N/A",
            "tpm_utilization": "N/A",
            "tpd_utilization": "N/A",
        }
    else:
        tier_comparison = {
            "rpm_limit": f"{_fmt(limits['rpm_high'])} (peak: {peak_rpm:.0f} → {'✅ fits' if rpm_fits else '❌ exceeds'})",
            "tpm_limit": f"{_fmt(limits['tpm_high'])} (effective peak: {_fmt(effective_peak_tpm)} → {'✅ fits' if effective_tpm_fits else '❌ exceeds'})",
            "rpm_utilization": f"{rpm_util:.0f}%",
            "tpm_utilization": f"{tpm_util:.0f}%",
        }
        if tpd_limit is not None and tpd_limit > 0:
            tier_comparison["tpd_limit"] = f"{_fmt(tpd_limit)} (estimated daily: {_fmt(estimated_tpd)} → {'✅ fits' if tpd_fits else '❌ exceeds'})"
            tier_comparison["tpd_utilization"] = f"{tpd_util:.0f}%"
        else:
            tier_comparison["tpd_limit"] = "No TPD quota for this model"
            tier_comparison["tpd_utilization"] = "N/A"

    explanation = {
        "rpm_calculation": {
            "active_minutes_per_month": f"{active_hours_per_day}h × 60 × {active_days_per_month}d = {_fmt(active_minutes_per_month)} min",
            "avg_questions_per_min": f"{_fmt(questions_per_month)} questions ÷ {_fmt(active_minutes_per_month)} min = {avg_questions_per_min:.2f} Q/min",
            "llm_calls_per_question": f"{llm_calls_per_question} LLM calls/question (from capacity_profile)",
            "avg_rpm": f"{avg_questions_per_min:.2f} Q/min × {llm_calls_per_question} calls = {avg_rpm:.1f} RPM",
            "peak_rpm": f"{avg_rpm:.1f} × {peak_to_avg_ratio}× peak ratio = {peak_rpm:.0f} RPM",
        },
        "tpm_calculation": {
            "avg_input_per_call": f"{_fmt(avg_input_per_call)} tokens (from capacity_profile)",
            "avg_output_per_call": f"{_fmt(avg_output_per_call)} tokens (from capacity_profile)",
            "avg_tpm": f"{avg_rpm:.1f} RPM × ({_fmt(avg_input_per_call)} in + {_fmt(avg_output_per_call)}{'×' + str(output_burndown_rate) if output_burndown_rate > 1 else ''} out) = {_fmt(avg_tpm)} TPM",
            "peak_tpm": f"{_fmt(avg_tpm)} × {peak_to_avg_ratio}× = {_fmt(peak_tpm)} TPM",
            "max_tokens_overhead": f"max_tokens={_fmt(max_tokens_setting)} − actual_output={_fmt(avg_output_per_call)} = {_fmt(max_tokens_overhead_per_req)} reserved/req",
            "effective_peak_tpm": f"{_fmt(peak_tpm)} + ({peak_rpm:.0f} RPM × {_fmt(max_tokens_overhead_per_req)}) = {_fmt(effective_peak_tpm)} TPM",
        },
        "tpd_calculation": {
            "tokens_per_question": f"{_fmt(tokens_per_question)} tokens/question (from capacity_profile)",
            "questions_per_day": f"{_fmt(questions_per_month)} questions/month ÷ {days_per_month} days = {_fmt(questions_per_day)} questions/day",
            "estimated_tpd": f"{_fmt(tokens_per_question)} × {_fmt(questions_per_day)} = {_fmt(estimated_tpd)} tokens/day",
        },
        "tier_comparison": tier_comparison,
    }

    full_result = {
        "avg_rpm": avg_rpm,
        "peak_rpm": peak_rpm,
        "avg_tpm": avg_tpm,
        "peak_tpm": peak_tpm,
        "effective_peak_tpm": effective_peak_tpm,
        "estimated_tpd": estimated_tpd,
        "max_tokens_overhead_per_req": max_tokens_overhead_per_req,
        "tier_limits": limits,
        "rpm_utilization_pct": rpm_util,
        "tpm_utilization_pct": tpm_util,
        "tpd_utilization_pct": tpd_util,
        "fits": fits,
        "rpm_fits": rpm_fits,
        "tpm_fits": tpm_fits,
        "tpd_fits": tpd_fits,
        "recommendations": recommendations,
        "optimization_checklist": optimization_checklist,
        "assumptions": {
            "peak_to_avg_ratio": peak_to_avg_ratio,
            "active_hours_per_day": active_hours_per_day,
            "active_days_per_month": active_days_per_month,
            "active_minutes_per_month": active_minutes_per_month,
            "output_burndown_rate": output_burndown_rate,
            "max_tokens_setting": max_tokens_setting,
        },
        "explanation": explanation,
    }

    # Resolve report path: report_file > output_dir/capacity-{model}.md > auto-generated flat file
    if report_file is not None:
        report_file = os.path.abspath(os.path.expanduser(report_file))
    elif output_dir is not None:
        output_dir = os.path.abspath(os.path.expanduser(output_dir))
        os.makedirs(output_dir, exist_ok=True)
        model_name = capacity_profile.get("model_name", "model")
        safe_name = _re.sub(r"[^a-z0-9]+", "-", model_name.lower()).strip("-") or "model"
        report_file = os.path.join(output_dir, f"capacity-{safe_name}.md")
    else:
        model_name = capacity_profile.get("model_name", "model")
        questions_per_session = capacity_profile.get("questions_per_session", 5)
        sessions = questions_per_month // questions_per_session if questions_per_session > 0 else questions_per_month
        report_file = _generate_report_path(
            f"capacity-{model_name}", sessions
        )

    # Write full detail to report file
    write_ok = _write_capacity_report(full_result, report_file)

    # Return compact summary (what the SKILL.md says to present)
    compact = {
        "fits": fits,
        "peak_rpm": round(peak_rpm, 1),
        "effective_peak_tpm": round(effective_peak_tpm, 0),
        "estimated_tpd": round(estimated_tpd, 0),
        "rpm_utilization_pct": round(rpm_util, 1) if rpm_util is not None else None,
        "tpm_utilization_pct": round(tpm_util, 1) if tpm_util is not None else None,
        "tpd_utilization_pct": round(tpd_util, 1) if tpd_util is not None else None,
        "rpm_fits": rpm_fits,
        "tpm_fits": tpm_fits,
        "tpd_fits": tpd_fits,
        "recommendations": recommendations,
        "report_file": report_file if write_ok else None,
        "_report_write_failed": not write_ok,
    }
    return compact




# ============================================================
# BUSINESS VALUE TIERS
# ============================================================
BUSINESS_VALUE_TIERS = {
    "Conservative": {"effectiveness": 0.50, "efficiency": 0.50},
    "Moderate":     {"effectiveness": 0.65, "efficiency": 0.60},
    "Optimistic":   {"effectiveness": 0.80, "efficiency": 0.70},
}


def calculate_agentcore_cost(
    # Prices (from cache — caller passes these in)
    runtime_vcpu_price_hr,
    runtime_mem_price_hr,
    gateway_invocation_price,
    gateway_search_price,
    gateway_indexing_price,
    stm_event_price,
    ltm_storage_price,
    ltm_retrieval_price,
    # Optional component prices (None = not included)
    browser_vcpu_price_hr=None,
    browser_mem_price_hr=None,
    ci_vcpu_price_hr=None,
    ci_mem_price_hr=None,
    # Workload params
    questions_per_month=1_000_000,
    questions_per_session=5,
    tools_invoked=5,
    tools_indexed=None,
    # Runtime params
    num_vcpus=None,
    peak_memory_gb=None,
    io_wait_pct=None,
    idle_time_between_questions_s=None,
    time_per_llm_turn_s=4.0,
    # Memory params
    stm_events_per_question=None,
    ltm_records_per_session=None,
    ltm_retrievals_per_question=None,
    # BrowserTool params
    browser_usage_pct=1.0,
    browser_vcpus=2,
    browser_memory_gb=4,
    browser_io_wait_pct=0.70,
    # CodeInterpreter params
    ci_usage_pct=1.0,
    ci_vcpus=2,
    ci_memory_gb=4,
    ci_io_wait_pct=0.20,
    # Report output directory (optional)
    output_dir=None,
):
    """Calculate AgentCore infrastructure costs (Runtime, Gateway, Memory, BrowserTool, CodeInterpreter).

    Args (required):
        runtime_vcpu_price_hr (float): Runtime vCPU price per hour.
        runtime_mem_price_hr (float): Runtime memory price per hour.
        gateway_invocation_price (float): Gateway per-invocation price.
        gateway_search_price (float): Gateway per-search price.
        gateway_indexing_price (float): Gateway per-index price.
        stm_event_price (float): Short-term memory per-event price.
        ltm_storage_price (float): Long-term memory per-record price.
        ltm_retrieval_price (float): Long-term memory per-retrieval price.
    Args (optional):
        questions_per_month (int): Total questions. Default 1,000,000.
        tools_invoked (int): Tool calls per question. Default 5.
        io_wait_pct (float): I/O wait fraction. Default 0.70.

    Example:
        calculate_agentcore_cost(0.05, 0.005, 0.001, 0.0005, 0.01, 0.0001, 0.0002, 0.0003)

    Returns: dict with total_monthly, total_annual, runtime/gateway/memory breakdowns, and explanation.

    --- Detailed Documentation ---

    Does NOT include Evaluations — use calculate_evaluation_cost() separately.

    Args:
        runtime_vcpu_price_hr, runtime_mem_price_hr (float): Runtime prices.
        gateway_invocation/search/indexing_price (float): Gateway prices.
        stm_event_price, ltm_storage/retrieval_price (float): Memory prices.
        browser_vcpu/mem_price_hr (float | None): BrowserTool prices. None = skip.
        ci_vcpu/mem_price_hr (float | None): CodeInterpreter prices. None = skip.
        questions_per_month (int): Total questions (default 1M).
        questions_per_session (int): Qs/session (default 5). Sessions derived.
        tools_invoked (int): Tool calls/question (default 5).
        num_vcpus (int): vCPUs per microVM (default 2).
        peak_memory_gb (int): Memory per microVM (default 4).
        io_wait_pct (float): I/O wait fraction, vCPU FREE during wait (default 0.70).
        time_per_llm_turn_s (float): Seconds per LLM turn (default 4.0).

    Returns:
        dict: assumptions, runtime/gateway/memory (dicts with cost breakdowns),
        total_monthly, total_annual (float),
        explanation (dict): session_profile, runtime, gateway, memory,
            grand_total, cost_composition.
    """
    # Resolve defaults from config (explicit values passed by caller always win)
    num_vcpus = _resolve_setting("agentcore_defaults", "num_vcpus", num_vcpus)
    peak_memory_gb = _resolve_setting("agentcore_defaults", "peak_memory_gb", peak_memory_gb)
    io_wait_pct = _resolve_setting("agentcore_defaults", "io_wait_pct", io_wait_pct)
    idle_time_between_questions_s = _resolve_setting("agentcore_defaults", "idle_time_between_questions_s", idle_time_between_questions_s)
    stm_events_per_question = _resolve_setting("agentcore_defaults", "stm_events_per_question", stm_events_per_question)
    ltm_records_per_session = _resolve_setting("agentcore_defaults", "ltm_records_per_session", ltm_records_per_session)
    ltm_retrievals_per_question = _resolve_setting("agentcore_defaults", "ltm_retrievals_per_question", ltm_retrievals_per_question)
    browser_io_wait_pct = _resolve_setting("agentcore_defaults", "browser_io_wait_pct", browser_io_wait_pct)
    ci_io_wait_pct = _resolve_setting("agentcore_defaults", "ci_io_wait_pct", ci_io_wait_pct)
    tools_indexed = _resolve_setting("agentcore_defaults", "tools_indexed", tools_indexed)

    # Input validation
    if questions_per_month <= 0:
        raise ValueError(f"questions_per_month must be > 0, got {questions_per_month}")
    if questions_per_session <= 0:
        raise ValueError(f"questions_per_session must be > 0, got {questions_per_session}")

    sessions_per_month = questions_per_month / questions_per_session

    # --- Runtime ---
    time_per_question_s = (1 + tools_invoked) * time_per_llm_turn_s
    active_cpu_per_question_s = time_per_question_s * (1 - io_wait_pct)
    total_active_cpu_per_session_s = active_cpu_per_question_s * questions_per_session

    idle_gaps_s = (questions_per_session - 1) * idle_time_between_questions_s
    total_session_duration_s = (time_per_question_s * questions_per_session) + idle_gaps_s

    runtime_cpu_cost = (total_active_cpu_per_session_s * num_vcpus
                        * (runtime_vcpu_price_hr / 3600) * sessions_per_month)
    runtime_mem_cost = (total_session_duration_s * peak_memory_gb
                        * (runtime_mem_price_hr / 3600) * sessions_per_month)
    runtime_total = runtime_cpu_cost + runtime_mem_cost

    # --- Gateway ---
    gateway_inv_count = (1 + tools_invoked) * questions_per_month
    gateway_search_count = questions_per_month
    gateway_inv_cost = gateway_inv_count * gateway_invocation_price
    gateway_search_cost = gateway_search_count * gateway_search_price
    gateway_index_cost = tools_indexed * gateway_indexing_price
    gateway_total = gateway_inv_cost + gateway_search_cost + gateway_index_cost

    # --- Memory ---
    stm_events = stm_events_per_question * questions_per_month
    ltm_records = ltm_records_per_session * sessions_per_month
    ltm_retrievals = ltm_retrievals_per_question * questions_per_month

    stm_cost = stm_events * stm_event_price
    ltm_storage_cost = ltm_records * ltm_storage_price
    ltm_retrieval_cost = ltm_retrievals * ltm_retrieval_price
    memory_total = stm_cost + ltm_storage_cost + ltm_retrieval_cost

    # --- BrowserTool (optional) ---
    browser_total = 0
    browser_cpu_cost = 0
    browser_mem_cost = 0
    if browser_vcpu_price_hr is not None and browser_mem_price_hr is not None:
        browser_questions = questions_per_month * browser_usage_pct
        browser_active_cpu_per_q_s = time_per_question_s * (1 - browser_io_wait_pct)
        browser_cpu_cost = (browser_active_cpu_per_q_s * browser_questions * browser_vcpus
                            * (browser_vcpu_price_hr / 3600))
        browser_mem_cost = (time_per_question_s * browser_questions * browser_memory_gb
                            * (browser_mem_price_hr / 3600))
        browser_total = browser_cpu_cost + browser_mem_cost

    # --- CodeInterpreter (optional) ---
    ci_total = 0
    ci_cpu_cost = 0
    ci_mem_cost = 0
    if ci_vcpu_price_hr is not None and ci_mem_price_hr is not None:
        ci_questions = questions_per_month * ci_usage_pct
        ci_active_cpu_per_q_s = time_per_question_s * (1 - ci_io_wait_pct)
        ci_cpu_cost = (ci_active_cpu_per_q_s * ci_questions * ci_vcpus
                       * (ci_vcpu_price_hr / 3600))
        ci_mem_cost = (time_per_question_s * ci_questions * ci_memory_gb
                       * (ci_mem_price_hr / 3600))
        ci_total = ci_cpu_cost + ci_mem_cost

    # --- Totals ---
    total_monthly = runtime_total + gateway_total + memory_total + browser_total + ci_total
    total_annual = total_monthly * 12

    # ── Build step-by-step explanation ──
    explanation = {
        "session_profile": {
            "sessions_per_month": _fmt(sessions_per_month),
            "questions_per_session": _fmt(questions_per_session),
            "questions_per_month": _fmt(questions_per_month),
            "time_per_question": f"({tools_invoked} tools + 1) × 4s = {time_per_question_s}s",
            "session_duration": f"{questions_per_session} Qs × {time_per_question_s}s + {questions_per_session - 1} × {idle_time_between_questions_s}s idle = {total_session_duration_s}s ({total_session_duration_s/60:.1f} min)",
        },
        "runtime": {
            "vcpu_billing": f"Active processing only: {time_per_question_s}s × {1 - io_wait_pct:.0%} active × {questions_per_session} Qs = {total_active_cpu_per_session_s:.1f}s/session",
            "vcpu_cost": f"{total_active_cpu_per_session_s:.1f}s × {num_vcpus} vCPU × ${runtime_vcpu_price_hr}/hr ÷ 3600 × {_fmt(sessions_per_month)} sessions = ${runtime_cpu_cost:,.2f}",
            "memory_billing": f"Full session duration: {total_session_duration_s}s/session (I/O wait still billed for memory)",
            "memory_cost": f"{total_session_duration_s}s × {peak_memory_gb} GB × ${runtime_mem_price_hr}/hr ÷ 3600 × {_fmt(sessions_per_month)} sessions = ${runtime_mem_cost:,.2f}",
            "total": f"${runtime_cpu_cost:,.2f} + ${runtime_mem_cost:,.2f} = ${runtime_total:,.2f}",
            "key_insight": f"vCPU is FREE during I/O wait ({io_wait_pct:.0%} of time). Memory is billed for full duration.",
        },
        "gateway": {
            "invocations": f"({tools_invoked} + 1) × {_fmt(questions_per_month)} Qs = {_fmt(gateway_inv_count)} calls × ${gateway_invocation_price} = ${gateway_inv_cost:,.2f}",
            "search": f"{_fmt(questions_per_month)} Qs × ${gateway_search_price} = ${gateway_search_cost:,.2f}",
            "indexing": f"{tools_indexed} tools × ${gateway_indexing_price}/tool-month = ${gateway_index_cost:,.2f}",
            "total": f"${gateway_total:,.2f}",
        },
        "memory": {
            "stm": f"{stm_events_per_question} events/Q × {_fmt(questions_per_month)} Qs × ${stm_event_price}/event = ${stm_cost:,.2f} (reads are FREE)",
            "ltm_storage": f"{ltm_records_per_session} records/session × {_fmt(sessions_per_month)} sessions × ${ltm_storage_price}/record = ${ltm_storage_cost:,.2f}",
            "ltm_retrieval": f"{ltm_retrievals_per_question}/Q × {_fmt(questions_per_month)} Qs × ${ltm_retrieval_price}/retrieval = ${ltm_retrieval_cost:,.2f}",
            "total": f"${memory_total:,.2f}",
        },
        "grand_total": f"${runtime_total:,.2f} (runtime) + ${gateway_total:,.2f} (gateway) + ${memory_total:,.2f} (memory)"
                       + (f" + ${browser_total:,.2f} (browser)" if browser_total > 0 else "")
                       + (f" + ${ci_total:,.2f} (code interpreter)" if ci_total > 0 else "")
                       + f" = ${total_monthly:,.2f}/mo",
        "cost_composition": {
            "runtime_pct": f"{runtime_total / total_monthly * 100:.0f}%" if total_monthly > 0 else "0%",
            "gateway_pct": f"{gateway_total / total_monthly * 100:.0f}%" if total_monthly > 0 else "0%",
            "memory_pct": f"{memory_total / total_monthly * 100:.0f}%" if total_monthly > 0 else "0%",
        },
    }

    result = {
        "assumptions": {
            "questions_per_month": questions_per_month,
            "questions_per_session": questions_per_session,
            "sessions_per_month": sessions_per_month,
            "tools_invoked": tools_invoked,
            "tools_indexed": tools_indexed,
            "num_vcpus": num_vcpus,
            "peak_memory_gb": peak_memory_gb,
            "io_wait_pct": io_wait_pct,
            "idle_time_between_questions_s": idle_time_between_questions_s,
            "time_per_question_s": time_per_question_s,
            "total_session_duration_s": total_session_duration_s,
        },
        "runtime": {
            "cpu_cost": runtime_cpu_cost,
            "mem_cost": runtime_mem_cost,
            "total": runtime_total,
        },
        "gateway": {
            "invocation_cost": gateway_inv_cost,
            "search_cost": gateway_search_cost,
            "indexing_cost": gateway_index_cost,
            "total": gateway_total,
        },
        "memory": {
            "stm_cost": stm_cost,
            "ltm_storage_cost": ltm_storage_cost,
            "ltm_retrieval_cost": ltm_retrieval_cost,
            "total": memory_total,
        },
        "browser": {
            "cpu_cost": browser_cpu_cost,
            "mem_cost": browser_mem_cost,
            "total": browser_total,
            "included": browser_vcpu_price_hr is not None,
        },
        "code_interpreter": {
            "cpu_cost": ci_cpu_cost,
            "mem_cost": ci_mem_cost,
            "total": ci_total,
            "included": ci_vcpu_price_hr is not None,
        },
        "total_monthly": total_monthly,
        "total_annual": total_annual,
        "explanation": explanation,
    }

    # ── Write report to file and return compact summary ──
    if output_dir is not None:
        file_path = os.path.join(output_dir, _AGENTCORE_REPORT_FILENAME)
    else:
        file_path = _generate_typed_report_path("agentcore", questions_per_month)

    inputs_dict = result["assumptions"]
    content = _build_typed_front_matter("agentcore", result, inputs_dict) + _format_agentcore_report(result)
    written_path = _try_write(file_path, content)

    # Cascade: if session dir failed, try flat file fallback
    if written_path is None and output_dir is not None:
        print(
            f"⚠️  Report: Could not write to session directory '{output_dir}', trying default location.",
            file=sys.stderr
        )
        fallback_path = _generate_typed_report_path("agentcore", questions_per_month)
        written_path = _try_write(fallback_path, content)

    if written_path is None:
        print(
            "⚠️  Report: Failed to write AgentCore report file. Returning full result inline.\n"
            "    This increases token usage and latency. Specify a writable folder via\n"
            "    reports.output_dir in ~/.bedrock_skills/config.yaml or pass output_dir='/writable/path/'.",
            file=sys.stderr
        )
        result["_file_write_failed"] = True
        return result

    # Auto-cleanup if configured
    if _resolve_setting("reports", "auto_cleanup"):
        _cleanup_old_reports()

    return _build_agentcore_summary(result, written_path)


def calculate_business_value(
    sessions_per_month,
    agent_cost_monthly=0,
    # Dim 1: Time savings
    time_without_ai_min=None,
    time_with_ai_min=None,
    human_cost_per_hour=None,
    revenue_per_hour=None,
    # Dim 2: Churn reduction (set total_customers=0 to skip)
    total_customers=0,
    churn_without_ai_pct=None,
    churn_with_ai_pct=None,
    revenue_per_customer_year=1000,
    # Dim 3: Sales increase (set annual_sales_revenue=0 to skip)
    annual_sales_revenue=0,
    sales_increase_pct=None,
    # Optional: override default business value tiers
    value_tiers=None,
    # Report output directory (optional)
    output_dir=None,
):
    """Calculate business value and ROI of an AI agent (time savings, churn, sales).

    Args (required):
        sessions_per_month (int): Agent sessions per month.
    Args (optional):
        agent_cost_monthly (float): Total agent cost $/month. Default 0.
        total_customers (int): Customer base for churn dim. Default 0 (skip).
        annual_sales_revenue (float): Annual sales for sales dim. Default 0 (skip).

    Example:
        calculate_business_value(10000, agent_cost_monthly=5000)

    Returns: dict with summary (roi_pct, net_value, payback_days), per-tier breakdowns, explanation.

    --- Detailed Documentation ---

    Dim 1 (always): Time savings → productivity or cost savings (3 tiers).
    Dim 2 (if total_customers > 0): Customer churn reduction.
    Dim 3 (if annual_sales_revenue > 0): Sales increase from better CX.

    Args:
        sessions_per_month (int): Agent sessions/month.
        agent_cost_monthly (float): Total agent cost (model + infra) $/month.
        time_without/with_ai_min (int): Minutes per task (default 20/10).
        human_cost_per_hour (float): Fully loaded cost (default $75).
        revenue_per_hour (float): Revenue per productive hour (default $300).
        total_customers (int): Customer base (default 0 = skip Dim 2).
        churn_without/with_ai_pct (float): Monthly churn rates (default 2.0/1.0% from config).
        revenue_per_customer_year (float): Annual revenue/customer (default $1000).
        annual_sales_revenue (float): Annual sales (default 0 = skip Dim 3).
        sales_increase_pct (float): AI sales uplift (default 10.0% from config).
        value_tiers (dict | None): Override default tiers. Each tier key maps to
            {effectiveness: float, efficiency: float}.

    Returns:
        dict: assumptions, dim1_cost_savings, dim1_productivity (per-tier dicts),
        dim2/dim3 (conditional dicts), summary (grand_total_annual, net_value,
        roi_pct, payback_days),
        explanation (dict): dim1_time_savings, dim2_churn_reduction (conditional),
            dim3_sales_increase (conditional), summary.
    """
    # Resolve defaults from config (explicit values passed by caller always win)
    time_without_ai_min = _resolve_setting("business_value_defaults", "time_without_ai_min", time_without_ai_min)
    time_with_ai_min = _resolve_setting("business_value_defaults", "time_with_ai_min", time_with_ai_min)
    human_cost_per_hour = _resolve_setting("business_value_defaults", "human_cost_per_hour", human_cost_per_hour)
    revenue_per_hour = _resolve_setting("business_value_defaults", "revenue_per_hour", revenue_per_hour)
    churn_without_ai_pct = _resolve_setting("business_value_defaults", "churn_without_ai_pct", churn_without_ai_pct)
    churn_with_ai_pct = _resolve_setting("business_value_defaults", "churn_with_ai_pct", churn_with_ai_pct)
    sales_increase_pct = _resolve_setting("business_value_defaults", "sales_increase_pct", sales_increase_pct)

    # Resolve effectiveness/efficiency from config (controls the Moderate tier)
    moderate_effectiveness = _resolve_setting("business_value_defaults", "agent_effectiveness_pct")
    moderate_efficiency = _resolve_setting("business_value_defaults", "efficiency_factor_pct")

    time_saved_min = time_without_ai_min - time_with_ai_min

    # --- Dimension 1: Time Savings (all 3 tiers) ---
    dim1_cost_savings = {}
    dim1_productivity = {}

    # Allow caller to override default tiers; otherwise build from config
    if value_tiers is not None:
        tiers = value_tiers
    else:
        tiers = {
            "Conservative": {"effectiveness": 0.50, "efficiency": 0.50},
            "Moderate":     {"effectiveness": moderate_effectiveness, "efficiency": moderate_efficiency},
            "Optimistic":   {"effectiveness": 0.80, "efficiency": 0.70},
        }

    for tier_name, params in tiers.items():
        eff = params["effectiveness"]
        effi = params["efficiency"]

        effective_sessions = sessions_per_month * eff
        time_saved_hrs = effective_sessions * time_saved_min / 60
        productive_hrs = time_saved_hrs * effi

        cost_savings_monthly = productive_hrs * human_cost_per_hour
        cost_savings_annual = cost_savings_monthly * 12

        revenue_uplift_monthly = productive_hrs * revenue_per_hour
        revenue_uplift_annual = revenue_uplift_monthly * 12

        dim1_cost_savings[tier_name] = {
            "monthly": cost_savings_monthly,
            "annual": cost_savings_annual,
            "productive_hrs_monthly": productive_hrs,
            "effectiveness": eff,
            "efficiency": effi,
        }
        dim1_productivity[tier_name] = {
            "monthly": revenue_uplift_monthly,
            "annual": revenue_uplift_annual,
            "productive_hrs_monthly": productive_hrs,
            "effectiveness": eff,
            "efficiency": effi,
        }

    # --- Dimension 2: Churn Reduction ---
    dim2_annual = 0
    dim2_details = {}
    if total_customers > 0:
        churn_reduction_pp = churn_without_ai_pct - churn_with_ai_pct
        customers_retained = total_customers * (churn_reduction_pp / 100)
        dim2_annual = customers_retained * revenue_per_customer_year
        dim2_details = {
            "churn_reduction_pp": churn_reduction_pp,
            "customers_retained": customers_retained,
            "annual": dim2_annual,
        }

    # --- Dimension 3: Sales Increase ---
    dim3_annual = 0
    dim3_details = {}
    if annual_sales_revenue > 0:
        dim3_annual = annual_sales_revenue * (sales_increase_pct / 100)
        dim3_details = {
            "sales_increase_pct": sales_increase_pct,
            "annual": dim3_annual,
        }

    # --- Summary (Moderate tier for Dim 1a) ---
    moderate_1a = dim1_productivity["Moderate"]["annual"]
    grand_total = moderate_1a + dim2_annual + dim3_annual
    agent_cost_annual = agent_cost_monthly * 12
    net_value = grand_total - agent_cost_annual
    roi_pct = (net_value / agent_cost_annual * 100) if agent_cost_annual > 0 else float("inf")
    payback_days = (agent_cost_annual / grand_total * 365) if grand_total > 0 else float("inf")

    # ── Build step-by-step explanation ──
    mod = tiers["Moderate"]
    mod_eff = mod["effectiveness"]
    mod_effi = mod["efficiency"]
    mod_effective_sessions = sessions_per_month * mod_eff
    mod_time_saved_hrs = mod_effective_sessions * time_saved_min / 60
    mod_productive_hrs = mod_time_saved_hrs * mod_effi

    explanation = {
        "dim1_time_savings": {
            "time_saved": f"{time_without_ai_min} min (without AI) − {time_with_ai_min} min (with AI) = {time_saved_min} min saved/session",
            "moderate_scenario": f"{_fmt(sessions_per_month)} sessions × {mod_eff:.0%} effectiveness = {_fmt(mod_effective_sessions)} effective sessions",
            "productive_hours": f"{_fmt(mod_effective_sessions)} × {time_saved_min} min ÷ 60 × {mod_effi:.0%} efficiency = {_fmt(mod_productive_hrs)} hrs/month",
            "cost_savings": f"{_fmt(mod_productive_hrs)} hrs × ${human_cost_per_hour}/hr = ${dim1_cost_savings['Moderate']['monthly']:,.2f}/mo",
            "productivity_uplift": f"{_fmt(mod_productive_hrs)} hrs × ${revenue_per_hour}/hr = ${dim1_productivity['Moderate']['monthly']:,.2f}/mo",
            "tiers_note": "Conservative (50%/50%), Moderate (65%/60%), Optimistic (80%/70%) — effectiveness/efficiency",
        },
    }

    if total_customers > 0:
        explanation["dim2_churn_reduction"] = {
            "churn_delta": f"{churn_without_ai_pct}% − {churn_with_ai_pct}% = {dim2_details['churn_reduction_pp']:.1f}pp reduction",
            "customers_retained": f"{_fmt(total_customers)} × {dim2_details['churn_reduction_pp']:.1f}% = {_fmt(dim2_details['customers_retained'])} customers",
            "annual_value": f"{_fmt(dim2_details['customers_retained'])} × ${_fmt(revenue_per_customer_year)}/yr = ${dim2_annual:,.2f}/yr",
        }

    if annual_sales_revenue > 0:
        explanation["dim3_sales_increase"] = {
            "uplift": f"${_fmt(annual_sales_revenue)}/yr × {sales_increase_pct}% = ${dim3_annual:,.2f}/yr",
        }

    explanation["summary"] = {
        "grand_total_annual": f"${moderate_1a:,.2f} (Dim1)" + (f" + ${dim2_annual:,.2f} (Dim2)" if dim2_annual > 0 else "") + (f" + ${dim3_annual:,.2f} (Dim3)" if dim3_annual > 0 else "") + f" = ${grand_total:,.2f}/yr",
        "agent_cost_annual": f"${agent_cost_monthly:,.2f}/mo × 12 = ${agent_cost_annual:,.2f}/yr",
        "net_value": f"${grand_total:,.2f} − ${agent_cost_annual:,.2f} = ${net_value:,.2f}/yr",
        "roi": f"{roi_pct:,.0f}%" if roi_pct != float("inf") else "∞ (no agent cost)",
        "payback": f"{payback_days:.0f} days" if payback_days != float("inf") else "N/A",
    }

    result = {
        "assumptions": {
            "sessions_per_month": sessions_per_month,
            "time_without_ai_min": time_without_ai_min,
            "time_with_ai_min": time_with_ai_min,
            "time_saved_min": time_saved_min,
            "human_cost_per_hour": human_cost_per_hour,
            "revenue_per_hour": revenue_per_hour,
            "agent_cost_monthly": agent_cost_monthly,
        },
        "dim1_cost_savings": dim1_cost_savings,
        "dim1_productivity": dim1_productivity,
        "dim2": dim2_details,
        "dim3": dim3_details,
        "summary": {
            "dim1_moderate_productivity_annual": moderate_1a,
            "dim1_moderate_cost_savings_annual": dim1_cost_savings["Moderate"]["annual"],
            "dim2_annual": dim2_annual,
            "dim3_annual": dim3_annual,
            "grand_total": grand_total,
            "agent_cost_annual": agent_cost_annual,
            "net_value": net_value,
            "roi_pct": roi_pct,
            "payback_days": payback_days,
        },
        "explanation": explanation,
    }

    # ── Write report to file and return compact summary ──
    # For BVA, total_monthly is not directly in result — add it for front-matter
    result["total_monthly"] = grand_total / 12 if grand_total else 0
    result["total_annual"] = grand_total

    if output_dir is not None:
        file_path = os.path.join(output_dir, _BVA_REPORT_FILENAME)
    else:
        file_path = _generate_typed_report_path("bva", sessions_per_month)

    inputs_dict = result["assumptions"]
    content = _build_typed_front_matter("business-value", result, inputs_dict) + _format_business_value_report(result)
    written_path = _try_write(file_path, content)

    # Cascade: if session dir failed, try flat file fallback
    if written_path is None and output_dir is not None:
        print(
            f"⚠️  Report: Could not write to session directory '{output_dir}', trying default location.",
            file=sys.stderr
        )
        fallback_path = _generate_typed_report_path("bva", sessions_per_month)
        written_path = _try_write(fallback_path, content)

    if written_path is None:
        print(
            "⚠️  Report: Failed to write business value report file. Returning full result inline.\n"
            "    This increases token usage and latency. Specify a writable folder via\n"
            "    reports.output_dir in ~/.bedrock_skills/config.yaml or pass output_dir='/writable/path/'.",
            file=sys.stderr
        )
        result["_file_write_failed"] = True
        return result

    # Auto-cleanup if configured
    if _resolve_setting("reports", "auto_cleanup"):
        _cleanup_old_reports()

    return _build_bva_summary(result, written_path)


if __name__ == "__main__":
    main()
