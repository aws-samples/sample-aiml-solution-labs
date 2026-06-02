#!/usr/bin/env python3
"""
test_model_index.py — Unit tests for list_models(), _generate_model_index(), and _classify_model_family().

Covers 4 test groups:
  1. _classify_model_family() — family classification from model names
  2. _generate_model_index() — index generation from real cache files
  3. list_models() — query-time lookup from generated index
  4. Integration — list_models() → get_model_prices() end-to-end

Run: python3 tests/test_model_index.py
"""

import sys
import os
import tempfile
import shutil
import json

# Load the script
sys.argv = ['bedrock_pricing.py']
os.environ['USE_IN_CLAUDE_CODE'] = '1'
_script_path = os.path.join(os.path.dirname(__file__), '..', 'skills', 'bedrock-pricing', 'scripts', 'bedrock_pricing.py')
_script_path = os.path.abspath(_script_path)
exec(open(_script_path).read())

# Test infrastructure
_pass_count = 0
_fail_count = 0


def _assert(condition, msg):
    global _pass_count, _fail_count
    if condition:
        _pass_count += 1
    else:
        _fail_count += 1
        print(f"  FAIL: {msg}")


def _run_group(name, fn):
    global _pass_count, _fail_count
    before_pass = _pass_count
    before_fail = _fail_count
    try:
        fn()
    except Exception as e:
        _fail_count += 1
        print(f"  FAIL: {name} raised exception: {e}")
        import traceback
        traceback.print_exc()
    group_pass = _pass_count - before_pass
    group_fail = _fail_count - before_fail
    status = "PASS" if group_fail == 0 else "FAIL"
    print(f"[{status}] {name} ({group_pass} passed, {group_fail} failed)")


# ═══════════════════════════════════════════════════════════════════════════════
# Group 1: _classify_model_family()
# ═══════════════════════════════════════════════════════════════════════════════
def test_classify_model_family():
    # Claude families
    _assert(_classify_model_family("Claude Sonnet 4.6") == "Sonnet",
            "Claude Sonnet 4.6 → Sonnet")
    _assert(_classify_model_family("Claude 3.5 Sonnet v2") == "Sonnet",
            "Claude 3.5 Sonnet v2 → Sonnet")
    _assert(_classify_model_family("Claude 3 Sonnet") == "Sonnet",
            "Claude 3 Sonnet → Sonnet")
    _assert(_classify_model_family("Claude Opus 4.7") == "Opus",
            "Claude Opus 4.7 → Opus")
    _assert(_classify_model_family("Claude 3 Opus") == "Opus",
            "Claude 3 Opus → Opus")
    _assert(_classify_model_family("Claude Haiku 4.5") == "Haiku",
            "Claude Haiku 4.5 → Haiku")
    _assert(_classify_model_family("Claude 3 Haiku") == "Haiku",
            "Claude 3 Haiku → Haiku")
    _assert(_classify_model_family("Claude Instant") == "Claude Instant",
            "Claude Instant → Claude Instant")
    _assert(_classify_model_family("Claude Instant (100K)") == "Claude Instant",
            "Claude Instant (100K) → Claude Instant")
    _assert(_classify_model_family("Claude 2.0") == "Claude",
            "Claude 2.0 → Claude")
    _assert(_classify_model_family("Claude") == "Claude",
            "Claude → Claude")

    # Non-Claude families
    _assert(_classify_model_family("Nova Pro") == "Nova Pro",
            "Nova Pro → Nova Pro")
    _assert(_classify_model_family("Nova Pro Latency Optimized") == "Nova Pro",
            "Nova Pro Latency Optimized → Nova Pro")
    _assert(_classify_model_family("Nova Lite") == "Nova Lite",
            "Nova Lite → Nova Lite")
    _assert(_classify_model_family("Nova 2.0 Omni") == "Nova Omni",
            "Nova 2.0 Omni → Nova Omni")
    _assert(_classify_model_family("Nova Sonic 2.0") == "Nova Sonic",
            "Nova Sonic 2.0 → Nova Sonic")
    _assert(_classify_model_family("Llama 3.1 70B") == "Llama 3.1",
            "Llama 3.1 70B → Llama 3.1")
    _assert(_classify_model_family("Llama 4 Maverick 17B") == "Llama 4",
            "Llama 4 Maverick 17B → Llama 4")
    _assert(_classify_model_family("Mistral Large 3") == "Mistral Large",
            "Mistral Large 3 → Mistral Large")
    _assert(_classify_model_family("Mixtral 8x7B") == "Mixtral",
            "Mixtral 8x7B → Mixtral")
    _assert(_classify_model_family("DeepSeek V3.1") == "DeepSeek",
            "DeepSeek V3.1 → DeepSeek")
    _assert(_classify_model_family("Qwen3 32B") == "Qwen",
            "Qwen3 32B → Qwen")
    _assert(_classify_model_family("Qwen4 72B") == "Qwen",
            "Qwen4 72B → Qwen (future-proof via prefix)")
    _assert(_classify_model_family("Qwen5 VL 128B") == "Qwen",
            "Qwen5 VL 128B → Qwen (future-proof via prefix)")
    _assert(_classify_model_family("GLM 4.7") == "GLM",
            "GLM 4.7 → GLM")
    _assert(_classify_model_family("Gemma 3 27B") == "Gemma",
            "Gemma 3 27B → Gemma")

    # Regression tests: substring matching bugs that are now fixed
    _assert(_classify_model_family("Llama 3.1 405B") == "Llama 3.1",
            "Llama 3.1 405B → Llama 3.1 (not Llama 4)")
    _assert(_classify_model_family("Meta Llama 2 Chat 13B") == "Llama 2",
            "Meta Llama 2 Chat 13B → Llama 2 (not Llama 3)")
    _assert(_classify_model_family("Cohere Generate Model - Command") == "Command",
            "Cohere Generate Model - Command → Command (not Command R)")
    _assert(_classify_model_family("Jamba-Instruct") == "Jamba",
            "Jamba-Instruct → Jamba (hyphen split)")

    # Unknown model → Other
    _assert(_classify_model_family("SomeNewModel v99") == "Other",
            "Unknown model → Other")
    _assert(_classify_model_family("") == "Other",
            "Empty string → Other")


# ═══════════════════════════════════════════════════════════════════════════════
# Group 2: _generate_model_index()
# ═══════════════════════════════════════════════════════════════════════════════
def test_generate_model_index():
    cache_dir = os.path.expanduser("~/bedrock_cache")

    # Generate into a temp dir (copy cache files there first)
    tmp_dir = tempfile.mkdtemp(prefix="test_index_")
    try:
        # Copy cache files to temp dir
        for fname in ["bedrock_pricing.json", "bedrock_pricing_3p.json", "bedrock_pricing_service.json"]:
            src = os.path.join(cache_dir, fname)
            if os.path.exists(src):
                os.symlink(src, os.path.join(tmp_dir, fname))

        # Generate index
        _generate_model_index(tmp_dir)

        index_path = os.path.join(tmp_dir, MODEL_INDEX_FILE)
        _assert(os.path.exists(index_path),
                "index file was created")

        with open(index_path) as f:
            index = json.load(f)

        # Structure checks
        _assert(isinstance(index, dict),
                "index is a dict")
        _assert(len(index) >= 30,
                f"index has >= 30 regions (got {len(index)})")

        # us-west-2 must exist
        _assert("us-west-2" in index,
                "us-west-2 is in the index")

        usw2 = index["us-west-2"]
        _assert(isinstance(usw2, dict),
                "us-west-2 value is a dict of families")

        # Known families exist
        _assert("Sonnet" in usw2,
                "Sonnet family exists in us-west-2")
        _assert("Opus" in usw2,
                "Opus family exists in us-west-2")
        _assert("Haiku" in usw2,
                "Haiku family exists in us-west-2")

        # Known models in correct families
        _assert("Claude Sonnet 4.6" in usw2.get("Sonnet", []),
                "Claude Sonnet 4.6 is in Sonnet family")
        _assert("Claude Opus 4.7" in usw2.get("Opus", []),
                "Claude Opus 4.7 is in Opus family")
        _assert("Claude Haiku 4.5" in usw2.get("Haiku", []),
                "Claude Haiku 4.5 is in Haiku family")

        # No empty families
        for region, families in index.items():
            for family, models in families.items():
                _assert(len(models) > 0,
                        f"No empty family: {region}/{family}")
                if len(models) == 0:
                    break
            else:
                continue
            break

        # All values in families are lists of strings
        for model in usw2.get("Sonnet", []):
            _assert(isinstance(model, str),
                    f"Model name is a string: {model}")

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ═══════════════════════════════════════════════════════════════════════════════
# Group 3: list_models()
# ═══════════════════════════════════════════════════════════════════════════════
def test_list_models():
    cache_dir = os.path.expanduser("~/bedrock_cache")

    # Ensure index exists
    index_path = os.path.join(cache_dir, MODEL_INDEX_FILE)
    if not os.path.exists(index_path):
        _generate_model_index(cache_dir)

    # Basic lookup
    sonnet_models = list_models(cache_dir, "us-west-2", "Sonnet")
    _assert(isinstance(sonnet_models, list),
            "list_models returns a list")
    _assert(len(sonnet_models) >= 5,
            f"Sonnet has >= 5 versions in us-west-2 (got {len(sonnet_models)})")
    _assert("Claude Sonnet 4.6" in sonnet_models,
            "Claude Sonnet 4.6 is in Sonnet list")
    _assert("Claude 3 Sonnet" in sonnet_models,
            "Claude 3 Sonnet is in Sonnet list")

    # Case-insensitive
    sonnet_lower = list_models(cache_dir, "us-west-2", "sonnet")
    _assert(sonnet_lower == sonnet_models,
            "list_models is case-insensitive for family")

    opus_upper = list_models(cache_dir, "us-west-2", "OPUS")
    _assert(len(opus_upper) >= 4,
            "OPUS (uppercase) returns Opus models")

    # Non-existent family
    empty = list_models(cache_dir, "us-west-2", "GPT-5")
    _assert(empty == [],
            "Non-existent family returns empty list")

    # Non-existent region
    empty_region = list_models(cache_dir, "xx-nowhere-99", "Sonnet")
    _assert(empty_region == [],
            "Non-existent region returns empty list")

    # All returned values are strings
    for model in sonnet_models:
        _assert(isinstance(model, str),
                f"list_models returns strings: {model}")

    # FileNotFoundError when NO cache files exist at all
    tmp_dir = tempfile.mkdtemp(prefix="test_no_cache_")
    try:
        raised = False
        try:
            list_models(tmp_dir, "us-west-2", "Sonnet")
        except FileNotFoundError:
            raised = True
        _assert(raised,
                "list_models raises FileNotFoundError when no cache files exist")
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    # Auto-generates index when cache files exist but index doesn't
    tmp_dir2 = tempfile.mkdtemp(prefix="test_auto_index_")
    try:
        cache_dir = os.path.expanduser("~/bedrock_cache")
        for fname in ["bedrock_pricing.json", "bedrock_pricing_3p.json", "bedrock_pricing_service.json"]:
            src = os.path.join(cache_dir, fname)
            if os.path.exists(src):
                os.symlink(src, os.path.join(tmp_dir2, fname))
        # No index file yet — list_models should auto-generate it
        result = list_models(tmp_dir2, "us-west-2", "Sonnet")
        _assert(len(result) >= 5,
                "list_models auto-generates index when cache exists but index doesn't")
        _assert(os.path.exists(os.path.join(tmp_dir2, MODEL_INDEX_FILE)),
                "auto-generated index file exists after list_models call")
    finally:
        shutil.rmtree(tmp_dir2, ignore_errors=True)


# ═══════════════════════════════════════════════════════════════════════════════
# Group 4: Integration (list_models → get_model_prices)
# ═══════════════════════════════════════════════════════════════════════════════
def test_integration():
    cache_dir = os.path.expanduser("~/bedrock_cache")

    # list_models → pick model → get_model_prices
    models = list_models(cache_dir, "us-west-2", "Sonnet")
    _assert(len(models) > 0,
            "integration: list_models returns non-empty for Sonnet")

    # Pick the latest (last in sorted list)
    picked = models[-1]
    _assert("Sonnet" in picked or "sonnet" in picked.lower(),
            f"integration: picked model contains 'Sonnet': {picked}")

    # get_model_prices should succeed with the exact name from list_models
    prices = get_model_prices(cache_dir, "us-west-2", picked)
    _assert(isinstance(prices, dict),
            "integration: get_model_prices returns dict")
    _assert("input_price" in prices,
            "integration: result has input_price")
    _assert("output_price" in prices,
            "integration: result has output_price")
    _assert("cache_read_price" in prices,
            "integration: result has cache_read_price")
    _assert("cache_write_price" in prices,
            "integration: result has cache_write_price")
    _assert("model_name" in prices,
            "integration: result has model_name")
    _assert(prices["model_name"] == picked,
            f"integration: model_name matches picked ({picked})")

    # Prices are positive numbers
    _assert(prices["input_price"] > 0,
            "integration: input_price > 0")
    _assert(prices["output_price"] > 0,
            "integration: output_price > 0")

    # Test with Opus family too
    opus_models = list_models(cache_dir, "us-west-2", "Opus")
    if opus_models:
        opus_prices = get_model_prices(cache_dir, "us-west-2", opus_models[-1])
        _assert(opus_prices["input_price"] > 0,
                f"integration: Opus ({opus_models[-1]}) has valid input_price")

    # Test with Haiku family
    haiku_models = list_models(cache_dir, "us-east-1", "Haiku")
    if haiku_models:
        haiku_prices = get_model_prices(cache_dir, "us-east-1", haiku_models[-1])
        _assert(haiku_prices["input_price"] > 0,
                f"integration: Haiku ({haiku_models[-1]}) has valid input_price")


# ═══════════════════════════════════════════════════════════════════════════════
# Group 5: list_agentcore_components()
# ═══════════════════════════════════════════════════════════════════════════════
def test_list_agentcore_components():
    cache_dir = os.path.expanduser("~/bedrock_cache")

    # Basic lookup
    components = list_agentcore_components(cache_dir, "us-west-2")
    _assert(isinstance(components, list),
            "list_agentcore_components returns a list")
    _assert(len(components) >= 4,
            f"us-west-2 has >= 4 components (got {len(components)})")

    # Known components exist
    _assert("Runtime" in components,
            "Runtime is in components")
    _assert("Gateway" in components,
            "Gateway is in components")
    _assert("Memory" in components,
            "Memory is in components")

    # List is sorted
    _assert(components == sorted(components),
            "components list is sorted")

    # All values are strings
    for c in components:
        _assert(isinstance(c, str),
                f"component is a string: {c}")

    # Non-existent region returns empty
    empty = list_agentcore_components(cache_dir, "xx-nowhere-99")
    _assert(empty == [],
            "non-existent region returns empty list")

    # FileNotFoundError when cache missing
    tmp_dir = tempfile.mkdtemp(prefix="test_no_ac_cache_")
    try:
        raised = False
        try:
            list_agentcore_components(tmp_dir, "us-west-2")
        except FileNotFoundError:
            raised = True
        _assert(raised,
                "raises FileNotFoundError when AgentCore cache missing")
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    # query_agentcore_pricing with component filter (case-insensitive)
    r1 = query_agentcore_pricing(cache_dir, "us-west-2", components=["Runtime"])
    r2 = query_agentcore_pricing(cache_dir, "us-west-2", components=["runtime"])
    r3 = query_agentcore_pricing(cache_dir, "us-west-2", components=["RUNTIME"])
    _assert(len(r1) == len(r2) == len(r3),
            "component filter is case-insensitive")
    _assert(len(r1) > 0,
            "Runtime filter returns entries")

    # Empty list returns empty
    r_empty = query_agentcore_pricing(cache_dir, "us-west-2", components=[])
    _assert(r_empty == [],
            "empty components list returns empty result")

    # Invalid type raises TypeError
    raised_type = False
    try:
        query_agentcore_pricing(cache_dir, "us-west-2", components="Runtime")
    except TypeError:
        raised_type = True
    _assert(raised_type,
            "string components raises TypeError")

    # Non-existent component returns empty
    r_fake = query_agentcore_pricing(cache_dir, "us-west-2", components=["VectorDB"])
    _assert(r_fake == [],
            "non-existent component returns empty")

    # None returns all (more than filtered)
    r_all = query_agentcore_pricing(cache_dir, "us-west-2", components=None)
    r_filtered = query_agentcore_pricing(cache_dir, "us-west-2", components=["Runtime", "Gateway", "Memory"])
    _assert(len(r_all) > len(r_filtered),
            "None returns more entries than filtered subset")


# ═══════════════════════════════════════════════════════════════════════════════
# Group 6: check_capacity_fit() compact return
# ═══════════════════════════════════════════════════════════════════════════════
def test_check_capacity_fit_compact():
    cache_dir = os.path.expanduser("~/bedrock_cache")

    # Get a capacity profile
    result = estimate_cost(cache_dir, "us-west-2", "Claude Sonnet 4.6", 100000)
    cp = result["capacity_profile"]["main_agent"]
    tier_limits = get_tier_limits_for_model(cache_dir, "Claude Sonnet 4.6", "us-west-2")

    # Basic call — returns compact dict
    fit = check_capacity_fit(
        capacity_profile=cp,
        questions_per_month=500000,
        tier_limits=tier_limits,
    )
    _assert(isinstance(fit, dict),
            "check_capacity_fit returns a dict")

    # Required keys in compact result
    required_keys = [
        "fits", "peak_rpm", "effective_peak_tpm", "estimated_tpd",
        "rpm_utilization_pct", "tpm_utilization_pct", "tpd_utilization_pct",
        "rpm_fits", "tpm_fits", "tpd_fits", "recommendations", "report_file",
        "_report_write_failed",
    ]
    for key in required_keys:
        _assert(key in fit, f"compact result has key '{key}'")

    # Keys that should NOT be in compact result (they belong in the report file)
    excluded_keys = ["explanation", "assumptions", "optimization_checklist",
                     "avg_rpm", "avg_tpm", "tier_limits", "max_tokens_overhead_per_req"]
    for key in excluded_keys:
        _assert(key not in fit, f"compact result does NOT have '{key}'")

    # Types are correct
    _assert(isinstance(fit["fits"], bool),
            "fits is a bool")
    _assert(isinstance(fit["peak_rpm"], float),
            "peak_rpm is a float")
    _assert(isinstance(fit["recommendations"], list),
            "recommendations is a list")
    _assert(fit["_report_write_failed"] is False,
            "_report_write_failed is False on success")

    # Report file was created
    _assert(fit["report_file"] is not None,
            "report_file is not None")
    _assert(os.path.exists(fit["report_file"]),
            "report file exists on disk")

    # Report file has content
    with open(fit["report_file"]) as f:
        content = f.read()
    _assert(len(content) > 500,
            "report file has substantial content")
    _assert("Capacity Fit Report" in content,
            "report file has header")
    _assert("Detailed Calculations" in content,
            "report file has detailed calculations section")
    _assert("Optimization Checklist" in content,
            "report file has optimization checklist")

    # output_dir puts report in session directory
    tmp_dir = tempfile.mkdtemp(prefix="test_cap_session_")
    try:
        fit2 = check_capacity_fit(
            capacity_profile=cp,
            questions_per_month=500000,
            tier_limits=tier_limits,
            output_dir=tmp_dir,
        )
        _assert(fit2["report_file"].startswith(tmp_dir),
                "output_dir places report in session directory")
        _assert("capacity-" in os.path.basename(fit2["report_file"]),
                "output_dir names file capacity-{model}.md")
        _assert(os.path.exists(fit2["report_file"]),
                "session dir report file exists")
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    # report_file overrides output_dir
    tmp_dir2 = tempfile.mkdtemp(prefix="test_cap_explicit_")
    try:
        explicit_path = os.path.join(tmp_dir2, "my-report.md")
        fit3 = check_capacity_fit(
            capacity_profile=cp,
            questions_per_month=500000,
            tier_limits=tier_limits,
            report_file=explicit_path,
            output_dir="/should/be/ignored",
        )
        _assert(fit3["report_file"] == explicit_path,
                "report_file takes precedence over output_dir")
        _assert(os.path.exists(explicit_path),
                "explicit report_file was created")
    finally:
        shutil.rmtree(tmp_dir2, ignore_errors=True)

    # Write failure reflected in return
    fit4 = check_capacity_fit(
        capacity_profile=cp,
        questions_per_month=500000,
        tier_limits=tier_limits,
        report_file="/proc/no/write/here.md",
    )
    _assert(fit4["_report_write_failed"] is True,
            "_report_write_failed is True when write fails")
    _assert(fit4["report_file"] is None,
            "report_file is None when write fails")
    _assert(fit4["fits"] is not None,
            "fits still computed even when write fails")

    # No tier_limits → fits is None
    fit5 = check_capacity_fit(
        capacity_profile=cp,
        questions_per_month=500000,
        tier_limits=None,
    )
    _assert(fit5["fits"] is None,
            "fits is None when no tier_limits")
    _assert(fit5["rpm_utilization_pct"] is None,
            "utilization is None when no tier_limits")

    # Workload that doesn't fit
    haiku_limits = get_tier_limits_for_model(cache_dir, "Claude Haiku 4.5", "us-west-2")
    fit6 = check_capacity_fit(
        capacity_profile=cp,
        questions_per_month=500000,
        tier_limits=haiku_limits,
    )
    _assert(fit6["fits"] is False,
            "Haiku at 500K questions doesn't fit (TPM exceeds)")
    _assert(fit6["tpm_utilization_pct"] > 100,
            "TPM utilization > 100% when it doesn't fit")
    _assert(len(fit6["recommendations"]) > 0,
            "recommendations non-empty when doesn't fit")


# ═══════════════════════════════════════════════════════════════════════════════
# Group 7: Guards and I/O wait parameters
# ═══════════════════════════════════════════════════════════════════════════════
def test_guards_and_io_wait():
    cache_dir = os.path.expanduser("~/bedrock_cache")

    # --- estimate_cost guard: rejects subagents ---
    raised = False
    try:
        estimate_cost(cache_dir, "us-west-2", "Claude Sonnet 4.6", 10000,
                      subagents=[{"type": "rag"}])
    except ValueError as e:
        raised = True
        _assert("multi-agent" in str(e).lower() or "sub-agent" in str(e).lower(),
                "estimate_cost error message mentions multi-agent")
    _assert(raised,
            "estimate_cost raises ValueError when subagents passed")

    # estimate_cost still works normally
    result = estimate_cost(cache_dir, "us-west-2", "Claude Sonnet 4.6", 10000)
    _assert(result["monthly_total"] > 0,
            "estimate_cost works without subagents")

    # estimate_cost accepts output_dir
    tmp_dir = tempfile.mkdtemp(prefix="test_estimate_dir_")
    try:
        session_dir = os.path.join(tmp_dir, "session")
        os.makedirs(session_dir)
        result2 = estimate_cost(cache_dir, "us-west-2", "Claude Sonnet 4.6", 10000,
                                output_dir=session_dir)
        _assert(session_dir in result2["file_path"],
                "estimate_cost respects output_dir")
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    # --- check_capacity_fit guard: rejects full profile with main_agent wrapper ---
    prices = get_model_prices(cache_dir, "us-west-2", "Claude Sonnet 4.6")
    cost = calculate_agent_session_compounded_cost(
        main_agent_config={**prices, "agent_sessions_per_month": 10000,
                           "model_name": "Claude Sonnet 4.6"}
    )
    tier_limits = get_tier_limits_for_model(cache_dir, "Claude Sonnet 4.6", "us-west-2")

    # Full profile (has main_agent key) should raise
    raised2 = False
    try:
        check_capacity_fit(
            capacity_profile=cost["capacity_profile"],
            questions_per_month=50000,
            tier_limits=tier_limits,
        )
    except ValueError as e:
        raised2 = True
        _assert("main_agent" in str(e),
                "check_capacity_fit error mentions main_agent")
    _assert(raised2,
            "check_capacity_fit raises ValueError for wrapped profile")

    # Explicit main_agent works
    cap = check_capacity_fit(
        capacity_profile=cost["capacity_profile"]["main_agent"],
        questions_per_month=50000,
        tier_limits=tier_limits,
    )
    _assert(cap["fits"] is not None,
            "check_capacity_fit works with explicit main_agent")

    # --- I/O wait parameters in calculate_agentcore_cost ---
    ac_prices = query_agentcore_pricing(cache_dir, "us-west-2",
                                         components=["Runtime", "BrowserTool", "CodeInterpreter"])
    price_map = {}
    for entry in ac_prices:
        sub = entry["sub_component"]
        price = float(entry["dimensions"][0]["price_usd"])
        if "Runtime" in sub and "vCPU" in sub: price_map["runtime_vcpu"] = price
        elif "Runtime" in sub and "Memory" in sub: price_map["runtime_mem"] = price
        elif "BrowserTool" in sub and "vCPU" in sub: price_map["browser_vcpu"] = price
        elif "BrowserTool" in sub and "Memory" in sub: price_map["browser_mem"] = price
        elif "CodeInterpreter" in sub and "vCPU" in sub: price_map["ci_vcpu"] = price
        elif "CodeInterpreter" in sub and "Memory" in sub: price_map["ci_mem"] = price

    # Need gateway and memory prices too
    ac_full = query_agentcore_pricing(cache_dir, "us-west-2",
                                       components=["Gateway", "Memory"])
    for entry in ac_full:
        sub = entry["sub_component"]
        price = float(entry["dimensions"][0]["price_usd"])
        if "API-Invocations" in sub: price_map["gateway_inv"] = price
        elif "Search-API" in sub: price_map["gateway_search"] = price
        elif "Tool-Indexing" in sub: price_map["gateway_index"] = price
        elif "Short-Term" in sub: price_map["stm"] = price
        elif "Long-Term-Memory-Storage" in sub and "Built-in" in sub: price_map["ltm_storage"] = price
        elif "Long-Term-Memory-Retrieval" in sub: price_map["ltm_retrieval"] = price

    # With browser and CI at different I/O wait rates
    result_70 = calculate_agentcore_cost(
        runtime_vcpu_price_hr=price_map["runtime_vcpu"],
        runtime_mem_price_hr=price_map["runtime_mem"],
        gateway_invocation_price=price_map["gateway_inv"],
        gateway_search_price=price_map["gateway_search"],
        gateway_indexing_price=price_map["gateway_index"],
        stm_event_price=price_map["stm"],
        ltm_storage_price=price_map["ltm_storage"],
        ltm_retrieval_price=price_map["ltm_retrieval"],
        browser_vcpu_price_hr=price_map["browser_vcpu"],
        browser_mem_price_hr=price_map["browser_mem"],
        ci_vcpu_price_hr=price_map["ci_vcpu"],
        ci_mem_price_hr=price_map["ci_mem"],
        questions_per_month=100000,
        browser_io_wait_pct=0.70,
        ci_io_wait_pct=0.20,
    )

    # With no I/O wait (0%) — should cost more for CPU
    result_0 = calculate_agentcore_cost(
        runtime_vcpu_price_hr=price_map["runtime_vcpu"],
        runtime_mem_price_hr=price_map["runtime_mem"],
        gateway_invocation_price=price_map["gateway_inv"],
        gateway_search_price=price_map["gateway_search"],
        gateway_indexing_price=price_map["gateway_index"],
        stm_event_price=price_map["stm"],
        ltm_storage_price=price_map["ltm_storage"],
        ltm_retrieval_price=price_map["ltm_retrieval"],
        browser_vcpu_price_hr=price_map["browser_vcpu"],
        browser_mem_price_hr=price_map["browser_mem"],
        ci_vcpu_price_hr=price_map["ci_vcpu"],
        ci_mem_price_hr=price_map["ci_mem"],
        questions_per_month=100000,
        browser_io_wait_pct=0.0,
        ci_io_wait_pct=0.0,
    )

    _assert(result_0["browser_monthly"] > result_70["browser_monthly"],
            "BrowserTool costs more with 0% I/O wait than 70%")
    _assert(result_0["code_interpreter_monthly"] > result_70["code_interpreter_monthly"],
            "CodeInterpreter costs more with 0% I/O wait than 20%")
    _assert(result_70["browser_monthly"] > 0,
            "BrowserTool cost is positive with 70% I/O wait")
    _assert(result_70["code_interpreter_monthly"] > 0,
            "CodeInterpreter cost is positive with 20% I/O wait")


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("=" * 70)
    print("test_model_index.py — Model Index, AgentCore & Capacity Tests")
    print("=" * 70)
    print()

    _run_group("Group 1: _classify_model_family()", test_classify_model_family)
    _run_group("Group 2: _generate_model_index()", test_generate_model_index)
    _run_group("Group 3: list_models()", test_list_models)
    _run_group("Group 4: Integration", test_integration)
    _run_group("Group 5: list_agentcore_components()", test_list_agentcore_components)
    _run_group("Group 6: check_capacity_fit() compact", test_check_capacity_fit_compact)
    _run_group("Group 7: Guards and I/O wait", test_guards_and_io_wait)

    print()
    print("=" * 70)
    total = _pass_count + _fail_count
    print(f"TOTAL: {_pass_count}/{total} passed, {_fail_count} failed")
    print("=" * 70)

    sys.exit(0 if _fail_count == 0 else 1)
