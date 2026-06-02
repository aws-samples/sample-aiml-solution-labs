# Implementation Plan:

## Overview

Implement YAML configuration file support for `bedrock_pricing.py` in 18 tasks. Tasks 1-4 build the core config infrastructure with unit tests for each. Tasks 5-10 integrate config with existing calculation functions and verify no regressions. Tasks 11-15 update SKILL.md files. Tasks 16-17 add comprehensive test coverage. Task 18 runs the full regression suite.

**Testing approach:** A dedicated test script (`tests/test_config.py`) contains all unit tests with explicit expected values. Each implementation task (1-10) has corresponding test cases that must pass before the task is considered complete. Tests use assert statements with descriptive messages and print PASS/FAIL per test group.

## Tasks

- [x] 1. Add CONFIG_SCHEMA and core config infrastructure to bedrock_pricing.py (Requirements: 1, 3, 14, 15)
  - Add `CONFIG_SCHEMA` dict with all 11 sections (reports, defaults, agent_defaults, rag_defaults, research_defaults, agentcore_defaults, business_value_defaults, capacity, pricing_cache, behavior, model_preferences) after imports block (~line 80)
  - Add guarded `import yaml` with clear error message if PyYAML not installed
  - Implement `_read_yaml_file(path)` using `yaml.safe_load()` with error handling
  - Implement `_validate_config(config, source_path)` that validates types, ranges, choices against schema
  - Implement `_deep_merge(base, override)` for recursive dict merge with override winning at leaf level
  - All warnings go to stderr, never stdout
  - **Verify:** Run test group "test_schema_and_utilities" (Task 17) — all assertions pass

- [x] 2. Implement load_config() and get_config() (Requirements: 1, 3)
  - Implement `load_config(user_path=None, project_path=None)` that discovers ~/.bedrock_skills/config.yaml and ./.bedrock_skills.yaml
  - Deep merge applies project config over user config at every nesting level
  - Returns empty dict when no config files exist
  - Handles empty files and permission errors gracefully
  - If PyYAML not available, returns empty dict with stderr warning
  - Implement `get_config(section=None, key=None)` for accessing merged config
  - Add module-level `_LOADED_CONFIG` with `_ensure_config_loaded()` lazy initialization (triggered by first resolve_setting() call)
  - **Verify:** Run test group "test_load_and_merge" (Task 17) — all assertions pass

- [x] 3. Implement resolve_setting() with full precedence chain (Requirements: 2)
  - Implement `resolve_setting(section, key, explicit_value=None, env_var=None)`
  - Precedence: explicit_value (not None) > env var (BEDROCK_{SECTION}_{KEY}) > config > schema default
  - Type conversion for env var strings: int(), float(), bool (true/1/yes), str as-is
  - Emit warning and skip env var if type conversion fails
  - Each setting resolves independently
  - **Verify:** Run test group "test_precedence_chain" (Task 17) — all assertions pass

- [x] 4. Implement generate_config_template() and --init-config CLI (Requirements: 12)
  - Implement `generate_config_template(output_path=None, force=False)` that iterates CONFIG_SCHEMA to produce commented YAML
  - Each section gets a header comment explaining purpose; each key gets type, description, valid options, default
  - Add `--init-config` and `--force` arguments to main() parser
  - Create ~/.bedrock_skills/ directory if needed
  - Prompt for confirmation before overwriting existing file (TTY only); abort with --force suggestion in non-TTY
  - `--force` flag overwrites without prompting
  - Print absolute path and section names on success
  - Report error on permission/filesystem failures
  - Template generation uses string formatting only (no PyYAML needed)
  - **Verify:** Run test group "test_template_generation" (Task 17) — all assertions pass

- [x] 5. Integrate config with calculate_agent_session_compounded_cost() and sub-agent token functions (Requirements: 5, 6, 7b, 7c)
  - Update main_agent_config.get() calls to use resolve_setting("agent_defaults", key) as fallback default
  - Resolve: questions_per_agent_session, input_tokens, output_tokens, system_prompt_tokens, tools_passed_to_agent, tool_spec_tokens, tools_invoked, tool_call_tokens, tool_result_tokens
  - Resolve history_mode from defaults.history_mode
  - Update calculate_rag_subagent_tokens() to resolve from "rag_defaults" when token_params doesn't specify a value
  - Update calculate_research_subagent_tokens() to resolve from "research_defaults" when token_params doesn't specify a value
  - Explicit values in main_agent_config or token_params always override config
  - Existing callers passing all values see no behavior change
  - **Verify:** Run test group "test_agent_session_integration" (Task 17) — confirms identical output with/without config

- [x] 6. Integrate config with check_capacity_fit() (Requirements: 9)
  - Change function signature defaults from hardcoded values to None for: peak_to_avg_ratio, active_hours_per_day, active_days_per_month, max_tokens_setting
  - Add resolve_setting() calls at top of function body
  - With no config file, behavior identical to current defaults (3.0, 12, 22, 4096)
  - Explicit parameters still override config values
  - **Verify:** Run test group "test_capacity_integration" (Task 17) — confirms identical output with/without config

- [x] 7. Integrate config with calculate_agentcore_cost() (Requirements: 7)
  - Change function signature defaults to None for: num_vcpus, peak_memory_gb, io_wait_pct, idle_time_between_questions_s, stm_events_per_question, ltm_records_per_session, ltm_retrievals_per_question, tools_indexed
  - Add resolve_setting("agentcore_defaults", key) calls at top of function body
  - With no config file, behavior identical to current defaults
  - Explicit parameters still override config values
  - **Verify:** Run test group "test_agentcore_integration" (Task 17) — confirms identical output with/without config

- [x] 8. Integrate config with calculate_business_value() (Requirements: 8)
  - Change function signature defaults to None for: time_without_ai_min, time_with_ai_min, human_cost_per_hour, revenue_per_hour, agent_effectiveness_pct, efficiency_factor_pct, churn_without_ai_pct, churn_with_ai_pct, sales_increase_pct
  - Add resolve_setting("business_value_defaults", key) calls at top of function body
  - With no config file, behavior identical to current defaults
  - Explicit parameters still override config values
  - **Verify:** Run test group "test_business_value_integration" (Task 17) — confirms identical output with/without config

- [x] 9. Integrate config with check_pricing_data_status() and pricing cache functions (Requirements: 10)
  - Update check_pricing_data_status() to use pricing_cache.dir from config as default cache directory
  - Replace hardcoded 7-day stale threshold with pricing_cache.max_age_days from config
  - Update load_cache_file() to respect configured cache directory
  - Implement pricing_cache.auto_refresh behavior (attempt refresh when stale)
  - Fall back to ~/bedrock_cache/ if configured dir not accessible
  - Explicit cache_dir parameter still overrides config
  - **Verify:** Run test group "test_pricing_cache_integration" (Task 17) — confirms stale threshold respects config

- [x] 10. Integrate config with model resolution (model_preferences) (Requirements: 14)
  - Make model_preferences accessible via resolve_setting() for each role (router, general, rag, research)
  - resolve_setting("model_preferences", "version") returns "latest" by default
  - User-specified model in prompt always overrides config preferences
  - Emit warning if configured model not found in pricing cache
  - **Verify:** Run test group "test_model_preferences" (Task 17) — confirms role-based resolution

- [x] 11. Update bedrock-pricing SKILL.md (Requirements: 13)
  - Replace "Parameter Defaults" section with "Configuration" section
  - Add precedence chain explanation, --init-config reference, and "config values are defaults only" callout
  - Remove default value column from parameter tables; retain names and descriptions
  - Keep all existing workflow steps and critical rules unchanged

- [x] 12. Update bedrock-capacity SKILL.md (Requirements: 13)
  - Replace "Defaults" section with "Configuration" section
  - Add precedence chain, --init-config reference, and "config values are defaults only" callout
  - Remove default value column from defaults table; retain names and notes
  - Keep all existing workflow steps, key concepts, and optimization framework unchanged

- [x] 13. Update agentcore-pricing SKILL.md (Requirements: 13)
  - Replace "Defaults" section with "Configuration" section
  - Add precedence chain, --init-config reference, and "config values are defaults only" callout
  - Remove default value column from defaults table; retain names and notes
  - Keep all existing workflow steps and cache key references unchanged

- [x] 14. Update agent-business-value SKILL.md (Requirements: 13)
  - Replace "Defaults & Sources" section with "Configuration" section + "Research Sources" subsection
  - Add precedence chain, --init-config reference, and "config values are defaults only" callout
  - Preserve research citations (BCG, Harvard, Gartner) in separate subsection
  - Remove default value column from tables; retain names and source columns

- [x] 15. Update bedrock-tier-advisor SKILL.md (Requirements: 13)
  - Add "Configuration" section referencing config system
  - Reference model_preferences section for default model selection
  - Reference defaults.tier_preference for tier selection
  - Include "config values are defaults only" callout
  - Keep all existing workflow steps and decision framework unchanged

- [x] 16. Create test_config.py with comprehensive unit tests (Requirements: 1, 2, 3, 12, 13, 15)
  - Create `tco_bva_capacity_skills/tests/test_config.py` with all test groups defined below
  - Each test group has explicit input → expected output assertions
  - Tests cover: happy path, boundary conditions, error cases, type edge cases, SKILL.md structural integrity
  - Test runner prints per-group PASS/FAIL with assertion count
  - Tests are self-contained (create/cleanup temp files, mock env vars)
  - **Test groups and cases defined in detail below (see Notes section)**

- [x] 17. Run unit tests and fix any failures (Requirements: 1, 2, 3, 5, 6, 7, 8, 9, 10, 14, 15)
  - Execute `python3 tests/test_config.py` — all test groups must pass
  - Fix any failures found during test execution
  - Verify all boundary conditions and corner cases pass
  - Verify all precedence chain tests pass
  - Verify all validation/error handling tests pass

- [x] 18. Run full regression suite — existing test cases produce identical results (Requirements: all)
  - Run UC-01 through UC-09 from test_cases.md with NO config file present
  - Compare output against existing text_output/ files — must be byte-identical
  - Run UC-01 with a config file that sets agent_defaults.input_tokens=50 (matching UC-01's explicit value) — output must be identical (explicit wins)
  - Run UC-01 with a config file that sets agent_defaults.input_tokens=999 but UC-01 specifies 50 in prompt — output must use 50 (user prompt wins via explicit param)
  - Verify --init-config produces a valid YAML file that can be loaded without errors
  - Verify --init-config output, when fully uncommented, loads without validation warnings

## Task Dependency Graph

```json
{
  "waves": [
    {"tasks": [1]},
    {"tasks": [2, 4]},
    {"tasks": [3]},
    {"tasks": [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]},
    {"tasks": [17]},
    {"tasks": [18]}
  ]
}
```

```
Task 1 (schema + utilities)
  ├── Task 2 (load_config)
  │     └── Task 3 (resolve_setting)
  │           ├── Task 5 (integrate: agent session cost)
  │           ├── Task 6 (integrate: capacity fit)
  │           ├── Task 7 (integrate: agentcore cost)
  │           ├── Task 8 (integrate: business value)
  │           ├── Task 9 (integrate: pricing cache)
  │           └── Task 10 (integrate: model preferences)
  └── Task 4 (template generator + CLI)

Task 16 (create test file) — can be done in parallel with Tasks 5-15
Task 17 (run tests) — depends on Tasks 1-10 and 16
Task 18 (regression) — depends on Task 17 passing
Tasks 11-15 (SKILL.md updates) — independent, depend on Tasks 1-4
```

## Notes

### Test File Structure: tests/test_config.py

The test file contains these test groups with explicit expected values:

**Group 1: test_schema_and_utilities**
- `_deep_merge({"a": 1, "b": {"x": 1}}, {"b": {"x": 2, "y": 3}})` → `{"a": 1, "b": {"x": 2, "y": 3}}`
- `_deep_merge({}, {"a": 1})` → `{"a": 1}`
- `_deep_merge({"a": 1}, {})` → `{"a": 1}`
- `_deep_merge({"a": {"b": 1}}, {"a": None})` → `{"a": None}` (override with None replaces subtree)
- `_validate_config({"agent_defaults": {"input_tokens": "not_int"}}, "test.yaml")` → warnings list contains type error, value discarded
- `_validate_config({"agent_defaults": {"input_tokens": -5}}, "test.yaml")` → warnings list contains range error
- `_validate_config({"unknown_section": {"key": 1}}, "test.yaml")` → warnings list contains unknown section warning
- `_validate_config({"behavior": {"detail_level": "invalid"}}, "test.yaml")` → warnings list contains invalid choice
- `_validate_config({"capacity": {"peak_to_avg_ratio": 0.5}}, "test.yaml")` → warnings (below min 1.0)
- `_validate_config({"capacity": {"peak_to_avg_ratio": 101.0}}, "test.yaml")` → warnings (above max 100.0)
- `_validate_config({"capacity": {"active_hours_per_day": 25}}, "test.yaml")` → warnings (above max 24)
- `_read_yaml_file("/nonexistent/path.yaml")` → returns None, no exception
- `_read_yaml_file(empty_temp_file)` → returns None or empty dict, no exception
- CONFIG_SCHEMA has exactly 11 top-level sections
- Every key in CONFIG_SCHEMA has "type" and "default" fields

**Group 2: test_load_and_merge**
- No config files exist → `load_config()` returns `{}`
- User config only (with `agent_defaults.input_tokens: 200`) → `get_config("agent_defaults", "input_tokens")` == 200
- Project config only at `.bedrock_skills.yaml` (with `capacity.peak_to_avg_ratio: 5.0`) → `get_config("capacity", "peak_to_avg_ratio")` == 5.0
- Both configs: user has `agent_defaults.input_tokens: 200`, project has `agent_defaults.input_tokens: 300` → merged value is 300 (project wins)
- Both configs: user has `agent_defaults.input_tokens: 200`, project has `capacity.peak_to_avg_ratio: 5.0` → both values preserved in merge
- Empty file (0 bytes) → no error, returns empty config
- File with only comments → no error, returns empty config
- Invalid YAML syntax → warning emitted to stderr, returns None for that file, other file still loads
- `get_config()` with no args → returns full merged dict
- `get_config("nonexistent_section")` → returns None or empty dict
- `get_config("agent_defaults", "nonexistent_key")` → returns None
- PyYAML not installed (mocked) → returns empty dict, stderr contains install instructions

**Group 3: test_precedence_chain**
- `resolve_setting("agent_defaults", "input_tokens", explicit_value=500)` → 500 (explicit wins)
- `resolve_setting("agent_defaults", "input_tokens", explicit_value=0)` → 0 (explicit 0 is not None, wins)
- `resolve_setting("agent_defaults", "input_tokens", explicit_value=None)` with env `BEDROCK_AGENT_DEFAULTS_INPUT_TOKENS=300` → 300 (env wins)
- `resolve_setting("agent_defaults", "input_tokens")` with config value 200, no env → 200 (config wins)
- `resolve_setting("agent_defaults", "input_tokens")` with no config, no env → 100 (schema default)
- `resolve_setting("agent_defaults", "input_tokens", explicit_value=500)` with env=300, config=200 → 500 (explicit beats all)
- Env var `BEDROCK_AGENT_DEFAULTS_INPUT_TOKENS=not_a_number` → warning emitted, falls through to config/default
- Env var `BEDROCK_BEHAVIOR_SKIP_CONFIRMATION=true` → resolves to True (bool conversion)
- Env var `BEDROCK_BEHAVIOR_SKIP_CONFIRMATION=yes` → resolves to True
- Env var `BEDROCK_BEHAVIOR_SKIP_CONFIRMATION=1` → resolves to True
- Env var `BEDROCK_BEHAVIOR_SKIP_CONFIRMATION=false` → resolves to False
- Env var `BEDROCK_CAPACITY_PEAK_TO_AVG_RATIO=2.5` → resolves to 2.5 (float conversion)
- Empty env var `BEDROCK_DEFAULTS_REGION=""` → treated as not set, falls through to config/default

**Group 4: test_template_generation**
- `generate_config_template()` output contains all 11 section headers
- Output contains "reports:", "defaults:", "agent_defaults:", "rag_defaults:", "research_defaults:", "agentcore_defaults:", "business_value_defaults:", "capacity:", "pricing_cache:", "behavior:", "model_preferences:"
- All keys from CONFIG_SCHEMA appear as commented-out entries (prefixed with `# `)
- Each key has a type annotation comment
- Template is valid YAML when all comment prefixes are removed (uncommented)
- Uncommented template loads without validation warnings
- Template contains "Claude Opus" for model_preferences.router default
- Template contains "latest" for model_preferences.version default

**Group 5: test_agent_session_integration**
- Call `calculate_agent_session_compounded_cost()` with full explicit main_agent_config (all values specified) and no config file → capture result as "baseline"
- Load config with `agent_defaults.input_tokens: 999`, call with same explicit main_agent_config → result identical to baseline (explicit wins)
- Call with main_agent_config missing `input_tokens` key, config has `agent_defaults.input_tokens: 200` → result uses 200
- Call with main_agent_config missing `input_tokens` key, no config → result uses schema default 100
- Call with RAG sub-agent, token_params has `rag_n_chunks: 5` explicitly, config has `rag_defaults.rag_n_chunks: 20` → uses 5 (explicit wins)
- Call with RAG sub-agent, token_params does NOT specify `system_prompt_tokens`, config has `rag_defaults.system_prompt_tokens: 800` → uses 800 (config wins)
- Call with RAG sub-agent, token_params does NOT specify `system_prompt_tokens`, no config → uses 500 (hardcoded default)
- Call with research sub-agent, token_params has `n_research_iterations: 6` explicitly, config has `research_defaults.n_research_iterations: 8` → uses 6 (explicit wins)
- Call with research sub-agent, token_params does NOT specify `fetch_probability`, config has `research_defaults.fetch_probability: 0.8` → uses 0.8 (config wins)

**Group 6: test_capacity_integration**
- Call `check_capacity_fit()` with explicit `peak_to_avg_ratio=3.0, active_hours_per_day=12, active_days_per_month=22, max_tokens_setting=4096` and no config → capture as "baseline"
- Call with all params as None, no config → result identical to baseline (schema defaults match)
- Load config with `capacity.peak_to_avg_ratio: 5.0`, call with `peak_to_avg_ratio=None` → uses 5.0
- Load config with `capacity.peak_to_avg_ratio: 5.0`, call with `peak_to_avg_ratio=2.0` → uses 2.0 (explicit wins)

**Group 7: test_agentcore_integration**
- Call `calculate_agentcore_cost()` with all explicit params matching current defaults, no config → capture as "baseline"
- Call with all params as None, no config → result identical to baseline
- Load config with `agentcore_defaults.num_vcpus: 4`, call with `num_vcpus=None` → uses 4
- Load config with `agentcore_defaults.io_wait_pct: 0.5`, call with `io_wait_pct=None` → uses 0.5

**Group 8: test_business_value_integration**
- Call `calculate_business_value()` with all explicit params matching current defaults, no config → capture as "baseline"
- Call with all params as None, no config → result identical to baseline
- Load config with `business_value_defaults.human_cost_per_hour: 100`, call with `human_cost_per_hour=None` → uses 100

**Group 9: test_pricing_cache_integration**
- `check_pricing_data_status()` with no config → uses ~/bedrock_cache/ and 7-day threshold
- Load config with `pricing_cache.max_age_days: 14` → stale threshold is 14 days
- Load config with `pricing_cache.dir: /tmp/test_cache` → uses that directory
- Explicit `cache_dir="/other/path"` overrides config

**Group 10: test_model_preferences**
- `resolve_setting("model_preferences", "general")` with no config → "Claude Sonnet"
- `resolve_setting("model_preferences", "router")` with no config → "Claude Opus"
- `resolve_setting("model_preferences", "rag")` with no config → "Claude Haiku"
- `resolve_setting("model_preferences", "research")` with no config → "Nova Lite"
- `resolve_setting("model_preferences", "version")` with no config → "latest"
- Load config with `model_preferences.general: "Nova Pro"` → resolves to "Nova Pro"

**Group 11: test_boundary_conditions**
- `capacity.peak_to_avg_ratio: 1.0` (minimum) → accepted, no warning
- `capacity.peak_to_avg_ratio: 100.0` (maximum) → accepted, no warning
- `capacity.active_hours_per_day: 1` (minimum) → accepted
- `capacity.active_hours_per_day: 24` (maximum) → accepted
- `pricing_cache.max_age_days: 1` (minimum) → accepted
- `pricing_cache.max_age_days: 365` (maximum) → accepted
- `agentcore_defaults.io_wait_pct: 0.0` (minimum) → accepted
- `agentcore_defaults.io_wait_pct: 1.0` (maximum) → accepted
- `agent_defaults.input_tokens: 1` (minimum positive) → accepted
- `agent_defaults.tools_passed: 0` (minimum, zero allowed for tools) → accepted
- `reports.naming_template` with 128 chars → accepted
- `reports.naming_template` with 129 chars → warning, discarded

**Group 12: test_error_handling**
- Config with `agent_defaults.input_tokens: -1` → warning, value discarded, default 100 used
- Config with `agent_defaults.input_tokens: 0` → warning, value discarded (min is 1)
- Config with `behavior.detail_level: "verbose"` → warning (not in choices), default "summary" used
- Config with `defaults.history_mode: "partial"` → warning, default "full" used
- Config with `capacity.peak_to_avg_ratio: "fast"` → warning (type error), default 3.0 used
- Config with `agentcore_defaults.io_wait_pct: 1.5` → warning (above max 1.0), default 0.70 used
- Config with `agentcore_defaults.io_wait_pct: -0.1` → warning (below min 0.0), default 0.70 used
- Config with multiple errors → all errors reported (not just first)
- Config with `business_value_defaults.time_with_ai_min: 25` and `time_without_ai_min: 20` → warning (time_with >= time_without), but values used as-is

**Group 13: test_skill_md_updates**
- bedrock-pricing SKILL.md contains "## Configuration" section
- bedrock-pricing SKILL.md "Configuration" section contains "Config values are defaults only"
- bedrock-pricing SKILL.md "Configuration" section contains "--init-config"
- bedrock-pricing SKILL.md "Configuration" section contains "Precedence"
- bedrock-pricing SKILL.md does NOT contain a table header matching `| Default |` or `| Default|`
- bedrock-pricing SKILL.md still contains "## Critical Rules", "## Workflow", "## Output Structure"
- bedrock-capacity SKILL.md contains "## Configuration" section with same callouts
- bedrock-capacity SKILL.md does NOT contain `| Default |` table header
- bedrock-capacity SKILL.md still contains "## Key Concepts", "## When Workload Doesn't Fit"
- agentcore-pricing SKILL.md contains "## Configuration" section with same callouts
- agentcore-pricing SKILL.md does NOT contain `| Default |` table header
- agentcore-pricing SKILL.md still contains "## Cache Key Reference"
- agent-business-value SKILL.md contains "## Configuration" section with same callouts
- agent-business-value SKILL.md still contains "BCG" and "Harvard" and "Gartner" (research citations preserved)
- agent-business-value SKILL.md does NOT contain `| Default |` table header
- bedrock-tier-advisor SKILL.md contains "## Configuration" section
- bedrock-tier-advisor SKILL.md "Configuration" section references "model_preferences"
- bedrock-tier-advisor SKILL.md still contains "## Workflow", "## Lessons Learned"

### Regression Verification (Task 18)

Task 18 captures the output of UC-01 through UC-09 before and after the config changes:
1. Before: run all 9 use cases, save outputs to `tests/text_output_baseline/`
2. After: run all 9 use cases with NO config file, save to `tests/text_output_after/`
3. Diff: outputs must be identical (byte-for-byte for numeric values, allowing whitespace normalization)

This proves the config system is purely additive — zero behavioral change when no config is present.

### Implementation Notes

- Tasks 1-4 form the foundation and must be completed in order.
- Tasks 5-10 can be done in any order after Task 3 is complete.
- Tasks 11-15 can be done in any order after Tasks 1-4 are complete.
- Task 16 should be done after Tasks 1-4 so tests can reference the actual functions.
- Tasks 17-18 are verification — done last.
- No existing tests should break — the config system is purely additive with backward-compatible defaults.
- **`reports` and `behavior` sections are schema-only in this PR** — they appear in the template and are validated, but no task integrates them with actual functionality. They're reserved for Spec 2 (file-based report output).
- **`defaults.tier_preference` is schema-only** — no integration task in this PR. The tier advisor skill already has its own selection logic.
- **PyYAML is required** — no custom YAML parser. If PyYAML is missing, config degrades gracefully to hardcoded defaults.
