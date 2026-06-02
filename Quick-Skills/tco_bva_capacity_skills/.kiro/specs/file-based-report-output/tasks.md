# Implementation Plan: File-Based Report Output

## Overview

Implement file-based report output in 10 tasks. The function `calculate_agent_session_compounded_cost()` loses its `detail_level` parameter and always: (1) computes full detail internally, (2) writes the report to a file, (3) returns a compact summary with `capacity_profile` and `file_path`.

## Tasks

- [ ] 1. Add helper functions: _sanitize_filename, _format_volume, _generate_report_path (Requirements: 4)
  - Implement `_sanitize_filename(name)` — lowercase, replace non-alphanumeric with hyphens, collapse multiples, handle empty string
  - Implement `_format_volume(sessions_per_month)` — "10k-sessions", "1m-sessions", "500-sessions"
  - Implement `_generate_report_path(model_name, sessions_per_month, output_dir, output_path)` — resolves full path using config
  - Add `import re` at top of config section (if not already present)
  - Place after config system functions, before `classify_provider()`
  - **Verify:** `_sanitize_filename("Claude Sonnet 4.6")` → `"claude-sonnet-4.6"`, `_format_volume(10000)` → `"10k-sessions"`

- [ ] 2. Add _build_front_matter function (Requirements: 5)
  - Implement `_build_front_matter(result, main_agent_config)` — generates YAML front-matter string
  - Include: generated_at, model, region, sessions_per_month, session_total, monthly_total, annual_total, savings_pct, inputs_hash
  - `inputs_hash` is SHA-256 of JSON-serialized main_agent_config (first 16 chars)
  - Returns empty string if `reports.include_metadata` config is False
  - **Verify:** Output starts with `---\n`, ends with `---\n`, contains all required keys

- [ ] 3. Add _write_report_to_file and _try_write functions (Requirements: 3, 5, 8)
  - Implement `_write_report_to_file(result, main_agent_config, subagents, output_path)` with cascade logic
  - Implement `_try_write(file_path, content)` — atomic write (temp file + os.rename)
  - Cascade: try configured/explicit path → try default dir (`~/bedrock_reports/`) → return None
  - Creates output directory if needed
  - Calls `_build_front_matter()` + `_format_full_output()` to build content
  - On `_format_full_output()` failure: fallback to token_table + JSON explanation
  - On any OSError: warning to stderr, try next in cascade
  - If `reports.auto_cleanup` is True, call `_cleanup_old_reports()` after successful write
  - **Verify:** File exists at returned path, content starts with front-matter, atomic write prevents partial files

- [ ] 4. Add _build_compact_summary and _identify_top_cost_driver (Requirements: 2)
  - Implement `_build_compact_summary(result, file_path)` — returns dict with required keys
  - Implement `_identify_top_cost_driver(result)` — returns string identifying largest cost component
  - Summary keys: file_path, sessions_per_month, monthly_total, annual_total, session_total, session_total_no_cache, savings_pct, main_agent_session_cost, subagent_session_cost, recommended_ttl, top_cost_driver, capacity_profile
  - Use safe nested access pattern for main agent cost (prompt-cache-aware: try `with_cache` then `no_cache`)
  - **Verify:** Summary dict has all required keys, capacity_profile is present

- [ ] 5. Modify calculate_agent_session_compounded_cost — remove detail_level, add file output (Requirements: 1, 9)
  - Remove `detail_level` parameter from function signature
  - Add `output_path=None` parameter
  - Remove the existing `if detail_level == "summary": return {...}` block at the end
  - Remove the `detail_level` forwarding to sub-functions (they keep their own internal detail_level="full")
  - At end of function: call `_write_report_to_file(result, ...)`
    - If write succeeds: return `_build_compact_summary(result, file_path)`
    - If write fails (returns None): add `_file_write_failed: True` to result, return full result dict inline
  - The internal `result` dict computation is unchanged — same token math, same cost calculations
  - **Verify:** Function returns compact summary with file_path when write succeeds; returns full dict with `_file_write_failed` when write fails

- [ ] 6. Add _cleanup_old_reports function and --cleanup-reports CLI (Requirements: 7)
  - Implement `_cleanup_old_reports(output_dir=None, max_age_days=None)`
  - Resolves dir and threshold from config if not provided
  - Derives filename match pattern from `reports.naming_template` (adapts if template changes)
  - Only deletes files matching the pattern — not arbitrary .md files
  - Returns `{"deleted_count": int, "freed_bytes": int}`
  - Add `--cleanup-reports` argument to main() parser
  - Handler calls `_cleanup_old_reports()` and prints summary
  - **Verify:** `python3 bedrock_pricing.py --cleanup-reports` runs without error; only matching files are deleted

- [ ] 7. Update internal callers and remove dead config (Requirements: 9, 11)
  - Search for any internal calls that pass `detail_level` to `calculate_agent_session_compounded_cost` — remove that argument
  - Update `_format_full_output()` if it's called externally with the result — it's now called internally only
  - Remove `behavior.detail_level` from CONFIG_SCHEMA (dead config after this change)
  - Update `generate_config_template()` output — `behavior` section will have one fewer key
  - Verify `calculate_main_agent_compounded_cost()` still accepts `detail_level` (it's a separate function, unchanged)
  - Verify sub-agent token functions still accept `detail_level` (unchanged)
  - **Verify:** Script loads without errors, no references to removed parameter in main function, CONFIG_SCHEMA behavior section has 2 keys (skip_confirmation, auto_capacity_check)

- [ ] 8. Update bedrock-pricing SKILL.md (Requirements: 10)
  - Remove all `detail_level` references from code examples
  - Remove "Summary mode" vs "Full mode" distinction in Output Structure
  - Add "## Report Output" section explaining file-based workflow
  - Show example of compact summary dict returned to agent
  - Update workflow step 6 (Calculate Cost) — remove detail_level from examples
  - Update workflow step 7 (Present Results) — explain file_path + summary workflow
  - Document `--cleanup-reports` CLI flag
  - Document `output_path` parameter for explicit path control
  - **Verify:** No `detail_level` in SKILL.md, "## Report Output" section exists

- [ ] 9. Create tests/test_report_output.py (Requirements: 11)
  - Group 1: Helper functions (_sanitize_filename, _format_volume, _generate_report_path)
  - Group 2: Front-matter generation (_build_front_matter)
  - Group 3: File writing (_write_report_to_file — success, failure, directory creation)
  - Group 4: Compact summary (_build_compact_summary — all keys present, capacity_profile included)
  - Group 5: Integration (calculate_agent_session_compounded_cost returns summary with file_path)
  - Group 6: Cleanup (_cleanup_old_reports — deletes old, preserves new)
  - **Verify:** All test groups pass

- [ ] 10. Run full test suite and verify (Requirements: 11)
  - Run `tests/test_config.py` — update any assertions that reference `detail_level` or old return structure
  - Run `tests/test_report_output.py` — all new tests pass
  - Verify capacity_profile in summary works with check_capacity_fit()
  - Clean up any temp report files created during testing
  - **Verify:** Both test files pass with 0 failures

## Task Dependency Graph

```json
{
  "waves": [
    {"tasks": [1, 2, 4]},
    {"tasks": [3]},
    {"tasks": [5, 6]},
    {"tasks": [7, 8, 9]},
    {"tasks": [10]}
  ]
}
```

```
Tasks 1, 2, 4 (helpers — independent)
  └── Task 3 (_write_report_to_file — depends on 1, 2)
        ├── Task 5 (modify main function — depends on 3, 4)
        └── Task 6 (cleanup — depends on 3)

Task 7 (update internal callers — depends on 5)
Task 8 (SKILL.md — depends on 5)
Task 9 (tests — depends on 5, 6)
Task 10 (regression — depends on 7, 8, 9)
```

## Notes

### What changes for the agent (SKILL.md perspective)

**Before:**
```python
# Agent had to choose detail_level
result = calculate_agent_session_compounded_cost(
    main_agent_config={...},
    detail_level="full",  # or "summary"
)
# Full mode: result is huge dict with token_table, per-cycle data, etc.
# Summary mode: result is small dict but no file
```

**After:**
```python
# No choice needed — always get summary + file
result = calculate_agent_session_compounded_cost(
    main_agent_config={...},
)
# result = {
#     "file_path": "/Users/x/bedrock_reports/claude-sonnet-4.6_10k-sessions_20260526-143022.md",
#     "sessions_per_month": 10000,
#     "monthly_total": 3247.48,
#     "annual_total": 38969.70,
#     "session_total": 0.324748,
#     "savings_pct": 60.9,
#     "recommended_ttl": "5min",
#     "top_cost_driver": "main agent (token compounding)",
#     "capacity_profile": {...},  # pass directly to check_capacity_fit()
#     ...
# }

# Capacity planning — unchanged:
cap_result = check_capacity_fit(
    capacity_profile=result["capacity_profile"]["main_agent"],
    questions_per_month=50000,
    tier_limits=tier_limits,
)
```

### Impact on test_config.py

The existing `test_config.py` Group 5 (Agent Session Integration) calls `calculate_rag_subagent_tokens()` and `calculate_research_subagent_tokens()` directly — those are unchanged. It does NOT call `calculate_agent_session_compounded_cost()` directly, so no changes needed there.

However, if any test references `detail_level` in the context of `calculate_agent_session_compounded_cost`, it needs updating. Check during Task 10.
