# Implementation Plan: Bedrock Capacity Planning Tool

## Overview

Extract the Bedrock Sizer's capacity planning logic from the Streamlit app into a single Strands `@tool` function (`calculator_capacity_planning.py`), wire it into the TCO/BVA agent, update the system prompt, and remove the original Streamlit code. All logic lives in one file following the existing calculator pattern.

## Tasks

- [x] 1. Create `calculator_capacity_planning.py` with S3 data loading and column normalization
  - [x] 1.1 Create `calculator_capacity_planning.py` with module-level imports, logger, cache variable, env var reading, `_read_csv_from_s3()`, `_get_region_mapping()`, and `_load_model_data()` that loads all five CSV datasets from S3 into the module-level `_model_data_cache` dict
    - Import boto3, pandas, os, logging, math, io.StringIO
    - Read `BEDROCK_SIZER_S3_BUCKET` and CSV key env vars with defaults from design
    - `_read_csv_from_s3(bucket, key, skiprows=None)` returns DataFrame or None on error
    - `_get_region_mapping()` returns the 26-region code-to-display-name dict
    - `_load_model_data()` loads on_demand, provisioned, embedding, image, video CSVs and stores in `_model_data_cache` with `region_mapping`
    - _Requirements: 1.1, 8.1, 8.2, 8.3, 8.4_
  - [x] 1.2 Implement `_normalize_columns()`, `_normalize_embedding_columns()`, `_normalize_image_columns()`, `_normalize_video_columns()` following the column mapping schemas from the design
    - Map source column names to standardized names per the design's Normalized Column Schemas
    - Clean numeric values: strip `$`, commas, whitespace; use `pd.to_numeric(errors='coerce').fillna(0)`
    - Handle boolean `cris_only` column
    - Call appropriate normalizer after each CSV load in `_load_model_data()`
    - _Requirements: 1.2, 1.7_
  - [ ]* 1.3 Write property test for column normalization
    - **Property 8: Column Normalization Produces Expected Schema**
    - **Validates: Requirements 1.2**

- [x] 2. Implement model lookup and capacity calculation helpers
  - [x] 2.1 Implement `_find_matching_model(df, model_name, region_display, cris_flag)` with multi-strategy matching (exact, case-insensitive, contains) and `_get_model_limits(model_name, region, model_type, cris_flag)` that returns pricing/limit dict or `not_found` flag
    - `_find_matching_model` tries exact match on model_name column, then case-insensitive, then substring contains, with optional CRIS filtering
    - `_get_model_limits` resolves region code to display name, selects correct DataFrame by model_type, calls `_find_matching_model`, returns dict with pricing and limit fields
    - Return `{"not_found": True}` when model is not found
    - _Requirements: 1.3, 1.4, 1.5, 1.6_
  - [ ]* 2.2 Write property test for multi-strategy model matching
    - **Property 9: Multi-Strategy Model Matching**
    - **Validates: Requirements 1.3, 1.6**
  - [x] 2.3 Implement `_calculate_capacity(params, model_limits)` that computes required RPM/TPM for steady-state and peak-state, compares against model limits, and returns capacity analysis dict with sufficiency flags and utilization percentages
    - Steady-state RPM = `(users * requests_per_hour) / 60`
    - Steady-state TPM = `RPM * (avg_input_tokens + avg_output_tokens)`
    - Same formulas for peak-state when peak params > 0
    - Compare against `model_max_rpm` and `model_max_tpm`, set `rpm_sufficient` and `tpm_sufficient` booleans
    - Calculate utilization as `(required / max) * 100`
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7_
  - [ ]* 2.4 Write property tests for RPM/TPM calculation and capacity sufficiency
    - **Property 1: RPM and TPM Calculation Correctness**
    - **Property 2: Capacity Sufficiency Comparison**
    - **Validates: Requirements 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7**

- [x] 3. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 4. Implement cost estimation, provisioned comparison, and alternative model discovery
  - [x] 4.1 Implement `_estimate_monthly_cost(params, model_limits, model_type)` that calculates monthly token volumes and costs for on-demand, embedding, image, and video model types
    - On-demand: `input_cost = (monthly_input_tokens / 1_000_000) * price_per_million_input_tokens`, same for output
    - Monthly tokens = `users * requests_per_hour * usage_hours * usage_days * avg_tokens`
    - Image: `price_per_image * total_monthly_images`
    - Video: `price_per_second * total_monthly_video_seconds`
    - Embedding: `price_per_million_tokens` based cost
    - Return cost breakdown with input_cost, output_cost, total_monthly_cost for both steady and peak states
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 9.1, 9.2, 9.3, 9.4_
  - [ ]* 4.2 Write property tests for monthly cost invariant and token volume formula
    - **Property 3: Monthly Cost Invariant**
    - **Property 4: Monthly Token Volume Formula**
    - **Property 10: Model-Type-Specific Cost Formulas**
    - **Validates: Requirements 3.1, 3.2, 3.3, 3.4, 9.1, 9.2, 9.3**
  - [x] 4.3 Implement `_compare_provisioned(required_tpm, model_limits)` that calculates provisioned units needed and commitment tier costs
    - Units = `ceil(required_TPM / provisioned_peak_TPM_per_unit)`
    - Monthly cost per tier = `hourly_rate * 24 * 30 * units_needed`
    - Return None if provisioned data unavailable or peak TPM is 0
    - Include on-demand vs provisioned savings comparison
    - _Requirements: 4.1, 4.2, 4.3, 4.4_
  - [ ]* 4.4 Write property test for provisioned throughput calculation
    - **Property 5: Provisioned Throughput Calculation**
    - **Validates: Requirements 4.1, 4.2**
  - [x] 4.5 Implement `_find_alternatives(region, required_rpm, required_tpm, model_type, cris_flag)` that searches for alternative models meeting capacity requirements, sorted by price ascending
    - Filter cached DataFrame for models in the same region where `max_rpm >= required_rpm` and (for on-demand) `max_tpm >= required_tpm`
    - Sort by input token price ascending
    - Return list of dicts with model_name, max_rpm, max_tpm, and pricing fields
    - Return empty list when no alternatives meet requirements
    - _Requirements: 5.1, 5.2, 5.3, 5.4_
  - [ ]* 4.6 Write property tests for alternative model filtering and sorting
    - **Property 6: Alternative Models Meet Capacity Requirements**
    - **Property 7: Alternative Models Sorted by Price**
    - **Validates: Requirements 5.1, 5.2, 5.3, 5.4**

- [x] 5. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 6. Implement the main `capacity_planning_calculator` @tool function
  - [x] 6.1 Implement `capacity_planning_calculator(params: dict) -> dict` decorated with `@tool`, with comprehensive docstring, parameter extraction with defaults, error handling, and orchestration of all helper functions
    - Validate `model_name` is present, return error dict if missing
    - Check `BEDROCK_SIZER_S3_BUCKET` env var, return error dict if missing
    - Call `_load_model_data()` (uses cache), `_get_model_limits()`, `_calculate_capacity()`, `_estimate_monthly_cost()`, `_compare_provisioned()`, `_find_alternatives()`
    - Handle model not found: add warning, still search alternatives
    - Handle all model types: on_demand, embedding, image, video, provisioned
    - Return structured dict matching the output schema from the design
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6_

- [x] 7. Wire tool into agent and update system prompt
  - [x] 7.1 Add `from calculator_capacity_planning import capacity_planning_calculator` to `aws_tco_bva_analyst.py` and add `capacity_planning_calculator` to the tools list in `_create_agent()`
    - _Requirements: 7.1_
  - [x] 7.2 Append capacity planning guidance section to `TCO_ANALYST_PROMPT` in `system_prompt.py` describing when to use the tool, expected input parameters with defaults, and instructions to gather model name, region, user count, and request volume before invoking
    - _Requirements: 7.2, 7.3, 7.4_

- [x] 8. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 9. Remove original Streamlit Bedrock Sizer directory
  - [x] 9.1 Delete the entire `usecases/mcp-aws-cost-biz-value-analysis-agent/agents/bedrock_sizer/` directory since all logic has been extracted into `calculator_capacity_planning.py`
    - Verify no remaining imports reference `bedrock_sizer` in the codebase before deleting

- [x] 10. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties from the design document
- Unit tests validate specific examples and edge cases
- All code is Python, following the existing `calculator_bedrock.py` pattern
- Property tests use `hypothesis` library with `@settings(max_examples=100)`
- S3 calls should be mocked in tests using `unittest.mock`
