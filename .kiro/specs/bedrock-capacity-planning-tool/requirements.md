# Requirements Document

## Introduction

This feature extracts the core Bedrock capacity planning logic from the existing Streamlit-based Bedrock Sizer application (`bedrock_sizer/`) and integrates it into the TCO/BVA analyst agent (`aws_tco_bva_analyst.py`) as a Strands `@tool` function. The tool enables the agent to perform Bedrock model capacity sizing, compare on-demand vs provisioned throughput pricing, find alternative models, and provide capacity increase recommendations — all without requiring the Streamlit UI.

## Glossary

- **Capacity_Planning_Tool**: The new Strands `@tool` function that performs Bedrock capacity planning calculations
- **TCO_Agent**: The existing AWS TCO & Business Value Analyst Agent (`aws_tco_bva_analyst.py`) that orchestrates cost analysis
- **Model_Data_Manager**: The refactored service class that loads and queries Bedrock model pricing/limits data from S3 CSV files
- **RPM**: Requests Per Minute — the rate limit for API calls to a Bedrock model
- **TPM**: Tokens Per Minute — the throughput limit for token processing by a Bedrock model
- **CRIS**: Cross-Region Inference Service — enables invoking Bedrock models across AWS regions
- **Provisioned_Throughput**: A Bedrock pricing model where dedicated capacity is reserved with commitment-based pricing
- **On_Demand**: The default Bedrock pricing model where usage is billed per token without capacity reservation
- **Steady_State_Usage**: Normal, day-to-day usage pattern with typical user counts and request volumes
- **Peak_State_Usage**: High-traffic usage pattern representing maximum expected load
- **S3_CSV_Data**: CSV files stored in S3 containing Bedrock model pricing, limits, and availability data

## Requirements

### Requirement 1: Extract and Refactor Model Data Manager

**User Story:** As a developer, I want the model data loading and lookup logic extracted from the Streamlit app into a standalone module, so that it can be used by the Strands tool without Streamlit dependencies.

#### Acceptance Criteria

1. THE Model_Data_Manager SHALL load on-demand, provisioned, embedding, image, and video model data from S3 CSV files using boto3 and pandas
2. THE Model_Data_Manager SHALL normalize CSV column names and clean numeric values into a consistent internal format
3. WHEN a model name and region are provided, THE Model_Data_Manager SHALL return pricing and limit data (RPM, TPM, per-token costs) for that model
4. WHEN a model name is not found in the specified region, THE Model_Data_Manager SHALL set a `not_found` flag in the returned dictionary
5. THE Model_Data_Manager SHALL map AWS region codes (e.g., `us-east-1`) to display names (e.g., `N.Virginia`) for CSV data lookups
6. THE Model_Data_Manager SHALL use a multi-strategy matching approach (exact match, case-insensitive match, contains match) with optional CRIS filtering to find models
7. THE Model_Data_Manager SHALL have zero dependencies on Streamlit or any UI framework

### Requirement 2: Capacity Calculation Logic

**User Story:** As a sales team member, I want the tool to calculate required RPM and TPM from usage parameters, so that I can determine if a Bedrock model has sufficient capacity for a workload.

#### Acceptance Criteria

1. WHEN steady-state usage parameters (users, requests per hour, usage hours, input/output tokens) are provided, THE Capacity_Planning_Tool SHALL calculate the required RPM as `(users * requests_per_hour) / 60`
2. WHEN steady-state usage parameters are provided, THE Capacity_Planning_Tool SHALL calculate the required TPM as `required_RPM * (avg_input_tokens + avg_output_tokens)`
3. WHEN peak-state usage parameters are provided, THE Capacity_Planning_Tool SHALL calculate peak RPM and peak TPM using the same formulas with peak values
4. WHEN both steady-state and peak-state calculations are complete, THE Capacity_Planning_Tool SHALL compare required RPM and TPM against the model limits retrieved from Model_Data_Manager
5. WHEN required RPM exceeds the model max RPM, THE Capacity_Planning_Tool SHALL flag the RPM as insufficient and include the deficit in the response
6. WHEN required TPM exceeds the model max TPM, THE Capacity_Planning_Tool SHALL flag the TPM as insufficient and include the deficit in the response
7. WHEN all capacity checks pass, THE Capacity_Planning_Tool SHALL report the capacity as sufficient with utilization percentages

### Requirement 3: Monthly Cost Estimation

**User Story:** As a sales team member, I want the tool to estimate monthly costs for on-demand usage, so that I can include cost projections in customer proposals.

#### Acceptance Criteria

1. WHEN on-demand model pricing and usage parameters are provided, THE Capacity_Planning_Tool SHALL calculate monthly input token cost as `(monthly_input_tokens / 1,000,000) * price_per_million_input_tokens`
2. WHEN on-demand model pricing and usage parameters are provided, THE Capacity_Planning_Tool SHALL calculate monthly output token cost as `(monthly_output_tokens / 1,000,000) * price_per_million_output_tokens`
3. THE Capacity_Planning_Tool SHALL calculate monthly token volumes from steady-state and peak-state parameters using `users * requests_per_hour * usage_hours * usage_days * avg_tokens`
4. THE Capacity_Planning_Tool SHALL return a cost breakdown including input cost, output cost, and total monthly cost

### Requirement 4: Provisioned Throughput Comparison

**User Story:** As a sales team member, I want the tool to compare on-demand vs provisioned throughput pricing, so that I can recommend the most cost-effective option to customers.

#### Acceptance Criteria

1. WHEN a model has provisioned throughput data available, THE Capacity_Planning_Tool SHALL calculate the number of provisioned model units needed as `ceil(required_TPM / provisioned_peak_TPM_per_unit)`
2. WHEN provisioned throughput data is available, THE Capacity_Planning_Tool SHALL calculate monthly costs for no-commitment, 1-month commitment, and 6-month commitment tiers as `hourly_rate * 24 * 30 * units_needed`
3. WHEN both on-demand and provisioned costs are calculated, THE Capacity_Planning_Tool SHALL include both pricing options in the response for comparison
4. IF provisioned throughput data is not available for the requested model, THEN THE Capacity_Planning_Tool SHALL omit the provisioned section and return only on-demand pricing

### Requirement 5: Alternative Model Discovery

**User Story:** As a sales team member, I want the tool to suggest alternative models that meet capacity requirements, so that I can present customers with multiple options.

#### Acceptance Criteria

1. WHEN capacity analysis is performed, THE Capacity_Planning_Tool SHALL search for alternative models in the same region that meet the required RPM and TPM
2. THE Capacity_Planning_Tool SHALL return alternative models sorted by input token price (lowest cost first)
3. THE Capacity_Planning_Tool SHALL include each alternative model's name, max RPM, max TPM, and pricing in the response
4. WHEN no alternative models meet the capacity requirements, THE Capacity_Planning_Tool SHALL return an empty alternatives list

### Requirement 6: Strands Tool Integration

**User Story:** As a developer, I want the capacity planning logic exposed as a Strands `@tool` function, so that the TCO agent can invoke it like the existing calculator tools.

#### Acceptance Criteria

1. THE Capacity_Planning_Tool SHALL be decorated with `@tool` from the `strands` package and accept a single `dict` parameter
2. THE Capacity_Planning_Tool SHALL accept the following input parameters: `model_name` (required), `region` (default: `us-east-1`), `model_type` (default: `on_demand`), `cris_flag` (default: `true`), steady-state usage fields, and optional peak-state usage fields
3. THE Capacity_Planning_Tool SHALL return a `dict` containing: capacity analysis (required RPM/TPM, model limits, sufficiency flags), cost estimation (on-demand monthly costs), provisioned throughput comparison (when available), and alternative models list
4. IF a required parameter `model_name` is missing, THEN THE Capacity_Planning_Tool SHALL return a `dict` with an `error` key describing the missing parameter
5. THE Capacity_Planning_Tool SHALL include a comprehensive docstring describing all input parameters and output structure, following the pattern used by `use_bedrock_calculator`
6. THE Capacity_Planning_Tool SHALL support all model types: on_demand, embedding, image, video, and provisioned

### Requirement 7: Agent Wiring and System Prompt Update

**User Story:** As a developer, I want the new tool registered in the TCO agent and the system prompt updated, so that the agent knows when and how to use the capacity planning tool.

#### Acceptance Criteria

1. THE TCO_Agent SHALL include the Capacity_Planning_Tool in its tools list alongside the existing calculator tools
2. THE TCO_Agent system prompt SHALL include guidance describing when to use the Capacity_Planning_Tool (capacity planning, sizing, quota sufficiency questions)
3. THE TCO_Agent system prompt SHALL describe the input parameters the Capacity_Planning_Tool expects, with default values
4. THE TCO_Agent system prompt SHALL instruct the agent to ask users for model name, region, user count, and request volume before invoking the Capacity_Planning_Tool

### Requirement 8: S3 Data Source Configuration

**User Story:** As a developer, I want the S3 bucket and CSV file paths to be configurable via environment variables, so that the tool can work across different deployment environments.

#### Acceptance Criteria

1. THE Capacity_Planning_Tool SHALL read the S3 bucket name from the `BEDROCK_SIZER_S3_BUCKET` environment variable
2. THE Capacity_Planning_Tool SHALL read CSV file key prefixes from environment variables with sensible defaults
3. IF the S3 bucket environment variable is not set, THEN THE Capacity_Planning_Tool SHALL return an error indicating the missing configuration
4. IF an S3 CSV file fails to load, THEN THE Capacity_Planning_Tool SHALL log the error and continue with available data, returning a warning in the response

### Requirement 9: Image and Video Model Support

**User Story:** As a sales team member, I want the tool to handle image generation and video generation models, so that I can size capacity for non-text workloads.

#### Acceptance Criteria

1. WHEN `model_type` is `image`, THE Capacity_Planning_Tool SHALL calculate required RPM from `images_per_minute` and estimate monthly cost using `price_per_image * total_monthly_images`
2. WHEN `model_type` is `video`, THE Capacity_Planning_Tool SHALL calculate required RPM from `videos_per_hour / 60` and estimate monthly cost using `price_per_second * total_monthly_video_seconds`
3. WHEN `model_type` is `embedding`, THE Capacity_Planning_Tool SHALL calculate costs using `price_per_million_tokens` and check both RPM and TPM limits
4. THE Capacity_Planning_Tool SHALL return model-type-specific fields in the response (e.g., `price_per_image` for image models, `price_per_second` for video models)
