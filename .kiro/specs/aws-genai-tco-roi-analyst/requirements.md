# Requirements Document

## Introduction

This document specifies the requirements for the AWS GenAI TCO & ROI Analyst — a full-stack chat agent solution that analyzes business use cases, estimates Amazon Bedrock and Amazon Bedrock AgentCore costs, advises on Bedrock LLM capacity planning, and provides Total Cost of Ownership (TCO) and Return on Investment (ROI) analysis. The solution comprises four components: a Strands-based AI agent deployed on AgentCore, a CloudFormation infrastructure stack, a React chatbot UI, and automated data scrapers that feed a Bedrock Knowledge Base.

## Glossary

- **Analyst_Agent**: The Strands Agent deployed on Amazon Bedrock AgentCore that processes user queries, invokes calculator tools, searches pricing/quota data, and returns structured JSON cost analysis responses.
- **Chatbot_UI**: The React web application using Cloudscape Design components that provides the user-facing chat interface, session management, authentication, and data visualization.
- **Proxy_Server**: The Express.js backend server that discovers the AgentCore runtime, invokes the agent, persists chat history to DynamoDB, and detects chart keywords in prompts.
- **Infrastructure_Stack**: The CloudFormation template that provisions all AWS resources including S3 buckets, OpenSearch Serverless, Bedrock Knowledge Base, DynamoDB tables, CloudFront, Cognito Identity Pool, and IAM roles.
- **Pricing_Scraper**: The Python script that fetches the AWS pricing index and saves product pricing files organized by service, region, and product into an S3-uploadable directory structure.
- **Quota_Scraper**: The Python script that collects Bedrock RPM and TPM quotas from the AWS Service Quotas API across specified regions and saves them as text files.
- **Knowledge_Base**: The Amazon Bedrock Knowledge Base backed by OpenSearch Serverless that stores and retrieves pricing documents and quota data via vector search.
- **Bedrock_Calculator**: The tool that calculates monthly Bedrock LLM costs for multi-model configurations including vector database, tool usage, system prompt, and conversation history token accounting.
- **AgentCore_Calculator**: The tool that calculates monthly AgentCore costs across runtime, browser, code interpreter, gateway, and memory components.
- **BVA_Calculator**: The Business Value Analysis tool that calculates cost savings, revenue growth, customer churn reduction, implementation costs, and ROI metrics.
- **Capacity_Planner**: The pure computation tool that performs Bedrock model capacity planning including RPM/TPM analysis, cost estimation, and provisioned throughput comparison.
- **Pricing_Search_Tool**: The tool that queries the Knowledge Base for AWS pricing data filtered by pricing_data path and region.
- **Quota_Search_Tool**: The tool that queries the Knowledge Base for Bedrock RPM/TPM quota data filtered by quota_data path and region.
- **MCP_Client**: The Model Context Protocol client that connects to the AWS Knowledge MCP server for documentation lookups.
- **Session**: A conversation between a user and the Analyst_Agent, identified by a userId and sessionId, persisted in DynamoDB.
- **Scraper_Schedule**: An EventBridge scheduled rule that triggers both scrapers weekly (Sunday 1 AM PST) via a Lambda function, which runs the scrapers, syncs output to S3, and triggers a Knowledge Base sync.

## Requirements

### Requirement 1: Agent Initialization and Deployment

**User Story:** As a platform operator, I want the Analyst Agent to be deployable on Amazon Bedrock AgentCore using the Strands framework, so that it can serve cost analysis requests at scale.

#### Acceptance Criteria

1. THE Analyst_Agent SHALL initialize using `BedrockAgentCoreApp` with an async entrypoint that accepts a payload containing a `prompt` field.
2. THE Analyst_Agent SHALL load a configurable Bedrock model via the `MODEL_ID` environment variable, defaulting to Claude Sonnet 4.5.
3. THE Analyst_Agent SHALL register all calculator tools (Bedrock_Calculator, AgentCore_Calculator, BVA_Calculator, Capacity_Planner), both search tools (Pricing_Search_Tool, Quota_Search_Tool), and MCP_Client tools at startup.
4. WHEN the Analyst_Agent receives a payload without a `prompt` field, THE Analyst_Agent SHALL return an error message indicating the missing prompt.
5. THE Analyst_Agent SHALL stream responses asynchronously via `stream_async` to the caller.
6. IF a throttling or service unavailable error occurs during agent invocation, THEN THE Analyst_Agent SHALL retry with exponential backoff up to 3 times before returning the error.

### Requirement 2: Pricing Data Retrieval

**User Story:** As a cost analyst, I want the agent to retrieve real-time AWS pricing data from the Knowledge Base, so that cost estimates are based on actual published prices rather than stale training data.

#### Acceptance Criteria

1. WHEN the Pricing_Search_Tool receives a query and target region, THE Pricing_Search_Tool SHALL search the Knowledge_Base using hybrid vector search filtered to documents under the `/pricing_data/` path and the specified region path.
2. THE Pricing_Search_Tool SHALL return up to 15 results with a minimum relevance score of 0.2.
3. THE Pricing_Search_Tool SHALL format each result with a document number, relevance score, content text, and source URI.
4. IF the Knowledge_Base returns no results matching the filters, THEN THE Pricing_Search_Tool SHALL return a message stating no pricing information was found.
5. IF an error occurs during Knowledge_Base retrieval, THEN THE Pricing_Search_Tool SHALL return the error message to the caller.

### Requirement 3: Quota Data Retrieval

**User Story:** As a capacity planner, I want the agent to retrieve Bedrock model RPM and TPM quota data from the Knowledge Base, so that capacity planning uses actual quota limits.

#### Acceptance Criteria

1. WHEN the Quota_Search_Tool receives a query and target region, THE Quota_Search_Tool SHALL search the Knowledge_Base using hybrid vector search filtered to documents under the `/quota_data/` path and the specified region path.
2. THE Quota_Search_Tool SHALL return up to 10 results with a minimum relevance score of 0.2.
3. THE Quota_Search_Tool SHALL format each result with a result number, relevance score, content text, and source URI.
4. IF the Knowledge_Base returns no results, THEN THE Quota_Search_Tool SHALL return a message stating no quota information was found.

### Requirement 4: Bedrock LLM Cost Calculation

**User Story:** As a solutions architect, I want to calculate monthly Bedrock costs for multi-model configurations, so that I can estimate the total LLM spend for a use case.

#### Acceptance Criteria

1. WHEN the Bedrock_Calculator receives parameters with `questions_per_month` and one or more model configurations, THE Bedrock_Calculator SHALL calculate per-model and total monthly costs.
2. THE Bedrock_Calculator SHALL account for input tokens from user queries, system prompt tokens, conversation history tokens, vector database retrieval tokens, and tool description and result tokens.
3. THE Bedrock_Calculator SHALL account for output tokens from model responses and tool invocation requests.
4. THE Bedrock_Calculator SHALL distribute questions across models according to `percent_questions_for_model`, defaulting to equal distribution when not specified.
5. IF model percentage allocations exceed 100%, THEN THE Bedrock_Calculator SHALL include a warning in the results.
6. IF model percentage allocations sum to less than 100%, THEN THE Bedrock_Calculator SHALL include an informational note about unallocated questions.
7. THE Bedrock_Calculator SHALL return step-by-step calculation explanations for each model showing the formula and intermediate values.
8. IF a required parameter (`model_name`, `cost_per_million_input_tokens`, or `cost_per_million_output_tokens`) is missing for any model, THEN THE Bedrock_Calculator SHALL return an error identifying the missing parameter.
9. WHEN `tools_passed_to_model` exceeds `number_of_tools` for a model, THE Bedrock_Calculator SHALL cap `tools_passed_to_model` at `number_of_tools`.

### Requirement 5: AgentCore Cost Calculation

**User Story:** As a solutions architect, I want to calculate monthly AgentCore costs across all components, so that I can estimate the infrastructure spend for running an agent.

#### Acceptance Criteria

1. WHEN the AgentCore_Calculator receives parameters with component configurations, THE AgentCore_Calculator SHALL calculate costs for each included component (runtime, browser, code_interpreter, gateway, memory).
2. THE AgentCore_Calculator SHALL calculate runtime and browser costs using vCPU hours, GB-hours, and the percentage of active CPU time (inverse of wait time).
3. THE AgentCore_Calculator SHALL calculate gateway costs from InvokeTool API calls, search API calls, and tool indexing fees.
4. THE AgentCore_Calculator SHALL calculate memory costs from short-term event ingestion, long-term record storage over configurable months, and long-term record retrieval.
5. THE AgentCore_Calculator SHALL return step-by-step calculation explanations for each component.
6. IF a required pricing parameter is missing for any included component, THEN THE AgentCore_Calculator SHALL return an error identifying the missing parameter.
7. THE AgentCore_Calculator SHALL sum costs across all included components into a `total_all_components` field.

### Requirement 6: Business Value Analysis

**User Story:** As a business stakeholder, I want to calculate ROI, payback period, and net business value from deploying an AI agent, so that I can justify the investment.

#### Acceptance Criteria

1. WHEN the BVA_Calculator receives parameters with `questions_per_month` and `ai_agent_cost_per_month`, THE BVA_Calculator SHALL calculate a business value summary combining all benefit and cost components.
2. WHEN a `cost_savings` component is provided, THE BVA_Calculator SHALL calculate monthly labor savings based on time saved per question, the percentage of questions that save time, and the labor cost per hour.
3. WHEN a `revenue_growth` component is provided, THE BVA_Calculator SHALL calculate additional revenue from time reallocated to new projects.
4. WHEN a `customer_churn_reduction` component is provided, THE BVA_Calculator SHALL calculate retained revenue and avoided customer acquisition costs.
5. WHEN an `implementation_costs` component is provided, THE BVA_Calculator SHALL include one-time implementation and training costs in the ROI calculation.
6. THE BVA_Calculator SHALL calculate net benefit, ROI percentage, and payback period over the specified analysis period.
7. THE BVA_Calculator SHALL return step-by-step calculation explanations for each component.
8. IF `minutes_per_question_with_ai` is greater than or equal to `minutes_per_question_without_ai`, THEN THE BVA_Calculator SHALL return an error indicating AI does not save time.
9. IF `customer_churn_after_ai` is greater than or equal to `customer_churn_before_ai`, THEN THE BVA_Calculator SHALL return an error indicating AI does not reduce churn.

### Requirement 7: Capacity Planning

**User Story:** As a solutions architect, I want to analyze whether a Bedrock model can handle my workload within quota limits, so that I can plan capacity and compare provisioned throughput options.

#### Acceptance Criteria

1. WHEN the Capacity_Planner receives parameters with `model_name`, `max_rpm`, and `max_tpm`, THE Capacity_Planner SHALL calculate required RPM and TPM for both steady-state and peak-state usage.
2. THE Capacity_Planner SHALL compare required RPM and TPM against model quota limits and report utilization percentages and sufficiency verdicts.
3. THE Capacity_Planner SHALL estimate monthly costs based on token volumes and per-million-token pricing for on-demand text, embedding, image, and video model types.
4. WHEN provisioned throughput pricing data is provided and the model type is on-demand, THE Capacity_Planner SHALL calculate required provisioned throughput units and compare no-commitment, 1-month, and 6-month commitment tier costs against on-demand costs.
5. IF both `max_rpm` and `max_tpm` are 0, THEN THE Capacity_Planner SHALL include a warning that quota values were not provided.
6. IF an invalid `model_type` is provided, THEN THE Capacity_Planner SHALL default to `on_demand` and include a warning.

### Requirement 8: What-If Analysis

**User Story:** As a cost analyst, I want to perform sensitivity analysis by varying one or two parameters, so that I can understand how cost drivers affect total spend.

#### Acceptance Criteria

1. WHEN the Bedrock what-if analysis tool receives a base configuration and a primary variable with a range of values, THE tool SHALL calculate costs for each value and return a 1D analysis with costs per scenario.
2. WHEN the Bedrock what-if analysis tool receives both primary and secondary variables with ranges, THE tool SHALL calculate costs for every combination and return a 2D analysis matrix.
3. THE what-if analysis tools SHALL support nested parameter paths using dot notation for model-specific parameters.
4. IF any scenario calculation fails, THEN THE what-if analysis tool SHALL return an error identifying the failing scenario.

### Requirement 9: System Prompt and Response Format

**User Story:** As a user, I want the agent to respond with structured JSON containing complete calculator outputs, so that the UI can render cost breakdowns, charts, and verification details.

#### Acceptance Criteria

1. THE Analyst_Agent SHALL respond with valid JSON conforming to the BedrockCosts, AgentCoreCosts, or BusinessValue Pydantic schemas as appropriate.
2. THE Analyst_Agent SHALL include the complete, unmodified output from all calculator tools in the response without filtering or omitting fields.
3. WHEN the Analyst_Agent retrieves pricing data, THE Analyst_Agent SHALL verify that the model name and region in the selected pricing chunk match the user's request before using the data.
4. IF a pricing chunk mismatch is detected, THEN THE Analyst_Agent SHALL discard the mismatched data and find the correct chunk from retrieved results.
5. THE Analyst_Agent SHALL include a verification section starting with the marker "**Verification:**" on its own line, so the UI can render it in a collapsible section.
6. WHEN the user asks a question unrelated to Amazon Bedrock or AgentCore, THE Analyst_Agent SHALL respond that it is a specialized agent for Bedrock and AgentCore questions only.

### Requirement 10: Infrastructure Provisioning

**User Story:** As a platform operator, I want a single CloudFormation template that provisions all required AWS resources, so that I can deploy the complete solution in one stack.

#### Acceptance Criteria

1. THE Infrastructure_Stack SHALL create an S3 bucket for pricing documents with server-side encryption and public access blocked.
2. THE Infrastructure_Stack SHALL create an OpenSearch Serverless collection configured for vector search with a FAISS HNSW index using the configured embedding dimension.
3. THE Infrastructure_Stack SHALL create a Bedrock Knowledge Base connected to the OpenSearch Serverless collection and the pricing documents S3 bucket with chunking strategy set to NONE.
4. THE Infrastructure_Stack SHALL create a DynamoDB table `tco-bva-chat-sessions` with `userId` as partition key and `sessionId` as sort key with point-in-time recovery enabled.
5. THE Infrastructure_Stack SHALL create a DynamoDB table `tco-bva-admin-users` with `userId` as partition key.
6. THE Infrastructure_Stack SHALL create a CloudFront distribution serving a frontend S3 bucket with Origin Access Control, HTTPS redirect, and SPA error page routing (403/404 to index.html).
7. THE Infrastructure_Stack SHALL create a Cognito Identity Pool that issues temporary AWS credentials to authenticated users with permissions for DynamoDB operations and AgentCore invocation.
8. THE Infrastructure_Stack SHALL deploy a Lambda custom resource that waits for the OpenSearch index to become ready before creating the Knowledge Base.
9. THE Infrastructure_Stack SHALL accept parameters for EmbeddingModelId, CognitoUserPoolId, CognitoClientId, CognitoDomain, and AgentRuntimeName.

### Requirement 11: Chatbot UI Authentication and Session Management

**User Story:** As a user, I want to authenticate via Cognito and manage multiple conversation sessions, so that my chat history is preserved and I can return to previous analyses.

#### Acceptance Criteria

1. WHEN a user accesses the Chatbot_UI without a valid stored token, THE Chatbot_UI SHALL redirect to the Cognito Hosted UI for authentication.
2. WHEN the Chatbot_UI receives an authorization code callback, THE Chatbot_UI SHALL exchange the code for tokens, retrieve user info, and obtain AWS credentials from the Cognito Identity Pool.
3. THE Chatbot_UI SHALL display a sidebar listing all previous sessions sorted by most recently updated, showing session title and timestamp.
4. WHEN the user clicks "New Conversation", THE Chatbot_UI SHALL create a new session with a unique UUID-based session ID and clear the message area.
5. WHEN the user selects an existing session from the sidebar, THE Chatbot_UI SHALL load and display the conversation history for that session from DynamoDB.
6. THE Chatbot_UI SHALL persist each user message and assistant response to DynamoDB with timestamps in Eastern Time format.

### Requirement 12: Chat Interaction and Agent Invocation

**User Story:** As a user, I want to send messages to the agent and receive structured cost analysis responses rendered with rich formatting, so that I can easily understand the results.

#### Acceptance Criteria

1. WHEN the user sends a message, THE Chatbot_UI SHALL discover the AgentCore runtime ARN by name, build a prompt with conversation history context (up to 20 recent messages), and invoke the agent.
2. WHEN the user's message contains chart-related keywords, THE Chatbot_UI SHALL append chart rendering instructions to the prompt requesting JSON chart data with `chart_type`, `chart_data`, and `chart_title` fields.
3. THE Chatbot_UI SHALL render assistant responses as markdown with support for tables, code blocks, bold text, and lists using remark-gfm.
4. WHEN the assistant response contains JSON code blocks, THE Chatbot_UI SHALL render them as structured sections with Cloudscape tables for arrays, key-value metric tables for objects, and collapsible expanders for calculation explanations.
5. WHEN the assistant response contains a "**Verification:**" section, THE Chatbot_UI SHALL extract it and render it in a collapsible "Verification details" expander.
6. WHEN the assistant response contains chart data with `chart_type` and `chart_data` fields, THE Chatbot_UI SHALL render interactive charts using Recharts supporting bar, pie, donut, line, area, stacked bar, grouped bar, radar, and heatmap chart types.

### Requirement 13: Model Selection and Admin Controls

**User Story:** As an admin user, I want access to a broader set of Bedrock models for the agent, so that I can test and compare different model capabilities.

#### Acceptance Criteria

1. THE Chatbot_UI SHALL provide a model selector dropdown in the chat input area.
2. WHILE a user is not an admin, THE Chatbot_UI SHALL display a restricted set of model options (Claude Sonnet 4.5, Claude Opus 4.6, Claude Opus 4.5, Claude Haiku 4.5).
3. WHILE a user is an admin (present in the `tco-bva-admin-users` DynamoDB table), THE Chatbot_UI SHALL display the full set of model options including Nova Premier, Nova Pro, Nova 2 Lite, Nova Lite, and Nova Micro.
4. WHEN a model is selected, THE Chatbot_UI SHALL pass the model ID in the agent invocation payload.

### Requirement 14: Pricing Data Scraping

**User Story:** As a platform operator, I want to automatically collect AWS pricing data from the public pricing API, so that the Knowledge Base stays current with published prices.

#### Acceptance Criteria

1. WHEN the Pricing_Scraper is executed, THE Pricing_Scraper SHALL fetch the AWS pricing index from `https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/index.json`.
2. THE Pricing_Scraper SHALL fetch pricing documents for the four Bedrock-related services: `AmazonBedrock`, `AmazonBedrockAgentCore`, `AmazonBedrockService`, and `AmazonBedrockFoundationModels`.
3. THE Pricing_Scraper SHALL save each product as a text file organized in the directory structure `{output_dir}/{service_name}/{region_code}/{product_name}_{sku}.txt` containing SKU, attributes, and on-demand and reserved pricing terms.
4. THE Pricing_Scraper SHALL scrape ALL regions (no region filtering by default).
5. THE Pricing_Scraper SHALL support command-line arguments for filtering by service name, specifying output directory, filtering by region, and listing available services.
6. THE Pricing_Scraper SHALL sanitize filenames by removing invalid characters and capping length at 200 characters.

### Requirement 15: Quota Data Scraping

**User Story:** As a platform operator, I want to automatically collect Bedrock model RPM and TPM quotas from the Service Quotas API, so that the Knowledge Base has current quota limits for capacity planning.

#### Acceptance Criteria

1. WHEN the Quota_Scraper is executed, THE Quota_Scraper SHALL query the Service Quotas API for ALL AWS regions, collecting all Bedrock service quotas and filtering to RPM and TPM quotas only, excluding model customization quotas.
2. THE Quota_Scraper SHALL classify each quota by inference type (on-demand, cross-region, global-cross-region) and metric type (rpm, tpm).
3. THE Quota_Scraper SHALL save each quota as a text file in the directory structure `{output_dir}/{region}/{sanitized_quota_name}.txt` containing quota name, code, region, inference type, metric, value, unit, adjustability, and global quota status.
4. THE Quota_Scraper SHALL save a `_summary.json` file per region with counts of quotas found and files saved.
5. IF the Service Quotas API throttles requests, THEN THE Quota_Scraper SHALL retry with exponential backoff up to 3 times.
6. THE Quota_Scraper SHALL support command-line arguments for specifying regions, output directory, scraping all discovered regions, and listing default regions.
7. THE Quota_Scraper SHALL introduce a 1-second delay between region scrapes to avoid throttling.

### Requirement 16: Scheduled Scraping and S3 Sync

**User Story:** As a platform operator, I want both scrapers to run automatically every Sunday at 1 AM PST, sync their output to the application's S3 bucket, and trigger a Knowledge Base sync, so that pricing and quota data stays current without manual intervention.

#### Acceptance Criteria

1. THE Infrastructure_Stack SHALL create an EventBridge Scheduler rule with a cron expression that fires every Sunday at 1 AM PST (9 AM UTC).
2. THE EventBridge rule SHALL trigger a Lambda function that executes both the Pricing_Scraper and Quota_Scraper.
3. WHEN the scrapers complete, THE Lambda function SHALL sync the scraped pricing data files to the `pricing_data/` prefix and quota data files to the `quota_data/` prefix in the application's S3 pricing documents bucket.
4. AFTER the S3 sync completes, THE Lambda function SHALL trigger a Bedrock Knowledge Base data source sync using the `start_ingestion_job` API.
5. THE Lambda function SHALL have an IAM role with permissions for S3 put/delete operations on the pricing documents bucket, Bedrock `StartIngestionJob` on the Knowledge Base, Service Quotas `ListServiceQuotas` for all regions, and outbound HTTPS access to the AWS pricing API.
6. THE Lambda function SHALL have a timeout of at least 15 minutes to accommodate scraping all regions for both pricing and quota data.
7. IF either scraper fails, THEN THE Lambda function SHALL log the error and continue with the other scraper's output.

### Requirement 17: Knowledge Base Data Ingestion

**User Story:** As a platform operator, I want scraped pricing and quota data to be ingested into the Bedrock Knowledge Base, so that the agent can retrieve current data via vector search.

#### Acceptance Criteria

1. THE Infrastructure_Stack SHALL configure the Knowledge Base data source to point to the pricing documents S3 bucket.
2. THE Knowledge_Base SHALL use the Titan Embed Text v2 model with 1024-dimensional vectors for embedding documents.
3. THE Knowledge_Base SHALL use chunking strategy NONE so that each scraped file is treated as a single document.
4. WHEN pricing or quota data files are uploaded to the S3 bucket and a Knowledge Base sync is triggered, THE Knowledge_Base SHALL index the documents into the OpenSearch Serverless vector store.

### Requirement 18: Conversation History Context

**User Story:** As a user, I want the agent to have context from my previous messages in the session, so that follow-up questions work without repeating information.

#### Acceptance Criteria

1. WHEN the Chatbot_UI sends a message to the agent, THE Chatbot_UI SHALL prepend up to 20 recent messages from the session history as context, with each message truncated to 2000 characters.
2. THE Chatbot_UI SHALL format the history as "User: {content}" and "Assistant: {content}" pairs separated by the marker "---" before the new user message.
3. WHEN no prior messages exist in the session, THE Chatbot_UI SHALL send only the user's new message without history context.

### Requirement 19: Agent Conversational Workflow

**User Story:** As a user, I want the agent to ask targeted clarifying questions with sensible defaults before performing calculations, so that I can quickly get results by accepting defaults or customizing parameters.

#### Acceptance Criteria

1. WHEN the user asks for a cost calculation, THE Analyst_Agent SHALL ask targeted questions in a single response, providing for each question the parameter name, why it matters, and the default value from the tool docstring.
2. WHEN the user describes a business scenario, THE Analyst_Agent SHALL ask probing questions that uncover cost-driving factors specific to the scenario.
3. WHEN the user asks about ROI or business value, THE Analyst_Agent SHALL first gather cost calculation parameters and then ask business impact questions.
4. THE Analyst_Agent SHALL end each set of questions with: "If you're ok with these defaults, just type 'Ok. Go.'"
5. WHEN pricing search results reveal multiple variants of the same component, THE Analyst_Agent SHALL present all variants with their costs and ask the user to choose before proceeding.
6. WHEN the user asks about capacity planning, THE Analyst_Agent SHALL ask a maximum of 2 follow-up questions before proceeding with calculations using available information and stated assumptions.

### Requirement 20: No Hardcoded Sensitive or Account-Specific Values

**User Story:** As a developer sharing this project publicly, I want all artifacts to be free of personal or AWS account-specific information, so that the repository is safe to publish without leaking credentials or identifiers.

#### Acceptance Criteria

1. ALL source code, CloudFormation templates, configuration files, and text files SHALL NOT contain hardcoded AWS account IDs, Cognito User Pool IDs, Cognito Client IDs, Cognito domain prefixes, or any other account-specific identifiers.
2. THE Infrastructure_Stack SHALL use CloudFormation parameters with no default values (or clearly placeholder defaults like `REPLACE_ME`) for all account-specific values including CognitoUserPoolId, CognitoClientId, and CognitoDomain.
3. ALL Python source files SHALL read account-specific or environment-specific values from environment variables or configuration files, never from hardcoded strings.
4. THE Chatbot_UI configuration files (`.env`, `.env.example`) SHALL use placeholder values and document which values need to be replaced.
5. IF a file requires account-specific values at runtime, THEN the file SHALL include comments or documentation explaining which values to provide and how.
