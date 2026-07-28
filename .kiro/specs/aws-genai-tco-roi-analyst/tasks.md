# Implementation Plan: AWS GenAI TCO & ROI Analyst

## Overview

This plan covers targeted changes to the existing codebase to align it with the spec. The agent code, calculator tools, chatbot UI, and proxy server are largely complete from a previous project. Tasks focus on: (1) updating scraper defaults, (2) removing hardcoded account-specific values, (3) adding scraper Lambda + EventBridge resources to the CFN template, (4) creating a project README, and (5) verifying existing code meets spec requirements.

## Tasks

- [x] 1. Update Pricing Scraper to scrape ALL regions and filter to 4 Bedrock services
  - [x] 1.1 Change default behavior to scrape ALL regions (no region filtering by default)
    - In `doc_scrapers/pricing-doc-scraper/price_doc_scraper.py`, change `DEFAULT_REGIONS` to be used only when `--region` is explicitly passed
    - Update `get_pricing_documents()` so that when no `--region` flag and no `--all-regions` flag is passed, `regions_filter` defaults to `None` (all regions)
    - Update `main()` CLI logic: when no `--region` is specified, pass `regions_filter=None` instead of `DEFAULT_REGIONS`
    - _Requirements: 14.4_
  - [x] 1.2 Set default service filter to the 4 Bedrock services
    - Add a `DEFAULT_SERVICES` constant: `['AmazonBedrock', 'AmazonBedrockAgentCore', 'AmazonBedrockService', 'AmazonBedrockFoundationModels']`
    - Update `main()` so that when no `--service` flag is passed, `services_filter` defaults to `DEFAULT_SERVICES` instead of `None` (all services)
    - _Requirements: 14.2_
  - [ ]* 1.3 Write property tests for pricing scraper
    - **Property 23: Filename sanitization** — generate random strings, verify no invalid chars, length ≤ 200, no leading/trailing spaces or dots
    - **Property 24: Pricing scraper file path structure** — verify output path matches `{output_dir}/{service_name}/{region_code}/{product_name}_{sku_prefix}.txt`
    - **Validates: Requirements 14.3, 14.6**

- [x] 2. Update Quota Scraper to scrape ALL regions by default
  - [x] 2.1 Change default behavior to scrape ALL regions
    - In `doc_scrapers/quota-doc-scraper/quota_doc_scraper.py`, update `main()` so that when no `--region` flag is passed, the scraper discovers and scrapes ALL regions (same behavior as current `--all-regions` flag)
    - Keep `DEFAULT_REGIONS` as a reference list but make all-regions the default path
    - Keep `--region` flag for explicit region filtering and `--list` for listing defaults
    - _Requirements: 15.1_
  - [ ]* 2.2 Write property tests for quota scraper
    - **Property 25: Quota scraper inference type and metric classification** — generate quota name strings, verify correct classification of inference type and metric
    - **Property 26: Quota scraper output file content completeness** — verify all required fields present in output
    - **Validates: Requirements 15.2, 15.3**

- [x] 3. Checkpoint - Verify scraper changes
  - Ensure all tests pass, ask the user if questions arise.

- [x] 4. Remove hardcoded account-specific values from CFN template
  - [x] 4.1 Replace Cognito parameter defaults with REPLACE_ME placeholders
    - In `cfn/aws-tco-biz-value-analysis.yaml`, change `CognitoUserPoolId` default from `us-west-2_6AuG0cghr` to `REPLACE_ME`
    - Change `CognitoClientId` default from `7sueti9imhqmlrubqvj97lqvse` to `REPLACE_ME`
    - Change `CognitoDomain` default from `us-west-26aug0cghr` to `REPLACE_ME`
    - _Requirements: 20.1, 20.2_
  - [ ]* 4.2 Write property test for CFN placeholder defaults
    - **Property 28: CFN parameters use placeholder defaults for account-specific values** — parse the YAML template and verify CognitoUserPoolId, CognitoClientId, CognitoDomain defaults are `REPLACE_ME`
    - **Validates: Requirements 20.2**

- [x] 5. Remove hardcoded account-specific values from Chatbot UI
  - [x] 5.1 Replace hardcoded Cognito values in authConfig.js with environment variable references
    - In `chatbot-ui/src/authConfig.js`, replace hardcoded `userPoolId`, `userPoolClientId`, `domain`, `redirectSignIn`, and `redirectSignOut` with `process.env.REACT_APP_*` environment variables
    - Add fallback placeholder values like `REPLACE_ME` for each
    - _Requirements: 20.1, 20.4_
  - [x] 5.2 Update .env.example with all required placeholder values
    - Add `REACT_APP_COGNITO_USER_POOL_ID=REPLACE_ME`
    - Add `REACT_APP_COGNITO_CLIENT_ID=REPLACE_ME`
    - Add `REACT_APP_COGNITO_DOMAIN=REPLACE_ME`
    - Add `REACT_APP_REDIRECT_URL=https://your-cloudfront-domain.cloudfront.net/`
    - Add `REACT_APP_IDENTITY_POOL_ID=REPLACE_ME`
    - Add comments explaining which values to provide and how to obtain them
    - _Requirements: 20.4, 20.5_
  - [x] 5.3 Replace hardcoded Identity Pool ID in .env
    - In `chatbot-ui/.env`, replace `us-west-2:96eda373-a329-4938-aa0d-d382e2a208e7` with `REPLACE_ME`
    - _Requirements: 20.1_

- [x] 6. Add Scraper Lambda, IAM Role, and EventBridge Scheduler to CFN template
  - [x] 6.1 Add ScraperLambdaRole IAM resource
    - Create IAM role with trust policy for `lambda.amazonaws.com`
    - Attach `AWSLambdaBasicExecutionRole` managed policy
    - Add inline policy with: S3 put/delete on PricingDocsBucket, Bedrock `StartIngestionJob` on the Knowledge Base, Service Quotas `ListServiceQuotas` for all regions, outbound HTTPS (via Lambda VPC or default)
    - _Requirements: 16.5_
  - [x] 6.2 Add ScraperLambdaFunction resource
    - Python 3.12 runtime, 15-minute timeout (900 seconds)
    - Environment variables: `S3_BUCKET_NAME` via `!Ref PricingDocsBucket`, `KNOWLEDGE_BASE_ID` via `!Ref PricingKnowledgeBase`, `DATA_SOURCE_ID` via `!GetAtt PricingDataSource.DataSourceId`
    - Inline code (ZipFile) or reference to S3 package that runs both scrapers, syncs output to S3 (`pricing_data/` and `quota_data/` prefixes), and calls `start_ingestion_job`
    - Error handling: catch errors from each scraper independently, log and continue
    - _Requirements: 16.2, 16.3, 16.4, 16.6, 16.7_
  - [x] 6.3 Add EventBridge Scheduler rule
    - Create `AWS::Events::Rule` (or `AWS::Scheduler::Schedule`) with cron expression `cron(0 9 ? * SUN *)` (9 AM UTC = 1 AM PST)
    - Target: the ScraperLambdaFunction
    - Add `AWS::Lambda::Permission` for EventBridge to invoke the Lambda
    - _Requirements: 16.1_
  - [x] 6.4 Add CFN Outputs for new resources
    - Add outputs for `ScraperLambdaFunctionArn` and `ScraperScheduleRuleName`
    - _Requirements: 16.1_

- [x] 7. Checkpoint - Verify CFN and hardcoded values cleanup
  - Ensure all tests pass, ask the user if questions arise.

- [x] 8. Scan all project files for remaining hardcoded account-specific values
  - [x] 8.1 Scan and fix any remaining hardcoded values across all files
    - Search all `.py`, `.js`, `.jsx`, `.yaml`, `.json`, `.env` files for patterns: 12-digit AWS account IDs, Cognito User Pool ID patterns (`us-\w+-\d_\w+`), Cognito Client IDs, CloudFront distribution IDs, Identity Pool IDs
    - Replace any found values with environment variable references or `REPLACE_ME` placeholders
    - Exclude files in `node_modules/`, `.venv/`, `__pycache__/`, and `package-lock.json`
    - _Requirements: 20.1, 20.3_
  - [ ]* 8.2 Write property test for no hardcoded values
    - **Property 27: No hardcoded account-specific values in source files** — scan all Python, JS, JSX, YAML source files for AWS account ID patterns, Cognito ID patterns, and verify none are found (excluding placeholders)
    - **Validates: Requirements 20.1, 20.2, 20.3**

- [x] 9. Create project README
  - [x] 9.1 Create README.md at `usecases/aws-genai-tco-roi-analyst/README.md`
    - Include: project overview, architecture diagram reference, prerequisites (AWS account, Cognito User Pool, Python 3.11+, Node.js 18+)
    - Deployment steps: (1) deploy CFN stack with required parameters, (2) configure AgentCore agent, (3) run scrapers to populate KB, (4) build and deploy chatbot UI
    - Configuration section: list all environment variables and CFN parameters that need to be set
    - Local development section: how to run the proxy server and React app locally
    - Project structure overview
    - _Requirements: 20.5_

- [x] 10. Verify agent code reads KB ID from environment variables
  - [x] 10.1 Verify search tools use env vars for KB ID
    - Confirm `search_pricing_info.py` reads `STRANDS_KNOWLEDGE_BASE_ID` from `os.environ.get()` with a placeholder default — already done, verify placeholder is `<PLACE_YOUR_KB_ID>` not a real ID
    - Confirm `search_bedrock_quota.py` reads `STRANDS_KNOWLEDGE_BASE_ID` from `os.environ.get()` with a placeholder default — already done, verify placeholder is `<PLACE_YOUR_KB_ID>` not a real ID
    - _Requirements: 20.3_
  - [ ]* 10.2 Write property tests for search tool formatting
    - **Property 2: Search result formatting contains all required fields** — generate mock KB results, verify formatted output contains sequential number, score, content, and source URI for each result
    - **Validates: Requirements 2.3, 3.3**

- [x] 11. Final checkpoint - Full verification
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- The existing calculator tools (calculator_bedrock.py, calculator_agentcore.py, calculator_bva.py, calculator_capacity_planning.py), system_prompt.py, main.py, and chatbot UI code are largely complete — tasks focus on targeted changes only
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties from the design document
