# Visual Explanation - Cost Calculation Logic

This document provides visual ASCII diagrams to explain how costs are calculated for Amazon Bedrock and Amazon Bedrock AgentCore.

## Table of Contents

1. [Bedrock Cost Calculation](#bedrock-cost-calculation)
2. [AgentCore Cost Calculation](#agentcore-cost-calculation)

---

## Bedrock Cost Calculation

### Overview

Bedrock costs are calculated based on token usage across multiple sources. The calculator aggregates tokens from queries, vector databases (per-model), tools, system prompts, and conversation history.

### High-Level Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    BEDROCK COST CALCULATOR                       │
│                                                                  │
│  Input: questions_per_month (100,000)                           │
│         model configurations                                     │
│         pricing data                                             │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│         STEP 1: Process Each Model (e.g., Claude Haiku)         │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              1A: Calculate Question Allocation                   │
│                                                                  │
│  questions_per_month (100,000)                                  │
│  × percent_questions_for_model (70% = 0.7)                      │
│  = questions_for_this_model (70,000)                            │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              1B: Calculate Base Query Tokens                     │
│                                                                  │
│  INPUT:                                                          │
│  input_tokens_per_question (100)                                │
│  × questions_for_this_model (70,000)                            │
│  = query_input_tokens_per_month (7,000,000)                     │
│                                                                  │
│  OUTPUT:                                                         │
│  output_tokens_per_question (500)                               │
│  × questions_for_this_model (70,000)                            │
│  = query_output_tokens_per_month (35,000,000)                   │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              1C: Calculate Vector Database Tokens                │
│              (Optional - only if configured for this model)      │
│                                                                  │
│  questions_using_vector_db:                                     │
│  questions_for_this_model (70,000)                              │
│  × percent_questions_using_vector_db (100% = 1.0)               │
│  = questions_using_vector_db (70,000)                           │
│                                                                  │
│  tokens_per_call:                                               │
│  chunks_per_call (10) × tokens_per_chunk (300)                  │
│  = tokens_per_call (3,000)                                      │
│                                                                  │
│  vector_tokens_for_this_model:                                  │
│  tokens_per_call (3,000) × questions_using_vector_db (70,000)   │
│  = vector_tokens_per_month (210,000,000)                        │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              1D: Calculate Tool Tokens (if tools configured)     │
│                                                                  │
│  Questions that invoke tools:                                    │
│  questions_for_this_model (70,000)                              │
│  × percent_questions_that_invoke_tools (80% = 0.8)              │
│  = questions_invoking_tools (56,000)                            │
│                                                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  TOOL INPUT TOKENS (sent to model)                        │  │
│  │                                                            │  │
│  │  Part 1: Tool Descriptions                                │  │
│  │  tools_passed_to_model (10)                               │  │
│  │  × input_tokens_per_tool (300)                            │  │
│  │  × questions_invoking_tools (56,000)                      │  │
│  │  = tool_description_tokens (168,000,000)                  │  │
│  │                                                            │  │
│  │  Part 2: Tool Results (returned from executions)          │  │
│  │  tool_invocations_per_question (3)                        │  │
│  │  × tokens_per_tool_result (500)                           │  │
│  │  × questions_invoking_tools (56,000)                      │  │
│  │  = tool_result_tokens (84,000,000)                        │  │
│  │                                                            │  │
│  │  Total Tool Input Tokens:                                 │  │
│  │  tool_description_tokens (168,000,000)                    │  │
│  │  + tool_result_tokens (84,000,000)                        │  │
│  │  = tool_input_tokens (252,000,000)                        │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  TOOL OUTPUT TOKENS (from model to agent)                 │  │
│  │                                                            │  │
│  │  tool_invocations_per_question (3)                        │  │
│  │  × output_tokens_for_tool_invocation (100)                │  │
│  │  × questions_invoking_tools (56,000)                      │  │
│  │  = tool_output_tokens (16,800,000)                        │  │
│  └───────────────────────────────────────────────────────────┘  │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              1E: Calculate System Prompt Tokens                  │
│                                                                  │
│  system_prompt_tokens (1,000)                                   │
│  × questions_for_this_model (70,000)                            │
│  = system_prompt_tokens_total (70,000,000)                      │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              1F: Calculate Conversation History Tokens           │
│                                                                  │
│  Tokens per Q&A pair:                                            │
│  input_tokens_per_question (100)                                │
│  + output_tokens_per_question (500)                             │
│  = tokens_per_qa_pair (600)                                     │
│                                                                  │
│  History tokens per question:                                    │
│  history_qa_pairs (3)                                           │
│  × tokens_per_qa_pair (600)                                     │
│  = history_tokens_per_question (1,800)                          │
│                                                                  │
│  Total history tokens:                                           │
│  history_tokens_per_question (1,800)                            │
│  × questions_for_this_model (70,000)                            │
│  = history_tokens_total (126,000,000)                           │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              1G: Sum All Input and Output Tokens                 │
│                                                                  │
│  TOTAL INPUT TOKENS:                                             │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │ query_input_tokens_per_month        (7,000,000)           │ │
│  │ + vector_tokens_per_month           (210,000,000)         │ │
│  │ + tool_input_tokens                 (252,000,000)         │ │
│  │ + system_prompt_tokens_total        (70,000,000)          │ │
│  │ + history_tokens_total              (126,000,000)         │ │
│  │ ─────────────────────────────────────────────────────────  │ │
│  │ = total_input_tokens                (665,000,000)         │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  TOTAL OUTPUT TOKENS:                                            │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │ query_output_tokens_per_month       (35,000,000)          │ │
│  │ + tool_output_tokens                (16,800,000)          │ │
│  │ ─────────────────────────────────────────────────────────  │ │
│  │ = total_output_tokens               (51,800,000)          │ │
│  └────────────────────────────────────────────────────────────┘ │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              1H: Calculate Final Costs                           │
│                                                                  │
│  INPUT COST:                                                     │
│  total_input_tokens (665,000,000) / 1,000,000                   │
│  = input_millions (665)                                         │
│  × cost_per_million_input_tokens ($0.25)                        │
│  = input_cost ($166.25)                                         │
│                                                                  │
│  OUTPUT COST:                                                    │
│  total_output_tokens (51,800,000) / 1,000,000                   │
│  = output_millions (51.8)                                       │
│  × cost_per_million_output_tokens ($1.25)                       │
│  = output_cost ($64.75)                                         │
│                                                                  │
│  TOTAL MODEL COST:                                               │
│  input_cost ($166.25) + output_cost ($64.75)                    │
│  = total_token_cost ($231.00)                                   │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              STEP 2: Aggregate All Models                        │
│                                                                  │
│  Model 1 (Claude Haiku - 70%):     $231.00                      │
│  Model 2 (Claude Sonnet - 30%):    $450.00                      │
│  ─────────────────────────────────────────                       │
│  Total Cost for All Models:        $681.00                      │
└─────────────────────────────────────────────────────────────────┘
```

### Key Concepts

**Token Flow Diagram:**

```
┌──────────────────────────────────────────────────────────────────┐
│                    TOKEN SOURCES → MODEL                          │
├──────────────────────────────────────────────────────────────────┤
│                                                                   │
│  INPUT TOKENS (sent to model):                                   │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │ 1. Query Input          (user question)                     │ │
│  │ 2. Vector Database      (retrieved chunks - per model)      │ │
│  │ 3. Tool Descriptions    (Gateway-selected tools)            │ │
│  │ 4. Tool Results         (execution outputs)                 │ │
│  │ 5. System Prompt        (agent instructions)                │ │
│  │ 6. Conversation History (previous Q&A pairs)                │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                                                                   │
│  OUTPUT TOKENS (from model):                                     │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │ 1. Query Output         (model's answer)                    │ │
│  │ 2. Tool Invocations     (function calls with arguments)     │ │
│  └─────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
```

**Agentic Workflow (Option B):**

```
┌────────────────────────────────────────────────────────────────┐
│                    AGENTIC TOOL WORKFLOW                        │
└────────────────────────┬───────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  STEP 1: Gateway Selects Tools                                  │
│  ────────────────────────────────────────────────────────────   │
│  Total tools indexed: 50                                        │
│  Gateway selects: 10 tools (based on semantic search)           │
│  → Sends 10 tool descriptions to model (INPUT TOKENS)           │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  STEP 2: Model Decides Which Tools to Invoke                    │
│  ────────────────────────────────────────────────────────────   │
│  Model analyzes query + 10 tool descriptions                    │
│  Decides to invoke: 3 tools                                     │
│  → Returns 3 function calls (OUTPUT TOKENS)                     │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  STEP 3: Agent Executes Tools via Gateway                       │
│  ────────────────────────────────────────────────────────────   │
│  Agent invokes 3 tools through Gateway                          │
│  Each tool returns results                                      │
│  → 3 tool results sent back to model (INPUT TOKENS)             │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  STEP 4: Model Synthesizes Final Response                       │
│  ────────────────────────────────────────────────────────────   │
│  Model receives tool results                                    │
│  Generates final answer                                         │
│  → Final response (OUTPUT TOKENS)                               │
└─────────────────────────────────────────────────────────────────┘
```

---

## AgentCore Cost Calculation

### Overview

AgentCore costs are calculated based on compute resources (vCPU, memory) and API usage (Gateway, Memory). Each component is optional and independent.

### High-Level Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                  AGENTCORE COST CALCULATOR                       │
│                                                                  │
│  Input: questions_per_day (33,333)                              │
│         days_per_month (30)                                     │
│         component configurations                                │
│         pricing data                                            │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              COMPONENT 1: RUNTIME COSTS                          │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              1A: Calculate Question Volume                       │
│                                                                  │
│  questions_per_day (33,333)                                     │
│  × percent_questions_using_runtime (100% = 1.0)                 │
│  = runtime_questions_per_day (33,333)                           │
│                                                                  │
│  runtime_questions_per_day (33,333)                             │
│  × days_per_month (30)                                          │
│  = total_questions_per_month (1,000,000)                        │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              1B: Calculate Total Hours                           │
│                                                                  │
│  total_questions_per_month (1,000,000)                          │
│  × seconds_per_question (60)                                    │
│  = total_seconds_per_month (60,000,000)                         │
│                                                                  │
│  total_seconds_per_month (60,000,000) / 3600                    │
│  = total_hours_per_month (16,667)                               │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              1C: Calculate vCPU Hours                            │
│                                                                  │
│  Wait time analysis:                                             │
│  percent_wait_time (90% = 0.9)                                  │
│  percent_cpu_time = 1 - 0.9 = 0.1 (10% active CPU)              │
│                                                                  │
│  vCPU hours calculation:                                         │
│  total_hours_per_month (16,667)                                 │
│  × num_cpus (2)                                                 │
│  × percent_cpu_time (0.1)                                       │
│  = vcpu_hours (3,333)                                           │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              1D: Calculate Memory Hours                          │
│                                                                  │
│  Memory is always 100% utilized (no wait time discount)         │
│                                                                  │
│  total_hours_per_month (16,667)                                 │
│  × gb_memory (4)                                                │
│  = gb_hours (66,667)                                            │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              1E: Calculate Runtime Costs                         │
│                                                                  │
│  CPU COST:                                                       │
│  vcpu_hours (3,333) × cost_per_vcpu_hour ($0.09)                │
│  = cpu_cost ($300.00)                                           │
│                                                                  │
│  MEMORY COST:                                                    │
│  gb_hours (66,667) × cost_per_gb_hour ($0.01)                   │
│  = memory_cost ($666.67)                                        │
│                                                                  │
│  TOTAL RUNTIME COST:                                             │
│  cpu_cost ($300.00) + memory_cost ($666.67)                     │
│  = total_runtime_cost ($966.67)                                 │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              COMPONENT 2: BROWSER TOOL COSTS                     │
│              (Optional - only if configured)                     │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              2A: Calculate Browser Question Volume               │
│                                                                  │
│  questions_per_day (33,333)                                     │
│  × percent_questions_using_browser (20% = 0.2)                  │
│  = browser_questions_per_day (6,667)                            │
│                                                                  │
│  browser_questions_per_day (6,667)                              │
│  × days_per_month (30)                                          │
│  = total_questions_per_month (200,000)                          │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              2B: Calculate Browser Hours and Costs               │
│                                                                  │
│  Browser tasks take longer (600 seconds vs 60 for runtime)      │
│  Browser needs more resources (4 vCPU, 16GB vs 2 vCPU, 4GB)     │
│                                                                  │
│  total_hours_per_month:                                          │
│  (200,000 × 600) / 3600 = 33,333 hours                          │
│                                                                  │
│  vcpu_hours:                                                     │
│  33,333 × 4 × 0.1 = 13,333 hours                                │
│                                                                  │
│  gb_hours:                                                       │
│  33,333 × 16 = 533,333 hours                                    │
│                                                                  │
│  cpu_cost: 13,333 × $0.09 = $1,200.00                           │
│  memory_cost: 533,333 × $0.01 = $5,333.33                       │
│  total_browser_cost: $6,533.33                                  │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              COMPONENT 3: CODE INTERPRETER COSTS                 │
│              (Optional - only if configured)                     │
│                                                                  │
│  Similar calculation to Browser but:                             │
│  - Different wait time (20% vs 90%)                             │
│  - Different seconds_per_question (60 vs 600)                   │
│  - Same resources (4 vCPU, 16GB)                                │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              COMPONENT 4: GATEWAY COSTS                          │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              4A: Calculate Questions Using Tools                 │
│                                                                  │
│  questions_per_day (33,333) × days_per_month (30)               │
│  = total_questions_per_month (1,000,000)                        │
│                                                                  │
│  total_questions_per_month (1,000,000)                          │
│  × percent_questions_using_tools (80% = 0.8)                    │
│  = questions_using_tools_per_month (800,000)                    │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              4B: Calculate API Calls                             │
│                                                                  │
│  INVOKE TOOL API CALLS:                                          │
│  questions_using_tools_per_month (800,000)                      │
│  × tool_invocations_per_question (3)                            │
│  = total_invoke_tool_calls (2,400,000)                          │
│                                                                  │
│  SEARCH API CALLS:                                               │
│  questions_using_tools_per_month (800,000)                      │
│  × search_api_calls_per_question (1)                            │
│  = total_search_api_calls (800,000)                             │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              4C: Calculate Gateway Costs                         │
│                                                                  │
│  INVOKE TOOL COST:                                               │
│  total_invoke_tool_calls (2,400,000)                            │
│  × cost_per_invoke_tool_api ($0.00025)                          │
│  = invoke_tool_cost ($600.00)                                   │
│                                                                  │
│  SEARCH API COST:                                                │
│  total_search_api_calls (800,000)                               │
│  × cost_per_search_api_invocation ($0.00025)                    │
│  = search_api_cost ($200.00)                                    │
│                                                                  │
│  INDEXING COST:                                                  │
│  total_tools_indexed (50)                                       │
│  × cost_per_tool_indexed_per_month ($0.10)                      │
│  = indexing_cost ($5.00)                                        │
│                                                                  │
│  TOTAL GATEWAY COST:                                             │
│  invoke_tool_cost ($600.00)                                     │
│  + search_api_cost ($200.00)                                    │
│  + indexing_cost ($5.00)                                        │
│  = total_gateway_cost ($805.00)                                 │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              COMPONENT 5: MEMORY COSTS                           │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              5A: Calculate Short-Term Memory Costs               │
│                                                                  │
│  total_questions_per_month (1,000,000)                          │
│  × percent_questions_storing_events (100% = 1.0)                │
│  = questions_storing_events (1,000,000)                         │
│                                                                  │
│  questions_storing_events (1,000,000)                           │
│  × events_per_question (2)                                      │
│  = total_events_per_month (2,000,000)                           │
│                                                                  │
│  total_events_per_month (2,000,000)                             │
│  × cost_per_raw_event ($0.00025)                                │
│  = short_term_cost ($500.00)                                    │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              5B: Calculate Long-Term Storage Costs               │
│                                                                  │
│  total_events_per_month (2,000,000)                             │
│  × percent_events_stored_as_records (20% = 0.2)                 │
│  = records_stored_per_month (400,000)                           │
│                                                                  │
│  records_stored_per_month (400,000)                             │
│  × months_to_store (3)                                          │
│  = total_records_stored (1,200,000)                             │
│                                                                  │
│  total_records_stored (1,200,000)                               │
│  × cost_per_memory_record_per_month ($0.000001)                 │
│  = long_term_storage_cost ($1.20)                               │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              5C: Calculate Long-Term Retrieval Costs             │
│                                                                  │
│  total_questions_per_month (1,000,000)                          │
│  × percent_questions_retrieving_records (100% = 1.0)            │
│  = questions_retrieving_records (1,000,000)                     │
│                                                                  │
│  questions_retrieving_records (1,000,000)                       │
│  × records_retrieved_per_question (1)                           │
│  = total_retrievals_per_month (1,000,000)                       │
│                                                                  │
│  total_retrievals_per_month (1,000,000)                         │
│  × cost_per_memory_retrieval ($0.00025)                         │
│  = long_term_retrieval_cost ($250.00)                           │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              5D: Calculate Total Memory Cost                     │
│                                                                  │
│  short_term_cost ($500.00)                                      │
│  + long_term_storage_cost ($1.20)                               │
│  + long_term_retrieval_cost ($250.00)                           │
│  = total_memory_cost ($751.20)                                  │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              FINAL STEP: Aggregate All Components                │
│                                                                  │
│  Runtime Cost:                    $966.67                        │
│  Browser Tool Cost:               $6,533.33                      │
│  Code Interpreter Cost:           $0.00 (not configured)         │
│  Gateway Cost:                    $805.00                        │
│  Memory Cost:                     $751.20                        │
│  ─────────────────────────────────────────                       │
│  Total All Components:            $9,056.20                      │
└─────────────────────────────────────────────────────────────────┘
```

---

## Key Differences Between Bedrock and AgentCore

### Bedrock Calculator
- **Token-based pricing**: Costs based on input/output tokens
- **Multiple token sources**: Query, vector DB, tools, system prompt, history
- **Model-specific**: Each model has its own pricing
- **Agentic workflow**: Tool descriptions and results flow through model

### AgentCore Calculator
- **Resource-based pricing**: Costs based on vCPU, memory, API calls
- **Component-based**: Runtime, Browser, Code Interpreter, Gateway, Memory
- **Wait time optimization**: CPU costs reduced by wait time percentage
- **Independent components**: Each component can be used separately

---

## Example Scenarios

### Scenario 1: Simple Chatbot (Bedrock Only)
```
Questions: 100,000/month
Model: Claude Haiku
Input: 100 tokens/question
Output: 500 tokens/question
No tools, no vector DB

Total Cost: ~$75/month
```

### Scenario 2: Agentic Chatbot (Bedrock + Tools)
```
Questions: 100,000/month
Model: Claude Haiku
Input: 100 tokens/question
Output: 500 tokens/question
Tools: 10 passed to model, 3 invoked
Tool results: 500 tokens each

Total Cost: ~$250/month
```

### Scenario 3: Full AgentCore Stack
```
Questions: 1,000,000/month (33,333/day)
Runtime: 2 vCPU, 4GB, 60s/question
Browser: 20% of questions
Gateway: 50 tools, 3 invocations/question
Memory: Short-term + long-term

Total Cost: ~$9,000/month
```

---

## Cost Optimization Tips

### For Bedrock:
1. **Reduce input tokens**: Optimize prompts and context
2. **Limit tool descriptions**: Pass only relevant tools to model
3. **Minimize history**: Keep conversation history short
4. **Choose right model**: Haiku for simple tasks, Sonnet for complex

### For AgentCore:
1. **Optimize wait time**: Higher wait time = lower CPU costs
2. **Right-size resources**: Match vCPU/memory to workload
3. **Selective components**: Only use components you need
4. **Batch operations**: Reduce per-question overhead

---

## Conclusion

Both calculators provide detailed, transparent cost breakdowns with step-by-step explanations. Understanding the calculation logic helps optimize costs and make informed architectural decisions.



---

## Business Value Analysis (BVA) Calculation

### Overview

Business Value Analysis calculates the ROI, payback period, and net value of implementing an AI Agent solution. It combines cost savings, revenue growth, and customer churn reduction benefits against implementation and recurring AI costs.

### High-Level Flow

```
┌─────────────────────────────────────────────────────────────────┐
│              BUSINESS VALUE ANALYSIS CALCULATOR                  │
│                                                                  │
│  Input: questions_per_month (1,000,000)                         │
│         time savings parameters                                 │
│         business impact parameters                              │
│         AI costs (from Bedrock + AgentCore)                     │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│         STEP 1: Calculate Time Savings (Foundation)             │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              1A: Calculate Effective Questions                   │
│                                                                  │
│  Not all questions benefit from AI equally                      │
│                                                                  │
│  questions_per_month (1,000,000)                                │
│  × percent_questions_that_save_time (80% = 0.8)                 │
│  = effective_questions_saving_time (800,000)                    │
│                                                                  │
│  Questions not saving time: 200,000                             │
│  (These still use original time)                                │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              1B: Calculate Time Saved Per Question               │
│                                                                  │
│  minutes_per_question_without_ai (10 min)                       │
│  - minutes_per_question_with_ai (2 min)                         │
│  = time_saved_per_question (8 min)                              │
│                                                                  │
│  Validation: If AI doesn't save time (≤0), return error         │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              1C: Calculate Total Monthly Time Saved              │
│                                                                  │
│  effective_questions_saving_time (800,000)                      │
│  × time_saved_per_question (8 min)                              │
│  = total_time_saved_minutes (6,400,000 min)                     │
│                                                                  │
│  total_time_saved_minutes (6,400,000) / 60                      │
│  = total_time_saved_hours (106,667 hours/month)                 │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│         STEP 2A: Cost Savings (Option 1 - Use OR Revenue)      │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              2A1: Calculate Labor Cost Savings                   │
│                                                                  │
│  IMPORTANT: Time savings used for EITHER cost savings OR        │
│  revenue growth, not both (to avoid double-counting)            │
│                                                                  │
│  total_time_saved_hours (106,667 hours/month)                   │
│  × labor_cost_per_hour ($100/hour)                              │
│  = monthly_gross_labor_savings ($10,666,667/month)              │
│                                                                  │
│  monthly_gross_labor_savings ($10,666,667)                      │
│  × analysis_period_months (12)                                  │
│  = total_gross_labor_savings_period ($128,000,000)              │
│                                                                  │
│  Note: "Gross" means AI costs NOT subtracted yet                │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│         STEP 2B: Revenue Growth (Option 2 - Use OR Cost)       │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              2B1: Calculate Additional Revenue                   │
│                                                                  │
│  IMPORTANT: Time savings used for EITHER cost savings OR        │
│  revenue growth, not both (to avoid double-counting)            │
│                                                                  │
│  total_time_saved_hours (106,667 hours/month)                   │
│  × percent_time_to_new_projects (60% = 0.6)                     │
│  = time_allocated_to_new_projects (64,000 hours/month)          │
│                                                                  │
│  time_allocated_to_new_projects (64,000 hours)                  │
│  × revenue_per_employee_per_hour ($150/hour)                    │
│  = monthly_gross_additional_revenue ($9,600,000/month)          │
│                                                                  │
│  monthly_gross_additional_revenue ($9,600,000)                  │
│  × analysis_period_months (12)                                  │
│  = total_gross_revenue_growth_period ($115,200,000)             │
│                                                                  │
│  Note: "Gross" means AI costs NOT subtracted yet                │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│         STEP 3: Customer Churn Reduction (Independent)          │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              3A: Calculate Churn Improvement                     │
│                                                                  │
│  customer_churn_before_ai (1.0% = 0.01)                         │
│  - customer_churn_after_ai (0.5% = 0.005)                       │
│  = churn_reduction_rate (0.5% = 0.005)                          │
│                                                                  │
│  Validation: If churn increased (≤0), return error              │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              3B: Calculate Customers Saved                       │
│                                                                  │
│  total_customer_count (100,000)                                 │
│  × churn_reduction_rate (0.005)                                 │
│  = customers_saved_per_month (500 customers/month)              │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              3C: Calculate Revenue Retained                      │
│                                                                  │
│  customers_saved_per_month (500)                                │
│  × average_monthly_revenue_per_customer ($100/month)            │
│  = monthly_revenue_retained ($50,000/month)                     │
│                                                                  │
│  monthly_revenue_retained ($50,000)                             │
│  × analysis_period_months (12)                                  │
│  = total_revenue_retained_period ($600,000)                     │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              3D: Calculate Acquisition Costs Avoided             │
│                                                                  │
│  Industry standard: Customer acquisition cost = 20% of          │
│  annual revenue per customer                                    │
│                                                                  │
│  average_monthly_revenue_per_customer ($100)                    │
│  × 12 months                                                    │
│  = annual_revenue_per_customer ($1,200)                         │
│                                                                  │
│  annual_revenue_per_customer ($1,200)                           │
│  × 0.20 (20%)                                                   │
│  = cost_of_acquiring_new_customer ($240)                        │
│                                                                  │
│  customers_saved_per_month (500)                                │
│  × cost_of_acquiring_new_customer ($240)                        │
│  = monthly_acquisition_cost_avoided ($120,000/month)            │
│                                                                  │
│  monthly_acquisition_cost_avoided ($120,000)                    │
│  × analysis_period_months (12)                                  │
│  = total_acquisition_cost_avoided_period ($1,440,000)           │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              3E: Calculate Total Churn Value                     │
│                                                                  │
│  total_revenue_retained_period ($600,000)                       │
│  + total_acquisition_cost_avoided_period ($1,440,000)           │
│  = total_churn_reduction_value_period ($2,040,000)              │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│         STEP 4: Implementation Costs (One-Time)                 │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              4A: Calculate Total Implementation Costs            │
│                                                                  │
│  one_time_implementation_cost ($100,000)                        │
│  + one_time_training_cost ($20,000)                             │
│  = total_implementation_costs ($120,000)                        │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│         STEP 5: Business Value Summary (The Big Picture)        │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              5A: Calculate Total Gross Benefits                  │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │ BENEFITS (before AI costs)                                 │ │
│  │                                                            │ │
│  │ Time Savings Benefit:                                      │ │
│  │   Cost Savings: $128,000,000                              │ │
│  │   OR Revenue Growth: $115,200,000                         │ │
│  │   (Use ONE, not both)                                     │ │
│  │                                                            │ │
│  │ Customer Churn Reduction: $2,040,000                      │ │
│  │   (Independent - can combine with either above)           │ │
│  │                                                            │ │
│  │ Total Gross Benefits: $130,040,000                        │ │
│  │   (Assuming Cost Savings option)                          │ │
│  └────────────────────────────────────────────────────────────┘ │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              5B: Calculate Total Costs                           │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │ COSTS                                                      │ │
│  │                                                            │ │
│  │ One-Time Implementation: $120,000                         │ │
│  │                                                            │ │
│  │ Recurring AI Costs:                                       │ │
│  │   ai_agent_cost_per_month ($26,000)                       │ │
│  │   × analysis_period_months (12)                           │ │
│  │   = recurring_ai_costs_over_period ($312,000)             │ │
│  │                                                            │ │
│  │ Total Costs: $432,000                                     │ │
│  │   ($120,000 + $312,000)                                   │ │
│  └────────────────────────────────────────────────────────────┘ │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              5C: Calculate Net Results                           │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │ NET RESULTS                                                │ │
│  │                                                            │ │
│  │ Net Value Over Period:                                    │ │
│  │   total_gross_benefits ($130,040,000)                     │ │
│  │   - total_costs ($432,000)                                │ │
│  │   = net_value_over_period ($129,608,000)                  │ │
│  │                                                            │ │
│  │ ROI Percent:                                              │ │
│  │   (net_value_over_period / total_costs) × 100            │ │
│  │   = ($129,608,000 / $432,000) × 100                       │ │
│  │   = 30,002% ROI                                           │ │
│  │                                                            │ │
│  │ Payback Months:                                           │ │
│  │   total_costs / monthly_net_benefit                       │ │
│  │   = $432,000 / $10,794,000                                │ │
│  │   = 0.04 months (1.2 days!)                               │ │
│  └────────────────────────────────────────────────────────────┘ │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              5D: Calculate Monthly Ongoing                       │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │ MONTHLY ONGOING (after payback)                           │ │
│  │                                                            │ │
│  │ Monthly Gross Benefit:                                    │ │
│  │   monthly_gross_labor_savings ($10,666,667)               │ │
│  │   + monthly_total_churn_value ($170,000)                  │ │
│  │   = monthly_gross_benefit ($10,836,667)                   │ │
│  │                                                            │ │
│  │ Monthly AI Costs: $26,000                                 │ │
│  │                                                            │ │
│  │ Monthly Net Benefit:                                      │ │
│  │   monthly_gross_benefit ($10,836,667)                     │ │
│  │   - monthly_ai_costs ($26,000)                            │ │
│  │   = monthly_net_benefit ($10,810,667)                     │ │
│  └────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

### Key Concepts

**Benefit Flow Diagram:**

```
┌──────────────────────────────────────────────────────────────────┐
│                    BENEFIT CALCULATION FLOW                       │
├──────────────────────────────────────────────────────────────────┤
│                                                                   │
│  TIME SAVINGS (Foundation):                                       │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │ 1M questions/month × 80% success × 8 min saved              │ │
│  │ = 106,667 hours/month saved                                 │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                                                                   │
│                    ┌──────────┴──────────┐                        │
│                    │                     │                        │
│                    ▼                     ▼                        │
│  ┌──────────────────────────┐  ┌──────────────────────────┐      │
│  │  OPTION 1: COST SAVINGS  │  │  OPTION 2: REVENUE GROWTH│      │
│  │                          │  │                          │      │
│  │  106,667 hrs × $100/hr   │  │  106,667 hrs × 60%       │      │
│  │  = $10.7M/month          │  │  × $150/hr               │      │
│  │                          │  │  = $9.6M/month           │      │
│  └──────────────────────────┘  └──────────────────────────┘      │
│                                                                   │
│           USE ONE OR THE OTHER (not both!)                        │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │  INDEPENDENT: CUSTOMER CHURN REDUCTION                      │ │
│  │                                                             │ │
│  │  500 customers saved × $100/month revenue                  │ │
│  │  + 500 customers × $240 acquisition cost avoided           │ │
│  │  = $170K/month                                             │ │
│  │                                                             │ │
│  │  (Can combine with either Cost Savings OR Revenue Growth)  │ │
│  └─────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
```

**Cost vs Benefit Timeline:**

```
┌──────────────────────────────────────────────────────────────────┐
│                    COST VS BENEFIT TIMELINE                       │
├──────────────────────────────────────────────────────────────────┤
│                                                                   │
│  Month 0: Implementation                                          │
│  ├─ One-time costs: $120,000                                     │
│  └─ Setup, training, integration                                 │
│                                                                   │
│  Month 1-12: Operations                                           │
│  ├─ Monthly AI costs: $26,000/month                              │
│  ├─ Monthly gross benefits: $10,837,000/month                    │
│  └─ Monthly net benefit: $10,811,000/month                       │
│                                                                   │
│  Payback: 0.04 months (1.2 days)                                 │
│  ├─ Total costs ($432K) / Monthly net benefit ($10.8M)           │
│  └─ Investment recovered in just over 1 day!                     │
│                                                                   │
│  12-Month Net Value: $129,608,000                                │
│  ├─ Total benefits: $130,040,000                                 │
│  ├─ Total costs: $432,000                                        │
│  └─ ROI: 30,002%                                                 │
│                                                                   │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │                    VISUAL TIMELINE                         │  │
│  │                                                            │  │
│  │  Day 1: ████████████████████████████████ Payback!         │  │
│  │  Month 1: ████████████████████████████████ $10.8M profit  │  │
│  │  Month 2: ████████████████████████████████ $10.8M profit  │  │
│  │  ...                                                       │  │
│  │  Month 12: ███████████████████████████████ $10.8M profit  │  │
│  │                                                            │  │
│  │  Total 12-Month Profit: $129.6M                           │  │
│  └────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
```

### Important Validation Rules

**Boundary Conditions:**

```
┌──────────────────────────────────────────────────────────────────┐
│                    VALIDATION CHECKS                              │
├──────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ✓ questions_per_month > 0                                       │
│  ✓ ai_agent_cost_per_month ≥ 0                                   │
│  ✓ analysis_period_months > 0                                    │
│  ✓ minutes_per_question_without_ai ≥ 0                           │
│  ✓ minutes_per_question_with_ai ≥ 0                              │
│  ✓ percent_questions_that_save_time: 0-100%                      │
│  ✓ labor_cost_per_hour ≥ 0                                       │
│                                                                   │
│  ✗ time_saved_per_question ≤ 0                                   │
│    → ERROR: "AI does not save time"                              │
│                                                                   │
│  ✓ percent_time_to_new_projects: 0-100%                          │
│  ✓ revenue_per_employee_per_hour ≥ 0                             │
│                                                                   │
│  ✓ total_customer_count > 0                                      │
│  ✓ customer_churn_before_ai: 0-100%                              │
│  ✓ customer_churn_after_ai: 0-100%                               │
│  ✓ average_monthly_revenue_per_customer ≥ 0                      │
│                                                                   │
│  ✗ churn_reduction_rate ≤ 0                                      │
│    → ERROR: "AI does not reduce churn"                           │
│                                                                   │
│  ✓ one_time_implementation_cost ≥ 0                              │
│  ✓ one_time_training_cost ≥ 0                                    │
└──────────────────────────────────────────────────────────────────┘
```

---

## Example Business Value Scenarios

### Scenario 1: Customer Support Automation
```
Questions: 1,000,000/month
Time saved: 8 min/question (10 min → 2 min)
Success rate: 80%
Labor cost: $100/hour
AI costs: $26,000/month

Benefits:
- Labor savings: $128M/year
- Churn reduction: $2M/year
- Total: $130M/year

Costs:
- Implementation: $120K (one-time)
- AI infrastructure: $312K/year
- Total: $432K/year

ROI: 30,002%
Payback: 1.2 days
Net Value: $129.6M/year
```

### Scenario 2: Sales Productivity Enhancement
```
Questions: 500,000/month
Time saved: 15 min/question (20 min → 5 min)
Success rate: 90%
Revenue per hour: $200/hour
Time to new projects: 70%
AI costs: $15,000/month

Benefits:
- Revenue growth: $94.5M/year
- Churn reduction: $1M/year
- Total: $95.5M/year

Costs:
- Implementation: $80K (one-time)
- AI infrastructure: $180K/year
- Total: $260K/year

ROI: 36,638%
Payback: 0.8 days
Net Value: $95.2M/year
```

### Scenario 3: Technical Support Optimization
```
Questions: 2,000,000/month
Time saved: 5 min/question (8 min → 3 min)
Success rate: 70%
Labor cost: $75/hour
AI costs: $40,000/month

Benefits:
- Labor savings: $105M/year
- Churn reduction: $3M/year
- Total: $108M/year

Costs:
- Implementation: $150K (one-time)
- AI infrastructure: $480K/year
- Total: $630K/year

ROI: 17,043%
Payback: 2.1 days
Net Value: $107.4M/year
```

---

## Business Value Optimization Tips

### Maximize Benefits:
1. **Increase success rate**: Improve AI accuracy to save time on more questions
2. **Target high-value tasks**: Focus on questions with longest manual time
3. **Improve churn reduction**: Better customer experience = lower churn
4. **Allocate saved time wisely**: Direct freed-up time to revenue-generating activities

### Minimize Costs:
1. **Right-size AI infrastructure**: Match Bedrock + AgentCore to actual needs
2. **Optimize token usage**: Reduce input/output tokens where possible
3. **Selective components**: Only use AgentCore components you need
4. **Efficient training**: Minimize one-time implementation costs

### Improve ROI:
1. **Scale volume**: Higher question volume = better ROI (costs scale linearly)
2. **Fast implementation**: Shorter time to value = faster payback
3. **Measure accurately**: Track actual time savings and churn reduction
4. **Iterate and optimize**: Continuously improve AI performance

---

## Key Takeaways

### Business Value Analysis Principles:

1. **Avoid Double-Counting**: Use EITHER cost savings OR revenue growth for time savings, not both
2. **Gross vs Net**: Benefits are "gross" (before AI costs), then AI costs subtracted for net value
3. **Independent Benefits**: Customer churn reduction is independent and can combine with either option
4. **Fast Payback**: Typical payback periods are measured in days, not months
5. **Exceptional ROI**: ROI typically ranges from 10,000% to 50,000% for successful implementations
6. **Linear Scaling**: Benefits and costs both scale linearly with volume

### When to Use Cost Savings vs Revenue Growth:

**Use Cost Savings when:**
- Primary goal is reducing labor costs
- Headcount reduction or reallocation is planned
- Support/operations efficiency is the focus

**Use Revenue Growth when:**
- Primary goal is increasing revenue
- Freed-up time goes to sales/product development
- Productivity increase is the focus

**Customer Churn Reduction:**
- Always independent
- Can combine with either Cost Savings or Revenue Growth
- Particularly valuable for subscription businesses

---

## Conclusion

Business Value Analysis provides a comprehensive view of the financial impact of AI Agent implementation. Understanding the calculation logic helps:
- Make informed investment decisions
- Set realistic expectations
- Track and measure actual ROI
- Optimize for maximum business value

The typical business case shows exceptional ROI (>10,000%) with payback periods measured in days, making AI Agent solutions highly attractive investments for organizations processing high volumes of questions.
