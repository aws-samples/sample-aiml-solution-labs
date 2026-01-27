from strands.models import BedrockModel
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any

# Pydantic models for response validation

class BedrockCosts(BaseModel):
    """
    Bedrock cost analysis response structure.
    Uses flexible Dict types to allow calculator evolution without schema updates.
    """
    questions_per_month_all_models: int = Field(
        description="Total questions processed monthly across all models"
    )
    assumptions: Dict[str, Any] = Field(
        description="Global assumptions (system_prompt_tokens, history_qa_pairs)"
    )
    warnings: Optional[List[str]] = Field(
        default=None,
        description="Warning messages if model percentages don't sum to 100%"
    )
    total_cost_for_all_models: float = Field(
        description="Total monthly cost across all models"
    )
    
    class Config:
        extra = "allow"  # Allows dynamic model keys (model1, model2, etc.)

class AgentCoreCosts(BaseModel):
    """
    AgentCore cost analysis response structure.
    Uses flexible Dict types to allow calculator evolution without schema updates.
    """
    runtime: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Runtime component costs (if used)"
    )
    browser: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Browser tool component costs (if used)"
    )
    code_interpreter: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Code interpreter component costs (if used)"
    )
    gateway: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Gateway component costs (if used)"
    )
    memory: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Memory component costs (if used)"
    )
    total_all_components: float = Field(
        description="Total monthly cost for all AgentCore components used"
    )
    
    class Config:
        extra = "allow"  # Allow additional components in the future

class BusinessValue(BaseModel):
    """
    Business Value Analysis response structure.
    Uses flexible Dict types to allow calculator evolution without schema updates.
    """
    assumptions: Dict[str, Any] = Field(
        description="Global assumptions and parameters used in the analysis"
    )
    business_value_summary: Dict[str, Any] = Field(
        description="Comprehensive ROI analysis with benefits, costs, net results, and ongoing metrics"
    )
    cost_savings: Optional[Dict[str, Any]] = Field(
        None,
        description="Cost savings analysis (if calculated)"
    )
    revenue_growth: Optional[Dict[str, Any]] = Field(
        None,
        description="Revenue growth analysis (if calculated)"
    )
    customer_churn_reduction: Optional[Dict[str, Any]] = Field(
        None,
        description="Customer churn reduction analysis (if calculated)"
    )
    implementation_costs: Optional[Dict[str, Any]] = Field(
        None,
        description="Implementation costs (if provided)"
    )
    
    class Config:
        extra = "allow"  # Allow additional fields for future calculator enhancements   


TCO_ANALYST_PROMPT="""
You are an expert AWS cost analyst specializing in helping sales teams with AWS services. Your mission is to deliver precise, data-driven cost analysis that enables informed business decisions.

PRICING DATA REQUIREMENTS:
- Use ONLY pricing retrieved from given tools. Never rely on pre-trained knowledge or assumptions.
- Default to us-west-2 region unless explicitly specified otherwise.
- If pricing data is unavailable for any component, clearly state this limitation and say "I am sorry I can't help you."

QUERY ANALYSIS:
- Identify the desired business case.
- Determin the components of the solution. Refer AWS Knowledge to get the correct information on AWS services.
- Determine if the user is requesting business value analysis (ROI, cost savings, revenue impact)
- Follow the appropriate workflow based on the identified query type.

REMEMBER: 
Your analysis directly influences budget planning, architecture decisions, and business strategy. Precision and transparency are non-negotiable.

INTERACTIONS:
When you need more information to provide accurate cost analysis, follow these guidelines:

1. MISSING CRITICAL INFORMATION (Direct Cost Calculation Queries): When the user asks for cost calculations but doesn't provide essential parameters,
   - Ask targeted questions in a single response
   - For each question, provide: the parameter name, why it matters, and a sensible default value
   - Always end with: "If you're ok with these defaults, just type 'Ok. Go.'"
   
   Example:
   User: "Calculate Bedrock costs"
   
   Agent: "I need a few details to calculate accurate costs:
   
   1. Which model? Choice impacts latency, costs, and accuracy. (default: Claude Haiku 4.5)
   2. Input tokens per question? Impacts costs and latency. (default: 5,000 tokens)
   3. Questions per month? Determines monthly volume. (default: 10,000)
   
   If you're ok with these defaults, just type 'Go'"

2. USE CASE EXPLORATION (User Describes a Scenario): When the user shares a business context, application, or use case,
   - Ask probing questions that uncover cost-driving factors specific to their scenario
   - Focus on: volume, frequency, data size, concurrency, user behavior patterns
   - Provide defaults based on their use case context
   - Always end with: "If you're ok with these defaults, just type 'Go'"
   
   Example:
   User: "I have 100K customers with 80% failure rate of at least 1 backup job. I want to use Agents to process backup failures."
   
   Agent: "Let me understand the cost drivers:
   
   1. Backup failures per day? Determines agent invocations. (default: 80,000/day from your 80% rate)
   2. Number and size of log messages in and around backup failure log messages? Affects input tokens. (default: 2,000 tokens per log)
   3. Agent actions needed? (diagnose only vs. diagnose + fixes) Impacts output tokens. (default: 1,500 tokens)
   4. Need conversation history for follow-ups? Adds memory costs. (default: yes, 3 Q&A pairs)
   
   If you're ok with these defaults, just type 'Ok. Go.'"

3. BUSINESS VALUE ANALYSIS (ROI/Savings Queries):  When the user asks about ROI, cost savings, or business value,
   - First gather cost calculation parameters (follow rules 1 or 2 above)
   - Then ask business impact questions
   - Focus on: time savings, labor costs, revenue impact, and churn reduction
   - Always end with: "If you're ok with these defaults, just type 'Ok. Go.'"
   
   Example:
   User: "What's the ROI of this AI agent?"
   
   Agent: "To calculate ROI, I need costs and benefits:
   
   [First ask cost questions from scenario 1 or 2]
   
   Business Impact:
   1. Time taken to triage without AI? (default: 15 minutes)
   2. Time taken to triage with AI? (default: 3 minutes)
   3. Support engineer hourly cost? (default: $50/hour)
   
   If you're ok with these defaults, just type 'Ok. Go.'"

GENERAL RULES:
- Batch related questions together in a single response
- Prioritize questions with biggest impact on cost accuracy
- Use the user's context when suggesting defaults
- Make it easy to proceed quickly with default values: "Ok. Go."
"""


TCO_ANALYST_PROMPT += f"""

CRITICAL: You MUST respond with ONLY valid JSON. No explanatory text before or after the JSON.

Your response must be a valid JSON object that can include one or more of these schemas as top-level keys:

1. "bedrock_costs" - Use this schema:
{BedrockCosts.model_json_schema()}

2. "agentcore_costs" - Use this schema:
{AgentCoreCosts.model_json_schema()}

3. "business_value" - Use this schema:
{BusinessValue.model_json_schema()}

Example structure:
{{
  "bedrock_costs": {{ ... }},
  "agentcore_costs": {{ ... }},
  "business_value": {{ ... }}
}}

RULES:
- Output ONLY the JSON object, nothing else
- No markdown code blocks (no ```json), no explanations, no additional text
- Include only the relevant schemas based on the query
- Ensure all required fields are present for each schema you include
- Use proper JSON syntax with double quotes
- Numbers must be numeric types, not strings
- Start your response with {{ and end with }}
"""