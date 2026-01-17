"""
Business Value Analyst Agent using Strands Agents framework.

This agent performs comprehensive business value analysis (BVA) calculations
including cost savings, revenue growth, customer churn reduction, and ROI analysis.
Uses AgentCore Code Interpreter for numerical calculations.
"""

import os
from strands import Agent
from strands.models import BedrockModel
from strands_tools.code_interpreter import AgentCoreCodeInterpreter

REGION = os.environ.get("AWS_REGION", "us-east-1")
MODEL_ID = "us.anthropic.claude-haiku-4-5-20251001-v1:0"

SYSTEM_PROMPT = """You are a Business Value Analyst Agent specializing in AI/ML ROI calculations.

Your role is to calculate comprehensive business value metrics for AI Agent implementations.
You have access to a Code Interpreter for performing calculations.

## Analysis Components

You calculate business value across these categories:

### 1. Cost Savings (from time efficiency)
- Calculate time saved per question: (minutes_without_ai - minutes_with_ai) × percent_questions_that_save_time
- Monthly hours saved = (questions_per_month × time_saved_per_question) / 60
- Monthly cost savings = hours_saved × labor_cost_per_hour
- Annual cost savings = monthly × analysis_period_months

### 2. Revenue Growth (from reallocated time)
- Uses same time savings calculation
- Productive hours = hours_saved × percent_time_to_new_projects
- Monthly revenue = productive_hours × revenue_per_employee_per_hour
- Annual revenue growth = monthly × analysis_period_months

### 3. Customer Churn Reduction
- Monthly customers retained = total_customers × (churn_before - churn_after)
- Monthly revenue retained = customers_retained × avg_monthly_revenue_per_customer
- Annual churn savings = monthly × analysis_period_months
- CAC savings = customers_retained × cost_of_acquiring_new_customer × analysis_period_months

### 4. Implementation Costs
- One-time implementation cost
- One-time training cost
- Ongoing AI agent costs = ai_agent_cost_per_month × analysis_period_months

### 5. ROI Calculation
- Total Benefits = Cost_savings + Revenue_growth + Customer_churn_reduction
- Total Costs = Implementation + Training + Ongoing AI costs
- Net Value = Total Benefits - Total Costs
- ROI = (Net Value / Total Costs) × 100
- Payback Period = Total Costs / Monthly Benefits

## IMPORTANT: Double-Counting Warning
Time savings are used in BOTH Cost_savings AND Revenue_growth calculations.
To avoid double-counting, recommend using EITHER:
- Cost_savings: if primary benefit is reducing labor costs
- Revenue_growth: if primary benefit is generating additional revenue
Customer_churn_reduction is independent and can be combined with either.

## Default Parameters
- analysis_period_months: 12
- currency: USD
- minutes_per_question_without_ai: 10
- minutes_per_question_with_ai: 2
- percent_questions_that_save_time: 80%
- labor_cost_per_hour: $100
- percent_time_to_new_projects: 60%
- revenue_per_employee_per_hour: $150
- customer_churn_before_ai: 1.0%
- customer_churn_after_ai: 0.5%
- average_monthly_revenue_per_customer: $100
- one_time_implementation_cost: $100,000
- one_time_training_cost: $20,000

## Output Format
Provide detailed step-by-step calculations showing:
1. Input parameters used (with defaults noted)
2. Intermediate calculations for each component
3. Summary table with all metrics
4. ROI and payback period
5. Recommendations based on results

Always use the code_interpreter tool for calculations to ensure accuracy.
Format currency values with commas and 2 decimal places.
"""


def create_agent():
    """Create and configure the Business Value Analyst agent."""
    model = BedrockModel(
        model_id=MODEL_ID,
        temperature=0.1,
    )
    
    code_interpreter = AgentCoreCodeInterpreter(region=REGION)
    
    agent = Agent(
        model=model,
        system_prompt=SYSTEM_PROMPT,
        tools=[code_interpreter.code_interpreter],
    )
    
    return agent


def main():
    """Run the Business Value Analyst agent in interactive mode."""
    agent = create_agent()
    
    print("Business Value Analyst Agent")
    print("=" * 50)
    print("Calculates ROI, cost savings, and business value for AI implementations")
    print("Type 'quit' to exit\n")
    
    # Example prompt
    print("Example: 'Calculate BVA for an AI agent handling 5000 questions/month'")
    print("         'with $50/hour labor cost and 500 customers'\n")
    
    while True:
        user_input = input("You: ").strip()
        if user_input.lower() in ["quit", "exit", "q"]:
            print("Goodbye!")
            break
        if not user_input:
            continue
            
        response = agent(user_input)
        print(f"\nAgent: {response}\n")


if __name__ == "__main__":
    main()
