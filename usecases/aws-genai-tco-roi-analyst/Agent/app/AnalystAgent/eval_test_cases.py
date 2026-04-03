"""Test cases for steering handler evaluation. 50 cases across L100-L400 user levels."""

TEST_CASES = [
    # ── L100: Vague, non-technical users ──
    {"name": "L100-01", "input": "how much would it cost to build a chatbot with AI on AWS?", "level": "L100", "category": "vague"},
    {"name": "L100-02", "input": "what's the cheapest way to use AI for my company?", "level": "L100", "category": "vague"},
    {"name": "L100-03", "input": "I want to use Claude for customer support, what will it cost me monthly?", "level": "L100", "category": "vague"},
    {"name": "L100-04", "input": "how much does Amazon AI cost?", "level": "L100", "category": "vague"},
    {"name": "L100-05", "input": "we need an AI assistant for our team of 50 people, what's the pricing?", "level": "L100", "category": "vague"},
    {"name": "L100-06", "input": "is bedrock expensive?", "level": "L100", "category": "vague"},
    {"name": "L100-07", "input": "can you compare the cost of using AI vs hiring someone?", "level": "L100", "category": "bva"},
    {"name": "L100-08", "input": "what's the ROI of using AI for document processing?", "level": "L100", "category": "bva"},

    # ── L200: Knows AWS, not Bedrock specifics ──
    {"name": "L200-01", "input": "what models are available on Bedrock and how much do they cost?", "level": "L200", "category": "general_pricing"},
    {"name": "L200-02", "input": "which is cheaper, Claude or Nova for text generation?", "level": "L200", "category": "comparison"},
    {"name": "L200-03", "input": "how does Bedrock pricing work? is it per request or per token?", "level": "L200", "category": "general_pricing"},
    {"name": "L200-04", "input": "what's the difference between on-demand and provisioned throughput pricing?", "level": "L200", "category": "general_pricing"},
    {"name": "L200-05", "input": "I'm running a RAG application, what are all the costs involved?", "level": "L200", "category": "architecture"},
    {"name": "L200-06", "input": "how much would it cost to process 1000 documents per day with Bedrock?", "level": "L200", "category": "estimation"},
    {"name": "L200-07", "input": "what's the cost of using Bedrock agents?", "level": "L200", "category": "agentcore"},
    {"name": "L200-08", "input": "compare Nova Pro vs Claude Haiku for a summarization use case", "level": "L200", "category": "comparison"},
    {"name": "L200-09", "input": "what are the quota limits for Bedrock models?", "level": "L200", "category": "quota"},
    {"name": "L200-10", "input": "how much does the knowledge base feature cost in Bedrock?", "level": "L200", "category": "general_pricing"},
    {"name": "L200-11", "input": "what's cheaper for a simple Q&A bot, Haiku or Nova Lite?", "level": "L200", "category": "comparison"},
    {"name": "L200-12", "input": "estimate monthly cost for a bedrock app with 10000 users", "level": "L200", "category": "estimation"},

    # ── L300: Knows Bedrock, asks specific questions ──
    {"name": "L300-01", "input": "what is the input token price for Claude Sonnet 4 in us-east-1?", "level": "L300", "category": "specific_pricing"},
    {"name": "L300-02", "input": "how much does Nova Pro cost per million output tokens in us-west-2?", "level": "L300", "category": "specific_pricing"},
    {"name": "L300-03", "input": "what's the RPM limit for Claude Haiku 4.5 on-demand in us-east-1?", "level": "L300", "category": "quota"},
    {"name": "L300-04", "input": "calculate the cost for 500M input tokens and 100M output tokens per month using Claude Sonnet 4.5", "level": "L300", "category": "calculation"},
    {"name": "L300-05", "input": "what's the price difference between Claude Sonnet 4 and Claude Sonnet 4.5 for input tokens?", "level": "L300", "category": "comparison"},
    {"name": "L300-06", "input": "how much does prompt caching cost for Claude Opus 4?", "level": "L300", "category": "specific_pricing"},
    {"name": "L300-07", "input": "what's the batch inference pricing for Nova Pro?", "level": "L300", "category": "specific_pricing"},
    {"name": "L300-08", "input": "give me the AgentCore runtime cost per hour for a session", "level": "L300", "category": "agentcore"},
    {"name": "L300-09", "input": "what are the TPM limits for cross-region inference with Claude Sonnet 4?", "level": "L300", "category": "quota"},
    {"name": "L300-10", "input": "calculate bedrock cost for a RAG app: 1000 queries/day, avg 2000 input tokens, 500 output tokens, using Haiku 4.5", "level": "L300", "category": "calculation"},
    {"name": "L300-11", "input": "what's the cost of Bedrock Guardrails per 1000 text units?", "level": "L300", "category": "specific_pricing"},
    {"name": "L300-12", "input": "compare the total monthly cost of Sonnet 4 vs Nova Pro for 10M tokens input, 2M output", "level": "L300", "category": "comparison"},

    # ── L400: Expert, complex scenarios ──
    {"name": "L400-01", "input": "I need a capacity plan for a multi-model setup: Claude Sonnet 4 for complex queries (20% traffic, avg 4000 input/1000 output tokens) and Nova Lite for simple queries (80% traffic, avg 500 input/200 output tokens), total 50000 requests per day in us-east-1", "level": "L400", "category": "capacity_planning"},
    {"name": "L400-02", "input": "do a what-if analysis on Bedrock costs varying input tokens from 1M to 100M monthly using Claude Sonnet 4.5, show me how cost scales", "level": "L400", "category": "what_if"},
    {"name": "L400-03", "input": "compare the TCO of building an agent with AgentCore vs self-hosting on ECS with Bedrock API calls, assuming 10000 sessions per day", "level": "L400", "category": "tco"},
    {"name": "L400-04", "input": "what's the cost optimization if I switch from Sonnet 4 to Haiku 4.5 for my classification pipeline doing 1M requests/month with avg 500 input tokens and 50 output tokens?", "level": "L400", "category": "optimization"},
    {"name": "L400-05", "input": "calculate the break-even point for provisioned throughput vs on-demand for Claude Sonnet 4 at various RPM levels from 100 to 10000", "level": "L400", "category": "capacity_planning"},
    {"name": "L400-06", "input": "I have a multi-turn agent that averages 8 turns per conversation, each turn ~1500 input tokens growing with history, 400 output tokens. 5000 conversations/day. What's the monthly cost with Sonnet 4.5 and how much would prompt caching save?", "level": "L400", "category": "calculation"},
    {"name": "L400-07", "input": "build a business value analysis: we're replacing 5 FTEs ($80k each) with a Bedrock-powered automation handling 2000 tickets/day. What's the ROI over 12 months including all Bedrock and AgentCore costs?", "level": "L400", "category": "bva"},
    {"name": "L400-08", "input": "what-if analysis: how does total cost change if I vary both the model (Haiku 4.5, Sonnet 4, Sonnet 4.5) and the daily request volume (1000, 5000, 10000, 50000)?", "level": "L400", "category": "what_if"},

    # ── Edge cases: wrong names, ambiguous, not in KB ──
    {"name": "EDGE-01", "input": "how much does GPT-4 cost on Bedrock?", "level": "edge", "category": "wrong_model"},
    {"name": "EDGE-02", "input": "what's the pricing for Claude 4?", "level": "edge", "category": "ambiguous_name"},
    {"name": "EDGE-03", "input": "cost of Titan Image Generator v3 per image", "level": "edge", "category": "specific_pricing"},
    {"name": "EDGE-04", "input": "how much is Llama 4 Maverick on Bedrock in eu-west-1?", "level": "edge", "category": "specific_pricing"},
    {"name": "EDGE-05", "input": "what's the pricing for Bedrock in the Sydney region?", "level": "edge", "category": "region_informal"},
    {"name": "EDGE-06", "input": "compare all Claude models pricing side by side", "level": "edge", "category": "broad_comparison"},
    {"name": "EDGE-07", "input": "I need pricing but I'm not sure which model to use. I just need something for email drafting.", "level": "edge", "category": "needs_guidance"},
    {"name": "EDGE-08", "input": "what's the cheapest model that supports tool use?", "level": "edge", "category": "feature_based"},
    {"name": "EDGE-09", "input": "how much would it cost to fine-tune Claude on Bedrock?", "level": "edge", "category": "not_in_kb"},
    {"name": "EDGE-10", "input": "give me pricing in euros for Bedrock in Frankfurt", "level": "edge", "category": "currency"},
]
