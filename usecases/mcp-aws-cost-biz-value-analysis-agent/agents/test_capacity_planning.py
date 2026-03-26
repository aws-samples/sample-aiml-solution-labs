#!/usr/bin/env python3
"""
Test script for the AWS TCO & BVA Analyst agent.

Runs two capacity-planning queries against the full AwsTcoBvaAnalyst agent
to verify quota lookup and capacity planning tools work end-to-end.

Usage:
    python test_capacity_planning.py
"""

from aws_tco_bva_analyst import AwsTcoBvaAnalyst

TEST_QUERIES = [
    # Q1: Simple quota lookup
    "What is the default quota of Claude 4.6 Sonnet?",
    # Q2: Capacity planning with explicit workload
    (
        "I have a customer who is building a chatbot using Claude 4.6 Sonnet. "
        "Expected traffic is 10,000 queries per a minute, each query consumes "
        "1,000 input tokens and 2,000 output tokens. "
        "Advise if we need quota increase"
    ),
    # Q3: RAG scenario — embedding + generation model, chunked docs
    (
        "A customer is building a RAG-based knowledge assistant. They have 500,000 "
        "documents averaging 2,000 tokens each. At query time, the system retrieves "
        "10 chunks of 500 tokens each, prepends a 200-token system prompt, and the "
        "user query averages 100 tokens. They expect 5,000 queries per hour during "
        "business hours (8 hours/day, 22 days/month). They plan to use Titan Text "
        "Embeddings V2 for retrieval and Claude Sonnet 4.6 for generation. "
        "What quotas do they need and will the defaults be sufficient?"
    ),
    # Q4: Multi-agent orchestration — supervisor + specialist agents
    (
        "We are designing a multi-agent system with 1 supervisor agent and 3 specialist "
        "agents. The supervisor uses Claude Sonnet 4.6 and routes each user request to "
        "one of the specialists. Each specialist also uses Claude Sonnet 4.6 and makes "
        "2-3 tool calls per task (each tool call is a separate LLM invocation). "
        "Expected load: 2,000 end-user requests per minute. Each supervisor call uses "
        "500 input / 200 output tokens. Each specialist call uses 1,500 input / 1,000 "
        "output tokens. So each user request generates ~1 supervisor call + ~3 specialist "
        "calls = 4 LLM calls total. What is the effective RPM/TPM requirement and do we "
        "need a quota increase?"
    ),
    # Q5: Voice + agentic AI — Nova Sonic + LangGraph backend agents
    (
        "I have a customer who wants to build an agentic AI enabled with voice interface "
        "for customer support order processing. The front-end voice interface will use "
        "Amazon Nova Sonic (Nova 2 Sonic) for speech-to-speech. Behind the voice layer, "
        "the system talks to the customer's existing agents built using LangGraph and "
        "Claude 4.6 Sonnet model. Each voice session averages 5 minutes with about 10 "
        "back-and-forth turns. Each turn triggers 1 Nova Sonic call plus 2 LangGraph "
        "agent calls (each using Claude 4.6 Sonnet with 2,000 input / 1,500 output "
        "tokens per call). Expected load: 1,000 customers per hour, 24/7. "
        "What are the quota requirements for both Nova Sonic and Claude 4.6 Sonnet, "
        "and do we need quota increases?"
    ),
]


def main():
    print("=" * 70)
    print("AWS TCO & BVA Analyst - Capacity Planning Test")
    print("=" * 70)

    agent = AwsTcoBvaAnalyst()
    print(f"Region : {agent.region}")
    print(f"Model  : {agent.model_id}")
    print()

    for i, query in enumerate(TEST_QUERIES, 1):
        print(f"\n{'─' * 70}")
        print(f"Query {i}: {query}")
        print(f"{'─' * 70}\n")

        try:
            response = agent.analyze(query)
            print(f"\nAgent response:\n{response}\n")
        except Exception as e:
            print(f"\nError: {e}\n")
            import traceback
            traceback.print_exc()

    print("=" * 70)
    print("Test complete")
    print("=" * 70)


if __name__ == "__main__":
    main()
