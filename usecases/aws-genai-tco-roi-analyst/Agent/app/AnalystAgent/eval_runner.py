"""Evaluation script for the steering handler experiment.

Runs test cases against the deployed agent, collects Strands Evals scores
and links them to S3 steering scores via session_id.

Usage:
    pip install strands-agents-evals
    python eval_runner.py [--cases 5] [--output eval_results.json]
"""

import os
import sys
import json
import time
import argparse
import boto3
from strands import Agent
from strands.models import BedrockModel
from strands_evals import Case, Experiment
from strands_evals.evaluators import FaithfulnessEvaluator, OutputEvaluator
from strands_evals.telemetry import StrandsEvalsTelemetry
from strands_evals.mappers import StrandsInMemorySessionMapper
from eval_test_cases import TEST_CASES

REGION = os.environ.get("AWS_REGION", "us-west-2")
LOG_BUCKET = os.environ.get("STEERING_LOG_BUCKET", "")
KB_ID = os.environ.get("STRANDS_KNOWLEDGE_BASE_ID", "")

# Setup telemetry
telemetry = StrandsEvalsTelemetry().setup_in_memory_exporter()


def create_agent_for_eval(session_id: str):
    """Create a local agent instance matching the deployed agent config."""
    from search_pricing_info import call_pricing_search_agent
    from search_bedrock_quota import call_bedrock_quota_agent
    from calculator_bedrock import use_bedrock_calculator, bedrock_what_if_analysis
    from calculator_agentcore import use_agentcore_calculator, agentcore_what_if_analysis
    from calculator_bva import bva_calculator, bva_what_if_analysis
    from calculator_capacity_planning import capacity_planning_calculator
    from system_prompt import TCO_ANALYST_PROMPT
    from relevancy_steering_handler import RelevancySteeringHandler

    model = BedrockModel(
        model_id=os.environ.get("MODEL_ID", "us.anthropic.claude-sonnet-4-5-20250929-v1:0"),
        temperature=0.1,
    )

    agent = Agent(
        model=model,
        system_prompt=TCO_ANALYST_PROMPT,
        tools=[
            call_pricing_search_agent, call_bedrock_quota_agent,
            use_bedrock_calculator, bedrock_what_if_analysis,
            use_agentcore_calculator, agentcore_what_if_analysis,
            bva_calculator, bva_what_if_analysis,
            capacity_planning_calculator,
        ],
        plugins=[RelevancySteeringHandler()],
        trace_attributes={
            "gen_ai.conversation.id": session_id,
            "session.id": session_id,
        },
        callback_handler=None,
    )
    agent._steering_session_id = session_id
    return agent


def task_function(case: Case) -> dict:
    """Run a test case through the agent and capture trajectory."""
    telemetry.in_memory_exporter.clear()

    agent = create_agent_for_eval(case.session_id)

    try:
        response = agent(case.input)
        output = str(response)
    except Exception as e:
        output = f"ERROR: {e}"

    # Map spans for trace-based evaluators
    finished_spans = telemetry.in_memory_exporter.get_finished_spans()
    mapper = StrandsInMemorySessionMapper()
    session = mapper.map_to_session(finished_spans, session_id=case.session_id)

    return {"output": output, "trajectory": session}


def fetch_s3_scores(session_id: str) -> list:
    """Pull steering scores from S3 for a given session_id."""
    if not LOG_BUCKET:
        return []
    s3 = boto3.client("s3", region_name=REGION)
    prefix = f"scores/"
    scores = []
    try:
        resp = s3.list_objects_v2(Bucket=LOG_BUCKET, Prefix=prefix)
        for obj in resp.get("Contents", []):
            if session_id in obj["Key"] or session_id == "unknown":
                body = s3.get_object(Bucket=LOG_BUCKET, Key=obj["Key"])["Body"].read()
                record = json.loads(body)
                if record.get("session_id") == session_id:
                    scores.append(record)
    except Exception as e:
        print(f"  Warning: could not fetch S3 scores: {e}")
    return scores


def main():
    parser = argparse.ArgumentParser(description="Run steering handler evaluation")
    parser.add_argument("--cases", type=int, default=len(TEST_CASES), help="Number of test cases to run")
    parser.add_argument("--output", type=str, default="eval_results.json", help="Output file for results")
    parser.add_argument("--level", type=str, default=None, help="Filter by level: L100, L200, L300, L400, edge")
    args = parser.parse_args()

    cases_to_run = TEST_CASES
    if args.level:
        cases_to_run = [c for c in cases_to_run if c["level"] == args.level]
    cases_to_run = cases_to_run[:args.cases]

    print(f"Running {len(cases_to_run)} test cases...")

    # Build Strands Eval cases
    eval_cases = [
        Case[str, str](
            name=tc["name"],
            input=tc["input"],
            metadata={"level": tc["level"], "category": tc["category"]},
        )
        for tc in cases_to_run
    ]

    # Evaluators
    faithfulness = FaithfulnessEvaluator()
    output_eval = OutputEvaluator(
        rubric="""Evaluate the pricing/cost response:
        1. Does it answer the specific question asked?
        2. Does it use the correct model/service names?
        3. Are the numbers plausible for AWS pricing?
        4. Does it specify the region if the user asked for one?
        Score 1.0 if accurate and complete, 0.5 if partially correct, 0.0 if wrong or irrelevant.""",
        include_inputs=True,
    )

    experiment = Experiment[str, str](
        cases=eval_cases,
        evaluators=[faithfulness, output_eval],
    )

    reports = experiment.run_evaluations(task_function)

    # Collect results and link to S3 steering scores
    results = []
    for report in reports:
        evaluator_name = report.evaluator_name
        for i, case in enumerate(report.cases):
            session_id = eval_cases[i].session_id if i < len(eval_cases) else "unknown"
            s3_scores = fetch_s3_scores(session_id)

            results.append({
                "case_name": cases_to_run[i]["name"],
                "input": cases_to_run[i]["input"],
                "level": cases_to_run[i]["level"],
                "category": cases_to_run[i]["category"],
                "evaluator": evaluator_name,
                "eval_score": report.scores[i] if i < len(report.scores) else None,
                "eval_pass": report.test_passes[i] if i < len(report.test_passes) else None,
                "eval_reason": report.reasons[i] if i < len(report.reasons) else None,
                "session_id": session_id,
                "steering_scores": s3_scores,
            })

    # Save results
    with open(args.output, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to {args.output}")

    # Print summary
    print(f"\n{'='*60}")
    print(f"EVALUATION SUMMARY")
    print(f"{'='*60}")
    for r in results:
        steering_judge = next(
            (s.get("llm_judge_score") for s in r["steering_scores"] if s.get("check_type") == "after_response"),
            None,
        )
        steering_grounding = next(
            (s.get("guardrail_grounding") for s in r["steering_scores"] if s.get("check_type") == "after_response"),
            None,
        )
        score_str = f"{r['eval_score']:.2f}" if r['eval_score'] is not None else "N/A "
        judge_str = f"{steering_judge:.2f}" if steering_judge is not None else "N/A "
        ground_str = f"{steering_grounding:.2f}" if steering_grounding is not None else "N/A "
        pass_str = "PASS" if r["eval_pass"] else "FAIL"
        print(f"  {r['case_name']:12s} [{r['level']:5s}] [{r['evaluator']:20s}] "
              f"eval={score_str}  judge={judge_str}  grounding={ground_str}  {pass_str}")

    # Summary by level
    print(f"\n{'='*60}")
    print("BY LEVEL:")
    for level in ["L100", "L200", "L300", "L400", "edge"]:
        level_results = [r for r in results if r["level"] == level and r["eval_score"] is not None]
        if not level_results:
            continue
        avg_score = sum(r["eval_score"] for r in level_results) / len(level_results)
        pass_rate = sum(1 for r in level_results if r["eval_pass"]) / len(level_results)
        print(f"  {level}: avg_score={avg_score:.2f}  pass_rate={pass_rate:.0%}  n={len(level_results)}")


if __name__ == "__main__":
    main()
