"""S3 logger for steering relevancy scores."""

import os
import json
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

BUCKET = os.environ.get("STEERING_LOG_BUCKET", "")
REGION = os.environ.get("AWS_REGION", "us-west-2")

_s3_client = None


def _get_s3():
    global _s3_client
    if _s3_client is None:
        import boto3
        _s3_client = boto3.client("s3", region_name=REGION)
    return _s3_client


def log_score(
    session_id: str,
    check_type: str,
    invocation_id: str = "",
    tool_name: str = "",
    user_query: str = "",
    tool_query: str = "",
    llm_judge_score: float = None,
    guardrail_grounding: float = None,
    guardrail_relevance: float = None,
    mode: str = "passive",
    retry_count: int = 0,
    details: str = "",
):
    """Log a relevancy score record to S3 (or local file as fallback)."""
    now = datetime.now(timezone.utc)
    record = {
        "timestamp": now.isoformat(),
        "session_id": session_id,
        "invocation_id": invocation_id,
        "check_type": check_type,
        "tool_name": tool_name,
        "user_query": user_query[:500],
        "tool_query": tool_query[:500],
        "llm_judge_score": llm_judge_score,
        "guardrail_grounding": guardrail_grounding,
        "guardrail_relevance": guardrail_relevance,
        "mode": mode,
        "retry_count": retry_count,
        "details": details[:1000],
    }

    if BUCKET:
        try:
            key = f"scores/{now.strftime('%Y/%m/%d')}/{session_id}/{invocation_id}_{check_type}_{now.strftime('%H%M%S_%f')}.json"
            _get_s3().put_object(
                Bucket=BUCKET,
                Key=key,
                Body=json.dumps(record).encode(),
                ContentType="application/json",
            )
            logger.info(f"Logged {check_type} score to s3://{BUCKET}/{key}")
        except Exception as e:
            logger.error(f"Failed to log to S3: {e}")
            _log_local(record)
    else:
        _log_local(record)


def _log_local(record: dict):
    """Fallback: append to local JSONL file."""
    path = "/tmp/steering_scores.jsonl"
    try:
        with open(path, "a") as f:
            f.write(json.dumps(record) + "\n")
        logger.info(f"Logged score locally to {path}")
    except Exception as e:
        logger.error(f"Failed to log locally: {e}")
