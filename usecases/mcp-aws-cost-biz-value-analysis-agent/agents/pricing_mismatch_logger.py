"""
Pricing mismatch logging tool for the Strands agent.

When the agent verifies retrieved pricing chunks against the user's request,
it may detect mismatches (wrong model version, wrong region, etc.). This tool
logs those corrections to console/CloudWatch so we have observability into
how often the verification safety net catches errors — without polluting
the user-facing response.

This tool is only called on the ~10% of queries where a mismatch is detected,
so it adds zero overhead on the happy path.
"""

import logging
from strands import tool

logger = logging.getLogger("pricing_mismatch_logger")


@tool
def log_pricing_mismatch(
    requested_model: str,
    requested_region: str,
    chunk_model: str,
    chunk_region: str,
    corrective_action: str,
) -> str:
    """
    Log a pricing chunk mismatch that was detected during verification.
    Call this ONLY when you detect that a retrieved pricing chunk does not match
    the user's requested model or region, and you are correcting it.

    Args:
        requested_model: The model name/version the user asked for
        requested_region: The region the user asked for
        chunk_model: The model name/version found in the mismatched chunk
        chunk_region: The region found in the mismatched chunk
        corrective_action: What you did to correct it (e.g., "replaced with correct chunk", "discarded, no matching chunk found")

    Returns:
        Confirmation that the mismatch was logged
    """
    logger.warning(
        "⚠️ PRICING MISMATCH DETECTED | "
        "Requested: model='%s' region='%s' | "
        "Chunk had: model='%s' region='%s' | "
        "Action: %s",
        requested_model,
        requested_region,
        chunk_model,
        chunk_region,
        corrective_action,
    )
    return "Mismatch logged. Continue with the corrected data."
