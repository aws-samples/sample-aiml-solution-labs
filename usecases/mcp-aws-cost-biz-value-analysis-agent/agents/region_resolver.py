"""
Region name to region code resolver.

Provides a deterministic lookup from human-friendly region names
(e.g., "Oregon", "US West (Oregon)") to AWS region codes (e.g., "us-west-2").
Uses the official mapping from region_mapping.txt so the agent doesn't
rely on its own memory for region resolution.
"""

import os
import logging
from strands import tool

logger = logging.getLogger("region_resolver")

# Load region mapping once at import time from the text file
_REGION_MAP = {}
_mapping_file = os.path.join(os.path.dirname(__file__), "region_mapping.txt")

with open(_mapping_file) as f:
    for line in f:
        line = line.strip()
        if not line or ":" not in line:
            continue
        long_name, region_code = line.rsplit(":", 1)
        long_name = long_name.strip()
        region_code = region_code.strip()
        _REGION_MAP[long_name.lower()] = region_code


@tool
def resolve_region(region_name: str) -> dict:
    """
    Resolve a human-friendly AWS region name to its region code.
    ALWAYS use this tool when the user specifies a region by name
    (e.g., "Oregon", "Mumbai", "Ireland") instead of by code.

    Args:
        region_name: Region name as provided by the user
                     (e.g., "Oregon", "US West (Oregon)", "Mumbai")

    Returns:
        dict with region_code and matched_name, or error if not found
    """
    query = region_name.strip().lower()

    # Try exact match first (e.g., "US West (Oregon)")
    if query in _REGION_MAP:
        code = _REGION_MAP[query]
        logger.info("Region resolved: '%s' → %s", region_name, code)
        return {"region_code": code, "matched_name": region_name}

    # Try partial match (e.g., "Oregon" matches "US West (Oregon)")
    matches = []
    for long_name, code in _REGION_MAP.items():
        if query in long_name:
            matches.append((long_name, code))

    if len(matches) == 1:
        matched_name, code = matches[0]
        logger.info("Region resolved: '%s' → %s (matched '%s')", region_name, code, matched_name)
        return {"region_code": code, "matched_name": matched_name}

    if len(matches) > 1:
        options = [f"{name} ({code})" for name, code in matches]
        return {
            "error": f"Ambiguous region '{region_name}'. Multiple matches: {', '.join(options)}",
            "action_required": "Ask the user to clarify which region they mean.",
        }

    return {
        "error": f"Unknown region '{region_name}'. Not found in region mapping.",
        "action_required": "Ask the user for a valid AWS region name or code.",
    }
