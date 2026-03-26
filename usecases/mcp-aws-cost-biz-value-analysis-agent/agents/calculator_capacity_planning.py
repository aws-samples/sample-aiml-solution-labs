"""
Bedrock Capacity Planning Calculator Tool.

Pure computation tool for capacity analysis. Takes model quota data (RPM/TPM)
and pricing as input parameters, performs capacity calculations, monthly cost
estimation, and provisioned throughput comparison.

Does NOT fetch any data from AWS APIs. The agent is responsible for calling
get_bedrock_quota first and passing the results here.
"""

import logging
import math
from strands import tool

# Configure logger for this module
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


def _calculate_capacity(params: dict, model_limits: dict) -> dict:
    """
    Compute required RPM/TPM and compare against model limits.

    Args:
        params: Usage parameters dict.
        model_limits: Dict with max_rpm, max_tpm.

    Returns:
        Dict with steady_state and peak_state (or None) sub-dicts.
    """
    model_type = params.get("model_type", "on_demand")
    model_max_rpm = float(model_limits.get("max_rpm", 0))
    model_max_tpm = float(model_limits.get("max_tpm", 0))

    def _compute_state(users, requests_per_hour, avg_input_tokens,
                       avg_output_tokens, images_per_minute,
                       videos_per_hour):
        if model_type == "image":
            required_rpm = float(images_per_minute)
            required_tpm = 0.0
        elif model_type == "video":
            required_rpm = float(videos_per_hour) / 60.0
            required_tpm = 0.0
        else:
            required_rpm = (float(users) * float(requests_per_hour)) / 60.0
            required_tpm = required_rpm * (float(avg_input_tokens) + float(avg_output_tokens))

        rpm_sufficient = required_rpm <= model_max_rpm
        tpm_sufficient = required_tpm <= model_max_tpm

        rpm_utilization = (required_rpm / model_max_rpm * 100.0) if model_max_rpm > 0 else 0.0
        tpm_utilization = (required_tpm / model_max_tpm * 100.0) if model_max_tpm > 0 else 0.0

        return {
            "required_rpm": required_rpm,
            "required_tpm": required_tpm,
            "model_max_rpm": model_max_rpm,
            "model_max_tpm": model_max_tpm,
            "rpm_sufficient": rpm_sufficient,
            "tpm_sufficient": tpm_sufficient,
            "rpm_utilization_pct": rpm_utilization,
            "tpm_utilization_pct": tpm_utilization,
        }

    steady = _compute_state(
        users=params.get("steady_state_users", 10),
        requests_per_hour=params.get("steady_state_requests_per_hour", 600),
        avg_input_tokens=params.get("steady_state_avg_input_tokens", 500),
        avg_output_tokens=params.get("steady_state_avg_output_tokens", 200),
        images_per_minute=params.get("steady_state_images_per_minute", 0),
        videos_per_hour=params.get("steady_state_videos_per_hour", 0),
    )

    peak_users = params.get("peak_state_users", 0)
    peak_rph = params.get("peak_state_requests_per_hour", 0)
    peak_images = params.get("peak_state_images_per_minute", 0)
    peak_videos = params.get("peak_state_videos_per_hour", 0)

    has_peak = False
    if model_type == "image" and peak_images > 0:
        has_peak = True
    elif model_type == "video" and peak_videos > 0:
        has_peak = True
    elif model_type not in ("image", "video") and peak_users > 0 and peak_rph > 0:
        has_peak = True

    peak = None
    if has_peak:
        peak = _compute_state(
            users=peak_users,
            requests_per_hour=peak_rph,
            avg_input_tokens=params.get("peak_state_avg_input_tokens", 0),
            avg_output_tokens=params.get("peak_state_avg_output_tokens", 0),
            images_per_minute=peak_images,
            videos_per_hour=peak_videos,
        )

    return {"steady_state": steady, "peak_state": peak}


def _estimate_monthly_cost(params: dict, model_limits: dict, model_type: str) -> dict:
    """
    Calculate monthly token volumes and costs.

    Args:
        params: Usage parameters dict.
        model_limits: Dict with pricing fields.
        model_type: One of 'on_demand', 'embedding', 'image', 'video'.

    Returns:
        Dict with steady_state, peak_state (or None), and combined_monthly_cost.
    """

    def _compute_cost_state(users, requests_per_hour, usage_hours, usage_days,
                            avg_input_tokens, avg_output_tokens,
                            images_per_minute, videos_per_hour, videos_duration):
        if model_type == "image":
            total_monthly_images = float(images_per_minute) * 60.0 * float(usage_hours) * float(usage_days)
            price_per_image = float(model_limits.get("price_per_image", 0))
            return {
                "monthly_input_tokens": 0.0, "monthly_output_tokens": 0.0,
                "total_monthly_images": total_monthly_images,
                "input_cost": 0.0, "output_cost": 0.0,
                "total_monthly_cost": price_per_image * total_monthly_images,
            }

        if model_type == "video":
            total_secs = (float(videos_per_hour) * float(videos_duration)
                          * float(usage_hours) * float(usage_days))
            price_per_second = float(model_limits.get("price_per_second", 0))
            return {
                "monthly_input_tokens": 0.0, "monthly_output_tokens": 0.0,
                "total_monthly_video_seconds": total_secs,
                "input_cost": 0.0, "output_cost": 0.0,
                "total_monthly_cost": price_per_second * total_secs,
            }

        monthly_input = (float(users) * float(requests_per_hour)
                         * float(usage_hours) * float(usage_days) * float(avg_input_tokens))
        monthly_output = (float(users) * float(requests_per_hour)
                          * float(usage_hours) * float(usage_days) * float(avg_output_tokens))

        if model_type == "embedding":
            price_m = float(model_limits.get("price_per_million_tokens", 0))
            input_cost = (monthly_input / 1_000_000) * price_m
            output_cost = 0.0
        else:
            input_cost = (monthly_input / 1_000_000) * float(model_limits.get("price_per_million_input_tokens", 0))
            output_cost = (monthly_output / 1_000_000) * float(model_limits.get("price_per_million_output_tokens", 0))

        return {
            "monthly_input_tokens": monthly_input, "monthly_output_tokens": monthly_output,
            "input_cost": input_cost, "output_cost": output_cost,
            "total_monthly_cost": input_cost + output_cost,
        }

    steady = _compute_cost_state(
        users=params.get("steady_state_users", 10),
        requests_per_hour=params.get("steady_state_requests_per_hour", 600),
        usage_hours=params.get("steady_state_usage_hours", 8),
        usage_days=params.get("steady_state_usage_days", 22),
        avg_input_tokens=params.get("steady_state_avg_input_tokens", 500),
        avg_output_tokens=params.get("steady_state_avg_output_tokens", 200),
        images_per_minute=params.get("steady_state_images_per_minute", 0),
        videos_per_hour=params.get("steady_state_videos_per_hour", 0),
        videos_duration=params.get("steady_state_videos_duration", 0),
    )

    peak = None
    peak_users = params.get("peak_state_users", 0)
    peak_rph = params.get("peak_state_requests_per_hour", 0)
    peak_images = params.get("peak_state_images_per_minute", 0)
    peak_videos = params.get("peak_state_videos_per_hour", 0)

    has_peak = False
    if model_type == "image" and peak_images > 0:
        has_peak = True
    elif model_type == "video" and peak_videos > 0:
        has_peak = True
    elif model_type not in ("image", "video") and peak_users > 0 and peak_rph > 0:
        has_peak = True

    if has_peak:
        peak = _compute_cost_state(
            users=peak_users, requests_per_hour=peak_rph,
            usage_hours=params.get("peak_state_usage_hours", 0),
            usage_days=params.get("peak_state_usage_days", 0),
            avg_input_tokens=params.get("peak_state_avg_input_tokens", 0),
            avg_output_tokens=params.get("peak_state_avg_output_tokens", 0),
            images_per_minute=peak_images, videos_per_hour=peak_videos,
            videos_duration=params.get("peak_state_videos_duration", 0),
        )

    combined = steady["total_monthly_cost"] + (peak["total_monthly_cost"] if peak else 0.0)
    return {"steady_state": steady, "peak_state": peak, "combined_monthly_cost": combined}


def _compare_provisioned(required_tpm: float, model_limits: dict) -> dict | None:
    """
    Calculate provisioned throughput units needed and commitment tier costs.

    Args:
        required_tpm: Total required tokens per minute.
        model_limits: Dict with provisioned pricing fields.

    Returns:
        Dict with units_needed, tier costs, and on_demand_comparison,
        or None if provisioned data is unavailable.
    """
    peak_input = float(model_limits.get("provisioned_peak_input_tpm", 0))
    peak_output = float(model_limits.get("provisioned_peak_output_tpm", 0))
    peak_per_unit = peak_input + peak_output

    if peak_per_unit <= 0:
        return None

    no_rate = float(model_limits.get("provisioned_no_commitment_price", 0))
    one_rate = float(model_limits.get("provisioned_1_month_price", 0))
    six_rate = float(model_limits.get("provisioned_6_months_price", 0))

    if no_rate == 0 and one_rate == 0 and six_rate == 0:
        return None

    units = math.ceil(required_tpm / peak_per_unit)
    no_monthly = no_rate * 24 * 30 * units
    one_monthly = one_rate * 24 * 30 * units
    six_monthly = six_rate * 24 * 30 * units

    tier_costs = [c for c in [no_monthly, one_monthly, six_monthly] if c > 0]
    cheapest = min(tier_costs) if tier_costs else 0.0

    price_in = float(model_limits.get("price_per_million_input_tokens", 0))
    price_out = float(model_limits.get("price_per_million_output_tokens", 0))
    avg_price = (price_in + price_out) / 2.0 if (price_in + price_out) > 0 else 0.0
    od_monthly = (required_tpm * 60 * 24 * 30 / 1_000_000) * avg_price

    savings = ((od_monthly - cheapest) / od_monthly * 100.0) if od_monthly > 0 and cheapest > 0 else 0.0

    return {
        "units_needed": units,
        "no_commitment_monthly": no_monthly,
        "one_month_commitment_monthly": one_monthly,
        "six_month_commitment_monthly": six_monthly,
        "on_demand_comparison": {
            "on_demand_monthly": od_monthly,
            "cheapest_provisioned_monthly": cheapest,
            "savings_pct": savings,
        },
    }


@tool
def capacity_planning_calculator(params: dict) -> dict:
    """
    Pure computation tool for Bedrock capacity planning. Does NOT call any AWS APIs.
    The agent MUST first call get_bedrock_quota to obtain RPM/TPM values, then pass
    them here via max_rpm and max_tpm parameters.

    Do NOT use this tool for simple quota lookups — use get_bedrock_quota instead.

    Input: dict with the following parameters:

    Required parameters:
    - model_name: str - Bedrock model name
    - max_rpm: float - Model's maximum requests per minute (from get_bedrock_quota)
    - max_tpm: float - Model's maximum tokens per minute (from get_bedrock_quota)

    Optional parameters:
    - region: str - AWS region code (default: "us-east-1")
    - model_type: str - One of: "on_demand", "embedding", "image", "video" (default: "on_demand")

    Pricing parameters (optional, for cost estimation):
    - price_per_million_input_tokens: float - Input token price (default: 0)
    - price_per_million_output_tokens: float - Output token price (default: 0)
    - price_per_million_tokens: float - Embedding token price (default: 0)
    - price_per_image: float - Per-image price (default: 0)
    - price_per_second: float - Per-second video price (default: 0)
    - provisioned_no_commitment_price: float - Hourly rate, no commitment (default: 0)
    - provisioned_1_month_price: float - Hourly rate, 1-month commitment (default: 0)
    - provisioned_6_months_price: float - Hourly rate, 6-month commitment (default: 0)
    - provisioned_peak_input_tpm: float - Provisioned input TPM per unit (default: 0)
    - provisioned_peak_output_tpm: float - Provisioned output TPM per unit (default: 0)

    Steady-state usage parameters:
    - steady_state_users: int (default: 10)
    - steady_state_requests_per_hour: int (default: 600)
    - steady_state_usage_hours: int (default: 8)
    - steady_state_usage_days: int (default: 22)
    - steady_state_avg_input_tokens: int (default: 500)
    - steady_state_avg_output_tokens: int (default: 200)

    Peak-state usage parameters (all default to 0 = no peak):
    - peak_state_users, peak_state_requests_per_hour, peak_state_usage_hours,
      peak_state_usage_days, peak_state_avg_input_tokens, peak_state_avg_output_tokens

    Image/Video parameters:
    - steady_state_images_per_minute, peak_state_images_per_minute
    - steady_state_videos_per_hour, steady_state_videos_duration,
      peak_state_videos_per_hour, peak_state_videos_duration

    Output: dict with capacity_analysis, cost_estimation, provisioned_throughput, warnings.
    """
    warnings = []

    model_name = params.get("model_name")
    if not model_name:
        return {"error": "Missing required parameter: model_name"}

    region = params.get("region", "us-east-1")
    model_type = params.get("model_type", "on_demand")

    valid_types = ["on_demand", "embedding", "image", "video"]
    if model_type not in valid_types:
        warnings.append(f"Invalid model_type '{model_type}', defaulting to 'on_demand'")
        model_type = "on_demand"

    # Build model_limits from input params (agent provides these from get_bedrock_quota)
    max_rpm = params.get("max_rpm", 0)
    max_tpm = params.get("max_tpm", 0)

    if max_rpm == 0 and max_tpm == 0:
        warnings.append(
            f"max_rpm and max_tpm are both 0 for '{model_name}'. "
            "Call get_bedrock_quota first to obtain quota values."
        )

    model_limits = {
        "max_rpm": float(max_rpm or 0),
        "max_tpm": float(max_tpm or 0),
        "price_per_million_input_tokens": float(params.get("price_per_million_input_tokens", 0)),
        "price_per_million_output_tokens": float(params.get("price_per_million_output_tokens", 0)),
        "price_per_million_tokens": float(params.get("price_per_million_tokens", 0)),
        "price_per_image": float(params.get("price_per_image", 0)),
        "price_per_second": float(params.get("price_per_second", 0)),
        "provisioned_no_commitment_price": float(params.get("provisioned_no_commitment_price", 0)),
        "provisioned_1_month_price": float(params.get("provisioned_1_month_price", 0)),
        "provisioned_6_months_price": float(params.get("provisioned_6_months_price", 0)),
        "provisioned_peak_input_tpm": float(params.get("provisioned_peak_input_tpm", 0)),
        "provisioned_peak_output_tpm": float(params.get("provisioned_peak_output_tpm", 0)),
    }

    # Calculate capacity
    capacity_analysis = _calculate_capacity(params, model_limits)

    # Estimate monthly cost
    cost_estimation = _estimate_monthly_cost(params, model_limits, model_type)

    # Compare provisioned (when pricing data is provided)
    provisioned_throughput = None
    if model_type == "on_demand":
        steady_tpm = capacity_analysis["steady_state"]["required_tpm"]
        peak_tpm = (capacity_analysis["peak_state"]["required_tpm"]
                    if capacity_analysis.get("peak_state") else 0.0)
        total_required_tpm = max(steady_tpm, peak_tpm)
        if total_required_tpm > 0:
            provisioned_throughput = _compare_provisioned(total_required_tpm, model_limits)

    return {
        "model_name": model_name,
        "region": region,
        "model_type": model_type,
        "capacity_analysis": capacity_analysis,
        "cost_estimation": cost_estimation,
        "provisioned_throughput": provisioned_throughput,
        "warnings": warnings,
    }
