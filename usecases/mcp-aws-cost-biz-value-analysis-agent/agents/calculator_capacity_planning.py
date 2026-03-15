"""
Bedrock Capacity Planning Calculator Tool.

Performs model data loading from S3, capacity calculations (RPM/TPM),
monthly cost estimation, provisioned throughput comparison, and alternative
model discovery. All logic is pure computation with no Streamlit or LLM
dependencies.
"""

import boto3
import pandas as pd
import os
import logging
import math
from io import StringIO
from strands import tool
from dotenv import load_dotenv

# Load environment variables from .env file (if present)
load_dotenv()

# Configure logger for this module
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Module-level cache for loaded model data
_model_data_cache: dict | None = None

# Environment variable configuration (read AFTER load_dotenv)
BEDROCK_SIZER_S3_BUCKET = os.environ.get('BEDROCK_SIZER_S3_BUCKET', '')
BEDROCK_SIZER_ON_DEMAND_KEY = os.environ.get('BEDROCK_SIZER_ON_DEMAND_KEY', 'bedrock_capacity_limit/od1.csv')
BEDROCK_SIZER_PROVISIONED_KEY = os.environ.get('BEDROCK_SIZER_PROVISIONED_KEY', 'bedrock_capacity_limit/pt1.csv')
BEDROCK_SIZER_EMBEDDING_KEY = os.environ.get('BEDROCK_SIZER_EMBEDDING_KEY', 'bedrock_capacity_limit/embedding.csv')
BEDROCK_SIZER_IMAGE_KEY = os.environ.get('BEDROCK_SIZER_IMAGE_KEY', 'bedrock_capacity_limit/image_model.csv')
BEDROCK_SIZER_VIDEO_KEY = os.environ.get('BEDROCK_SIZER_VIDEO_KEY', 'bedrock_capacity_limit/video_model.csv')


def _read_csv_from_s3(bucket: str, key: str, skiprows=None) -> pd.DataFrame | None:
    """
    Read a CSV file from S3 into a pandas DataFrame.

    Args:
        bucket: S3 bucket name.
        key: S3 object key (path to CSV file).
        skiprows: Number of rows to skip (optional).

    Returns:
        pandas DataFrame or None if the file could not be read.
    """
    s3 = boto3.client('s3')
    try:
        logger.info(f"Reading {key} from S3 bucket {bucket}")
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(content), skiprows=skiprows)
        logger.info(f"Successfully read CSV with shape: {df.shape}")
        return df
    except Exception as e:
        logger.error(f"Error reading {key} from S3: {str(e)}")
        return None


def _get_region_mapping() -> dict:
    """
    Return a mapping of AWS region codes to display names used in CSV data.

    Returns:
        Dict mapping 26 region codes to their display names.
    """
    return {
        "us-east-1": "N.Virginia",
        "us-east-2": "Ohio",
        "us-west-1": "N.California",
        "us-west-2": "Oregon",
        "af-south-1": "Cape Town",
        "ap-east-1": "Hong Kong",
        "ap-south-1": "Mumbai",
        "ap-northeast-1": "Tokyo",
        "ap-northeast-2": "Seoul",
        "ap-northeast-3": "Osaka",
        "ap-southeast-1": "Singapore",
        "ap-southeast-2": "Sydney",
        "ap-southeast-3": "Jakarta",
        "ca-central-1": "Canada Central",
        "eu-central-1": "Frankfurt",
        "eu-central-2": "Zurich",
        "eu-west-1": "Dublin",
        "eu-west-2": "London",
        "eu-west-3": "Paris",
        "eu-north-1": "Stockholm",
        "eu-south-1": "Milan",
        "eu-south-2": "Spain",
        "il-central-1": "Tel Aviv",
        "me-central-1": "UAE",
        "me-south-1": "Bahrain",
        "sa-east-1": "Sao Paulo",
    }



def _clean_numeric(value) -> str:
    """
    Clean a raw value by stripping $, commas, and whitespace.

    Args:
        value: Raw cell value from CSV.

    Returns:
        Cleaned string suitable for pd.to_numeric conversion.
    """
    if pd.isna(value):
        return ''
    s = str(value).strip()
    s = s.replace('$', '').replace(',', '').strip()
    return s


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize on-demand model CSV columns to standardized names and clean values.

    Maps source column names to the internal schema, cleans numeric price/limit
    columns, and converts the CRIS_Only column to boolean.

    Args:
        df: Raw on-demand model DataFrame.

    Returns:
        DataFrame with normalized column names and cleaned values.
    """
    df.columns = [col.strip() for col in df.columns]

    column_mapping = {
        'Region': 'region',
        'Model_Name': 'model_name',
        'On_Demand_Price_Per_Million_Input_Tokens': 'price_per_million_input_tokens',
        'On_Demand_Price_Per_Million_Output_Tokens': 'price_per_million_output_tokens',
        'On_Demand_Max_RPM': 'max_rpm',
        'On_Demand_Max_TPM': 'max_tpm',
        'CRIS_Only': 'cris_only',
        'Provisioned_Throughput_Peak_TPM_Input_Tokens': 'provisioned_peak_input_tpm',
        'Provisioned_Throughput_Peak_TPM_Output_Tokens': 'provisioned_peak_output_tpm',
        'Provisioned_Throughput_Concurrency': 'provisioned_concurrency',
        'Provisioned_Throughput_No_Commitment_Price': 'provisioned_no_commitment_price',
        'Provisioned_Throughput_1_Month_Commitment_Price': 'provisioned_1_month_price',
        'Provisioned_Throughput_6_Months_Commitment_Price': 'provisioned_6_months_price',
    }

    for original, new_name in column_mapping.items():
        if original in df.columns:
            df.rename(columns={original: new_name}, inplace=True)

    # Clean and convert price columns
    price_columns = [
        'price_per_million_input_tokens',
        'price_per_million_output_tokens',
        'provisioned_no_commitment_price',
        'provisioned_1_month_price',
        'provisioned_6_months_price',
    ]
    for col in price_columns:
        if col in df.columns:
            df[col] = df[col].apply(_clean_numeric)
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Clean and convert numeric limit columns
    numeric_columns = [
        'max_rpm',
        'max_tpm',
        'provisioned_peak_input_tpm',
        'provisioned_peak_output_tpm',
        'provisioned_concurrency',
    ]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = df[col].apply(_clean_numeric)
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Handle boolean cris_only column
    if 'cris_only' in df.columns:
        df['cris_only'] = df['cris_only'].astype(str).apply(
            lambda x: x.strip().lower() in ['true', '1', 'yes', 'y']
        )

    return df


def _normalize_embedding_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize embedding model CSV columns to standardized names and clean values.

    Args:
        df: Raw embedding model DataFrame.

    Returns:
        DataFrame with normalized column names and cleaned values.
    """
    df.columns = [col.strip() for col in df.columns]

    column_mapping = {
        'Region': 'region',
        'Model': 'model_name',
        'Price per million Input tokens': 'price_per_million_tokens',
        'Price per Image (Thousands)': 'price_per_image_thousands',
        'Max RPM': 'max_rpm',
        'Max TPM': 'max_tpm',
    }

    for original, new_name in column_mapping.items():
        if original in df.columns:
            df.rename(columns={original: new_name}, inplace=True)

    numeric_columns = ['price_per_million_tokens', 'price_per_image_thousands', 'max_rpm', 'max_tpm']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = df[col].apply(_clean_numeric)
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    return df


def _normalize_image_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize image model CSV columns to standardized names and clean values.

    Args:
        df: Raw image model DataFrame.

    Returns:
        DataFrame with normalized column names and cleaned values.
    """
    df.columns = [col.strip() for col in df.columns]

    column_mapping = {
        'Region': 'region',
        'Text-to-Image pricing': 'model_name',
        'Price per Image': 'price_per_image',
        'Max RPM': 'max_rpm',
    }

    for original, new_name in column_mapping.items():
        if original in df.columns:
            df.rename(columns={original: new_name}, inplace=True)

    numeric_columns = ['price_per_image', 'max_rpm']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = df[col].apply(_clean_numeric)
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    return df


def _normalize_video_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize video model CSV columns to standardized names and clean values.

    Args:
        df: Raw video model DataFrame.

    Returns:
        DataFrame with normalized column names and cleaned values.
    """
    df.columns = [col.strip() for col in df.columns]

    column_mapping = {
        'Region': 'region',
        'Text-to-Image pricing': 'model_name',
        'Price per second of video generated': 'price_per_second',
        'Max RPM': 'max_rpm',
    }

    for original, new_name in column_mapping.items():
        if original in df.columns:
            df.rename(columns={original: new_name}, inplace=True)

    numeric_columns = ['price_per_second', 'max_rpm']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = df[col].apply(_clean_numeric)
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    return df


def _load_model_data() -> dict:
    """
    Load all CSV datasets from S3 and cache them in the module-level variable.

    On first call, reads on-demand, provisioned, embedding, image, and video
    CSVs from S3, normalizes their columns, and stores them in
    ``_model_data_cache``. Subsequent calls return the cached data.

    Returns:
        Dict with keys: on_demand, provisioned, embedding, image, video,
        region_mapping. Non-critical datasets are None if loading fails.
    """
    global _model_data_cache

    if _model_data_cache is not None:
        return _model_data_cache

    bucket = BEDROCK_SIZER_S3_BUCKET

    # Load on-demand models (required)
    on_demand_df = _read_csv_from_s3(bucket, BEDROCK_SIZER_ON_DEMAND_KEY)
    if on_demand_df is not None:
        on_demand_df = _normalize_columns(on_demand_df)

    # Load provisioned models (optional)
    provisioned_df = _read_csv_from_s3(bucket, BEDROCK_SIZER_PROVISIONED_KEY)
    if provisioned_df is not None:
        provisioned_df = _normalize_columns(provisioned_df)

    # Load embedding models (optional)
    embedding_df = _read_csv_from_s3(bucket, BEDROCK_SIZER_EMBEDDING_KEY)
    if embedding_df is not None:
        embedding_df = _normalize_embedding_columns(embedding_df)

    # Load image models (optional)
    image_df = _read_csv_from_s3(bucket, BEDROCK_SIZER_IMAGE_KEY)
    if image_df is not None:
        image_df = _normalize_image_columns(image_df)

    # Load video models (optional)
    video_df = _read_csv_from_s3(bucket, BEDROCK_SIZER_VIDEO_KEY)
    if video_df is not None:
        video_df = _normalize_video_columns(video_df)

    _model_data_cache = {
        "on_demand": on_demand_df,
        "provisioned": provisioned_df,
        "embedding": embedding_df,
        "image": image_df,
        "video": video_df,
        "region_mapping": _get_region_mapping(),
    }

    return _model_data_cache


def _find_matching_model(df: pd.DataFrame, model_name: str, region_display: str,
                         cris_flag: bool = True) -> dict | None:
    """
    Find the best matching model row using multi-strategy matching.

    Tries exact match, then case-insensitive, then substring contains on the
    ``model_name`` column, all filtered to the given region display name.
    When a ``cris_only`` column exists the CRIS flag is applied; if no match
    is found with CRIS filtering the search is retried without it.

    Args:
        df: Normalized model DataFrame (must have ``region`` and ``model_name`` columns).
        model_name: Model name to search for.
        region_display: Region display name (e.g. ``"N.Virginia"``).
        cris_flag: Whether to filter for CRIS-enabled rows.

    Returns:
        Dict of the matched row, or None if no match is found.
    """
    if df is None or df.empty:
        return None

    clean_model_name = model_name.strip()

    def _search(apply_cris: bool):
        """Run the three matching strategies with optional CRIS filtering."""
        # Strategy 1: Exact match
        matches = df[(df['region'] == region_display) &
                     (df['model_name'].str.strip() == clean_model_name)]
        if apply_cris and 'cris_only' in df.columns:
            matches = matches[matches['cris_only'] == cris_flag]
        if not matches.empty:
            logger.debug(f"Exact match found (CRIS={apply_cris})")
            return matches.iloc[0].to_dict()

        # Strategy 2: Case-insensitive match
        matches = df[(df['region'] == region_display) &
                     (df['model_name'].str.lower().str.strip() == clean_model_name.lower())]
        if apply_cris and 'cris_only' in df.columns:
            matches = matches[matches['cris_only'] == cris_flag]
        if not matches.empty:
            logger.debug(f"Case-insensitive match found (CRIS={apply_cris})")
            return matches.iloc[0].to_dict()

        # Strategy 3: Substring contains match
        matches = df[(df['region'] == region_display) &
                     (df['model_name'].str.contains(clean_model_name, case=False, na=False))]
        if apply_cris and 'cris_only' in df.columns:
            matches = matches[matches['cris_only'] == cris_flag]
        if not matches.empty:
            logger.debug(f"Contains match found (CRIS={apply_cris})")
            return matches.iloc[0].to_dict()

        return None

    # First attempt: with CRIS filtering (when column exists)
    result = _search(apply_cris=True)
    if result is not None:
        return result

    # Retry without CRIS filtering if the column exists
    if 'cris_only' in df.columns:
        logger.info(f"No match with CRIS={cris_flag}, retrying without CRIS filtering")
        result = _search(apply_cris=False)
        if result is not None:
            return result

    return None


def _get_model_limits(model_name: str, region: str,
                      model_type: str = "on_demand",
                      cris_flag: bool = True) -> dict:
    """
    Return pricing and limit data for a specific model.

    Resolves the region code to a display name, selects the correct cached
    DataFrame by ``model_type``, and calls :func:`_find_matching_model`.
    For on-demand models the provisioned dataset is also checked.

    Args:
        model_name: Bedrock model name.
        region: AWS region code (e.g. ``"us-east-1"``).
        model_type: One of ``"on_demand"``, ``"embedding"``, ``"image"``,
                    ``"video"``, ``"provisioned"``.
        cris_flag: Whether to prefer CRIS-enabled rows.

    Returns:
        Dict with all pricing/limit fields and a ``not_found`` flag.
    """
    # Default limits dict
    model_limits = {
        "max_rpm": 0,
        "max_tpm": 0,
        "price_per_million_input_tokens": 0,
        "price_per_million_output_tokens": 0,
        "provisioned_peak_input_tpm": 0,
        "provisioned_peak_output_tpm": 0,
        "provisioned_concurrency": 0,
        "provisioned_no_commitment_price": 0,
        "provisioned_1_month_price": 0,
        "provisioned_6_months_price": 0,
        "price_per_million_tokens": 0,
        "price_per_image_thousands": 0,
        "price_per_image": 0,
        "price_per_second": 0,
        "not_found": False,
    }

    if not model_name:
        model_limits["not_found"] = True
        return model_limits

    data = _load_model_data()
    region_mapping = data.get("region_mapping", {})
    region_display = region_mapping.get(region, region)

    logger.info(f"Looking up limits for {model_name} in {region_display} "
                f"as {model_type} model with CRIS={cris_flag}")

    found_match = False

    if model_type == "embedding" and data.get("embedding") is not None:
        match = _find_matching_model(data["embedding"], model_name, region_display)
        if match:
            found_match = True
            for field in ["max_rpm", "max_tpm", "price_per_million_tokens",
                          "price_per_image_thousands"]:
                if field in match:
                    model_limits[field] = float(match.get(field, 0))

    elif model_type == "image" and data.get("image") is not None:
        match = _find_matching_model(data["image"], model_name, region_display)
        if match:
            found_match = True
            for field in ["max_rpm", "price_per_image"]:
                if field in match:
                    model_limits[field] = float(match.get(field, 0))

    elif model_type == "video" and data.get("video") is not None:
        match = _find_matching_model(data["video"], model_name, region_display)
        if match:
            found_match = True
            for field in ["max_rpm", "price_per_second"]:
                if field in match:
                    model_limits[field] = float(match.get(field, 0))

    else:
        # Default: on-demand (text generation) lookup
        on_demand_df = data.get("on_demand")
        if on_demand_df is not None:
            match = _find_matching_model(on_demand_df, model_name,
                                         region_display, cris_flag)
            if match:
                found_match = True
                for field in ["max_rpm", "max_tpm",
                              "price_per_million_input_tokens",
                              "price_per_million_output_tokens"]:
                    if field in match:
                        model_limits[field] = float(match[field])

        # Also check provisioned data for on-demand models
        provisioned_df = data.get("provisioned")
        if provisioned_df is not None:
            prov_match = _find_matching_model(provisioned_df, model_name,
                                              region_display)
            if prov_match:
                found_match = True
                for field in ["provisioned_peak_input_tpm",
                              "provisioned_peak_output_tpm",
                              "provisioned_concurrency",
                              "provisioned_no_commitment_price",
                              "provisioned_1_month_price",
                              "provisioned_6_months_price"]:
                    if field in prov_match:
                        model_limits[field] = float(prov_match[field])

    if not found_match:
        model_limits["not_found"] = True
        logger.warning(f"No matching model found for {model_name} in "
                       f"{region_display} as {model_type} with CRIS={cris_flag}")

    return model_limits


def _calculate_capacity(params: dict, model_limits: dict) -> dict:
    """
    Compute required RPM/TPM and compare against model limits.

    Calculates steady-state (and optionally peak-state) request and token
    rates, then checks whether the model's published limits are sufficient.

    For image models the RPM comes from ``steady_state_images_per_minute``
    and TPM is 0.  For video models the RPM is
    ``steady_state_videos_per_hour / 60`` and TPM is 0.

    Args:
        params: Usage parameters dict (users, requests_per_hour, tokens, etc.).
        model_limits: Dict returned by :func:`_get_model_limits`.

    Returns:
        Dict with ``steady_state`` and ``peak_state`` (or None) sub-dicts,
        each containing required/max RPM/TPM, sufficiency flags, and
        utilization percentages.
    """
    model_type = params.get("model_type", "on_demand")
    model_max_rpm = float(model_limits.get("max_rpm", 0))
    model_max_tpm = float(model_limits.get("max_tpm", 0))

    def _compute_state(users, requests_per_hour, avg_input_tokens,
                       avg_output_tokens, images_per_minute,
                       videos_per_hour):
        """Return capacity metrics for a single usage state."""
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

    # --- Steady state ---
    steady = _compute_state(
        users=params.get("steady_state_users", 10),
        requests_per_hour=params.get("steady_state_requests_per_hour", 600),
        avg_input_tokens=params.get("steady_state_avg_input_tokens", 500),
        avg_output_tokens=params.get("steady_state_avg_output_tokens", 200),
        images_per_minute=params.get("steady_state_images_per_minute", 0),
        videos_per_hour=params.get("steady_state_videos_per_hour", 0),
    )

    # --- Peak state (only when peak params are provided and > 0) ---
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

    return {
        "steady_state": steady,
        "peak_state": peak,
    }


def _estimate_monthly_cost(params: dict, model_limits: dict, model_type: str) -> dict:
    """
    Calculate monthly token volumes and costs for all model types.

    Computes steady-state costs (and optionally peak-state costs) based on
    the model type:

    - **on_demand / embedding**: token-based pricing using per-million-token rates
    - **image**: per-image pricing
    - **video**: per-second pricing

    For embedding models, only input cost applies (using ``price_per_million_tokens``).

    Args:
        params: Usage parameters dict with steady/peak state fields.
        model_limits: Dict returned by :func:`_get_model_limits`.
        model_type: One of ``"on_demand"``, ``"embedding"``, ``"image"``, ``"video"``.

    Returns:
        Dict with ``steady_state``, ``peak_state`` (or None), and
        ``combined_monthly_cost`` keys.
    """

    def _compute_cost_state(users, requests_per_hour, usage_hours, usage_days,
                            avg_input_tokens, avg_output_tokens,
                            images_per_minute, videos_per_hour, videos_duration):
        """Compute cost breakdown for a single usage state."""
        if model_type == "image":
            total_monthly_images = float(images_per_minute) * 60.0 * float(usage_hours) * float(usage_days)
            price_per_image = float(model_limits.get("price_per_image", 0))
            monthly_cost = price_per_image * total_monthly_images
            return {
                "monthly_input_tokens": 0.0,
                "monthly_output_tokens": 0.0,
                "total_monthly_images": total_monthly_images,
                "input_cost": 0.0,
                "output_cost": 0.0,
                "total_monthly_cost": monthly_cost,
            }

        if model_type == "video":
            total_monthly_video_seconds = (float(videos_per_hour) * float(videos_duration)
                                           * float(usage_hours) * float(usage_days))
            price_per_second = float(model_limits.get("price_per_second", 0))
            monthly_cost = price_per_second * total_monthly_video_seconds
            return {
                "monthly_input_tokens": 0.0,
                "monthly_output_tokens": 0.0,
                "total_monthly_video_seconds": total_monthly_video_seconds,
                "input_cost": 0.0,
                "output_cost": 0.0,
                "total_monthly_cost": monthly_cost,
            }

        # on_demand or embedding: token-based pricing
        monthly_input_tokens = (float(users) * float(requests_per_hour)
                                * float(usage_hours) * float(usage_days)
                                * float(avg_input_tokens))
        monthly_output_tokens = (float(users) * float(requests_per_hour)
                                 * float(usage_hours) * float(usage_days)
                                 * float(avg_output_tokens))

        if model_type == "embedding":
            price_per_million = float(model_limits.get("price_per_million_tokens", 0))
            input_cost = (monthly_input_tokens / 1_000_000) * price_per_million
            output_cost = 0.0
        else:
            input_cost = ((monthly_input_tokens / 1_000_000)
                          * float(model_limits.get("price_per_million_input_tokens", 0)))
            output_cost = ((monthly_output_tokens / 1_000_000)
                           * float(model_limits.get("price_per_million_output_tokens", 0)))

        return {
            "monthly_input_tokens": monthly_input_tokens,
            "monthly_output_tokens": monthly_output_tokens,
            "input_cost": input_cost,
            "output_cost": output_cost,
            "total_monthly_cost": input_cost + output_cost,
        }

    # --- Steady state ---
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

    # --- Peak state (only when peak params > 0) ---
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
            users=peak_users,
            requests_per_hour=peak_rph,
            usage_hours=params.get("peak_state_usage_hours", 0),
            usage_days=params.get("peak_state_usage_days", 0),
            avg_input_tokens=params.get("peak_state_avg_input_tokens", 0),
            avg_output_tokens=params.get("peak_state_avg_output_tokens", 0),
            images_per_minute=peak_images,
            videos_per_hour=peak_videos,
            videos_duration=params.get("peak_state_videos_duration", 0),
        )

    combined = steady["total_monthly_cost"] + (peak["total_monthly_cost"] if peak else 0.0)

    return {
        "steady_state": steady,
        "peak_state": peak,
        "combined_monthly_cost": combined,
    }


def _compare_provisioned(required_tpm: float, model_limits: dict) -> dict | None:
    """
    Calculate provisioned throughput units needed and commitment tier costs.

    Determines how many provisioned model units are required to handle the
    given TPM, then computes monthly costs for each commitment tier
    (no-commitment, 1-month, 6-month). Also compares the cheapest
    provisioned option against on-demand monthly cost.

    Args:
        required_tpm: Total required tokens per minute (steady + peak).
        model_limits: Dict returned by :func:`_get_model_limits`, must
            include provisioned pricing and peak TPM fields.

    Returns:
        Dict with ``units_needed``, tier costs, and ``on_demand_comparison``,
        or None if provisioned data is unavailable or peak TPM is 0.
    """
    provisioned_peak_input_tpm = float(model_limits.get("provisioned_peak_input_tpm", 0))
    provisioned_peak_output_tpm = float(model_limits.get("provisioned_peak_output_tpm", 0))
    provisioned_peak_tpm_per_unit = provisioned_peak_input_tpm + provisioned_peak_output_tpm

    # Return None if provisioned data unavailable or peak TPM is 0
    if provisioned_peak_tpm_per_unit <= 0:
        return None

    no_commitment_rate = float(model_limits.get("provisioned_no_commitment_price", 0))
    one_month_rate = float(model_limits.get("provisioned_1_month_price", 0))
    six_month_rate = float(model_limits.get("provisioned_6_months_price", 0))

    # If all hourly rates are 0, provisioned data is effectively unavailable
    if no_commitment_rate == 0 and one_month_rate == 0 and six_month_rate == 0:
        return None

    units_needed = math.ceil(required_tpm / provisioned_peak_tpm_per_unit)

    # Monthly cost per tier: hourly_rate * 24 hours * 30 days * units
    no_commitment_monthly = no_commitment_rate * 24 * 30 * units_needed
    one_month_monthly = one_month_rate * 24 * 30 * units_needed
    six_month_monthly = six_month_rate * 24 * 30 * units_needed

    # Find cheapest provisioned option (among non-zero tiers)
    tier_costs = []
    if no_commitment_monthly > 0:
        tier_costs.append(no_commitment_monthly)
    if one_month_monthly > 0:
        tier_costs.append(one_month_monthly)
    if six_month_monthly > 0:
        tier_costs.append(six_month_monthly)

    cheapest_provisioned = min(tier_costs) if tier_costs else 0.0

    # On-demand comparison: estimate on-demand monthly cost from TPM
    # On-demand monthly tokens = required_tpm * 60 * 24 * 30
    price_input = float(model_limits.get("price_per_million_input_tokens", 0))
    price_output = float(model_limits.get("price_per_million_output_tokens", 0))
    avg_price = (price_input + price_output) / 2.0 if (price_input + price_output) > 0 else 0.0
    on_demand_monthly_tokens = required_tpm * 60 * 24 * 30
    on_demand_monthly = (on_demand_monthly_tokens / 1_000_000) * avg_price

    savings_pct = 0.0
    if on_demand_monthly > 0 and cheapest_provisioned > 0:
        savings_pct = ((on_demand_monthly - cheapest_provisioned) / on_demand_monthly) * 100.0

    return {
        "units_needed": units_needed,
        "no_commitment_monthly": no_commitment_monthly,
        "one_month_commitment_monthly": one_month_monthly,
        "six_month_commitment_monthly": six_month_monthly,
        "on_demand_comparison": {
            "on_demand_monthly": on_demand_monthly,
            "cheapest_provisioned_monthly": cheapest_provisioned,
            "savings_pct": savings_pct,
        },
    }


def _find_alternatives(region: str, required_rpm: float, required_tpm: float,
                       model_type: str = "on_demand",
                       cris_flag: bool = True) -> list[dict]:
    """
    Search for alternative models meeting capacity requirements.

    Filters the cached model DataFrame for the given region, keeping only
    models whose ``max_rpm >= required_rpm`` (and for on-demand/embedding,
    ``max_tpm >= required_tpm``). Results are sorted by the model-type-specific
    price field in ascending order.

    Args:
        region: AWS region code (e.g. ``"us-east-1"``).
        required_rpm: Minimum required requests per minute.
        required_tpm: Minimum required tokens per minute (ignored for image/video).
        model_type: One of ``"on_demand"``, ``"embedding"``, ``"image"``, ``"video"``.
        cris_flag: Whether to filter for CRIS-enabled rows (on-demand only).

    Returns:
        List of dicts, each containing model name, limits, and pricing.
        Empty list when no alternatives meet requirements.
    """
    data = _load_model_data()
    region_mapping = data.get("region_mapping", {})
    region_display = region_mapping.get(region, region)

    alternatives = []

    if model_type == "embedding" and data.get("embedding") is not None:
        df = data["embedding"]
        filtered = df[df["region"] == region_display]
        for _, row in filtered.iterrows():
            if row["max_rpm"] >= required_rpm:
                alternatives.append({
                    "model_name": row["model_name"],
                    "max_rpm": float(row["max_rpm"]),
                    "max_tpm": float(row.get("max_tpm", 0)),
                    "price_per_million_tokens": float(row.get("price_per_million_tokens", 0)),
                    "price_per_image_thousands": float(row.get("price_per_image_thousands", 0)),
                })
        return sorted(alternatives, key=lambda x: x.get("price_per_million_tokens", 0))

    elif model_type == "image" and data.get("image") is not None:
        df = data["image"]
        filtered = df[df["region"] == region_display]
        for _, row in filtered.iterrows():
            if row["max_rpm"] >= required_rpm:
                alternatives.append({
                    "model_name": row["model_name"],
                    "max_rpm": float(row["max_rpm"]),
                    "price_per_image": float(row.get("price_per_image", 0)),
                })
        return sorted(alternatives, key=lambda x: x.get("price_per_image", 0))

    elif model_type == "video" and data.get("video") is not None:
        df = data["video"]
        filtered = df[df["region"] == region_display]
        for _, row in filtered.iterrows():
            if row["max_rpm"] >= required_rpm:
                alternatives.append({
                    "model_name": row["model_name"],
                    "max_rpm": float(row["max_rpm"]),
                    "price_per_second": float(row.get("price_per_second", 0)),
                })
        return sorted(alternatives, key=lambda x: x.get("price_per_second", 0))

    else:
        # Default: on-demand (text generation)
        on_demand_df = data.get("on_demand")
        if on_demand_df is None:
            return []

        filtered = on_demand_df[on_demand_df["region"] == region_display]
        for _, row in filtered.iterrows():
            if row["max_rpm"] >= required_rpm and row["max_tpm"] >= required_tpm:
                model_data = {
                    "model_name": row["model_name"],
                    "max_rpm": float(row["max_rpm"]),
                    "max_tpm": float(row["max_tpm"]),
                    "price_per_million_input_tokens": float(row["price_per_million_input_tokens"]),
                    "price_per_million_output_tokens": float(row["price_per_million_output_tokens"]),
                }
                if "cris_only" in row:
                    model_data["cris_only"] = bool(row["cris_only"])
                alternatives.append(model_data)

        return sorted(alternatives, key=lambda x: x.get("price_per_million_input_tokens", 0))


@tool
def capacity_planning_calculator(params: dict) -> dict:
    """
    Calculates Bedrock model capacity requirements, monthly costs, provisioned throughput
    comparison, and discovers alternative models. Returns a structured analysis to help
    size Bedrock deployments for customer workloads.

    Input: dict with the following parameters:

    Required parameters:
    - model_name: str - Bedrock model name (e.g., "anthropic.claude-3-sonnet-20240229-v1:0")

    Optional parameters:
    - region: str - AWS region code (default: "us-east-1")
    - model_type: str - One of: "on_demand", "embedding", "image", "video", "provisioned" (default: "on_demand")
    - cris_flag: bool - Enable cross-region inference filtering (default: True)

    Steady-state usage parameters (normal daily usage):
    - steady_state_users: int - Concurrent users (default: 10)
    - steady_state_requests_per_hour: int - Requests per hour per user (default: 600)
    - steady_state_usage_hours: int - Hours per day of usage (default: 8)
    - steady_state_usage_days: int - Days per month of usage (default: 22)
    - steady_state_avg_input_tokens: int - Average input tokens per request (default: 500)
    - steady_state_avg_output_tokens: int - Average output tokens per request (default: 200)

    Peak-state usage parameters (high-traffic periods, all default to 0 = no peak):
    - peak_state_users: int - Concurrent users during peak (default: 0)
    - peak_state_requests_per_hour: int - Requests per hour per user during peak (default: 0)
    - peak_state_usage_hours: int - Hours per day of peak usage (default: 0)
    - peak_state_usage_days: int - Days per month of peak usage (default: 0)
    - peak_state_avg_input_tokens: int - Average input tokens per request during peak (default: 0)
    - peak_state_avg_output_tokens: int - Average output tokens per request during peak (default: 0)

    Image model parameters (used when model_type is "image"):
    - steady_state_images_per_minute: int - Images per minute during steady state (default: 0)
    - peak_state_images_per_minute: int - Images per minute during peak (default: 0)

    Video model parameters (used when model_type is "video"):
    - steady_state_videos_per_hour: int - Videos per hour during steady state (default: 0)
    - steady_state_videos_duration: int - Average video duration in seconds (default: 0)
    - peak_state_videos_per_hour: int - Videos per hour during peak (default: 0)
    - peak_state_videos_duration: int - Average video duration in seconds during peak (default: 0)

    Output: dict with the following structure:
    - model_name: str - The requested model name
    - region: str - The AWS region used
    - model_type: str - The model type used
    - capacity_analysis: dict - RPM/TPM requirements vs model limits with sufficiency flags
      - steady_state: dict with required_rpm, required_tpm, model_max_rpm, model_max_tpm,
        rpm_sufficient, tpm_sufficient, rpm_utilization_pct, tpm_utilization_pct
      - peak_state: dict (same structure) or None if no peak params provided
    - cost_estimation: dict - Monthly cost breakdown
      - steady_state: dict with monthly_input_tokens, monthly_output_tokens, input_cost,
        output_cost, total_monthly_cost
      - peak_state: dict (same structure) or None
      - combined_monthly_cost: float - Total of steady + peak costs
    - provisioned_throughput: dict or None - Provisioned throughput comparison (on_demand only)
      - units_needed, no_commitment_monthly, one_month_commitment_monthly,
        six_month_commitment_monthly, on_demand_comparison
    - alternative_models: list[dict] - Models meeting capacity requirements, sorted by price
    - warnings: list[str] - Any non-fatal issues encountered
    """
    warnings = []

    # --- Validate required parameters ---
    model_name = params.get("model_name")
    if not model_name:
        return {"error": "Missing required parameter: model_name"}

    # --- Check S3 bucket env var ---
    if not BEDROCK_SIZER_S3_BUCKET:
        return {"error": "BEDROCK_SIZER_S3_BUCKET environment variable not set"}

    # --- Extract parameters with defaults ---
    region = params.get("region", "us-east-1")
    model_type = params.get("model_type", "on_demand")
    cris_flag = params.get("cris_flag", True)

    # Validate model_type
    valid_model_types = ["on_demand", "embedding", "image", "video", "provisioned"]
    if model_type not in valid_model_types:
        warnings.append(f"Invalid model_type '{model_type}', defaulting to 'on_demand'")
        model_type = "on_demand"

    # --- Load model data (uses cache) ---
    try:
        _load_model_data()
    except Exception as e:
        logger.error(f"Error loading model data: {str(e)}")
        warnings.append(f"Error loading model data: {str(e)}")

    # --- Get model limits ---
    lookup_type = model_type if model_type != "provisioned" else "on_demand"
    model_limits = _get_model_limits(model_name, region, lookup_type, cris_flag)

    if model_limits.get("not_found"):
        warnings.append(
            f"Model '{model_name}' not found in region '{region}' "
            f"as '{model_type}' model. Searching for alternatives."
        )

    # --- Calculate capacity ---
    capacity_analysis = _calculate_capacity(params, model_limits)

    # --- Estimate monthly cost ---
    cost_estimation = _estimate_monthly_cost(params, model_limits, lookup_type)

    # --- Compare provisioned (for on_demand and provisioned types) ---
    provisioned_throughput = None
    if model_type in ("on_demand", "provisioned") and not model_limits.get("not_found"):
        steady_tpm = capacity_analysis["steady_state"]["required_tpm"]
        peak_tpm = (capacity_analysis["peak_state"]["required_tpm"]
                    if capacity_analysis.get("peak_state") else 0.0)
        total_required_tpm = max(steady_tpm, peak_tpm)
        if total_required_tpm > 0:
            provisioned_throughput = _compare_provisioned(total_required_tpm, model_limits)

    # --- Find alternative models ---
    steady_rpm = capacity_analysis["steady_state"]["required_rpm"]
    steady_tpm = capacity_analysis["steady_state"]["required_tpm"]
    peak_rpm = (capacity_analysis["peak_state"]["required_rpm"]
                if capacity_analysis.get("peak_state") else 0.0)
    peak_tpm = (capacity_analysis["peak_state"]["required_tpm"]
                if capacity_analysis.get("peak_state") else 0.0)
    required_rpm = max(steady_rpm, peak_rpm)
    required_tpm = max(steady_tpm, peak_tpm)

    alternative_models = _find_alternatives(
        region, required_rpm, required_tpm, lookup_type, cris_flag
    )

    # --- Build and return result ---
    return {
        "model_name": model_name,
        "region": region,
        "model_type": model_type,
        "capacity_analysis": capacity_analysis,
        "cost_estimation": cost_estimation,
        "provisioned_throughput": provisioned_throughput,
        "alternative_models": alternative_models,
        "warnings": warnings,
    }
