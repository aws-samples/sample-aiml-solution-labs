#!/usr/bin/env python3
"""
AWS Bedrock Quota Scraper.

Collects RPM and TPM quotas from the AWS Service Quotas API for Bedrock
models across ALL discovered AWS regions by default. Saves one text file
per quota entry into quota_data/{region}/{sanitized_quota_name}.txt

By default (no flags), the scraper discovers all AWS regions via
ec2.describe_regions() and scrapes each one. Use --region to limit
scraping to specific regions.

Usage:
    python quota_doc_scraper.py                              # scrape ALL regions
    python quota_doc_scraper.py --region us-east-1 --region eu-west-1  # specific regions
    python quota_doc_scraper.py --all-regions                # same as default (backward compat)
    python quota_doc_scraper.py --list                       # list reference regions
"""

import argparse
import boto3
import json
import logging
import os
import re
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

DEFAULT_REGIONS = [
    'us-east-1', 'us-west-2',
    'us-gov-west-1', 'us-gov-east-1',
    'eu-west-1', 'eu-west-2', 'eu-west-3',
    'eu-central-1', 'eu-central-2',
    'eu-north-1', 'eu-south-1', 'eu-south-2',
]

SERVICE_CODE = 'bedrock'


def sanitize_filename(name: str) -> str:
    """Convert a quota name into a safe filename."""
    s = re.sub(r'[^\w\s\-.]', '', name)
    s = re.sub(r'\s+', '_', s.strip())
    return s[:200]  # cap length


def scrape_region(region: str, output_dir: str, max_retries: int = 3) -> dict:
    """
    Scrape all Bedrock RPM/TPM quotas for a single region.

    Args:
        region: AWS region code.
        output_dir: Root output directory.
        max_retries: Retries on throttle.

    Returns:
        Dict with region, quotas_found, files_saved.
    """
    region_dir = os.path.join(output_dir, region)
    os.makedirs(region_dir, exist_ok=True)

    try:
        client = boto3.client('service-quotas', region_name=region)
    except Exception as e:
        logger.warning(f"Cannot create client for {region}: {e}")
        return {'region': region, 'quotas_found': 0, 'files_saved': 0, 'error': str(e)}

    quotas_found = 0
    files_saved = 0

    try:
        paginator = client.get_paginator('list_service_quotas')
        retries = 0

        for page in paginator.paginate(ServiceCode=SERVICE_CODE):
            for quota in page['Quotas']:
                name = quota['QuotaName']
                name_lower = name.lower()

                # Skip non-RPM/TPM quotas
                if 'requests per minute' not in name_lower and 'tokens per minute' not in name_lower:
                    continue
                # Skip customization quotas
                if 'model customization' in name_lower or 'custom model deployment' in name_lower:
                    continue

                quotas_found += 1
                value = quota['Value']
                quota_code = quota.get('QuotaCode', '')

                # Classify inference type
                if 'global cross-region' in name_lower or 'global-cross-region' in name_lower:
                    inference_type = 'global-cross-region'
                elif 'cross-region' in name_lower:
                    inference_type = 'cross-region'
                elif 'on-demand' in name_lower:
                    inference_type = 'on-demand'
                else:
                    inference_type = 'unknown'

                # Classify metric type
                if 'requests per minute' in name_lower:
                    metric = 'rpm'
                else:
                    metric = 'tpm'

                # Build file content
                content = (
                    f"Quota Name: {name}\n"
                    f"Quota Code: {quota_code}\n"
                    f"Region: {region}\n"
                    f"Inference Type: {inference_type}\n"
                    f"Metric: {metric}\n"
                    f"Value: {value}\n"
                    f"Unit: {quota.get('Unit', 'None')}\n"
                    f"Adjustable: {quota.get('Adjustable', False)}\n"
                    f"Global Quota: {quota.get('GlobalQuota', False)}\n"
                )

                filename = f"{sanitize_filename(name)}.txt"
                filepath = os.path.join(region_dir, filename)

                with open(filepath, 'w') as f:
                    f.write(content)
                files_saved += 1

            retries = 0  # reset on success

    except client.exceptions.TooManyRequestsException:
        retries += 1
        if retries <= max_retries:
            wait = 2 ** retries
            logger.warning(f"Throttled on {region}, waiting {wait}s (retry {retries}/{max_retries})")
            time.sleep(wait)
        else:
            logger.error(f"Max retries exceeded for {region}")
    except Exception as e:
        logger.error(f"Error scraping {region}: {e}")
        return {'region': region, 'quotas_found': quotas_found, 'files_saved': files_saved, 'error': str(e)}

    # Also save a summary JSON per region
    summary = {
        'region': region,
        'service_code': SERVICE_CODE,
        'quotas_found': quotas_found,
        'files_saved': files_saved,
    }
    with open(os.path.join(region_dir, '_summary.json'), 'w') as f:
        json.dump(summary, f, indent=2)

    return summary


def scrape_quotas(regions: list[str] = None, output_dir: str = './quota_data') -> list[dict]:
    """
    Scrape Bedrock quotas across multiple regions.

    Args:
        regions: List of region codes. Defaults to DEFAULT_REGIONS.
        output_dir: Root output directory.

    Returns:
        List of per-region summary dicts.
    """
    if regions is None:
        regions = DEFAULT_REGIONS

    os.makedirs(output_dir, exist_ok=True)
    logger.info(f"Scraping Bedrock quotas for {len(regions)} regions")
    logger.info(f"Regions: {regions}")
    logger.info(f"Output: {output_dir}")

    results = []
    for i, region in enumerate(regions, 1):
        logger.info(f"[{i}/{len(regions)}] Scraping {region}...")
        summary = scrape_region(region, output_dir)
        results.append(summary)
        logger.info(f"  ✓ {region}: {summary.get('quotas_found', 0)} quotas, "
                     f"{summary.get('files_saved', 0)} files")

        # Small delay between regions to avoid throttling
        if i < len(regions):
            time.sleep(1)

    return results


def main():
    parser = argparse.ArgumentParser(description='AWS Bedrock Quota Scraper')
    parser.add_argument('--region', type=str, action='append',
                        help='Region to scrape (can specify multiple)')
    parser.add_argument('--output', type=str, default='./quota_data',
                        help='Output directory (default: ./quota_data)')
    parser.add_argument('--all-regions', action='store_true',
                        help='No-op, kept for backward compatibility (all regions is now the default)')
    parser.add_argument('--list', action='store_true',
                        help='List reference regions and exit')
    args = parser.parse_args()

    if args.list:
        print(f"Reference regions ({len(DEFAULT_REGIONS)}):")
        for r in DEFAULT_REGIONS:
            print(f"  • {r}")
        return

    if args.region:
        # Explicit region filtering
        regions = args.region
    else:
        # Default: discover and scrape ALL regions
        ec2 = boto3.client('ec2', region_name='us-east-1')
        resp = ec2.describe_regions(AllRegions=False)
        regions = sorted(r['RegionName'] for r in resp['Regions'])
        logger.info(f"Discovered {len(regions)} regions")

    results = scrape_quotas(regions=regions, output_dir=args.output)

    print(f"\n{'=' * 60}")
    print(f"Bedrock Quota Scraper Summary")
    print(f"{'=' * 60}")
    print(f"Output directory: {args.output}")
    print(f"Regions scraped: {len(results)}")

    total_quotas = sum(r.get('quotas_found', 0) for r in results)
    total_files = sum(r.get('files_saved', 0) for r in results)
    print(f"Total quotas found: {total_quotas}")
    print(f"Total files saved: {total_files}")

    print(f"\nPer region:")
    for r in results:
        status = '✓' if 'error' not in r else '✗'
        print(f"  {status} {r['region']}: {r.get('files_saved', 0)} files")
        if 'error' in r:
            print(f"    Error: {r['error']}")

    print(f"\nFiles saved to: {args.output}/{{region}}/{{quota_name}}.txt")


if __name__ == '__main__':
    main()
