#!/usr/bin/env python3
"""
Batch data loader for the TCO & BVA Analyst agent.

Runs both the pricing-doc-scraper and quota-doc-scraper for all regions,
syncs the output to S3, and triggers a Knowledge Base sync.

Environment variables:
    TCO_ANALYSIS_S3_BUCKET  - S3 bucket for pricing/quota data (required)
    STRANDS_KNOWLEDGE_BASE_ID - Bedrock Knowledge Base ID to sync (required)
    AWS_REGION              - Region for KB sync API calls (default: us-east-1)

Usage:
    python scrape_data.py
    python scrape_data.py --skip-scrape      # S3 sync + KB sync only
    python scrape_data.py --skip-sync        # scrape only, no S3/KB sync
"""

import argparse
import logging
import os
import shutil
import subprocess
import sys
import time

import boto3

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

# ── Paths ──
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PRICING_SCRAPER = os.path.join(SCRIPT_DIR, 'pricing-doc-scraper', 'price_doc_scraper.py')
QUOTA_SCRAPER = os.path.join(SCRIPT_DIR, 'quota-doc-scraper', 'quota_doc_scraper.py')
PRICING_DATA_DIR = os.path.join(SCRIPT_DIR, 'pricing-doc-scraper', 'pricing_data')
QUOTA_DATA_DIR = os.path.join(SCRIPT_DIR, 'quota-doc-scraper', 'quota_data')

# ── Config ──
BEDROCK_SERVICES = [
    'AmazonBedrock',
    'AmazonBedrockAgentCore',
    'AmazonBedrockFoundationModels',
    'AmazonBedrockService',
]


def clean_data_dirs():
    """Remove and recreate pricing and quota data directories."""
    for data_dir in [PRICING_DATA_DIR, QUOTA_DATA_DIR]:
        if os.path.isdir(data_dir):
            logger.info(f"Cleaning {data_dir}")
            shutil.rmtree(data_dir)
        os.makedirs(data_dir, exist_ok=True)
    logger.info("Data directories cleaned")


def run_pricing_scraper():
    """Run the pricing doc scraper for Bedrock services across all regions."""
    logger.info("=" * 60)
    logger.info("Step 1: Running pricing-doc-scraper")
    logger.info("=" * 60)

    cmd = [
        sys.executable, PRICING_SCRAPER,
        '--all-regions',
        '--output', PRICING_DATA_DIR,
    ]
    for svc in BEDROCK_SERVICES:
        cmd.extend(['--service', svc])

    logger.info(f"Services: {BEDROCK_SERVICES}")
    logger.info(f"Output: {PRICING_DATA_DIR}")

    result = subprocess.run(cmd, capture_output=False)
    if result.returncode != 0:
        logger.error(f"Pricing scraper failed with exit code {result.returncode}")
        return False

    logger.info("Pricing scraper completed")
    return True


def run_quota_scraper():
    """Run the quota doc scraper for Bedrock quotas across all regions."""
    logger.info("=" * 60)
    logger.info("Step 2: Running quota-doc-scraper")
    logger.info("=" * 60)

    cmd = [
        sys.executable, QUOTA_SCRAPER,
        '--all-regions',
        '--output', QUOTA_DATA_DIR,
    ]

    logger.info(f"Output: {QUOTA_DATA_DIR}")

    result = subprocess.run(cmd, capture_output=False)
    if result.returncode != 0:
        logger.error(f"Quota scraper failed with exit code {result.returncode}")
        return False

    logger.info("Quota scraper completed")
    return True


def sync_to_s3(bucket: str):
    """Sync pricing and quota data directories to S3."""
    logger.info("=" * 60)
    logger.info("Step 3: Syncing data to S3")
    logger.info("=" * 60)

    syncs = [
        (PRICING_DATA_DIR, f's3://{bucket}/pricing_data/'),
        (QUOTA_DATA_DIR, f's3://{bucket}/quota_data/'),
    ]

    for local_dir, s3_uri in syncs:
        if not os.path.isdir(local_dir):
            logger.warning(f"Skipping {local_dir} — directory does not exist")
            continue

        logger.info(f"Syncing {local_dir} → {s3_uri}")
        result = subprocess.run(
            ['aws', 's3', 'sync', local_dir, s3_uri, '--delete'],
            capture_output=False,
        )
        if result.returncode != 0:
            logger.error(f"S3 sync failed for {local_dir}")
            return False

        logger.info(f"Synced {local_dir} → {s3_uri}")

    return True


def sync_knowledge_base(kb_id: str, region: str):
    """Start a Knowledge Base ingestion job and wait for completion."""
    logger.info("=" * 60)
    logger.info("Step 4: Syncing Knowledge Base")
    logger.info("=" * 60)
    logger.info(f"Knowledge Base ID: {kb_id}")
    logger.info(f"Region: {region}")

    client = boto3.client('bedrock-agent', region_name=region)

    # List data sources for this KB
    ds_response = client.list_data_sources(knowledgeBaseId=kb_id)
    data_sources = ds_response.get('dataSourceSummaries', [])

    if not data_sources:
        logger.error("No data sources found for this Knowledge Base")
        return False

    logger.info(f"Found {len(data_sources)} data source(s)")

    for ds in data_sources:
        ds_id = ds['dataSourceId']
        ds_name = ds.get('name', ds_id)
        logger.info(f"Starting ingestion for data source: {ds_name} ({ds_id})")

        try:
            ing_response = client.start_ingestion_job(
                knowledgeBaseId=kb_id,
                dataSourceId=ds_id,
            )
            job_id = ing_response['ingestionJob']['ingestionJobId']
            logger.info(f"Ingestion job started: {job_id}")

            # Poll for completion
            while True:
                status_resp = client.get_ingestion_job(
                    knowledgeBaseId=kb_id,
                    dataSourceId=ds_id,
                    ingestionJobId=job_id,
                )
                status = status_resp['ingestionJob']['status']
                logger.info(f"  Job {job_id}: {status}")

                if status in ('COMPLETE', 'FAILED', 'STOPPED'):
                    break
                time.sleep(10)

            if status != 'COMPLETE':
                logger.error(f"Ingestion job {job_id} ended with status: {status}")
                failure = status_resp['ingestionJob'].get('failureReasons', [])
                if failure:
                    logger.error(f"Failure reasons: {failure}")
                return False

            logger.info(f"Ingestion job {job_id} completed successfully")

        except Exception as e:
            logger.error(f"Error starting ingestion for {ds_id}: {e}")
            return False

    return True


def main():
    parser = argparse.ArgumentParser(description='Batch data loader for TCO & BVA Analyst')
    parser.add_argument('--skip-scrape', action='store_true',
                        help='Skip scraping, only sync to S3 and KB')
    parser.add_argument('--skip-sync', action='store_true',
                        help='Only scrape, skip S3 and KB sync')
    args = parser.parse_args()

    bucket = os.environ.get('TCO_ANALYSIS_S3_BUCKET', '')
    kb_id = os.environ.get('STRANDS_KNOWLEDGE_BASE_ID', '')
    region = os.environ.get('AWS_REGION', 'us-east-1')

    if not args.skip_sync:
        if not bucket:
            logger.error("TCO_ANALYSIS_S3_BUCKET environment variable is required")
            sys.exit(1)
        if not kb_id:
            logger.error("STRANDS_KNOWLEDGE_BASE_ID environment variable is required")
            sys.exit(1)

    start = time.time()

    # Step 1 & 2: Scrape
    if not args.skip_scrape:
        clean_data_dirs()

        if not run_pricing_scraper():
            logger.error("Pricing scraper failed — aborting")
            sys.exit(1)

        if not run_quota_scraper():
            logger.error("Quota scraper failed — aborting")
            sys.exit(1)

    # Step 3: Sync to S3
    if not args.skip_sync:
        if not sync_to_s3(bucket):
            logger.error("S3 sync failed — aborting")
            sys.exit(1)

        # Step 4: Sync Knowledge Base
        if not sync_knowledge_base(kb_id, region):
            logger.error("Knowledge Base sync failed")
            sys.exit(1)

    elapsed = time.time() - start
    logger.info("=" * 60)
    logger.info(f"All steps completed in {elapsed:.1f}s")
    logger.info("=" * 60)


if __name__ == '__main__':
    main()
