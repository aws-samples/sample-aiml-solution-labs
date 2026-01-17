#!/usr/bin/env python3
"""
AWS Pricing Document Scraper

Reads the AWS pricing index and fetches pricing documents for all services.
Saves product information to files organized by service/region/product.

Output structure:
    {output_dir}/{service_name}/{region_code}/{product_name}.txt

Usage:
    python price_doc_scraper.py
    python price_doc_scraper.py --service AmazonEC2
    python price_doc_scraper.py --output ./pricing_data
"""

import requests
import json
import os
import re
import logging
from typing import Dict, List, Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BASE_URL = "https://pricing.us-east-1.amazonaws.com"
INDEX_URL = f"{BASE_URL}/offers/v1.0/aws/index.json"

# Default regions to capture
DEFAULT_REGIONS = ['us-west-2', 'us-east-1', 'us-gov-west-1', 'us-gov-east-1']


def fetch_json(url: str) -> Optional[Dict]:
    """
    Fetch JSON from a URL.
    
    Args:
        url: URL to fetch
        
    Returns:
        Parsed JSON as dictionary, or None on error
    """
    try:
        logger.info(f"Fetching: {url}")
        response = requests.get(url, timeout=120)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Error fetching {url}: {e}")
        return None


def sanitize_filename(name: str) -> str:
    """
    Sanitize a string to be used as a filename.
    
    Args:
        name: Original name
        
    Returns:
        Sanitized filename
    """
    # Replace invalid characters with underscore
    sanitized = re.sub(r'[<>:"/\\|?*]', '_', name)
    # Remove leading/trailing spaces and dots
    sanitized = sanitized.strip(' .')
    # Limit length
    if len(sanitized) > 200:
        sanitized = sanitized[:200]
    return sanitized or 'unknown'


def save_product_files(
    pricing_doc: Dict, 
    service_name: str, 
    output_dir: str,
    regions_filter: List[str]
) -> int:
    """
    Save product information to files organized by service/region/product.
    
    Args:
        pricing_doc: Pricing document JSON
        service_name: Name of the service
        output_dir: Base output directory
        regions_filter: List of region codes to include
        
    Returns:
        Number of files saved
    """
    products = pricing_doc.get('products', {})
    terms = pricing_doc.get('terms', {})
    files_saved = 0
    
    for sku, product in products.items():
        attributes = product.get('attributes', {})
        
        # Get region code (try different attribute names)
        region_code = (
            attributes.get('regionCode') or 
            attributes.get('location') or 
            attributes.get('region') or
            'global'
        )
        
        # Filter by region
        if region_code not in regions_filter and region_code != 'global':
            continue
        
        region_code = sanitize_filename(region_code)
        
        # Get product name/identifier
        product_name = (
            attributes.get('instanceType') or
            attributes.get('usagetype') or
            attributes.get('productFamily') or
            sku
        )
        product_name = sanitize_filename(product_name)
        
        # Create directory structure: {service}/{region}/
        dir_path = os.path.join(output_dir, service_name, region_code)
        os.makedirs(dir_path, exist_ok=True)
        
        # Build file content
        content_lines = [
            f"SKU: {sku}",
            f"Service: {service_name}",
            f"Region: {region_code}",
            "",
            "=== ATTRIBUTES ===",
        ]
        
        for key, value in sorted(attributes.items()):
            content_lines.append(f"{key}: {value}")
        
        # Add pricing terms if available
        on_demand = terms.get('OnDemand', {}).get(sku, {})
        reserved = terms.get('Reserved', {}).get(sku, {})
        
        if on_demand:
            content_lines.append("")
            content_lines.append("=== ON-DEMAND PRICING ===")
            for term_code, term_data in on_demand.items():
                price_dimensions = term_data.get('priceDimensions', {})
                for dim_code, dim_data in price_dimensions.items():
                    unit = dim_data.get('unit', '')
                    price = dim_data.get('pricePerUnit', {}).get('USD', 'N/A')
                    description = dim_data.get('description', '')
                    content_lines.append(f"  {unit}: ${price} - {description}")
        
        if reserved:
            content_lines.append("")
            content_lines.append("=== RESERVED PRICING ===")
            for term_code, term_data in reserved.items():
                term_attrs = term_data.get('termAttributes', {})
                lease_term = term_attrs.get('LeaseContractLength', '')
                purchase_option = term_attrs.get('PurchaseOption', '')
                content_lines.append(f"  Term: {lease_term}, Option: {purchase_option}")
        
        # Write file
        file_path = os.path.join(dir_path, f"{product_name}_{sku[:8]}.txt")
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(content_lines))
            files_saved += 1
        except IOError as e:
            logger.error(f"Error writing {file_path}: {e}")
    
    return files_saved


def get_pricing_documents(
    services_filter: Optional[List[str]] = None,
    regions_filter: Optional[List[str]] = None,
    output_dir: str = './pricing_data'
) -> List[Dict]:
    """
    Fetch pricing documents for all services and save to files.
    
    Args:
        services_filter: Optional list of service names to filter
        regions_filter: List of region codes to include (default: US regions)
        output_dir: Directory to save output files
        
    Returns:
        List of pricing document metadata
    """
    pricing_documents = []
    total_files = 0
    
    # Use default regions if not specified
    if regions_filter is None:
        regions_filter = DEFAULT_REGIONS
    
    logger.info(f"Filtering to regions: {regions_filter}")
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Step 1: Read main index
    logger.info("Step 1: Reading main index...")
    index = fetch_json(INDEX_URL)
    if not index:
        logger.error("Failed to fetch main index")
        return pricing_documents
    
    # Step 2: Get offers
    offers = index.get('offers', {})
    logger.info(f"Found {len(offers)} service offers")
    
    # Apply filter if specified
    if services_filter:
        offers = {k: v for k, v in offers.items() if k in services_filter}
        logger.info(f"Filtered to {len(offers)} services")
    
    # Step 3: For each offer, get currentVersionUrl and fetch content
    for offer_name, offer_data in offers.items():
        current_version_url = offer_data.get('currentVersionUrl')
        
        if not current_version_url:
            logger.warning(f"No currentVersionUrl for {offer_name}")
            continue
        
        # Build full URL
        full_url = f"{BASE_URL}{current_version_url}"
        logger.info(f"Fetching pricing for {offer_name}...")
        
        # Fetch pricing document
        pricing_doc = fetch_json(full_url)
        
        if pricing_doc:
            # Save product files
            files_saved = save_product_files(pricing_doc, offer_name, output_dir, regions_filter)
            total_files += files_saved
            
            pricing_documents.append({
                'service': offer_name,
                'url': full_url,
                'products_count': len(pricing_doc.get('products', {})),
                'files_saved': files_saved
            })
            logger.info(f"✓ {offer_name}: saved {files_saved} product files")
        else:
            logger.warning(f"✗ Failed to fetch {offer_name}")
    
    logger.info(f"\nTotal documents fetched: {len(pricing_documents)}")
    logger.info(f"Total files saved: {total_files}")
    return pricing_documents


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='AWS Pricing Document Scraper')
    parser.add_argument('--service', type=str, action='append',
                        help='Service to fetch (can specify multiple)')
    parser.add_argument('--output', type=str, default='./pricing_data',
                        help='Output directory (default: ./pricing_data)')
    parser.add_argument('--region', type=str, action='append',
                        help='Region to include (default: us-west-2, us-east-1, us-gov-west-1, us-gov-east-1)')
    parser.add_argument('--all-regions', action='store_true',
                        help='Include all regions (override default filter)')
    parser.add_argument('--list', action='store_true',
                        help='List available services only')
    
    args = parser.parse_args()
    
    if args.list:
        # Just list services
        index = fetch_json(INDEX_URL)
        if index:
            offers = index.get('offers', {})
            print(f"\nAvailable services ({len(offers)}):")
            for name in sorted(offers.keys()):
                print(f"  • {name}")
        return
    
    # Fetch pricing documents and save files
    documents = get_pricing_documents(
        services_filter=args.service,
        output_dir=args.output
    )
    
    print(f"\n{'='*60}")
    print(f"Summary")
    print(f"{'='*60}")
    print(f"Output directory: {args.output}")
    print(f"Services processed: {len(documents)}")
    
    total_products = sum(d['products_count'] for d in documents)
    total_files = sum(d['files_saved'] for d in documents)
    print(f"Total products: {total_products}")
    print(f"Total files saved: {total_files}")
    
    print(f"\nPer service:")
    for doc in documents:
        print(f"  • {doc['service']}: {doc['files_saved']} files")
    
    print(f"\nFiles saved to: {args.output}/{{service}}/{{region}}/{{product}}.txt")


if __name__ == "__main__":
    main()
