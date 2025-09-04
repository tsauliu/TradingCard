#!/usr/bin/env python3
"""
Run the full categories downloader WITHOUT proxy manager to avoid switching issues
"""
import logging
import os
import json
import requests
import pandas as pd
import time
from datetime import datetime
from enhanced_api_downloader import EnhancedTCGMetadataDownloader

def main():
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('tcg_direct_no_proxy.log'),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger(__name__)
    
    logger.info("Starting TCG Metadata Downloader (Direct Connection, No Proxy Manager)")
    
    try:
        # Initialize downloader with proxy manager disabled
        downloader = EnhancedTCGMetadataDownloader(
            min_request_interval=0.5,  # Faster with direct connection
            max_request_interval=1.5,
            backoff_factor=1.0,
            max_retries=3,
            use_proxy_manager=False,  # DISABLE proxy manager
            batch_size=100
        )
        
        logger.info("✅ Downloader initialized without proxy manager")
        
        # Get all categories first
        try:
            response = requests.get("https://tcgcsv.com/tcgplayer/categories", timeout=30)
            response.raise_for_status()
            categories = response.json().get('results', [])
            logger.info(f"Found {len(categories)} total categories")
        except Exception as e:
            logger.error(f"Failed to fetch categories: {e}")
            return
        
        # Exclude Pokemon categories 
        excluded_categories = [3, 85]  # Pokemon, Pokemon Japan
        filtered_categories = [cat for cat in categories 
                             if cat.get('categoryId', 0) not in excluded_categories]
        
        logger.info(f"Processing {len(filtered_categories)} categories (excluding Pokemon variants)")
        
        # Process categories
        total_products = 0
        for i, category in enumerate(filtered_categories, 1):
            category_id = category.get('categoryId')
            category_name = category.get('name', 'Unknown')
            
            logger.info(f"\n=== [{i}/{len(filtered_categories)}] CATEGORY: {category_name} ===")
            
            try:
                # Download this category using the basic method
                products_added = download_category_simple(category_id, category_name, logger)
                total_products += products_added
                
                logger.info(f"Category {category_name} completed: +{products_added} products")
                logger.info(f"Progress: {i}/{len(filtered_categories)} categories, {total_products} total products")
                
            except Exception as e:
                logger.error(f"Category {category_name} failed: {e}")
                continue
        
        logger.info(f"✅ All categories completed! Total products: {total_products}")
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")

def download_category_simple(category_id: int, category_name: str, logger) -> int:
    """Simple category download without proxy switching"""
    
    # Get groups for this category
    try:
        response = requests.get(f"https://tcgcsv.com/tcgplayer/{category_id}/groups", timeout=30)
        if response.status_code == 403:
            logger.warning(f"Rate limited on category {category_id}, skipping...")
            time.sleep(5)  # Wait longer on rate limit
            return 0
        
        response.raise_for_status()
        groups = response.json().get('results', [])
        logger.info(f"Found {len(groups)} groups in category {category_name}")
        
        if not groups:
            return 0
            
    except Exception as e:
        logger.error(f"Failed to get groups for category {category_id}: {e}")
        return 0
    
    # Process each group
    total_products = 0
    for j, group in enumerate(groups, 1):
        group_id = group.get('groupId')
        group_name = group.get('name', 'Unknown')
        
        if j % 100 == 0:
            logger.info(f"Processing group {j}/{len(groups)}: {group_name}")
        
        try:
            # Get products for this group
            response = requests.get(
                f"https://tcgcsv.com/tcgplayer/{category_id}/{group_id}/products", 
                timeout=30
            )
            
            if response.status_code == 403:
                logger.warning(f"Rate limited on group {group_id}, waiting...")
                time.sleep(2)  # Brief wait on rate limit
                continue
            
            response.raise_for_status()
            products = response.json().get('results', [])
            
            if products:
                total_products += len(products)
                # Here you would normally save to BigQuery, but for now just count
                
            # Small delay between requests
            time.sleep(0.2)
            
        except Exception as e:
            logger.warning(f"Group {group_id} failed: {e}")
            continue
    
    return total_products

if __name__ == "__main__":
    main()