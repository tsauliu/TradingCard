#!/usr/bin/env python3
"""
Script to update BigQuery table with card metadata from psa_card_list.csv
"""

import pandas as pd
import re
from google.cloud import bigquery
from dotenv import load_dotenv
import os
from typing import Dict, Any

def parse_card_metadata(row) -> Dict[str, Any]:
    """Parse card metadata from CSV row"""
    card_name = row['card_name']
    url = row['url']
    item_id = row['card_id']
    
    # Extract set from URL: /auctionprices/tcg-cards/1999-pokemon-game/...
    url_parts = url.strip('/').split('/')
    card_set = url_parts[2] if len(url_parts) > 2 else None
    
    # Extract year from set
    year_match = re.search(r'(\d{4})', card_set) if card_set else None
    card_year = int(year_match.group(1)) if year_match else None
    
    # Identify variant from card name
    card_variant = None
    if '1st Edition' in card_name:
        card_variant = '1st Edition'
    elif 'Shadowless' in card_name:
        card_variant = 'Shadowless'
    
    return {
        'item_id': str(item_id),
        'card_name': card_name,
        'card_set': card_set,
        'card_year': card_year,
        'card_variant': card_variant,
        'psa_url': url
    }

def main():
    load_dotenv()
    
    # Initialize BigQuery client
    client = bigquery.Client(project='rising-environs-456314-a3')
    
    # Read CSV file
    df = pd.read_csv('/home/caoliu/TradingCard/PSA/psa_card_list.csv')
    print(f"Processing {len(df)} cards from CSV")
    
    # Process each card
    update_statements = []
    for _, row in df.iterrows():
        metadata = parse_card_metadata(row)
        
        # Create UPDATE statement for this card
        update_query = f"""
        UPDATE `tcg_data.psa_auction_prices`
        SET 
            card_name = '{metadata['card_name']}',
            card_set = '{metadata['card_set']}',
            card_year = {metadata['card_year'] if metadata['card_year'] else 'NULL'},
            card_variant = {'NULL' if not metadata['card_variant'] else f"'{metadata['card_variant']}'"},
            psa_url = '{metadata['psa_url']}'
        WHERE item_id = '{metadata['item_id']}'
        """
        
        update_statements.append((metadata['item_id'], update_query))
        print(f"Prepared update for {metadata['card_name']} (ID: {metadata['item_id']})")
    
    # Execute updates
    print(f"\nExecuting {len(update_statements)} update statements...")
    
    for item_id, query in update_statements:
        try:
            result = client.query(query)
            result.result()  # Wait for completion
            
            # Check how many rows were updated
            updated_rows = result.num_dml_affected_rows
            print(f"✅ Updated {updated_rows} records for item_id {item_id}")
            
        except Exception as e:
            print(f"❌ Error updating item_id {item_id}: {e}")
    
    print("\n✅ Card metadata update completed!")

if __name__ == "__main__":
    main()