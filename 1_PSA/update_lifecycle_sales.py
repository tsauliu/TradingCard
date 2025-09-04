#!/usr/bin/env python3
"""
Script to update BigQuery table with lifecycle sales count from enhanced psa_card_list.csv
"""

import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv
import os

def main():
    load_dotenv()
    
    # Initialize BigQuery client
    client = bigquery.Client(project='rising-environs-456314-a3')
    
    # Read enhanced CSV file
    df = pd.read_csv('/home/caoliu/TradingCard/PSA/psa_card_list.csv')
    print(f"Processing lifecycle sales counts for {len(df)} cards from CSV")
    
    # Process each card
    update_statements = []
    for _, row in df.iterrows():
        item_id = str(row['card_id'])
        card_name = row['card_name']
        lifecycle_sales_count = int(row['lifecycle_sales_count'])
        
        # Create UPDATE statement for this card
        update_query = f"""
        UPDATE `tcg_data.psa_auction_prices`
        SET lifecycle_sales_count = {lifecycle_sales_count}
        WHERE item_id = '{item_id}'
        """
        
        update_statements.append((item_id, card_name, lifecycle_sales_count, update_query))
        print(f"Prepared update for {card_name} (ID: {item_id}) - {lifecycle_sales_count:,} sales")
    
    # Execute updates
    print(f"\nExecuting {len(update_statements)} update statements...")
    
    total_updated = 0
    for item_id, card_name, sales_count, query in update_statements:
        try:
            result = client.query(query)
            result.result()  # Wait for completion
            
            # Check how many rows were updated
            updated_rows = result.num_dml_affected_rows
            total_updated += updated_rows
            print(f"✅ Updated {updated_rows:,} records for {card_name} ({sales_count:,} sales)")
            
        except Exception as e:
            print(f"❌ Error updating item_id {item_id}: {e}")
    
    print(f"\n✅ Lifecycle sales count update completed!")
    print(f"Total records updated: {total_updated:,}")
    print(f"Total sales volume: {sum(row['lifecycle_sales_count'] for _, row in df.iterrows()):,}")

if __name__ == "__main__":
    main()