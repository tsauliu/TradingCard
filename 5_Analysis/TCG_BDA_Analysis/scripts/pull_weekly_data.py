#!/usr/bin/env python3
"""
Script to pull weekly TCG price data from BigQuery
Generates weekly average mid_price by category_id with unique product counts
"""

import os
from google.cloud import bigquery
import pandas as pd

def pull_weekly_data():
    # Set up credentials
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/caoliu/TradingCard/5_Analysis/service-account.json'
    client = bigquery.Client()

    # SQL query for weekly mid_price by category_id with unique product count
    query = '''
    SELECT 
      category_id,
      DATE_TRUNC(price_date, WEEK(MONDAY)) as week_start_date,
      AVG(mid_price) as avg_mid_price,
      COUNT(DISTINCT product_id) as product_count
    FROM `rising-environs-456314-a3.tcg_data.tcg_prices`
    WHERE mid_price IS NOT NULL
    GROUP BY category_id, DATE_TRUNC(price_date, WEEK(MONDAY))
    ORDER BY category_id, week_start_date
    '''

    print('Executing query to pull weekly TCG data...')
    
    # Execute query
    query_job = client.query(query)
    results = query_job.result()

    # Convert to DataFrame
    df = results.to_dataframe()
    print(f'Query completed. Retrieved {len(df)} rows.')

    # Save to CSV
    output_file = '/home/caoliu/TradingCard/analysis/weekly_mid_prices_with_product_count.csv'
    df.to_csv(output_file, index=False)
    print(f'Data saved to: {output_file}')

    # Display summary
    print(f'\nSummary:')
    print(f'Categories: {df["category_id"].nunique()}')
    print(f'Total weeks: {len(df)}')
    print(f'Date range: {df["week_start_date"].min()} to {df["week_start_date"].max()}')
    print(f'Mid price range: ${df["avg_mid_price"].min():.2f} to ${df["avg_mid_price"].max():.2f}')
    print(f'Product count range: {df["product_count"].min()} to {df["product_count"].max()} unique products per week')
    
    print(f'\nFirst 5 rows:')
    print(df.head())
    
    return df

if __name__ == "__main__":
    data = pull_weekly_data()