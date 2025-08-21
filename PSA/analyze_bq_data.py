#!/usr/bin/env python3
"""
BigQuery Data Analysis - Check for Duplicates
Analyzes the PSA auction price data for duplicates and data quality
"""

from google.cloud import bigquery
import pandas as pd
from datetime import datetime
import os

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

def analyze_psa_data():
    """Analyze PSA data in BigQuery for duplicates and quality"""
    
    # Initialize BigQuery client
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'rising-environs-456314-a3')
    client = bigquery.Client(project=project_id)
    
    print("🔍 PSA BigQuery Data Analysis")
    print("=" * 50)
    
    # 1. Basic data overview
    overview_query = """
    SELECT 
      COUNT(*) as total_records,
      COUNT(DISTINCT item_id) as unique_items,
      COUNT(DISTINCT grade) as unique_grades,
      COUNT(DISTINCT CONCAT(item_id, '-', grade, '-', record_type)) as unique_combinations,
      MIN(scraped_at) as first_scrape,
      MAX(scraped_at) as last_scrape
    FROM `rising-environs-456314-a3.tcg_data.psa_auction_prices`
    """
    
    print("\n📊 Data Overview:")
    df_overview = client.query(overview_query).to_dataframe()
    for col in df_overview.columns:
        print(f"  {col}: {df_overview[col].iloc[0]}")
    
    # 2. Check for exact duplicates (key fields only)
    duplicate_query = """
    WITH duplicates AS (
      SELECT 
        *,
        ROW_NUMBER() OVER (
          PARTITION BY 
            item_id, grade, record_type, sale_date, 
            CAST(sale_price AS STRING), CAST(total_sales_count AS STRING)
          ORDER BY scraped_at
        ) as rn
      FROM `rising-environs-456314-a3.tcg_data.psa_auction_prices`
    )
    SELECT 
      COUNT(*) as duplicate_records
    FROM duplicates 
    WHERE rn > 1
    """
    
    print("\n🔍 Exact Duplicate Check:")
    df_duplicates = client.query(duplicate_query).to_dataframe()
    duplicate_count = df_duplicates['duplicate_records'].iloc[0]
    print(f"  Exact duplicates: {duplicate_count}")
    
    # 3. Check for summary record duplicates (same item/grade/date combination)
    summary_duplicate_query = """
    SELECT 
      item_id,
      grade,
      DATE(scraped_at) as scrape_date,
      COUNT(*) as duplicate_count
    FROM `rising-environs-456314-a3.tcg_data.psa_auction_prices`
    WHERE record_type = 'summary'
    GROUP BY item_id, grade, DATE(scraped_at)
    HAVING COUNT(*) > 1
    ORDER BY duplicate_count DESC, item_id, grade
    """
    
    print("\n📋 Summary Record Duplicates:")
    df_summary_dups = client.query(summary_duplicate_query).to_dataframe()
    if len(df_summary_dups) > 0:
        print(f"  Found {len(df_summary_dups)} duplicate summary combinations:")
        print(df_summary_dups.to_string(index=False))
    else:
        print("  ✅ No summary record duplicates found")
    
    # 4. Data by item breakdown
    item_breakdown_query = """
    SELECT 
      item_id,
      COUNT(*) as total_records,
      COUNT(DISTINCT grade) as grades_covered,
      SUM(CASE WHEN record_type = 'summary' THEN 1 ELSE 0 END) as summary_records,
      SUM(CASE WHEN record_type = 'sale' THEN 1 ELSE 0 END) as sale_records,
      MIN(scraped_at) as first_scraped,
      MAX(scraped_at) as last_scraped
    FROM `rising-environs-456314-a3.tcg_data.psa_auction_prices`
    GROUP BY item_id
    ORDER BY item_id
    """
    
    print("\n📈 Data by Item:")
    df_items = client.query(item_breakdown_query).to_dataframe()
    print(df_items.to_string(index=False))
    
    # 5. Grade coverage analysis
    grade_coverage_query = """
    SELECT 
      item_id,
      STRING_AGG(grade ORDER BY 
        CASE 
          WHEN grade = '0' THEN 0
          ELSE CAST(grade AS FLOAT64)
        END DESC
      ) as grades_list,
      COUNT(DISTINCT grade) as grade_count
    FROM `rising-environs-456314-a3.tcg_data.psa_auction_prices`
    WHERE record_type = 'summary'
    GROUP BY item_id
    ORDER BY grade_count DESC, item_id
    """
    
    print("\n🎯 Grade Coverage by Item:")
    df_grades = client.query(grade_coverage_query).to_dataframe()
    print(df_grades.to_string(index=False))
    
    # 6. Recent scraping activity
    recent_activity_query = """
    SELECT 
      DATE(scraped_at) as scrape_date,
      COUNT(*) as records_scraped,
      COUNT(DISTINCT item_id) as items_scraped,
      MIN(scraped_at) as first_scrape_time,
      MAX(scraped_at) as last_scrape_time
    FROM `rising-environs-456314-a3.tcg_data.psa_auction_prices`
    GROUP BY DATE(scraped_at)
    ORDER BY scrape_date DESC
    """
    
    print("\n📅 Scraping Activity by Date:")
    df_activity = client.query(recent_activity_query).to_dataframe()
    print(df_activity.to_string(index=False))
    
    # 7. Data quality checks
    quality_query = """
    SELECT 
      'Records with missing sale_price (sale records)' as metric,
      COUNT(*) as count
    FROM `rising-environs-456314-a3.tcg_data.psa_auction_prices`
    WHERE record_type = 'sale' AND (sale_price IS NULL OR sale_price <= 0)
    
    UNION ALL
    
    SELECT 
      'Records with missing total_sales_count (summary records)' as metric,
      COUNT(*) as count
    FROM `rising-environs-456314-a3.tcg_data.psa_auction_prices`
    WHERE record_type = 'summary' AND (total_sales_count IS NULL OR total_sales_count <= 0)
    
    UNION ALL
    
    SELECT 
      'Records with invalid scraped_at' as metric,
      COUNT(*) as count
    FROM `rising-environs-456314-a3.tcg_data.psa_auction_prices`
    WHERE scraped_at IS NULL
    """
    
    print("\n⚠️ Data Quality Issues:")
    df_quality = client.query(quality_query).to_dataframe()
    print(df_quality.to_string(index=False))
    
    # 8. Sample of recent data
    sample_query = """
    SELECT 
      item_id,
      grade,
      record_type,
      total_sales_count,
      sale_price,
      scraped_at
    FROM `rising-environs-456314-a3.tcg_data.psa_auction_prices`
    ORDER BY scraped_at DESC
    LIMIT 10
    """
    
    print("\n📋 Sample of Recent Data:")
    df_sample = client.query(sample_query).to_dataframe()
    print(df_sample.to_string(index=False))
    
    print(f"\n✅ Analysis completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    return {
        'total_records': df_overview['total_records'].iloc[0],
        'duplicate_count': duplicate_count,
        'summary_duplicates': len(df_summary_dups),
        'items_processed': len(df_items)
    }

if __name__ == "__main__":
    try:
        results = analyze_psa_data()
        print(f"\n🎯 Summary: {results['total_records']} total records, {results['duplicate_count']} exact duplicates, {results['items_processed']} items processed")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()