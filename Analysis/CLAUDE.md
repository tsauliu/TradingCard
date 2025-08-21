# TradingCard Analysis Project

## Project Overview
This project pulls data from BigQuery and performs analysis on trading card data.

## Data Safety Rules

### Critical Rule: Data Protection
**NEVER delete or modify data - READ-ONLY ACCESS ONLY**

- All data operations must be read-only
- No DELETE, UPDATE, or INSERT operations on source data
- Always use SELECT statements for data retrieval
- Create copies or views for analysis, never modify original datasets
- When performing analysis, work with temporary tables or local copies

## Project Structure
- Data source: BigQuery
- Purpose: Trading card data analysis
- Access pattern: Read-only data pulling and analysis

### Directory Organization

#### /scripts
Contains reusable, generic scripts that can be used across different analyses:
- `pull_weekly_data.py` - Generic script to pull weekly TCG price data from BigQuery
- `create_pivot_tables.py` - Creates formatted Excel pivot tables by category  
- `create_single_pivot.py` - Creates single-sheet pivot with price and count sections
- `create_enhanced_pivot.py` - Enhanced pivot tables with category display names

#### /tempfile  
Contains one-time, specific-use scripts for particular analyses:
- `create_bda_pivot.py` - Specific analysis for BDA (Business Data Analytics) data
- `create_bda_condition_pivot.py` - BDA data analysis grouped by card condition
- `create_alakazam_condition_pivot.py` - Product-specific analysis for Alakazam (Product 42346)

#### /output
Directory for generated data files and analysis results. Generated files should use datetime prefix format (YYYY-MM-DD_HHMMSS) for easy sorting and identification:
- CSV exports from BigQuery queries
- Excel pivot tables and reports  
- Analysis summaries and charts

## BigQuery Table Schemas

### tcg_metadata (118,339 rows)
Contains category, group, and product metadata information.

**Schema:**
- category_categoryId: INTEGER
- category_name: STRING
- category_modifiedOn: STRING
- category_displayName: STRING
- category_seoCategoryName: STRING
- category_categoryDescription: STRING
- category_categoryPageTitle: STRING
- category_sealedLabel: STRING
- category_nonSealedLabel: STRING
- category_conditionGuideUrl: STRING
- category_isScannable: STRING
- category_popularity: STRING
- category_isDirect: STRING
- group_groupId: INTEGER
- group_name: STRING
- group_abbreviation: STRING
- group_isSupplemental: STRING
- group_publishedOn: STRING
- group_modifiedOn: STRING
- group_categoryId: INTEGER
- product_productId: INTEGER
- product_name: STRING
- product_cleanName: STRING
- product_imageUrl: STRING
- product_categoryId: INTEGER
- product_groupId: INTEGER
- product_url: STRING
- product_modifiedOn: STRING
- product_imageCount: STRING
- product_presaleInfo: STRING
- product_extendedData: STRING
- update_date: DATE

### tcg_prices (232,834,370 rows)
Contains pricing data with timestamps for trading card products.

**Schema:**
- price_date: DATE
- product_id: INTEGER
- sub_type_name: STRING
- low_price: FLOAT
- mid_price: FLOAT
- high_price: FLOAT
- market_price: FLOAT
- direct_low_price: FLOAT
- category_id: INTEGER
- group_id: INTEGER
- update_timestamp: TIMESTAMP

### tcg_prices_bda (8,429,824 rows)
Contains sales transaction data and business analytics information.

**Schema:**
- product_id: STRING (REQUIRED)
- sku_id: STRING
- variant: STRING
- language: STRING
- condition: STRING
- average_daily_quantity_sold: STRING
- average_daily_transaction_count: STRING
- total_quantity_sold: STRING
- total_transaction_count: STRING
- file_processed_at: TIMESTAMP
- bucket_start_date: DATE
- market_price: FLOAT
- quantity_sold: INTEGER
- low_sale_price: FLOAT
- low_sale_price_with_shipping: FLOAT
- high_sale_price: FLOAT
- high_sale_price_with_shipping: FLOAT
- transaction_count: INTEGER
- always output as formatted excel