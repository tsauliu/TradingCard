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
- `create_tcg_weekly_summary.py` - Weekly summary analysis with ASP, volume, and unique product metrics
- `create_top50_analysis.py` - Top 15 products analysis by lifecycle quantity sold with Excel date formatting

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

## Key Analysis Scripts

### create_tcg_weekly_summary.py (Modified 2025-08-21)
**Location:** `/tempfile/create_tcg_weekly_summary.py`
**Purpose:** Comprehensive weekly summary analysis of BDA data with three key metrics

**Output Sections:**
1. **Weekly Average Selling Price (ASP) by Condition** - Price trends across 6 card conditions
2. **Weekly Quantity Sold by Condition** - Volume trends across 6 card conditions  
3. **Weekly Number of Unique Products with Sales** - Count of unique product IDs with quantity_sold > 0 per week

**Data Coverage:** 2024-present (53+ weeks)
**Output Format:** Formatted Excel with datetime prefix (YYYY-MM-DD_HHMMSS_bda_weekly_summary_formatted.xlsx)
**Card Conditions:** Damaged, Heavily Played, Lightly Played, Moderately Played, Near Mint, Unopened

### create_top50_analysis.py (Modified 2025-08-21)
**Location:** `/tempfile/create_top50_analysis.py`
**Purpose:** Top 15 products analysis by lifecycle quantity sold with comprehensive filtering

**Filters Applied:**
1. **ASP > $5** - Only products with average selling price above $5
2. **First Week Quantity > 0** - Excludes products with zero sales in their chronological first week
3. **Lifecycle Quantity Sold** - Ranked by total quantity sold across all time periods

**Output Structure:**
- **3-Sheet Excel Format** with datetime prefix (YYYY-MM-DD_HHMMSS_top15_products_analysis.xlsx)
- **Metadata Sheet** (15 products × 6 columns): product_id, lifecycle_quantity_sold, group_name, product_cleanName, product_url, group_product_id (combined)
- **Prices Sheet** (15 products × 53 weeks): product_id + weekly price data with Excel date formatting
- **Quantities Sheet** (15 products × 53 weeks): product_id + weekly quantity data with Excel date formatting
- **Detailed Sheet**: Raw weekly data by condition for reference

**Data Source:** tcg_prices_bda (quantity_sold × market_price calculations)
**Date Range:** 2024-present, starting from each product's first sales week
