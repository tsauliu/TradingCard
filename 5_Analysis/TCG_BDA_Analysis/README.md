# Pokemon TCG Analysis Scripts

## Overview
Streamlined Pokemon trading card data analysis with two focused scripts covering both English and Japanese Pokemon products.

## Scripts

### 1. pokemon_weekly_summary.py
Generates overall Pokemon (EN + JP) weekly market analysis with:
- **ASP (Average Selling Price)** - Weighted by condition and volume
- **Trading Volumes** - Broken down by card condition  
- **Unique Pokemon Products** - Count of distinct products with sales (both English and Japanese)

**Output:** Excel file with 3 sections showing weekly trends from 2024-present

### 2. pokemon_top15_products.py  
Analyzes specific Pokemon products defined in `top15products.csv` with:
- **ASP by Product** - Weekly weighted average prices for each card
- **Trading Volumes by Product** - Weekly sales volumes for each card
- **Product Metadata** - Detailed information from BigQuery

**Required Input File:**
- `top15products.csv` - Contains TCGPlayer product URLs (one URL per line)
  - Example: `https://www.tcgplayer.com/product/453466/pokemon-crown-zenith-crown-zenith-booster-pack`
  - Product IDs are automatically extracted from URLs

**Output:**
- Excel file with ASP and volume data

## Usage

```bash
# Run weekly summary analysis (default scrape_date=2025-09-04)
python3 pokemon_weekly_summary.py

# Run with specific scrape_date
python3 pokemon_weekly_summary.py --scrape_date 2025-09-04

# Run top 15 products analysis (reads from top15products.csv)
python3 pokemon_top15_products.py

# Run with specific scrape_date
python3 pokemon_top15_products.py --scrape_date 2025-09-04
```

## Data Filters
- **Categories:** 
  - Pokemon (category_categoryId = 3)
  - Pokemon Japan (category_categoryId = 85)
- **Date Range:** 2024-01-01 to present
- **scrape_date:** Configurable via command-line parameter (default: 2025-09-04)

## Output Location
All files are saved to `../output/` with timestamp prefix (YYYY-MM-DD_HHMMSS)

## Card Conditions
- Damaged
- Heavily Played
- Lightly Played
- Moderately Played
- Near Mint
- Unopened