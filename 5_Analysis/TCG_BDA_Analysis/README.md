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
Analyzes top 15 Pokemon products (English and Japanese combined) by lifetime sales volume with:
- **ASP by Product** - Weekly weighted average prices for each card
- **Trading Volumes by Product** - Weekly sales volumes for each card
- **Product Metadata** - Detailed information including TCGPlayer URLs

**Outputs:**
- Excel file with ASP and volume data
- Text file with product links and metadata for reference

## Usage

```bash
# Run weekly summary analysis
python3 pokemon_weekly_summary.py

# Run top 15 products analysis  
python3 pokemon_top15_products.py
```

## Data Filters
- **Categories:** 
  - Pokemon (category_categoryId = 3)
  - Pokemon Japan (category_categoryId = 85)
- **Date Range:** 2024-01-01 to present
- **Top Products Criteria:**
  - ASP > $5
  - First week quantity > 0
  - Ranked by lifetime quantity sold

## Output Location
All files are saved to `../output/` with timestamp prefix (YYYY-MM-DD_HHMMSS)

## Card Conditions
- Damaged
- Heavily Played
- Lightly Played
- Moderately Played
- Near Mint
- Unopened