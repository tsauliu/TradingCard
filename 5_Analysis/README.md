# Trading Card Analysis Project - Reorganized Structure

## Directory Organization

### PSA_Analysis/
Contains all PSA (Professional Sports Authenticator) graded card analysis scripts:
- `create_psa_weekly_analysis.py` - Analyzes PSA auction price data with outlier detection and weekly aggregation

### TCG_BDA_Analysis/
Contains all TCG (Trading Card Game) and BDA (Business Data Analytics) related analysis:

#### Main Scripts:
- `create_tcg_weekly_summary.py` - BDA weekly summary with ASP and trading volume by condition
- `create_top50_analysis.py` - Top products analysis from tcg_prices_bda table
- `pull_price_history_counts.py` - Pull price history counts by category from tcg_prices

#### Subdirectories:
- `scripts/` - Reusable generic TCG data processing scripts:
  - `pull_weekly_data.py` - Generic script to pull weekly TCG price data
  - `create_pivot_tables.py` - Creates formatted Excel pivot tables by category
  - `create_single_pivot.py` - Creates single-sheet pivot with price and count sections
  - `create_enhanced_pivot.py` - Enhanced pivot tables with category display names

- `analysis/` - Analysis output files:
  - `pokemon card.json` - Pokemon card analysis data
  - `trading card.json` - Trading card analysis data
  - `trading_cards_metrics.xlsx` - Trading card metrics spreadsheet

#### Data Files:
- `Categories.csv` - TCG category reference data

### Root Directory Files:
- `CLAUDE.md` - Project instructions and documentation
- `service-account.json` - Google Cloud service account credentials
- `.env` - Environment variables configuration
- `.claude/` - Claude-specific settings

## Output Directory
All scripts generate output to `../output/` (relative to their location), which creates an output directory at the root level when run from either PSA_Analysis or TCG_BDA_Analysis.

## Path Updates
All Python scripts have been updated to reference:
- Service account: `/home/caoliu/TradingCard/5_Analysis/service-account.json`
- Categories CSV: `/home/caoliu/TradingCard/5_Analysis/TCG_BDA_Analysis/Categories.csv`