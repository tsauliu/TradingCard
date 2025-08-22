# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an eBay trading card metrics data processing repository that converts eBay auction data from JSON format to formatted Excel spreadsheets for analysis. The data contains weekly trading card auction metrics including average prices, quantities sold, and trend lines.

## Key Architecture & Data Processing Pattern

### Data Source Format
- **Input**: eBay MetricsTrendsModule JSON data with multiple concatenated JSON objects
- **Structure**: Each JSON contains time series data with Unix millisecond timestamps and metric values
- **Granularity**: Weekly data points spanning multiple years (2022-2025)
- **Metrics**: Average selling price (平均售价), total quantity sold (售出总计), trend lines (趋势线)

### Core Processing Flow
1. **JSON Parsing**: Handle concatenated JSON objects using custom decoder logic
2. **Time Series Extraction**: Convert Unix timestamps to readable dates, extract metric values
3. **Data Transformation**: Pivot data from time-series format to analysis-ready Excel format
4. **Excel Generation**: Create formatted spreadsheets with dates as columns, metrics as rows

## File Structure & Purpose

### Core Scripts
- `convert_json_to_excel.py` - **Primary converter**: Transforms JSON to Excel with dates as columns, variables as rows
- `combine_json_to_excel.py` - **Bulk processor**: Processes all JSON files in directory into single pivot-ready Excel
- `preview_data.py` - **Data validator**: Previews Excel output structure and validates conversion
- `preview_excel.py` - **Legacy previewer**: Views combined Excel data structure

### Data Files
- `pokemon card.json` - eBay Pokemon card auction metrics data
- `trading card.json` - eBay general trading card auction metrics data  
- `trading_cards_metrics.xlsx` - Generated Excel output with formatted time series data

## Common Commands

### Convert Specific JSON Files to Excel
```bash
python convert_json_to_excel.py
```
Converts the two main JSON files (pokemon card.json, trading card.json) to trading_cards_metrics.xlsx with weekly dates as columns and metrics as rows.

### Process All JSON Files in Directory
```bash
python combine_json_to_excel.py
```
Processes all *.json files in the current directory and creates combined_trading_card_data.xlsx in pivot-ready format.

### Preview Generated Data
```bash
python preview_data.py
```
Shows structure and sample data from the generated Excel file for validation.

## Data Processing Specifics

### JSON Structure Handling
The codebase handles complex eBay JSON responses that contain:
- Multiple concatenated JSON objects in single files
- MetricsTrendsModule objects with embedded time series data
- Mixed data types including regression line calculations (filtered out)
- Currency-specific formatting (USD prices)

### Excel Output Format
- **Columns**: Weekly dates in YYYY-MM-DD format (157+ time periods)
- **Rows**: Metric variables (6 total: 2 card types × 3 metrics each)
- **Variable naming**: `{CardType}_{MetricName}` format
- **Formatting**: Auto-adjusted column widths, wider variable name column

### Time Series Processing
- **Timestamp conversion**: Unix milliseconds → Python datetime objects
- **Date range**: Typically spans 2022-2025 (3+ years of weekly data)
- **Missing data**: Handled as empty cells in Excel output
- **Data validation**: Filters out null values and regression line calculations

## Dependencies

### Required Python Packages
```bash
pip install pandas openpyxl
```

### Core Libraries Used
- `pandas` - DataFrame operations and Excel I/O
- `json` - Custom JSON parsing for concatenated objects  
- `datetime` - Timestamp conversion utilities
- `re` - Regular expressions for JSON splitting
- `openpyxl` - Excel formatting and auto-sizing

## Data Schema Reference

### Input JSON Schema (MetricsTrendsModule)
```json
{
  "_type": "MetricsTrendsModule",
  "series": [
    {
      "id": "averageSold|quantity|quantityRegressionLine",
      "label": "平均售价|售出总计|趋势线",
      "currencyCode": "USD",
      "data": [[timestamp_ms, value, null]]
    }
  ]
}
```

### Output Excel Schema
- **Variable** (column A): Metric identifier string
- **Date columns**: YYYY-MM-DD format headers
- **Data cells**: Numeric values or empty for missing data
- **Dimensions**: ~6 rows × 158 columns (including Variable column)

## Important Processing Notes

### JSON Parsing Strategy
The JSON files contain multiple objects concatenated without separators. The code uses a custom parsing approach:
1. Split on `}{` boundaries
2. Reconstruct valid JSON strings  
3. Parse each object separately
4. Filter for MetricsTrendsModule types

### Metric Filtering
- **Included**: averageSold, quantity (actual metrics)
- **Excluded**: quantityRegressionLine (calculated trend data)
- **Currency handling**: USD prices preserved as-is
- **Null handling**: Skip data points with null values

### Excel Formatting Features
- Auto-adjusted column widths (max 15 characters)
- Variable names column set to 25 characters wide
- Professional formatting with proper headers
- Sheet naming: 'Trading_Card_Metrics'