# BigQuery Price Data Loader

This script loads JSON price data from the `product_details/` directory to Google BigQuery, extracting product IDs from filenames and adding them as a column.

## Files

- `load_to_bigquery.py` - Main script for loading data to BigQuery
- `test_script.py` - Test script to validate processing logic
- `config.py` - Configuration file for BigQuery settings
- `requirements.txt` - Python dependencies

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set up Google Cloud authentication:
```bash
# Option 1: Service account key
export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account-key.json"

# Option 2: Use gcloud CLI
gcloud auth application-default login
```

3. Update configuration in `config.py`:
```python
BIGQUERY_CONFIG = {
    "project_id": "your-gcp-project-id",
    "dataset_id": "your_dataset_name", 
    "table_id": "product_prices",
    "json_directory": "./product_details",
    "batch_size": 1000
}
```

## Usage

### Test the processing logic first:
```bash
python test_script.py
```

### Load data to BigQuery:
```bash
python load_to_bigquery.py
```

## Data Schema

The script creates a BigQuery table with the following schema:

| Column | Type | Description |
|--------|------|-------------|
| product_id | STRING | Extracted from filename (e.g., 481225 from 481225.0.json) |
| sku_id | STRING | Product SKU identifier |
| variant | STRING | Product variant (Normal, Foil, etc.) |
| language | STRING | Card language |
| condition | STRING | Card condition |
| average_daily_quantity_sold | STRING | Average daily sales quantity |
| average_daily_transaction_count | STRING | Average daily transaction count |
| total_quantity_sold | STRING | Total quantity sold |
| total_transaction_count | STRING | Total transaction count |
| bucket_start_date | DATE | Price bucket start date |
| market_price | FLOAT | Market price for the period |
| quantity_sold | INTEGER | Quantity sold in bucket |
| low_sale_price | FLOAT | Lowest sale price |
| low_sale_price_with_shipping | FLOAT | Lowest price including shipping |
| high_sale_price | FLOAT | Highest sale price |
| high_sale_price_with_shipping | FLOAT | Highest price including shipping |
| transaction_count | INTEGER | Number of transactions in bucket |
| file_processed_at | TIMESTAMP | When the file was processed |

## Features

- Extracts product ID from filename pattern `{product_id}.0.json`
- Flattens nested JSON structure (buckets array becomes separate rows)
- Handles data type conversions for BigQuery
- Batch processing for efficient uploads
- Error handling for individual file failures
- Progress tracking during processing

## Example

For file `481225.0.json`, the script:
1. Extracts product_id = "481225"
2. Processes the JSON structure
3. Creates one row per bucket with product_id included
4. Uploads to BigQuery with proper data types