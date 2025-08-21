"""Configuration file for BigQuery loading"""

# BigQuery Configuration
BIGQUERY_CONFIG = {
    "project_id": "your-project-id",     # Replace with your GCP project ID
    "dataset_id": "tcg_data",            # Replace with your dataset ID  
    "table_id": "tcg_prices_bda",        # Replace with your table ID
    "json_directory": "/tmp/product_details",
    "batch_size": 1000
}

# Schema mapping for data type conversions
SCHEMA_MAPPING = {
    "numeric_columns": [
        "market_price", 
        "low_sale_price", 
        "low_sale_price_with_shipping",
        "high_sale_price", 
        "high_sale_price_with_shipping"
    ],
    "integer_columns": [
        "quantity_sold", 
        "transaction_count"
    ],
    "date_columns": [
        "bucket_start_date"
    ]
}