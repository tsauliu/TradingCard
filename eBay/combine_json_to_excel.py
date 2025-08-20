import json
import pandas as pd
from datetime import datetime
import os
import glob

def process_json_files():
    """
    Process all JSON files in the current directory and combine them into a pivot-ready Excel file
    """
    # Get all JSON files in current directory
    json_files = glob.glob("*.json")
    
    all_data = []
    
    for json_file in json_files:
        # Extract card name from filename (remove .json extension)
        card_name = os.path.splitext(json_file)[0]
        
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                content = f.read()
                
            # Parse JSON objects that are concatenated together
            json_objects = []
            decoder = json.JSONDecoder()
            idx = 0
            
            while idx < len(content):
                content = content[idx:].lstrip()
                if not content:
                    break
                try:
                    obj, end_idx = decoder.raw_decode(content)
                    json_objects.append(obj)
                    idx += end_idx
                except json.JSONDecodeError:
                    break
            
            for data in json_objects:
                if isinstance(data, dict) and data.get('_type') == 'MetricsTrendsModule' and 'series' in data:
                    process_metrics_data(data, card_name, all_data)
                        
        except Exception as e:
            print(f"Error processing file {json_file}: {e}")
            continue
    
    # Convert to DataFrame
    if all_data:
        df = pd.DataFrame(all_data)
        
        # Convert timestamp to readable date
        df['Date'] = pd.to_datetime(df['Timestamp'], unit='ms')
        
        # Reorder columns for better readability
        columns = ['Filename', 'Date', 'Timestamp', 'Metric_Type', 'Value', 'Currency_Code']
        df = df[columns]
        
        # Sort by filename and date
        df = df.sort_values(['Filename', 'Date'])
        
        # Export to Excel
        output_file = 'combined_trading_card_data.xlsx'
        df.to_excel(output_file, index=False, sheet_name='TradingCardData')
        
        print(f"Successfully exported {len(df)} rows to {output_file}")
        print(f"Files processed: {', '.join(json_files)}")
        print(f"Unique cards: {df['Filename'].nunique()}")
        print(f"Date range: {df['Date'].min()} to {df['Date'].max()}")
        
        return df
    else:
        print("No valid data found in JSON files")
        return None

def process_metrics_data(data, card_name, all_data):
    """
    Extract time series data from MetricsTrendsModule
    """
    for series in data['series']:
        series_id = series['id']
        series_label = series['label']
        currency_code = series.get('currencyCode', '')
        
        # Skip regression line data as it's calculated
        if 'regression' in series_id.lower():
            continue
            
        for data_point in series['data']:
            timestamp = data_point[0]
            value = data_point[1]
            
            # Skip null values
            if value is not None:
                all_data.append({
                    'Filename': card_name,
                    'Timestamp': timestamp,
                    'Metric_Type': series_label,
                    'Value': value,
                    'Currency_Code': currency_code
                })

if __name__ == "__main__":
    df = process_json_files()