import json
import pandas as pd
from datetime import datetime
import re

def extract_metrics_data(json_file_path, card_type_name):
    """Extract time series data from JSON file"""
    with open(json_file_path, 'r', encoding='utf-8') as file:
        content = file.read()
    
    # Split multiple JSON objects if they exist
    json_objects = []
    json_strings = re.split(r'}\s*{', content)
    
    for i, json_str in enumerate(json_strings):
        if i == 0 and not json_str.endswith('}'):
            json_str += '}'
        elif i == len(json_strings) - 1 and not json_str.startswith('{'):
            json_str = '{' + json_str
        elif not json_str.startswith('{') and not json_str.endswith('}'):
            json_str = '{' + json_str + '}'
        
        try:
            obj = json.loads(json_str)
            json_objects.append(obj)
        except json.JSONDecodeError:
            continue
    
    # Find the MetricsTrendsModule object
    metrics_data = None
    for obj in json_objects:
        if obj.get('_type') == 'MetricsTrendsModule':
            metrics_data = obj
            break
    
    if not metrics_data:
        raise ValueError(f"No MetricsTrendsModule found in {json_file_path}")
    
    # Extract series data
    series_dict = {}
    for series in metrics_data.get('series', []):
        series_id = series.get('id')
        series_label = series.get('label', series_id)
        data_points = series.get('data', [])
        
        # Convert timestamp and values
        timestamps = []
        values = []
        for point in data_points:
            if len(point) >= 2:
                timestamp = point[0]
                value = point[1]
                if timestamp and value is not None:
                    # Convert milliseconds to datetime
                    dt = datetime.fromtimestamp(timestamp / 1000)
                    timestamps.append(dt)
                    values.append(value)
        
        if timestamps and values:
            series_dict[f"{card_type_name}_{series_label}"] = dict(zip(timestamps, values))
    
    return series_dict

def create_excel_from_json_files(pokemon_file, trading_file, output_file):
    """Convert both JSON files to Excel with dates as columns and variables as rows"""
    
    # Extract data from both files
    pokemon_data = extract_metrics_data(pokemon_file, "Pokemon")
    trading_data = extract_metrics_data(trading_file, "Trading_Card")
    
    # Combine all series data
    all_series = {}
    all_series.update(pokemon_data)
    all_series.update(trading_data)
    
    # Get all unique dates
    all_dates = set()
    for series_data in all_series.values():
        all_dates.update(series_data.keys())
    
    # Sort dates
    sorted_dates = sorted(all_dates)
    
    # Create DataFrame
    data = []
    for series_name, series_data in all_series.items():
        row = [series_name]  # Variable name as first column
        for date in sorted_dates:
            value = series_data.get(date, '')  # Empty string if no data for this date
            row.append(value)
        data.append(row)
    
    # Create column headers
    date_columns = [date.strftime('%Y-%m-%d') for date in sorted_dates]
    columns = ['Variable'] + date_columns
    
    # Create DataFrame
    df = pd.DataFrame(data, columns=columns)
    
    # Write to Excel
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Trading_Card_Metrics', index=False)
        
        # Get the workbook and worksheet for formatting
        workbook = writer.book
        worksheet = writer.sheets['Trading_Card_Metrics']
        
        # Auto-adjust column widths
        for column in worksheet.columns:
            max_length = 0
            column_letter = column[0].column_letter
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 15)  # Cap width at 15
            worksheet.column_dimensions[column_letter].width = adjusted_width
        
        # Format variable names column to be wider
        worksheet.column_dimensions['A'].width = 25
    
    print(f"Excel file created successfully: {output_file}")
    print(f"Data shape: {df.shape[0]} variables x {df.shape[1]-1} time periods")
    print(f"Variables included:")
    for var in df['Variable'].tolist():
        print(f"  - {var}")

if __name__ == "__main__":
    # File paths
    pokemon_json = "pokemon card.json"
    trading_json = "trading card.json"
    output_excel = "trading_cards_metrics.xlsx"
    
    try:
        create_excel_from_json_files(pokemon_json, trading_json, output_excel)
    except Exception as e:
        print(f"Error: {e}")