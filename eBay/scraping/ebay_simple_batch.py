#!/usr/bin/env python3
"""Minimal eBay batch search - essentials only"""

import json
import time
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path

def batch_search(keywords_list, cookie_file='ebay_cookies.txt', output_dir='raw_jsons', days=1095):
    """Search eBay for keywords and save raw JSONs"""
    
    # Read cookies
    with open(cookie_file, 'r') as f:
        cookies = f.read().strip()
    
    # Setup session
    session = requests.Session()
    session.headers.update({
        'accept': '*/*',
        'cookie': cookies,
        'referer': 'https://www.ebay.com/sh/research'
    })
    
    # Create output dir
    Path(output_dir).mkdir(exist_ok=True)
    
    # Calculate timestamps
    end_date = datetime.now()
    start_date = end_date - pd.Timedelta(days=days)
    end_timestamp = int(end_date.timestamp() * 1000)
    start_timestamp = int(start_date.timestamp() * 1000)
    
    # Search each keyword
    for i, keyword in enumerate(keywords_list):
        print(f"[{i+1}/{len(keywords_list)}] Searching: {keyword}")
        
        # Build URL - exactly like nodejs.js
        params = {
            'marketplace': 'EBAY-US',
            'keywords': keyword,
            'dayRange': days,
            'endDate': end_timestamp,
            'startDate': start_timestamp,
            'categoryId': 0,
            'offset': 0,
            'limit': 50,
            'tabName': 'SOLD',
            'tz': 'Asia/Shanghai',
            'modules': 'metricsTrends'
        }
        
        # Make request
        response = session.get(
            'https://www.ebay.com/sh/research/api/search',
            params=params,
            timeout=30
        )
        
        # Check auth
        if "auth_required" in response.text.lower():
            print("ERROR: Cookies expired! Update ebay_cookies.txt")
            break
        
        # Save raw response
        safe_keyword = "".join(c if c.isalnum() else '_' for c in keyword)[:50]
        output_file = Path(output_dir) / f"{i:04d}_{safe_keyword}.json"
        with open(output_file, 'w') as f:
            f.write(response.text)
        
        # Wait 10 seconds (except for last item)
        if i < len(keywords_list) - 1:
            time.sleep(60)


def jsons_to_excel(json_dir='raw_jsons', output_file='ebay_pivot.xlsx'):
    """Combine JSON files into Excel pivot table"""
    
    all_data = {}
    
    # Read all JSON files
    for json_file in sorted(Path(json_dir).glob('*.json')):
        keyword = json_file.stem.split('_', 1)[1]  # Remove index prefix
        
        with open(json_file, 'r') as f:
            content = f.read()
        
        # Parse JSON lines
        price_data = {}
        quantity_data = {}
        
        for line in content.strip().split('\n'):
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
                if obj.get('_type') == 'MetricsTrendsModule':
                    for serie in obj.get('series', []):
                        if serie.get('id') == 'averageSold':
                            for point in serie.get('data', []):
                                if point[1] is not None:
                                    date = datetime.fromtimestamp(point[0]/1000).strftime('%Y-%m-%d')
                                    price_data[date] = point[1]
                        elif serie.get('id') == 'quantity':
                            for point in serie.get('data', []):
                                if point[1] is not None:
                                    date = datetime.fromtimestamp(point[0]/1000).strftime('%Y-%m-%d')
                                    quantity_data[date] = point[1]
            except:
                continue
        
        all_data[keyword] = {'prices': price_data, 'quantities': quantity_data}
    
    # Get all dates
    all_dates = set()
    for item in all_data.values():
        all_dates.update(item['prices'].keys())
        all_dates.update(item['quantities'].keys())
    all_dates = sorted(list(all_dates))
    
    # Create DataFrames
    price_rows = []
    quantity_rows = []
    
    for keyword, data in all_data.items():
        price_row = {'Keyword': keyword}
        quantity_row = {'Keyword': keyword}
        for date in all_dates:
            price_row[date] = data['prices'].get(date, None)
            quantity_row[date] = data['quantities'].get(date, None)
        price_rows.append(price_row)
        quantity_rows.append(quantity_row)
    
    price_df = pd.DataFrame(price_rows).set_index('Keyword')
    quantity_df = pd.DataFrame(quantity_rows).set_index('Keyword')
    
    # Save to Excel
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        price_df.to_excel(writer, sheet_name='Prices')
        quantity_df.to_excel(writer, sheet_name='Quantities')
    
    print(f"Saved to {output_file}")
    print(f"  Keywords: {len(price_df)}")
    print(f"  Dates: {len(price_df.columns)}")


# Example usage:
if __name__ == '__main__':
    # Example 1: Search keywords
    keywords = ['pokemon cards', 'magic the gathering', 'yugioh cards']
    batch_search(keywords)
    
    # Example 2: Convert to Excel
    jsons_to_excel()