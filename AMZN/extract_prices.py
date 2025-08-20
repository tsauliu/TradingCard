#!/usr/bin/env python3
"""
Keepa API Price History Extractor
Extracts historical price data for Amazon products using Keepa API
"""

import os
import requests
import json
import csv
from datetime import datetime
from typing import Dict, List, Optional, Tuple

class KeepaExtractor:
    def __init__(self):
        self.api_key = self.load_api_key()
        self.base_url = "https://api.keepa.com"
        
    def load_api_key(self) -> str:
        """Load Keepa API key from .env file"""
        env_path = os.path.join(os.path.dirname(__file__), '.env')
        if not os.path.exists(env_path):
            raise FileNotFoundError(".env file not found")
            
        with open(env_path, 'r') as f:
            for line in f:
                if line.startswith('keepakey='):
                    key = line.split('=', 1)[1].strip().strip('"\'')
                    if key:
                        return key
        raise ValueError("keepakey not found in .env file")
    
    def keepa_time_to_datetime(self, keepa_time: int) -> datetime:
        """Convert Keepa time format to datetime object"""
        unix_timestamp = (keepa_time + 21564000) * 60
        return datetime.fromtimestamp(unix_timestamp)
    
    def extract_price_history(self, asin: str, domain: int = 1) -> Dict:
        """
        Extract price history for given ASIN
        domain: 1=com, 2=co.uk, 3=de, etc.
        """
        url = f"{self.base_url}/product"
        params = {
            'key': self.api_key,
            'domain': domain,
            'asin': asin,
            'stats': 1  # Include statistics
        }
        
        print(f"Requesting data for ASIN: {asin}")
        response = requests.get(url, params=params)
        
        if response.status_code != 200:
            raise Exception(f"API request failed: {response.status_code} - {response.text}")
        
        data = response.json()
        
        # Check for API errors
        if 'error' in data and data['error']:
            raise Exception(f"API Error: {data['error']}")
            
        if not data.get('products'):
            raise Exception("No product data returned")
            
        return data['products'][0]
    
    def parse_csv_data(self, csv_data: List[List[int]], price_type_index: int) -> List[Tuple[datetime, float]]:
        """Parse CSV price history data for specific price type"""
        if not csv_data or len(csv_data) <= price_type_index or not csv_data[price_type_index]:
            return []
        
        price_history = csv_data[price_type_index]
        parsed_data = []
        
        # Process pairs of [keepa_time, price]
        for i in range(0, len(price_history), 2):
            if i + 1 < len(price_history):
                keepa_time = price_history[i]
                price_cents = price_history[i + 1]
                
                if price_cents != -1:  # -1 means out of stock
                    date = self.keepa_time_to_datetime(keepa_time)
                    price_dollars = price_cents / 100.0
                    parsed_data.append((date, price_dollars))
        
        return parsed_data
    
    def get_all_price_histories(self, product_data: Dict) -> Dict[str, List[Tuple[datetime, float]]]:
        """Extract all available price histories"""
        csv_data = product_data.get('csv', [])
        
        price_types = {
            'amazon': 0,           # Amazon price
            'new': 1,              # Marketplace new
            'used': 2,             # Marketplace used
            'sales_rank': 3,       # Sales rank
            'list_price': 4,       # List price
            'collectible': 5,      # Collectible
            'refurbished': 6,      # Refurbished
            'warehouse': 9,        # Amazon Warehouse
            'new_fba': 10,         # New FBA
            'lightning_deal': 8,   # Lightning deals
        }
        
        histories = {}
        for name, index in price_types.items():
            if name == 'sales_rank':  # Sales rank is not a price
                continue
            history = self.parse_csv_data(csv_data, index)
            if history:
                histories[name] = history
                
        return histories
    
    def save_to_csv(self, asin: str, product_data: Dict, price_histories: Dict) -> str:
        """Save price histories to CSV file"""
        filename = f"{asin}_price_history.csv"
        
        # Collect all unique dates
        all_dates = set()
        for history in price_histories.values():
            for date, _ in history:
                all_dates.add(date)
        
        sorted_dates = sorted(all_dates)
        
        # Create CSV with date and price columns
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['date'] + list(price_histories.keys())
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            
            for date in sorted_dates:
                row = {'date': date.strftime('%Y-%m-%d %H:%M:%S')}
                
                for price_type, history in price_histories.items():
                    # Find price for this date
                    price = None
                    for hist_date, hist_price in history:
                        if hist_date == date:
                            price = hist_price
                            break
                    row[price_type] = price
                
                writer.writerow(row)
        
        return filename
    
    def print_summary(self, asin: str, product_data: Dict, price_histories: Dict):
        """Print summary of extracted data"""
        print(f"\n=== Price History Summary for {asin} ===")
        print(f"Title: {product_data.get('title', 'N/A')}")
        print(f"Brand: {product_data.get('brand', 'N/A')}")
        print(f"Current Amazon Price: ${product_data.get('stats', {}).get('current', [-1])[0] / 100:.2f}" if product_data.get('stats', {}).get('current') else "N/A")
        
        print(f"\nAvailable Price Histories:")
        for price_type, history in price_histories.items():
            if history:
                latest_date, latest_price = history[-1]
                earliest_date, earliest_price = history[0]
                print(f"  {price_type.title()}: {len(history)} data points")
                print(f"    Latest: ${latest_price:.2f} on {latest_date.strftime('%Y-%m-%d')}")
                print(f"    Earliest: ${earliest_price:.2f} on {earliest_date.strftime('%Y-%m-%d')}")

def main():
    try:
        extractor = KeepaExtractor()
        
        # Extract data for specified ASIN
        asin = "B001DIJ48C"
        product_data = extractor.extract_price_history(asin)
        
        # Parse price histories
        price_histories = extractor.get_all_price_histories(product_data)
        
        # Print summary
        extractor.print_summary(asin, product_data, price_histories)
        
        # Save to CSV
        filename = extractor.save_to_csv(asin, product_data, price_histories)
        print(f"\nPrice history saved to: {filename}")
        
        # Print API token usage info
        if 'tokensLeft' in product_data:
            print(f"\nAPI Token Info:")
            print(f"  Tokens left: {product_data.get('tokensLeft', 'N/A')}")
            print(f"  Tokens consumed: {product_data.get('tokensConsumed', 'N/A')}")
            
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())