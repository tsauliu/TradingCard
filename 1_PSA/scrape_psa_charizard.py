#!/usr/bin/env python3
"""
PSA Charizard Auction Prices Scraper

Scrapes PSA auction price data for Charizard cards from pages 1-19
with proper rate limiting (5+ seconds between requests).
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import logging
from datetime import datetime
import os
import sys

class PSAScraper:
    def __init__(self):
        self.base_url = "https://www.psacard.com/auctionprices/search"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.session.verify = False  # Disable SSL verification for this site
        
        # Disable SSL warnings
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('psa_scraper.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        self.all_data = []
    
    def get_page(self, page_num):
        """Fetch a single page with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                params = {
                    'q': 'charizard',
                    'page': str(page_num)
                }
                
                self.logger.info(f"Fetching page {page_num} (attempt {attempt + 1})")
                response = self.session.get(self.base_url, params=params, timeout=30)
                
                if response.status_code == 200:
                    self.logger.info(f"Successfully fetched page {page_num}")
                    return response.text
                elif response.status_code == 403:
                    self.logger.warning(f"Page {page_num}: Access forbidden (403). Waiting longer...")
                    time.sleep(10 + random.uniform(5, 10))
                else:
                    self.logger.warning(f"Page {page_num}: HTTP {response.status_code}")
                    
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Page {page_num} attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(5 + random.uniform(2, 8))
        
        self.logger.error(f"Failed to fetch page {page_num} after {max_retries} attempts")
        return None
    
    def extract_table_data(self, html_content, page_num):
        """Extract auction data from HTML content"""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Look for common table selectors
        table = None
        possible_selectors = [
            'table.auction-results',
            'table.search-results',
            'table[class*="auction"]',
            'table[class*="results"]',
            '.auction-table table',
            '.results-table table',
            'table'
        ]
        
        for selector in possible_selectors:
            table = soup.select_one(selector)
            if table:
                self.logger.info(f"Found table using selector: {selector}")
                break
        
        if not table:
            # Try to find any table with auction-like data
            tables = soup.find_all('table')
            if tables:
                # Use the largest table (most likely to contain the data)
                table = max(tables, key=lambda t: len(t.find_all('tr')))
                self.logger.info(f"Using largest table with {len(table.find_all('tr'))} rows")
            else:
                self.logger.warning(f"No table found on page {page_num}")
                return []
        
        if not table:
            return []
        
        # Extract headers
        header_row = table.find('thead')
        if header_row:
            headers = [th.get_text().strip() for th in header_row.find_all(['th', 'td'])]
        else:
            # Try first row as headers
            first_row = table.find('tr')
            if first_row:
                headers = [th.get_text().strip() for th in first_row.find_all(['th', 'td'])]
            else:
                headers = []
        
        self.logger.info(f"Found headers: {headers}")
        
        # Extract data rows
        rows = []
        tbody = table.find('tbody')
        if tbody:
            data_rows = tbody.find_all('tr')
        else:
            data_rows = table.find_all('tr')[1:]  # Skip header row
        
        for row in data_rows:
            cells = row.find_all(['td', 'th'])
            if cells:
                row_data = [cell.get_text().strip() for cell in cells]
                if row_data and any(cell for cell in row_data):  # Skip empty rows
                    rows.append(row_data)
        
        self.logger.info(f"Extracted {len(rows)} data rows from page {page_num}")
        
        # Convert to dictionaries
        data = []
        for row in rows:
            if headers and len(row) >= len(headers):
                row_dict = dict(zip(headers, row[:len(headers)]))
                row_dict['page'] = page_num
                row_dict['scraped_at'] = datetime.now().isoformat()
                data.append(row_dict)
            elif row:
                # Fallback: use generic column names
                row_dict = {f'column_{i}': cell for i, cell in enumerate(row)}
                row_dict['page'] = page_num
                row_dict['scraped_at'] = datetime.now().isoformat()
                data.append(row_dict)
        
        return data
    
    def scrape_all_pages(self, start_page=1, end_page=19):
        """Scrape all pages from start_page to end_page"""
        self.logger.info(f"Starting scrape from page {start_page} to {end_page}")
        
        for page in range(start_page, end_page + 1):
            # Rate limiting: 5-7 seconds between requests
            if page > start_page:
                delay = random.uniform(5, 7)
                self.logger.info(f"Waiting {delay:.1f} seconds before next request...")
                time.sleep(delay)
            
            html_content = self.get_page(page)
            if html_content:
                page_data = self.extract_table_data(html_content, page)
                self.all_data.extend(page_data)
                self.logger.info(f"Page {page}: Added {len(page_data)} records")
            else:
                self.logger.error(f"Failed to get page {page}, continuing...")
                continue
            
            # Save intermediate results every 5 pages
            if page % 5 == 0:
                self.save_to_csv(f"psa_charizard_intermediate_page_{page}.csv")
        
        self.logger.info(f"Scraping completed. Total records: {len(self.all_data)}")
        return self.all_data
    
    def save_to_csv(self, filename="psa_charizard_auction_prices.csv"):
        """Save scraped data to CSV file"""
        if not self.all_data:
            self.logger.warning("No data to save")
            return
        
        df = pd.DataFrame(self.all_data)
        df.to_csv(filename, index=False)
        self.logger.info(f"Saved {len(self.all_data)} records to {filename}")
        self.logger.info(f"Columns: {list(df.columns)}")
        return df

def main():
    """Main execution function"""
    scraper = PSAScraper()
    
    try:
        # Test with first page to verify structure
        print("Testing with page 1 first...")
        test_data = []
        html = scraper.get_page(1)
        if html:
            test_data = scraper.extract_table_data(html, 1)
            if test_data:
                print(f"Test successful: Found {len(test_data)} records")
                print("Sample record:", test_data[0] if test_data else "None")
                
                # Ask user to continue
                response = input("Continue with full scrape (pages 1-19)? (y/n): ").lower()
                if response != 'y':
                    print("Scraping cancelled")
                    return
            else:
                print("No data found in test page. Check the website structure.")
                return
        else:
            print("Failed to fetch test page")
            return
        
        # Full scrape
        all_data = scraper.scrape_all_pages(1, 19)
        
        # Save final results
        if all_data:
            df = scraper.save_to_csv("psa_charizard_auction_prices_final.csv")
            print(f"\n=== SCRAPING COMPLETE ===")
            print(f"Total records scraped: {len(all_data)}")
            print(f"Data saved to: psa_charizard_auction_prices_final.csv")
            print(f"Columns: {list(df.columns)}")
            
            # Show summary stats
            if 'page' in df.columns:
                print(f"Pages scraped: {df['page'].nunique()}")
                print(f"Records per page: {df.groupby('page').size().describe()}")
        else:
            print("No data was scraped")
            
    except KeyboardInterrupt:
        print("\nScraping interrupted by user")
        if scraper.all_data:
            scraper.save_to_csv("psa_charizard_partial_results.csv")
            print(f"Partial results saved: {len(scraper.all_data)} records")
    except Exception as e:
        print(f"Error during scraping: {e}")
        if scraper.all_data:
            scraper.save_to_csv("psa_charizard_error_backup.csv")
            print(f"Backup saved: {len(scraper.all_data)} records")

if __name__ == "__main__":
    main()