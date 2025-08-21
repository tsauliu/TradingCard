#!/usr/bin/env python3
"""
PSA Charizard Auction Prices Scraper using Playwright

Scrapes PSA auction price data for Charizard cards from pages 1-19
with proper rate limiting (5+ seconds between requests).
"""

import asyncio
import pandas as pd
import time
import random
import logging
from datetime import datetime
import sys
import os
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

class PSAScraperPlaywright:
    def __init__(self):
        self.base_url = "https://www.psacard.com/auctionprices/search"
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('psa_scraper_playwright.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        self.all_data = []
    
    async def setup_browser(self):
        """Setup browser with realistic settings"""
        self.playwright = await async_playwright().start()
        
        # Try to launch browser (may show warnings but should work)
        try:
            self.browser = await self.playwright.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor',
                    '--ignore-certificate-errors',
                    '--ignore-ssl-errors',
                    '--disable-extensions',
                    '--no-first-run'
                ]
            )
            
            self.context = await self.browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080},
                extra_http_headers={
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'DNT': '1',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1'
                }
            )
            
            self.page = await self.context.new_page()
            
            # Set realistic timeouts
            self.page.set_default_timeout(30000)
            self.page.set_default_navigation_timeout(30000)
            
            self.logger.info("Browser setup successful")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to setup browser: {e}")
            return False
    
    async def get_page_data(self, page_num):
        """Fetch a single page and extract data"""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                url = f"{self.base_url}?q=charizard&page={page_num}"
                self.logger.info(f"Fetching page {page_num} (attempt {attempt + 1}): {url}")
                
                # Navigate to page
                response = await self.page.goto(url, wait_until='domcontentloaded')
                
                if response and response.status == 200:
                    # Wait for content to load
                    try:
                        await self.page.wait_for_selector('table, .auction-table, .results-table, [class*="table"]', timeout=10000)
                    except PlaywrightTimeoutError:
                        self.logger.warning(f"Page {page_num}: No table found within timeout")
                    
                    # Get page content
                    content = await self.page.content()
                    data = self.extract_table_data_from_html(content, page_num)
                    
                    self.logger.info(f"Successfully scraped page {page_num}: {len(data)} records")
                    return data
                
                elif response:
                    self.logger.warning(f"Page {page_num}: HTTP {response.status}")
                    if response.status == 403:
                        self.logger.info("Access forbidden, waiting longer...")
                        await asyncio.sleep(15 + random.uniform(5, 10))
                else:
                    self.logger.warning(f"Page {page_num}: No response received")
                    
            except Exception as e:
                self.logger.error(f"Page {page_num} attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(5 + random.uniform(2, 8))
        
        self.logger.error(f"Failed to fetch page {page_num} after {max_retries} attempts")
        return []
    
    def extract_table_data_from_html(self, html_content, page_num):
        """Extract auction data from HTML content using BeautifulSoup"""
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Look for tables
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
            tables = soup.find_all('table')
            if tables:
                table = max(tables, key=lambda t: len(t.find_all('tr')))
                self.logger.info(f"Using largest table with {len(table.find_all('tr'))} rows")
            else:
                self.logger.warning(f"No table found on page {page_num}")
                # Let's also check for any card-like structures
                cards = soup.find_all(['div', 'article'], class_=lambda x: x and ('card' in str(x).lower() or 'item' in str(x).lower() or 'result' in str(x).lower()))
                if cards:
                    self.logger.info(f"Found {len(cards)} card-like elements, trying to extract data")
                    return self.extract_card_data(cards, page_num)
                return []
        
        # Extract headers
        headers = []
        header_row = table.find('thead')
        if header_row:
            headers = [th.get_text().strip() for th in header_row.find_all(['th', 'td'])]
        else:
            first_row = table.find('tr')
            if first_row:
                headers = [th.get_text().strip() for th in first_row.find_all(['th', 'td'])]
        
        self.logger.info(f"Found headers: {headers}")
        
        # Extract data rows
        rows = []
        tbody = table.find('tbody')
        if tbody:
            data_rows = tbody.find_all('tr')
        else:
            data_rows = table.find_all('tr')[1:] if headers else table.find_all('tr')
        
        for row in data_rows:
            cells = row.find_all(['td', 'th'])
            if cells:
                row_data = [cell.get_text().strip() for cell in cells]
                if row_data and any(cell for cell in row_data):
                    rows.append(row_data)
        
        self.logger.info(f"Extracted {len(rows)} data rows from page {page_num}")
        
        # Convert to dictionaries
        data = []
        for row in rows:
            if headers and len(row) >= len(headers):
                row_dict = dict(zip(headers, row[:len(headers)]))
            else:
                row_dict = {f'column_{i}': cell for i, cell in enumerate(row)}
            
            row_dict['page'] = page_num
            row_dict['scraped_at'] = datetime.now().isoformat()
            data.append(row_dict)
        
        return data
    
    def extract_card_data(self, cards, page_num):
        """Extract data from card-like elements when no table is found"""
        data = []
        for i, card in enumerate(cards):
            card_data = {
                'card_text': card.get_text().strip(),
                'card_html': str(card),
                'card_index': i,
                'page': page_num,
                'scraped_at': datetime.now().isoformat()
            }
            data.append(card_data)
        return data
    
    async def scrape_all_pages(self, start_page=1, end_page=19):
        """Scrape all pages from start_page to end_page"""
        self.logger.info(f"Starting scrape from page {start_page} to {end_page}")
        
        if not await self.setup_browser():
            self.logger.error("Failed to setup browser, aborting")
            return []
        
        try:
            for page in range(start_page, end_page + 1):
                # Rate limiting: 5-7 seconds between requests
                if page > start_page:
                    delay = random.uniform(5, 7)
                    self.logger.info(f"Waiting {delay:.1f} seconds before next request...")
                    await asyncio.sleep(delay)
                
                page_data = await self.get_page_data(page)
                if page_data:
                    self.all_data.extend(page_data)
                    self.logger.info(f"Page {page}: Added {len(page_data)} records")
                
                # Save intermediate results every 5 pages
                if page % 5 == 0:
                    self.save_to_csv(f"psa_charizard_intermediate_page_{page}.csv")
            
        finally:
            await self.cleanup()
        
        self.logger.info(f"Scraping completed. Total records: {len(self.all_data)}")
        return self.all_data
    
    async def cleanup(self):
        """Clean up browser resources"""
        try:
            if hasattr(self, 'page'):
                await self.page.close()
            if hasattr(self, 'context'):
                await self.context.close()
            if hasattr(self, 'browser'):
                await self.browser.close()
            if hasattr(self, 'playwright'):
                await self.playwright.stop()
            self.logger.info("Browser cleanup completed")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
    
    def save_to_csv(self, filename="psa_charizard_auction_prices.csv"):
        """Save scraped data to CSV file"""
        if not self.all_data:
            self.logger.warning("No data to save")
            return None
        
        df = pd.DataFrame(self.all_data)
        df.to_csv(filename, index=False)
        self.logger.info(f"Saved {len(self.all_data)} records to {filename}")
        self.logger.info(f"Columns: {list(df.columns)}")
        return df

async def main():
    """Main execution function"""
    scraper = PSAScraperPlaywright()
    
    try:
        # Test with first page
        print("Testing with page 1 first...")
        if not await scraper.setup_browser():
            print("Failed to setup browser")
            return
        
        test_data = await scraper.get_page_data(1)
        await scraper.cleanup()
        
        if test_data:
            print(f"Test successful: Found {len(test_data)} records")
            print("Sample record:", test_data[0] if test_data else "None")
            
            # Ask user to continue
            response = input("Continue with full scrape (pages 1-19)? (y/n): ").lower()
            if response != 'y':
                print("Scraping cancelled")
                return
        else:
            print("No data found in test page. The site may be blocking requests or structure has changed.")
            return
        
        # Full scrape
        scraper = PSAScraperPlaywright()  # Fresh instance
        all_data = await scraper.scrape_all_pages(1, 19)
        
        # Save final results
        if all_data:
            df = scraper.save_to_csv("psa_charizard_auction_prices_final.csv")
            print(f"\n=== SCRAPING COMPLETE ===")
            print(f"Total records scraped: {len(all_data)}")
            print(f"Data saved to: psa_charizard_auction_prices_final.csv")
            if df is not None:
                print(f"Columns: {list(df.columns)}")
                if 'page' in df.columns:
                    print(f"Pages scraped: {df['page'].nunique()}")
        else:
            print("No data was scraped")
            
    except KeyboardInterrupt:
        print("\nScraping interrupted by user")
        await scraper.cleanup()
        if scraper.all_data:
            scraper.save_to_csv("psa_charizard_partial_results.csv")
            print(f"Partial results saved: {len(scraper.all_data)} records")
    except Exception as e:
        print(f"Error during scraping: {e}")
        await scraper.cleanup()
        if scraper.all_data:
            scraper.save_to_csv("psa_charizard_error_backup.csv")
            print(f"Backup saved: {len(scraper.all_data)} records")

if __name__ == "__main__":
    asyncio.run(main())