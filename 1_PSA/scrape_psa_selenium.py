#!/usr/bin/env python3
"""
PSA Charizard Auction Prices Scraper using Selenium

Scrapes PSA auction price data for Charizard cards from pages 1-19
with proper rate limiting (5+ seconds between requests).
"""

import time
import random
import logging
import pandas as pd
from datetime import datetime
import sys
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
# ChromeDriver is now installed system-wide
from bs4 import BeautifulSoup

class PSAScraperSelenium:
    def __init__(self):
        self.base_url = "https://www.psacard.com/auctionprices/search"
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('psa_scraper_selenium.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        self.all_data = []
        self.driver = None
    
    def setup_driver(self):
        """Setup Chrome WebDriver with realistic settings"""
        try:
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--disable-web-security')
            chrome_options.add_argument('--ignore-certificate-errors')
            chrome_options.add_argument('--ignore-ssl-errors')
            chrome_options.add_argument('--disable-extensions')
            chrome_options.add_argument('--no-first-run')
            chrome_options.add_argument('--disable-default-apps')
            chrome_options.add_argument('--window-size=1920,1080')
            
            # User agent
            chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            
            # Use system-installed ChromeDriver
            service = Service('/usr/local/bin/chromedriver')
            
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.driver.set_page_load_timeout(30)
            self.driver.implicitly_wait(10)
            
            self.logger.info("Chrome WebDriver setup successful")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to setup WebDriver: {e}")
            return False
    
    def get_page_data(self, page_num):
        """Fetch a single page and extract data"""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                url = f"{self.base_url}?q=charizard&page={page_num}"
                self.logger.info(f"Fetching page {page_num} (attempt {attempt + 1}): {url}")
                
                # Navigate to page
                self.driver.get(url)
                
                # Wait a bit for page to fully load
                time.sleep(3)
                
                # Try to find table or wait for it to load
                try:
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.TAG_NAME, "table"))
                    )
                except TimeoutException:
                    self.logger.warning(f"Page {page_num}: No table found within timeout, trying to extract anyway")
                
                # Get page source
                page_source = self.driver.page_source
                
                # Check page title to confirm we're on the right page
                page_title = self.driver.title
                self.logger.info(f"Page {page_num}: Title = '{page_title}'")
                
                # Check if we got blocked (more specific check)
                if "access denied" in page_title.lower() or "error" in page_title.lower():
                    self.logger.warning(f"Page {page_num}: Access denied based on title")
                    if attempt < max_retries - 1:
                        wait_time = 15 + random.uniform(5, 15)
                        self.logger.info(f"Waiting {wait_time:.1f} seconds before retry...")
                        time.sleep(wait_time)
                        continue
                
                # Extract data from page source
                data = self.extract_table_data_from_html(page_source, page_num)
                
                if data:
                    self.logger.info(f"Successfully scraped page {page_num}: {len(data)} records")
                    return data
                else:
                    self.logger.warning(f"Page {page_num}: No data extracted")
                    
            except WebDriverException as e:
                self.logger.error(f"WebDriver error on page {page_num} attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(5 + random.uniform(2, 8))
            except Exception as e:
                self.logger.error(f"Page {page_num} attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(5 + random.uniform(2, 8))
        
        self.logger.error(f"Failed to fetch page {page_num} after {max_retries} attempts")
        return []
    
    def extract_table_data_from_html(self, html_content, page_num):
        """Extract auction data from HTML content using BeautifulSoup"""
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
                # Try to find card-like structures
                cards = soup.find_all(['div', 'article'], class_=lambda x: x and ('card' in str(x).lower() or 'item' in str(x).lower() or 'result' in str(x).lower()))
                if cards:
                    self.logger.info(f"Found {len(cards)} card-like elements")
                    return self.extract_card_data(cards, page_num)
                
                # Last resort: look for any structured data
                structured_data = soup.find_all(['div', 'span', 'p'], string=lambda text: text and ('$' in text or 'price' in text.lower()))
                if structured_data:
                    self.logger.info(f"Found {len(structured_data)} price-related elements")
                    return [{'text': elem.get_text().strip(), 'page': page_num, 'scraped_at': datetime.now().isoformat()} for elem in structured_data[:20]]
                
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
        
        if headers:
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
                'card_class': card.get('class', []),
                'card_index': i,
                'page': page_num,
                'scraped_at': datetime.now().isoformat()
            }
            data.append(card_data)
        return data
    
    def scrape_all_pages(self, start_page=1, end_page=19):
        """Scrape all pages from start_page to end_page"""
        self.logger.info(f"Starting scrape from page {start_page} to {end_page}")
        
        if not self.setup_driver():
            self.logger.error("Failed to setup WebDriver, aborting")
            return []
        
        try:
            for page in range(start_page, end_page + 1):
                # Rate limiting: 5-7 seconds between requests
                if page > start_page:
                    delay = random.uniform(5, 7)
                    self.logger.info(f"Waiting {delay:.1f} seconds before next request...")
                    time.sleep(delay)
                
                page_data = self.get_page_data(page)
                if page_data:
                    self.all_data.extend(page_data)
                    self.logger.info(f"Page {page}: Added {len(page_data)} records")
                
                # Save intermediate results every 5 pages
                if page % 5 == 0:
                    self.save_to_csv(f"psa_charizard_selenium_intermediate_page_{page}.csv")
            
        finally:
            self.cleanup()
        
        self.logger.info(f"Scraping completed. Total records: {len(self.all_data)}")
        return self.all_data
    
    def cleanup(self):
        """Clean up WebDriver resources"""
        try:
            if self.driver:
                self.driver.quit()
                self.logger.info("WebDriver cleanup completed")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
    
    def save_to_csv(self, filename="psa_charizard_selenium_results.csv"):
        """Save scraped data to CSV file"""
        if not self.all_data:
            self.logger.warning("No data to save")
            return None
        
        df = pd.DataFrame(self.all_data)
        df.to_csv(filename, index=False)
        self.logger.info(f"Saved {len(self.all_data)} records to {filename}")
        self.logger.info(f"Columns: {list(df.columns)}")
        return df

def main():
    """Main execution function"""
    scraper = PSAScraperSelenium()
    
    try:
        # Test with first page
        print("Testing with page 1 first...")
        if not scraper.setup_driver():
            print("Failed to setup WebDriver")
            return
        
        test_data = scraper.get_page_data(1)
        scraper.cleanup()
        
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
        scraper = PSAScraperSelenium()  # Fresh instance
        all_data = scraper.scrape_all_pages(1, 19)
        
        # Save final results
        if all_data:
            df = scraper.save_to_csv("psa_charizard_selenium_final.csv")
            print(f"\n=== SCRAPING COMPLETE ===")
            print(f"Total records scraped: {len(all_data)}")
            print(f"Data saved to: psa_charizard_selenium_final.csv")
            if df is not None:
                print(f"Columns: {list(df.columns)}")
                if 'page' in df.columns:
                    print(f"Pages scraped: {df['page'].nunique()}")
        else:
            print("No data was scraped")
            
    except KeyboardInterrupt:
        print("\nScraping interrupted by user")
        scraper.cleanup()
        if scraper.all_data:
            scraper.save_to_csv("psa_charizard_selenium_partial.csv")
            print(f"Partial results saved: {len(scraper.all_data)} records")
    except Exception as e:
        print(f"Error during scraping: {e}")
        scraper.cleanup()
        if scraper.all_data:
            scraper.save_to_csv("psa_charizard_selenium_backup.csv")
            print(f"Backup saved: {len(scraper.all_data)} records")

if __name__ == "__main__":
    main()