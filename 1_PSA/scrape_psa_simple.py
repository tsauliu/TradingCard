#!/usr/bin/env python3
"""
Simple PSA Charizard Scraper - Page by Page

This script scrapes one page at a time to avoid triggering anti-bot measures.
Run it multiple times with different page numbers to get all data.
"""

import time
import random
import sys
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup

def scrape_psa_page(page_num):
    """Scrape a single page of PSA auction data"""
    
    # Chrome options for realistic browsing
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    
    service = Service('/usr/local/bin/chromedriver')
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    try:
        url = f"https://www.psacard.com/auctionprices/search?q=charizard&page={page_num}"
        print(f"Scraping page {page_num}: {url}")
        
        # Navigate to page
        driver.get(url)
        
        # Wait for page to load
        time.sleep(5)
        
        # Check if Cloudflare challenge
        page_title = driver.title
        print(f"Page title: {page_title}")
        
        if "just a moment" in page_title.lower() or "cloudflare" in driver.page_source.lower():
            print("⚠️  Cloudflare challenge detected. Waiting longer...")
            time.sleep(15)
            
            # Check if it resolved
            page_title = driver.title
            if "just a moment" in page_title.lower():
                print("❌ Still blocked by Cloudflare. Try again later or use VPN.")
                return []
        
        # Wait for table to load
        try:
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            print("✅ Table found")
        except TimeoutException:
            print("⚠️  No table found within timeout")
        
        # Get page source and parse
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        
        # Find the main table
        table = soup.find('table')
        if not table:
            print("❌ No table found in page source")
            return []
        
        # Extract data
        data = []
        
        # Get headers
        headers = ['Item', 'Category', 'Auction Results']  # We know these from debug
        print(f"Using headers: {headers}")
        
        # Get all rows except header
        rows = table.find_all('tr')[1:]  # Skip header row
        
        for row in rows:
            cells = row.find_all(['td', 'th'])
            if len(cells) >= len(headers):
                row_data = [cell.get_text().strip() for cell in cells[:len(headers)]]
                if any(cell for cell in row_data):  # Skip empty rows
                    record = dict(zip(headers, row_data))
                    record['page'] = page_num
                    record['scraped_at'] = datetime.now().isoformat()
                    data.append(record)
        
        print(f"✅ Extracted {len(data)} records from page {page_num}")
        
        # Show sample if data found
        if data:
            print("Sample record:")
            print(f"  Item: {data[0]['Item'][:60]}...")
            print(f"  Category: {data[0]['Category']}")
            print(f"  Auction Results: {data[0]['Auction Results']}")
        
        return data
        
    except Exception as e:
        print(f"❌ Error scraping page {page_num}: {e}")
        return []
        
    finally:
        driver.quit()

def save_data(data, filename):
    """Save data to CSV"""
    if not data:
        print("No data to save")
        return
    
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    print(f"✅ Saved {len(data)} records to {filename}")
    return df

def main():
    """Main function"""
    if len(sys.argv) > 1:
        # Single page mode
        try:
            page_num = int(sys.argv[1])
            print(f"Scraping single page: {page_num}")
            
            data = scrape_psa_page(page_num)
            
            if data:
                filename = f"psa_charizard_page_{page_num}.csv"
                save_data(data, filename)
            else:
                print("No data scraped")
                
        except ValueError:
            print("Please provide a valid page number")
            sys.exit(1)
    else:
        # Interactive mode
        print("PSA Charizard Scraper - Simple Mode")
        print("=" * 40)
        
        while True:
            try:
                page_input = input("Enter page number (1-19) or 'q' to quit: ").strip()
                
                if page_input.lower() == 'q':
                    break
                    
                page_num = int(page_input)
                if not 1 <= page_num <= 19:
                    print("Page number must be between 1 and 19")
                    continue
                
                print(f"\n--- Scraping Page {page_num} ---")
                
                # Add delay between requests
                if page_num > 1:
                    delay = random.uniform(8, 12)
                    print(f"Waiting {delay:.1f} seconds to be respectful...")
                    time.sleep(delay)
                
                data = scrape_psa_page(page_num)
                
                if data:
                    filename = f"psa_charizard_page_{page_num}.csv"
                    save_data(data, filename)
                else:
                    print("❌ No data scraped from this page")
                
                print("-" * 40)
                
            except ValueError:
                print("Please enter a valid number or 'q'")
            except KeyboardInterrupt:
                print("\n\nScraping interrupted by user")
                break
            except Exception as e:
                print(f"Error: {e}")

if __name__ == "__main__":
    main()