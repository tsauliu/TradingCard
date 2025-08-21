#!/usr/bin/env python3
"""
Debug script to check what content we're getting from PSA
"""

import time
import random
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup

def debug_psa_page():
    """Debug what we're actually getting from the PSA page"""
    
    # Setup Chrome options
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
        url = "https://www.psacard.com/auctionprices/search?q=charizard&page=1"
        print(f"Navigating to: {url}")
        
        driver.get(url)
        time.sleep(5)  # Wait for page to load
        
        # Get page source
        page_source = driver.page_source
        
        # Save to file for inspection
        with open('psa_page_debug.html', 'w', encoding='utf-8') as f:
            f.write(page_source)
        
        print(f"Page title: {driver.title}")
        print(f"Current URL: {driver.current_url}")
        print(f"Page source length: {len(page_source)} characters")
        
        # Check for common blocking indicators
        blocking_indicators = [
            "access denied", "blocked", "forbidden", "503", "502", "404",
            "cloudflare", "captcha", "security check", "rate limit",
            "please try again", "temporarily unavailable"
        ]
        
        page_lower = page_source.lower()
        found_indicators = [indicator for indicator in blocking_indicators if indicator in page_lower]
        
        if found_indicators:
            print(f"Blocking indicators found: {found_indicators}")
        else:
            print("No obvious blocking indicators found")
        
        # Look for tables
        soup = BeautifulSoup(page_source, 'html.parser')
        tables = soup.find_all('table')
        print(f"Tables found: {len(tables)}")
        
        if tables:
            for i, table in enumerate(tables[:3]):  # Show first 3 tables
                rows = table.find_all('tr')
                print(f"Table {i+1}: {len(rows)} rows")
                if rows:
                    first_row = rows[0]
                    cells = first_row.find_all(['th', 'td'])
                    print(f"  First row: {[cell.get_text().strip() for cell in cells][:5]}")  # First 5 cells
        
        # Look for any auction-related content
        auction_elements = soup.find_all(text=lambda text: text and ('auction' in text.lower() or 'price' in text.lower() or 'charizard' in text.lower()))
        if auction_elements:
            print(f"Found {len(auction_elements)} auction-related text elements")
            for elem in auction_elements[:5]:  # First 5
                print(f"  - {elem.strip()[:100]}")
        
        # Look for specific PSA elements
        psa_elements = soup.find_all(['div', 'span', 'p'], class_=lambda x: x and ('psa' in str(x).lower() or 'card' in str(x).lower()))
        print(f"PSA/card elements found: {len(psa_elements)}")
        
        # Check for JavaScript requirements
        scripts = soup.find_all('script')
        print(f"Script tags found: {len(scripts)}")
        
        # Save a snippet of the content
        print("\n=== First 1000 characters of page content ===")
        visible_text = soup.get_text()[:1000]
        print(visible_text)
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        driver.quit()

if __name__ == "__main__":
    debug_psa_page()