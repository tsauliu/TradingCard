# PSA Charizard Auction Price Scraper

This directory contains multiple approaches to scrape PSA auction price data for Charizard cards from pages 1-19 with proper rate limiting (5+ seconds between requests).

## Available Scripts

1. **`scrape_psa_selenium.py`** - Selenium-based scraper (Recommended)
2. **`scrape_psa_playwright.py`** - Playwright-based scraper (Alternative)
3. **`scrape_psa_charizard.py`** - Basic requests-based scraper (Fallback)

## Current Status

The PSA website (https://www.psacard.com/auctionprices/) has anti-scraping measures:
- Blocks direct HTTP requests (403 Forbidden)
- Requires browser automation with proper headers and behavior
- May require system dependencies for browser automation tools

## System Requirements

### For Selenium (Recommended approach)
```bash
pip install selenium webdriver-manager pandas beautifulsoup4
```

### For Playwright (Alternative)
```bash
pip install playwright pandas beautifulsoup4
playwright install chromium
# May require system dependencies:
sudo playwright install-deps
```

## Browser Dependencies Issue

Currently experiencing system dependency issues for browser automation. To resolve:

### Option 1: Install system dependencies
```bash
sudo apt-get update
sudo apt-get install -y \
    libnspr4 \
    libnss3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libatspi2.0-0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libcairo2 \
    libpango-1.0-0 \
    libasound2
```

### Option 2: Use Docker (Recommended for production)
Create a Docker container with all dependencies:
```dockerfile
FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    libnspr4 \
    libnss3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libatspi2.0-0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libcairo2 \
    libpango-1.0-0 \
    libasound2 \
    && rm -rf /var/lib/apt/lists/*

RUN pip install selenium playwright pandas beautifulsoup4 webdriver-manager
RUN playwright install chromium
```

## Features

All scripts include:
- **Rate Limiting**: 5-7 second random delays between page requests
- **Error Handling**: Retry logic for failed requests
- **Progress Tracking**: Console output showing current progress
- **Data Storage**: Save results to CSV format
- **Resume Capability**: Intermediate saves every 5 pages
- **Comprehensive Logging**: Detailed logs for debugging

## Expected Data Structure

The scraped data will include:
- Card details (name, year, set, condition)
- Grade information
- Sale price and date
- Auction platform information
- Page number and scrape timestamp

## Usage Instructions

### 1. Test with single page first
```bash
python scrape_psa_selenium.py
# or
python scrape_psa_playwright.py
```

### 2. Review test results
The script will first test page 1 and show sample data before proceeding.

### 3. Full scrape (if test succeeds)
Confirm to proceed with pages 1-19 when prompted.

## Output Files

- `psa_charizard_[method]_final.csv` - Complete results
- `psa_charizard_[method]_intermediate_page_X.csv` - Backup saves every 5 pages
- `psa_scraper_[method].log` - Detailed execution logs

## Troubleshooting

### 403 Forbidden Errors
- The site may be blocking your IP temporarily
- Wait 10-15 minutes and try again
- Consider using a VPN or different network

### System Dependencies
- Follow the dependency installation steps above
- Consider using Docker for a clean environment

### Rate Limiting
- Scripts already include 5-7 second delays
- If blocked, increase delays in the script
- Consider running during off-peak hours

## Legal and Ethical Considerations

- This scraper respects rate limits (5+ seconds between requests)
- Only scrapes publicly available data
- Follow PSA's terms of service
- Use scraped data responsibly
- Consider contacting PSA for official API access

## Alternative Approaches

If browser automation continues to fail:
1. Use a service like ScrapingBee or Crawlera
2. Consider running on a different system with proper dependencies
3. Use a cloud-based scraping solution
4. Contact PSA for official data access methods