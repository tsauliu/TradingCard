# eBay Scraping - Minimal Version

## Quick Start

```python
from ebay_simple_batch import batch_search, jsons_to_excel

# Search multiple keywords (60s delay between each)
keywords = ['pokemon cards', 'magic the gathering', 'yugioh cards']
batch_search(keywords, days=1095)  # 3 years of weekly data

# Convert to Excel pivot table
jsons_to_excel()  # Creates ebay_pivot.xlsx
```

## Two Functions Only

### `batch_search(keywords_list, cookie_file='ebay_cookies.txt', output_dir='raw_jsons', days=1095)`
- Searches eBay sold items for each keyword
- Saves raw JSON responses to `raw_jsons/` directory
- 60 second delay between requests (hardcoded)
- Stops if cookies expired

### `jsons_to_excel(json_dir='raw_jsons', output_file='ebay_pivot.xlsx')`
- Reads all JSON files from directory
- Creates pivot table: keywords as rows, dates as columns
- Two sheets: Prices and Quantities

## Cookie Setup

1. Open https://www.ebay.com/sh/research in browser
2. F12 → Network tab → Search something
3. Find `/sh/research/api/search` request
4. Copy entire 'cookie' header value
5. Save to `ebay_cookies.txt`

## Notes

- API returns WEEK granularity for requests > 365 days
- API returns DAY granularity for requests ≤ 365 days
- No logging, no retry, no resume - just raw functionality
- Total code: 142 lines in `ebay_simple_batch.py`