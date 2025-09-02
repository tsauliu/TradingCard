# eBay Search API Scripts

Advanced Python scripts for searching eBay sold items with automatic resume capability, Excel pivot tables, and intelligent rate limiting.

## Features

- **Automatic Resume**: Saves progress after every search, resume from exact stopping point
- **Rate Limiting**: Enforced 10-second minimum delay with adaptive adjustments
- **Session Management**: Organized temp folder structure with state persistence
- **Excel Pivot Tables**: Generate pivot tables with prices and quantities by week
- **Progress Tracking**: Visual progress bars with estimated time remaining
- **Error Recovery**: Intelligent retry logic with exponential backoff
- **Batch Processing**: Search multiple keywords with checkpoint saves
- **Cookie Management**: Handle authentication via cookies
- **Time Aggregation**: Weekly, monthly, or quarterly data aggregation
- **Concurrent Protection**: Session locking prevents data corruption

## Installation

```bash
pip install -r requirements.txt
```

## Setup

1. **Get eBay Cookies** (Required for full data access):
   - Open eBay Research page in browser: https://www.ebay.com/sh/research
   - Open Developer Tools (F12)
   - Go to Network tab
   - Search for something on the page
   - Find a request to `/sh/research/api/search`
   - Copy the entire `cookie` header value
   - Save to `ebay_cookies.txt`

## Usage

### Single Search

Basic search (JSON output):
```bash
python ebay_search.py "pokemon cards"
```

Excel pivot table output:
```bash
python ebay_search.py "pokemon cards" --excel --output pokemon.xlsx
python ebay_search.py "magic cards" --excel --no-charts  # Without charts
```

With options:
```bash
python ebay_search.py "magic the gathering" \
  --days 365 \
  --limit 100 \
  --extract-metrics \
  --output mtg_data.json
```

Search without proxy:
```bash
python ebay_search.py "vintage electronics" --no-proxy
```

### Batch Search with Resume

Start new search with automatic resume capability:
```bash
# Start new batch (automatically saves to .ebay_temp/)
python ebay_batch_search.py --file keywords.txt --min-delay 15 --checkpoint-every 5

# If interrupted (Ctrl+C), resume from last position:
python ebay_batch_search.py --resume last --file keywords.txt

# Resume specific session:
python ebay_batch_search.py --resume 20250902_143022_hostname --file keywords.txt

# Retry failed searches:
python ebay_batch_search.py --resume last --retry-failed
```

Session management:
```bash
# List all sessions
python ebay_batch_search.py --list-sessions

# Clean old sessions (>7 days)
python ebay_batch_search.py --cleanup-sessions 7
```

Generate Excel pivot table:
```bash
# Create pivot table with all searches
python ebay_batch_search.py "pokemon" "magic" "yugioh" --excel-pivot trading_cards

# Monthly aggregation
python ebay_batch_search.py --file keywords.txt --excel-pivot analysis --time-period monthly

# Quarterly without charts
python ebay_batch_search.py "pokemon" "magic" --excel-pivot quarterly_report --time-period quarterly --no-charts
```

From file (one keyword per line):
```bash
python ebay_batch_search.py --file keywords.txt --days 365 --continue-on-error
```

With custom output directory:
```bash
python ebay_batch_search.py --file cards.txt --output-dir results/ --min-delay 20
```

## Command Line Options

### ebay_search.py

| Option | Description | Default |
|--------|-------------|---------|
| `keywords` | Search keywords (required) | - |
| `--output, -o` | Output JSON filename | Auto-generated |
| `--days, -d` | Number of days to look back | 1095 (3 years) |
| `--marketplace, -m` | eBay marketplace | EBAY-US |
| `--category, -c` | Category ID (0 for all) | 0 |
| `--offset` | Result offset for pagination | 0 |
| `--limit, -l` | Results per page (max 200) | 50 |
| `--proxy, -p` | Proxy URL | http://127.0.0.1:20171 |
| `--no-proxy` | Disable proxy | False |
| `--cookie-file` | Cookie file path | ebay_cookies.txt |
| `--extract-metrics` | Extract and display metrics | False |
| `--compact` | Save JSON in compact format | False |
| `--excel` | Save as Excel pivot table | False |
| `--no-charts` | Disable charts in Excel | False |
| `--verbose, -v` | Enable verbose logging | False |

### ebay_batch_search.py

| Option | Description | Default |
|--------|-------------|---------|
| `keywords` | Search keywords (multiple) | - |
| `--file, -f` | File with keywords | - |
| `--days, -d` | Number of days to look back | 1095 |
| `--marketplace, -m` | eBay marketplace | EBAY-US |
| `--limit, -l` | Results per search | 50 |
| `--min-delay` | Minimum delay between requests (≥10s) | 10.0 |
| **Resume Options** | | |
| `--resume` | Resume session (ID or "last") | None |
| `--temp-dir` | Temp directory for sessions | .ebay_temp |
| `--keep-temp` | Keep temp files after completion | False |
| `--checkpoint-every` | Save checkpoint every N searches | 10 |
| `--retry-failed` | Retry previously failed searches | False |
| `--continue-on-error` | Continue on failures | False |
| `--list-sessions` | List all sessions and exit | False |
| `--cleanup-sessions` | Clean sessions older than N days | None |
| **Output Options** | | |
| `--output-dir, -o` | Output directory | . |
| `--no-report` | Skip report generation | False |
| `--excel-pivot` | Generate pivot Excel with name | None |
| `--time-period` | Aggregation (weekly/monthly/quarterly) | weekly |
| `--no-charts` | Disable charts in Excel | False |
| **Connection Options** | | |
| `--proxy, -p` | Proxy URL | http://127.0.0.1:20171 |
| `--no-proxy` | Disable proxy | False |
| `--cookie-file` | Cookie file path | ebay_cookies.txt |

## Output Files

### Single Search
- **JSON File**: Complete API response with extracted metrics
  ```json
  {
    "PageErrorModule": {...},
    "MetricsTrendsModule": {...},
    "extracted_metrics": {
      "statistics": {
        "avg_price": 33.34,
        "total_sold": 12221985,
        ...
      }
    },
    "search_metadata": {...}
  }
  ```

- **Excel File** (with --excel flag): Pivot table format with 4 sheets:
  - **Prices**: Weekly average prices (columns are dates)
  - **Quantities**: Weekly sales volumes (columns are dates)
  - **Statistics**: Summary statistics for the search
  - **Metadata**: Search parameters and metadata

### Batch Search
- **batch_results_*.json**: Raw results for all searches
- **comparison_*.csv**: Comparison table
- **comparison_*.xlsx**: Excel report with formatting
- **summary_*.txt**: Text summary of all searches
- **pivot_table_*.xlsx** (or custom name): Multi-keyword pivot table with:
  - **Sheet 1 - Prices**: Rows are keywords, columns are weeks
  - **Sheet 2 - Quantities**: Rows are keywords, columns are weeks
  - **Sheet 3 - Statistics**: Summary stats for all keywords
  - **Sheet 4 - Metadata**: Search parameters

### Excel Pivot Table Structure
```
Sheet 1 - Prices:
Keywords         | 2024-08-05 | 2024-08-12 | ... | 2025-08-26 |
pokemon cards    |     45.60  |     48.25  | ... |     58.64  |
magic gathering  |     32.30  |     34.80  | ... |     35.12  |
yugioh cards     |     24.75  |     26.20  | ... |     28.12  |

Sheet 2 - Quantities:
Keywords         | 2024-08-05 | 2024-08-12 | ... | 2025-08-26 |
pokemon cards    |     18234  |     19567  | ... |     19682  |
magic gathering  |     12456  |     13234  | ... |     14096  |
yugioh cards     |       856  |       923  | ... |       732  |
```

## API Response Structure

The eBay API returns multiple modules:
- **PageErrorModule**: Error information (if any)
- **MetricsTrendsModule**: Price and sales data over time
  - `averageSold`: Weekly average prices
  - `quantity`: Weekly sales volumes
  - `quantityRegressionLine`: Trend line

## Examples

### Example 1: Search Trading Cards
```bash
# Search Pokemon cards for last year
python ebay_search.py "pokemon cards charizard" --days 365 --extract-metrics

# Output shows:
# Average Price: $41.74
# Total Sold: 6,042,856
# Data saved to: ebay_search_pokemon_cards_charizard_20250902_101500.json
```

### Example 2: Compare Multiple Products
```bash
# Create keywords file
echo "pokemon cards booster box" > keywords.txt
echo "magic the gathering booster box" >> keywords.txt
echo "yugioh booster box" >> keywords.txt

# Run batch search
python ebay_batch_search.py --file keywords.txt --days 90

# Creates comparison table showing price differences
```

### Example 3: Track Price Trends
```bash
# Get 3 years of data with metrics
python ebay_search.py "vintage rolex submariner" \
  --days 1095 \
  --extract-metrics \
  --output rolex_trends.json

# Extracts weekly price points for trend analysis
```

## Troubleshooting

### "auth_required" Error
- You need valid cookies from eBay
- Cookies may have expired - get fresh ones

### PageErrorModule in Response
- This is normal - the API returns both error and data modules
- Check if MetricsTrendsModule is present for actual data

### Proxy Connection Issues
- Check if proxy is running: `curl -x http://127.0.0.1:20171 http://example.com`
- Use `--no-proxy` to bypass proxy

### No Metrics Data
- Some searches may not have enough sales data
- Try broader search terms or longer time periods

## Advanced Usage

### Programmatic Usage
```python
from ebay_search import eBaySearchAPI

# Create API client
api = eBaySearchAPI(proxy="http://127.0.0.1:20171")

# Search
results = api.search(
    keywords="rare pokemon cards",
    days=365,
    limit=100
)

# Extract metrics
metrics = api.extract_metrics(results)
print(f"Average price: ${metrics['statistics']['avg_price']:.2f}")
```

### Custom Analysis
```python
import pandas as pd
import json

# Load results
with open("pokemon_test.json") as f:
    data = json.load(f)

# Convert to DataFrame
metrics = data['extracted_metrics']
df = pd.DataFrame(metrics['avg_prices'])
df['date'] = pd.to_datetime(df['date'])
df.set_index('date', inplace=True)

# Analyze trends
print(df['price'].resample('M').mean())  # Monthly averages
```

## Notes

- The API returns weekly aggregated data, not individual listings
- Maximum lookback period is typically 3 years
- Rate limiting: Use delays between requests in batch mode
- Cookies expire periodically and need refreshing
- Some marketplaces may have different data availability

## Excel Pivot Table Features

### Time Period Aggregation
The scripts support different time period aggregations:
- **Weekly** (default): Raw weekly data as returned by API
- **Monthly**: Averages grouped by month (YYYY-MM format)
- **Quarterly**: Averages grouped by quarter (YYYY-Q# format)

### Formatting and Styling
Excel files include:
- Color-coded headers and keywords column
- Number formatting (currency for prices, thousands separator for quantities)
- Auto-adjusted column widths
- Borders for better readability
- Conditional formatting for high/low values

### Charts (optional)
When charts are enabled (default), the Excel includes:
- Price trend line charts
- Sales volume trend charts
- Each keyword as a separate series
- Proper axis labels and titles

### Use Cases
1. **Price Comparison**: Compare prices across different products over time
2. **Trend Analysis**: Identify seasonal patterns and trends
3. **Market Research**: Analyze market size and competition
4. **Investment Analysis**: Track collectible card values over time
5. **Reporting**: Generate professional reports with charts

## Resume System

The resume system automatically saves progress and allows recovery from any interruption:

### How It Works
1. **Automatic State Saving**: After every search, the state is saved to `.ebay_temp/session_*/state.json`
2. **Individual Result Files**: Each search result is saved separately in `.ebay_temp/session_*/results/`
3. **Checkpoint Backups**: Periodic checkpoints saved every N searches (configurable)
4. **Session Locking**: Prevents concurrent access to the same session
5. **Intelligent Recovery**: On resume, skips completed searches and continues from exact stopping point

### Session Structure
```
.ebay_temp/
└── session_20250902_143022_hostname/
    ├── state.json              # Current session state
    ├── results/                # Individual search results
    │   ├── 0001_pokemon_cards.json
    │   ├── 0002_magic_gathering.json
    │   └── 0003_yugioh_cards.json
    ├── logs/                   # Session logs
    │   ├── session.log
    │   └── errors.log
    ├── checkpoints/            # Periodic state backups
    │   └── checkpoint_20250902_144530.json
    └── exports/                # Exported data
```

### Rate Limiting Details
- **Minimum Delay**: 10 seconds (enforced, cannot be reduced)
- **Adaptive Delays**: Automatically increases on errors (10s → 15s → 22.5s → 33.75s...)
- **Success Recovery**: Gradually reduces delay after consistent successes (never below 10s)
- **Request Monitoring**: Increases delay if request rate exceeds 6/minute

## Files

- `ebay_search.py` - Main search script with Excel support and rate limiting
- `ebay_batch_search.py` - Batch search with resume capability
- `ebay_resume_manager.py` - Session persistence and recovery system
- `ebay_excel_utils.py` - Excel pivot table utilities
- `requirements.txt` - Python dependencies
- `CLAUDE.md` - Comprehensive Claude instructions
- `README_EBAY_SEARCH.md` - This documentation