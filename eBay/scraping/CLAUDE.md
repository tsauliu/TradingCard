# eBay Scraping Project - Claude Code Instructions

## 🎯 Project Overview

Advanced eBay sold item data scraping with Excel pivot tables, automatic resume capability, and intelligent rate limiting.

**Key Features:**
- 🔄 Automatic session persistence and resume
- ⏱️ Enforced 10-second minimum delay between requests
- 📊 Excel pivot table generation (rows=keywords, columns=dates)
- 🛡️ Intelligent error recovery and retry logic
- 📈 Progress tracking with visual progress bars
- 🔐 Session locking to prevent concurrent access

## 📁 Project Structure

```
eBay/scraping/
├── Core Scripts
│   ├── ebay_search.py              # Single keyword search
│   ├── ebay_batch_search.py        # Multi-keyword batch search
│   ├── ebay_excel_utils.py         # Excel pivot table utilities
│   └── ebay_resume_manager.py      # Session persistence & resume
│
├── Configuration
│   ├── ebay_cookies.txt            # eBay authentication cookies
│   ├── requirements.txt            # Python dependencies
│   └── CLAUDE.md                   # This documentation
│
├── Temp Directory (.ebay_temp/)
│   └── session_YYYYMMDD_HHMMSS/
│       ├── state.json              # Session state
│       ├── results/                # Individual search results
│       ├── logs/                   # Session logs
│       ├── checkpoints/            # Periodic backups
│       └── exports/                # Exported data
│
└── Output Files
    ├── *.xlsx                      # Excel pivot tables
    ├── *.json                      # JSON results
    └── *.csv                       # Comparison tables
```

## ⚠️ CRITICAL RULES

### 1. **Rate Limiting (MANDATORY)**
```python
# MINIMUM 10 SECOND DELAY - NON-NEGOTIABLE
min_delay = 10.0  # seconds

# This is enforced in:
# - eBaySearchAPI.__init__(min_delay=10.0)
# - eBaySearchAPI._apply_rate_limit()
# - Adaptive delays increase on errors
```

**Why:** Prevents IP blocking and respects eBay's servers

### 2. **Cookie Management**
```bash
# Cookies expire - check regularly
cat ebay_cookies.txt | head -c 100  # Should show valid cookie string

# Get fresh cookies:
1. Open https://www.ebay.com/sh/research in browser
2. F12 → Network tab → Search something
3. Find /sh/research/api/search request
4. Copy entire 'cookie' header value
5. Save to ebay_cookies.txt
```

### 3. **Proxy Configuration**
```bash
# Default proxy for China mainland
export http_proxy=http://127.0.0.1:20171
export https_proxy=http://127.0.0.1:20171

# Or use in script
--proxy http://127.0.0.1:20171
```

## 🚀 Common Use Cases

### 1. **Simple Search with Excel Output**
```bash
# Single keyword
python ebay_search.py "pokemon cards" --excel --days 365

# Result: pokemon_cards_TIMESTAMP.xlsx with 4 sheets:
# - Prices (weekly averages)
# - Quantities (weekly sales)
# - Statistics (summary)
# - Metadata (search params)
```

### 2. **Batch Search with Resume**
```bash
# Start new batch search
python ebay_batch_search.py --file keywords.txt \
  --min-delay 15 \
  --checkpoint-every 5 \
  --continue-on-error

# If interrupted (Ctrl+C), resume:
python ebay_batch_search.py --resume last --file keywords.txt

# Resume specific session:
python ebay_batch_search.py --resume 20250902_143022_hostname --file keywords.txt
```

### 3. **Generate Pivot Table from Multiple Searches**
```bash
# Create pivot table with all keywords
python ebay_batch_search.py \
  "pokemon cards" "magic the gathering" "yugioh cards" \
  --excel-pivot trading_cards_analysis \
  --time-period monthly \
  --days 365

# Output: trading_cards_analysis.xlsx
# Sheet 1: Prices (keywords × months)
# Sheet 2: Quantities (keywords × months)
```

### 4. **Long-Running Search with Safety**
```bash
# For large keyword lists
python ebay_batch_search.py --file big_list.txt \
  --min-delay 20 \
  --checkpoint-every 5 \
  --continue-on-error \
  --retry-failed \
  --keep-temp

# Features activated:
# - 20s delay (safer for long runs)
# - Checkpoint every 5 searches
# - Continue on errors
# - Retry failed searches
# - Keep temp files for analysis
```

## 📊 Resume System Details

### Session State Structure
```json
{
  "session_id": "20250902_143022_hostname",
  "start_time": "2025-09-02T14:30:22",
  "total_keywords": 100,
  "completed": 45,
  "failed": 3,
  "searches": [
    {
      "keyword": "pokemon cards",
      "status": "success",
      "file": "0001_pokemon_cards.json",
      "duration": 12.5,
      "response_size": 45678
    }
  ],
  "rate_limit": {
    "min_delay": 10.0,
    "adaptive_delay": 15.0,
    "total_requests": 48
  }
}
```

### Session Management Commands
```bash
# List all sessions
python ebay_batch_search.py --list-sessions

# Clean old sessions (older than 7 days)
python ebay_batch_search.py --cleanup-sessions 7

# Manual session inspection
cat .ebay_temp/session_*/state.json | jq '.progress'

# Check failed searches
cat .ebay_temp/session_*/state.json | jq '.searches[] | select(.status=="failed")'
```

## 🔧 Advanced Features

### Adaptive Rate Limiting
```python
# System automatically adjusts delays based on:
- Consecutive errors → Exponential backoff (10s, 15s, 22.5s, 33.75s...)
- Success streak → Gradual reduction (never below 10s)
- Request rate monitoring → Increases if >6 req/min
- Error types → Different strategies for different errors
```

### Error Recovery Strategies
```python
error_strategies = {
    'ConnectionTimeout': 'retry_with_backoff',
    'RateLimitExceeded': 'wait_60s_and_retry',
    'AuthenticationError': 'check_cookies',
    'PageErrorModule': 'normal_continue',  # API quirk
}
```

### Progress Tracking
```bash
# Visual progress bar (if tqdm installed)
Searching: 45%|████████████░░░░░░░| 45/100 [05:23<06:12]

# Without tqdm: text updates
[45/100] Searching: pokemon cards charizard
  → Avg Price: $125.50
  → Total Sold: 15,234
```

## 🐛 Debugging & Troubleshooting

### Common Issues

#### 1. **"PageErrorModule" in response**
```python
# NORMAL - eBay API returns both error and data modules
# Check for MetricsTrendsModule for actual data
# The scripts handle this automatically
```

#### 2. **Cookie expired**
```bash
# Symptoms: All searches fail immediately
# Solution: Get fresh cookies (see Cookie Management above)
```

#### 3. **Rate limit errors**
```bash
# Increase delay
python ebay_batch_search.py --min-delay 30 --continue-on-error

# System will adapt automatically with exponential backoff
```

#### 4. **Session locked**
```bash
# If previous run crashed without cleanup
rm .ebay_temp/session_*/lock

# Or wait 5 seconds (lock timeout)
```

### Enable Debug Logging
```bash
# Verbose mode
python ebay_search.py "test" --verbose

# Check session logs
tail -f .ebay_temp/session_*/logs/session.log
```

## 📈 Excel Pivot Table Format

### Sheet Structure
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
```

### Time Aggregation Options
- **weekly** (default): Raw weekly data from API
- **monthly**: Averaged by month (YYYY-MM)
- **quarterly**: Averaged by quarter (YYYY-Q#)

## 🎯 Best Practices

### 1. **Always Use Resume for Large Batches**
```bash
# Good - can recover from any failure
python ebay_batch_search.py --file keywords.txt --checkpoint-every 10

# Bad - loses all progress if interrupted
python ebay_batch_search.py --file keywords.txt --no-resume
```

### 2. **Test with Small Batches First**
```bash
# Test setup with 3 keywords
python ebay_batch_search.py "test1" "test2" "test3" --min-delay 10

# If successful, run full batch
python ebay_batch_search.py --file keywords.txt
```

### 3. **Monitor Early Results**
```bash
# Watch first few searches for issues
# - Cookie validity
# - Data quality
# - Error patterns
```

### 4. **Use Appropriate Delays**
- **10s**: Minimum, for small batches (<50 keywords)
- **15-20s**: Recommended for medium batches (50-200)
- **30s+**: Large batches or after rate limit errors

## 🧹 Maintenance

### Clean Up Test Files
```bash
# Remove test outputs
rm -f test_*.xlsx test_*.json

# Archive completed sessions
tar -czf ebay_sessions_$(date +%Y%m%d).tar.gz .ebay_temp/completed/
rm -rf .ebay_temp/completed/*

# Remove old temp files
find .ebay_temp -name "session_*" -mtime +7 -exec rm -rf {} \;
```

### Backup Important Data
```bash
# Backup keywords
cp keywords.txt keywords_backup_$(date +%Y%m%d).txt

# Backup cookies (encrypted)
gpg -c ebay_cookies.txt

# Backup session data
rsync -av .ebay_temp/ backup/ebay_temp_$(date +%Y%m%d)/
```

## 🔍 Quick Reference

### Most Common Commands
```bash
# Standard batch search with Excel output
python ebay_batch_search.py \
  --file keywords.txt \
  --excel-pivot analysis \
  --days 365 \
  --min-delay 15 \
  --continue-on-error

# Resume after interruption
python ebay_batch_search.py --resume last --file keywords.txt

# Check progress
python ebay_batch_search.py --list-sessions

# Export current results (even if incomplete)
python ebay_resume_manager.py export 20250902_143022
```

### Command Line Options Reference
```
Key Options:
--resume [ID|last]      Resume from session
--min-delay N           Minimum seconds between requests (≥10)
--checkpoint-every N    Save state every N searches
--continue-on-error     Don't stop on failures
--retry-failed          Retry previously failed searches
--keep-temp            Don't archive session after completion
--excel-pivot NAME      Generate pivot Excel file
--time-period PERIOD    weekly|monthly|quarterly aggregation
```

## 📝 Notes for Future Development

1. **Rate Limit is Sacred**: Never reduce below 10s
2. **Session Files**: JSON format, human-readable for debugging
3. **Interruption Safe**: Saves after EVERY search
4. **Cookie Refresh**: Consider automating with Playwright
5. **Data Validation**: Check for outliers in price/quantity data
6. **API Quirks**: PageErrorModule + MetricsTrendsModule is normal

## 🚨 Emergency Procedures

### If Everything Fails
```bash
# 1. Stop all processes
pkill -f ebay_batch_search.py

# 2. Clear locks
rm -f .ebay_temp/session_*/.lock

# 3. Check cookies
python -c "
import requests
r = requests.get('https://www.ebay.com/sh/research')
print('Cookies OK' if r.status_code == 200 else 'Need new cookies')
"

# 4. Test with single search
python ebay_search.py "test" --verbose

# 5. If still failing, increase delay and get new cookies
python ebay_search.py "test" --min-delay 60 --verbose
```

### Data Recovery
```bash
# Even if session is corrupted, individual results are saved
ls .ebay_temp/session_*/results/*.json

# Manually combine results
python -c "
import json, glob
results = []
for f in glob.glob('.ebay_temp/session_*/results/*.json'):
    with open(f) as file:
        results.append(json.load(file))
with open('recovered_data.json', 'w') as out:
    json.dump(results, out, indent=2)
"
```

## 📞 Contact & Support

For issues or questions:
1. Check logs: `.ebay_temp/session_*/logs/`
2. Review this documentation
3. Test with minimal example
4. Ensure cookies are fresh
5. Verify proxy connectivity

---

**Remember**: This system is designed for reliability over speed. The 10-second delay and session persistence ensure you never lose work and avoid getting blocked. When in doubt, use `--resume last`!