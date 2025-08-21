# PSA Scraper Ubuntu Screen Setup - Complete Guide

## 🚀 Quick Start Commands

### Test Run (Dry Run)
```bash
./run_psa_scraper.sh --dry-run
```

### Production Run
```bash
./run_psa_scraper.sh
```

### Monitor Progress
```bash
./monitor_psa_scraper.sh
```

### Stop Scraper
```bash
./kill_psa_scraper.sh
```

## 📋 Management Scripts

### 1. `run_psa_scraper.sh` - Main Launcher
- Creates screen session named "psa_scraper"
- Sets up comprehensive logging
- Handles PID tracking and process management
- Supports both dry-run and production modes

**Features:**
- ✅ Automatic screen installation check
- ✅ Prevents duplicate instances
- ✅ Comprehensive error logging
- ✅ Process ID tracking
- ✅ Graceful error handling

### 2. `monitor_psa_scraper.sh` - Real-time Monitor
- Live status dashboard with auto-refresh
- Shows CPU/memory usage
- Displays recent progress and errors
- Interactive commands for log viewing

**Monitor Commands:**
- `q` - Quit monitor
- `l` - View live logs
- `e` - View error logs  
- `s` - Attach to screen session
- `k` - Kill scraper

### 3. `kill_psa_scraper.sh` - Safe Termination
- Gracefully terminates scraper process
- Cleans up PID files and screen sessions
- Handles both running and stuck processes

### 4. `cleanup_logs.sh` - Log Management
- Removes logs older than 30 days
- Compresses logs larger than 100MB
- Shows disk space savings

## 📁 Log Structure

```
logs/
├── psa_production_YYYYMMDD_HHMMSS.log     # Main execution log
├── psa_production_errors_YYYYMMDD_HHMMSS.log  # Error-only log
├── psa_monitor_YYYYMMDD_HHMMSS.log        # Monitor session log
└── psa_scraper.pid                        # Process ID file
```

## 🔧 Screen Session Management

### Basic Commands
```bash
# List all screen sessions
screen -list

# Attach to PSA scraper session
screen -r psa_scraper

# Detach from session (while attached)
Ctrl+A, then D

# Kill session
screen -S psa_scraper -X quit
```

### Advanced Usage
```bash
# Create new window in session
Ctrl+A, then C

# Switch between windows
Ctrl+A, then N (next) or P (previous)

# View session logs
Ctrl+A, then H
```

## 📊 Expected Behavior

### Production Run Timeline
1. **Initialization** (0-30s)
   - BigQuery connection check
   - Duplicate detection scan
   - Card list loading

2. **Processing Phase** (~110 minutes)
   - 10 cards × 19 grades = 190 API calls
   - 35-second delays between requests
   - Checkpoint saves every 2 cards

3. **Completion**
   - Final BigQuery load
   - Summary statistics
   - Log archival

### Log Patterns to Watch
```bash
# Success indicators
✅ Success: ID 544027, Grade 10 - 1234 sales
🎯 Completed scraping for Charizard-Holo: 1500 total records
✅ Loaded 1500 records to BigQuery

# Progress tracking
🎴 Processing card 3/10: Machamp-Holo 1st Edition
📊 Processing PSA 10 (1/19)
📈 Progress: 3/10 cards | ETA: 14:25:30

# Error patterns
❌ Failed to fetch data for ID 544027, Grade 10
⚠️ No data found for PSA 8.5 (404)
🚨 BigQuery load failed: Connection timeout
```

## 🚨 Troubleshooting

### Scraper Won't Start
```bash
# Check for existing processes
ps aux | grep scrape_psa_production
screen -list

# Clean up manually
./kill_psa_scraper.sh
rm -f logs/psa_scraper.pid
```

### Screen Session Issues
```bash
# If screen not found
sudo apt update && sudo apt install screen

# If session exists but not responding
screen -S psa_scraper -X quit
./run_psa_scraper.sh
```

### Log File Problems
```bash
# Create logs directory
mkdir -p logs

# Check disk space
df -h .

# Clean up logs
./cleanup_logs.sh
```

### BigQuery Connection Issues
```bash
# Verify credentials
ls -la service-account.json
cat .env | grep GOOGLE

# Test connection
python -c "from google.cloud import bigquery; client = bigquery.Client(); print('Connection OK')"
```

## 💡 Best Practices

### Before Starting
1. ✅ Verify BigQuery credentials work
2. ✅ Check available disk space (>1GB recommended)
3. ✅ Test with dry-run first
4. ✅ Ensure stable internet connection

### During Execution
1. ✅ Monitor progress with `./monitor_psa_scraper.sh`
2. ✅ Don't manually kill screen session
3. ✅ Check error logs if progress stalls
4. ✅ Keep terminal session active

### After Completion
1. ✅ Review final summary in logs
2. ✅ Run BigQuery deduplication queries
3. ✅ Archive log files
4. ✅ Clean up checkpoint files

## 🎯 Success Indicators

```bash
# Check if scraper completed successfully
tail -20 logs/psa_production_*.log | grep -E "(SUCCESS|completed|Summary)"

# Verify data in BigQuery
# Use queries from bigquery_deduplication_queries.sql

# Expected final output
📊 Total records processed: 25000+
🌐 Total API calls: 190
✅ Combinations processed this session: 190
```

Ready for production deployment! 🚀