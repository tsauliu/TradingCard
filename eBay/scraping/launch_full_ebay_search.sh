#!/bin/bash
# Full eBay Batch Search Launch Script
# 76 keywords, 4 years of data, with permanent JSON storage

# Configuration
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
SCREEN_NAME="ebay_full_${TIMESTAMP}"
LOG_DIR="logs/production_${TIMESTAMP}"
EXCEL_NAME="trading_cards_4years_full_${TIMESTAMP}"

# Create all necessary directories
mkdir -p ${LOG_DIR}/{results,monitoring}
mkdir -p permanent_raw_json
mkdir -p raw_api_responses

# Log launch details
echo "========================================" | tee ${LOG_DIR}/launch.log
echo "eBay Full Batch Search Launch" | tee -a ${LOG_DIR}/launch.log
echo "Time: $(date)" | tee -a ${LOG_DIR}/launch.log
echo "Session: ${SCREEN_NAME}" | tee -a ${LOG_DIR}/launch.log
echo "Keywords: 76 (deduplicated from keywords.xlsx)" | tee -a ${LOG_DIR}/launch.log
echo "Period: 4 years (1460 days)" | tee -a ${LOG_DIR}/launch.log
echo "Min delay: 60 seconds" | tee -a ${LOG_DIR}/launch.log
echo "Estimated time: 76-90 minutes" | tee -a ${LOG_DIR}/launch.log
echo "========================================" | tee -a ${LOG_DIR}/launch.log

# Launch in screen with comprehensive logging
screen -dmS ${SCREEN_NAME} -L -Logfile ${LOG_DIR}/screen.log bash -c "
    # Set proxy for China mainland
    export http_proxy=http://127.0.0.1:20171
    export https_proxy=http://127.0.0.1:20171
    
    # Start time tracking
    START_TIME=\$(date +%s)
    echo 'Search started at: '\$(date) | tee ${LOG_DIR}/status.txt
    echo 'PID: '\$\$ | tee -a ${LOG_DIR}/status.txt
    
    # Main search command with all safety features
    python3 ebay_batch_search.py \
        --file keywords_full_dedup.txt \
        --excel-pivot ${EXCEL_NAME} \
        --days 1460 \
        --min-delay 60 \
        --checkpoint-every 5 \
        --continue-on-error \
        --retry-failed \
        --keep-temp \
        --time-period weekly \
        --output-dir ${LOG_DIR}/results \
        2>&1 | tee ${LOG_DIR}/execution.log
    
    # Calculate duration
    END_TIME=\$(date +%s)
    DURATION=\$((END_TIME - START_TIME))
    HOURS=\$((DURATION / 3600))
    MINUTES=\$(((DURATION % 3600) / 60))
    SECONDS=\$((DURATION % 60))
    
    echo '' | tee -a ${LOG_DIR}/status.txt
    echo '========================================' | tee -a ${LOG_DIR}/status.txt
    echo 'Search completed at: '\$(date) | tee -a ${LOG_DIR}/status.txt
    echo \"Total duration: \${HOURS}h \${MINUTES}m \${SECONDS}s\" | tee -a ${LOG_DIR}/status.txt
    echo '========================================' | tee -a ${LOG_DIR}/status.txt
    
    # Generate final summary
    python3 -c \"
import json
import os
from pathlib import Path

print('\\n=== Final Summary ===')

# Count raw JSON files saved
raw_api_count = len(list(Path('raw_api_responses').rglob('*.json')))
permanent_count = len(list(Path('permanent_raw_json').rglob('*.json')))
print(f'Raw API responses saved: {raw_api_count}')
print(f'Permanent JSON files saved: {permanent_count}')

# Check Excel file
excel_file = '${LOG_DIR}/results/${EXCEL_NAME}.xlsx'
if os.path.exists(excel_file):
    size_mb = os.path.getsize(excel_file) / 1024 / 1024
    print(f'Excel pivot table size: {size_mb:.2f} MB')
    print(f'Excel location: {excel_file}')
else:
    print('WARNING: Excel file not found!')

# Count session files
session_dirs = list(Path('.ebay_temp').glob('session_*'))
if session_dirs:
    latest_session = max(session_dirs, key=lambda x: x.stat().st_mtime)
    session_files = len(list(latest_session.glob('results/*.json')))
    print(f'Session result files: {session_files}')
    print(f'Session path: {latest_session}')
\" | tee -a ${LOG_DIR}/summary.txt
"

# Show monitoring instructions
echo ""
echo "✅ Screen session started: ${SCREEN_NAME}"
echo "📁 Log directory: ${LOG_DIR}"
echo ""
echo "📊 Monitor commands:"
echo "  screen -r ${SCREEN_NAME}                      # Attach to session (Ctrl+A,D to detach)"
echo "  tail -f ${LOG_DIR}/execution.log              # Watch live progress"
echo "  watch -n 5 'tail -20 ${LOG_DIR}/execution.log'  # Auto-refresh every 5s"
echo ""
echo "📈 Check progress:"
echo "  cat .ebay_temp/session_*/state.json | jq '.progress'"
echo "  ls -la permanent_raw_json/*/ | wc -l          # Count saved JSONs"
echo "  ls -la raw_api_responses/ | wc -l             # Count raw API files"
echo ""
echo "⚠️ To stop gracefully:"
echo "  screen -r ${SCREEN_NAME}  # Attach"
echo "  Ctrl+C                    # Interrupt (will save state)"
echo ""
echo "💾 Data will be saved in:"
echo "  - permanent_raw_json/     # Never deleted"
echo "  - raw_api_responses/      # Raw API responses"
echo "  - ${LOG_DIR}/             # Logs and results"