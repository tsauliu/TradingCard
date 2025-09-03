#!/bin/bash
# eBay Search Real-time Monitor
# Shows progress, stats, and recent activity

# Colors for better visibility
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

while true; do
    clear
    
    echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}          eBay Search Monitor - $(date +"%Y-%m-%d %H:%M:%S")${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
    echo ""
    
    # Get latest session
    SESSION=$(ls -td .ebay_temp/session_* 2>/dev/null | head -1)
    
    if [ -n "$SESSION" ]; then
        # Parse session state
        if [ -f "$SESSION/state.json" ]; then
            echo -e "${YELLOW}📊 Progress:${NC}"
            python3 -c "
import json
import sys
from datetime import datetime

try:
    with open('$SESSION/state.json') as f:
        state = json.load(f)
    
    total = state.get('total_keywords', 0)
    completed = state.get('completed', 0)
    failed = state.get('failed', 0)
    pending = total - completed - failed
    
    if total > 0:
        percent = (completed / total) * 100
        
        # Progress bar
        bar_length = 50
        filled = int(bar_length * completed / total)
        bar = '█' * filled + '░' * (bar_length - filled)
        
        print(f'  [{bar}] {percent:.1f}%')
        print(f'  Completed: {completed}/{total} | Failed: {failed} | Pending: {pending}')
        
        # Current keyword
        if 'current_keyword' in state and state['current_keyword']:
            print(f'  Current: {state[\"current_keyword\"]}')
        
        # Time estimate
        if 'start_time' in state and completed > 0:
            start = datetime.fromisoformat(state['start_time'])
            elapsed = (datetime.now() - start).total_seconds()
            avg_time = elapsed / completed
            remaining = (total - completed) * avg_time
            
            hours = int(remaining // 3600)
            minutes = int((remaining % 3600) // 60)
            print(f'  Est. remaining: {hours}h {minutes}m')
        
        # Rate limiting info
        if 'rate_limit' in state:
            delay = state['rate_limit'].get('adaptive_delay', 20)
            print(f'  Current delay: {delay:.1f}s')
    
except Exception as e:
    print(f'  Error reading state: {e}')
"
        fi
        
        echo ""
        echo -e "${YELLOW}📁 Storage Statistics:${NC}"
        
        # Count files
        RAW_API_COUNT=$(find raw_api_responses -name "*.json" 2>/dev/null | wc -l)
        PERM_COUNT=$(find permanent_raw_json -name "*.json" 2>/dev/null | wc -l)
        SESSION_COUNT=$(find $SESSION/results -name "*.json" 2>/dev/null | wc -l)
        
        echo "  Raw API responses: $RAW_API_COUNT files"
        echo "  Permanent storage: $PERM_COUNT files"
        echo "  Session results: $SESSION_COUNT files"
        
        # Calculate sizes
        if [ -d "permanent_raw_json" ]; then
            PERM_SIZE=$(du -sh permanent_raw_json 2>/dev/null | cut -f1)
            echo "  Total permanent size: $PERM_SIZE"
        fi
        
        echo ""
        echo -e "${YELLOW}📜 Recent Activity:${NC}"
        
        # Find most recent log file
        LATEST_LOG=$(ls -t logs/production_*/execution.log 2>/dev/null | head -1)
        if [ -n "$LATEST_LOG" ]; then
            tail -5 "$LATEST_LOG" | sed 's/^/  /'
        else
            echo "  No active log found"
        fi
        
        echo ""
        echo -e "${YELLOW}🔄 Latest Results:${NC}"
        
        # Show most recent result files
        if [ -d "$SESSION/results" ]; then
            ls -lt $SESSION/results/*.json 2>/dev/null | head -3 | while read line; do
                # Extract just filename and time
                echo "$line" | awk '{print "  " $6 " " $7 " " $8 " - " $9}'
            done
        fi
        
    else
        echo -e "${RED}No active session found${NC}"
        echo ""
        echo "Start a search with: ./launch_full_ebay_search.sh"
    fi
    
    echo ""
    echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}Commands:${NC}"
    echo "  Press Ctrl+C to exit monitor (search continues in background)"
    echo "  Run 'screen -ls' to see all screen sessions"
    echo "  Run 'screen -r ebay_full_*' to attach to search session"
    echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
    
    # Refresh every 5 seconds
    sleep 5
done