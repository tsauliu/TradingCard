#!/bin/bash
# PSA Scraper Monitoring Script
# Real-time monitoring of scraper progress and health

LOG_DIR="logs"
SCREEN_NAME="psa_scraper"
PID_FILE="$LOG_DIR/psa_scraper.pid"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# Function to get latest log file
get_latest_log() {
    find "$LOG_DIR" -name "psa_production_*.log" -type f -printf '%T@ %p\n' 2>/dev/null | sort -n | tail -1 | cut -d' ' -f2-
}

# Function to get latest error log
get_latest_error_log() {
    find "$LOG_DIR" -name "psa_production_errors_*.log" -type f -printf '%T@ %p\n' 2>/dev/null | sort -n | tail -1 | cut -d' ' -f2-
}

# Function to show status
show_status() {
    clear
    echo -e "${BLUE}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║                    PSA Scraper Monitor                       ║${NC}"
    echo -e "${BLUE}║                  Updated: $(date '+%H:%M:%S')                         ║${NC}"
    echo -e "${BLUE}╚══════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    
    # Check if scraper is running
    if [ -f "$PID_FILE" ] && kill -0 $(cat "$PID_FILE") 2>/dev/null; then
        PID=$(cat "$PID_FILE")
        echo -e "${GREEN}🟢 STATUS: RUNNING (PID: $PID)${NC}"
        
        # Show CPU and memory usage
        if command -v ps &> /dev/null; then
            CPU_MEM=$(ps -p "$PID" -o %cpu,%mem --no-headers 2>/dev/null || echo "N/A N/A")
            echo -e "${CYAN}💻 Resource Usage: CPU: $(echo $CPU_MEM | awk '{print $1}')% | Memory: $(echo $CPU_MEM | awk '{print $2}')%${NC}"
        fi
    else
        echo -e "${RED}🔴 STATUS: NOT RUNNING${NC}"
    fi
    
    # Check screen session
    if screen -list | grep -q "$SCREEN_NAME" 2>/dev/null; then
        echo -e "${GREEN}📺 Screen Session: ACTIVE${NC}"
    else
        echo -e "${YELLOW}📺 Screen Session: INACTIVE${NC}"
    fi
    
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    
    # Show recent progress from log
    LATEST_LOG=$(get_latest_log)
    if [ -n "$LATEST_LOG" ] && [ -f "$LATEST_LOG" ]; then
        echo -e "${YELLOW}📊 RECENT PROGRESS:${NC}"
        echo ""
        
        # Extract key progress indicators
        tail -n 50 "$LATEST_LOG" | grep -E "(Processing card|🎴|📊|✅|❌|ETA|Progress|Completed)" | tail -10 | while read line; do
            if echo "$line" | grep -q "🎴\|Processing card"; then
                echo -e "${CYAN}$line${NC}"
            elif echo "$line" | grep -q "✅\|Completed\|Success"; then
                echo -e "${GREEN}$line${NC}"
            elif echo "$line" | grep -q "❌\|Error\|Failed"; then
                echo -e "${RED}$line${NC}"
            else
                echo -e "${YELLOW}$line${NC}"
            fi
        done
    else
        echo -e "${YELLOW}📊 No log file found${NC}"
    fi
    
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    
    # Show errors if any
    ERROR_LOG=$(get_latest_error_log)
    if [ -n "$ERROR_LOG" ] && [ -f "$ERROR_LOG" ] && [ -s "$ERROR_LOG" ]; then
        echo -e "${RED}🚨 RECENT ERRORS:${NC}"
        echo ""
        tail -n 5 "$ERROR_LOG" | while read line; do
            echo -e "${RED}$line${NC}"
        done
    else
        echo -e "${GREEN}🟢 No errors detected${NC}"
    fi
    
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    
    # Show statistics if available
    if [ -n "$LATEST_LOG" ] && [ -f "$LATEST_LOG" ]; then
        echo -e "${YELLOW}📈 STATISTICS:${NC}"
        
        # Count API calls
        API_CALLS=$(grep -c "Fetching data for ID" "$LATEST_LOG" 2>/dev/null || echo "0")
        echo -e "${CYAN}API Calls Made: $API_CALLS${NC}"
        
        # Count successful requests
        SUCCESS_CALLS=$(grep -c "✅ Success:" "$LATEST_LOG" 2>/dev/null || echo "0")
        echo -e "${GREEN}Successful Requests: $SUCCESS_CALLS${NC}"
        
        # Count failed requests
        FAILED_CALLS=$(grep -c "❌.*failed\|No data found" "$LATEST_LOG" 2>/dev/null || echo "0")
        echo -e "${RED}Failed Requests: $FAILED_CALLS${NC}"
        
        # Show uptime
        if [ -f "$PID_FILE" ] && kill -0 $(cat "$PID_FILE") 2>/dev/null; then
            START_TIME=$(grep "PSA Scraper started at" "$LATEST_LOG" 2>/dev/null | tail -1 | sed 's/.*started at //')
            if [ -n "$START_TIME" ]; then
                echo -e "${BLUE}Started: $START_TIME${NC}"
            fi
        fi
    fi
    
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${YELLOW}Commands: [q]uit | [l]ogs | [e]rrors | [s]creen | [k]ill${NC}"
}

# Main monitoring loop
while true; do
    show_status
    
    # Wait for user input with timeout
    read -t 5 -n 1 key 2>/dev/null || key=""
    
    case $key in
        q|Q)
            echo -e "\n${GREEN}Exiting monitor...${NC}"
            exit 0
            ;;
        l|L)
            LATEST_LOG=$(get_latest_log)
            if [ -n "$LATEST_LOG" ]; then
                echo -e "\n${BLUE}Opening log file: $LATEST_LOG${NC}"
                echo -e "${YELLOW}Press Ctrl+C to return to monitor${NC}"
                tail -f "$LATEST_LOG"
            fi
            ;;
        e|E)
            ERROR_LOG=$(get_latest_error_log)
            if [ -n "$ERROR_LOG" ]; then
                echo -e "\n${BLUE}Opening error log: $ERROR_LOG${NC}"
                echo -e "${YELLOW}Press Ctrl+C to return to monitor${NC}"
                tail -f "$ERROR_LOG"
            fi
            ;;
        s|S)
            if screen -list | grep -q "$SCREEN_NAME"; then
                echo -e "\n${BLUE}Attaching to screen session...${NC}"
                screen -r "$SCREEN_NAME"
            else
                echo -e "\n${RED}No screen session found${NC}"
                sleep 2
            fi
            ;;
        k|K)
            echo -e "\n${RED}Killing scraper...${NC}"
            ./kill_psa_scraper.sh
            exit 0
            ;;
    esac
done