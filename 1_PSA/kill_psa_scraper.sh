#!/bin/bash
# PSA Scraper Kill Script
# Safely terminates the PSA scraper and cleans up

set -e

SCREEN_NAME="psa_scraper"
LOG_DIR="logs"
PID_FILE="$LOG_DIR/psa_scraper.pid"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${RED}🛑 PSA Scraper Kill Script${NC}"
echo -e "${RED}=========================${NC}"

# Check for PID file
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    echo -e "${YELLOW}Found PID file with process ID: $PID${NC}"
    
    # Check if process is running
    if kill -0 "$PID" 2>/dev/null; then
        echo -e "${YELLOW}⚠️ Terminating PSA scraper process (PID: $PID)...${NC}"
        kill -TERM "$PID"
        
        # Wait for graceful shutdown
        sleep 5
        
        # Force kill if still running
        if kill -0 "$PID" 2>/dev/null; then
            echo -e "${RED}Process still running, force killing...${NC}"
            kill -KILL "$PID"
        fi
        
        echo -e "${GREEN}✅ Process terminated${NC}"
    else
        echo -e "${YELLOW}Process not running, cleaning up stale PID file${NC}"
    fi
    
    rm -f "$PID_FILE"
    echo -e "${GREEN}✅ PID file removed${NC}"
else
    echo -e "${YELLOW}No PID file found${NC}"
fi

# Kill screen session
if screen -list | grep -q "$SCREEN_NAME"; then
    echo -e "${YELLOW}Terminating screen session: $SCREEN_NAME${NC}"
    screen -S "$SCREEN_NAME" -X quit
    echo -e "${GREEN}✅ Screen session terminated${NC}"
else
    echo -e "${YELLOW}No screen session found${NC}"
fi

echo -e "${GREEN}🎯 PSA scraper cleanup completed${NC}"