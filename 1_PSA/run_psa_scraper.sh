#!/bin/bash
# PSA Scraper Screen Session Setup Script
# Usage: ./run_psa_scraper.sh [--dry-run]

set -e  # Exit on any error

# Configuration
SCREEN_NAME="psa_scraper"
LOG_DIR="logs"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
MAIN_LOG="$LOG_DIR/psa_production_${TIMESTAMP}.log"
ERROR_LOG="$LOG_DIR/psa_production_errors_${TIMESTAMP}.log"
MONITOR_LOG="$LOG_DIR/psa_monitor_${TIMESTAMP}.log"
PID_FILE="$LOG_DIR/psa_scraper.pid"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 PSA Production Scraper Setup${NC}"
echo -e "${BLUE}================================${NC}"

# Create log directory
mkdir -p "$LOG_DIR"

# Check if screen is installed
if ! command -v screen &> /dev/null; then
    echo -e "${RED}❌ Screen is not installed. Installing...${NC}"
    sudo apt update && sudo apt install -y screen
fi

# Check if scraper is already running
if screen -list | grep -q "$SCREEN_NAME"; then
    echo -e "${YELLOW}⚠️ PSA scraper screen session already exists!${NC}"
    echo -e "${YELLOW}Use: screen -r $SCREEN_NAME to attach${NC}"
    echo -e "${YELLOW}Or: ./kill_psa_scraper.sh to stop it${NC}"
    exit 1
fi

# Check for existing PID file
if [ -f "$PID_FILE" ]; then
    echo -e "${YELLOW}⚠️ Found existing PID file. Checking if process is running...${NC}"
    if kill -0 $(cat "$PID_FILE") 2>/dev/null; then
        echo -e "${RED}❌ PSA scraper is already running (PID: $(cat $PID_FILE))${NC}"
        exit 1
    else
        echo -e "${GREEN}✅ Stale PID file removed${NC}"
        rm "$PID_FILE"
    fi
fi

# Determine run mode
DRY_RUN=""
if [ "$1" = "--dry-run" ]; then
    DRY_RUN="--dry-run"
    echo -e "${YELLOW}🧪 Running in DRY RUN mode${NC}"
else
    echo -e "${GREEN}🏭 Running in PRODUCTION mode${NC}"
fi

# Create comprehensive logging command
PYTHON_CMD="python scrape_psa_production.py $DRY_RUN 2>&1 | tee -a $MAIN_LOG"

# Create the screen session with proper logging
echo -e "${BLUE}📺 Creating screen session: $SCREEN_NAME${NC}"
echo -e "${BLUE}📝 Main log: $MAIN_LOG${NC}"
echo -e "${BLUE}🚨 Error log: $ERROR_LOG${NC}"
echo -e "${BLUE}📊 Monitor log: $MONITOR_LOG${NC}"

# Create screen session and run scraper
screen -dmS "$SCREEN_NAME" bash -c "
    # Set up error handling
    set -e
    trap 'echo \"[ERROR] Script failed at line \$LINENO\" >> $ERROR_LOG' ERR
    
    # Record start time and PID
    echo \"PSA Scraper started at \$(date)\" >> $MAIN_LOG
    echo \"PID: \$\$\" >> $MAIN_LOG
    echo \$\$ > $PID_FILE
    
    # Change to correct directory
    cd /home/caoliu/TradingCard/PSA
    
    # Log environment info
    echo \"Working directory: \$(pwd)\" >> $MAIN_LOG
    echo \"Python version: \$(python --version)\" >> $MAIN_LOG
    echo \"Git status: \$(git status --porcelain | wc -l) uncommitted files\" >> $MAIN_LOG
    echo \"========================================\" >> $MAIN_LOG
    
    # Run the scraper with comprehensive logging
    {
        echo \"[INFO] Starting PSA scraper...\"
        $PYTHON_CMD
        echo \"[SUCCESS] PSA scraper completed successfully at \$(date)\"
    } 2> >(tee -a $ERROR_LOG >&2)
    
    # Clean up PID file on successful completion
    rm -f $PID_FILE
    echo \"[CLEANUP] PID file removed\" >> $MAIN_LOG
    
    # Keep screen session open for review
    echo \"\"
    echo \"========================================\"
    echo \"PSA Scraper finished. Press any key to close this screen session.\"
    echo \"Logs available at: $MAIN_LOG\"
    echo \"Error logs at: $ERROR_LOG\"
    echo \"========================================\"
    read -n 1
"

# Verify screen session was created
sleep 2
if screen -list | grep -q "$SCREEN_NAME"; then
    echo -e "${GREEN}✅ Screen session '$SCREEN_NAME' created successfully${NC}"
    echo -e "${GREEN}📊 Process ID saved to: $PID_FILE${NC}"
else
    echo -e "${RED}❌ Failed to create screen session${NC}"
    exit 1
fi

echo ""
echo -e "${BLUE}📋 Management Commands:${NC}"
echo -e "${YELLOW}  Attach to session:${NC} screen -r $SCREEN_NAME"
echo -e "${YELLOW}  Detach from session:${NC} Ctrl+A, then D"
echo -e "${YELLOW}  Monitor logs:${NC} tail -f $MAIN_LOG"
echo -e "${YELLOW}  Check errors:${NC} tail -f $ERROR_LOG"
echo -e "${YELLOW}  Kill scraper:${NC} ./kill_psa_scraper.sh"
echo -e "${YELLOW}  List sessions:${NC} screen -list"

echo ""
echo -e "${GREEN}🎯 PSA scraper is now running in background!${NC}"
echo -e "${BLUE}Use 'screen -r $SCREEN_NAME' to attach and monitor progress${NC}"