#!/bin/bash

# Create log directory if it doesn't exist
mkdir -p log

# Generate timestamp for log file
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOGFILE="log/ebay_scraper_${TIMESTAMP}.log"

# Start screen session with logging
screen -L -Logfile "$LOGFILE" -dmS ebay_scraper python3 run_batch.py

echo "Started eBay scraper in screen session 'ebay_scraper'"
echo "Logging to: $LOGFILE"
echo ""
echo "Commands:"
echo "  screen -r ebay_scraper    # Attach to session"
echo "  screen -ls                # List sessions"
echo "  tail -f $LOGFILE          # Watch log"
echo "  Ctrl+A then D             # Detach from screen"