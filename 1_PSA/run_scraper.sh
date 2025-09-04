#!/bin/bash
# PSA Scraper Runner with Logging

LOG_DIR="logs"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${LOG_DIR}/psa_scraper_${TIMESTAMP}.log"

# Create logs directory if it doesn't exist
mkdir -p "$LOG_DIR"

echo "========================================" | tee -a "$LOG_FILE"
echo "PSA Scraper - Full Production Run" | tee -a "$LOG_FILE"
echo "Started: $(date)" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

echo "Configuration:" | tee -a "$LOG_FILE"
echo "- 25 Pokemon cards" | tee -a "$LOG_FILE"
echo "- 19 PSA grades per card" | tee -a "$LOG_FILE"
echo "- Total API calls: 475" | tee -a "$LOG_FILE"
echo "- Rate limit: 30 seconds between API calls" | tee -a "$LOG_FILE"
echo "- Estimated runtime: ~4 hours" | tee -a "$LOG_FILE"
echo "- Log file: $LOG_FILE" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

# Run the scraper with unbuffered output
python3 -u scrape_psa.py 2>&1 | while IFS= read -r line; do
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $line" | tee -a "$LOG_FILE"
done

echo "" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"
echo "Completed: $(date)" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"