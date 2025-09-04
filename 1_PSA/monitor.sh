#!/bin/bash
# Monitor PSA scraper progress

echo "PSA Scraper Monitor"
echo "=================="
echo ""
echo "Screen session: $(screen -list | grep psa_scraper)"
echo ""
echo "Latest log entries:"
echo "-------------------"
tail -n 30 logs/psa_scraper_*.log | tail -n 20
echo ""
echo "Commands:"
echo "  screen -r psa_scraper    # Attach to screen session (Ctrl+A,D to detach)"
echo "  tail -f logs/psa_scraper_*.log  # Follow log in real-time"
echo "  ./monitor.sh             # Show this status"