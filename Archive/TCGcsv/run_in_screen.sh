#!/bin/bash
# TCG Data Download in Screen Session with Logging

SESSION_NAME="tcg-download"
LOG_FILE="tcg_download_screen.log"
PID_FILE="tcg_download.pid"

# Function to check if screen session exists
session_exists() {
    screen -list | grep -q "$SESSION_NAME"
}

# Function to start download
start_download() {
    if session_exists; then
        echo "Download session already running. Use 'screen -r $SESSION_NAME' to attach."
        exit 1
    fi
    
    echo "Starting TCG download in screen session: $SESSION_NAME"
    echo "Logs will be saved to: $LOG_FILE"
    echo ""
    echo "Commands to manage the session:"
    echo "  Attach to session:   screen -r $SESSION_NAME"
    echo "  Detach from session: Ctrl+A, then D"
    echo "  Kill session:        screen -S $SESSION_NAME -X quit"
    echo "  View logs:           tail -f $LOG_FILE"
    echo ""
    
    # Set up environment and start download in screen
    screen -dmS "$SESSION_NAME" bash -c "
        export GOOGLE_APPLICATION_CREDENTIALS=service-account.json
        source ~/.bashrc
        cd $(pwd)
        echo 'Starting resumable TCG download at $(date)' | tee -a $LOG_FILE
        python3 product_downloader/resumable_downloader.py 2>&1 | tee -a $LOG_FILE
        echo 'Download completed at $(date)' | tee -a $LOG_FILE
        echo 'Session will remain open for review. Use screen -r $SESSION_NAME to view results.'
        exec bash  # Keep session alive
    "
    
    # Save PID for reference
    screen -list | grep "$SESSION_NAME" | awk '{print $1}' | cut -d. -f1 > $PID_FILE
    
    echo "Download started in background!"
    echo "To attach: screen -r $SESSION_NAME"
}

# Function to stop download
stop_download() {
    if ! session_exists; then
        echo "No download session found"
        exit 1
    fi
    
    echo "Stopping TCG download session..."
    screen -S "$SESSION_NAME" -X stuff "^C"  # Send Ctrl+C
    sleep 2
    
    if session_exists; then
        echo "Session still running. Use 'screen -S $SESSION_NAME -X quit' to force kill."
    else
        echo "Download session stopped"
    fi
    
    rm -f $PID_FILE
}

# Function to show status
show_status() {
    if session_exists; then
        echo "Download session is RUNNING: $SESSION_NAME"
        
        if [ -f "product_downloader/download_progress.json" ]; then
            echo ""
            echo "Progress summary:"
            python3 -c "
import json, os
if os.path.exists('product_downloader/download_progress.json'):
    with open('product_downloader/download_progress.json') as f:
        p = json.load(f)
    print(f'  Started: {p.get(\"started_at\", \"N/A\")}')
    print(f'  Completed categories: {p.get(\"completed_categories\", 0)}')
    print(f'  Total products: {p.get(\"total_products_downloaded\", 0):,}')
    print(f'  Current category: {p.get(\"current_category\", \"N/A\")}')
    print(f'  Failed categories: {len(p.get(\"failed_categories\", []))}')
    print(f'  Failed groups: {len(p.get(\"failed_groups\", []))}')
else:
    print('  No progress file found')
"
        fi
        
        echo ""
        echo "Latest log entries:"
        tail -10 $LOG_FILE 2>/dev/null || echo "No log file found"
        
    else
        echo "Download session is NOT RUNNING"
        
        if [ -f $PID_FILE ]; then
            rm $PID_FILE
        fi
    fi
}

# Function to show help
show_help() {
    echo "TCG Download Manager"
    echo ""
    echo "Usage: $0 {start|stop|status|attach|logs|help}"
    echo ""
    echo "Commands:"
    echo "  start   - Start download in screen session"
    echo "  stop    - Stop download (sends Ctrl+C)"
    echo "  status  - Show current status and progress"
    echo "  attach  - Attach to running session"
    echo "  logs    - Follow live logs"
    echo "  help    - Show this help"
    echo ""
    echo "Files:"
    echo "  download_progress.json - Progress checkpoint"
    echo "  tcg_download.log      - Application logs"
    echo "  tcg_download_screen.log - Screen session logs"
}

# Function to attach to session
attach_session() {
    if ! session_exists; then
        echo "No download session found"
        exit 1
    fi
    
    echo "Attaching to session $SESSION_NAME"
    echo "Use Ctrl+A then D to detach without stopping the download"
    screen -r "$SESSION_NAME"
}

# Function to follow logs
follow_logs() {
    if [ ! -f "$LOG_FILE" ]; then
        echo "Log file not found: $LOG_FILE"
        exit 1
    fi
    
    echo "Following logs (Ctrl+C to stop viewing):"
    tail -f "$LOG_FILE"
}

# Main script
case "$1" in
    start)
        start_download
        ;;
    stop)
        stop_download
        ;;
    status)
        show_status
        ;;
    attach)
        attach_session
        ;;
    logs)
        follow_logs
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        echo "Usage: $0 {start|stop|status|attach|logs|help}"
        exit 1
        ;;
esac