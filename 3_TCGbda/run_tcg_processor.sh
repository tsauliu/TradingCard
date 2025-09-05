#!/bin/bash
#
# TCG Data Processor Runner Script
# Runs the TCG data processor in a screen session with logging
#

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_SCRIPT="${SCRIPT_DIR}/process_tcg_data.py"
LOG_DIR="/logs"
SCREEN_NAME="tcg_processor"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${LOG_DIR}/tcg_processor_${TIMESTAMP}.log"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Functions
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_requirements() {
    print_info "Checking requirements..."
    
    # Check if screen is installed
    if ! command -v screen &> /dev/null; then
        print_error "screen is not installed. Installing..."
        sudo apt-get update && sudo apt-get install -y screen
    fi
    
    # Check if Python script exists
    if [ ! -f "$PYTHON_SCRIPT" ]; then
        print_error "Python script not found: $PYTHON_SCRIPT"
        exit 1
    fi
    
    # Check if log directory exists, create if not
    if [ ! -d "$LOG_DIR" ]; then
        print_info "Creating log directory: $LOG_DIR"
        sudo mkdir -p "$LOG_DIR"
        sudo chmod 755 "$LOG_DIR"
    fi
    
    # Check Python dependencies
    print_info "Checking Python dependencies..."
    pip list 2>/dev/null | grep -E "google-cloud-bigquery|pandas|python-dotenv" > /dev/null
    if [ $? -ne 0 ]; then
        print_warning "Some Python dependencies might be missing. Installing..."
        pip install google-cloud-bigquery pandas python-dotenv
    fi
    
    print_success "All requirements met"
}

check_existing_session() {
    # Check if screen session already exists
    screen -list | grep -q "$SCREEN_NAME"
    if [ $? -eq 0 ]; then
        print_warning "Screen session '$SCREEN_NAME' already exists"
        echo "Options:"
        echo "  1) Kill existing session and start new"
        echo "  2) Attach to existing session"
        echo "  3) Exit"
        read -p "Choose option [1-3]: " choice
        
        case $choice in
            1)
                print_info "Killing existing session..."
                screen -S "$SCREEN_NAME" -X quit
                sleep 2
                ;;
            2)
                print_info "Attaching to existing session..."
                screen -r "$SCREEN_NAME"
                exit 0
                ;;
            3)
                print_info "Exiting..."
                exit 0
                ;;
            *)
                print_error "Invalid option"
                exit 1
                ;;
        esac
    fi
}

start_processor() {
    print_info "Starting TCG Data Processor with Deduplication Tracking..."
    print_info "Screen session: $SCREEN_NAME"
    print_info "Log file: $LOG_FILE"
    
    # Parse command line arguments
    MODE="${1:-append}"
    DIRECTORY="${2:-./product_details}"
    PROCESS_ZIP="${3:-no}"
    BATCH_SIZE="${4:-500}"
    MAX_MEMORY="${5:-512}"
    TRACKING_CSV="${6:-uploaded_files_tracker.csv}"
    
    if [ "$MODE" = "replace" ]; then
        print_warning "MODE: REPLACE - This will DELETE ALL existing data in BigQuery!"
        print_warning "Consider using 'append' mode to preserve existing data"
        echo "Press Ctrl+C within 5 seconds to cancel..."
        sleep 5
    else
        print_info "Mode: $MODE (safe - preserves existing data)"
    fi
    print_info "Data directory: $DIRECTORY"
    print_info "Process ZIP: $PROCESS_ZIP"
    print_info "Batch size: $BATCH_SIZE files"
    print_info "Max memory: $MAX_MEMORY MB"
    print_info "Tracking CSV: $TRACKING_CSV"
    
    # Build command based on options
    CMD="python3 $PYTHON_SCRIPT --mode $MODE --directory $DIRECTORY --batch-size $BATCH_SIZE --max-memory $MAX_MEMORY --tracking-csv $TRACKING_CSV"
    
    if [ "$PROCESS_ZIP" = "yes" ] || [ "$PROCESS_ZIP" = "true" ]; then
        CMD="$CMD --process-zip"
    elif [ "$PROCESS_ZIP" = "zip-only" ]; then
        CMD="python3 $PYTHON_SCRIPT --directory $DIRECTORY --zip-only --tracking-csv $TRACKING_CSV"
    fi
    
    CMD="$CMD 2>&1 | tee $LOG_FILE"
    
    # Start screen session
    screen -dmS "$SCREEN_NAME" bash -c "$CMD"
    
    # Wait a moment for the session to start
    sleep 2
    
    # Check if session started successfully
    screen -list | grep -q "$SCREEN_NAME"
    if [ $? -eq 0 ]; then
        print_success "Processor started successfully in screen session '$SCREEN_NAME'"
        print_info "Commands:"
        echo "  - Attach to session: screen -r $SCREEN_NAME"
        echo "  - Detach from session: Ctrl+A then D"
        echo "  - View log: tail -f $LOG_FILE"
        echo "  - Kill session: screen -S $SCREEN_NAME -X quit"
        echo ""
        print_info "Following log output (Ctrl+C to stop following)..."
        echo "=========================================="
        
        # Follow the log file
        tail -f "$LOG_FILE" 2>/dev/null || print_warning "Log file not yet created. Use 'tail -f $LOG_FILE' to view when available"
    else
        print_error "Failed to start processor"
        exit 1
    fi
}

show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  start [mode] [dir] [zip] [batch] [mem] [csv]  Start the processor with deduplication"
    echo "                                   mode: append/replace (default: append - SAFE)"
    echo "                                   WARNING: 'replace' will DELETE ALL existing data!"
    echo "                                   dir: JSON directory (default: ./product_details)"
    echo "                                   zip: yes/no/zip-only (default: no)"
    echo "                                   batch: batch size (default: 500)"
    echo "                                   mem: max memory in MB (default: 512)"
    echo "                                   csv: tracking CSV file (default: uploaded_files_tracker.csv)"
    echo "  extract                          Extract latest ZIP file only (preserves structure)"
    echo "  stop                             Stop the processor"
    echo "  status                           Check processor status"
    echo "  logs                             View recent logs"
    echo "  attach                           Attach to screen session"
    echo "  tracker                          View uploaded files tracking status"
    echo "  help                             Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 start                         # Start with default settings (append mode - safe)"
    echo "  $0 start append                  # Start in append mode with deduplication"
    echo "  $0 start append ./product_details yes  # Process ZIP then upload with tracking"
    echo "  $0 start append ./product_details no 1000 1024  # Custom batch/memory settings"
    echo "  $0 extract                       # Extract latest ZIP only (preserves structure)"
    echo "  $0 tracker                       # View tracking statistics"
    echo "  $0 stop                          # Stop the processor"
    echo "  $0 status                        # Check if processor is running"
    echo "  $0 logs                          # View recent log files"
}

# Main execution
case "${1:-start}" in
    start)
        check_requirements
        check_existing_session
        start_processor "${2:-append}" "${3:-./product_details}" "${4:-no}" "${5:-500}" "${6:-512}" "${7:-uploaded_files_tracker.csv}"
        ;;
    extract)
        check_requirements
        print_info "Extracting latest ZIP file with preserved structure..."
        python3 $PYTHON_SCRIPT --zip-only
        ;;
    tracker)
        TRACKING_CSV="${2:-uploaded_files_tracker.csv}"
        if [ -f "$TRACKING_CSV" ]; then
            print_info "Tracking Statistics for: $TRACKING_CSV"
            echo "========================================="
            
            # Count total uploaded files
            TOTAL_FILES=$(tail -n +2 "$TRACKING_CSV" 2>/dev/null | wc -l)
            print_info "Total uploaded files: $TOTAL_FILES"
            
            # Count unique scrape dates
            UNIQUE_DATES=$(tail -n +2 "$TRACKING_CSV" 2>/dev/null | cut -d',' -f2 | sort -u | wc -l)
            print_info "Unique scrape dates: $UNIQUE_DATES"
            
            # Show recent uploads
            print_info "Last 5 uploaded files:"
            tail -5 "$TRACKING_CSV" 2>/dev/null | column -t -s,
            
            # Show scrape date summary
            print_info "\nUploads per scrape date:"
            tail -n +2 "$TRACKING_CSV" 2>/dev/null | cut -d',' -f2 | sort | uniq -c | sort -rn | head -10
        else
            print_warning "No tracking CSV found at: $TRACKING_CSV"
            print_info "Run the processor at least once to create tracking data"
        fi
        ;;
    stop)
        print_info "Stopping processor..."
        screen -S "$SCREEN_NAME" -X quit 2>/dev/null
        if [ $? -eq 0 ]; then
            print_success "Processor stopped"
        else
            print_warning "No running processor found"
        fi
        ;;
    status)
        screen -list | grep -q "$SCREEN_NAME"
        if [ $? -eq 0 ]; then
            print_success "Processor is running"
            screen -list | grep "$SCREEN_NAME"
        else
            print_info "Processor is not running"
        fi
        ;;
    logs)
        print_info "Recent log files:"
        ls -lt ${LOG_DIR}/tcg_*.log 2>/dev/null | head -10
        if [ $? -ne 0 ]; then
            print_warning "No log files found"
        else
            LATEST_LOG=$(ls -t ${LOG_DIR}/tcg_*.log 2>/dev/null | head -1)
            if [ -n "$LATEST_LOG" ]; then
                print_info "Showing last 50 lines of latest log: $LATEST_LOG"
                echo "=========================================="
                tail -50 "$LATEST_LOG"
            fi
        fi
        ;;
    attach)
        screen -r "$SCREEN_NAME"
        if [ $? -ne 0 ]; then
            print_error "No screen session found"
        fi
        ;;
    help|--help|-h)
        show_usage
        ;;
    *)
        print_error "Invalid command: $1"
        show_usage
        exit 1
        ;;
esac