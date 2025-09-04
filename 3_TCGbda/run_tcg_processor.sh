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
    print_info "Starting TCG Data Processor..."
    print_info "Screen session: $SCREEN_NAME"
    print_info "Log file: $LOG_FILE"
    
    # Parse command line arguments
    MODE="${1:-replace}"
    DIRECTORY="${2:-./product_details}"
    
    print_info "Mode: $MODE"
    print_info "Data directory: $DIRECTORY"
    
    # Create the command to run
    CMD="python3 $PYTHON_SCRIPT --mode $MODE --directory $DIRECTORY 2>&1 | tee $LOG_FILE"
    
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
    echo "  start [mode] [directory]  Start the processor (mode: replace/append, default: replace)"
    echo "  stop                      Stop the processor"
    echo "  status                    Check processor status"
    echo "  logs                      View recent logs"
    echo "  attach                    Attach to screen session"
    echo "  help                      Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 start                  # Start with default settings (replace mode)"
    echo "  $0 start append           # Start in append mode"
    echo "  $0 start replace /data    # Start with custom data directory"
    echo "  $0 stop                   # Stop the processor"
    echo "  $0 status                 # Check if processor is running"
    echo "  $0 logs                   # View recent log files"
}

# Main execution
case "${1:-start}" in
    start)
        check_requirements
        check_existing_session
        start_processor "${2:-replace}" "${3:-./product_details}"
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