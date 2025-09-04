#!/bin/bash
# PSA Scraper Log Cleanup and Rotation Script
# Manages log files and disk space

LOG_DIR="logs"
MAX_LOG_AGE_DAYS=30
MAX_LOG_SIZE_MB=100

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}🧹 PSA Scraper Log Cleanup${NC}"
echo -e "${BLUE}===========================${NC}"

# Create logs directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Function to convert bytes to MB
bytes_to_mb() {
    echo "scale=2; $1 / 1024 / 1024" | bc
}

# Function to get file size in bytes
get_file_size() {
    stat -f%z "$1" 2>/dev/null || stat -c%s "$1" 2>/dev/null || echo 0
}

echo -e "${YELLOW}📊 Current log directory status:${NC}"
if [ -d "$LOG_DIR" ]; then
    TOTAL_SIZE=$(du -sb "$LOG_DIR" 2>/dev/null | cut -f1)
    TOTAL_SIZE_MB=$(bytes_to_mb $TOTAL_SIZE)
    FILE_COUNT=$(find "$LOG_DIR" -type f | wc -l)
    echo -e "${CYAN}Directory: $LOG_DIR${NC}"
    echo -e "${CYAN}Total size: ${TOTAL_SIZE_MB} MB${NC}"
    echo -e "${CYAN}File count: $FILE_COUNT${NC}"
else
    echo -e "${YELLOW}Log directory doesn't exist yet${NC}"
    exit 0
fi

echo ""
echo -e "${YELLOW}🗑️ Cleaning up old log files...${NC}"

# Remove logs older than MAX_LOG_AGE_DAYS
DELETED_COUNT=0
while IFS= read -r -d '' file; do
    if [ -f "$file" ]; then
        echo -e "${RED}Deleting old log: $(basename "$file")${NC}"
        rm "$file"
        ((DELETED_COUNT++))
    fi
done < <(find "$LOG_DIR" -type f -name "*.log" -mtime +$MAX_LOG_AGE_DAYS -print0 2>/dev/null)

if [ $DELETED_COUNT -gt 0 ]; then
    echo -e "${GREEN}✅ Deleted $DELETED_COUNT old log files${NC}"
else
    echo -e "${GREEN}✅ No old log files to delete${NC}"
fi

echo ""
echo -e "${YELLOW}📦 Compressing large log files...${NC}"

# Compress logs larger than MAX_LOG_SIZE_MB
COMPRESSED_COUNT=0
while IFS= read -r -d '' file; do
    if [ -f "$file" ]; then
        SIZE_BYTES=$(get_file_size "$file")
        SIZE_MB=$(bytes_to_mb $SIZE_BYTES)
        SIZE_MB_INT=$(echo "$SIZE_MB" | cut -d. -f1)
        
        if [ "$SIZE_MB_INT" -gt "$MAX_LOG_SIZE_MB" ]; then
            echo -e "${YELLOW}Compressing large log: $(basename "$file") (${SIZE_MB} MB)${NC}"
            gzip "$file"
            if [ $? -eq 0 ]; then
                ((COMPRESSED_COUNT++))
                echo -e "${GREEN}✅ Compressed: $(basename "$file").gz${NC}"
            else
                echo -e "${RED}❌ Failed to compress: $(basename "$file")${NC}"
            fi
        fi
    fi
done < <(find "$LOG_DIR" -type f -name "*.log" -print0 2>/dev/null)

if [ $COMPRESSED_COUNT -gt 0 ]; then
    echo -e "${GREEN}✅ Compressed $COMPRESSED_COUNT large log files${NC}"
else
    echo -e "${GREEN}✅ No large log files to compress${NC}"
fi

echo ""
echo -e "${YELLOW}📈 Final log directory status:${NC}"
FINAL_SIZE=$(du -sb "$LOG_DIR" 2>/dev/null | cut -f1)
FINAL_SIZE_MB=$(bytes_to_mb $FINAL_SIZE)
FINAL_COUNT=$(find "$LOG_DIR" -type f | wc -l)
echo -e "${CYAN}Total size: ${FINAL_SIZE_MB} MB${NC}"
echo -e "${CYAN}File count: $FINAL_COUNT${NC}"

# Show savings
if [ $TOTAL_SIZE -gt 0 ]; then
    SAVINGS=$(echo "scale=2; ($TOTAL_SIZE - $FINAL_SIZE) / 1024 / 1024" | bc)
    echo -e "${GREEN}💾 Space saved: ${SAVINGS} MB${NC}"
fi

echo ""
echo -e "${BLUE}🎯 Log cleanup completed${NC}"

# List current log files
echo ""
echo -e "${YELLOW}📋 Current log files:${NC}"
find "$LOG_DIR" -type f \( -name "*.log" -o -name "*.log.gz" \) -exec ls -lh {} \; | while read line; do
    if echo "$line" | grep -q "\.gz"; then
        echo -e "${BLUE}$line${NC}"
    else
        echo -e "${CYAN}$line${NC}"
    fi
done