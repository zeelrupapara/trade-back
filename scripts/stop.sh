#!/bin/bash

# Trade-back server stop script

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Stopping trade-back server...${NC}"

# Find and kill trade-back processes
if pgrep -f "trade-back" > /dev/null; then
    echo -e "${YELLOW}Found running trade-back processes:${NC}"
    ps aux | grep -E "trade-back|main.*server" | grep -v grep
    
    # Kill the processes
    pkill -f "trade-back"
    
    # Also kill any process named 'main' that might be our server
    if pgrep -x "main" > /dev/null; then
        # Check if it's listening on port 8080-8090
        for port in {8080..8090}; do
            pid=$(lsof -ti:$port 2>/dev/null)
            if [ ! -z "$pid" ]; then
                kill -9 $pid 2>/dev/null
            fi
        done
    fi
    
    echo -e "${GREEN}Server stopped!${NC}"
else
    echo -e "${YELLOW}No trade-back server is running.${NC}"
fi

# Check if any common ports are still in use
echo -e "\n${YELLOW}Checking common ports...${NC}"
for port in 8080 8081 8082; do
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
        echo -e "${RED}Port $port is still in use by:${NC}"
        lsof -i :$port
    fi
done