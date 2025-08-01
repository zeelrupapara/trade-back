#!/bin/bash

# Trade-back server start script

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default port
PORT=${PORT:-8080}

# Check if custom port is provided
if [ "$1" == "--port" ] && [ -n "$2" ]; then
    PORT=$2
fi

echo -e "${YELLOW}Starting trade-back server on port $PORT...${NC}"

# Check if port is already in use
if lsof -Pi :$PORT -sTCP:LISTEN -t >/dev/null ; then
    echo -e "${RED}Port $PORT is already in use!${NC}"
    echo "Current process using port $PORT:"
    lsof -i :$PORT
    
    echo -e "\n${YELLOW}Options:${NC}"
    echo "1. Kill the process and start fresh (y)"
    echo "2. Use a different port (n)"
    echo -n "Kill the existing process? [y/n]: "
    read -r response
    
    if [[ "$response" =~ ^[Yy]$ ]]; then
        echo -e "${YELLOW}Killing process on port $PORT...${NC}"
        lsof -ti:$PORT | xargs -r kill -9
        sleep 1
    else
        echo -e "${YELLOW}Please specify a different port:${NC}"
        echo "Example: $0 --port 8081"
        exit 1
    fi
fi

# Build the application
echo -e "${YELLOW}Building application...${NC}"
if go build -o trade-back main.go; then
    echo -e "${GREEN}Build successful!${NC}"
else
    echo -e "${RED}Build failed!${NC}"
    exit 1
fi

# Start the server
echo -e "${GREEN}Starting server on port $PORT...${NC}"
./trade-back server --port $PORT