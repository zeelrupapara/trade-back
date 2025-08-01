#!/bin/bash

# Get the PID of trade-back
PID=$(pgrep -f "trade-back server")

if [ -z "$PID" ]; then
    echo "trade-back is not running"
    exit 1
fi

echo "Monitoring goroutines for trade-back (PID: $PID)"
echo "Press Ctrl+C to stop"
echo

# Function to get goroutine count
get_goroutine_count() {
    curl -s http://localhost:8080/debug/pprof/goroutine?debug=1 2>/dev/null | grep -c "goroutine"
}

# Monitor goroutines
while true; do
    COUNT=$(get_goroutine_count)
    TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
    MEM=$(ps -p $PID -o rss= | awk '{print $1/1024 " MB"}')
    CPU=$(ps -p $PID -o %cpu= | awk '{print $1 "%"}')
    
    echo "[$TIMESTAMP] Goroutines: $COUNT | Memory: $MEM | CPU: $CPU"
    
    # Alert if goroutines exceed threshold
    if [ "$COUNT" -gt 1000 ]; then
        echo "WARNING: High goroutine count detected!"
    fi
    
    sleep 5
done