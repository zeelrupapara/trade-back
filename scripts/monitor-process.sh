#!/bin/bash

# Monitor trade-back process resources
while true; do
    PID=$(pgrep -f "trade-back server")
    
    if [ -z "$PID" ]; then
        echo "trade-back is not running"
        sleep 5
        continue
    fi
    
    # Get process stats
    STATS=$(ps -p $PID -o pid,vsz,rss,cpu,etime,nlwp --no-headers)
    TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
    
    # Parse stats
    VSZ=$(echo $STATS | awk '{print $2/1024 " MB"}')
    RSS=$(echo $STATS | awk '{print $3/1024 " MB"}')
    CPU=$(echo $STATS | awk '{print $4 "%"}')
    ETIME=$(echo $STATS | awk '{print $5}')
    THREADS=$(echo $STATS | awk '{print $6}')
    
    # Get Go runtime stats from logs
    GOROUTINES=$(grep -o "goroutines: [0-9]*" server.log 2>/dev/null | tail -1 | awk '{print $2}')
    if [ -z "$GOROUTINES" ]; then
        GOROUTINES="N/A"
    fi
    
    echo "[$TIMESTAMP] PID: $PID | Threads: $THREADS | VSZ: $VSZ | RSS: $RSS | CPU: $CPU | Uptime: $ETIME | Goroutines: $GOROUTINES"
    
    # Alert if threads are excessive
    if [ "$THREADS" -gt 100 ]; then
        echo "WARNING: High thread count detected!"
    fi
    
    sleep 5
done