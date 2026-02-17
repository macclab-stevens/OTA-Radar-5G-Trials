#!/bin/bash
# Batch process all gnb.log files in subdirectories

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="${1:-/home/eric/OTA-Experiment-Runs}"

echo "Processing gnb.log files in: $BASE_DIR"

for dir in "$BASE_DIR"/*/; do
    gnb_log=$(find "$dir" -maxdepth 1 -name "*_gnb.log" | head -1)
    
    if [ -f "$gnb_log" ]; then
        echo "Processing: $gnb_log"
        python3 "$SCRIPT_DIR/analyze_harq.py" "$gnb_log" --csv-only
        
        if [ $? -eq 0 ]; then
            echo "✓ Completed: $gnb_log"
        else
            echo "✗ Failed: $gnb_log"
        fi
    fi
done

echo "Batch processing complete!"
