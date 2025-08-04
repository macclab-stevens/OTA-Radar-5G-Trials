#!/bin/bash

INPUT_ROOT="runs-20250724"
OUTPUT_ROOT="processed_logs_20250724"

mkdir -p "$OUTPUT_ROOT"

find "$INPUT_ROOT" -type f -name '*_gnb.log' | while read -r gnb_log; do
    # Find matching iperf3 log in the same folder
    dir=$(dirname "$gnb_log")
    base=$(basename "$gnb_log" _gnb.log)
    iperf_log="$dir/${base}_iperf3.log"
    prefix="$base"
    if [[ -f "$iperf_log" ]]; then
        echo "Processing $gnb_log and $iperf_log"
        python3 LogProcessing.py \
            --gnb-log "$gnb_log" \
            --iperf-log "$iperf_log" \
            --out-dir "$OUTPUT_ROOT" \
            --prefix "$prefix"
    else
        echo "Warning: No matching iperf3 log for $gnb_log"
    fi
done