#!/usr/bin/env python3
"""
Batch Radar Config Extractor

This script recursively searches for gnb.log files in all subfolders
and extracts the Radar_Char configuration line to individual CSV files.

Usage:
    python3 batch_extract_radar_config.py <root_folder>
    
Example:
    python3 batch_extract_radar_config.py ./radarPW005t5000-5x-results/
"""

import os
import sys
import glob
import re
import csv
from pathlib import Path


def extract_radar_config_from_log(gnb_log_path):
    """
    Extract radar configuration from gnb.log file.
    Looks for the "Radar_Char" line at the end of the log.
    
    Returns:
        dict: Radar configuration parameters, or None if not found
    """
    try:
        with open(gnb_log_path, 'r') as f:
            lines = f.readlines()
        
        # Search backwards from end of file for Radar_Char line
        for line in reversed(lines):
            if 'Radar_Char' in line:
                # Parse the radar config line
                # Format: Radar_Char,prf=1000,gain=80,waveform=custom,...
                
                # Remove whitespace and split by commas
                parts = line.strip().split(',')
                
                if len(parts) < 2:
                    continue
                
                # First part should be "Radar_Char"
                if parts[0].strip() != 'Radar_Char':
                    continue
                
                # Parse key=value pairs
                config = {}
                for part in parts[1:]:
                    if '=' in part:
                        key, value = part.split('=', 1)
                        config[key.strip()] = value.strip()
                
                return config
        
        return None
    
    except Exception as e:
        print(f"  Error reading {gnb_log_path}: {e}")
        return None


def write_radar_config_csv(config, output_csv_path):
    """
    Write radar configuration to CSV file.
    
    Args:
        config: dict of radar parameters
        output_csv_path: path to output CSV file
    """
    try:
        # Write CSV with header and one data row
        with open(output_csv_path, 'w', newline='') as f:
            writer = csv.writer(f)
            
            # Write header
            writer.writerow(config.keys())
            
            # Write values
            writer.writerow(config.values())
        
        return True
    
    except Exception as e:
        print(f"  Error writing {output_csv_path}: {e}")
        return False


def find_gnb_logs_recursive(root_folder):
    """
    Recursively find all gnb.log files (with any prefix) in subfolders.
    
    Returns:
        list: List of gnb.log file paths
    """
    gnb_log_files = []
    
    # Search for files matching *gnb.log pattern recursively
    pattern = os.path.join(root_folder, '**', '*gnb.log')
    gnb_log_files = glob.glob(pattern, recursive=True)
    
    # Also check for files named exactly "gnb.log"
    pattern2 = os.path.join(root_folder, '**', 'gnb.log')
    gnb_log_files.extend(glob.glob(pattern2, recursive=True))
    
    # Remove duplicates
    gnb_log_files = list(set(gnb_log_files))
    
    return sorted(gnb_log_files)


def process_folder(root_folder):
    """
    Process all subfolders and extract radar configurations.
    
    Args:
        root_folder: Root directory to search for gnb.log files
    """
    print("=" * 70)
    print("Batch Radar Config Extractor")
    print("=" * 70)
    print(f"\nRoot folder: {root_folder}")
    
    # Find all gnb.log files
    print("\nSearching for gnb.log files...")
    gnb_log_files = find_gnb_logs_recursive(root_folder)
    
    if not gnb_log_files:
        print(f"\nNo gnb.log files found in '{root_folder}' or its subfolders.")
        return
    
    print(f"Found {len(gnb_log_files)} gnb.log file(s)\n")
    
    # Process each file
    success_count = 0
    failed_count = 0
    no_radar_count = 0
    
    for gnb_log_path in gnb_log_files:
        # Get relative path for display
        rel_path = os.path.relpath(gnb_log_path, root_folder)
        print(f"Processing: {rel_path}")
        
        # Extract radar config
        config = extract_radar_config_from_log(gnb_log_path)
        
        if config is None:
            print(f"  ⚠️  No radar configuration found")
            no_radar_count += 1
            continue
        
        # Determine output CSV path (same folder as gnb.log)
        folder = os.path.dirname(gnb_log_path)
        output_csv = os.path.join(folder, 'radar_config.csv')
        
        # Write CSV
        if write_radar_config_csv(config, output_csv):
            print(f"  ✓ Created: {os.path.basename(output_csv)}")
            print(f"     Config: {', '.join([f'{k}={v}' for k, v in config.items()])}")
            success_count += 1
        else:
            print(f"  ✗ Failed to write CSV")
            failed_count += 1
    
    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Total gnb.log files found: {len(gnb_log_files)}")
    print(f"  ✓ Successfully extracted: {success_count}")
    print(f"  ⚠️  No radar config found: {no_radar_count}")
    print(f"  ✗ Failed: {failed_count}")
    print()


def main():
    """
    Main entry point.
    """
    if len(sys.argv) < 2:
        print("\nUsage: python3 batch_extract_radar_config.py <root_folder>")
        print("\nExample:")
        print("  python3 batch_extract_radar_config.py ./radarPW005t5000-5x-results/")
        print("\nThis script will:")
        print("  1. Recursively search for all *gnb.log files in subfolders")
        print("  2. Extract the Radar_Char configuration line from each")
        print("  3. Create a radar_config.csv file in each subfolder")
        print()
        sys.exit(1)
    
    root_folder = sys.argv[1]
    
    # Check if folder exists
    if not os.path.exists(root_folder):
        print(f"\nError: Folder '{root_folder}' does not exist!")
        sys.exit(1)
    
    if not os.path.isdir(root_folder):
        print(f"\nError: '{root_folder}' is not a directory!")
        sys.exit(1)
    
    # Process the folder
    process_folder(root_folder)


if __name__ == "__main__":
    main()
