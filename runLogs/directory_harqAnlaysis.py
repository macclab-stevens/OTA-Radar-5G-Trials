#!/usr/bin/env python3
"""
HARQ Analysis Script
Analyzes harq_analysis.csv files across multiple run directories.
"""

import pandas as pd
import os
from pathlib import Path
from collections import defaultdict


def find_start_index(df):
    """
    Find the start index after the first two consecutive 5889 TBS values.
    
    Args:
        df: DataFrame with harq data
        
    Returns:
        Index to start analysis, or None if not found
    """
    tbs_values = df['tbs'].values
    
    for i in range(len(tbs_values) - 1):
        if tbs_values[i] == 5889.0 and tbs_values[i + 1] == 5889.0:
            # Return index after these two consecutive values
            return i + 2
    
    return None


def find_end_index(df, start_idx):
    """
    Find the end index when there are 5 consecutive 233 TBS values.
    
    Args:
        df: DataFrame with harq data
        start_idx: Starting index for the search
        
    Returns:
        Index to end analysis, or None if not found
    """
    tbs_values = df['tbs'].values
    count_233 = 0
    
    for i in range(start_idx, len(tbs_values)):
        if tbs_values[i] == 233.0:
            count_233 += 1
            if count_233 >= 5:
                # Return index of the 5th consecutive 233
                return i + 1
        else:
            count_233 = 0  # Reset counter if not 233
    
    return None


def analyze_harq_file(csv_path):
    """
    Analyze a single harq_analysis.csv file.
    
    Args:
        csv_path: Path to the CSV file
        
    Returns:
        Dictionary with analysis results, or None if analysis couldn't be performed
    """
    try:
        # Read the CSV file (only needed columns for efficiency)
        df = pd.read_csv(csv_path, usecols=['slot_tx', 'ack', 'tbs'])
        
        # Find start index (after first two consecutive 5889 TBS values)
        start_idx = find_start_index(df)
        if start_idx is None:
            return None, 'no_start'
        
        # Find end index (when there are 5 consecutive 233 TBS values)
        end_idx = find_end_index(df, start_idx)
        if end_idx is None:
            return None, 'no_end'
        
        # Extract the relevant portion of data
        analysis_df = df.iloc[start_idx:end_idx].copy()
        
        # Extract slot number from slot_tx (fractional part)
        # For example: 299.10 -> 10, 800.01 -> 1
        analysis_df['slot'] = (analysis_df['slot_tx'] % 1 * 100).round().astype(int)
        
        # Count ack types (0 and 2 only) for each slot
        # Filter to only ack 0 and 2 for faster processing
        filtered_df = analysis_df[analysis_df['ack'].isin([0, 2])]
        
        results = {}
        grouped = filtered_df.groupby(['slot', 'ack']).size().unstack(fill_value=0)
        
        for slot in grouped.index:
            ack_0_count = grouped.loc[slot, 0] if 0 in grouped.columns else 0
            ack_2_count = grouped.loc[slot, 2] if 2 in grouped.columns else 0
            
            results[slot] = {
                'ack_0': int(ack_0_count),
                'ack_2': int(ack_2_count),
                'total': int(ack_0_count + ack_2_count)
            }
        
        return ({
            'start_idx': start_idx,
            'end_idx': end_idx,
            'total_rows_analyzed': end_idx - start_idx,
            'slot_results': results
        }, None)
        
    except Exception as e:
        return None, f'error: {str(e)}'


def analyze_all_runs(base_dir, max_runs=1000):
    """
    Analyze harq_analysis.csv files in all run directories.
    
    Args:
        base_dir: Base directory containing run folders
        max_runs: Maximum number of run directories to process
    """
    base_path = Path(base_dir)
    all_results = {}
    
    # Track failure reasons
    no_start_count = 0
    no_end_count = 0
    
    # Find all run directories
    run_dirs = sorted([d for d in base_path.iterdir() if d.is_dir() and d.name.startswith('run')])
    
    print(f"Found {len(run_dirs)} run directories")
    print(f"Processing up to {max_runs} runs...\n")
    
    processed_count = 0
    success_count = 0
    
    for run_dir in run_dirs[:max_runs]:
        # Find harq_analysis.csv files in subdirectories
        harq_files = list(run_dir.glob('*/harq_analysis.csv'))
        
        if not harq_files:
            print(f"Skipping {run_dir.name}: No harq_analysis.csv found")
            continue
        
        for harq_file in harq_files:
            processed_count += 1
            print(f"[{processed_count}/{min(max_runs, len(run_dirs))}] Processing: {run_dir.name}/{harq_file.parent.name}/harq_analysis.csv", flush=True)
            
            result, failure_reason = analyze_harq_file(harq_file)
            
            if result:
                success_count += 1
                all_results[str(harq_file.relative_to(base_path))] = result
                # Calculate totals for display
                slot_results = result['slot_results']
                total_ack_0 = sum(s['ack_0'] for s in slot_results.values())
                total_ack_2 = sum(s['ack_2'] for s in slot_results.values())
                print(f"  ✓ Rows: {result['total_rows_analyzed']}, Slots: {len(slot_results)}, ACK0: {total_ack_0}, ACK2: {total_ack_2}", flush=True)
            else:
                # Track failure reasons
                if failure_reason == 'no_end':
                    no_end_count += 1
                elif failure_reason == 'no_start':
                    no_start_count += 1
                print(f"  ✗ Failed ({failure_reason})", flush=True)
    
    # Summary
    print("=" * 80)
    print(f"SUMMARY:")
    print(f"  Total files processed: {processed_count}")
    print(f"  Successful analyses: {success_count}")
    print(f"  Failed analyses: {processed_count - success_count}")
    print(f"    - No start condition (2x consecutive 5889): {no_start_count}")
    print(f"    - No end condition (5x consecutive 233): {no_end_count}")
    print("=" * 80)
    
    return all_results


def save_summary_results(results, output_file='harq_analysis_summary.csv'):
    """
    Save summary results to a CSV file with one row per slot (0-19) per run.
    
    Args:
        results: Dictionary of analysis results
        output_file: Output CSV filename
    """
    summary_data = []
    
    for file_path, result in results.items():
        run_name = Path(file_path).parts[0]
        timestamp = Path(file_path).parts[1]
        
        # Create a row for each slot 0-19
        for slot in range(20):
            slot_data = result['slot_results'].get(slot, {'ack_0': 0, 'ack_2': 0})
            
            summary_data.append({
                'run': run_name,
                'timestamp': timestamp,
                'slot': slot,
                'ack_0': slot_data['ack_0'],
                'ack_2': slot_data['ack_2'],
                'total_acks': slot_data['ack_0'] + slot_data['ack_2']
            })
    
    summary_df = pd.DataFrame(summary_data)
    summary_df.to_csv(output_file, index=False)
    print(f"\nDetailed results saved to: {output_file}")
    print(f"Total rows: {len(summary_data)} (20 slots x {len(results)} runs)")


if __name__ == "__main__":
    # Get the directory where this script is located
    script_dir = Path(__file__).parent
    
    print("HARQ Analysis Script")
    print("=" * 80)
    print(f"Working directory: {script_dir}")
    print()
    
    # Run analysis on all subdirectories
    results = analyze_all_runs(script_dir, max_runs=1000)
    
    # Save summary results
    if results:
        output_path = script_dir / 'harq_analysis_summary.csv'
        save_summary_results(results, output_path)
        print(f"\nAnalysis complete! Processed {len(results)} files successfully.")
    else:
        print("\nNo results to save.")
