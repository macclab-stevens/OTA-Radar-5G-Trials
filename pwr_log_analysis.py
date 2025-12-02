#!/usr/bin/env python3
"""
Power Log Analysis Script
Analyzes throughput vs radar gain from OTA experiment logs
"""

import os
import re
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import argparse

def extract_radar_params_from_gnb_log(gnb_log_path):
    """Extract radar parameters from the gnb log file"""
    params = {}
    try:
        with open(gnb_log_path, 'r') as f:
            # Read last few lines where radar params are appended
            lines = f.readlines()
            for line in reversed(lines):
                if 'Radar_Char' in line:
                    # Parse: Radar_Char,prf=200,gain=80,cFreq=3417100000.0,PW=0.0001,T=20,bw=2000000.0,sampRate=20000000.0
                    parts = line.strip().split(',')[1:]  # Skip 'Radar_Char'
                    for part in parts:
                        if '=' in part:
                            key, value = part.split('=')
                            try:
                                params[key] = float(value)
                            except:
                                params[key] = value
                    break
    except Exception as e:
        print(f"Error extracting radar params from {gnb_log_path}: {e}")
    return params

def create_radar_params_csv(log_folder):
    """Create radar_params.csv for a log folder if it doesn't exist"""
    csv_path = os.path.join(log_folder, 'radar_params.csv')
    
    # Check if already exists
    if os.path.exists(csv_path):
        return csv_path
    
    # Find gnb log file
    gnb_logs = [f for f in os.listdir(log_folder) if f.endswith('_gnb.log')]
    if not gnb_logs:
        print(f"No gnb log found in {log_folder}")
        return None
    
    gnb_log_path = os.path.join(log_folder, gnb_logs[0])
    params = extract_radar_params_from_gnb_log(gnb_log_path)
    
    if not params:
        print(f"Could not extract radar params from {gnb_log_path}")
        return None
    
    # Create CSV
    try:
        with open(csv_path, 'w') as f:
            f.write(','.join(params.keys()) + '\n')
            f.write(','.join(str(v) for v in params.values()) + '\n')
        print(f"Created {csv_path}")
        return csv_path
    except Exception as e:
        print(f"Error creating CSV: {e}")
        return None

def read_iperf_csv(iperf_csv_path):
    """Read iperf CSV and calculate average throughput"""
    try:
        df = pd.read_csv(iperf_csv_path)
        if 'Bitrate' in df.columns:
            # Convert bitrate to Mbps if needed
            avg_throughput = df['Bitrate'].mean()
            return avg_throughput
        return None
    except Exception as e:
        print(f"Error reading {iperf_csv_path}: {e}")
        return None

def read_metrics_csv(metrics_csv_path):
    """Read metrics CSV and extract total_dl_brate values"""
    try:
        df = pd.read_csv(metrics_csv_path)
        if 'total_dl_brate' in df.columns:
            # Filter out empty or NaN values
            brates = df['total_dl_brate'].dropna()
            brates = brates[brates != '']
            
            # Convert to Mbps
            brates_mbps = []
            for rate in brates:
                if isinstance(rate, str):
                    rate_str = rate.strip()
                    if rate_str == '' or rate_str == '0.0bps':
                        continue
                    
                    # Parse the rate string
                    if 'Mbps' in rate_str:
                        value = float(rate_str.replace('Mbps', ''))
                        brates_mbps.append(value)
                    elif 'kbps' in rate_str or 'kBps' in rate_str:
                        value = float(rate_str.replace('kbps', '').replace('kBps', ''))
                        brates_mbps.append(value / 1000.0)  # Convert to Mbps
                    elif 'bps' in rate_str:
                        value = float(rate_str.replace('bps', ''))
                        brates_mbps.append(value / 1e6)  # Convert to Mbps
                elif isinstance(rate, (int, float)):
                    brates_mbps.append(float(rate))
            
            return brates_mbps
        return []
    except Exception as e:
        print(f"Error reading {metrics_csv_path}: {e}")
        return []

def analyze_logs_folder(base_folder):
    """Analyze all log folders in the base folder"""
    results = []
    
    # Find all log folders
    log_folders = sorted([d for d in os.listdir(base_folder) 
                         if os.path.isdir(os.path.join(base_folder, d)) and d.startswith('logs_')])
    
    print(f"Found {len(log_folders)} log folders")
    
    for log_folder in log_folders:
        log_path = os.path.join(base_folder, log_folder)
        print(f"\nProcessing {log_folder}...")
        
        # Create radar_params.csv if it doesn't exist
        radar_csv = create_radar_params_csv(log_path)
        
        # Read radar parameters
        if radar_csv and os.path.exists(radar_csv):
            radar_df = pd.read_csv(radar_csv)
            if len(radar_df) > 0:
                gain = radar_df['gain'].iloc[0] if 'gain' in radar_df.columns else None
                prf = radar_df['prf'].iloc[0] if 'prf' in radar_df.columns else None
            else:
                gain, prf = None, None
        else:
            gain, prf = None, None
        
        # Find iperf CSV
        iperf_csvs = [f for f in os.listdir(log_path) if 'iperf' in f.lower() and f.endswith('.csv')]
        
        if iperf_csvs:
            iperf_csv_path = os.path.join(log_path, iperf_csvs[0])
            avg_throughput = read_iperf_csv(iperf_csv_path)
            
            if gain is not None and avg_throughput is not None:
                results.append({
                    'folder': log_folder,
                    'gain': gain,
                    'prf': prf,
                    'avg_throughput_mbps': avg_throughput
                })
                print(f"  Gain: {gain}, PRF: {prf}, Avg Throughput: {avg_throughput:.2f} Mbps")
        else:
            print(f"  No iperf CSV found")
        
        # Find metrics CSV for total_dl_brate
        metrics_csvs = [f for f in os.listdir(log_path) if 'metrics' in f.lower() and f.endswith('.csv')]
        if metrics_csvs and gain is not None:
            metrics_csv_path = os.path.join(log_path, metrics_csvs[0])
            dl_brates = read_metrics_csv(metrics_csv_path)
            
            # Add each dl_brate measurement as a separate entry
            for brate in dl_brates:
                results.append({
                    'folder': log_folder,
                    'gain': gain,
                    'prf': prf,
                    'metric_type': 'dl_brate',
                    'dl_brate_mbps': brate
                })
    
    return pd.DataFrame(results)

def plot_throughput_vs_gain(results_df, output_dir):
    """Plot throughput vs radar gain"""
    if results_df.empty:
        print("No data to plot")
        return
    
    # Filter for iperf data only
    iperf_df = results_df[results_df['avg_throughput_mbps'].notna()].copy()
    
    # Sort by gain
    iperf_df = iperf_df.sort_values('gain')
    
    # Group by gain and calculate statistics
    grouped = iperf_df.groupby('gain')['avg_throughput_mbps'].agg(['mean', 'std', 'count']).reset_index()
    
    # Create figure
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Plot all individual points
    ax.scatter(iperf_df['gain'], iperf_df['avg_throughput_mbps'], 
              alpha=0.5, s=50, label='Individual runs', color='lightblue')
    
    # Plot mean with error bars
    ax.errorbar(grouped['gain'], grouped['mean'], yerr=grouped['std'],
               fmt='o-', linewidth=2, markersize=8, capsize=5,
               label='Mean ± Std Dev', color='darkblue')
    
    ax.set_xlabel('Radar Gain (dB)', fontsize=14)
    ax.set_ylabel('Average Throughput (Mbps)', fontsize=14)
    ax.set_title('5G Throughput vs Radar Gain', fontsize=16, fontweight='bold')
    ax.grid(True, alpha=0.3)
    ax.legend(fontsize=12)
    
    # Add text with total number of runs
    total_runs = len(iperf_df)
    ax.text(0.02, 0.98, f'Total runs: {total_runs}\nGain values: {len(grouped)}',
           transform=ax.transAxes, verticalalignment='top',
           bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    
    plt.tight_layout()
    
    # Save plot
    plot_path = os.path.join(output_dir, 'throughput_vs_gain.png')
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    print(f"\nPlot saved to: {plot_path}")
    
    # Save summary statistics
    summary_path = os.path.join(output_dir, 'throughput_summary.csv')
    grouped.to_csv(summary_path, index=False)
    print(f"Summary statistics saved to: {summary_path}")
    
    plt.show()

def plot_dl_brate_vs_gain(results_df, output_dir):
    """Plot downlink bitrate from metrics vs radar gain"""
    # Filter for dl_brate data only
    dl_df = results_df[results_df['dl_brate_mbps'].notna()].copy()
    
    if dl_df.empty:
        print("No DL bitrate data to plot")
        return
    
    # Sort by gain
    dl_df = dl_df.sort_values('gain')
    
    # Group by gain and calculate statistics
    grouped = dl_df.groupby('gain')['dl_brate_mbps'].agg(['mean', 'std', 'count', 'median']).reset_index()
    
    # Create figure
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Plot all individual points (with transparency due to large number)
    ax.scatter(dl_df['gain'], dl_df['dl_brate_mbps'], 
              alpha=0.1, s=20, label='Individual samples', color='lightcoral')
    
    # # Plot mean with error bars
    # ax.errorbar(grouped['gain'], grouped['mean'], yerr=grouped['std'],
    #            fmt='o-', linewidth=2, markersize=8, capsize=5,
    #            label='Mean ± Std Dev', color='darkred', zorder=10)
    
    # # Plot median as well
    # ax.plot(grouped['gain'].values, grouped['median'].values, 
    #        'g^--', linewidth=1.5, markersize=6, 
    #        label='Median', zorder=9)
    
    ax.set_xlabel('Radar Gain (dB)', fontsize=14)
    ax.set_ylabel('Downlink Bitrate (Mbps)', fontsize=14)
    ax.set_title('5G Downlink Bitrate vs Radar Gain (from Metrics)', fontsize=16, fontweight='bold')
    ax.grid(True, alpha=0.3)
    ax.legend(fontsize=12)
    
    # Add text with total number of samples
    total_samples = len(dl_df)
    ax.text(0.02, 0.98, f'Total samples: {total_samples}\nGain values: {len(grouped)}',
           transform=ax.transAxes, verticalalignment='top',
           bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    
    plt.tight_layout()
    
    # Save plot
    plot_path = os.path.join(output_dir, 'dl_brate_vs_gain-noBars.png')
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    print(f"\nDL Bitrate plot saved to: {plot_path}")
    
    # Save summary statistics
    summary_path = os.path.join(output_dir, 'dl_brate_summary.csv')
    grouped.to_csv(summary_path, index=False)
    print(f"DL Bitrate summary saved to: {summary_path}")
    
    plt.show()

def main():
    parser = argparse.ArgumentParser(description='Analyze throughput vs radar gain from OTA logs')
    parser.add_argument('--folder', type=str, default='./radarPwr80t0-5x',
                       help='Base folder containing log folders')
    parser.add_argument('--output', type=str, default=None,
                       help='Output directory for plots and results (default: same as input folder)')
    
    args = parser.parse_args()
    
    base_folder = args.folder
    output_dir = args.output if args.output else base_folder
    
    if not os.path.exists(base_folder):
        print(f"Error: Folder {base_folder} does not exist")
        return
    
    print(f"Analyzing logs in: {base_folder}")
    print(f"Output directory: {output_dir}\n")
    
    # Analyze all log folders
    results_df = analyze_logs_folder(base_folder)
    
    if not results_df.empty:
        print(f"\n{'='*60}")
        print(f"Analysis complete! Found {len(results_df)} valid results")
        print(f"{'='*60}\n")
        print(results_df.to_string(index=False))
        
        # Save all results
        results_path = os.path.join(output_dir, 'all_results.csv')
        results_df.to_csv(results_path, index=False)
        print(f"\nAll results saved to: {results_path}")
        
        # Generate throughput plot
        plot_throughput_vs_gain(results_df, output_dir)
        
        # Generate DL bitrate plot
        plot_dl_brate_vs_gain(results_df, output_dir)
    else:
        print("No valid results found")

if __name__ == '__main__':
    main()
