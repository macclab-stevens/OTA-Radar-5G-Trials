#!/usr/bin/env python3
"""
HARQ Analysis Script v2 for 5G gNB Logs

This script analyzes UL PHY scheduler logs to identify:
- PUCCH status (valid/invalid) per slot
- PUSCH CRC status (OK/FAIL) per slot
- Complete slot timeline showing all scheduled transmissions
"""

import re
import pandas as pd
import argparse
from collections import defaultdict
from pathlib import Path

def format_slot(slot_string):
    """
    Format slot as string with zero-padded slot number to preserve .10 vs .1.
    slot_string format: "frame.slot" as string from regex (e.g., "295.1" or "295.10")
    The decimal part directly represents the slot number (0-19).
    Returns: string with format "frame.SS" where SS is zero-padded (e.g., "295.01" or "295.10")
    """
    if slot_string is None:
        return None
    # Parse as string to preserve the original slot number
    parts = slot_string.split('.')
    if len(parts) != 2:
        return slot_string  # Return as-is if format is unexpected
    
    frame = int(parts[0])
    slot = int(parts[1])  # This will correctly parse "1" as 1 and "10" as 10
    return f"{frame}.{slot:02d}"

def parse_ul_phy_logs(log_file):
    """
    Parse gnb.log file to extract UL PHY PUCCH and PUSCH information with status.
    """
    pucch_data = []
    pusch_data = []
    current_timestamp = None
    
    with open(log_file, 'r') as f:
        for line in f:
            # Extract timestamp from lines that have it
            ts_match = re.match(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+)', line)
            if ts_match:
                current_timestamp = ts_match.group(1)
            
            # Parse PUCCH PHY lines
            # Example: 2026-01-23T20:00:22.264797 [PHY     ] [D] [  743.18] PUCCH: rnti=0x4601 format=1 prb1=0 prb2=50 symb=[0, 14) cs=0 occ=0 ack=1 metric=1075.6 sinr=18.6dB t=30.1us
            if '[PHY     ] [D]' in line and 'PUCCH:' in line:
                slot_match = re.search(r'\[\s*([\d.]+)\]\s+PUCCH:', line)
                rnti_match = re.search(r'rnti=(0x[0-9a-fA-F]+)', line)
                format_match = re.search(r'format=(\d+)', line)
                ack_match = re.search(r'ack=(\d+)', line)
                sr_match = re.search(r'sr=(yes|no)', line)
                csi_match = re.search(r'csi1?=([01]+)', line)
                sinr_match = re.search(r'sinr=([-\d.]+)dB', line)
                metric_match = re.search(r'metric=([\d.]+)', line)
                
                if slot_match and rnti_match:
                    slot = slot_match.group(1)
                    # Determine PUCCH status based on metric and SINR
                    # Low metric (<1.0) and very low SINR typically indicates invalid
                    status = 'valid'
                    metric = float(metric_match.group(1)) if metric_match else None
                    sinr = float(sinr_match.group(1)) if sinr_match else None
                    
                    # Heuristic: invalid if metric is very low or SINR is very negative
                    if metric is not None and metric < 1.0:
                        status = 'invalid'
                    elif sinr is not None and sinr < -10.0:
                        status = 'invalid'
                    
                    # Extract UCI content
                    uci_type = None
                    uci_value = None
                    if ack_match:
                        uci_type = 'ack'
                        uci_value = ack_match.group(1)
                    elif sr_match:
                        uci_type = 'sr'
                        uci_value = sr_match.group(1)
                    elif csi_match:
                        uci_type = 'csi'
                        uci_value = csi_match.group(1)
                    
                    pucch_data.append({
                        'time': current_timestamp,
                        'slot': format_slot(slot),
                        'rnti': rnti_match.group(1),
                        'format': int(format_match.group(1)) if format_match else None,
                        'uci_type': uci_type,
                        'uci_value': uci_value,
                        'metric': metric,
                        'sinr': sinr,
                        'status': status,
                        'type': 'PUCCH'
                    })
            
            # Parse PUSCH PHY lines
            # Example: 2026-01-23T20:00:22.254903 [PHY     ] [D] [  742.18] PUSCH: rnti=0x4601 h_id=0 prb=[8, 11) symb=[0, 14) mod=QPSK rv=0 tbs=11 crc=OK iter=1.0 sinr=30.0dB t=197.8us uci_t=0.0us ret_t=0.0us
            if '[PHY     ] [D]' in line and 'PUSCH:' in line:
                slot_match = re.search(r'\[\s*([\d.]+)\]\s+PUSCH:', line)
                rnti_match = re.search(r'rnti=(0x[0-9a-fA-F]+)', line)
                h_id_match = re.search(r'h_id=(\d+)', line)
                tbs_match = re.search(r'tbs=(\d+)', line)
                crc_match = re.search(r'crc=(OK|FAIL)', line)
                sinr_match = re.search(r'sinr=([-\d.]+)dB', line)
                iter_match = re.search(r'iter=([\d.]+)', line)
                
                if slot_match and rnti_match and crc_match:
                    pusch_data.append({
                        'time': current_timestamp,
                        'slot': format_slot(slot_match.group(1)),
                        'rnti': rnti_match.group(1),
                        'h_id': int(h_id_match.group(1)) if h_id_match else None,
                        'tbs': int(tbs_match.group(1)) if tbs_match else None,
                        'crc': crc_match.group(1),
                        'sinr': float(sinr_match.group(1)) if sinr_match else None,
                        'iterations': float(iter_match.group(1)) if iter_match else None,
                        'type': 'PUSCH'
                    })
    
    return pucch_data, pusch_data

def create_slot_timeline(pucch_df, pusch_df):
    """
    Create a comprehensive timeline showing status for all slots.
    Groups multiple PUCCH/PUSCH entries per slot into single row.
    """
    # Get all unique slots from both dataframes
    all_slots = set()
    if not pucch_df.empty:
        all_slots.update(pucch_df['slot'].unique())
    if not pusch_df.empty:
        all_slots.update(pusch_df['slot'].unique())
    
    # Sort slots
    all_slots = sorted(all_slots, key=lambda x: float(x))
    
    timeline_data = []
    
    for slot in all_slots:
        # Get PUCCH entries for this slot
        pucch_slot = pucch_df[pucch_df['slot'] == slot] if not pucch_df.empty else pd.DataFrame()
        pusch_slot = pusch_df[pusch_df['slot'] == slot] if not pusch_df.empty else pd.DataFrame()
        
        # Extract frame and slot number
        frame, slot_num = slot.split('.')
        
        # Aggregate PUCCH data
        pucch_count = len(pucch_slot)
        pucch_valid = len(pucch_slot[pucch_slot['status'] == 'valid']) if pucch_count > 0 else 0
        pucch_invalid = len(pucch_slot[pucch_slot['status'] == 'invalid']) if pucch_count > 0 else 0
        pucch_status = '-'
        if pucch_count > 0:
            if pucch_invalid > 0:
                pucch_status = f'{pucch_valid}v/{pucch_invalid}i'
            else:
                pucch_status = f'{pucch_valid}v'
        
        # Get PUCCH UCI info
        pucch_uci_info = []
        if pucch_count > 0:
            for _, row in pucch_slot.iterrows():
                if row['uci_type'] and row['uci_value']:
                    pucch_uci_info.append(f"{row['uci_type']}={row['uci_value']}")
        pucch_uci = '; '.join(pucch_uci_info) if pucch_uci_info else '-'
        
        # Aggregate PUSCH data
        pusch_count = len(pusch_slot)
        pusch_ok = len(pusch_slot[pusch_slot['crc'] == 'OK']) if pusch_count > 0 else 0
        pusch_fail = len(pusch_slot[pusch_slot['crc'] == 'FAIL']) if pusch_count > 0 else 0
        pusch_status = '-'
        if pusch_count > 0:
            if pusch_fail > 0:
                pusch_status = f'{pusch_ok}OK/{pusch_fail}FAIL'
            else:
                pusch_status = f'{pusch_ok}OK'
        
        # Get timestamp (use first entry from either)
        timestamp = None
        if pucch_count > 0:
            timestamp = pucch_slot.iloc[0]['time']
        elif pusch_count > 0:
            timestamp = pusch_slot.iloc[0]['time']
        
        timeline_data.append({
            'frame': int(frame),
            'slot': int(slot_num),
            'slot_full': slot,
            'time': timestamp,
            'pucch_status': pucch_status,
            'pucch_uci': pucch_uci,
            'pucch_count': pucch_count,
            'pusch_status': pusch_status,
            'pusch_count': pusch_count
        })
    
    return pd.DataFrame(timeline_data)

def analyze_ul_performance(pucch_df, pusch_df):
    """
    Analyze UL performance metrics.
    """
    print("\n" + "="*70)
    print("UL PHY PERFORMANCE ANALYSIS")
    print("="*70)
    
    # PUCCH Analysis
    if not pucch_df.empty:
        total_pucch = len(pucch_df)
        pucch_valid = len(pucch_df[pucch_df['status'] == 'valid'])
        pucch_invalid = len(pucch_df[pucch_df['status'] == 'invalid'])
        
        print(f"\n📡 PUCCH ANALYSIS:")
        print(f"  Total PUCCH transmissions: {total_pucch:,}")
        print(f"  Valid: {pucch_valid:,} ({pucch_valid/total_pucch*100:.2f}%)")
        print(f"  Invalid: {pucch_invalid:,} ({pucch_invalid/total_pucch*100:.2f}%)")
        
        # Analyze by slot number
        pucch_df['slot_num'] = pucch_df['slot'].apply(lambda x: int(x.split('.')[1]))
        slot_invalid_counts = pucch_df[pucch_df['status'] == 'invalid']['slot_num'].value_counts().sort_index()
        
        if not slot_invalid_counts.empty:
            print(f"\n  Invalid PUCCH by slot number:")
            for slot_num, count in slot_invalid_counts.head(10).items():
                print(f"    Slot x.{slot_num:02d}: {count:,} invalid")
    else:
        print("\n📡 PUCCH ANALYSIS: No PUCCH data found")
    
    # PUSCH Analysis
    if not pusch_df.empty:
        total_pusch = len(pusch_df)
        pusch_ok = len(pusch_df[pusch_df['crc'] == 'OK'])
        pusch_fail = len(pusch_df[pusch_df['crc'] == 'FAIL'])
        
        print(f"\n📶 PUSCH ANALYSIS:")
        print(f"  Total PUSCH transmissions: {total_pusch:,}")
        print(f"  CRC OK: {pusch_ok:,} ({pusch_ok/total_pusch*100:.2f}%)")
        print(f"  CRC FAIL: {pusch_fail:,} ({pusch_fail/total_pusch*100:.2f}%)")
    else:
        print("\n📶 PUSCH ANALYSIS: No PUSCH data found")
    
    print("="*70)

def main():
    parser = argparse.ArgumentParser(description='Analyze UL PHY logs for PUCCH/PUSCH status')
    parser.add_argument('log_file', help='Path to gnb.log file')
    parser.add_argument('--output-dir', default='.', help='Output directory for results')
    parser.add_argument('--output-name', default='harq_analysis_v2.csv', help='Output CSV filename')
    
    args = parser.parse_args()
    
    print(f"Analyzing log file: {args.log_file}")
    
    # Parse UL PHY logs
    pucch_data, pusch_data = parse_ul_phy_logs(args.log_file)
    
    print(f"\nFound {len(pucch_data):,} PUCCH transmissions")
    print(f"Found {len(pusch_data):,} PUSCH transmissions")
    
    # Convert to DataFrames
    pucch_df = pd.DataFrame(pucch_data)
    pusch_df = pd.DataFrame(pusch_data)
    
    if not pucch_df.empty:
        pucch_df['time'] = pd.to_datetime(pucch_df['time'])
    if not pusch_df.empty:
        pusch_df['time'] = pd.to_datetime(pusch_df['time'])
    
    # Analyze UL performance
    analyze_ul_performance(pucch_df, pusch_df)
    
    # Create slot timeline
    timeline_df = create_slot_timeline(pucch_df, pusch_df)
    
    # Export timeline CSV
    output_file = Path(args.output_dir) / args.output_name
    timeline_df.to_csv(output_file, index=False)
    print(f"\nSlot timeline saved to: {output_file}")
    
    # Export detailed PUCCH data
    if not pucch_df.empty:
        pucch_file = Path(args.output_dir) / 'pucch_detailed.csv'
        pucch_df.to_csv(pucch_file, index=False)
        print(f"Detailed PUCCH data saved to: {pucch_file}")
    
    # Export detailed PUSCH data
    if not pusch_df.empty:
        pusch_file = Path(args.output_dir) / 'pusch_detailed.csv'
        pusch_df.to_csv(pusch_file, index=False)
        print(f"Detailed PUSCH data saved to: {pusch_file}")
    
    print("\nAnalysis complete!")

if __name__ == "__main__":
    main()
