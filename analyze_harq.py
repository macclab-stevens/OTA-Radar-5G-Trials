#!/usr/bin/env python3
"""
Comprehensive HARQ Analysis Script for 5G gNB Logs

This script analyzes both DL and UL transmissions:

DOWNLINK (DL):
- PDSCH transmissions and retransmissions
- HARQ-ACK messages (ack=1/0/2)
- Discarded HARQ processes
- Patterns of interference based on HARQ feedback

UPLINK (UL):
- PUCCH status (valid/invalid) per slot
- PUSCH CRC status (OK/FAIL) per slot
- Complete slot timeline showing all scheduled transmissions
"""

import re
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import argparse
from collections import defaultdict
from pathlib import Path
import os

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

def parse_gnb_log(log_file):
    """
    Parse gnb.log file to extract PDSCH, HARQ-ACK, and discard information.
    """
    pdsch_data = []
    harq_data = []
    discard_data = []
    current_timestamp = None
    current_slot = None
    
    # Track recent PDSCH transmissions by (rnti, h_id) to match with HARQ-ACKs
    # Key: (rnti, h_id), Value: {'slot': dl_slot, 'k1': k1_value}
    recent_pdsch = {}
    
    with open(log_file, 'r') as f:
        for line in f:
            # Extract timestamp from lines that have it
            ts_match = re.match(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+)', line)
            if ts_match:
                current_timestamp = ts_match.group(1)
            
            # Extract slot from "Slot decisions" lines
            # Example: [  295.12] Slot decisions
            slot_line_match = re.search(r'\[\s*([\d.]+)\]\s+Slot decision', line)
            if slot_line_match:
                current_slot = slot_line_match.group(1)  # Keep as string to preserve .1 vs .10
            
            # Parse PDSCH lines (may not have timestamp on same line)
            # Example: - UE PDSCH: ue=0 c-rnti=0x4601 h_id=0 rb=[0..23) symb=[2..14) tbs=317 mcs=7 rv=0 nrtx=0 k1=7 dl_bo=0 olla=0
            if '- UE PDSCH:' in line:
                ue_match = re.search(r'ue=(\d+)', line)
                rnti_match = re.search(r'c-rnti=(0x[0-9a-fA-F]+)', line)
                h_id_match = re.search(r'h_id=(\d+)', line)
                mcs_match = re.search(r'mcs=(\d+)', line)
                tbs_match = re.search(r'tbs=(\d+)', line)
                nrtx_match = re.search(r'nrtx=(\d+)', line)
                rv_match = re.search(r'rv=(\d+)', line)
                k1_match = re.search(r'k1=(\d+)', line)
                
                if ue_match and rnti_match and h_id_match:
                    rnti = rnti_match.group(1)
                    h_id = int(h_id_match.group(1))
                    k1 = int(k1_match.group(1)) if k1_match else None
                    
                    # Store PDSCH with DL slot for later HARQ-ACK matching
                    if current_slot is not None and k1 is not None:
                        recent_pdsch[(rnti, h_id)] = {
                            'slot': current_slot,
                            'k1': k1
                        }
                    
                    pdsch_data.append({
                        'time': current_timestamp,
                        'ue': int(ue_match.group(1)),
                        'rnti': rnti,
                        'h_id': h_id,
                        'slot_tx': format_slot(current_slot),
                        'k1': k1,
                        'mcs': int(mcs_match.group(1)) if mcs_match else None,
                        'tbs': int(tbs_match.group(1)) if tbs_match else None,
                        'nrtx': int(nrtx_match.group(1)) if nrtx_match else 0,
                        'rv': int(rv_match.group(1)) if rv_match else 0,
                        'type': 'PDSCH'
                    })
            
            # Parse HARQ-ACK lines
            # Example: - HARQ-ACK: ue=0 rnti=0x4601 pci=1 slot_rx=710.17 h_id=0 ack=1 tbs=317
            if '- HARQ-ACK:' in line:
                ue_match = re.search(r'ue=(\d+)', line)
                rnti_match = re.search(r'rnti=(0x[0-9a-fA-F]+)', line)
                slot_rx_match = re.search(r'slot_rx=([\d.]+)', line)
                h_id_match = re.search(r'h_id=(\d+)', line)
                ack_match = re.search(r'ack=(\d+)', line)
                tbs_match = re.search(r'tbs=(\d+)', line)
                
                if ue_match and rnti_match and slot_rx_match and h_id_match and ack_match:
                    rnti = rnti_match.group(1)
                    h_id = int(h_id_match.group(1))
                    slot_rx = slot_rx_match.group(1)  # Keep as string to preserve .1 vs .10
                    
                    # Use actual DL slot from the matched PDSCH transmission
                    dl_slot = slot_rx  # Default to slot_rx if no match found
                    k1_value = None
                    pdsch_key = (rnti, h_id)
                    
                    if pdsch_key in recent_pdsch:
                        pdsch_info = recent_pdsch[pdsch_key]
                        k1_value = pdsch_info['k1']
                        # Use the actual DL transmission slot from PDSCH
                        dl_slot = pdsch_info['slot']
                        # Clean up old entry
                        del recent_pdsch[pdsch_key]
                    
                    harq_data.append({
                        'time': current_timestamp,
                        'ue': int(ue_match.group(1)),
                        'rnti': rnti,
                        'h_id': h_id,
                        'slot_rx': format_slot(slot_rx),  # UL slot where ACK was received
                        'slot_tx': format_slot(dl_slot),  # DL slot where PDSCH was transmitted
                        'k1': k1_value,
                        'ack': int(ack_match.group(1)),
                        'tbs': int(tbs_match.group(1)) if tbs_match else None,
                        'type': 'HARQ-ACK'
                    })
            
            # Parse Discarding DL HARQ lines
            # Example: 2025-11-02T18:56:14.577854 [SCHED] [I] [692.5] rnti=0x4601 h_id=2: Discarding DL HARQ process TB with tbs=2817. Cause: Maximum number of reTxs 4 exceeded
            if 'Discarding DL HARQ' in line:
                rnti_match = re.search(r'rnti=(0x[0-9a-fA-F]+)', line)
                h_id_match = re.search(r'h_id=(\d+)', line)
                slot_match = re.search(r'\[\s*([\d.]+)\]', line)
                tbs_match = re.search(r'tbs=(\d+)', line)
                retx_match = re.search(r'reTxs (\d+)', line)
                
                if rnti_match and h_id_match:
                    discard_data.append({
                        'time': current_timestamp,
                        'rnti': rnti_match.group(1),
                        'h_id': int(h_id_match.group(1)),
                        'slot_tx': format_slot(slot_match.group(1)) if slot_match else None,
                        'tbs': int(tbs_match.group(1)) if tbs_match else None,
                        'max_retx': int(retx_match.group(1)) if retx_match else 4,
                        'type': 'DISCARD'
                    })
    
    return pdsch_data, harq_data, discard_data

def parse_ul_phy_logs(log_file):
    """
    Parse gnb.log file to extract UL PHY PUCCH and PUSCH information with status.
    PUCCH status is read from the continuation lines following each PUCCH PHY log entry.
    """
    pucch_data = []
    pusch_data = []
    current_timestamp = None
    
    with open(log_file, 'r') as f:
        lines = f.readlines()
    
    # Parse PUCCH data
    i = 0
    while i < len(lines):
        line = lines[i]
        
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
                metric = float(metric_match.group(1)) if metric_match else None
                sinr = float(sinr_match.group(1)) if sinr_match else None
                
                # Look for status field in the continuation lines (indented lines following the PUCCH line)
                status = 'valid'  # default
                j = i + 1
                while j < len(lines) and lines[j].startswith('  '):
                    status_match = re.search(r'status=(valid|invalid)', lines[j])
                    if status_match:
                        status = status_match.group(1)
                        break
                    j += 1
                
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
        
        i += 1
    
    # Parse PUSCH data
    current_timestamp = None
    for line in lines:
        # Extract timestamp from lines that have it
        ts_match = re.match(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+)', line)
        if ts_match:
            current_timestamp = ts_match.group(1)
        
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

def analyze_harq_performance(harq_df, pdsch_df, discard_df):
    """
    Analyze HARQ performance metrics including retransmissions.
    """
    if harq_df.empty:
        print("No HARQ-ACK data found.")
        return None
    
    total_harq = len(harq_df)
    ack_counts = harq_df['ack'].value_counts()
    
    # Analyze retransmissions from PDSCH data
    total_pdsch = len(pdsch_df)
    first_tx = len(pdsch_df[pdsch_df['nrtx'] == 0])
    retx = len(pdsch_df[pdsch_df['nrtx'] > 0])
    total_discards = len(discard_df)
    
    # Count transmissions by retransmission count
    retx_counts = pdsch_df['nrtx'].value_counts().sort_index()
    
    print("\n" + "="*70)
    print("HARQ PERFORMANCE ANALYSIS")
    print("="*70)
    
    print(f"\n📊 TRANSMISSION SUMMARY:")
    print(f"  Total PDSCH transmissions: {total_pdsch:,}")
    print(f"  First transmissions (nrtx=0): {first_tx:,} ({first_tx/total_pdsch*100:.2f}%)")
    print(f"  Retransmissions (nrtx>0): {retx:,} ({retx/total_pdsch*100:.2f}%)")
    print(f"  Discarded after max retries: {total_discards:,}")
    
    print(f"\n📈 RETRANSMISSION BREAKDOWN:")
    for nrtx in sorted(retx_counts.index):
        count = retx_counts[nrtx]
        pct = count/total_pdsch*100
        if nrtx == 0:
            print(f"  nrtx={nrtx} (First TX):      {count:,} ({pct:.2f}%)")
        else:
            print(f"  nrtx={nrtx} (Retransmission): {count:,} ({pct:.2f}%)")
    
    print(f"\n✅ HARQ-ACK FEEDBACK:")
    print(f"  Total HARQ-ACK messages: {total_harq:,}")
    print(f"  ACK (ack=1): {ack_counts.get(1, 0):,} ({ack_counts.get(1, 0)/total_harq*100:.2f}%)")
    print(f"  NACK (ack=0): {ack_counts.get(0, 0):,} ({ack_counts.get(0, 0)/total_harq*100:.2f}%)")
    print(f"  DTX (ack=2): {ack_counts.get(2, 0):,} ({ack_counts.get(2, 0)/total_harq*100:.2f}%)")
    
    # Calculate success rates
    nack_rate = ack_counts.get(0, 0) / total_harq * 100
    first_tx_success_rate = first_tx / (first_tx + retx + total_discards) * 100 if (first_tx + retx + total_discards) > 0 else 0
    
    print(f"\n📉 ERROR RATES:")
    print(f"  NACK Rate: {nack_rate:.2f}%")
    print(f"  Discard Rate: {total_discards/(first_tx + retx + total_discards)*100:.2f}%" if (first_tx + retx + total_discards) > 0 else "  Discard Rate: 0.00%")
    print(f"  First TX Success Rate: {first_tx_success_rate:.2f}%")
    
    print("="*70)
    
    return {
        'total_pdsch': total_pdsch,
        'first_tx': first_tx,
        'retx': retx,
        'total_discards': total_discards,
        'total_harq': total_harq,
        'ack': ack_counts.get(1, 0),
        'nack': ack_counts.get(0, 0),
        'dtx': ack_counts.get(2, 0),
        'nack_rate': nack_rate,
        'first_tx_success_rate': first_tx_success_rate,
        'retx_counts': retx_counts.to_dict()
    }

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
        pucch_df_copy = pucch_df.copy()
        pucch_df_copy['slot_num'] = pucch_df_copy['slot'].apply(lambda x: int(x.split('.')[1]))
        slot_invalid_counts = pucch_df_copy[pucch_df_copy['status'] == 'invalid']['slot_num'].value_counts().sort_index()
        
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

def create_comprehensive_timeline(harq_df, pucch_df, pusch_df):
    """
    Create a comprehensive timeline combining DL HARQ-ACK and UL PUCCH/PUSCH data.
    Each row represents one transmission with direction (DL/UL), slot_id, and relevant metrics.
    """
    combined_data = []
    
    # Add DL HARQ-ACK data
    if not harq_df.empty:
        for _, row in harq_df.iterrows():
            combined_data.append({
                'time': row['time'],
                'slot_id': row['slot_tx'],
                'direction': 'DL',
                'rnti': row['rnti'],
                'type': 'HARQ-ACK',
                'ack_status': row['ack'],  # 0=NACK, 1=ACK, 2=DTX
                'h_id': row.get('h_id', None),
                'tbs': row.get('tbs', None),
                'ul_status': None,  # Not applicable for DL
                'ul_sinr': None,    # Not applicable for DL
                'ul_metric': None   # Not applicable for DL
            })
    
    # Add UL PUCCH data
    if not pucch_df.empty:
        for _, row in pucch_df.iterrows():
            uci_info = f"{row['uci_type']}={row['uci_value']}" if row['uci_type'] and row['uci_value'] else None
            combined_data.append({
                'time': row['time'],
                'slot_id': row['slot'],
                'direction': 'UL',
                'rnti': row['rnti'],
                'type': f"PUCCH-F{row['format']}",
                'ack_status': None,  # Not applicable for UL
                'h_id': None,
                'tbs': None,
                'ul_status': row['status'],  # valid/invalid
                'ul_sinr': row['sinr'],
                'ul_metric': row['metric'],
                'uci_info': uci_info
            })
    
    # Add UL PUSCH data
    if not pusch_df.empty:
        for _, row in pusch_df.iterrows():
            combined_data.append({
                'time': row['time'],
                'slot_id': row['slot'],
                'direction': 'UL',
                'rnti': row['rnti'],
                'type': 'PUSCH',
                'ack_status': None,  # Not applicable for UL
                'h_id': row.get('h_id', None),
                'tbs': row.get('tbs', None),
                'ul_status': row['crc'],  # OK/FAIL
                'ul_sinr': row['sinr'],
                'ul_metric': None,
                'uci_info': None
            })
    
    # Create DataFrame and sort chronologically with slots in numeric order within each frame
    df = pd.DataFrame(combined_data)
    if not df.empty:
        # Add frame and slot number as separate numeric columns for proper sorting
        df['_frame'] = df['slot_id'].apply(lambda x: int(x.split('.')[0]))
        df['_slot'] = df['slot_id'].apply(lambda x: int(x.split('.')[1]))
        
        # Create time windows of 1 second to group events processed together
        df['_datetime'] = pd.to_datetime(df['time'])
        df['_time_1s'] = (df['_datetime'].astype('int64') // 10**9)  # 1 second windows
        
        # Sort by 1s time window (chronological), then frame, then slot (0-19)
        # This keeps chronological order with slots ordered within each frame
        df = df.sort_values(['_time_1s', '_frame', '_slot', 'time']).reset_index(drop=True)
        
        # Drop temporary sorting columns
        df = df.drop(columns=['_frame', '_slot', '_datetime', '_time_1s'])
    
    return df

def identify_interference_slots(harq_df, window_size=10, nack_threshold=0.3):
    """
    Identify slots with potential interference based on NACK patterns.
    Uses a sliding window to detect bursts of NACKs.
    """
    if harq_df.empty:
        return []
    
    harq_df = harq_df.sort_values('slot_tx').reset_index(drop=True)
    interference_slots = []
    
    # Sliding window analysis
    for i in range(len(harq_df) - window_size + 1):
        window = harq_df.iloc[i:i+window_size]
        nack_ratio = (window['ack'] == 0).sum() / window_size
        
        if nack_ratio >= nack_threshold:
            interference_slots.extend(window['slot_tx'].tolist())
    
    return sorted(set(interference_slots))

def plot_harq_timeline(harq_df, output_file='harq_timeline.png'):
    """
    Plot HARQ-ACK results over time/slots.
    """
    if harq_df.empty:
        print("No data to plot.")
        return
    
    harq_df = harq_df.copy()
    # Convert slot_tx string back to float for plotting
    harq_df['slot_tx_numeric'] = harq_df['slot_tx'].astype(float)
    harq_df = harq_df.sort_values('slot_tx_numeric')
    
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
    
    # Plot 1: HARQ-ACK status over slots
    colors = {0: 'red', 1: 'green', 2: 'orange'}
    labels = {0: 'NACK', 1: 'ACK', 2: 'DTX'}
    
    for ack_val in [0, 1, 2]:
        subset = harq_df[harq_df['ack'] == ack_val]
        ax1.scatter(subset['slot_tx_numeric'], [ack_val]*len(subset), 
                   c=colors[ack_val], label=labels[ack_val], 
                   alpha=0.6, s=20)
    
    ax1.set_xlabel('Slot Number')
    ax1.set_ylabel('HARQ-ACK Status')
    ax1.set_yticks([0, 1, 2])
    ax1.set_yticklabels(['NACK (0)', 'ACK (1)', 'DTX (2)'])
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.set_title('HARQ-ACK Status Over Slots')
    
    # Plot 2: NACK rate over time (moving average)
    window = 10
    harq_df['is_nack'] = (harq_df['ack'] == 0).astype(int)
    harq_df['nack_rate_ma'] = harq_df['is_nack'].rolling(window=window, min_periods=1).mean()
    
    ax2.plot(harq_df['slot_tx_numeric'].values, harq_df['nack_rate_ma'].values, 'b-', linewidth=2)
    ax2.axhline(y=0.1, color='orange', linestyle='--', label='10% NACK threshold')
    ax2.axhline(y=0.2, color='red', linestyle='--', label='20% NACK threshold')
    ax2.set_xlabel('Slot Number')
    ax2.set_ylabel(f'NACK Rate (Moving Avg, window={window})')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    ax2.set_title('NACK Rate Over Time (Interference Detection)')
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"\nPlot saved to: {output_file}")
    plt.close()

def plot_harq_comprehensive(harq_df, pdsch_df, discard_df, stats, output_dir='.'):
    """
    Create comprehensive HARQ analysis plots with retransmission details.
    """
    if harq_df.empty and pdsch_df.empty:
        print("No data to plot.")
        return
    
    # Create figure with multiple subplots
    fig = plt.figure(figsize=(16, 12))
    gs = fig.add_gridspec(3, 2, hspace=0.3, wspace=0.3)
    
    # Plot 1: Retransmission Distribution (Pie Chart)
    ax1 = fig.add_subplot(gs[0, 0])
    if 'retx_counts' in stats and stats['retx_counts']:
        retx_data = stats['retx_counts']
        labels = [f"nrtx={k}" + (" (First TX)" if k == 0 else "") for k in sorted(retx_data.keys())]
        sizes = [retx_data[k] for k in sorted(retx_data.keys())]
        colors = ['#2ecc71', '#e74c3c', '#e67e22', '#9b59b6', '#34495e']
        explode = [0.1 if k == 0 else 0 for k in sorted(retx_data.keys())]
        
        ax1.pie(sizes, explode=explode, labels=labels, colors=colors[:len(sizes)],
                autopct='%1.1f%%', startangle=90)
        ax1.set_title('Transmission Distribution by Retransmission Count', fontsize=12, fontweight='bold')
    
    # Plot 2: HARQ-ACK Distribution (Pie Chart)
    ax2 = fig.add_subplot(gs[0, 1])
    if not harq_df.empty:
        ack_data = [stats['ack'], stats['nack'], stats['dtx']]
        labels_ack = ['ACK (Success)', 'NACK (Failure)', 'DTX (Missed)']
        colors_ack = ['#2ecc71', '#e74c3c', '#f39c12']
        
        ax2.pie(ack_data, labels=labels_ack, colors=colors_ack,
                autopct='%1.1f%%', startangle=90)
        ax2.set_title('HARQ-ACK Feedback Distribution', fontsize=12, fontweight='bold')
    
    # Plot 3: HARQ-ACK Status Over Slots (Timeline)
    ax3 = fig.add_subplot(gs[1, :])
    if not harq_df.empty:
        harq_sorted = harq_df.copy()
        # Convert slot_tx string back to float for plotting
        harq_sorted['slot_tx_numeric'] = harq_sorted['slot_tx'].astype(float)
        harq_sorted = harq_sorted.sort_values('slot_tx_numeric')
        colors_harq = {0: '#e74c3c', 1: '#2ecc71', 2: '#f39c12'}
        labels_harq = {0: 'NACK', 1: 'ACK', 2: 'DTX'}
        
        for ack_val in [0, 1, 2]:
            subset = harq_sorted[harq_sorted['ack'] == ack_val]
            if not subset.empty:
                ax3.scatter(subset['slot_tx_numeric'], [ack_val]*len(subset), 
                           c=colors_harq[ack_val], label=labels_harq[ack_val], 
                           alpha=0.6, s=10)
        
        ax3.set_xlabel('Slot Number', fontsize=10)
        ax3.set_ylabel('HARQ-ACK Status', fontsize=10)
        ax3.set_yticks([0, 1, 2])
        ax3.set_yticklabels(['NACK (0)', 'ACK (1)', 'DTX (2)'])
        ax3.legend(loc='upper right')
        ax3.grid(True, alpha=0.3)
        ax3.set_title('HARQ-ACK Status Over Slots', fontsize=12, fontweight='bold')
    
    # Plot 4: NACK Rate Over Time (Moving Average)
    ax4 = fig.add_subplot(gs[2, 0])
    if not harq_df.empty:
        window = 100
        harq_sorted = harq_df.copy()
        # Convert slot_tx string back to float for plotting
        harq_sorted['slot_tx_numeric'] = harq_sorted['slot_tx'].astype(float)
        harq_sorted = harq_sorted.sort_values('slot_tx_numeric')
        harq_sorted['is_nack'] = (harq_sorted['ack'] == 0).astype(int)
        harq_sorted['nack_rate_ma'] = harq_sorted['is_nack'].rolling(window=window, min_periods=1).mean()
        
        ax4.plot(harq_sorted['slot_tx_numeric'].values, harq_sorted['nack_rate_ma'].values, 'b-', linewidth=2, label='NACK Rate')
        ax4.axhline(y=0.1, color='orange', linestyle='--', linewidth=1, label='10% threshold')
        ax4.axhline(y=0.2, color='red', linestyle='--', linewidth=1, label='20% threshold')
        ax4.set_xlabel('Slot Number', fontsize=10)
        ax4.set_ylabel(f'NACK Rate (MA, window={window})', fontsize=10)
        ax4.legend(loc='upper right')
        ax4.grid(True, alpha=0.3)
        ax4.set_title('NACK Rate Over Time (Interference Detection)', fontsize=12, fontweight='bold')
        ax4.set_ylim([0, max(1.0, harq_sorted['nack_rate_ma'].max() * 1.1)])
    
    # Plot 5: Retransmission Count Over Time
    ax5 = fig.add_subplot(gs[2, 1])
    if not pdsch_df.empty and 'nrtx' in pdsch_df.columns:
        # Group by time windows and calculate average retransmissions
        pdsch_sorted = pdsch_df.copy()
        if 'time' in pdsch_sorted.columns:
            pdsch_sorted['time'] = pd.to_datetime(pdsch_sorted['time'])
            pdsch_sorted = pdsch_sorted.sort_values('time')
            
            # Use rolling window on index
            window_size = 100
            pdsch_sorted['nrtx_ma'] = pdsch_sorted['nrtx'].rolling(window=window_size, min_periods=1).mean()
            
            ax5.plot(range(len(pdsch_sorted)), pdsch_sorted['nrtx_ma'].values, 'r-', linewidth=2, label='Avg Retrans')
            ax5.set_xlabel('Transmission Index', fontsize=10)
            ax5.set_ylabel(f'Avg Retransmission Count (MA, window={window_size})', fontsize=10)
            ax5.legend(loc='upper right')
            ax5.grid(True, alpha=0.3)
            ax5.set_title('Average Retransmission Count Over Time', fontsize=12, fontweight='bold')
    
    plt.savefig(f'{output_dir}/harq_comprehensive_analysis.png', dpi=300, bbox_inches='tight')
    print(f"\nComprehensive analysis plot saved to: {output_dir}/harq_comprehensive_analysis.png")
    plt.close()
    
    # Create a separate detailed plot showing first TX vs retransmissions
    fig2, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    
    # Bar chart of retransmission counts
    if 'retx_counts' in stats and stats['retx_counts']:
        retx_dict = stats['retx_counts']
        nrtx_vals = sorted(retx_dict.keys())
        counts = [retx_dict[k] for k in nrtx_vals]
        colors_bar = ['#2ecc71' if k == 0 else '#e74c3c' for k in nrtx_vals]
        
        ax1.bar(nrtx_vals, counts, color=colors_bar, alpha=0.7, edgecolor='black')
        ax1.set_xlabel('Number of Retransmissions (nrtx)', fontsize=11)
        ax1.set_ylabel('Count', fontsize=11)
        ax1.set_title('Transmission Count by Retransmission Number', fontsize=12, fontweight='bold')
        ax1.grid(True, alpha=0.3, axis='y')
        
        # Add value labels on bars
        for i, (nrtx, count) in enumerate(zip(nrtx_vals, counts)):
            pct = count/sum(counts)*100
            ax1.text(nrtx, count, f'{count:,}\n({pct:.1f}%)', 
                    ha='center', va='bottom', fontsize=9)
    
    # Success rate comparison
    if stats:
        categories = ['First TX\n(No Retrans)', 'With\nRetransmissions', 'Discarded\n(Max Retries)']
        values = [stats.get('first_tx', 0), stats.get('retx', 0), stats.get('total_discards', 0)]
        colors_comp = ['#2ecc71', '#f39c12', '#e74c3c']
        
        ax2.bar(categories, values, color=colors_comp, alpha=0.7, edgecolor='black')
        ax2.set_ylabel('Count', fontsize=11)
        ax2.set_title('Transmission Outcomes', fontsize=12, fontweight='bold')
        ax2.grid(True, alpha=0.3, axis='y')
        
        # Add value labels
        for i, (cat, val) in enumerate(zip(categories, values)):
            total = sum(values)
            pct = val/total*100 if total > 0 else 0
            ax2.text(i, val, f'{val:,}\n({pct:.1f}%)', 
                    ha='center', va='bottom', fontsize=9)
    
    plt.tight_layout()
    plt.savefig(f'{output_dir}/harq_retransmission_analysis.png', dpi=300, bbox_inches='tight')
    print(f"Retransmission analysis plot saved to: {output_dir}/harq_retransmission_analysis.png")
    plt.close()

def export_interference_report(harq_df, interference_slots, output_file='interference_report.csv'):
    """
    Export detailed report of slots with interference.
    """
    if not interference_slots:
        print("\nNo interference detected.")
        return
    
    interference_data = harq_df[harq_df['slot_tx'].isin(interference_slots)].copy()
    interference_data = interference_data.sort_values('slot_tx')
    
    interference_data.to_csv(output_file, index=False)
    print(f"\nInterference report saved to: {output_file}")
    print(f"Number of affected slots: {len(interference_slots)}")

def main():
    parser = argparse.ArgumentParser(description='Comprehensive HARQ analysis: DL (PDSCH/HARQ-ACK) and UL (PUCCH/PUSCH) in gNB logs')
    parser.add_argument('log_file', help='Path to gnb.log file')
    parser.add_argument('--output-dir', default='.', help='Output directory for results')
    parser.add_argument('--window', type=int, default=10, help='Window size for interference detection')
    parser.add_argument('--threshold', type=float, default=0.3, help='NACK threshold for interference (0-1)')
    parser.add_argument('--csv-only', action='store_true', help='Only generate CSV files, skip plots and analysis output')
    
    args = parser.parse_args()
    
    if not args.csv_only:
        print("="*70)
        print("COMPREHENSIVE HARQ ANALYSIS - DL & UL")
        print("="*70)
        print(f"Analyzing log file: {args.log_file}")
        print(f"Output directory: {args.output_dir}")
        print(f"Interference detection window: {args.window}")
        print(f"NACK threshold: {args.threshold*100}%")
    
    # Parse DL data (PDSCH, HARQ-ACK, discard)
    pdsch_data, harq_data, discard_data = parse_gnb_log(args.log_file)
    
    # Parse UL data (PUCCH, PUSCH)
    pucch_data, pusch_data = parse_ul_phy_logs(args.log_file)
    
    if not args.csv_only:
        print(f"\n📥 DOWNLINK DATA:")
        print(f"  Found {len(pdsch_data):,} PDSCH transmissions")
        print(f"  Found {len(harq_data):,} HARQ-ACK messages")
        print(f"  Found {len(discard_data):,} discarded HARQ processes")
        
        print(f"\n📤 UPLINK DATA:")
        print(f"  Found {len(pucch_data):,} PUCCH transmissions")
        print(f"  Found {len(pusch_data):,} PUSCH transmissions")
    
    # Convert to DataFrames
    pdsch_df = pd.DataFrame(pdsch_data)
    harq_df = pd.DataFrame(harq_data)
    discard_df = pd.DataFrame(discard_data)
    pucch_df = pd.DataFrame(pucch_data)
    pusch_df = pd.DataFrame(pusch_data)
    
    # Convert timestamps
    if not harq_df.empty:
        harq_df['time'] = pd.to_datetime(harq_df['time'])
    if not pdsch_df.empty:
        pdsch_df['time'] = pd.to_datetime(pdsch_df['time'])
    if not discard_df.empty:
        discard_df['time'] = pd.to_datetime(discard_df['time'])
    if not pucch_df.empty:
        pucch_df['time'] = pd.to_datetime(pucch_df['time'])
    if not pusch_df.empty:
        pusch_df['time'] = pd.to_datetime(pusch_df['time'])
    
    # Analyze DL HARQ performance (skip if csv-only)
    if not args.csv_only:
        if not harq_df.empty or not pdsch_df.empty:
            stats = analyze_harq_performance(harq_df, pdsch_df, discard_df)
        
            # Identify interference slots
            if not harq_df.empty:
                interference_slots = identify_interference_slots(harq_df, 
                                                                 window_size=args.window,
                                                                 nack_threshold=args.threshold)
            else:
                interference_slots = []
            
            # Create comprehensive DL plots
            if not harq_df.empty or not pdsch_df.empty:
                plot_harq_comprehensive(harq_df, pdsch_df, discard_df, stats, args.output_dir)
                
                # Also create the original timeline plot
                if not harq_df.empty:
                    plot_file = f"{args.output_dir}/harq_timeline.png"
                    plot_harq_timeline(harq_df, plot_file)
            
            # Export interference report
            if interference_slots:
                report_file = f"{args.output_dir}/interference_report.csv"
                export_interference_report(harq_df, interference_slots, report_file)
        
        # Analyze UL performance
        if not pucch_df.empty or not pusch_df.empty:
            analyze_ul_performance(pucch_df, pusch_df)
    
    # Create comprehensive timeline combining DL and UL data
    comprehensive_timeline = create_comprehensive_timeline(harq_df, pucch_df, pusch_df)
    
    # Export comprehensive HARQ analysis CSV (DL + UL combined)
    if not comprehensive_timeline.empty:
        harq_csv = f"{args.output_dir}/harq_analysis.csv"
        comprehensive_timeline.to_csv(harq_csv, index=False)
        if not args.csv_only:
            print(f"\n💾 COMPREHENSIVE OUTPUT FILES:")
            print(f"  Combined DL+UL timeline: {harq_csv}")
    
    # Export separate detailed files
    if not pdsch_df.empty:
        pdsch_csv = f"{args.output_dir}/pdsch_analysis.csv"
        pdsch_df.to_csv(pdsch_csv, index=False)
        if not args.csv_only:
            print(f"  PDSCH data: {pdsch_csv}")
    
    if not discard_df.empty:
        discard_csv = f"{args.output_dir}/discard_analysis.csv"
        discard_df.to_csv(discard_csv, index=False)
        if not args.csv_only:
            print(f"  Discarded HARQ processes: {discard_csv}")
    
    # Export UL slot timeline (aggregated view)
    if not pucch_df.empty or not pusch_df.empty:
        timeline_df = create_slot_timeline(pucch_df, pusch_df)
        timeline_csv = f"{args.output_dir}/ul_slot_timeline.csv"
        timeline_df.to_csv(timeline_csv, index=False)
        
        if not args.csv_only:
            print(f"  UL slot timeline (aggregated): {timeline_csv}")
    
    if not pucch_df.empty:
        pucch_csv = f"{args.output_dir}/pucch_detailed.csv"
        pucch_df.to_csv(pucch_csv, index=False)
        if not args.csv_only:
            print(f"  PUCCH detailed data: {pucch_csv}")
    
    if not pusch_df.empty:
        pusch_csv = f"{args.output_dir}/pusch_detailed.csv"
        pusch_df.to_csv(pusch_csv, index=False)
        if not args.csv_only:
            print(f"  PUSCH detailed data: {pusch_csv}")
    
    if not args.csv_only:
        print("\n" + "="*70)
        print("ANALYSIS COMPLETE!")
        print("="*70)

if __name__ == "__main__":
    main()

