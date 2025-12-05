#!/usr/bin/env python3

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
import sys
import os
import re
import glob

def parse_gnb_log_to_harq_csv(gnb_log_file, output_harq_csv):
    """
    Parse the gnb.log file and extract HARQ-related information.
    Creates a harqLog.csv file with HARQ entries.
    
    This is a simplified version that focuses only on HARQ-relevant logs.
    """
    print(f"\n=== Parsing {gnb_log_file} ===")
    
    records = []
    with open(gnb_log_file, 'r') as f:
        lines = f.readlines()
    
    i = 0
    while i < len(lines):
        line = lines[i]
        
        # Look for SCHED lines with "Processed slot events" (contains HARQ-ACK, CRC, etc.)
        if '[SCHED   ]' in line and ('Processed slot events' in line or 'Slot decisions' in line):
            # Extract timestamp
            time_match = re.match(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+)', line)
            timestamp = time_match.group(1) if time_match else None
            
            # Extract slot (in brackets)
            brackets = re.findall(r'\[([^\]]+)\]', line)
            slot = brackets[2].strip() if len(brackets) > 2 else None
            
            # Extract pci from the main line
            pci_match = re.search(r'pci=(\d+)', line)
            pci = pci_match.group(1) if pci_match else None
            
            # Parse following indented event lines (start with "- ")
            j = i + 1
            while j < len(lines) and lines[j].startswith('- '):
                event_line = lines[j].strip()
                # Extract event type
                event_type_match = re.match(r'^-\s+([^:]+):', event_line)
                if event_type_match:
                    event_type = event_type_match.group(1).strip()
                    
                    # Only process HARQ-relevant events
                    if event_type in ['HARQ-ACK', 'UE PDSCH', 'CRC', 'DL PDCCH', 'UE PUSCH']:
                        # Extract all key=value pairs
                        pairs = {}
                        
                        # Handle quoted values
                        quoted_pairs = re.findall(r'(\w+)="([^"]+)"', event_line)
                        for k, v in quoted_pairs:
                            pairs[k] = v
                        
                        # Handle range values like prb=[0..1)
                        range_pairs = re.findall(r'(\w+)=(\[[^\]]+?\))', event_line)
                        for k, v in range_pairs:
                            if k not in pairs:
                                pairs[k] = v
                        
                        # Handle regular key=value pairs
                        regular_pairs = re.findall(r'(\w+)=([^\s"\[]+)(?:\s|$)', event_line)
                        for k, v in regular_pairs:
                            if k not in pairs:
                                pairs[k] = v
                        
                        # Build the record
                        record = {
                            'time': timestamp,
                            'slot': slot,
                            'log_source': 'SCHED',
                            'log_type': event_type,
                            'pci': pci
                        }
                        record.update(pairs)
                        records.append(record)
                j += 1
            i = j
            
        # Look for "Discarding DL HARQ" events
        elif '[SCHED   ]' in line and 'Discarding DL HARQ process' in line:
            time_match = re.match(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+)', line)
            timestamp = time_match.group(1) if time_match else None
            
            brackets = re.findall(r'\[([^\]]+)\]', line)
            slot = brackets[2].strip() if len(brackets) > 2 else None
            
            pairs = {}
            
            # Extract rnti and h_id
            rnti_match = re.search(r'rnti=(0x[0-9a-fA-F]+)', line)
            h_id_match = re.search(r'h_id=(\d+)', line)
            
            if rnti_match:
                pairs['rnti'] = rnti_match.group(1)
            if h_id_match:
                pairs['h_id'] = h_id_match.group(1)
            
            # Extract tbs
            tbs_match = re.search(r'tbs=(\d+)', line)
            if tbs_match:
                pairs['tbs'] = tbs_match.group(1)
            
            # Extract cause
            cause_match = re.search(r'Cause:\s*(.+)$', line)
            if cause_match:
                pairs['discard_cause'] = cause_match.group(1).strip()
            
            # Extract max retx
            retx_match = re.search(r'reTxs\s+(\d+)', line)
            if retx_match:
                pairs['max_retx'] = retx_match.group(1)
            
            record = {
                'time': timestamp,
                'slot': slot,
                'log_source': 'SCHED',
                'log_type': 'Discarding DL HARQ',
                'pci': None
            }
            record.update(pairs)
            records.append(record)
            i += 1
        else:
            i += 1
    
    # Create DataFrame
    df = pd.DataFrame(records)
    
    if df.empty:
        print(f"Warning: No HARQ entries found in {gnb_log_file}")
        return False
    
    df['time'] = pd.to_datetime(df['time'])
    
    # Rename 'ack' to 'harq_ack' for clarity if it exists
    if 'ack' in df.columns:
        df = df.rename(columns={'ack': 'harq_ack'})
    
    # Sort by time
    df = df.sort_values('time').reset_index(drop=True)
    
    # Filter to only rows with h_id (HARQ-specific)
    harq_df = df[df['h_id'].notna()].copy()
    
    if harq_df.empty:
        print(f"Warning: No entries with h_id found")
        return False
    
    # Select relevant columns
    desired_cols = ['time', 'slot', 'log_source', 'log_type', 'h_id', 'slot_rx', 'crc', 'harq_bits', 'harq_ack', 'tbs', 'rv', 'nrtx']
    available_cols = [col for col in desired_cols if col in harq_df.columns]
    harq_df = harq_df[available_cols]
    
    # Save to CSV
    harq_df.to_csv(output_harq_csv, index=False)
    print(f"✓ Created {output_harq_csv} with {len(harq_df)} HARQ entries")
    
    return True

def parse_slot(slot_str):
    """
    Parse slot string in format 'SFN.slot' (e.g., '105.7')
    Returns tuple of (SFN, slot)
    For 30kHz SCS: 20 slots per SFN (0-19), each slot = 0.5ms, SFN = 10ms
    """
    if pd.isna(slot_str):
        return None, None
    try:
        sfn, slot = str(slot_str).split('.')
        return int(sfn), int(slot)
    except:
        return None, None

def slot_to_time_ms(slot_str):
    """
    Convert slot string to absolute time in milliseconds.
    For 30kHz SCS: SFN duration = 10ms, slot duration = 0.5ms
    Time = SFN * 10ms + slot * 0.5ms
    """
    sfn, slot = parse_slot(slot_str)
    if sfn is None or slot is None:
        return None
    return sfn * 10.0 + slot * 0.5

def analyze_dl_pdsch_acks(harq_log_file='./harqLog.csv'):
    """
    Analyze DL PDSCH transmissions using HARQ state machine logic.
    
    HARQ State Machine:
    1. nrtx=0, no subsequent retransmission → ACK'd (success)
    2. nrtx=0, followed by nrtx>0 with same h_id → NACK'd, will be retransmitted
    3. nrtx>0, no subsequent retransmission → ACK'd (success)
    4. nrtx>0, followed by higher nrtx with same h_id → NACK'd, will be retransmitted
    5. "Discarding DL HARQ" event → Dropped after max retransmissions (failure)
    
    This approach uses the transmission sequence itself to determine outcome,
    rather than time-window matching which can miss delayed ACKs.
    """
    
    print("=" * 70)
    print("HARQ DL PDSCH Transmission Analysis - State Machine Approach")
    print("=" * 70)
    
    # Load the HARQ log
    print("\nLoading HARQ log...")
    df = pd.read_csv(harq_log_file)
    print(f"Total entries: {len(df):,}")
    
    # Filter for UE PDSCH (downlink transmissions) and HARQ-ACK events
    pdsch_df = df[df['log_type'] == 'UE PDSCH'].copy()
    harq_ack_df = df[df['log_type'] == 'HARQ-ACK'].copy()
    
    print(f"\nUE PDSCH entries: {len(pdsch_df):,}")
    print(f"HARQ-ACK entries: {len(harq_ack_df):,}")
    
    # Add time in ms for sorting/plotting
    pdsch_df['time_ms'] = pdsch_df['slot'].apply(slot_to_time_ms)
    harq_ack_df['time_ms'] = harq_ack_df['slot'].apply(slot_to_time_ms)
    
    # Convert timestamps to datetime for easier comparison
    pdsch_df['time_dt'] = pd.to_datetime(pdsch_df['time'])
    harq_ack_df['time_dt'] = pd.to_datetime(harq_ack_df['time'])
    
    # Sort by time
    pdsch_df = pdsch_df.sort_values('time_dt').reset_index(drop=True)
    
    # Get discarding events (max retx exceeded)
    discarding_df = df[df['log_type'] == 'Discarding DL HARQ'].copy() if 'Discarding DL HARQ' in df['log_type'].values else pd.DataFrame()
    
    print("\nAnalyzing HARQ state machine...")
    print("Strategy: Track h_id transmission sequences to determine outcomes")
    print("  - If nrtx doesn't increase → ACK'd (success)")
    print("  - If nrtx increases → Previous attempt failed (retransmission)")
    print("  - If 'Discarding DL HARQ' → Dropped after max retx (failure)")
    
    # Add next transmission info for each h_id group
    print("\nProcessing transmissions by h_id...")
    pdsch_df['next_nrtx'] = None
    pdsch_df['next_tbs'] = None
    pdsch_df['next_time_diff_ms'] = None
    
    for h_id in pdsch_df['h_id'].dropna().unique():
        mask = pdsch_df['h_id'] == h_id
        h_id_df = pdsch_df[mask].copy()
        
        # Shift to get next transmission values
        h_id_df['next_nrtx'] = h_id_df['nrtx'].shift(-1)
        h_id_df['next_tbs'] = h_id_df['tbs'].shift(-1)
        h_id_df['next_time_dt'] = h_id_df['time_dt'].shift(-1)
        h_id_df['next_time_diff_ms'] = (h_id_df['next_time_dt'] - h_id_df['time_dt']).dt.total_seconds() * 1000
        
        # Update main dataframe
        pdsch_df.loc[mask, 'next_nrtx'] = h_id_df['next_nrtx'].values
        pdsch_df.loc[mask, 'next_tbs'] = h_id_df['next_tbs'].values
        pdsch_df.loc[mask, 'next_time_diff_ms'] = h_id_df['next_time_diff_ms'].values
    
    print("Determining outcomes...")
    
    # Determine outcome for each transmission
    def determine_outcome(row):
        nrtx = row['nrtx']
        next_nrtx = row['next_nrtx']
        tbs = row['tbs']
        next_tbs = row['next_tbs']
        next_time_diff = row['next_time_diff_ms']
        
        # Check if retransmitted (next TX has higher nrtx and similar TBS, within reasonable time)
        if pd.notna(next_nrtx) and next_nrtx > nrtx:
            # Check if it's same TB (similar TBS and reasonable time gap)
            if pd.notna(next_tbs) and pd.notna(next_time_diff):
                if abs(next_tbs - tbs) < 100 and next_time_diff < 100:  # Within 100ms
                    return {'success': False, 'outcome': 'retransmitted', 'reason': f'Retransmitted at nrtx={int(next_nrtx)}'}
        
        # If not retransmitted, assume ACK'd (implicit success)
        return {'success': True, 'outcome': 'acked', 'reason': 'No retransmission (implicit ACK)'}
    
    # Apply outcome determination
    outcomes = pdsch_df.apply(determine_outcome, axis=1, result_type='expand')
    pdsch_df['success'] = outcomes['success']
    pdsch_df['outcome'] = outcomes['outcome']
    pdsch_df['reason'] = outcomes['reason']
    
    # Check for dropped transmissions
    if len(discarding_df) > 0:
        print(f"Found {len(discarding_df)} 'Discarding DL HARQ' events, marking as dropped...")
        discarding_df['time_dt'] = pd.to_datetime(discarding_df['time'])
        
        for _, discard in discarding_df.iterrows():
            h_id = discard['h_id']
            discard_time = discard['time_dt']
            
            # Find transmissions with this h_id within 100ms before discard event
            mask = (
                (pdsch_df['h_id'] == h_id) &
                (pdsch_df['time_dt'] <= discard_time) &
                ((discard_time - pdsch_df['time_dt']).dt.total_seconds() * 1000 <= 100)
            )
            
            if mask.any():
                # Mark the most recent one as dropped
                candidates = pdsch_df[mask]
                latest_idx = candidates['time_dt'].idxmax()
                pdsch_df.loc[latest_idx, 'success'] = False
                pdsch_df.loc[latest_idx, 'outcome'] = 'dropped'
                pdsch_df.loc[latest_idx, 'reason'] = 'Max retransmissions exceeded'
    
    # Create results dataframe
    results_df = pdsch_df[['time', 'slot', 'time_ms', 'h_id', 'tbs', 'rv', 'nrtx', 'success', 'outcome', 'reason']].copy()
    results_df.columns = ['tx_time', 'tx_slot', 'tx_time_ms', 'h_id', 'tbs', 'rv', 'nrtx', 'success', 'outcome', 'reason']
    
    # Statistics
    print("\n" + "=" * 70)
    print("ANALYSIS RESULTS - HARQ State Machine")
    print("=" * 70)
    
    total_transmissions = len(results_df)
    successful = results_df['success'].sum()
    unsuccessful = total_transmissions - successful
    
    print(f"\nTotal PDSCH transmissions: {total_transmissions:,}")
    print(f"Successful (ACK'd): {successful:,} ({100*successful/total_transmissions:.2f}%)")
    print(f"Unsuccessful (NACK'd/Dropped): {unsuccessful:,} ({100*unsuccessful/total_transmissions:.2f}%)")
    
    # Break down by outcome
    print(f"\nOutcome distribution:")
    outcome_counts = results_df['outcome'].value_counts()
    for outcome, count in outcome_counts.items():
        print(f"  {outcome}: {count:,} ({100*count/total_transmissions:.2f}%)")
    
    print(f"\nOverall Success Rate:")
    print(f"  Successful: {successful:,} ({100*successful/total_transmissions:.2f}%)")
    print(f"  Failed: {unsuccessful:,} ({100*unsuccessful/total_transmissions:.2f}%)")
    
    # Break down by retransmission count
    print("\n" + "-" * 70)
    print("Breakdown by retransmission count (nrtx):")
    print("-" * 70)
    for nrtx_val in sorted(results_df['nrtx'].dropna().unique()):
        nrtx_subset = results_df[results_df['nrtx'] == nrtx_val]
        nrtx_success = nrtx_subset['success'].sum()
        nrtx_total = len(nrtx_subset)
        print(f"nrtx={int(nrtx_val)}: {nrtx_success:,}/{nrtx_total:,} successful ({100*nrtx_success/nrtx_total:.2f}%)")
        
        # Show outcome breakdown for this nrtx
        nrtx_outcomes = nrtx_subset['outcome'].value_counts()
        outcome_str = ", ".join([f"{outcome}={count}" for outcome, count in nrtx_outcomes.items()])
        print(f"  Outcomes: {outcome_str}")
    
    # Break down by redundancy version
    print("\n" + "-" * 70)
    print("Breakdown by redundancy version (rv):")
    print("-" * 70)
    for rv_val in sorted(results_df['rv'].dropna().unique()):
        rv_subset = results_df[results_df['rv'] == rv_val]
        rv_success = rv_subset['success'].sum()
        rv_total = len(rv_subset)
        print(f"rv={int(rv_val)}: {rv_success:,}/{rv_total:,} successful ({100*rv_success/rv_total:.2f}%)")
    
    # Save results to CSV
    output_file = './pdsch_ack_analysis.csv'
    results_df.to_csv(output_file, index=False)
    print(f"\n✓ Detailed results saved to: {output_file}")
    
    # Verify HARQ retransmission logic
    verify_harq_retransmission_logic(results_df)
    
    # Additional analysis: Interference intervals and time between losses
    analyze_interference_intervals(results_df)
    
    return results_df

def verify_harq_retransmission_logic(results_df):
    """
    Verify that retransmissions (nrtx > 0) only occur when previous transmissions
    for the same h_id failed to receive ACK.
    """
    print("\n" + "=" * 70)
    print("HARQ RETRANSMISSION LOGIC VERIFICATION")
    print("=" * 70)
    
    # Sort by time
    results_sorted = results_df.sort_values('tx_time_ms').copy()
    
    # For each retransmission (nrtx > 0), check if previous transmission with same h_id failed
    retransmissions = results_sorted[results_sorted['nrtx'] > 0].copy()
    
    if len(retransmissions) == 0:
        print("\nNo retransmissions found (all nrtx=0).")
        return
    
    print(f"\nTotal retransmissions (nrtx > 0): {len(retransmissions):,}")
    
    verification_results = []
    
    for idx, retx_row in retransmissions.iterrows():
        h_id = retx_row['h_id']
        nrtx = retx_row['nrtx']
        tx_time_ms = retx_row['tx_time_ms']
        tx_slot = retx_row['tx_slot']
        
        # Find previous transmission with same h_id
        previous_txs = results_sorted[
            (results_sorted['h_id'] == h_id) &
            (results_sorted['tx_time_ms'] < tx_time_ms)
        ]
        
        if len(previous_txs) > 0:
            # Get the most recent previous transmission
            prev_tx = previous_txs.iloc[-1]
            prev_success = prev_tx['success']
            prev_slot = prev_tx['tx_slot']
            prev_nrtx = prev_tx['nrtx']
            
            # Expected: previous transmission should have failed (success=False)
            is_correct = not prev_success
            
            verification_results.append({
                'h_id': h_id,
                'retx_slot': tx_slot,
                'retx_nrtx': nrtx,
                'prev_slot': prev_slot,
                'prev_nrtx': prev_nrtx,
                'prev_success': prev_success,
                'logic_correct': is_correct
            })
        else:
            # No previous transmission found - might be from before log started
            verification_results.append({
                'h_id': h_id,
                'retx_slot': tx_slot,
                'retx_nrtx': nrtx,
                'prev_slot': None,
                'prev_nrtx': None,
                'prev_success': None,
                'logic_correct': None  # Cannot verify
            })
    
    verification_df = pd.DataFrame(verification_results)
    
    # Count verifiable retransmissions
    verifiable = verification_df[verification_df['logic_correct'].notna()]
    
    if len(verifiable) == 0:
        print("\nNo verifiable retransmissions (all occurred before log start).")
        return
    
    correct_count = verifiable['logic_correct'].sum()
    incorrect_count = (~verifiable['logic_correct']).sum()
    
    print(f"\nVerifiable retransmissions: {len(verifiable):,}")
    print(f"  ✓ Correct (prev TX failed): {correct_count:,} ({100*correct_count/len(verifiable):.2f}%)")
    print(f"  ✗ Incorrect (prev TX succeeded): {incorrect_count:,} ({100*incorrect_count/len(verifiable):.2f}%)")
    
    if incorrect_count > 0:
        print(f"\n⚠️  WARNING: Found {incorrect_count} retransmissions where previous transmission succeeded!")
        print(f"    This may indicate a matching logic issue or unexpected HARQ behavior.")
        
        # Show examples
        incorrect_cases = verification_df[verification_df['logic_correct'] == False].head(10)
        print(f"\n  First {min(10, len(incorrect_cases))} incorrect cases:")
        for idx, row in incorrect_cases.iterrows():
            print(f"    h_id={row['h_id']:.0f}: retx at {row['retx_slot']} (nrtx={row['retx_nrtx']:.0f}) "
                  f"but prev at {row['prev_slot']} (nrtx={row['prev_nrtx']:.0f}) succeeded")
    else:
        print(f"\n✓ All retransmissions correctly follow failed previous transmissions!")
    
    # Check unverifiable cases
    unverifiable = verification_df[verification_df['logic_correct'].isna()]
    if len(unverifiable) > 0:
        print(f"\nUnverifiable retransmissions: {len(unverifiable):,} (no previous TX in log)")
        print(f"  These likely occurred at the start of the log capture.")

def analyze_interference_intervals(results_df):
    """
    Analyze interference intervals: time between lost packets and 
    identify periods of sustained interference.
    H_ID AGNOSTIC - looks at ALL transmissions chronologically.
    """
    print("\n" + "=" * 70)
    print("INTERFERENCE INTERVAL ANALYSIS (H_ID AGNOSTIC)")
    print("=" * 70)
    
    # Sort by time - THIS IS KEY: analyze all TXs chronologically regardless of h_id
    results_sorted = results_df.sort_values('tx_time_ms').copy()
    
    # Get ALL successful and unsuccessful transmissions
    print(f"\nTotal transmissions (all h_ids): {len(results_sorted):,}")
    print(f"  Successful: {results_sorted['success'].sum():,} ({100*results_sorted['success'].sum()/len(results_sorted):.2f}%)")
    print(f"  Failed: {(~results_sorted['success']).sum():,} ({100*(~results_sorted['success']).sum()/len(results_sorted):.2f}%)")
    
    # Get unsuccessful transmissions (failures)
    failures = results_sorted[results_sorted['success'] == False].copy()
    
    if len(failures) == 0:
        print("\nNo failed transmissions found - no interference detected!")
        return
    
    print(f"\nAnalyzing {len(failures):,} failed transmissions...")
    
    # Calculate time between consecutive failures (h_id agnostic)
    failures['time_diff_ms'] = failures['tx_time_ms'].diff()
    failures['time_diff_s'] = failures['time_diff_ms'] / 1000.0
    
    # Skip first row (no previous failure to compare to)
    time_diffs = failures['time_diff_s'].dropna()
    time_diffs_ms = failures['time_diff_ms'].dropna()
    
    if len(time_diffs) == 0:
        print("\nOnly one failure detected - cannot calculate intervals.")
        return
    
    print("\n" + "-" * 70)
    print("Time Between Consecutive Lost Packets (ANY h_id):")
    print("-" * 70)
    print(f"  Mean:   {time_diffs.mean():.3f} seconds ({time_diffs.mean()*1000:.2f} ms)")
    print(f"  Median: {time_diffs.median():.3f} seconds ({time_diffs.median()*1000:.2f} ms)")
    print(f"  Std Dev: {time_diffs.std():.3f} seconds ({time_diffs.std()*1000:.2f} ms)")
    print(f"  Min:    {time_diffs.min():.3f} seconds ({time_diffs.min()*1000:.2f} ms)")
    print(f"  Max:    {time_diffs.max():.3f} seconds ({time_diffs.max()*1000:.2f} ms)")
    
    # Percentiles
    print(f"\n  Percentiles:")
    for pct in [25, 50, 75, 90, 95, 99]:
        val = time_diffs.quantile(pct/100.0)
        print(f"    {pct}th: {val:.3f} seconds ({val*1000:.2f} ms)")
    
    # Check for periodic patterns - 155Hz = 6.45ms, or every 6ms
    print("\n" + "-" * 70)
    print("Checking for Periodic Interference Patterns:")
    print("-" * 70)
    
    # Check for 6ms periodicity
    period_6ms = time_diffs_ms[(time_diffs_ms >= 5.5) & (time_diffs_ms <= 6.5)]
    period_155hz = time_diffs_ms[(time_diffs_ms >= 6.0) & (time_diffs_ms <= 7.0)]  # 155Hz = 6.45ms
    
    print(f"\n  Failures with ~6ms spacing (5.5-6.5ms):")
    print(f"    Count: {len(period_6ms):,} ({100*len(period_6ms)/len(time_diffs_ms):.2f}% of inter-failure intervals)")
    if len(period_6ms) > 0:
        print(f"    Mean: {period_6ms.mean():.3f} ms")
        print(f"    Std: {period_6ms.std():.3f} ms")
    
    print(f"\n  Failures with ~155Hz spacing (6.0-7.0ms = ~155Hz):")
    print(f"    Count: {len(period_155hz):,} ({100*len(period_155hz)/len(time_diffs_ms):.2f}% of inter-failure intervals)")
    if len(period_155hz) > 0:
        print(f"    Mean: {period_155hz.mean():.3f} ms (= {1000/period_155hz.mean():.1f} Hz)")
        print(f"    Std: {period_155hz.std():.3f} ms")
    
    # Histogram of inter-failure times
    print(f"\n  Inter-failure time distribution (0-20ms, 0.5ms bins):")
    import numpy as np
    bins = np.arange(0, 20.5, 0.5)
    hist, bin_edges = np.histogram(time_diffs_ms, bins=bins)
    
    # Show top 10 most common intervals
    top_bins = sorted(zip(hist, bin_edges[:-1]), reverse=True)[:10]
    print(f"\n  Top 10 most common inter-failure intervals:")
    for count, bin_start in top_bins:
        if count > 0:
            bin_end = bin_start + 0.5
            pct = 100 * count / len(time_diffs_ms)
            print(f"    {bin_start:.1f}-{bin_end:.1f} ms: {count:,} occurrences ({pct:.2f}%)")
    
    # Identify interference bursts
    # Define a burst as consecutive failures within a short time window
    burst_threshold_ms = 100  # 100ms - failures closer than this are considered a burst
    
    print("\n" + "-" * 70)
    print(f"Interference Burst Detection (threshold: {burst_threshold_ms}ms)")
    print("-" * 70)
    
    # Mark bursts
    failures['is_burst'] = failures['time_diff_ms'] < burst_threshold_ms
    failures['is_burst'] = failures['is_burst'].fillna(False)
    
    # Assign burst IDs
    failures['burst_id'] = (~failures['is_burst']).cumsum()
    
    # Analyze bursts (groups with multiple failures)
    burst_groups = failures.groupby('burst_id').agg({
        'tx_time': ['first', 'last'],
        'tx_time_ms': ['first', 'last', 'count'],
        'tx_slot': ['first', 'last'],
        'h_id': list,
        'nrtx': list
    })
    
    # Filter to bursts with 2+ failures
    burst_groups['count'] = burst_groups[('tx_time_ms', 'count')]
    significant_bursts = burst_groups[burst_groups['count'] >= 2]
    
    if len(significant_bursts) == 0:
        print(f"\nNo interference bursts detected (no consecutive failures within {burst_threshold_ms}ms).")
    else:
        print(f"\nDetected {len(significant_bursts)} interference bursts (2+ failures within {burst_threshold_ms}ms):")
        print(f"  Total failures in bursts: {significant_bursts['count'].sum():.0f}")
        print(f"  Mean burst size: {significant_bursts['count'].mean():.2f} failures")
        print(f"  Max burst size: {significant_bursts['count'].max():.0f} failures")
        
        # Calculate burst durations
        burst_groups['duration_ms'] = burst_groups[('tx_time_ms', 'last')] - burst_groups[('tx_time_ms', 'first')]
        significant_bursts = burst_groups[burst_groups['count'] >= 2]
        
        print(f"\n  Burst duration statistics:")
        print(f"    Mean: {significant_bursts['duration_ms'].mean():.2f} ms")
        print(f"    Median: {significant_bursts['duration_ms'].median():.2f} ms")
        print(f"    Max: {significant_bursts['duration_ms'].max():.2f} ms")
        
        # Show largest bursts
        print(f"\n  Top 10 largest interference bursts:")
        top_bursts = significant_bursts.nlargest(10, 'count')
        for idx, (burst_id, row) in enumerate(top_bursts.iterrows(), 1):
            start_time = row[('tx_time', 'first')]
            end_time = row[('tx_time', 'last')]
            duration = float(row['duration_ms'])
            count = float(row['count'])
            start_slot = row[('tx_slot', 'first')]
            end_slot = row[('tx_slot', 'last')]
            print(f"    {idx:2d}. Burst #{burst_id}: {count:.0f} failures, "
                  f"{duration:.2f}ms duration, slots {start_slot} to {end_slot}")
    
    # Identify isolated failures vs burst failures
    isolated_failures = failures[~failures['is_burst']].copy()
    burst_failures = failures[failures['is_burst']].copy()
    
    print("\n" + "-" * 70)
    print("Failure Classification:")
    print("-" * 70)
    print(f"  Isolated failures: {len(isolated_failures):,} ({100*len(isolated_failures)/len(failures):.2f}%)")
    print(f"  Burst failures: {len(burst_failures):,} ({100*len(burst_failures)/len(failures):.2f}%)")
    
    # Check HARQ process ID (h_id) patterns
    print("\n" + "-" * 70)
    print("HARQ Process Analysis for Failed Transmissions:")
    print("-" * 70)
    h_id_failure_counts = failures['h_id'].value_counts().sort_index()
    print(f"\nFailures by HARQ Process ID (h_id):")
    for h_id, count in h_id_failure_counts.items():
        print(f"  h_id={h_id:.0f}: {count:,} failures ({100*count/len(failures):.2f}%)")
    
    # Check retransmission patterns
    print("\n" + "-" * 70)
    print("Retransmission Pattern Analysis:")
    print("-" * 70)
    nrtx_failure_counts = failures['nrtx'].value_counts().sort_index()
    print(f"\nFailed transmissions by retransmission count (nrtx):")
    for nrtx, count in nrtx_failure_counts.items():
        print(f"  nrtx={nrtx:.0f}: {count:,} failures")
    
    # Save detailed failure analysis
    failures_output = './failure_intervals_analysis.csv'
    failures.to_csv(failures_output, index=False)
    print(f"\n✓ Detailed failure interval data saved to: {failures_output}")

def plot_harq_acks_over_time(results_df, output_file='./harq_acks_over_time.png', output_file2='./harq_acks_by_sfn.png'):
    """
    Create visualization of HARQ-ACK success/failure over time.
    """
    print("\n" + "=" * 70)
    print("GENERATING VISUALIZATIONS")
    print("=" * 70)
    
    # Create figure with multiple subplots
    fig, axes = plt.subplots(4, 1, figsize=(16, 12))
    fig.suptitle('DL PDSCH HARQ-ACK Analysis Over Time', fontsize=16, fontweight='bold')
    
    # Convert time to relative seconds from start
    results_df['time_dt'] = pd.to_datetime(results_df['tx_time'])
    start_time = results_df['time_dt'].min()
    results_df['relative_time_s'] = (results_df['time_dt'] - start_time).dt.total_seconds()
    
    # 1. Success/Failure scatter plot
    ax1 = axes[0]
    successful = results_df[results_df['success'] == True]
    unsuccessful = results_df[results_df['success'] == False]
    
    ax1.scatter(successful['relative_time_s'], successful['h_id'], 
                c='green', alpha=0.6, s=10, label=f'Success (n={len(successful):,})')
    ax1.scatter(unsuccessful['relative_time_s'], unsuccessful['h_id'], 
                c='red', alpha=0.6, s=10, label=f'Fail (n={len(unsuccessful):,})')
    ax1.set_ylabel('HARQ Process ID')
    ax1.set_title('HARQ-ACK Status per Transmission')
    ax1.legend(loc='upper right')
    ax1.grid(True, alpha=0.3)
    
    # 2. Success rate over time (rolling window)
    ax2 = axes[1]
    window_size = 100  # transactions
    results_df_sorted = results_df.sort_values('relative_time_s')
    rolling_success = results_df_sorted['success'].rolling(window=window_size, min_periods=1).mean() * 100
    
    ax2.plot(results_df_sorted['relative_time_s'], rolling_success, 
             color='blue', linewidth=1.5, label=f'{window_size}-transmission moving average')
    ax2.axhline(y=rolling_success.mean(), color='red', linestyle='--', 
                label=f'Overall average: {rolling_success.mean():.2f}%')
    ax2.set_ylabel('Success Rate (%)')
    ax2.set_title(f'HARQ-ACK Success Rate Over Time (Rolling Window: {window_size} transmissions)')
    ax2.legend(loc='lower right')
    ax2.grid(True, alpha=0.3)
    ax2.set_ylim([0, 105])
    
    # 3. Retransmission count distribution over time
    ax3 = axes[2]
    for nrtx_val in sorted(results_df['nrtx'].dropna().unique()):
        nrtx_data = results_df[results_df['nrtx'] == nrtx_val]
        ax3.scatter(nrtx_data['relative_time_s'], [nrtx_val] * len(nrtx_data), 
                   alpha=0.5, s=5, label=f'nrtx={int(nrtx_val)} (n={len(nrtx_data):,})')
    ax3.set_ylabel('Number of Retransmissions')
    ax3.set_title('Retransmission Count per PDSCH Transmission')
    ax3.legend(loc='upper right', fontsize=8)
    ax3.grid(True, alpha=0.3)
    
    # 4. Time-binned success rate
    ax4 = axes[3]
    # Bin data into time intervals
    bin_duration_s = 1.0  # 1 second bins
    max_time = results_df['relative_time_s'].max()
    bins = np.arange(0, max_time + bin_duration_s, bin_duration_s)
    results_df['time_bin'] = pd.cut(results_df['relative_time_s'], bins=bins)
    
    binned_stats = results_df.groupby('time_bin').agg({
        'success': ['sum', 'count', 'mean']
    })
    binned_stats.columns = ['successful', 'total', 'success_rate']
    binned_stats['success_rate_pct'] = binned_stats['success_rate'] * 100
    
    # Get bin centers for plotting
    bin_centers = [(b.left + b.right) / 2 for b in binned_stats.index]
    
    ax4.bar(bin_centers, binned_stats['success_rate_pct'], 
            width=bin_duration_s*0.8, color='steelblue', alpha=0.7)
    ax4.axhline(y=results_df['success'].mean() * 100, color='red', linestyle='--', 
                label=f'Overall: {results_df["success"].mean()*100:.2f}%')
    ax4.set_xlabel('Time (seconds from start)')
    ax4.set_ylabel('Success Rate (%)')
    ax4.set_title(f'HARQ-ACK Success Rate (binned by {bin_duration_s}s intervals)')
    ax4.legend(loc='lower right')
    ax4.grid(True, alpha=0.3, axis='y')
    ax4.set_ylim([0, 105])
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    print(f"\n✓ Visualization saved to: {output_file}")
    
    # Create additional plot: Success rate by SFN
    fig2, ax = plt.subplots(figsize=(16, 6))
    
    # Extract SFN from slot
    results_df['sfn'] = results_df['tx_slot'].apply(lambda x: parse_slot(x)[0])
    
    sfn_stats = results_df.groupby('sfn').agg({
        'success': ['sum', 'count', 'mean']
    })
    sfn_stats.columns = ['successful', 'total', 'success_rate']
    sfn_stats['success_rate_pct'] = sfn_stats['success_rate'] * 100
    
    ax.plot(sfn_stats.index, sfn_stats['success_rate_pct'], 
            marker='o', markersize=3, linewidth=1, color='blue', alpha=0.7)
    ax.axhline(y=results_df['success'].mean() * 100, color='red', linestyle='--', 
               label=f'Overall: {results_df["success"].mean()*100:.2f}%')
    ax.set_xlabel('SFN (Sub-Frame Number)')
    ax.set_ylabel('Success Rate (%)')
    ax.set_title('HARQ-ACK Success Rate by SFN')
    ax.legend(loc='lower right')
    ax.grid(True, alpha=0.3)
    ax.set_ylim([0, 105])
    
    plt.tight_layout()
    plt.savefig(output_file2, dpi=150, bbox_inches='tight')
    print(f"✓ SFN-based visualization saved to: {output_file2}")
    
    return fig, fig2

def main():
    """
    Main analysis function.
    """
    # Parse command line arguments for folder path
    if len(sys.argv) > 1:
        folder_path = sys.argv[1]
    else:
        print("\nUsage: python3 analyze_harq_acks.py <folder_path>")
        print("Example: python3 analyze_harq_acks.py logs_20251103_055023")
        print("\nDefaulting to current directory...")
        folder_path = '.'  # Default to current directory
    
    # Ensure folder path exists
    if not os.path.exists(folder_path):
        print(f"Error: Folder '{folder_path}' does not exist!")
        sys.exit(1)
    
    # Construct paths
    harq_log_file = os.path.join(folder_path, 'harqLog.csv')
    output_csv = os.path.join(folder_path, 'pdsch_ack_analysis.csv')
    output_png1 = os.path.join(folder_path, 'harq_acks_over_time.png')
    output_png2 = os.path.join(folder_path, 'harq_acks_by_sfn.png')
    
    # If harqLog.csv doesn't exist, try to parse it from gnb.log
    if not os.path.exists(harq_log_file):
        print(f"harqLog.csv not found in '{folder_path}'")
        print("Looking for gnb.log file to parse...")
        
        # Find gnb.log file (may have timestamp prefix)
        gnb_log_pattern = os.path.join(folder_path, '*_gnb.log')
        gnb_log_files = glob.glob(gnb_log_pattern)
        
        # Also try without wildcard
        if not gnb_log_files:
            gnb_log_pattern2 = os.path.join(folder_path, 'gnb.log')
            if os.path.exists(gnb_log_pattern2):
                gnb_log_files = [gnb_log_pattern2]
        
        if not gnb_log_files:
            print(f"Error: No gnb.log file found in '{folder_path}'!")
            print("Expected file pattern: *_gnb.log or gnb.log")
            sys.exit(1)
        
        # Use the first gnb.log file found
        gnb_log_file = gnb_log_files[0]
        print(f"Found: {os.path.basename(gnb_log_file)}")
        
        # Parse the log file to create harqLog.csv
        success = parse_gnb_log_to_harq_csv(gnb_log_file, harq_log_file)
        
        if not success:
            print("Error: Failed to parse gnb.log file!")
            sys.exit(1)
    
    print("\n" + "=" * 70)
    print("HARQ DL PDSCH ACK Analysis Tool")
    print("=" * 70)
    print(f"\nAnalyzing folder: {folder_path}")
    print("This tool analyzes DL PDSCH transmissions and their HARQ-ACK status.")
    print("It identifies successful vs unsuccessful transmissions over time.\n")
    
    # Analyze HARQ-ACKs
    results_df = analyze_dl_pdsch_acks(harq_log_file)
    
    # Save results to the specified folder
    results_df.to_csv(output_csv, index=False)
    print(f"\n✓ Saved detailed results to: {output_csv}")
    
    # Generate visualizations
    plot_harq_acks_over_time(results_df, output_file=output_png1, output_file2=output_png2)
    
    print("\n" + "=" * 70)
    print("ANALYSIS COMPLETE")
    print("=" * 70)
    print("\nGenerated files:")
    print(f"  - {output_csv} : Detailed transmission results")
    print(f"  - {output_png1} : Time-series visualizations")
    print(f"  - {output_png2} : Success rate by SFN")
    print("\n")

if __name__ == "__main__":
    main()
