#!/usr/bin/env python3
"""
Comprehensive HARQ Analysis Script for 5G gNB Logs

Parses gnb.log files and produces harq_analysis.csv combining DL and UL data.

DOWNLINK (DL):
- PDSCH transmissions and retransmissions
- HARQ-ACK messages (ack=1/0/2)
- Discarded HARQ processes

UPLINK (UL):
- PUCCH status (valid/invalid) per slot - any format (F0/F1/F2/F3/F4)
- PUSCH CRC status (OK/FAIL) per slot
- Slots may contain PUCCH only, PUSCH only, or both together
"""

import re
import warnings
import pandas as pd
from pathlib import Path
import argparse
import os
import sys
from multiprocessing import Pool, cpu_count

warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=UserWarning)

# ── Pre-compiled regex patterns ──────────────────────────────────────────────
_RE_TIMESTAMP = re.compile(r'^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+)')
_RE_SLOT_DECISION = re.compile(r'\[\s*([\d.]+)\]\s+Slot decision')
_RE_PDSCH = re.compile(
    r'- UE PDSCH:.*?ue=(\d+).*?c-rnti=(0x[0-9a-fA-F]+).*?h_id=(\d+)'
)
# Individual field patterns (order varies in log output)
_RE_MCS = re.compile(r'mcs=(\d+)')
_RE_TBS = re.compile(r'tbs=(\d+)')
_RE_NRTX = re.compile(r'nrtx=(\d+)')
_RE_RV = re.compile(r'rv=(\d+)')
_RE_K1 = re.compile(r'k1=(\d+)')
_RE_HARQ_ACK = re.compile(
    r'- HARQ-ACK:.*?ue=(\d+).*?rnti=(0x[0-9a-fA-F]+).*?slot_rx=([\d.]+).*?h_id=(\d+).*?ack=(\d+)'
)
_RE_HARQ_ACK_TBS = re.compile(r'tbs=(\d+)')
_RE_DISCARD = re.compile(
    r'rnti=(0x[0-9a-fA-F]+).*?h_id=(\d+)'
)
_RE_DISCARD_SLOT = re.compile(r'\[\s*([\d.]+)\]')
_RE_DISCARD_TBS = re.compile(r'tbs=(\d+)')
_RE_DISCARD_RETX = re.compile(r'reTxs (\d+)')
_RE_PUCCH = re.compile(
    r'\[\s*([\d.]+)\]\s+PUCCH:.*?rnti=(0x[0-9a-fA-F]+).*?format=(\d+)'
)
_RE_PUCCH_ACK = re.compile(r'ack=([0-9]+)')
_RE_PUCCH_SR = re.compile(r'sr=(yes|no)')
_RE_PUCCH_CSI = re.compile(r'csi1?=([01]+)')
_RE_PUCCH_SINR = re.compile(r'sinr=([-\d.]+)dB')
_RE_PUCCH_METRIC = re.compile(r'metric=([\d.]+)')
_RE_PUCCH_STATUS = re.compile(r'status=(valid|invalid)')
_RE_PUSCH = re.compile(
    r'\[\s*([\d.]+)\]\s+PUSCH:.*?rnti=(0x[0-9a-fA-F]+)'
)
_RE_PUSCH_FIELDS = re.compile(
    r'h_id=(\d+).*?tbs=(\d+).*?crc=(OK|KO|FAIL).*?sinr=([-\d.]+)dB'
)
# Scheduler-level CRC indication (PUSCH CRC result reported by MAC/scheduler)
# Format: - CRC: ue=0 rnti=0x4601 pci=1 rx_slot=17.8 h_id=0 crc=false sinr=25dB
_RE_SCHED_CRC = re.compile(
    r'- CRC:.*?rnti=(0x[0-9a-fA-F]+).*?rx_slot=([\d.]+).*?h_id=(\d+).*?crc=(true|false).*?sinr=([-\d.]+)dB'
)


def format_slot(slot_string):
    """
    Format slot as "frame.SS" with zero-padded slot number.
    E.g. "295.1" -> "295.01", "295.10" -> "295.10"
    """
    if slot_string is None:
        return None
    parts = slot_string.split('.')
    if len(parts) != 2:
        return slot_string
    return f"{int(parts[0])}.{int(parts[1]):02d}"


def parse_gnb_log_single_pass(log_file):
    """
    Single-pass parser: reads gnb.log once and extracts DL + UL data together.

    Returns:
        (pdsch_data, harq_data, discard_data, pucch_data, pusch_data, sched_crc_data)
    """
    pdsch_data = []
    harq_data = []
    discard_data = []
    pucch_data = []
    pusch_data = []
    sched_crc_data = []

    current_timestamp = None
    current_slot = None
    recent_pdsch = {}  # (rnti, h_id) -> {'slot': dl_slot, 'k1': k1}

    # Track lines for PUCCH status lookahead
    prev_pucch_entry = None

    with open(log_file, 'r') as f:
        for line in f:
            # Extract timestamp
            if line[0:2] == '20':  # Fast pre-check before regex
                ts_m = _RE_TIMESTAMP.match(line)
                if ts_m:
                    current_timestamp = ts_m.group(1)

            # ── PUCCH status continuation lines ──
            # If we have a pending PUCCH and current line is indented, check for status
            if prev_pucch_entry is not None:
                if line.startswith('  '):
                    sm = _RE_PUCCH_STATUS.search(line)
                    if sm:
                        prev_pucch_entry['status'] = sm.group(1)
                    # Keep looking at continuation lines
                    continue
                else:
                    # Non-continuation line: finalize PUCCH entry
                    pucch_data.append(prev_pucch_entry)
                    prev_pucch_entry = None

            # ── Slot decisions (DL scheduling context) ──
            if 'Slot decision' in line:
                sm = _RE_SLOT_DECISION.search(line)
                if sm:
                    current_slot = sm.group(1)
                continue

            # ── DL: PDSCH ──
            if '- UE PDSCH:' in line:
                m = _RE_PDSCH.search(line)
                if m:
                    rnti = m.group(2)
                    h_id = int(m.group(3))

                    # Extract fields independently (order varies in log)
                    mcs_m = _RE_MCS.search(line)
                    tbs_m = _RE_TBS.search(line)
                    nrtx_m = _RE_NRTX.search(line)
                    rv_m = _RE_RV.search(line)
                    k1_m = _RE_K1.search(line)
                    k1 = int(k1_m.group(1)) if k1_m else None

                    if current_slot is not None and k1 is not None:
                        recent_pdsch[(rnti, h_id)] = {
                            'slot': current_slot, 'k1': k1
                        }

                    pdsch_data.append({
                        'time': current_timestamp,
                        'ue': int(m.group(1)),
                        'rnti': rnti,
                        'h_id': h_id,
                        'slot_tx': format_slot(current_slot),
                        'k1': k1,
                        'mcs': int(mcs_m.group(1)) if mcs_m else None,
                        'tbs': int(tbs_m.group(1)) if tbs_m else None,
                        'nrtx': int(nrtx_m.group(1)) if nrtx_m else 0,
                        'rv': int(rv_m.group(1)) if rv_m else 0,
                        'type': 'PDSCH'
                    })
                continue

            # ── DL: HARQ-ACK ──
            if '- HARQ-ACK:' in line:
                m = _RE_HARQ_ACK.search(line)
                if m:
                    rnti = m.group(2)
                    h_id = int(m.group(4))
                    slot_rx = m.group(3)

                    dl_slot = slot_rx
                    k1_value = None
                    pdsch_key = (rnti, h_id)
                    if pdsch_key in recent_pdsch:
                        info = recent_pdsch.pop(pdsch_key)
                        k1_value = info['k1']
                        dl_slot = info['slot']

                    tbs_m = _RE_HARQ_ACK_TBS.search(line)

                    harq_data.append({
                        'time': current_timestamp,
                        'ue': int(m.group(1)),
                        'rnti': rnti,
                        'h_id': h_id,
                        'slot_rx': format_slot(slot_rx),
                        'slot_tx': format_slot(dl_slot),
                        'k1': k1_value,
                        'ack': int(m.group(5)),
                        'tbs': int(tbs_m.group(1)) if tbs_m else None,
                        'type': 'HARQ-ACK'
                    })
                continue

            # ── DL: Discarding HARQ ──
            if 'Discarding DL HARQ' in line:
                m = _RE_DISCARD.search(line)
                if m:
                    slot_m = _RE_DISCARD_SLOT.search(line)
                    tbs_m = _RE_DISCARD_TBS.search(line)
                    retx_m = _RE_DISCARD_RETX.search(line)
                    discard_data.append({
                        'time': current_timestamp,
                        'rnti': m.group(1),
                        'h_id': int(m.group(2)),
                        'slot_tx': format_slot(slot_m.group(1)) if slot_m else None,
                        'tbs': int(tbs_m.group(1)) if tbs_m else None,
                        'max_retx': int(retx_m.group(1)) if retx_m else 4,
                        'type': 'DISCARD'
                    })
                continue

            # ── UL: Scheduler CRC (PUSCH CRC result from scheduler) ──
            if '- CRC:' in line and 'rx_slot=' in line:
                m = _RE_SCHED_CRC.search(line)
                if m:
                    sched_crc_data.append({
                        'time': current_timestamp,
                        'rnti': m.group(1),
                        'slot': format_slot(m.group(2)),
                        'h_id': int(m.group(3)),
                        'crc': 'OK' if m.group(4) == 'true' else 'FAIL',
                        'sinr': float(m.group(5)),
                        'type': 'SCHED-CRC'
                    })
                continue

            # ── UL: PUCCH (PHY debug line) ──
            if '[PHY     ] [D]' in line and 'PUCCH:' in line:
                m = _RE_PUCCH.search(line)
                if m:
                    sinr_m = _RE_PUCCH_SINR.search(line)
                    metric_m = _RE_PUCCH_METRIC.search(line)

                    # Determine UCI content
                    uci_type = None
                    uci_value = None
                    ack_m = _RE_PUCCH_ACK.search(line[line.index('PUCCH:'):])
                    if ack_m:
                        uci_type = 'ack'
                        uci_value = ack_m.group(1)
                    else:
                        sr_m = _RE_PUCCH_SR.search(line)
                        if sr_m:
                            uci_type = 'sr'
                            uci_value = sr_m.group(1)
                        else:
                            csi_m = _RE_PUCCH_CSI.search(line)
                            if csi_m:
                                uci_type = 'csi'
                                uci_value = csi_m.group(1)

                    # Start PUCCH entry; status will be read from continuation lines
                    prev_pucch_entry = {
                        'time': current_timestamp,
                        'slot': format_slot(m.group(1)),
                        'rnti': m.group(2),
                        'format': int(m.group(3)),
                        'uci_type': uci_type,
                        'uci_value': uci_value,
                        'metric': float(metric_m.group(1)) if metric_m else None,
                        'sinr': float(sinr_m.group(1)) if sinr_m else None,
                        'status': 'valid',  # default, overridden by continuation
                        'type': 'PUCCH'
                    }
                continue

            # ── UL: PUSCH (PHY debug line) ──
            if '[PHY     ] [D]' in line and 'PUSCH:' in line:
                m = _RE_PUSCH.search(line)
                if m:
                    fm = _RE_PUSCH_FIELDS.search(line)
                    if fm:
                        pusch_data.append({
                            'time': current_timestamp,
                            'slot': format_slot(m.group(1)),
                            'rnti': m.group(2),
                            'h_id': int(fm.group(1)),
                            'tbs': int(fm.group(2)),
                            'crc': 'FAIL' if fm.group(3) in ('KO', 'FAIL') else 'OK',
                            'sinr': float(fm.group(4)),
                            'type': 'PUSCH'
                        })
                continue

    # Flush any pending PUCCH entry
    if prev_pucch_entry is not None:
        pucch_data.append(prev_pucch_entry)

    return pdsch_data, harq_data, discard_data, pucch_data, pusch_data, sched_crc_data


def build_comprehensive_timeline(harq_data, pucch_data, pusch_data, sched_crc_data=None):
    """
    Build combined DL+UL timeline using pd.concat (vectorized, no iterrows).

    UL slots can contain:
    - PUCCH only (e.g. SR, CSI reporting)
    - PUSCH only (data transmission)
    - Both PUCCH and PUSCH in the same slot (combined UCI on PUSCH or separate)
    - Scheduler CRC indications (PUSCH CRC result from MAC scheduler)

    Returns: DataFrame sorted by time/slot
    """
    if sched_crc_data is None:
        sched_crc_data = []

    frames = []

    # DL HARQ-ACK entries
    if harq_data:
        df_harq = pd.DataFrame(harq_data)
        dl = pd.DataFrame({
            'time': pd.to_datetime(df_harq['time'], format='ISO8601'),
            'slot_id': df_harq['slot_tx'],
            'direction': 'DL',
            'rnti': df_harq['rnti'],
            'type': 'HARQ-ACK',
            'ack_status': df_harq['ack'].astype(float),
            'h_id': df_harq['h_id'].astype(float),
            'tbs': df_harq['tbs'].astype(float),
            'ul_status': None,
            'ul_sinr': None,
            'ul_metric': None,
            'uci_info': None,
        })
        frames.append(dl)

    # UL PUCCH entries (any format: F0, F1, F2, F3, F4)
    if pucch_data:
        df_pucch = pd.DataFrame(pucch_data)
        uci = df_pucch.apply(
            lambda r: f"{r['uci_type']}={r['uci_value']}"
            if r['uci_type'] and r['uci_value'] else None,
            axis=1
        )
        ul_pucch = pd.DataFrame({
            'time': pd.to_datetime(df_pucch['time'], format='ISO8601'),
            'slot_id': df_pucch['slot'],
            'direction': 'UL',
            'rnti': df_pucch['rnti'],
            'type': 'PUCCH-F' + df_pucch['format'].astype(str),
            'ack_status': pd.array([pd.NA] * len(df_pucch), dtype='Float64'),
            'h_id': pd.array([pd.NA] * len(df_pucch), dtype='Float64'),
            'tbs': pd.array([pd.NA] * len(df_pucch), dtype='Float64'),
            'ul_status': df_pucch['status'],
            'ul_sinr': df_pucch['sinr'],
            'ul_metric': df_pucch['metric'],
            'uci_info': uci,
        })
        frames.append(ul_pucch)

    # UL PUSCH entries
    if pusch_data:
        df_pusch = pd.DataFrame(pusch_data)
        ul_pusch = pd.DataFrame({
            'time': pd.to_datetime(df_pusch['time'], format='ISO8601'),
            'slot_id': df_pusch['slot'],
            'direction': 'UL',
            'rnti': df_pusch['rnti'],
            'type': 'PUSCH',
            'ack_status': pd.array([pd.NA] * len(df_pusch), dtype='Float64'),
            'h_id': df_pusch['h_id'].astype(float),
            'tbs': df_pusch['tbs'].astype(float),
            'ul_status': df_pusch['crc'],
            'ul_sinr': df_pusch['sinr'],
            'ul_metric': pd.array([pd.NA] * len(df_pusch), dtype='Float64'),
            'uci_info': None,
        })
        frames.append(ul_pusch)

    # Note: SCHED-CRC (scheduler "- CRC:" lines) are NOT included in the timeline
    # because PHY PUSCH lines with crc=KO/OK already capture the same data.
    # sched_crc_data is kept for logging/stats only.

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)

    # Sort by frame.slot numerically
    parts = df['slot_id'].str.split('.', expand=True)
    df['_frame'] = parts[0].astype(int)
    df['_slot'] = parts[1].astype(int)
    df['_datetime'] = pd.to_datetime(df['time'])
    df['_time_1s'] = df['_datetime'].astype('int64') // 10**9

    df = df.sort_values(['_time_1s', '_frame', '_slot', 'time']).reset_index(drop=True)
    df = df.drop(columns=['_frame', '_slot', '_datetime', '_time_1s'])

    return df


def process_single_log(log_file, output_dir, all_csvs=False, quiet=False):
    """
    Process a single gnb.log file and write harq_analysis.csv.

    Args:
        log_file: Path to gnb.log
        output_dir: Directory to write output CSVs
        all_csvs: If True, also write pdsch/discard/pucch/pusch/ul_timeline CSVs
        quiet: Suppress output

    Returns:
        (log_file, success, message)
    """
    log_file = str(log_file)
    output_dir = str(output_dir)

    try:
        os.makedirs(output_dir, exist_ok=True)

        # Single-pass parse
        pdsch_data, harq_data, discard_data, pucch_data, pusch_data, sched_crc_data = \
            parse_gnb_log_single_pass(log_file)

        # Build combined timeline
        timeline = build_comprehensive_timeline(harq_data, pucch_data, pusch_data, sched_crc_data)

        if not timeline.empty:
            timeline.to_csv(f"{output_dir}/harq_analysis.csv", index=False)

        # Extra CSVs if requested
        if all_csvs:
            if pdsch_data:
                pd.DataFrame(pdsch_data).to_csv(
                    f"{output_dir}/pdsch_analysis.csv", index=False)
            if discard_data:
                pd.DataFrame(discard_data).to_csv(
                    f"{output_dir}/discard_analysis.csv", index=False)
            if pucch_data:
                pd.DataFrame(pucch_data).to_csv(
                    f"{output_dir}/pucch_detailed.csv", index=False)
            if pusch_data:
                pd.DataFrame(pusch_data).to_csv(
                    f"{output_dir}/pusch_detailed.csv", index=False)

        crc_fail = sum(1 for c in sched_crc_data if c['crc'] == 'FAIL')
        crc_total = len(sched_crc_data)
        msg = (f"DL: {len(harq_data)} HARQ-ACK, {len(pdsch_data)} PDSCH, "
               f"{len(discard_data)} discard | "
               f"UL: {len(pucch_data)} PUCCH, {len(pusch_data)} PUSCH, "
               f"{crc_fail}/{crc_total} CRC-FAIL")

        if not quiet:
            print(f"  OK  {msg}")

        return (log_file, True, msg)

    except Exception as e:
        msg = str(e)
        if not quiet:
            print(f"  FAIL  {msg}")
        return (log_file, False, msg)


def _worker(args):
    """Multiprocessing worker wrapper."""
    log_file, output_dir, all_csvs = args
    return process_single_log(log_file, output_dir, all_csvs, quiet=True)


def batch_process(base_dir, workers=None, all_csvs=False):
    """
    Process all gnb.log files under base_dir using multiprocessing.

    Expects structure: base_dir/runN/TIMESTAMP/*_gnb.log
    Writes harq_analysis.csv alongside each gnb.log.
    """
    base_path = Path(base_dir)

    # Find all gnb.log files
    log_files = sorted(base_path.glob('run*/*/*_gnb.log'))
    if not log_files:
        print(f"No gnb.log files found under {base_dir}")
        return

    print(f"Found {len(log_files)} gnb.log files")

    # Build work items: (log_file, output_dir, all_csvs)
    work = []
    for lf in log_files:
        work.append((str(lf), str(lf.parent), all_csvs))

    if workers is None:
        workers = min(cpu_count(), 8)

    print(f"Processing with {workers} workers...")

    success = 0
    fail = 0

    with Pool(workers) as pool:
        for i, (lf, ok, msg) in enumerate(pool.imap_unordered(_worker, work), 1):
            run_name = Path(lf).parent.parent.name
            status = "OK" if ok else "FAIL"
            if ok:
                success += 1
            else:
                fail += 1

            # Print progress every 50 files or on failure
            if i % 50 == 0 or not ok:
                print(f"  [{i}/{len(work)}] {run_name}: {status}  {msg}")

    print(f"\nDone: {success} succeeded, {fail} failed out of {len(work)}")


def main():
    parser = argparse.ArgumentParser(
        description='HARQ analysis: parse gnb.log -> harq_analysis.csv (DL + UL)')

    # Positional: single log file (optional if --batch is used)
    parser.add_argument('log_file', nargs='?', help='Path to gnb.log file')
    parser.add_argument('--output-dir', default='.', help='Output directory for results')
    parser.add_argument('--all-csvs', action='store_true',
                        help='Also generate pdsch/discard/pucch/pusch CSVs')

    # Batch mode
    parser.add_argument('--batch', metavar='DIR',
                        help='Batch process all runs under DIR (e.g. run*/TIMESTAMP/*_gnb.log)')
    parser.add_argument('--workers', type=int, default=None,
                        help='Number of parallel workers for batch mode (default: min(cpus, 8))')

    # Backward compat: accept and ignore these
    parser.add_argument('--csv-only', action='store_true',
                        help='(deprecated, now always CSV-only)')
    parser.add_argument('--window', type=int, default=10, help=argparse.SUPPRESS)
    parser.add_argument('--threshold', type=float, default=0.3, help=argparse.SUPPRESS)

    args = parser.parse_args()

    if args.batch:
        batch_process(args.batch, workers=args.workers, all_csvs=args.all_csvs)
    elif args.log_file:
        print(f"Processing: {args.log_file}")
        process_single_log(args.log_file, args.output_dir, all_csvs=args.all_csvs)
    else:
        parser.error('Either provide a log_file or use --batch DIR')


if __name__ == "__main__":
    main()
