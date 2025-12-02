#!/usr/bin/python3

import pandas as pd
import re
from datetime import datetime,timedelta
import json
import yaml
import os
import argparse

def read_iperf_log(filepath):
    with open(filepath, 'r') as f:
        lines = f.readlines()

    iperfStartTime = None
    data_lines = []

    # Find RadarStartTime from the last non-empty line
    for line in reversed(lines):
        if 'RadarStartTime' in line:
            #support Me being silly and haivng runs with different time stamps: 
            try:
                iperfStartTime = datetime.strptime(line.split(',')[1].strip(), "%Y-%m-%dT%H:%M:%S.%f")
                break
            except Exception as e:
                print(f"Failed to parse RadarStartTime %Y-%m-%dT%H:%M%S.%f: {e}")
                iperfStartTime = None
            try:
                iperfStartTime = datetime.strptime(line.split(',')[1].strip(), "%Y%m%d_%H%M%S")
            except Exception as e:
                print(f"Failed to parse RadarStartTime %Y%m%d_%H%M%S: {e}")
                iperfStartTime = None
            break

    for line in lines[2:]:
        if line.startswith('- - -'):
            break
        if line.strip() and line.startswith('['):
            data_lines.append(line)

    columns = ['ID', 'Interval', 'Transfer', 'Bitrate', 'Retr', 'Cwnd']
    records = []
    for line in data_lines:
        match = re.match(
            r"\[\s*(\d+)\]\s+([\d.]+-[\d.]+)\s+sec\s+([\d.]+)\s+\w+\s+([\d.]+)\s+\w+/sec\s+(\d+)\s+([\d.]+)\s+\w+",
            line
        )
        if match:
            record = {
                'ID': int(match.group(1)),
                'Interval': match.group(2),
                'Transfer': float(match.group(3)),
                'Bitrate': float(match.group(4)),
                'Retr': int(match.group(5)),
                'Cwnd': float(match.group(6))
            }
            records.append(record)

    df = pd.DataFrame(records, columns=columns)

    if iperfStartTime is not None and not df.empty:
        interval_times = []
        for i in range(len(df)):
            new_time = iperfStartTime + timedelta(seconds=i)
            interval_times.append(new_time.strftime("%Y-%m-%dT%H:%M:%S.%f"))
        df["Interval"] = interval_times
        df = df.rename(columns={'Interval': 'time'})
        df['time'] = pd.to_datetime(df['time'])           # Parse as datetime (naive)
        df['time'] = df['time'] + pd.Timedelta(hours=4) # Adjust to UTC-4 (America/New_York)
    return df

def combine_cell_and_ue_metrics(df, time_col='time', type_col='metric_type', time_thresh_us=50):
    """
    Combine consecutive 'Cell Scheduler Metrics' and 'Scheduler UE Metrics' rows with nearly identical timestamps.
    All columns from 'Scheduler UE Metrics' are prefixed with 'ue_' (except for time and metric_type).
    Returns a new DataFrame with merged rows where applicable, dropping metric_type column.
    """
    merged_rows = []
    i = 0
    n = len(df)
    while i < n:
        row = df.iloc[i]
        if row[type_col] == "Cell Scheduler Metrics":
            merged = row.to_dict()
            # Look ahead for Scheduler UE Metrics within threshold
            if i+1 < n:
                next_row = df.iloc[i+1]
                if (next_row[type_col] == "Scheduler UE Metrics" and
                    abs((next_row[time_col] - row[time_col]).total_seconds()*1e6) < time_thresh_us):
                    # Merge: prefix UE columns
                    for col in df.columns:
                        if col in ['time', 'metric_type']:
                            continue
                        merged[f'ue_{col}'] = next_row[col]
                    i += 1  # Skip the next row, as it's merged
            merged_rows.append(merged)
        elif row[type_col] != "Scheduler UE Metrics":
            merged_rows.append(row.to_dict())
        i += 1
    merged_df = pd.DataFrame(merged_rows)
    # Drop metric_type column if present
    if 'metric_type' in merged_df.columns:
        merged_df = merged_df.drop(columns=['metric_type'])
    return merged_df

def read_gnbLog_METRICS(filepath):
    metrics = []
    with open(filepath, 'r') as f:
        for line in f:
            if '[METRICS ]' in line:
                # Extract timestamp at the beginning of the line
                time_match = re.match(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+)', line)
                timestamp = time_match.group(1) if time_match else None

                # Extract metric type (between '[METRICS ]' and ':')
                metric_type_match = re.search(r'\[METRICS\s*\]\s*([^:]+):', line)
                metric_type = metric_type_match.group(1).strip() if metric_type_match else None

                # Extract all key=value pairs (handles units and floats/ints/strings)
                pairs = re.findall(r'(\w+)=([^\s\],]+)', line)
                row = {k: v for k, v in pairs}

                # Set log_type as 'metric'
                ordered_row = {'time': timestamp, 'log_type': 'metric', 'metric_type': metric_type}
                ordered_row.update(row)

                # Extract first event if present
                events_match = re.search(r'events=\[(.*?)\]', line)
                if events_match:
                    events_str = events_match.group(1)
                    first_event_match = re.search(r'\{([^}]+)\}', events_str)
                    if first_event_match:
                        first_event_str = first_event_match.group(1)
                        event_pairs = dict(re.findall(r'(\w+)=([^\s,]+)', first_event_str))
                        ordered_row['events'] = event_pairs.get('rnti')
                        ordered_row['slot'] = event_pairs.get('slot')
                        ordered_row['type'] = event_pairs.get('type')
                metrics.append(ordered_row)
            
    df = pd.DataFrame(metrics)
    if 'latency_hist' in df.columns:
        df.drop(columns=['latency_hist'], inplace=True)
    # Ensure 'time' and 'log_type' are the first columns
    cols = df.columns.tolist()
    for col in ['time', 'log_type']:
        if col in cols:
            cols.remove(col)
    df = df[['time', 'log_type'] + cols]
    # Convert 'time' column to datetime if it's not already
    df['time'] = pd.to_datetime(df['time'])

    # Sort by 'time'
    df = df.sort_values('time').reset_index(drop=True)

    # Combine consecutive Cell Scheduler Metrics & Scheduler UE Metrics, drop metric_type column
    df_merged = combine_cell_and_ue_metrics(df)
    # Ensure 'time' is first column
    if 'time' in df_merged.columns:
        cols = df_merged.columns.tolist()
        cols.remove('time')
        df_merged = df_merged[['time'] + cols]
    return df_merged

def read_gnbLog_ULmeasurementReport(filepath):
    reports = []
    with open(filepath, 'r') as f:
        lines = f.readlines()

    i = 0
    while i < len(lines):
        line = lines[i]
        if "Containerized measurementReport: [" in line:
            # Extract timestamp, c-rnti, and ue id from the RRC line
            timestamp = line.split()[0]
            c_rnti_match = re.search(r'c-rnti=0x([0-9a-fA-F]+)', line)
            c_rnti = c_rnti_match.group(1) if c_rnti_match else None
            ue_match = re.search(r'ue=(\d+)', line)
            ue_id = ue_match.group(1) if ue_match else None

            # Extract JSON block (starts after the first colon)
            json_start = line.find(': [') + 2
            json_lines = [line[json_start:].lstrip()]
            bracket_count = json_lines[0].count('[') - json_lines[0].count(']')
            i += 1
            while i < len(lines) and bracket_count > 0:
                json_lines.append(lines[i])
                bracket_count += lines[i].count('[') - lines[i].count(']')
                i += 1
            json_str = ''.join(json_lines)
            json_str = re.sub(r',\s*([\]}])', r'\1', json_str)
            try:
                json_obj = json.loads(json_str)
                for entry in json_obj:
                    try:
                        cell = entry["UL-DCCH-Message"]["message"]["c1"]["measurementReport"]["criticalExtensions"]["measurementReport"]["measResults"]["measResultServingMOList"][0]["measResultServingCell"]["measResult"]["cellResults"]["resultsSSB-Cell"]
                        reports.append({
                            "time": timestamp,
                            "log_type": "ULMeasRept",
                            "ue_id": ue_id,
                            "c_rnti": c_rnti,
                            "dl_rsrp": cell.get("rsrp") -156,
                            "dl_rsrq": cell.get("rsrq")/2 -43,
                            "dl_sinr": cell.get("sinr")/2 -23
                        })
                    except Exception as e:
                        print(f"Could not extract cell results: {e}")
            except Exception as e:
                print(f"JSON parse error: {e} in block:\n{json_str}")
        else:
            i += 1
    df = pd.DataFrame(reports)
    if not df.empty and 'time' in df.columns:
        df['time'] = pd.to_datetime(df['time'])
        cols = df.columns.tolist()
        for col in ['time', 'log_type']:
            if col in cols:
                cols.remove(col)
        df = df[['time', 'log_type'] + cols]
    return df

def extract_gnb_config_block(logfile):
    config_lines = []
    in_config = False
    with open(logfile, 'r') as f:
        for line in f:
            if '[CONFIG  ] [I] Input configuration' in line:
                in_config = True
                continue
            if in_config:
                # Stop if we hit a new timestamped log line
                if re.match(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+', line):
                    break
                config_lines.append(line)
    config_str = ''.join(config_lines)
    return config_str

def flatten_dict(d, parent_key='', sep='.'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict) or isinstance(v, list):
            # Serialize any dict or list as a compact JSON string
            items.append((new_key, json.dumps(v, separators=(',', ':'))))
        else:
            items.append((new_key, v))
    return dict(items)

def prepend_config_to_csv(config_str, csv_path):
    config_dict = yaml.safe_load(config_str)
    flat_config = flatten_dict(config_dict)
    # Join as a single line, escaping any newlines
    config_csv_line = "# gnb_config," + ",".join(f"{k}={v}" for k, v in flat_config.items()) + "\n"
    # print(f"Prepending config to CSV: {config_csv_line.strip()}")
    # Read original CSV
    with open(csv_path, 'r') as f:
        original = f.read()
    # Write new file with config at the top
    with open(csv_path, 'w') as f:
        f.write(config_csv_line)
        f.write(original)

def extract_radar_char_line(logfile):
    """Extract the last Radar_Char line from the log and format as a single-line CSV comment."""
    radar_line = None
    with open(logfile, 'r') as f:
        for line in reversed(f.readlines()):
            if line.startswith("Radar_Char,"):
                radar_line = line.strip()
                break
    if radar_line:
        return f"# {radar_line}\n"
    return ""

def extract_iperf_cmd_line(logfile):
    """Extract the last iperf3 command line from the log and format as a single-line CSV comment."""
    iperf_line = None
    with open(logfile, 'r') as f:
        for line in reversed(f.readlines()):
            if line.strip().startswith("iperf3 "):
                iperf_line = line.strip()
                break
    if iperf_line:
        return f"# {iperf_line}\n"
    return ""

def prepend_radar_to_csv(logfile, csv_path):
    radar_csv = extract_radar_char_line(logfile)
    with open(csv_path, 'r') as f:
        original = f.read()
    with open(csv_path, 'w') as f:
        f.write(radar_csv)
        f.write(original)

def prepend_iperf_to_csv(logfile, csv_path):
    iperf_csv = extract_iperf_cmd_line(logfile)
    with open(csv_path, 'r') as f:
        original = f.read()
    with open(csv_path, 'w') as f:
        f.write(iperf_csv)
        f.write(original)

def read_gnbLog_PHY_PUCCH(filepath):
    """
    Extracts PUCCH PHY blocks from gnb.log and returns a DataFrame.
    Each block starts with a timestamped line containing '[PHY     ]' and 'PUCCH:',
    followed by indented key=value lines.
    The returned DataFrame always includes a 'time' column with the timestamp.
    The second column is always 'log_type' ('PHY' or 'CSI').
    """
    records = []
    with open(filepath, 'r') as f:
        lines = f.readlines()

    i = 0
    while i < len(lines):
        line = lines[i]
        if '[PHY     ]' in line and 'PUCCH:' in line:
            # Extract timestamp
            time_match = re.match(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+)', line)
            timestamp = time_match.group(1) if time_match else None

            # Extract inline key=value pairs from the header line
            pairs = dict(re.findall(r'(\w+)=([^\s]+)', line))

            # Parse following indented lines for more key=value pairs
            j = i + 1
            while j < len(lines) and (lines[j].startswith('  ') or lines[j].strip() == ''):
                kv_match = re.match(r'\s*(\w+)=([^\s]+)', lines[j])
                if kv_match:
                    k, v = kv_match.groups()
                    pairs[k] = v
                j += 1

            # Set log_type: if any key contains 'csi', use 'CSI', else 'PHY'
            log_type = "CSI" if any('csi' in k.lower() for k in pairs.keys()) else "PHY"
            record = {'time': timestamp, 'log_type': log_type}
            record.update(pairs)
            records.append(record)
            i = j
        else:
            i += 1

    df = pd.DataFrame(records)
    if not df.empty and 'time' in df.columns and 'log_type' in df.columns:
        df['time'] = pd.to_datetime(df['time'])
        # Ensure 'time' and 'log_type' are the first columns
        cols = df.columns.tolist()
        for col in ['time', 'log_type']:
            if col in cols:
                cols.remove(col)
        df = df[['time', 'log_type'] + cols]
    return df

def process_one_pair(gnbLogFileName, iperfLogFileName, out_folder, prefix=None):
    os.makedirs(out_folder, exist_ok=True)
    if prefix:
        out_prefix = os.path.join(out_folder, prefix)
    else:
        base = os.path.basename(gnbLogFileName)
        if base.endswith('_gnb.log'):
            base = base[:-8]
        out_prefix = os.path.join(out_folder, base)
    print(f"Processing gnb log: {gnbLogFileName}")
    print(f"output prefix: {out_prefix}")
    gnbLog_metrics_df = read_gnbLog_METRICS(gnbLogFileName)
    gnbLog_ULMeas_df = read_gnbLog_ULmeasurementReport(gnbLogFileName)
    iperfLog = read_iperf_log(iperfLogFileName)
    phy_pucch_df = read_gnbLog_PHY_PUCCH(gnbLogFileName) 

    # Save with dynamic filenames (in output directory)
    phy_pucch_df.to_csv(f"{out_prefix}_phy_pucch.csv", index=False)
    gnbLog_metrics_df.to_csv(f"{out_prefix}_metrics.csv", index=False)
    gnbLog_ULMeas_df.to_csv(f"{out_prefix}_ULMeas.csv", index=False)
    iperfLog.to_csv(f"{out_prefix}_iperf.csv", index=False)
    radar_config_to_csv(gnbLogFileName, f"{out_prefix}_radar_config.csv")

    return None

def radar_config_to_csv(gnb_log_path, out_csv_path):
    """
    Reads the last Radar_Char line from the gnb.log file and writes it as a CSV row.
    """
    radar_line = None
    with open(gnb_log_path, 'r') as f:
        for line in reversed(f.readlines()):
            if line.startswith("Radar_Char,"):
                radar_line = line.strip()
                break
    if radar_line is None:
        print("No Radar_Char line found in log.")
        return

    # Parse the Radar_Char line into a dict
    parts = radar_line.split(',')
    radar_dict = {}
    for part in parts[1:]:
        if '=' in part:
            k, v = part.split('=', 1)
            radar_dict[k.strip()] = v.strip()
    # Write to CSV
    df = pd.DataFrame([radar_dict])
    df.to_csv(out_csv_path, index=False)

def main():
    print("Log Processing Script")

    #Process gnb.log
    # gnbLog = read_gnb_log('/home/eric/scripts/logs_20250531_100928/20250531_103310_gnb.log') #toMatchIperf
    # gnbLogFileName='/home/eric/scripts/logs_20250602_221431/20250603_054406_gnb.log'
    gnbLogFileName='/home/eric/scripts/logs_20250601_204143/20250601_214426_gnb.log'
    iperfLogFileName='/home/eric/scripts/logs_20250601_204143/20250601_214426_iperf3.log'
    gnbLog_metrics = read_gnbLog_METRICS(gnbLogFileName)
    print(f"{gnbLog_metrics}")
    gnbLog_metrics.to_csv('/home/eric/scripts/test_gnb_metrics.csv', index=False)
    gnbLog_ULMeas = read_gnbLog_ULmeasurementReport(gnbLogFileName)
    print(f"{gnbLog_ULMeas}")
    gnbLog_ULMeas.to_csv('/home/eric/scripts/test_gnb_ULMeas.csv', index=False)
    # exit()
    #Process iperf3.log
    # iperfLog = read_iperf_log('/home/eric/scripts/logs_20250530_105021/prf-0980_iperf3.log') #without IperfStartTimeAppended
    iperfLog = read_iperf_log(iperfLogFileName)
    low_bitrate_df = iperfLog[iperfLog['Bitrate'] < 45.1]
    print(f"{iperfLog} \n\
    LowPoint: {iperfLog['Bitrate'].min()}\n\
    LowPointAvg: {low_bitrate_df['Bitrate'].mean()}\n\
    LowPointCount: {len(low_bitrate_df)}\n\
    HighPoint: {iperfLog['Bitrate'].max()}")

    # Merge gnbLog_metrics and gnbLog_ULMeas (asof, nearest time)
    merged1 = pd.merge_asof(
        gnbLog_metrics.sort_values('time'),
        gnbLog_ULMeas.sort_values('time'),
        on='time',
        direction='nearest',
        tolerance=pd.Timedelta('500ms')  # adjust as needed
    )

    # Merge with iperfLog (asof, nearest time)
    results_df = pd.merge_asof(
        merged1.sort_values('time'),
        iperfLog.sort_values('time'),
        on='time',
        direction='nearest',
        tolerance=pd.Timedelta('500ms')  # adjust as needed
    )

    print(results_df)
    results_df.to_csv('/home/eric/scripts/test_results_merged.csv', index=False)

    gnb_config_str = extract_gnb_config_block(gnbLogFileName)
    prepend_config_to_csv(gnb_config_str, '/home/eric/scripts/test_results_merged.csv')
    prepend_radar_to_csv(gnbLogFileName, '/home/eric/scripts/test_results_merged.csv')
    prepend_iperf_to_csv(gnbLogFileName, '/home/eric/scripts/test_results_merged.csv')

    return None

def process_all_log_pairs(root_dir, output_dir, overwrite=False):
    """
    Finds all *_gnb.log and *_iperf3.log pairs in root_dir and processes them.
    """
    for dirpath, _, filenames in os.walk(root_dir):
        gnb_logs = [f for f in filenames if f.endswith('_gnb.log')]
        for gnb_log in gnb_logs:
            base = gnb_log[:-8]  # Remove '_gnb.log'
            iperf_log = f"{base}_iperf3.log"
            gnb_log_path = os.path.join(dirpath, gnb_log)
            iperf_log_path = os.path.join(dirpath, iperf_log)
            if os.path.exists(iperf_log_path):
                out_folder = output_dir
                out_prefix = os.path.join(out_folder, base)
                # Skip if already processed and not overwriting
                if not overwrite and all(
                    os.path.exists(f"{out_prefix}{suffix}")
                    for suffix in [
                        "_phy_pucch.csv",
                        "_metrics.csv",
                        "_ULMeas.csv",
                        "_iperf.csv",
                        "_radar_config.csv"
                    ]
                ):
                    print(f"Skipping {base}: already processed.")
                    continue
                print(f"Processing pair: {gnb_log_path}, {iperf_log_path}")
                process_one_pair(gnb_log_path, iperf_log_path, out_folder)
            else:
                print(f"Warning: No matching iperf3 log for {gnb_log_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process 5G gnb logs and generate CSVs.")
    parser.add_argument(
        "--gnb-log", type=str, default='/tmp/gnb.log',
        help="Path to a single gnb.log file to process (e.g., /tmp/gnb.log)"
    )
    parser.add_argument(
        "--iperf-log", type=str, default='/tmp/iperf3.log',
        help="Path to a single iperf3.log file to process (optional, for single mode)"
    )
    parser.add_argument(
        "--out-dir", type=str, default="/home/eric/OTA-Radar-5G-Trials/",
        help="Directory to save generated CSV files"
    )
    parser.add_argument(
        "--batch-root", type=str, default=None,
        help="If set, process all log pairs in this directory tree (batch mode)"
    )
    parser.add_argument(
        "--batch-out", type=str, default=None,
        help="Output directory for batch mode"
    )
    parser.add_argument(
        "--overwrite", action="store_true",
        help="Overwrite existing CSVs in batch mode"
    )
    parser.add_argument(
        "--prefix", type=str, default=None,
        help="Custom prefix for output CSV files (default: base name of gnb.log file)"
    )
    args = parser.parse_args()

    print("Running LogProcessing.py")

    if args.gnb_log:
        # Single file mode
        gnb_log = args.gnb_log
        iperf_log = args.iperf_log if args.iperf_log else gnb_log.replace("_gnb.log", "_iperf3.log")
        out_dir = args.out_dir
        os.makedirs(out_dir, exist_ok=True)
        process_one_pair(gnb_log, iperf_log, out_dir, prefix=args.prefix)
    elif args.batch_root and args.batch_out:
        # Batch mode
        os.makedirs(args.batch_out, exist_ok=True)
        process_all_log_pairs(args.batch_root, args.batch_out, overwrite=args.overwrite)
    else:
        print("Please specify either --gnb-log (and optionally --iperf-log and --out-dir) for single file mode, "
              "or --batch-root and --batch-out for batch mode.")

