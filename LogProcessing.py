#!/usr/bin/python3

import pandas as pd
import re
from datetime import datetime,timedelta
import json
import yaml
import os

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
                print(f"Failed to parse RadarStartTime %Y-%m-%dT%H:%M:%S.%f: {e}")
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

                # Store the timestamp and metric type as first keys
                ordered_row = {'time': timestamp, 'metric_type': metric_type}
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
    # Ensure 'time' and 'metric_type' are the first columns
    cols = df.columns.tolist()
    for col in ['time', 'metric_type']:
        if col in cols:
            cols.remove(col)
    df = df[['time', 'metric_type'] + cols]
    # Convert 'time' column to datetime if it's not already
    df['time'] = pd.to_datetime(df['time'])

    # Sort by 'time'
    df = df.sort_values('time').reset_index(drop=True)

    # Combine consecutive Cell Scheduler Metrics & Scheduler UE Metrics, drop metric_type column
    df_merged = combine_cell_and_ue_metrics(df)

    return df_merged

def read_gnbLog_ULmeasurementReport(filepath):
    reports = []
    with open(filepath, 'r') as f:
        lines = f.readlines()

    i = 0
    while i < len(lines):
        line = lines[i]
        if "Containerized measurementReport: [" in line:
            # Extract timestamp, c-rnti, and ue id
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
    # print(df)
    if df.empty == False : df['time'] = pd.to_datetime(df['time'])
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

def process_all_log_pairs(root_dir, output_dir, overwrite=False):
    for dirpath, dirnames, filenames in os.walk(root_dir):
        gnb_logs = [f for f in filenames if f.endswith('_gnb.log')]
        for gnb_log in gnb_logs:
            base = gnb_log.replace('_gnb.log', '')
            iperf_log = f"{base}_iperf3.log"
            gnb_log_path = os.path.join(dirpath, gnb_log)
            iperf_log_path = os.path.join(dirpath, iperf_log)
            out_csv = os.path.join(output_dir, f"{base}_merged.csv")
            if os.path.exists(iperf_log_path):
                if os.path.exists(out_csv) and not overwrite:
                    print(f"Skipping (already exists): {out_csv}")
                    continue
                print(f"Processing: {gnb_log_path} + {iperf_log_path}")
                try:
                    process_one_pair(gnb_log_path, iperf_log_path, out_csv)
                except Exception as e:
                    print(f"Exception processing {gnb_log_path} + {iperf_log_path}: {e}")
            else:
                print(f"Missing iperf3 log for {gnb_log_path}")

def process_one_pair(gnbLogFileName, iperfLogFileName, out_csv):
    gnbLog_metrics = read_gnbLog_METRICS(gnbLogFileName)
    gnbLog_ULMeas = read_gnbLog_ULmeasurementReport(gnbLogFileName)
    iperfLog = read_iperf_log(iperfLogFileName)

    # Only merge with ULMeas if it's not empty
    if gnbLog_ULMeas is not None and not gnbLog_ULMeas.empty:
        merged1 = pd.merge_asof(
            gnbLog_metrics.sort_values('time'),
            gnbLog_ULMeas.sort_values('time'),
            on='time',
            direction='nearest',
            tolerance=pd.Timedelta('500ms')
        )
    else:
        merged1 = gnbLog_metrics

    # Only merge with iperfLog if it's not empty
    if iperfLog is not None and not iperfLog.empty:
        results_df = pd.merge_asof(
            merged1.sort_values('time'),
            iperfLog.sort_values('time'),
            on='time',
            direction='nearest',
            tolerance=pd.Timedelta('500ms')
        )
    else:
        results_df = merged1

    results_df.to_csv(out_csv, index=False)
    gnb_config_str = extract_gnb_config_block(gnbLogFileName)
    prepend_config_to_csv(gnb_config_str, out_csv)
    prepend_radar_to_csv(gnbLogFileName, out_csv)
    prepend_iperf_to_csv(gnbLogFileName, out_csv)

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

if __name__ == "__main__":
    root_dir = "/home/eric/scripts"  # or wherever your logs are
    output_dir = "/home/eric/scripts/merged_results"
    os.makedirs(output_dir, exist_ok=True)
    # Set overwrite=True to force re-processing
    process_all_log_pairs(root_dir, output_dir, overwrite=False)
    # testDir = '/home/eric/scripts/logs_20250524_074549/1410_'
    # gnbLogFileName=f'{testDir}_gnb.log'
    # iperfLogFileName='{testDir}_iperf3.log'
    # process_one_pair(f'{testDir}_gnb.log', f'{testDir}_iperf3.log', "/home/eric/scripts/test_results_merged.csv")