#!/usr/bin/python3
import subprocess
import re
from time import sleep
import os
import shutil
from datetime import datetime, timedelta
import yaml
import numpy as np
from android_controller import AndroidController
import time
import argparse
import random

#Files
gnbConfigDFLT ="/home/eric/srsRAN_Project/configs/00101__gnb_rf_b200_tdd_n78_20mhz.yml"
gnbConfigRadar ="/home/eric/srsRAN_Project/configs/radar_00101__gnb_rf_b200_tdd_n78_20mhz.yml"


#Bash Commands
# export GNB_CONFIG_PATH_DEFAULT="/home/eric/srsRAN_Project/configs/00101__gnb_rf_b200_tdd_n78_20mhz.yml"
# export GNB_CONFIG_PATH_RADAR="/home/eric/srsRAN_Project/configs/radar_00101__gnb_rf_b200_tdd_n78_20mhz.yml"
gnbStart = 'sudo systemctl start gnb.service'
gnbStat = 'sudo systemctl status gnb.service'
gnbStop = 'sudo systemctl stop gnb.service'
iperfStartUL = 'iperf3 -p 5202 -c 10.45.0.2 -b 12M -t 0 -u -R --logfile /tmp/iperf3_UL.log &'
iperfStartDL = 'iperf3 -p 5201 -c 10.45.0.2 -b 68M -u -t 0 --logfile /tmp/iperf3_DL.log &'
iperfStop = 'pkill iperf3'

#set Default Radar Params
radarData = {
        "prf": 100,  # Initial PRF value
        "gain": 70,
        "cFreq": 3417.1e6,
        "PW": 100e-6,
        "T": 20,
        "bw": 2e6,
        "sampRate": 20e6
    }


def check_ping(host, count=1, timeout=1):
    try:
        output = subprocess.run(
            ["ping", "-c", str(count), "-W", str(timeout), host],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        return output.returncode == 0
    except Exception:
        return False

def wait_for_ping(host, wait_time=15):
    print(f"Waiting for {host} to respond to ping... within {wait_time} seconds")
    for _ in range(wait_time):
        if check_ping(host):
            return True
        sleep(1)
    return False

def bashCMDbckGrnd(command):
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return process

def bashCMD(CMD):
    process = subprocess.Popen(CMD.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    print(output)

def radarStart(cmd):
    try:
        print(f"Running radar command: {cmd}")
        process = subprocess.Popen(
            cmd, shell=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, bufsize=1
        )
        output_lines = []
        # Stream stdout live while capturing for validation parsing
        for line in process.stdout:
            line = line.rstrip('\n')
            print(f"  [RADAR] {line}")
            output_lines.append(line)
        # Wait for process to finish and capture stderr
        process.wait()
        error = process.stderr.read()
        if error:
            for err_line in error.strip().split('\n'):
                print(f"  [RADAR-ERR] {err_line}")
            if "usb tx2 transfer status: LIBUSB_TRANSFER_NO_DEVICE" in error:
                print("USB device disconnected error detected!")
                raise RuntimeError("USB device disconnected")
        if process.returncode != 0:
            raise RuntimeError(f"Command failed (exit code {process.returncode}): {cmd}\nError: {error}")
        return '\n'.join(output_lines)
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"Command failed: {cmd}\nError: {e}")
def adbCMD(CMD):
    process = subprocess.Popen(CMD.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    print(output)

def ProcessGnbLogs(radarData, iperfStart_T, folder, LogNametimestamp,
                   radar_start_T=None, radar_end_T=None, gnb_start_T=None):
    """
    Collect logs, append radar info, and process gnb.log to generate CSVs.
    Creates a single timestamped subfolder for each iteration's logs.

    Args:
        radarData: dict of radar parameters
        iperfStart_T: timestamp when iperf started
        folder: base run directory (e.g., .../run1)
        LogNametimestamp: pre-generated timestamp string (YYYYMMDD_HHMMSS)
        radar_start_T: timestamp when radar TX started (optional)
        radar_end_T: timestamp when radar TX finished (optional)
        gnb_start_T: datetime string when gnb was started, used to slice journalctl (optional)
    """
    gnb_src = "/tmp/gnb.log"
    
    # Create a timestamped subdirectory for this iteration
    iteration_folder = os.path.join(folder, LogNametimestamp)
    os.makedirs(iteration_folder, exist_ok=True)
    
    # Fix permissions so non-root processes can write
    try:
        subprocess.run(['chmod', '777', iteration_folder], check=True)
    except Exception as e:
        print(f"Warning: Could not set permissions on {iteration_folder}: {e}")
    
    # Create destination paths in the timestamped folder
    iperf_ul_src = "/tmp/iperf3_UL.log"
    iperf_dl_src = "/tmp/iperf3_DL.log"
    radar_tx_src = "/tmp/radar_tx.log"
    terminal_log_src = "/tmp/terminal.log"
    iperf_ul_dst = os.path.join(iteration_folder, f"{LogNametimestamp}_iperf3_UL.log")
    iperf_dl_dst = os.path.join(iteration_folder, f"{LogNametimestamp}_iperf3_DL.log")
    radar_tx_dst = os.path.join(iteration_folder, f"{LogNametimestamp}_radar_tx.log")
    terminal_log_dst = os.path.join(iteration_folder, f"{LogNametimestamp}_terminal.log")
    gnb_dst = os.path.join(iteration_folder, f"{LogNametimestamp}_gnb.log")
    
    # gnb writes its logs directly to /tmp/gnb.log (configured in the gnb yaml)
    # No need to extract from journalctl - just use the existing file
    print("Using gNB logs from /tmp/gnb.log (written directly by gnb)")
    
    # Copy logs to destination folder, then clean up source files
    if os.path.exists(iperf_ul_src):
        try:
            shutil.copy(iperf_ul_src, iperf_ul_dst)
            print(f"Saved {iperf_ul_src} as {iperf_ul_dst}")
            os.remove(iperf_ul_src)
        except Exception as e:
            print(f"Could not copy {iperf_ul_src}: {e}")
    else:
        print(f"Note: {iperf_ul_src} not found (may not be flushed yet)")

    if os.path.exists(iperf_dl_src):
        try:
            shutil.copy(iperf_dl_src, iperf_dl_dst)
            print(f"Saved {iperf_dl_src} as {iperf_dl_dst}")
            os.remove(iperf_dl_src)
        except Exception as e:
            print(f"Could not copy {iperf_dl_src}: {e}")
    else:
        print(f"Note: {iperf_dl_src} not found (may not be flushed yet)")

    if os.path.exists(radar_tx_src):
        try:
            shutil.copy(radar_tx_src, radar_tx_dst)
            print(f"Saved {radar_tx_src} as {radar_tx_dst}")
            os.remove(radar_tx_src)
        except Exception as e:
            print(f"Could not copy {radar_tx_src}: {e}")
    else:
        print(f"Note: {radar_tx_src} not found (radar may not have run)")

    if os.path.exists(terminal_log_src):
        try:
            shutil.copy(terminal_log_src, terminal_log_dst)
            print(f"Saved {terminal_log_src} as {terminal_log_dst}")
        except Exception as e:
            print(f"Could not copy {terminal_log_src}: {e}")

    # Save journalctl gnb.service log for this run
    journal_dst = os.path.join(iteration_folder, f"{LogNametimestamp}_gnb_journal.log")
    try:
        jctl_cmd = ["sudo", "journalctl", "-u", "gnb.service", "--no-pager", "--output=short-iso"]
        if gnb_start_T:
            # journalctl --since expects "YYYY-MM-DD HH:MM:SS"
            since_str = gnb_start_T.replace("T", " ").split(".")[0]
            jctl_cmd += ["--since", since_str]
        result = subprocess.run(jctl_cmd, capture_output=True, text=True, timeout=15)
        with open(journal_dst, "w") as f:
            f.write(result.stdout)
        print(f"Saved journalctl gnb.service as {journal_dst}")
    except Exception as e:
        print(f"Could not save journalctl log: {e}")

    try:
        shutil.copy(gnb_src, gnb_dst)
        print(f"Saved {gnb_src} as {gnb_dst}")
    except Exception as e:
        print(f"Could not copy {gnb_src}: {e}")
        return

    # Append radarData as CSV line to the COPIED gnb log
    try:
        with open(gnb_dst, "a") as f:
            csv_line = "Radar_Char," + ",".join(f"{k}={radarData[k]}" for k in radarData.keys()) + "\n"
            f.write(csv_line)
            f.write("DL: " + iperfStartDL + "\n")
            if radar_start_T:
                f.write(f"RadarTxStart,{radar_start_T}\n")
            if radar_end_T:
                f.write(f"RadarTxEnd,{radar_end_T}\n")
        print(f"Appended radar data to {gnb_dst}")
    except Exception as e:
        print(f"Could not append radarData to {gnb_dst}: {e}")
        
    try:
        if os.path.exists(iperf_dl_dst):
            with open(iperf_dl_dst, "a") as f:
                csv_line = f"RadarStartTime,{iperfStart_T}\n"
                f.write(csv_line)
            print(f"Appended radar start time to {iperf_dl_dst}")
    except Exception as e:
        print(f"Could not append radarData to {iperf_dl_dst}: {e}")
    
    # Process the COPIED log files (now in the timestamped folder)
    # Note: LogProcessing.py expects [METRICS ] tags which may not be present in journalctl output
    # It will process iperf logs and any metrics if they exist
    try:
        prefix = LogNametimestamp
        log_proc_cmd = [
            "python3",
            "/home/eric/OTA-Radar-5G-Trials/LogProcessing.py",
            "--gnb-log", gnb_dst,       # Use copied file
            "--iperf-log", iperf_dl_dst if os.path.exists(iperf_dl_dst) else gnb_dst,
            "--out-dir", iteration_folder,  # Output to timestamped folder
            "--prefix", prefix
        ]
        print(f"Processing logs with: {' '.join(log_proc_cmd)}")
        result = subprocess.run(log_proc_cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print("Log processing completed successfully")
            if result.stdout:
                print(f"LogProcessing output: {result.stdout}")
        else:
            print(f"LogProcessing.py completed with warnings/errors (return code {result.returncode})")
            if result.stderr:
                # Only print first few lines of error to avoid clutter
                error_lines = result.stderr.strip().split('\n')
                if 'KeyError' in result.stderr and 'log_type' in result.stderr:
                    print("Note: No [METRICS ] data found in gnb logs. This is normal if metrics aren't being logged.")
                else:
                    print(f"Error output: {result.stderr}")
    except Exception as e:
        print(f"Could not process logs with LogProcessing.py: {e}")

def readGnbConfig(config_path):
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config

def update_yaml_parameter(yaml_path, param_path, value):
    """
    Update a parameter in a YAML file by modifying the line directly.
    Uses the dot-separated param_path to locate the correct section first,
    so duplicate parameter names in different sections (e.g. pusch vs pdsch)
    are handled correctly.

    Example:
        update_yaml_parameter(gnbConfigRadar, "cell_cfg.pdsch.min_ue_mcs", 15)
    """
    parts = param_path.split('.')
    param_name = parts[-1]
    parent_section = parts[-2] if len(parts) >= 2 else None

    with open(yaml_path, 'r') as f:
        lines = f.readlines()

    in_section = (parent_section is None)
    parent_indent = None
    modified = False

    for i, line in enumerate(lines):
        stripped = line.strip()
        if not stripped or stripped.startswith('#'):
            continue
        indent = len(line) - len(line.lstrip())

        if not in_section:
            # Wait until we find the parent section header
            if stripped.startswith(f'{parent_section}:'):
                in_section = True
                parent_indent = indent
            continue

        # If we've returned to the same (or shallower) indent as the section
        # header, we've left the section without finding the param
        if parent_indent is not None and indent <= parent_indent:
            break

        if stripped.startswith(f'{param_name}:'):
            if '#' in stripped:
                comment_idx = line.find('#')
                comment = line[comment_idx:]
                lines[i] = f"{' ' * indent}{param_name}: {value}  {comment}"
            else:
                lines[i] = f"{' ' * indent}{param_name}: {value}\n"
            modified = True
            print(f"Updated {param_path} = {value}")
            break

    if not modified:
        print(f"Warning: {param_path} not found in {yaml_path}")
        return False

    with open(yaml_path, 'w') as f:
        f.writelines(lines)
    return True

def uncomment_yaml_line(yaml_path, parameter_name):
    """
    Uncomment a line in YAML file that starts with # followed by the parameter name.
    This is useful for parameters that are commented out by default.
    
    Args:
        yaml_path: Path to the YAML file
        parameter_name: Name of the parameter to uncomment (e.g., "min_ue_mcs")
    """
    with open(yaml_path, 'r') as f:
        lines = f.readlines()
    
    modified = False
    for i, line in enumerate(lines):
        stripped = line.strip()
        # Check if line is commented and contains the parameter
        if stripped.startswith('#') and parameter_name in stripped:
            # Check if it's the parameter definition (has ':' after parameter name)
            if f'{parameter_name}:' in stripped:
                # Remove the '#' and any spaces after it
                lines[i] = line.replace('#', '', 1).lstrip()
                modified = True
                print(f"Uncommented line: {lines[i].strip()}")
                break
    
    if modified:
        with open(yaml_path, 'w') as f:
            f.writelines(lines)
        print(f"Uncommented {parameter_name} in {yaml_path}")
    else:
        print(f"Parameter {parameter_name} not found or already uncommented in {yaml_path}")

def _reset_usb_device(lsusb_out, label, pattern):
    """Find a device matching pattern in lsusb output and reset it via usbreset."""
    match = re.search(
        r'Bus (\d{3}) Device (\d{3}): ID ([0-9a-f]{4}:[0-9a-f]{4})' + pattern,
        lsusb_out, re.IGNORECASE
    )
    if not match:
        print(f"  {label}: not found in lsusb output — skipping")
        return False
    bus, device, usb_id = match.group(1), match.group(2), match.group(3)
    dev_path = f"/dev/bus/usb/{bus}/{device}"
    print(f"  {label}: Bus {bus} Device {device} ID {usb_id} ({dev_path}) — resetting...")
    try:
        subprocess.run(["sudo", "usbreset", dev_path], check=True)
        print(f"  {label}: reset OK")
        return True
    except Exception as e:
        print(f"  {label}: reset failed — {e}")
        return False


def reset_usb_devices():
    """Reset the B200-mini USRP and the Nexus/Pixel phone via usbreset."""
    print("Resetting USB devices...")
    try:
        lsusb_out = subprocess.check_output("lsusb", shell=True).decode()
    except Exception as e:
        print(f"  lsusb failed: {e}")
        return

    _reset_usb_device(lsusb_out, "USRP B200-mini",  r'.*B200-mini')
    _reset_usb_device(lsusb_out, "Nexus/Pixel phone", r'.*(?:Nexus|Pixel|18d1:4ee7)')
    sleep(2)  # give both devices time to re-enumerate


def reset_usrp_usb():
    """Legacy helper — resets the B200-mini only (called on radar USB disconnect)."""
    try:
        lsusb_out = subprocess.check_output("lsusb", shell=True).decode()
        _reset_usb_device(lsusb_out, "USRP B200-mini", r'.*B200-mini')
        sleep(2)
    except Exception as e:
        print(f"Failed to reset USRP USB device: {e}")


def validate_radar_output(output):
    """Parse radarTX.py output to validate transmission."""
    result = {
        "burst_ack": False,
        "underflow": False,
        "seq_error": False,
        "time_error": False,
        "send_errors": 0,
        "rate_error_pct": None,
        "throughput_ok": False,
        "success": False,
    }
    for line in output.splitlines():
        if "BURST_ACK received" in line:
            result["burst_ack"] = True
        elif "UNDERFLOW" in line:
            result["underflow"] = True
        elif "SEQ_ERROR" in line:
            result["seq_error"] = True
        elif "TIME_ERROR" in line:
            result["time_error"] = True
        elif "Send errors:" in line:
            try:
                result["send_errors"] = int(line.strip().split()[-1])
            except ValueError:
                pass
        elif "Rate error:" in line:
            try:
                result["rate_error_pct"] = float(line.strip().split()[-1].replace('%', ''))
            except ValueError:
                pass
        elif "Host throughput OK" in line:
            result["throughput_ok"] = True
        elif "TX RESULT: SUCCESS" in line:
            result["success"] = True
    return result

def wait_for_iperf_stable(direction="ul", ul_log="/tmp/iperf3_UL.log", dl_log="/tmp/iperf3_DL.log",
                          min_wait=10, timeout=45):
    """
    Wait until the gating iperf3 log(s) show active interval data, then enforce min_wait.
    direction: 'ul'   — gate on UL only  (DL shown for info)
               'dl'   — gate on DL only  (UL shown for info)
               'both' — gate on both UL and DL
    Returns True if confirmed active, False on timeout.
    """
    _rate_pat = re.compile(r'\d+\.\d+-\d+\.\d+\s+sec.*?(\d+\.?\d*)\s+(M|G|K)bits/sec')

    def get_latest_rate(path):
        if not os.path.exists(path):
            return None
        try:
            with open(path, 'r') as f:
                content = f.read()
        except Exception:
            return None
        for line in reversed(content.splitlines()):
            m = _rate_pat.search(line)
            if m:
                val = float(m.group(1))
                unit = m.group(2)
                if unit == 'G':
                    val *= 1000
                elif unit == 'K':
                    val /= 1000
                return val
        return None

    gate_label = {"ul": "UL", "dl": "DL", "both": "UL+DL"}[direction]
    print(f"Waiting for iperf3 {gate_label} to stabilize (min {min_wait}s, timeout {timeout}s)...")
    elapsed = 0
    ul_ok = dl_ok = False

    while elapsed < timeout:
        sleep(1)
        elapsed += 1

        ul_rate = get_latest_rate(ul_log)
        dl_rate = get_latest_rate(dl_log)
        ul_ok = ul_rate is not None
        dl_ok = dl_rate is not None

        ul_str = f"{ul_rate:.1f}M" if ul_ok else '--'
        dl_str = f"{dl_rate:.1f}M" if dl_ok else '--'
        status = f"  [{elapsed:>3}s] UL={ul_str}  DL={dl_str}"

        if direction == "ul":
            gate_ok = ul_ok
        elif direction == "dl":
            gate_ok = dl_ok
        else:  # both
            gate_ok = ul_ok and dl_ok

        if gate_ok and elapsed >= min_wait:
            print(status + f"  -> {gate_label} active, proceeding")
            return True
        else:
            print(status)

    print(f"WARNING: iperf3 stabilization timeout after {timeout}s  "
          f"(UL={'OK' if ul_ok else 'MISSING'}, DL={'OK' if dl_ok else 'MISSING'})")
    return False


# Minimum acceptable throughput thresholds
IPERF_DL_MIN_MBPS = 60.0   # DL target ~66 Mbps
IPERF_UL_MIN_MBPS = 10.0   # UL target ~11-12 Mbps
# gNB METRICS thresholds — low enough to work across all MCS values; just confirms the link is active
GNB_DL_MIN_MBPS = 1.0
GNB_UL_MIN_MBPS = 1.0


def check_iperf_throughput(log_path, n_intervals=3):
    """
    Read the last n_intervals interval lines from an iperf3 log and return
    the average throughput in Mbps.  Returns None if the log can't be parsed.
    """
    import re
    if not os.path.exists(log_path):
        return None
    try:
        with open(log_path, 'r') as f:
            lines = f.readlines()
    except Exception:
        return None

    # Match interval lines: contain a time range (e.g. "0.00-1.00") and a bitrate
    pattern = re.compile(r'\d+\.\d+-\d+\.\d+\s+sec.*?(\d+\.?\d*)\s+(M|G|K)bits/sec')
    rates = []
    for line in reversed(lines):
        m = pattern.search(line)
        if m:
            val = float(m.group(1))
            unit = m.group(2)
            if unit == 'G':
                val *= 1000
            elif unit == 'K':
                val /= 1000
            rates.append(val)
            if len(rates) == n_intervals:
                break

    if not rates:
        return None
    return sum(rates) / len(rates)


def parse_brate_to_mbps(s):
    """Convert a gNB brate string (e.g. '66.5Mbps', '2.00kbps', '1.2Gbps') to float Mbps."""
    m = re.match(r'([\d.]+)\s*(k|M|G)?bps', s, re.IGNORECASE)
    if not m:
        return None
    val = float(m.group(1))
    unit = (m.group(2) or '').lower()
    if unit == 'g':
        val *= 1000
    elif unit == 'k':
        val /= 1000
    return val


def read_gnb_metrics(gnb_log="/tmp/gnb.log", n_recent=3):
    """
    Extract the last n_recent Scheduler [METRICS ] lines from gnb.log and
    return (avg_dl_mbps, avg_ul_mbps).  Uses tail+grep because MAC/RRC debug
    logging is dense enough that a fixed-byte Python seek would miss them.
    Returns (None, None) if the log is unreadable or has no METRICS yet.
    """
    if not os.path.exists(gnb_log):
        return None, None
    try:
        result = subprocess.run(
            f'tail -c 4194304 {gnb_log} | grep -a "\\[METRICS \\].*total_dl_brate"',
            shell=True, capture_output=True, text=True, timeout=5
        )
        lines = result.stdout.strip().splitlines()
    except Exception:
        return None, None

    pat = re.compile(r'\[METRICS \].*?total_dl_brate=\s*(\S+).*?total_ul_brate=\s*(\S+)')
    dl_rates, ul_rates = [], []
    for line in reversed(lines):
        m = pat.search(line)
        if m:
            dl = parse_brate_to_mbps(m.group(1))
            ul = parse_brate_to_mbps(m.group(2))
            if dl is not None:
                dl_rates.append(dl)
            if ul is not None:
                ul_rates.append(ul)
            if len(dl_rates) >= n_recent:
                break

    avg_dl = sum(dl_rates) / len(dl_rates) if dl_rates else None
    avg_ul = sum(ul_rates) / len(ul_rates) if ul_rates else None
    return avg_dl, avg_ul


def start_iperf_with_gnb_verify(UE, max_restarts=6, settle_secs=10, timeout_per_attempt=45):
    """
    Start iperf3 client(s), then verify actual air-interface rates via gNB [METRICS] lines.
    Kills and restarts iperf3 up to max_restarts times if rates don't meet thresholds.

    Returns the iperf start timestamp (str) on success, or None if all attempts fail.
    """
    _rate_pat = re.compile(r'\d+\.\d+-\d+\.\d+\s+sec.*?(\d+\.?\d*)\s+(M|G|K)bits/sec')

    def get_iperf_rate(path):
        if not os.path.exists(path):
            return None
        try:
            with open(path, 'r') as f:
                content = f.read()
        except Exception:
            return None
        for line in reversed(content.splitlines()):
            m = _rate_pat.search(line)
            if m:
                val = float(m.group(1))
                unit = m.group(2)
                if unit == 'G':
                    val *= 1000
                elif unit == 'K':
                    val /= 1000
                return val
        return None

    for attempt in range(1, max_restarts + 1):
        print(f"\n--- iperf3 attempt {attempt}/{max_restarts} ---")

        # Kill any running iperf3 and remove stale logs
        subprocess.run("pkill iperf3", shell=True, capture_output=True)
        sleep(0.5)
        try:
            if os.path.exists("/tmp/iperf3_DL.log"):
                os.remove("/tmp/iperf3_DL.log")
        except Exception:
            pass

        # (Re)start UE iperf3 server then host DL client
        UE.restart_termux_iperf3()
        sleep(1)

        bashCMDbckGrnd(iperfStartDL)

        iperf_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
        start = time.time()
        rates_confirmed = False
        timed_out = True

        while time.time() - start < timeout_per_attempt:
            sleep(1)
            elapsed = int(time.time() - start)

            iDL = get_iperf_rate("/tmp/iperf3_DL.log")
            gDL, gUL = read_gnb_metrics()

            iDL_s = f"{iDL:.1f}M" if iDL is not None else '--'
            gDL_s = f"{gDL:.1f}M" if gDL is not None else '--'
            gUL_s = f"{gUL:.1f}M" if gUL is not None else '--'
            print(f"  [{elapsed:>3}s] iperf DL={iDL_s} | gNB DL={gDL_s} UL={gUL_s}")

            if elapsed < settle_secs:
                continue

            # After settle window, require DL > 1 Mbps
            dl_ok = gDL is not None and gDL >= GNB_DL_MIN_MBPS

            if dl_ok:
                print(f"  -> gNB rates confirmed (DL={gDL_s}, UL={gUL_s}). Proceeding.")
                rates_confirmed = True
                timed_out = False
                break
            else:
                print(f"  -> Insufficient DL rate ({gDL_s} < {GNB_DL_MIN_MBPS}Mbps), restarting iperf3...")
                timed_out = False
                break

        if timed_out:
            print(f"  -> Timeout ({timeout_per_attempt}s) waiting for gNB metrics on attempt {attempt}.")

        if rates_confirmed:
            return iperf_timestamp

    print(f"ERROR: Could not confirm iperf3 rates via gNB after {max_restarts} attempts.")
    return None


def restart_core():
    """
    Restart the AMF, SMF, and UPF so every gNB run starts with a completely
    clean core — no stale UE contexts, no leftover PDU sessions.
    The AMF holds in-memory UE registrations across gNB restarts; without this
    it floods the new gNB connection with phantom RNTIs from previous runs.
    """
    print("Restarting 5GC (AMF / SMF / UPF) to clear stale UE state...")
    for svc in ("open5gs-amfd", "open5gs-smfd", "open5gs-upfd"):
        result = subprocess.run(
            ["sudo", "systemctl", "restart", svc],
            capture_output=True, text=True
        )
        status = "OK" if result.returncode == 0 else f"FAILED ({result.stderr.strip()})"
        print(f"  {svc}: {status}")
    sleep(3)   # give services time to finish initialising before gNB connects


def runLoop1(UE, radarValues, gnbConfig, logDIR, direction="dl"):
    print(f"Running Loop 1  [iperf direction: {direction.upper()}]")
    # Stop any stale gNB first so systemctl start doesn't become a no-op.
    bashCMD(gnbStop)
    sleep(1)
    gnb_start_T = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
    try:
        print(f"Starting gNB...")
        bashCMD(gnbStart)
    except RuntimeError as e:
        print(f"Failed to start gNB: {e}")
        return False
    sleep(3)
    UE.disable_airplane_mode()
    sleep(1)
    if not wait_for_ping('10.45.0.2', wait_time=30):
        print("UE unable to attach to network in 15s. Exiting...")
        return False
    print("UE attached to network. Starting iperf3 and verifying rates via gNB...")

    def _cleanup_and_fail(reason):
        print(f"SCRAPPING RUN: {reason}")
        bashCMD(iperfStop)
        sleep(1)
        UE.enable_airplane_mode()
        sleep(2)
        bashCMD(gnbStop)
        return False

    iperf_timestamp = start_iperf_with_gnb_verify(UE)
    if iperf_timestamp is None:
        return _cleanup_and_fail("Could not confirm iperf3 rates via gNB after all restarts")

    print("  gNB rate gate passed — proceeding with radar.")
    # Random 0-1s jitter before radar pulse to decorrelate timing
    radar_jitter = random.uniform(0, 1)
    print(f"Radar start jitter: {radar_jitter:.3f}s")
    sleep(radar_jitter)
    radarExeString = f'''python3 radarTX.py \
        --center-freq {radarValues['cFreq']}  \
        --prf {radarValues['prf']} \
        --pulse-width {radarValues['PW']} \
        --gain {radarValues['gain']} \
        --total-duration {radarValues['T']} \
        --bw {radarValues['bw']} \
        --sample-rate {radarValues['sampRate']}'''
    radar_start_T = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
    radar_end_T = None
    try:
        radar_output = radarStart(radarExeString)
        radar_end_T = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
        # Validate radar TX using async metadata and buffer health
        radar_result = validate_radar_output(radar_output)
        print(f"\n=== Radar TX Validation ===")
        print(f"  BURST_ACK:     {'YES' if radar_result['burst_ack'] else 'NO'}")
        print(f"  Underflows:    {'YES — WARNING' if radar_result['underflow'] else 'None'}")
        print(f"  Seq errors:    {'YES — WARNING' if radar_result['seq_error'] else 'None'}")
        print(f"  Time errors:   {'YES — WARNING' if radar_result['time_error'] else 'None'}")
        print(f"  Send errors:   {radar_result['send_errors']}")
        if radar_result['rate_error_pct'] is not None:
            print(f"  Rate error:    {radar_result['rate_error_pct']:.4f}%")
        print(f"  Throughput OK: {'YES' if radar_result['throughput_ok'] else 'NO'}")
        print(f"  Overall:       {'SUCCESS' if radar_result['success'] else 'CHECK WARNINGS'}")
        if not radar_result['burst_ack']:
            print("  WARNING: No BURST_ACK — radar transmission may not have completed!")
        # Save radar log to temp for ProcessGnbLogs to pick up
        with open("/tmp/radar_tx.log", "w") as f:
            f.write(radar_output)
    except RuntimeError as e:
        print(f"Radar command failed: {e}")
        # reset_usrp_usb()  # usbreset is logical-only, not a power cycle — disabled until uhubctl support added
        return False
    sleep(5)
    print("radar stopped...")
    print("Stopping iperf3...")
    bashCMD(iperfStop)
    sleep(1)
    UE.enable_airplane_mode()
    sleep(2)
    print("Stopping gNB...")
    bashCMD(gnbStop)
    print("Collecting logs...")
    log_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    ProcessGnbLogs(radarValues, iperf_timestamp, logDIR, log_timestamp,
                   radar_start_T=radar_start_T, radar_end_T=radar_end_T,
                   gnb_start_T=gnb_start_T)
    sleep(1)
    return True

class _SessionLogger:
    """
    Tee a stream to two destinations:
      - session.log  : persistent for the whole experiment session, timestamped,
                       line-buffered so `tail -f` works in real time.
      - /tmp/terminal.log : reset at the start of each run, archived per run
                            by ProcessGnbLogs.
    Pass `session_file` as an already-open file object to share one session log
    between stdout and stderr.
    """
    def __init__(self, stream, session_file):
        self._stream   = stream
        self._session  = session_file          # shared across stdout + stderr
        self._run_log  = open('/tmp/terminal.log', 'a', buffering=1)
        self._buf      = ''

    def reset_run_log(self):
        """Call at the start of each run to start a fresh per-run capture."""
        self._run_log.close()
        self._run_log = open('/tmp/terminal.log', 'w', buffering=1)

    def write(self, data):
        self._stream.write(data)
        self._buf += data
        while '\n' in self._buf:
            line, self._buf = self._buf.split('\n', 1)
            ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self._session.write(f'[{ts}] {line}\n')
            self._run_log.write(f'{line}\n')

    def flush(self):
        self._stream.flush()
        self._session.flush()
        self._run_log.flush()

    def __getattr__(self, attr):
        return getattr(self._stream, attr)


def set_log_dir(base_experiment_dir, run_number):
    """
    Create a subdirectory for each run within the base experiment directory.
    
    Args:
        base_experiment_dir: The main directory for this experiment (e.g., /home/eric/OTA-Experiment-Runs/logs_20251205_123456)
        run_number: The current run number (e.g., 1, 2, 3...)
    
    Returns:
        Path to the run-specific directory (e.g., /home/eric/OTA-Experiment-Runs/logs_20251205_123456/run1)
    """
    run_dir = os.path.join(base_experiment_dir, f"run{run_number}")
    os.makedirs(run_dir, exist_ok=True)
    
    # Fix permissions so non-root processes can write
    try:
        subprocess.run(['chmod', '777', run_dir], check=True)
    except Exception as e:
        print(f"Warning: Could not set permissions on {run_dir}: {e}")
    
    return run_dir

def main():
    """
    Main experiment control function.
    
    EXAMPLE: MCS Sweep (no radar parameter changes)
    To sweep min_ue_mcs from 0 to 28 without changing radar characteristics:
    
    1. Uncomment the MCS sweep section below
    2. Comment out the PW sweep section
    3. Run: python3 OTAexpCTL.py -f mcs_sweep_experiment
    
    EXAMPLE: Radar Parameter Sweep
    To sweep radar parameters (PW, PRF, gain, etc.):
    
    1. Keep the PW sweep section uncommented
    2. Modify radarData parameters as needed
    3. Run: python3 OTAexpCTL.py -f radar_pw_sweep
    
    EXAMPLE: Combined MCS + Radar Sweep
    To sweep both MCS and radar parameters:
    
    1. Add a nested loop for MCS inside the radar sweep
    2. Update min_ue_mcs before each runLoop1 call
    """
    print("Main")
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='OTA Experiment Control Script')
    parser.add_argument('-f', '--folder', type=str, required=True,
                        help='Folder name (relative to ~/OTA-Experiment-Runs) or absolute path')
    parser.add_argument('-n', '--n-repeats', type=int, default=100,
                        help='Number of repeats (default: 100)')
    parser.add_argument('--harq-analysis', action='store_true', default=False,
                        help='Run batch HARQ analysis on saved gNB logs after all runs complete')
    parser.add_argument('--direction', choices=['ul', 'dl', 'both'], default='dl',
                        help='iperf3 traffic direction: dl (default), ul, or both')
    args = parser.parse_args()

    # Setup experiment directory — absolute path used directly, relative joined with base
    if os.path.isabs(args.folder):
        experiment_dir = args.folder
    else:
        base_dir = "/home/eric/OTA-Experiment-Runs"
        os.makedirs(base_dir, exist_ok=True)
        experiment_dir = os.path.join(base_dir, args.folder)

    # Each script launch gets its own session subfolder so restarts never
    # collide with a previous session's run dirs.
    session_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    experiment_dir = os.path.join(experiment_dir, session_ts)
    os.makedirs(experiment_dir, exist_ok=True)

    # Session logger: persistent timestamped log shared by stdout + stderr.
    # Stdout also resets /tmp/terminal.log per run for per-run archiving.
    import sys
    session_log_path = os.path.join(experiment_dir, 'session.log')
    _session_file    = open(session_log_path, 'a', buffering=1)
    _stdout_logger   = _SessionLogger(sys.__stdout__, _session_file)
    sys.stdout       = _stdout_logger
    sys.stderr       = _SessionLogger(sys.__stderr__, _session_file)

    print(f"Experiment directory: {experiment_dir}")
    print(f"Session log:          {session_log_path}")
    print(f"Live monitor:         tail -f {session_log_path}")

    # reset_usb_devices()  # usbreset is logical-only, not a power cycle — disabled until uhubctl support added

    cfg = readGnbConfig(gnbConfigRadar)
    UE = AndroidController()
    stop_requested = False

    # ==================== PDSCH MCS SWEEP (DL) ====================
    # Sweep PDSCH MCS 27 → 0, radar params fixed, 100 repeats per MCS level
    mcs_values    = list(range(27, -1, -1))   # [27, 26, ..., 0]
    n_repeats     = args.n_repeats            # default 100
    total_runs    = len(mcs_values) * n_repeats
    iteration_count = 0
    run_durations = []

    print(f"\nPDSCH MCS Sweep Configuration (DL):")
    print(f"  MCS sweep:  {mcs_values[0]} → {mcs_values[-1]}")
    print(f"  Repeats per MCS: {n_repeats}")
    print(f"  Total runs: {total_runs}  ({len(mcs_values)} MCS × {n_repeats} repeats)")
    print(f"  PRF:   {radarData['prf']} Hz  |  PW: {radarData['PW']*1e6:.0f} µs  |  Gain: {radarData['gain']} dB")

    try:
        for mcs in mcs_values:
            # Only pin PDSCH — this is a DL-only test
            update_yaml_parameter(gnbConfigRadar, "cell_cfg.pdsch.min_ue_mcs", mcs)
            update_yaml_parameter(gnbConfigRadar, "cell_cfg.pdsch.max_ue_mcs", mcs)
            print(f"\n{'#'*70}")
            print(f"PDSCH MCS = {mcs}  ({n_repeats} repeats)")
            print(f"{'#'*70}")

            for repeat in range(n_repeats):
                iteration_count += 1
                run_log_dir = set_log_dir(experiment_dir, iteration_count)
                print(f"\n{'='*70}")
                print(f"Run {iteration_count}/{total_runs}  |  MCS {mcs}  |  Repeat {repeat + 1}/{n_repeats}")
                print(f"Saving to: {run_log_dir}")
                print(f"{'='*70}\n")

                if run_durations:
                    avg_duration  = sum(run_durations) / len(run_durations)
                    est_remaining = avg_duration * (total_runs - iteration_count + 1)
                    est_end_time  = datetime.now() + timedelta(seconds=est_remaining)
                    hours   = int(est_remaining // 3600)
                    minutes = int((est_remaining % 3600) // 60)
                    seconds = int(est_remaining % 60)
                    if hours > 0:
                        print(f"Est. remaining: {hours}h {minutes}m {seconds}s  (done ~{est_end_time.strftime('%Y-%m-%d %H:%M')})")
                    else:
                        print(f"Est. remaining: {minutes}m {seconds}s  (done ~{est_end_time.strftime('%H:%M')})")
                else:
                    print("Estimating time after first run...")

                # Reset per-run log so only this run's output is archived
                _stdout_logger.reset_run_log()

                start_time = time.time()
                runLoop1(UE, radarData, cfg, run_log_dir, direction="dl")
                run_durations.append(time.time() - start_time)

                # Run HARQ analysis on this run's folder immediately after
                try:
                    result = subprocess.run(
                        ["python3", "/home/eric/OTA-Radar-5G-Trials/analyze_harq.py",
                         "--batch", run_log_dir],
                        capture_output=True, text=True
                    )
                    if result.returncode == 0:
                        print(f"HARQ analysis complete for run {iteration_count}")
                    else:
                        print(f"HARQ analysis warning (code {result.returncode}): {result.stderr[:200]}")
                except Exception as e:
                    print(f"HARQ analysis failed: {e}")

                # Sync this run's folder to NAS:/OTA/<experiment_name>/
                nas_dest = f"nas:~/OTA/{args.folder}/{os.path.relpath(run_log_dir, os.path.dirname(experiment_dir))}/"
                try:
                    rsync_result = subprocess.run(
                        ["rsync", "-a", "--mkpath", f"{run_log_dir}/", nas_dest],
                        capture_output=True, text=True
                    )
                    if rsync_result.returncode == 0:
                        print(f"Synced run {iteration_count} → {nas_dest}")
                    else:
                        print(f"NAS sync warning: {rsync_result.stderr[:200]}")
                except Exception as e:
                    print(f"NAS sync failed: {e}")

                if stop_requested:
                    print("Keyboard interrupt received. Exiting after current run.")
                    return
    except KeyboardInterrupt:
        print("\nKeyboard interrupt detected. Will exit after the current runLoop1 finishes.")
        stop_requested = True

    # Run batch HARQ analysis on all saved logs
    if args.harq_analysis:
        print(f"\n{'='*70}")
        print(f"Running batch HARQ analysis on {experiment_dir}...")
        print(f"{'='*70}")
        try:
            harq_cmd = [
                "python3",
                "/home/eric/OTA-Radar-5G-Trials/analyze_harq.py",
                "--batch", experiment_dir
            ]
            result = subprocess.run(harq_cmd, text=True)
            if result.returncode != 0:
                print(f"HARQ analysis exited with code {result.returncode}")
        except Exception as e:
            print(f"Failed to run HARQ analysis: {e}")

if __name__ == "__main__":
    main()