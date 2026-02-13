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

#Files
gnbConfigDFLT ="/home/eric/srsRAN_Project/configs/00101__gnb_rf_b200_tdd_n78_20mhz.yml"
gnbConfigRadar ="/home/eric/srsRAN_Project/configs/radar_00101__gnb_rf_b200_tdd_n78_20mhz.yml"


#ADB Commands
apEnable = 'adb shell cmd connectivity airplane-mode enable' #enable AP Mode. Make UE go IDLE
apDisable = 'adb shell cmd connectivity airplane-mode disable' #disable AP Mode. Make UE Connect!

#Bash Commands 
# export GNB_CONFIG_PATH_DEFAULT="/home/eric/srsRAN_Project/configs/00101__gnb_rf_b200_tdd_n78_20mhz.yml"
# export GNB_CONFIG_PATH_RADAR="/home/eric/srsRAN_Project/configs/radar_00101__gnb_rf_b200_tdd_n78_20mhz.yml"
gnbStart = 'sudo systemctl start gnb.service'
gnbStat = 'sudo systemctl status gnb.service'
gnbStop = 'sudo systemctl stop gnb.service'
iperfStart = 'iperf3 -p 5201 -c 10.45.0.2 -b 70M -t 0 --logfile /tmp/iperf3.log &'
iperfStop = 'pkill -f iperf3'

#set Default Radar Params
radarData = {
        "prf": 100,  # Initial PRF value
        "gain": 80,
        "cFreq": 3417.1e6,
        "PW": 20e-6,
        "T": 20,
        "bw": 2e6,
        "sampRate": 20e6
    }

# Add this flag near the top of your script
collectingGNBLogs = True  # Set to True to enable log collection and processing

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
        result = subprocess.run(cmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output = result.stdout.decode('utf-8')
        error = result.stderr.decode('utf-8')
        if error:
            # Check for specific error
            if "usb tx2 transfer status: LIBUSB_TRANSFER_NO_DEVICE" in error:
                print("USB device disconnected error detected!")
                # Handle accordingly, e.g., raise a custom exception or return a special value
                raise RuntimeError("USB device disconnected")
        return output
    except subprocess.CalledProcessError as e:
        error = e.stderr.decode('utf-8') if e.stderr else str(e)
        if "usb tx2 transfer status: LIBUSB_TRANSFER_NO_DEVICE" in error:
            print("USB device disconnected error detected!")
            raise RuntimeError("USB device disconnected")
        else:
            raise RuntimeError(f"Command failed: {e.cmd}\nError: {error}")
def adbCMD(CMD):
    process = subprocess.Popen(CMD.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    print(output)

def collectLogs(radarData, iperfStart_T, folder):
    """
    Collect raw logs and save them to a timestamped subfolder.
    """
    LogNametimestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Create a timestamped subdirectory for this iteration
    iteration_folder = os.path.join(folder, LogNametimestamp)
    os.makedirs(iteration_folder, exist_ok=True)
    
    # Fix permissions so non-root processes can write
    try:
        subprocess.run(['chmod', '777', iteration_folder], check=True)
    except Exception as e:
        print(f"Warning: Could not set permissions on {iteration_folder}: {e}")
    
    iperf_dst = os.path.join(iteration_folder, f"{LogNametimestamp}_iperf3.log")
    gnb_dst = os.path.join(iteration_folder, f"{LogNametimestamp}_gnb.log")
    
    try:
        shutil.copy("/tmp/iperf3.log", iperf_dst)
        print(f"Saved /tmp/iperf3.log as {iperf_dst}")
        os.remove("/tmp/iperf3.log")
    except Exception as e:
        print(f"Could not copy /tmp/iperf3.log: {e}")
    try:
        shutil.copy("/tmp/gnb.log", gnb_dst)
        print(f"Saved /tmp/gnb.log as {gnb_dst}")
    except Exception as e:
        print(f"Could not copy /tmp/gnb.log: {e}")

    # Append radarData as CSV line to gnb_dst
    try:
        with open(gnb_dst, "a") as f:
            csv_line = "Radar_Char," + ",".join(f"{k}={radarData[k]}" for k in radarData.keys()) + "\n"
            f.write(csv_line)
            f.write(iperfStart + "\n") #write the iperf command used
    except Exception as e:
        print(f"Could not append radarData to {gnb_dst}: {e}")
    try: 
        with open(iperf_dst, "a") as f:
            csv_line = f"RadarStartTime,{iperfStart_T}\n"
            f.write(csv_line)
    except Exception as e:
        print(f"Could not append radarData to {iperf_dst}: {e}")

def ProcessGnbLogs(radarData, iperfStart_T, folder):
    """
    appends radar info, and processes gnb.log to generate CSVs.
    Creates a timestamped subfolder for each iteration's logs.
    """
    LogNametimestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    iperf_src = "/tmp/iperf3.log"
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
    iperf_dst = os.path.join(iteration_folder, f"{LogNametimestamp}_iperf3.log")
    gnb_dst = os.path.join(iteration_folder, f"{LogNametimestamp}_gnb.log")
    
    # gnb writes its logs directly to /tmp/gnb.log (configured in the gnb yaml)
    # No need to extract from journalctl - just use the existing file
    print("Using gNB logs from /tmp/gnb.log (written directly by gnb)")
    
    # Copy logs to destination folder FIRST
    try:
        shutil.copy(iperf_src, iperf_dst)
        print(f"Saved {iperf_src} as {iperf_dst}")
    except Exception as e:
        print(f"Could not copy {iperf_src}: {e}")
        
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
            f.write(iperfStart + "\n")
        print(f"Appended radar data to {gnb_dst}")
    except Exception as e:
        print(f"Could not append radarData to {gnb_dst}: {e}")
        
    try: 
        with open(iperf_dst, "a") as f:
            csv_line = f"RadarStartTime,{iperfStart_T}\n"
            f.write(csv_line)
        print(f"Appended radar start time to {iperf_dst}")
    except Exception as e:
        print(f"Could not append radarData to {iperf_dst}: {e}")
    
    # Process the COPIED log files (now in the timestamped folder)
    # Note: LogProcessing.py expects [METRICS ] tags which may not be present in journalctl output
    # It will process iperf logs and any metrics if they exist
    try:
        prefix = LogNametimestamp
        log_proc_cmd = [
            "python3",
            "/home/eric/OTA-Radar-5G-Trials/LogProcessing.py",
            "--gnb-log", gnb_dst,       # Use copied file
            "--iperf-log", iperf_dst,   # Use copied file
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
    This preserves comments and formatting.
    
    Args:
        yaml_path: Path to the YAML file
        param_path: Dot-separated path to the parameter (e.g., "cell_cfg.pdsch.min_ue_mcs")
        value: New value to set
    
    Example:
        update_yaml_parameter(gnbConfigRadar, "cell_cfg.pdsch.min_ue_mcs", 15)
    """
    # Get the final parameter name
    param_name = param_path.split('.')[-1]
    
    # Read the file
    with open(yaml_path, 'r') as f:
        lines = f.readlines()
    
    # Find and update the parameter line
    modified = False
    for i, line in enumerate(lines):
        stripped = line.strip()
        # Check if this line defines the parameter (not commented, has the parameter name followed by :)
        if not stripped.startswith('#') and stripped.startswith(f'{param_name}:'):
            # Get the indentation
            indent = len(line) - len(line.lstrip())
            # Find if there's a comment on the line
            if '#' in stripped:
                # Preserve the comment
                comment_idx = line.find('#')
                comment = line[comment_idx:]
                lines[i] = f"{' ' * indent}{param_name}: {value}  {comment}"
            else:
                lines[i] = f"{' ' * indent}{param_name}: {value}\n"
            modified = True
            print(f"Updated {param_name} = {value}")
            break
    
    if not modified:
        print(f"Warning: Parameter {param_name} not found in {yaml_path}")
        return False
    
    # Write back to file
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

def reset_usrp_usb():
    try:
        # Get lsusb output
        lsusb_out = subprocess.check_output("lsusb", shell=True).decode()
        # Find Ettus USRP device
        match = re.search(r'Bus (\d{3}) Device (\d{3}): ID ([0-9a-f]{4}:[0-9a-f]{4}) .*B200-mini', lsusb_out)
        if match:
            print(f"Found B200-mini: {match}")
            bus = match.group(1)
            device = match.group(2)
            usb_id = match.group(3)
            print(f"Found B200-mini: Bus {bus}, Device {device}, ID {usb_id}")
            usb_path = f"/dev/bus/usb/{bus}/{device}"
            print(f"Resetting USRP device at {usb_id}")
            subprocess.run(f"usbreset {usb_id}", shell=True, check=True)
        else:
            print("USRP device not found in lsusb output.")
    except Exception as e:
        print(f"Failed to reset USRP USB device: {e}")


def runLoop1(UE, radarValues, gnbConfig, logDIR):
    print("Running Loop 1")
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
    print("UE attached to network. Starting iperf3 on UE...")
    UE.restart_termux_iperf3()
    sleep(1)
    print("Starting iperf3 client...")
    try:
        bashCMDbckGrnd(iperfStart)
    except RuntimeError as e:
        print(f"Failed to start iperf3: {e}")
        return False
    iperf_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
    sleep(5)
    radarExeString = f'''./radarTX.py \
        --center-freq {radarValues['cFreq']}  \
        --prf {radarValues['prf']} \
        --pulse-width {radarValues['PW']} \
        --gain {radarValues['gain']} \
        --total-duration {radarValues['T']} \
        --bw {radarValues['bw']} \
        --sample-rate {radarValues['sampRate']}'''
    try:
        radarStart(radarExeString)
    except RuntimeError as e:
        print(f"Radar command failed: {e}")
        reset_usrp_usb() #handle USB disconnect specifically here
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
    if collectingGNBLogs:
        collectLogs(radarValues, iperf_timestamp, logDIR)
    ProcessGnbLogs(radarValues, iperf_timestamp, logDIR)
    sleep(1)
    return True

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
                        help='Folder name for saving results')
    args = parser.parse_args()
    
    # Setup base experiment directory
    base_dir = "/home/eric/OTA-Experiment-Runs"
    os.makedirs(base_dir, exist_ok=True)
    
    # Use user-specified folder name
    experiment_dir = os.path.join(base_dir, args.folder)
    
    os.makedirs(experiment_dir, exist_ok=True)
    print(f"Experiment directory: {experiment_dir}")
    
    cfg = readGnbConfig(gnbConfigRadar)
    UE = AndroidController()
    stop_requested = False

    # ==================== MCS SWEEP CONFIGURATION ====================
    # MCS sweep from 27 to 0 (decrementally)
    # Both min_ue_mcs and max_ue_mcs will be set to the same value
    # Radar parameters remain FIXED
    mcs_values = [27]  # Only MCS 27
    # gain_values = list(range(20, 51, 2))  # Radar gain from 20 to 50 (step 2)
    # cFreq_values = [round(3417.1e6 + i * 0.2e6, 1) for i in range(int((3423 - 3417.1) / 0.2) + 1)]  # Center freq from 3417.1 to 3423 MHz (step 0.2 MHz)
    n_repeats = 1000  # Number of times to repeat each MCS value
    total_runs = len(mcs_values) * n_repeats
    iteration_count = 0
    run_durations = []
    
    print(f"\nMCS, Radar Gain, and Center Frequency Sweep Configuration:")
    print(f"  MCS values: {mcs_values}")
    # print(f"  Radar Gain values: {gain_values[0]} to {gain_values[-1]} dB (step: 2)")
    # print(f"  Center Frequency values: {cFreq_values[0]/1e6:.1f} to {cFreq_values[-1]/1e6:.1f} MHz (step: 0.2 MHz)")
    print(f"  Repeats per configuration: {n_repeats}")
    print(f"  Total runs: {total_runs}")
    print(f"  Radar settings (FIXED):")
    print(f"    PRF: {radarData['prf']} Hz")
    print(f"    PW: {radarData['PW']*1e6:.2f} µs")
    print(f"    T: {radarData['T']} seconds")

    try:
        for repeat in range(n_repeats):
            # Create run-specific directory for this repeat
            run_log_dir = set_log_dir(experiment_dir, repeat + 1)
            print(f"\n{'='*70}")
            print(f"Starting Repeat {repeat + 1}/{n_repeats}")
            print(f"Saving to: {run_log_dir}")
            print(f"{'='*70}\n")
            mcs = mcs_values[0]  # Only one MCS value in this configuration
            gain = radarData['gain']  # Fixed gain
            cFreq = radarData['cFreq']  # Fixed center frequency
            # for gain in gain_values:
            #     radarData['gain'] = gain
            #     for cFreq in cFreq_values:
            #         radarData['cFreq'] = cFreq
            #         for mcs in mcs_values:
            #             iteration_count += 1
                
                # # Update both min_ue_mcs and max_ue_mcs to the same value in the gNB config file
                # # print(f"\nUpdating min_ue_mcs and max_ue_mcs to {mcs}...")
                # # update_yaml_parameter(gnbConfigRadar, "cell_cfg.pdsch.min_ue_mcs", mcs)
                # # update_yaml_parameter(gnbConfigRadar, "cell_cfg.pdsch.max_ue_mcs", mcs)

                    # Estimate time
            if run_durations:
                avg_duration = sum(run_durations) / len(run_durations)
                runs_left = total_runs - iteration_count + 1
                est_remaining = avg_duration * runs_left
                est_end_time = datetime.now() + timedelta(seconds=est_remaining)
                hours = int(est_remaining // 3600)
                minutes = int((est_remaining % 3600) // 60)
                seconds = int(est_remaining % 60)
                print(f"\nIteration {iteration_count}/{total_runs} | Repeat {repeat + 1}/{n_repeats} | MCS: {mcs} | Gain: {gain} dB | CFreq: {cFreq/1e6:.1f} MHz")
                if hours > 0:
                    print(f"Estimated time left: {hours} hr {minutes} min {seconds} sec")
                else:
                    print(f"Estimated time left: {minutes} min {seconds} sec")
                print(f"Estimated end time: {est_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            else:
                print(f"\nIteration {iteration_count}/{total_runs} | Repeat {repeat + 1}/{n_repeats} | MCS: {mcs} | Gain: {gain} dB | CFreq: {cFreq/1e6:.1f} MHz")
                print("Estimating time after first run...")

            # Print current settings
            print(f"Current config: MCS={mcs}, PRF={radarData['prf']}, Gain={radarData['gain']}, CFreq={radarData['cFreq']/1e6:.1f} MHz, PW={radarData['PW']*1e6:.2f}µs")

            # Start the run and time it
            start_time = time.time()
            runLoop1(UE, radarData, cfg, run_log_dir)
            duration = time.time() - start_time
            run_durations.append(duration)

            if stop_requested:
                print("Keyboard interrupt received. Exiting after current runLoop1.")
                return
    except KeyboardInterrupt:
        print("\nKeyboard interrupt detected. Will exit after the current runLoop1 finishes.")
        stop_requested = True
        # The loop will check stop_requested after the current runLoop1 and exit.

if __name__ == "__main__":
    main()