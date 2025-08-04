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

#Files
gnbConfigDFLT ="/home/eric/srsRAN_Project/configs/00101__gnb_rf_b200_tdd_n78_20mhz.yml"
gnbConfigRadar ="/home/eric/srsRAN_Project/configs/radar_00101__gnb_rf_b200_tdd_n78_20mhz.yml"


#ADB Commands
apEnable = 'adb shell cmd connectivity airplane-mode enable' #enable AP Mode. Make UE go IDLE
apDisable = 'adb shell cmd connectivity airplane-mode disable' #disable AP Mode. Make UE Connect!

#Bash Commands 
# export GNB_CONFIG_PATH_DEFAULT="/home/eric/srsRAN_Project/configs/00101__gnb_rf_b200_tdd_n78_20mhz.yml"
# export GNB_CONFIG_PATH_RADAR="/home/eric/srsRAN_Project/configs/radar_00101__gnb_rf_b200_tdd_n78_20mhz.yml"
gnbStart = 'systemctl --user start gnb_radar.service'
gnbStat = 'systemctl --user status gnb_radar.service'
gnbStop = 'systemctl --user stop gnb_radar.service'
iperfStart = 'iperf3 -p 5201 -c 10.45.0.2 -b 60M -t 0 --logfile /tmp/iperf3.log &'
iperfStop = 'pkill -f iperf3'

#set Default Radar Params
radarData = {
        "prf": 3000,  # Initial PRF value
        "gain": 60,
        "cFreq": 3410.1e6,
        "PW": 100e-6,
        "T": 20,
        "bw": 2e6,
        "sampRate": 20e6
    }

# Add this flag near the top of your script
collectingGNBLogs = False  # Set to True to enable log collection and processing

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

def collectLogs(radarData, iperfStart_T,folder ):
    LogNametimestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    iperf_dst = os.path.join(folder, f"{LogNametimestamp}_iperf3.log")
    gnb_dst = os.path.join(folder, f"{LogNametimestamp}_gnb.log")
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
    """
    LogNametimestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    iperf_dst = "/tmp/iperf3.log"  # Assuming iperf3.log is already copied to /tmp
    gnb_dst = "/tmp/gnb.log"  # Assuming gnb.log is already copied to /tmp
    # try:
    #     shutil.copy("/tmp/iperf3.log", iperf_dst)
    #     print(f"Saved /tmp/iperf3.log as {iperf_dst}")
    #     os.remove("/tmp/iperf3.log")
    # except Exception as e:
    #     print(f"Could not copy /tmp/iperf3.log: {e}")
    # try:
    #     shutil.copy("/tmp/gnb.log", gnb_dst)
    #     print(f"Saved /tmp/gnb.log as {gnb_dst}")
    # except Exception as e:
    #     print(f"Could not copy /tmp/gnb.log: {e}")

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

    # Process the just-copied gnb.log file to generate CSVs
    try:
        prefix = LogNametimestamp
        out_dir = folder
        gnb_log = gnb_dst
        iperf_log = iperf_dst
        log_proc_cmd = [
            "python3",
            "/home/eric/OTA-Radar-5G-Trials/LogProcessing.py",
            "--gnb-log", gnb_log,
            "--iperf-log", iperf_log,
            "--out-dir", out_dir,
            "--prefix", prefix
        ]
        print(f"Processing logs with: {' '.join(log_proc_cmd)}")
        subprocess.run(log_proc_cmd, check=True)
    except Exception as e:
        print(f"Could not process logs with LogProcessing.py: {e}")

def readGnbConfig(config_path):
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config

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

def set_log_dir():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_dir = f"./logs_{timestamp}"
    os.makedirs(log_dir, exist_ok=True)
    return log_dir

def main():
    print("Main")
    cfg = readGnbConfig(gnbConfigRadar)
    UE = AndroidController()
    stop_requested = False

    # Prepare sweep parameters
    gain_values = list(range(30, 100, 1))  # 30 to 99 inclusive
    prf = 3000  # Fixed PRF for all runs
    n_repeats = 3  # Number of times to repeat each gain
    total_runs = len(gain_values) * n_repeats
    run_count = 0
    run_durations = []

    try:
        for repeat in range(n_repeats):
            for gain in gain_values:
                radarData.update({'gain': gain, 'prf': prf})
                run_count += 1

                # Estimate time
                if run_durations:
                    avg_duration = sum(run_durations) / len(run_durations)
                    runs_left = total_runs - run_count + 1
                    est_remaining = avg_duration * runs_left
                    est_end_time = datetime.now() + timedelta(seconds=est_remaining)
                    hours = int(est_remaining // 3600)
                    minutes = int((est_remaining % 3600) // 60)
                    seconds = int(est_remaining % 60)
                    print(f"\nRun {run_count}/{total_runs} | PRF: {prf}, Gain: {gain}")
                    if hours > 0:
                        print(f"Estimated time left: {hours} hr {minutes} min {seconds} sec")
                    else:
                        print(f"Estimated time left: {minutes} min {seconds} sec")
                    print(f"Estimated end time: {est_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
                else:
                    print(f"\nRun {run_count}/{total_runs} | PRF: {prf}, Gain: {gain}")
                    print("Estimating time after first run...")

                print(f"PRF: {radarData.get('prf')}, Gain: {radarData.get('gain')}, CFreq: {radarData.get('cFreq')}, PW: {radarData.get('PW')}")

                start_time = time.time()
                runLoop1(UE, radarData, cfg, set_log_dir())
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