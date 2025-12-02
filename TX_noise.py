#!/usr/bin/python3
import uhd
import numpy as np
import argparse
import time
import os
from scipy import signal

PLATFORM = "b200"
SERIAL = "31577EF"
CONTROL_FILE = "/tmp/usrp_gain.txt"

def parse_args():
    parser = argparse.ArgumentParser(description="USRP SGWN Noise Generator with Dynamic Gain Control")
    parser.add_argument('--center-freq', type=float, default=3410.1e6, help='Center frequency (Hz)')
    parser.add_argument('--sample-rate', type=float, default=20e6, help='Sample rate (Hz)')
    parser.add_argument('--gain', type=float, default=60, help='Initial TX gain (dB)')
    parser.add_argument('--block-duration', type=float, default=0.1, help='Block duration for each transmission (seconds)')
    parser.add_argument('--noise-power', type=float, default=1.0, help='Noise power (linear, not dB)')
    parser.add_argument('--infinite', action='store_true', help='Run indefinitely until Ctrl+C')
    return parser.parse_args()

def get_gain_from_file(default_gain):
    """Read gain from control file, return default if file doesn't exist or is invalid"""
    try:
        if os.path.exists(CONTROL_FILE):
            with open(CONTROL_FILE, 'r') as f:
                content = f.read().strip()
                if content:  # Check if file is not empty
                    gain = float(content)
                    return gain
    except (ValueError, IOError) as e:
        print(f"Error reading gain file: {e}")
    return default_gain

def write_initial_control_file(initial_gain):
    """Create initial control file with default gain"""
    try:
        with open(CONTROL_FILE, 'w') as f:
            f.write(str(initial_gain))
        print(f"Control file created: {CONTROL_FILE}")
        print(f"Change gain by writing new value to this file (e.g., echo '50' > {CONTROL_FILE})")
    except IOError:
        print(f"Warning: Could not create control file {CONTROL_FILE}")

def main():
    args = parse_args()
    usrp_addr = f"type={PLATFORM},serial={SERIAL}"
    usrp = uhd.usrp.MultiUSRP(usrp_addr)
    center_freq = args.center_freq
    sample_rate = args.sample_rate
    initial_gain = args.gain
    block_duration = args.block_duration
    noise_power = args.noise_power
    current_gain = initial_gain
    
    # Create initial control file
    write_initial_control_file(current_gain)
    
    # Configure USRP once
    print(f"Configuring USRP: center_freq={center_freq}, sample_rate={sample_rate}, initial_gain={initial_gain}, tx_bandwidth=20e6")
    usrp.set_tx_bandwidth(20e6, 0)  # Set TX bandwidth to 20 MHz
    usrp.set_tx_gain(current_gain, 0)  # Set initial TX gain
    
    block_samples = int(block_duration * sample_rate)
    print(f"Block duration: {block_duration}s ({block_samples} samples)")
    print(f"Starting continuous noise transmission. Use Ctrl+C to stop.")
    print(f"Change gain dynamically: echo 'NEW_GAIN' > {CONTROL_FILE}")
    
    # Pre-generate noise with 20 MHz bandwidth
    # Generate white noise and then filter to 20 MHz BW
    oversample_factor = 2  # Oversample for filtering
    oversample_rate = sample_rate * oversample_factor
    oversample_samples = block_samples * oversample_factor
    
    # Generate oversampled white noise
    noise_oversample = (np.random.normal(0, np.sqrt(noise_power/2), oversample_samples) +
                       1j * np.random.normal(0, np.sqrt(noise_power/2), oversample_samples)).astype(np.complex64)
    
    # Design low-pass filter for 20 MHz bandwidth
    from scipy import signal
    nyquist = oversample_rate / 2
    cutoff = 10e6  # 10 MHz (half of 20 MHz BW)
    normalized_cutoff = cutoff / nyquist
    
    # Design and apply filter
    b, a = signal.butter(6, normalized_cutoff, btype='low', analog=False)
    noise_filtered = signal.filtfilt(b, a, noise_oversample)
    
    # Decimate back to original sample rate
    noise = noise_filtered[::oversample_factor].astype(np.complex64)
    
    print(f"Generated 20 MHz bandwidth noise (filtered and decimated)")
    print(f"Noise samples per block: {len(noise)}")
    
    try:
        print("Starting continuous transmission with repeated blocks...")
        
        # Use shorter duration but repeat in a loop for dynamic gain control
        # This allows us to check for gain updates between transmissions
        short_duration = 0.1  # 100ms blocks
        
        print("Continuous transmission started. Monitoring for gain changes...")
        
        while True:
            # Check for gain updates before each transmission block
            new_gain = get_gain_from_file(current_gain)
            if new_gain != current_gain:
                print(f"Gain change detected: {current_gain} -> {new_gain} dB")
                current_gain = new_gain
                usrp.set_tx_gain(current_gain, 0)  # Update USRP gain
                print(f"USRP TX gain updated to: {current_gain} dB")
            
            # Send waveform for short duration (will repeat automatically if duration > waveform length)
            usrp.send_waveform(noise, short_duration, center_freq, sample_rate, [0], current_gain)
            
    except KeyboardInterrupt:
        print("\nStopping transmission...")
        # Clean up control file
        try:
            os.remove(CONTROL_FILE)
            print(f"Removed control file: {CONTROL_FILE}")
        except OSError:
            pass

if __name__ == "__main__":
    main()
