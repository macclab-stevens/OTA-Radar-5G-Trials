#!/usr/bin/python3
import uhd
import numpy as np
import scipy.signal
import argparse

PLATFORM = "b200"
SERIAL = "31577FF"

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--center-freq', type=float, default=3410.1e6, help='Center frequency (Hz)')
    parser.add_argument('--sample-rate', type=float, default=20e6, help='Sample rate (Hz)')
    parser.add_argument('--gain', type=float, default=60, help='TX gain (dB)')
    parser.add_argument('--pulse-width', type=float, default=100e-6, help='Pulse width (seconds)')
    parser.add_argument('--prf', type=float, default=1000, help='Pulse repetition frequency (Hz)')
    parser.add_argument('--total-duration', type=float, default=10, help='Total transmission duration (seconds)')
    parser.add_argument('--chirp-f0', type=float, default=0e6, help='Chirp start frequency (Hz)')
    parser.add_argument('--bw', type=float, default=5e6, help='Chirp bandwidth (Hz)')
    parser.add_argument('--chop-samples', type=int, default=0, help='Number of IQ samples to chop off each pulse')
    parser.add_argument('--duty-cycle', type=float, default=None, help='Duty cycle (0-1). Overrides pulse-width if set.')
    return parser.parse_args()

def main():
    args = parse_args()
    
    # UHD device address string with platform and serial
    usrp_addr = f"type={PLATFORM},serial={SERIAL}"
    usrp = uhd.usrp.MultiUSRP(usrp_addr)
    
    center_freq = args.center_freq
    sample_rate = args.sample_rate
    gain = args.gain
    total_duration = args.total_duration
    prf = args.prf
    pri = 1 / prf
    f0 = args.chirp_f0
    bw = args.bw
    f1 = f0 + bw
    chop_samples = args.chop_samples
    
    # Calculate pulse width based on duty cycle if specified
    if args.duty_cycle is not None:
        if not 0 < args.duty_cycle <= 1:
            raise ValueError("Duty cycle must be between 0 and 1")
        pulse_width = args.duty_cycle * pri
        print(f"Using duty cycle: {args.duty_cycle * 100:.1f}%")
    else:
        pulse_width = args.pulse_width
        duty_cycle = pulse_width / pri
        print(f"Calculated duty cycle: {duty_cycle * 100:.1f}%")
    
    # Warn if duty cycle is very close to or exceeds 100%
    if pulse_width >= pri:
        print("WARNING: Pulse width >= PRI (100% duty cycle). This is continuous transmission.")
        pulse_width = pri  # Cap at PRI to avoid overlap
    
    num_pulses = int(total_duration / pri)
    pulse_samples = int(pulse_width * sample_rate)
    pri_samples = int(pri * sample_rate)
    
    print("=== Radar Transmission Parameters ===")
    print(f"Center Frequency (Hz): {center_freq}")
    print(f"Sample Rate (Hz): {sample_rate}")
    print(f"Gain (dB): {gain}")
    print(f"Pulse Width (s): {pulse_width}")
    print(f"Pulse Width (µs): {pulse_width * 1e6}")
    print(f"PRI (s): {pri}")
    print(f"PRF (Hz): {prf}")
    print(f"Duty Cycle (%): {(pulse_width / pri) * 100:.2f}")
    print(f"Number of pulses: {num_pulses}")
    print(f"Chirp start freq (Hz): {f0}")
    print(f"Chirp bandwidth (Hz): {bw}")
    print(f"Chirp end freq (Hz): {f1}")
    print(f"Samples per pulse: {pulse_samples}")
    print(f"Samples per PRI: {pri_samples}")

    # Generate chirp pulse
    t = np.linspace(0, pulse_width, pulse_samples, endpoint=False)
    k = (f1 - f0) / pulse_width  # Chirp rate
    phase = 2 * np.pi * (f0 * t + 0.5 * k * t**2)
    
    # Apply window (for <100% duty cycle to reduce spectral leakage)
    if pulse_width < pri * 0.99:  # Not continuous
        window = scipy.signal.windows.hann(pulse_samples)
    else:  # Continuous (100% duty cycle) - no windowing needed
        window = np.ones(pulse_samples)
        print("100% duty cycle detected - no windowing applied")
    
    single_pulse = np.exp(1j * phase) * window
    single_pulse = single_pulse.astype(np.complex64)

    # Chop samples if requested
    if chop_samples > 0:
        single_pulse = single_pulse[:-chop_samples]
    pulse_samples_chopped = len(single_pulse)
    print(f"Samples per pulse after chopping: {pulse_samples_chopped}")

    # Build full waveform
    waveform = np.zeros(pri_samples * num_pulses, dtype=np.complex64)
    for i in range(num_pulses):
        start = i * pri_samples
        waveform[start:start + pulse_samples_chopped] = single_pulse

    print(f"Total waveform duration (s): {len(waveform) / sample_rate}")
    print(f"Total waveform samples: {len(waveform)}")

    # Transmit
    usrp.set_tx_bandwidth(f1, 0)
    usrp.set_clock_source("external")
    usrp.send_waveform(waveform, len(waveform) / sample_rate, center_freq, sample_rate, [0], gain)
    print("Transmission complete!")

if __name__ == "__main__":
    main()