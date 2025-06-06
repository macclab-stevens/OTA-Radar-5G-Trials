#!/usr/bin/python3
import uhd
import numpy as np
import scipy.signal
import argparse

PLATFORM = "b200"
SERIAL = "31577EF"

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
    return parser.parse_args()

def main():
    args = parse_args()
    # UHD device address string with platform and serial
    usrp_addr = f"type={PLATFORM},serial={SERIAL}"
    usrp = uhd.usrp.MultiUSRP(usrp_addr)
    center_freq = args.center_freq
    sample_rate = args.sample_rate
    gain = args.gain
    pulse_width = args.pulse_width
    prf = args.prf
    pri = 1 / prf
    total_duration = args.total_duration
    num_pulses = int(total_duration / pri)
    f0 = args.chirp_f0
    bw = args.bw
    f1 = f0 + bw
    chop_samples = args.chop_samples

    pulse_samples = int(pulse_width * sample_rate)
    pri_samples = int(pri * sample_rate)
    print(f"Pulse duration (s): {pulse_width}")
    print(f"PRI (s): {pri}")
    print(f"PRF (Hz): {prf}")
    print(f"Number of pulses: {num_pulses}")
    print(f"Number of samples per pulse: {pulse_samples}")

    t = np.linspace(0, pulse_width, pulse_samples, endpoint=False)
    k = (f1 - f0) / pulse_width
    phase = 2 * np.pi * (f0 * t + 0.5 * k * t**2)
    window = scipy.signal.windows.hann(pulse_samples)
    single_pulse = np.exp(1j * phase) * window
    single_pulse = single_pulse.astype(np.complex64)

    if chop_samples > 0:
        single_pulse = single_pulse[:-chop_samples]
    pulse_samples_chopped = len(single_pulse)
    print(f"Number of samples per pulse after chopping: {pulse_samples_chopped}")

    waveform = np.zeros(pri_samples * num_pulses, dtype=np.complex64)
    for i in range(num_pulses):
        start = i * pri_samples
        waveform[start:start + pulse_samples_chopped] = single_pulse

    print(f"Total waveform duration (s): {len(waveform) / sample_rate}")

    usrp.set_tx_bandwidth(f1, 0)
    usrp.send_waveform(waveform, len(waveform) / sample_rate, center_freq, sample_rate, [0], gain)

if __name__ == "__main__":
    main()