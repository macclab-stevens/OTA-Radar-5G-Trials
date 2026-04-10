#!/usr/bin/python3
import uhd
import numpy as np
import scipy.signal
import argparse
import time

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

    # Configure USRP
    usrp.set_tx_bandwidth(bw * 1.2, 0)
    usrp.set_clock_source("external")
    usrp.set_tx_rate(sample_rate, 0)
    usrp.set_tx_freq(uhd.libpyuhd.types.tune_request(center_freq), 0)
    usrp.set_tx_gain(gain, 0)

    # Verify actual vs requested sample rate
    actual_rate = usrp.get_tx_rate()
    rate_error = abs(actual_rate - sample_rate) / sample_rate * 100
    print(f"\n=== Sample Rate Health ===")
    print(f"Requested sample rate: {sample_rate / 1e6:.3f} MHz")
    print(f"Actual sample rate:    {actual_rate / 1e6:.3f} MHz")
    print(f"Rate error:            {rate_error:.4f}%")
    if rate_error > 0.01:
        print("WARNING: Sample rate mismatch > 0.01% — check clock source")

    # Set up TX streamer
    st_args = uhd.usrp.StreamArgs("fc32", "sc16")
    st_args.channels = [0]
    tx_streamer = usrp.get_tx_stream(st_args)
    max_samps_per_buf = tx_streamer.get_max_num_samps()

    # TX metadata for first packet (timed burst)
    tx_md = uhd.types.TXMetadata()
    tx_md.has_time_spec = True
    tx_md.time_spec = usrp.get_time_now() + uhd.types.TimeSpec(0.2)
    tx_md.start_of_burst = True
    tx_md.end_of_burst = False

    # Transmit waveform in chunks
    total_sent = 0
    send_errors = 0
    print(f"\n=== Transmitting ({len(waveform)} samples) ===")
    tx_start_time = time.time()

    while total_sent < len(waveform):
        remaining = len(waveform) - total_sent
        chunk_size = min(remaining, max_samps_per_buf)
        chunk = waveform[total_sent:total_sent + chunk_size]

        # Mark end of burst on last chunk
        if total_sent + chunk_size >= len(waveform):
            tx_md.end_of_burst = True

        num_sent = tx_streamer.send(chunk, tx_md)
        if num_sent != len(chunk):
            send_errors += 1
            print(f"  WARNING: Sent {num_sent}/{len(chunk)} samples at offset {total_sent}")

        total_sent += num_sent

        # After first packet, clear time spec
        tx_md.has_time_spec = False
        tx_md.start_of_burst = False

    tx_elapsed = time.time() - tx_start_time

    # === UHD Async Metadata Validation (EVENT_CODE_BURST_ACK) ===
    print(f"\n=== TX Validation ===")
    print(f"Samples sent:    {total_sent} / {len(waveform)}")
    print(f"Send errors:     {send_errors}")
    print(f"TX elapsed time: {tx_elapsed:.3f}s")

    async_md = uhd.types.TXAsyncMetadata()
    burst_ack_received = False
    timeout = 2.0
    got_msg = tx_streamer.recv_async_msg(async_md, timeout)

    event_codes = uhd.libpyuhd.types.tx_metadata_event_code
    if got_msg:
        event = async_md.event_code
        if event == event_codes.burst_ack:
            burst_ack_received = True
            print("BURST_ACK received — burst was transmitted successfully")
        elif event == event_codes.underflow:
            print("WARNING: UNDERFLOW — host did not stream data fast enough")
        elif event == event_codes.seq_error:
            print("WARNING: SEQ_ERROR — packet loss between host and device")
        elif event == event_codes.time_error:
            print("WARNING: TIME_ERROR — packet had time that was late")
        elif event == event_codes.underflow_in_packet:
            print("WARNING: UNDERFLOW_IN_PACKET — underflow mid-packet")
        elif event == event_codes.seq_error_in_packet:
            print("WARNING: SEQ_ERROR_IN_PACKET — sequence error within burst")
        else:
            print(f"WARNING: Unexpected async event code: {event}")
    else:
        print("WARNING: No async message received within timeout — could not confirm TX")

    # === Buffer Health Summary ===
    expected_duration = len(waveform) / sample_rate
    throughput = total_sent / tx_elapsed if tx_elapsed > 0 else 0
    print(f"\n=== Buffer Health ===")
    print(f"Expected waveform duration: {expected_duration:.3f}s")
    print(f"Host TX throughput:         {throughput / 1e6:.3f} Msps")
    print(f"Required throughput:        {sample_rate / 1e6:.3f} Msps")
    if throughput < sample_rate * 0.95:
        print("WARNING: Host throughput below 95% of sample rate — risk of underflows")
    else:
        print("Host throughput OK")

    if burst_ack_received and send_errors == 0:
        print("\n=== TX RESULT: SUCCESS ===")
    else:
        print("\n=== TX RESULT: CHECK WARNINGS ABOVE ===")

if __name__ == "__main__":
    main()