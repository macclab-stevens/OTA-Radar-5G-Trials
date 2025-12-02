import numpy as np
from scipy.signal import chirp

class CustomFMWaveform:
    def __init__(self, sample_rate, pulse_width, num_pulses, prf, freq_mod, freq_offset=0, num_samples=None, output_format='pulses'):
        self.sample_rate = sample_rate
        self.pulse_width = pulse_width
        self.num_pulses = num_pulses
        self.prf = prf
        self.freq_mod = freq_mod  # e.g., ('linear', f0, f1)
        self.freq_offset = freq_offset
        self.num_samples = num_samples
        self.output_format = output_format

    def generate(self):
        pulse_samples = int(self.pulse_width * self.sample_rate)
        if self.num_samples is None:
            self.num_samples = pulse_samples * self.num_pulses
        t = np.arange(0, self.pulse_width, 1/self.sample_rate)
        if self.freq_mod[0] == 'linear':
            f0, f1 = self.freq_mod[1], self.freq_mod[2]
            pulse = chirp(t, f0=f0+self.freq_offset, f1=f1+self.freq_offset, t1=self.pulse_width, method='linear')
        else:
            raise NotImplementedError("Only linear FM supported")
        pulses = np.zeros((self.num_pulses, pulse_samples))
        for i in range(self.num_pulses):
            pulses[i, :] = pulse
        if self.output_format.lower() == 'pulses':
            return pulses
        else:
            return pulses.flatten()[:self.num_samples]

# Example usage:
# fm = CustomFMWaveform(sample_rate=1e6, pulse_width=1e-3, num_pulses=10, prf=1e3, freq_mod=('linear', 0, 1e5), freq_offset=1e3)
# waveform = fm.generate()