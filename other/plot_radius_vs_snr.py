"""
Plot coverage radius R (km) vs required SNR (dB) using TR 38.901 UMa LOS piecewise path-loss,
with axes swapped (R on x, SNR on y) and common MCS markers plotted as dots.

Usage:
    python plot_radius_vs_snr.py

Adjust parameters in the "Parameters" section as needed.
"""
import numpy as np
import matplotlib.pyplot as plt

# -------------------------
# Parameters (adjustable)
# -------------------------
fc_GHz = 3.5           # carrier frequency in GHz (n78)
h_BS = 30.0            # base station height (m)
h_UT = 1.5             # UE height (m)
T_dBm = 8.7            # effective power per sub-carrier (dBm)
noise_density_dBmHz = -173.8
W_Hz = 15000.0
sigma2_dBm = noise_density_dBmHz + 10.0 * np.log10(W_Hz)  # total thermal noise in channel (dBm)
M_f = 5.0              # fading margin (dB)
M_mu = 2.0             # interference degradation margin (dB)

# SNR plotting range (high to low)
snr_high = 32.0
snr_low = -2.1
num_points = 300
snr_list = np.linspace(snr_high, snr_low, num_points)

# Typical (approximate) MCS SNR thresholds (adjust as desired)
# These are approximate required SNRs in dB for a target BLER and implementation; tune for your system.
mcs_thresholds = {
    "256-QAM": 32.0,
    "64-QAM": 20.0,
    "16-QAM": 12.0,
    "QPSK": 0.0,
}

# -------------------------
# Derived path-loss constants
# -------------------------
c = 3e8                # speed of light (m/s)
C = 32.4 + 20.0 * np.log10(fc_GHz)   # dB, for f in GHz, d in metres
lambda_m = c / (fc_GHz * 1e9)        # wavelength (m)
d_bp_m = 4.0 * h_BS * h_UT / lambda_m  # breakpoint distance (m)
PL_bp = C + 20.0 * np.log10(d_bp_m)    # PL at the breakpoint (dB)

def compute_radius_for_Lmax(L_max_dB):
    """
    Solve for R (meters) given L_max using piecewise LOS PL (TR 38.901 UMa LOS form).
    Returns R in meters.
    """
    if L_max_dB <= PL_bp:
        # pre-breakpoint branch
        R_m = 10 ** ((L_max_dB - C) / 20.0)
    else:
        # post-breakpoint branch
        R_m = 10 ** ((L_max_dB - C + 20.0 * np.log10(d_bp_m)) / 40.0)
    return R_m

# Compute R for each SNR in the scan
R_m_list = []
for snr in snr_list:
    L_max = T_dBm - (snr + sigma2_dBm + M_f + M_mu)
    R_m = compute_radius_for_Lmax(L_max)
    R_m_list.append(R_m)

R_km_list = np.array(R_m_list) / 1000.0

# Compute R for MCS thresholds and collect for plotting
mcs_R_km = {}
for mcs, snr_thresh in mcs_thresholds.items():
    L_max_mcs = T_dBm - (snr_thresh + sigma2_dBm + M_f + M_mu)
    R_mcs = compute_radius_for_Lmax(L_max_mcs)
    mcs_R_km[mcs] = (R_mcs / 1000.0, snr_thresh)

# -------------------------
# Plotting (swapped axes: R on x, SNR on y)
# -------------------------
plt.figure(figsize=(9,6))
plt.plot(R_km_list, snr_list, lw=2, label='Required SNR for radius R')

# Plot MCS thresholds as dots and annotate
for mcs, (R_km, snr_thresh) in mcs_R_km.items():
    plt.scatter([R_km], [snr_thresh], s=80, label=f'{mcs} (SNR={snr_thresh} dB)')
    # Annotate slightly offset to avoid overlap
    xoff = 0.02 * max(R_km_list)  # offset proportional to plot width
    plt.annotate(mcs, (R_km + xoff, snr_thresh), va='center', fontsize=9)

# Mark breakpoint as a vertical line
plt.axvline(d_bp_m/1000.0, color='grey', ls='--', lw=1.2, label=f'breakpoint d_bp = {d_bp_m/1000.0:.2f} km')

plt.xlabel('Coverage radius R (km)')
plt.ylabel('Required SNR (dB)')
plt.title(f'Coverage radius vs Required SNR (UMa LOS, f={fc_GHz} GHz)')
plt.grid(True)
plt.legend(loc='best', fontsize=9)

# Place the axes so high SNR is at the top (invert y-axis)
plt.gca().invert_yaxis()

plt.tight_layout()
plt.savefig('radius_vs_snr_swapped_axes.png', dpi=300)

# Optionally save to file:
# plt.savefig('radius_vs_snr_swapped_axes.png', dpi=300)

# Print a small summary
print(f"fc = {fc_GHz} GHz, h_BS = {h_BS} m, h_UT = {h_UT} m")
print(f"C = {C:.3f} dB, lambda = {lambda_m:.6f} m, d_bp = {d_bp_m:.1f} m ({d_bp_m/1000.0:.3f} km)")
print(f"PL(d_bp) = {PL_bp:.3f} dB")
for mcs, (R_km, snr) in mcs_R_km.items():
    print(f"{mcs}: SNR={snr} dB -> R = {R_km:.3f} km")