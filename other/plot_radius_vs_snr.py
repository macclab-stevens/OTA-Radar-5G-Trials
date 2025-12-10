"""
Plot coverage radius R (km) vs required SNR (dB) using TR 38.901 UMa LOS piecewise path-loss,
with editable top-of-file parameters (defaults taken from your table/discussion).

How to use:
 - Edit parameters in the "User-configurable parameters" section below.
 - Run: python plot_radius_vs_snr.py
 - The script computes derived values (EiRP, per-subcarrier power, noise, L_max) and
   uses the TR 38.901 UMa LOS piecewise PL (breakpoint form).
 - The plot has R on the x-axis (km) and required SNR on the y-axis (dB). Common MCS
   thresholds are plotted as dots (editable).

Notes:
 - Units: frequencies in GHz at the path-loss formulas, convert to Hz where needed.
 - Distances in metres for the PL equations; radii printed/saved are in km.
"""
import numpy as np
import matplotlib.pyplot as plt

# -------------------------
# User-configurable parameters (edit here)
# -------------------------
# Transmitter / RF chain
tx_total_power_dBm = 37.0      # total transmit power (dBm)
tx_antenna_gain_dBi = 13.0     # transmit antenna gain (dBi)
tx_cable_loss_dB = 3         # cable/connector/combiner losses (dB)

# Per-subcarrier / bandwidth
D_sub = 3276#100Mhz = 273PRBs * 12sc = 3276  600                    # number of subcarriers (used to get per-subcarrier power)
subcarrier_bandwidth_Hz = 30000.0

# Receiver
rx_antenna_gain_dBi = 0.0      # receive antenna gain (dBi)
rx_noise_figure_dB = 7.0       # receiver noise figure (dB)
penetration_loss_dB = 0     # penetration loss (dB) if applicable

# Margins
fading_margin_dB = 5.0
interference_margin_dB = 2.0

# Thermal noise
noise_density_dBm_per_Hz = -173.8  # dBm/Hz

# TR 38.901 / geometry (UMa LOS)
fc_GHz = 3.5                   # carrier frequency in GHz (n78)
h_BS = 30.0                    # base station antenna height (m)
h_UT = 1.5                     # UE antenna height (m)

# SNR plotting range and resolution
snr_max_dB = 32.0              # highest SNR to plot (dB), e.g., 256-QAM region
snr_min_dB = -2.1              # lowest SNR to plot (dB) (table value)
num_snr_points = 300

# Common MCS approximate thresholds (editable)
mcs_thresholds_dB = {
    "256-QAM": 32.0,
    "64-QAM": 20.0,
    "16-QAM": 12.0,
    "QPSK": 0.0,
}

# Plot / output options
save_png = True
output_png = "5g_radius_vs_snr.png"

# -------------------------
# Derived / calculated parameters (do not edit below unless you know what you do)
# -------------------------
# Effective isotropic radiated power (EiRP) and per-subcarrier power
eirp_dBm = tx_total_power_dBm + tx_antenna_gain_dBi - tx_cable_loss_dB
per_subcarrier_power_dBm = eirp_dBm - 10.0 * np.log10(D_sub)  # per-subcarrier dBm

# Noise power in channel (per subcarrier)
sigma2_dBm = noise_density_dBm_per_Hz + 10.0 * np.log10(subcarrier_bandwidth_Hz)

# Total gains/losses at receiver side for the effective receive power per subcarrier
# Following earlier table convention: T = per_subcarrier_power + G_R - (noise_figure + penetration_loss)
total_rx_gains_dB = rx_antenna_gain_dBi
total_rx_losses_dB = rx_noise_figure_dB + penetration_loss_dB
T_eff_dBm = per_subcarrier_power_dBm + total_rx_gains_dB - total_rx_losses_dB

# Path-loss constants (TR form)
c = 3e8  # m/s
lambda_m = c / (fc_GHz * 1e9)
C_PL = 32.4 + 20.0 * np.log10(fc_GHz)  # PL constant (f in GHz, d in metres)
d_bp_m = 4.0 * h_BS * h_UT / lambda_m
PL_at_bp = C_PL + 20.0 * np.log10(d_bp_m)

# Prepare SNR sweep
snr_list = np.linspace(snr_max_dB, snr_min_dB, num_snr_points)

def solve_radius_for_Lmax(L_max_dB):
    """
    Solve for coverage radius R (metres) given L_max (dB) using TR piecewise UMa LOS PL.
    Uses d_3D ≈ d_2D for the radial solution (slant effect negligible for typical macros).
    """
    # Determine branch by comparing L_max to PL at breakpoint
    if L_max_dB <= PL_at_bp:
        # pre-breakpoint (20*log10 slope)
        R_m = 10.0 ** ((L_max_dB - C_PL) / 20.0)
    else:
        # post-breakpoint (40*log10 slope)
        R_m = 10.0 ** ((L_max_dB - C_PL + 20.0 * np.log10(d_bp_m)) / 40.0)
    return R_m

# Compute radius for each SNR
R_m_list = np.zeros_like(snr_list)
for i, snr in enumerate(snr_list):
    L_max = T_eff_dBm - (snr + sigma2_dBm + fading_margin_dB + interference_margin_dB)
    R_m_list[i] = solve_radius_for_Lmax(L_max)

R_km_list = R_m_list / 1000.0

# Compute MCS marker points
mcs_points = {}
for mcs_name, snr_thr in mcs_thresholds_dB.items():
    L_max_mcs = T_eff_dBm - (snr_thr + sigma2_dBm + fading_margin_dB + interference_margin_dB)
    R_mcs = solve_radius_for_Lmax(L_max_mcs)
    mcs_points[mcs_name] = (R_mcs / 1000.0, snr_thr)

# -------------------------
# Plot (swapped axes: R on x, SNR on y)
# -------------------------
plt.figure(figsize=(10, 6))
plt.plot(R_km_list, snr_list, lw=2, label='Required SNR for radius R (median, no shadowing)')

# MCS markers
marker_colors = ["C1", "C2", "C3", "C4"]
for (mcs_name, (R_km, snr_thr)), col in zip(mcs_points.items(), marker_colors):
    plt.scatter([R_km], [snr_thr], color=col, s=80, zorder=10, label=f'{mcs_name}: SNR={snr_thr} dB')
    # annotate
    x_off = 0.02 * max(R_km_list) if max(R_km_list) > 0 else 0.01
    plt.annotate(mcs_name, (R_km + x_off, snr_thr), va='center', fontsize=9)

# Breakpoint vertical line
plt.axvline(d_bp_m / 1000.0, color='grey', ls='--', lw=1.2, label=f'breakpoint d_bp = {d_bp_m/1000.0:.2f} km')

plt.xlabel('Coverage radius R (km)')
plt.ylabel('Required SNR (dB)')
plt.title(f'Coverage Radius vs Required SNR (UMa LOS, f={fc_GHz} GHz)')
plt.grid(True)
plt.legend(loc='best', fontsize=9)
plt.gca().invert_yaxis()  # high SNR at top
plt.tight_layout()

if save_png:
    plt.savefig(output_png, dpi=300)
else:
    plt.show()

# -------------------------
# Summary printout (helpful numbers)
# -------------------------
print("==== Configuration / Derived values ====")
print(f"tx_total_power_dBm = {tx_total_power_dBm:.2f} dBm")
print(f"tx_antenna_gain_dBi = {tx_antenna_gain_dBi:.2f} dBi")
print(f"tx_cable_loss_dB = {tx_cable_loss_dB:.2f} dB")
print(f"EiRP (dBm) = {eirp_dBm:.2f} dBm")
print(f"Per-subcarrier power (dBm) = {per_subcarrier_power_dBm:.2f} dBm (D_sub={D_sub})")
print(f"Noise power sigma2 (dBm) = {sigma2_dBm:.2f} dBm (W={subcarrier_bandwidth_Hz} Hz)")
print(f"Receiver total gains (dB) = {total_rx_gains_dB:.2f} dB")
print(f"Receiver total losses (dB) = {total_rx_losses_dB:.2f} dB")
print(f"Effective per-subcarrier transmit T (dBm) = {T_eff_dBm:.2f} dBm")
print(f"C_PL = {C_PL:.3f} dB, lambda = {lambda_m:.6f} m")
print(f"d_bp = {d_bp_m:.1f} m ({d_bp_m/1000.0:.3f} km), PL(d_bp) = {PL_at_bp:.3f} dB")
print("---- MCS thresholds -> radius ----")
for mcs, (R_km, snr_thr) in mcs_points.items():
    print(f"{mcs:8s}: SNR={snr_thr:+5.1f} dB -> R = {R_km:6.3f} km")

# Example: radius for the table SNR default
table_snr = snr_min_dB
L_max_table = T_eff_dBm - (table_snr + sigma2_dBm + fading_margin_dB + interference_margin_dB)
R_table_m = solve_radius_for_Lmax(L_max_table)
print(f"\nExample table SNR = {table_snr} dB -> L_max = {L_max_table:.3f} dB -> R = {R_table_m/1000.0:.3f} km")