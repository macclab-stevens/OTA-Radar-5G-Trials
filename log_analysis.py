import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

folder = "processed_logs_20250805"

prf_to_dlbrate = {}

def normalize_brate(val):
    logging.debug(f"Normalizing bitrate value: {val}")
    if pd.isna(val):
        return None
    val = str(val).strip()
    if val.endswith('Mbps'):
        try:
            return float(val.replace('Mbps', '').strip())
        except ValueError:
            logging.warning(f"Could not convert {val} to float (Mbps).")
            return None
    elif val.endswith('kbps'):
        try:
            return float(val.replace('kbps', '').strip()) / 1000.0
        except ValueError:
            logging.warning(f"Could not convert {val} to float (kbps).")
            return None
    else:
        try:
            return float(val)
        except ValueError:
            logging.warning(f"Could not convert {val} to float (unknown unit).")
            return None

for fname in os.listdir(folder):
    if fname.endswith("_radar_config.csv"):
        path = os.path.join(folder, fname)
        logging.info(f"Processing radar config: {path}")
        df = pd.read_csv(path)
        if 'prf' in df.columns:
            matches = df[df['gain'] == 90]
            if not matches.empty:
                prf = matches.iloc[0]['prf']
                metrics_fname = fname.replace("_radar_config.csv", "_metrics.csv")
                metrics_path = os.path.join(folder, metrics_fname)
                if os.path.exists(metrics_path):
                    logging.info(f"Processing metrics: {metrics_path}")
                    metrics_df = pd.read_csv(metrics_path)
                    if 'total_dl_brate' in metrics_df.columns:
                        normalized = metrics_df['total_dl_brate'].dropna().apply(normalize_brate)
                        prf_to_dlbrate.setdefault(prf, []).extend(normalized.dropna().tolist())

# Build DataFrame for plotting
plot_data = []
for prf in sorted(prf_to_dlbrate.keys()):
    for brate in prf_to_dlbrate[prf]:
        plot_data.append({'prf': prf, 'total_dl_brate': brate})
plot_df = pd.DataFrame(plot_data)

logging.info("Data to be plotted:")
print(plot_df)

# Plot
plt.figure(figsize=(10,6))
for prf in sorted(prf_to_dlbrate.keys()):
    y = sorted(prf_to_dlbrate[prf])
    x = [prf] * len(y)
    plt.scatter(x, y, s=10, alpha=0.5, color='tab:blue')

plt.xlabel("PRF")
plt.ylabel("total_dl_brate")
plt.title("DL Brate per PRF for gain=90")
plt.gca().yaxis.set_major_locator(ticker.MultipleLocator(10))  # or another interval that fits your data
plt.tick_params(axis='y', which='major', labelsize=10)         # ensures labels are shown
plt.grid(True, which='major', axis='y')
plt.savefig("prfVbrate_gain90_v2.png")
# plt.show()

