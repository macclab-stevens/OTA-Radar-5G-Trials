import pandas as pd

df1 = pd.read_csv("/home/eric/OTA-Radar-5G-Trials/logs_20250807_221955/20250807_222044_metrics.csv")
print(df1.head())
print(df1["total_dl_brate"])

df2 = pd.read_csv("/home/eric/OTA-Radar-5G-Trials/logs_20250807_225305/20250807_225354_metrics.csv")
print(df2.head())
print(df2["total_dl_brate"])

df3 = df1