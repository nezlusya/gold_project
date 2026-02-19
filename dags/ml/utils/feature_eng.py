import pandas as pd

def add_lags(df, n_lags=12):
    df = df.copy()
    for i in range(1, n_lags + 1):
        df[f"lag_{i}"] = df["y"].shift(i)
    return df.dropna()
